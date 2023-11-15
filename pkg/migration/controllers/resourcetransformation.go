package controllers

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/libopenstorage/stork/drivers/volume"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	coreops "github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var pathRegexp = regexp.MustCompile(`^([a-zA-Z_/][a-zA-Z0-9_/]*(\[[0-9]+\])?\.)*[a-zA-Z_/][a-zA-Z0-9_/]*$`)

const (
	// ResourceTransformationControllerName of resource transformation CR handler
	ResourceTransformationControllerName = "resource-transformation-controller"
)

// NewResourceTransformation creates a new instance of ResourceTransformation Manager
func NewResourceTransformation(mgr manager.Manager, d volume.Driver, r record.EventRecorder, rc resourcecollector.ResourceCollector) *ResourceTransformationController {
	return &ResourceTransformationController{
		client:            mgr.GetClient(),
		recorder:          r,
		resourceCollector: rc,
	}
}

// ResourceTransformationController controller to watch over ResourceTransformation CR
type ResourceTransformationController struct {
	client runtimeclient.Client

	resourceCollector resourcecollector.ResourceCollector
	recorder          record.EventRecorder
}

// Init initialize the resource transformation controller
func (r *ResourceTransformationController) Init(mgr manager.Manager) error {
	err := r.createCRD()
	if err != nil {
		return err
	}

	return controllers.RegisterTo(mgr, ResourceTransformationControllerName, r, &stork_api.ResourceTransformation{})
}

// Reconcile manages ResourceTransformation resources.
func (r *ResourceTransformationController) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	resourceTransformation := &stork_api.ResourceTransformation{}
	err := r.client.Get(context.TODO(), request.NamespacedName, resourceTransformation)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	if !controllers.ContainsFinalizer(resourceTransformation, controllers.FinalizerCleanup) {
		controllers.SetFinalizer(resourceTransformation, controllers.FinalizerCleanup)
		return reconcile.Result{Requeue: true}, r.client.Update(context.TODO(), resourceTransformation)
	}

	if err = r.handle(context.TODO(), resourceTransformation); err != nil {
		logrus.Errorf("%s: %s/%s: %s", reflect.TypeOf(r), resourceTransformation.Namespace, resourceTransformation.Name, err)
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}

func getTransformNamespace(ns string) string {
	return StorkNamespacePrefix + "-" + ns
}
func (r *ResourceTransformationController) handle(ctx context.Context, transform *stork_api.ResourceTransformation) error {
	var err error
	if transform.DeletionTimestamp != nil {
		if transform.GetFinalizers() != nil {
			controllers.RemoveFinalizer(transform, controllers.FinalizerCleanup)
			return r.client.Update(ctx, transform)
		}

		return nil
	}
	switch transform.Status.Status {
	case stork_api.ResourceTransformationStatusInitial:
		err = r.validateSpecPath(transform)
		if err != nil {
			message := fmt.Sprintf("Unsupported resource for resource transformation found: %v", err)
			log.TransformLog(transform).Errorf(message)
			r.recorder.Event(transform,
				v1.EventTypeWarning,
				string(stork_api.ResourceTransformationStatusFailed),
				message)
			transform.Status.Status = stork_api.ResourceTransformationStatusFailed
			err := r.client.Update(ctx, transform)
			if err != nil {
				return err
			}
			return nil
		}
		transform.Status.Status = stork_api.ResourceTransformationStatusInProgress
		if err = r.client.Update(ctx, transform); err != nil {
			return err
		}
	case stork_api.ResourceTransformationStatusInProgress:
		err = r.validateTransformResource(ctx, transform)
		if err != nil {
			message := fmt.Sprintf("Error validating resource transformation specs: %v", err)
			log.TransformLog(transform).Errorf(message)
			r.recorder.Event(transform,
				v1.EventTypeWarning,
				string(stork_api.ResourceTransformationStatusFailed),
				message)
			transform.Status.Status = stork_api.ResourceTransformationStatusFailed
			err := r.client.Update(ctx, transform)
			if err != nil {
				return err
			}
		}
	case stork_api.ResourceTransformationStatusReady:
	case stork_api.ResourceTransformationStatusFailed:
		return nil
	default:
		log.TransformLog(transform).Errorf("Invalid status for ResourceTransformation: %v", transform.Status.Status)
	}
	return nil
}

func (r *ResourceTransformationController) validateSpecPath(transform *stork_api.ResourceTransformation) error {
	for _, spec := range transform.Spec.Objects {
		_, _, kind, err := getGVK(spec.Resource)
		if err != nil {
			return err
		}
		if !resourcecollector.GetSupportedK8SResources(kind, []string{}) {
			return fmt.Errorf("unsupported resource kind for transformation: %s", kind)
		}
		for _, path := range spec.Paths {
			// TODO: this can be validated via CRDs as well, when we have defined schema
			// for stork crds
			// https://portworx.atlassian.net/browse/PWX-26465
			if path.Operation == stork_api.JsonResourcePatch {
				return fmt.Errorf("json patch for resources is not supported, operation: %s", path.Operation)
			}
			if !(path.Operation == stork_api.AddResourcePath || path.Operation == stork_api.DeleteResourcePath ||
				path.Operation == stork_api.ModifyResourcePathValue) {
				return fmt.Errorf("unsupported resource patch operation given for kind :%s, operation: %s", kind, path.Operation)
			}
			if !(path.Type == stork_api.BoolResourceType || path.Type == stork_api.IntResourceType ||
				path.Type == stork_api.StringResourceType || path.Type == stork_api.SliceResourceType ||
				path.Type == stork_api.KeyPairResourceType) {
				return fmt.Errorf("unsupported type for resource %s, path %s, type: %s", kind, path.Path, path.Type)
			}
			if !pathRegexp.MatchString(path.Path) {
				return fmt.Errorf("invalid path for resource %s, path %s, type: %s", kind, path.Path, path.Type)
			}
		}
	}
	log.TransformLog(transform).Infof("validated paths ")
	return nil
}

func (r *ResourceTransformationController) validateTransformResource(ctx context.Context, transform *stork_api.ResourceTransformation) error {
	resourceCollectorOpts := resourcecollector.Options{}
	config, err := clientcmd.BuildConfigFromFlags("", "")
	if err != nil {
		return err
	}
	localInterface, err := dynamic.NewForConfig(config)
	if err != nil {
		return err
	}
	localOps, err := coreops.NewForConfig(config)
	if err != nil {
		r.recorder.Event(transform,
			v1.EventTypeWarning,
			string(stork_api.ResourceTransformationStatusFailed),
			err.Error())
		return nil
	}

	// temp namespace to run dry-run of transformed resource option
	remoteTempNamespace := getTransformNamespace(transform.Namespace)
	ns := &v1.Namespace{}
	ns.Name = remoteTempNamespace
	_, err = localOps.CreateNamespace(ns)
	if err != nil && !errors.IsAlreadyExists(err) {
		message := fmt.Sprintf("Unable to create resource transformation namespace: %v", err)
		log.TransformLog(transform).Errorf(message)
		r.recorder.Event(transform,
			v1.EventTypeWarning,
			string(stork_api.ResourceTransformationStatusFailed),
			message)
		transform.Status.Status = stork_api.ResourceTransformationStatusFailed
		err := r.client.Update(ctx, transform)
		if err != nil {
			return err
		}
		return nil
	}

	ruleset := resourcecollector.GetDefaultRuleSet()

	for _, spec := range transform.Spec.Objects {
		group, version, kind, err := getGVK(spec.Resource)
		if err != nil {
			return fmt.Errorf("invalid resource type should be in format <group>/<version>/<kind>, actual: %s", spec.Resource)
		}
		resource := metav1.APIResource{
			Name:       strings.ToLower(ruleset.Pluralize(strings.ToLower(kind))),
			Kind:       kind,
			Version:    version,
			Namespaced: true,
			Group:      group,
		}
		objects, _, err := r.resourceCollector.GetResourcesForType(
			resource,
			nil,
			[]string{transform.Namespace},
			spec.Selectors,
			nil,
			nil,
			false,
			resourceCollectorOpts,
		)
		logrus.Debugf("Objects %v", objects)
		if err != nil {
			r.recorder.Event(transform,
				v1.EventTypeWarning,
				string(stork_api.ResourceTransformationStatusFailed),
				fmt.Sprintf("Error getting resource kind:%s, err: %v", kind, err))
			log.TransformLog(transform).Errorf("Error getting resources kind:%s, err: %v", kind, err)
			return err
		}
		for _, object := range objects.Items {
			logrus.Debugf("Objects Item %v", object)

			metadata, err := meta.Accessor(object)
			if err != nil {
				log.TransformLog(transform).Errorf("Unable to read metadata for resource %v, err: %v", kind, err)
				return err
			}
			resInfo := &stork_api.TransformResourceInfo{
				Name:             metadata.GetName(),
				Namespace:        metadata.GetNamespace(),
				GroupVersionKind: metav1.GroupVersionKind(object.GetObjectKind().GroupVersionKind()),
				Specs:            spec,
			}
			logrus.Debugf("resInfo %v", resInfo)
			if err := resourcecollector.TransformResources(object, []stork_api.TransformResourceInfo{*resInfo}, metadata.GetName(), metadata.GetNamespace()); err != nil {
				log.TransformLog(transform).Errorf("Unable to transform resource: %s/%s having gvk:%v with error: %v", resInfo.Namespace, resInfo.Name, resInfo.GroupVersionKind, err)
				resInfo.Status = stork_api.ResourceTransformationStatusFailed
				resInfo.Reason = err.Error()
				return err
			}
			unstructured, ok := object.(*unstructured.Unstructured)
			if !ok {
				return fmt.Errorf("unable to cast object to unstructured: %v", object)
			}
			resource := &metav1.APIResource{
				Name:       strings.ToLower(ruleset.Pluralize(strings.ToLower(kind))),
				Namespaced: len(metadata.GetNamespace()) > 0,
			}
			dynamicClient := localInterface.Resource(
				object.GetObjectKind().GroupVersionKind().GroupVersion().WithResource(resource.Name)).Namespace(getTransformNamespace(transform.Namespace))

			unstructured.SetNamespace(getTransformNamespace(transform.Namespace))
			log.TransformLog(transform).Infof("Applying object %s, %s",
				object.GetObjectKind().GroupVersionKind().Kind,
				metadata.GetName())
			_, err = dynamicClient.Create(context.TODO(), unstructured, metav1.CreateOptions{DryRun: []string{"All"}})
			if err != nil {
				log.TransformLog(transform).Errorf("Error while dryrun of resource: %s/%s having gvk:%v with error: %v", resInfo.Namespace, resInfo.Name, resInfo.GroupVersionKind, err)
				resInfo.Status = stork_api.ResourceTransformationStatusFailed
				resInfo.Reason = err.Error()
			} else {
				log.TransformLog(transform).Infof("DryRun is successfull for resource: %s/%s having gvk:%v ", resInfo.Namespace, resInfo.Name, resInfo.GroupVersionKind)
				resInfo.Status = stork_api.ResourceTransformationStatusReady
				resInfo.Reason = ""
			}
			logrus.Debugf("resInfo %v", resInfo)
			transform.Status.Resources = append(transform.Status.Resources, resInfo)
		}
	}

	transform.Status.Status = stork_api.ResourceTransformationStatusReady
	// verify if all resource dry-run is successful
	for _, resource := range transform.Status.Resources {
		if resource.Status != stork_api.ResourceTransformationStatusReady {
			transform.Status.Status = stork_api.ResourceTransformationStatusFailed
		}
	}

	if err := localOps.DeleteNamespace(remoteTempNamespace); err != nil {
		// log & generate event, but lets not fail resource transformation if
		// transformation controller could not delete temp namespace created
		message := fmt.Sprintf("Unable to delete resource transformation namespace %s: %v", ns.Name, err)
		log.TransformLog(transform).Errorf(message)
		r.recorder.Event(transform,
			v1.EventTypeWarning,
			string(stork_api.ResourceTransformationStatusFailed),
			message)
	}
	return r.client.Update(ctx, transform)
}

// return group,version,kind from give resource type
func getGVK(resource string) (string, string, string, error) {
	gvk := strings.Split(resource, "/")
	if len(gvk) != 3 {
		return "", "", "", fmt.Errorf("invalid resource kind :%s", resource)
	}
	return gvk[0], gvk[1], gvk[2], nil
}

func (c *ResourceTransformationController) createCRD() error {
	resource := apiextensions.CustomResource{
		Name:    stork_api.ResourceTransformationResourceName,
		Plural:  stork_api.ResourceTransformationResourcePlural,
		Group:   stork_api.SchemeGroupVersion.Group,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.ResourceTransformation{}).Name(),
	}
	ok, err := version.RequiresV1Registration()
	if err != nil {
		return err
	}
	if ok {
		err := k8sutils.CreateCRDV1(resource)
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
		return apiextensions.Instance().ValidateCRD(resource.Plural+"."+resource.Group, validateCRDTimeout, validateCRDInterval)
	}
	err = apiextensions.Instance().CreateCRDV1beta1(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	return apiextensions.Instance().ValidateCRDV1beta1(resource, validateCRDTimeout, validateCRDInterval)
}
