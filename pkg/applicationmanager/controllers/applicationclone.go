package controllers

import (
	"context"
	"fmt"
	"reflect"

	"github.com/libopenstorage/stork/drivers/volume"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/libopenstorage/stork/pkg/rule"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	pvNamePrefix = "pvc-"
)

// NewApplicationClone create a new instance of ApplicationCloneController.
func NewApplicationClone(mgr manager.Manager, d volume.Driver, r record.EventRecorder, rc resourcecollector.ResourceCollector) *ApplicationCloneController {
	return &ApplicationCloneController{
		client:            mgr.GetClient(),
		volDriver:         d,
		recorder:          r,
		resourceCollector: rc,
	}
}

// ApplicationCloneController reconciles applicationclone objects
type ApplicationCloneController struct {
	client runtimeclient.Client

	volDriver         volume.Driver
	recorder          record.EventRecorder
	resourceCollector resourcecollector.ResourceCollector
	dynamicInterface  dynamic.Interface
	adminNamespace    string
}

// Init Initialize the application clone controller
func (a *ApplicationCloneController) Init(mgr manager.Manager, adminNamespace string) error {
	err := a.createCRD()
	if err != nil {
		return err
	}

	a.adminNamespace = adminNamespace
	if err := a.performRuleRecovery(); err != nil {
		logrus.Errorf("Failed to perform recovery for application clone rules: %v", err)
		return err
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error getting cluster config: %v", err)
	}

	a.dynamicInterface, err = dynamic.NewForConfig(config)
	if err != nil {
		return err
	}

	return controllers.RegisterTo(mgr, "application-clone-controller", a, &stork_api.ApplicationClone{})
}

func (a *ApplicationCloneController) setKind(snap *stork_api.ApplicationClone) {
	snap.Kind = "ApplicationClone"
	snap.APIVersion = stork_api.SchemeGroupVersion.String()
}

// performRuleRecovery terminates potential background commands running pods for
// all applicationClone objects
func (a *ApplicationCloneController) performRuleRecovery() error {
	applicationClones, err := storkops.Instance().ListApplicationClones(v1.NamespaceAll)
	if err != nil {
		logrus.Errorf("Failed to list all application clones during rule recovery: %v", err)
		return err
	}

	if applicationClones == nil {
		return nil
	}

	var lastError error
	for _, applicationClone := range applicationClones.Items {
		a.setKind(&applicationClone)
		err := rule.PerformRuleRecovery(&applicationClone)
		if err != nil {
			lastError = err
		}
	}
	return lastError
}

func (a *ApplicationCloneController) setDefaults(clone *stork_api.ApplicationClone) {
	if clone.Spec.ReplacePolicy == "" {
		clone.Spec.ReplacePolicy = stork_api.ApplicationCloneReplacePolicyRetain
	}
}

// Make sure the source namespaces exists and create the destination
// namespace if it doesn't exist
func (a *ApplicationCloneController) verifyNamespaces(clone *stork_api.ApplicationClone) error {
	_, err := core.Instance().GetNamespace(clone.Spec.SourceNamespace)
	if err != nil {
		return fmt.Errorf("error getting source namespace %v: %v", clone.Spec.SourceNamespace, err)
	}
	_, err = core.Instance().CreateNamespace(clone.Spec.DestinationNamespace, nil)
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("error creating destination namespace %v: %v", clone.Spec.DestinationNamespace, err)
	}
	return nil
}

// Reconcile updates for ApplicationClone objects.
func (a *ApplicationCloneController) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	logrus.Tracef("Reconciling ApplicationClone %s/%s", request.Namespace, request.Name)

	// Fetch the ApplicationBackup instance
	clone := &stork_api.ApplicationClone{}
	err := a.client.Get(context.TODO(), request.NamespacedName, clone)
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

	if !controllers.ContainsFinalizer(clone, controllers.FinalizerCleanup) {
		controllers.SetFinalizer(clone, controllers.FinalizerCleanup)
		return reconcile.Result{Requeue: true}, a.client.Update(context.TODO(), clone)
	}

	if err = a.handle(context.TODO(), clone); err != nil {
		logrus.Errorf("%s: %s/%s: %s", reflect.TypeOf(a), clone.Namespace, clone.Name, err)
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}

// Handle updates for ApplicationClone objects
func (a *ApplicationCloneController) handle(ctx context.Context, clone *stork_api.ApplicationClone) error {
	if clone.DeletionTimestamp != nil {
		if controllers.ContainsFinalizer(clone, controllers.FinalizerCleanup) {
			if err := a.deleteClone(clone); err != nil {
				logrus.Errorf("%s: cleanup: %s", reflect.TypeOf(a), err)
			}
		}

		if clone.GetFinalizers() != nil {
			controllers.RemoveFinalizer(clone, controllers.FinalizerCleanup)
			return a.client.Update(ctx, clone)
		}

		return nil
	}

	// Check whether namespace is allowed to be backed before each stage
	// Restrict clone to only the namespace that the object belongs
	// except for the namespace designated by the admin
	if !a.namespaceCloneAllowed(clone) {
		err := fmt.Errorf("application clone objects can only be created in the admin namespace (%v)", a.adminNamespace)
		log.ApplicationCloneLog(clone).Errorf(err.Error())
		a.recorder.Event(clone,
			v1.EventTypeWarning,
			string(stork_api.ApplicationCloneStatusFailed),
			err.Error())
		return nil
	}

	var terminationChannel chan bool
	var err error

	a.setDefaults(clone)
	switch clone.Status.Stage {
	case stork_api.ApplicationCloneStageInitial:
		err = a.verifyNamespaces(clone)
		if err != nil {
			log.ApplicationCloneLog(clone).Errorf(err.Error())
			a.recorder.Event(clone,
				v1.EventTypeWarning,
				string(stork_api.ApplicationCloneStatusFailed),
				err.Error())
			return nil
		}
		// Make sure the rules exist if configured
		if clone.Spec.PreExecRule != "" {
			_, err := storkops.Instance().GetRule(clone.Spec.PreExecRule, clone.Namespace)
			if err != nil {
				message := fmt.Sprintf("Error getting PreExecRule %v: %v", clone.Spec.PreExecRule, err)
				log.ApplicationCloneLog(clone).Errorf(message)
				a.recorder.Event(clone,
					v1.EventTypeWarning,
					string(stork_api.ApplicationCloneStatusFailed),
					message)
				return nil
			}
		}
		if clone.Spec.PostExecRule != "" {
			_, err := storkops.Instance().GetRule(clone.Spec.PostExecRule, clone.Namespace)
			if err != nil {
				message := fmt.Sprintf("Error getting PostExecRule %v: %v", clone.Spec.PostExecRule, err)
				log.ApplicationCloneLog(clone).Errorf(message)
				a.recorder.Event(clone,
					v1.EventTypeWarning,
					string(stork_api.ApplicationCloneStatusFailed),
					message)
				return nil
			}
		}
		fallthrough
	case stork_api.ApplicationCloneStagePreExecRule:
		terminationChannel, err = a.runPreExecRule(clone)
		if err != nil {
			message := fmt.Sprintf("Error running PreExecRule: %v", err)
			log.ApplicationCloneLog(clone).Errorf(message)
			a.recorder.Event(clone,
				v1.EventTypeWarning,
				string(stork_api.ApplicationCloneStatusFailed),
				message)
			clone.Status.Stage = stork_api.ApplicationCloneStageInitial
			clone.Status.Status = stork_api.ApplicationCloneStatusInitial
			err := a.client.Update(context.TODO(), clone)
			if err != nil {
				return err
			}
			return nil
		}
		fallthrough
	case stork_api.ApplicationCloneStageVolumes:
		err := a.cloneVolumes(clone, terminationChannel)
		if err != nil {
			message := fmt.Sprintf("Error cloning volumes: %v", err)
			log.ApplicationCloneLog(clone).Errorf(message)
			a.recorder.Event(clone,
				v1.EventTypeWarning,
				string(stork_api.ApplicationCloneStatusFailed),
				message)
			return nil
		}
	case stork_api.ApplicationCloneStageApplications:
		err := a.cloneResources(clone)
		if err != nil {
			message := fmt.Sprintf("Error cloning resources: %v", err)
			log.ApplicationCloneLog(clone).Errorf(message)
			a.recorder.Event(clone,
				v1.EventTypeWarning,
				string(stork_api.ApplicationCloneStatusFailed),
				message)
			return nil
		}

	case stork_api.ApplicationCloneStageFinal:
		// Do Nothing
		return nil
	default:
		log.ApplicationCloneLog(clone).Errorf("Invalid stage for clone: %v", clone.Status.Stage)
	}

	return nil
}

func (a *ApplicationCloneController) namespaceCloneAllowed(clone *stork_api.ApplicationClone) bool {
	// Restrict clones to only the namespace that the object belongs to
	// except for the namespace designated by the admin
	return clone.Namespace == a.adminNamespace
}

func (a *ApplicationCloneController) generateCloneVolumeNames(clone *stork_api.ApplicationClone) error {
	pvcList, err := core.Instance().GetPersistentVolumeClaims(clone.Spec.SourceNamespace, clone.Spec.Selectors)
	if err != nil {
		return fmt.Errorf("error getting list of volumes to clone: %v", err)
	}

	volumeInfos := make([]*stork_api.ApplicationCloneVolumeInfo, 0)
	for _, pvc := range pvcList.Items {
		if !a.volDriver.OwnsPVC(&pvc) {
			continue
		}
		volume, err := core.Instance().GetVolumeForPersistentVolumeClaim(&pvc)
		if err != nil {
			return fmt.Errorf("error getting volume for PVC: %v", err)
		}

		volumeInfo := &stork_api.ApplicationCloneVolumeInfo{
			PersistentVolumeClaim: pvc.Name,
			Volume:                volume,
			CloneVolume:           pvNamePrefix + string(uuid.NewUUID()),
			Status:                stork_api.ApplicationCloneStatusInProgress,
		}
		volumeInfos = append(volumeInfos, volumeInfo)
	}
	clone.Status.Volumes = volumeInfos
	return a.client.Update(context.TODO(), clone)
}

func (a *ApplicationCloneController) cloneVolumes(clone *stork_api.ApplicationClone, terminationChannel chan bool) error {
	defer func() {
		if terminationChannel != nil {
			terminationChannel <- true
		}
	}()

	// Generate volume names for the clone and persist it
	// If this hits an error the pre-exec rule will be aborted and retried since
	// the status hasn't been updated
	clone.Status.Stage = stork_api.ApplicationCloneStageVolumes
	if clone.Status.Volumes == nil {
		if err := a.generateCloneVolumeNames(clone); err != nil {
			return err
		}
		clone.Status.Status = stork_api.ApplicationCloneStatusInProgress
		if err := a.client.Update(context.TODO(), clone); err != nil {
			return err
		}
	}

	// Start clone of the volumes if it hasn't started yet
	if clone.Status.Stage == stork_api.ApplicationCloneStageVolumes &&
		clone.Status.Status == stork_api.ApplicationCloneStatusInProgress {
		if err := a.volDriver.CreateVolumeClones(clone); err != nil {
			return err
		}

		// Terminate any background rules that were started
		if terminationChannel != nil {
			terminationChannel <- true
			terminationChannel = nil
		}

		// Run any post exec rules once clone is triggered
		if clone.Spec.PostExecRule != "" {
			if err := a.runPostExecRule(clone); err != nil {
				message := fmt.Sprintf("Error running PostExecRule: %v", err)
				log.ApplicationCloneLog(clone).Errorf(message)
				a.recorder.Event(clone,
					v1.EventTypeWarning,
					string(stork_api.ApplicationCloneStatusFailed),
					message)

				clone.Status.Stage = stork_api.ApplicationCloneStageFinal
				clone.Status.FinishTimestamp = metav1.Now()
				clone.Status.Status = stork_api.ApplicationCloneStatusFailed
				err = a.client.Update(context.TODO(), clone)
				if err != nil {
					return err
				}
				return fmt.Errorf("%v", message)
			}
		}
	}

	// Skip checking status if no volumes are being cloned up
	if len(clone.Status.Volumes) != 0 {
		// Now check if there is any failure or success
		// TODO: On failure of one volume cancel other clones?
		for _, vInfo := range clone.Status.Volumes {
			if vInfo.Status == stork_api.ApplicationCloneStatusFailed {
				a.recorder.Event(clone,
					v1.EventTypeWarning,
					string(vInfo.Status),
					fmt.Sprintf("Error cloning volume %v: %v", vInfo.Volume, vInfo.Reason))
				clone.Status.Stage = stork_api.ApplicationCloneStageFinal
				clone.Status.FinishTimestamp = metav1.Now()
				clone.Status.Status = stork_api.ApplicationCloneStatusFailed
			} else if vInfo.Status == stork_api.ApplicationCloneStatusSuccessful {
				a.recorder.Event(clone,
					v1.EventTypeNormal,
					string(vInfo.Status),
					fmt.Sprintf("Volume %v cloned successfully", vInfo.Volume))
			}
		}
	}

	// If the clone hasn't failed move on to the next stage.
	if clone.Status.Status != stork_api.ApplicationCloneStatusFailed {
		clone.Status.Stage = stork_api.ApplicationCloneStageApplications
		clone.Status.Status = stork_api.ApplicationCloneStatusInProgress
		// Update the current state and then move on to cloning resources
		err := a.client.Update(context.TODO(), clone)
		if err != nil {
			return err
		}
		err = a.cloneResources(clone)
		if err != nil {
			message := fmt.Sprintf("Error cloning resources: %v", err)
			log.ApplicationCloneLog(clone).Errorf(message)
			a.recorder.Event(clone,
				v1.EventTypeWarning,
				string(stork_api.ApplicationCloneStatusFailed),
				message)
			return err
		}
	}

	err := a.client.Update(context.TODO(), clone)
	if err != nil {
		return err
	}
	return nil
}

func (a *ApplicationCloneController) runPreExecRule(clone *stork_api.ApplicationClone) (chan bool, error) {
	if clone.Spec.PreExecRule == "" {
		clone.Status.Stage = stork_api.ApplicationCloneStageVolumes
		clone.Status.Status = stork_api.ApplicationCloneStatusPending
		err := a.client.Update(context.TODO(), clone)
		if err != nil {
			return nil, err
		}
		return nil, nil
	} else if clone.Status.Stage == stork_api.ApplicationCloneStageInitial {
		clone.Status.Stage = stork_api.ApplicationCloneStagePreExecRule
		clone.Status.Status = stork_api.ApplicationCloneStatusPending
	}

	if clone.Status.Stage == stork_api.ApplicationCloneStagePreExecRule {
		if clone.Status.Status == stork_api.ApplicationCloneStatusPending {
			clone.Status.Status = stork_api.ApplicationCloneStatusInProgress
			err := a.client.Update(context.TODO(), clone)
			if err != nil {
				return nil, err
			}
		} else if clone.Status.Status == stork_api.ApplicationCloneStatusInProgress {
			a.recorder.Event(clone,
				v1.EventTypeNormal,
				string(stork_api.ApplicationCloneStatusInProgress),
				fmt.Sprintf("Waiting for PreExecRule %v", clone.Spec.PreExecRule))
			return nil, nil
		}
	}
	r, err := storkops.Instance().GetRule(clone.Spec.PreExecRule, clone.Namespace)
	if err != nil {
		return nil, err
	}

	ch, err := rule.ExecuteRule(r, rule.PreExecRule, clone, clone.Spec.SourceNamespace)
	if err != nil {
		return nil, fmt.Errorf("error executing PreExecRule for namespace %v: %v", clone.Spec.SourceNamespace, err)
	}
	return ch, nil
}

func (a *ApplicationCloneController) runPostExecRule(clone *stork_api.ApplicationClone) error {
	r, err := storkops.Instance().GetRule(clone.Spec.PostExecRule, clone.Namespace)
	if err != nil {
		return err
	}

	_, err = rule.ExecuteRule(r, rule.PostExecRule, clone, clone.Spec.SourceNamespace)
	if err != nil {
		return fmt.Errorf("error executing PreExecRule for namespace %v: %v", clone.Namespace, err)
	}
	return nil
}

func (a *ApplicationCloneController) prepareResources(
	clone *stork_api.ApplicationClone,
	objects []runtime.Unstructured,
) ([]runtime.Unstructured, error) {
	tempObjects := make([]runtime.Unstructured, 0)
	pvNameMappings, err := a.getPVNameMappings(clone)
	if err != nil {
		return nil, err
	}

	namespaceMapping := make(map[string]string)
	namespaceMapping[clone.Spec.SourceNamespace] = clone.Spec.DestinationNamespace

	for _, o := range objects {
		if !a.resourceToBeCloned(o) {
			continue
		}

		metadata, err := meta.Accessor(o)
		if err != nil {
			return nil, err
		}

		switch o.GetObjectKind().GroupVersionKind().Kind {
		case "PersistentVolume":
			err := a.preparePVResource(o)
			if err != nil {
				return nil, fmt.Errorf("error preparing PV resource %v: %v", metadata.GetName(), err)
			}
		}
		_, err = a.resourceCollector.PrepareResourceForApply(
			o,
			namespaceMapping,
			pvNameMappings,
			clone.Spec.IncludeOptionalResourceTypes)
		if err != nil {
			return nil, err
		}
		tempObjects = append(tempObjects, o)
	}
	return tempObjects, nil
}

func (a *ApplicationCloneController) preparePVResource(
	object runtime.Unstructured,
) error {
	var pv v1.PersistentVolume
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &pv); err != nil {
		return err
	}

	_, err := a.volDriver.UpdateMigratedPersistentVolumeSpec(&pv)
	if err != nil {
		return err
	}

	o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pv)
	if err != nil {
		return err
	}
	object.SetUnstructuredContent(o)

	return err
}

func (a *ApplicationCloneController) getPVNameMappings(
	clone *stork_api.ApplicationClone,
) (map[string]string, error) {
	pvNameMappings := make(map[string]string)
	for _, vInfo := range clone.Status.Volumes {
		if vInfo.Volume == "" {
			return nil, fmt.Errorf("volume missing for clone")
		}
		if vInfo.CloneVolume == "" {
			return nil, fmt.Errorf("cloneVolume missing for clone")
		}
		pvNameMappings[vInfo.Volume] = vInfo.CloneVolume
	}
	return pvNameMappings, nil
}

func (a *ApplicationCloneController) updateResourceStatus(
	clone *stork_api.ApplicationClone,
	object runtime.Unstructured,
	status stork_api.ApplicationCloneStatusType,
	reason string,
) error {
	metadata, err := meta.Accessor(object)
	if err != nil {
		return err
	}

	resourceInfo := &stork_api.ApplicationCloneResourceInfo{
		Name:   metadata.GetName(),
		Status: status,
		Reason: reason,
	}
	gvk := object.GetObjectKind().GroupVersionKind()
	resourceInfo.Kind = gvk.Kind
	resourceInfo.Group = gvk.Group
	// core Group doesn't have a name, so override it
	if resourceInfo.Group == "" {
		resourceInfo.Group = "core"
	}
	resourceInfo.Version = gvk.Version

	if clone.Status.Resources == nil {
		clone.Status.Resources = make([]*stork_api.ApplicationCloneResourceInfo, 0)
	}

	eventType := v1.EventTypeNormal
	if status == stork_api.ApplicationCloneStatusFailed {
		eventType = v1.EventTypeWarning
	}

	eventMessage := fmt.Sprintf("%v %v: %v",
		gvk,
		resourceInfo.Name,
		reason)
	a.recorder.Event(clone, eventType, string(status), eventMessage)

	clone.Status.Resources = append(clone.Status.Resources, resourceInfo)
	return nil
}

func (a *ApplicationCloneController) resourceToBeCloned(
	object runtime.Unstructured,
) bool {
	switch object.GetObjectKind().GroupVersionKind().Kind {
	case "ClusterRole":
		return false
	default:
		return true
	}
}

func (a *ApplicationCloneController) applyResources(
	clone *stork_api.ApplicationClone,
	objects []runtime.Unstructured,
) error {
	namespaceMapping := make(map[string]string)
	namespaceMapping[clone.Spec.SourceNamespace] = clone.Spec.DestinationNamespace
	// First delete the existing objects if they exist and replace policy is set
	// to Delete
	if clone.Spec.ReplacePolicy == stork_api.ApplicationCloneReplacePolicyDelete {
		err := a.resourceCollector.DeleteResources(
			a.dynamicInterface,
			objects)
		if err != nil {
			return err
		}
	}

	for _, o := range objects {
		metadata, err := meta.Accessor(o)
		if err != nil {
			return err
		}
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return err
		}

		log.ApplicationCloneLog(clone).Infof("Applying %v %v", objectType.GetKind(), metadata.GetName())
		retained := false
		err = a.resourceCollector.ApplyResource(
			a.dynamicInterface,
			o)
		if err != nil && errors.IsAlreadyExists(err) {
			switch clone.Spec.ReplacePolicy {
			case stork_api.ApplicationCloneReplacePolicyDelete:
				log.ApplicationCloneLog(clone).Errorf("Error deleting %v %v during clone: %v", objectType.GetKind(), metadata.GetName(), err)
			case stork_api.ApplicationCloneReplacePolicyRetain:
				log.ApplicationCloneLog(clone).Warningf("Error deleting %v %v during clone, ReplacePolicy set to Retain: %v", objectType.GetKind(), metadata.GetName(), err)
				retained = true
				err = nil
			}
			if metadata.GetNamespace() == "" {
				retained = true
				err = nil
			}
		}

		if err != nil {
			if err := a.updateResourceStatus(
				clone,
				o,
				stork_api.ApplicationCloneStatusFailed,
				fmt.Sprintf("Error applying resource: %v", err)); err != nil {
				return err
			}
		} else if retained {
			if err := a.updateResourceStatus(
				clone,
				o,
				stork_api.ApplicationCloneStatusRetained,
				"Resource clone skipped as it was already present and ReplacePolicy is set to Retain"); err != nil {
				return err
			}
		} else {
			if err := a.updateResourceStatus(
				clone,
				o,
				stork_api.ApplicationCloneStatusSuccessful,
				fmt.Sprintf("Resource cloned successfully for namespace %v", clone.Spec.DestinationNamespace)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *ApplicationCloneController) cloneResources(
	clone *stork_api.ApplicationClone,
) error {
	allObjects, err := a.resourceCollector.GetResources(
		[]string{clone.Spec.SourceNamespace},
		clone.Spec.Selectors,
		clone.Spec.IncludeOptionalResourceTypes,
		false)
	if err != nil {
		log.ApplicationCloneLog(clone).Errorf("Error getting resources: %v", err)
		return err
	}

	// Do any additional preparation for the resources if required
	if allObjects, err = a.prepareResources(clone, allObjects); err != nil {
		a.recorder.Event(clone,
			v1.EventTypeWarning,
			string(stork_api.ApplicationCloneStatusFailed),
			fmt.Sprintf("Error preparing resource: %v", err))
		log.ApplicationCloneLog(clone).Errorf("Error preparing resources: %v", err)
		return err
	}

	if err = a.applyResources(clone, allObjects); err != nil {
		return err
	}

	clone.Status.Stage = stork_api.ApplicationCloneStageFinal
	clone.Status.FinishTimestamp = metav1.Now()
	clone.Status.Status = stork_api.ApplicationCloneStatusSuccessful
	for _, resource := range clone.Status.Resources {
		if resource.Status != stork_api.ApplicationCloneStatusSuccessful {
			clone.Status.Status = stork_api.ApplicationCloneStatusPartialSuccess
			break
		}
	}

	if err = a.client.Update(context.TODO(), clone); err != nil {
		return err
	}

	return nil
}

func (a *ApplicationCloneController) deleteClone(clone *stork_api.ApplicationClone) error {
	return nil
}

func (a *ApplicationCloneController) createCRD() error {
	resource := apiextensions.CustomResource{
		Name:    stork_api.ApplicationCloneResourceName,
		Plural:  stork_api.ApplicationCloneResourcePlural,
		Group:   stork_api.SchemeGroupVersion.Group,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.ApplicationClone{}).Name(),
	}
	err := apiextensions.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return apiextensions.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}
