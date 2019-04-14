package k8s

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	snap_v1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	k8s_ops "github.com/portworx/sched-ops/k8s"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/sirupsen/logrus"
	apps_api "k8s.io/api/apps/v1beta2"
	"k8s.io/api/core/v1"
	storage_api "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/scheme"
)

const (
	// SchedName is the name of the kubernetes scheduler driver implementation
	SchedName = "k8s"
	// SnapshotParent is the parameter key for the parent of a snapshot
	SnapshotParent = "snapshot_parent"
	k8sPodsRootDir = "/var/lib/kubelet/pods"
	// DeploymentSuffix is the suffix for deployment names stored as keys in maps
	DeploymentSuffix = "-dep"
	// StatefulSetSuffix is the suffix for statefulset names stored as keys in maps
	StatefulSetSuffix = "-ss"
	// SystemdSchedServiceName is the name of the system service resposible for scheduling
	// TODO Change this when running on openshift for the proper service name
	SystemdSchedServiceName = "kubelet"
)

const (
	statefulSetValidateTimeout   = 20 * time.Minute
	k8sNodeReadyTimeout          = 5 * time.Minute
	volDirCleanupTimeout         = 5 * time.Minute
	k8sObjectCreateTimeout       = 2 * time.Minute
	k8sDestroyTimeout            = 2 * time.Minute
	findFilesOnWorkerTimeout     = 1 * time.Minute
	deleteTasksWaitTimeout       = 3 * time.Minute
	defaultRetryInterval         = 10 * time.Second
	defaultTimeout               = 2 * time.Minute
	resizeSupportedAnnotationKey = "torpedo/resize-supported"
)

var (
	namespaceRegex = regexp.MustCompile("{{NAMESPACE}}")
)

type k8s struct {
	specFactory    *spec.Factory
	nodeDriverName string
}

func (k *k8s) IsNodeReady(n node.Node) error {
	t := func() (interface{}, bool, error) {
		if err := k8s_ops.Instance().IsNodeReady(n.Name); err != nil {
			return "", true, &scheduler.ErrNodeNotReady{
				Node:  n,
				Cause: err.Error(),
			}
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, k8sNodeReadyTimeout, defaultRetryInterval); err != nil {
		return err
	}

	return nil
}

// String returns the string name of this driver.
func (k *k8s) String() string {
	return SchedName
}

func (k *k8s) Init(specDir, volDriverName, nodeDriverName string) error {
	nodes, err := k8s_ops.Instance().GetNodes()
	if err != nil {
		return err
	}

	for _, n := range nodes.Items {
		newNode := k.parseK8SNode(n)
		if err := k.IsNodeReady(newNode); err != nil {
			return err
		}
		if err := node.AddNode(newNode); err != nil {
			return err
		}
	}

	k.specFactory, err = spec.NewFactory(specDir, k)
	if err != nil {
		return err
	}

	k.nodeDriverName = nodeDriverName
	return nil
}

func (k *k8s) RescanSpecs(specDir string) error {
	var err error
	logrus.Infof("Rescanning specs for %v", specDir)
	k.specFactory, err = spec.NewFactory(specDir, k)
	if err != nil {
		return err
	}
	return nil
}

func (k *k8s) ParseSpecs(specDir string) ([]interface{}, error) {
	fileList := []string{}
	if err := filepath.Walk(specDir, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			fileList = append(fileList, path)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	var specs []interface{}

	for _, fileName := range fileList {
		file, err := os.Open(fileName)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		reader := bufio.NewReader(file)
		specReader := yaml.NewYAMLReader(reader)

		for {
			specContents, err := specReader.Read()
			if err == io.EOF {
				break
			}

			if len(bytes.TrimSpace(specContents)) > 0 {
				obj, err := decodeSpec(specContents)
				if err != nil {
					logrus.Warnf("Error decoding spec from %v: %v", fileName, err)
					return nil, err
				}

				specObj, err := validateSpec(obj)
				if err != nil {
					logrus.Warnf("Error parsing spec from %v: %v", fileName, err)
					return nil, err
				}

				specs = append(specs, specObj)
			}
		}
	}

	return specs, nil
}

func decodeSpec(specContents []byte) (runtime.Object, error) {
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode([]byte(specContents), nil, nil)
	if err != nil {
		scheme := runtime.NewScheme()
		if err := snap_v1.AddToScheme(scheme); err != nil {
			return nil, err
		}

		if err := stork_api.AddToScheme(scheme); err != nil {
			return nil, err
		}

		codecs := serializer.NewCodecFactory(scheme)
		obj, _, err = codecs.UniversalDeserializer().Decode([]byte(specContents), nil, nil)
		if err != nil {
			return nil, err
		}
	}
	return obj, nil
}

func validateSpec(in interface{}) (interface{}, error) {
	if specObj, ok := in.(*apps_api.Deployment); ok {
		return specObj, nil
	} else if specObj, ok := in.(*apps_api.StatefulSet); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.Service); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.PersistentVolumeClaim); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storage_api.StorageClass); ok {
		return specObj, nil
	} else if specObj, ok := in.(*snap_v1.VolumeSnapshot); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.GroupVolumeSnapshot); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.Secret); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.ConfigMap); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.Rule); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.Pod); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.ClusterPair); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.Migration); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.MigrationSchedule); ok {
		return specObj, nil
	} else if specObj, ok := in.(*stork_api.SchedulePolicy); ok {
		return specObj, nil
	}

	return nil, fmt.Errorf("Unsupported object: %v", reflect.TypeOf(in))
}

func (k *k8s) getAddressesForNode(n v1.Node) []string {
	var addrs []string
	for _, addr := range n.Status.Addresses {
		if addr.Type == v1.NodeExternalIP || addr.Type == v1.NodeInternalIP {
			addrs = append(addrs, addr.Address)
		}
	}
	return addrs
}

func (k *k8s) parseK8SNode(n v1.Node) node.Node {
	var nodeType node.Type
	if k8s_ops.Instance().IsNodeMaster(n) {
		nodeType = node.TypeMaster
	} else {
		nodeType = node.TypeWorker
	}

	return node.Node{
		Name:      n.Name,
		Addresses: k.getAddressesForNode(n),
		Type:      nodeType,
	}
}

func (k *k8s) Schedule(instanceID string, options scheduler.ScheduleOptions) ([]*scheduler.Context, error) {
	var apps []*spec.AppSpec
	if len(options.AppKeys) > 0 {
		for _, key := range options.AppKeys {
			spec, err := k.specFactory.Get(key)
			if err != nil {
				return nil, err
			}
			apps = append(apps, spec)
		}
	} else {
		apps = k.specFactory.GetAll()
	}

	var contexts []*scheduler.Context
	for _, app := range apps {

		appNamespace := app.GetID(instanceID)
		specObjects, err := k.createSpecObjects(app, appNamespace)
		if err != nil {
			return nil, err
		}

		ctx := &scheduler.Context{
			UID: instanceID,
			App: &spec.AppSpec{
				Key:      app.Key,
				SpecList: specObjects,
				Enabled:  app.Enabled,
			},
		}

		contexts = append(contexts, ctx)
	}

	return contexts, nil
}

func (k *k8s) createSpecObjects(app *spec.AppSpec, namespace string) ([]interface{}, error) {
	var specObjects []interface{}
	ns, err := k.createNamespace(app, namespace)
	if err != nil {
		return nil, err
	}

	for _, spec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createMigrationObjects(spec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}
		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, defaultRetryInterval)
		if err != nil {
			return nil, err
		}
		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, spec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createStorageObject(spec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}

		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, defaultRetryInterval)
		if err != nil {
			return nil, err
		}

		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, spec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createCoreObject(spec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}

		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, defaultRetryInterval)
		if err != nil {
			return nil, err
		}

		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}
	return specObjects, nil
}

// AddTasks adds tasks to an existing context
func (k *k8s) AddTasks(ctx *scheduler.Context, options scheduler.ScheduleOptions) error {
	if ctx == nil {
		return fmt.Errorf("Context to add tasks to cannot be nil")
	}
	if len(options.AppKeys) == 0 {
		return fmt.Errorf("Need to specify list of applications to add to context")
	}

	appNamespace := ctx.GetID()
	var apps []*spec.AppSpec
	specObjects := ctx.App.SpecList
	for _, key := range options.AppKeys {
		spec, err := k.specFactory.Get(key)
		if err != nil {
			return err
		}
		apps = append(apps, spec)
	}
	for _, app := range apps {
		objects, err := k.createSpecObjects(app, appNamespace)
		if err != nil {
			return err
		}
		specObjects = append(specObjects, objects...)
	}
	ctx.App.SpecList = specObjects
	return nil
}

func (k *k8s) createNamespace(app *spec.AppSpec, namespace string) (*v1.Namespace, error) {
	k8sOps := k8s_ops.Instance()

	t := func() (interface{}, bool, error) {
		ns, err := k8sOps.CreateNamespace(namespace,
			map[string]string{
				"creater": "torpedo",
				"app":     app.Key,
			})

		if errors.IsAlreadyExists(err) {
			if ns, err = k8sOps.GetNamespace(namespace); err == nil {
				return ns, false, nil
			}
		}

		if err != nil {
			return nil, true, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create namespace: %v. Err: %v", namespace, err),
			}
		}

		return ns, false, nil
	}

	nsObj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, defaultRetryInterval)
	if err != nil {
		return nil, err
	}

	return nsObj.(*v1.Namespace), nil
}

func (k *k8s) createStorageObject(spec interface{}, ns *v1.Namespace, app *spec.AppSpec) (interface{}, error) {
	k8sOps := k8s_ops.Instance()
	if obj, ok := spec.(*storage_api.StorageClass); ok {
		obj.Namespace = ns.Name
		sc, err := k8sOps.CreateStorageClass(obj)
		if errors.IsAlreadyExists(err) {
			if sc, err = k8sOps.GetStorageClass(obj.Name); err == nil {
				logrus.Infof("[%v] Found existing storage class: %v", app.Key, sc.Name)
				return sc, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create storage class: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created storage class: %v", app.Key, sc.Name)
		return sc, nil

	} else if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
		obj.Namespace = ns.Name
		k.substituteNamespaceInPVC(obj, ns.Name)
		pvc, err := k8sOps.CreatePersistentVolumeClaim(obj)
		if errors.IsAlreadyExists(err) {
			if pvc, err = k8sOps.GetPersistentVolumeClaim(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing PVC: %v", app.Key, pvc.Name)
				return pvc, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create PVC: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created PVC: %v", app.Key, pvc.Name)
		return pvc, nil

	} else if obj, ok := spec.(*snap_v1.VolumeSnapshot); ok {
		obj.Metadata.Namespace = ns.Name
		snap, err := k8sOps.CreateSnapshot(obj)
		if errors.IsAlreadyExists(err) {
			if snap, err = k8sOps.GetSnapshot(obj.Metadata.Name, obj.Metadata.Namespace); err == nil {
				logrus.Infof("[%v] Found existing snapshot: %v", app.Key, snap.Metadata.Name)
				return snap, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Snapshot: %v. Err: %v", obj.Metadata.Name, err),
			}
		}

		logrus.Infof("[%v] Created Snapshot: %v", app.Key, snap.Metadata.Name)
		return snap, nil
	} else if obj, ok := spec.(*stork_api.GroupVolumeSnapshot); ok {
		obj.Namespace = ns.Name
		snap, err := k8sOps.CreateGroupSnapshot(obj)
		if errors.IsAlreadyExists(err) {
			if snap, err = k8sOps.GetGroupSnapshot(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing group snapshot: %v", app.Key, snap.Name)
				return snap, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create group snapshot: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created Group snapshot: %v", app.Key, snap.Name)
		return snap, nil
	}

	return nil, nil
}

func (k *k8s) substituteNamespaceInPVC(pvc *v1.PersistentVolumeClaim, ns string) {
	pvc.Name = namespaceRegex.ReplaceAllString(pvc.Name, ns)
	for k, v := range pvc.Annotations {
		pvc.Annotations[k] = namespaceRegex.ReplaceAllString(v, ns)
	}
}

func (k *k8s) createCoreObject(spec interface{}, ns *v1.Namespace, app *spec.AppSpec) (interface{}, error) {
	k8sOps := k8s_ops.Instance()
	if obj, ok := spec.(*apps_api.Deployment); ok {
		obj.Namespace = ns.Name
		obj.Spec.Template.Spec.Volumes = k.substituteNamespaceInVolumes(obj.Spec.Template.Spec.Volumes, ns.Name)
		dep, err := k8sOps.CreateDeployment(obj)
		if errors.IsAlreadyExists(err) {
			if dep, err = k8sOps.GetDeployment(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing deployment: %v", app.Key, dep.Name)
				return dep, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Deployment: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created deployment: %v", app.Key, dep.Name)
		return dep, nil

	} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
		obj.Namespace = ns.Name
		obj.Spec.Template.Spec.Volumes = k.substituteNamespaceInVolumes(obj.Spec.Template.Spec.Volumes, ns.Name)
		ss, err := k8sOps.CreateStatefulSet(obj)
		if errors.IsAlreadyExists(err) {
			if ss, err = k8sOps.GetStatefulSet(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing StatefulSet: %v", app.Key, ss.Name)
				return ss, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create StatefulSet: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created StatefulSet: %v", app.Key, ss.Name)
		return ss, nil

	} else if obj, ok := spec.(*v1.Service); ok {
		obj.Namespace = ns.Name
		svc, err := k8sOps.CreateService(obj)
		if errors.IsAlreadyExists(err) {
			if svc, err = k8sOps.GetService(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing Service: %v", app.Key, svc.Name)
				return svc, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Service: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created Service: %v", app.Key, svc.Name)
		return svc, nil

	} else if obj, ok := spec.(*v1.Secret); ok {
		obj.Namespace = ns.Name
		secret, err := k8sOps.CreateSecret(obj)
		if errors.IsAlreadyExists(err) {
			if secret, err = k8sOps.GetSecret(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing Secret: %v", app.Key, secret.Name)
				return secret, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Secret: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created Secret: %v", app.Key, secret.Name)
		return secret, nil
	} else if obj, ok := spec.(*stork_api.Rule); ok {
		obj.Namespace = ns.Name
		rule, err := k8sOps.CreateRule(obj)
		if errors.IsAlreadyExists(err) {
			if rule, err = k8sOps.GetRule(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing Rule: %v", app.Key, rule.GetName())
				return rule, nil
			}
		}

		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Rule: %v, Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created Rule: %v", app.Key, rule.GetName())
		return rule, nil
	} else if obj, ok := spec.(*v1.Pod); ok {
		obj.Namespace = ns.Name
		pod, err := k8sOps.CreatePod(obj)
		if errors.IsAlreadyExists(err) {
			if pod, err := k8sOps.GetPodByName(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing Pods: %v", app.Key, pod.Name)
				return pod, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToSchedulePod{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Pod: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created Pod: %v", app.Key, pod.Name)
		return pod, nil
	}

	return nil, nil
}

func (k *k8s) destroyCoreObject(spec interface{}, opts map[string]bool, app *spec.AppSpec) (interface{}, error) {
	k8sOps := k8s_ops.Instance()
	var pods interface{}
	var podList []*v1.Pod
	var err error
	if obj, ok := spec.(*apps_api.Deployment); ok {
		if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
			if pods, err = k8sOps.GetDeploymentPods(obj); err != nil {
				logrus.Warnf("[%s] Error getting deployment pods. Err: %v", app.Key, err)
			}
		}
		err := k8sOps.DeleteDeployment(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Deployment: %v. Err: %v", obj.Name, err),
			}
		}
	} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
		if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
			if pods, err = k8sOps.GetStatefulSetPods(obj); err != nil {
				logrus.Warnf("[%v] Error getting statefulset pods. Err: %v", app.Key, err)
			}
		}
		err := k8sOps.DeleteStatefulSet(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy stateful set: %v. Err: %v", obj.Name, err),
			}
		}
	} else if obj, ok := spec.(*v1.Service); ok {
		err := k8sOps.DeleteService(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Service: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Destroyed Service: %v", app.Key, obj.Name)
	} else if obj, ok := spec.(*stork_api.Rule); ok {
		err := k8sOps.DeleteRule(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Rule: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Destroyed Rule: %v", app.Key, obj.Name)
	} else if obj, ok := spec.(*v1.Pod); ok {
		if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
			pod, err := k8sOps.GetPodByName(obj.Name, obj.Namespace)
			if err != nil {
				logrus.Warnf("[%v] Error getting pods. Err: %v", app.Key, err)
			}
			podList = append(podList, pod)
			pods = podList
		}
		err := k8sOps.DeletePod(obj.Name, obj.Namespace, false)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyPod{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Pod: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Destroyed Pod: %v", app.Key, obj.Name)
	}

	return pods, nil

}

func (k *k8s) substituteNamespaceInVolumes(volumes []v1.Volume, ns string) []v1.Volume {
	var updatedVolumes []v1.Volume
	for _, vol := range volumes {
		if vol.VolumeSource.PersistentVolumeClaim != nil {
			claimName := namespaceRegex.ReplaceAllString(vol.VolumeSource.PersistentVolumeClaim.ClaimName, ns)
			vol.VolumeSource.PersistentVolumeClaim.ClaimName = claimName
		}
		updatedVolumes = append(updatedVolumes, vol)
	}
	return updatedVolumes
}

func (k *k8s) WaitForRunning(ctx *scheduler.Context, timeout, retryInterval time.Duration) error {
	k8sOps := k8s_ops.Instance()
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			if err := k8sOps.ValidateDeployment(obj, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Deployment: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated deployment: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			if err := k8sOps.ValidateStatefulSet(obj, timeout*time.Duration(*obj.Spec.Replicas)); err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated statefulset: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*v1.Service); ok {
			svc, err := k8sOps.GetService(obj.Name, obj.Namespace)
			if err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Service: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated Service: %v", ctx.App.Key, svc.Name)
		} else if obj, ok := spec.(*stork_api.Rule); ok {
			svc, err := k8sOps.GetRule(obj.Name, obj.Namespace)
			if err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Rule: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated Rule: %v", ctx.App.Key, svc.Name)
		} else if obj, ok := spec.(*v1.Pod); ok {
			if err := k8sOps.ValidatePod(obj, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidatePod{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Pod: [%s] %s. Err: Pod is not ready %v", obj.Namespace, obj.Name, obj.Status),
				}
			}

			logrus.Infof("[%v] Validated pod: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*stork_api.ClusterPair); ok {
			if err := k8sOps.ValidateClusterPair(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate cluster Pair: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated ClusterPair: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*stork_api.Migration); ok {
			if err := k8sOps.ValidateMigration(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate Migration: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated Migration: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*stork_api.MigrationSchedule); ok {
			if _, err := k8sOps.ValidateMigrationSchedule(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate MigrationSchedule: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated MigrationSchedule: %v", ctx.App.Key, obj.Name)
		} else {
			logrus.Infof("[%v] Skipping validate for %v", ctx.App.Key, reflect.TypeOf(spec))
		}
	}

	return nil
}

func (k *k8s) Destroy(ctx *scheduler.Context, opts map[string]bool) error {
	var podList []v1.Pod
	var pods interface{}
	var err error
	for _, spec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			currPods, err := k.destroyCoreObject(spec, opts, ctx.App)
			if err != nil {
				return nil, true, err
			}
			return currPods, false, nil
		}
		pods, err = task.DoRetryWithTimeout(t, k8sDestroyTimeout, defaultRetryInterval)
		if err != nil {
			podList = append(podList, pods.(v1.Pod))
		}
	}

	for _, spec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyMigrationObject(spec, ctx.App)
			if err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}
		pods, err = task.DoRetryWithTimeout(t, k8sDestroyTimeout, defaultRetryInterval)
		if err != nil {
			podList = append(podList, pods.(v1.Pod))
		}
	}

	if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
		if err = k.WaitForDestroy(ctx); err != nil {
			return err
		}
		if err = k.waitForCleanup(ctx, podList); err != nil {
			return err
		}
	} else if value, ok := opts[scheduler.OptionsWaitForDestroy]; ok && value {
		if err = k.WaitForDestroy(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (k *k8s) waitForCleanup(ctx *scheduler.Context, podList []v1.Pod) error {
	for _, pod := range podList {
		t := func() (interface{}, bool, error) {
			return nil, true, k.validateVolumeDirCleanup(pod.UID, ctx.App)
		}
		if _, err := task.DoRetryWithTimeout(t, volDirCleanupTimeout, defaultRetryInterval); err != nil {
			return err
		}
		logrus.Infof("Validated resource cleanup for pod: %v", pod.UID)
	}
	return nil
}

func (k *k8s) validateVolumeDirCleanup(podUID types.UID, app *spec.AppSpec) error {
	podVolDir := k.getVolumeDirPath(podUID)
	driver, _ := node.Get(k.nodeDriverName)
	options := node.FindOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         findFilesOnWorkerTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		},
		MinDepth: 1,
		MaxDepth: 1,
	}

	for _, n := range node.GetWorkerNodes() {
		if volDir, err := driver.FindFiles(podVolDir, n, options); err != nil {
			return err
		} else if strings.TrimSpace(volDir) != "" {
			return &scheduler.ErrFailedToDeleteVolumeDirForPod{
				App:   app,
				Cause: fmt.Sprintf("Volume directory for pod %v still exists in node: %v", podUID, n.Name),
			}
		}
	}

	return nil
}

func (k *k8s) getVolumeDirPath(podUID types.UID) string {
	return filepath.Join(k8sPodsRootDir, string(podUID), "volumes")
}

func (k *k8s) WaitForDestroy(ctx *scheduler.Context) error {
	k8sOps := k8s_ops.Instance()
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			if err := k8sOps.ValidateTerminatedDeployment(obj); err != nil {
				return &scheduler.ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of deployment: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated destroy of Deployment: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			if err := k8sOps.ValidateTerminatedStatefulSet(obj); err != nil {
				return &scheduler.ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of statefulset: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated destroy of StatefulSet: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*v1.Service); ok {
			if err := k8sOps.ValidateDeletedService(obj.Name, obj.Namespace); err != nil {
				return &scheduler.ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of service: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated destroy of Service: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*stork_api.Rule); ok {
			_, err := k8sOps.GetRule(obj.Name, obj.Namespace)
			if err == nil {
				return &scheduler.ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("stork rule: %v is still present.", obj.Name),
				}
			}

			if errors.IsNotFound(err) {
				logrus.Infof("[%v] Validated destroy of Rule: %v", ctx.App.Key, obj.Name)
			} else {
				return &scheduler.ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to validate destroy of stork rule: %v due to: %v", obj.Name, err),
				}
			}
		} else if obj, ok := spec.(*v1.Pod); ok {
			if err := k8sOps.WaitForPodDeletion(obj.UID, obj.Namespace, deleteTasksWaitTimeout); err != nil {
				return &scheduler.ErrFailedToValidatePodDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of pod: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated destroy of Pod: %v", ctx.App.Key, obj.Name)
		}
	}

	return nil
}

func (k *k8s) DeleteTasks(ctx *scheduler.Context) error {
	k8sOps := k8s_ops.Instance()
	pods, err := k.getPodsForApp(ctx)
	if err != nil {
		return &scheduler.ErrFailedToDeleteTasks{
			App:   ctx.App,
			Cause: fmt.Sprintf("failed to get pods due to: %v", err),
		}
	}

	if err := k8sOps.DeletePods(pods, false); err != nil {
		return &scheduler.ErrFailedToDeleteTasks{
			App:   ctx.App,
			Cause: fmt.Sprintf("failed to delete pods due to: %v", err),
		}
	}

	// Ensure the pods are deleted and removed from the system
	for _, pod := range pods {
		err = k8sOps.WaitForPodDeletion(pod.UID, pod.Namespace, deleteTasksWaitTimeout)
		if err != nil {
			logrus.Errorf("k8s DeleteTasks failed to wait for pod: [%s] %s to terminate. err: %v", pod.Namespace, pod.Name, err)
			return err
		}
	}

	return nil
}

func (k *k8s) GetVolumeParameters(ctx *scheduler.Context) (map[string]map[string]string, error) {
	k8sOps := k8s_ops.Instance()
	result := make(map[string]map[string]string)

	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			params, err := k8sOps.GetPersistentVolumeClaimParams(obj)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get params for volume: %v. Err: %v", obj.Name, err),
				}
			}

			pvc, err := k8sOps.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get PVC: %v. Err: %v", obj.Name, err),
				}
			}

			for k, v := range pvc.Annotations {
				params[k] = v
			}

			result[pvc.Spec.VolumeName] = params
		} else if obj, ok := spec.(*snap_v1.VolumeSnapshot); ok {
			snap, err := k8sOps.GetSnapshot(obj.Metadata.Name, obj.Metadata.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get Snapshot: %v. Err: %v", obj.Metadata.Name, err),
				}
			}

			snapDataName := snap.Spec.SnapshotDataName
			if len(snapDataName) == 0 {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("snapshot: [%s] %s does not have snapshotdata set", snap.Metadata.Namespace, snap.Metadata.Name),
				}
			}

			snapData, err := k8sOps.GetSnapshotData(snapDataName)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get volumesnapshotdata: %s due to: %v", snapDataName, err),
				}
			}

			if snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot == nil ||
				len(snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot.SnapshotID) == 0 {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("volumesnapshotdata: %s does not have portworx volume source set", snapDataName),
				}
			}

			result[snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot.SnapshotID] = map[string]string{
				SnapshotParent: snap.Spec.PersistentVolumeClaimName,
			}
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			pvcList, err := k8sOps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVCs for StatefulSet: %v. Err: %v", ss.Name, err),
				}
			}

			for _, pvc := range pvcList.Items {
				params, err := k8sOps.GetPersistentVolumeClaimParams(&pvc)
				if err != nil {
					return nil, &scheduler.ErrFailedToGetVolumeParameters{
						App:   ctx.App,
						Cause: fmt.Sprintf("failed to get params for volume: %v. Err: %v", pvc.Name, err),
					}
				}

				for k, v := range pvc.Annotations {
					params[k] = v
				}

				result[pvc.Spec.VolumeName] = params
			}
		}
	}

	return result, nil
}

func (k *k8s) InspectVolumes(ctx *scheduler.Context, timeout, retryInterval time.Duration) error {
	k8sOps := k8s_ops.Instance()
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*storage_api.StorageClass); ok {
			if _, err := k8sOps.GetStorageClass(obj.Name); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate StorageClass: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated storage class: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			if err := k8sOps.ValidatePersistentVolumeClaim(obj, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate PVC: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated PVC: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*snap_v1.VolumeSnapshot); ok {
			if err := k8sOps.ValidateSnapshot(obj.Metadata.Name, obj.Metadata.Namespace, true, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate snapshot: %v. Err: %v", obj.Metadata.Name, err),
				}
			}

			logrus.Infof("[%v] Validated snapshot: %v", ctx.App.Key, obj.Metadata.Name)
		} else if obj, ok := spec.(*stork_api.GroupVolumeSnapshot); ok {
			if err := k8sOps.ValidateGroupSnapshot(obj.Name, obj.Namespace, true, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate group snapshot: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated group snapshot: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			if err := k8sOps.ValidatePVCsForStatefulSet(ss, timeout*time.Duration(*obj.Spec.Replicas), retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate PVCs for statefulset: %v. Err: %v", ss.Name, err),
				}
			}

			logrus.Infof("[%v] Validated PVCs from StatefulSet: %v", ctx.App.Key, obj.Name)
		}
	}

	return nil
}

func (k *k8s) isPVCShared(pvc *v1.PersistentVolumeClaim) bool {
	for _, mode := range pvc.Spec.AccessModes {
		if mode == v1.PersistentVolumeAccessMode(v1.ReadOnlyMany) ||
			mode == v1.PersistentVolumeAccessMode(v1.ReadWriteMany) {
			return true
		}
	}
	return false
}

func (k *k8s) DeleteVolumes(ctx *scheduler.Context) ([]*volume.Volume, error) {
	k8sOps := k8s_ops.Instance()
	var vols []*volume.Volume
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*storage_api.StorageClass); ok {
			if err := k8sOps.DeleteStorageClass(obj.Name); err != nil {
				if !errors.IsNotFound(err) {
					return nil, &scheduler.ErrFailedToDestroyStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to destroy storage class: %v. Err: %v", obj.Name, err),
					}
				}
			}

			logrus.Infof("[%v] Destroyed storage class: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			vols = append(vols, &volume.Volume{
				ID:        string(obj.UID),
				Name:      obj.Name,
				Namespace: obj.Namespace,
				Shared:    k.isPVCShared(obj),
			})

			if err := k8sOps.DeletePersistentVolumeClaim(obj.Name, obj.Namespace); err != nil {
				if !errors.IsNotFound(err) {
					return nil, &scheduler.ErrFailedToDestroyStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to destroy PVC: %v. Err: %v", obj.Name, err),
					}
				}
			}

			logrus.Infof("[%v] Destroyed PVC: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*snap_v1.VolumeSnapshot); ok {
			if err := k8sOps.DeleteSnapshot(obj.Metadata.Name, obj.Metadata.Namespace); err != nil {
				if !errors.IsNotFound(err) {
					return nil, &scheduler.ErrFailedToDestroyStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to destroy Snapshot: %v. Err: %v", obj.Metadata.Name, err),
					}
				}
			}

			logrus.Infof("[%v] Destroyed snapshot: %v", ctx.App.Key, obj.Metadata.Name)
		} else if obj, ok := spec.(*stork_api.GroupVolumeSnapshot); ok {
			if err := k8sOps.DeleteGroupSnapshot(obj.Name, obj.Namespace); err != nil {
				if !errors.IsNotFound(err) {
					return nil, &scheduler.ErrFailedToDestroyStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to destroy group snapshot: %v. Err: %v", obj.Name, err),
					}
				}
			}

			logrus.Infof("[%v] Destroyed group snapshot: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			pvcList, err := k8sOps.GetPVCsForStatefulSet(obj)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToDestroyStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVCs for StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			for _, pvc := range pvcList.Items {
				vols = append(vols, &volume.Volume{
					ID:        string(pvc.UID),
					Name:      pvc.Name,
					Namespace: pvc.Namespace,
					Shared:    k.isPVCShared(&pvc),
				})

				if err := k8sOps.DeletePersistentVolumeClaim(pvc.Name, pvc.Namespace); err != nil {
					if !errors.IsNotFound(err) {
						return nil, &scheduler.ErrFailedToDestroyStorage{
							App:   ctx.App,
							Cause: fmt.Sprintf("Failed to destroy PVC: %v. Err: %v", pvc.Name, err),
						}
					}
				}
			}

			logrus.Infof("[%v] Destroyed PVCs for StatefulSet: %v", ctx.App.Key, obj.Name)
		}
	}

	return vols, nil
}

func (k *k8s) GetVolumes(ctx *scheduler.Context) ([]*volume.Volume, error) {
	k8sOps := k8s_ops.Instance()
	var vols []*volume.Volume
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			vol := &volume.Volume{
				ID:        string(obj.UID),
				Name:      obj.Name,
				Namespace: obj.Namespace,
				Shared:    k.isPVCShared(obj),
			}
			vols = append(vols, vol)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			pvcList, err := k8sOps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToGetStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVC from StatefulSet: %v. Err: %v", ss.Name, err),
				}
			}

			for _, pvc := range pvcList.Items {
				vols = append(vols, &volume.Volume{
					ID:        string(pvc.UID),
					Name:      pvc.Name,
					Namespace: pvc.Namespace,
					Shared:    k.isPVCShared(&pvc),
				})
			}
		}
	}

	return vols, nil
}

func (k *k8s) ResizeVolume(ctx *scheduler.Context) ([]*volume.Volume, error) {
	k8sOps := k8s_ops.Instance()
	var vols []*volume.Volume
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			updatedPVC, _ := k8sOps.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			vol, err := k.resizePVCBy1GB(ctx, updatedPVC)
			if err != nil {
				return nil, err
			}
			vols = append(vols, vol)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToResizeStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			pvcList, err := k8sOps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToResizeStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVC from StatefulSet: %v. Err: %v", ss.Name, err),
				}
			}

			for _, pvc := range pvcList.Items {
				vol, err := k.resizePVCBy1GB(ctx, &pvc)
				if err != nil {
					return nil, err
				}
				vols = append(vols, vol)
			}
		}
	}

	return vols, nil
}

func (k *k8s) resizePVCBy1GB(ctx *scheduler.Context, pvc *v1.PersistentVolumeClaim) (*volume.Volume, error) {
	k8sOps := k8s_ops.Instance()
	storageSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]

	// TODO this test is required since stork snapshot doesn't support resizing, remove when feature is added
	resizeSupported := true
	if annotationValue, hasKey := pvc.Annotations[resizeSupportedAnnotationKey]; hasKey {
		resizeSupported, _ = strconv.ParseBool(annotationValue)
	}
	if resizeSupported {
		extraAmount, _ := resource.ParseQuantity("1Gi")
		storageSize.Add(extraAmount)
		pvc.Spec.Resources.Requests[v1.ResourceStorage] = storageSize
		if _, err := k8sOps.UpdatePersistentVolumeClaim(pvc); err != nil {
			return nil, &scheduler.ErrFailedToResizeStorage{
				App:   ctx.App,
				Cause: err.Error(),
			}
		}
	}
	sizeInt64, _ := storageSize.AsInt64()
	vol := &volume.Volume{
		ID:        string(pvc.UID),
		Name:      pvc.Name,
		Namespace: pvc.Namespace,
		Size:      uint64(sizeInt64),
		Shared:    k.isPVCShared(pvc),
	}
	return vol, nil
}

func (k *k8s) GetSnapshots(ctx *scheduler.Context) ([]*volume.Snapshot, error) {
	var snaps []*volume.Snapshot
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*snap_v1.VolumeSnapshot); ok {
			snap := &volume.Snapshot{
				ID:        string(obj.Metadata.UID),
				Name:      obj.Metadata.Name,
				Namespace: obj.Metadata.Namespace,
			}
			snaps = append(snaps, snap)
		} else if obj, ok := spec.(*stork_api.GroupVolumeSnapshot); ok {
			snapsForGroupsnap, err := k8s_ops.Instance().GetSnapshotsForGroupSnapshot(obj.Name, obj.Namespace)
			if err != nil {
				return nil, err
			}

			for _, snapForGroupsnap := range snapsForGroupsnap {
				snap := &volume.Snapshot{
					ID:        string(snapForGroupsnap.Metadata.UID),
					Name:      snapForGroupsnap.Metadata.Name,
					Namespace: snapForGroupsnap.Metadata.Namespace,
				}
				snaps = append(snaps, snap)
			}
		}
	}

	return snaps, nil
}

func (k *k8s) GetNodesForApp(ctx *scheduler.Context) ([]node.Node, error) {
	t := func() (interface{}, bool, error) {
		pods, err := k.getPodsForApp(ctx)
		if err != nil {
			return nil, false, &scheduler.ErrFailedToGetNodesForApp{
				App:   ctx.App,
				Cause: fmt.Sprintf("failed to get pods due to: %v", err),
			}
		}

		// We should have pods from a supported application at this point
		var result []node.Node
		nodeMap := node.GetNodesByName()

		for _, p := range pods {
			n, ok := nodeMap[p.Spec.NodeName]
			if !ok {
				return nil, true, &scheduler.ErrFailedToGetNodesForApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("node: %v not present in node map", p.Spec.NodeName),
				}
			}

			if node.Contains(result, n) {
				continue
			}

			if k8s_ops.Instance().IsPodRunning(p) {
				result = append(result, n)
			}
		}

		if len(result) > 0 {
			return result, false, nil
		}

		return result, true, &scheduler.ErrFailedToGetNodesForApp{
			App:   ctx.App,
			Cause: fmt.Sprintf("no pods in running state %v", pods),
		}
	}

	nodes, err := task.DoRetryWithTimeout(t, defaultTimeout, defaultRetryInterval)
	if err != nil {
		return nil, err
	}

	return nodes.([]node.Node), nil
}

func (k *k8s) getPodsForApp(ctx *scheduler.Context) ([]v1.Pod, error) {
	k8sOps := k8s_ops.Instance()
	var pods []v1.Pod

	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			depPods, err := k8sOps.GetDeploymentPods(obj)
			if err != nil {
				return nil, err
			}
			pods = append(pods, depPods...)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			ssPods, err := k8sOps.GetStatefulSetPods(obj)
			if err != nil {
				return nil, err
			}
			pods = append(pods, ssPods...)
		}
	}

	return pods, nil
}

func (k *k8s) Describe(ctx *scheduler.Context) (string, error) {
	k8sOps := k8s_ops.Instance()
	var buf bytes.Buffer
	var err error
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			buf.WriteString(insertLineBreak(obj.Name))
			var depStatus *apps_api.DeploymentStatus
			if depStatus, err = k8sOps.DescribeDeployment(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetAppStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of deployment: %v. Err: %v", obj.Name, err),
				}))
			}
			//Dump depStatus
			buf.WriteString(fmt.Sprintf("%v\n", *depStatus))
			pods, _ := k8sOps.GetDeploymentPods(obj)
			for _, pod := range pods {
				buf.WriteString(dumpPodStatusRecursively(pod))
			}
			buf.WriteString(insertLineBreak("END Deployment"))
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			buf.WriteString(insertLineBreak(obj.Name))
			var ssetStatus *apps_api.StatefulSetStatus
			if ssetStatus, err = k8sOps.DescribeStatefulSet(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetAppStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of statefulset: %v. Err: %v", obj.Name, err),
				}))
			}
			//Dump ssetStatus
			buf.WriteString(fmt.Sprintf("%v\n", *ssetStatus))
			pods, _ := k8sOps.GetStatefulSetPods(obj)
			for _, pod := range pods {
				buf.WriteString(dumpPodStatusRecursively(pod))
			}
			buf.WriteString(insertLineBreak("END StatefulSet"))
		} else if obj, ok := spec.(*v1.Service); ok {
			buf.WriteString(insertLineBreak(obj.Name))
			var svcStatus *v1.ServiceStatus
			if svcStatus, err = k8sOps.DescribeService(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetAppStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of service: %v. Err: %v", obj.Name, err),
				}))
			}
			//Dump service status
			buf.WriteString(fmt.Sprintf("%v\n", *svcStatus))
			buf.WriteString(insertLineBreak("END Service"))
		} else if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			buf.WriteString(insertLineBreak(obj.Name))
			var pvcStatus *v1.PersistentVolumeClaimStatus
			if pvcStatus, err = k8sOps.GetPersistentVolumeClaimStatus(obj); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetStorageStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of persistent volume claim: %v. Err: %v", obj.Name, err),
				}))
			}
			//Dump persistent volume claim status
			buf.WriteString(fmt.Sprintf("%v\n", *pvcStatus))
			buf.WriteString(insertLineBreak("END PersistentVolumeClaim"))
		} else if obj, ok := spec.(*storage_api.StorageClass); ok {
			buf.WriteString(insertLineBreak(obj.Name))
			var scParams map[string]string
			if scParams, err = k8sOps.GetStorageClassParams(obj); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get parameters of storage class: %v. Err: %v", obj.Name, err),
				}))
			}
			//Dump storage class parameters
			buf.WriteString(fmt.Sprintf("%v\n", scParams))
			buf.WriteString(insertLineBreak("END Storage Class"))
		} else if obj, ok := spec.(*v1.Pod); ok {
			buf.WriteString(insertLineBreak(obj.Name))
			var podStatus *v1.PodList
			if podStatus, err = k8sOps.GetPods(obj.Name, nil); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetPodStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of pod: %v. Err: %v", obj.Name, err),
				}))
			}
			buf.WriteString(fmt.Sprintf("%v\n", podStatus))
			buf.WriteString(insertLineBreak("END Pod"))
		} else {
			logrus.Warnf("Object type unknown/not supported: %v", obj)
		}
	}
	return buf.String(), nil
}

func (k *k8s) ScaleApplication(ctx *scheduler.Context, scaleFactorMap map[string]int32) error {
	k8sOps := k8s_ops.Instance()
	for _, spec := range ctx.App.SpecList {
		logrus.Infof("Scale all Deployments")
		if obj, ok := spec.(*apps_api.Deployment); ok {
			dep, err := k8sOps.GetDeployment(obj.Name, obj.Namespace)
			if err != nil {
				return err
			}
			newScaleFactor := scaleFactorMap[obj.Name+DeploymentSuffix]
			*dep.Spec.Replicas = newScaleFactor
			if _, err := k8sOps.UpdateDeployment(dep); err != nil {
				return &scheduler.ErrFailedToUpdateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to update Deployment: %v. Err: %v", obj.Name, err),
				}
			}
			logrus.Infof("Deployment %s scaled to %d successfully.", obj.Name, newScaleFactor)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			logrus.Infof("Scale all Stateful sets")
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return err
			}
			newScaleFactor := scaleFactorMap[obj.Name+StatefulSetSuffix]
			*ss.Spec.Replicas = newScaleFactor
			if _, err := k8sOps.UpdateStatefulSet(ss); err != nil {
				return &scheduler.ErrFailedToUpdateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to update StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}
			logrus.Infof("StatefulSet %s scaled to %d successfully.", obj.Name, int(newScaleFactor))
		}
	}
	return nil
}

func (k *k8s) GetScaleFactorMap(ctx *scheduler.Context) (map[string]int32, error) {
	k8sOps := k8s_ops.Instance()
	scaleFactorMap := make(map[string]int32, len(ctx.App.SpecList))
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			dep, err := k8sOps.GetDeployment(obj.Name, obj.Namespace)
			if err != nil {
				return scaleFactorMap, err
			}
			scaleFactorMap[obj.Name+DeploymentSuffix] = *dep.Spec.Replicas
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return scaleFactorMap, err
			}
			scaleFactorMap[obj.Name+StatefulSetSuffix] = *ss.Spec.Replicas
		}
	}
	return scaleFactorMap, nil
}

func (k *k8s) StopSchedOnNode(n node.Node) error {
	driver, _ := node.Get(k.nodeDriverName)
	systemOpts := node.SystemctlOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         findFilesOnWorkerTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		},
		Action: "stop",
	}
	err := driver.Systemctl(n, SystemdSchedServiceName, systemOpts)
	if err != nil {
		return &scheduler.ErrFailedToStopSchedOnNode{
			Node:          n,
			SystemService: SystemdSchedServiceName,
			Cause:         err.Error(),
		}
	}
	return nil
}

func (k *k8s) StartSchedOnNode(n node.Node) error {
	driver, _ := node.Get(k.nodeDriverName)
	systemOpts := node.SystemctlOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         defaultTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		},
		Action: "start",
	}
	err := driver.Systemctl(n, SystemdSchedServiceName, systemOpts)
	if err != nil {
		return &scheduler.ErrFailedToStartSchedOnNode{
			Node:          n,
			SystemService: SystemdSchedServiceName,
			Cause:         err.Error(),
		}
	}
	return nil
}

func (k *k8s) createMigrationObjects(
	specObj interface{},
	ns *v1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {
	k8sOps := k8s_ops.Instance()
	if obj, ok := specObj.(*stork_api.ClusterPair); ok {
		obj.Namespace = ns.Name
		clusterPair, err := k8sOps.CreateClusterPair(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create ClusterPair: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created ClusterPair: %v", app.Key, clusterPair.Name)
		return clusterPair, nil
	} else if obj, ok := specObj.(*stork_api.Migration); ok {
		obj.Namespace = ns.Name
		migration, err := k8sOps.CreateMigration(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Migration: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created Migration: %v", app.Key, migration.Name)
		return migration, nil
	} else if obj, ok := specObj.(*stork_api.MigrationSchedule); ok {
		obj.Namespace = ns.Name
		migrationSchedule, err := k8sOps.CreateMigrationSchedule(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create MigrationSchedule: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created MigrationSchedule: %v", app.Key, migrationSchedule.Name)
		return migrationSchedule, nil
	} else if obj, ok := specObj.(*stork_api.SchedulePolicy); ok {
		schedPolicy, err := k8sOps.CreateSchedulePolicy(obj)
		if errors.IsAlreadyExists(err) {
			if schedPolicy, err = k8sOps.GetSchedulePolicy(obj.Name); err == nil {
				logrus.Infof("[%v] Found existing schedule policy: %v", app.Key, schedPolicy.Name)
				return schedPolicy, nil
			}
		}

		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create SchedulePolicy: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created SchedulePolicy: %v", app.Key, schedPolicy.Name)
		return schedPolicy, nil
	}

	return nil, nil
}

func (k *k8s) destroyMigrationObject(
	specObj interface{},
	app *spec.AppSpec,
) error {
	k8sOps := k8s_ops.Instance()
	if obj, ok := specObj.(*stork_api.ClusterPair); ok {
		err := k8sOps.DeleteClusterPair(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete ClusterPair: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed ClusterPair: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*stork_api.Migration); ok {
		err := k8sOps.DeleteMigration(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete Migration: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed Migration: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*stork_api.MigrationSchedule); ok {
		err := k8sOps.DeleteMigrationSchedule(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete MigrationSchedule: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed MigrationSchedule: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*stork_api.SchedulePolicy); ok {
		err := k8sOps.DeleteSchedulePolicy(obj.Name)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete SchedulePolicy: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed SchedulePolicy: %v", app.Key, obj.Name)
	}
	return nil
}

func insertLineBreak(note string) string {
	return fmt.Sprintf("------------------------------\n%s\n------------------------------\n", note)
}

func dumpPodStatusRecursively(pod v1.Pod) string {
	var buf bytes.Buffer
	buf.WriteString(insertLineBreak(pod.Name))
	buf.WriteString(fmt.Sprintf("%v\n", pod.Status))
	for _, conStat := range pod.Status.ContainerStatuses {
		buf.WriteString(insertLineBreak(conStat.Name))
		buf.WriteString(fmt.Sprintf("%v\n", conStat))
		buf.WriteString(insertLineBreak("END container"))
	}
	buf.WriteString(insertLineBreak("END pod"))
	return buf.String()
}

func init() {
	k := &k8s{}
	scheduler.Register(SchedName, k)
}
