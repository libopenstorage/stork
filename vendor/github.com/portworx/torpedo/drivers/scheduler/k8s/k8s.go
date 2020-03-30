package k8s

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/libopenstorage/openstorage/pkg/units"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/autopilot"
	k8sCommon "github.com/portworx/sched-ops/k8s/common"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/externalstorage"
	"github.com/portworx/sched-ops/k8s/rbac"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/aututils"
	tp_errors "github.com/portworx/torpedo/pkg/errors"
	"github.com/sirupsen/logrus"
	appsapi "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storageapi "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
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
	// SystemdSchedServiceName is the name of the system service responsible for scheduling
	SystemdSchedServiceName = "kubelet"
	// ZoneK8SNodeLabel is label describing zone of the k8s node
	ZoneK8SNodeLabel = "failure-domain.beta.kubernetes.io/zone"
	// RegionK8SNodeLabel is node label describing region of the k8s node
	RegionK8SNodeLabel = "failure-domain.beta.kubernetes.io/region"
)

const (
	k8sNodeReadyTimeout    = 5 * time.Minute
	volDirCleanupTimeout   = 5 * time.Minute
	k8sObjectCreateTimeout = 2 * time.Minute
	k8sDestroyTimeout      = 2 * time.Minute
	// FindFilesOnWorkerTimeout timeout for find files on worker
	FindFilesOnWorkerTimeout = 1 * time.Minute
	deleteTasksWaitTimeout   = 3 * time.Minute
	// DefaultRetryInterval  Default retry interval
	DefaultRetryInterval = 10 * time.Second
	// DefaultTimeout default timeout
	DefaultTimeout = 2 * time.Minute

	defaultTriggerCheckInterval          = 5 * time.Second
	defaultTriggerCheckTimeout           = 5 * time.Minute
	resizeSupportedAnnotationKey         = "torpedo.io/resize-supported"
	autopilotEnabledAnnotationKey        = "torpedo.io/autopilot-enabled"
	deploymentAppEnvEnabledAnnotationKey = "torpedo.io/appenv-enabled"
	specObjAppWorkloadSizeEnvVar         = "SIZE"
)

const (
	secretNameKey      = "secret_name"
	secretNamespaceKey = "secret_namespace"
	secretName         = "openstorage.io/auth-secret-name"
	secretNamespace    = "openstorage.io/auth-secret-namespace"
)

var (
	// use underscore to avoid conflicts to text/template from golang
	namespaceRegex      = regexp.MustCompile("_NAMESPACE_")
	defaultTorpedoLabel = map[string]string{
		"creator": "torpedo",
	}
	k8sCore            = core.Instance()
	k8sApps            = apps.Instance()
	k8sStork           = stork.Instance()
	k8sStorage         = storage.Instance()
	k8sExternalStorage = externalstorage.Instance()
	k8sAutopilot       = autopilot.Instance()
	k8sRbac            = rbac.Instance()
)

// K8s  The kubernetes structure
type K8s struct {
	SpecFactory         *spec.Factory
	NodeDriverName      string
	VolDriverName       string
	secretConfigMapName string
	customConfig        map[string]scheduler.AppConfig
	eventsStorage       map[string][]scheduler.Event
}

// IsNodeReady  Check whether the cluster node is ready
func (k *K8s) IsNodeReady(n node.Node) error {
	t := func() (interface{}, bool, error) {
		if err := k8sCore.IsNodeReady(n.Name); err != nil {
			return "", true, &scheduler.ErrNodeNotReady{
				Node:  n,
				Cause: err.Error(),
			}
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, k8sNodeReadyTimeout, DefaultRetryInterval); err != nil {
		return err
	}

	return nil
}

// String returns the string name of this driver.
func (k *K8s) String() string {
	return SchedName
}

// Init Initialize the driver
func (k *K8s) Init(schedOpts scheduler.InitOptions) error {
	k.NodeDriverName = schedOpts.NodeDriverName
	k.VolDriverName = schedOpts.VolDriverName
	k.secretConfigMapName = schedOpts.SecretConfigMapName
	k.customConfig = schedOpts.CustomAppConfig
	k.eventsStorage = make(map[string][]scheduler.Event)

	nodes, err := k8sCore.GetNodes()
	if err != nil {
		return err
	}

	for _, n := range nodes.Items {
		if err = k.addNewNode(n); err != nil {
			return err
		}
	}

	k.SpecFactory, err = spec.NewFactory(schedOpts.SpecDir, schedOpts.VolDriverName, k)
	if err != nil {
		return err
	}

	go func() {
		err := k.collectEvents()
		if err != nil {
			logrus.Fatal(err)
		}
	}()
	return nil
}

func (k *K8s) addNewNode(newNode v1.Node) error {
	n := k.parseK8SNode(newNode)
	if err := k.IsNodeReady(n); err != nil {
		return err
	}
	if err := node.AddNode(n); err != nil {
		return err
	}
	return nil
}

// RescanSpecs Rescan the application spec file
//
func (k *K8s) RescanSpecs(specDir string) error {
	var err error
	logrus.Infof("Rescanning specs for %v", specDir)
	k.SpecFactory, err = spec.NewFactory(specDir, volume.GetStorageProvisioner(), k)
	if err != nil {
		return err
	}
	return nil
}

// RefreshNodeRegistry update the k8 node list registry
//
func (k *K8s) RefreshNodeRegistry() error {

	nodes, err := k8sCore.GetNodes()
	if err != nil {
		return err
	}

	node.CleanupRegistry()

	for _, n := range nodes.Items {
		if err = k.addNewNode(n); err != nil {
			return err
		}
	}
	return nil
}

// ParseSpecs parses the application spec file
func (k *K8s) ParseSpecs(specDir, storageProvisioner string) ([]interface{}, error) {
	fileList := make([]string, 0)
	if err := filepath.Walk(specDir, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			if !isValidProvider(path) {
				fileList = append(fileList, path)
			} else { // specs from cloud provider directory
				if strings.Contains(path, "/"+storageProvisioner+"/") {
					fileList = append(fileList, path)
				}
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	var specs []interface{}

	splitPath := strings.Split(specDir, "/")
	appName := splitPath[len(splitPath)-1]

	for _, fileName := range fileList {
		file, err := ioutil.ReadFile(fileName)
		if err != nil {
			return nil, err
		}

		var customConfig scheduler.AppConfig
		var ok bool

		if customConfig, ok = k.customConfig[appName]; !ok {
			customConfig = scheduler.AppConfig{}
		}

		tmpl, err := template.New("customConfig").Parse(string(file))
		if err != nil {
			return nil, err
		}
		var processedFile bytes.Buffer
		err = tmpl.Execute(&processedFile, customConfig)
		if err != nil {
			return nil, err
		}

		reader := bufio.NewReader(&processedFile)
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

func isValidProvider(specPath string) bool {
	for _, driver := range volume.GetVolumeDrivers() {
		if strings.Contains(specPath, "/"+driver+"/") { // Check for directories for cloud providers, ignore files
			return true
		}
	}
	return false
}

func decodeSpec(specContents []byte) (runtime.Object, error) {
	obj, _, err := scheme.Codecs.UniversalDeserializer().Decode([]byte(specContents), nil, nil)
	if err != nil {
		schemeObj := runtime.NewScheme()
		if err := snapv1.AddToScheme(schemeObj); err != nil {
			return nil, err
		}

		if err := storkapi.AddToScheme(schemeObj); err != nil {
			return nil, err
		}

		if err := apapi.AddToScheme(schemeObj); err != nil {
			return nil, err
		}

		codecs := serializer.NewCodecFactory(schemeObj)
		obj, _, err = codecs.UniversalDeserializer().Decode([]byte(specContents), nil, nil)
		if err != nil {
			return nil, err
		}
	}
	return obj, nil
}

func validateSpec(in interface{}) (interface{}, error) {
	if specObj, ok := in.(*appsapi.Deployment); ok {
		return specObj, nil
	} else if specObj, ok := in.(*appsapi.StatefulSet); ok {
		return specObj, nil
	} else if specObj, ok := in.(*appsapi.DaemonSet); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.Service); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.PersistentVolumeClaim); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storageapi.StorageClass); ok {
		return specObj, nil
	} else if specObj, ok := in.(*snapv1.VolumeSnapshot); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.GroupVolumeSnapshot); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.Secret); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.ConfigMap); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.Rule); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.Pod); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.ClusterPair); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.Migration); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.MigrationSchedule); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.BackupLocation); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.ApplicationBackup); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.SchedulePolicy); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.ApplicationRestore); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.ApplicationClone); ok {
		return specObj, nil
	} else if specObj, ok := in.(*storkapi.VolumeSnapshotRestore); ok {
		return specObj, nil
	} else if specObj, ok := in.(*apapi.AutopilotRule); ok {
		return specObj, nil
	} else if specObj, ok := in.(*v1.ServiceAccount); ok {
		return specObj, nil
	} else if specObj, ok := in.(*rbacv1.Role); ok {
		return specObj, nil
	} else if specObj, ok := in.(*rbacv1.RoleBinding); ok {
		return specObj, nil
	}

	return nil, fmt.Errorf("unsupported object: %v", reflect.TypeOf(in))
}

// getAddressesForNode  Get IP address for the nodes in the cluster
//
func (k *K8s) getAddressesForNode(n v1.Node) []string {
	var addrs []string
	for _, addr := range n.Status.Addresses {
		if addr.Type == v1.NodeExternalIP || addr.Type == v1.NodeInternalIP {
			addrs = append(addrs, addr.Address)
		}
	}
	return addrs
}

// parseK8SNode Parse the kubernetes clsuter nodes
//
func (k *K8s) parseK8SNode(n v1.Node) node.Node {
	var nodeType node.Type
	var zone, region string
	if k8sCore.IsNodeMaster(n) {
		nodeType = node.TypeMaster
	} else {
		nodeType = node.TypeWorker
	}

	nodeLabels, err := k8sCore.GetLabelsOnNode(n.GetName())
	if err != nil {
		logrus.Warn("failed to get node label for ", n.GetName())
	}

	for key, value := range nodeLabels {
		switch key {
		case ZoneK8SNodeLabel:
			zone = value
		case RegionK8SNodeLabel:
			region = value
		}
	}

	return node.Node{
		Name:      n.Name,
		Addresses: k.getAddressesForNode(n),
		Type:      nodeType,
		Zone:      zone,
		Region:    region,
	}
}

// Schedule Schedule the application
func (k *K8s) Schedule(instanceID string, options scheduler.ScheduleOptions) ([]*scheduler.Context, error) {
	var apps []*spec.AppSpec
	if len(options.AppKeys) > 0 {
		for _, key := range options.AppKeys {
			appSpec, err := k.SpecFactory.Get(key)
			if err != nil {
				return nil, err
			}
			apps = append(apps, appSpec)
		}
	} else {
		apps = k.SpecFactory.GetAll()
	}

	var contexts []*scheduler.Context
	for _, app := range apps {

		appNamespace := app.GetID(instanceID)
		specObjects, err := k.CreateSpecObjects(app, appNamespace, options)
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
			ScheduleOptions: options,
		}

		contexts = append(contexts, ctx)
	}

	return contexts, nil
}

// CreateSpecObjects Create application
func (k *K8s) CreateSpecObjects(app *spec.AppSpec, namespace string, options scheduler.ScheduleOptions) ([]interface{}, error) {
	var specObjects []interface{}
	ns, err := k.createNamespace(app, namespace, options)
	if err != nil {
		return nil, err
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createMigrationObjects(appSpec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}
		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}
		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createVolumeSnapshotRestore(appSpec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}

		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}

		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createStorageObject(appSpec, ns, app, options)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}

		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}

		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}

	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createCoreObject(appSpec, ns, app, options)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}

		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
		if err != nil {
			return nil, err
		}

		if obj != nil {
			specObjects = append(specObjects, obj)
		}
	}
	for _, appSpec := range app.SpecList {
		t := func() (interface{}, bool, error) {
			obj, err := k.createBackupObjects(appSpec, ns, app)
			if err != nil {
				return nil, true, err
			}
			return obj, false, nil
		}
		obj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
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
func (k *K8s) AddTasks(ctx *scheduler.Context, options scheduler.ScheduleOptions) error {
	if ctx == nil {
		return fmt.Errorf("context to add tasks to cannot be nil")
	}
	if len(options.AppKeys) == 0 {
		return fmt.Errorf("need to specify list of applications to add to context")
	}

	appNamespace := ctx.GetID()
	var apps []*spec.AppSpec
	specObjects := ctx.App.SpecList
	for _, key := range options.AppKeys {
		appSpec, err := k.SpecFactory.Get(key)
		if err != nil {
			return err
		}
		apps = append(apps, appSpec)
	}
	for _, app := range apps {
		objects, err := k.CreateSpecObjects(app, appNamespace, options)
		if err != nil {
			return err
		}
		specObjects = append(specObjects, objects...)
	}
	ctx.App.SpecList = specObjects
	return nil
}

// UpdateTasksID updates task IDs in the given context
func (k *K8s) UpdateTasksID(ctx *scheduler.Context, id string) error {
	ctx.UID = id

	for _, appSpec := range ctx.App.SpecList {
		metadata, err := meta.Accessor(appSpec)
		if err != nil {
			return err
		}
		metadata.SetNamespace(id)
	}
	return nil
}

func (k *K8s) createNamespace(app *spec.AppSpec, namespace string, options scheduler.ScheduleOptions) (*v1.Namespace, error) {
	k8sOps := k8sCore

	t := func() (interface{}, bool, error) {
		metadata := defaultTorpedoLabel
		metadata["app"] = app.Key
		if len(options.Labels) > 0 {
			for k, v := range options.Labels {
				metadata[k] = v
			}
		}
		ns, err := k8sOps.CreateNamespace(namespace, metadata)

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

	nsObj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
	if err != nil {
		return nil, err
	}

	return nsObj.(*v1.Namespace), nil
}

func (k *K8s) createStorageObject(spec interface{}, ns *v1.Namespace, app *spec.AppSpec,
	options scheduler.ScheduleOptions) (interface{}, error) {

	// Add security annotations if running with auth-enabled
	configMapName := k.secretConfigMapName
	if configMapName != "" {
		configMap, err := k8sCore.GetConfigMap(configMapName, "default")
		if err != nil {
			return nil, &scheduler.ErrFailedToGetConfigMap{
				Name:  configMapName,
				Cause: fmt.Sprintf("Failed to get config map: Err: %v", err),
			}
		}

		err = k.addSecurityAnnotation(spec, configMap)
		if err != nil {
			return nil, fmt.Errorf("failed to add annotations to storage object: %v", err)
		}

	}

	if obj, ok := spec.(*storageapi.StorageClass); ok {
		obj.Namespace = ns.Name
		logrus.Infof("Setting provisioner of %v to %v", obj.Name, volume.GetStorageProvisioner())
		obj.Provisioner = volume.GetStorageProvisioner()

		sc, err := k8sStorage.CreateStorageClass(obj)
		if errors.IsAlreadyExists(err) {
			if sc, err = k8sStorage.GetStorageClass(obj.Name); err == nil {
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
		if len(options.Labels) > 0 {
			k.addLabelsToPVC(obj, options.Labels)
		}

		pvc, err := k8sCore.CreatePersistentVolumeClaim(obj)
		if errors.IsAlreadyExists(err) {
			if pvc, err = k8sCore.GetPersistentVolumeClaim(obj.Name, obj.Namespace); err == nil {
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

		autopilotEnabled := false
		if pvcAnnotationValue, ok := pvc.Annotations[autopilotEnabledAnnotationKey]; ok {
			autopilotEnabled, _ = strconv.ParseBool(pvcAnnotationValue)
		}
		if autopilotEnabled {
			apRule := options.AutopilotRule
			labels := options.Labels
			if apRule.Name != "" {
				apRule.Labels = defaultTorpedoLabel
				labelSelector := metav1.LabelSelector{MatchLabels: labels}
				apRule.Spec.Selector = apapi.RuleObjectSelector{LabelSelector: labelSelector}
				apRule.Spec.NamespaceSelector = apapi.RuleObjectSelector{LabelSelector: labelSelector}
				aRule, err := k.CreateAutopilotRule(apRule)
				if err != nil {
					return nil, err
				}
				logrus.Infof("[%v] Created Autopilot rule: %+v", app.Key, aRule)
			}
		}

		return pvc, nil

	} else if obj, ok := spec.(*snapv1.VolumeSnapshot); ok {
		obj.Metadata.Namespace = ns.Name
		snap, err := k8sExternalStorage.CreateSnapshot(obj)
		if errors.IsAlreadyExists(err) {
			if snap, err = k8sExternalStorage.GetSnapshot(obj.Metadata.Name, obj.Metadata.Namespace); err == nil {
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
	} else if obj, ok := spec.(*storkapi.GroupVolumeSnapshot); ok {
		obj.Namespace = ns.Name
		snap, err := k8sStork.CreateGroupSnapshot(obj)
		if errors.IsAlreadyExists(err) {
			if snap, err = k8sStork.GetGroupSnapshot(obj.Name, obj.Namespace); err == nil {
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
	} else if obj, ok := spec.(*v1.ServiceAccount); ok {
		obj.Namespace = ns.Name
		snap, err := k8sCore.CreateServiceAccount(obj)
		if errors.IsAlreadyExists(err) {
			if snap, err = k8sCore.GetServiceAccount(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing Service Account: %v", app.Key, snap.Name)
				return snap, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Service Account: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created Service Account: %v", app.Key, snap.Name)
		return snap, nil
	} else if obj, ok := spec.(*rbacv1.Role); ok {
		obj.Namespace = ns.Name
		snap, err := k8sRbac.CreateRole(obj)
		if errors.IsAlreadyExists(err) {
			if snap, err = k8sRbac.GetRole(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing Role: %v", app.Key, snap.Name)
				return snap, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Role: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created Role: %v", app.Key, snap.Name)
		return snap, nil
	} else if obj, ok := spec.(*rbacv1.RoleBinding); ok {
		obj.Namespace = ns.Name
		snap, err := k8sRbac.CreateRoleBinding(obj)
		if errors.IsAlreadyExists(err) {
			if snap, err = k8sRbac.GetRoleBinding(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing Role Binding: %v", app.Key, snap.Name)
				return snap, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Role Binding: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created Role Binding: %v", app.Key, snap.Name)
		return snap, nil
	}

	return nil, nil
}

func (k *K8s) substituteNamespaceInPVC(pvc *v1.PersistentVolumeClaim, ns string) {
	pvc.Name = namespaceRegex.ReplaceAllString(pvc.Name, ns)
	for k, v := range pvc.Annotations {
		pvc.Annotations[k] = namespaceRegex.ReplaceAllString(v, ns)
	}
}

func (k *K8s) createVolumeSnapshotRestore(specObj interface{},
	ns *v1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {

	if obj, ok := specObj.(*storkapi.VolumeSnapshotRestore); ok {
		obj.Namespace = ns.Name
		snapRestore, err := k8sStork.CreateVolumeSnapshotRestore(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create VolumeSnapshotRestore: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created VolumeSnapshotRestore: %v", app.Key, snapRestore.Name)
		return snapRestore, nil
	}

	return nil, nil
}

func (k *K8s) addSecurityAnnotation(spec interface{}, configMap *v1.ConfigMap) error {
	logrus.Debugf("Config Map details: %v", configMap.Data)
	if _, ok := configMap.Data[secretNameKey]; !ok {
		return fmt.Errorf("failed to get secret name from config map")
	}
	if _, ok := configMap.Data[secretNamespaceKey]; !ok {
		return fmt.Errorf("failed to get secret namespace from config map")
	}
	if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*snapv1.VolumeSnapshot); ok {
		if obj.Metadata.Annotations == nil {
			obj.Metadata.Annotations = make(map[string]string)
		}
		obj.Metadata.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Metadata.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*appsapi.StatefulSet); ok {
		var pvcList []v1.PersistentVolumeClaim
		for _, pvc := range obj.Spec.VolumeClaimTemplates {
			if pvc.Annotations == nil {
				pvc.Annotations = make(map[string]string)
			}
			pvc.Annotations[secretName] = configMap.Data[secretNameKey]
			pvc.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
			pvcList = append(pvcList, pvc)
		}
		obj.Spec.VolumeClaimTemplates = pvcList
	} else if obj, ok := spec.(*storkapi.ApplicationBackup); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*storkapi.ApplicationClone); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*storkapi.ApplicationRestore); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*storkapi.Migration); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*storkapi.VolumeSnapshotRestore); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*storkapi.GroupVolumeSnapshot); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	}
	return nil
}

func (k *K8s) createCoreObject(spec interface{}, ns *v1.Namespace, app *spec.AppSpec,
	options scheduler.ScheduleOptions) (interface{}, error) {
	if obj, ok := spec.(*appsapi.Deployment); ok {
		obj.Namespace = ns.Name
		obj.Spec.Template.Spec.Volumes = k.substituteNamespaceInVolumes(obj.Spec.Template.Spec.Volumes, ns.Name)
		if options.Scheduler != "" {
			obj.Spec.Template.Spec.SchedulerName = options.Scheduler
		}
		dep, err := k8sApps.CreateDeployment(obj)
		if errors.IsAlreadyExists(err) {
			if dep, err = k8sApps.GetDeployment(obj.Name, obj.Namespace); err == nil {
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

	} else if obj, ok := spec.(*appsapi.StatefulSet); ok {
		// Add security annotations if running with auth-enabled
		configMapName := k.secretConfigMapName
		if configMapName != "" {
			configMap, err := k8sCore.GetConfigMap(configMapName, "default")
			if err != nil {
				return nil, &scheduler.ErrFailedToGetConfigMap{
					Name:  configMapName,
					Cause: fmt.Sprintf("Failed to get config map: Err: %v", err),
				}
			}

			err = k.addSecurityAnnotation(obj, configMap)
			if err != nil {
				return nil, fmt.Errorf("failed to add annotations to core object: %v", err)
			}
		}

		obj.Namespace = ns.Name
		obj.Spec.Template.Spec.Volumes = k.substituteNamespaceInVolumes(obj.Spec.Template.Spec.Volumes, ns.Name)
		if options.Scheduler != "" {
			obj.Spec.Template.Spec.SchedulerName = options.Scheduler
		}
		ss, err := k8sApps.CreateStatefulSet(obj)
		if errors.IsAlreadyExists(err) {
			if ss, err = k8sApps.GetStatefulSet(obj.Name, obj.Namespace); err == nil {
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
		svc, err := k8sCore.CreateService(obj)
		if errors.IsAlreadyExists(err) {
			if svc, err = k8sCore.GetService(obj.Name, obj.Namespace); err == nil {
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
		secret, err := k8sCore.CreateSecret(obj)
		if errors.IsAlreadyExists(err) {
			if secret, err = k8sCore.GetSecret(obj.Name, obj.Namespace); err == nil {
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
	} else if obj, ok := spec.(*storkapi.Rule); ok {
		if obj.Namespace != "kube-system" {
			obj.Namespace = ns.Name
		}
		rule, err := k8sStork.CreateRule(obj)
		if errors.IsAlreadyExists(err) {
			if rule, err = k8sStork.GetRule(obj.Name, obj.Namespace); err == nil {
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
		if options.Scheduler != "" {
			obj.Spec.SchedulerName = options.Scheduler
		}
		pod, err := k8sCore.CreatePod(obj)
		if errors.IsAlreadyExists(err) {
			if pod, err := k8sCore.GetPodByName(obj.Name, obj.Namespace); err == nil {
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
	} else if obj, ok := spec.(*v1.ConfigMap); ok {
		obj.Namespace = ns.Name
		configMap, err := k8sCore.CreateConfigMap(obj)
		if errors.IsAlreadyExists(err) {
			if configMap, err = k8sCore.GetConfigMap(obj.Name, obj.Namespace); err == nil {
				logrus.Infof("[%v] Found existing Config Maps: %v", app.Key, configMap.Name)
				return configMap, nil
			}
		}
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Config Map: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Created Config Map: %v", app.Key, configMap.Name)
		return configMap, nil
	}

	return nil, nil
}

func (k *K8s) destroyCoreObject(spec interface{}, opts map[string]bool, app *spec.AppSpec) (interface{}, error) {
	var pods interface{}
	var podList []*v1.Pod
	var err error
	if obj, ok := spec.(*appsapi.Deployment); ok {
		if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
			if pods, err = k8sApps.GetDeploymentPods(obj); err != nil {
				logrus.Warnf("[%s] Error getting deployment pods. Err: %v", app.Key, err)
			}
		}
		err := k8sApps.DeleteDeployment(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Deployment: %v. Err: %v", obj.Name, err),
			}
		}
	} else if obj, ok := spec.(*appsapi.StatefulSet); ok {
		if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
			if pods, err = k8sApps.GetStatefulSetPods(obj); err != nil {
				logrus.Warnf("[%v] Error getting statefulset pods. Err: %v", app.Key, err)
			}
		}
		err := k8sApps.DeleteStatefulSet(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy stateful set: %v. Err: %v", obj.Name, err),
			}
		}
	} else if obj, ok := spec.(*v1.Service); ok {
		err := k8sCore.DeleteService(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Service: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Destroyed Service: %v", app.Key, obj.Name)
	} else if obj, ok := spec.(*storkapi.Rule); ok {
		err := k8sStork.DeleteRule(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Rule: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Destroyed Rule: %v", app.Key, obj.Name)
	} else if obj, ok := spec.(*v1.Pod); ok {
		if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
			pod, err := k8sCore.GetPodByName(obj.Name, obj.Namespace)
			if err != nil {
				logrus.Warnf("[%v] Error getting pods. Err: %v", app.Key, err)
			}
			podList = append(podList, pod)
			pods = podList
		}
		err := k8sCore.DeletePod(obj.Name, obj.Namespace, false)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyPod{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy Pod: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Destroyed Pod: %v", app.Key, obj.Name)
	} else if obj, ok := spec.(*v1.ConfigMap); ok {
		if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
			_, err := k8sCore.GetConfigMap(obj.Name, obj.Namespace)
			if err != nil {
				logrus.Warnf("[%v] Error getting config maps. Err: %v", app.Key, err)
			}
		}
		err := k8sCore.DeleteConfigMap(obj.Name, obj.Namespace)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy config map: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Destroyed Config Map: %v", app.Key, obj.Name)
	} else if obj, ok := spec.(*apapi.AutopilotRule); ok {
		err := k8sAutopilot.DeleteAutopilotRule(obj.Name)
		if err != nil {
			return pods, &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to destroy AutopilotRule: %v. Err: %v", obj.Name, err),
			}
		}

		logrus.Infof("[%v] Destroyed AutopilotRule: %v", app.Key, obj.Name)
	}

	return pods, nil

}

func (k *K8s) substituteNamespaceInVolumes(volumes []v1.Volume, ns string) []v1.Volume {
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

// WaitForRunning   wait for running
//
func (k *K8s) WaitForRunning(ctx *scheduler.Context, timeout, retryInterval time.Duration) error {
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			if err := k8sApps.ValidateDeployment(obj, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Deployment: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated deployment: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			if err := k8sApps.ValidateStatefulSet(obj, timeout*time.Duration(*obj.Spec.Replicas)); err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated statefulset: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*v1.Service); ok {
			svc, err := k8sCore.GetService(obj.Name, obj.Namespace)
			if err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Service: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated Service: %v", ctx.App.Key, svc.Name)
		} else if obj, ok := specObj.(*storkapi.Rule); ok {
			svc, err := k8sStork.GetRule(obj.Name, obj.Namespace)
			if err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Rule: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated Rule: %v", ctx.App.Key, svc.Name)
		} else if obj, ok := specObj.(*v1.Pod); ok {
			if err := k8sCore.ValidatePod(obj, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidatePod{
					App: ctx.App,
					Cause: fmt.Sprintf("Failed to validate Pod: [%s] %s. Err: Pod is not ready %v",
						obj.Namespace, obj.Name, obj.Status),
				}
			}

			logrus.Infof("[%v] Validated pod: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*storkapi.ClusterPair); ok {
			if err := k8sStork.ValidateClusterPair(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate cluster Pair: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated ClusterPair: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*storkapi.Migration); ok {
			if err := k8sStork.ValidateMigration(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate Migration: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated Migration: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*storkapi.MigrationSchedule); ok {
			if _, err := k8sStork.ValidateMigrationSchedule(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate MigrationSchedule: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated MigrationSchedule: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*storkapi.BackupLocation); ok {
			if err := k8sStork.ValidateBackupLocation(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate BackupLocation: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated BackupLocation: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*storkapi.ApplicationBackup); ok {
			if err := k8sStork.ValidateApplicationBackup(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate ApplicationBackup: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated ApplicationBackup: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*storkapi.ApplicationRestore); ok {
			if err := k8sStork.ValidateApplicationRestore(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate ApplicationRestore: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated ApplicationRestore: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*storkapi.ApplicationClone); ok {
			if err := k8sStork.ValidateApplicationClone(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate ApplicationClone: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated ApplicationClone: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*storkapi.VolumeSnapshotRestore); ok {
			if err := k8sStork.ValidateVolumeSnapshotRestore(obj.Name, obj.Namespace, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate VolumeSnapshotRestore: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated VolumeSnapshotRestore: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*snapv1.VolumeSnapshot); ok {
			if err := k8sExternalStorage.ValidateSnapshot(obj.Metadata.Name, obj.Metadata.Namespace, true, timeout,
				retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Metadata.Name,
					Cause: fmt.Sprintf("Failed to validate VolumeSnapshotRestore: %v. Err: %v", obj.Metadata.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated VolumeSnapshotRestore: %v", ctx.App.Key, obj.Metadata.Name)
		} else if obj, ok := specObj.(*apapi.AutopilotRule); ok {
			if _, err := k8sAutopilot.GetAutopilotRule(obj.Name); err != nil {
				return &scheduler.ErrFailedToValidateCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to validate AutopilotRule: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}
			}
			logrus.Infof("[%v] Validated AutopilotRule: %v", ctx.App.Key, obj.Name)
		}
	}

	return nil
}

// Destroy destroy
func (k *K8s) Destroy(ctx *scheduler.Context, opts map[string]bool) error {
	var podList []v1.Pod
	k8sOps := k8sAutopilot
	apRule := ctx.ScheduleOptions.AutopilotRule
	if apRule.Name != "" {
		if err := k8sOps.DeleteAutopilotRule(apRule.ObjectMeta.Name); err != nil {
			if err != nil {
				return err
			}
		}
	}
	for _, appSpec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			currPods, err := k.destroyCoreObject(appSpec, opts, ctx.App)
			if err != nil {
				return nil, true, err
			}
			return currPods, false, nil
		}
		pods, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval)
		if err != nil {
			// in case we're not waiting for resource cleanup
			if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; !ok || !value {
				return err
			}
			if pods != nil {
				podList = append(podList, pods.([]v1.Pod)...)
			}
			// we're ignoring this error since we want to verify cleanup down below, so simply logging it
			logrus.Warnf("Failed to destroy core objects. Cause: %v", err)
		}
	}
	for _, appSpec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyVolumeSnapshotRestoreObject(appSpec, ctx.App)
			if err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval); err != nil {
			return err
		}
	}
	for _, appSpec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyMigrationObject(appSpec, ctx.App)
			if err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval); err != nil {
			return err
		}
	}

	for _, appSpec := range ctx.App.SpecList {
		t := func() (interface{}, bool, error) {
			err := k.destroyBackupObjects(appSpec, ctx.App)
			if err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}

		if _, err := task.DoRetryWithTimeout(t, k8sDestroyTimeout, DefaultRetryInterval); err != nil {
			return err
		}
	}

	if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
		if err := k.WaitForDestroy(ctx, DefaultTimeout); err != nil {
			return err
		}
		if err := k.waitForCleanup(ctx, podList); err != nil {
			return err
		}
	} else if value, ok = opts[scheduler.OptionsWaitForDestroy]; ok && value {
		if err := k.WaitForDestroy(ctx, DefaultTimeout); err != nil {
			return err
		}
	}
	return nil
}

func (k *K8s) waitForCleanup(ctx *scheduler.Context, podList []v1.Pod) error {
	for _, pod := range podList {
		t := func() (interface{}, bool, error) {
			return nil, true, k.validateVolumeDirCleanup(pod.UID, ctx.App)
		}
		if _, err := task.DoRetryWithTimeout(t, volDirCleanupTimeout, DefaultRetryInterval); err != nil {
			return err
		}
		logrus.Infof("Validated resource cleanup for pod: %v", pod.UID)
	}
	return nil
}

func (k *K8s) validateVolumeDirCleanup(podUID types.UID, app *spec.AppSpec) error {
	podVolDir := k.getVolumeDirPath(podUID)
	driver, _ := node.Get(k.NodeDriverName)
	options := node.FindOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         FindFilesOnWorkerTimeout,
			TimeBeforeRetry: DefaultRetryInterval,
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

func (k *K8s) getVolumeDirPath(podUID types.UID) string {
	return filepath.Join(k8sPodsRootDir, string(podUID), "volumes")
}

// WaitForDestroy wait for schedule context destroy
//
func (k *K8s) WaitForDestroy(ctx *scheduler.Context, timeout time.Duration) error {
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			if err := k8sApps.ValidateTerminatedDeployment(obj, timeout, DefaultRetryInterval); err != nil {
				return &scheduler.ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of deployment: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated destroy of Deployment: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			if err := k8sApps.ValidateTerminatedStatefulSet(obj, timeout, DefaultRetryInterval); err != nil {
				return &scheduler.ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of statefulset: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated destroy of StatefulSet: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*v1.Service); ok {
			if err := k8sCore.ValidateDeletedService(obj.Name, obj.Namespace); err != nil {
				return &scheduler.ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of service: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated destroy of Service: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*v1.Pod); ok {
			if err := k8sCore.WaitForPodDeletion(obj.UID, obj.Namespace, deleteTasksWaitTimeout); err != nil {
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

// DeleteTasks delete the task
func (k *K8s) DeleteTasks(ctx *scheduler.Context, opts *scheduler.DeleteTasksOptions) error {
	fn := "DeleteTasks"
	deleteTasks := func() error {
		k8sOps := k8sCore
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

	if opts == nil || opts.TriggerCb == nil { // caller hasn't provided any trigger checks
		return deleteTasks()
	}

	if opts.TriggerCheckTimeout == time.Duration(0) {
		opts.TriggerCheckTimeout = defaultTriggerCheckTimeout
	}

	if opts.TriggerCheckInterval == time.Duration(0) {
		opts.TriggerCheckInterval = defaultTriggerCheckInterval
	}

	// perform trigger checks and then perform the actual deletion
	t := func() (interface{}, bool, error) {
		triggered, err := opts.TriggerCb()
		if err != nil {
			logrus.Warnf("failed to invoke trigger callback function due to: %v", err)
			return false, false, err
		}

		if triggered {
			return triggered, false, nil // done
		}

		return false, true, fmt.Errorf("%s: trigger check hasn't been met yet", fn)
	}

	_, err := task.DoRetryWithTimeout(t, opts.TriggerCheckTimeout, opts.TriggerCheckInterval)
	if err != nil {
		// timeout error is expected if the trigger conditions don't meet within above timeouts. For any other error,
		// return the error
		_, timedOut := err.(*task.ErrTimedOut)
		if timedOut {
			err = &tp_errors.ErrOperationNotPerformed{
				Operation: fn,
				Reason:    fmt.Sprintf("Trigger checks did not pass"),
			}
		} else {
			err = &tp_errors.ErrOperationNotPerformed{
				Operation: fn,
				Reason:    fmt.Sprintf("Trigger checks could not be performed: %v", err),
			}
		}
		return err
	}

	// perform the actual delete tasks logic
	return deleteTasks()
}

// GetVolumeParameters Get the volume parameters
func (k *K8s) GetVolumeParameters(ctx *scheduler.Context) (map[string]map[string]string, error) {
	result := make(map[string]map[string]string)

	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*v1.PersistentVolumeClaim); ok {
			params, err := k8sCore.GetPersistentVolumeClaimParams(obj)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get params for volume: %v. Err: %v", obj.Name, err),
				}
			}

			pvc, err := k8sCore.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
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
		} else if obj, ok := specObj.(*snapv1.VolumeSnapshot); ok {
			snap, err := k8sExternalStorage.GetSnapshot(obj.Metadata.Name, obj.Metadata.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get Snapshot: %v. Err: %v", obj.Metadata.Name, err),
				}
			}

			snapDataName := snap.Spec.SnapshotDataName
			if len(snapDataName) == 0 {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App: ctx.App,
					Cause: fmt.Sprintf("snapshot: [%s] %s does not have snapshotdata set",
						snap.Metadata.Namespace, snap.Metadata.Name),
				}
			}

			snapData, err := k8sExternalStorage.GetSnapshotData(snapDataName)
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
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			ss, err := k8sApps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			pvcList, err := k8sApps.GetPVCsForStatefulSet(ss)
			if err != nil || pvcList == nil {
				return nil, &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get PVCs for StatefulSet: %v. Err: %v", ss.Name, err),
				}
			}

			for _, pvc := range pvcList.Items {
				params, err := k8sCore.GetPersistentVolumeClaimParams(&pvc)
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

// ValidateVolumes Validates the volumes
func (k *K8s) ValidateVolumes(ctx *scheduler.Context, timeout, retryInterval time.Duration) error {
	var err error
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*storageapi.StorageClass); ok {
			if _, err := k8sStorage.GetStorageClass(obj.Name); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate StorageClass: %v. Err: %v", obj.Name, err),
				}
			}
			logrus.Infof("[%v] Validated storage class: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*v1.PersistentVolumeClaim); ok {
			if err := k8sCore.ValidatePersistentVolumeClaim(obj, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate PVC: %v. Err: %v", obj.Name, err),
				}
			}
			logrus.Infof("[%v] Validated PVC: %v", ctx.App.Key, obj.Name)

			autopilotEnabled := false
			if pvcAnnotationValue, ok := obj.Annotations[autopilotEnabledAnnotationKey]; ok {
				autopilotEnabled, err = strconv.ParseBool(pvcAnnotationValue)
				if err != nil {
					return err
				}
			}
			if autopilotEnabled {
				listApRules, err := k8sAutopilot.ListAutopilotRules()
				if err != nil {
					return err
				}
				for _, rule := range listApRules.Items {
					for _, a := range rule.Spec.Actions {
						if a.Name == aututils.VolumeSpecAction {
							err := k.validatePVCSize(ctx, obj, rule, timeout, retryInterval)
							if err != nil {
								return err
							}
						}
					}
				}
				logrus.Infof("[%v] Validated PVC: %v size based on Autopilot rules", ctx.App.Key, obj.Name)
			}
		} else if obj, ok := specObj.(*snapv1.VolumeSnapshot); ok {
			if err := k8sExternalStorage.ValidateSnapshot(obj.Metadata.Name, obj.Metadata.Namespace, true, timeout,
				retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate snapshot: %v. Err: %v", obj.Metadata.Name, err),
				}
			}

			logrus.Infof("[%v] Validated snapshot: %v", ctx.App.Key, obj.Metadata.Name)
		} else if obj, ok := specObj.(*storkapi.GroupVolumeSnapshot); ok {
			if err := k8sStork.ValidateGroupSnapshot(obj.Name, obj.Namespace, true, timeout, retryInterval); err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate group snapshot: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated group snapshot: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			ss, err := k8sApps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return &scheduler.ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}
			if err := k8sApps.ValidatePVCsForStatefulSet(ss, timeout*time.Duration(*obj.Spec.Replicas), retryInterval); err != nil {
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

// GetWorkloadSizeFromAppSpec gets workload size from an application spec
func (k *K8s) GetWorkloadSizeFromAppSpec(context *scheduler.Context) (uint64, error) {
	var err error
	var wSize uint64
	appEnvVar := getSpecAppEnvVar(context, specObjAppWorkloadSizeEnvVar)
	if appEnvVar != "" {
		wSize, err = strconv.ParseUint(appEnvVar, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("can't parse value %v of environment variable. Err: %v", appEnvVar, err)
		}

		// if size less than 1024 we assume that value is in Gb
		if wSize < 1024 {
			return wSize * units.GiB, nil
		}
	}
	return 0, nil
}

func getSpecAppEnvVar(ctx *scheduler.Context, key string) string {
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			for _, container := range obj.Spec.Template.Spec.Containers {
				for _, env := range container.Env {
					if env.Name == key {
						return env.Value
					}
				}
			}
		}
	}
	return ""
}

func (k *K8s) validatePVCSize(ctx *scheduler.Context, obj *v1.PersistentVolumeClaim, rule apapi.AutopilotRule, timeout time.Duration, retryInterval time.Duration) error {
	wSize, err := k.GetWorkloadSizeFromAppSpec(ctx)
	if err != nil {
		return err
	}
	expectedPVCSize, _, err := k.EstimatePVCExpansion(obj, rule, wSize)
	if err != nil {
		return err
	}
	logrus.Infof("[%v] expecting PVC size: %v\n", ctx.App.Key, expectedPVCSize)
	err = k8sCore.ValidatePersistentVolumeClaimSize(obj, int64(expectedPVCSize), timeout, retryInterval)
	if err != nil {
		return &scheduler.ErrFailedToValidateStorage{
			App:   ctx.App,
			Cause: fmt.Sprintf("Failed to validate size: %v of PVC: %v. Err: %v", expectedPVCSize, obj.Name, err),
		}
	}
	return nil
}

func (k *K8s) isPVCShared(pvc *v1.PersistentVolumeClaim) bool {
	for _, mode := range pvc.Spec.AccessModes {
		if mode == v1.PersistentVolumeAccessMode(v1.ReadOnlyMany) ||
			mode == v1.PersistentVolumeAccessMode(v1.ReadWriteMany) {
			return true
		}
	}
	return false
}

// DeleteVolumes  delete the volumes
func (k *K8s) DeleteVolumes(ctx *scheduler.Context) ([]*volume.Volume, error) {
	var vols []*volume.Volume
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*storageapi.StorageClass); ok {
			if err := k8sStorage.DeleteStorageClass(obj.Name); err != nil {
				if !errors.IsNotFound(err) {
					return nil, &scheduler.ErrFailedToDestroyStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to destroy storage class: %v. Err: %v", obj.Name, err),
					}
				}
			}

			logrus.Infof("[%v] Destroyed storage class: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*v1.PersistentVolumeClaim); ok {
			vols = append(vols, &volume.Volume{
				ID:        string(obj.UID),
				Name:      obj.Name,
				Namespace: obj.Namespace,
				Shared:    k.isPVCShared(obj),
			})

			if err := k8sCore.DeletePersistentVolumeClaim(obj.Name, obj.Namespace); err != nil {
				if !errors.IsNotFound(err) {
					return nil, &scheduler.ErrFailedToDestroyStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to destroy PVC: %v. Err: %v", obj.Name, err),
					}
				}
			}

			logrus.Infof("[%v] Destroyed PVC: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*snapv1.VolumeSnapshot); ok {
			if err := k8sExternalStorage.DeleteSnapshot(obj.Metadata.Name, obj.Metadata.Namespace); err != nil {
				if !errors.IsNotFound(err) {
					return nil, &scheduler.ErrFailedToDestroyStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to destroy Snapshot: %v. Err: %v", obj.Metadata.Name, err),
					}
				}
			}

			logrus.Infof("[%v] Destroyed snapshot: %v", ctx.App.Key, obj.Metadata.Name)
		} else if obj, ok := specObj.(*storkapi.GroupVolumeSnapshot); ok {
			if err := k8sStork.DeleteGroupSnapshot(obj.Name, obj.Namespace); err != nil {
				if !errors.IsNotFound(err) {
					return nil, &scheduler.ErrFailedToDestroyStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to destroy group snapshot: %v. Err: %v", obj.Name, err),
					}
				}
			}

			logrus.Infof("[%v] Destroyed group snapshot: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			pvcList, err := k8sApps.GetPVCsForStatefulSet(obj)
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

				if err := k8sCore.DeletePersistentVolumeClaim(pvc.Name, pvc.Namespace); err != nil {
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

// GetVolumes  Get the volumes
//
func (k *K8s) GetVolumes(ctx *scheduler.Context) ([]*volume.Volume, error) {
	k8sOps := k8sApps
	var vols []*volume.Volume
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*v1.PersistentVolumeClaim); ok {
			pvcObj, err := k8sCore.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			if err != nil {
				return nil, err
			}
			pvcSizeObj := pvcObj.Spec.Resources.Requests[v1.ResourceStorage]
			pvcSize, _ := pvcSizeObj.AsInt64()
			vol := &volume.Volume{
				ID:          string(obj.UID),
				Name:        obj.Name,
				Namespace:   obj.Namespace,
				Shared:      k.isPVCShared(obj),
				Annotations: make(map[string]string),
				Labels:      pvcObj.Labels,
				Size:        uint64(pvcSize),
			}
			for key, val := range obj.Annotations {
				vol.Annotations[key] = val
			}
			vols = append(vols, vol)
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
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

// ResizeVolume  Resize the volume
func (k *K8s) ResizeVolume(ctx *scheduler.Context, configMapName string) ([]*volume.Volume, error) {
	var vols []*volume.Volume
	for _, specObj := range ctx.App.SpecList {
		// Add security annotations if running with auth-enabled
		if configMapName != "" {
			configMap, err := k8sCore.GetConfigMap(configMapName, "default")
			if err != nil {
				return nil, &scheduler.ErrFailedToGetConfigMap{
					Name:  configMapName,
					Cause: fmt.Sprintf("Failed to get config map: Err: %v", err),
				}
			}

			err = k.addSecurityAnnotation(specObj, configMap)
			if err != nil {
				return nil, fmt.Errorf("failed to add annotations to storage object: %v", err)
			}

		}
		if obj, ok := specObj.(*v1.PersistentVolumeClaim); ok {
			updatedPVC, _ := k8sCore.GetPersistentVolumeClaim(obj.Name, obj.Namespace)
			vol, err := k.resizePVCBy1GB(ctx, updatedPVC)
			if err != nil {
				return nil, err
			}
			vols = append(vols, vol)
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			ss, err := k8sApps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return nil, &scheduler.ErrFailedToResizeStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			pvcList, err := k8sApps.GetPVCsForStatefulSet(ss)
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

func (k *K8s) resizePVCBy1GB(ctx *scheduler.Context, pvc *v1.PersistentVolumeClaim) (*volume.Volume, error) {
	k8sOps := k8sCore
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
		ID:            string(pvc.UID),
		Name:          pvc.Name,
		Namespace:     pvc.Namespace,
		RequestedSize: uint64(sizeInt64),
		Shared:        k.isPVCShared(pvc),
	}
	return vol, nil
}

// GetSnapshots  Get the snapshots
func (k *K8s) GetSnapshots(ctx *scheduler.Context) ([]*volume.Snapshot, error) {
	var snaps []*volume.Snapshot
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*snapv1.VolumeSnapshot); ok {
			snap := &volume.Snapshot{
				ID:        string(obj.Metadata.UID),
				Name:      obj.Metadata.Name,
				Namespace: obj.Metadata.Namespace,
			}
			snaps = append(snaps, snap)
		} else if obj, ok := specObj.(*storkapi.GroupVolumeSnapshot); ok {
			snapsForGroupsnap, err := k8sStork.GetSnapshotsForGroupSnapshot(obj.Name, obj.Namespace)
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

// GetNodesForApp get the node for the app
//
func (k *K8s) GetNodesForApp(ctx *scheduler.Context) ([]node.Node, error) {
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
			if strings.TrimSpace(p.Spec.NodeName) == "" {
				return nil, true, &scheduler.ErrFailedToGetNodesForApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("pod %s is not scheduled to any node yet", p.Name),
				}
			}
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

			if k8sCommon.IsPodRunning(p) {
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

	nodes, err := task.DoRetryWithTimeout(t, DefaultTimeout, DefaultRetryInterval)
	if err != nil {
		return nil, err
	}

	return nodes.([]node.Node), nil
}

func (k *K8s) getPodsForApp(ctx *scheduler.Context) ([]v1.Pod, error) {
	k8sOps := k8sApps
	var pods []v1.Pod

	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			depPods, err := k8sOps.GetDeploymentPods(obj)
			if err != nil {
				return nil, err
			}
			pods = append(pods, depPods...)
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			ssPods, err := k8sOps.GetStatefulSetPods(obj)
			if err != nil {
				return nil, err
			}
			pods = append(pods, ssPods...)
		}
	}

	return pods, nil
}

// Describe describe the test case
func (k *K8s) Describe(ctx *scheduler.Context) (string, error) {
	var buf bytes.Buffer
	var err error
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("Deployment: [%s] %s", obj.Namespace, obj.Name)))
			var depStatus *appsapi.DeploymentStatus
			if depStatus, err = k8sApps.DescribeDeployment(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetAppStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of deployment: %v. Err: %v", obj.Name, err),
				}))
			}
			// Dump depStatus
			buf.WriteString(fmt.Sprintf("%+v\n", *depStatus))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "Deployment", obj.Name)))
			pods, _ := k8sApps.GetDeploymentPods(obj)
			for _, pod := range pods {
				buf.WriteString(dumpPodStatusRecursively(pod))
			}
			buf.WriteString(insertLineBreak("END Deployment"))
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("StatefulSet: [%s] %s", obj.Namespace, obj.Name)))
			var ssetStatus *appsapi.StatefulSetStatus
			if ssetStatus, err = k8sApps.DescribeStatefulSet(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetAppStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of statefulset: %v. Err: %v", obj.Name, err),
				}))
			}
			// Dump ssetStatus
			buf.WriteString(fmt.Sprintf("%+v\n", *ssetStatus))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "StatefulSet", obj.Name)))
			pods, _ := k8sApps.GetStatefulSetPods(obj)
			for _, pod := range pods {
				buf.WriteString(dumpPodStatusRecursively(pod))
			}
			buf.WriteString(insertLineBreak("END StatefulSet"))
		} else if obj, ok := specObj.(*v1.Service); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("Service: [%s] %s", obj.Namespace, obj.Name)))
			var svcStatus *v1.ServiceStatus
			if svcStatus, err = k8sCore.DescribeService(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetAppStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of service: %v. Err: %v", obj.Name, err),
				}))
			}
			// Dump service status
			buf.WriteString(fmt.Sprintf("%+v\n", *svcStatus))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "Service", obj.Name)))
			buf.WriteString(insertLineBreak("END Service"))
		} else if obj, ok := specObj.(*v1.PersistentVolumeClaim); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("PersistentVolumeClaim: [%s] %s", obj.Namespace, obj.Name)))
			var pvcStatus *v1.PersistentVolumeClaimStatus
			if pvcStatus, err = k8sCore.GetPersistentVolumeClaimStatus(obj); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetStorageStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of persistent volume claim: %v. Err: %v", obj.Name, err),
				}))
			}
			// Dump persistent volume claim status
			buf.WriteString(fmt.Sprintf("%+v\n", *pvcStatus))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "PersistentVolumeClaim", obj.Name)))
			buf.WriteString(insertLineBreak("END PersistentVolumeClaim"))
		} else if obj, ok := specObj.(*storageapi.StorageClass); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("StorageClass: %s", obj.Name)))
			var scParams map[string]string
			if scParams, err = k8sStorage.GetStorageClassParams(obj); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetVolumeParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get parameters of storage class: %v. Err: %v", obj.Name, err),
				}))
			}
			// Dump storage class parameters
			buf.WriteString(fmt.Sprintf("%+v\n", scParams))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "StorageClass", obj.Name)))
			buf.WriteString(insertLineBreak("END StorageClass"))
		} else if obj, ok := specObj.(*v1.Pod); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("Pod: [%s] %s", obj.Namespace, obj.Name)))
			var podStatus *v1.PodList
			if podStatus, err = k8sCore.GetPods(obj.Name, nil); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetPodStatus{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get status of pod: %v. Err: %v", obj.Name, err),
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", podStatus))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "Pod", obj.Name)))
			buf.WriteString(insertLineBreak("END Pod"))
		} else if obj, ok := specObj.(*storkapi.ClusterPair); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("ClusterPair: [%s] %s", obj.Namespace, obj.Name)))
			var clusterPair *storkapi.ClusterPair
			if clusterPair, err = k8sStork.GetClusterPair(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get cluster Pair: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", clusterPair))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "ClusterPair", obj.Name)))
			buf.WriteString(insertLineBreak("END ClusterPair"))
		} else if obj, ok := specObj.(*storkapi.Migration); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("Migration: [%s] %s", obj.Namespace, obj.Name)))
			var migration *storkapi.Migration
			if migration, err = k8sStork.GetMigration(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get Migration: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", migration))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "Migration", obj.Name)))
			buf.WriteString(insertLineBreak("END Migration"))
		} else if obj, ok := specObj.(*storkapi.MigrationSchedule); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("MigrationSchedule: [%s] %s", obj.Namespace, obj.Name)))
			var migrationSchedule *storkapi.MigrationSchedule
			if migrationSchedule, err = k8sStork.GetMigrationSchedule(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get MigrationSchedule: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", migrationSchedule))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "MigrationSchedule", obj.Name)))
			buf.WriteString(insertLineBreak("END MigrationSchedule"))
		} else if obj, ok := specObj.(*storkapi.BackupLocation); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("BackupLocation: [%s] %s", obj.Namespace, obj.Name)))
			var backupLocation *storkapi.BackupLocation
			if backupLocation, err = k8sStork.GetBackupLocation(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get BackupLocation: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", backupLocation))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "BackupLocation", obj.Name)))
			buf.WriteString(insertLineBreak("END BackupLocation"))
		} else if obj, ok := specObj.(*storkapi.ApplicationBackup); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("ApplicationBackup: [%s] %s", obj.Namespace, obj.Name)))
			var applicationBackup *storkapi.ApplicationBackup
			if applicationBackup, err = k8sStork.GetApplicationBackup(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get ApplicationBackup: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", applicationBackup))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "ApplicationBackup", obj.Name)))
			buf.WriteString(insertLineBreak("END ApplicationBackup"))
		} else if obj, ok := specObj.(*storkapi.ApplicationRestore); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("ApplicationRestore: [%s] %s", obj.Namespace, obj.Name)))
			var applicationRestore *storkapi.ApplicationRestore
			if applicationRestore, err = k8sStork.GetApplicationRestore(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get ApplicationRestore: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", applicationRestore))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "ApplicationRestore", obj.Name)))
			buf.WriteString(insertLineBreak("END ApplicationRestore"))
		} else if obj, ok := specObj.(*storkapi.ApplicationClone); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("ApplicationClone: [%s] %s", obj.Namespace, obj.Name)))
			var applicationClone *storkapi.ApplicationClone
			if applicationClone, err = k8sStork.GetApplicationClone(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get ApplicationClone: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", applicationClone))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "ApplicationClone", obj.Name)))
			buf.WriteString(insertLineBreak("END ApplicationClone"))
		} else if obj, ok := specObj.(*storkapi.VolumeSnapshotRestore); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("VolumeSnapshotRestore: [%s] %s", obj.Namespace, obj.Name)))
			var volumeSnapshotRestore *storkapi.VolumeSnapshotRestore
			if volumeSnapshotRestore, err = k8sStork.GetVolumeSnapshotRestore(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get VolumeSnapshotRestore: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", volumeSnapshotRestore))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "VolumeSnapshotRestore", obj.Name)))
			buf.WriteString(insertLineBreak("END VolumeSnapshotRestore"))
		} else if obj, ok := specObj.(*snapv1.VolumeSnapshot); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("VolumeSnapshot: [%s] %s", obj.Metadata.Namespace, obj.Metadata.Name)))
			var volumeSnapshotStatus *snapv1.VolumeSnapshotStatus
			if volumeSnapshotStatus, err = k8sExternalStorage.GetSnapshotStatus(obj.Metadata.Name, obj.Metadata.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Metadata.Name,
					Cause: fmt.Sprintf("Failed to get VolumeSnapshot: %v. Err: %v", obj.Metadata.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", volumeSnapshotStatus))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Metadata.Name, "VolumeSnapshot", obj.Metadata.Name)))
			buf.WriteString(insertLineBreak("END VolumeSnapshot"))
		} else if obj, ok := specObj.(*apapi.AutopilotRule); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("AutopilotRule: [%s] %s", obj.Namespace, obj.Name)))
			var autopilotRule *apapi.AutopilotRule
			if autopilotRule, err = k8sAutopilot.GetAutopilotRule(obj.Name); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetCustomSpec{
					Name:  obj.Name,
					Cause: fmt.Sprintf("Failed to get AutopilotRule: %v. Err: %v", obj.Name, err),
					Type:  obj,
				}))
			}
			buf.WriteString(fmt.Sprintf("%v\n", autopilotRule))
			buf.WriteString(fmt.Sprintf("%v", dumpEvents(obj.Namespace, "AutopilotRule", obj.Name)))
			buf.WriteString(insertLineBreak("END AutopilotRule"))
		} else if obj, ok := specObj.(*v1.Secret); ok {
			buf.WriteString(insertLineBreak(fmt.Sprintf("Secret: [%s] %s", obj.Namespace, obj.Name)))
			var secret *v1.Secret
			if secret, err = k8sCore.GetSecret(obj.Name, obj.Namespace); err != nil {
				buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetSecret{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get secret : %v. Error is : %v", obj.Name, err),
				}))
			}
			buf.WriteString(fmt.Sprintf("%+v\n", secret))
			buf.WriteString(insertLineBreak("END Secret"))
		} else {
			logrus.Warnf("Object type unknown/not supported: %v", obj)
		}
	}
	return buf.String(), nil
}

// ScaleApplication  Scale the application
func (k *K8s) ScaleApplication(ctx *scheduler.Context, scaleFactorMap map[string]int32) error {
	k8sOps := k8sApps
	for _, specObj := range ctx.App.SpecList {
		if !k.IsScalable(specObj) {
			continue
		}
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			logrus.Infof("Scale all Deployments")
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
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
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

// GetScaleFactorMap Get scale Factory map
//
func (k *K8s) GetScaleFactorMap(ctx *scheduler.Context) (map[string]int32, error) {
	k8sOps := k8sApps
	scaleFactorMap := make(map[string]int32, len(ctx.App.SpecList))
	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			dep, err := k8sOps.GetDeployment(obj.Name, obj.Namespace)
			if err != nil {
				return scaleFactorMap, err
			}
			scaleFactorMap[obj.Name+DeploymentSuffix] = *dep.Spec.Replicas
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			ss, err := k8sOps.GetStatefulSet(obj.Name, obj.Namespace)
			if err != nil {
				return scaleFactorMap, err
			}
			scaleFactorMap[obj.Name+StatefulSetSuffix] = *ss.Spec.Replicas
		}
	}
	return scaleFactorMap, nil
}

// StopSchedOnNode stop schedule on node
func (k *K8s) StopSchedOnNode(n node.Node) error {
	driver, _ := node.Get(k.NodeDriverName)
	systemOpts := node.SystemctlOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         FindFilesOnWorkerTimeout,
			TimeBeforeRetry: DefaultRetryInterval,
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

// StartSchedOnNode start schedule on node
func (k *K8s) StartSchedOnNode(n node.Node) error {
	driver, _ := node.Get(k.NodeDriverName)
	systemOpts := node.SystemctlOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         DefaultTimeout,
			TimeBeforeRetry: DefaultRetryInterval,
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

// EnableSchedulingOnNode enable apps to be scheduled to a given k8s worker node
func (k *K8s) EnableSchedulingOnNode(n node.Node) error {
	return k8sCore.UnCordonNode(n.Name, DefaultTimeout, DefaultRetryInterval)
}

// DisableSchedulingOnNode disable apps to be scheduled to a given k8s worker node
func (k *K8s) DisableSchedulingOnNode(n node.Node) error {
	return k8sCore.CordonNode(n.Name, DefaultTimeout, DefaultRetryInterval)
}

// IsScalable check whether scalable
func (k *K8s) IsScalable(spec interface{}) bool {
	if obj, ok := spec.(*appsapi.Deployment); ok {
		dep, err := k8sApps.GetDeployment(obj.Name, obj.Namespace)
		if err != nil {
			logrus.Errorf("Failed to retrieve deployment [%s] %s. Cause: %v", obj.Namespace, obj.Name, err)
			return false
		}
		for _, vol := range dep.Spec.Template.Spec.Volumes {
			pvcName := vol.PersistentVolumeClaim.ClaimName
			pvc, err := k8sCore.GetPersistentVolumeClaim(pvcName, dep.Namespace)
			if err != nil {
				logrus.Errorf("Failed to retrieve PVC [%s] %s. Cause: %v", obj.Namespace, pvcName, err)
				return false
			}
			for _, ac := range pvc.Spec.AccessModes {
				if ac == v1.ReadWriteOnce {
					return false
				}
			}
		}
	} else if _, ok := spec.(*appsapi.StatefulSet); ok {
		return true
	}
	return false
}

// GetTokenFromConfigMap -  Retrieve the config map object and get auth-token
func (k *K8s) GetTokenFromConfigMap(configMapName string) (string, error) {
	var token string
	var err error
	var configMap *v1.ConfigMap
	k8sOps := k8sCore
	if configMap, err = k8sOps.GetConfigMap(configMapName, "default"); err == nil {
		if secret, err := k8sOps.GetSecret(configMap.Data[secretNameKey], configMap.Data[secretNamespaceKey]); err == nil {
			if tk, ok := secret.Data["auth-token"]; ok {
				token = string(tk)
			}
		}
	}
	logrus.Infof("Token from secret: %s", token)
	return token, err
}

func (k *K8s) createMigrationObjects(
	specObj interface{},
	ns *v1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {
	k8sOps := k8sStork
	if obj, ok := specObj.(*storkapi.ClusterPair); ok {
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
	} else if obj, ok := specObj.(*storkapi.Migration); ok {
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
	} else if obj, ok := specObj.(*storkapi.MigrationSchedule); ok {
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
	} else if obj, ok := specObj.(*storkapi.SchedulePolicy); ok {
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

func (k *K8s) getPodsUsingStorage(pods []v1.Pod, provisioner string) []v1.Pod {
	k8sOps := k8sCore
	podsUsingStorage := make([]v1.Pod, 0)
	for _, pod := range pods {
		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim == nil {
				continue
			}
			pvc, err := k8sOps.GetPersistentVolumeClaim(vol.PersistentVolumeClaim.ClaimName, pod.Namespace)
			if err != nil {
				logrus.Errorf("failed to get pvc [%s] %s. Cause: %v", vol.PersistentVolumeClaim.ClaimName, pod.Namespace, err)
				return podsUsingStorage
			}
			if scProvisioner, err := k8sOps.GetStorageProvisionerForPVC(pvc); err == nil && scProvisioner == volume.GetStorageProvisioner() {
				podsUsingStorage = append(podsUsingStorage, pod)
				break
			}
		}
	}
	return podsUsingStorage
}

// PrepareNodeToDecommission Prepare the Node for decommission
func (k *K8s) PrepareNodeToDecommission(n node.Node, provisioner string) error {
	k8sOps := k8sCore
	pods, err := k8sOps.GetPodsByNode(n.Name, "")
	if err != nil {
		return &scheduler.ErrFailedToDecommissionNode{
			Node:  n,
			Cause: fmt.Sprintf("Failed to get pods on the node: %v. Err: %v", n.Name, err),
		}
	}
	podsUsingStorage := k.getPodsUsingStorage(pods.Items, provisioner)
	// double the timeout every 40 pods
	timeout := DefaultTimeout * time.Duration(len(podsUsingStorage)/40+1)
	if err = k8sOps.DrainPodsFromNode(n.Name, podsUsingStorage, timeout, DefaultRetryInterval); err != nil {
		return &scheduler.ErrFailedToDecommissionNode{
			Node:  n,
			Cause: fmt.Sprintf("Failed to drain pods from node: %v. Err: %v", n.Name, err),
		}
	}
	return nil
}

func (k *K8s) destroyMigrationObject(
	specObj interface{},
	app *spec.AppSpec,
) error {
	k8sOps := k8sStork
	if obj, ok := specObj.(*storkapi.ClusterPair); ok {
		err := k8sOps.DeleteClusterPair(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete ClusterPair: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed ClusterPair: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*storkapi.Migration); ok {
		err := k8sOps.DeleteMigration(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete Migration: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed Migration: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*storkapi.MigrationSchedule); ok {
		err := k8sOps.DeleteMigrationSchedule(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete MigrationSchedule: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed MigrationSchedule: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*storkapi.SchedulePolicy); ok {
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

func (k *K8s) destroyVolumeSnapshotRestoreObject(
	specObj interface{},
	app *spec.AppSpec,
) error {
	k8sOps := k8sStork
	if obj, ok := specObj.(*storkapi.VolumeSnapshotRestore); ok {
		err := k8sOps.DeleteVolumeSnapshotRestore(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete VolumeSnapshotRestore: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed VolumeSnapshotRestore: %v", app.Key, obj.Name)
	}
	return nil
}

// ValidateVolumeSnapshotRestore return nil if snapshot is restored successuflly to
// parent volumes
func (k *K8s) ValidateVolumeSnapshotRestore(ctx *scheduler.Context, timeStart time.Time) error {
	var err error
	var snapRestore *storkapi.VolumeSnapshotRestore
	if ctx == nil {
		return fmt.Errorf("no context provided")
	}
	// extract volume name and snapshotname from context
	// can do it using snapRestore.Status.Volume
	k8sOps := k8sStork
	specObjects := ctx.App.SpecList
	driver, err := volume.Get(k.VolDriverName)
	if err != nil {
		return err
	}

	for _, specObj := range specObjects {
		if obj, ok := specObj.(*storkapi.VolumeSnapshotRestore); ok {
			snapRestore, err = k8sOps.GetVolumeSnapshotRestore(obj.Name, obj.Namespace)
			if err != nil {
				return fmt.Errorf("unable to restore volumesnapshotrestore details %v", err)
			}
			err = k8sOps.ValidateVolumeSnapshotRestore(snapRestore.Name, snapRestore.Namespace, DefaultTimeout,
				DefaultRetryInterval)
			if err != nil {
				return err
			}

		}
	}
	if snapRestore == nil {
		return fmt.Errorf("no valid volumesnapshotrestore specs found")
	}

	for _, vol := range snapRestore.Status.Volumes {
		logrus.Infof("validating volume %v is restored from %v", vol.Volume, vol.Snapshot)
		snapshotData, err := k8sExternalStorage.GetSnapshotData(vol.Snapshot)
		if err != nil {
			return fmt.Errorf("failed to retrieve VolumeSnapshotData %s: %v",
				vol.Snapshot, err)
		}
		err = k8sExternalStorage.ValidateSnapshotData(snapshotData.Metadata.Name, false, DefaultTimeout, DefaultRetryInterval)
		if err != nil {
			return fmt.Errorf("snapshot: %s is not complete. %v", snapshotData.Metadata.Name, err)
		}
		// validate each snap restore
		if err := driver.ValidateVolumeSnapshotRestore(vol.Volume, snapshotData, timeStart); err != nil {
			return err
		}
	}

	return nil
}

func (k *K8s) createBackupObjects(
	specObj interface{},
	ns *v1.Namespace,
	app *spec.AppSpec,
) (interface{}, error) {
	k8sOps := k8sStork
	if obj, ok := specObj.(*storkapi.BackupLocation); ok {
		obj.Namespace = ns.Name
		backupLocation, err := k8sOps.CreateBackupLocation(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create BackupLocation: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created BackupLocation: %v", app.Key, backupLocation.Name)
		return backupLocation, nil
	} else if obj, ok := specObj.(*storkapi.ApplicationBackup); ok {
		obj.Namespace = ns.Name
		applicationBackup, err := k8sOps.CreateApplicationBackup(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create ApplicationBackup: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created ApplicationBackup: %v", app.Key, applicationBackup.Name)
		return applicationBackup, nil
	} else if obj, ok := specObj.(*storkapi.ApplicationRestore); ok {
		obj.Namespace = ns.Name
		applicationRestore, err := k8sOps.CreateApplicationRestore(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create ApplicationRestore: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created ApplicationRestore: %v", app.Key, applicationRestore.Name)
		return applicationRestore, nil
	} else if obj, ok := specObj.(*storkapi.ApplicationClone); ok {
		applicationClone, err := k8sOps.CreateApplicationClone(obj)
		if err != nil {
			return nil, &scheduler.ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create ApplicationClone: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Created ApplicationClone: %v", app.Key, applicationClone.Name)
		return applicationClone, nil
	}
	return nil, nil
}

func (k *K8s) destroyBackupObjects(
	specObj interface{},
	app *spec.AppSpec,
) error {
	k8sOps := k8sStork
	if obj, ok := specObj.(*storkapi.BackupLocation); ok {
		err := k8sOps.DeleteBackupLocation(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete BackupLocation: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed BackupLocation: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*storkapi.ApplicationBackup); ok {
		err := k8sOps.DeleteApplicationBackup(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete ApplicationBackup: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed ApplicationBackup: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*storkapi.ApplicationRestore); ok {
		err := k8sOps.DeleteApplicationRestore(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete ApplicationRestore: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed ApplicationRestore: %v", app.Key, obj.Name)
	} else if obj, ok := specObj.(*storkapi.ApplicationClone); ok {
		err := k8sOps.DeleteApplicationClone(obj.Name, obj.Namespace)
		if err != nil {
			return &scheduler.ErrFailedToDestroyApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to delete ApplicationClone: %v. Err: %v", obj.Name, err),
			}
		}
		logrus.Infof("[%v] Destroyed ApplicationClone: %v", app.Key, obj.Name)
	}
	return nil
}

// EstimatePVCExpansion calculates expected size of PVC based on autopilot rule and workload
func (k *K8s) EstimatePVCExpansion(pvc *v1.PersistentVolumeClaim, apRule apapi.AutopilotRule, wSize uint64) (uint64, int, error) {
	pvcObjSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	pvcSize, _ := pvcObjSize.AsInt64()
	expectedPVCSize := uint64(pvcSize)

	volDriver, err := volume.Get(k.VolDriverName)
	if err != nil {
		return 0, 0, err
	}
	if isAutopilotMatchPvcLabels(apRule, pvc) {
		return volDriver.EstimateVolumeExpand(apRule, expectedPVCSize, wSize)
	}
	return expectedPVCSize, 0, nil
}

// ValidateAutopilotEvents verifies proper alerts and events on resize completion
func (k *K8s) ValidateAutopilotEvents(ctx *scheduler.Context) error {

	eventMap := make(map[string]int32)
	for _, event := range k.GetEvents()["AutopilotRule"] {
		eventMap[event.Message] = event.Count
	}

	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*v1.PersistentVolumeClaim); ok {

			autopilotEnabled := false
			var err error
			if pvcAnnotationValue, ok := obj.Annotations[autopilotEnabledAnnotationKey]; ok {
				autopilotEnabled, err = strconv.ParseBool(pvcAnnotationValue)
				if err != nil {
					return err
				}
			}
			if autopilotEnabled {
				listApRules, err := autopilot.Instance().ListAutopilotRules()
				if err != nil {
					return err
				}
				wSize, err := k.GetWorkloadSizeFromAppSpec(ctx)
				if err != nil {
					return err
				}
				for _, rule := range listApRules.Items {
					_, resizeCount, err := k.EstimatePVCExpansion(obj, rule, wSize)
					if err != nil {
						return err
					}
					coolDownPeriod := rule.Spec.ActionsCoolDownPeriod
					if coolDownPeriod == 0 {
						coolDownPeriod = 5 // default autopilot cool down period
					}
					// sleep to wait until all events are published
					time.Sleep(time.Second*time.Duration(coolDownPeriod) + 10)

					objectToValidateName := fmt.Sprintf("%s:pvc-%s", rule.Name, obj.UID)
					logrus.Infof("[%s] Validating events", objectToValidateName)
					err = validateEvents(objectToValidateName, eventMap, int32(resizeCount))
					if err != nil {
						return err
					}
				}
			}
		}
	}
	logrus.Infof("Finished validating events")
	return nil
}

func validateEvents(objName string, events map[string]int32, count int32) error {
	logrus.Debugf("expected %d resized in events validation", count)
	if count == 0 {
		// nothing to validate
		return nil
	}
	expectedEvents := map[string]int32{
		fmt.Sprintf("rule: %s transition from %s => %s", objName, apapi.RuleStateInit, apapi.RuleStateNormal):                                  1,
		fmt.Sprintf("rule: %s transition from %s => %s", objName, apapi.RuleStateNormal, apapi.RuleStateTriggered):                             count,
		fmt.Sprintf("rule: %s transition from %s => %s", objName, apapi.RuleStateTriggered, apapi.RuleStateActiveActionsPending):               count,
		fmt.Sprintf("rule: %s transition from %s => %s", objName, apapi.RuleStateActiveActionsPending, apapi.RuleStateActiveActionsInProgress): count,
		fmt.Sprintf("rule: %s transition from %s => %s", objName, apapi.RuleStateActiveActionsInProgress, apapi.RuleStateActiveActionsTaken):   count,
		fmt.Sprintf("rule: %s transition from %s => %s", objName, apapi.RuleStateActiveActionsTaken, apapi.RuleStateNormal):                    count,
	}

	for message, expectedCount := range expectedEvents {
		if _, ok := events[message]; !ok {
			return fmt.Errorf("expected event with message '%s'", message)
		}
		occurrences := events[message]
		// Object should change its state exactly as expected amount for all following states
		if strings.Contains(message, fmt.Sprintf("%s => %s", apapi.RuleStateInit, apapi.RuleStateNormal)) ||
			strings.Contains(message, fmt.Sprintf("%s => %s", apapi.RuleStateActiveActionsInProgress, apapi.RuleStateActiveActionsTaken)) ||
			strings.Contains(message, fmt.Sprintf("%s => %s", apapi.RuleStateActiveActionsTaken, apapi.RuleStateNormal)) {
			if occurrences != expectedCount {
				return fmt.Errorf("expected number of occurances %d for event message '%s'. Got: %d", expectedCount, message, occurrences)
			}
			continue
		}
		// it's possible that object changes the following states more times than expected if action is declined
		// Normal => Triggered
		// Triggered => ActiveActionsPending
		// ActiveActionsPending => ActiveActionsInProgress
		if !(occurrences >= expectedCount) {
			return fmt.Errorf("expected number of occurances >= %d for event message '%s'. Got: %d", expectedCount, message, occurrences)
		}
	}

	// TODO: for negative case (when added) if action is declined, check for the following states:
	// ActiveActionsInProgress => ActionsDeclined
	// ActionsDeclined => Triggered
	return nil
}

func isAutopilotMatchPvcLabels(apRule apapi.AutopilotRule, pvc *v1.PersistentVolumeClaim) bool {
	apRuleLabels := apRule.Spec.Selector.LabelSelector.MatchLabels
	for k, v := range apRuleLabels {
		if pvcLabelValue, ok := pvc.Labels[k]; ok {
			if pvcLabelValue == v {
				return true
			}
		}
	}
	return false
}

// collectEvents collects all autopilot events until caller stops the process
func (k *K8s) collectEvents() error {
	logrus.Info("Started collecting events")

	lock := sync.Mutex{}
	t := time.Now()
	namespace := ""
	clientset, err := getKubeClient("")
	if err != nil {
		return err
	}

	iface, err := clientset.CoreV1().Events(namespace).Watch(metav1.ListOptions{})
	if err != nil {
		return err
	}
	for i := range iface.ResultChan() {
		event, ok := i.Object.(*v1.Event)
		if event == nil {
			continue
		}
		if ok {
			if !event.LastTimestamp.After(t) {
				// skip this event since it happened before event collection has been started
				continue
			}
			e := scheduler.Event{
				Message:   event.Message,
				EventTime: event.EventTime,
				Count:     event.Count,
				LastSeen:  event.LastTimestamp,
				Kind:      event.Kind,
				Type:      event.Type,
			}

			lock.Lock()
			storage := k.eventsStorage[event.InvolvedObject.Kind]
			storage = append(storage, e)
			k.eventsStorage[event.InvolvedObject.Kind] = storage
			lock.Unlock()
		}
	}
	return nil
}

func getKubeClient(kubeconfig string) (kubernetes.Interface, error) {
	var cfg *rest.Config
	var err error

	if len(kubeconfig) == 0 {
		kubeconfig = os.Getenv("KUBECONFIG")
	}

	if len(kubeconfig) > 0 {
		logrus.Debugf("using kubeconfig: %s to create k8s client", kubeconfig)
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		logrus.Debugf("will use in-cluster config to create k8s client")
		cfg, err = rest.InClusterConfig()
	}

	if err != nil {
		return nil, err
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return kubeClient, nil
}

// GetEvents dumps events from event storage
func (k *K8s) GetEvents() map[string][]scheduler.Event {
	logrus.Infof("Getting events for validation")
	copyMap := make(map[string][]scheduler.Event)
	// Copy from the original map to the target map
	for key, value := range k.eventsStorage {
		copyMap[key] = value
	}
	return copyMap
}

// AddLabelOnNode adds label for a given node
func (k *K8s) AddLabelOnNode(n node.Node, lKey string, lValue string) error {
	k8sOps := k8sCore

	if err := k8sOps.AddLabelOnNode(n.Name, lKey, lValue); err != nil {
		return &scheduler.ErrFailedToAddLabelOnNode{
			Key:   lKey,
			Value: lValue,
			Node:  n,
			Cause: fmt.Sprintf("Failed to add label on node. Err: %v", err),
		}
	}
	logrus.Infof("Added label on %s node: %s=%s", n.Name, lKey, lValue)
	return nil
}

// IsAutopilotEnabledForVolume checks if autopilot enabled for a given volume
func (k *K8s) IsAutopilotEnabledForVolume(vol *volume.Volume) bool {
	autopilotEnabled := false
	if volAnnotationValue, ok := vol.Annotations[autopilotEnabledAnnotationKey]; ok {
		autopilotEnabled, _ = strconv.ParseBool(volAnnotationValue)
	}
	return autopilotEnabled
}

// SaveSchedulerLogsToFile gathers all scheduler logs into a file
func (k *K8s) SaveSchedulerLogsToFile(n node.Node, location string) error {
	driver, _ := node.Get(k.NodeDriverName)
	cmd := fmt.Sprintf("journalctl -lu %s* > %s/kubelet.log", SystemdSchedServiceName, location)
	_, err := driver.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         DefaultTimeout,
		TimeBeforeRetry: DefaultRetryInterval,
		Sudo:            true,
	})
	return err
}

func (k *K8s) addLabelsToPVC(pvc *v1.PersistentVolumeClaim, labels map[string]string) {
	if len(pvc.Labels) == 0 {
		pvc.Labels = map[string]string{}
	}
	for k, v := range labels {
		pvc.Labels[k] = v
	}
}

// CreateAutopilotRule creates the AutopilotRule object
func (k *K8s) CreateAutopilotRule(apRule apapi.AutopilotRule) (*apapi.AutopilotRule, error) {
	t := func() (interface{}, bool, error) {
		apRule.Labels = defaultTorpedoLabel
		aRule, err := k8sAutopilot.CreateAutopilotRule(&apRule)
		if errors.IsAlreadyExists(err) {
			if rule, err := k8sAutopilot.GetAutopilotRule(apRule.Name); err == nil {
				logrus.Infof("Using existing AutopilotRule: %v", rule.Name)
				return aRule, false, nil
			}
		}
		if err != nil {
			return nil, true, fmt.Errorf("Failed to create autopilot rule: %v. Err: %v", apRule.Name, err)
		}
		return aRule, false, nil
	}

	apRuleObj, err := task.DoRetryWithTimeout(t, k8sObjectCreateTimeout, DefaultRetryInterval)
	if err != nil {
		return nil, err
	}
	logrus.Infof("Created autopilot rule: %+v", apRuleObj)

	return apRuleObj.(*apapi.AutopilotRule), nil
}

// UpdateAutopilotRule updates the AutopilotRule
func (k *K8s) UpdateAutopilotRule(apRule apapi.AutopilotRule) (*apapi.AutopilotRule, error) {
	aRule, err := k8sAutopilot.GetAutopilotRule(apRule.Name)
	if err != nil {
		return nil, err
	}
	aRule.Spec = apRule.Spec

	return k8sAutopilot.UpdateAutopilotRule(aRule)
}

// ListAutopilotRules lists AutopilotRules
func (k *K8s) ListAutopilotRules() (*apapi.AutopilotRuleList, error) {
	return k8sAutopilot.ListAutopilotRules()
}

func insertLineBreak(note string) string {
	return fmt.Sprintf("------------------------------\n%s\n------------------------------\n", note)
}

func dumpPodStatusRecursively(pod v1.Pod) string {
	var buf bytes.Buffer
	buf.WriteString(insertLineBreak(fmt.Sprintf("Pod: [%s] %s", pod.Namespace, pod.Name)))
	buf.WriteString(fmt.Sprintf("%v\n", pod.Status))
	for _, conStat := range pod.Status.ContainerStatuses {
		buf.WriteString(insertLineBreak(fmt.Sprintf("container: %s", conStat.Name)))
		buf.WriteString(fmt.Sprintf("%v\n", conStat))
		buf.WriteString(insertLineBreak("END container"))
	}
	buf.WriteString(dumpEvents(pod.Namespace, "Pod", pod.Name))
	buf.WriteString(insertLineBreak("END pod"))
	return buf.String()
}

func dumpEvents(namespace, resourceType, name string) string {
	var buf bytes.Buffer
	fields := fmt.Sprintf("involvedObject.kind=%s,involvedObject.name=%s", resourceType, name)
	events, err := k8sCore.ListEvents(namespace, metav1.ListOptions{FieldSelector: fields})
	if err != nil {
		buf.WriteString(fmt.Sprintf("%v", &scheduler.ErrFailedToGetEvents{
			Type:  resourceType,
			Name:  name,
			Cause: err.Error(),
		}))
	}
	buf.WriteString(insertLineBreak("Events:"))
	buf.WriteString(fmt.Sprintf("%v\n", events))
	return buf.String()
}

func init() {
	k := &K8s{}
	scheduler.Register(SchedName, k)
}
