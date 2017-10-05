package k8s

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	k8s_ops "github.com/portworx/torpedo/pkg/k8sops"
	"github.com/portworx/torpedo/pkg/task"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/pkg/api/v1"
	apps_api "k8s.io/client-go/pkg/apis/apps/v1beta1"
	storage_api "k8s.io/client-go/pkg/apis/storage/v1"
)

// SchedName is the name of the kubernetes scheduler driver implementation
const (
	SchedName      = "k8s"
	k8sPodsRootDir = "/var/lib/kubelet/pods"
)

type k8s struct {
	nodes          map[string]node.Node
	specFactory    *spec.Factory
	nodeDriverName string
}

func (k *k8s) GetNodes() []node.Node {
	var ret []node.Node
	for _, val := range k.nodes {
		ret = append(ret, val)
	}
	return ret
}

func (k *k8s) IsNodeReady(n node.Node) error {
	t := func() (interface{}, error) {
		if err := k8s_ops.Instance().IsNodeReady(n.Name); err != nil {
			return "", &ErrNodeNotReady{
				Node:  n,
				Cause: err.Error(),
			}
		}

		return "", nil
	}

	if _, err := task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second); err != nil {
		logrus.Infof("[debug] node timed out. %#v", n)
		return err
	}

	return nil
}

// String returns the string name of this driver.
func (k *k8s) String() string {
	return SchedName
}

func (k *k8s) Init(specDir string, nodeDriverName string) error {
	nodes, err := k8s_ops.Instance().GetNodes()
	if err != nil {
		return err
	}

	for _, n := range nodes.Items {
		k.nodes[n.Name] = k.parseK8SNode(n)
	}

	k.specFactory, err = spec.NewFactory(specDir, k)
	if err != nil {
		return err
	}

	k.nodeDriverName = nodeDriverName
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
				obj, _, err := scheme.Codecs.UniversalDeserializer().Decode([]byte(specContents), nil, nil)
				if err != nil {
					return nil, err
				}

				specObj, err := validateSpec(obj)
				if err != nil {
					logrus.Warnf("%s. Parser skipping the spec", err)
					return nil, nil
				}

				specs = append(specs, specObj)
			}
		}
	}

	return specs, nil
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
	}
	return nil, fmt.Errorf("Unsupported object")
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

func getAppNamespaceName(app *spec.AppSpec, instanceID string) string {
	return fmt.Sprintf("%s-%s", app.Key, instanceID)
}

func (k *k8s) Schedule(instanceID string, options scheduler.ScheduleOptions) ([]*scheduler.Context, error) {
	var apps []*spec.AppSpec
	if options.AppKeys != nil && len(options.AppKeys) > 0 {
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
		appNamespace := getAppNamespaceName(app, instanceID)
		ns, err := k8s_ops.Instance().CreateNamespace(appNamespace, map[string]string{
			"creater": "torpedo",
			"app":     app.Key,
		})
		if err != nil {
			return nil, &ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create namespace: %v. Err: %v", appNamespace, err),
			}
		}

		var specObjects []interface{}

		for _, spec := range app.SpecList {
			obj, err := k.createStorageObject(spec, ns, app)
			if err != nil {
				return nil, err
			}
			if obj != nil {
				specObjects = append(specObjects, obj)
			}
		}

		for _, spec := range app.SpecList {
			obj, err := k.createCoreObject(spec, ns, app)
			if err != nil {
				return nil, err
			}
			if obj != nil {
				specObjects = append(specObjects, obj)
			}
		}

		ctx := &scheduler.Context{
			UID: instanceID,
			App: &spec.AppSpec{
				Key:      app.Key,
				SpecList: specObjects,
				Enabled:  app.Enabled,
			},
			// Status: TODO
			// Stdout: TODO
			// Stderr: TODO
		}

		contexts = append(contexts, ctx)
	}

	return contexts, nil
}

func (k *k8s) createStorageObject(spec interface{}, ns *v1.Namespace, app *spec.AppSpec) (interface{}, error) {
	k8sOps := k8s_ops.Instance()
	if obj, ok := spec.(*storage_api.StorageClass); ok {
		obj.Namespace = ns.Name
		sc, err := k8sOps.CreateStorageClass(obj)
		if err != nil {
			if matched, _ := regexp.MatchString(".+ already exists", err.Error()); !matched {
				return nil, &ErrFailedToScheduleApp{
					App:   app,
					Cause: fmt.Sprintf("Failed to create storage class: %v. Err: %v", sc.Name, err),
				}
			}

			sc, err = k8sOps.ValidateStorageClass(obj.Name)
			if err != nil {
				return nil, &ErrFailedToScheduleApp{
					App:   app,
					Cause: fmt.Sprintf("Failed to create storage class: %v. Err: %v", sc.Name, err),
				}
			}
		}

		logrus.Infof("[%v] Created storage class: %v", app.Key, sc.Name)
		return sc, nil
	} else if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
		obj.Namespace = ns.Name
		pvc, err := k8sOps.CreatePersistentVolumeClaim(obj)
		if err != nil {
			return nil, &ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create PVC: %v. Err: %v", pvc.Name, err),
			}
		}

		logrus.Infof("[%v] Created PVC: %v", app.Key, pvc.Name)
		return pvc, nil
	}

	return nil, nil
}

func (k *k8s) createCoreObject(spec interface{}, ns *v1.Namespace, app *spec.AppSpec) (interface{}, error) {
	k8sOps := k8s_ops.Instance()
	if obj, ok := spec.(*apps_api.Deployment); ok {
		obj.Namespace = ns.Name
		dep, err := k8sOps.CreateDeployment(obj)
		if err != nil {
			return nil, &ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Deployment: %v. Err: %v", dep.Name, err),
			}
		}
		logrus.Infof("[%v] Created deployment: %v", app.Key, dep.Name)
		return dep, nil

	} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
		obj.Namespace = ns.Name
		ss, err := k8sOps.CreateStatefulSet(obj)
		if err != nil {
			return nil, &ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create StatefulSet: %v. Err: %v", ss.Name, err),
			}
		}
		logrus.Infof("[%v] Created StatefulSet: %v", app.Key, ss.Name)
		return ss, nil

	} else if obj, ok := spec.(*v1.Service); ok {
		obj.Namespace = ns.Name
		svc, err := k8sOps.CreateService(obj)
		if err != nil {
			return nil, &ErrFailedToScheduleApp{
				App:   app,
				Cause: fmt.Sprintf("Failed to create Service: %v. Err: %v", svc.Name, err),
			}
		}
		logrus.Infof("[%v] Created Service: %v", app.Key, svc.Name)
		return svc, nil
	}

	return nil, nil
}

func (k *k8s) WaitForRunning(ctx *scheduler.Context) error {
	k8sOps := k8s_ops.Instance()
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			if err := k8sOps.ValidateDeployment(obj); err != nil {
				return &ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Deployment: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated deployment: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			if err := k8sOps.ValidateStatefulSet(obj); err != nil {
				return &ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated statefulset: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*v1.Service); ok {
			svc, err := k8sOps.GetService(obj.Name, obj.Namespace)
			if err != nil {
				return &ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Service: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated Service: %v", ctx.App.Key, svc.Name)
		}
	}

	return nil
}

func (k *k8s) Destroy(ctx *scheduler.Context, opts map[string]bool) error {
	k8sOps := k8s_ops.Instance()
	var podList []v1.Pod
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
				if pods, err := k8sOps.GetDeploymentPods(obj); err != nil {
					logrus.Warnf("[%v] Error getting deployment pods. Err: %v", ctx.App.Key, err)
				} else {
					podList = append(podList, pods...)
				}
			}
			if err := k8sOps.DeleteDeployment(obj); err != nil {
				return &ErrFailedToDestroyApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to destroy Deployment: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Destroyed deployment: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
				if pods, err := k8sOps.GetStatefulSetPods(obj); err != nil {
					logrus.Warnf("[%v] Error getting statefulset pods. Err: %v", ctx.App.Key, err)
				} else {
					podList = append(podList, pods...)
				}
			}
			if err := k8sOps.DeleteStatefulSet(obj); err != nil {
				return &ErrFailedToDestroyApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to destroy StatefulSet: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v]Destroyed StatefulSet: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*v1.Service); ok {
			if err := k8sOps.DeleteService(obj); err != nil {
				return &ErrFailedToDestroyApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to destroy Service: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Destroyed Service: %v", ctx.App.Key, obj.Name)
		}
	}

	appNamespace := getAppNamespaceName(ctx.App, ctx.UID)
	if err := k8sOps.DeleteNamespace(appNamespace); err != nil {
		return &ErrFailedToDestroyApp{
			App:   ctx.App,
			Cause: fmt.Sprintf("Failed to destroy namespace: %#v. Err: %v", appNamespace, err),
		}
	}

	logrus.Infof("[%v] Destroyed Namespace: %v", ctx.App.Key, appNamespace)

	if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
		if err := k.WaitForDestroy(ctx); err != nil {
			return err
		}
		if err := k.waitForCleanup(ctx, podList); err != nil {
			return err
		}
	} else if value, ok := opts[scheduler.OptionsWaitForDestroy]; ok && value {
		if err := k.WaitForDestroy(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (k *k8s) waitForCleanup(ctx *scheduler.Context, podList []v1.Pod) error {
	for _, pod := range podList {
		t := func() (interface{}, error) {
			return nil, k.validateVolumeDirCleanup(pod.UID, ctx.App)
		}
		if _, err := task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second); err != nil {
			return err
		}
		logrus.Infof("Validated resource cleanup for pod: %v", pod.UID)
	}
	return nil
}

func (k *k8s) validateVolumeDirCleanup(podUID types.UID, app *spec.AppSpec) error {
	podVolDir := k.getVolumeDirPath(podUID)
	options := node.ConnectionOpts{
		Timeout:         1 * time.Minute,
		TimeBeforeRetry: 10 * time.Second,
	}
	driver, _ := node.Get(k.nodeDriverName)

	for _, n := range k.GetNodes() {
		if n.Type == node.TypeWorker {
			if exists, err := driver.CheckIfPathExists(podVolDir, n, options); err != nil {
				return err
			} else if exists {
				return &ErrFailedToDeleteVolumeDirForPod{
					App:   app,
					Cause: fmt.Sprintf("Volume directory for pod %v still exists in node: %v", podUID, n.Name),
				}
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
				return &ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of deployment: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated destroy of Deployment: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			if err := k8sOps.ValidateTerminatedStatefulSet(obj); err != nil {
				return &ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of statefulset: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated destroy of StatefulSet: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*v1.Service); ok {
			if err := k8sOps.ValidateDeletedService(obj.Name, obj.Namespace); err != nil {
				return &ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate destroy of service: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated destroy of Service: %v", ctx.App.Key, obj.Name)
		}
	}
	return nil
}

func (k *k8s) DeleteTasks(ctx *scheduler.Context) error {
	k8sOps := k8s_ops.Instance()
	var pods []v1.Pod
	var err error
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			pods, err = k8sOps.GetDeploymentPods(obj)
			if err != nil {
				return &ErrFailedToDeleteTasks{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get pods due to: %v", err),
				}
			}
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			pods, err = k8sOps.GetStatefulSetPods(obj)
			if err != nil {
				return &ErrFailedToDeleteTasks{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get pods due to: %v", err),
				}
			}
		}
	}

	if err := k8sOps.DeletePods(pods); err != nil {
		return &ErrFailedToDeleteTasks{
			App:   ctx.App,
			Cause: fmt.Sprintf("failed to delete pods due to: %v", err),
		}
	}
	return nil
}

func (k *k8s) GetVolumes(ctx *scheduler.Context) ([]string, error) {
	var volumes []string
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			vol, err := k8s_ops.Instance().GetVolumeForPersistentVolumeClaim(obj)
			if err != nil {
				return nil, &ErrFailedToGetVolumesForApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get volume for PVC: %v. Err: %v", obj.Name, err),
				}
			}

			volumes = append(volumes, vol)
		}
	}

	return volumes, nil
}

func (k *k8s) GetVolumeParameters(ctx *scheduler.Context) (map[string]map[string]string, error) {
	k8sOps := k8s_ops.Instance()
	result := make(map[string]map[string]string)

	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			vol, err := k8sOps.GetVolumeForPersistentVolumeClaim(obj)
			if err != nil {
				return nil, &ErrFailedToGetVolumesParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get volume for PVC: %v. Err: %v", obj.Name, err),
				}
			}

			params, err := k8sOps.GetPersistentVolumeClaimParams(obj)
			if err != nil {
				return nil, &ErrFailedToGetVolumesParameters{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get params for volume: %v. Err: %v", obj.Name, err),
				}
			}
			result[vol] = params
		}
	}

	return result, nil
}

func (k *k8s) InspectVolumes(ctx *scheduler.Context) error {
	k8sOps := k8s_ops.Instance()
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*storage_api.StorageClass); ok {
			if _, err := k8sOps.ValidateStorageClass(obj.Name); err != nil {
				return &ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate StorageClass: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated storage class: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			if err := k8sOps.ValidatePersistentVolumeClaim(obj); err != nil {
				return &ErrFailedToValidateStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate PVC: %v. Err: %v", obj.Name, err),
				}
			}

			logrus.Infof("[%v] Validated PVC: %v", ctx.App.Key, obj.Name)
		}
	}

	return nil
}

func (k *k8s) DeleteVolumes(ctx *scheduler.Context) error {
	k8sOps := k8s_ops.Instance()
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*storage_api.StorageClass); ok {
			if err := k8sOps.DeleteStorageClass(obj.Name); err != nil {
				return &ErrFailedToDestroyStorage{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to destroy storage class: %v. Err: %v", obj.Name, err),
				}
			}
			logrus.Infof("[%v] Destroyed storage class: %v", ctx.App.Key, obj.Name)
		} else if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
			if err := k8sOps.DeletePersistentVolumeClaim(obj); err != nil {
				if matched, _ := regexp.MatchString(".+ not found", err.Error()); !matched {
					return &ErrFailedToDestroyStorage{
						App:   ctx.App,
						Cause: fmt.Sprintf("Failed to destroy PVC: %v. Err: %v", obj.Name, err),
					}
				}
			}
			logrus.Infof("[%v] Destroyed PVC: %v", ctx.App.Key, obj.Name)
		}
	}

	return nil
}

func (k *k8s) GetNodesForApp(ctx *scheduler.Context) ([]node.Node, error) {
	k8sOps := k8s_ops.Instance()
	var result []node.Node
	var pods []v1.Pod
	var err error
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*apps_api.Deployment); ok {
			pods, err = k8sOps.GetDeploymentPods(obj)
			if err != nil {
				return nil, &ErrFailedToGetNodesForApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to get pods due to: %v", err),
				}
			}
		} else if obj, ok := spec.(*apps_api.StatefulSet); ok {
			pods, err = k8sOps.GetStatefulSetPods(obj)
			if err != nil {
				return nil, &ErrFailedToGetNodesForApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get pods due to: %v", err),
				}
			}
		}
	}

	// We should have pods from a supported application at this point
	for _, p := range pods {
		if len(p.Spec.NodeName) > 0 {
			n, ok := k.nodes[p.Spec.NodeName]
			if !ok {
				return nil, &ErrFailedToGetNodesForApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("node: %v not present in k8s map", p.Spec.NodeName),
				}
			}

			if contains(result, n) {
				continue
			}

			result = append(result, n)
		}
	}

	return result, nil
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
				buf.WriteString(fmt.Sprintf("%v", &ErrFailedToGetAppStatus{
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
				buf.WriteString(fmt.Sprintf("%v", &ErrFailedToGetAppStatus{
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
				buf.WriteString(fmt.Sprintf("%v", &ErrFailedToGetAppStatus{
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
				buf.WriteString(fmt.Sprintf("%v", &ErrFailedToGetPvcStatus{
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
				buf.WriteString(fmt.Sprintf("%v", &ErrFailedToGetScParams{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get parameters of storage class: %v. Err: %v", obj.Name, err),
				}))
			}
			//Dump storage class parameters
			buf.WriteString(fmt.Sprintf("%v\n", scParams))
			buf.WriteString(insertLineBreak("END Storage Class"))
		} else {
			logrus.Warnf("Object type unknown/not supported: %v", obj)
		}
	}
	return buf.String(), nil
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
		buf.WriteString(insertLineBreak("END Container"))
	}
	buf.WriteString(insertLineBreak("END Pod"))
	return buf.String()
}

func contains(nodes []node.Node, n node.Node) bool {
	for _, value := range nodes {
		if value.Name == n.Name {
			return true
		}
	}
	return false
}

func init() {
	k := &k8s{
		nodes: make(map[string]node.Node),
	}
	scheduler.Register(SchedName, k)
}
