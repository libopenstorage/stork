package dcos

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	docker "github.com/docker/docker/client"
	marathon "github.com/gambol99/go-marathon"
	volsnapv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/portworx/torpedo/pkg/log"
	"golang.org/x/net/context"

	corev1 "k8s.io/api/core/v1"
	storageapi "k8s.io/api/storage/v1"
)

const (
	// SchedName is the name of the dcos scheduler driver implementation
	SchedName      = "dcos"
	defaultTimeout = 5 * time.Minute
)

type dcos struct {
	dockerClient  *docker.Client
	specFactory   *spec.Factory
	volDriverName string
}

func (d *dcos) Init(schedOpts scheduler.InitOptions) error {
	privateAgents, err := MesosClient().GetPrivateAgentNodes()
	if err != nil {
		return err
	}

	for _, n := range privateAgents {
		newNode := d.parseMesosNode(n)
		if err := d.IsNodeReady(newNode); err != nil {
			return err
		}
		if err := node.AddNode(newNode); err != nil {
			return err
		}
	}

	d.specFactory, err = spec.NewFactory(schedOpts.SpecDir, schedOpts.VolDriverName, d)
	if err != nil {
		return err
	}

	d.dockerClient, err = docker.NewEnvClient()
	if err != nil {
		return err
	}

	d.volDriverName = schedOpts.VolDriverName
	return nil
}

func (d *dcos) parseMesosNode(n AgentNode) node.Node {
	return node.Node{
		Name:      n.ID,
		Addresses: []string{n.Hostname},
		Type:      node.TypeWorker,
	}
}

func (d *dcos) String() string {
	return SchedName
}

// GetEvents dumps events from event storage
func (d *dcos) GetEvents() map[string][]scheduler.Event {
	return nil
}

// ValidateAutopilotEvents validates events for PVCs injected by autopilot
func (d *dcos) ValidateAutopilotEvents(ctx *scheduler.Context) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateAutopilotEvents()",
	}
}

// ValidateAutopilotRuleObjects validates autopilot rule objects
func (d *dcos) ValidateAutopilotRuleObjects() error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateAutopilotRuleObjects()",
	}
}

// GetSnapShotData retruns given snapshots
func (d *dcos) GetSnapShotData(ctx *scheduler.Context, snapshotName, snapshotNameSpace string) (*snapv1.VolumeSnapshotData, error) {
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateSnapShot()",
	}
}

// DeleteSnapshots  delete the snapshots
func (d *dcos) DeleteSnapShot(ctx *scheduler.Context, snapshotName, snapshotNameSpace string) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "DeleteSnapShot()",
	}
}

// GetSnapshotsInNameSpace get the snapshots list for the namespace
func (d *dcos) GetSnapshotsInNameSpace(ctx *scheduler.Context, snapshotNameSpace string) (*snapv1.VolumeSnapshotList, error) {

	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "DeleteSnapShot()",
	}
}

func (d *dcos) ParseSpecs(specDir, storageProvisioner string) ([]interface{}, error) {
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
		raw, err := ioutil.ReadFile(fileName)
		if err != nil {
			return nil, err
		}

		app := new(marathon.Application)
		if err := json.Unmarshal(raw, app); err != nil {
			return nil, err
		}

		specs = append(specs, app)
	}

	return specs, nil
}

func (d *dcos) IsNodeReady(n node.Node) error {
	// TODO: Implement this method
	return nil
}

func (d *dcos) GetNodesForApp(ctx *scheduler.Context) ([]node.Node, error) {
	var tasks []marathon.Task
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*marathon.Application); ok {
			appTasks, err := MarathonClient().GetApplicationTasks(obj.ID)
			if err != nil {
				return nil, &scheduler.ErrFailedToGetNodesForApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to get tasks for application %v. %v", obj.ID, err),
				}
			}
			tasks = append(tasks, appTasks...)
		} else {
			log.Warnf("Invalid spec received for app %v in GetNodesForApp", ctx.App.Key)
		}
	}

	var result []node.Node
	nodeMap := node.GetNodesByName()

	for _, task := range tasks {
		n, ok := nodeMap[task.SlaveID]
		if !ok {
			return nil, &scheduler.ErrFailedToGetNodesForApp{
				App:   ctx.App,
				Cause: fmt.Sprintf("node [%v] not present in node map", task.SlaveID),
			}
		}

		if node.Contains(result, n) {
			continue
		}
		result = append(result, n)
	}

	return result, nil
}

func (d *dcos) Schedule(instanceID string, options scheduler.ScheduleOptions) ([]*scheduler.Context, error) {
	var apps []*spec.AppSpec
	if len(options.AppKeys) > 0 {
		for _, key := range options.AppKeys {
			spec, err := d.specFactory.Get(key)
			if err != nil {
				return nil, err
			}
			apps = append(apps, spec)
		}
	} else {
		apps = d.specFactory.GetAll()
	}

	var contexts []*scheduler.Context
	for _, app := range apps {
		var specObjects []interface{}
		for _, spec := range app.SpecList {
			if application, ok := spec.(*marathon.Application); ok {
				if err := d.randomizeVolumeNames(application); err != nil {
					return nil, &scheduler.ErrFailedToScheduleApp{
						App:   app,
						Cause: err.Error(),
					}
				}
				obj, err := MarathonClient().CreateApplication(application)
				if err != nil {
					return nil, &scheduler.ErrFailedToScheduleApp{
						App:   app,
						Cause: err.Error(),
					}
				}

				specObjects = append(specObjects, obj)
			} else {
				return nil, fmt.Errorf("Unsupported object received in app %v while scheduling", app.Key)
			}
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

// ScheduleWithCustomAppSpecs Schedules the application with custom app specs
func (d *dcos) ScheduleWithCustomAppSpecs(apps []*spec.AppSpec, instanceID string, options scheduler.ScheduleOptions) ([]*scheduler.Context, error) {
	var contexts []*scheduler.Context
	for _, app := range apps {
		var specObjects []interface{}
		for _, spec := range app.SpecList {
			if application, ok := spec.(*marathon.Application); ok {
				if err := d.randomizeVolumeNames(application); err != nil {
					return nil, &scheduler.ErrFailedToScheduleApp{
						App:   app,
						Cause: err.Error(),
					}
				}
				obj, err := MarathonClient().CreateApplication(application)
				if err != nil {
					return nil, &scheduler.ErrFailedToScheduleApp{
						App:   app,
						Cause: err.Error(),
					}
				}

				specObjects = append(specObjects, obj)
			} else {
				return nil, fmt.Errorf("Unsupported object received in app %v while scheduling", app.Key)
			}
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

// AddTasks adds tasks to an existing context
func (d *dcos) AddTasks(ctx *scheduler.Context, options scheduler.ScheduleOptions) error {
	if ctx == nil {
		return fmt.Errorf("Context to add tasks to cannot be nil")
	}
	if len(options.AppKeys) == 0 {
		return fmt.Errorf("Need to specify list of applications to add to context")
	}

	var apps []*spec.AppSpec
	for _, key := range options.AppKeys {
		spec, err := d.specFactory.Get(key)
		if err != nil {
			return err
		}
		apps = append(apps, spec)
	}

	specObjects := ctx.App.SpecList
	for _, app := range apps {
		for _, spec := range app.SpecList {
			if application, ok := spec.(*marathon.Application); ok {
				if err := d.randomizeVolumeNames(application); err != nil {
					return &scheduler.ErrFailedToScheduleApp{
						App:   app,
						Cause: err.Error(),
					}
				}
				obj, err := MarathonClient().CreateApplication(application)
				if err != nil {
					return &scheduler.ErrFailedToScheduleApp{
						App:   app,
						Cause: err.Error(),
					}
				}
				specObjects = append(specObjects, obj)
			} else {
				return fmt.Errorf("Unsupported object received in app %v while scheduling", app.Key)
			}

		}
	}
	ctx.App.SpecList = specObjects
	return nil
}

// ScheduleUninstall uninstalls tasks from an existing context
func (d *dcos) ScheduleUninstall(ctx *scheduler.Context, options scheduler.ScheduleOptions) error {
	// TODO implement this method
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ScheduleUninstall()",
	}
}

// RemoveAppSpecsByName removes certain specs from list to avoid validation
func (d *dcos) RemoveAppSpecsByName(ctx *scheduler.Context, removeSpecs []interface{}) error {
	// TODO implement this method
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RemoveAppSpecsByName()",
	}
}

func (d *dcos) UpdateTasksID(ctx *scheduler.Context, id string) error {
	// TODO: Add implementation
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpdateTasksID()",
	}
}

func (d *dcos) randomizeVolumeNames(application *marathon.Application) error {
	volDriver, err := volume.Get(d.volDriverName)
	if err != nil {
		return err
	}

	params := *application.Container.Docker.Parameters
	for i := range params {
		p := &params[i]
		if p.Key == "volume" {
			p.Value = volDriver.RandomizeVolumeName(p.Value)
		}
	}
	return nil
}

func (d *dcos) WaitForRunning(ctx *scheduler.Context, timeout, retryInterval time.Duration) error {
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*marathon.Application); ok {
			if err := MarathonClient().WaitForApplicationStart(obj.ID); err != nil {
				return &scheduler.ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Application: %v. Err: %v", obj.ID, err),
				}
			}
			log.Infof("[%v] Validated application: %v", ctx.App.Key, obj.ID)
		} else {
			log.Warnf("Invalid spec received for app %v in WaitForRunning", ctx.App.Key)
		}
	}
	return nil
}

func (d *dcos) Destroy(ctx *scheduler.Context, opts map[string]bool) error {
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*marathon.Application); ok {
			if err := MarathonClient().DeleteApplication(obj.ID); err != nil {
				return &scheduler.ErrFailedToDestroyApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to destroy Application: %v. Err: %v", obj.ID, err),
				}
			}
			log.Infof("[%v] Destroyed application: %v", ctx.App.Key, obj.ID)
		} else {
			log.Warnf("Invalid spec received for app %v in Destroy", ctx.App.Key)
		}
	}

	if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
		// TODO: wait until all the resources have been cleaned up properly
		if err := d.WaitForDestroy(ctx, defaultTimeout); err != nil {
			return err
		}
	} else if value, ok := opts[scheduler.OptionsWaitForDestroy]; ok && value {
		if err := d.WaitForDestroy(ctx, defaultTimeout); err != nil {
			return err
		}
	}

	return nil
}

func (d *dcos) WaitForDestroy(ctx *scheduler.Context, timeout time.Duration) error {
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*marathon.Application); ok {
			if err := MarathonClient().WaitForApplicationTermination(obj.ID); err != nil {
				return &scheduler.ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to destroy Application: %v. Err: %v", obj.ID, err),
				}
			}
			log.Infof("[%v] Validated destroy of Application: %v", ctx.App.Key, obj.ID)
		} else {
			log.Warnf("Invalid spec received for app %v in WaitForDestroy", ctx.App.Key)
		}
	}

	return nil
}

// SelectiveWaitForTermination waits for application pods to be terminated except on the nodes
// provided in the exclude list
func (d *dcos) SelectiveWaitForTermination(ctx *scheduler.Context, timeout time.Duration, excludeList []node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "SelectiveWaitForTermination",
	}
}

func (d *dcos) DeleteTasks(ctx *scheduler.Context, opts *scheduler.DeleteTasksOptions) error {
	if opts != nil {
		log.Warnf("DCOS driver doesn't yet support delete task options")
	}

	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*marathon.Application); ok {
			if err := MarathonClient().KillApplicationTasks(obj.ID); err != nil {
				return &scheduler.ErrFailedToDeleteTasks{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to delete tasks for application: %v. %v", obj.ID, err),
				}
			}
		} else {
			log.Warnf("Invalid spec received for app %v in DeleteTasks", ctx.App.Key)
		}
	}
	return nil
}

func (d *dcos) GetVolumeParameters(ctx *scheduler.Context) (map[string]map[string]string, error) {
	result := make(map[string]map[string]string)
	populateParamsFunc := func(volName string, volParams map[string]string) error {
		result[volName] = volParams
		return nil
	}

	if err := d.volumeOperation(ctx, populateParamsFunc); err != nil {
		return nil, err
	}
	return result, nil
}

func (d *dcos) ValidateVolumes(ctx *scheduler.Context, timeout, retryInterval time.Duration,
	options *scheduler.VolumeOptions) error {
	inspectDockerVolumeFunc := func(volName string, _ map[string]string) error {
		t := func() (interface{}, bool, error) {
			out, err := d.dockerClient.VolumeInspect(context.Background(), volName)
			return out, true, err
		}

		if _, err := task.DoRetryWithTimeout(t, 2*time.Minute, 10*time.Second); err != nil {
			return &scheduler.ErrFailedToValidateStorage{
				App:   ctx.App,
				Cause: fmt.Sprintf("Failed to inspect docker volume: %v. Err: %v", volName, err),
			}
		}
		return nil
	}

	return d.volumeOperation(ctx, inspectDockerVolumeFunc)
}

func (d *dcos) DeleteVolumes(ctx *scheduler.Context, options *scheduler.VolumeOptions) ([]*volume.Volume, error) {
	var vols []*volume.Volume

	deleteDockerVolumeFunc := func(volName string, _ map[string]string) error {
		vols = append(vols, &volume.Volume{Name: volName})
		t := func() (interface{}, bool, error) {
			return nil, true, d.dockerClient.VolumeRemove(context.Background(), volName, false)
		}

		if _, err := task.DoRetryWithTimeout(t, 2*time.Minute, 10*time.Second); err != nil {
			return &scheduler.ErrFailedToDestroyStorage{
				App:   ctx.App,
				Cause: fmt.Sprintf("Failed to remove docker volume: %v. Err: %v", volName, err),
			}
		}
		return nil
	}

	if err := d.volumeOperation(ctx, deleteDockerVolumeFunc); err != nil {
		return nil, err
	}
	return vols, nil
}

func (d *dcos) GetVolumeDriverVolumeName(name string, namespace string) (string, error) {
	// TODO: Add implementation
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetVolumeDriverVolumeName()",
	}
}

func (d *dcos) GetVolumes(ctx *scheduler.Context) ([]*volume.Volume, error) {
	// TODO: Add implementation
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetVolumes()",
	}
}

func (d *dcos) GetPureVolumes(ctx *scheduler.Context, pureVolType string) ([]*volume.Volume, error) {
	// TODO: Add implementation
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetPureVolumes()",
	}
}

func (d *dcos) GetPodsForPVC(pvcname, namespace string) ([]corev1.Pod, error) {
	// TODO: Add implementation
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetPodsForPVC()",
	}
}

// GetPodLog returns logs for all the pods in the specified context
func (d *dcos) GetPodLog(ctx *scheduler.Context, sinceSeconds int64, containerName string) (map[string]string, error) {
	// TODO: Add implementation
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetPodLog()",
	}
}

func (d *dcos) ResizeVolume(cxt *scheduler.Context, configMap string) ([]*volume.Volume, error) {
	// TODO implement this method
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ResizeVolume()",
	}
}

func (d *dcos) ResizePVC(cxt *scheduler.Context, pvc *corev1.PersistentVolumeClaim, sizeInGb uint64) (*volume.Volume, error) {
	// TODO implement this method
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ResizePVC()",
	}
}

func (d *dcos) GetSnapshots(ctx *scheduler.Context) ([]*volume.Snapshot, error) {
	// TODO: Add implementation
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetSnapshots()",
	}
}

func (d *dcos) volumeOperation(ctx *scheduler.Context, f func(string, map[string]string) error) error {
	// DC/OS does not have volume objects like Kubernetes. We get the volume information from
	// the app spec and get the options parsed from the respective volume driver
	volDriver, err := volume.Get(d.volDriverName)
	if err != nil {
		return err
	}

	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*marathon.Application); ok {
			// TODO: This handles only docker volumes. Implement for UCR/mesos containers
			params := *obj.Container.Docker.Parameters
			for _, p := range params {
				if p.Key == "volume" {
					volName, volParams, err := volDriver.ExtractVolumeInfo(p.Value)
					if err != nil {
						return &scheduler.ErrFailedToGetVolumeParameters{
							App:   ctx.App,
							Cause: fmt.Sprintf("Failed to extract volume info: %v. Err: %v", p.Value, err),
						}
					}
					if err := f(volName, volParams); err != nil {
						return err
					}
				}
			}
		} else {
			log.Warnf("Invalid spec received for app %v", ctx.App.Key)
		}
	}

	return nil
}

func (d *dcos) SetConfig(configPath string) error {
	// TODO: Implement this method
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "SetConfig()",
	}
}

func (d *dcos) Describe(ctx *scheduler.Context) (string, error) {
	// TODO: Implement this method
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "Describe()",
	}
}

func (d *dcos) ScaleApplication(ctx *scheduler.Context, scaleFactorMap map[string]int32) error {
	// TODO implement this method
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ScaleApplication()",
	}
}

func (d *dcos) GetScaleFactorMap(ctx *scheduler.Context) (map[string]int32, error) {
	// TODO implement this method
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetScaleFactorMap()",
	}
}

func (d *dcos) StopSchedOnNode(node node.Node) error {
	// TODO implement this method
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "StopSchedOnNode()",
	}
}

func (d *dcos) StartSchedOnNode(node node.Node) error {
	// TODO implement this method
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "StartSchedOnNode()",
	}
}
func (d *dcos) RescanSpecs(specDir, storageDriver string) error {
	// TODO implement this method
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RescanSpecs()",
	}
}

func (d *dcos) PrepareNodeToDecommission(n node.Node, provisioner string) error {
	// TODO implement this method
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "PrepareNodeToDecommission()",
	}
}

func (d *dcos) EnableSchedulingOnNode(n node.Node) error {
	// TODO implement this method
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "EnableSchedulingOnNode()",
	}
}

func (d *dcos) DisableSchedulingOnNode(n node.Node) error {
	// TODO implement this method
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "DisableSchedulingOnNode()",
	}
}

func (d *dcos) RefreshNodeRegistry() error {
	// TODO implement this method
	return nil
}

func (d *dcos) IsScalable(spec interface{}) bool {
	// TODO implement this method
	return false
}

func (d *dcos) ValidateVolumeSnapshotRestore(ctx *scheduler.Context, timeStart time.Time) error {
	return fmt.Errorf("not implemenented")
}

func (d *dcos) GetTokenFromConfigMap(string) (string, error) {
	// TODO implement this method
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetTokenFromConfigMap()",
	}
}

func (d *dcos) AddLabelOnNode(n node.Node, lKey string, lValue string) error {
	// TODO implement this method
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "AddLabelOnNode()",
	}
}

func (d *dcos) RemoveLabelOnNode(n node.Node, lKey string) error {
	// TODO implement this method
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RemoveLabelOnNode()",
	}
}

func (d *dcos) IsAutopilotEnabledForVolume(*volume.Volume) bool {
	// TODO implement this method
	return false
}

func (d *dcos) GetSpecAppEnvVar(ctx *scheduler.Context, key string) string {
	// TODO implement this method
	return ""
}

func (d *dcos) SaveSchedulerLogsToFile(n node.Node, location string) error {
	// TODO implement this method
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "SaveSchedulerLogsToFile()",
	}
}

// GetWorkloadSizeFromAppSpec gets workload size from an application spec
func (d *dcos) GetWorkloadSizeFromAppSpec(ctx *scheduler.Context) (uint64, error) {
	// TODO: not implemented
	return 0, nil
}

func (d *dcos) GetAutopilotNamespace() (string, error) {
	// TODO implement this method
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetAutopilotNamespace()",
	}
}

// GetIOBandwidth returns the IO bandwidth for the given pod name and namespace
func (d *dcos) GetIOBandwidth(string, string) (int, error) {
	// TODO implement this method
	return 0, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetIOBandwidth()",
	}
}

func (d *dcos) CreateAutopilotRule(apRule apapi.AutopilotRule) (*apapi.AutopilotRule, error) {
	// TODO implement this method
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CreateAutopilotRule()",
	}
}

func (d *dcos) GetAutopilotRule(name string) (*apapi.AutopilotRule, error) {
	// TODO implement this method
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetAutopilotRule()",
	}
}

func (d *dcos) UpdateAutopilotRule(*apapi.AutopilotRule) (*apapi.AutopilotRule, error) {
	// TODO implement this method
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpdateAutopilotRule()",
	}
}

func (d *dcos) ListAutopilotRules() (*apapi.AutopilotRuleList, error) {
	// TODO implement this method
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ListAutopilotRules()",
	}
}

func (d *dcos) DeleteAutopilotRule(name string) error {
	// TODO implement this method
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "DeleteAutopilotRule()",
	}
}

func (d *dcos) GetActionApproval(namespace, name string) (*apapi.ActionApproval, error) {
	// TODO implement this method
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetActionApproval()",
	}
}

func (d *dcos) UpdateActionApproval(namespace string, actionApproval *apapi.ActionApproval) (*apapi.ActionApproval, error) {
	// TODO implement this method
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpdateActionApproval()",
	}
}

func (d *dcos) DeleteActionApproval(namespace, name string) error {
	// TODO implement this method
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "DeleteActionApproval()",
	}
}

func (d *dcos) ListActionApprovals(namespace string) (*apapi.ActionApprovalList, error) {
	// TODO implement this method
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ListActionApprovals()",
	}
}

func (d *dcos) UpgradeScheduler(version string) error {
	// TODO: Add implementation
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "UpgradeScheduler()",
	}
}

func (d *dcos) CreateSecret(namespace, name, dataField, secretDataString string) error {
	// TODO: Add implementation
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CreateSecret()",
	}
}

func (d *dcos) GetSecretData(namespace, name, dataField string) (string, error) {
	// TODO: Add implementation
	return "", &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetSecret()",
	}
}

func (d *dcos) DeleteSecret(namespace, name string) error {
	// TODO: Add implementation
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "DeleteSecret()",
	}
}

func (d *dcos) ParseCharts(chartDir string) (*scheduler.HelmRepo, error) {
	// TODO implement this method
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ParseCharts()",
	}
}

func (d *dcos) RecycleNode(n node.Node) error {
	//Recycle is not supported
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RecycleNode()",
	}
}

func (d *dcos) ValidateTopologyLabel(ctx *scheduler.Context) error {
	//ValidateTopologyLabel is not supported
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateTopologyLabel()",
	}
}

func (d *dcos) CreateCsiSnapshotClass(snapClassName string, deleionPolicy string) (*volsnapv1.VolumeSnapshotClass, error) {
	//CreateCsiSnapshotClass is not supported
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CreateCsiSnapshotClass()",
	}
}

func (d *dcos) CreateCsiSnapshot(name string, namespace string, class string, pvc string) (*volsnapv1.VolumeSnapshot, error) {
	//CreateCsiSanpshot is not supported
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CreateCsiSanpshot()",
	}
}

func (d *dcos) CreateCsiSnapsForVolumes(ctx *scheduler.Context, snapClass string) (map[string]*volsnapv1.VolumeSnapshot, error) {
	//CreateCsiSnapsForVolumes is not supported
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CreateCsiSnapsForVolumes()",
	}
}

func (d *dcos) CSICloneTest(ctx *scheduler.Context, request scheduler.CSICloneRequest) error {
	//CSICloneTest is not supported for DCOS
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CSICloneTest()",
	}
}

func (d *dcos) CSISnapshotTest(ctx *scheduler.Context, request scheduler.CSISnapshotRequest) error {
	//CSISnapshotTest is not supported for DCOS
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CSISnapshotTest()",
	}
}

func (d *dcos) CSISnapshotAndRestoreMany(ctx *scheduler.Context, request scheduler.CSISnapshotRequest) error {
	//CSISnapshotAndRestoreMany is not supported for DCOS
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "CSISnapshotAndRestoreMany()",
	}
}

func (d *dcos) GetCsiSnapshots(namespace string, pvcName string) ([]*volsnapv1.VolumeSnapshot, error) {
	// GetCsiSnapshots is not supported
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetCsiSnapshots()",
	}
}

func (d *dcos) ValidateCsiSnapshots(ctx *scheduler.Context, volSnapMa map[string]*volsnapv1.VolumeSnapshot) error {
	// ValidateCsiSnapshots is not supported
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateCsiSnapshots()",
	}
}

func (d *dcos) RestoreCsiSnapAndValidate(ctx *scheduler.Context, scList map[string]*storageapi.StorageClass) (map[string]corev1.PersistentVolumeClaim, error) {
	// RestoreCsiSnapAndValidate is not supported
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RestoreCsiSnapAndValidate()",
	}

}

func (d *dcos) DeleteCsiSnapsForVolumes(ctx *scheduler.Context, retainCount int) error {
	// DeleteCsiSnapsForVolumes is not supported
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "DeleteCsiSnapsForVolumes()",
	}

}

func (d *dcos) DeleteCsiSnapshot(ctx *scheduler.Context, snapshotName string, snapshotNameSpace string) error {
	// DeleteCsiSnapshot is not supported
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "DeleteCsiSnapshot()",
	}

}

// GetAllSnapshotClasses returns the list of all volume snapshot classes present in the cluster
func (d *dcos) GetAllSnapshotClasses() (*volsnapv1.VolumeSnapshotClassList, error) {
	// GetAllSnapshotClasses is not supported
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetAllSnapshotClasses()",
	}

}

func (d *dcos) GetPodsRestartCount(namespace string, label map[string]string) (map[*corev1.Pod]int32, error) {
	// GetPodsRestartCount is not supported
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetPodsRestartCoun()",
	}
}

func (d *dcos) AddNamespaceLabel(namespace string, labelMap map[string]string) error {
	// AddNamespaceLabel is not supported
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "AddNamespaceLabel()",
	}
}

func (d *dcos) RemoveNamespaceLabel(namespace string, labelMap map[string]string) error {
	// RemoveNamespaceLabel is not supported
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "RemoveNamespaceLabel()",
	}
}

func (d *dcos) GetNamespaceLabel(namespace string) (map[string]string, error) {
	// GetNamespaceLabel is not supported
	return nil, &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "GetNamespaceLabel()",
	}
}

func init() {
	d := &dcos{}
	scheduler.Register(SchedName, d)
}
