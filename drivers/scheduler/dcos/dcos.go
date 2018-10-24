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
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

const (
	// SchedName is the name of the dcos scheduler driver implementation
	SchedName = "dcos"
)

type dcos struct {
	dockerClient  *docker.Client
	specFactory   *spec.Factory
	volDriverName string
}

func (d *dcos) Init(specDir, volDriver, nodeDriver string) error {
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

	d.specFactory, err = spec.NewFactory(specDir, d)
	if err != nil {
		return err
	}

	d.dockerClient, err = docker.NewEnvClient()
	if err != nil {
		return err
	}

	d.volDriverName = volDriver
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

func (d *dcos) ParseSpecs(specDir string) ([]interface{}, error) {
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
			logrus.Warnf("Invalid spec received for app %v in GetNodesForApp", ctx.App.Key)
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
			logrus.Infof("[%v] Validated application: %v", ctx.App.Key, obj.ID)
		} else {
			logrus.Warnf("Invalid spec received for app %v in WaitForRunning", ctx.App.Key)
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
			logrus.Infof("[%v] Destroyed application: %v", ctx.App.Key, obj.ID)
		} else {
			logrus.Warnf("Invalid spec received for app %v in Destroy", ctx.App.Key)
		}
	}

	if value, ok := opts[scheduler.OptionsWaitForResourceLeakCleanup]; ok && value {
		// TODO: wait until all the resources have been cleaned up properly
		if err := d.WaitForDestroy(ctx); err != nil {
			return err
		}
	} else if value, ok := opts[scheduler.OptionsWaitForDestroy]; ok && value {
		if err := d.WaitForDestroy(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (d *dcos) WaitForDestroy(ctx *scheduler.Context) error {
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*marathon.Application); ok {
			if err := MarathonClient().WaitForApplicationTermination(obj.ID); err != nil {
				return &scheduler.ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to destroy Application: %v. Err: %v", obj.ID, err),
				}
			}
			logrus.Infof("[%v] Validated destroy of Application: %v", ctx.App.Key, obj.ID)
		} else {
			logrus.Warnf("Invalid spec received for app %v in WaitForDestroy", ctx.App.Key)
		}
	}

	return nil
}

func (d *dcos) DeleteTasks(ctx *scheduler.Context) error {
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*marathon.Application); ok {
			if err := MarathonClient().KillApplicationTasks(obj.ID); err != nil {
				return &scheduler.ErrFailedToDeleteTasks{
					App:   ctx.App,
					Cause: fmt.Sprintf("failed to delete tasks for application: %v. %v", obj.ID, err),
				}
			}
		} else {
			logrus.Warnf("Invalid spec received for app %v in DeleteTasks", ctx.App.Key)
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

func (d *dcos) InspectVolumes(ctx *scheduler.Context, timeout, retryInterval time.Duration) error {
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

func (d *dcos) DeleteVolumes(ctx *scheduler.Context) ([]*volume.Volume, error) {
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

func (d *dcos) GetVolumes(ctx *scheduler.Context) ([]*volume.Volume, error) {
	// TODO: Add implementation
	return nil, nil
}

func (d *dcos) ResizeVolume(cxt *scheduler.Context) ([]*volume.Volume, error) {
	//TODO implement this method
	return nil, nil
}

func (d *dcos) GetSnapshots(ctx *scheduler.Context) ([]*volume.Snapshot, error) {
	// TODO: Add implementation
	return nil, nil
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
			logrus.Warnf("Invalid spec received for app %v", ctx.App.Key)
		}
	}

	return nil
}

func (d *dcos) Describe(ctx *scheduler.Context) (string, error) {
	// TODO: Implement this method
	return "", nil
}

func (d *dcos) ScaleApplication(ctx *scheduler.Context, scaleFactorMap map[string]int32) error {
	//TODO implement this method
	return nil
}

func (d *dcos) GetScaleFactorMap(ctx *scheduler.Context) (map[string]int32, error) {
	//TODO implement this method
	return nil, nil
}

func (d *dcos) StopSchedOnNode(node node.Node) error {
	//TODO implement this method
	return nil
}

func (d *dcos) StartSchedOnNode(node node.Node) error {
	//TODO implement this method
	return nil
}

func (d *dcos) CreateCRDObjects(pathCRDSpec string) error {
	// TODO: Implement this method
	return nil
}

func init() {
	d := &dcos{}
	scheduler.Register(SchedName, d)
}
