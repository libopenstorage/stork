package dcos

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	marathon "github.com/gambol99/go-marathon"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/sirupsen/logrus"
)

const (
	// SchedName is the name of the dcos scheduler driver implementation
	SchedName = "dcos"
)

type dcos struct {
	specFactory *spec.Factory
}

func (d *dcos) Init(specDir string, nodeDriver string) error {
	nodes, _ := getNodes()

	for _, n := range nodes {
		if err := d.IsNodeReady(n); err != nil {
			return err
		}
		if err := node.AddNode(n); err != nil {
			return err
		}
	}

	var err error
	d.specFactory, err = spec.NewFactory(specDir, d)
	if err != nil {
		return err
	}

	return nil
}

// TODO: Remove hardcoded nodes and query mesos to get the nodes from the cluster
func getNodes() ([]node.Node, error) {
	var nodes []node.Node
	nodes = append(nodes, node.Node{
		Name:      "a1.dcos",
		Addresses: []string{"192.168.65.111"},
		Type:      node.TypeWorker,
	})
	nodes = append(nodes, node.Node{
		Name:      "a2.dcos",
		Addresses: []string{"192.168.65.121"},
		Type:      node.TypeWorker,
	})
	nodes = append(nodes, node.Node{
		Name:      "a3.dcos",
		Addresses: []string{"192.168.65.131"},
		Type:      node.TypeWorker,
	})
	return nodes, nil
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

func (d *dcos) GetNodesForApp(*scheduler.Context) ([]node.Node, error) {
	// TODO: Implement this method
	return nil, nil
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
				obj, err := MarathonClient().CreateApplication(application)
				if err != nil {
					return nil, &ErrFailedToScheduleApp{
						App:   app,
						Cause: err.Error(),
					}
				}

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
		}

		contexts = append(contexts, ctx)
	}

	return contexts, nil
}

func (d *dcos) WaitForRunning(ctx *scheduler.Context) error {
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*marathon.Application); ok {
			if err := MarathonClient().WaitForApplicationStart(obj.ID); err != nil {
				return &ErrFailedToValidateApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to validate Application: %v. Err: %v", obj.ID, err),
				}
			}
			logrus.Infof("[%v] Validated application: %v", ctx.App.Key, obj.ID)
		}
	}
	return nil
}

func (d *dcos) Destroy(ctx *scheduler.Context, opts map[string]bool) error {
	for _, spec := range ctx.App.SpecList {
		if obj, ok := spec.(*marathon.Application); ok {
			if err := MarathonClient().DeleteApplication(obj.ID); err != nil {
				return &ErrFailedToDestroyApp{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to destroy Application: %v. Err: %v", obj.ID, err),
				}
			}
			logrus.Infof("[%v] Destoyed application: %v", ctx.App.Key, obj.ID)
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
				return &ErrFailedToValidateAppDestroy{
					App:   ctx.App,
					Cause: fmt.Sprintf("Failed to destroy Application: %v. Err: %v", obj.ID, err),
				}
			}
			logrus.Infof("[%v] Validated destroy of Application: %v", ctx.App.Key, obj.ID)
		}
	}

	return nil
}

func (d *dcos) DeleteTasks(ctx *scheduler.Context) error {
	// TODO: Implement this method
	return nil
}

func (d *dcos) GetVolumes(ctx *scheduler.Context) ([]string, error) {
	// TODO: Implement this method
	return nil, nil
}

func (d *dcos) GetVolumeParameters(ctx *scheduler.Context) (map[string]map[string]string, error) {
	// TODO: Implement this method
	return nil, nil
}

func (d *dcos) InspectVolumes(ctx *scheduler.Context) error {
	// TODO: Implement this method
	return nil
}

func (d *dcos) DeleteVolumes(ctx *scheduler.Context) ([]*volume.Volume, error) {
	// TODO: Implement this method
	return nil, nil
}

func (d *dcos) Describe(ctx *scheduler.Context) (string, error) {
	// TODO: Implement this method
	return "", nil
}

func init() {
	d := &dcos{}
	scheduler.Register(SchedName, d)
}
