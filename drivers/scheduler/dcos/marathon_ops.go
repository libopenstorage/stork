package dcos

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	marathon "github.com/gambol99/go-marathon"
	"github.com/portworx/sched-ops/task"
)

const (
	defaultMarathonHostname = "marathon.mesos"
	defaultMarathonPort     = "8080"
)

// MarathonOps is an interface to perform Marathon related operations
type MarathonOps interface {
	ApplicationOps
}

// ApplicationOps is an interface to perform application operations
type ApplicationOps interface {
	// CreateApplication creates an application in Marathon
	CreateApplication(*marathon.Application) (*marathon.Application, error)
	// ValidateAppliation checks if the aplication is running and healthy
	WaitForApplicationStart(string) error
	// GetApplicationTasks gets all the tasks/instances for the application
	GetApplicationTasks(string) ([]marathon.Task, error)
	// KillApplicationTasks kills all tasks of an application
	KillApplicationTasks(string) error
	// DeleteApplication deletes the given application
	DeleteApplication(string) error
	// WaitForApplicationTermination validates if the application is terminated
	WaitForApplicationTermination(string) error
}

var (
	marathonInstance MarathonOps
	marathonOnce     sync.Once
)

type marathonOps struct {
	client marathon.Marathon
}

// MarathonClient returns a singleton instance of MarathonOps type
func MarathonClient() MarathonOps {
	marathonOnce.Do(func() {
		marathonInstance = &marathonOps{}
	})
	return marathonInstance
}

// Initialize Marathon client if not initialized
func (m *marathonOps) initMarathonClient() error {
	if m.client == nil {
		hostname := os.Getenv("MARATHON_HOSTNAME")
		if len(hostname) == 0 {
			hostname = defaultMarathonHostname
		}

		port := os.Getenv("MARATHON_PORT")
		if len(port) == 0 {
			port = defaultMarathonPort
		}

		config := marathon.NewDefaultConfig()
		marathonURL := &url.URL{Scheme: "http", Host: hostname + ":" + port}
		config.URL = marathonURL.String()

		client, err := marathon.NewClient(config)
		if err != nil {
			return err
		}

		m.client = client
	}
	return nil
}

func (m *marathonOps) CreateApplication(app *marathon.Application) (*marathon.Application, error) {
	if err := m.initMarathonClient(); err != nil {
		return nil, err
	}

	return m.client.CreateApplication(app)
}

func (m *marathonOps) WaitForApplicationStart(name string) error {
	if err := m.initMarathonClient(); err != nil {
		return err
	}

	return m.client.WaitOnApplication(name, 5*time.Minute)
}

func (m *marathonOps) GetApplicationTasks(name string) ([]marathon.Task, error) {
	if err := m.initMarathonClient(); err != nil {
		return nil, err
	}

	tasks, err := m.client.Tasks(name)
	if err != nil {
		return nil, err
	}
	return tasks.Tasks, nil
}

func (m *marathonOps) KillApplicationTasks(name string) error {
	tasks, err := m.GetApplicationTasks(name)
	if err != nil {
		return err
	}

	var taskIds []string
	for _, task := range tasks {
		taskIds = append(taskIds, task.ID)
	}

	return m.client.KillTasks(taskIds, &marathon.KillTaskOpts{})
}

func (m *marathonOps) DeleteApplication(name string) error {
	if err := m.initMarathonClient(); err != nil {
		return err
	}

	if _, err := m.client.DeleteApplication(name, false); err != nil {
		return err
	}
	return nil
}

func (m *marathonOps) WaitForApplicationTermination(name string) error {
	if err := m.initMarathonClient(); err != nil {
		return err
	}

	t := func() (interface{}, bool, error) {
		_, err := m.client.Application(name)
		if err == nil {
			return nil, true, fmt.Errorf("Application %v is not yet deleted", name)
		}

		if !strings.Contains(err.Error(), "does not exist") {
			return nil, true, err
		}

		// Delete stuck deployments that are no more relevant, as the app is deleted
		deps, err := m.client.Deployments()
		if err != nil {
			return nil, true, err
		}
		for _, dep := range deps {
			for _, affectedApp := range dep.AffectedApps {
				if name == affectedApp {
					// Wait for the deployment to exit properly if it is not stuck
					// If we delete a successful deployment without waiting, it may
					// rollback and bring the app back.
					m.client.WaitOnDeployment(dep.ID, 2*time.Minute)
					// Delete the deployment assuming it is stuck
					m.client.DeleteDeployment(dep.ID, true)
					break
				}
			}
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second); err != nil {
		return err
	}
	return nil
}
