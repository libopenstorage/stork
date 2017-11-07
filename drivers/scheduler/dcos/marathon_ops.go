package dcos

import (
	"regexp"
	"sync"
	"time"

	marathon "github.com/gambol99/go-marathon"
	"github.com/portworx/sched-ops/task"
)

// Ops is an interface to perform Marathon related operations
type Ops interface {
	ApplicationOps
}

// ApplicationOps is an interface to perform application operations
type ApplicationOps interface {
	// CreateApplication creates an application in Marathon
	CreateApplication(*marathon.Application) (*marathon.Application, error)
	// ValidateAppliation checks if the aplication is running and healthy
	WaitForApplicationStart(string) error
	// DeleteApplication deletes the given application
	DeleteApplication(string) error
	// WaitForApplicationTermination validates if the application is terminated
	WaitForApplicationTermination(string) error
}

var (
	instance Ops
	once     sync.Once
)

type marathonOps struct {
	client marathon.Marathon
}

// MarathonClient returns a singleton instance of Marathon Ops type
func MarathonClient() Ops {
	once.Do(func() {
		instance = &marathonOps{}
	})
	return instance
}

// Initialize Marathon client if not initialized
func (m *marathonOps) initMarathonClient() error {
	if m.client == nil {
		// TODO: Use environment variable to get the master node ip/marathon ip
		// Instead of talking to marathon directly use http://dcos/marathon/..
		marathonURL := "http://192.168.65.90:8080"
		config := marathon.NewDefaultConfig()
		config.URL = marathonURL

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
	t := func() (interface{}, error) {
		if err := m.initMarathonClient(); err != nil {
			return nil, err
		}

		if _, err := m.client.Application(name); err != nil {
			if matched, _ := regexp.MatchString(".+ does not exist", err.Error()); !matched {
				return nil, err
			}
		}

		return nil, nil
	}

	if _, err := task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second); err != nil {
		return err
	}
	return nil
}
