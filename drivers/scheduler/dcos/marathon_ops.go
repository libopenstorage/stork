package dcos

import (
	"net/url"
	"os"
	"regexp"
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
