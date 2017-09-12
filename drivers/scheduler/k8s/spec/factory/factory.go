package factory

import (
	"github.com/Sirupsen/logrus"
	"github.com/portworx/torpedo/drivers/scheduler/k8s/spec"
	"github.com/portworx/torpedo/pkg/errors"
)

var appSpecFactory = make(map[string]spec.AppSpec)

// Register registers a new spec with the factory
func Register(id string, app spec.AppSpec) {
	logrus.Infof("Registering app: %v", id)
	appSpecFactory[id] = app
}

// Get returns a registered application
func Get(id string) (spec.AppSpec, error) {
	if d, ok := appSpecFactory[id]; ok && d.IsEnabled() {
		return d, nil
	}

	return nil, &errors.ErrNotFound{
		ID:   id,
		Type: "AppSpec",
	}
}

// GetAll returns all registered enabled applications
func GetAll() []spec.AppSpec {
	var specs []spec.AppSpec
	for _, val := range appSpecFactory {
		if val.IsEnabled() {
			specs = append(specs, val)
		}
	}

	return specs
}
