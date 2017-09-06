package factory

import (
	"github.com/portworx/torpedo/drivers/scheduler/k8s/spec"
	"github.com/portworx/torpedo/pkg/errors"
	"log"
)

var appSpecFactory = make(map[string]spec.AppSpec)

// Register registers a new spec with the factory
func Register(id string, app spec.AppSpec) {
	log.Printf("Registering app: %v\n", id)
	appSpecFactory[id] = app
}

// Get returns a registered application
func Get(id string) (spec.AppSpec, error) {
	if d, ok := appSpecFactory[id]; ok {
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