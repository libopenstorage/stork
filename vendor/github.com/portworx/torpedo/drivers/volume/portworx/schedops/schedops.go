package schedops

import (
	"fmt"

	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/errors"
)

// Driver is the interface for portworx operations under various schedulers
type Driver interface {
	// ValidateOnNode validates portworx on given node (from scheduler perspective)
	ValidateOnNode(n node.Node) error
	// ValidateAddLabels validates whether the labels for the volume are applied appropriately on the vol replica nodes
	ValidateAddLabels(replicaNodes []api.Node, v *api.Volume) error
	// ValidateRemoveLabels validates whether the volume labels have been removed
	ValidateRemoveLabels(v *volume.Volume) error
	// ValidateVolumeCleanup validates that volume dir does not exist and no data present inside it
	ValidateVolumeCleanup(d node.Driver) error
	// GetVolumeName returns the volume name based on the volume object recevied
	GetVolumeName(v *volume.Volume) string
	// GetServiceEndpoint returns the hostname of portworx service if it is present
	GetServiceEndpoint() (string, error)
	// UpgradePortworx upgrades portworx to the given docker image and tag
	UpgradePortworx(image, tag string) error
}

var (
	schedOpsRegistry = make(map[string]Driver)
)

// Register registers the given portworx scheduler operator
func Register(name string, d Driver) error {
	if _, ok := schedOpsRegistry[name]; !ok {
		schedOpsRegistry[name] = d
	} else {
		return fmt.Errorf("portworx scheduler ops driver: %s is already registered", name)
	}

	return nil
}

// Get a driver to perform portworx operations for the given scheduler
func Get(name string) (Driver, error) {
	d, ok := schedOpsRegistry[name]
	if ok {
		return d, nil
	}

	return nil, &errors.ErrNotFound{
		ID:   name,
		Type: "Portworx Scheduler Operator",
	}
}
