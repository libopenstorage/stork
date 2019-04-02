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
	// StartPxOnNode enables portworx service on given node
	StartPxOnNode(n node.Node) error
	// StopPxOnNode disables portworx service  on given node
	StopPxOnNode(n node.Node) error
	// ValidateOnNode validates portworx on given node (from scheduler perspective)
	ValidateOnNode(n node.Node) error
	// ValidateAddLabels validates whether the labels for the volume are applied appropriately on the vol replica nodes
	ValidateAddLabels(replicaNodes []api.Node, v *api.Volume) error
	// ValidateRemoveLabels validates whether the volume labels have been removed
	ValidateRemoveLabels(v *volume.Volume) error
	// ValidateVolumeCleanup validates that volume dir does not exist and no data present inside it
	ValidateVolumeCleanup(d node.Driver) error
	// ValidateVolumeSetup checks if the given volume is setup correctly
	ValidateVolumeSetup(v *volume.Volume, d node.Driver) error
	// ValidateSnapshot validates the snapshot volume
	ValidateSnapshot(volumeParams map[string]string, parent *api.Volume) error
	// GetVolumeName returns the volume name based on the volume object recevied
	GetVolumeName(v *volume.Volume) string
	// GetServiceEndpoint returns the hostname of portworx service if it is present
	GetServiceEndpoint() (string, error)
	// UpgradePortworx upgrades portworx to the given docker image and tag
	UpgradePortworx(image, tag string) error
	// IsPXReadyOnNode returns true if PX pod is up on that node, else returns false
	IsPXReadyOnNode(n node.Node) bool
	// IsPXEnabled returns true if portworx is enabled on given node
	IsPXEnabled(n node.Node) (bool, error)
	// GetRemotePXNodes returns list of PX node found on destination k8s cluster
	// refereced by kubeconfig
	GetRemotePXNodes(destKubeConfig string) ([]node.Node, error)
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
