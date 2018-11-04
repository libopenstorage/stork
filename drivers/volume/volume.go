package volume

import (
	"net"
	"strings"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	stork_crd "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	// Default snapshot type if drivers don't support different types or can't
	// find driver
	defaultSnapType = "Local"
)

// Driver defines an external volume driver interface.
// Any driver that wants to be used with stork needs to implement these
// interfaces.
type Driver interface {
	// Init initializes the volume driver.
	Init(interface{}) error

	// String returns the string name of this driver.
	String() string

	// InspectVolume returns information about a volume.
	InspectVolume(volumeID string) (*Info, error)

	// GetNodes Get the list of nodes where the driver is available
	GetNodes() ([]*NodeInfo, error)

	// GetPodVolumes Get all the volumes used by a pod backed by the driver
	GetPodVolumes(*v1.PodSpec, string) ([]*Info, error)

	// GetVolumeClaimTemplates Get all the volume templates from the list backed by
	// the driver
	GetVolumeClaimTemplates([]v1.PersistentVolumeClaim) ([]v1.PersistentVolumeClaim, error)

	// OwnsPVC returns true if the PVC is owned by the driver
	OwnsPVC(pvc *v1.PersistentVolumeClaim) bool

	// GetSnapshotPlugin Get the snapshot plugin to be used for the driver
	GetSnapshotPlugin() snapshotVolume.Plugin

	// GetSnapshotType Get the type of the snapshot. Return error is snapshot
	// doesn't belong to driver
	GetSnapshotType(snap *snapv1.VolumeSnapshot) (string, error)

	// Stop the driver
	Stop() error

	// ClusterPairPluginInterface Interface to pair clusters
	ClusterPairPluginInterface
	// MigratePluginInterface Interface to migrate data between clusters
	MigratePluginInterface
}

// ClusterPairPluginInterface Interface to pair clusters
type ClusterPairPluginInterface interface {
	// Create a pair with a remote cluster
	CreatePair(*stork_crd.ClusterPair) (string, error)
	// Deletes a paring with a remote cluster
	DeletePair(*stork_crd.ClusterPair) error
}

// MigratePluginInterface Interface to migrate data between clusters
type MigratePluginInterface interface {
	// Start migration of volumes specified by the spec. Should only migrate
	// volumes, not the specs associated with them
	StartMigration(*stork_crd.Migration) ([]*stork_crd.VolumeInfo, error)
	// Get the status of migration of the volumes specified in the status
	// for the migration spec
	GetMigrationStatus(*stork_crd.Migration) ([]*stork_crd.VolumeInfo, error)
	// Cancel the migration of volumes specified in the status
	CancelMigration(*stork_crd.Migration) error
	// Update the PVC spec to point to the migrated volume on the destination
	// cluster
	UpdateMigratedPersistentVolumeSpec(object runtime.Unstructured) (runtime.Unstructured, error)
}

// Info Information about a volume
type Info struct {
	// VolumeID is a unique identifier for the volume
	VolumeID string
	// VolumeName is the name for the volume
	VolumeName string
	// DataNodes is a list of nodes where the data for the volume resides
	DataNodes []string
	// Size is the size of the volume in GB
	Size uint64
	// ParentID points to the ID of the parent volume for snapshots
	ParentID string
	// Labels are user applied labels on the volume
	Labels map[string]string
	// VolumeSourceRef is a optional reference to the source of the volume
	VolumeSourceRef interface{}
}

// NodeStatus Status of driver on a node
type NodeStatus string

const (
	// NodeOnline Node is online
	NodeOnline NodeStatus = "Online"
	// NodeOffline Node is Offline
	NodeOffline NodeStatus = "Offline"
	// NodeDegraded Node is in degraded state
	NodeDegraded NodeStatus = "Degraded"
)

// NodeInfo Information about a node
type NodeInfo struct {
	// ID is a unique identifier for the node
	ID string
	// Hostname of the node. Should be in lower case because Kubernetes
	// converts it to lower case
	Hostname string
	// IPs List of IPs associated with the node
	IPs []string
	// Rack Specifies the rack within the datacenter where the node is located
	Rack string
	// Zone Specifies the zone where the rack is located
	Zone string
	// Region Specifies the region where the datacenter is located
	Region string
	// Status of the node
	Status NodeStatus
}

var (
	volDrivers = make(map[string]Driver)
)

// Register registers the given volume driver
func Register(name string, d Driver) error {
	logrus.Debugf("Registering volume driver: %v", name)
	volDrivers[name] = d
	return nil
}

// Get an external storage provider to be used with Stork
func Get(name string) (Driver, error) {
	d, ok := volDrivers[name]
	if ok {
		return d, nil
	}

	return nil, &errors.ErrNotFound{
		ID:   name,
		Type: "VolumeDriver",
	}
}

// ClusterPairNotSupported to be used by drivers that don't support pairing
type ClusterPairNotSupported struct{}

// CreatePair Returns ErrNotSupported
func (c *ClusterPairNotSupported) CreatePair(*stork_crd.ClusterPair) (string, error) {
	return "", &errors.ErrNotSupported{}
}

// DeletePair Returns ErrNotSupported
func (c *ClusterPairNotSupported) DeletePair(*stork_crd.ClusterPair) error {
	return &errors.ErrNotSupported{}
}

// MigrationNotSupported to be used by drivers that don't support migration
type MigrationNotSupported struct{}

// StartMigration returns ErrNotSupported
func (m *MigrationNotSupported) StartMigration(*stork_crd.Migration) ([]*stork_crd.VolumeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

// GetMigrationStatus returns ErrNotSupported
func (m *MigrationNotSupported) GetMigrationStatus(*stork_crd.Migration) ([]*stork_crd.VolumeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

// CancelMigration returns ErrNotSupported
func (m *MigrationNotSupported) CancelMigration(*stork_crd.Migration) error {
	return &errors.ErrNotSupported{}
}

// UpdateMigratedPersistentVolumeSpec returns ErrNotSupported
func (m *MigrationNotSupported) UpdateMigratedPersistentVolumeSpec(
	runtime.Unstructured,
) (runtime.Unstructured, error) {
	return nil, &errors.ErrNotSupported{}
}

// IsNodeMatch There are a couple of things that need to be checked to see if the driver
// node matched the k8s node since different k8s installs set the node name,
// hostname and IPs differently
func IsNodeMatch(k8sNode *v1.Node, driverNode *NodeInfo) bool {
	if driverNode == nil {
		return false
	}

	if isHostnameMatch(driverNode.ID, k8sNode.Name) {
		return true
	}
	for _, address := range k8sNode.Status.Addresses {
		switch address.Type {
		case v1.NodeHostName:
			if isHostnameMatch(driverNode.Hostname, address.Address) {
				return true
			}
			if isResolvedHostnameMatch(driverNode.IPs, address.Address) {
				return true
			}
		case v1.NodeInternalIP:
			for _, ip := range driverNode.IPs {
				if ip == address.Address {
					return true
				}
			}
		}
	}
	return false
}

// The driver might not return fully qualified hostnames, so check if the short
// hostname matches too
func isHostnameMatch(driverHostname string, k8sHostname string) bool {
	if driverHostname == k8sHostname {
		return true
	}
	if strings.HasPrefix(k8sHostname, driverHostname+".") {
		return true
	}
	return false
}

// For K8s on DC/OS lookup IPs of the hostname. Could block for invalid
// hostnames so don't want to do this for other environments currently.
func isResolvedHostnameMatch(driverIPs []string, k8sHostname string) bool {
	if strings.HasSuffix(k8sHostname, ".mesos") {
		k8sIPs, err := net.LookupHost(k8sHostname)
		if err != nil {
			return false
		}
		for _, dip := range driverIPs {
			for _, kip := range k8sIPs {
				if dip == kip {
					return true
				}
			}
		}
	}
	return false
}

// GetSnapshotType gets the snapshot type
func GetSnapshotType(snap *snapv1.VolumeSnapshot) string {
	for _, driver := range volDrivers {
		snapType, err := driver.GetSnapshotType(snap)
		if err == nil {
			return snapType
		}
	}

	return defaultSnapType
}
