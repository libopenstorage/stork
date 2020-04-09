package volume

import (
	"net"
	"strings"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
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

	// InspectNode using ID
	InspectNode(id string) (*NodeInfo, error)

	// GetPodVolumes Get all the volumes used by a pod backed by the driver
	GetPodVolumes(*v1.PodSpec, string) ([]*Info, error)

	// GetVolumeClaimTemplates Get all the volume templates from the list backed by
	// the driver
	GetVolumeClaimTemplates([]v1.PersistentVolumeClaim) ([]v1.PersistentVolumeClaim, error)

	// OwnsPVC returns true if the PVC is owned by the driver
	OwnsPVC(pvc *v1.PersistentVolumeClaim) bool

	// OwnsPV returns true if the PV is owned by the driver
	OwnsPV(pvc *v1.PersistentVolume) bool

	// GetSnapshotPlugin Get the snapshot plugin to be used for the driver
	GetSnapshotPlugin() snapshotVolume.Plugin

	// GetSnapshotType Get the type of the snapshot. Return error is snapshot
	// doesn't belong to driver
	GetSnapshotType(snap *snapv1.VolumeSnapshot) (string, error)

	// Stop the driver
	Stop() error

	// GetClusterID returns the clusterID for the driver
	GetClusterID() (string, error)

	// GroupSnapshotPluginInterface Interface for group snapshots
	GroupSnapshotPluginInterface
	// ClusterPairPluginInterface Interface to pair clusters
	ClusterPairPluginInterface
	// MigratePluginInterface Interface to migrate data between clusters
	MigratePluginInterface
	// ClusterDomainsPluginInterface Interface to manage cluster domains
	ClusterDomainsPluginInterface
	// BackupRestorePluginInterface Interface to backup and restore volumes
	BackupRestorePluginInterface
	// ClonePluginInterface Interface to clone volumes
	ClonePluginInterface
	// SnapshotRestorePluginInterface Interface to do in-place restore of volumes
	SnapshotRestorePluginInterface
}

// GroupSnapshotCreateResponse is the response for the group snapshot operation
type GroupSnapshotCreateResponse struct {
	Snapshots []*storkapi.VolumeSnapshotStatus
}

// GroupSnapshotPluginInterface is used to perform group snapshot operations
type GroupSnapshotPluginInterface interface {
	// CreateGroupSnapshot creates a group snapshot with the given pvcs
	CreateGroupSnapshot(snap *storkapi.GroupVolumeSnapshot) (*GroupSnapshotCreateResponse, error)
	// GetGroupSnapshotStatus returns status of group snapshot
	GetGroupSnapshotStatus(snap *storkapi.GroupVolumeSnapshot) (*GroupSnapshotCreateResponse, error)
	// DeleteGroupSnapshot delete a group snapshot with the given spec
	DeleteGroupSnapshot(snap *storkapi.GroupVolumeSnapshot) error
}

// ClusterPairPluginInterface Interface to pair clusters
type ClusterPairPluginInterface interface {
	// Create a pair with a remote cluster
	CreatePair(*storkapi.ClusterPair) (string, error)
	// Deletes a paring with a remote cluster
	DeletePair(*storkapi.ClusterPair) error
}

// MigratePluginInterface Interface to migrate data between clusters
type MigratePluginInterface interface {
	// Start migration of volumes specified by the spec. Should only migrate
	// volumes, not the specs associated with them
	StartMigration(*storkapi.Migration) ([]*storkapi.MigrationVolumeInfo, error)
	// Get the status of migration of the volumes specified in the status
	// for the migration spec
	GetMigrationStatus(*storkapi.Migration) ([]*storkapi.MigrationVolumeInfo, error)
	// Cancel the migration of volumes specified in the status
	CancelMigration(*storkapi.Migration) error
	// Update the PVC spec to point to the migrated volume on the destination
	// cluster
	UpdateMigratedPersistentVolumeSpec(*v1.PersistentVolume) (*v1.PersistentVolume, error)
}

// ClusterDomainsPluginInterface Interface to manage cluster domains
type ClusterDomainsPluginInterface interface {
	// GetClusterDomains returns all the cluster domains and their status
	GetClusterDomains() (*storkapi.ClusterDomains, error)
	// ActivateClusterDomain activates a cluster domain
	ActivateClusterDomain(*storkapi.ClusterDomainUpdate) error
	// DeactivateClusterDomain deactivates a cluster domain
	DeactivateClusterDomain(*storkapi.ClusterDomainUpdate) error
}

// BackupRestorePluginInterface Interface to backup and restore volumes
type BackupRestorePluginInterface interface {
	// Start backup of volumes specified by the spec. Should only backup
	// volumes, not the specs associated with them
	StartBackup(*storkapi.ApplicationBackup, []v1.PersistentVolumeClaim) ([]*storkapi.ApplicationBackupVolumeInfo, error)
	// Get the status of backup of the volumes specified in the status
	// for the backup spec
	GetBackupStatus(*storkapi.ApplicationBackup) ([]*storkapi.ApplicationBackupVolumeInfo, error)
	// Cancel the backup of volumes specified in the status
	CancelBackup(*storkapi.ApplicationBackup) error
	// Delete the backups specified in the status
	DeleteBackup(*storkapi.ApplicationBackup) error
	// Start restore of volumes specified by the spec. Should only restore
	// volumes, not the specs associated with them
	StartRestore(*storkapi.ApplicationRestore, []*storkapi.ApplicationBackupVolumeInfo) ([]*storkapi.ApplicationRestoreVolumeInfo, error)
	// Get the status of restore of the volumes specified in the status
	// for the restore spec
	GetRestoreStatus(*storkapi.ApplicationRestore) ([]*storkapi.ApplicationRestoreVolumeInfo, error)
	// Cancel the restore of volumes specified in the status
	CancelRestore(*storkapi.ApplicationRestore) error
}

// SnapshotRestorePluginInterface Interface to perform in place restore of volume
type SnapshotRestorePluginInterface interface {
	// StartVolumeSnapshotRestore will prepare volume for restore
	StartVolumeSnapshotRestore(*storkapi.VolumeSnapshotRestore) error

	// CompleteVolumeSnapshotRestore will perform in-place restore for given snapshot and associated pvc
	// Returns error if restore failed
	CompleteVolumeSnapshotRestore(*storkapi.VolumeSnapshotRestore) error

	// GetVolumeSnapshotRestore returns snapshot restore status
	GetVolumeSnapshotRestoreStatus(*storkapi.VolumeSnapshotRestore) error

	// CleanupSnapshotRestoreObjects deletes restore objects if any
	CleanupSnapshotRestoreObjects(*storkapi.VolumeSnapshotRestore) error
}

// ClonePluginInterface Interface to clone volumes
type ClonePluginInterface interface {
	CreateVolumeClones(*storkapi.ApplicationClone) error
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
	// StorageID is a unique identifier for the storage node
	StorageID string
	// SchedulerID is a unique identifier for the scheduler node
	SchedulerID string
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

// GetPVCDriver gets the driver associated with a PVC. Returns ErrNotFound if the PVC is
// not owned by any available driver
func GetPVCDriver(pvc *v1.PersistentVolumeClaim) (string, error) {
	for driverName, d := range volDrivers {
		if d.OwnsPVC(pvc) {
			return driverName, nil
		}
	}
	return "", &errors.ErrNotFound{
		ID:   pvc.Name,
		Type: "VolumeDriver",
	}
}

// GetPVDriver gets the driver associated with a PV. Returns ErrNotFound if the PV is
// not owned by any available driver
func GetPVDriver(pv *v1.PersistentVolume) (string, error) {
	for driverName, d := range volDrivers {
		if d.OwnsPV(pv) {
			return driverName, nil
		}
	}
	return "", &errors.ErrNotFound{
		ID:   pv.Name,
		Type: "VolumeDriver",
	}
}

// ClusterPairNotSupported to be used by drivers that don't support pairing
type ClusterPairNotSupported struct{}

// CreatePair Returns ErrNotSupported
func (c *ClusterPairNotSupported) CreatePair(*storkapi.ClusterPair) (string, error) {
	return "", &errors.ErrNotSupported{}
}

// DeletePair Returns ErrNotSupported
func (c *ClusterPairNotSupported) DeletePair(*storkapi.ClusterPair) error {
	return &errors.ErrNotSupported{}
}

// MigrationNotSupported to be used by drivers that don't support migration
type MigrationNotSupported struct{}

// StartMigration returns ErrNotSupported
func (m *MigrationNotSupported) StartMigration(*storkapi.Migration) ([]*storkapi.MigrationVolumeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

// GetMigrationStatus returns ErrNotSupported
func (m *MigrationNotSupported) GetMigrationStatus(*storkapi.Migration) ([]*storkapi.MigrationVolumeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

// CancelMigration returns ErrNotSupported
func (m *MigrationNotSupported) CancelMigration(*storkapi.Migration) error {
	return &errors.ErrNotSupported{}
}

// UpdateMigratedPersistentVolumeSpec returns ErrNotSupported
func (m *MigrationNotSupported) UpdateMigratedPersistentVolumeSpec(
	*v1.PersistentVolume,
) (*v1.PersistentVolume, error) {
	return nil, &errors.ErrNotSupported{}
}

// GroupSnapshotNotSupported to be used by drivers that don't support group snapshots
type GroupSnapshotNotSupported struct{}

// CreateGroupSnapshot returns ErrNotSupported
func (g *GroupSnapshotNotSupported) CreateGroupSnapshot(*storkapi.GroupVolumeSnapshot) (*GroupSnapshotCreateResponse, error) {
	return nil, &errors.ErrNotSupported{}
}

// GetGroupSnapshotStatus returns ErrNotSupported
func (g *GroupSnapshotNotSupported) GetGroupSnapshotStatus(*storkapi.GroupVolumeSnapshot) (*GroupSnapshotCreateResponse, error) {
	return nil, &errors.ErrNotSupported{}
}

// DeleteGroupSnapshot returns ErrNotSupported
func (g *GroupSnapshotNotSupported) DeleteGroupSnapshot(*storkapi.GroupVolumeSnapshot) error {
	return &errors.ErrNotSupported{}
}

// ClusterDomainsNotSupported to be used by drivers that don't support cluster domains
type ClusterDomainsNotSupported struct{}

// GetClusterDomains returns all the cluster domains and their status
func (c *ClusterDomainsNotSupported) GetClusterDomains() (*storkapi.ClusterDomains, error) {
	return nil, &errors.ErrNotSupported{}
}

// ActivateClusterDomain activates a cluster domain
func (c *ClusterDomainsNotSupported) ActivateClusterDomain(*storkapi.ClusterDomainUpdate) error {
	return &errors.ErrNotSupported{}
}

// DeactivateClusterDomain deactivates a cluster domain
func (c *ClusterDomainsNotSupported) DeactivateClusterDomain(*storkapi.ClusterDomainUpdate) error {
	return &errors.ErrNotSupported{}
}

// BackupRestoreNotSupported to be used by drivers that don't support backup
type BackupRestoreNotSupported struct{}

// StartBackup returns ErrNotSupported
func (b *BackupRestoreNotSupported) StartBackup(
	*storkapi.ApplicationBackup,
	[]v1.PersistentVolumeClaim,
) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

// GetBackupStatus returns ErrNotSupported
func (b *BackupRestoreNotSupported) GetBackupStatus(*storkapi.ApplicationBackup) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

// CancelBackup returns ErrNotSupported
func (b *BackupRestoreNotSupported) CancelBackup(*storkapi.ApplicationBackup) error {
	return &errors.ErrNotSupported{}
}

// DeleteBackup returns ErrNotSupported
func (b *BackupRestoreNotSupported) DeleteBackup(*storkapi.ApplicationBackup) error {
	return &errors.ErrNotSupported{}
}

// StartRestore returns ErrNotSupported
func (b *BackupRestoreNotSupported) StartRestore(
	*storkapi.ApplicationRestore,
	[]*storkapi.ApplicationBackupVolumeInfo,
) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

// GetRestoreStatus returns ErrNotSupported
func (b *BackupRestoreNotSupported) GetRestoreStatus(*storkapi.ApplicationRestore) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

// CancelRestore returns ErrNotSupported
func (b *BackupRestoreNotSupported) CancelRestore(*storkapi.ApplicationRestore) error {
	return &errors.ErrNotSupported{}
}

// CloneNotSupported to be used by drivers that don't support volume clone
type CloneNotSupported struct{}

// CreateVolumeClones returns ErrNotSupported
func (c *CloneNotSupported) CreateVolumeClones(*storkapi.ApplicationClone) error {
	return &errors.ErrNotSupported{}
}

// SnapshotRestoreNotSupported to be used by drivers that don't support
// volume snapshot restore
type SnapshotRestoreNotSupported struct{}

// CompleteVolumeSnapshotRestore returns ErrNotSupported
func (s *SnapshotRestoreNotSupported) CompleteVolumeSnapshotRestore(snap *storkapi.VolumeSnapshotRestore) error {
	return &errors.ErrNotSupported{}
}

// StartVolumeSnapshotRestore returns ErrNotSupported
func (s *SnapshotRestoreNotSupported) StartVolumeSnapshotRestore(*storkapi.VolumeSnapshotRestore) error {
	return &errors.ErrNotSupported{}
}

// GetVolumeSnapshotRestoreStatus returns ErrNotSupported
func (s *SnapshotRestoreNotSupported) GetVolumeSnapshotRestoreStatus(*storkapi.VolumeSnapshotRestore) error {
	return &errors.ErrNotSupported{}
}

// CleanupSnapshotRestoreObjects deletes restore objects if any
func (s *SnapshotRestoreNotSupported) CleanupSnapshotRestoreObjects(*storkapi.VolumeSnapshotRestore) error {
	return &errors.ErrNotImplemented{}
}

// IsNodeMatch There are a couple of things that need to be checked to see if the driver
// node matched the k8s node since different k8s installs set the node name,
// hostname and IPs differently
func IsNodeMatch(k8sNode *v1.Node, driverNode *NodeInfo) bool {
	if driverNode == nil {
		return false
	}

	if k8sNode.Name == driverNode.SchedulerID {
		return true
	}
	if isHostnameMatch(driverNode.StorageID, k8sNode.Name) {
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

// RemoveDuplicateOfflineNodes Removes duplicate offline nodes from the list which have
// the same IP as an online node
func RemoveDuplicateOfflineNodes(nodes []*NodeInfo) []*NodeInfo {
	updatedNodes := make([]*NodeInfo, 0)
	offlineNodes := make([]*NodeInfo, 0)
	onlineIPs := make([]string, 0)
	// First add the online nodes to the list
	for _, node := range nodes {
		if node.Status == NodeOnline {
			updatedNodes = append(updatedNodes, node)
			onlineIPs = append(onlineIPs, node.IPs...)
		} else {
			offlineNodes = append(offlineNodes, node)
		}
	}

	// Then go through the offline nodes and ignore any which have
	// the same IP as an online node
	for _, offlineNode := range offlineNodes {
		found := false
		for _, offlineIP := range offlineNode.IPs {
			for _, onlineIP := range onlineIPs {
				if offlineIP == onlineIP {
					found = true
					break
				}
			}
		}
		if !found {
			updatedNodes = append(updatedNodes, offlineNode)
		}
	}
	return updatedNodes
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

// GetApplicationBackupLabels Gets the labels that need to be applied to a
// snapshot when creating a backup
func GetApplicationBackupLabels(
	backup *storkapi.ApplicationBackup,
	pvc *v1.PersistentVolumeClaim,
) map[string]string {
	return map[string]string{
		"created-by":           "stork",
		"backup-uid":           string(backup.UID),
		"source-pvc-name":      pvc.Name,
		"source-pvc-namespace": pvc.Namespace,
	}
}

// GetApplicationRestoreLabels Gets the labels that need to be applied to a
// volume when restoring from a backup
func GetApplicationRestoreLabels(
	restore *storkapi.ApplicationRestore,
	volumeInfo *storkapi.ApplicationRestoreVolumeInfo,
) map[string]string {
	return map[string]string{
		"created-by":           "stork",
		"restore-uid":          string(restore.UID),
		"source-pvc-name":      volumeInfo.PersistentVolumeClaim,
		"source-pvc-namespace": volumeInfo.SourceNamespace,
	}
}
