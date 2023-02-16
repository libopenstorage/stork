package volume

import (
	"fmt"
	"net"
	"regexp"
	"strings"

	aws_sdk "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	"github.com/kubernetes-sigs/aws-ebs-csi-driver/pkg/cloud"
	"github.com/libopenstorage/stork/drivers"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/common"
)

const (
	// Default snapshot type if drivers don't support different types or can't
	// find driver
	defaultSnapType = "Local"
	// PortworxDriverName is the name of the portworx driver implementation
	PortworxDriverName = "pxd"
	// AWSDriverName is the name of the aws driver implementation
	AWSDriverName = "aws"
	// AzureDriverName is the name of the azure driver implementation
	AzureDriverName = "azure"
	// CSIDriverName is the name of the k8s driver implementation.
	CSIDriverName = "csi"
	// GCEDriverName is the name of the gcp driver implementation
	GCEDriverName = "gce"
	// LinstorDriverName is the name of the Linstor driver implementation
	LinstorDriverName = "linstor"
	// KDMPDriverName is the name of the kdmp driver implementation
	KDMPDriverName = "kdmp"
	// ZoneSeperator zone separator
	ZoneSeperator = "__"
	// EbsProvisionerName EBS provisioner name
	EbsProvisionerName      = "kubernetes.io/aws-ebs"
	pureCSIProvisioner      = "pure-csi"
	ocpCephfsProvisioner    = "openshift-storage.cephfs.csi.ceph.com"
	ocpRbdProvisioner       = "openshift-storage.rbd.csi.ceph.com"
	vSphereCSIProvisioner   = "csi.vsphere.vmware.com"
	efsCSIProvisioner       = "efs.csi.aws.com"
	azureFileCSIProvisioner = "file.csi.azure.com"

	azureFileIntreeProvisioner = "kubernetes.io/azure-file"
	googleFileCSIProvisioner   = "com.google.csi.filestore"
	// Note: filestore.csi.storage.gke.io this provisoner supports snapshot. So not adding in csiDriverWithoutSnapshotSupport list
	gkeFileCSIProvisioner       = "filestore.csi.storage.gke.io"
	pureBackendParam            = "backend"
	pureFileParam               = "file"
	csiDriverWithOutSnapshotKey = "CSI_DRIVER_WITHOUT_SNAPSHOT"
)

var (
	// orderedListOfDrivers is an ordered list of drivers in which stork
	// will check if a PVC is owned by a particular driver. This ordered list
	// is mainly required since we want the KDMP driver to be the fallback
	// mechanism if none of drivers satisfy a PVC
	orderedListOfDrivers = []string{
		PortworxDriverName,
		AWSDriverName,
		AzureDriverName,
		GCEDriverName,
		LinstorDriverName,
		CSIDriverName,
		KDMPDriverName,
	}
	csiDriverWithoutSnapshotSupport = []string{
		vSphereCSIProvisioner,
		efsCSIProvisioner,
		azureFileCSIProvisioner,
		azureFileIntreeProvisioner,
		googleFileCSIProvisioner,
	}
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

	// GetPodVolumes Gets the volumes used by a pod backed by the driver
	// If includePendingWFFC is set, also returns volumes that are pending due to WaitForFirstConsumer binding
	GetPodVolumes(podSpec *v1.PodSpec, namespace string, includePendingWFFC bool) ([]*Info, []*Info, error)

	// GetVolumeClaimTemplates Get all the volume templates from the list backed by
	// the driver
	GetVolumeClaimTemplates([]v1.PersistentVolumeClaim) ([]v1.PersistentVolumeClaim, error)

	// OwnsPVC returns true if the PVC is owned by the driver
	OwnsPVC(coreOps core.Ops, pvc *v1.PersistentVolumeClaim) bool

	// OwnsPVCForBackup returns true if the PVC is owned by the driver
	// Since we have extra check need to done for backup case, added seperate version of API.
	OwnsPVCForBackup(coreOps core.Ops, pvc *v1.PersistentVolumeClaim, cmBackupType string, crBackupType string, blType storkapi.BackupLocationType) bool

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

	// GetPodPatches returns driver-specific json patches to mutate the pod in a webhook
	GetPodPatches(podNamespace string, pod *v1.Pod) ([]k8sutils.JSONPatchOp, error)

	// GetCSIPodPrefix returns prefix for the csi pod names in the deployment
	GetCSIPodPrefix() (string, error)

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
	UpdateMigratedPersistentVolumeSpec(*v1.PersistentVolume, *storkapi.ApplicationRestoreVolumeInfo, map[string]string) (*v1.PersistentVolume, error)
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
	// CleanupBackupResources the backup of resource specified backup
	CleanupBackupResources(*storkapi.ApplicationBackup) error
	// Delete the backups specified in the status
	DeleteBackup(*storkapi.ApplicationBackup) (bool, error)
	// Get any resources that should be created before the restore is started
	GetPreRestoreResources(*storkapi.ApplicationBackup, *storkapi.ApplicationRestore, []runtime.Unstructured, []byte) ([]runtime.Unstructured, error)
	// Start restore of volumes specified by the spec. Should only restore
	// volumes, not the specs associated with them
	StartRestore(*storkapi.ApplicationRestore, []*storkapi.ApplicationBackupVolumeInfo, []runtime.Unstructured) ([]*storkapi.ApplicationRestoreVolumeInfo, error)
	// Get the status of restore of the volumes specified in the status
	// for the restore spec
	GetRestoreStatus(*storkapi.ApplicationRestore) ([]*storkapi.ApplicationRestoreVolumeInfo, error)
	// Cancel the restore of volumes specified in the status
	CancelRestore(*storkapi.ApplicationRestore) error
	// CleanupRestoreResources for specigied restore
	CleanupRestoreResources(*storkapi.ApplicationRestore) error
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
	// NeedsAntiHyperconvergence is a flag for figuring if Pod needs anti-hyperconvergence
	NeedsAntiHyperconvergence bool
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
	// RawStatus as returned by the driver
	RawStatus string
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

// GetDefaultDriverName returns the default driver name in case on isn't set
func GetDefaultDriverName() string {
	return "pxd"
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

// GetPVCDriverForBackup  gets the driver associated with a PVC for backup operation. Returns ErrNotSupported if the PVC is
// not owned by any available driver
func GetPVCDriverForBackup(coreOps core.Ops,
	pvc *v1.PersistentVolumeClaim,
	cmBackupType string,
	crBackupType string,
	blType storkapi.BackupLocationType,
) (string, error) {
	for _, driverName := range orderedListOfDrivers {
		d, ok := volDrivers[driverName]
		if !ok {
			continue
		}
		if d.OwnsPVCForBackup(coreOps, pvc, cmBackupType, crBackupType, blType) {
			return driverName, nil
		}
	}
	return "", &errors.ErrNotSupported{
		Feature: "VolumeDriver",
		Reason:  fmt.Sprintf("PVC %v/%v provisioned using unsupported driver", pvc.Namespace, pvc.Name),
	}
}

// GetPVCDriver gets the driver associated with a PVC. Returns ErrNotFound if the PVC is
// not owned by any available driver
func GetPVCDriver(coreOps core.Ops,
	pvc *v1.PersistentVolumeClaim,
) (string, error) {
	for _, driverName := range orderedListOfDrivers {
		d, ok := volDrivers[driverName]
		if !ok {
			continue
		}
		if d.OwnsPVC(coreOps, pvc) {
			return driverName, nil
		}
	}
	return "", &errors.ErrNotSupported{
		Feature: "VolumeDriver",
		Reason:  fmt.Sprintf("PVC %v/%v provisioned using unsupported driver", pvc.Namespace, pvc.Name),
	}
}

// GetPVDriver gets the driver associated with a PV. Returns ErrNotFound if the PV is
// not owned by any available driver
func GetPVDriver(pv *v1.PersistentVolume) (string, error) {
	for _, driverName := range orderedListOfDrivers {
		d, ok := volDrivers[driverName]
		if !ok {
			continue
		}
		if d.OwnsPV(pv) {
			return driverName, nil
		}
	}
	return "", &errors.ErrNotSupported{
		Feature: "VolumeDriver",
		Reason:  fmt.Sprintf("PV %v provisioned using unsupported driver", pv.Name),
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
func (b *BackupRestoreNotSupported) DeleteBackup(*storkapi.ApplicationBackup) (bool, error) {
	return true, &errors.ErrNotSupported{}
}

// CleanupBackupResources returns ErrNotSupported
func (b *BackupRestoreNotSupported) CleanupBackupResources(*storkapi.ApplicationBackup) error {
	return &errors.ErrNotSupported{}
}

// GetPreRestoreResources returns ErrNotSupported
func (b *BackupRestoreNotSupported) GetPreRestoreResources(
	*storkapi.ApplicationBackup,
	*storkapi.ApplicationRestore,
	[]runtime.Unstructured,
) ([]runtime.Unstructured, error) {
	return nil, &errors.ErrNotSupported{}
}

// StartRestore returns ErrNotSupported
func (b *BackupRestoreNotSupported) StartRestore(
	*storkapi.ApplicationRestore,
	[]*storkapi.ApplicationBackupVolumeInfo,
	[]runtime.Unstructured,
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

// CleanupRestoreResources returns ErrNotSupported
func (b *BackupRestoreNotSupported) CleanupRestoreResources(*storkapi.ApplicationRestore) error {
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

// GetPVCFromObjects gets the pvc object from the objects for a particular volume
func GetPVCFromObjects(objects []runtime.Unstructured, volumeBackupInfo *storkapi.ApplicationBackupVolumeInfo) (*v1.PersistentVolumeClaim, error) {
	var pvc v1.PersistentVolumeClaim
	for _, o := range objects {
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return &pvc, err
		}

		if objectType.GetKind() == "PersistentVolumeClaim" {
			var tempPVC v1.PersistentVolumeClaim
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.UnstructuredContent(), &tempPVC)
			if err != nil {
				return &pvc, nil
			}
			if tempPVC.Name == volumeBackupInfo.PersistentVolumeClaim {
				pvc = tempPVC
				break
			}
		}
	}
	return &pvc, nil
}

// IsCSIDriverWithoutSnapshotSupport - check whether CSI PV supports snapshot
// This function will called in csi driver and kdmp volume driver.
// In csi driver, this api is called to default to kdmp by not owning in CSI driver.
// In kdmp driver, this api is called to decide whether we need to set the volumesnapclass,
// to try local snapshot first.
func IsCSIDriverWithoutSnapshotSupport(pv *v1.PersistentVolume) bool {
	// check if CSI volume
	if pv.Spec.CSI != nil {
		driverName := pv.Spec.CSI.Driver
		// pure FB csi driver does not support snapshot
		if driverName == pureCSIProvisioner {
			if pv.Spec.CSI.VolumeAttributes[pureBackendParam] == pureFileParam {
				return true
			}
		}
		// For now, it is decided to take generic backup for OCP provisoiners.
		if driverName == ocpCephfsProvisioner || driverName == ocpRbdProvisioner {
			return true
		}
		// vsphere, efs, azure file and google file does not support snapshot.
		// So defaulting to kdmp by not setting volumesnapshot class.
		for _, name := range csiDriverWithoutSnapshotSupport {
			if name == driverName {
				return true
			}
		}
		// If csiDriverWithOutSnapshotKey is set in kdmp-config configmap,
		// include it in the check.
		kdmpData, err := core.Instance().GetConfigMap(drivers.KdmpConfigmapName, drivers.KdmpConfigmapNamespace)
		if err != nil {
			logrus.Warnf("IsCSIDriverWithoutSnapshotSupport: error in reading configMap [%v/%v]",
				drivers.KdmpConfigmapName, drivers.KdmpConfigmapNamespace)
			return false
		}
		if len(kdmpData.Data[csiDriverWithOutSnapshotKey]) != 0 {
			nameList := strings.Split(kdmpData.Data[csiDriverWithOutSnapshotKey], ",")
			for _, name := range nameList {
				if driverName == name {
					return true
				}
			}
		}
		return false
	}
	return true
}

// GetNodeRegion - get the node region
func GetNodeRegion() (string, error) {
	nodes, err := core.Instance().GetNodes()
	if err != nil {
		logrus.Errorf("getNodeZones: getting node details failed: %v", err)
		return "", fmt.Errorf("failed in getting the nodes: %v", err)
	}
	for _, node := range nodes.Items {
		return node.Labels[v1.LabelTopologyRegion], nil
	}
	return "", fmt.Errorf("failed in getting the nodes: %v", err)
}

// GetNodeZone gets the node zone
func GetNodeZone() (string, error) {
	nodes, err := core.Instance().GetNodes()
	if err != nil {
		logrus.Errorf("getNodeZones: getting node details failed: %v", err)
		return "", fmt.Errorf("failed in getting the nodes: %v", err)
	}
	for _, node := range nodes.Items {
		return node.Labels[v1.LabelTopologyZone], nil
	}
	return "", fmt.Errorf("failed in getting the nodes zone: %v", err)
}

// GetNodeZones - get zone details of the nodes in the cluster.
func GetNodeZones() ([]string, error) {
	nodes, err := core.Instance().GetNodes()
	if err != nil {
		logrus.Errorf("getNodeZones: getting node details failed: %v", err)
		return nil, fmt.Errorf("failed in getting the nodes: %v", err)
	}
	nodeZoneList := []string{}
	exists := map[string]bool{}
	for _, node := range nodes.Items {
		nodeZone := strings.Split(node.Labels[v1.LabelTopologyZone], "-")
		if !exists[nodeZone[2]] {
			exists[nodeZone[2]] = true
			nodeZoneList = append(nodeZoneList, nodeZone[2])
		}
	}
	return nodeZoneList, nil
}

// GetVolumeBackupZones - Get zone details from the volumeBackupInfo
func GetVolumeBackupZones(
	volumeBackupInfos []*storkapi.ApplicationBackupVolumeInfo,
) []string {
	backupZoneList := []string{}
	exists := map[string]bool{}
	for _, volume := range volumeBackupInfos {
		if len(volume.Zones) != 0 {
			backupZone := strings.Split(volume.Zones[0], "-")
			if !exists[backupZone[2]] {
				exists[backupZone[2]] = true
				backupZoneList = append(backupZoneList, backupZone[2])
			}
		}
	}
	return backupZoneList
}

// MapZones - return the map with a relation between the source and destination zones
func MapZones(sourceZones, destZones []string) map[string]string {
	zoneMap := make(map[string]string)
	for i, sourceZone := range sourceZones {
		foundExactMatch := false
		for _, destZone := range destZones {
			if destZone == sourceZone {
				zoneMap[sourceZone] = destZone
				foundExactMatch = true
				break
			}
		}
		if foundExactMatch {
			continue
		}
		if i < len(destZones) {
			zoneMap[sourceZone] = destZones[i]
		} else {
			// More zones in source
			zoneMap[sourceZone] = destZones[i%len(destZones)]
		} // We don't care if we have more zones in dest
	}
	return zoneMap
}

// GetEBSVolume gets EBS volume
func GetEBSVolume(volumeID string, filters map[string]string, client *ec2.EC2) (*ec2.Volume, error) {
	input := &ec2.DescribeVolumesInput{}
	if volumeID != "" {
		input.VolumeIds = []*string{&volumeID}
	}
	if len(filters) > 0 {
		input.Filters = getFiltersFromMap(filters)
	}

	output, err := client.DescribeVolumes(input)
	if err != nil {
		return nil, err
	}

	if len(output.Volumes) != 1 {
		return nil, fmt.Errorf("received %v volumes for %v", len(output.Volumes), volumeID)
	}
	return output.Volumes[0], nil
}

func getFiltersFromMap(filters map[string]string) []*ec2.Filter {
	tagFilters := make([]*ec2.Filter, 0)
	for k, v := range filters {
		tagFilters = append(tagFilters, &ec2.Filter{
			Name:   aws_sdk.String(fmt.Sprintf("tag:%v", k)),
			Values: []*string{aws_sdk.String(v)},
		})
	}
	return tagFilters
}

// GetEBSVolumeID get EBS vol ID
func GetEBSVolumeID(pv *v1.PersistentVolume) string {
	var volumeID string
	if pv.Spec.AWSElasticBlockStore != nil {
		volumeID = pv.Spec.AWSElasticBlockStore.VolumeID
	} else if pv.Spec.CSI != nil {
		volumeID = pv.Spec.CSI.VolumeHandle
	}
	return regexp.MustCompile("vol-.*").FindString(volumeID)
}

// GetAWSClient client handle for EC2 instance
func GetAWSClient() (*ec2.EC2, error) {
	s, err := session.NewSession(&aws_sdk.Config{})
	if err != nil {
		return nil, err
	}
	creds := credentials.NewChainCredentials(
		[]credentials.Provider{
			&credentials.EnvProvider{},
			&ec2rolecreds.EC2RoleProvider{
				Client: ec2metadata.New(s),
			},
			&credentials.SharedCredentialsProvider{},
		})
	metadata, err := cloud.NewMetadata()
	if err != nil {
		return nil, err
	}

	s, err = session.NewSession(&aws_sdk.Config{
		Region:      aws_sdk.String(metadata.GetRegion()),
		Credentials: creds,
	})
	if err != nil {
		return nil, err
	}

	return ec2.New(s), nil
}

// GetGCPZones get zones from GCP
func GetGCPZones(pv *v1.PersistentVolume) []string {
	if pv.Spec.GCEPersistentDisk != nil {
		var zone string
		val, ok := pv.Labels[v1.LabelZoneFailureDomain]
		if ok {
			zone = val
		} else if val, ok := pv.Labels[v1.LabelZoneFailureDomainStable]; ok {
			zone = val
		}
		if IsRegional(zone) {
			return GetRegionalZones(zone)
		}
		return []string{zone}
	}
	if pv.Spec.NodeAffinity != nil &&
		pv.Spec.NodeAffinity.Required != nil &&
		len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) != 0 {
		for _, selector := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions {
			if selector.Key == common.TopologyKeyZone {
				return selector.Values
			}
		}
	}
	return nil
}

// IsRegional is zone is regional
func IsRegional(zone string) bool {
	return strings.Contains(zone, ZoneSeperator)
}

// GetRegionalZones kist of regional zone
func GetRegionalZones(zones string) []string {
	return strings.Split(zones, ZoneSeperator)
}
