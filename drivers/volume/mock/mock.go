package mock

import (
	"fmt"
	"strconv"
	"strings"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/pborman/uuid"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8shelper "k8s.io/component-helpers/storage/volume"
)

const (
	driverName = "MockDriver"
	// mockStorageClassName is the storage class for mock driver PVCs
	mockStorageClassName = "mockDriverStorageClass"
	// MockStorageClassNameWFFC is storage class used to mock having WaitForFirstConsumer
	MockStorageClassNameWFFC = "mockDriverStorageClassWFFC"
	provisionerName          = "kubernetes.io/mock-volume"
	clusterID                = "MockClusterID"
	// RackLabel Label used for the mock driver to set rack information
	RackLabel = "mock/rack"
	// ZoneLabel Label used for the mock driver to set zone information
	ZoneLabel = "mock/zone"
	// RegionLabel Label used for the mock driver to set region information
	RegionLabel = "mock/region"
)

// Driver Mock driver for tests
type Driver struct {
	storkvolume.ClusterPairNotSupported
	storkvolume.MigrationNotSupported
	storkvolume.GroupSnapshotNotSupported
	storkvolume.ClusterDomainsNotSupported
	storkvolume.BackupRestoreNotSupported
	storkvolume.CloneNotSupported
	storkvolume.SnapshotRestoreNotSupported
	nodes          []*storkvolume.NodeInfo
	volumes        map[string]*storkvolume.Info
	pvcs           map[string]*v1.PersistentVolumeClaim
	interfaceError error
	clusterID      string
}

// String Returns the name for the driver
func (m Driver) String() string {
	return driverName
}

// Init Initialize the mock driver
func (m Driver) Init(_ interface{}) error {
	return nil
}

// Stop Stops the mock driver
func (m Driver) Stop() error {
	return nil
}

func (m *Driver) GetPreRestoreResources(
	*storkapi.ApplicationBackup,
	*storkapi.ApplicationRestore,
	[]runtime.Unstructured,
	[]byte,
) ([]runtime.Unstructured, error) {
	return nil, nil
}

// CreateCluster Creates a cluster with specified number of nodes
func (m *Driver) CreateCluster(numNodes int, nodes *v1.NodeList) error {
	if len(m.nodes) > 0 {
		m.nodes = m.nodes[:0]
	}
	for i := 0; i < numNodes; i++ {
		node := &storkvolume.NodeInfo{
			StorageID:   "node" + strconv.Itoa(i+1),
			Hostname:    "node" + strconv.Itoa(i+1),
			SchedulerID: "node" + strconv.Itoa(i+1),
			Status:      storkvolume.NodeOnline,
		}
		node.IPs = append(node.IPs, "192.168.0."+strconv.Itoa(i+1))
		for _, n := range nodes.Items {
			found := false
			if node.StorageID == n.Name {
				found = true
			} else {
				for _, address := range n.Status.Addresses {
					if address.Type == v1.NodeHostName {
						if address.Address == node.Hostname ||
							strings.HasPrefix(address.Address, node.Hostname+".") {
							found = true
							break
						}
					} else if address.Type == v1.NodeInternalIP && address.Address == node.IPs[0] {
						found = true
						break
					}
				}
			}
			if found {
				node.Rack = n.Labels[RackLabel]
				node.Zone = n.Labels[ZoneLabel]
				node.Region = n.Labels[RegionLabel]
			}
		}
		m.nodes = append(m.nodes, node)
	}
	m.volumes = make(map[string]*storkvolume.Info)
	m.pvcs = make(map[string]*v1.PersistentVolumeClaim)
	m.interfaceError = nil
	m.clusterID = "stork-test-" + uuid.New()
	return nil
}

// GetStorageClassName Returns the storageclass name to be used by tests
func (m *Driver) GetStorageClassName() string {
	return mockStorageClassName
}

// GetClusterID returns the clusterID for the driver
func (m *Driver) GetClusterID() (string, error) {
	return m.clusterID, nil
}

// NewPVC Create a new PVC reference
func (m *Driver) NewPVC(volumeName string) *v1.PersistentVolumeClaim {
	pvc := &v1.PersistentVolumeClaim{}
	pvc.Name = volumeName
	pvc.Spec.VolumeName = volumeName
	storageClassName := m.GetStorageClassName()
	pvc.Spec.StorageClassName = &storageClassName
	m.pvcs[volumeName] = pvc
	return pvc
}

// AddPVC adds an already created PVC
func (m *Driver) AddPVC(pvc *v1.PersistentVolumeClaim) {
	m.pvcs[pvc.Name] = pvc
}

// ProvisionVolume Provision a volume in the mock driver
func (m *Driver) ProvisionVolume(
	volumeName string,
	replicaIndexes []int,
	size uint64,
	labels map[string]string,
	needsAntiHyperconvergence bool,
) error {
	if _, ok := m.volumes[volumeName]; ok {
		return fmt.Errorf("volume %v already exists", volumeName)
	}

	volume := &storkvolume.Info{
		VolumeID:                  volumeName,
		VolumeName:                volumeName,
		Size:                      size,
		Labels:                    labels,
		NeedsAntiHyperconvergence: needsAntiHyperconvergence,
	}

	for i := 0; i < len(replicaIndexes); i++ {
		nodeIndex := replicaIndexes[i]
		if len(m.nodes) <= nodeIndex {
			return fmt.Errorf("node not found")
		}
		volume.DataNodes = append(volume.DataNodes, m.nodes[nodeIndex].StorageID)
	}

	m.volumes[volumeName] = volume
	return nil
}

// UpdateNodeStatus Update status for a node
func (m *Driver) UpdateNodeStatus(
	nodeIndex int,
	nodeStatus storkvolume.NodeStatus,
) error {
	if len(m.nodes) <= nodeIndex {
		return fmt.Errorf("node %v not found", nodeIndex)
	}
	m.nodes[nodeIndex].Status = nodeStatus
	return nil
}

// UpdateNodeIP Update IP for a node
func (m *Driver) UpdateNodeIP(
	nodeIndex int,
	ip string,
) error {
	if len(m.nodes) <= nodeIndex {
		return fmt.Errorf("node %v not found", nodeIndex)
	}
	m.nodes[nodeIndex].IPs[0] = ip
	return nil
}

// SetInterfaceError to the specified error. Used for negative testing
func (m *Driver) SetInterfaceError(err error) {
	m.interfaceError = err
}

// InspectVolume Return information for a given volume
func (m Driver) InspectVolume(volumeID string) (*storkvolume.Info, error) {
	if m.interfaceError != nil {
		return nil, m.interfaceError
	}

	volume, ok := m.volumes[volumeID]
	if !ok {
		return nil, &errors.ErrNotFound{
			ID:   volumeID,
			Type: "volume",
		}
	}
	return volume, nil
}

// GetNodes Get info about the nodes where the driver is running
func (m Driver) GetNodes() ([]*storkvolume.NodeInfo, error) {
	if m.interfaceError != nil {
		return nil, m.interfaceError
	}
	return m.nodes, nil
}

// InspectNode using ID
func (m Driver) InspectNode(id string) (*storkvolume.NodeInfo, error) {
	return nil, &errors.ErrNotSupported{}
}

// GetPodVolumes Get the Volumes in the Pod that use the mock driver
func (m Driver) GetPodVolumes(podSpec *v1.PodSpec, namespace string, includePendingWFFC bool) ([]*storkvolume.Info, []*storkvolume.Info, error) {
	if m.interfaceError != nil {
		return nil, nil, m.interfaceError
	}
	var volumes []*storkvolume.Info
	var pendingWFFC []*storkvolume.Info
	for _, volume := range podSpec.Volumes {
		isWFFC := false
		if volume.PersistentVolumeClaim != nil {
			pvc, ok := m.pvcs[volume.PersistentVolumeClaim.ClaimName]
			if !ok {
				logrus.Debugf("PVCs: %+v", m.pvcs)
				return nil, nil, &errors.ErrNotFound{
					ID:   volume.PersistentVolumeClaim.ClaimName,
					Type: "PVC",
				}
			}

			storageClassName := k8shelper.GetPersistentVolumeClaimClass(pvc)
			if storageClassName == "" {
				logrus.Debugf("Empty StorageClass in PVC %v , ignoring", pvc.Name)
				continue
			}

			// Assume all mock volumes have the same storageclass
			if storageClassName != mockStorageClassName {
				if storageClassName == MockStorageClassNameWFFC {
					isWFFC = true
				} else {
					continue
				}
			}

			volumeInfo, err := m.InspectVolume(pvc.Spec.VolumeName)
			if err != nil {
				return nil, nil, err
			}
			if includePendingWFFC && isWFFC {
				pendingWFFC = append(pendingWFFC, volumeInfo)
			} else {
				volumes = append(volumes, volumeInfo)
			}
		}
	}

	return volumes, pendingWFFC, nil
}

// OwnsPVCForBackup returns true because it owns all PVCs created by tests
func (m *Driver) OwnsPVCForBackup(coreOps core.Ops, pvc *v1.PersistentVolumeClaim, cmBackupType string, crBackupType string) bool {
	return m.OwnsPVC(coreOps, pvc)
}

// OwnsPVC returns true because it owns all PVCs created by tests
func (m *Driver) OwnsPVC(coreOps core.Ops, pvc *v1.PersistentVolumeClaim) bool {
	return true
}

// OwnsPV returns true because it owns all PVC created by tests
func (m *Driver) OwnsPV(pv *v1.PersistentVolume) bool {
	return true
}

// GetSnapshotPlugin Returns nil since snapshot is not supported in the mock driver
func (m *Driver) GetSnapshotPlugin() snapshotVolume.Plugin {
	return nil
}

// GetVolumeClaimTemplates Not implemented for mock driver
func (m *Driver) GetVolumeClaimTemplates([]v1.PersistentVolumeClaim) (
	[]v1.PersistentVolumeClaim, error) {
	return nil, &errors.ErrNotImplemented{}
}

// GetSnapshotType Not implemented for mock driver
func (m *Driver) GetSnapshotType(snap *snapv1.VolumeSnapshot) (string, error) {
	return "", &errors.ErrNotImplemented{}
}

// UpdateMigratedPersistentVolumeSpec Not implemented for mock driver
func (m *Driver) UpdateMigratedPersistentVolumeSpec(
	pv *v1.PersistentVolume,
	vInfo *storkapi.ApplicationRestoreVolumeInfo,
	namespaceMapping map[string]string,
) (*v1.PersistentVolume, error) {

	return pv, nil

}

// GetPodPatches returns driver-specific json patches to mutate the pod in a webhook
func (m *Driver) GetPodPatches(podNamespace string, pod *v1.Pod) ([]k8sutils.JSONPatchOp, error) {
	return nil, nil
}

// GetCSIPodPrefix returns prefix for the csi pod names in the deployment
func (a *Driver) GetCSIPodPrefix() (string, error) {
	return "px-csi-ext", nil
}

func init() {
	if err := storkvolume.Register(driverName, &Driver{}); err != nil {
		logrus.Panicf("Error registering mock volume driver: %v", err)
	}
}
