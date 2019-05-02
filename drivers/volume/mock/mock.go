package mock

import (
	"fmt"
	"strconv"
	"strings"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	stork_crd "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	k8shelper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

const (
	driverName = "MockDriver"
	// storageClassName is the storage class for mock driver PVCs
	storageClassName = "mockDriverStorageClass"
	provisionerName  = "kubernetes.io/mock-volume"
	clusterID        = "MockClusterID"
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

// CreateCluster Creates a cluster with specified number of nodes
func (m *Driver) CreateCluster(numNodes int, nodes *v1.NodeList) error {
	if len(m.nodes) > 0 {
		m.nodes = m.nodes[:0]
	}
	for i := 0; i < numNodes; i++ {
		node := &storkvolume.NodeInfo{
			StorageID: "node" + strconv.Itoa(i+1),
			Hostname:  "node" + strconv.Itoa(i+1),
			Status:    storkvolume.NodeOnline,
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
	return storageClassName
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

// ProvisionVolume Provision a volume in the mock driver
func (m *Driver) ProvisionVolume(
	volumeName string,
	replicaIndexes []int,
	size uint64,
) error {
	if _, ok := m.volumes[volumeName]; ok {
		return fmt.Errorf("Volume %v already exists", volumeName)
	}

	volume := &storkvolume.Info{
		VolumeID:   volumeName,
		VolumeName: volumeName,
		Size:       size,
	}

	for i := 0; i < len(replicaIndexes); i++ {
		nodeIndex := replicaIndexes[i]
		if len(m.nodes) <= nodeIndex {
			return fmt.Errorf("Node not found")
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
		return fmt.Errorf("Node not found")
	}
	m.nodes[nodeIndex].Status = nodeStatus
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

// GetPodVolumes Get the Volumes in the Pod that use the mock driver
func (m Driver) GetPodVolumes(podSpec *v1.PodSpec, namespace string) ([]*storkvolume.Info, error) {
	if m.interfaceError != nil {
		return nil, m.interfaceError
	}
	var volumes []*storkvolume.Info
	for _, volume := range podSpec.Volumes {
		if volume.PersistentVolumeClaim != nil {
		}
	}
	for _, volume := range podSpec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			pvc, ok := m.pvcs[volume.PersistentVolumeClaim.ClaimName]
			if !ok {
				logrus.Debugf("PVCs: %+v", m.pvcs)
				return nil, &errors.ErrNotFound{
					ID:   volume.PersistentVolumeClaim.ClaimName,
					Type: "PVC",
				}
			}

			storageClassName := k8shelper.GetPersistentVolumeClaimClass(pvc)
			if storageClassName == "" {
				logrus.Debugf("Empty StorageClass in PVC %v , ignoring", pvc.Name)
				continue
			}

			// Assume all mock volume have the same
			// storageclass
			if storageClassName != storageClassName {
				continue
			}

			volumeInfo, err := m.InspectVolume(pvc.Spec.VolumeName)
			if err != nil {
				return nil, err
			}
			volumes = append(volumes, volumeInfo)
		}
	}

	return volumes, nil
}

// OwnsPVC returns false since mock driver doesn't own any PVCs
func (m *Driver) OwnsPVC(pvc *v1.PersistentVolumeClaim) bool {
	return false
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

// VolumeSnapshotRestore Not implemented
func (m *Driver) VolumeSnapshotRestore(snap *stork_crd.VolumeSnapshotRestore) error {
	return &errors.ErrNotImplemented{}
}

func init() {
	if err := storkvolume.Register(driverName, &Driver{}); err != nil {
		logrus.Panicf("Error registering mock volume driver: %v", err)
	}
}
