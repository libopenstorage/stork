package mock

import (
	"fmt"
	"strconv"
	"strings"

	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	k8shelper "k8s.io/kubernetes/pkg/api/v1/helper"
)

const (
	driverName       = "MockDriver"
	storageClassName = "mockDriverStorageClass"
	provisionerName  = "kubernetes.io/mock-volume"
	// RackLabel Label used for the mock driver to set rack information
	RackLabel = "mock/rack"
	// ZoneLabel Label used for the mock driver to set zone information
	ZoneLabel = "mock/zone"
	// RegionLabel Label used for the mock driver to set region information
	RegionLabel = "mock/region"
)

// Driver Mock driver for tests
type Driver struct {
	nodes          []*storkvolume.NodeInfo
	volumes        map[string]*storkvolume.Info
	pvcs           map[string]*v1.PersistentVolumeClaim
	interfaceError error
}

// String Returns the name for the driver
func (m Driver) String() string {
	return driverName
}

// Init Initialize the mock driver
func (m Driver) Init(_ interface{}) error {
	return nil
}

// CreateCluster Creates a cluster with specified number of nodes
func (m *Driver) CreateCluster(numNodes int, nodes *v1.NodeList) error {
	if len(m.nodes) > 0 {
		m.nodes = m.nodes[:0]
	}
	for i := 0; i < numNodes; i++ {
		node := &storkvolume.NodeInfo{
			ID:       "node" + strconv.Itoa(i+1),
			Hostname: "node" + strconv.Itoa(i+1),
			Status:   storkvolume.NodeOnline,
		}
		for _, n := range nodes.Items {
			for _, address := range n.Status.Addresses {
				if address.Type == v1.NodeHostName {
					if address.Address == node.Hostname || strings.HasPrefix(address.Address, node.Hostname+".") {
						node.Rack = n.Labels[RackLabel]
						node.Zone = n.Labels[ZoneLabel]
						node.Region = n.Labels[RegionLabel]
					}
				}
			}

		}
		m.nodes = append(m.nodes, node)
	}
	m.volumes = make(map[string]*storkvolume.Info)
	m.pvcs = make(map[string]*v1.PersistentVolumeClaim)
	m.interfaceError = nil
	return nil
}

// GetStorageClassName Returns the storageclass name to be used by tests
func (m *Driver) GetStorageClassName() string {
	return storageClassName
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
		volume.DataNodes = append(volume.DataNodes, m.nodes[nodeIndex].ID)
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
func (m Driver) GetPodVolumes(pod *v1.Pod) ([]*storkvolume.Info, error) {
	if m.interfaceError != nil {
		return nil, m.interfaceError
	}
	var volumes []*storkvolume.Info
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
		}
	}
	for _, volume := range pod.Spec.Volumes {
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
				logrus.Debugf("Empty StorageClass in PVC %v for pod %v, ignoring",
					pvc.Name, pod.Name)
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

//GetSnapshotPlugin Returns nil since snapshot is not supported in the mock driver
func (m *Driver) GetSnapshotPlugin() snapshotVolume.Plugin {
	return nil
}

func init() {
	if err := storkvolume.Register(driverName, &Driver{}); err != nil {
		logrus.Panicf("Error registering mock volume driver: %v", err)
	}
}
