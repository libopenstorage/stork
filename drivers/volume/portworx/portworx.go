package portworx

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/libopenstorage/openstorage/api"
	clusterclient "github.com/libopenstorage/openstorage/api/client/cluster"
	volumeclient "github.com/libopenstorage/openstorage/api/client/volume"
	"github.com/libopenstorage/openstorage/cluster"
	"github.com/libopenstorage/openstorage/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"k8s.io/api/core/v1"
)

// TODO: Make some of these configurable
const (
	// driverName is the name of the portworx driver implementation
	driverName = "pxd"

	// serviceName is the name of the portworx service
	serviceName = "portworx-service"

	// namespace is the kubernetes namespace in which portworx daemon set runs
	namespace = "kube-system"

	//provisionerName is the name for the driver provisioner
	provisionerName = "kubernetes.io/portworx-volume"
)

type portworx struct {
	clusterManager cluster.Cluster
	volDriver      volume.VolumeDriver
}

func (p *portworx) String() string {
	return driverName
}

func (p *portworx) Init() error {
	var endpoint string
	svc, err := k8sutils.GetService(serviceName, namespace)
	if err == nil {
		endpoint = svc.Spec.ClusterIP
	} else {
		return fmt.Errorf("Failed to get k8s service spec: %v", err)
	}

	if len(endpoint) == 0 {
		return fmt.Errorf("Failed to get endpoint for portworx volume driver")
	}

	logrus.Infof("Using %v as endpoint for portworx volume driver", endpoint)
	clnt, err := clusterclient.NewClusterClient("http://"+endpoint+":9001", "v1")
	if err != nil {
		return err
	}
	p.clusterManager = clusterclient.ClusterManager(clnt)

	clnt, err = volumeclient.NewDriverClient("http://"+endpoint+":9001", "pxd", "", "stork")
	if err != nil {
		return err
	}
	p.volDriver = volumeclient.VolumeDriver(clnt)
	return err
}

func (p *portworx) InspectVolume(volumeID string) (*storkvolume.Info, error) {
	vols, err := p.volDriver.Inspect([]string{volumeID})
	if err != nil {
		return nil, &ErrFailedToInspectVolme{
			ID:    volumeID,
			Cause: fmt.Sprintf("Volume inspect returned err: %v", err),
		}
	}

	info := &storkvolume.Info{}
	info.VolumeID = volumeID
	for _, rset := range vols[0].ReplicaSets {
		for _, node := range rset.Nodes {
			info.DataNodes = append(info.DataNodes, node)
		}
	}
	return info, nil
}

func (p *portworx) mapNodeStatus(status api.Status) storkvolume.NodeStatus {
	switch status {
	case api.Status_STATUS_NONE:
		fallthrough
	case api.Status_STATUS_INIT:
		fallthrough
	case api.Status_STATUS_OFFLINE:
		fallthrough
	case api.Status_STATUS_ERROR:
		fallthrough
	case api.Status_STATUS_NOT_IN_QUORUM:
		fallthrough
	case api.Status_STATUS_DECOMMISSION:
		fallthrough
	case api.Status_STATUS_MAINTENANCE:
		fallthrough
	case api.Status_STATUS_NEEDS_REBOOT:
		return storkvolume.NodeOffline

	case api.Status_STATUS_OK:
		fallthrough
	case api.Status_STATUS_STORAGE_DOWN:
		return storkvolume.NodeOnline

	case api.Status_STATUS_STORAGE_DEGRADED:
		fallthrough
	case api.Status_STATUS_STORAGE_REBALANCE:
		fallthrough
	case api.Status_STATUS_STORAGE_DRIVE_REPLACE:
		return storkvolume.NodeDegraded
	default:
		return storkvolume.NodeOffline
	}
}

func (p *portworx) GetNodes() ([]*storkvolume.NodeInfo, error) {
	cluster, err := p.clusterManager.Enumerate()
	if err != nil {
		return nil, &ErrFailedToInspectVolme{
			Cause: err.Error(),
		}
	}

	var nodes []*storkvolume.NodeInfo
	for _, n := range cluster.Nodes {
		nodeInfo := &storkvolume.NodeInfo{
			ID:       n.Id,
			Hostname: n.Hostname,
			Status:   p.mapNodeStatus(n.Status),
		}
		nodes = append(nodes, nodeInfo)
	}

	return nodes, nil

}

func (p *portworx) GetPodVolumes(pod *v1.Pod) ([]*storkvolume.Info, error) {

	var volumes []*storkvolume.Info
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			pvc, err := k8sutils.GetPVC(volume.PersistentVolumeClaim.ClaimName, pod.Namespace)
			if err != nil {
				return nil, err
			}

			storageClassName := k8sutils.GetStorageClassName(pvc)
			if storageClassName == "" {
				logrus.Debugf("Empty StorageClass in PVC %v for pod %v, ignoring",
					pvc.Name, pod.Name)
				continue
			}

			storageClass, err := k8sutils.GetStorageClass(storageClassName, pod.Namespace)
			if err != nil {
				return nil, err
			}

			if storageClass.Provisioner != provisionerName {
				logrus.Debugf("Provisioner in Storageclass not Portworx, ignoring")
				continue
			}

			if pvc.Status.Phase == v1.ClaimPending {
				return nil, &storkvolume.ErrPVCPending{
					Name: volume.PersistentVolumeClaim.ClaimName,
				}
			}

			volumeInfo, err := p.InspectVolume(pvc.Spec.VolumeName)
			if err != nil {
				return nil, err
			}
			volumes = append(volumes, volumeInfo)

		}
	}
	return volumes, nil
}

func init() {
	storkvolume.Register(driverName, &portworx{})
}
