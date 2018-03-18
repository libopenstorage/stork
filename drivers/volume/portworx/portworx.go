package portworx

import (
	"fmt"
	"strings"

	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/kubernetes-incubator/external-storage/snapshot/pkg/controller/snapshotter"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	"github.com/libopenstorage/openstorage/api"
	clusterclient "github.com/libopenstorage/openstorage/api/client/cluster"
	volumeclient "github.com/libopenstorage/openstorage/api/client/volume"
	"github.com/libopenstorage/openstorage/cluster"
	"github.com/libopenstorage/openstorage/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/snapshot"
	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func (p *portworx) Init(_ interface{}) error {
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
		return nil, &ErrFailedToInspectVolume{
			ID:    volumeID,
			Cause: fmt.Sprintf("Volume inspect returned err: %v", err),
		}
	}

	if len(vols) == 0 {
		return nil, &errors.ErrNotFound{
			ID:   volumeID,
			Type: "Volume",
		}
	}

	info := &storkvolume.Info{}
	info.VolumeID = vols[0].Id
	info.VolumeName = vols[0].Locator.Name
	for _, rset := range vols[0].ReplicaSets {
		for _, node := range rset.Nodes {
			info.DataNodes = append(info.DataNodes, node)
		}
	}
	if vols[0].Source != nil {
		info.ParentID = vols[0].Source.Parent
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
		return nil, &ErrFailedToGetNodes{
			Cause: err.Error(),
		}
	}

	var nodes []*storkvolume.NodeInfo
	for _, n := range cluster.Nodes {
		nodeInfo := &storkvolume.NodeInfo{
			ID:       n.Id,
			Hostname: strings.ToLower(n.Hostname),
			Status:   p.mapNodeStatus(n.Status),
		}
		nodeInfo.IPs = append(nodeInfo.IPs, n.MgmtIp)
		nodeInfo.IPs = append(nodeInfo.IPs, n.DataIp)

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

			if storageClass.Provisioner != provisionerName && storageClass.Provisioner != snapshotcontroller.GetProvisionerName() {
				logrus.Debugf("Provisioner in Storageclass not Portworx or from the snapshot Provisioner, ignoring")
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

func (p *portworx) GetSnapshotPlugin() snapshotVolume.Plugin {
	return p
}

func (p *portworx) SnapshotCreate(pv *v1.PersistentVolume, tags *map[string]string) (*crdv1.VolumeSnapshotDataSource, *[]crdv1.VolumeSnapshotCondition, error) {
	if pv == nil || pv.Spec.PortworxVolume == nil {
		return nil, nil, fmt.Errorf("Invalid PV: %v", pv)
	}
	spec := &pv.Spec
	volumeID := spec.PortworxVolume.VolumeID

	logrus.Debugf("SnapshotCreate for pv: %+v \n tags: %v", pv, tags)
	locator := &api.VolumeLocator{
		Name: (*tags)[snapshotter.CloudSnapshotCreatedForVolumeSnapshotNameTag],
	}
	snapshotID, err := p.volDriver.Snapshot(volumeID, true, locator)
	if err != nil {
		return nil, nil, err
	}

	return &crdv1.VolumeSnapshotDataSource{
		PortworxSnapshot: &crdv1.PortworxVolumeSnapshotSource{
			SnapshotID: snapshotID,
		},
	}, nil, nil
}

func (p *portworx) SnapshotDelete(snapshot *crdv1.VolumeSnapshotDataSource, _ *v1.PersistentVolume) error {
	if snapshot == nil || snapshot.PortworxSnapshot == nil {
		return fmt.Errorf("Invalid Snaphsot source %v", snapshot)
	}
	return p.volDriver.Delete(snapshot.PortworxSnapshot.SnapshotID)
}

func (p *portworx) SnapshotRestore(
	snapshotData *crdv1.VolumeSnapshotData,
	pvc *v1.PersistentVolumeClaim,
	pvName string,
	parameters map[string]string,
) (*v1.PersistentVolumeSource, map[string]string, error) {
	if snapshotData == nil || snapshotData.Spec.PortworxSnapshot == nil {
		return nil, nil, fmt.Errorf("Invalid Snapshot spec")
	}
	if pvc == nil {
		return nil, nil, fmt.Errorf("Invalid PVC spec")
	}

	snapID := snapshotData.Spec.PortworxSnapshot.SnapshotID

	logrus.Debugf("SnapshotRestore for pvc: %+v", pvc)
	locator := &api.VolumeLocator{
		Name: "pvc-" + string(pvc.UID),
	}
	volumeID, err := p.volDriver.Snapshot(snapID, false, locator)
	if err != nil {
		return nil, nil, err
	}

	vols, err := p.volDriver.Inspect([]string{volumeID})
	if err != nil {
		return nil, nil, &ErrFailedToInspectVolume{
			ID:    volumeID,
			Cause: fmt.Sprintf("Volume inspect returned err: %v", err),
		}
	}

	if len(vols) == 0 {
		return nil, nil, &errors.ErrNotFound{
			ID:   volumeID,
			Type: "Volume",
		}
	}

	pv := &v1.PersistentVolumeSource{
		PortworxVolume: &v1.PortworxVolumeSource{
			VolumeID: volumeID,
			FSType:   vols[0].Format.String(),
			ReadOnly: vols[0].Readonly,
		},
	}

	labels := make(map[string]string)

	return pv, labels, nil
}

func (p *portworx) DescribeSnapshot(snapshotData *crdv1.VolumeSnapshotData) (*[]crdv1.VolumeSnapshotCondition, bool, error) {
	if snapshotData == nil || snapshotData.Spec.PortworxSnapshot == nil {
		return nil, false, fmt.Errorf("Invalid VolumeSnapshotDataSource: %v", snapshotData)
	}
	snapshotID := snapshotData.Spec.PortworxSnapshot.SnapshotID
	_, err := p.InspectVolume(snapshotID)
	if err != nil {
		return nil, false, err
	}
	var snapConditions []crdv1.VolumeSnapshotCondition
	snapConditions = []crdv1.VolumeSnapshotCondition{
		{
			Type:               crdv1.VolumeSnapshotConditionReady,
			Status:             v1.ConditionTrue,
			Message:            "Snapshot created successfully and it is ready",
			LastTransitionTime: metav1.Now(),
		},
	}
	return &snapConditions, true, err
}

// TODO: Implement FindSnapshot
func (p *portworx) FindSnapshot(tags *map[string]string) (*crdv1.VolumeSnapshotDataSource, *[]crdv1.VolumeSnapshotCondition, error) {
	return nil, nil, nil
}

func (p *portworx) VolumeDelete(pv *v1.PersistentVolume) error {
	if pv == nil || pv.Spec.PortworxVolume == nil {
		return fmt.Errorf("Invalid PV: %v", pv)
	}
	return p.volDriver.Delete(pv.Spec.PortworxVolume.VolumeID)
}

func init() {
	if err := storkvolume.Register(driverName, &portworx{}); err != nil {
		logrus.Panicf("Error registering portworx volume driver: %v", err)
	}
}
