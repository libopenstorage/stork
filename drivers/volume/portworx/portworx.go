package portworx

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"time"

	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	crdclient "github.com/kubernetes-incubator/external-storage/snapshot/pkg/client"
	"github.com/kubernetes-incubator/external-storage/snapshot/pkg/controller/snapshotter"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	"github.com/libopenstorage/openstorage/api"
	clusterclient "github.com/libopenstorage/openstorage/api/client/cluster"
	volumeclient "github.com/libopenstorage/openstorage/api/client/volume"
	"github.com/libopenstorage/openstorage/cluster"
	"github.com/libopenstorage/openstorage/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/snapshot"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	k8shelper "k8s.io/kubernetes/pkg/api/v1/helper"
	kubeletapis "k8s.io/kubernetes/pkg/kubelet/apis"
)

// TODO: Make some of these configurable
const (
	// driverName is the name of the portworx driver implementation
	driverName = "pxd"

	// serviceName is the name of the portworx service
	serviceName = "portworx-service"

	// namespace is the kubernetes namespace in which portworx daemon set runs
	namespace = "kube-system"

	// provisionerName is the name for the driver provisioner
	provisionerName = "kubernetes.io/portworx-volume"

	// pvcProvisionerAnnotation is the annotation on PVC which has the provisioner name
	pvcProvisionerAnnotation = "volume.beta.kubernetes.io/storage-provisioner"

	// pvcNameLabel is the key of the label used to store the PVC name
	pvcNameLabel = "pvc"

	// storkSnapNameLabel is the key of the label used to store the stork Snapshot name
	storkSnapNameLabel = "stork-snap"

	// namespaceLabel is the key of the label used to store the namespace of PVC
	// and snapshots
	namespaceLabel = "namespace"

	// pxRackLabelKey Label for rack information
	pxRackLabelKey         = "px/rack"
	snapshotDataNamePrefix = "k8s-volume-snapshot"
	readySnapshotMsg       = "Snapshot created successfully and it is ready"
	// volumeSnapshot* is configuration of exponential backoff for
	// waiting for snapshot operation to complete. Starting with 2
	// seconds, multiplying by 1.5 with each step and taking 20 steps at maximum.
	// It will time out after 20 steps (6650 seconds).
	volumeSnapshotInitialDelay = 2 * time.Second
	volumeSnapshotFactor       = 1.5
	volumeSnapshotSteps        = 20

	// will timeout after 30 minutes
	cloudSnapshotInitialDelay = 5 * time.Second
	cloudSnapshotFactor       = 1
	cloudSnapshotSteps        = 360
)

// snapshot annotation constants
const (
	pxAnnotationPrefix        = "portworx/"
	pxSnapshotTypeKey         = pxAnnotationPrefix + "snapshot-type"
	pxSnapshotGroupIDKey      = pxAnnotationPrefix + "group-id"
	pxCloudSnapshotCredsIDKey = pxAnnotationPrefix + "cloud-cred-id"
)

var snapAPICallBackoff = wait.Backoff{
	Duration: volumeSnapshotInitialDelay,
	Factor:   volumeSnapshotFactor,
	Steps:    volumeSnapshotSteps,
}

var cloudsnapBackoff = wait.Backoff{
	Duration: cloudSnapshotInitialDelay,
	Factor:   cloudSnapshotFactor,
	Steps:    cloudSnapshotSteps,
}

type portworx struct {
	clusterManager cluster.Cluster
	volDriver      volume.VolumeDriver
}

func (p *portworx) String() string {
	return driverName
}

func (p *portworx) Init(_ interface{}) error {
	var endpoint string
	svc, err := k8s.Instance().GetService(serviceName, namespace)
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

		labels, err := k8s.Instance().GetLabelsOnNode(nodeInfo.Hostname)
		if err == nil {
			if rack, ok := labels[pxRackLabelKey]; ok {
				nodeInfo.Rack = rack
			}
			if zone, ok := labels[kubeletapis.LabelZoneFailureDomain]; ok {
				nodeInfo.Zone = zone
			}
			if region, ok := labels[kubeletapis.LabelZoneRegion]; ok {
				nodeInfo.Region = region
			}
		} else {
			logrus.Errorf("Error getting labels for node %v: %v", nodeInfo.Hostname, err)
		}

		nodes = append(nodes, nodeInfo)
	}
	return nodes, nil
}

func (p *portworx) GetPodVolumes(pod *v1.Pod) ([]*storkvolume.Info, error) {
	var volumes []*storkvolume.Info
	for _, volume := range pod.Spec.Volumes {
		volumeName := ""
		if volume.PersistentVolumeClaim != nil {
			pvc, err := k8s.Instance().GetPersistentVolumeClaim(
				volume.PersistentVolumeClaim.ClaimName,
				pod.Namespace)
			if err != nil {
				return nil, err
			}

			storageClassName := k8shelper.GetPersistentVolumeClaimClass(pvc)
			if storageClassName == "" {
				logrus.Debugf("Empty StorageClass in PVC %v for pod %v, ignoring",
					pvc.Name, pod.Name)
				continue
			}

			provisioner := ""
			// Check for the provisioner in the PVC annotation. If not populated
			// try getting the provisioner from the Storage class.
			if val, ok := pvc.Annotations[pvcProvisionerAnnotation]; ok {
				provisioner = val
			} else {
				storageClass, err := k8s.Instance().GetStorageClass(storageClassName)
				if err != nil {
					return nil, err
				}
				provisioner = storageClass.Provisioner
			}

			if provisioner != provisionerName && provisioner != snapshotcontroller.GetProvisionerName() {
				logrus.Debugf("Provisioner in Storageclass not Portworx or from the snapshot Provisioner, ignoring")
				continue
			}

			if pvc.Status.Phase == v1.ClaimPending {
				return nil, &storkvolume.ErrPVCPending{
					Name: volume.PersistentVolumeClaim.ClaimName,
				}
			}
			volumeName = pvc.Spec.VolumeName
		} else if volume.PortworxVolume != nil {
			volumeName = volume.PortworxVolume.VolumeID
		}

		if volumeName != "" {
			volumeInfo, err := p.InspectVolume(volumeName)
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

func (p *portworx) getSnapshotName(tags *map[string]string) string {
	return "snapshot-" + (*tags)[snapshotter.CloudSnapshotCreatedForVolumeSnapshotUIDTag]
}

func (p *portworx) SnapshotCreate(
	snap *crdv1.VolumeSnapshot,
	pv *v1.PersistentVolume,
	tags *map[string]string,
) (*crdv1.VolumeSnapshotDataSource, *[]crdv1.VolumeSnapshotCondition, error) {
	var err error
	if pv == nil || pv.Spec.PortworxVolume == nil {
		err = fmt.Errorf("invalid PV: %v", pv)
		return nil, getErrorSnapshotConditions(err), err
	}

	if snap == nil {
		err = fmt.Errorf("snapshot spec not supplied to the create API")
		return nil, getErrorSnapshotConditions(err), err
	}

	snapStatusConditions := []crdv1.VolumeSnapshotCondition{}
	var snapshotID, snapshotDataName, snapshotCredID string
	spec := &pv.Spec
	volumeID := spec.PortworxVolume.VolumeID

	snapType, err := getSnapshotType(snap)
	if err != nil {
		return nil, getErrorSnapshotConditions(err), err
	}

	switch snapType {
	case crdv1.PortworxSnapshotTypeCloud:
		logrus.Debugf("Cloud SnapshotCreate for pv: %+v \n tags: %v", pv, tags)
		snapshotCredID = getCredIDFromSnapshot(snap)
		request := &api.CloudBackupCreateRequest{
			VolumeID:       volumeID,
			CredentialUUID: snapshotCredID,
		}
		err := p.volDriver.CloudBackupCreate(request)
		if err != nil {
			return nil, getErrorSnapshotConditions(err), err
		}

		logrus.Infof("cloudsnap backup started successfully. waiting for completion...")
		err = wait.ExponentialBackoff(cloudsnapBackoff, func() (bool, error) {
			snapshotID, err = p.checkCloudSnapStatus(volumeID)
			if err != nil {
				logrus.Error(err)
				return false, nil
			}

			return true, nil
		})
		if err != nil {
			return nil, getErrorSnapshotConditions(err), err
		}
	case crdv1.PortworxSnapshotTypeLocal:
		if isGroupSnap(snap) {
			groupID := snap.Metadata.Annotations[pxSnapshotGroupIDKey]
			groupLabels := parseGroupLabelsFromAnnotations(snap.Metadata.Annotations)

			infoStr := fmt.Sprintf("creating group snapshot: [%s] %s", snap.Metadata.Namespace, snap.Metadata.Name)
			if len(groupID) > 0 {
				infoStr = fmt.Sprintf("%s. Group ID: %s", infoStr, groupID)
			}

			if len(groupLabels) > 0 {
				infoStr = fmt.Sprintf("%s. Group labels: %v", infoStr, groupLabels)
			}

			logrus.Infof(infoStr)

			resp, err := p.volDriver.SnapshotGroup(groupID, groupLabels)
			if err != nil {
				return nil, getErrorSnapshotConditions(err), err
			}

			if len(resp.Snapshots) == 0 {
				err = fmt.Errorf("found 0 snapshots using given group selector/ID")
				return nil, getErrorSnapshotConditions(err), err
			}

			snapIDs := make([]string, 0)
			snapDataNames := make([]string, 0)
			failedSnapVols := make([]string, 0)

			// Loop through the response and check if all succeeded. Don't create any k8s objects if
			// any of the snapshots failed
			for volID, newSnapResp := range resp.Snapshots {
				if newSnapResp.GetVolumeCreateResponse() == nil {
					err = fmt.Errorf("Portworx API returned empty response on creation of group snapshot")
					return nil, getErrorSnapshotConditions(err), err
				}

				if newSnapResp.GetVolumeCreateResponse().GetVolumeResponse() != nil &&
					len(newSnapResp.GetVolumeCreateResponse().GetVolumeResponse().GetError()) != 0 {
					logrus.Errorf("Failed to create snapshot for volume: %s due to: %v",
						volID, newSnapResp.GetVolumeCreateResponse().GetVolumeResponse().GetError())
					failedSnapVols = append(failedSnapVols, volID)
					continue
				}
			}

			if len(failedSnapVols) > 0 {
				err = fmt.Errorf("all snapshots in the group did not succeed. failed: %v succeeded: %v",
					failedSnapVols, snapIDs)
				return nil, getErrorSnapshotConditions(err), err
			}

			for _, newSnapResp := range resp.Snapshots {
				newSnapID := newSnapResp.GetVolumeCreateResponse().GetId()
				snapObj, err := p.createVolumeSnapshotCRD(newSnapID, newSnapResp, groupID, groupLabels, snap)
				if err != nil {
					err = fmt.Errorf("failed to create CRD for PX snapshot: %s due to: %v", newSnapID, err)
					return nil, getErrorSnapshotConditions(err), err
				}

				snapIDs = append(snapIDs, newSnapID)
				snapDataNames = append(snapDataNames, snapObj.Spec.SnapshotDataName)
			}

			snapshotID = strings.Join(snapIDs, ",")
			snapshotDataName = strings.Join(snapDataNames, ",")
			snapStatusConditions = getReadySnapshotConditions()
		} else {
			logrus.Debugf("SnapshotCreate for pv: %+v \n tags: %v", pv, tags)
			snapName := p.getSnapshotName(tags)
			locator := &api.VolumeLocator{
				Name: snapName,
				VolumeLabels: map[string]string{
					storkSnapNameLabel: (*tags)[snapshotter.CloudSnapshotCreatedForVolumeSnapshotNameTag],
					namespaceLabel:     (*tags)[snapshotter.CloudSnapshotCreatedForVolumeSnapshotNamespaceTag],
				},
			}
			snapshotID, err = p.volDriver.Snapshot(volumeID, true, locator)
			if err != nil {
				return nil, getErrorSnapshotConditions(err), err
			}
			snapStatusConditions = getReadySnapshotConditions()
		}
	}

	return &crdv1.VolumeSnapshotDataSource{
		PortworxSnapshot: &crdv1.PortworxVolumeSnapshotSource{
			SnapshotID:          snapshotID,
			SnapshotData:        snapshotDataName,
			SnapshotType:        snapType,
			SnapshotCloudCredID: snapshotCredID,
		},
	}, &snapStatusConditions, nil
}

func (p *portworx) SnapshotDelete(snapDataSrc *crdv1.VolumeSnapshotDataSource, _ *v1.PersistentVolume) error {
	if snapDataSrc == nil || snapDataSrc.PortworxSnapshot == nil {
		return fmt.Errorf("Invalid snaphsot source %v", snapDataSrc)
	}

	switch snapDataSrc.PortworxSnapshot.SnapshotType {
	case crdv1.PortworxSnapshotTypeCloud:
		input := &api.CloudBackupDeleteRequest{
			ID:             snapDataSrc.PortworxSnapshot.SnapshotID,
			CredentialUUID: snapDataSrc.PortworxSnapshot.SnapshotCloudCredID,
			// TODO add force support
		}
		return p.volDriver.CloudBackupDelete(input)
	default:
		if len(snapDataSrc.PortworxSnapshot.SnapshotData) > 0 {
			r := csv.NewReader(strings.NewReader(snapDataSrc.PortworxSnapshot.SnapshotData))
			snapDataNames, err := r.Read()
			if err != nil {
				logrus.Errorf("failed to parse snap data csv. err: %v", err)
				return err
			}

			if len(snapDataNames) > 0 {
				var lastError error
				for _, snapDataName := range snapDataNames {
					snapData, err := k8s.Instance().GetSnapshotData(snapDataName)
					if err != nil {
						lastError = err
						logrus.Errorf("failed to get volume snapshot data: %s due to err: %v", snapDataName, err)
						continue
					}

					snapNamespace := snapData.Spec.VolumeSnapshotRef.Namespace
					snapName := snapData.Spec.VolumeSnapshotRef.Name

					logrus.Infof("Deleting VolumeSnapshot:[%s] %s", snapNamespace, snapName)
					err = k8s.Instance().DeleteSnapshot(snapName, snapNamespace)
					if err != nil {
						lastError = err
						logrus.Errorf("failed to get volume snapshot: [%s] %s due to err: %v", snapNamespace, snapName, err)
						continue
					}
				}
				return lastError
			}
		}

		return p.volDriver.Delete(snapDataSrc.PortworxSnapshot.SnapshotID)
	}
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

	snapshotName, present := pvc.ObjectMeta.Annotations[crdclient.SnapshotPVCAnnotation]
	if !present || len(snapshotName) == 0 {
		return nil, nil, fmt.Errorf("source volumesnapshot name not present in PVC annotations")
	}

	// Check if this is a group snapshot
	snap, err := k8s.Instance().GetSnapshot(snapshotName, pvc.GetNamespace())
	if err != nil {
		return nil, nil, err
	}

	if isGroupSnap(snap) {
		return nil, nil, fmt.Errorf("volumesnapshot: [%s] %s was used to create group snapshots. "+
			"To restore, use the volumesnapshots that were created by this group snapshot.", pvc.GetNamespace(), snapshotName)
	}

	snapID := snapshotData.Spec.PortworxSnapshot.SnapshotID

	logrus.Debugf("SnapshotRestore for pvc: %+v", pvc)
	locator := &api.VolumeLocator{
		Name: "pvc-" + string(pvc.UID),
		VolumeLabels: map[string]string{
			pvcNameLabel:   pvc.Name,
			namespaceLabel: pvc.Namespace,
		},
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
	var err error
	if snapshotData == nil || snapshotData.Spec.PortworxSnapshot == nil {
		err = fmt.Errorf("Invalid VolumeSnapshotDataSource: %v", snapshotData)
		return getErrorSnapshotConditions(err), false, nil
	}

	if snapshotData.Spec.PersistentVolumeRef == nil {
		err = fmt.Errorf("PersistentVolume reference not set for snapshot: %v", snapshotData)
		return getErrorSnapshotConditions(err), false, err
	}

	switch snapshotData.Spec.PortworxSnapshot.SnapshotType {
	case crdv1.PortworxSnapshotTypeLocal:
		r := csv.NewReader(strings.NewReader(snapshotData.Spec.PortworxSnapshot.SnapshotID))
		snapshotIDs, err := r.Read()
		if err != nil {
			return getErrorSnapshotConditions(err), false, err
		}

		for _, snapshotID := range snapshotIDs {
			_, err = p.InspectVolume(snapshotID)
			if err != nil {
				return getErrorSnapshotConditions(err), false, err
			}
		}
	case crdv1.PortworxSnapshotTypeCloud:
		pv, err := k8s.Instance().GetPersistentVolume(snapshotData.Spec.PersistentVolumeRef.Name)
		if err != nil {
			return getErrorSnapshotConditions(err), false, err
		}

		if pv.Spec.PortworxVolume == nil {
			err = fmt.Errorf("Parent PV: %s for snapshot is not a Portworx volume", pv.Name)
			return getErrorSnapshotConditions(err), false, err
		}

		_, err = p.checkCloudSnapStatus(pv.Spec.PortworxVolume.VolumeID)
		if err != nil {
			return getErrorSnapshotConditions(err), false, err
		}
	}

	snapConditions := getReadySnapshotConditions()
	return &snapConditions, true, err
}

// TODO: Implement FindSnapshot
func (p *portworx) FindSnapshot(tags *map[string]string) (*crdv1.VolumeSnapshotDataSource, *[]crdv1.VolumeSnapshotCondition, error) {
	return nil, nil, &errors.ErrNotImplemented{}
}

func (p *portworx) VolumeDelete(pv *v1.PersistentVolume) error {
	if pv == nil || pv.Spec.PortworxVolume == nil {
		return fmt.Errorf("Invalid PV: %v", pv)
	}
	return p.volDriver.Delete(pv.Spec.PortworxVolume.VolumeID)
}

func (p *portworx) createVolumeSnapshotCRD(
	pxSnapID string,
	newSnapResp *api.SnapCreateResponse,
	groupID string,
	groupLabels map[string]string,
	groupSnap *crdv1.VolumeSnapshot) (*crdv1.VolumeSnapshot, error) {
	parentPVC, err := p.findParentPVCBySnapID(pxSnapID)
	if err != nil {
		return nil, err
	}

	namespace := groupSnap.Metadata.Namespace
	volumeSnapshotName := fmt.Sprintf("%s-%s", groupSnap.Metadata.Name, parentPVC.GetName())
	snapDataSource := &crdv1.VolumeSnapshotDataSource{
		PortworxSnapshot: &crdv1.PortworxVolumeSnapshotSource{
			SnapshotID:   pxSnapID,
			SnapshotType: crdv1.PortworxSnapshotTypeLocal,
		},
	}

	snapStatus := getReadySnapshotConditions()

	snapLabels := make(map[string]string, 0)
	if len(groupID) > 0 {
		snapLabels[pxSnapshotGroupIDKey] = groupID
	}

	for k, v := range groupLabels {
		snapLabels[k] = v
	}

	snapData, err := p.createVolumeSnapshotData(volumeSnapshotName, namespace, snapDataSource, &snapStatus, snapLabels)
	if err != nil {
		return nil, err
	}

	snapDataSource.PortworxSnapshot.SnapshotData = snapData.Metadata.Name

	snap := &crdv1.VolumeSnapshot{
		Metadata: metav1.ObjectMeta{
			Name:      volumeSnapshotName,
			Namespace: namespace,
			Labels:    snapLabels,
		},
		Spec: crdv1.VolumeSnapshotSpec{
			SnapshotDataName:          snapData.Metadata.Name,
			PersistentVolumeClaimName: parentPVC.Name,
		},
		Status: crdv1.VolumeSnapshotStatus{
			Conditions: snapStatus,
		},
	}

	logrus.Infof("Creating VolumeSnapshot object [%#v]", snap)
	err = wait.ExponentialBackoff(snapAPICallBackoff, func() (bool, error) {
		_, err := k8s.Instance().CreateSnapshot(snap)
		if err != nil {
			logrus.Errorf("failed to create volumesnapshot due to err: %v", err)
			return false, nil
		}

		return true, nil
	})
	return snap, err
}

func (p *portworx) createVolumeSnapshotData(
	snapshotName, snapshotNamespace string,
	snapDataSource *crdv1.VolumeSnapshotDataSource,
	snapStatus *[]crdv1.VolumeSnapshotCondition,
	snapLabels map[string]string) (*crdv1.VolumeSnapshotData, error) {
	var lastCondition crdv1.VolumeSnapshotDataCondition
	if snapStatus != nil && len(*snapStatus) > 0 {
		conditions := *snapStatus
		ind := len(conditions) - 1
		lastCondition = crdv1.VolumeSnapshotDataCondition{
			Type:    (crdv1.VolumeSnapshotDataConditionType)(conditions[ind].Type),
			Status:  conditions[ind].Status,
			Message: conditions[ind].Message,
		}
	}

	snapshotData := &crdv1.VolumeSnapshotData{
		Metadata: metav1.ObjectMeta{
			Name:   snapshotName,
			Labels: snapLabels,
		},
		Spec: crdv1.VolumeSnapshotDataSpec{
			VolumeSnapshotRef: &v1.ObjectReference{
				Kind:      "VolumeSnapshot",
				Name:      snapshotName,
				Namespace: snapshotNamespace,
			},
			VolumeSnapshotDataSource: *snapDataSource,
		},
		Status: crdv1.VolumeSnapshotDataStatus{
			Conditions: []crdv1.VolumeSnapshotDataCondition{
				lastCondition,
			},
		},
	}

	var result *crdv1.VolumeSnapshotData
	err := wait.ExponentialBackoff(snapAPICallBackoff, func() (bool, error) {
		var err error
		result, err = k8s.Instance().CreateSnapshotData(snapshotData)
		if err != nil {
			logrus.Errorf("failed to create snapshotdata due to err: %v", err)
			return false, nil
		}

		return true, nil
	})

	if err != nil {
		logrus.Errorf("Error creating the VolumeSnapshotData for snap %s: %v", snapshotName, err)
		return nil, fmt.Errorf("Failed to create the VolumeSnapshotData for snap %s", snapshotName)
	}

	return result, nil
}

func (p *portworx) findParentPVCBySnapID(snapID string) (*v1.PersistentVolumeClaim, error) {
	pxSnaps, err := p.volDriver.Inspect([]string{snapID})
	if err != nil {
		return nil, err
	}

	if len(pxSnaps) != 1 {
		return nil, fmt.Errorf("failed to inspect Portworx snapshot: %s. Received: %d entries in response",
			snapID, len(pxSnaps))
	}

	pxSnap := pxSnaps[0]

	if len(pxSnap.Source.Parent) == 0 {
		return nil, fmt.Errorf("Portworx snapshot: %s does not have parent set", snapID)
	}

	snapParents, err := p.volDriver.Inspect([]string{pxSnap.Source.Parent})
	if err != nil {
		return nil, err
	}

	if len(snapParents) != 1 {
		return nil, fmt.Errorf("failed to inspect snapshot parent: %s. Received: %d entries in response",
			pxSnap.Source.Parent, len(snapParents))
	}

	snapParent := snapParents[0]
	parentPVName := snapParent.Locator.Name
	parentPV, err := k8s.Instance().GetPersistentVolume(parentPVName)
	if err != nil {
		return nil, err
	}

	return k8s.Instance().GetPersistentVolumeClaim(parentPV.Spec.ClaimRef.Name, parentPV.Spec.ClaimRef.Namespace)
}

func (p *portworx) checkCloudSnapStatus(volID string) (string, error) {
	response, err := p.volDriver.CloudBackupStatus(&api.CloudBackupStatusRequest{
		SrcVolumeID: volID,
	})
	if err != nil {
		return "", err
	}

	csStatus, present := response.Statuses[volID]
	if !present {
		return "", fmt.Errorf("failed to get cloudsnap status for volume: %s", volID)
	}

	statusStr := getCloudSnapStatusString(&csStatus)
	if csStatus.CompletedTime.IsZero() {
		return "", fmt.Errorf("cloudsnap for %s still not completed. %s", volID, statusStr)

	}

	if !strings.Contains(csStatus.Status, "Done") {
		return "", fmt.Errorf("cloudsnap %s still not done. %s", volID, statusStr)
	}

	logrus.Infof("cloudsnap backup: %s created successfully. %s", csStatus.ID, statusStr)
	return csStatus.ID, nil
}

func getSnapshotType(snap *crdv1.VolumeSnapshot) (crdv1.PortworxSnapshotType, error) {
	if snap.Metadata.Annotations != nil {
		if val, snapTypePresent := snap.Metadata.Annotations[pxSnapshotTypeKey]; snapTypePresent {
			if crdv1.PortworxSnapshotType(val) == crdv1.PortworxSnapshotTypeLocal ||
				crdv1.PortworxSnapshotType(val) == crdv1.PortworxSnapshotTypeCloud {
				return crdv1.PortworxSnapshotType(val), nil
			}

			return "", fmt.Errorf("unsupported Portworx snapshot type: %s", val)
		}
	}

	return crdv1.PortworxSnapshotTypeLocal, nil
}

func getCredIDFromSnapshot(snap *crdv1.VolumeSnapshot) (credID string) {
	if snap.Metadata.Annotations != nil {
		credID = snap.Metadata.Annotations[pxCloudSnapshotCredsIDKey]
	}

	return
}

func parseGroupLabelsFromAnnotations(annotations map[string]string) map[string]string {
	groupLabels := make(map[string]string, 0)
	for k, v := range annotations {
		// filter out known labels
		if k == pxCloudSnapshotCredsIDKey ||
			k == pxSnapshotTypeKey ||
			k == pxSnapshotGroupIDKey ||
			k == v1.LastAppliedConfigAnnotation {
			continue
		}

		if strings.HasPrefix(k, pxAnnotationPrefix) || k == namespaceLabel {
			groupLabels[k] = v
		}
	}

	return groupLabels
}

func isGroupSnap(snap *crdv1.VolumeSnapshot) bool {
	for k := range snap.Metadata.Annotations {
		if k == pxCloudSnapshotCredsIDKey || k == pxSnapshotTypeKey {
			continue
		}

		if k == namespaceLabel || k == pxSnapshotGroupIDKey {
			return true
		}

		if strings.HasPrefix(k, pxAnnotationPrefix) {
			return true
		}
	}

	return false
}

func getReadySnapshotConditions() []crdv1.VolumeSnapshotCondition {
	return []crdv1.VolumeSnapshotCondition{
		{
			Type:               crdv1.VolumeSnapshotConditionReady,
			Status:             v1.ConditionTrue,
			Message:            readySnapshotMsg,
			LastTransitionTime: metav1.Now(),
		},
	}
}

func getErrorSnapshotConditions(err error) *[]crdv1.VolumeSnapshotCondition {
	return &[]crdv1.VolumeSnapshotCondition{
		{
			Type:               crdv1.VolumeSnapshotConditionError,
			Status:             v1.ConditionTrue,
			Message:            fmt.Sprintf("snapshot failed due to err: %v", err),
			LastTransitionTime: metav1.Now(),
		},
	}
}

func getCloudSnapStatusString(status *api.CloudBackupStatus) string {
	var elapsedTime string
	if !status.StartTime.IsZero() {
		if status.CompletedTime.IsZero() {
			elapsedTime = time.Since(status.StartTime).String()
		} else {
			elapsedTime = status.CompletedTime.Sub(status.StartTime).String()
		}
	}

	statusStr := fmt.Sprintf("Status: %s Bytes done: %s", status.Status, strconv.FormatUint(status.BytesDone, 10))
	if len(elapsedTime) > 0 {
		statusStr = fmt.Sprintf("%s Elapsed time: %s", statusStr, elapsedTime)
	}

	if !status.CompletedTime.IsZero() {
		statusStr = fmt.Sprintf("%s Completion time: %v", statusStr, status.CompletedTime)
	}
	return statusStr
}

func init() {
	if err := storkvolume.Register(driverName, &portworx{}); err != nil {
		logrus.Panicf("Error registering portworx volume driver: %v", err)
	}
}
