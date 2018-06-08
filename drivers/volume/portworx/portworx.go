package portworx

import (
	"encoding/csv"
	"fmt"
	"math"
	"regexp"
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
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	k8shelper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
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

	// for cloud snaps, we use 5 * maxint32 seconds as timeout since we cannot
	// accurately predict a correct timeout
	cloudSnapshotInitialDelay = 5 * time.Second
	cloudSnapshotFactor       = 1
	cloudSnapshotSteps        = math.MaxInt32
)

const (
	cloudSnapStatusDone    = "Done"
	cloudSnapStatusPending = "Pending"
	cloudSnapStatusFailed  = "Failed"
)

type cloudSnapStatus struct {
	status      string
	msg         string
	cloudSnapID string
}

// snapshot annotation constants
const (
	pxAnnotationSelectorKeyPrefix = "portworx.selector/"
	pxSnapshotNamespaceIDKey      = pxAnnotationSelectorKeyPrefix + "namespace"
	pxSnapshotGroupIDKey          = pxAnnotationSelectorKeyPrefix + "group-id"
	pxAnnotationKeyPrefix         = "portworx/"
	pxSnapshotTypeKey             = pxAnnotationKeyPrefix + "snapshot-type"
	pxCloudSnapshotCredsIDKey     = pxAnnotationKeyPrefix + "cloud-cred-id"
)

var pxGroupSnapSelectorRegex = regexp.MustCompile(`portworx\.selector/(.+)`)
var pre14VersionRegex = regexp.MustCompile(`1\.2\..+|1\.3\..+`)

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
	store          cache.Store
	stopChannel    chan struct{}
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

	p.stopChannel = make(chan struct{})
	err = p.startNodeCache()
	return err
}

func (p *portworx) Stop() error {
	close(p.stopChannel)
	return nil
}

func (p *portworx) startNodeCache() error {
	resyncPeriod := 30 * time.Second

	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("Error getting cluster config: %v", err)
	}

	k8sClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("Error getting client, %v", err)
	}

	restClient := k8sClient.Core().RESTClient()

	watchlist := cache.NewListWatchFromClient(restClient, "nodes", v1.NamespaceAll, fields.Everything())
	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return watchlist.List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return watchlist.Watch(options)
		},
	}
	store, controller := cache.NewInformer(lw, &v1.Node{}, resyncPeriod,
		cache.ResourceEventHandlerFuncs{},
	)
	p.store = store

	go controller.Run(p.stopChannel)
	return nil
}

func (p *portworx) InspectVolume(volumeID string) (*storkvolume.Info, error) {
	vols, err := p.volDriver.Inspect([]string{volumeID})
	if err != nil {
		return nil, &ErrFailedToInspectVolume{
			ID:    volumeID,
			Cause: fmt.Sprintf("Volume inspect returned err: %v", err),
		}
	}

	if len(vols) != 1 {
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

	if len(vols[0].Locator.GetVolumeLabels()) > 0 {
		info.Labels = vols[0].Locator.GetVolumeLabels()
	} else {
		info.Labels = make(map[string]string, 0)
	}

	for k, v := range vols[0].Spec.GetVolumeLabels() {
		info.Labels[k] = v
	}

	info.VolumeSourceRef = vols[0]
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

func (p *portworx) getNodeLabels(hostname string) (map[string]string, error) {
	obj, exists, err := p.store.GetByKey(hostname)
	if err != nil {
		return nil, err
	} else if !exists {
		return nil, fmt.Errorf("Node %v not found in cache", hostname)
	}
	node := obj.(*v1.Node)
	return node.Labels, nil
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

		labels, err := p.getNodeLabels(nodeInfo.Hostname)
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

func (p *portworx) isPortworxPVC(pvc *v1.PersistentVolumeClaim) bool {
	storageClassName := k8shelper.GetPersistentVolumeClaimClass(pvc)
	if storageClassName == "" {
		logrus.Debugf("Empty StorageClass in PVC %v", pvc.Name)
		return false
	}

	provisioner := ""
	// Check for the provisioner in the PVC annotation. If not populated
	// try getting the provisioner from the Storage class.
	if val, ok := pvc.Annotations[pvcProvisionerAnnotation]; ok {
		provisioner = val
	} else {
		storageClass, err := k8s.Instance().GetStorageClass(storageClassName)
		if err != nil {
			logrus.Errorf("Error getting storageclass for storageclass %v in pvc %v: %v", storageClassName, pvc.Name, err)
			return false
		}
		provisioner = storageClass.Provisioner
	}

	if provisioner != provisionerName && provisioner != snapshotcontroller.GetProvisionerName() {
		logrus.Debugf("Provisioner in Storageclass not Portworx or from the snapshot Provisioner: %v", provisioner)
		return false
	}
	return true
}

func (p *portworx) GetPodVolumes(podSpec *v1.PodSpec, namespace string) ([]*storkvolume.Info, error) {
	var volumes []*storkvolume.Info
	for _, volume := range podSpec.Volumes {
		volumeName := ""
		if volume.PersistentVolumeClaim != nil {
			pvc, err := k8s.Instance().GetPersistentVolumeClaim(
				volume.PersistentVolumeClaim.ClaimName,
				namespace)
			if err != nil {
				return nil, err
			}

			if !p.isPortworxPVC(pvc) {
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

func (p *portworx) GetVolumeClaimTemplates(templates []v1.PersistentVolumeClaim) (
	[]v1.PersistentVolumeClaim, error) {
	var pxTemplates []v1.PersistentVolumeClaim
	for _, t := range templates {
		if p.isPortworxPVC(&t) {
			pxTemplates = append(pxTemplates, t)
		}
	}
	return pxTemplates, nil
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
		ok, msg, err := p.ensureNodesDontMatchVersionPrefix(pre14VersionRegex)
		if err != nil {
			return nil, nil, err
		}

		if !ok {
			err = fmt.Errorf("cloud snapshot is supported only on PX version 1.4 and above. %s", msg)
			return nil, getErrorSnapshotConditions(err), err
		}

		snapshotCredID = getCredIDFromSnapshot(snap)
		request := &api.CloudBackupCreateRequest{
			VolumeID:       volumeID,
			CredentialUUID: snapshotCredID,
		}
		err = p.volDriver.CloudBackupCreate(request)
		if err != nil {
			return nil, getErrorSnapshotConditions(err), err
		}

		status, err := p.waitForCloudSnapCompletion(api.CloudBackupOp, volumeID, false)
		if err != nil {
			logrus.Errorf("Cloudsnap backup: %s failed due to: %v", status.cloudSnapID, err)
			return nil, getErrorSnapshotConditions(err), err
		}

		snapshotID = status.cloudSnapID
		logrus.Infof("Cloudsnap backup: %s of vol: %s created successfully.", snapshotID, volumeID)
		snapStatusConditions = getReadySnapshotConditions()
	case crdv1.PortworxSnapshotTypeLocal:
		if isGroupSnap(snap) {
			ok, msg, err := p.ensureNodesDontMatchVersionPrefix(pre14VersionRegex)
			if err != nil {
				return nil, nil, err
			}

			if !ok {
				err = fmt.Errorf("group snapshot is supported only on PX version 1.4 and above. %s", msg)
				return nil, getErrorSnapshotConditions(err), err
			}

			groupID := snap.Metadata.Annotations[pxSnapshotGroupIDKey]
			groupLabels := parseGroupLabelsFromAnnotations(snap.Metadata.Annotations)

			err = p.validatePVForGroupSnap(pv.GetName(), groupID, groupLabels)
			if err != nil {
				return nil, getErrorSnapshotConditions(err), err
			}

			infoStr := fmt.Sprintf("Creating group snapshot: [%s] %s", snap.Metadata.Namespace, snap.Metadata.Name)
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

			snapDataNames := make([]string, 0)
			failedSnapVols := make([]string, 0)
			snapIDs := make([]string, 0)

			// Loop through the response and check if all succeeded. Don't create any k8s objects if
			// any of the snapshots failed
			for volID, newSnapResp := range resp.Snapshots {
				if newSnapResp.GetVolumeCreateResponse() == nil {
					err = fmt.Errorf("Portworx API returned empty response on creation of group snapshot")
					return nil, getErrorSnapshotConditions(err), err
				}

				if newSnapResp.GetVolumeCreateResponse().GetVolumeResponse() != nil &&
					len(newSnapResp.GetVolumeCreateResponse().GetVolumeResponse().GetError()) != 0 {
					logrus.Errorf("failed to create snapshot for volume: %s due to: %v",
						volID, newSnapResp.GetVolumeCreateResponse().GetVolumeResponse().GetError())
					failedSnapVols = append(failedSnapVols, volID)
					continue
				}

				snapIDs = append(snapIDs, newSnapResp.GetVolumeCreateResponse().GetId())
			}

			if len(failedSnapVols) > 0 {
				p.revertPXSnaps(snapIDs)

				err = fmt.Errorf("all snapshots in the group did not succeed. failed: %v succeeded: %v", failedSnapVols, snapIDs)
				return nil, getErrorSnapshotConditions(err), err
			}

			createdSnapObjs := make([]*crdv1.VolumeSnapshot, 0)
			for _, newSnapResp := range resp.Snapshots {
				newSnapID := newSnapResp.GetVolumeCreateResponse().GetId()
				snapObj, err := p.createVolumeSnapshotCRD(newSnapID, newSnapResp, groupID, groupLabels, snap)
				if err != nil {
					p.revertPXSnaps(snapIDs)
					p.revertSnapObjs(createdSnapObjs)

					err = fmt.Errorf("failed to create VolumeSnapshot object for PX snapshot: %s due to: %v", newSnapID, err)
					return nil, getErrorSnapshotConditions(err), err
				}

				createdSnapObjs = append(createdSnapObjs, snapObj)
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
		return fmt.Errorf("Invalid snapshot source %v", snapDataSrc)
	}

	switch snapDataSrc.PortworxSnapshot.SnapshotType {
	case crdv1.PortworxSnapshotTypeCloud:
		input := &api.CloudBackupDeleteRequest{
			ID:             snapDataSrc.PortworxSnapshot.SnapshotID,
			CredentialUUID: snapDataSrc.PortworxSnapshot.SnapshotCloudCredID,
		}
		return p.volDriver.CloudBackupDelete(input)
	default:
		if len(snapDataSrc.PortworxSnapshot.SnapshotData) > 0 {
			r := csv.NewReader(strings.NewReader(snapDataSrc.PortworxSnapshot.SnapshotData))
			snapDataNames, err := r.Read()
			if err != nil {
				logrus.Errorf("Failed to parse snap data csv. err: %v", err)
				return err
			}

			if len(snapDataNames) > 0 {
				var lastError error
				for _, snapDataName := range snapDataNames {
					snapData, err := k8s.Instance().GetSnapshotData(snapDataName)
					if err != nil {
						lastError = err
						logrus.Errorf("Failed to get volume snapshot data: %s due to err: %v", snapDataName, err)
						continue
					}

					snapNamespace := snapData.Spec.VolumeSnapshotRef.Namespace
					snapName := snapData.Spec.VolumeSnapshotRef.Name

					logrus.Infof("Deleting VolumeSnapshot:[%s] %s", snapNamespace, snapName)
					err = wait.ExponentialBackoff(snapAPICallBackoff, func() (bool, error) {
						err = k8s.Instance().DeleteSnapshot(snapName, snapNamespace)
						if err != nil {
							return false, nil
						}

						return true, nil
					})
					if err != nil {
						lastError = err
						logrus.Errorf("Failed to get volume snapshot: [%s] %s due to err: %v", snapNamespace, snapName, err)
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
	_ string,
	parameters map[string]string,
) (*v1.PersistentVolumeSource, map[string]string, error) {
	if snapshotData == nil || snapshotData.Spec.PortworxSnapshot == nil {
		return nil, nil, fmt.Errorf("Invalid Snapshot spec")
	}
	if pvc == nil {
		return nil, nil, fmt.Errorf("Invalid PVC spec")
	}

	logrus.Debugf("SnapshotRestore for pvc: %+v", pvc)

	snapshotName, present := pvc.ObjectMeta.Annotations[crdclient.SnapshotPVCAnnotation]
	if !present || len(snapshotName) == 0 {
		return nil, nil, fmt.Errorf("source volumesnapshot name not present in PVC annotations")
	}

	// Let's verify if source snapshot is complete
	_, isCompleted, err := p.DescribeSnapshot(snapshotData)
	if err != nil {
		return nil, nil, fmt.Errorf("snapshot: %s may not be complete. %v", snapshotName, err)
	}

	if !isCompleted {
		return nil, nil, fmt.Errorf("snapshot: %s is not yet completed", snapshotName)
	}

	snapID := snapshotData.Spec.PortworxSnapshot.SnapshotID
	restoreVolumeName := "pvc-" + string(pvc.UID)
	var restoredVolumeID string

	switch snapshotData.Spec.PortworxSnapshot.SnapshotType {
	case crdv1.PortworxSnapshotTypeLocal:
		snapshotNamespace, ok := pvc.Annotations[snapshotcontroller.StorkSnapshotSourceNamespaceAnnotation]
		if !ok {
			snapshotNamespace = pvc.GetNamespace()
		}

		// Check if this is a group snapshot
		snap, err := k8s.Instance().GetSnapshot(snapshotName, snapshotNamespace)
		if err != nil {
			return nil, nil, err
		}

		if isGroupSnap(snap) {
			return nil, nil, fmt.Errorf("volumesnapshot: [%s] %s was used to create group snapshots. "+
				"To restore, use the volumesnapshots that were created by this group snapshot.", snapshotNamespace, snapshotName)
		}

		locator := &api.VolumeLocator{
			Name: restoreVolumeName,
			VolumeLabels: map[string]string{
				pvcNameLabel:   pvc.Name,
				namespaceLabel: pvc.Namespace,
			},
		}
		restoredVolumeID, err = p.volDriver.Snapshot(snapID, false, locator)
		if err != nil {
			return nil, nil, err
		}
	case crdv1.PortworxSnapshotTypeCloud:
		response, err := p.volDriver.CloudBackupRestore(&api.CloudBackupRestoreRequest{
			ID:                snapID,
			RestoreVolumeName: restoreVolumeName,
			CredentialUUID:    snapshotData.Spec.PortworxSnapshot.SnapshotCloudCredID,
		})
		if err != nil {
			return nil, nil, err
		}

		restoredVolumeID = response.RestoreVolumeID
		logrus.Infof("Cloudsnap restore of %s to %s started successfully.", snapID, restoredVolumeID)

		_, err = p.waitForCloudSnapCompletion(api.CloudRestoreOp, restoredVolumeID, true)
		if err != nil {
			return nil, nil, err
		}
	}

	// create PV from restored volume
	vols, err := p.volDriver.Inspect([]string{restoredVolumeID})
	if err != nil {
		return nil, nil, &ErrFailedToInspectVolume{
			ID:    restoredVolumeID,
			Cause: fmt.Sprintf("Volume inspect returned err: %v", err),
		}
	}

	if len(vols) == 0 {
		return nil, nil, &errors.ErrNotFound{
			ID:   restoredVolumeID,
			Type: "Volume",
		}
	}

	pv := &v1.PersistentVolumeSource{
		PortworxVolume: &v1.PortworxVolumeSource{
			VolumeID: restoredVolumeID,
			FSType:   vols[0].Format.String(),
			ReadOnly: vols[0].Readonly,
		},
	}

	labels := make(map[string]string)
	return pv, labels, nil
}

func (p *portworx) DescribeSnapshot(snapshotData *crdv1.VolumeSnapshotData) (*[]crdv1.VolumeSnapshotCondition, bool /* isCompleted */, error) {
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

		csStatus := p.checkCloudSnapStatus(api.CloudBackupOp, pv.Spec.PortworxVolume.VolumeID)
		if csStatus.status == cloudSnapStatusFailed {
			err = fmt.Errorf(csStatus.msg)
			return getErrorSnapshotConditions(err), false, err
		}

		if csStatus.status == cloudSnapStatusPending {
			pendingCond := getPendingSnapshotConditions(csStatus.msg)
			return &pendingCond, false, nil
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
	parentPVCOrVolID, err := p.findParentPVCOrVolID(pxSnapID)
	if err != nil {
		return nil, err
	}

	namespace := groupSnap.Metadata.Namespace
	volumeSnapshotName := fmt.Sprintf("%s-%s-%s", groupSnap.Metadata.Name, parentPVCOrVolID, groupSnap.GetObjectMeta().GetUID())
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
			SnapshotDataName: snapData.Metadata.Name,
			// this is best effort as can be vol ID if PVC is deleted
			PersistentVolumeClaimName: parentPVCOrVolID,
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
	if err != nil {
		// revert snapdata
		deleteErr := wait.ExponentialBackoff(snapAPICallBackoff, func() (bool, error) {
			deleteErr := k8s.Instance().DeleteSnapshotData(snapData.Metadata.Name)
			if err != nil {
				logrus.Errorf("failed to delete volumesnapshotdata due to err: %v", deleteErr)
				return false, nil
			}

			return true, nil
		})
		if deleteErr != nil {
			logrus.Errorf("failed to revert volumesnapshotdata due to: %v", deleteErr)
		}

		return nil, err
	}

	return snap, nil
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
		err = fmt.Errorf("error creating the VolumeSnapshotData for snap %s due to err: %v", snapshotName, err)
		logrus.Println(err)
		return nil, err
	}

	return result, nil
}

// findParentPVCOrVolID is a best effort routine that attempts to find parent PVC name for
// given snapshot ID. If not found, it returns the Parent volumeID
func (p *portworx) findParentPVCOrVolID(snapID string) (string, error) {
	snapInfo, err := p.InspectVolume(snapID)
	if err != nil {
		return "", err
	}

	if len(snapInfo.ParentID) == 0 {
		return "", fmt.Errorf("Portworx snapshot: %s does not have parent set", snapID)
	}

	snapParentInfo, err := p.InspectVolume(snapInfo.ParentID)
	if err != nil {
		logrus.Warnf("Parent volume: %s for snapshot: %s is not found due to: %v", snapInfo.ParentID, snapID, err)
		return snapInfo.ParentID, nil
	}

	parentPV, err := k8s.Instance().GetPersistentVolume(snapParentInfo.VolumeName)
	if err != nil {
		logrus.Warnf("Parent PV: %s of snapshot: %s is not found due to: %v", snapParentInfo.VolumeName, snapID, err)
		return snapInfo.ParentID, nil
	}

	pvc, err := k8s.Instance().GetPersistentVolumeClaim(parentPV.Spec.ClaimRef.Name, parentPV.Spec.ClaimRef.Namespace)
	if err != nil {
		return snapInfo.ParentID, nil
	}

	return pvc.GetName(), nil
}

func (p *portworx) waitForCloudSnapCompletion(op api.CloudBackupOpType, volID string, verbose bool) (cloudSnapStatus, error) {
	csStatus := cloudSnapStatus{
		status: cloudSnapStatusFailed,
		msg:    fmt.Sprintf("cloudsnap status unknown"),
	}

	err := wait.ExponentialBackoff(cloudsnapBackoff, func() (bool, error) {
		csStatus = p.checkCloudSnapStatus(op, volID)
		switch csStatus.status {
		case cloudSnapStatusFailed:
			err := fmt.Errorf("Cloudsnap %s of %s failed due to: %s", op, volID, csStatus.msg)
			if verbose {
				logrus.Errorf(err.Error())
			}
			return true, err
		case cloudSnapStatusDone:
			if verbose {
				logrus.Infof("Cloudsnap %s of %s completed successfully", op, volID)
			}
			return true, nil
		case cloudSnapStatusPending:
			if verbose {
				logrus.Infof("Cloudsnap %s of %s is still pending. %s", op, volID, csStatus.msg)
			}
			return false, nil
		default:
			err := fmt.Errorf("received unexpected status for cloudsnap %s of %s. status: %s", op, volID, csStatus.status)
			return false, err
		}
	})

	return csStatus, err
}

func (p *portworx) checkCloudSnapStatus(op api.CloudBackupOpType, volID string) cloudSnapStatus {
	response, err := p.volDriver.CloudBackupStatus(&api.CloudBackupStatusRequest{
		SrcVolumeID: volID,
	})
	if err != nil {
		return cloudSnapStatus{
			status: cloudSnapStatusFailed,
			msg:    err.Error(),
		}
	}

	csStatus, present := response.Statuses[volID]
	if !present {
		return cloudSnapStatus{
			status: cloudSnapStatusFailed,
			msg:    fmt.Sprintf("failed to get cloudsnap status for volume: %s", volID),
		}
	}

	statusStr := getCloudSnapStatusString(&csStatus)

	if csStatus.Status == cloudSnapStatusFailed {
		return cloudSnapStatus{
			status:      cloudSnapStatusFailed,
			cloudSnapID: csStatus.ID,
			msg:         fmt.Sprintf("cloudsnap %s for %s failed. %s", op, volID, statusStr),
		}
	}

	if csStatus.CompletedTime.IsZero() {
		return cloudSnapStatus{
			status:      cloudSnapStatusPending,
			cloudSnapID: csStatus.ID,
			msg:         fmt.Sprintf("cloudsnap %s for %s still not completed. %s", op, volID, statusStr),
		}
	}

	if !strings.Contains(csStatus.Status, cloudSnapStatusDone) {
		return cloudSnapStatus{
			status:      cloudSnapStatusPending,
			cloudSnapID: csStatus.ID,
			msg:         fmt.Sprintf("cloudsnap %s for %s still not done. %s", op, volID, statusStr),
		}
	}

	return cloudSnapStatus{
		status:      cloudSnapStatusDone,
		cloudSnapID: csStatus.ID,
		msg:         fmt.Sprintf("cloudsnap %s for %s done.", op, volID),
	}
}

// revertPXSnaps deletes all given snapIDs
func (p *portworx) revertPXSnaps(snapIDs []string) {
	failedDeletions := make(map[string]error, 0)
	for _, id := range snapIDs {
		err := p.volDriver.Delete(id)
		if err != nil {
			failedDeletions[id] = err
		}
	}

	if len(failedDeletions) > 0 {
		errString := ""
		for failedID, failedErr := range failedDeletions {
			errString = fmt.Sprintf("%s delete of %s failed due to err: %v.\n", errString, failedID, failedErr)
		}

		logrus.Errorf("failed to revert created PX snapshots. err: %s", errString)
		return
	}

	logrus.Infof("successfully reverted PX snapshots")
	return
}

func (p *portworx) revertSnapObjs(snapObjs []*crdv1.VolumeSnapshot) {
	failedDeletions := make(map[string]error, 0)

	for _, snap := range snapObjs {
		err := wait.ExponentialBackoff(snapAPICallBackoff, func() (bool, error) {
			deleteErr := k8s.Instance().DeleteSnapshot(snap.Metadata.Name, snap.Metadata.Namespace)
			if deleteErr != nil {
				logrus.Infof("failed to delete volumesnapshot due to: %v", deleteErr)
				return false, nil
			}

			return true, nil
		})
		if err != nil {
			failedDeletions[fmt.Sprintf("[%s] %s", snap.Metadata.Namespace, snap.Metadata.Name)] = err
		}
	}

	if len(failedDeletions) > 0 {
		errString := ""
		for failedID, failedErr := range failedDeletions {
			errString = fmt.Sprintf("%s delete of %s failed due to err: %v.\n", errString, failedID, failedErr)
		}

		logrus.Errorf("failed to revert created volumesnapshots. err: %s", errString)
		return
	}

	logrus.Infof("successfully reverted volumesnapshots")
}

func (p *portworx) validatePVForGroupSnap(pvName, groupID string, groupLabels map[string]string) error {
	volInfo, err := p.InspectVolume(pvName)
	if err != nil {
		return err
	}

	if volInfo.VolumeSourceRef == nil {
		return fmt.Errorf("inspect of volume: %s did not set the volume source ref", pvName)
	}

	pxSource, ok := volInfo.VolumeSourceRef.(*api.Volume)
	if !ok {
		return fmt.Errorf("source for volume: %s is not a Portworx volume", pvName)
	}

	// validate group ID
	if len(groupID) > 0 {
		if pxSource.GetGroup() == nil {
			return fmt.Errorf("group ID for volume: %s is not set. expected: %s", pvName, groupID)
		}

		if pxSource.GetGroup().GetId() != groupID {
			return fmt.Errorf("group ID for volume: %s does not match. expected: %s actual: %s",
				pvName, groupID, pxSource.GetGroup().GetId())
		}
	}

	// validate label selectors
	for k, v := range groupLabels {
		if value, present := volInfo.Labels[k]; !present || value != v {
			return fmt.Errorf("annotation/label '%s:%s' is not present in volume: %v. Found: %v",
				k, v, pvName, volInfo.Labels)
		}
	}

	return nil
}

func (p *portworx) ensureNodesDontMatchVersionPrefix(versionRegex *regexp.Regexp) (bool, string, error) {
	result, err := p.clusterManager.Enumerate()
	if err != nil {
		return false, "", err
	}

	for _, node := range result.Nodes {
		version := node.NodeLabels["PX Version"]
		if versionRegex.MatchString(version) {
			return false, fmt.Sprintf("node: %s has version: %s", node.Hostname, version), nil
		}
	}

	return true, "all nodes have expected version", nil
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
		// skip group id annotation
		if k == pxSnapshotGroupIDKey {
			continue
		}

		matches := pxGroupSnapSelectorRegex.FindStringSubmatch(k)
		if len(matches) == 2 {
			groupLabels[matches[1]] = v
		}
	}

	return groupLabels
}

func isGroupSnap(snap *crdv1.VolumeSnapshot) bool {
	for k := range snap.Metadata.Annotations {
		matches := pxGroupSnapSelectorRegex.FindStringSubmatch(k)
		if len(matches) == 2 {
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

func getPendingSnapshotConditions(msg string) []crdv1.VolumeSnapshotCondition {
	return []crdv1.VolumeSnapshotCondition{
		{
			Type:               crdv1.VolumeSnapshotConditionPending,
			Status:             v1.ConditionUnknown,
			Message:            msg,
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
