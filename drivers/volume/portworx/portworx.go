package portworx

import (
	"context"
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	version "github.com/hashicorp/go-version"
	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	crdclient "github.com/kubernetes-incubator/external-storage/snapshot/pkg/client"
	"github.com/kubernetes-incubator/external-storage/snapshot/pkg/controller/snapshotter"
	snapshotVolume "github.com/kubernetes-incubator/external-storage/snapshot/pkg/volume"
	"github.com/libopenstorage/openstorage/api"
	apiclient "github.com/libopenstorage/openstorage/api/client"
	clusterclient "github.com/libopenstorage/openstorage/api/client/cluster"
	volumeclient "github.com/libopenstorage/openstorage/api/client/volume"
	ost_errors "github.com/libopenstorage/openstorage/api/errors"
	"github.com/libopenstorage/openstorage/cluster"
	"github.com/libopenstorage/openstorage/pkg/auth"
	auth_secrets "github.com/libopenstorage/openstorage/pkg/auth/secrets"
	"github.com/libopenstorage/openstorage/pkg/grpcserver"
	"github.com/libopenstorage/openstorage/volume"
	"github.com/libopenstorage/openstorage/volume/drivers/pwx"
	lsecrets "github.com/libopenstorage/secrets"
	k8s_secrets "github.com/libopenstorage/secrets/k8s"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	applicationcontrollers "github.com/libopenstorage/stork/pkg/applicationmanager/controllers"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/libopenstorage/stork/pkg/snapshot"
	snapshotcontrollers "github.com/libopenstorage/stork/pkg/snapshot/controllers"
	"github.com/portworx/sched-ops/k8s/core"
	k8sextops "github.com/portworx/sched-ops/k8s/externalstorage"
	"github.com/portworx/sched-ops/k8s/storage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	k8shelper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

// TODO: Make some of these configurable
const (
	// driverName is the name of the portworx driver implementation
	driverName = "pxd"

	// default API port
	defaultAPIPort = 9001

	// provisioner names for portworx volumes
	provisionerName = "kubernetes.io/portworx-volume"

	// pvcProvisionerAnnotation is the annotation on PVC which has the provisioner name
	pvcProvisionerAnnotation = "volume.beta.kubernetes.io/storage-provisioner"
	// pvProvisionedByAnnotation is the annotation on PV which has the provisioner name
	pvProvisionedByAnnotation = "pv.kubernetes.io/provisioned-by"

	// pvcNameLabel is the key of the label used to store the PVC name
	pvcNameLabel = "pvc"

	// storkSnapNameLabel is the key of the label used to store the stork Snapshot name
	storkSnapNameLabel = "stork-snap"

	// namespaceLabel is the key of the label used to store the namespace of PVC
	// and snapshots
	namespaceLabel = "namespace"

	// pxRackLabelKey Label for rack information
	pxRackLabelKey = "px/rack"
	// px annotation for secret name
	pxSecretNameAnnotation = "px/secret-name"
	// px annotation for secret namespace
	pxSecretNamespaceAnnotation = "px/secret-namespace"

	// auth issuer to use for px 2.6.0 and higher
	pxJwtIssuerApps = "apps.portworx.io"
	// [deprecated] auth issuer to use for below px 2.6.0
	pxJwtIssuerStork = "stork.openstorage.io"

	snapshotDataNamePrefix = "k8s-volume-snapshot"
	readySnapshotMsg       = "Snapshot created successfully and it is ready"
	pvNamePrefix           = "pvc-"

	// volumeSnapshot* is configuration of exponential backoff for
	// waiting for snapshot operation to complete. Starting with 2
	// seconds, multiplying by 1.5 with each step and taking 20 steps at maximum.
	// It will time out after 20 steps (6650 seconds).
	volumeSnapshotInitialDelay = 2 * time.Second
	volumeSnapshotFactor       = 1.5
	volumeSnapshotSteps        = 20

	// for cloud snaps, we use 5 * maxint32 seconds as timeout since we cannot
	// accurately predict a correct timeout
	cloudSnapshotInitialDelay       = 5 * time.Second
	cloudSnapshotFactor             = 1
	cloudSnapshotSteps              = math.MaxInt32
	cloudBackupOwnerLabel           = "owner"
	cloudBackupExternalManagerLabel = "externalManager"

	// volumesnapshotRestore, since volume detach can take time even though pod is deleted
	// we retry restore for 5 times with 5second delay
	volumeRestoreInitialDelay = 5 * time.Second
	volumeRestoreFactor       = 1
	volumeRestoreSteps        = 5

	// cloudBackupCreateAPI, if fails, retry with 5 sec delay for 10 times
	cloudBackupCreateInitialDelay = 5 * time.Second
	cloudBackupCreateFactor       = 1
	cloudBackupCreateSteps        = 10

	// volumesnapshotRestoreState, since volume restore is async & take time to complete
	// we check restore completion for 30 times with 10 second delay
	volumeRestoreStateInitialDelay = 10 * time.Second
	volumeRestoreStateFactor       = 1
	volumeRestoreStateSteps        = 30

	validateSnapshotTimeout       = 5 * time.Minute
	validateSnapshotRetryInterval = 10 * time.Second

	clusterDomainsTimeout = 1 * time.Minute
	cloudBackupTimeout    = 1 * time.Minute

	pxSharedSecret = "PX_SHARED_SECRET"
	pxJwtIssuer    = "PX_JWT_ISSUER"

	restoreNamePrefix = "in-place-restore-"
	restoreTaskPrefix = "restore-"

	// Annotation to skip checking if the backup is being done to the same
	// BackupLocationName
	skipBackupLocationNameCheckAnnotation = "portworx.io/skip-backup-location-name-check"
	incrementalCountAnnotation            = "portworx.io/cloudsnap-incremental-count"
	deleteLocalSnapAnnotation             = "portworx.io/cloudsnap-delete-local-snap"
)

type cloudSnapStatus struct {
	sourceVolumeID string
	terminal       bool
	status         api.CloudBackupStatusType
	msg            string
	cloudSnapID    string
	bytesTotal     uint64
	bytesDone      uint64
	etaSeconds     int64
	credentialID   string
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

var pxGroupSnapSelectorRegex = regexp.MustCompile(`^portworx\.selector/(.+)`)

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

var restoreAPICallBackoff = wait.Backoff{
	Duration: volumeRestoreInitialDelay,
	Factor:   volumeRestoreFactor,
	Steps:    volumeRestoreSteps,
}

var cloudBackupCreateBackoff = wait.Backoff{
	Duration: cloudBackupCreateInitialDelay,
	Factor:   cloudBackupCreateFactor,
	Steps:    cloudBackupCreateSteps,
}

var restoreStateCallBackoff = wait.Backoff{
	Duration: volumeRestoreStateInitialDelay,
	Factor:   volumeRestoreStateFactor,
	Steps:    volumeRestoreStateSteps,
}

type portworx struct {
	store           cache.Store
	stopChannel     chan struct{}
	sdkConn         *portworxGrpcConnection
	id              string
	endpoint        string
	jwtSharedSecret string
	jwtIssuer       string
	initDone        bool
}

type portworxGrpcConnection struct {
	conn        *grpc.ClientConn
	dialOptions []grpc.DialOption
	endpoint    string
}

func (p *portworx) String() string {
	return driverName
}

func (p *portworx) Init(_ interface{}) error {
	p.stopChannel = make(chan struct{})
	switch os.Getenv(pxJwtIssuer) {
	case pxJwtIssuerStork:
		p.jwtIssuer = pxJwtIssuerStork
	case pxJwtIssuerApps:
		p.jwtIssuer = pxJwtIssuerApps
	case "":
		// default to stork issuer for older versions of PX and backwards compatibility
		p.jwtIssuer = pxJwtIssuerStork
	default:
		return fmt.Errorf("invalid jwt issuer to authenticate with Portworx")
	}

	return p.startNodeCache()
}

func (p *portworx) Stop() error {
	close(p.stopChannel)
	return nil
}

func (pg *portworxGrpcConnection) getGrpcConn() (*grpc.ClientConn, error) {

	if pg.conn == nil {
		var err error
		pg.conn, err = grpcserver.Connect(pg.endpoint, pg.dialOptions)
		if err != nil {
			return nil, fmt.Errorf("error connecting to GRPC server[%s]: %v", pg.endpoint, err)
		}
	}
	return pg.conn, nil
}

func (p *portworx) getClusterDomainClient() (api.OpenStorageClusterDomainsClient, error) {
	conn, err := p.sdkConn.getGrpcConn()
	if err != nil {
		return nil, err
	}
	return api.NewOpenStorageClusterDomainsClient(conn), nil
}

func (p *portworx) getCloudBackupClient() (api.OpenStorageCloudBackupClient, error) {
	conn, err := p.sdkConn.getGrpcConn()
	if err != nil {
		return nil, err
	}
	return api.NewOpenStorageCloudBackupClient(conn), nil
}

// getClusterManagerClient returns cluster manager client with proper configuration and updated authorization token
// if authorization is enabled
func (p *portworx) getClusterManagerClient() (cluster.Cluster, error) {
	token, err := p.tokenGenerator()
	if err != nil {
		return nil, err
	}

	clnt, err := clusterclient.NewAuthClusterClient(p.endpoint, "v1", token, "")
	if err != nil {
		return nil, err
	}

	return clusterclient.ClusterManager(clnt), nil
}

func (p *portworx) initPortworxClients() error {
	kubeOps := core.Instance()

	pbc := pwx.NewConnectionParamsBuilderDefaultConfig()

	p.jwtSharedSecret = os.Getenv(pxSharedSecret)
	if len(p.jwtSharedSecret) > 0 {
		pbc.AuthTokenGenerator = p.tokenGenerator
		pbc.AuthEnabled = true
	}

	paramsBuilder, err := pwx.NewConnectionParamsBuilder(kubeOps, pbc)
	if err != nil {
		return err
	}

	pxMgmtEndpoint, sdkEndpoint, err := paramsBuilder.BuildClientsEndpoints()
	if err != nil {
		return err
	}

	sdkDialOps, err := paramsBuilder.BuildDialOps()
	if err != nil {
		return err
	}

	p.endpoint = pxMgmtEndpoint

	// Setup gRPC clients
	p.sdkConn = &portworxGrpcConnection{
		endpoint:    sdkEndpoint,
		dialOptions: sdkDialOps,
	}

	// Setup secrets instance
	k8sSecrets, err := k8s_secrets.New(nil)
	if err != nil {
		return fmt.Errorf("failed to initialize secrets provider: %v", err)
	}
	err = lsecrets.SetInstance(k8sSecrets)
	if err != nil {
		return fmt.Errorf("failed to set secrets provider: %v", err)
	}

	// Create a unique identifier
	p.id, err = os.Hostname()
	if err != nil {
		return fmt.Errorf("unable to get hostname: %v", err)
	}

	p.initDone = true
	return err
}

// 	tokenGenerator generates authorization token for system.admin
//	when shared secret is not configured authz token is empty string
//	this let Openstorage API clients be bootstrapped with no authorization (by accepting empty token)
func (p *portworx) tokenGenerator() (string, error) {
	if len(p.jwtSharedSecret) == 0 {
		return "", nil
	}

	claims := &auth.Claims{
		Issuer: p.jwtIssuer,
		Name:   "Stork",

		// Unique id for stork
		// this id must be unique across all accounts accessing the px system
		Subject: p.jwtIssuer + "." + p.id,

		// Only allow certain calls
		Roles: []string{"system.admin"},

		// Be in all groups to have access to all resources
		Groups: []string{"*"},
	}

	// This never returns an error, but just in case, check the value
	signature, err := auth.NewSignatureSharedSecret(p.jwtSharedSecret)
	if err != nil {
		return "", err
	}

	// Set the token expiration
	options := &auth.Options{
		Expiration:  time.Now().Add(time.Hour * 1).Unix(),
		IATSubtract: 1 * time.Minute,
	}

	token, err := auth.Token(claims, signature, options)
	if err != nil {
		return "", err
	}

	return token, nil
}

func (p *portworx) startNodeCache() error {
	resyncPeriod := 30 * time.Second

	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error getting cluster config: %v", err)
	}

	k8sClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("error getting client, %v", err)
	}

	restClient := k8sClient.CoreV1().RESTClient()

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
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return nil, err
		}
	}

	volDriver, err := p.getAdminVolDriver()
	if err != nil {
		return nil, err
	}
	return p.inspectVolume(volDriver, volumeID)
}

func (p *portworx) inspectVolume(volDriver volume.VolumeDriver, volumeID string) (*storkvolume.Info, error) {
	vols, err := volDriver.Inspect([]string{volumeID})
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
		info.DataNodes = append(info.DataNodes, rset.Nodes...)
	}
	if vols[0].Source != nil {
		info.ParentID = vols[0].Source.Parent
	}

	if len(vols[0].Locator.GetVolumeLabels()) > 0 {
		info.Labels = vols[0].Locator.GetVolumeLabels()
	} else {
		info.Labels = make(map[string]string)
	}

	for k, v := range vols[0].Spec.GetVolumeLabels() {
		info.Labels[k] = v
	}

	for k, v := range vols[0].Locator.GetVolumeLabels() {
		info.Labels[k] = v
	}

	info.VolumeSourceRef = vols[0]
	return info, nil
}

func (p *portworx) mapNodeStatus(status api.Status) storkvolume.NodeStatus {
	switch status {
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

	case api.Status_STATUS_NONE:
		fallthrough
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

func (p *portworx) getNodeLabels(nodeInfo *storkvolume.NodeInfo) (map[string]string, error) {
	obj, exists, err := p.store.GetByKey(nodeInfo.SchedulerID)
	if err != nil {
		return nil, err
	} else if !exists {
		obj, exists, err = p.store.GetByKey(nodeInfo.StorageID)
		if err != nil {
			return nil, err
		} else if !exists {
			obj, exists, err = p.store.GetByKey(nodeInfo.Hostname)
			if err != nil {
				return nil, err
			} else if !exists {
				return nil, fmt.Errorf("node %v/%v/%v not found in cache", nodeInfo.StorageID, nodeInfo.SchedulerID, nodeInfo.Hostname)
			}
		}
	}
	node := obj.(*v1.Node)
	return node.Labels, nil
}

func (p *portworx) InspectNode(id string) (*storkvolume.NodeInfo, error) {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return nil, err
		}
	}

	if id == "" {
		return nil, fmt.Errorf("invalid node id")
	}

	clusterManager, err := p.getClusterManagerClient()
	if err != nil {
		return nil, fmt.Errorf("cannot get cluster manager, err: %s", err.Error())
	}

	node, err := clusterManager.Inspect(id)
	if err != nil {
		return nil, &ErrFailedToGetNodes{
			Cause: err.Error(),
		}
	}
	return &storkvolume.NodeInfo{
		StorageID:   node.Id,
		SchedulerID: node.SchedulerNodeName,
		Hostname:    strings.ToLower(node.Hostname),
		Status:      p.mapNodeStatus(node.Status),
		RawStatus:   node.Status.String(),
	}, nil
}

func (p *portworx) GetNodes() ([]*storkvolume.NodeInfo, error) {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return nil, err
		}
	}

	clusterManager, err := p.getClusterManagerClient()
	if err != nil {
		return nil, fmt.Errorf("cannot get cluster manager, err: %s", err.Error())
	}

	cluster, err := clusterManager.Enumerate()
	if err != nil {
		return nil, &ErrFailedToGetNodes{
			Cause: err.Error(),
		}
	}

	var nodes []*storkvolume.NodeInfo
	for _, n := range cluster.Nodes {
		nodeInfo := &storkvolume.NodeInfo{
			StorageID:   n.Id,
			SchedulerID: n.SchedulerNodeName,
			Hostname:    strings.ToLower(n.Hostname),
			Status:      p.mapNodeStatus(n.Status),
			RawStatus:   n.Status.String(),
		}
		nodeInfo.IPs = append(nodeInfo.IPs, n.MgmtIp)
		nodeInfo.IPs = append(nodeInfo.IPs, n.DataIp)

		labels, err := p.getNodeLabels(nodeInfo)
		if err == nil {
			if rack, ok := labels[pxRackLabelKey]; ok {
				nodeInfo.Rack = rack
			}
			if zone, ok := labels[v1.LabelZoneFailureDomain]; ok {
				nodeInfo.Zone = zone
			}
			if region, ok := labels[v1.LabelZoneRegion]; ok {
				nodeInfo.Region = region
			}
		}

		nodes = append(nodes, nodeInfo)
	}
	return nodes, nil
}

func (p *portworx) GetClusterID() (string, error) {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return "", err
		}
	}

	clusterManager, err := p.getClusterManagerClient()
	if err != nil {
		return "", fmt.Errorf("cannot get cluster manager, err: %s", err.Error())
	}

	cluster, err := clusterManager.Enumerate()
	if err != nil {
		return "", &ErrFailedToGetClusterID{
			Cause: err.Error(),
		}
	}
	if len(cluster.Id) == 0 {
		return "", &ErrFailedToGetClusterID{
			Cause: "Portworx driver returned empty cluster UUID",
		}
	}
	return cluster.Id, nil
}

func (p *portworx) OwnsPVC(coreOps core.Ops, pvc *v1.PersistentVolumeClaim) bool {

	provisioner := ""
	// Check for the provisioner in the PVC annotation. If not populated
	// try getting the provisioner from the Storage class.
	if val, ok := pvc.Annotations[pvcProvisionerAnnotation]; ok {
		provisioner = val
	} else {
		storageClassName := k8shelper.GetPersistentVolumeClaimClass(pvc)
		if storageClassName != "" {
			storageClass, err := storage.Instance().GetStorageClass(storageClassName)
			if err == nil {
				provisioner = storageClass.Provisioner
			} else {
				logrus.Warnf("Error getting storageclass %v for pvc %v: %v", storageClassName, pvc.Name, err)
			}
		}
	}

	if provisioner == "" {
		// Try to get info from the PV since storage class could be deleted
		pv, err := coreOps.GetPersistentVolume(pvc.Spec.VolumeName)
		if err != nil {
			logrus.Warnf("Error getting pv %v for pvc %v: %v", pvc.Spec.VolumeName, pvc.Name, err)
			return false
		}
		return p.OwnsPV(pv)
	}

	if provisioner != provisionerName &&
		!isCsiProvisioner(provisioner) &&
		provisioner != snapshot.GetProvisionerName() {
		logrus.Tracef("Provisioner in Storageclass not Portworx or from the snapshot Provisioner: %v", provisioner)
		return false
	}
	return true
}

func (p *portworx) OwnsPV(pv *v1.PersistentVolume) bool {
	var provisioner string
	// Check the annotation in the PV for the provisioner
	if val, ok := pv.Annotations[pvProvisionedByAnnotation]; ok {
		provisioner = val
	} else {
		// Finally check the volume reference in the spec
		if pv.Spec.PortworxVolume != nil {
			return true
		}
	}
	if provisioner != provisionerName &&
		!isCsiProvisioner(provisioner) &&
		provisioner != snapshot.GetProvisionerName() {
		logrus.Tracef("Provisioner in Storageclass not Portworx or from the snapshot Provisioner: %v", provisioner)
		return false
	}
	return true
}

func (p *portworx) GetPodVolumes(podSpec *v1.PodSpec, namespace string) ([]*storkvolume.Info, error) {
	var volumes []*storkvolume.Info
	for _, volume := range podSpec.Volumes {
		volumeName := ""
		var (
			pvc *v1.PersistentVolumeClaim
			err error
		)
		if volume.PersistentVolumeClaim != nil {
			pvc, err = core.Instance().GetPersistentVolumeClaim(
				volume.PersistentVolumeClaim.ClaimName,
				namespace)
			if err != nil {
				return nil, err
			}

			if !p.OwnsPVC(core.Instance(), pvc) {
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
				logrus.Warnf("Failed to inspect volume %v: %v", volumeName, err)
				// If the inspect volume fails return with atleast some info
				volumeInfo = &storkvolume.Info{
					VolumeName: volumeName,
				}
			}
			// Add the annotations as volume labels
			if len(volumeInfo.Labels) == 0 {
				volumeInfo.Labels = make(map[string]string)
			}
			for k, v := range pvc.ObjectMeta.Annotations {
				volumeInfo.Labels[k] = v
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
		if p.OwnsPVC(core.Instance(), &t) {
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

func (p *portworx) getUserContext(ctx context.Context, annotations map[string]string) (context.Context, error) {
	if v, ok := annotations[auth_secrets.SecretNameKey]; ok {
		token, err := auth_secrets.GetToken(&api.TokenSecretContext{
			SecretName:      v,
			SecretNamespace: annotations[auth_secrets.SecretNamespaceKey],
		})
		if err != nil {
			return nil, err
		}
		return grpcserver.AddMetadataToContext(ctx, "authorization", "bearer "+token), nil
	}
	return ctx, nil
}

func (p *portworx) secretRefFromAnnotations(annotations map[string]string) *v1.SecretReference {
	if name, ok := annotations[auth_secrets.SecretNameKey]; ok {
		if namespace, ok := annotations[auth_secrets.SecretNamespaceKey]; ok {
			return &v1.SecretReference{
				Name:      name,
				Namespace: namespace,
			}
		}
	}

	return nil
}

func (p *portworx) getUserVolDriver(annotations map[string]string) (volume.VolumeDriver, error) {
	if v, ok := annotations[auth_secrets.SecretNameKey]; ok {
		token, err := auth_secrets.GetToken(&api.TokenSecretContext{
			SecretName:      v,
			SecretNamespace: annotations[auth_secrets.SecretNamespaceKey],
		})
		if err != nil {
			return nil, err
		}
		clnt, err := p.getRestClientWithAuth(token)
		if err != nil {
			return nil, err
		}
		return volumeclient.VolumeDriver(clnt), nil
	}

	clnt, err := p.getRestClient()
	if err != nil {
		return nil, err
	}
	return volumeclient.VolumeDriver(clnt), nil

}

func (p *portworx) getAdminVolDriver() (volume.VolumeDriver, error) {
	if len(p.jwtSharedSecret) != 0 {
		claims := &auth.Claims{
			Issuer: p.jwtIssuer,
			Name:   "Stork",

			// Unique id for stork
			// this id must be unique across all accounts accessing the px system
			Subject: p.jwtIssuer + "." + p.id,

			// Only allow certain calls
			Roles: []string{"system.user"},

			// Be in all groups to have access to all resources
			Groups: []string{"*"},
		}

		// This never returns an error, but just in case, check the value
		signature, err := auth.NewSignatureSharedSecret(p.jwtSharedSecret)
		if err != nil {
			return nil, err
		}

		// Set the token expiration
		options := &auth.Options{
			Expiration:  time.Now().Add(time.Hour).Unix(),
			IATSubtract: 1 * time.Minute,
		}

		token, err := auth.Token(claims, signature, options)
		if err != nil {
			return nil, err
		}

		clnt, err := p.getRestClientWithAuth(token)
		if err != nil {
			return nil, err
		}
		return volumeclient.VolumeDriver(clnt), nil
	}

	clnt, err := p.getRestClient()
	if err != nil {
		return nil, err
	}
	return volumeclient.VolumeDriver(clnt), nil
}

func (p *portworx) getRestClientWithAuth(token string) (*apiclient.Client, error) {
	return volumeclient.NewAuthDriverClient(p.endpoint, driverName, "", token, "", "stork")
}

func (p *portworx) getRestClient() (*apiclient.Client, error) {
	return volumeclient.NewDriverClient(p.endpoint, driverName, "", "stork")
}

func (p *portworx) addCloudsnapInfo(
	request *api.CloudBackupCreateRequest,
	snap *crdv1.VolumeSnapshot,
) {
	if snap.Metadata.Annotations != nil {
		if scheduleName, exists := snap.Metadata.Annotations[snapshotcontrollers.SnapshotScheduleNameAnnotation]; exists {
			if policyType, exists := snap.Metadata.Annotations[snapshotcontrollers.SnapshotSchedulePolicyTypeAnnotation]; exists {
				request.Labels[cloudBackupExternalManagerLabel] = "Stork-" + scheduleName + "-" + snap.Metadata.Namespace + "-" + policyType
				// Use full backups for weekly and monthly snaps
				if policyType == string(storkapi.SchedulePolicyTypeWeekly) ||
					policyType == string(storkapi.SchedulePolicyTypeMonthly) {
					// Only set to full backup if the frequency isn't
					// provided
					if request.FullBackupFrequency == 0 {
						request.Full = true
					}
				}
				return
			}
		}
	}
	request.Labels[cloudBackupExternalManagerLabel] = "StorkManual"
}

func (p *portworx) SnapshotCreate(
	snap *crdv1.VolumeSnapshot,
	pv *v1.PersistentVolume,
	tags *map[string]string,
) (*crdv1.VolumeSnapshotDataSource, *[]crdv1.VolumeSnapshotCondition, error) {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return nil, nil, err
		}
	}

	volDriver, err := p.getUserVolDriver(snap.Metadata.Annotations)
	if err != nil {
		return nil, nil, err
	}

	if err := p.verifyPortworxPv(pv); err != nil {
		return nil, getErrorSnapshotConditions(err), err
	}

	if snap == nil {
		err = fmt.Errorf("snapshot spec not supplied to the create API")
		return nil, getErrorSnapshotConditions(err), err
	}

	pvcsForSnapshot, err := p.getPVCsForSnapshot(snap)
	if err != nil {
		return nil, getErrorSnapshotConditions(err), err
	}

	backgroundCommandTermChan, err := snapshot.ExecutePreSnapRule(snap, pvcsForSnapshot)

	defer func() {
		if backgroundCommandTermChan != nil {
			backgroundCommandTermChan <- true // regardless of what happens, always terminate commands
		}
	}()

	if err != nil {
		err = fmt.Errorf("failed to run pre-snap rule due to: %v", err)
		log.SnapshotLog(snap).Errorf(err.Error())
		return nil, getErrorSnapshotConditions(err), err
	}

	snapStatusConditions := []crdv1.VolumeSnapshotCondition{}
	var snapshotID, snapshotDataName, snapshotCredID string
	volumeID, err := p.getVolumeIDFromPV(pv)
	if err != nil {
		return nil, getErrorSnapshotConditions(err), err
	}

	snapType, err := getSnapshotType(snap.Metadata.Annotations)
	if err != nil {
		return nil, getErrorSnapshotConditions(err), err
	}

	var taskID string
	switch snapType {
	case crdv1.PortworxSnapshotTypeCloud:
		log.SnapshotLog(snap).Debugf("Cloud SnapshotCreate for pv: %+v \n tags: %v", pv, tags)
		ok, msg, err := p.ensureNodesHaveMinVersion("2.0")
		if err != nil {
			return nil, nil, err
		}

		if !ok {
			err = &errors.ErrNotSupported{
				Feature: "Cloud snapshots",
				Reason:  "API changes require PX version 2.0 onwards: " + msg,
			}

			return nil, getErrorSnapshotConditions(err), err
		}

		snapshotCredID = getCredIDFromSnapshot(snap.Metadata.Annotations)
		taskID = string(snap.Metadata.UID)
		request := &api.CloudBackupCreateRequest{
			VolumeID:       volumeID,
			CredentialUUID: snapshotCredID,
			Name:           taskID,
		}
		request.Labels = make(map[string]string)
		request.Labels[cloudBackupOwnerLabel] = "stork"
		p.addCloudsnapInfo(request, snap)

		if value, present := snap.Metadata.Annotations[incrementalCountAnnotation]; present {
			incrementalCount, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid cloudsnap-incremental-count specified %v: %v", p, err)
			}
			if incrementalCount <= 0 {
				request.Full = true
			} else {
				request.FullBackupFrequency = uint32(incrementalCount)
				request.Full = false
			}
		}
		if val, ok := snap.Metadata.Annotations[deleteLocalSnapAnnotation]; ok {
			skip, err := strconv.ParseBool(val)
			if err != nil {
				msg := fmt.Errorf("invalid value for delete local snapshot annotation, %v", err.Error())
				log.SnapshotLog(snap).Errorf(msg.Error())
				return nil, nil, msg
			}
			if skip {
				log.SnapshotLog(snap).Warnf("setting delete-local-snap to true")
				request.DeleteLocal = true
			}
		}

		_, err = volDriver.CloudBackupCreate(request)
		if err != nil {
			if _, ok := err.(*ost_errors.ErrExists); !ok {
				return nil, getErrorSnapshotConditions(err), err
			}
		}

		status, err := p.waitForCloudSnapCompletion(api.CloudBackupOp, taskID, false, backgroundCommandTermChan)
		if err != nil {
			log.SnapshotLog(snap).Errorf("Cloudsnap backup: %s failed due to: %v", status.cloudSnapID, err)
			return nil, getErrorSnapshotConditions(err), err
		}

		snapshotID = status.cloudSnapID
		log.SnapshotLog(snap).Infof("Cloudsnap backup: %s of vol: %s created successfully.", snapshotID, volumeID)
		snapStatusConditions = getReadySnapshotConditions()
	case crdv1.PortworxSnapshotTypeLocal:
		if isGroupSnap(snap) {
			err = &errors.ErrNotSupported{
				Feature: "Group snapshots using VolumeSnapshot with annotations",
				Reason:  "Since 2.0.2 group snapshots are only supported using GroupVolumeSnapshot CRD.",
			}

			return nil, getErrorSnapshotConditions(err), err
		}

		log.SnapshotLog(snap).Debugf("SnapshotCreate for pv: %+v \n tags: %v", pv, tags)
		snapName := p.getSnapshotName(tags)
		locator := &api.VolumeLocator{
			Name: snapName,
			VolumeLabels: map[string]string{
				storkSnapNameLabel: (*tags)[snapshotter.CloudSnapshotCreatedForVolumeSnapshotNameTag],
				namespaceLabel:     (*tags)[snapshotter.CloudSnapshotCreatedForVolumeSnapshotNamespaceTag],
			},
		}
		snapshotID, err = volDriver.Snapshot(volumeID, true, locator, true)
		if err != nil {
			// Check already exists error and return existing snapshot if found
			if isAlreadyExistsError(err) {
				snapInfo, err := p.inspectVolume(volDriver, snapName)
				if err != nil {
					return nil, getErrorSnapshotConditions(err), err
				}

				if snapInfo.ParentID != volumeID {
					err := fmt.Errorf("found existing snapshot with name: %s but parent volume ID doesn't match."+
						"expected: %s found: %s", snapName, volumeID, snapInfo.ParentID)
					return nil, getErrorSnapshotConditions(err), err
				}

				snapshotID = snapInfo.VolumeID
			} else {
				return nil, getErrorSnapshotConditions(err), err
			}
		}
		snapStatusConditions = getReadySnapshotConditions()
	}

	err = snapshot.ExecutePostSnapRule(pvcsForSnapshot, snap)
	if err != nil {
		err = fmt.Errorf("failed to run post-snap rule due to: %v", err)
		log.SnapshotLog(snap).Errorf(err.Error())
		return nil, getErrorSnapshotConditions(err), err
	}

	provisioner, err := getDriverTypeFromPV(pv)
	if err != nil {
		log.SnapshotLog(snap).Errorf(err.Error())
		return nil, getErrorSnapshotConditions(err), err
	}

	return &crdv1.VolumeSnapshotDataSource{
		PortworxSnapshot: &crdv1.PortworxVolumeSnapshotSource{
			SnapshotID:          snapshotID,
			SnapshotData:        snapshotDataName,
			SnapshotType:        snapType,
			SnapshotCloudCredID: snapshotCredID,
			SnapshotTaskID:      taskID,
			VolumeProvisioner:   provisioner,
		},
	}, &snapStatusConditions, nil
}

func (p *portworx) SnapshotDelete(snapDataSrc *crdv1.VolumeSnapshotDataSource, _ *v1.PersistentVolume) error {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return err
		}
	}

	volDriver, err := p.getAdminVolDriver()
	if err != nil {
		return err
	}

	if snapDataSrc == nil || snapDataSrc.PortworxSnapshot == nil {
		return fmt.Errorf("invalid snapshot source %v", snapDataSrc)
	}

	switch snapDataSrc.PortworxSnapshot.SnapshotType {
	case crdv1.PortworxSnapshotTypeCloud:
		input := &api.CloudBackupDeleteRequest{
			ID:             snapDataSrc.PortworxSnapshot.SnapshotID,
			CredentialUUID: snapDataSrc.PortworxSnapshot.SnapshotCloudCredID,
		}
		return volDriver.CloudBackupDelete(input)
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
					snapData, err := k8sextops.Instance().GetSnapshotData(snapDataName)
					if err != nil {
						lastError = err
						logrus.Errorf("Failed to get volume snapshot data: %s due to err: %v", snapDataName, err)
						continue
					}

					snapNamespace := snapData.Spec.VolumeSnapshotRef.Namespace
					snapName := snapData.Spec.VolumeSnapshotRef.Name

					logrus.Infof("Deleting VolumeSnapshot:[%s] %s", snapNamespace, snapName)
					err = wait.ExponentialBackoff(snapAPICallBackoff, func() (bool, error) {
						err = k8sextops.Instance().DeleteSnapshot(snapName, snapNamespace)
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

		return volDriver.Delete(snapDataSrc.PortworxSnapshot.SnapshotID)
	}
}

// CleanupSnapshotRestoreObjects deletes restore objects if any
func (p *portworx) CleanupSnapshotRestoreObjects(snapRestore *storkapi.VolumeSnapshotRestore) error {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return err
		}
	}

	logrus.Infof("Cleaning up in-place restore objects")

	for _, vol := range snapRestore.Status.Volumes {
		_, snapType, _, err := getSnapshotDetails(vol.Snapshot)
		if err != nil {
			log.VolumeSnapshotRestoreLog(snapRestore).Errorf("Invalid snapshot data %v", err)
			return err
		}
		if snapType == crdv1.PortworxSnapshotTypeLocal {
			continue
		}

		volName := restoreNamePrefix + getUidforRestore(vol.Volume, string(snapRestore.GetUID()))
		taskName := restoreTaskPrefix + getUidforRestore(vol.Volume, string(snapRestore.GetUID()))

		log.VolumeSnapshotRestoreLog(snapRestore).Infof("Deleting volume %v", volName)
		// Get volume client from user context
		volDriver, err := p.getUserVolDriver(snapRestore.Annotations)
		if err != nil {
			return err
		}

		// stop cs restore
		input := &api.CloudBackupStateChangeRequest{
			Name:           taskName,
			RequestedState: api.CloudBackupRequestedStateStop,
		}
		err = volDriver.CloudBackupStateChange(input)
		if err != nil {
			if !(strings.Contains(err.Error(), "Failed to change state: No active backup/restore for the volume") ||
				strings.Contains(err.Error(), "not found")) {
				return err
			}
		}
		// Delete restore volume
		err = volDriver.Delete(volName)
		if err != nil {
			log.VolumeSnapshotRestoreLog(snapRestore).Errorf("Failed to delete volume %v", err)
			return fmt.Errorf("failed to delete volume %v: %v", volName, err)
		}
	}

	return nil
}

func getUidforRestore(volID, restoreID string) string {
	return volID + "-" + restoreID
}

// StartVolumeSnapshotRestore will prepare volume for restore
func (p *portworx) StartVolumeSnapshotRestore(snapRestore *storkapi.VolumeSnapshotRestore) error {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return err
		}
	}

	if len(snapRestore.Status.Volumes) == 0 {
		return fmt.Errorf("no restore volumes information")
	}

	// anyone snapshot info should suffice to start volumesnapshotrestore
	_, snapType, _, err := getSnapshotDetails(snapRestore.Status.Volumes[0].Snapshot)
	if err != nil {
		log.VolumeSnapshotRestoreLog(snapRestore).Errorf("Invalid snapshot data %v", err)
		return err
	}

	switch snapType {
	case "", crdv1.PortworxSnapshotTypeLocal:
		return nil
	case crdv1.PortworxSnapshotTypeCloud:
		ok, msg, err := p.ensureNodesHaveMinVersion("2.3.2")
		if err != nil {
			return err
		}

		if !ok {
			err = &errors.ErrNotSupported{
				Feature: "VolumeSnapshotRestore for Cloudsnaps",
				Reason:  "Only supported on PX version 2.3.2 onwards: " + msg,
			}

			return err
		}

		var poolID string
		isRes := true
		labels := make(map[string]string)
		locator := &api.VolumeLocator{VolumeLabels: labels}
		ok, msg, err = p.ensureNodesHaveMinVersion("2.6.0")
		if err != nil || !ok {
			log.VolumeSnapshotRestoreLog(snapRestore).Errorf("Fast restore is supported onword PX version 2.6.0: %v", msg)
			isRes = false
		}
		// Get volume client from user context
		volDriver, err := p.getUserVolDriver(snapRestore.Annotations)
		if err != nil {
			return err
		}
		for _, vol := range snapRestore.Status.Volumes {
			volID := vol.Volume
			snapID, _, credID, err := getSnapshotDetails(vol.Snapshot)
			if err != nil {
				return fmt.Errorf("failed to get snapshot details for snap %v, err %v", vol.Snapshot, err)
			}

			// Get Volume Info
			uid := getUidforRestore(volID, string(snapRestore.GetUID()))
			vols, err := volDriver.Inspect([]string{volID})
			if err != nil {
				return &ErrFailedToInspectVolume{
					ID:    volID,
					Cause: fmt.Sprintf("Volume inspect returned err: %v", err),
				}
			} else if len(vols) == 0 {
				return &ErrFailedToInspectVolume{
					ID:    volID,
					Cause: fmt.Sprintf("Volume not found err: %v", err),
				}
			}
			if isRes {
				locator.VolumeLabels[api.SpecMatchSrcVolProvision] = "true"
			} else {
				replNodes := vols[0].GetReplicaSets()[0]
				poolID = replNodes.GetPoolUuids()[0]
				locator = nil
			}

			taskID := restoreTaskPrefix + uid
			restoreName := restoreNamePrefix + uid
			log.VolumeSnapshotRestoreLog(snapRestore).Infof("Restoring cloudsnapshot to volume %v on pool %v", restoreName, poolID)
			_, err = volDriver.CloudBackupRestore(&api.CloudBackupRestoreRequest{
				Name:              taskID,
				ID:                snapID,
				RestoreVolumeName: restoreName,
				CredentialUUID:    credID,
				NodeID:            poolID,
				Locator:           locator,
			})
			if err != nil {
				if _, ok := err.(*ost_errors.ErrExists); !ok {
					return err
				}
			}
			vol.Reason = "Volume restore in progress"
			vol.RestoreStatus = storkapi.VolumeSnapshotRestoreStatusInProgress
		}
		return nil
	default:
		return fmt.Errorf("invalid SourceType for snapshot(local/cloud)")
	}
}

func (p *portworx) GetVolumeSnapshotRestoreStatus(snapRestore *storkapi.VolumeSnapshotRestore) error {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return err
		}
	}

	// Get volume client from user context
	volDriver, err := p.getUserVolDriver(snapRestore.Annotations)
	if err != nil {
		return err
	}

	for _, vol := range snapRestore.Status.Volumes {
		_, snapType, _, err := getSnapshotDetails(vol.Snapshot)
		if err != nil {
			vol.RestoreStatus = storkapi.VolumeSnapshotRestoreStatusFailed
			vol.Reason = fmt.Sprintf("Restore failed for volume: %v", err)
			continue
		}
		// Nothing to do for local snapshot
		switch snapType {
		case "", crdv1.PortworxSnapshotTypeLocal:
			vol.RestoreStatus = storkapi.VolumeSnapshotRestoreStatusStaged
			vol.Reason = "Restore object is ready"
		case crdv1.PortworxSnapshotTypeCloud:
			uid := getUidforRestore(vol.Volume, string(snapRestore.GetUID()))
			taskID := restoreTaskPrefix + uid
			csStatus := p.getCloudSnapStatus(volDriver, api.CloudRestoreOp, taskID)
			if isCloudsnapStatusActive(csStatus.status) {
				vol.RestoreStatus = storkapi.VolumeSnapshotRestoreStatusInProgress
				vol.Reason = "Volume restore in progress"
			} else if isCloudsnapStatusFailed(csStatus.status) {
				vol.RestoreStatus = storkapi.VolumeSnapshotRestoreStatusFailed
				vol.Reason = fmt.Sprintf("Restore failed for volume: %v", csStatus.msg)
			} else {
				// check for ha update and then mark as successful
				isUpdate, err := p.checkAndUpdateHaLevel(vol.Volume, uid, snapRestore.Annotations)
				if err != nil {
					vol.RestoreStatus = storkapi.VolumeSnapshotRestoreStatusFailed
					vol.Reason = fmt.Sprintf("Restore ha-update failed for volume: %v", err.Error())
				}
				vol.RestoreStatus = storkapi.VolumeSnapshotRestoreStatusStaged
				vol.Reason = "Restore object is ready"
				if !isUpdate {
					vol.RestoreStatus = storkapi.VolumeSnapshotRestoreStatusInProgress
					vol.Reason = "Volume is in resync state"
				}
			}
		default:
			vol.Reason = fmt.Sprintf("invalid SourceType for snapshot(local/cloud), found: %v", snapType)
			vol.RestoreStatus = storkapi.VolumeSnapshotRestoreStatusFailed
		}
	}

	return nil
}

func (p *portworx) checkAndUpdateHaLevel(volID, uid string, params map[string]string) (bool, error) {
	// Get volume client from user context
	volDriver, err := p.getUserVolDriver(params)
	if err != nil {
		return false, err
	}
	// Get Volume Info
	parentVols, err := volDriver.Inspect([]string{volID})
	if err != nil {
		return false, &ErrFailedToInspectVolume{
			ID:    volID,
			Cause: fmt.Sprintf("Volume inspect returned err: %v", err),
		}
	} else if len(parentVols) == 0 {
		return false, &ErrFailedToInspectVolume{
			ID:    volID,
			Cause: fmt.Sprintf("Volume not found err: %v", err),
		}
	}
	restoreVols, err := volDriver.Inspect([]string{restoreNamePrefix + uid})
	if err != nil {
		return false, &ErrFailedToInspectVolume{
			ID:    volID,
			Cause: fmt.Sprintf("Volume inspect returned err: %v", err),
		}
	} else if len(restoreVols) == 0 {
		return false, &ErrFailedToInspectVolume{
			ID:    volID,
			Cause: fmt.Sprintf("Volume not found err: %v", err),
		}
	}

	logrus.Debugf("volume info %v \t %v", parentVols[0].GetLocator().GetName(), restoreVols[0].GetLocator().GetName())
	restoreVol := restoreVols[0]
	if restoreVol.Status != api.VolumeStatus_VOLUME_STATUS_UP {
		logrus.Warnf("Volume is not ready yet %v", restoreVol.GetLocator().GetName())
		return false, nil
	}
	logrus.Infof("Restore Volume: %v \t Status %v \t state %v", restoreVol.Id, restoreVol.Status, restoreVol.State)
	// find replica pools ids
	var pPools []string
	var rPools []string
	var replPool string
	// get all parent volume replica pools
	for _, rs := range parentVols[0].GetReplicaSets() {
		pPools = append(pPools, rs.GetPoolUuids()...)
	}
	// get all restore volume replica nodes
	for _, rs := range restoreVol.GetReplicaSets() {
		rPools = append(rPools, rs.GetPoolUuids()...)
	}

	logrus.Debugf("parent volume pools %v", pPools)
	logrus.Debugf("restore volume pools %v", rPools)
	// get pool to perform ha update
	for _, pool := range pPools {
		isPart := false
		for _, rpool := range rPools {
			// since restore volumes rs will always be 1
			if pool == rpool {
				isPart = true
				break
			}
		}
		if !isPart {
			replPool = pool
			break
		}
	}

	if restoreVol.GetSpec().GetHaLevel() != parentVols[0].GetSpec().GetHaLevel() {
		logrus.Infof("Updating HA Level of volume %v to pool %v", restoreVol.GetLocator().GetName(), replPool)
		spec := &api.VolumeSpec{
			HaLevel:    restoreVol.GetSpec().GetHaLevel() + 1,
			ReplicaSet: &api.ReplicaSet{Nodes: []string{replPool}},
		}
		if err := volDriver.Set(restoreVol.GetId(), restoreVol.GetLocator(), spec); err != nil {
			return false, fmt.Errorf("failed to perform ha-update: %v", err)
		}
		// check for vol is sync
		return false, nil
	}
	return true, nil
}

// CompleteVolumeSnapshotRestore does in-place restore of snapshot to it's parent volume
func (p *portworx) CompleteVolumeSnapshotRestore(snapRestore *storkapi.VolumeSnapshotRestore) error {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return err
		}
	}

	// TODO: Restoring snapshot to volume other than parent volume is not supported by PX
	if snapRestore.Spec.DestinationPVC != nil {
		return fmt.Errorf("restore to volume other than parent is not supported")
	}
	return p.pxSnapshotRestore(snapRestore)
}

// getSnapshotDetails returns PX snapID, snapshotType, credID(if any)
// error if unable to retrive snap data
func getSnapshotDetails(snapDataName string) (string, crdv1.PortworxSnapshotType, string, error) {
	snapshotData, err := k8sextops.Instance().GetSnapshotData(snapDataName)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to retrieve VolumeSnapshotData %s: %v",
			snapDataName, err)
	}
	// Let's verify if source snapshotdata is complete
	err = k8sextops.Instance().ValidateSnapshotData(snapshotData.Metadata.Name, false, validateSnapshotTimeout, validateSnapshotRetryInterval)
	if err != nil {
		return "", "", "", fmt.Errorf("snapshot: %s is not complete. %v", snapshotData.Metadata.Name, err)
	}
	snapID := snapshotData.Spec.PortworxSnapshot.SnapshotID
	snapType := snapshotData.Spec.PortworxSnapshot.SnapshotType
	credID := snapshotData.Spec.PortworxSnapshot.SnapshotCloudCredID

	return snapID, snapType, credID, nil
}

func (p *portworx) pxSnapshotRestore(snapRestore *storkapi.VolumeSnapshotRestore) error {
	// Get volume client from user context
	volDriver, err := p.getUserVolDriver(snapRestore.Annotations)
	if err != nil {
		return err
	}
	restoreID := string(snapRestore.GetUID())
	for _, vol := range snapRestore.Status.Volumes {
		// Detect cloudsnap or local snapshot here
		snapID, snapType, _, err := getSnapshotDetails(vol.Snapshot)
		if err != nil {
			log.VolumeSnapshotRestoreLog(snapRestore).Errorf("Invalid snapshot data %v", err)
			return err
		}

		log.VolumeSnapshotRestoreLog(snapRestore).Infof("Restoring volume %v with snapshot %v", vol.Volume, snapID)
		if snapType == crdv1.PortworxSnapshotTypeCloud {
			snapID = restoreNamePrefix + getUidforRestore(vol.Volume, restoreID)
		}
		var restoreErr error
		err = wait.ExponentialBackoff(restoreAPICallBackoff, func() (bool, error) {
			restoreErr = volDriver.Restore(vol.Volume, snapID)
			if restoreErr != nil {
				log.VolumeSnapshotRestoreLog(snapRestore).Warnf("In-place restore failed for %v: %v", vol.Volume, restoreErr)
				return false, nil
			}

			return true, nil
		})
		if err != nil || restoreErr != nil {
			logrus.Errorf("Unable to restore volume %v with ID %v, err %v",
				vol.Volume, snapID, err)
			vol.Reason = fmt.Sprintf("Failed to perform in-place restore %v", restoreErr.Error())
			vol.RestoreStatus = storkapi.VolumeSnapshotRestoreStatusFailed
			return err
		}

		stateErr := wait.ExponentialBackoff(restoreStateCallBackoff, func() (bool, error) {
			log.VolumeSnapshotRestoreLog(snapRestore).Infof("checking volume state for restore status %v", vol.Volume)
			// Get Volume Info
			orgVol, err := volDriver.Inspect([]string{vol.Volume})
			if err != nil {
				log.VolumeSnapshotRestoreLog(snapRestore).Warnf("volume inspect failed %v:%v", vol.Volume, err)
				return false, nil
			} else if len(orgVol) == 0 {
				log.VolumeSnapshotRestoreLog(snapRestore).Warnf("empty volume inspect response for %v", vol.Volume)
				return false, nil
			}
			// check restore status
			if orgVol[0].State == api.VolumeState_VOLUME_STATE_RESTORE {
				// restore is in progress mark volume as stage again & return
				if orgVol[0].Error == "" {
					vol.RestoreStatus = storkapi.VolumeSnapshotRestoreStatusInProgress
					log.VolumeSnapshotRestoreLog(snapRestore).Infof("volume is in restore state %v:%v", vol.Volume, orgVol[0].State)
					return false, nil
				}
				// restore is failed, stop and return error
				log.VolumeSnapshotRestoreLog(snapRestore).Errorf("volume snapshot restore failed, volume %v with snap %v, err %v",
					vol.Volume, snapID, orgVol[0].Error)
				vol.RestoreStatus = storkapi.VolumeSnapshotRestoreStatusFailed
				return false, fmt.Errorf("failed to perform in-place restore: %v", orgVol[0].Error)
			}
			return true, nil
		})
		if stateErr != nil {
			logrus.Errorf("Volume restore is not succesful for volume %v with ID %v, err %v",
				vol.Volume, snapID, stateErr)
			vol.Reason = fmt.Sprintf("Failed to perform in-place restore %v", stateErr.Error())
			vol.RestoreStatus = storkapi.VolumeSnapshotRestoreStatusFailed
			return stateErr
		}

		log.VolumeSnapshotRestoreLog(snapRestore).Infof("Completed restore for volume %v with Snapshotshot %v", vol.Volume, snapID)
		vol.Reason = "Restore is successful"
		vol.RestoreStatus = storkapi.VolumeSnapshotRestoreStatusSuccessful
		if snapType == crdv1.PortworxSnapshotTypeCloud {
			if err := volDriver.Delete(snapID); err != nil {
				logrus.Errorf("Unable to delete volume  %v, err %v",
					snapID, err)
			}
		}
	}
	return nil
}

func (p *portworx) SnapshotRestore(
	snapshotData *crdv1.VolumeSnapshotData,
	pvc *v1.PersistentVolumeClaim,
	_ string,
	parameters map[string]string,
) (*v1.PersistentVolumeSource, map[string]string, error) {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return nil, nil, err
		}
	}

	volDriver, err := p.getUserVolDriver(pvc.ObjectMeta.Annotations)
	if err != nil {
		return nil, nil, err
	}

	if snapshotData == nil || snapshotData.Spec.PortworxSnapshot == nil {
		return nil, nil, fmt.Errorf("invalid Snapshot spec")
	}
	if pvc == nil {
		return nil, nil, fmt.Errorf("invalid PVC spec")
	}

	logrus.Debugf("SnapshotRestore for pvc: %+v", pvc)

	snapshotName, present := pvc.ObjectMeta.Annotations[crdclient.SnapshotPVCAnnotation]
	if !present || len(snapshotName) == 0 {
		return nil, nil, fmt.Errorf("source volumesnapshot name not present in PVC annotations")
	}

	// Let's verify if source snapshotdata is complete
	err = k8sextops.Instance().ValidateSnapshotData(snapshotData.Metadata.Name, false, validateSnapshotTimeout, validateSnapshotRetryInterval)
	if err != nil {
		return nil, nil, fmt.Errorf("snapshot: %s is not complete. %v", snapshotName, err)
	}

	snapID := snapshotData.Spec.PortworxSnapshot.SnapshotID
	restoreVolumeName := "pvc-" + string(pvc.UID)
	var restoreVolumeID string

	switch snapshotData.Spec.PortworxSnapshot.SnapshotType {
	case "", crdv1.PortworxSnapshotTypeLocal:
		snapshotNamespace, ok := pvc.Annotations[snapshotcontrollers.StorkSnapshotSourceNamespaceAnnotation]
		if !ok {
			snapshotNamespace = pvc.GetNamespace()
		}

		// Check if this is a group snapshot
		snap, err := k8sextops.Instance().GetSnapshot(snapshotName, snapshotNamespace)
		if err != nil {
			return nil, nil, err
		}

		if isGroupSnap(snap) {
			return nil, nil, fmt.Errorf("volumesnapshot: [%s] %s was used to create group snapshots. "+
				"To restore, use the volumesnapshots that were created by this group snapshot", snapshotNamespace, snapshotName)
		}

		locator := &api.VolumeLocator{
			Name: restoreVolumeName,
			VolumeLabels: map[string]string{
				pvcNameLabel:   pvc.Name,
				namespaceLabel: pvc.Namespace,
			},
		}
		restoreVolumeID, err = volDriver.Snapshot(snapID, false, locator, true)
		if err != nil {
			return nil, nil, err
		}
	case crdv1.PortworxSnapshotTypeCloud:
		taskID := string(pvc.UID)
		_, err := volDriver.CloudBackupRestore(&api.CloudBackupRestoreRequest{
			Name:              taskID,
			ID:                snapID,
			RestoreVolumeName: restoreVolumeName,
			CredentialUUID:    snapshotData.Spec.PortworxSnapshot.SnapshotCloudCredID,
		})
		if err != nil {
			if _, ok := err.(*ost_errors.ErrExists); !ok {
				return nil, nil, err
			}
		}

		logrus.Infof("Cloudsnap restore of %s started successfully with taskId %v", snapID, taskID)

		_, err = p.waitForCloudSnapCompletion(api.CloudRestoreOp, taskID, true, nil)
		if err != nil {
			return nil, nil, err
		}
		restoreVolumeID = restoreVolumeName
	}

	// create PV from restored volume
	vols, err := volDriver.Inspect([]string{restoreVolumeID})
	if err != nil {
		return nil, nil, &ErrFailedToInspectVolume{
			ID:    restoreVolumeID,
			Cause: fmt.Sprintf("Volume inspect returned err: %v", err),
		}
	}

	if len(vols) == 0 {
		return nil, nil, &errors.ErrNotFound{
			ID:   restoreVolumeID,
			Type: "Volume",
		}
	}

	var pv *v1.PersistentVolumeSource
	// Get the source provisioner value and default to in-tree
	switch snapshotData.Spec.PortworxSnapshot.VolumeProvisioner {
	case crdv1.PortworxCsiDeprecatedProvisionerName:
		fallthrough
	case crdv1.PortworxCsiProvisionerName:
		pv = &v1.PersistentVolumeSource{
			CSI: &v1.CSIPersistentVolumeSource{
				Driver:       snapshotData.Spec.PortworxSnapshot.VolumeProvisioner,
				VolumeHandle: vols[0].Id,
				FSType:       vols[0].Format.String(),
				ReadOnly:     vols[0].Readonly,
			},
		}
		if secretRef := p.secretRefFromAnnotations(pvc.ObjectMeta.Annotations); secretRef != nil {
			// no need to add secret to NodeStageSecretRef since Portworx does not support it
			pv.CSI.ControllerPublishSecretRef = secretRef
			pv.CSI.NodePublishSecretRef = secretRef
		}
	default:
		pv = &v1.PersistentVolumeSource{
			PortworxVolume: &v1.PortworxVolumeSource{
				VolumeID: vols[0].Id,
				FSType:   vols[0].Format.String(),
				ReadOnly: vols[0].Readonly,
			},
		}
	}

	labels := make(map[string]string)
	return pv, labels, nil
}

func (p *portworx) DescribeSnapshot(snapshotData *crdv1.VolumeSnapshotData) (*[]crdv1.VolumeSnapshotCondition, bool /* isCompleted */, error) {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return nil, false, err
		}
	}

	var err error
	if snapshotData == nil || snapshotData.Spec.PortworxSnapshot == nil {
		err = fmt.Errorf("invalid VolumeSnapshotDataSource: %v", snapshotData)
		return getErrorSnapshotConditions(err), false, nil
	}

	// Get volume client from user context
	volDriver, err := p.getUserVolDriver(snapshotData.Metadata.Annotations)
	if err != nil {
		return getErrorSnapshotConditions(err), false, err
	}

	switch snapshotData.Spec.PortworxSnapshot.SnapshotType {
	case "", crdv1.PortworxSnapshotTypeLocal:
		r := csv.NewReader(strings.NewReader(snapshotData.Spec.PortworxSnapshot.SnapshotID))
		snapshotIDs, err := r.Read()
		if err != nil {
			return getErrorSnapshotConditions(err), false, err
		}

		for _, snapshotID := range snapshotIDs {
			_, err = p.inspectVolume(volDriver, snapshotID)
			if err != nil {
				return getErrorSnapshotConditions(err), false, err
			}
		}
	case crdv1.PortworxSnapshotTypeCloud:
		if snapshotData.Status.Conditions != nil && len(snapshotData.Status.Conditions) != 0 {
			lastCondition := snapshotData.Status.Conditions[len(snapshotData.Status.Conditions)-1]
			if lastCondition.Type == crdv1.VolumeSnapshotDataConditionReady && lastCondition.Status == v1.ConditionTrue {
				conditions := getReadySnapshotConditions()
				return &conditions, true, nil
			} else if lastCondition.Type == crdv1.VolumeSnapshotDataConditionError && lastCondition.Status == v1.ConditionTrue {
				return getErrorSnapshotConditions(fmt.Errorf(lastCondition.Message)), true, nil
			}
		}

		csStatus := p.getCloudSnapStatus(volDriver, api.CloudBackupOp, snapshotData.Spec.PortworxSnapshot.SnapshotTaskID)
		if isCloudsnapStatusFailed(csStatus.status) {
			err = fmt.Errorf(csStatus.msg)
			return getErrorSnapshotConditions(err), false, err
		}

		if csStatus.status == api.CloudBackupStatusNotStarted {
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

func (p *portworx) GetSnapshotType(snap *crdv1.VolumeSnapshot) (string, error) {
	// TODO: Check if is a portworx snapshot
	snapType, err := getSnapshotType(snap.Metadata.Annotations)
	if err != nil {
		return "", err
	}
	if isGroupSnap(snap) {
		return "group " + string(snapType), nil
	}
	return string(snapType), nil
}

func (p *portworx) VolumeDelete(pv *v1.PersistentVolume) error {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return err
		}
	}

	volDriver, err := p.getAdminVolDriver()
	if err != nil {
		return err
	}

	id, err := p.getVolumeIDFromPV(pv)
	if err != nil {
		return err
	}

	return volDriver.Delete(id)
}

func (p *portworx) findParentVolID(snapID string) (string, error) {
	snapInfo, err := p.InspectVolume(snapID)
	if err != nil {
		return "", err
	}

	if len(snapInfo.ParentID) == 0 {
		return "", fmt.Errorf("portworx snapshot: %s does not have parent set", snapID)
	}

	return snapInfo.ParentID, nil
}

func (p *portworx) waitForCloudSnapCompletion(
	op api.CloudBackupOpType,
	taskID string,
	verbose bool,
	backgroundCommandTermChan chan bool) (cloudSnapStatus, error) {

	csStatus := cloudSnapStatus{
		status: api.CloudBackupStatusFailed,
		msg:    "cloudsnap status unknown",
	}

	err := wait.ExponentialBackoff(cloudsnapBackoff, func() (bool, error) {

		// Wait for completion using an admin auth context if any
		volDriver, err := p.getAdminVolDriver()
		if err != nil {
			return true, err
		}

		csStatus = p.getCloudSnapStatus(volDriver, op, taskID)
		if verbose {
			logrus.Infof(csStatus.msg)
		}

		if isCloudsnapStatusFailed(csStatus.status) {
			err := fmt.Errorf(csStatus.msg)
			logrus.Errorf(err.Error())
			return csStatus.terminal, err
		} else if csStatus.status == api.CloudBackupStatusActive {
			if backgroundCommandTermChan != nil {
				// since cloudsnap is already triggered and active, send signal to terminate background jobs
				backgroundCommandTermChan <- true
			}

			return csStatus.terminal, nil
		} else {
			return csStatus.terminal, nil
		}
	})

	return csStatus, err
}

// getCloudSnapStatus fetches the cloudsnapshot status for given op and cloudsnap task ID
func (p *portworx) getCloudSnapStatus(volDriver volume.VolumeDriver, op api.CloudBackupOpType, taskID string) cloudSnapStatus {
	response, err := volDriver.CloudBackupStatus(&api.CloudBackupStatusRequest{
		ID: taskID,
	})
	if err != nil {
		return cloudSnapStatus{
			terminal: true,
			status:   api.CloudBackupStatusFailed,
			msg:      err.Error(),
		}
	}

	csStatus, present := response.Statuses[taskID]
	if !present {
		return cloudSnapStatus{
			terminal: true,
			status:   api.CloudBackupStatusFailed,
			msg:      fmt.Sprintf("failed to get cloudsnap status for task %s", taskID),
		}
	}

	statusStr := getCloudSnapStatusString(&csStatus)

	if isCloudsnapStatusFailed(csStatus.Status) {
		return cloudSnapStatus{
			sourceVolumeID: csStatus.SrcVolumeID,
			terminal:       true,
			bytesTotal:     csStatus.BytesTotal,
			bytesDone:      csStatus.BytesDone,
			status:         csStatus.Status,
			cloudSnapID:    csStatus.ID,
			credentialID:   csStatus.CredentialUUID,
			msg: fmt.Sprintf("cloudsnap %s id: %s for %s did not succeed: %v",
				op, csStatus.ID, taskID, csStatus.Info),
		}
	}

	if csStatus.Status == api.CloudBackupStatusActive {
		return cloudSnapStatus{
			sourceVolumeID: csStatus.SrcVolumeID,
			status:         api.CloudBackupStatusActive,
			bytesTotal:     csStatus.BytesTotal,
			bytesDone:      csStatus.BytesDone,
			etaSeconds:     csStatus.EtaSeconds,
			cloudSnapID:    csStatus.ID,
			credentialID:   csStatus.CredentialUUID,
			msg: fmt.Sprintf("cloudsnap %s id: %s for %s has started and is active.",
				op, csStatus.ID, taskID),
		}
	}

	if csStatus.Status != api.CloudBackupStatusDone {
		return cloudSnapStatus{
			sourceVolumeID: csStatus.SrcVolumeID,
			status:         api.CloudBackupStatusNotStarted,
			bytesTotal:     csStatus.BytesTotal,
			bytesDone:      csStatus.BytesDone,
			etaSeconds:     csStatus.EtaSeconds,
			cloudSnapID:    csStatus.ID,
			credentialID:   csStatus.CredentialUUID,
			msg: fmt.Sprintf("cloudsnap %s id: %s for %s queued. status: %s",
				op, csStatus.ID, taskID, statusStr),
		}
	}

	return cloudSnapStatus{
		sourceVolumeID: csStatus.SrcVolumeID,
		terminal:       true,
		status:         api.CloudBackupStatusDone,
		bytesTotal:     csStatus.BytesTotal,
		bytesDone:      csStatus.BytesDone,
		etaSeconds:     csStatus.EtaSeconds,
		cloudSnapID:    csStatus.ID,
		credentialID:   csStatus.CredentialUUID,
		msg:            fmt.Sprintf("cloudsnap %s id: %s for %s done.", op, csStatus.ID, taskID),
	}
}

// revertPXSnaps deletes all given snapIDs
func (p *portworx) revertPXSnaps(snapIDs []string) {
	volDriver, err := p.getAdminVolDriver()
	if err != nil {
		return
	}

	if len(snapIDs) == 0 {
		return
	}

	failedDeletions := make(map[string]error)
	for _, id := range snapIDs {
		err := volDriver.Delete(id)
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

	logrus.Infof("Successfully reverted PX snapshots")
}

func (p *portworx) getPVCsForSnapshot(snap *crdv1.VolumeSnapshot) ([]v1.PersistentVolumeClaim, error) {
	snapType, err := getSnapshotType(snap.Metadata.Annotations)
	if err != nil {
		return nil, err
	}

	switch snapType {
	case crdv1.PortworxSnapshotTypeCloud:
		pvc, err := core.Instance().GetPersistentVolumeClaim(snap.Spec.PersistentVolumeClaimName, snap.Metadata.Namespace)
		if err != nil {
			return nil, err
		}

		return []v1.PersistentVolumeClaim{*pvc}, nil
	case crdv1.PortworxSnapshotTypeLocal:
		if isGroupSnap(snap) {
			return nil, &errors.ErrNotSupported{
				Feature: "Group snapshots using VolumeSnapshot",
				Reason:  "Since 2.0.2 group snapshots are only supported using GroupVolumeSnapshot CRD.",
			}
		}

		// local single snapshot
		pvc, err := core.Instance().GetPersistentVolumeClaim(snap.Spec.PersistentVolumeClaimName, snap.Metadata.Namespace)
		if err != nil {
			return nil, err
		}

		return []v1.PersistentVolumeClaim{*pvc}, nil
	default:
		return nil, fmt.Errorf("invalid snapshot type: %s", snapType)
	}
}

func getSnapshotType(options map[string]string) (crdv1.PortworxSnapshotType, error) {
	if options != nil {
		if val, snapTypePresent := options[pxSnapshotTypeKey]; snapTypePresent {
			if crdv1.PortworxSnapshotType(val) == crdv1.PortworxSnapshotTypeLocal ||
				crdv1.PortworxSnapshotType(val) == crdv1.PortworxSnapshotTypeCloud {
				return crdv1.PortworxSnapshotType(val), nil
			}

			return "", fmt.Errorf("unsupported Portworx snapshot type: %s", val)
		}
	}

	return crdv1.PortworxSnapshotTypeLocal, nil
}

func getCredIDFromSnapshot(metadata map[string]string) (credID string) {
	if metadata != nil {
		credID = metadata[pxCloudSnapshotCredsIDKey]
	}

	return
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
			Status:             v1.ConditionTrue,
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

func (p *portworx) CreatePair(pair *storkapi.ClusterPair) (string, error) {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return "", err
		}
	}

	ok, msg, err := p.ensureNodesHaveMinVersion("2.0")
	if err != nil {
		return "", err
	}

	if !ok {
		err = &errors.ErrNotSupported{
			Feature: "Cluster pair",
			Reason:  "Only supported on PX version 2.0 onwards: " + msg,
		}
		return "", err
	}

	port := uint64(defaultAPIPort)

	if p, ok := pair.Spec.Options["port"]; ok {
		port, err = strconv.ParseUint(p, 10, 32)
		if err != nil {
			return "", fmt.Errorf("invalid port (%v) specified for cluster pair: %v", p, err)
		}
	}
	mode := api.ClusterPairMode_Default
	if val, ok := pair.Spec.Options["mode"]; ok {
		if _, valid := api.ClusterPairMode_Mode_value[val]; valid {
			mode = api.ClusterPairMode_Mode(api.ClusterPairMode_Mode_value[val])
		}
	}

	clusterManager, err := p.getClusterManagerClient()
	if err != nil {
		return "", fmt.Errorf("cannot get cluster manager, err: %s", err.Error())
	}

	var credID string
	if bkpl, ok := pair.Spec.Options[storkapi.BackupLocationResourceName]; ok {
		supported, msg, err := p.ensureNodesHaveMinVersion("2.6.0")
		if err != nil {
			return "", err
		}

		if !supported {
			err = &errors.ErrNotSupported{
				Feature: "Migration Using Backuplocation CR",
				Reason:  "Only supported on PX version 2.6.0 onwards: " + msg,
			}
			return "", err
		}
		credID = p.getCredID(bkpl, pair.GetNamespace())
	}
	resp, err := clusterManager.CreatePair(&api.ClusterPairCreateRequest{
		RemoteClusterIp:    pair.Spec.Options["ip"],
		RemoteClusterToken: pair.Spec.Options["token"],
		RemoteClusterPort:  uint32(port),
		Mode:               mode,
		CredentialId:       credID,
	})
	if err != nil {
		return "", err
	}
	return resp.RemoteClusterId, nil
}

func (p *portworx) DeletePair(pair *storkapi.ClusterPair) error {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return err
		}
	}

	clusterManager, err := p.getClusterManagerClient()
	if err != nil {
		return fmt.Errorf("cannot get cluster manager, err: %s", err.Error())
	}

	return clusterManager.DeletePair(pair.Status.RemoteStorageID)
}

func (p *portworx) StartMigration(migration *storkapi.Migration) ([]*storkapi.MigrationVolumeInfo, error) {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return nil, err
		}
	}

	volDriver, err := p.getUserVolDriver(migration.Annotations)
	if err != nil {
		return nil, err
	}
	ok, msg, err := p.ensureNodesHaveMinVersion("2.0")
	if err != nil {
		return nil, err
	}

	if !ok {
		err = &errors.ErrNotSupported{
			Feature: "Migration",
			Reason:  "Only supported on PX version 2.0 onwards: " + msg,
		}
		return nil, err
	}

	if len(migration.Spec.Namespaces) == 0 {
		return nil, fmt.Errorf("namespaces for migration cannot be empty")
	}

	clusterPair, err := storkops.Instance().GetClusterPair(migration.Spec.ClusterPair, migration.Namespace)
	if err != nil {
		return nil, fmt.Errorf("error getting clusterpair: %v", err)
	}
	volumeInfos := make([]*storkapi.MigrationVolumeInfo, 0)
	for _, namespace := range migration.Spec.Namespaces {
		pvcList, err := core.Instance().GetPersistentVolumeClaims(namespace, migration.Spec.Selectors)
		if err != nil {
			return nil, fmt.Errorf("error getting list of volumes to migrate: %v", err)
		}
		for _, pvc := range pvcList.Items {
			if !p.OwnsPVC(core.Instance(), &pvc) {
				continue
			}
			if resourcecollector.SkipResource(pvc.Annotations) {
				continue
			}
			volumeInfo := &storkapi.MigrationVolumeInfo{
				PersistentVolumeClaim: pvc.Name,
				Namespace:             pvc.Namespace,
			}
			volumeInfos = append(volumeInfos, volumeInfo)

			volume, err := core.Instance().GetVolumeForPersistentVolumeClaim(&pvc)
			if err != nil {
				return nil, fmt.Errorf("error getting volume for PVC: %v", err)
			}
			volumeInfo.Volume = volume
			taskID := p.getMigrationTaskID(migration, volumeInfo)
			_, err = volDriver.CloudMigrateStart(&api.CloudMigrateStartRequest{
				TaskId:    taskID,
				Operation: api.CloudMigrate_MigrateVolume,
				ClusterId: clusterPair.Status.RemoteStorageID,
				TargetId:  volume,
			})
			if err != nil {
				if _, ok := err.(*ost_errors.ErrExists); !ok {
					return nil, fmt.Errorf("error starting migration for volume: %v", err)
				}
			}
			volumeInfo.Status = storkapi.MigrationStatusInProgress
			volumeInfo.Reason = "Volume migration has started. Backup in progress."
		}
	}

	return volumeInfos, nil
}

func (p *portworx) getMigrationTaskID(migration *storkapi.Migration, volumeInfo *storkapi.MigrationVolumeInfo) string {
	return string(migration.UID) + "-" + volumeInfo.Namespace + "-" + volumeInfo.PersistentVolumeClaim
}

func (p *portworx) getBackupRestoreTaskID(operationUID types.UID, namespace string, pvc string) string {
	return string(operationUID) + "-" + namespace + "-" + pvc
}

func (p *portworx) getCredID(backupLocation string, namespace string) string {
	return "k8s/" + namespace + "/" + backupLocation
}

func (p *portworx) GetMigrationStatus(migration *storkapi.Migration) ([]*storkapi.MigrationVolumeInfo, error) {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return nil, err
		}
	}

	volDriver, err := p.getUserVolDriver(migration.Annotations)
	if err != nil {
		return nil, err
	}
	status, err := volDriver.CloudMigrateStatus(nil)
	if err != nil {
		return nil, err
	}

	clusterPair, err := storkops.Instance().GetClusterPair(migration.Spec.ClusterPair, migration.Namespace)
	if err != nil {
		return nil, fmt.Errorf("error getting clusterpair: %v", err)
	}

	clusterInfo, ok := status.Info[clusterPair.Status.RemoteStorageID]
	if !ok {
		return nil, fmt.Errorf("migration status not found for remote cluster %v", clusterPair.Status.RemoteStorageID)
	}

	for _, vInfo := range migration.Status.Volumes {
		found := false
		for _, mInfo := range clusterInfo.List {
			taskID := p.getMigrationTaskID(migration, vInfo)
			if taskID == mInfo.TaskId {
				found = true
				if mInfo.Status == api.CloudMigrate_Failed || mInfo.Status == api.CloudMigrate_Canceled {
					vInfo.Status = storkapi.MigrationStatusFailed
					vInfo.Reason = fmt.Sprintf("Migration %v failed for volume: %v", mInfo.CurrentStage, mInfo.ErrorReason)
				} else if mInfo.CurrentStage == api.CloudMigrate_Done &&
					mInfo.Status == api.CloudMigrate_Complete {
					vInfo.Status = storkapi.MigrationStatusSuccessful
					vInfo.Reason = "Migration successful for volume"
				} else if mInfo.Status == api.CloudMigrate_InProgress {
					vInfo.Reason = fmt.Sprintf("Volume migration has started. %v in progress. BytesDone: %v BytesTotal: %v ETA: %v seconds",
						mInfo.CurrentStage.String(),
						mInfo.BytesDone,
						mInfo.BytesTotal,
						mInfo.EtaSeconds)
				}
				break
			}
		}

		// If we didn't get the status for a volume mark it as failed
		if !found {
			vInfo.Status = storkapi.MigrationStatusFailed
			vInfo.Reason = "Unable to find migration status for volume"
		}
	}

	return migration.Status.Volumes, nil
}

func (p *portworx) CancelMigration(migration *storkapi.Migration) error {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return err
		}
	}

	volDriver, err := p.getUserVolDriver(migration.Annotations)
	if err != nil {
		return err
	}
	for _, volumeInfo := range migration.Status.Volumes {
		taskID := p.getMigrationTaskID(migration, volumeInfo)
		err := volDriver.CloudMigrateCancel(&api.CloudMigrateCancelRequest{
			TaskId: taskID,
		})
		// Cancellation is best-effort, so don't return error
		if err != nil {
			log.MigrationLog(migration).Warnf("Error canceling migration for PVC: %v Namespace: %v: %v",
				volumeInfo.PersistentVolumeClaim,
				volumeInfo.Namespace,
				err)
		}
	}
	return nil
}

func (p *portworx) UpdateMigratedPersistentVolumeSpec(
	pv *v1.PersistentVolume,
) (*v1.PersistentVolume, error) {

	if pv.Spec.CSI != nil {
		pv.Spec.CSI.VolumeHandle = pv.Name
		return pv, nil
	}

	pv.Spec.PortworxVolume.VolumeID = pv.Name
	return pv, nil

}

func (p *portworx) CreateGroupSnapshot(snap *storkapi.GroupVolumeSnapshot) (
	*storkvolume.GroupSnapshotCreateResponse, error) {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return nil, err
		}
	}

	ok, msg, err := p.ensureNodesHaveMinVersion("2.0.2")
	if err != nil {
		return nil, err
	}

	if !ok {
		err = &errors.ErrNotSupported{
			Feature: "Group snapshots using CRD",
			Reason:  "Only supported on PX version 2.0.2 onwards: " + msg,
		}

		return nil, err
	}

	volNames, err := k8sutils.GetVolumeNamesFromLabelSelector(snap.Namespace, snap.Spec.PVCSelector.MatchLabels)
	if err != nil {
		return nil, err
	}

	snapType, err := getSnapshotType(snap.Spec.Options)
	if err != nil {
		return nil, err
	}
	switch snapType {
	case crdv1.PortworxSnapshotTypeCloud:
		return p.createGroupCloudSnapFromVolumes(snap, volNames, snap.Spec.Options)
	case crdv1.PortworxSnapshotTypeLocal:
		return p.createGroupLocalSnapFromPVCs(snap, volNames, snap.Spec.Options)
	default:
		return nil, fmt.Errorf("unsupported snapshot type: %s", snapType)
	}
}

func (p *portworx) GetGroupSnapshotStatus(snap *storkapi.GroupVolumeSnapshot) (
	*storkvolume.GroupSnapshotCreateResponse, error) {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return nil, err
		}
	}

	snapType, err := getSnapshotType(snap.Spec.Options)
	if err != nil {
		return nil, err
	}
	switch snapType {
	case crdv1.PortworxSnapshotTypeCloud:
		return p.getGroupCloudSnapStatus(snap)
	case crdv1.PortworxSnapshotTypeLocal:
		return nil, &errors.ErrNotSupported{
			Feature: "Group snapshots",
			Reason:  "status API not supported for local group snapshots",
		}
	}

	return nil, fmt.Errorf("unsupported snapshot type: %s", snapType)
}

func (p *portworx) DeleteGroupSnapshot(snap *storkapi.GroupVolumeSnapshot) error {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return err
		}
	}

	var lastError error
	for _, vs := range snap.Status.VolumeSnapshots {
		if len(vs.VolumeSnapshotName) == 0 {
			log.GroupSnapshotLog(snap).Infof("no volumesnapshot object exists for %v. Skipping delete", vs)
			continue
		}

		err := k8sextops.Instance().DeleteSnapshot(vs.VolumeSnapshotName, snap.Namespace)
		if err != nil {
			if !k8s_errors.IsNotFound(err) {
				log.GroupSnapshotLog(snap).Errorf("failed to delete snapshot due to: %v", err)
				lastError = err
			}
		}
	}

	return lastError
}

func (p *portworx) GetClusterDomains() (*storkapi.ClusterDomains, error) {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return nil, err
		}
	}

	clusterManager, err := p.getClusterManagerClient()
	if err != nil {
		return nil, fmt.Errorf("cannot get cluster manager, err: %s", err.Error())
	}

	// get the cluster details
	cluster, err := clusterManager.Enumerate()
	if err != nil {
		return nil, err
	}

	// get the node to domain-name map
	nodeToDomainMap, err := p.getNodesToDomainMap(cluster.Nodes)
	if err != nil {
		logrus.Errorf("Failed to get node to domain mapping: %v", err)
		return nil, err
	}

	// get the domain to state (active/inactive) map
	domainStateMap, err := p.getDomainStateMap()
	if err != nil {
		if strings.HasSuffix(err.Error(), "node is not initialized with cluster domains") {
			return &storkapi.ClusterDomains{}, nil
		}
		logrus.Errorf("Failed to get domain name to state map: %v", err)
		return nil, err
	}

	// get the default domain to sync status map
	// the default sync status is decided based on the active state of the domain
	// after inspecting the volumes and their replicas a cluster domain's sync
	// status might change
	domainSyncStatusMap := p.getDefaultDomainSyncStatusMap(domainStateMap)

	clusterDomains := &storkapi.ClusterDomains{
		LocalDomain: nodeToDomainMap[cluster.NodeId],
	}

	var (
		vols []*api.Volume
	)

	syncStatusUnknown := true
	volDriver, err := p.getAdminVolDriver()
	if err != nil {
		logrus.Errorf("Failed to get a volumeDriver: %v", err)
		goto cluster_domain_info
	}

	// get all the volumes in this cluster
	vols, err = volDriver.Enumerate(&api.VolumeLocator{}, nil)
	if err != nil {
		logrus.Errorf("Failed to enumerate volumes: %v", err)
		goto cluster_domain_info
	}

	syncStatusUnknown = false
	for _, vol := range vols {
		// get the node (replicas) which are not in the current set
		// but exist in the create set. These are the replica nodes
		// which need to resync their data
		replicasNotInCurrent := p.getReplicasNotInCurrent(vol)
		for _, replicaNotInCurrent := range replicasNotInCurrent {
			// get the domain of the node which is not in the current set
			if domain, ok := nodeToDomainMap[replicaNotInCurrent]; ok {
				// only update the sync status of the domain to sync in progress
				// if it is currently active
				if isActive := domainStateMap[domain]; isActive {
					domainSyncStatusMap[domain] = storkapi.ClusterDomainSyncStatusInProgress
				} // else the domain is inactive which means that sync is not in progress
			}
		}
	}

cluster_domain_info:

	// Build the cluster domain infos object
	for domain, isActive := range domainStateMap {
		syncStatus := storkapi.ClusterDomainSyncStatusUnknown
		if !syncStatusUnknown {
			syncStatus = domainSyncStatusMap[domain]
		}
		clusterDomainState := storkapi.ClusterDomainInactive
		if isActive {
			clusterDomainState = storkapi.ClusterDomainActive
		}
		clusterDomainInfo := storkapi.ClusterDomainInfo{
			Name:       domain,
			State:      clusterDomainState,
			SyncStatus: syncStatus,
		}
		clusterDomains.ClusterDomainInfos = append(clusterDomains.ClusterDomainInfos, clusterDomainInfo)
	}
	return clusterDomains, nil
}

func (p *portworx) ActivateClusterDomain(cdu *storkapi.ClusterDomainUpdate) error {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return err
		}
	}

	clusterDomainClient, err := p.getClusterDomainClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), clusterDomainsTimeout)
	defer cancel()

	// Get user token from secret if any
	ctx, err = p.getUserContext(ctx, cdu.Annotations)
	if err != nil {
		return err
	}

	_, err = clusterDomainClient.Activate(ctx, &api.SdkClusterDomainActivateRequest{
		ClusterDomainName: cdu.Spec.ClusterDomain,
	})
	return err
}

func (p *portworx) DeactivateClusterDomain(cdu *storkapi.ClusterDomainUpdate) error {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return err
		}
	}

	clusterDomainClient, err := p.getClusterDomainClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), clusterDomainsTimeout)
	defer cancel()

	// Get user token from secret if any
	ctx, err = p.getUserContext(ctx, cdu.Annotations)
	if err != nil {
		return err
	}

	_, err = clusterDomainClient.Deactivate(ctx, &api.SdkClusterDomainDeactivateRequest{
		ClusterDomainName: cdu.Spec.ClusterDomain,
	})
	return err
}

func (p *portworx) getNodesToDomainMap(nodes []*api.Node) (map[string]string, error) {
	clusterManager, err := p.getClusterManagerClient()
	if err != nil {
		return nil, fmt.Errorf("cannot get cluster manager, err: %s", err.Error())
	}

	nodeToDomainMap := make(map[string]string)
	for _, node := range nodes {
		nodeConfig, err := clusterManager.GetNodeConf(node.Id)
		if err != nil {
			return nil, err
		}
		nodeToDomainMap[node.Id] = nodeConfig.ClusterDomain
	}
	return nodeToDomainMap, nil
}

func (p *portworx) getDomainStateMap() (map[string]bool, error) {
	domainMap := make(map[string]bool)
	clusterDomainClient, err := p.getClusterDomainClient()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), clusterDomainsTimeout)
	defer cancel()

	enumerateResp, err := clusterDomainClient.Enumerate(ctx, &api.SdkClusterDomainsEnumerateRequest{})
	if err != nil {
		return nil, err
	}

	for _, domainName := range enumerateResp.ClusterDomainNames {
		insCtx, insCancel := context.WithTimeout(context.Background(), clusterDomainsTimeout)
		defer insCancel()

		inspectResp, err := clusterDomainClient.Inspect(insCtx, &api.SdkClusterDomainInspectRequest{
			ClusterDomainName: domainName,
		})
		if err != nil {
			return nil, err
		}
		domainMap[domainName] = inspectResp.IsActive
	}
	return domainMap, nil
}

func (p *portworx) getDefaultDomainSyncStatusMap(domainStateMap map[string]bool) map[string]storkapi.ClusterDomainSyncStatus {
	domainSyncStatusMap := make(map[string]storkapi.ClusterDomainSyncStatus)
	for domain, isActive := range domainStateMap {
		if isActive {
			// default state is in sync
			domainSyncStatusMap[domain] = storkapi.ClusterDomainSyncStatusInSync
		} else {
			// default status is not in sync
			domainSyncStatusMap[domain] = storkapi.ClusterDomainSyncStatusNotInSync
		}
	}
	return domainSyncStatusMap
}

func (p *portworx) belongsToCurrentSet(nodeReplica string, currentSet []string) bool {
	for _, v := range currentSet {
		if v == nodeReplica {
			return true
		}
	}
	return false
}

func (p *portworx) getReplicasNotInCurrent(v *api.Volume) []string {
	var replicasNotInCurrent []string

	if v == nil {
		// safeguarding against a nil panic
		return nil
	}

	for _, runtimeState := range v.RuntimeState {
		var (
			currentNodes, createNodes []string
		)
		if runtimeState == nil {
			// safeguarding against a nil panic
			continue
		}
		if currMidStr, ok := runtimeState.RuntimeState["ReplicaSetCurrMid"]; ok {
			currentNodes = strings.Split(currMidStr, ",")
		}
		if createMidStr, ok := runtimeState.RuntimeState["ReplicaSetCreateMid"]; ok {
			createNodes = strings.Split(createMidStr, ",")
		}
		if len(currentNodes) == len(createNodes) {
			continue
		}
		for _, nodeReplica := range createNodes {
			if !p.belongsToCurrentSet(nodeReplica, currentNodes) {
				replicasNotInCurrent = append(replicasNotInCurrent, nodeReplica)
			}
		}
	}
	return replicasNotInCurrent
}

func (p *portworx) addApplicationBackupCloudsnapInfo(
	request *api.CloudBackupCreateRequest,
	backup *storkapi.ApplicationBackup,
) {
	if backup.Annotations != nil {
		if scheduleName, exists := backup.Annotations[applicationcontrollers.ApplicationBackupScheduleNameAnnotation]; exists {
			if policyType, exists := backup.Annotations[applicationcontrollers.ApplicationBackupSchedulePolicyTypeAnnotation]; exists {
				request.Labels[cloudBackupExternalManagerLabel] = "StorkApplicationBackup-" + scheduleName + "-" + backup.Namespace + "-" + policyType
				// Use full backups for weekly and monthly snaps
				if policyType == string(storkapi.SchedulePolicyTypeWeekly) ||
					policyType == string(storkapi.SchedulePolicyTypeMonthly) {
					// Only set to full backup if the frequency isn't
					// provided
					if request.FullBackupFrequency == 0 {
						request.Full = true
					}
				}
				return
			}
		}
	}
	request.Labels[cloudBackupExternalManagerLabel] = "StorkApplicationBackupManual"
}

func (p *portworx) StartBackup(backup *storkapi.ApplicationBackup,
	pvcs []v1.PersistentVolumeClaim,
) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return nil, err
		}
	}

	ok, msg, err := p.ensureNodesHaveMinVersion("2.2.0")
	if err != nil {
		return nil, err
	}

	if !ok {
		err = &errors.ErrNotSupported{
			Feature: "ApplicationBackup",
			Reason:  "Only supported on PX version 2.2.0 onwards: " + msg,
		}

		return nil, err
	}

	volDriver, err := p.getUserVolDriver(backup.Annotations)
	if err != nil {
		return nil, err
	}
	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)
	for _, pvc := range pvcs {
		if pvc.DeletionTimestamp != nil {
			log.ApplicationBackupLog(backup).Warnf("Ignoring PVC %v which is being deleted", pvc.Name)
			continue
		}
		volumeInfo := &storkapi.ApplicationBackupVolumeInfo{}
		volumeInfo.PersistentVolumeClaim = pvc.Name
		volumeInfo.Namespace = pvc.Namespace
		volumeInfo.DriverName = driverName

		volume, err := core.Instance().GetVolumeForPersistentVolumeClaim(&pvc)
		if err != nil {
			return nil, fmt.Errorf("Error getting volume for PVC: %v", err)
		}
		volumeInfo.Volume = volume
		taskID := p.getBackupRestoreTaskID(backup.UID, volumeInfo.Namespace, volumeInfo.PersistentVolumeClaim)
		credID := p.getCredID(backup.Spec.BackupLocation, backup.Namespace)
		request := &api.CloudBackupCreateRequest{
			VolumeID:       volume,
			CredentialUUID: credID,
			Name:           taskID,
		}
		request.Labels = make(map[string]string)
		request.Labels[cloudBackupOwnerLabel] = "stork"
		if val, ok := backup.Annotations[skipBackupLocationNameCheckAnnotation]; ok {
			request.Labels[api.OptCloudBackupIgnoreCreds] = val
		}
		if val, ok := backup.Spec.Options[deleteLocalSnapAnnotation]; ok {
			skip, err := strconv.ParseBool(val)
			if err != nil {
				msg := fmt.Errorf("invalid value for delete local snapshot specified, %v", err.Error())
				log.ApplicationBackupLog(backup).Errorf(msg.Error())
				return nil, msg
			}
			if skip {
				log.ApplicationBackupLog(backup).Warnf("setting delete-local-snap to true")
				request.DeleteLocal = true
			}
		}
		if value, present := backup.Spec.Options[incrementalCountAnnotation]; present {
			incrementalCount, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid cloudsnap-incremental-count specified %v: %v", p, err)
			}
			if incrementalCount <= 0 {
				request.Full = true
				request.DeleteLocal = true
			} else {
				request.FullBackupFrequency = uint32(incrementalCount)
				request.Full = false
			}
		}

		p.addApplicationBackupCloudsnapInfo(request, backup)
		var cloudBackupCreateErr error
		err = wait.ExponentialBackoff(cloudBackupCreateBackoff, func() (bool, error) {
			_, cloudBackupCreateErr = volDriver.CloudBackupCreate(request)
			if cloudBackupCreateErr != nil {
				if _, ok := cloudBackupCreateErr.(*ost_errors.ErrExists); !ok {
					logrus.Warnf("failed to start backup for %v (%v/%v): %v, retrying again",
						volume, pvc.Namespace, pvc.Name, cloudBackupCreateErr)
					return false, nil
				}
			}
			return true, nil
		})

		if err != nil || cloudBackupCreateErr != nil {
			if isCloudBackupServerBusyError(cloudBackupCreateErr) {
				return volumeInfos, &storkvolume.ErrStorageProviderBusy{Reason: cloudBackupCreateErr.Error()}
			}
			if _, ok := cloudBackupCreateErr.(*ost_errors.ErrExists); !ok {
				return nil, fmt.Errorf("failed to start backup for %v (%v/%v): %v",
					volume, pvc.Namespace, pvc.Name, cloudBackupCreateErr)
			}
		} else if err == nil {
			// Only add volumeInfos if this was a successful backup
			volumeInfos = append(volumeInfos, volumeInfo)
		}
	}
	return volumeInfos, nil
}

func (p *portworx) GetBackupStatus(backup *storkapi.ApplicationBackup) ([]*storkapi.ApplicationBackupVolumeInfo, error) {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return nil, err
		}
	}

	volDriver, err := p.getUserVolDriver(backup.Annotations)
	if err != nil {
		return nil, err
	}

	cloudBackupClient, err := p.getCloudBackupClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), cloudBackupTimeout)
	defer cancel()
	// Get user token from secret if any
	ctx, err = p.getUserContext(ctx, backup.Annotations)
	if err != nil {
		return nil, err
	}

	volumeInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)
	for _, vInfo := range backup.Status.Volumes {
		if vInfo.DriverName != driverName {
			continue
		}
		taskID := p.getBackupRestoreTaskID(backup.UID, vInfo.Namespace, vInfo.PersistentVolumeClaim)
		csStatus := p.getCloudSnapStatus(volDriver, api.CloudBackupOp, taskID)
		vInfo.BackupID = csStatus.cloudSnapID
		if isCloudsnapStatusActive(csStatus.status) {
			vInfo.Status = storkapi.ApplicationBackupStatusInProgress
			vInfo.Reason = fmt.Sprintf("Volume backup in progress. BytesDone: %v BytesTotal: %v ETA: %v seconds",
				csStatus.bytesDone,
				csStatus.bytesTotal,
				csStatus.etaSeconds)
		} else if isCloudsnapStatusFailed(csStatus.status) {
			vInfo.Status = storkapi.ApplicationBackupStatusFailed
			vInfo.Reason = fmt.Sprintf("Backup failed for volume: %v", csStatus.msg)
		} else {
			vInfo.Status = storkapi.ApplicationBackupStatusSuccessful
			vInfo.Reason = "Backup successful for volume"
			vInfo.ActualSize = csStatus.bytesDone
			req := &api.SdkCloudBackupSizeRequest{
				BackupId:     csStatus.cloudSnapID,
				CredentialId: csStatus.credentialID,
			}
			resp, err := cloudBackupClient.Size(ctx, req)
			st, _ := status.FromError(err)
			if err != nil {
				// On error explicitly make the value to zero
				vInfo.TotalSize = 0
				if st.Code() == codes.Unimplemented {
					// if using old porx version which doesn't support this API, log an
					// warning and continue to next vol if available
					log.ApplicationBackupLog(backup).Warnf("Unsupported porx version to fetch backup size for backup ID %v: %v", csStatus.cloudSnapID, err)
				} else {
					log.ApplicationBackupLog(backup).Errorf("Failed to fetch backup size for backup ID %v: %v", csStatus.cloudSnapID, err)
					// TODO: Temporarily not returning error due to porx timing out to fetch size having more backups
					// Will be fixed once porx fixes the same
					// return nil, err
				}
			} else {
				vInfo.TotalSize = resp.GetSize()
			}
		}
		volumeInfos = append(volumeInfos, vInfo)
	}

	return volumeInfos, nil
}

func (p *portworx) DeleteBackup(backup *storkapi.ApplicationBackup) error {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return err
		}
	}

	volDriver, err := p.getUserVolDriver(backup.Annotations)
	if err != nil {
		return err
	}
	for _, vInfo := range backup.Status.Volumes {
		if vInfo.BackupID != "" {
			input := &api.CloudBackupDeleteRequest{
				ID:             vInfo.BackupID,
				CredentialUUID: p.getCredID(backup.Spec.BackupLocation, backup.Namespace),
			}
			if err := volDriver.CloudBackupDelete(input); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *portworx) CancelBackup(backup *storkapi.ApplicationBackup) error {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return err
		}
	}

	volDriver, err := p.getUserVolDriver(backup.Annotations)
	if err != nil {
		return err
	}
	for _, vInfo := range backup.Status.Volumes {
		taskID := p.getBackupRestoreTaskID(backup.UID, vInfo.Namespace, vInfo.PersistentVolumeClaim)
		if err := p.stopCloudBackupTask(volDriver, taskID); err != nil {
			return err
		}
	}
	return nil
}

func (p *portworx) stopCloudBackupTask(volDriver volume.VolumeDriver, taskID string) error {
	input := &api.CloudBackupStateChangeRequest{
		Name:           taskID,
		RequestedState: api.CloudBackupRequestedStateStop,
	}
	err := volDriver.CloudBackupStateChange(input)
	if err != nil {
		if strings.Contains(err.Error(), "Failed to change state: No active backup/restore for the volume") {
			return nil
		}
	}
	return nil
}

func (p *portworx) generatePVName() string {
	return pvNamePrefix + string(uuid.NewUUID())
}

func (p *portworx) GetPreRestoreResources(
	backup *storkapi.ApplicationBackup,
	objects []runtime.Unstructured,
) ([]runtime.Unstructured, error) {
	secretsToRestore := make(map[string]bool)
	objectsToRestore := make([]runtime.Unstructured, 0)

	for _, o := range objects {
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return nil, err
		}
		if objectType.GetKind() == "PersistentVolumeClaim" {
			metadata, err := meta.Accessor(o)
			if err != nil {
				return nil, err
			}
			annotations := metadata.GetAnnotations()
			if annotations == nil {
				continue
			}
			if secretName, present := annotations[pxSecretNameAnnotation]; present && secretName != "" {
				// Also pick up the namespace from the annotation, if absent
				// it'll be empty
				secretsToRestore[filepath.Join(annotations[pxSecretNamespaceAnnotation], secretName)] = true
			}
		}
	}
	if len(secretsToRestore) > 0 {
		for _, o := range objects {
			objectType, err := meta.TypeAccessor(o)
			if err != nil {
				return nil, err
			}
			if objectType.GetKind() == "Secret" {
				metadata, err := meta.Accessor(o)
				if err != nil {
					return nil, err
				}
				if secretsToRestore[filepath.Join(metadata.GetNamespace(), metadata.GetName())] {
					objectsToRestore = append(objectsToRestore, o)
				}
			}
		}
	}
	return objectsToRestore, nil
}

func (p *portworx) StartRestore(
	restore *storkapi.ApplicationRestore,
	volumeBackupInfos []*storkapi.ApplicationBackupVolumeInfo,
) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return nil, err
		}
	}

	ok, msg, err := p.ensureNodesHaveMinVersion("2.2.0")
	if err != nil {
		return nil, err
	}

	if !ok {
		err = &errors.ErrNotSupported{
			Feature: "ApplicationRestore",
			Reason:  "Only supported on PX version 2.2.0 onwards: " + msg,
		}

		return nil, err
	}

	volDriver, err := p.getUserVolDriver(restore.Annotations)
	if err != nil {
		return nil, err
	}
	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	for _, backupVolumeInfo := range volumeBackupInfos {
		volumeInfo := &storkapi.ApplicationRestoreVolumeInfo{}
		volumeInfo.PersistentVolumeClaim = backupVolumeInfo.PersistentVolumeClaim
		volumeInfo.SourceNamespace = backupVolumeInfo.Namespace
		volumeInfo.SourceVolume = backupVolumeInfo.Volume
		volumeInfo.RestoreVolume = p.generatePVName()
		volumeInfo.DriverName = driverName

		taskID := p.getBackupRestoreTaskID(restore.UID, volumeInfo.SourceNamespace, volumeInfo.PersistentVolumeClaim)
		credID := p.getCredID(restore.Spec.BackupLocation, restore.Namespace)
		request := &api.CloudBackupRestoreRequest{
			ID:                backupVolumeInfo.BackupID,
			RestoreVolumeName: volumeInfo.RestoreVolume,
			CredentialUUID:    credID,
			Name:              taskID,
			Spec:              &api.RestoreVolumeSpec{IoProfileBkupSrc: true},
		}

		var cloudRestoreErr error
		err = wait.ExponentialBackoff(cloudBackupCreateBackoff, func() (bool, error) {
			_, cloudRestoreErr = volDriver.CloudBackupRestore(request)
			if cloudRestoreErr != nil {
				if _, ok := cloudRestoreErr.(*ost_errors.ErrExists); !ok {
					return false, nil
				}
			}
			return true, nil
		})

		if err != nil || cloudRestoreErr != nil {
			if isCloudBackupServerBusyError(cloudRestoreErr) {
				return volumeInfos, &storkvolume.ErrStorageProviderBusy{Reason: cloudRestoreErr.Error()}
			}
			if _, ok := cloudRestoreErr.(*ost_errors.ErrExists); !ok {
				return nil, fmt.Errorf("failed to start restore for %v (%v/%v): %v",
					volumeInfo.SourceVolume, volumeInfo.SourceNamespace, volumeInfo.PersistentVolumeClaim, cloudRestoreErr)
			}
		} else if err == nil {
			// Only add volumeInfos if this was a successful backup
			volumeInfos = append(volumeInfos, volumeInfo)
		}
	}
	return volumeInfos, nil
}

func (p *portworx) GetRestoreStatus(restore *storkapi.ApplicationRestore) ([]*storkapi.ApplicationRestoreVolumeInfo, error) {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return nil, err
		}
	}

	volDriver, err := p.getUserVolDriver(restore.Annotations)
	if err != nil {
		return nil, err
	}

	volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	for _, vInfo := range restore.Status.Volumes {
		if vInfo.DriverName != driverName {
			continue
		}
		taskID := p.getBackupRestoreTaskID(restore.UID, vInfo.SourceNamespace, vInfo.PersistentVolumeClaim)
		csStatus := p.getCloudSnapStatus(volDriver, api.CloudRestoreOp, taskID)
		if isCloudsnapStatusActive(csStatus.status) {
			vInfo.Status = storkapi.ApplicationRestoreStatusInProgress
			vInfo.Reason = fmt.Sprintf("Volume restore in progress. BytesDone: %v BytesTotal: %v ETA: %v seconds",
				csStatus.bytesDone,
				csStatus.bytesTotal,
				csStatus.etaSeconds)
		} else if isCloudsnapStatusFailed(csStatus.status) {
			vInfo.Status = storkapi.ApplicationRestoreStatusFailed
			vInfo.Reason = fmt.Sprintf("Restore failed for volume: %v", csStatus.msg)
		} else {
			vInfo.TotalSize = csStatus.bytesDone
			vInfo.Status = storkapi.ApplicationRestoreStatusSuccessful
			vInfo.Reason = "Restore successful for volume"
		}
		volumeInfos = append(volumeInfos, vInfo)
	}

	return volumeInfos, nil
}

func (p *portworx) CancelRestore(restore *storkapi.ApplicationRestore) error {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return err
		}
	}

	volDriver, err := p.getUserVolDriver(restore.Annotations)
	if err != nil {
		return err
	}
	for _, vInfo := range restore.Status.Volumes {
		taskID := p.getBackupRestoreTaskID(restore.UID, vInfo.SourceNamespace, vInfo.PersistentVolumeClaim)
		if err := p.stopCloudBackupTask(volDriver, taskID); err != nil {
			return err
		}
	}
	return nil
}

func (p *portworx) CreateVolumeClones(clone *storkapi.ApplicationClone) error {
	if !p.initDone {
		if err := p.initPortworxClients(); err != nil {
			return err
		}
	}

	createdClones := make([]string, 0)
	volDriver, err := p.getUserVolDriver(clone.Annotations)
	if err != nil {
		return err
	}
	for _, vInfo := range clone.Status.Volumes {
		locator := &api.VolumeLocator{
			Name: vInfo.CloneVolume,
			VolumeLabels: map[string]string{
				pvcNameLabel:   vInfo.PersistentVolumeClaim,
				namespaceLabel: clone.Spec.DestinationNamespace,
			},
		}
		_, err := volDriver.Snapshot(vInfo.Volume, false, locator, true)
		if err != nil {
			// Mark this clone for deletion too if it already existed, so that
			// all clones can be recreated on the next try
			if isAlreadyExistsError(err) {
				createdClones = append(createdClones, vInfo.Volume)
			}
			// Delete the clones that we already created
			for _, cloneVolume := range createdClones {
				if err := volDriver.Delete(cloneVolume); err != nil {
					log.ApplicationCloneLog(clone).Warnf("error deleting cloned volume %v on failure: %v", cloneVolume, err)
				}
			}
			return fmt.Errorf("error creating clone %v for volume %v: %v", vInfo.CloneVolume, vInfo.Volume, err)
		}
		createdClones = append(createdClones, vInfo.CloneVolume)
	}
	// Update the status for all the volumes only once we are all done
	for _, vInfo := range clone.Status.Volumes {
		vInfo.Status = storkapi.ApplicationCloneStatusSuccessful
		vInfo.Reason = "Volume cloned succesfully"
	}
	return nil
}

func (p *portworx) createGroupLocalSnapFromPVCs(groupSnap *storkapi.GroupVolumeSnapshot, volNames []string, options map[string]string) (
	*storkvolume.GroupSnapshotCreateResponse, error) {
	volDriver, err := p.getUserVolDriver(groupSnap.Annotations)
	if err != nil {
		return nil, err
	}
	resp, err := volDriver.SnapshotGroup("", nil, volNames, true)
	if err != nil {
		return nil, err
	}

	if len(resp.Snapshots) == 0 {
		err = fmt.Errorf("found 0 snapshots using given group selector/ID. resp: %v", resp)
		return nil, err
	}

	failedSnapVols := make([]string, 0)
	snapIDs := make([]string, 0)
	snapIDsPendingRevert := make([]string, 0)
	response := &storkvolume.GroupSnapshotCreateResponse{
		Snapshots: make([]*storkapi.VolumeSnapshotStatus, 0),
	}

	// Loop through the response and check if all succeeded. Don't create any k8s objects if
	// any of the snapshots failed
	for volID, newSnapResp := range resp.Snapshots {
		if newSnapResp.GetVolumeCreateResponse() == nil {
			err = fmt.Errorf("portworx API returned empty response on creation of group snapshot")
			return nil, err
		}

		if newSnapResp.GetVolumeCreateResponse().GetVolumeResponse() != nil &&
			len(newSnapResp.GetVolumeCreateResponse().GetVolumeResponse().GetError()) != 0 {

			logrus.Errorf("failed to create snapshot for volume: %s due to: %v",
				volID, newSnapResp.GetVolumeCreateResponse().GetVolumeResponse().GetError())
			failedSnapVols = append(failedSnapVols, volID)

			p.revertPXSnaps(snapIDsPendingRevert)
			snapIDsPendingRevert = make([]string, 0)
			continue
		}

		newSnapID := newSnapResp.GetVolumeCreateResponse().GetId()
		snapIDs = append(snapIDs, newSnapID)
		snapIDsPendingRevert = append(snapIDsPendingRevert, newSnapID)

		parentVolID, err := p.findParentVolID(newSnapID)
		if err != nil {
			return nil, err
		}

		dataSource := &crdv1.VolumeSnapshotDataSource{
			PortworxSnapshot: &crdv1.PortworxVolumeSnapshotSource{
				SnapshotType: crdv1.PortworxSnapshotTypeLocal,
				SnapshotID:   newSnapID,
			},
		}

		snapshotResp := &storkapi.VolumeSnapshotStatus{
			ParentVolumeID: parentVolID,
			DataSource:     dataSource,
			Conditions:     getReadySnapshotConditions(),
		}

		response.Snapshots = append(response.Snapshots, snapshotResp)
	}

	if len(failedSnapVols) > 0 {
		err = fmt.Errorf("all snapshots in the group did not succeed. failed: %v succeeded: %v", failedSnapVols, snapIDs)
		return nil, err
	}

	return response, nil
}

// createGroupCloudSnapFromVolumes creates cloud group snapshots
func (p *portworx) createGroupCloudSnapFromVolumes(
	groupSnap *storkapi.GroupVolumeSnapshot,
	volNames []string,
	options map[string]string) (
	*storkvolume.GroupSnapshotCreateResponse, error) {
	volDriver, err := p.getUserVolDriver(groupSnap.Annotations)
	if err != nil {
		return nil, err
	}

	credID := getCredIDFromSnapshot(groupSnap.Spec.Options)

	resp, err := volDriver.CloudBackupGroupCreate(&api.CloudBackupGroupCreateRequest{
		CredentialUUID: credID,
		VolumeIDs:      volNames})
	if err != nil {
		return nil, err
	}

	if len(resp.Names) == 0 {
		return nil, fmt.Errorf("group cloudsnapshot request returned 0 tasks")
	}

	return p.generateStatusReponseFromTaskIDs(groupSnap, resp.Names, credID)
}

// getGroupCloudSnapStatus fetches the current group cloudsnapshot status by using the task ID in the given
// volumesnapshot object and returns the updated volumesnapshot object
func (p *portworx) getGroupCloudSnapStatus(snap *storkapi.GroupVolumeSnapshot) (
	*storkvolume.GroupSnapshotCreateResponse, error) {

	if len(snap.Status.VolumeSnapshots) == 0 {
		return nil, fmt.Errorf("group cloudsnapshot has 0 snapshots in status")
	}

	credID := getCredIDFromSnapshot(snap.Spec.Options)

	taskIDs := make([]string, 0)
	for _, snapshotStatus := range snap.Status.VolumeSnapshots {
		taskIDs = append(taskIDs, snapshotStatus.TaskID)
	}
	return p.generateStatusReponseFromTaskIDs(snap, taskIDs, credID)
}

func (p *portworx) generateStatusReponseFromTaskIDs(
	groupSnap *storkapi.GroupVolumeSnapshot, taskIDs []string, credID string) (
	*storkvolume.GroupSnapshotCreateResponse, error) {
	response := &storkvolume.GroupSnapshotCreateResponse{
		Snapshots: make([]*storkapi.VolumeSnapshotStatus, 0),
	}

	// Get volume client from user context
	volDriver, err := p.getUserVolDriver(groupSnap.Annotations)
	if err != nil {
		return nil, err
	}

	failedTasks := make([]string, 0)
	doneTasks := make([]string, 0)
	activeTasks := make([]string, 0)
	doneSnapIDs := make([]string, 0)
	activeSnapIDs := make([]string, 0)
	for _, taskID := range taskIDs {
		csStatus := p.getCloudSnapStatus(volDriver, api.CloudBackupOp, taskID)

		dataSource := &crdv1.VolumeSnapshotDataSource{
			PortworxSnapshot: &crdv1.PortworxVolumeSnapshotSource{
				SnapshotID:          csStatus.cloudSnapID,
				SnapshotType:        crdv1.PortworxSnapshotTypeCloud,
				SnapshotCloudCredID: credID,
			},
		}

		var conditions []crdv1.VolumeSnapshotCondition

		if isCloudsnapStatusFailed(csStatus.status) {
			log.GroupSnapshotLog(groupSnap).Errorf(csStatus.msg)
			failedTasks = append(failedTasks, taskID)

			p.revertPXCloudSnaps(doneSnapIDs, credID)
			p.revertPXCloudSnaps(activeSnapIDs, credID)
			doneSnapIDs = make([]string, 0)
			activeSnapIDs = make([]string, 0)

			conditions = *getErrorSnapshotConditions(fmt.Errorf(csStatus.msg))
		} else if csStatus.status == api.CloudBackupStatusDone {
			conditions = getReadySnapshotConditions()
			doneTasks = append(doneTasks, taskID)
			doneSnapIDs = append(doneSnapIDs, csStatus.cloudSnapID)
		} else if csStatus.status != api.CloudBackupStatusNotStarted {
			conditions = getPendingSnapshotConditions(csStatus.msg)
			activeTasks = append(activeTasks, taskID)
			activeSnapIDs = append(activeSnapIDs, csStatus.cloudSnapID)
		}

		snapshotResp := &storkapi.VolumeSnapshotStatus{
			TaskID:         taskID,
			ParentVolumeID: csStatus.sourceVolumeID,
			DataSource:     dataSource,
			Conditions:     conditions,
		}

		response.Snapshots = append(response.Snapshots, snapshotResp)
	}

	if len(failedTasks) > 0 {
		log.GroupSnapshotLog(groupSnap).Errorf("all snapshots in the group did not succeed. failed: %v done: %v active: %v",
			failedTasks, doneTasks, activeTasks)
	}

	return response, nil
}

// revertPXCloudSnaps deletes all cloudsnaps with given IDs
func (p *portworx) revertPXCloudSnaps(cloudSnapIDs []string, credID string) {
	volDriver, err := p.getAdminVolDriver()
	if err != nil {
		logrus.Errorf("Failed to get a volumeDriver: %v", err)
		return
	}

	if len(cloudSnapIDs) == 0 {
		return
	}

	failedDeletions := make(map[string]error)
	for _, cloudSnapID := range cloudSnapIDs {
		input := &api.CloudBackupDeleteRequest{
			ID:             cloudSnapID,
			CredentialUUID: credID,
		}
		err := volDriver.CloudBackupDelete(input)
		if err != nil {
			failedDeletions[cloudSnapID] = err
		}
	}

	if len(failedDeletions) > 0 {
		errString := ""
		for failedID, failedErr := range failedDeletions {
			errString = fmt.Sprintf("%s delete of %s failed due to err: %v.\n", errString, failedID, failedErr)
		}

		err := fmt.Errorf("failed to revert created PX snapshots. err: %s", errString)
		logrus.Errorf(err.Error())
	} else {
		logrus.Infof("Successfully reverted PX cloudsnap snapshots")
	}
}

// ensureNodesHaveMinVersion ensures that all PX nodes are atleast running the given minVersionStr
func (p *portworx) ensureNodesHaveMinVersion(minVersionStr string) (bool, string, error) {
	minVersion, err := version.NewVersion(minVersionStr)
	if err != nil {
		return false, "", err
	}

	clusterManager, err := p.getClusterManagerClient()
	if err != nil {
		return false, "", fmt.Errorf("cannot get cluster manager, err: %s", err.Error())
	}

	result, err := clusterManager.Enumerate()
	if err != nil {
		return false, "", err
	}

	pxVerRegex := regexp.MustCompile(`^(\d+\.\d+\.\d+).*`)
	for _, node := range result.Nodes {
		if p.mapNodeStatus(node.Status) != storkvolume.NodeOnline {
			continue
		}
		if nodeVersion, ok := node.NodeLabels["PX Version"]; ok {
			matches := pxVerRegex.FindStringSubmatch(nodeVersion)
			if len(matches) < 2 {
				return false, "", fmt.Errorf("failed to parse PX version: %s on node", nodeVersion)
			}

			currentVer, err := version.NewVersion(matches[1])
			if err != nil {
				return false, "", err
			}

			if currentVer.LessThan(minVersion) {
				return false, fmt.Sprintf("node: %s has version: %s", node.Hostname, nodeVersion), nil
			}
		}
	}

	return true, "all nodes have expected version", nil
}

func (p *portworx) verifyPortworxPv(pv *v1.PersistentVolume) error {
	_, err := p.getVolumeIDFromPV(pv)
	return err
}

func (p *portworx) getVolumeIDFromPV(pv *v1.PersistentVolume) (string, error) {
	if pv == nil {
		return "", fmt.Errorf("nil PV passed into getVolumeIDFromPV")
	}

	var id string
	if pv.Spec.CSI != nil {
		if !isCsiProvisioner(pv.Spec.CSI.Driver) {
			return "", fmt.Errorf("PV contains unsupported driver name: %s",
				pv.Spec.CSI.Driver)
		}
		id = pv.Spec.CSI.VolumeHandle
	} else if pv.Spec.PortworxVolume != nil {
		id = pv.Spec.PortworxVolume.VolumeID
	} else {
		return "", fmt.Errorf("invalid PV: %v", pv)
	}

	if len(id) == 0 {
		return "", fmt.Errorf("invalid PV, no volume id found: %v", pv)
	}
	return id, nil
}

func isCsiProvisioner(provisioner string) bool {
	return provisioner == crdv1.PortworxCsiProvisionerName ||
		provisioner == crdv1.PortworxCsiDeprecatedProvisionerName
}

func isCloudsnapStatusFailed(st api.CloudBackupStatusType) bool {
	return st == api.CloudBackupStatusFailed ||
		st == api.CloudBackupStatusStopped ||
		st == api.CloudBackupStatusAborted
}

func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}

	return strings.HasSuffix(err.Error(), "already exists")
}

func isCloudsnapStatusActive(st api.CloudBackupStatusType) bool {
	return st == api.CloudBackupStatusNotStarted ||
		st == api.CloudBackupStatusQueued ||
		st == api.CloudBackupStatusActive ||
		st == api.CloudBackupStatusPaused
}

// getDriverTypeFromPV returns the name of the provisioner driver managing
// the persistent volume. Supports in-tree and CSI PVs
func getDriverTypeFromPV(pv *v1.PersistentVolume) (string, error) {
	var volumeType string

	// Check for CSI
	if pv.Spec.CSI != nil {
		volumeType = pv.Spec.CSI.Driver
		if len(volumeType) == 0 {
			return "", fmt.Errorf("CSI Driver not found in PV %#v", *pv)
		}
		return volumeType, nil
	}

	// Fall back to Kubernetes in-tree driver names
	volumeType = crdv1.GetSupportedVolumeFromPVSpec(&pv.Spec)
	if len(volumeType) == 0 {
		return "", fmt.Errorf("unsupported volume type found in PV %#v", *pv)
	}

	return volumeType, nil
}

func isCloudBackupServerBusyError(err error) bool {
	if err == nil {
		return false
	}
	cloudBackupErr := &ost_errors.ErrCloudBackupServerBusy{}
	return strings.Contains(err.Error(), cloudBackupErr.Error())
}

func init() {
	p := &portworx{}
	err := p.Init(nil)
	if err != nil {
		logrus.Debugf("Error init'ing portworx driver: %v", err)
	}

	if err := storkvolume.Register(driverName, p); err != nil {
		logrus.Panicf("Error registering portworx volume driver: %v", err)
	}
}
