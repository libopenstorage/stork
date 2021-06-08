package portworx

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/hashicorp/go-version"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/openstorage/api/client"
	clusterclient "github.com/libopenstorage/openstorage/api/client/cluster"
	"github.com/libopenstorage/openstorage/api/spec"
	"github.com/libopenstorage/openstorage/cluster"
	"github.com/pborman/uuid"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	driver_api "github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/node"
	torpedok8s "github.com/portworx/torpedo/drivers/scheduler/k8s"
	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/portworx/torpedo/pkg/aututils"
	tp_errors "github.com/portworx/torpedo/pkg/errors"
	"github.com/portworx/torpedo/pkg/osutils"
	"github.com/portworx/torpedo/pkg/units"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	// PortworxStorage portworx storage name
	PortworxStorage torpedovolume.StorageProvisionerType = "portworx"
	// PortworxCsi csi storage name
	PortworxCsi torpedovolume.StorageProvisionerType = "csi"
)

const (
	// DriverName is the name of the portworx driver implementation
	DriverName                  = "pxd"
	pxDiagPath                  = "/remotediags"
	pxVersionLabel              = "PX Version"
	enterMaintenancePath        = "/entermaintenance"
	exitMaintenancePath         = "/exitmaintenance"
	pxSystemdServiceName        = "portworx.service"
	tokenKey                    = "token"
	clusterIP                   = "ip"
	clusterPort                 = "port"
	remoteKubeConfigPath        = "/tmp/kubeconfig"
	pxMinVersionForStorkUpgrade = "2.1"
)

const (
	defaultTimeout                    = 2 * time.Minute
	defaultRetryInterval              = 10 * time.Second
	maintenanceOpTimeout              = 1 * time.Minute
	maintenanceWaitTimeout            = 2 * time.Minute
	inspectVolumeTimeout              = 30 * time.Second
	inspectVolumeRetryInterval        = 2 * time.Second
	validateDeleteVolumeTimeout       = 3 * time.Minute
	validateReplicationUpdateTimeout  = 10 * time.Minute
	validateClusterStartTimeout       = 2 * time.Minute
	validatePXStartTimeout            = 5 * time.Minute
	validateNodeStopTimeout           = 5 * time.Minute
	validateStoragePoolSizeTimeout    = 3 * time.Hour
	validateStoragePoolSizeInterval   = 30 * time.Second
	getNodeTimeout                    = 3 * time.Minute
	getNodeRetryInterval              = 5 * time.Second
	stopDriverTimeout                 = 5 * time.Minute
	crashDriverTimeout                = 2 * time.Minute
	startDriverTimeout                = 2 * time.Minute
	upgradeTimeout                    = 10 * time.Minute
	upgradeRetryInterval              = 30 * time.Second
	upgradePerNodeTimeout             = 15 * time.Minute
	waitVolDriverToCrash              = 1 * time.Minute
	waitDriverDownOnNodeRetryInterval = 2 * time.Second
)

const (
	secretName      = "openstorage.io/auth-secret-name"
	secretNamespace = "openstorage.io/auth-secret-namespace"
)

// Provisioners types of supported provisioners
var provisioners = map[torpedovolume.StorageProvisionerType]torpedovolume.StorageProvisionerType{
	PortworxStorage: "kubernetes.io/portworx-volume",
	PortworxCsi:     "pxd.portworx.com",
}

var deleteVolumeLabelList = []string{
	"auth-token",
	"pv.kubernetes.io",
	"volume.beta.kubernetes.io",
	"kubectl.kubernetes.io",
	"volume.kubernetes.io",
	"pvc_name",
	"pvc_namespace",
	torpedok8s.CsiProvisionerSecretName,
	torpedok8s.CsiProvisionerSecretNamespace,
	torpedok8s.CsiNodePublishSecretName,
	torpedok8s.CsiNodePublishSecretNamespace,
	torpedok8s.CsiControllerExpandSecretName,
	torpedok8s.CsiControllerExpandSecretNamespace,
}

var k8sCore = core.Instance()

type portworx struct {
	legacyClusterManager cluster.Cluster
	clusterManager       api.OpenStorageClusterClient
	nodeManager          api.OpenStorageNodeClient
	mountAttachManager   api.OpenStorageMountAttachClient
	volDriver            api.OpenStorageVolumeClient
	clusterPairManager   api.OpenStorageClusterPairClient
	alertsManager        api.OpenStorageAlertsClient
	csbackupManager      api.OpenStorageCloudBackupClient
	schedOps             schedops.Driver
	nodeDriver           node.Driver
	refreshEndpoint      bool
	token                string
}

// TODO temporary solution until sdk supports metadataNode response
type metadataNode struct {
	PeerUrls   []string `json:"PeerUrls"`
	ClientUrls []string `json:"ClientUrls"`
	Leader     bool     `json:"Leader"`
	DbSize     int      `json:"DbSize"`
	IsHealthy  bool     `json:"IsHealthy"`
	ID         string   `json:"ID"`
}

// DiagRequestConfig is a request object which provides all the configuration details
// to PX for running diagnostics on a node. This object can also be passed over
// the wire through an API server for remote diag requests.
type DiagRequestConfig struct {
	// OutputFile for the diags.tgz
	OutputFile string
	// DockerHost config
	DockerHost string
	// ContainerName for PX
	ContainerName string
	// ExecPath of the program making this request (pxctl)
	ExecPath string
	// Profile when set diags command only dumps the go profile
	Profile bool
	// Live gets live diagnostics
	Live bool
	// Upload uploads the diags.tgz to s3
	Upload bool
	// All gets all possible diagnostics from PX
	All bool
	// Force overwrite of existing diags file.
	Force bool
	// OnHost indicates whether diags is being run on the host
	// or inside the container
	OnHost bool
	// Token for security authentication (if enabled)of the program making this request (pxctl)
	Token string
	// Extra indicates whether diags should attempt to collect extra information
	Extra bool
}

func (d *portworx) String() string {
	return DriverName
}

func (d *portworx) Init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap string) error {
	logrus.Infof("Using the Portworx volume driver with provisioner %s under scheduler: %v", storageProvisioner, sched)
	var err error

	d.token = token

	if d.nodeDriver, err = node.Get(nodeDriver); err != nil {
		return err
	}

	if d.schedOps, err = schedops.Get(sched); err != nil {
		return fmt.Errorf("failed to get scheduler operator for portworx. Err: %v", err)
	}

	if err = d.setDriver(); err != nil {
		return err
	}

	storageNodes, err := d.getStorageNodesOnStart()
	if err != nil {
		return err
	}

	if len(storageNodes) == 0 {
		return fmt.Errorf("cluster inspect returned empty nodes")
	}

	err = d.updateNodes(storageNodes)
	if err != nil {
		return err
	}
	for _, n := range node.GetStorageDriverNodes() {
		if err = d.WaitDriverUpOnNode(n, validatePXStartTimeout); err != nil {
			return err
		}
	}

	logrus.Infof("The following Portworx nodes are in the cluster:")
	for _, n := range storageNodes {
		logrus.Infof(
			"Node UID: %v Node IP: %v Node Status: %v",
			n.Id,
			n.DataIp,
			n.Status,
		)
	}
	torpedovolume.StorageDriver = DriverName
	// Set provisioner for torpedo
	if storageProvisioner != "" {
		if p, ok := provisioners[torpedovolume.StorageProvisionerType(storageProvisioner)]; ok {
			torpedovolume.StorageProvisioner = p
		} else {
			return fmt.Errorf("driver %s, does not support provisioner %s", DriverName, storageProvisioner)
		}
	} else {
		torpedovolume.StorageProvisioner = provisioners[torpedovolume.DefaultStorageProvisioner]
	}
	return nil
}

func (d *portworx) RefreshDriverEndpoints() error {
	// Force update px endpoints
	d.refreshEndpoint = true
	storageNodes, err := d.getStorageNodesOnStart()
	if err != nil {
		return err
	}

	if len(storageNodes) == 0 {
		return fmt.Errorf("cluster inspect returned empty nodes")
	}

	err = d.updateNodes(storageNodes)
	if err != nil {
		return err
	}
	return nil
}

func (d *portworx) updateNodes(pxNodes []api.StorageNode) error {
	for _, n := range node.GetNodes() {
		if err := d.updateNode(&n, pxNodes); err != nil {
			return err
		}
	}

	return nil
}

func (d *portworx) updateNode(n *node.Node, pxNodes []api.StorageNode) error {
	logrus.Infof("Updating node: %+v", *n)
	isPX, err := d.schedOps.IsPXEnabled(*n)
	if err != nil {
		return err
	}

	// No need to check in pxNodes if px is not installed
	if !isPX {
		return nil
	}

	for _, address := range n.Addresses {
		for _, pxNode := range pxNodes {
			logrus.Infof("Checking PX node %+v for address %s", pxNode, address)
			if address == pxNode.DataIp || address == pxNode.MgmtIp || n.Name == pxNode.SchedulerNodeName {
				if len(pxNode.Id) > 0 {
					n.StorageNode = pxNode
					n.VolDriverNodeID = pxNode.Id
					n.IsStorageDriverInstalled = isPX
					// TODO: PTX-2445 Replace isMetadataNode API call with SDK call
					isMetadataNode, err := d.isMetadataNode(*n, address)
					if err != nil {
						logrus.Warnf("can not check if %v is metadata node", *n)
					}
					n.IsMetadataNode = isMetadataNode

					if n.StoragePools == nil {
						for _, pxNodePool := range pxNode.Pools {
							storagePool := node.StoragePool{
								StoragePool:       pxNodePool,
								StoragePoolAtInit: pxNodePool,
							}
							n.StoragePools = append(n.StoragePools, storagePool)
						}
					} else {
						for idx, nodeStoragePool := range n.StoragePools {
							for _, pxNodePool := range pxNode.Pools {
								if nodeStoragePool.Uuid == pxNodePool.Uuid {
									n.StoragePools[idx].StoragePool = pxNodePool
								}
							}
						}
					}
					if err = node.UpdateNode(*n); err != nil {
						return fmt.Errorf("failed to update node %s. Cause: %v", n.Name, err)
					}
				} else {
					return fmt.Errorf("StorageNodeId is empty for node %v", pxNode)
				}
				return nil
			}
		}
	}

	// Return error where PX is not explicitly disabled but was not found installed
	return fmt.Errorf("failed to find px node for node: %v PX nodes: %v", n, pxNodes)
}

func (d *portworx) isMetadataNode(node node.Node, address string) (bool, error) {
	members, err := d.getKvdbMembers(node)
	if err != nil {
		return false, fmt.Errorf("failed to get metadata nodes. Cause: %v", err)
	}

	ipRegex := regexp.MustCompile(`http://(?P<address>.*):d+`)
	for _, value := range members {
		for _, url := range value.ClientUrls {
			result := getGroupMatches(ipRegex, url)
			if val, ok := result["address"]; ok && address == val {
				logrus.Debugf("Node %s is a metadata node", node.Name)
				return true, nil
			}
		}
	}
	return false, nil
}

func (d *portworx) CleanupVolume(volumeName string) error {
	volDriver := d.getVolDriver()
	volumes, err := volDriver.Enumerate(d.getContext(), &api.SdkVolumeEnumerateRequest{}, nil)
	if err != nil {
		return err
	}

	for _, volumeID := range volumes.GetVolumeIds() {
		volumeInspectResponse, err := volDriver.Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volumeID})
		if err != nil {
			return err
		}
		pxVolume := volumeInspectResponse.Volume
		if pxVolume.Locator.Name == volumeName {
			// First unmount this volume at all mount paths...
			for _, path := range pxVolume.AttachPath {
				if _, err = d.getMountAttachManager().Unmount(d.getContext(), &api.SdkVolumeUnmountRequest{VolumeId: pxVolume.Id, MountPath: path}); err != nil {
					err = fmt.Errorf(
						"error while unmounting %v at %v because of: %v",
						pxVolume.Id,
						path,
						err,
					)
					logrus.Infof("%v", err)
					return err
				}
			}

			if _, err = d.mountAttachManager.Detach(d.getContext(), &api.SdkVolumeDetachRequest{VolumeId: pxVolume.Id}); err != nil {
				err = fmt.Errorf(
					"error while detaching %v because of: %v",
					pxVolume.Id,
					err,
				)
				logrus.Infof("%v", err)
				return err
			}

			if _, err := volDriver.Delete(d.getContext(), &api.SdkVolumeDeleteRequest{VolumeId: pxVolume.Id}); err != nil {
				err = fmt.Errorf(
					"error while deleting %v because of: %v",
					pxVolume.Id,
					err,
				)
				logrus.Infof("%v", err)
				return err
			}

			logrus.Infof("successfully removed Portworx volume %v", volumeName)

			return nil
		}
	}

	return nil
}

func (d *portworx) getPxNode(n *node.Node, nManager ...api.OpenStorageNodeClient) (api.StorageNode, error) {
	if len(nManager) == 0 {
		nManager = []api.OpenStorageNodeClient{d.getNodeManager()}
	}
	logrus.Debugf("Inspecting node [%s] with volume driver node id [%s]", n.Name, n.VolDriverNodeID)
	nodeInspectResponse, err := nManager[0].Inspect(d.getContext(), &api.SdkNodeInspectRequest{NodeId: n.VolDriverNodeID})
	if err != nil {
		if isNodeNotFound(err) {
			logrus.Warnf("node %s with ID %s not found, trying to update node ID...", n.Name, n.VolDriverNodeID)
			n, err = d.updateNodeID(n, nManager...)
			if err == nil {
				return d.getPxNode(n, nManager...)
			}
		}
		return api.StorageNode{Status: api.Status_STATUS_NONE}, err
	}
	return *nodeInspectResponse.Node, nil
}

func isNodeNotFound(err error) bool {
	st, _ := status.FromError(err)
	// TODO when a node is not found sometimes we get an error code internal, as workaround we check for internal error and substring
	return err != nil && (st.Code() == codes.NotFound || (st.Code() == codes.Internal && strings.Contains(err.Error(), "Unable to locate node")))
}

func (d *portworx) getPxVersionOnNode(n node.Node, nodeManager ...api.OpenStorageNodeClient) (string, error) {

	t := func() (interface{}, bool, error) {
		logrus.Debugf("Getting PX Version on node [%s]", n.Name)
		pxNode, err := d.getPxNode(&n, nodeManager...)
		if err != nil {
			return "", false, err
		}
		if pxNode.Status != api.Status_STATUS_OK {
			return "", true, fmt.Errorf("px cluster is usable but node status is not ok. Expected: %v Actual: %v",
				api.Status_STATUS_OK, pxNode.Status)
		}
		pxVersion := pxNode.NodeLabels[pxVersionLabel]
		return pxVersion, false, nil
	}
	pxVersion, err := task.DoRetryWithTimeout(t, getNodeTimeout, getNodeRetryInterval)
	if err != nil {
		return "", fmt.Errorf("Timeout after %v waiting to get PX Version", getNodeTimeout)
	}
	return fmt.Sprintf("%v", pxVersion), nil
}

func (d *portworx) GetStorageDevices(n node.Node) ([]string, error) {
	pxNode, err := d.getPxNode(&n)
	if err != nil {
		return nil, err
	}

	devPaths := make([]string, 0)
	for _, value := range pxNode.Disks {
		devPaths = append(devPaths, value.Path)
	}
	return devPaths, nil
}

func (d *portworx) RecoverDriver(n node.Node) error {

	t := func() (interface{}, bool, error) {
		if err := d.maintenanceOp(n, enterMaintenancePath); err != nil {
			return nil, true, err
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, maintenanceOpTimeout, defaultRetryInterval); err != nil {
		return err
	}
	t = func() (interface{}, bool, error) {
		apiNode, err := d.getPxNode(&n)
		if err != nil {
			return nil, true, err
		}
		if apiNode.Status == api.Status_STATUS_MAINTENANCE {
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("Node %v is not in Maintenance mode", n.Name)
	}

	if _, err := task.DoRetryWithTimeout(t, maintenanceWaitTimeout, defaultRetryInterval); err != nil {
		return &ErrFailedToRecoverDriver{
			Node:  n,
			Cause: err.Error(),
		}
	}
	t = func() (interface{}, bool, error) {
		if err := d.maintenanceOp(n, exitMaintenancePath); err != nil {
			return nil, true, err
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, maintenanceOpTimeout, defaultRetryInterval); err != nil {
		return err
	}

	t = func() (interface{}, bool, error) {
		apiNode, err := d.getPxNode(&n)
		if err != nil {
			return nil, true, err
		}
		if apiNode.Status == api.Status_STATUS_OK {
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("Node %v is not up after exiting  Maintenance mode", n.Name)
	}

	if _, err := task.DoRetryWithTimeout(t, maintenanceWaitTimeout, defaultRetryInterval); err != nil {
		return err
	}

	return nil
}

func (d *portworx) ValidateCreateVolume(volumeName string, params map[string]string) error {
	var token string
	token = d.getTokenForVolume(volumeName, params)
	if val, hasKey := params["refresh-endpoint"]; hasKey {
		refreshEndpoint, _ := strconv.ParseBool(val)
		d.refreshEndpoint = refreshEndpoint
	}
	volDriver := d.getVolDriver()
	t := func() (interface{}, bool, error) {
		volumeInspectResponse, err := volDriver.Inspect(d.getContextWithToken(context.Background(), token), &api.SdkVolumeInspectRequest{VolumeId: volumeName})
		if err != nil {
			return nil, true, err
		}
		return volumeInspectResponse.Volume, false, nil
	}

	out, err := task.DoRetryWithTimeout(t, inspectVolumeTimeout, inspectVolumeRetryInterval)
	if err != nil {
		return &ErrFailedToInspectVolume{
			ID:    volumeName,
			Cause: fmt.Sprintf("Volume inspect returned err: %v", err),
		}
	}

	vol := out.(*api.Volume)

	// Status
	if vol.Status != api.VolumeStatus_VOLUME_STATUS_UP {
		return &ErrFailedToInspectVolume{
			ID: volumeName,
			Cause: fmt.Sprintf("Volume has invalid status. Expected:%v Actual:%v",
				api.VolumeStatus_VOLUME_STATUS_UP, vol.Status),
		}
	}

	// State
	if vol.State == api.VolumeState_VOLUME_STATE_ERROR || vol.State == api.VolumeState_VOLUME_STATE_DELETED {
		return &ErrFailedToInspectVolume{
			ID:    volumeName,
			Cause: fmt.Sprintf("Volume has invalid state. Actual:%v", vol.State),
		}
	}

	// if the volume is a clone or a snap, validate it's parent
	if vol.IsSnapshot() || vol.IsClone() {
		parentResp, err := volDriver.Inspect(d.getContextWithToken(context.Background(), token), &api.SdkVolumeInspectRequest{VolumeId: vol.Source.Parent})
		if err != nil {
			return &ErrFailedToInspectVolume{
				ID:    volumeName,
				Cause: fmt.Sprintf("Could not get parent with ID [%s]", vol.Source.Parent),
			}
		}
		if err := d.schedOps.ValidateSnapshot(params, parentResp.Volume); err != nil {
			return &ErrFailedToInspectVolume{
				ID:    volumeName,
				Cause: fmt.Sprintf("Snapshot/Clone validation failed. %v", err),
			}
		}
		return nil
	}

	// Labels
	var pxNodes []api.StorageNode
	for _, rs := range vol.ReplicaSets {
		for _, n := range rs.Nodes {
			nodeResponse, err := d.getNodeManager().Inspect(d.getContextWithToken(context.Background(), token), &api.SdkNodeInspectRequest{NodeId: n})
			if err != nil {
				return &ErrFailedToInspectVolume{
					ID:    volumeName,
					Cause: fmt.Sprintf("Failed to inspect replica set node: %s err: %v", n, err),
				}
			}

			pxNodes = append(pxNodes, *nodeResponse.Node)
		}
	}

	// Spec
	requestedSpec, requestedLocator, _, err := spec.NewSpecHandler().SpecFromOpts(params)
	if err != nil {
		return &ErrFailedToInspectVolume{
			ID:    volumeName,
			Cause: fmt.Sprintf("failed to parse requested spec of volume. Err: %v", err),
		}
	}

	delete(vol.Locator.VolumeLabels, "pvc") // special handling for the new pvc label added in k8s
	deleteLabelsFromRequestedSpec(requestedLocator)

	// Params/Options
	for k, v := range params {
		switch k {
		case api.SpecNodes:
			if !reflect.DeepEqual(v, vol.Spec.ReplicaSet.Nodes) {
				return errFailedToInspectVolume(volumeName, k, v, vol.Spec.ReplicaSet.Nodes)
			}
		case api.SpecParent:
			if v != vol.Source.Parent {
				return errFailedToInspectVolume(volumeName, k, v, vol.Source.Parent)
			}
		case api.SpecEphemeral:
			if requestedSpec.Ephemeral != vol.Spec.Ephemeral {
				return errFailedToInspectVolume(volumeName, k, requestedSpec.Ephemeral, vol.Spec.Ephemeral)
			}
		case api.SpecFilesystem:
			if requestedSpec.Format != vol.Spec.Format {
				return errFailedToInspectVolume(volumeName, k, requestedSpec.Format, vol.Spec.Format)
			}
		case api.SpecBlockSize:
			if requestedSpec.BlockSize != vol.Spec.BlockSize {
				return errFailedToInspectVolume(volumeName, k, requestedSpec.BlockSize, vol.Spec.BlockSize)
			}
		case api.SpecHaLevel:
			if requestedSpec.HaLevel != vol.Spec.HaLevel {
				return errFailedToInspectVolume(volumeName, k, requestedSpec.HaLevel, vol.Spec.HaLevel)
			}
		case api.SpecPriorityAlias:
			// Since IO priority isn't guaranteed, we aren't validating it here.
		case api.SpecSnapshotInterval:
			if requestedSpec.SnapshotInterval != vol.Spec.SnapshotInterval {
				return errFailedToInspectVolume(volumeName, k, requestedSpec.SnapshotInterval, vol.Spec.SnapshotInterval)
			}
		case api.SpecSnapshotSchedule:
			// TODO currently volume spec has a different format than request
			// i.e request "daily=12:00,7" turns into "- freq: daily\n  hour: 12\n  retain: 7\n" in volume spec
			// if requestedSpec.SnapshotSchedule != vol.Spec.SnapshotSchedule {
			//	return errFailedToInspectVolume(name, k, requestedSpec.SnapshotSchedule, vol.Spec.SnapshotSchedule)
			// }
		case api.SpecAggregationLevel:
			if requestedSpec.AggregationLevel != vol.Spec.AggregationLevel {
				return errFailedToInspectVolume(volumeName, k, requestedSpec.AggregationLevel, vol.Spec.AggregationLevel)
			}
		case api.SpecShared:
			if requestedSpec.Shared != vol.Spec.Shared {
				return errFailedToInspectVolume(volumeName, k, requestedSpec.Shared, vol.Spec.Shared)
			}
		case api.SpecSticky:
			if requestedSpec.Sticky != vol.Spec.Sticky {
				return errFailedToInspectVolume(volumeName, k, requestedSpec.Sticky, vol.Spec.Sticky)
			}
		case api.SpecGroup:
			if !reflect.DeepEqual(requestedSpec.Group, vol.Spec.Group) {
				return errFailedToInspectVolume(volumeName, k, requestedSpec.Group, vol.Spec.Group)
			}
		case api.SpecGroupEnforce:
			if requestedSpec.GroupEnforced != vol.Spec.GroupEnforced {
				return errFailedToInspectVolume(volumeName, k, requestedSpec.GroupEnforced, vol.Spec.GroupEnforced)
			}
		// portworx injects pvc name and namespace labels so response object won't be equal to request
		case api.SpecLabels:
			for requestedLabelKey, requestedLabelValue := range requestedLocator.VolumeLabels {
				// check requested label is not in 'ignore' list
				if labelValue, exists := vol.Locator.VolumeLabels[requestedLabelKey]; !exists || requestedLabelValue != labelValue {
					return errFailedToInspectVolume(volumeName, k, requestedLocator.VolumeLabels, vol.Locator.VolumeLabels)
				}
			}
		case api.SpecIoProfile:
			if requestedSpec.IoProfile != vol.Spec.IoProfile {
				return errFailedToInspectVolume(volumeName, k, requestedSpec.IoProfile, vol.Spec.IoProfile)
			}
		case api.SpecSize:
			if requestedSpec.Size != vol.Spec.Size {
				return errFailedToInspectVolume(volumeName, k, requestedSpec.Size, vol.Spec.Size)
			}
		default:
		}
	}

	logrus.Infof("Successfully inspected volume: %v (%v)", vol.Locator.Name, vol.Id)
	return nil
}

func (d *portworx) ValidateUpdateVolume(vol *torpedovolume.Volume, params map[string]string) error {
	var token string
	volumeName := d.schedOps.GetVolumeName(vol)
	token = d.getTokenForVolume(volumeName, params)
	t := func() (interface{}, bool, error) {
		volumeInspectResponse, err := d.getVolDriver().Inspect(d.getContextWithToken(context.Background(), token), &api.SdkVolumeInspectRequest{VolumeId: volumeName})
		if err != nil {
			return nil, true, err
		}

		respVol := volumeInspectResponse.Volume

		// Size Update
		if respVol.Spec.Size != vol.RequestedSize {
			return nil, true, &ErrFailedToInspectVolume{
				ID: volumeName,
				Cause: fmt.Sprintf("Volume size differs. Expected:%v Actual:%v",
					vol.RequestedSize, respVol.Spec.Size),
			}
		}
		return nil, false, nil
	}

	_, err := task.DoRetryWithTimeout(t, inspectVolumeTimeout, inspectVolumeRetryInterval)
	if err != nil {
		return &ErrFailedToInspectVolume{
			ID:    volumeName,
			Cause: fmt.Sprintf("Volume inspect returned err: %v", err),
		}
	}

	return nil
}

func errIsNotFound(err error) bool {
	statusErr, _ := status.FromError(err)
	return statusErr.Code() == codes.NotFound || strings.Contains(err.Error(), "code = NotFound")
}

func (d *portworx) ValidateDeleteVolume(vol *torpedovolume.Volume) error {
	volumeName := d.schedOps.GetVolumeName(vol)
	t := func() (interface{}, bool, error) {
		volumeInspectResponse, err := d.getVolDriver().Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volumeName})
		if err != nil && errIsNotFound(err) {
			return nil, false, nil
		} else if err != nil {
			return nil, true, err
		}
		// TODO remove shared validation when PWX-6894 and PWX-8790 are fixed
		if volumeInspectResponse.Volume != nil && !vol.Shared {
			return nil, true, fmt.Errorf("Volume %v is not yet removed from the system", volumeName)
		}
		return nil, false, nil
	}

	_, err := task.DoRetryWithTimeout(t, validateDeleteVolumeTimeout, defaultRetryInterval)
	if err != nil {
		return &ErrFailedToDeleteVolume{
			ID:    volumeName,
			Cause: err.Error(),
		}
	}

	return nil
}

func (d *portworx) ValidateVolumeCleanup() error {
	return d.schedOps.ValidateVolumeCleanup(d.nodeDriver)
}

func (d *portworx) ValidateVolumeSetup(vol *torpedovolume.Volume) error {
	return d.schedOps.ValidateVolumeSetup(vol, d.nodeDriver)
}

func (d *portworx) StopDriver(nodes []node.Node, force bool, triggerOpts *driver_api.TriggerOptions) error {
	stopFn := func() error {
		var err error
		for _, n := range nodes {
			logrus.Infof("Stopping volume driver on %s.", n.Name)
			if force {
				pxCrashCmd := "sudo pkill -9 px-storage"
				_, err = d.nodeDriver.RunCommand(n, pxCrashCmd, node.ConnectionOpts{
					Timeout:         crashDriverTimeout,
					TimeBeforeRetry: defaultRetryInterval,
				})
				if err != nil {
					logrus.Warnf("failed to run cmd : %s. on node %s err: %v", pxCrashCmd, n.Name, err)
					return err
				}
				logrus.Infof("Sleeping for %v for volume driver to go down.", waitVolDriverToCrash)
				time.Sleep(waitVolDriverToCrash)
			} else {
				err = d.schedOps.StopPxOnNode(n)
				if err != nil {
					return err
				}
				err = d.nodeDriver.Systemctl(n, pxSystemdServiceName, node.SystemctlOpts{
					Action: "stop",
					ConnectionOpts: node.ConnectionOpts{
						Timeout:         stopDriverTimeout,
						TimeBeforeRetry: defaultRetryInterval,
					}})
				if err != nil {
					logrus.Warnf("failed to run systemctl stopcmd  on node %s err: %v", n.Name, err)
					return err
				}
				logrus.Infof("Sleeping for %v for volume driver to gracefully go down.", waitVolDriverToCrash/6)
				time.Sleep(waitVolDriverToCrash / 6)
			}

		}
		return nil
	}
	return driver_api.PerformTask(stopFn, triggerOpts)
}

func (d *portworx) GetNodeForVolume(vol *torpedovolume.Volume, timeout time.Duration, retryInterval time.Duration) (*node.Node, error) {
	volumeName := d.schedOps.GetVolumeName(vol)
	r := func() (interface{}, bool, error) {
		t := func() (interface{}, bool, error) {
			volumeInspectResponse, err := d.getVolDriver().Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volumeName})
			if err != nil {
				logrus.Warnf("Failed to inspect volume: %s due to: %v", volumeName, err)
				return nil, true, err
			}
			return volumeInspectResponse.Volume, false, nil
		}

		v, err := task.DoRetryWithTimeout(t, inspectVolumeTimeout, inspectVolumeRetryInterval)
		if err != nil {
			return nil, false, &ErrFailedToInspectVolume{
				ID:    volumeName,
				Cause: err.Error(),
			}
		}
		pxVol := v.(*api.Volume)
		for _, n := range node.GetStorageDriverNodes() {
			if isVolumeAttachedOnNode(pxVol, n) {
				return &n, false, nil
			}
		}

		// Snapshots may not be attached to a node
		if pxVol.Source.Parent != "" {
			return nil, false, nil
		}

		return nil, true, fmt.Errorf("Volume: %s is not attached on any node", volumeName)
	}

	n, err := task.DoRetryWithTimeout(r, timeout, retryInterval)
	if err != nil {
		return nil, &ErrFailedToValidateAttachment{
			ID:    volumeName,
			Cause: err.Error(),
		}
	}

	if n != nil {
		node := n.(*node.Node)
		return node, nil
	}

	return nil, nil
}

func (d *portworx) GetNodeForBackup(backupID string) (node.Node, error) {
	nodeMap := node.GetNodesByVoDriverNodeID()
	csStatuses, err := d.csbackupManager.Status(context.Background(), &api.SdkCloudBackupStatusRequest{})
	if err != nil {
		return node.Node{}, err
	}
	for _, backup := range csStatuses.Statuses {
		if backup.GetBackupId() == backupID {
			return nodeMap[backup.NodeId], nil
		}
	}
	return node.Node{}, fmt.Errorf("node where backup with id [%s] running, not found", backupID)
}

func isVolumeAttachedOnNode(volume *api.Volume, node node.Node) bool {
	if node.VolDriverNodeID == volume.AttachedOn {
		return true
	}
	for _, ip := range node.Addresses {
		if ip == volume.AttachedOn {
			return true
		}
	}
	return false
}

func (d *portworx) ExtractVolumeInfo(params string) (string, map[string]string, error) {
	ok, volParams, volumeName := spec.NewSpecHandler().SpecOptsFromString(params)
	if !ok {
		return params, nil, fmt.Errorf("Unable to parse the volume options")
	}
	return volumeName, volParams, nil
}

func (d *portworx) RandomizeVolumeName(params string) string {
	re := regexp.MustCompile("(name=)([0-9A-Za-z_-]+)(,)?")
	return re.ReplaceAllString(params, "${1}${2}_"+uuid.New()+"${3}")
}

func (d *portworx) getStorageNodesOnStart() ([]api.StorageNode, error) {
	t := func() (interface{}, bool, error) {
		cluster, err := d.getClusterManager().InspectCurrent(d.getContext(), &api.SdkClusterInspectCurrentRequest{})
		if err != nil {
			return nil, true, err
		}
		if cluster.Cluster.Status != api.Status_STATUS_OK {
			return nil, true, &ErrFailedToWaitForPx{
				Cause: fmt.Sprintf("px cluster is still not up. Status: %v", cluster.Cluster.Status),
			}
		}
		return &cluster.Cluster, false, nil
	}

	_, err := task.DoRetryWithTimeout(t, validateClusterStartTimeout, defaultRetryInterval)
	if err != nil {
		return nil, err
	}

	return d.getPxNodes()
}

func (d *portworx) getPxNodes(nManagers ...api.OpenStorageNodeClient) ([]api.StorageNode, error) {
	var nodeManager api.OpenStorageNodeClient
	if nManagers == nil {
		nodeManager = d.getNodeManager()
	} else {
		nodeManager = nManagers[0]
	}
	nodes := make([]api.StorageNode, 0)
	nodeEnumerateResp, err := nodeManager.Enumerate(d.getContext(), &api.SdkNodeEnumerateRequest{})
	if err != nil {
		return nodes, err
	}
	for _, n := range nodeEnumerateResp.GetNodeIds() {
		nodeResp, err := nodeManager.Inspect(d.getContext(), &api.SdkNodeInspectRequest{NodeId: n})
		if err != nil {
			return nodes, err
		}
		nodes = append(nodes, *nodeResp.Node)
	}
	return nodes, nil
}

func (d *portworx) WaitDriverUpOnNode(n node.Node, timeout time.Duration) error {
	logrus.Debugf("waiting for PX node to be up: %s", n.Name)
	t := func() (interface{}, bool, error) {
		logrus.Debugf("Getting node info: %s", n.Name)
		for _, addr := range n.Addresses {
			err := d.testAndSetEndpointUsingNodeIP(addr)
			if err != nil {
				return "", true, &ErrFailedToWaitForPx{
					Node:  n,
					Cause: fmt.Sprintf("failed to get node info [%s]. Err: %v", n.Name, err),
				}
			}
		}
		nodeInspectResponse, err := d.getNodeManager().Inspect(d.getContext(), &api.SdkNodeInspectRequest{NodeId: n.VolDriverNodeID})

		if err != nil {
			return "", true, &ErrFailedToWaitForPx{
				Node:  n,
				Cause: fmt.Sprintf("failed to get node info [%s]. Err: %v", n.Name, err),
			}
		}

		logrus.Debugf("checking PX status on node: %s", n.Name)
		pxNode := nodeInspectResponse.Node
		switch pxNode.Status {
		case api.Status_STATUS_DECOMMISSION, api.Status_STATUS_OK: // do nothing
		case api.Status_STATUS_OFFLINE:
			// in case node is offline and it is a storageless node, the id might have changed so update it
			if len(pxNode.Pools) == 0 {
				d.updateNodeID(&n, d.getNodeManager())
			}
			return "", true, &ErrFailedToWaitForPx{
				Node: n,
				Cause: fmt.Sprintf("node %s status is up but px cluster is not ok. Expected: %v Actual: %v",
					n.Name, api.Status_STATUS_OK, pxNode.Status),
			}
		default:
			return "", true, &ErrFailedToWaitForPx{
				Node: n,
				Cause: fmt.Sprintf("px cluster is usable but node %s status is not ok. Expected: %v Actual: %v",
					n.Name, api.Status_STATUS_OK, pxNode.Status),
			}
		}

		logrus.Infof("px on node: %s is now up. status: %v", n.Name, pxNode.Status)

		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, timeout, defaultRetryInterval); err != nil {
		return err
	}

	// Check if PX pod is up
	logrus.Debugf("checking if PX pod is up on node: %s", n.Name)
	t = func() (interface{}, bool, error) {
		if !d.schedOps.IsPXReadyOnNode(n) {
			return "", true, &ErrFailedToWaitForPx{
				Node:  n,
				Cause: fmt.Sprintf("px pod is not ready on node: %s after %v", n.Name, timeout),
			}
		}
		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, defaultRetryInterval); err != nil {
		return err
	}

	logrus.Debugf("px is fully operational on node: %s", n.Name)
	return nil
}

func (d *portworx) WaitDriverDownOnNode(n node.Node) error {
	t := func() (interface{}, bool, error) {

		for _, addr := range n.Addresses {
			err := d.testAndSetEndpointUsingNodeIP(addr)
			if err != nil {
				if !strings.Contains(err.Error(), "connect: connection refused") {
					return "", true, &ErrFailedToWaitForPx{
						Node:  n,
						Cause: fmt.Sprintf("px is not yet down on node"),
					}
				}
			}
		}

		logrus.Infof("px on node %s is now down.", n.Name)
		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, validateNodeStopTimeout, waitDriverDownOnNodeRetryInterval); err != nil {
		return err
	}

	return nil
}

func (d *portworx) ValidateStoragePools() error {
	listApRules, err := d.schedOps.ListAutopilotRules()
	if err != nil {
		return err
	}

	if len(listApRules.Items) != 0 {
		expectedPoolSizes, err := d.getExpectedPoolSizes(listApRules)
		if err != nil {
			return err
		}

		// start a task to check if the pools are at their expected sizes
		t := func() (interface{}, bool, error) {
			allDone := true
			if err := d.RefreshDriverEndpoints(); err != nil {
				return nil, true, err
			}

			for _, n := range node.GetWorkerNodes() {
				for _, pool := range n.StoragePools {
					expectedSize := expectedPoolSizes[pool.Uuid]
					if expectedSize != pool.TotalSize {
						if pool.TotalSize > expectedSize {
							// no need to retry with this state as pool is already at larger size than expected
							err := fmt.Errorf("node: %s pool: %s was expanded to size: %d larger than expected: %d",
								n.Name, pool.Uuid, pool.TotalSize, expectedSize)
							logrus.Errorf(err.Error())
							return "", false, err
						}

						logrus.Infof("node: %s, pool: %s, size is not as expected. Expected: %v, Actual: %v",
							n.Name, pool.Uuid, expectedSize, pool.TotalSize)
						allDone = false
					} else {
						logrus.Infof("node: %s, pool: %s, size is as expected. Expected: %v",
							n.Name, pool.Uuid, expectedSize)
					}
				}
			}
			if allDone {
				return "", false, nil
			}
			return "", true, fmt.Errorf("some sizes of pools are not as expected")
		}

		if _, err := task.DoRetryWithTimeout(t, validateStoragePoolSizeTimeout, validateStoragePoolSizeInterval); err != nil {
			return err
		}
	}
	return nil
}

func (d *portworx) getExpectedPoolSizes(listApRules *apapi.AutopilotRuleList) (map[string]uint64, error) {
	fn := "getExpectedPoolSizes"
	var (
		expectedPoolSizes = map[string]uint64{}
		err               error
	)
	d.RefreshDriverEndpoints()
	for _, apRule := range listApRules.Items {
		for _, n := range node.GetWorkerNodes() {
			for _, pool := range n.StoragePools {
				apRuleLabels := apRule.Spec.Selector.LabelSelector.MatchLabels
				labelsMatch := false
				for k, v := range apRuleLabels {
					if apRuleLabels[k] == pool.Labels[k] && apRuleLabels[v] == pool.Labels[v] {
						labelsMatch = true
					}
				}

				if labelsMatch {
					expectedPoolSizes[pool.Uuid], err = d.EstimatePoolExpandSize(apRule, pool, n)
					if err != nil {
						return nil, err
					}
				} else {
					if _, ok := expectedPoolSizes[pool.Uuid]; !ok {
						expectedPoolSizes[pool.Uuid] = pool.StoragePoolAtInit.TotalSize
					}
				}
			}
		}
	}
	logrus.Debugf("%s: expected sizes of storage pools: %+v", fn, expectedPoolSizes)
	return expectedPoolSizes, nil
}

// pickAlternateClusterManager returns a different node than given one, useful in case you want to skip nodes which are down
func (d *portworx) pickAlternateClusterManager(n node.Node) (api.OpenStorageNodeClient, error) {
	// Check if px is down on all node addresses. We don't want to keep track
	// which was the actual interface px was listening on before it went down
	for _, alternateNode := range node.GetWorkerNodes() {
		if alternateNode.Name == n.Name {
			continue
		}

		for _, addr := range alternateNode.Addresses {
			nodeManager, err := d.getNodeManagerByAddress(addr)
			if err != nil {
				return nil, err
			}
			ns, err := nodeManager.Enumerate(d.getContext(), &api.SdkNodeEnumerateRequest{})
			if err != nil {
				// if not responding in this addr, continue and pick another one, log the error
				logrus.Warnf("failed to check node %s on addr %s. Cause: %v", n.Name, addr, err)
				continue
			}
			if len(ns.NodeIds) != 0 {
				return nodeManager, nil
			}
		}
	}
	return nil, fmt.Errorf("failed to get an alternate cluster manager for %s", n.Name)
}

func (d *portworx) IsStorageExpansionEnabled() (bool, error) {
	var listApRules *apapi.AutopilotRuleList
	var err error
	d.RefreshDriverEndpoints()
	if listApRules, err = d.schedOps.ListAutopilotRules(); err != nil {
		return false, err
	}

	if len(listApRules.Items) != 0 {
		for _, apRule := range listApRules.Items {
			for _, n := range node.GetWorkerNodes() {
				if isAutopilotMatchStoragePoolLabels(apRule, n.StoragePools) {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func isAutopilotMatchStoragePoolLabels(apRule apapi.AutopilotRule, sPools []node.StoragePool) bool {
	apRuleLabels := apRule.Spec.Selector.LabelSelector.MatchLabels
	for k, v := range apRuleLabels {
		for _, pool := range sPools {
			if poolLabelValue, ok := pool.Labels[k]; ok {
				if poolLabelValue == v {
					return true
				}
			}
		}
	}
	return false
}

func (d *portworx) WaitForUpgrade(n node.Node, tag string) error {
	t := func() (interface{}, bool, error) {

		// filter out first 3 octets from the tag
		matches := regexp.MustCompile(`^(\d+\.\d+\.\d+).*`).FindStringSubmatch(tag)
		if len(matches) != 2 {
			return nil, false, &ErrFailedToUpgradeVolumeDriver{
				Version: fmt.Sprintf("%s", tag),
				Cause:   fmt.Sprintf("failed to parse first 3 octets of version from new version tag: %s", tag),
			}
		}

		pxVersion, err := d.getPxVersionOnNode(n)
		if err != nil {
			return nil, true, &ErrFailedToWaitForPx{
				Node:  n,
				Cause: fmt.Sprintf("failed to get PX Version with error: %s", err),
			}
		}
		if !strings.HasPrefix(pxVersion, matches[1]) {
			return nil, true, &ErrFailedToUpgradeVolumeDriver{
				Version: fmt.Sprintf("%s", tag),
				Cause: fmt.Sprintf("version on node %s is still %s. It was expected to begin with: %s",
					n.VolDriverNodeID, pxVersion, matches[1]),
			}
		}

		logrus.Infof("version on node %s is %s. Expected version is %s", n.VolDriverNodeID, pxVersion, matches[1])

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, upgradeTimeout, upgradeRetryInterval); err != nil {
		return err
	}
	return nil
}

func (d *portworx) GetReplicationFactor(vol *torpedovolume.Volume) (int64, error) {
	name := d.schedOps.GetVolumeName(vol)
	t := func() (interface{}, bool, error) {
		volumeInspectResponse, err := d.getVolDriver().Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: name})
		if err != nil && errIsNotFound(err) {
			return 0, false, err
		} else if err != nil {
			return 0, true, err
		}
		return volumeInspectResponse.Volume.Spec.HaLevel, false, nil
	}

	iReplFactor, err := task.DoRetryWithTimeout(t, validateReplicationUpdateTimeout, defaultRetryInterval)
	if err != nil {
		return 0, &ErrFailedToGetReplicationFactor{
			ID:    name,
			Cause: err.Error(),
		}
	}
	replFactor, ok := iReplFactor.(int64)
	if !ok {
		return 0, &ErrFailedToGetReplicationFactor{
			ID:    name,
			Cause: fmt.Sprintf("Replication factor is not of type int64"),
		}
	}
	logrus.Debugf("Replication factor for volume: %s is %d", vol.ID, replFactor)

	return replFactor, nil
}

func (d *portworx) SetReplicationFactor(vol *torpedovolume.Volume, replFactor int64, opts ...torpedovolume.Options) error {
	volumeName := d.schedOps.GetVolumeName(vol)
	var replicationUpdateTimeout time.Duration
	if len(opts) > 0 {
		replicationUpdateTimeout = opts[0].ValidateReplicationUpdateTimeout
	} else {
		replicationUpdateTimeout = validateReplicationUpdateTimeout
	}
	logrus.Infof("Setting ReplicationUpdateTimeout to %s-%v\n", replicationUpdateTimeout, replicationUpdateTimeout)

	t := func() (interface{}, bool, error) {
		volDriver := d.getVolDriver()
		volumeInspectResponse, err := volDriver.Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volumeName})
		if err != nil && errIsNotFound(err) {
			return nil, false, err
		} else if err != nil {
			return nil, true, err
		}

		volumeSpecUpdate := &api.VolumeSpecUpdate{
			HaLevelOpt:          &api.VolumeSpecUpdate_HaLevel{HaLevel: int64(replFactor)},
			SnapshotIntervalOpt: &api.VolumeSpecUpdate_SnapshotInterval{SnapshotInterval: math.MaxUint32},
			ReplicaSet:          &api.ReplicaSet{},
		}
		_, err = volDriver.Update(d.getContext(), &api.SdkVolumeUpdateRequest{
			VolumeId: volumeInspectResponse.Volume.Id,
			Spec:     volumeSpecUpdate,
		})
		if err != nil {
			return nil, false, err
		}
		quitFlag := false
		wdt := time.After(replicationUpdateTimeout)
		for !quitFlag && !(areRepSetsFinal(volumeInspectResponse.Volume, replFactor) && isClean(volumeInspectResponse.Volume)) {
			select {
			case <-wdt:
				quitFlag = true
			default:
				volumeInspectResponse, err = volDriver.Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volumeName})
				if err != nil && errIsNotFound(err) {
					return nil, false, err
				} else if err != nil {
					return nil, true, err
				}
				time.Sleep(defaultRetryInterval)
			}
		}
		if !(areRepSetsFinal(volumeInspectResponse.Volume, replFactor) && isClean(volumeInspectResponse.Volume)) {
			return 0, false, fmt.Errorf("Volume didn't successfully change to replication factor of %d", replFactor)
		}
		return 0, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, replicationUpdateTimeout, defaultRetryInterval); err != nil {
		return &ErrFailedToSetReplicationFactor{
			ID:    volumeName,
			Cause: err.Error(),
		}
	}

	return nil
}

func (d *portworx) GetMaxReplicationFactor() int64 {
	return 3
}

func (d *portworx) GetMinReplicationFactor() int64 {
	return 1
}

func (d *portworx) GetAggregationLevel(vol *torpedovolume.Volume) (int64, error) {
	volumeName := d.schedOps.GetVolumeName(vol)
	t := func() (interface{}, bool, error) {
		volResp, err := d.getVolDriver().Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volumeName})
		if err != nil && errIsNotFound(err) {
			return 0, false, err
		} else if err != nil {
			return 0, true, err
		}
		return volResp.Volume.Spec.AggregationLevel, false, nil
	}

	iAggrLevel, err := task.DoRetryWithTimeout(t, inspectVolumeTimeout, inspectVolumeRetryInterval)
	if err != nil {
		return 0, &ErrFailedToGetAggregationLevel{
			ID:    volumeName,
			Cause: err.Error(),
		}
	}
	aggrLevel, ok := iAggrLevel.(uint32)
	if !ok {
		return 0, &ErrFailedToGetAggregationLevel{
			ID:    volumeName,
			Cause: fmt.Sprintf("Aggregation level is not of type uint32"),
		}
	}
	logrus.Debugf("Aggregation level for volume: %s is %d", vol.ID, aggrLevel)

	return int64(aggrLevel), nil
}

func isClean(vol *api.Volume) bool {
	for _, v := range vol.RuntimeState {
		if v.GetRuntimeState()["RuntimeState"] != "clean" {
			return false
		}
	}
	return true
}

func areRepSetsFinal(vol *api.Volume, replFactor int64) bool {
	for _, rs := range vol.ReplicaSets {
		if int64(len(rs.GetNodes())) != replFactor {
			return false
		}
	}
	return true
}

func (d *portworx) setDriver() error {
	var err error
	var endpoint string

	// Try portworx-service first
	endpoint, err = d.schedOps.GetServiceEndpoint()
	if err == nil && endpoint != "" {
		if err = d.testAndSetEndpointUsingService(endpoint); err == nil {
			d.refreshEndpoint = false
			return nil
		}
		logrus.Infof("testAndSetEndpoint failed for %v: %v", endpoint, err)
	} else if err != nil && len(node.GetWorkerNodes()) == 0 {
		return err
	}

	// Try direct address of cluster nodes
	// Set refresh endpoint to true so that we try and get the new
	// and working driver if the endpoint we are hooked onto goes
	// down
	d.refreshEndpoint = true
	logrus.Infof("Getting new driver.")
	for _, n := range node.GetWorkerNodes() {
		for _, addr := range n.Addresses {
			if err = d.testAndSetEndpointUsingNodeIP(addr); err == nil {
				return nil
			}
			logrus.Infof("testAndSetEndpoint failed for %v: %v", endpoint, err)
		}
	}

	return fmt.Errorf("failed to get endpoint for portworx volume driver")
}

func (d *portworx) testAndSetEndpointUsingService(endpoint string) error {
	sdkPort, err := getSDKPort()
	if err != nil {
		return err
	}

	restPort, err := getRestPort()
	if err != nil {
		return err
	}

	return d.testAndSetEndpoint(endpoint, sdkPort, restPort)
}

func (d *portworx) testAndSetEndpointUsingNodeIP(ip string) error {
	sdkPort, err := getSDKContainerPort()
	if err != nil {
		return err
	}

	restPort, err := getRestContainerPort()
	if err != nil {
		return err
	}

	return d.testAndSetEndpoint(ip, sdkPort, restPort)
}

func (d *portworx) testAndSetEndpoint(endpoint string, sdkport, apiport int32) error {
	pxEndpoint := fmt.Sprintf("%s:%d", endpoint, sdkport)
	conn, err := grpc.Dial(pxEndpoint, grpc.WithInsecure())
	if err != nil {
		return err
	}

	d.clusterManager = api.NewOpenStorageClusterClient(conn)
	_, err = d.clusterManager.InspectCurrent(d.getContext(), &api.SdkClusterInspectCurrentRequest{})
	if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable {
		return err
	}

	d.volDriver = api.NewOpenStorageVolumeClient(conn)
	d.nodeManager = api.NewOpenStorageNodeClient(conn)
	d.mountAttachManager = api.NewOpenStorageMountAttachClient(conn)
	d.clusterPairManager = api.NewOpenStorageClusterPairClient(conn)
	d.alertsManager = api.NewOpenStorageAlertsClient(conn)
	d.csbackupManager = api.NewOpenStorageCloudBackupClient(conn)
	if legacyClusterManager, err := d.getLegacyClusterManager(endpoint, apiport); err == nil {
		d.legacyClusterManager = legacyClusterManager
	} else {
		return err
	}
	logrus.Infof("Using %v as endpoint for portworx volume driver", pxEndpoint)

	return nil
}

func (d *portworx) getLegacyClusterManager(endpoint string, pxdRestPort int32) (cluster.Cluster, error) {
	pxEndpoint := fmt.Sprintf("http://%s:%d", endpoint, pxdRestPort)
	var cClient *client.Client
	var err error
	if d.token != "" {
		cClient, err = clusterclient.NewAuthClusterClient(pxEndpoint, "v1", d.token, "")
		if err != nil {
			return nil, err
		}
	} else {
		cClient, err = clusterclient.NewClusterClient(pxEndpoint, "v1")
		if err != nil {
			return nil, err
		}
	}

	clusterManager := clusterclient.ClusterManager(cClient)
	_, err = clusterManager.Enumerate()
	if err != nil {
		return nil, err
	}
	return clusterManager, nil
}

func (d *portworx) getContextWithToken(ctx context.Context, token string) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = metadata.Join(md, metadata.New(map[string]string{
		"authorization": "bearer " + token,
	}))
	return metadata.NewOutgoingContext(ctx, md)
}

func (d *portworx) getContext() context.Context {
	ctx := context.Background()
	if len(d.token) > 0 {
		return d.getContextWithToken(ctx, d.token)
	}
	return ctx
}

func (d *portworx) StartDriver(n node.Node) error {
	logrus.Infof("Starting volume driver on %s.", n.Name)
	err := d.schedOps.StartPxOnNode(n)
	if err != nil {
		return err
	}
	return d.nodeDriver.Systemctl(n, pxSystemdServiceName, node.SystemctlOpts{
		Action: "start",
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         startDriverTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		}})
}

func (d *portworx) UpgradeDriver(endpointURL string, endpointVersion string, enableStork bool) error {
	if endpointURL == "" {
		return fmt.Errorf("no link supplied for upgrading driver")
	}
	if endpointVersion == "" {
		return fmt.Errorf("no endpoint supplied for upgrading driver")
	}

	if err := d.upgradePortworx(endpointURL, endpointVersion); err != nil {
		return err
	}

	if enableStork {
		if err := d.upgradeStork(endpointURL, endpointVersion); err != nil {
			return err
		}
	} else {
		logrus.Infof("stork upgrade is disabled, skipping...")
	}
	return nil
}

func (d *portworx) RestartDriver(n node.Node, triggerOpts *driver_api.TriggerOptions) error {
	return driver_api.PerformTask(
		func() error {
			return d.schedOps.RestartPxOnNode(n)
		},
		triggerOpts)
}

// upgradePortworx upgrades Portworx
func (d *portworx) upgradePortworx(endpointURL string, endpointVersion string) error {
	upgradeFileName := "/upgrade.sh"
	fullEndpointURL := fmt.Sprintf("%s/%s/upgrade", endpointURL, endpointVersion)

	logrus.Infof("upgrading portworx from %s URL and %s endpoint version", endpointURL, endpointVersion)
	// Getting upgrade script
	if err := osutils.Wget(fullEndpointURL, upgradeFileName, true); err != nil {
		return fmt.Errorf("%+v", err)
	}

	// Change permission on file to be able to execute
	if err := osutils.Chmod("+x", upgradeFileName); err != nil {
		return err
	}

	nodeList := node.GetStorageDriverNodes()
	pxNode := nodeList[0]
	pxVersion, err := d.getPxVersionOnNode(pxNode)
	if err != nil {
		return fmt.Errorf("error on getting PX Version on node %s with err: %v", pxNode.Name, err)
	}
	// If PX Version less than 2.x.x.x, then we have to add timeout parameter to avoid test failure
	// more details in https://portworx.atlassian.net/browse/PWX-10108
	cmdArgs := []string{upgradeFileName, "-f"}
	majorPxVersion := pxVersion[:1]
	if majorPxVersion < "2" {
		cmdArgs = append(cmdArgs, "-u", strconv.Itoa(int(upgradePerNodeTimeout/time.Second)))
	}

	// Run upgrade script
	if err := osutils.Sh(cmdArgs); err != nil {
		return err
	}

	logrus.Infof("Portworx cluster upgraded successfully")

	for _, n := range node.GetStorageDriverNodes() {
		if err := d.WaitForUpgrade(n, endpointVersion); err != nil {
			return err
		}
	}
	return nil
}

// upgradeStork upgrades stork
func (d *portworx) upgradeStork(endpointURL string, endpointVersion string) error {
	storkSpecFileName := "/stork.yaml"
	nodeList := node.GetStorageDriverNodes()
	pxNode := nodeList[0]
	pxVersion, err := d.getPxVersionOnNode(pxNode)
	if err != nil {
		return fmt.Errorf("error on getting PX Version on node %s with err: %v", pxNode.Name, err)
	}
	pVersion, err := version.NewVersion(pxVersion)
	if err != nil {
		return err
	}

	storkMinVersion, err := version.NewVersion(pxMinVersionForStorkUpgrade)
	if err != nil {
		return err
	}
	if pVersion.LessThan(storkMinVersion) {
		logrus.Debugf("skipping stork upgrade as PX Version is less than %s", pxMinVersionForStorkUpgrade)
		return nil
	}
	kubeVersion, err := d.schedOps.GetKubernetesVersion()
	if err != nil {
		return err
	}

	// Getting stork spec
	URL := fmt.Sprintf("%s/%s?kbver=%s&comp=stork", endpointURL, endpointVersion, kubeVersion)
	logrus.Debugf("getting stork spec from: %s", URL)
	if err := osutils.Wget(URL, storkSpecFileName, true); err != nil {
		return err
	}

	// Getting context of the file
	if _, err := osutils.Cat(storkSpecFileName); err != nil {
		return err
	}

	// Apply stork spec
	cmdArgs := []string{"apply", "-f", storkSpecFileName}
	if err := osutils.Kubectl(cmdArgs); err != nil {
		return err
	}

	return nil
}

// GetClusterPairingInfo returns cluster pair information
func (d *portworx) GetClusterPairingInfo() (map[string]string, error) {
	pairInfo := make(map[string]string)
	pxNodes, err := d.schedOps.GetRemotePXNodes(remoteKubeConfigPath)
	if err != nil {
		logrus.Errorf("err retrieving remote px nodes: %v", err)
		return nil, err
	}
	if len(pxNodes) == 0 {
		return nil, fmt.Errorf("No PX Node found")
	}

	clusterPairManager, err := d.getClusterPairManagerByAddress(pxNodes[0].Addresses[0])
	if err != nil {
		return nil, err
	}

	resp, err := clusterPairManager.GetToken(d.getContext(), &api.SdkClusterPairGetTokenRequest{})
	if err != nil {
		return nil, err
	}
	logrus.Infof("Response for token: %v", resp.Result.Token)

	// file up cluster pair info
	pairInfo[clusterIP] = pxNodes[0].Addresses[0]
	pairInfo[tokenKey] = resp.Result.Token
	pwxServicePort, err := getRestContainerPort()
	if err != nil {
		return nil, err
	}
	pairInfo[clusterPort] = fmt.Sprintf("%d", pwxServicePort)

	return pairInfo, nil
}

func (d *portworx) DecommissionNode(n *node.Node) error {

	if err := k8sCore.AddLabelOnNode(n.Name, schedops.PXEnabledLabelKey, "remove"); err != nil {
		return &ErrFailedToDecommissionNode{
			Node:  n.Name,
			Cause: fmt.Sprintf("Failed to set label on node: %v. Err: %v", n.Name, err),
		}
	}

	if err := d.StopDriver([]node.Node{*n}, false, nil); err != nil {
		return &ErrFailedToDecommissionNode{
			Node:  n.Name,
			Cause: fmt.Sprintf("Failed to stop driver on node: %v. Err: %v", n.Name, err),
		}
	}

	nodeResp, err := d.getNodeManager().Inspect(d.getContext(), &api.SdkNodeInspectRequest{NodeId: n.VolDriverNodeID})
	if err != nil {
		return &ErrFailedToDecommissionNode{
			Node:  n.Name,
			Cause: fmt.Sprintf("Failed to inspect node: %v. Err: %v", nodeResp.Node, err),
		}
	}

	// TODO replace when sdk supports node removal
	if err = d.legacyClusterManager.Remove([]api.Node{{Id: nodeResp.Node.Id}}, false); err != nil {
		return &ErrFailedToDecommissionNode{
			Node:  n.Name,
			Cause: err.Error(),
		}
	}

	// update node in registry
	n.IsStorageDriverInstalled = false
	if err = node.UpdateNode(*n); err != nil {
		return fmt.Errorf("failed to update node %s. Cause: %v", n.Name, err)
	}

	// force refresh endpoint
	d.refreshEndpoint = true

	return nil
}

func (d *portworx) RejoinNode(n *node.Node) error {

	opts := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
	}
	if _, err := d.nodeDriver.RunCommand(*n, "/opt/pwx/bin/pxctl sv node-wipe --all", opts); err != nil {
		return &ErrFailedToRejoinNode{
			Node:  n.Name,
			Cause: err.Error(),
		}
	}
	if err := k8sCore.RemoveLabelOnNode(n.Name, schedops.PXServiceLabelKey); err != nil {
		return &ErrFailedToRejoinNode{
			Node:  n.Name,
			Cause: fmt.Sprintf("Failed to set label on node: %v. Err: %v", n.Name, err),
		}
	}
	if err := k8sCore.RemoveLabelOnNode(n.Name, schedops.PXEnabledLabelKey); err != nil {
		return &ErrFailedToRejoinNode{
			Node:  n.Name,
			Cause: fmt.Sprintf("Failed to set label on node: %v. Err: %v", n.Name, err),
		}
	}
	if err := k8sCore.UnCordonNode(n.Name, defaultTimeout, defaultRetryInterval); err != nil {
		return &ErrFailedToRejoinNode{
			Node:  n.Name,
			Cause: fmt.Sprintf("Failed to uncordon node: %v. Err: %v", n.Name, err),
		}
	}
	return nil
}

func (d *portworx) GetNodeStatus(n node.Node) (*api.Status, error) {
	nodeResponse, err := d.getNodeManager().Inspect(d.getContext(), &api.SdkNodeInspectRequest{NodeId: n.VolDriverNodeID})
	if err != nil {
		if isNodeNotFound(err) {
			apiSt := api.Status_STATUS_NONE
			return &apiSt, nil
		}
		return nil, &ErrFailedToGetNodeStatus{
			Node:  n.Name,
			Cause: fmt.Sprintf("Failed to check node status: %v. Err: %v", n.Name, err),
		}
	}
	return &nodeResponse.Node.Status, nil
}

func (d *portworx) getVolDriver() api.OpenStorageVolumeClient {
	if d.refreshEndpoint {
		d.setDriver()
	}
	return d.volDriver
}

func (d *portworx) getClusterManager() api.OpenStorageClusterClient {
	if d.refreshEndpoint {
		d.setDriver()
	}
	return d.clusterManager

}

func (d *portworx) getNodeManager() api.OpenStorageNodeClient {
	if d.refreshEndpoint {
		d.setDriver()
	}
	return d.nodeManager

}

func (d *portworx) getMountAttachManager() api.OpenStorageMountAttachClient {
	if d.refreshEndpoint {
		d.setDriver()
	}
	return d.mountAttachManager

}

func (d *portworx) getClusterPairManager() api.OpenStorageClusterPairClient {
	if d.refreshEndpoint {
		d.setDriver()
	}
	return d.clusterPairManager

}

func (d *portworx) getClusterPairManagerByAddress(addr string) (api.OpenStorageClusterPairClient, error) {
	pxPort, err := getSDKContainerPort()
	if err != nil {
		return nil, err
	}
	pxEndpoint := fmt.Sprintf("%s:%d", addr, pxPort)
	conn, err := grpc.Dial(pxEndpoint, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	dClient := api.NewOpenStorageClusterPairClient(conn)
	_, err = dClient.Enumerate(d.getContext(), &api.SdkClusterPairEnumerateRequest{})
	if err != nil {
		return nil, err
	}

	return dClient, nil
}

func (d *portworx) getAlertsManager() api.OpenStorageAlertsClient {
	if d.refreshEndpoint {
		d.setDriver()
	}
	return d.alertsManager

}

func (d *portworx) getNodeManagerByAddress(addr string) (api.OpenStorageNodeClient, error) {
	pxPort, err := getSDKContainerPort()
	if err != nil {
		return nil, err
	}
	pxEndpoint := fmt.Sprintf("%s:%d", addr, pxPort)
	conn, err := grpc.Dial(pxEndpoint, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	dClient := api.NewOpenStorageNodeClient(conn)
	_, err = dClient.Enumerate(d.getContext(), &api.SdkNodeEnumerateRequest{})
	if err != nil {
		return nil, err
	}

	return dClient, nil
}

func (d *portworx) maintenanceOp(n node.Node, op string) error {
	// TODO replace by sdk call whenever it is available
	pxdRestPort, err := getRestPort()
	if err != nil {
		return err
	}
	endpoint, err := d.schedOps.GetServiceEndpoint()
	var url string
	if err != nil {
		logrus.Warnf("unable to get service endpoint falling back to node addr %v", err)
		pxdRestPort, err = getRestContainerPort()
		if err != nil {
			return err
		}
		url = fmt.Sprintf("http://%s:%d", n.Addresses[0], pxdRestPort)
	} else {
		url = fmt.Sprintf("http://%s:%d", endpoint, pxdRestPort)
	}
	c, err := client.NewClient(url, "", "")
	if err != nil {
		return err
	}
	req := c.Get().Resource(op)
	resp := req.Do()
	return resp.Error()
}

func (d *portworx) GetReplicaSets(torpedovol *torpedovolume.Volume) ([]*api.ReplicaSet, error) {
	volumeName := d.schedOps.GetVolumeName(torpedovol)
	volumeInspectResponse, err := d.getVolDriver().Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volumeName})
	if err != nil {
		return nil, &ErrFailedToInspectVolume{
			ID:    torpedovol.Name,
			Cause: err.Error(),
		}
	}

	return volumeInspectResponse.Volume.ReplicaSets, nil
}

func (d *portworx) updateNodeID(n *node.Node, nManager ...api.OpenStorageNodeClient) (*node.Node, error) {
	nodes, err := d.getPxNodes(nManager...)
	if err != nil {
		return n, err
	}
	if err = d.updateNode(n, nodes); err != nil {
		return &node.Node{}, fmt.Errorf("failed to update node ID for node %s. Cause: %v", n.Name, err)
	}
	return n, fmt.Errorf("node %v not found in cluster", n)
}

func getGroupMatches(groupRegex *regexp.Regexp, str string) map[string]string {
	match := groupRegex.FindStringSubmatch(str)
	result := make(map[string]string)
	if len(match) > 0 {
		for i, name := range groupRegex.SubexpNames() {
			if i != 0 && name != "" {
				result[name] = match[i]
			}
		}
	}
	return result
}

// ValidateVolumeSnapshotRestore return nil if snapshot is restored successuflly to
// given volumes
// TODO: additionally check for restore objects in case of cloudsnap
func (d *portworx) ValidateVolumeSnapshotRestore(vol string, snapshotData *snapv1.VolumeSnapshotData, timeStart time.Time) error {
	snap := snapshotData.Spec.PortworxSnapshot.SnapshotID
	if snapshotData.Spec.PortworxSnapshot.SnapshotType == snapv1.PortworxSnapshotTypeCloud {
		snap = "in-place-restore-" + vol
	}

	tsStart := timestamp.Timestamp{
		Nanos:   int32(timeStart.UnixNano()),
		Seconds: timeStart.Unix(),
	}
	currentTime := time.Now()
	tsEnd := timestamp.Timestamp{
		Nanos:   int32(currentTime.UnixNano()),
		Seconds: currentTime.Unix(),
	}
	alerts, err := d.alertsManager.EnumerateWithFilters(d.getContext(), &api.SdkAlertsEnumerateWithFiltersRequest{
		Queries: []*api.SdkAlertsQuery{
			{
				Query: &api.SdkAlertsQuery_ResourceTypeQuery{
					ResourceTypeQuery: &api.SdkAlertsResourceTypeQuery{
						ResourceType: api.ResourceType_RESOURCE_TYPE_VOLUME,
					},
				},
				Opts: []*api.SdkAlertsOption{
					{Opt: &api.SdkAlertsOption_TimeSpan{
						TimeSpan: &api.SdkAlertsTimeSpan{
							StartTime: &tsStart,
							EndTime:   &tsEnd,
						},
					}},
				},
			},
		},
	})

	if err != nil {
		return err
	}
	// get volume and snap info
	volDriver := d.getVolDriver()
	pvcVol, err := volDriver.Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: vol})
	if err != nil {
		return fmt.Errorf("inspect failed for %v: %v", vol, err)
	}
	// form alert msg for snapshot restore
	grepMsg := "Volume " + pvcVol.Volume.GetLocator().GetName() +
		" (" + pvcVol.Volume.GetId() + ") restored from snapshot "
	snapVol, err := volDriver.Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: snap})
	if err != nil {
		// Restore object get deleted in case of cloudsnap
		logrus.Warnf("Snapshot volume %v not found: %v", snap, err)
		grepMsg = grepMsg + snap
	} else {
		grepMsg = grepMsg + snapVol.Volume.GetLocator().GetName() +
			" (" + snap + ")"
	}

	isSuccess := false
	alertsResp, err := alerts.Recv()
	if err != nil {
		return err
	}
	for _, alert := range alertsResp.Alerts {
		if strings.Contains(alert.GetMessage(), grepMsg) {
			isSuccess = true
			break
		}
	}
	if isSuccess {
		return nil
	}
	return fmt.Errorf("restore failed, expected alert to be present : %v", grepMsg)
}

func (d *portworx) getTokenForVolume(name string, params map[string]string) string {
	token := d.token
	var volSecret string
	var volSecretNamespace string
	if secret, ok := params[secretName]; ok {
		volSecret = secret
	}
	if namespace, ok := params[secretNamespace]; ok {
		volSecretNamespace = namespace
	}
	if volSecret != "" && volSecretNamespace != "" {
		if tk, ok := params["auth-token"]; ok {
			token = tk
		}
	}
	return token
}

func deleteLabelsFromRequestedSpec(expectedLocator *api.VolumeLocator) {
	for labelKey := range expectedLocator.VolumeLabels {
		if hasIgnorePrefix(labelKey) {
			delete(expectedLocator.VolumeLabels, labelKey)
		}
	}
}

func hasIgnorePrefix(str string) bool {
	for _, label := range deleteVolumeLabelList {
		if strings.HasPrefix(str, label) {
			return true
		}
	}
	return false
}

func (d *portworx) getKvdbMembers(n node.Node) (map[string]metadataNode, error) {
	kvdbMembers := make(map[string]metadataNode)
	pxdRestPort, err := getRestPort()
	if err != nil {
		return kvdbMembers, err
	}
	endpoint, err := d.schedOps.GetServiceEndpoint()
	var url string
	if err != nil {
		logrus.Warnf("unable to get service endpoint falling back to node addr %v", err)
		pxdRestPort, err = getRestContainerPort()
		if err != nil {
			return kvdbMembers, err
		}
		url = fmt.Sprintf("http://%s:%d", n.Addresses[0], pxdRestPort)
	} else {
		url = fmt.Sprintf("http://%s:%d", endpoint, pxdRestPort)
	}
	// TODO replace by sdk call whenever it is available
	logrus.Infof("Url to call %v", url)
	c, err := client.NewClient(url, "", "")
	if err != nil {
		return nil, err
	}
	req := c.Get().Resource("kvmembers")
	resp := req.Do()
	if resp.Error() != nil {
		if strings.Contains(resp.Error().Error(), "command not supported") {
			return kvdbMembers, nil
		}
		return kvdbMembers, resp.Error()
	}
	err = resp.Unmarshal(&kvdbMembers)
	return kvdbMembers, err
}

func (d *portworx) CollectDiags(n node.Node) error {
	var err error

	pxNode, err := d.getPxNode(&n)
	if err != nil {
		return err
	}

	opts := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
		Sudo:            true,
	}

	logrus.Debugf("Collecting diags on node %v, because there was an error", pxNode.Hostname)

	if pxNode.Status == api.Status_STATUS_OFFLINE {
		logrus.Debugf("Node %v is offline, collecting diags using pxctl", pxNode.Hostname)

		// Only way to collect diags when PX is offline is using pxctl
		out, err := d.nodeDriver.RunCommand(n, "pxctl sv diags -a -f", opts)
		if err != nil {
			return fmt.Errorf("failed to collect diags on node %v, Err: %v %v", pxNode.Hostname, err, out)
		}
		logrus.Debugf("Successfully collected diags on node %v", pxNode.Hostname)
		return nil
	}

	url := fmt.Sprintf("http://%s:9014", n.Addresses[0])

	r := &DiagRequestConfig{
		DockerHost:    "unix:///var/run/docker.sock",
		OutputFile:    "/var/cores/diags.tar.gz",
		ContainerName: "",
		Profile:       false,
		Live:          true,
		Upload:        false,
		All:           true,
		Force:         true,
		OnHost:        true,
		Extra:         false,
	}

	c, err := client.NewClient(url, "", "")
	if err != nil {
		return err
	}
	req := c.Post().Resource(pxDiagPath).Body(r)
	resp := req.Do()
	if resp.Error() != nil {
		return fmt.Errorf("failed to collect diags on node %v, Err: %v", pxNode.Hostname, resp.Error())
	}
	logrus.Debugf("Successfully collected diags on node %v", pxNode.Hostname)
	return nil
}

// EstimatePoolExpandSize calculates the expected size based on autopilot rule, initial and workload sizes
func (d *portworx) EstimatePoolExpandSize(apRule apapi.AutopilotRule, pool node.StoragePool, node node.Node) (uint64, error) {
	// this method calculates expected pool size for given initial and workload sizes.
	// for ex: autopilot rule says scale storage pool by 50% with scale type adding disks when
	// available storage pool capacity is less that 70%. Initial storage pool size is 32Gb and
	// workload size on this pool is 10Gb
	// First, we get PX metric from the rule and calculate it's own value based on initial storage
	// pool size. In our example metric value will be (32Gb-10Gb*100) / 32Gb = 68.75
	// Second, we check if above metric matches condition in the rule conditions. Metric value is
	// less than 70% and we have to apply condition action, which will add another disk with 32Gb.
	// It will continue until metric value won't match condition in the rule

	// first check if the apRule is supported by torpedo
	var actionScaleType string
	for _, ruleAction := range apRule.Spec.Actions {
		if ruleAction.Name != aututils.StorageSpecAction {
			return 0, &tp_errors.ErrNotSupported{
				Type:      ruleAction.Name,
				Operation: "EstimatePoolExpandSize for action",
			}
		}

		if len(ruleAction.Params) == 0 {
			return 0, &tp_errors.ErrNotSupported{
				Type:      "without params",
				Operation: "Pool expand action",
			}
		}

		actionScaleType = ruleAction.Params[aututils.RuleScaleType]
		if len(actionScaleType) == 0 {
			return 0, &tp_errors.ErrNotSupported{
				Type:      "without param for scale type",
				Operation: "Pool expand action",
			}
		}
	}

	var (
		initialSize         = pool.StoragePoolAtInit.TotalSize
		workloadSize        = pool.WorkloadSize
		calculatedTotalSize = initialSize
		baseDiskSize        uint64
	)

	// adjust workloadSize by the initial usage that PX pools start with
	// TODO get this from porx: func (bm *btrfsMount) MkReserve(volname string, available uint64) error {
	poolBaseUsage := uint64(float64(initialSize) / 10)
	if initialSize < (32 * units.GiB) {
		poolBaseUsage = 3 * units.GiB
	}
	workloadSize += poolBaseUsage

	// get base disk size for the pool from the node spec
	for _, disk := range node.Disks {
		// NOTE: below medium check if a weak assumption and will fail if the installation has multiple pools on the node
		// with the same medium (pools with disks of different sizes but same medium). The SDK does not provide a direct
		// mapping of disks to pools so this the best we can do from SDK right now.
		if disk.Medium == pool.StoragePoolAtInit.Medium {
			baseDiskSize = disk.Size
		}
	}

	if baseDiskSize == 0 {
		return 0, fmt.Errorf("failed to detect base disk size for pool: %s", pool.Uuid)
	}

	//	The goal of the below for loop is to keep increasing calculatedTotalSize until the rule conditions match
	for {
		for _, conditionExpression := range apRule.Spec.Conditions.Expressions {
			var metricValue float64
			switch conditionExpression.Key {
			case aututils.PxPoolAvailableCapacityMetric:
				availableSize := int64(calculatedTotalSize) - int64(workloadSize)
				metricValue = float64(availableSize*100) / float64(calculatedTotalSize)
			case aututils.PxPoolTotalCapacityMetric:
				metricValue = float64(calculatedTotalSize) / units.GiB
			default:
				return 0, &tp_errors.ErrNotSupported{
					Type:      conditionExpression.Key,
					Operation: "Pool Condition Expression Key",
				}
			}
			if doesConditionMatch(metricValue, conditionExpression) {
				for _, ruleAction := range apRule.Spec.Actions {
					var requiredScaleSize float64
					if actionScalePercentageValue, ok := ruleAction.Params[aututils.RuleActionsScalePercentage]; ok {
						actionScalePercentage, err := strconv.ParseUint(actionScalePercentageValue, 10, 64)
						if err != nil {
							return 0, err
						}

						requiredScaleSize = float64(calculatedTotalSize * actionScalePercentage / 100)
					} else if actionScaleSizeValue, ok := ruleAction.Params[aututils.RuleActionsScaleSize]; ok {
						actionScaleSize, err := strconv.ParseUint(actionScaleSizeValue, 10, 64)
						if err != nil {
							a, parseErr := resource.ParseQuantity(actionScaleSizeValue)
							if parseErr != nil {
								logrus.Errorf("Can't parse actionScaleSize: '%d', cause err: %s/%s", actionScaleSize, err, parseErr)
								return 0, err
							}
							actionScaleSize = uint64(a.Value())
						}
						requiredScaleSize = float64(actionScaleSize)
					}
					if actionScaleType == aututils.RuleScaleTypeAddDisk {
						requiredNewDisks := uint64(math.Ceil(requiredScaleSize / float64(baseDiskSize)))
						calculatedTotalSize += requiredNewDisks * baseDiskSize
					} else {
						calculatedTotalSize += uint64(requiredScaleSize)
					}
				}
			} else {
				return calculatedTotalSize, nil
			}
		}
	}
}

// EstimateVolumeExpand calculates the expected size of a volume based on autopilot rule, initial and workload sizes
func (d *portworx) EstimateVolumeExpand(apRule apapi.AutopilotRule, initialSize, workloadSize uint64) (uint64, int, error) {
	resizeCount := 0
	// this method calculates expected autopilot object size for given initial and workload sizes.
	for _, ruleAction := range apRule.Spec.Actions {
		if ruleAction.Name != aututils.VolumeSpecAction {
			return 0, resizeCount, &tp_errors.ErrNotSupported{
				Type:      ruleAction.Name,
				Operation: "EstimateVolumeExpand for action",
			}
		}
	}

	calculatedTotalSize := initialSize

	//	The goal of the below for loop is to keep increasing calculatedTotalSize until the rule conditions match
	for {
		for _, conditionExpression := range apRule.Spec.Conditions.Expressions {
			var expectedMetricValue float64
			switch conditionExpression.Key {
			case aututils.PxVolumeUsagePercentMetric:
				expectedMetricValue = float64(int64(workloadSize)) * 100 / float64(calculatedTotalSize)
			case aututils.PxVolumeTotalCapacityMetric:
				expectedMetricValue = float64(calculatedTotalSize) / units.GB
			default:
				return 0, resizeCount, &tp_errors.ErrNotSupported{
					Type:      conditionExpression.Key,
					Operation: "Volume Condition Expression Key",
				}
			}

			if doesConditionMatch(expectedMetricValue, conditionExpression) {
				for _, ruleAction := range apRule.Spec.Actions {
					if actionMaxSize, ok := ruleAction.Params[aututils.RuleMaxSize]; ok {
						maxSize := parseMaxSize(actionMaxSize)
						if calculatedTotalSize >= maxSize {
							return calculatedTotalSize, 0, nil
						}
					}
					actionScalePercentage, err := strconv.ParseUint(ruleAction.Params[aututils.RuleActionsScalePercentage], 10, 64)
					if err != nil {
						return 0, 0, err
					}

					requiredScaleSize := float64(calculatedTotalSize * actionScalePercentage / 100)
					calculatedTotalSize += uint64(requiredScaleSize)
					if calculatedTotalSize != initialSize {
						resizeCount++
					}

					// check if calculated size is more than maxsize
					if actionMaxSize, ok := ruleAction.Params[aututils.RuleMaxSize]; ok {
						maxSize := parseMaxSize(actionMaxSize)
						if maxSize != 0 && calculatedTotalSize >= maxSize {
							return maxSize, resizeCount, nil
						}
					}
				}
			} else {
				return calculatedTotalSize, resizeCount, nil
			}
		}
	}
}

func doesConditionMatch(expectedMetricValue float64, conditionExpression *apapi.LabelSelectorRequirement) bool {
	condExprValue, _ := strconv.ParseFloat(conditionExpression.Values[0], 64)
	return expectedMetricValue < condExprValue && conditionExpression.Operator == apapi.LabelSelectorOpLt ||
		expectedMetricValue > condExprValue && conditionExpression.Operator == apapi.LabelSelectorOpGt
}

func parseMaxSize(maxSize string) uint64 {
	mSize, err := strconv.ParseUint(maxSize, 10, 64)
	if err != nil {
		a, parseErr := resource.ParseQuantity(maxSize)
		if parseErr != nil {
			logrus.Errorf("Can't parse maxSize: '%s', cause err: %s/%s", maxSize, err, parseErr)
			return 0
		}
		mSize = uint64(a.Value())
	}
	return mSize
}

// getRestPort gets the service port for rest api, required when using service endpoint
func getRestPort() (int32, error) {
	svc, err := k8sCore.GetService(schedops.PXServiceName, schedops.PXNamespace)
	if err != nil {
		return 0, err
	}
	for _, port := range svc.Spec.Ports {
		if port.Name == "px-api" {
			return port.Port, nil
		}
	}
	return 0, fmt.Errorf("px-api port not found in service")
}

// getRestContainerPort gets the rest api container port exposed in the node, required when using node ip
func getRestContainerPort() (int32, error) {
	svc, err := k8sCore.GetService(schedops.PXServiceName, schedops.PXNamespace)
	if err != nil {
		return 0, err
	}
	for _, port := range svc.Spec.Ports {
		if port.Name == "px-api" {
			return port.TargetPort.IntVal, nil
		}
	}
	return 0, fmt.Errorf("px-api target port not found in service")
}

// getSDKPort gets sdk service port, required when using service endpoint
func getSDKPort() (int32, error) {
	svc, err := k8sCore.GetService(schedops.PXServiceName, schedops.PXNamespace)
	if err != nil {
		return 0, err
	}
	for _, port := range svc.Spec.Ports {
		if port.Name == "px-sdk" {
			return port.Port, nil
		}
	}
	return 0, fmt.Errorf("px-sdk port not found in service")
}

// getSDKContainerPort gets the sdk container port in the node, required when using node ip
func getSDKContainerPort() (int32, error) {
	svc, err := k8sCore.GetService(schedops.PXServiceName, schedops.PXNamespace)
	if err != nil {
		return 0, err
	}
	for _, port := range svc.Spec.Ports {
		if port.Name == "px-sdk" {
			return port.TargetPort.IntVal, nil
		}
	}
	return 0, fmt.Errorf("px-sdk target port not found in service")
}

func init() {
	torpedovolume.Register(DriverName, provisioners, &portworx{})
}
