package portworx

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"reflect"
	"regexp"
	"sort"
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
	pxapi "github.com/libopenstorage/operator/api/px"
	v1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	optest "github.com/libopenstorage/operator/pkg/util/test"
	"github.com/pborman/uuid"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/operator"
	"github.com/portworx/sched-ops/task"
	driver_api "github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	torpedok8s "github.com/portworx/torpedo/drivers/scheduler/k8s"
	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/portworx/torpedo/pkg/aututils"
	tp_errors "github.com/portworx/torpedo/pkg/errors"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/netutil"
	"github.com/portworx/torpedo/pkg/osutils"
	"github.com/portworx/torpedo/pkg/s3utils"
	"github.com/portworx/torpedo/pkg/units"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// PortworxStorage portworx storage name
	PortworxStorage torpedovolume.StorageProvisionerType = "portworx"
	// PortworxCsi csi storage name
	PortworxCsi torpedovolume.StorageProvisionerType = "csi"
	// PortworxStrict provisioner for using same provisioner as provided in the spec
	PortworxStrict torpedovolume.StorageProvisionerType = "strict"
)

const (
	// DriverName is the name of the portworx driver implementation
	DriverName                                = "pxd"
	pxDiagPath                                = "/remotediags"
	pxVersionLabel                            = "PX Version"
	enterMaintenancePath                      = "/entermaintenance"
	exitMaintenancePath                       = "/exitmaintenance"
	pxSystemdServiceName                      = "portworx.service"
	tokenKey                                  = "token"
	clusterIP                                 = "ip"
	clusterPort                               = "port"
	remoteKubeConfigPath                      = "/tmp/kubeconfig"
	pxMinVersionForStorkUpgrade               = "2.1"
	formattingCommandPxctlLocalSnapshotCreate = "pxctl volume snapshot create %s --name %s"
	formattingCommandPxctlCloudSnapCreate     = "pxctl cloudsnap backup %s"
	pxctlVolumeList                           = "pxctl volume list "
	pxctlVolumeListFilter                     = "pxctl volume list -l %s=%s"
	pxctlVolumeUpdate                         = "pxctl volume update "
	pxctlGroupSnapshotCreate                  = "pxctl volume snapshot group"
	pxctlClusterOptionsUpdate                 = "pxctl cluster options update"
	pxctlDriveAddStart                        = "%s -j service drive add %s -o start"
	pxctlDriveAddStatus                       = "%s -j service drive add %s -o status"
	pxctlCloudDriveInspect                    = "%s -j cd inspect --node %s"
	refreshEndpointParam                      = "refresh-endpoint"
	defaultPXAPITimeout                       = 5 * time.Minute
	envSkipPXServiceEndpoint                  = "SKIP_PX_SERVICE_ENDPOINT"
	envSkipPxOperatorUpgrade                  = "SKIP_PX_OPERATOR_UPGRADE"
	pxCustomRegistryConfigmapName             = "px-custom-registry"
	pxVersionsConfigmapName                   = "px-versions"
	pureKey                                   = "backend"
	pureBlockValue                            = "pure_block"
	clusterUUIDFile                           = "cluster_uuid"
	pxReleaseManifestURLEnvVarName            = "PX_RELEASE_MANIFEST_URL"
	pxServiceLocalEndpoint                    = "portworx-service.kube-system.svc.cluster.local"
	mountGrepVolume                           = "mount | grep %s"
	mountGrepFirstColumn                      = "mount | grep %s | awk '{print $1}'"
	PxLabelNameKey                            = "name"
	PxLabelValue                              = "portworx"
)

const (
	defaultTimeout                    = 2 * time.Minute
	defaultRetryInterval              = 10 * time.Second
	podUpRetryInterval                = 30 * time.Second
	maintenanceOpTimeout              = 1 * time.Minute
	maintenanceWaitTimeout            = 10 * time.Minute
	inspectVolumeTimeout              = 2 * time.Minute
	inspectVolumeRetryInterval        = 3 * time.Second
	validateDeleteVolumeTimeout       = 15 * time.Minute
	validateReplicationUpdateTimeout  = 60 * time.Minute
	validateClusterStartTimeout       = 2 * time.Minute
	validatePXStartTimeout            = 5 * time.Minute
	validatePxPodsUpTimeout           = 30 * time.Minute
	validateNodeStopTimeout           = 5 * time.Minute
	validateStoragePoolSizeTimeout    = 3 * time.Hour
	validateStoragePoolSizeInterval   = 30 * time.Second
	validateRebalanceJobsTimeout      = 30 * time.Minute
	validateRebalanceJobsInterval     = 30 * time.Second
	validateDeploymentTimeout         = 3 * time.Minute
	validateDeploymentInterval        = 5 * time.Second
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
	sdkDiagCollectionTimeout          = 30 * time.Minute
	sdkDiagCollectionRetryInterval    = 10 * time.Second
	validateDiagsOnS3RetryTimeout     = 60 * time.Minute
	validateDiagsOnS3RetryInterval    = 30 * time.Second
	validateStorageClusterTimeout     = 40 * time.Minute
	expandStoragePoolTimeout          = 2 * time.Minute
	volumeUpdateTimeout               = 2 * time.Minute
	skinnySnapRetryInterval           = 5 * time.Second
)
const (
	telemetryNotEnabled = "15"
	telemetryEnabled    = "100"
)
const (
	secretName      = "openstorage.io/auth-secret-name"
	secretNamespace = "openstorage.io/auth-secret-namespace"
	// DeviceMapper is a string in dev mapper path
	DeviceMapper = "mapper"
)

const (
	driveAddSuccessStatus    = "Drive add done"
	driveExitsStatus         = "Device already exists"
	metadataAddSuccessStatus = "Successfully added metadata device"
	journalAddSuccessStatus  = "Successfully added journal device"
)

// Provisioners types of supported provisioners
var provisioners = map[torpedovolume.StorageProvisionerType]torpedovolume.StorageProvisionerType{
	PortworxStorage: "kubernetes.io/portworx-volume",
	PortworxCsi:     "pxd.portworx.com",
	PortworxStrict:  "strict",
}

var csiProvisionerOnly = map[torpedovolume.StorageProvisionerType]torpedovolume.StorageProvisionerType{
	PortworxCsi: "pxd.portworx.com",
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
var pxOperator = operator.Instance()
var apiExtentions = apiextensions.Instance()

type portworx struct {
	legacyClusterManager  cluster.Cluster
	clusterManager        api.OpenStorageClusterClient
	nodeManager           api.OpenStorageNodeClient
	mountAttachManager    api.OpenStorageMountAttachClient
	volDriver             api.OpenStorageVolumeClient
	clusterPairManager    api.OpenStorageClusterPairClient
	alertsManager         api.OpenStorageAlertsClient
	csbackupManager       api.OpenStorageCloudBackupClient
	storagePoolManager    api.OpenStoragePoolClient
	diagsManager          api.OpenStorageDiagsClient
	diagsJobManager       api.OpenStorageJobClient
	licenseManager        pxapi.PortworxLicenseClient
	licenseFeatureManager pxapi.PortworxLicensedFeatureClient
	autoFsTrimManager     api.OpenStorageFilesystemTrimClient
	portworxServiceClient pxapi.PortworxServiceClient
	schedOps              schedops.Driver
	nodeDriver            node.Driver
	namespace             string
	refreshEndpoint       bool
	token                 string
	skipPXSvcEndpoint     bool
	skipPxOperatorUpgrade bool
	DiagsFile             string

	pureDeviceBaseline map[string]map[string]pureLocalPathEntry // Stores a list of Pure mapper devices present on each storage node
}

type statusJSON struct {
	Status string
	Error  string
	Cmd    string
}

// ExpandPool resizes a pool of a given ID
func (d *portworx) ExpandPool(poolUUID string, operation api.SdkStoragePool_ResizeOperationType, size uint64, skipWaitForCleanVolumes bool) error {
	log.Infof("Initiating pool %v resize by %v with operation type %v", poolUUID, size, operation.String())

	// start a task to check if pool  resize is done
	t := func() (interface{}, bool, error) {
		jobListResp, err := d.storagePoolManager.Resize(d.getContext(), &api.SdkStoragePoolResizeRequest{
			Uuid: poolUUID,
			ResizeFactor: &api.SdkStoragePoolResizeRequest_Size{
				Size: size,
			},
			OperationType:           operation,
			SkipWaitForCleanVolumes: skipWaitForCleanVolumes,
		})
		if err != nil {
			return nil, true, err
		}
		if jobListResp.String() != "" {
			log.Debugf("Resize response: %v", jobListResp.String())
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, expandStoragePoolTimeout, defaultRetryInterval); err != nil {
		return err
	}
	return nil
}

// ExpandPoolUsingPxctlCmd resizes a pool of a given UUID using CLI command
func (d *portworx) ExpandPoolUsingPxctlCmd(n node.Node, poolUUID string, operation api.SdkStoragePool_ResizeOperationType, size uint64, skipWaitForCleanVolumes bool) error {

	var operationString string

	switch operation.String() {
	case "RESIZE_TYPE_ADD_DISK":
		operationString = "add-disk"
	case "RESIZE_TYPE_RESIZE_DISK":
		operationString = "resize-disk"
	default:
		operationString = "auto"
	}

	log.InfoD("Initiate Pool %v resize by %v with operationtype %v using CLI", poolUUID, size, operation.String())
	cmd := fmt.Sprintf("pxctl sv pool expand --uid %v --size %v --operation %v", poolUUID, size, operationString)
	if skipWaitForCleanVolumes {
		cmd = fmt.Sprintf("%s -f", cmd)
	}
	out, err := d.nodeDriver.RunCommandWithNoRetry(
		n,
		cmd,
		node.ConnectionOpts{
			Timeout:         maintenanceWaitTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		})
	if err != nil {
		return err
	}
	log.Infof("Expanding Pool with UUID [%v] to Size [%v] Successful. response: [%v]", poolUUID, size, out)
	return nil
}

// DeletePool deletes the pool with given poolID
func (d *portworx) DeletePool(n node.Node, poolID string, retry bool) error {
	log.Infof("Initiating pool deletion for ID %s on node %s", poolID, n.Name)
	cmd := fmt.Sprintf("pxctl sv pool delete %s -y", poolID)
	var out string
	var err error
	if retry {
		out, err = d.nodeDriver.RunCommand(
			n,
			cmd,
			node.ConnectionOpts{
				Timeout:         maintenanceWaitTimeout,
				TimeBeforeRetry: defaultRetryInterval,
			})
	} else {
		out, err = d.nodeDriver.RunCommandWithNoRetry(
			n,
			cmd,
			node.ConnectionOpts{
				Timeout:         maintenanceWaitTimeout,
				TimeBeforeRetry: defaultRetryInterval,
			})
	}

	if err != nil {
		return fmt.Errorf("error deleting pool on node [%s], Err: %v", n.Name, err)
	}
	log.Infof("poolID %s deletion: %s", poolID, out)
	return nil
}

// GetStorageSpec get the storage spec used to deploy portworx
func (d *portworx) GetStorageSpec() (*pxapi.StorageSpec, error) {
	storageSpecResp, err := d.portworxServiceClient.GetStorageSpec(d.getContext(), &pxapi.PxGetStorageSpecRequest{})
	var storageSpec *pxapi.StorageSpec
	if err != nil {
		err = fmt.Errorf("Error getting StorageSpec resource, Err: %v", err)
		return nil, err
	}
	storageSpec = storageSpecResp.GetSpec()
	return storageSpec, nil
}

// ListStoragePools returns all PX storage pools
func (d *portworx) ListStoragePools(labelSelector metav1.LabelSelector) (map[string]*api.StoragePool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultPXAPITimeout)
	defer cancel()

	// TODO PX SDK currently does not have a way of directly getting storage pool objects.
	// We need to list nodes and then inspect each node
	resp, err := d.nodeManager.Enumerate(ctx, &api.SdkNodeEnumerateRequest{})
	if err != nil {
		return nil, err
	}

	pools := make(map[string]*api.StoragePool)
	for _, nodeID := range resp.NodeIds {
		nodeResp, err := d.nodeManager.Inspect(ctx, &api.SdkNodeInspectRequest{NodeId: nodeID})
		if err != nil {
			return nil, err
		}

		for _, pool := range nodeResp.Node.Pools {
			matches := true
			for k, v := range labelSelector.MatchLabels {
				if v != pool.Labels[k] {
					matches = false
					break
				}
			}

			if matches {
				pools[pool.GetUuid()] = pool
			}
		}
	}
	return pools, nil
}

func (d *portworx) String() string {
	return DriverName
}

func (d *portworx) GetVolumeDriverNamespace() (string, error) {
	return d.schedOps.GetPortworxNamespace()
}

// init is all the functionality of Init, but allowing you to set a custom driver name
func (d *portworx) init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap, driverName string) error {
	log.Infof("Using the Portworx volume driver with provisioner %s under scheduler: %v", storageProvisioner, sched)
	var err error
	pxLabel := make(map[string]string)
	pxLabel[PxLabelNameKey] = PxLabelValue

	if skipStr := os.Getenv(envSkipPXServiceEndpoint); skipStr != "" {
		d.skipPXSvcEndpoint, _ = strconv.ParseBool(skipStr)
	}

	// If true, will skip upgrade of PX Operator along with PX during upgrade hops
	if skipStr := os.Getenv(envSkipPxOperatorUpgrade); skipStr != "" {
		d.skipPxOperatorUpgrade, _ = strconv.ParseBool(skipStr)
	}

	d.token = token

	if d.nodeDriver, err = node.Get(nodeDriver); err != nil {
		return err
	}

	if d.schedOps, err = schedops.Get(sched); err != nil {
		return fmt.Errorf("failed to get scheduler operator for portworx. Err: %v", err)
	}
	d.schedOps.Init()

	namespace, err := d.GetVolumeDriverNamespace()
	if err != nil {
		return err
	}
	d.namespace = namespace

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

	if err := d.updateNodes(storageNodes); err != nil {
		return err
	}

	for _, n := range node.GetStorageDriverNodes() {
		if err := d.WaitDriverUpOnNode(n, validatePXStartTimeout); err != nil {
			return err
		}
	}

	log.Infof("The following Portworx nodes are in the cluster:")
	for _, n := range storageNodes {
		log.Infof(
			"Node UID: %v Node IP: %v Node Status: %v",
			n.Id,
			n.DataIp,
			n.Status,
		)
	}
	torpedovolume.StorageDriver = driverName
	// Set provisioner for torpedo
	if storageProvisioner != "" {
		if p, ok := provisioners[torpedovolume.StorageProvisionerType(storageProvisioner)]; ok {
			torpedovolume.StorageProvisioner = p
		} else {
			return fmt.Errorf("driver %s, does not support provisioner %s", driverName, storageProvisioner)
		}
	} else {
		torpedovolume.StorageProvisioner = provisioners[torpedovolume.DefaultStorageProvisioner]
	}

	// Update node PxPodRestartCount during init
	schedDriver, err := scheduler.Get(sched)
	if err != nil {
		return fmt.Errorf("scheduler with name: [%s] not found. Error: [%v]", sched, err)
	}

	pxPodRestartCountMap, err := schedDriver.GetPodsRestartCount(namespace, pxLabel)
	if err != nil {
		return fmt.Errorf("unable to get portworx pods restart count. Error: [%v]", err)
	}

	for pod, value := range pxPodRestartCountMap {
		n, err := node.GetNodeByIP(pod.Status.HostIP)
		if err != nil {
			return err
		}
		n.PxPodRestartCount = value
		if err = node.UpdateNode(n); err != nil {
			return fmt.Errorf("updating the restart count fails for a node: [%s]. Error: [%v]", n.Name, err)
		}
	}
	return nil
}

func (d *portworx) Init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap string) error {
	return d.init(sched, nodeDriver, token, storageProvisioner, csiGenericDriverConfigMap, DriverName)
}

func (d *portworx) RefreshDriverEndpoints() error {

	secretConfigMap := flag.Lookup("config-map").Value.(flag.Getter).Get().(string)
	if secretConfigMap != "" {
		log.Infof("Fetching token from configmap: %s", secretConfigMap)
		token, err := d.schedOps.GetTokenFromConfigMap(secretConfigMap)
		if err != nil {
			return err
		}
		d.token = token
	}

	// getting namespace again (refreshing it) as namespace of portworx in switched context might have changed
	namespace, err := d.GetVolumeDriverNamespace()
	if err != nil {
		return err
	}
	d.namespace = namespace
	log.InfoD("RefreshDriverEndpoints: volume driver's namespace (portworx namespace) is updated to [%s]", namespace)

	// Force update px endpoints
	d.refreshEndpoint = true
	storageNodes, err := d.getStorageNodesOnStart()
	if err != nil {
		return err
	}

	if len(storageNodes) == 0 {
		return fmt.Errorf("cluster inspect returned empty nodes")
	}

	if err := d.updateNodes(storageNodes); err != nil {
		return err
	}
	return nil
}

func (d *portworx) updateNodes(pxNodes []*api.StorageNode) error {
	for _, n := range node.GetNodes() {
		if err := d.updateNode(&n, pxNodes); err != nil {
			return err
		}
	}
	return nil
}

func (d *portworx) printNodes(pxnodes []*api.StorageNode) {
	log.Infof("The following Portworx nodes are in the cluster:")
	for _, n := range pxnodes {
		log.Infof(
			"Node UID: %v Node IP: %v Node Status: %v",
			n.Id,
			n.DataIp,
			n.Status,
		)
	}
}

// getStoragelessNode filter out storageless nodes from all px nodes and return it
func (d *portworx) GetStoragelessNodes() ([]*api.StorageNode, error) {
	storagelessNodes := make([]*api.StorageNode, 0)

	pxnodes, err := d.getPxNodes()
	if err != nil {
		return storagelessNodes, err
	}

	for _, pxnode := range pxnodes {
		if len(pxnode.Pools) == 0 {
			storagelessNodes = append(storagelessNodes, pxnode)
		}
	}
	return storagelessNodes, nil
}

// UpdateNodeWithStorageInfo update nthe storage info to a new node object
func (d *portworx) UpdateNodeWithStorageInfo(n node.Node, skipNodeName string) error {
	log.Infof("Updating the storage info for new node [%s] ", n.Name)

	// Getting all PX nodes
	storageNodes, err := d.getPxNodes()
	if err != nil {
		return err
	}

	// Deleted node should be removed from storageNodes otherwise updateNode fails
	// finding the deletedNode index and need to remove from slice
	skipIdx := -1
	for idx, sn := range storageNodes {
		if len(skipNodeName) > 0 && sn.Hostname == skipNodeName {
			skipIdx = idx
			break
		}
	}

	// Removing from slice if it is present on slice.
	if skipIdx >= 0 {
		storageNodes = append(storageNodes[:skipIdx], storageNodes[skipIdx:]...)
	}

	if err := d.updateNode(&n, storageNodes); err != nil {
		return err
	}
	return nil
}

func (d *portworx) WaitForPxPodsToBeUp(n node.Node) error {
	// Check if PX pod is up
	log.Debugf("checking if PX pod is up on node: %s", n.Name)
	t := func() (interface{}, bool, error) {
		if !d.schedOps.IsPXReadyOnNode(n) {
			return "", true, &ErrFailedToWaitForPx{
				Node:  n,
				Cause: fmt.Sprintf("px pod is not ready on node: %s after %v", n.Name, validatePXStartTimeout),
			}
		}
		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, validatePxPodsUpTimeout, podUpRetryInterval); err != nil {
		return fmt.Errorf("PX pod failed to come up on node : [%s]. Error: [%v]", n.Name, err)
	}
	return nil
}

// ValidateNodeAfterPickingUpNodeID validates the pools and drives
func (d *portworx) ValidateNodeAfterPickingUpNodeID(delNode *api.StorageNode, newNode *api.StorageNode, storagelessNodes []*api.StorageNode) error {
	// If node is a storageless node below validation steps not needed
	if !d.validateNodeIDMigration(delNode, newNode, storagelessNodes) {
		return fmt.Errorf("validation failed: NodeId: [%s] pick-up failed by new Node: [%s]", delNode.Id, newNode.Hostname)
	}

	log.Infof("Pools and Disks are matching after new node:[%s] picked the NodeId: [%s]", newNode.Hostname, newNode.Id)
	log.Infof("After recyling a node, Node [%s] is having following pools:", newNode.Hostname)
	for _, pool := range newNode.Pools {
		log.Infof("Node [%s] is having pool id: [%s]", newNode.Hostname, pool.Uuid)
	}

	log.Infof("After recyling a node, Node [%s] is having disks: [%v]", newNode.Hostname, newNode.Disks)
	return nil
}

// Contains checks if px node is present in the given list of px nodes
func (d *portworx) Contains(nodes []*api.StorageNode, n *api.StorageNode) bool {
	for _, value := range nodes {
		if value.Hostname == n.Hostname {
			return true
		}
	}
	return false
}

// comparePoolsAndDisks compares pools and disks between two StorageNode objects
func (d *portworx) comparePoolsAndDisks(srcNode *api.StorageNode,
	dstNode *api.StorageNode) bool {
	srcPools := srcNode.Pools
	dstPools := dstNode.Pools

	// Comparing pool ids
	if len(srcPools) != len(dstPools) {
		return false
	}

	for x, pool := range srcPools {
		if pool.Uuid != dstPools[x].Uuid {
			log.Errorf("Source pools: [%v] not macthing with Destination pools: [%v]", srcPools, dstPools)
			return false
		}
	}

	// Comparing disks
	srcDisks := srcNode.Disks
	dstDisks := dstNode.Disks

	for disk, value := range srcDisks {
		if !srcDisks[disk].Metadata && !dstDisks[disk].Metadata {
			if value.Id != dstDisks[disk].Id {
				return false
			}
		} else if srcDisks[disk].Metadata && dstDisks[disk].Metadata {
			if value.Id != dstDisks[disk].Id {
				return false
			}
		}
	}
	return true
}

// validateNodeIDMigration validate the nodeID is picked by another storageless node
func (d *portworx) validateNodeIDMigration(delNode *api.StorageNode, newNode *api.StorageNode,
	storagelessNodes []*api.StorageNode) bool {
	// delNode is a deleted node and newNode is a node which picked the NodeId from delNode

	// Validate that nodeID is picked up by the storage-less node
	if len(storagelessNodes) != 0 && !d.Contains(storagelessNodes, newNode) {
		log.Errorf("Delete NodeId [%s] is not pick up by storageless node", delNode.Id)
		return false
	}

	// Validate that dirves and pool IDs are same after picking up by storage-less node
	if !d.comparePoolsAndDisks(delNode, newNode) {
		log.Errorf("Pools [%v] in deleted node are not macthing with new node pools [%v]", delNode.Pools, newNode.Pools)
		return false
	}
	return true
}

// waitForNodeIDToBePickedByAnotherNode Waits for a given nodeID to be picked up by another node
func (d *portworx) WaitForNodeIDToBePickedByAnotherNode(
	delNode *api.StorageNode) (*api.StorageNode, error) {

	t := func() (interface{}, bool, error) {
		nNode, err := d.getPxNodeByID(delNode.Id)
		if err != nil {
			return nil, true, err
		}
		if nNode.Hostname == delNode.Hostname {
			return nil, true, fmt.Errorf("waiting for NodeId [%v] to be picked by another node", delNode.Id)
		}
		if len(nNode.Pools) == 0 {
			return nil, true, fmt.Errorf("waiting for storage to be up in node [%s]", nNode.Hostname)
		}
		return nNode, false, nil
	}

	result, err := task.DoRetryWithTimeout(t, validateStoragePoolSizeTimeout, validateStoragePoolSizeInterval)
	if err != nil {
		return nil, err
	}

	if result == nil {
		return nil, fmt.Errorf("failed to pick NodeId [%s] by PX nodes in a cluster", delNode.Id)
	}
	return result.(*api.StorageNode), nil
}

// IsDriverInstalled returns if PX is installed on a node
func (d *portworx) IsDriverInstalled(n node.Node) (bool, error) {
	log.Infof("Checking if PX is installed on node [%s]", n.Name)
	pxInstalled, err := d.schedOps.IsPXEnabled(n)
	if err != nil {
		return false, fmt.Errorf("Failed to check if PX is installed on node [%s], Err: %v", n.Name, err)
	}
	return pxInstalled, nil
}

func (d *portworx) updateNode(n *node.Node, pxNodes []*api.StorageNode) error {
	//log.Infof("Updating node %+v", *n) // NOTE: Do we really need to print the whole node?
	log.Infof("Updating node [%s]", n.Name)
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
			//log.Debugf("Checking PX node %+v for address %s", pxNode, address) // NOTE: Do we really need to print the whole node?
			log.Debugf("Checking PX node [%s] for address [%s]", pxNode.Hostname, address)
			if address == pxNode.DataIp || address == pxNode.MgmtIp || n.Name == pxNode.SchedulerNodeName {
				if len(pxNode.Id) > 0 {
					n.StorageNode = pxNode
					n.VolDriverNodeID = pxNode.Id
					n.IsStorageDriverInstalled = isPX
					// TODO: PTX-2445 Replace isMetadataNode API call with SDK call
					isMetadataNode, err := d.isMetadataNode(*n, address)
					if err != nil {
						log.Warnf("Can not check if node [%s] is a metadata node", n.Name)
					}
					n.IsMetadataNode = isMetadataNode

					if pxNode.Pools != nil && len(pxNode.Pools) > 0 {
						log.Infof("Updating node [%s] as storage node", n.Name)
					}

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
						return fmt.Errorf("failed to update node [%s], Err: %v", n.Name, err)
					}
				} else {
					return fmt.Errorf("StorageNodeId is empty for node [%s]", pxNode.Hostname)
				}
				return nil
			}
		}
	}

	// Return error where PX is not explicitly disabled but was not found installed
	return fmt.Errorf("failed to find PX node for node [%s] in PX node list: %v", n.Name, pxNodes)
}

func (d *portworx) isMetadataNode(node node.Node, address string) (bool, error) {
	members, err := d.GetKvdbMembers(node)
	if err != nil {
		return false, fmt.Errorf("failed to get metadata nodes, Err: %v", err)
	}

	ipRegex := regexp.MustCompile(`http://(?P<address>.*):d+`)
	for _, value := range members {
		for _, url := range value.ClientUrls {
			result := getGroupMatches(ipRegex, url)
			if val, ok := result["address"]; ok {
				val = strings.Trim(val, "[]")
				if address == val {
					log.Debugf("Node [%s] is a metadata node", node.Name)
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func (d *portworx) ListAllVolumes() ([]string, error) {
	volDriver := d.getVolDriver()
	volumes, err := volDriver.Enumerate(d.getContext(), &api.SdkVolumeEnumerateRequest{})
	if err != nil {
		return nil, err
	}

	return volumes.GetVolumeIds(), nil
}

func (d *portworx) CreateVolume(volName string, size uint64, haLevel int64) (string, error) {
	volDriver := d.getVolDriver()
	resp, err := volDriver.Create(d.getContext(),
		&api.SdkVolumeCreateRequest{
			Name: volName,
			Spec: &api.VolumeSpec{
				Size:    size,
				HaLevel: haLevel,
				Format:  api.FSType_FS_TYPE_EXT4,
			},
		})
	if err != nil {
		return "", fmt.Errorf("failed to create volume, Err: %v", err)
	}

	log.Infof("Successfully created Portworx volume [%s], size %v, ha %v", resp.VolumeId, size, haLevel)
	return resp.VolumeId, nil
}

// CreateVolumeUsingPxctlCmd resizes a pool of a given UUID using CLI command
func (d *portworx) CreateVolumeUsingPxctlCmd(n node.Node, volName string, size uint64, haLevel int64) error {
	log.InfoD("Initiate Volume create with Volume Name %s", volName)
	cmd := fmt.Sprintf("pxctl volume create %s --size %v --repl %v", volName, size, haLevel)
	out, err := d.nodeDriver.RunCommandWithNoRetry(
		n,
		cmd,
		node.ConnectionOpts{
			Timeout:         maintenanceWaitTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		})
	if err != nil {
		return err
	}
	log.Infof("Create Volume with Name [%v] to Size [%v] Successful. response: [%v]", volName, size, out)
	return nil
}

// ResizeVolume resizes Volume to specific size provided
func (d *portworx) ResizeVolume(volName string, size uint64) error {
	volDriver := d.getVolDriver()
	volumeInspectResponse, err := volDriver.Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volName})
	if err != nil {
		return fmt.Errorf("failed to find volume %v due to %v", volName, err)
	}
	volumeSpecUpdate := &api.VolumeSpecUpdate{
		SizeOpt: &api.VolumeSpecUpdate_Size{Size: size},
	}
	_, err = volDriver.Update(d.getContext(), &api.SdkVolumeUpdateRequest{
		VolumeId: volumeInspectResponse.Volume.Id,
		Spec:     volumeSpecUpdate,
	})
	if err != nil {
		return err
	}
	return nil
}

func (d *portworx) CreateVolumeUsingRequest(request *api.SdkVolumeCreateRequest) (string, error) {
	volDriver := d.getVolDriver()
	resp, err := volDriver.Create(d.getContext(), request)
	if err != nil {
		return "", fmt.Errorf("failed to create volume, Err: %v", err)
	}

	log.Infof("Successfully created Portworx volume [%s]", resp.VolumeId)
	return resp.VolumeId, nil
}

func (d *portworx) CloneVolume(volumeID string) (string, error) {
	volDriver := d.getVolDriver()
	volumeInspectResponse, err := volDriver.Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volumeID})
	if err != nil {
		return "", fmt.Errorf("failed to find volume %v due to %v", volumeID, err)
	}
	pxVolume := volumeInspectResponse.Volume

	volID := pxVolume.Id
	cloneVolumeName := pxVolume.Locator.Name + "_clone"

	volumeCloneResp, err := volDriver.Clone(d.getContext(), &api.SdkVolumeCloneRequest{ParentId: volID, Name: cloneVolumeName})
	if err != nil {
		return "", fmt.Errorf("failed to clone volume [%s], Err: %v", pxVolume.Id, err)
	}
	if volumeCloneResp.VolumeId == "" {
		return "", fmt.Errorf("cloned volume id returned was null")
	}
	log.Infof("Successfully cloned volume [%s] as [%s]", volumeID, volumeCloneResp.VolumeId)
	return volumeCloneResp.VolumeId, nil
}

func (d *portworx) AttachVolume(volumeID string) (string, error) {
	manager := d.getMountAttachManager()
	resp, err := manager.Attach(d.getContext(),
		&api.SdkVolumeAttachRequest{
			VolumeId: volumeID,
		})
	if err != nil {
		return "", fmt.Errorf("failed to attach volume [%s], Err: %v", volumeID, err)
	}
	log.Infof("successfully attached Portworx volume [%s]", volumeID)
	return resp.DevicePath, nil
}

func (d *portworx) DetachVolume(volumeID string) error {
	manager := d.getMountAttachManager()
	_, err := manager.Detach(d.getContext(),
		&api.SdkVolumeDetachRequest{
			VolumeId: volumeID,
		})
	if err != nil {
		return fmt.Errorf("failed to detach volume [%s], Err: %v", volumeID, err)
	}
	log.Infof("Successfully detached Portworx volume [%s] ", volumeID)
	return nil
}

func (d *portworx) DeleteVolume(volumeID string) error {
	volDriver := d.getVolDriver()
	volumeInspectResponse, err := volDriver.Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volumeID})
	if err != nil {
		return fmt.Errorf("failed to find volume [%s], Err: %v", volumeID, err)
	}

	pxVolume := volumeInspectResponse.Volume
	volID := pxVolume.Id
	if _, err := volDriver.Delete(d.getContext(), &api.SdkVolumeDeleteRequest{VolumeId: volID}); err != nil {
		return fmt.Errorf("failed to delete volume [%s], Err: %v", pxVolume.Id, err)
	}

	log.Infof("Successfully deleted Portworx volume [%s]", volID)
	return nil
}

func (d *portworx) CreateSnapshot(volumeID string, snapName string) (*api.SdkVolumeSnapshotCreateResponse, error) {
	volDriver := d.getVolDriver()
	snapshotResponse, err := volDriver.SnapshotCreate(d.getContext(), &api.SdkVolumeSnapshotCreateRequest{VolumeId: volumeID, Name: snapName})
	if err != nil {
		return nil, fmt.Errorf("Error while creating snapshot [%s] on the volume [%s], Err: %v", snapName, volumeID, err)
	}
	return snapshotResponse, nil

}

func (d *portworx) InspectVolume(name string) (*api.Volume, error) {
	response, err := d.getVolDriver().Inspect(d.getContextWithToken(context.Background(), d.token), &api.SdkVolumeInspectRequest{VolumeId: name})
	if err != nil {
		return nil, err
	}

	return response.Volume, nil
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
				if _, err := d.getMountAttachManager().Unmount(d.getContext(), &api.SdkVolumeUnmountRequest{VolumeId: pxVolume.Id, MountPath: path}); err != nil {
					return fmt.Errorf("failed to unmount volume [%s] at %s, Err: %v", pxVolume.Id, path, err)
				}
			}

			if _, err := d.mountAttachManager.Detach(d.getContext(), &api.SdkVolumeDetachRequest{VolumeId: pxVolume.Id}); err != nil {
				return fmt.Errorf("failed to detach volume [%s], Err: %v", pxVolume.Id, err)
			}

			if _, err := volDriver.Delete(d.getContext(), &api.SdkVolumeDeleteRequest{VolumeId: pxVolume.Id}); err != nil {
				return fmt.Errorf("failed to delete volume [%s], Err: %v", pxVolume.Id, err)
			}

			log.Infof("Successfully removed Portworx volume [%s]", volumeName)
			return nil
		}
	}
	return nil
}

// GetDriverNode gets and returns PX node
func (d *portworx) GetDriverNode(n *node.Node, nManager ...api.OpenStorageNodeClient) (*api.StorageNode, error) {
	if len(nManager) == 0 {
		d.refreshEndpoint = true
		nManager = []api.OpenStorageNodeClient{d.getNodeManager()}
	}
	log.Debugf("Inspecting node [%s] with volume driver node id [%s]", n.Name, n.VolDriverNodeID)
	nodeInspectResponse, err := nManager[0].Inspect(d.getContext(), &api.SdkNodeInspectRequest{NodeId: n.VolDriverNodeID})
	if err != nil {
		if isNodeNotFound(err) {
			log.Warnf("Node [%s] with ID [%s] is not found, trying to update node ID...", n.Name, n.VolDriverNodeID)
			n, err = d.updateNodeID(n, nManager...)
			if err == nil {
				return d.GetDriverNode(n, nManager...)
			}
		}
		return &api.StorageNode{Status: api.Status_STATUS_NONE}, err
	}
	return nodeInspectResponse.Node, nil
}

func isNodeNotFound(err error) bool {
	st, _ := status.FromError(err)
	// TODO when a node is not found sometimes we get an error code internal, as workaround we check for internal error and substring
	return err != nil && (st.Code() == codes.NotFound || (st.Code() == codes.Internal && strings.Contains(err.Error(), "Unable to locate node")))
}

// GetDriverVersion gets and return PX version from any first PX node
func (d *portworx) GetDriverVersion() (string, error) {
	nodeList := node.GetStorageDriverNodes()
	if len(nodeList) == 0 {
		return "", fmt.Errorf("failed to find any PX nodes")
	}
	pxNode := nodeList[0]
	pxVersion, err := d.getPxVersionOnNode(pxNode)
	if err != nil {
		return "", fmt.Errorf("failed to get PX version on node [%s], Err: %v", pxNode.Name, err)
	}
	return pxVersion, nil
}

// GetDriverVersionOnNode gets and return PX version from a given PX node
func (d *portworx) GetDriverVersionOnNode(n node.Node) (string, error) {
	pxVersion, err := d.getPxVersionOnNode(n)
	if err != nil {
		return "", fmt.Errorf("failed to get PX version on node [%s], Err: %v", n.Name, err)
	}
	return pxVersion, nil
}

func (d *portworx) getPxVersionOnNode(n node.Node, nodeManager ...api.OpenStorageNodeClient) (string, error) {
	t := func() (interface{}, bool, error) {
		log.Debugf("Getting PX version on node [%s]", n.Name)
		pxNode, err := d.GetDriverNode(&n, nodeManager...)
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
	return fmt.Sprintf("%s", pxVersion), nil
}

func (d *portworx) GetStorageDevices(n node.Node) ([]string, error) {
	pxNode, err := d.GetDriverNode(&n)
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
	if err := d.EnterMaintenance(n); err != nil {
		return err
	}
	//wait for node to enter maintenance mode
	time.Sleep(1 * time.Minute)

	if err := d.ExitMaintenance(n); err != nil {
		return err
	}
	return nil
}

// UpdatePoolIOPriority Updates IO Priority of the pool
func (d *portworx) UpdatePoolIOPriority(n node.Node, poolUUID string, IOPriority string) error {
	cmd := fmt.Sprintf("pxctl sv pool update -u %s --io_priority %s", poolUUID, IOPriority)
	out, err := d.nodeDriver.RunCommand(
		n,
		cmd,
		node.ConnectionOpts{
			Timeout:         maintenanceWaitTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		})
	if err != nil {
		return fmt.Errorf("Updating Pool IO Priority failed on Node [%s], Err: [%v]", n.Name, err)
	}
	log.Infof("Updated Pool IO Priority to [%s] successfully, output: [%s]", IOPriority, out)
	return nil
}

func (d *portworx) EnterMaintenance(n node.Node) error {
	t := func() (interface{}, bool, error) {
		if err := d.maintenanceOp(n, enterMaintenancePath); err != nil {

			return nil, true, err
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, maintenanceOpTimeout, defaultRetryInterval); err != nil {
		return err
	}
	log.Infof("waiting for 3 mins allowing node to completely transition to maintenance mode")
	time.Sleep(3 * time.Minute)
	t = func() (interface{}, bool, error) {
		apiNode, err := d.GetDriverNode(&n)
		if err != nil {
			return nil, true, err
		}
		if apiNode.Status == api.Status_STATUS_MAINTENANCE {
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("node [%s], Current status: %v, Expected: %v", n.Name, apiNode.Status, api.Status_STATUS_MAINTENANCE)
	}

	if _, err := task.DoRetryWithTimeout(t, maintenanceWaitTimeout, defaultRetryInterval); err != nil {
		return &ErrFailedToEnterMaintenence{
			Node:  n,
			Cause: err.Error(),
		}
	}

	return nil
}

func (d *portworx) ExitMaintenance(n node.Node) error {
	t := func() (interface{}, bool, error) {
		if err := d.maintenanceOp(n, exitMaintenancePath); err != nil {
			return nil, true, err
		}
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, maintenanceOpTimeout, defaultRetryInterval); err != nil {
		return err
	}

	t = func() (interface{}, bool, error) {
		apiNode, err := d.GetDriverNode(&n)
		if err != nil {
			return nil, true, err
		}
		if apiNode.Status == api.Status_STATUS_OK {
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("Node [%s] is not up after exiting Maintenance mode", n.Name)
	}

	if _, err := task.DoRetryWithTimeout(t, maintenanceWaitTimeout, defaultRetryInterval); err != nil {
		return err
	}
	return nil
}

func (d *portworx) EnterPoolMaintenance(n node.Node) error {
	cmd := fmt.Sprintf("pxctl sv pool maintenance -e -y")
	out, err := d.nodeDriver.RunCommand(
		n,
		cmd,
		node.ConnectionOpts{
			Timeout:         maintenanceWaitTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		})
	if err != nil {
		return fmt.Errorf("error when entering pool maintenance on node [%s], Err: %v", n.Name, err)
	}
	log.Infof("Enter pool maintenance %s", out)
	log.Infof("waiting for 3 mins allowing pool to completely transition to maintenance mode")
	time.Sleep(3 * time.Minute)
	return nil
}

func (d *portworx) ExitPoolMaintenance(n node.Node) error {

	// no need to exit pool maintenance if node status is up
	pxStatus, err := d.GetPxctlStatus(n)
	if err == nil && pxStatus == api.Status_STATUS_OK.String() {
		log.Infof("node is up, no need to exit pool maintenance mode")
		return nil
	}

	cmd := fmt.Sprintf("pxctl sv pool maintenance -x -y")
	out, err := d.nodeDriver.RunCommand(
		n,
		cmd,
		node.ConnectionOpts{
			Timeout:         maintenanceWaitTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		})
	if err != nil {
		return fmt.Errorf("error when exiting pool maintenance on node [%s], Err: %v", n.Name, err)
	}
	log.Infof("Exit pool maintenance %s", out)
	log.Infof("waiting for 3 mins allowing pool to completely transition out of maintenance mode")
	time.Sleep(3 * time.Minute)
	return nil
}

func (d *portworx) RecoverPool(n node.Node) error {

	if err := d.EnterPoolMaintenance(n); err != nil {
		return err
	}

	//wait for pool to enter maintenance mode
	time.Sleep(1 * time.Minute)

	if err := d.ExitPoolMaintenance(n); err != nil {
		return err
	}

	return nil
}

func (d *portworx) GetNodePoolsStatus(n node.Node) (map[string]string, error) {
	cmd := fmt.Sprintf("%s sv pool show | grep -e UUID -e Status", d.getPxctlPath(n))
	out, err := d.nodeDriver.RunCommand(
		n,
		cmd,
		node.ConnectionOpts{
			Timeout:         validatePXStartTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		})
	if err != nil {
		return nil, fmt.Errorf("error getting pool status on node [%s], Err: %v", n.Name, err)
	}
	log.Debugf("GetNodePoolsStatus output: %s", out)
	outLines := strings.Split(out, "\n")

	poolsData := make(map[string]string)
	var poolId string
	var status string
	for _, l := range outLines {
		line := strings.Trim(l, " ")
		if strings.Contains(line, "UUID") {
			poolId = strings.Split(line, ":")[1]
			poolId = strings.Trim(poolId, " ")
		}
		if strings.Contains(line, "Status") {
			status = strings.Split(line, ":")[1]
			status = strings.Trim(status, " ")
		}
		if poolId != "" && status != "" {
			// this condition is required to consider only pool status when pool has both pool status and LastOperation status fields
			if _, ok := poolsData[poolId]; !ok {
				poolsData[poolId] = status
			}
			poolId = ""
		}
		status = ""
	}
	return poolsData, nil
}

// Return latest node PoolUUID -> ID
func (d *portworx) GetNodePools(n node.Node) (map[string]string, error) {
	cmd := fmt.Sprintf("%s sv pool show | grep -e Pool -e UUID", d.getPxctlPath(n))
	out, err := d.nodeDriver.RunCommand(
		n,
		cmd,
		node.ConnectionOpts{
			Timeout:         validatePXStartTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		})
	if err != nil {
		return nil, fmt.Errorf("error getting pool info on node [%s], Err: %v", n.Name, err)
	}
	outLines := strings.Split(out, "\n")

	poolsData := make(map[string]string)
	var poolId string
	var poolUUID string
	for _, l := range outLines {
		line := strings.Trim(l, " ")
		if strings.Contains(line, "UUID") {
			poolUUID = strings.Split(line, ":")[1]
			poolUUID = strings.Trim(poolUUID, " ")
		}
		if strings.Contains(line, "Pool") {
			poolId = strings.Split(line, ":")[1]
			poolId = strings.Trim(poolId, " ")
		}
		if poolId != "" && poolUUID != "" {
			if _, ok := poolsData[poolUUID]; !ok {
				poolsData[poolUUID] = poolId
			}
			poolUUID = ""
		}
		poolId = ""
	}
	return poolsData, nil
}

func (d *portworx) ValidateCreateVolume(volumeName string, params map[string]string) error {
	var token string
	token = d.getTokenForVolume(volumeName, params)
	if val, hasKey := params[refreshEndpointParam]; hasKey {
		refreshEndpoint, _ := strconv.ParseBool(val)
		d.refreshEndpoint = refreshEndpoint
	}
	volDriver := d.getVolDriver()
	t := func() (interface{}, bool, error) {
		volumeInspectResponse, err := volDriver.Inspect(d.getContextWithToken(context.Background(), token),
			&api.SdkVolumeInspectRequest{
				VolumeId: volumeName,
				Options: &api.VolumeInspectOptions{
					Deep: true,
				},
			})
		if err != nil {
			return nil, true, err
		}

		vol := volumeInspectResponse.Volume

		// Status
		if vol.Status != api.VolumeStatus_VOLUME_STATUS_UP {
			return nil, true, &ErrFailedToInspectVolume{
				ID: volumeName,
				Cause: fmt.Sprintf("Volume has invalid status. Expected: %v, Actual: %v",
					api.VolumeStatus_VOLUME_STATUS_UP, vol.Status),
			}
		}

		// State
		if vol.State == api.VolumeState_VOLUME_STATE_ERROR || vol.State == api.VolumeState_VOLUME_STATE_DELETED {
			return nil, true, &ErrFailedToInspectVolume{
				ID:    volumeName,
				Cause: fmt.Sprintf("Volume has invalid state. Actual:%v", vol.State),
			}
		}

		// DevicePath
		// TODO: remove this retry once PWX-27773 is fixed
		// It is noted that the DevicePath is intermittently empty.
		// This check ensures the device path is not empty for attached volumes
		if vol.State == api.VolumeState_VOLUME_STATE_ATTACHED && vol.AttachedState == api.AttachState_ATTACH_STATE_EXTERNAL && vol.DevicePath == "" {
			return vol, true, fmt.Errorf("device path is not present for volume: %s", volumeName)
		}

		return vol, false, nil
	}

	out, err := task.DoRetryWithTimeout(t, inspectVolumeTimeout, inspectVolumeRetryInterval)
	if err != nil {
		return &ErrFailedToInspectVolume{
			ID:    volumeName,
			Cause: fmt.Sprintf("Volume inspect returned err: %v", err),
		}
	}

	vol := out.(*api.Volume)

	// if the volume is a clone or a snap, validate its parent
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
				Cause: fmt.Sprintf("Snapshot/Clone validation failed, Err: %v", err),
			}
		}
		return nil
	}

	// Validate Device Path for a Volume
	if vol.Spec.ProxySpec != nil && vol.Spec.ProxySpec.ProxyProtocol == api.ProxyProtocol_PROXY_PROTOCOL_PURE_BLOCK {
		// Checking the device path when state is attached
		if vol.State == api.VolumeState_VOLUME_STATE_ATTACHED && !strings.Contains(vol.DevicePath, DeviceMapper) {
			return &ErrFailedToInspectVolume{
				ID:    volumeName,
				Cause: fmt.Sprintf("Failed to validate device path [%s]", vol.DevicePath),
			}
		}
		log.Debugf("Successfully validated the device path for a volume [%s]", volumeName)
	}

	// If CSI Topology key is set in param, validate volume attached on right node
	// Skiping the check for FB volumes as they never be attached
	if _, hasKey := params[torpedok8s.TopologyZoneK8sNodeLabel]; hasKey {
		if vol.Spec.ProxySpec != nil && vol.Spec.ProxySpec.ProxyProtocol != api.ProxyProtocol_PROXY_PROTOCOL_PURE_FILE {
			if err := d.ValidateVolumeTopology(vol, params); err != nil {
				return &ErrFailedToInspectVolume{
					ID:    volumeName,
					Cause: fmt.Sprintf("Topology Inspect failed, Err: %v", err),
				}
			}
		}
	}

	// Labels
	var pxNodes []*api.StorageNode
	for _, rs := range vol.ReplicaSets {
		for _, n := range rs.Nodes {
			nodeResponse, err := d.getNodeManager().Inspect(d.getContextWithToken(context.Background(), token), &api.SdkNodeInspectRequest{NodeId: n})
			if err != nil {
				return &ErrFailedToInspectVolume{
					ID:    volumeName,
					Cause: fmt.Sprintf("Failed to inspect ReplicaSet on node %s, Err: %v", n, err),
				}
			}

			pxNodes = append(pxNodes, nodeResponse.Node)
		}
	}

	// Spec
	requestedSpec, requestedLocator, _, err := spec.NewSpecHandler().SpecFromOpts(params)
	if err != nil {
		return &ErrFailedToInspectVolume{
			ID:    volumeName,
			Cause: fmt.Sprintf("failed to parse requested spec of volume, Err: %v", err),
		}
	}

	delete(vol.Locator.VolumeLabels, "pvc") // special handling for the new pvc label added in k8s
	deleteLabelsFromRequestedSpec(requestedLocator)

	// Params/Options
	// TODO check why PX-Backup does not copy group params correctly after restore
	checkVolSpecGroup := true
	if _, ok := params["backupGroupCheckSkip"]; ok {
		log.Infof("Skipping group/label check, specifically for PX-Backup")
		checkVolSpecGroup = false
	}
	for k, v := range params {
		switch k {
		case api.SpecNodes:
			if v != strings.Join(vol.Spec.ReplicaSet.Nodes, ",") {
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
			/* Ignore shared setting.
			case api.SpecShared:
				if requestedSpec.Shared != vol.Spec.Shared {
					return errFailedToInspectVolume(volumeName, k, requestedSpec.Shared, vol.Spec.Shared)
				}
			*/
		case api.SpecSticky:
			if requestedSpec.Sticky != vol.Spec.Sticky {
				return errFailedToInspectVolume(volumeName, k, requestedSpec.Sticky, vol.Spec.Sticky)
			}
		case api.SpecGroup:
			// TODO Check Px-backup labels not getting restored
			if checkVolSpecGroup {
				if !reflect.DeepEqual(requestedSpec.Group, vol.Spec.Group) {
					return errFailedToInspectVolume(volumeName, k, requestedSpec.Group, vol.Spec.Group)
				}
			}
		case api.SpecGroupEnforce:
			if requestedSpec.GroupEnforced != vol.Spec.GroupEnforced {
				return errFailedToInspectVolume(volumeName, k, requestedSpec.GroupEnforced, vol.Spec.GroupEnforced)
			}
		// portworx injects pvc name and namespace labels so response object won't be equal to request
		case api.SpecLabels:
			// TODO Check Px-backup labels not getting restored
			if checkVolSpecGroup {
				for requestedLabelKey, requestedLabelValue := range requestedLocator.VolumeLabels {
					// check requested label is not in 'ignore' list
					if labelValue, exists := vol.Locator.VolumeLabels[requestedLabelKey]; !exists || requestedLabelValue != labelValue {
						return errFailedToInspectVolume(volumeName, k, requestedLocator.VolumeLabels, vol.Locator.VolumeLabels)
					}
				}
			}
		case api.SpecIoProfile:
			// Reference: https://docs.portworx.com/portworx-enterprise/concepts/io-profiles
			if requestedSpec.IoProfile != vol.DerivedIoProfile {
				switch requestedSpec.IoProfile {
				// The "db" and "sequential" IO profiles are deprecated in newer versions of
				// Portworx, with legacy volumes labeled as such now internally treated as "auto"
				// profile volumes.
				case api.IoProfile_IO_PROFILE_AUTO, api.IoProfile_IO_PROFILE_DB, api.IoProfile_IO_PROFILE_SEQUENTIAL:
					// The "auto" IO profile selects "db_remote" for volumes with a replication factor
					// of 2 or greater, and selects "none" otherwise.
					if vol.DerivedIoProfile != api.IoProfile_IO_PROFILE_DB_REMOTE && vol.DerivedIoProfile != api.IoProfile_IO_PROFILE_NONE {
						log.Infof("requested Spec: %+v", requestedSpec)
						log.Infof("actual Spec: %+v", vol)
						return errFailedToInspectVolume(volumeName, k, requestedSpec.IoProfile.String(), vol.DerivedIoProfile.String())
					}
				case api.IoProfile_IO_PROFILE_AUTO_JOURNAL:
					// The "auto_journal" IO profile dynamically switches between "none" and "journal" based on
					// 24-second analyses of write patterns, optimizing application performance accordingly.
					if vol.DerivedIoProfile != api.IoProfile_IO_PROFILE_JOURNAL && vol.DerivedIoProfile != api.IoProfile_IO_PROFILE_NONE {
						log.Infof("requested Spec: %+v", requestedSpec)
						log.Infof("actual Spec: %+v", vol)
						return errFailedToInspectVolume(volumeName, k, requestedSpec.IoProfile.String(), vol.DerivedIoProfile.String())
					}
				case api.IoProfile_IO_PROFILE_DB_REMOTE:
					// The "db_remote" IO profile utilizes a write-back flush coalescing algorithm
					// that consolidates syncs within 100ms into a single operation, necessitating at
					// least two replications (HA factor) for reliability.
					if vol.Spec.HaLevel >= 2 {
						log.Infof("requested Spec: %+v", requestedSpec)
						log.Infof("actual Spec: %+v", vol)
						return errFailedToInspectVolume(volumeName, k, requestedSpec.IoProfile.String(), vol.DerivedIoProfile.String())
					}
				default:
					log.Infof("requested Spec: %+v", requestedSpec)
					log.Infof("actual Spec: %+v", vol)
					return errFailedToInspectVolume(volumeName, k, requestedSpec.IoProfile.String(), vol.DerivedIoProfile.String())
				}
			}
		case api.SpecSize:
			if requestedSpec.Size != vol.Spec.Size {
				return errFailedToInspectVolume(volumeName, k, requestedSpec.Size, vol.Spec.Size)
			}
		default:
		}
	}

	log.Infof("Successfully inspected volume [%v] (%v)", vol.Locator.Name, vol.Id)
	return nil
}

// ValidateVolumeTopology validate volume attached to matched topology label node
func (d *portworx) ValidateVolumeTopology(vol *api.Volume, params map[string]string) error {
	var topoMatches bool
	var err error
	zone := params[torpedok8s.TopologyZoneK8sNodeLabel]
	nodes := node.GetNodesByTopologyZoneLabel(zone)
	for _, node := range nodes {
		if topoMatches, err = d.IsVolumeAttachedOnNode(vol, node); err != nil {
			return err
		}
		if topoMatches {
			return nil
		}
	}
	return &ErrCsiTopologyMismatch{
		VolName: vol.Locator.Name,
		Cause:   fmt.Errorf("volume [%s] is not attched on nodes with topology label [%s]", vol.Id, zone),
	}
}

func (d *portworx) ValidateCreateSnapshot(volumeName string, params map[string]string) error {
	// TODO: this should be refactored so we apply snapshot specs from the app specs instead
	var token string
	token = d.getTokenForVolume(volumeName, params)
	if val, hasKey := params[refreshEndpointParam]; hasKey {
		refreshEndpoint, _ := strconv.ParseBool(val)
		d.refreshEndpoint = refreshEndpoint
	}

	volDriver := d.getVolDriver()
	if _, err := volDriver.SnapshotCreate(d.getContextWithToken(context.Background(), token), &api.SdkVolumeSnapshotCreateRequest{VolumeId: volumeName, Name: volumeName + "_snapshot"}); err != nil {
		return fmt.Errorf("failed to create local snapshot, Err: %v", err)
	}
	return nil
}

func (d *portworx) ValidateCreateSnapshotUsingPxctl(volumeName string) error {
	// TODO: this should be refactored so we apply snapshot specs from the app specs instead
	nodes := node.GetStorageDriverNodes()
	_, err := d.nodeDriver.RunCommandWithNoRetry(nodes[0], fmt.Sprintf(formattingCommandPxctlLocalSnapshotCreate, volumeName, constructSnapshotName(volumeName)), node.ConnectionOpts{
		Timeout:         crashDriverTimeout,
		TimeBeforeRetry: defaultRetryInterval,
	})
	if err != nil {
		return fmt.Errorf("failed to create local snapshot using PXCTL, Err: %v", err)
	}
	return nil
}

func (d *portworx) UpdateIOPriority(volumeName string, priorityType string) error {
	nodes := node.GetStorageDriverNodes()
	cmd := fmt.Sprintf("%s --io_priority %s  %s", pxctlVolumeUpdate, priorityType, volumeName)
	_, err := d.nodeDriver.RunCommandWithNoRetry(
		nodes[0],
		cmd,
		node.ConnectionOpts{
			Timeout:         crashDriverTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		})
	if err != nil {
		return fmt.Errorf("failed setting IO priority, Err: %v", err)
	}
	return nil
}

func (d *portworx) UpdateStickyFlag(volumeName, stickyOption string) error {
	nodes := node.GetStorageDriverNodes()
	cmd := fmt.Sprintf("%s %s --sticky %s", pxctlVolumeUpdate, volumeName, stickyOption)
	_, err := d.nodeDriver.RunCommandWithNoRetry(
		nodes[0],
		cmd,
		node.ConnectionOpts{
			Timeout:         crashDriverTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		})
	if err != nil {
		return fmt.Errorf("failed setting sticky option to %s for volume %s, Err: %v", stickyOption, volumeName, err)
	}
	return nil
}

func (d *portworx) ValidatePureFaFbMountOptions(volumeName string, mountoption []string, volumeNode *node.Node) error {
	cmd := fmt.Sprintf(mountGrepVolume, volumeName)
	out, err := d.nodeDriver.RunCommandWithNoRetry(
		*volumeNode,
		cmd,
		node.ConnectionOpts{
			Timeout:         crashDriverTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		})
	if err != nil {
		return fmt.Errorf("Failed to get mount response for volume %s", volumeName)
	}
	for _, m := range mountoption {
		if strings.Contains(out, m) {
			log.Infof("%s option is available in the mount options of volume %s", m, volumeName)
		} else {
			return fmt.Errorf("Failed to get %s option in the mount options", m)
		}
	}
	return nil

}

// ValidatePureFaCreateOptions validates FStype and createoptions with block size 2048 on those FStypes
func (d *portworx) ValidatePureFaCreateOptions(volumeName string, FStype string, volumeNode *node.Node) error {
	// Checking if file systems are properly set
	FScmd := fmt.Sprintf(mountGrepVolume, volumeName)
	FSout, err := d.nodeDriver.RunCommandWithNoRetry(
		*volumeNode,
		FScmd,
		node.ConnectionOpts{
			Timeout:         crashDriverTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		})
	if err != nil {
		return fmt.Errorf("Failed to get mount response for volume %s, Err: %v", volumeName, err)
	}
	if strings.Contains(FSout, FStype) {
		log.Infof("%s file system is available in the volume %s", FStype, volumeName)
	} else {
		return fmt.Errorf("Failed to get %s File system, Err: %v", FStype, err)
	}

	// Getting mapper volumename where createoptions are applied
	mapperCmd := fmt.Sprintf(mountGrepFirstColumn, volumeName)
	mapperOut, err := d.nodeDriver.RunCommandWithNoRetry(
		*volumeNode,
		mapperCmd,
		node.ConnectionOpts{
			Timeout:         crashDriverTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		})
	if err != nil {
		return fmt.Errorf("Failed to get attached volume for create option for pvc %s, Err: %v", volumeName, err)
	}

	// Validating implementation of create options
	if FStype == "xfs" {
		xfsInfoCmd := fmt.Sprintf("xfs_info %s ", strings.ReplaceAll(mapperOut, "\n", ""))
		xfsInfoOut, err := d.nodeDriver.RunCommandWithNoRetry(
			*volumeNode,
			xfsInfoCmd,
			node.ConnectionOpts{
				Timeout:         crashDriverTimeout,
				TimeBeforeRetry: defaultRetryInterval,
			})
		if err != nil {
			return fmt.Errorf("Failed to get bsize for create option for pvc %s, Err: %v", volumeName, err)
		}
		if strings.Contains(xfsInfoOut, "bsize=2048") {
			log.Infof("Blocksize 2048 is correctly configured by the create option of volume %s", volumeName)
		} else {
			log.Warnf("The filesystem type %s isn't properly implemented as block size 2048 has not been set, Err: %v", FStype, err)
			return fmt.Errorf("Failed to get %s proper block size in the %s file system, Err: %v", xfsInfoOut, FStype, err)
		}
	} else if FStype == "ext4" {
		ext4InfoCmd := fmt.Sprintf("tune2fs -l %s ", strings.ReplaceAll(mapperOut, "\n", ""))
		ext4InfoOut, err := d.nodeDriver.RunCommandWithNoRetry(
			*volumeNode,
			ext4InfoCmd,
			node.ConnectionOpts{
				Timeout:         crashDriverTimeout,
				TimeBeforeRetry: defaultRetryInterval,
			})
		if err != nil {
			return fmt.Errorf("Failed to get bsize for create option for pvc %s, Err: %v", volumeName, err)
		}
		blockSize := false
		for _, b := range strings.Split(ext4InfoOut, "\n") {
			if strings.Contains(b, "Block size") && strings.Contains(b, "2048") {
				blockSize = true
				break
			}
		}
		if blockSize {
			log.Infof("Blocksize 2048 is correctly configured by the create options of volume %s", volumeName)
		} else {
			log.Warnf("The filesystem type %s isn't properly implemented as block size 2048 has not been set, Err: %v", FStype, err)
			return fmt.Errorf("Failed to get %s proper block size in the %s file system, Err: %v", ext4InfoOut, FStype, err)
		}
	}
	return nil
}

func (d *portworx) UpdateSharedv4FailoverStrategyUsingPxctl(volumeName string, strategy api.Sharedv4FailoverStrategy_Value) error {
	nodes := node.GetStorageDriverNodes()
	var strategyStr string
	if strategy == api.Sharedv4FailoverStrategy_NORMAL {
		strategyStr = "normal"
	} else if strategy == api.Sharedv4FailoverStrategy_AGGRESSIVE {
		strategyStr = "aggressive"
	} else {
		return fmt.Errorf("invalid failover strategy: %v", strategy)
	}
	cmd := fmt.Sprintf("%s %s --sharedv4_failover_strategy %s", pxctlVolumeUpdate, volumeName, strategyStr)
	_, err := d.nodeDriver.RunCommandWithNoRetry(
		nodes[0],
		cmd,
		node.ConnectionOpts{
			Timeout:         crashDriverTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		})
	if err != nil {
		return fmt.Errorf("failed to create local snapshot using PXCTL, Err: %v", err)
	}
	return nil
}

func constructSnapshotName(volumeName string) string {
	return volumeName + "-snapshot"
}

// GetCloudsnaps returns all the cloud snaps of all volumes
func (d *portworx) GetCloudsnaps(volumeName string, params map[string]string) ([]*api.SdkCloudBackupInfo, error) {
	var token string
	token = d.getTokenForVolume(volumeName, params)
	if val, hasKey := params[refreshEndpointParam]; hasKey {
		refreshEndpoint, _ := strconv.ParseBool(val)
		d.refreshEndpoint = refreshEndpoint
	}

	cloudSnapResponse, err := d.csbackupManager.EnumerateWithFilters(d.getContextWithToken(context.Background(), token), &api.SdkCloudBackupEnumerateWithFiltersRequest{})

	if err != nil {
		return nil, fmt.Errorf("failed to get cloudsnap, Err: %v", err)
	}
	return cloudSnapResponse.GetBackups(), nil

}

// GetCloudsnaps returns all the cloud snaps of the given volume
func (d *portworx) GetCloudsnapsOfGivenVolume(volumeName string, sourceVolumeID string, params map[string]string) ([]*api.SdkCloudBackupInfo, error) {
	var token string
	token = d.getTokenForVolume(volumeName, params)
	if val, hasKey := params[refreshEndpointParam]; hasKey {
		refreshEndpoint, _ := strconv.ParseBool(val)
		d.refreshEndpoint = refreshEndpoint
	}
	cloudSnapResponse, err := d.csbackupManager.EnumerateWithFilters(d.getContextWithToken(context.Background(), token), &api.SdkCloudBackupEnumerateWithFiltersRequest{SrcVolumeId: sourceVolumeID})
	if err != nil {
		return nil, fmt.Errorf("failed to get cloudsnap, Err: %v", err)
	}
	return cloudSnapResponse.GetBackups(), nil
}

// DeleteAllCloudsnaps delete all cloud snaps for a given volume
func (d *portworx) DeleteAllCloudsnaps(volumeName, sourceVolumeID string, params map[string]string) error {
	var token string
	token = d.getTokenForVolume(volumeName, params)
	if val, hasKey := params[refreshEndpointParam]; hasKey {
		refreshEndpoint, _ := strconv.ParseBool(val)
		d.refreshEndpoint = refreshEndpoint
	}
	_, err := d.csbackupManager.DeleteAll(d.getContextWithToken(context.Background(), token), &api.SdkCloudBackupDeleteAllRequest{SrcVolumeId: sourceVolumeID})

	if err != nil {
		return fmt.Errorf("failed to delete cloudsnap, Err: %v", err)
	}

	return nil
}

func (d *portworx) ValidateCreateCloudsnap(volumeName string, params map[string]string) error {
	var token string
	token = d.getTokenForVolume(volumeName, params)
	if val, hasKey := params[refreshEndpointParam]; hasKey {
		refreshEndpoint, _ := strconv.ParseBool(val)
		d.refreshEndpoint = refreshEndpoint
	}
	_, err := d.csbackupManager.Create(d.getContextWithToken(context.Background(), token), &api.SdkCloudBackupCreateRequest{VolumeId: volumeName})
	if err != nil {
		return fmt.Errorf("failed to create cloudsnap, Err: %v", err)
	}
	return nil
}

func (d *portworx) ValidateCreateCloudsnapUsingPxctl(volumeName string) error {
	nodes := node.GetStorageDriverNodes()
	_, err := d.nodeDriver.RunCommandWithNoRetry(nodes[0], fmt.Sprintf(formattingCommandPxctlCloudSnapCreate, volumeName), node.ConnectionOpts{
		Timeout:         crashDriverTimeout,
		TimeBeforeRetry: defaultRetryInterval,
	})
	if err != nil {
		return fmt.Errorf("failed to create cloudsnap using PXCTL, Err: %v", err)
	}
	return nil
}

func (d *portworx) ValidateGetByteUsedForVolume(volumeName string, params map[string]string) (uint64, error) {
	var token string
	token = d.getTokenForVolume(volumeName, params)
	if val, hasKey := params[refreshEndpointParam]; hasKey {
		refreshEndpoint, _ := strconv.ParseBool(val)
		d.refreshEndpoint = refreshEndpoint
	}
	statistic, err := d.volDriver.Stats(d.getContextWithToken(context.Background(), token), &api.SdkVolumeStatsRequest{VolumeId: volumeName})
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve volume statistic, Err: %v", err)
	}
	return statistic.GetStats().BytesUsed, nil
}

func (d *portworx) ValidateCreateGroupSnapshotUsingPxctl() error {
	nodes := node.GetStorageDriverNodes()
	_, err := d.nodeDriver.RunCommandWithNoRetry(nodes[0], pxctlGroupSnapshotCreate, node.ConnectionOpts{
		Timeout:         crashDriverTimeout,
		TimeBeforeRetry: defaultRetryInterval,
	})
	if err != nil {
		return fmt.Errorf("failed to create groupsnapshot using PXCTL, Err: %v", err)
	}
	return nil
}

func (d *portworx) ValidateVolumeInPxctlList(volumeName string) error {
	nodes := node.GetStorageDriverNodes()
	out, err := d.nodeDriver.RunCommandWithNoRetry(nodes[0], pxctlVolumeList, node.ConnectionOpts{
		Timeout:         crashDriverTimeout,
		TimeBeforeRetry: defaultRetryInterval,
	})
	if err != nil {
		return fmt.Errorf("failed to list volumes using PXCTL, Err: %v", err)
	}

	if !strings.Contains(out, volumeName) {
		return fmt.Errorf("volume name [%s] is not present in PXCTL volume list", volumeName)
	}
	return nil
}

func (d *portworx) UpdateFBDANFSEndpoint(volumeName string, newEndpoint string) error {
	nodes := node.GetStorageDriverNodes()
	cmd := fmt.Sprintf("%s --pure_nfs_endpoint %s %s", pxctlVolumeUpdate, newEndpoint, volumeName)
	_, err := d.nodeDriver.RunCommandWithNoRetry(
		nodes[0],
		cmd,
		node.ConnectionOpts{
			Timeout:         crashDriverTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		})
	if err != nil {
		return fmt.Errorf("failed setting FBDA NFS endpoint for volume [%s] to [%s] from node [%s], Err: %v", volumeName, newEndpoint, nodes[0], err)
	}
	return nil
}

func (d *portworx) ValidatePureFBDAMountSource(nodes []node.Node, vols []*torpedovolume.Volume, expectedIP string) error {
	// For each node
	//   Run `mount` on node
	//   Search through lines for our volume names, check that all contain right IP
	return fmt.Errorf("not implemented (ValidatePureFBDAMountSource)")
}

func (d *portworx) ValidatePureVolumesNoReplicaSets(volumeName string, params map[string]string) error {
	var token string
	token = d.getTokenForVolume(volumeName, params)
	if val, hasKey := params[refreshEndpointParam]; hasKey {
		refreshEndpoint, _ := strconv.ParseBool(val)
		d.refreshEndpoint = refreshEndpoint
	}
	volumeInspectResponse, err := d.getVolDriver().Inspect(d.getContextWithToken(context.Background(), token), &api.SdkVolumeInspectRequest{VolumeId: volumeName})
	if err != nil {
		return err
	}

	respVol := volumeInspectResponse.Volume

	// check that replicationset is nil
	if len(respVol.GetReplicaSets()) > 0 {
		return fmt.Errorf("purevolumes [%s] has replicationset and it should not", volumeName)
	}
	return nil
}

type pureLocalPathEntry struct {
	WWID        string
	SinglePaths []string
	Size        uint64
}

func GetSerialFromWWID(wwid string) (string, error) {
	if !strings.Contains(wwid, schedops.PureVolumeOUI) {
		return "", fmt.Errorf("not a Pure Storage multipath WWID '%s'", wwid)
	}

	if strings.HasPrefix(wwid, "eui.") {
		// NVMe
		return strings.ToLower(fmt.Sprintf("%s%s", wwid[6:20], wwid[26:36])), nil
	}
	// SCSI
	return strings.TrimPrefix(strings.ToLower(wwid), fmt.Sprintf("36%s0", schedops.PureVolumeOUI)), nil
}

func parseLsblkOutput(out string) (map[string]pureLocalPathEntry, error) {
	/* Parses output like this
	[root@akrpan-pxone-1 ~]# lsblk --inverse --ascii --noheadings -o NAME,SIZE -b
	sdd                               137438953472
	sdb                                34359738368
	3624a9370ea876434795b4b54000a4128   6442450944
	|-sdf                               6442450944
	|-sdi                               6442450944
	|-sdg                               6442450944
	`-sdh                               6442450944
	sde2                              134217728000
	`-sde                             137438953472
	sde1                                3219128320
	`-sde                             137438953472
	sdc                               137438953472
	sda2                              133438636032
	`-sda                             137438953472
	sda1                                3999268864
	`-sda                             137438953472
	*/

	foundDevices := map[string]pureLocalPathEntry{}

	var currentEntry *pureLocalPathEntry = nil

	for _, l := range strings.Split(out, "\n") {
		line := strings.TrimSpace(l)
		if len(line) == 0 {
			continue
		}

		// If we see a WWID, we are starting a new entry
		if strings.Contains(line, schedops.PureVolumeOUI) {
			if currentEntry != nil {
				foundDevices[currentEntry.WWID] = *currentEntry
			}
			parts := strings.Fields(line)
			wwid := parts[0]
			sizeStr := parts[1]
			size, err := strconv.ParseUint(sizeStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse size '%s' from lsblk output, Err: %v", sizeStr, err)
			}
			currentEntry = &pureLocalPathEntry{
				WWID:        wwid,
				Size:        size,
				SinglePaths: []string{},
			}
			continue
		}

		if currentEntry != nil {
			// If we see a pipe or a tick, we are starting a single path inside a WWID
			if strings.HasPrefix(line, "|-") || strings.HasPrefix(line, "`-") {
				parts := strings.Fields(line[2:])
				currentEntry.SinglePaths = append(currentEntry.SinglePaths, parts[0])
				continue
			} else {
				// This is some other path not part of a WWID, ignore it and also finish off any WWID we had going before
				foundDevices[currentEntry.WWID] = *currentEntry
				currentEntry = nil
			}
		}
	}
	if currentEntry != nil {
		foundDevices[currentEntry.WWID] = *currentEntry
	}

	return foundDevices, nil
}

// collectLocalNodeInfo interrogates dmsetup and lsblk to get a comprehensive list of mapper devices, their single paths,
// and checks for common error conditions such as devices missing single paths or only appearing in dmsetup/lsblk
func (d *portworx) collectLocalNodeInfo(n node.Node) (map[string]pureLocalPathEntry, error) {
	// Run `dmsetup ls` to get all the known mapper devices
	out, err := d.nodeDriver.RunCommandWithNoRetry(n, "dmsetup ls", node.ConnectionOpts{
		Timeout:         crashDriverTimeout,
		TimeBeforeRetry: defaultRetryInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to check dmsetup ls output on node %s, Err: %v", n.MgmtIp, err)
	}
	dmsetupFoundMappers := []string{}
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, schedops.PureVolumeOUI) {
			continue
		}
		mapperName := strings.Split(line, "\t")[0]
		dmsetupFoundMappers = append(dmsetupFoundMappers, mapperName)
	}

	// Then run `lsblk` to get the WWN and size of each device. Flags:
	// * --inverse: show parents before children (instead of the default output that has single paths as the top level)
	// * --ascii: show ASCII characters only (no unicode, normally it displays the fancy tree characters, this makes it only show |- and `-)
	// * --noheadings: don't show the header line
	// * -o NAME,SIZE: only show the name and size columns
	// * -b: show size in bytes instead of human-readable with a suffix
	out, err = d.nodeDriver.RunCommandWithNoRetry(n, "lsblk --inverse --ascii --noheadings -o NAME,SIZE -b", node.ConnectionOpts{
		Timeout:         crashDriverTimeout,
		TimeBeforeRetry: defaultRetryInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to check 'lsblk --inverse --ascii --noheadings -o NAME,SIZE' output on node %s, Err: %v", n.MgmtIp, err)
	}

	lsblkParsed, err := parseLsblkOutput(out)
	if err != nil {
		return nil, fmt.Errorf("failed to parse lsblk output on node %s, Err: %v", n.MgmtIp, err)
	}

	if len(lsblkParsed) != len(dmsetupFoundMappers) {
		return nil, fmt.Errorf("found %d mappers in dmsetup but %d devices in lsblk on node %s, inconsistent disk state (we didn't clean something up right?)", len(dmsetupFoundMappers), len(lsblkParsed), n.MgmtIp)
	}

	// Also, raise an error if there is a mismatch in the devices in the two outputs (e.g. if there is a mapper
	// in dmsetup but not lsblk, something is very wrong and we didn't clean up right)
	for _, mapper := range dmsetupFoundMappers {
		lsblkEntry, exists := lsblkParsed[mapper]
		if !exists {
			return nil, fmt.Errorf("found mapper %s in dmsetup but not lsblk on node %s, inconsistent disk state (we didn't clean something up right?)", mapper, n.MgmtIp)
		}
		// Ensure there are no WWIDs with empty single paths, as that is a malformed device
		if len(lsblkEntry.SinglePaths) == 0 {
			return nil, fmt.Errorf("found mapper %s with no single paths on node %s, this should not happen (we didn't clean something up right?)", mapper, n.MgmtIp)
		}
	}

	return lsblkParsed, nil
}

func (d *portworx) getCurrentPureLocalVolumePaths() (map[string]map[string]pureLocalPathEntry, error) {
	// Iterate through all nodes, and for each node collect local paths
	currentDevices := make(map[string]map[string]pureLocalPathEntry)
	nodes := node.GetStorageDriverNodes()
	for _, n := range nodes {
		localPathMap, err := d.collectLocalNodeInfo(n)
		if err != nil {
			return nil, err
		}
		currentDevices[n.MgmtIp] = localPathMap
	}

	return currentDevices, nil
}

// InitializePureLocalVolumePaths sets the baseline for how many Pure devices are attached to each node, so that we can compare against it later
func (d *portworx) InitializePureLocalVolumePaths() error {
	currentDevices, err := d.getCurrentPureLocalVolumePaths()
	if err != nil {
		return err
	}
	logrus.Infof("Pure device baseline initialized to: %+v", currentDevices)
	d.pureDeviceBaseline = currentDevices

	// TODO: we should only include non-FACD drives here. Eventually, we should handle FACD as its own kind of volume in ValidatePureLocalVolumePaths and validate it matches the pxctl cd l output

	return nil
}

// ValidatePureLocalVolumePaths checks that the given volumes all have the proper local paths present, *and that no other unexpected ones are present*
func (d *portworx) ValidatePureLocalVolumePaths() error {
	t := func() error {
		currentDevices, err := d.getCurrentPureLocalVolumePaths()
		if err != nil {
			return err
		}
		logrus.Infof("Current pure devices: %+v", currentDevices)

		// TODO: handle FACD drive failovers properly (these will show up as new devices but they're actually fine)
		// Remove all devices that are in the baseline (complex set math time!). Also warn if any baseline devices are now missing?
		for node, baselineDevices := range d.pureDeviceBaseline {
			currentNodeDevices := currentDevices[node]
			for wwid := range baselineDevices {
				if _, exists := currentNodeDevices[wwid]; exists {
					delete(currentNodeDevices, wwid)
				} else {
					return fmt.Errorf("baseline FA device %s originally present on node %s is missing, this should not happen", wwid, node)
				}
			}
		}

		allVolNames, err := d.ListAllVolumes()
		if err != nil {
			return err
		}
		// Inspect all volumes provided to get the PX spec
		fadaVolumes := []*api.Volume{}
		for _, volName := range allVolNames {
			v, err := d.InspectVolume(volName)
			if err != nil {
				return err
			}
			if !v.Spec.IsPureVolume() {
				continue
			}
			if v.Spec.ProxySpec.PureBlockSpec == nil {
				// TODO: handle FBDA volumes properly as well
				continue
			}
			fadaVolumes = append(fadaVolumes, v)
		}

		// For each volume, check which nodes it should be on. Remove it from the list of devices on that node. Error if we can't find it.
		for _, v := range fadaVolumes {
			// Find the node this volume is on
			var foundNode *node.Node
			attachedOn := v.GetAttachedOn()
			for _, n := range node.GetStorageDriverNodes() {
				if n.MgmtIp == attachedOn { // TODO: RWX support
					tempVar := n
					foundNode = &tempVar
					break
				}
			}
			if foundNode == nil {
				logrus.Infof("Volume %s is not attached to any node, skipping", v.Locator.Name)
				continue
			}

			// Find the device for this volume
			var device *pureLocalPathEntry
			for _, deviceEntry := range currentDevices[foundNode.MgmtIp] {
				serial, err := GetSerialFromWWID(deviceEntry.WWID)
				if err != nil {
					return err
				}
				if serial == strings.ToLower(v.Spec.GetProxySpec().PureBlockSpec.SerialNum) {
					device = &deviceEntry
					break
				}
			}
			if device == nil {
				return fmt.Errorf("volume %s is attached to node %s but not found in any local path", v.Locator.Name, foundNode.MgmtIp)
			}

			// Check that the device is actually of the correct size
			if device.Size != v.Spec.Size {
				return fmt.Errorf("volume %s is attached to node %s but has incorrect size %d instead of expected size %d", v.Locator.Name, foundNode.MgmtIp, device.Size, v.Spec.Size)
			}

			// Remove the device from the list of devices on that node
			delete(currentDevices[foundNode.MgmtIp], device.WWID)
		}

		logrus.Infof("Remaining devices after removing all attached FADA volumes: %+v", currentDevices)

		// At the very end, ensure that all the lists are empty. If they aren't, there is an extra device on that node not matching to a pod.
		// TODO: check if this is an FACD drive device, if so then it's also fine
		for node, devices := range currentDevices {
			if len(devices) > 0 {
				return fmt.Errorf("found %d extra devices on node %s that are not attached to any torpedo app volume (it may be from a clone test pod)", len(devices), node)
			}
		}

		return nil
	}
	_, err := task.DoRetryWithTimeout(func() (interface{}, bool, error) {
		err := t()
		if err != nil {
			return nil, true, err
		}
		return nil, false, nil
	}, time.Minute*2, defaultRetryInterval)
	return err
}

func (d *portworx) SetIoBandwidth(vol *torpedovolume.Volume, readBandwidthMBps uint32, writeBandwidthMBps uint32) error {
	volumeName := d.schedOps.GetVolumeName(vol)
	log.Infof("Setting IO Throttle for volume [%s]", volumeName)
	volDriver := d.getVolDriver()
	_, err := volDriver.Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volumeName})
	if err != nil && errIsNotFound(err) {
		return err
	} else if err != nil {
		return err
	}
	log.Debugf("Updating volume [%s]", volumeName)
	t := func() (interface{}, bool, error) {
		volumeSpecUpdate := &api.VolumeSpecUpdate{
			IoThrottleOpt: &api.VolumeSpecUpdate_IoThrottle{
				IoThrottle: &api.IoThrottle{
					ReadBwMbytes:  readBandwidthMBps,
					WriteBwMbytes: writeBandwidthMBps,
				},
			},
		}
		_, err = volDriver.Update(d.getContext(), &api.SdkVolumeUpdateRequest{
			VolumeId: volumeName,
			Spec:     volumeSpecUpdate,
		})
		if err != nil {
			return nil, true, fmt.Errorf("volume [%s] not updated yet", volumeName)
		}
		log.Debugf("Updated volume [%s]", volumeName)
		return nil, false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, inspectVolumeTimeout, defaultRetryInterval); err != nil {
		return fmt.Errorf("failed to set set IOps for volumeName [%s], Err: %v", volumeName, err)
	}
	return nil
}

// UpdateVolumeSpec updates given volume with provided spec
func (d *portworx) UpdateVolumeSpec(vol *torpedovolume.Volume, volumeSpec *api.VolumeSpecUpdate) error {
	volumeName := d.schedOps.GetVolumeName(vol)
	log.Infof("Updating volume spec for volume [%s]", volumeName)
	log.Infof("Volume Spec : %+v", volumeSpec)
	volDriver := d.getVolDriver()
	_, err := volDriver.Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volumeName})
	if err != nil && errIsNotFound(err) {
		return err
	} else if err != nil {
		return err
	}
	log.Debugf("Updating volume [%s]", volumeName)
	t := func() (interface{}, bool, error) {

		_, err = volDriver.Update(d.getContext(), &api.SdkVolumeUpdateRequest{
			VolumeId: volumeName,
			Spec:     volumeSpec,
		})
		if err != nil {
			return nil, true, fmt.Errorf("volume [%s] not updated yet", volumeName)
		}
		log.Debugf("Updated volume [%s]", volumeName)
		return nil, false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, inspectVolumeTimeout, defaultRetryInterval); err != nil {
		return fmt.Errorf("failed to set set IOps for volumeName [%s], Err: %v", volumeName, err)
	}
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
		volumeInspectResponse, err := d.getVolDriver().Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: vol.ID})
		if err != nil && errIsNotFound(err) {
			return nil, false, nil
		} else if err != nil {
			return nil, true, err
		}
		// TODO remove shared validation when PWX-6894 and PWX-8790 are fixed
		if volumeInspectResponse.Volume != nil && !vol.Shared {
			return nil, true, fmt.Errorf("volume [%s] with ID [%s] in namespace [%s] is not yet removed from the system", volumeName, vol.ID, vol.Namespace)
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

			log.InfoD("Stopping volume driver on [%s].", n.Name)
			if force {
				pxCrashCmd := "sudo pkill -9 px-storage"
				_, err = d.nodeDriver.RunCommand(n, pxCrashCmd, node.ConnectionOpts{
					Timeout:         crashDriverTimeout,
					TimeBeforeRetry: defaultRetryInterval,
				})
				if err != nil {
					return fmt.Errorf("failed to run cmd [%s] on node [%s], Err: %v", pxCrashCmd, n.Name, err)
				}
				log.Infof("Sleeping for %v for volume driver to go down", waitVolDriverToCrash)
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
					log.Warnf("failed to run [systemctl stopcmd] on node [%s], Err: %v", n.Name, err)
					return err
				}
				log.Infof("Sleeping for %v for volume driver to gracefully go down.", waitVolDriverToCrash/6)
				time.Sleep(waitVolDriverToCrash / 6)
			}

		}
		return nil
	}
	return driver_api.PerformTask(stopFn, triggerOpts)
}

func (d *portworx) KillPXDaemon(nodes []node.Node, triggerOpts *driver_api.TriggerOptions) error {
	stopFn := func() error {
		for _, n := range nodes {

			log.InfoD("Stopping px-daemon on [%s].", n.Name)
			var processPid string
			command := "ps -ef | grep \"px -daemon\""
			out, err := d.nodeDriver.RunCommand(n, command, node.ConnectionOpts{
				Timeout:         20 * time.Second,
				TimeBeforeRetry: 5 * time.Second,
				Sudo:            true,
			})
			if err != nil {
				return err
			}

			lines := strings.Split(string(out), "\n")
			for _, line := range lines {
				if strings.Contains(line, "/usr/local/bin/px -daemon") && !strings.Contains(line, "grep") {
					fields := strings.Fields(line)
					processPid = fields[1]
					break
				}
			}

			if processPid == "" {
				return fmt.Errorf("unable to find PID for px daemon in output [%s]", out)
			}

			pxCrashCmd := fmt.Sprintf("sudo kill -9 %s", processPid)
			_, err = d.nodeDriver.RunCommand(n, pxCrashCmd, node.ConnectionOpts{
				Timeout:         crashDriverTimeout,
				TimeBeforeRetry: defaultRetryInterval,
			})
			if err != nil {
				return fmt.Errorf("failed to run cmd [%s] on node [%s], Err: %v", pxCrashCmd, n.Name, err)
			}
			log.Infof("Sleeping for %v for volume driver to go down", waitVolDriverToCrash)
			time.Sleep(waitVolDriverToCrash)
		}
		return nil
	}
	return driver_api.PerformTask(stopFn, triggerOpts)
}

// GetNodeForVolume returns the node on which volume is attached
func (d *portworx) GetNodeForVolume(vol *torpedovolume.Volume, timeout time.Duration, retryInterval time.Duration) (*node.Node, error) {
	volumeName := d.schedOps.GetVolumeName(vol)
	d.refreshEndpoint = true
	t := func() (interface{}, bool, error) {
		volumeInspectResponse, err := d.getVolDriver().Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volumeName})
		if err != nil {
			log.Warnf("Failed to inspect volume [%s], Err: %v", volumeName, err)
			return nil, false, &ErrFailedToInspectVolume{
				ID:    volumeName,
				Cause: err.Error(),
			}
		}
		isPureFile, err := d.IsPureFileVolume(vol)
		if err != nil {
			return nil, false, err
		}
		if isPureFile {
			return nil, false, nil
		}

		pxVol := volumeInspectResponse.Volume
		for _, n := range node.GetStorageDriverNodes() {

			ok, err := d.IsVolumeAttachedOnNode(pxVol, n)
			if err != nil {
				return nil, false, err
			}
			if ok {
				return &n, false, err
			}
		}

		// Snapshots may not be attached to a node
		if pxVol.Source.Parent != "" {
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("volume [%s] is not attached on any node", volumeName)
	}

	n, err := task.DoRetryWithTimeout(t, timeout, retryInterval)
	d.refreshEndpoint = false
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

// check all the possible attachment options (node ID or node IP)
func (d *portworx) IsVolumeAttachedOnNode(volume *api.Volume, node node.Node) (bool, error) {
	log.Debugf("Volume [%s] attached on [%s] checking for node [%s]", volume.Id, volume.AttachedOn, node.VolDriverNodeID)
	if node.VolDriverNodeID == volume.AttachedOn {
		return true, nil
	}
	resp, err := d.nodeManager.Inspect(context.Background(), &api.SdkNodeInspectRequest{NodeId: node.VolDriverNodeID})
	if err != nil {
		return false, err
	}
	// in case of single interface
	if resp.Node.MgmtIp == volume.AttachedOn {
		return true, nil
	}
	// in case node has data and management interface
	if resp.Node.DataIp == volume.AttachedOn {
		return true, nil
	}

	// check for alternate IPs
	for _, ip := range node.Addresses {
		if ip == volume.AttachedOn {
			return true, nil
		}
	}
	return false, nil
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

func (d *portworx) InspectCurrentCluster() (*api.SdkClusterInspectCurrentResponse, error) {
	currentClusterResponse, err := d.getClusterManager().InspectCurrent(d.getContext(), &api.SdkClusterInspectCurrentRequest{})
	if err != nil {
		return nil, err
	}
	return currentClusterResponse, nil
}

func (d *portworx) getStorageNodesOnStart() ([]*api.StorageNode, error) {
	t := func() (interface{}, bool, error) {
		cluster, err := d.InspectCurrentCluster()
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

// getPxNodeByID return px node by provding node id
func (d *portworx) getPxNodeByID(nodeID string) (*api.StorageNode, error) {
	log.Infof("Getting the node using nodeId [%s]", nodeID)
	var nodeManager api.OpenStorageNodeClient = d.getNodeManager()

	nodeResponse, err := nodeManager.Inspect(d.getContext(), &api.SdkNodeInspectRequest{NodeId: nodeID})
	if err != nil {
		return nil, err
	}

	if nodeResponse.Node.MgmtIp == "" {
		return nil, fmt.Errorf("got an empty MgmtIp from SdkNodeInspectRequest")
	}
	return nodeResponse.Node, nil
}

// GetDriverNodes gets and return list of PX nodes
func (d *portworx) GetDriverNodes() ([]*api.StorageNode, error) {
	return d.getPxNodes()
}

func (d *portworx) getPxNodes(nManagers ...api.OpenStorageNodeClient) ([]*api.StorageNode, error) {
	var nodeManager api.OpenStorageNodeClient
	if nManagers == nil {
		d.refreshEndpoint = true
		nodeManager = d.getNodeManager()
		d.refreshEndpoint = false
	} else {
		nodeManager = nManagers[0]
	}
	nodes := make([]*api.StorageNode, 0)
	nodeEnumerateResp, err := nodeManager.Enumerate(d.getContext(), &api.SdkNodeEnumerateRequest{})
	if err != nil {
		return nodes, err
	}
	for _, n := range nodeEnumerateResp.GetNodeIds() {
		t := func() (interface{}, bool, error) {
			nodeResponse, err := nodeManager.Inspect(d.getContext(), &api.SdkNodeInspectRequest{NodeId: n})
			if err != nil {
				return nil, true, err
			}
			if nodeResponse.Node.MgmtIp == "" {
				return nil, true, fmt.Errorf("got an empty MgmtIp from SdkNodeInspectRequest, response: %v", nodeResponse)
			}
			return nodeResponse, false, nil
		}
		nodeResp, err := task.DoRetryWithTimeout(t, defaultTimeout, defaultRetryInterval)
		if err != nil {
			return nodes, err
		}
		nodes = append(nodes, nodeResp.(*api.SdkNodeInspectResponse).Node)
	}
	return nodes, nil
}

// GetDriveSet runs `pxctl cd i --node <node ID>` on the given node
func (d *portworx) GetDriveSet(n *node.Node) (*torpedovolume.DriveSet, error) {
	out, err := d.nodeDriver.RunCommandWithNoRetry(*n, fmt.Sprintf(pxctlCloudDriveInspect, d.getPxctlPath(*n), n.VolDriverNodeID), node.ConnectionOpts{
		Timeout:         crashDriverTimeout,
		TimeBeforeRetry: defaultRetryInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("error when inspecting drive sets for node ID [%s] using PXCTL, Err: %v", n.VolDriverNodeID, err)
	}
	var driveSetInspect torpedovolume.DriveSet
	err = json.Unmarshal([]byte(out), &driveSetInspect)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal drive set inspect, Err: %v", err)
	}
	return &driveSetInspect, nil
}

func (d *portworx) PrintCommandOutput(cmnd string, n node.Node) {
	output, err := d.nodeDriver.RunCommand(n, cmnd, node.ConnectionOpts{
		Timeout:         crashDriverTimeout,
		TimeBeforeRetry: defaultRetryInterval,
		Sudo:            true,
	})
	if err != nil {
		log.Errorf("failed to run command [%s], Err: %v", cmnd, err)
	}
	log.Infof(output)

}

// WaitDriverUpOnNode waits for PX to be up on a given node
func (d *portworx) WaitDriverUpOnNode(n node.Node, timeout time.Duration) error {
	log.Debugf("Waiting for PX node to be up [%s/%s]", n.Name, n.VolDriverNodeID)
	t := func() (interface{}, bool, error) {
		log.Debugf("Getting node info for node [%s/%s]", n.Name, n.VolDriverNodeID)
		nodeInspectResponse, err := d.getNodeManager().Inspect(d.getContext(), &api.SdkNodeInspectRequest{NodeId: n.VolDriverNodeID})

		if err != nil {
			return "", true, &ErrFailedToWaitForPx{
				Node:  n,
				Cause: fmt.Sprintf("failed to get node info [%s/%s], Err: %v", n.Name, n.VolDriverNodeID, err),
			}
		}

		log.Debugf("Checking PX status on node [%s/%s]", n.Name, n.VolDriverNodeID)
		pxNode := nodeInspectResponse.Node
		switch pxNode.Status {
		case api.Status_STATUS_DECOMMISSION: // do nothing
		case api.Status_STATUS_OK:
			pxStatus, err := d.GetPxctlStatus(n)
			if err != nil {
				return "", true, &ErrFailedToWaitForPx{
					Node:  n,
					Cause: fmt.Sprintf("failed to get pxctl status on node [%s/%s], Err: %v", n.Name, n.VolDriverNodeID, err),
				}
			}

			if pxStatus != api.Status_STATUS_OK.String() {
				return "", true, &ErrFailedToWaitForPx{
					Node: n,
					Cause: fmt.Sprintf("node [%s/%s] status is up but PX cluster is not ok. Expected: %v Actual: %v",
						n.Name, n.VolDriverNodeID, api.Status_STATUS_OK, pxStatus),
				}
			}

		case api.Status_STATUS_OFFLINE:
			// in case node is offline and it is a storageless node, the id might have changed so update it
			if len(pxNode.Pools) == 0 {
				_, err = d.updateNodeID(&n, d.getNodeManager())
				if err != nil {
					return "", true, &ErrFailedToWaitForPx{
						Node:  n,
						Cause: fmt.Sprintf("failed to update node id [%s/%s], Err: %v", n.Name, n.VolDriverNodeID, err),
					}
				}
			}
			return "", true, &ErrFailedToWaitForPx{
				Node: n,
				Cause: fmt.Sprintf("node [%s/%s] status is up but PX cluster is not ok. Expected: %v Actual: %v",
					n.Name, n.VolDriverNodeID, api.Status_STATUS_OK, pxNode.Status),
			}
		default:
			log.Infof("Status PX available %s", pxNode.Status.String())
			return "", true, &ErrFailedToWaitForPx{
				Node: n,
				Cause: fmt.Sprintf("PX cluster is usable but node [%s/%s] status is not ok. Expected: %v Actual: %v",
					n.Name, n.VolDriverNodeID, api.Status_STATUS_OK, pxNode.Status),
			}
		}

		log.Infof("PX on node [%s/%s] is now up. status: %v", n.Name, n.VolDriverNodeID, pxNode.Status)

		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, timeout, defaultRetryInterval); err != nil {
		log.InfoD(fmt.Sprintf("------Printing the px logs on the node:%s ----------", n.Name))
		d.PrintCommandOutput("journalctl -lu portworx* -n 100 --no-pager ", n)
		log.InfoD(fmt.Sprintf("------Finished Printing the px logs on the node:%s ----------", n.Name))
		return fmt.Errorf("PX failed to come up on node [%s/%s], Err: %v", n.Name, n.VolDriverNodeID, err)
	}

	// Check if PX pod is up
	log.Debugf("Checking if PX pod is up on node [%s/%s]", n.Name, n.VolDriverNodeID)
	t = func() (interface{}, bool, error) {
		if !d.schedOps.IsPXReadyOnNode(n) {
			return "", true, &ErrFailedToWaitForPx{
				Node:  n,
				Cause: fmt.Sprintf("PX pod is not ready on node [%s/%s] after %v", n.Name, n.VolDriverNodeID, timeout),
			}
		}
		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, defaultRetryInterval); err != nil {
		return fmt.Errorf("PX pod failed to come up on node [%s/%s], Err: %v", n.Name, n.VolDriverNodeID, err)
	}

	log.Debugf("PX is fully operational on node [%s/%s]", n.Name, n.VolDriverNodeID)
	return nil
}

func (d *portworx) WaitDriverDownOnNode(n node.Node) error {
	t := func() (interface{}, bool, error) {

		for _, addr := range n.Addresses {
			err := d.testAndSetEndpointUsingNodeIP(addr)
			if (err == nil || !strings.Contains(err.Error(), "connect: connection refused")) && (err == nil || !strings.Contains(err.Error(), "i/o timeout")) {
				return "", true, &ErrFailedToWaitForPx{
					Node:  n,
					Cause: "px is not yet down on node",
				}
			}
			log.Warn(err.Error())
		}

		log.Infof("PX on node [%s] is now down.", n.Name)
		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, validateNodeStopTimeout, waitDriverDownOnNodeRetryInterval); err != nil {
		return fmt.Errorf("failed to stop PX on node [%s], Err: %v", n.Name, err)
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
							err := fmt.Errorf("node [%s], pool: %s was expanded to size: %d larger than expected: %d",
								n.Name, pool.Uuid, pool.TotalSize, expectedSize)
							log.Errorf(err.Error())
							return "", false, nil
						}

						log.Infof("node [%s], pool: %s, size is not as expected. Expected: %v, Actual: %v",
							n.Name, pool.Uuid, expectedSize, pool.TotalSize)
						allDone = false
					} else {
						log.Infof("node: [%s], pool: %s, size is as expected. Expected: %v",
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

func (d *portworx) ValidateRebalanceJobs() error {
	// start a task to check if all rebalance jobs are done
	t := func() (interface{}, bool, error) {
		jobListResp, err := d.storagePoolManager.EnumerateRebalanceJobs(d.getContext(), &api.SdkEnumerateRebalanceJobsRequest{})
		if err != nil {
			return nil, true, err
		}
		for _, job := range jobListResp.Jobs {
			if job.State != api.StorageRebalanceJobState_DONE {
				return "", true, fmt.Errorf("rebalance job is not done. Job ID: %s, State: %s", job.Id, job.State.String())
			}
		}
		return nil, false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, validateRebalanceJobsTimeout, validateRebalanceJobsInterval); err != nil {
		return err
	}
	return nil
}

func (d *portworx) ResizeStoragePoolByPercentage(poolUUID string, e api.SdkStoragePool_ResizeOperationType, percentage uint64) error {
	log.InfoD("Initiating pool %v resize by %v with operationtype %v", poolUUID, percentage, e.String())

	// Start a task to check if pool  resize is done
	t := func() (interface{}, bool, error) {
		jobListResp, err := d.storagePoolManager.Resize(d.getContext(), &api.SdkStoragePoolResizeRequest{
			Uuid: poolUUID,
			ResizeFactor: &api.SdkStoragePoolResizeRequest_Percentage{
				Percentage: percentage,
			},
			OperationType:           e,
			SkipWaitForCleanVolumes: true,
		})
		if err != nil {
			return nil, true, err
		}
		if jobListResp.String() != "" {
			log.Debugf("Resize response: %v", jobListResp.String())
		}
		return nil, false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, validateRebalanceJobsTimeout, validateRebalanceJobsInterval); err != nil {
		return err
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
	log.Debugf("%s: expected sizes of storage pools: %+v", fn, expectedPoolSizes)
	return expectedPoolSizes, nil
}

// GetAutoFsTrimStatus get status of autofstrim
func (d *portworx) GetAutoFsTrimStatus(endpoint string) (map[string]api.FilesystemTrim_FilesystemTrimStatus, error) {
	sdkport, _ := d.getSDKPort()
	pxEndpoint := net.JoinHostPort(endpoint, strconv.Itoa(int(sdkport)))
	newConn, err := grpc.Dial(pxEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to set the connection endpoint [%s], Err: %v", endpoint, err)
	}
	d.autoFsTrimManager = api.NewOpenStorageFilesystemTrimClient(newConn)

	autoFstrimResp, err := d.autoFsTrimManager.AutoFSTrimStatus(d.getContext(), &api.SdkAutoFSTrimStatusRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get auto fstrim status, Err: %v", err)

	}
	log.Infof("Trim Status is [%v]", autoFstrimResp.GetTrimStatus())
	return autoFstrimResp.GetTrimStatus(), nil
}

// GetAutoFsTrimUsage get status of autofstrim
func (d *portworx) GetAutoFsTrimUsage(endpoint string) (map[string]*api.FstrimVolumeUsageInfo, error) {
	sdkport, _ := d.getSDKPort()
	pxEndpoint := net.JoinHostPort(endpoint, strconv.Itoa(int(sdkport)))
	newConn, err := grpc.Dial(pxEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to set the connection endpoint [%s], Err: %v", endpoint, err)
	}
	d.autoFsTrimManager = api.NewOpenStorageFilesystemTrimClient(newConn)

	autoFstrimResp, err := d.autoFsTrimManager.AutoFSTrimUsage(d.getContext(), &api.SdkAutoFSTrimUsageRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get auto fstrim usage stats, Err: %v", err)
	}
	log.Infof("Trim Usage is [%v]", autoFstrimResp.GetUsage())
	return autoFstrimResp.GetUsage(), nil
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
				log.Warnf("failed to check node [%s] on addr [%s], Err: %v", n.Name, addr, err)
				continue
			}
			if len(ns.NodeIds) != 0 {
				return nodeManager, nil
			}
		}
	}
	return nil, fmt.Errorf("failed to get an alternate cluster manager for node [%s]", n.Name)
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

// IsPureVolume returns true if volume is pure volume else returns false
func (d *portworx) IsPureVolume(volume *torpedovolume.Volume) (bool, error) {
	var proxySpec *api.ProxySpec
	var err error
	if proxySpec, err = d.GetProxySpecForAVolume(volume); err != nil {
		return false, err
	}

	if proxySpec == nil {
		return false, nil
	}

	if proxySpec.ProxyProtocol == api.ProxyProtocol_PROXY_PROTOCOL_PURE_BLOCK || proxySpec.ProxyProtocol == api.ProxyProtocol_PROXY_PROTOCOL_PURE_FILE {
		log.Debugf("Volume [%s] is Pure volume", volume.ID)
		return true, nil
	}

	log.Debugf("Volume [%s] is not Pure Block volume", volume.ID)
	return false, nil
}

// GetProxySpecForAVolume return proxy spec for a pure volumes
func (d *portworx) GetProxySpecForAVolume(volume *torpedovolume.Volume) (*api.ProxySpec, error) {
	name := d.schedOps.GetVolumeName(volume)
	t := func() (interface{}, bool, error) {
		volumeInspectResponse, err := d.getVolDriver().Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: name})
		if err != nil && errIsNotFound(err) {
			return 0, false, err
		} else if err != nil {
			return 0, true, err
		}
		return volumeInspectResponse.Volume.Spec.ProxySpec, false, nil
	}

	proxySpec, err := task.DoRetryWithTimeout(t, validateReplicationUpdateTimeout, defaultRetryInterval)
	if err != nil {
		return nil, &ErrFailedToGetVolumeProxySpec{
			ID:    name,
			Cause: err.Error(),
		}
	}
	return proxySpec.(*api.ProxySpec), nil
}

// IsPureFileVolume returns true if volume is FB volume else returns false
func (d *portworx) IsPureFileVolume(volume *torpedovolume.Volume) (bool, error) {
	var proxySpec *api.ProxySpec
	var err error
	if proxySpec, err = d.GetProxySpecForAVolume(volume); err != nil {
		return false, err
	}
	if proxySpec == nil {
		return false, nil
	}

	if proxySpec.ProxyProtocol == api.ProxyProtocol_PROXY_PROTOCOL_PURE_FILE {
		log.Debugf("Volume [%s] is Pure File volume", volume.ID)
		return true, nil
	}

	log.Debugf("Volume [%s] is not Pure File volume", volume.ID)
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

// WaitForUpgrade waits for a given PX node to be upgraded to a specific version
func (d *portworx) WaitForUpgrade(n node.Node, tag string) error {
	log.Infof("Waiting for PX node [%s] to be upgraded to PX version [%s]", n.Name, tag)
	t := func() (interface{}, bool, error) {
		// filter out first 3 octets from the tag
		matches := regexp.MustCompile(`^(\d+\.\d+\.\d+).*`).FindStringSubmatch(tag)
		if len(matches) != 2 {
			return nil, false, &ErrFailedToUpgradeVolumeDriver{
				Version: fmt.Sprintf("%s", tag),
				Cause:   fmt.Sprintf("failed to parse first 3 octets of PX version from new version tag [%s]", tag),
			}
		}

		pxVersion, err := d.getPxVersionOnNode(n)
		if err != nil {
			return nil, true, &ErrFailedToWaitForPx{
				Node:  n,
				Cause: fmt.Sprintf("failed to get PX version from node [%s], Err: %v", n.Name, err),
			}
		}

		if !strings.HasPrefix(pxVersion, matches[1]) {
			return nil, true, &ErrFailedToUpgradeVolumeDriver{
				Version: fmt.Sprintf("%s", tag),
				Cause: fmt.Sprintf("PX version on node [%s] is still [%s]. It was expected to begin with [%s]",
					n.VolDriverNodeID, pxVersion, matches[1]),
			}
		}

		log.Infof("PX version on node [%s] is [%s]. Expected version is [%s]", n.VolDriverNodeID, pxVersion, matches[1])
		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, upgradeTimeout, upgradeRetryInterval); err != nil {
		return err
	}

	log.Infof("PX node [%s] was successfully upgraded to PX version [%s]", n.Name, tag)
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
	log.Debugf("Replication factor for volume: %s is %d", vol.ID, replFactor)

	return replFactor, nil
}

func (d *portworx) SetReplicationFactor(vol *torpedovolume.Volume, replFactor int64, nodesToBeUpdated []string, poolsToBeUpdated []string, waitForUpdateToFinish bool, opts ...torpedovolume.Options) error {
	volumeName := d.schedOps.GetVolumeName(vol)
	var replicationUpdateTimeout time.Duration
	if len(opts) > 0 {
		replicationUpdateTimeout = opts[0].ValidateReplicationUpdateTimeout
	} else {
		replicationUpdateTimeout = validateReplicationUpdateTimeout
	}
	log.Infof("Setting ReplicationUpdateTimeout to %s-%v\n", replicationUpdateTimeout, replicationUpdateTimeout)
	log.Infof("Setting ReplicationFactor to: %v", replFactor)

	t := func() (interface{}, bool, error) {
		volDriver := d.getVolDriver()
		volumeInspectResponse, err := volDriver.Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volumeName})
		if err != nil && errIsNotFound(err) {
			return nil, false, err
		} else if err != nil {
			return nil, true, err
		}

		replicaSet := &api.ReplicaSet{}
		if len(nodesToBeUpdated) > 0 {
			replicaSet = &api.ReplicaSet{Nodes: nodesToBeUpdated}
			log.Infof("Updating ReplicaSet of node(s): %v", nodesToBeUpdated)
		}

		if len(poolsToBeUpdated) > 0 {
			replicaSet.PoolUuids = append(replicaSet.PoolUuids, poolsToBeUpdated...)
			log.Infof("Updating ReplicaSet of pool: %v", poolsToBeUpdated)
		}

		volumeSpecUpdate := &api.VolumeSpecUpdate{
			HaLevelOpt:          &api.VolumeSpecUpdate_HaLevel{HaLevel: int64(replFactor)},
			SnapshotIntervalOpt: &api.VolumeSpecUpdate_SnapshotInterval{SnapshotInterval: math.MaxUint32},
			ReplicaSet:          replicaSet,
		}
		_, err = volDriver.Update(d.getContext(), &api.SdkVolumeUpdateRequest{
			VolumeId: volumeInspectResponse.Volume.Id,
			Spec:     volumeSpecUpdate,
		})
		if err != nil {
			return nil, false, err
		}
		if !waitForUpdateToFinish {
			return nil, false, nil
		}
		err = d.WaitForReplicationToComplete(vol, replFactor, replicationUpdateTimeout)
		if err != nil && errIsNotFound(err) {
			return nil, false, err
		} else if err != nil {
			return nil, true, err
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

func (d *portworx) WaitForReplicationToComplete(vol *torpedovolume.Volume, replFactor int64, replicationUpdateTimeout time.Duration) error {
	volumeName := d.schedOps.GetVolumeName(vol)
	volDriver := d.getVolDriver()
	volumeInspectResponse, err := volDriver.Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volumeName})
	if err != nil {
		return err
	}

	quitFlag := false
	waitTime := time.After(replicationUpdateTimeout)
	for !quitFlag && !(areRepSetsFinal(volumeInspectResponse.Volume, replFactor) && isClean(volumeInspectResponse.Volume)) {
		select {
		case <-waitTime:
			quitFlag = true
		default:
			volumeInspectResponse, err = volDriver.Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volumeName})
			if err != nil {
				return err
			}
			time.Sleep(defaultRetryInterval)
		}
	}
	if !(areRepSetsFinal(volumeInspectResponse.Volume, replFactor) && isClean(volumeInspectResponse.Volume)) {
		return fmt.Errorf("volume didn't successfully change to replication factor of %d", replFactor)
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
	log.Debugf("Aggregation level for volume: %s is %d", vol.ID, aggrLevel)

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
	if !d.skipPXSvcEndpoint {
		// Try portworx-service first
		endpoint, err := d.schedOps.GetServiceEndpoint()
		if err == nil && endpoint != "" {
			if err = d.testAndSetEndpointUsingService(endpoint); err == nil {
				d.refreshEndpoint = false
				return nil
			}
			log.Warnf("testAndSetEndpoint failed for %v: %v", endpoint, err)
		} else if err != nil && len(node.GetWorkerNodes()) == 0 {
			return err
		}
	}

	// Try direct address of cluster nodes
	// Set refresh endpoint to true so that we try and get the new
	// and working driver if the endpoint we are hooked onto goes
	// down
	d.refreshEndpoint = true
	log.Infof("Getting new driver.")
	for _, n := range node.GetWorkerNodes() {
		for _, addr := range n.Addresses {
			if err := d.testAndSetEndpointUsingNodeIP(addr); err != nil {
				log.Warnf("testAndSetEndpoint failed for %v: %v", addr, err)
				continue
			}
			return nil
		}
	}

	return fmt.Errorf("failed to get endpoint for portworx volume driver")
}

func (d *portworx) testAndSetEndpointUsingService(endpoint string) error {
	sdkPort, err := d.getSDKPort()
	if err != nil {
		return err
	}

	restPort, err := d.getRestPort()
	if err != nil {
		return err
	}

	return d.testAndSetEndpoint(endpoint, sdkPort, restPort)
}

func (d *portworx) testAndSetEndpointUsingNodeIP(ip string) error {
	sdkPort, err := d.getSDKContainerPort()
	if err != nil {
		return err
	}

	restPort, err := d.getRestContainerPort()
	if err != nil {
		return err
	}

	return d.testAndSetEndpoint(ip, sdkPort, restPort)
}

func (d *portworx) testAndSetEndpoint(endpoint string, sdkport, apiport int32) error {
	pxEndpoint := net.JoinHostPort(endpoint, strconv.Itoa(int(sdkport)))
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
	d.storagePoolManager = api.NewOpenStoragePoolClient(conn)
	d.nodeManager = api.NewOpenStorageNodeClient(conn)
	d.mountAttachManager = api.NewOpenStorageMountAttachClient(conn)
	d.clusterPairManager = api.NewOpenStorageClusterPairClient(conn)
	d.alertsManager = api.NewOpenStorageAlertsClient(conn)
	d.csbackupManager = api.NewOpenStorageCloudBackupClient(conn)
	d.licenseManager = pxapi.NewPortworxLicenseClient(conn)
	d.diagsManager = api.NewOpenStorageDiagsClient(conn)
	d.diagsJobManager = api.NewOpenStorageJobClient(conn)
	d.licenseFeatureManager = pxapi.NewPortworxLicensedFeatureClient(conn)
	d.autoFsTrimManager = api.NewOpenStorageFilesystemTrimClient(conn)
	d.portworxServiceClient = pxapi.NewPortworxServiceClient(conn)
	if legacyClusterManager, err := d.getLegacyClusterManager(endpoint, apiport); err == nil {
		d.legacyClusterManager = legacyClusterManager
	} else {
		return err
	}
	log.Infof("Using %v as endpoint for portworx volume driver", pxEndpoint)

	return nil
}

func (d *portworx) getLegacyClusterManager(endpoint string, pxdRestPort int32) (cluster.Cluster, error) {
	pxEndpoint := netutil.MakeURL("http://", endpoint, int(pxdRestPort))
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
	log.InfoD("Starting volume driver on %s.", n.Name)
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

// UpgradeDriver upgrades PX to a specific version, based on a given Spec Generator URL
func (d *portworx) UpgradeDriver(endpointVersion string) error {
	if endpointVersion == "" {
		return fmt.Errorf("Unable to upgrade PX, the Spec Generator URL was not passed")
	}
	specGenUrl := endpointVersion
	log.Infof("Upgrade PX using URL [%s]", specGenUrl)

	// If PX Operator based install, perform PX StorageCluster upgrade
	isOperatorBasedInstall, _ := d.IsOperatorBasedInstall()
	if isOperatorBasedInstall {
		// Check to see if px-versions configmap exists in PX namespace
		deployPxVersionsConfigMap := false
		if doesConfigMapExist(pxVersionsConfigmapName, d.namespace) {
			log.Infof("Configmap [%s] is found", pxVersionsConfigmapName)

			// Delete px-versions configmap
			if err := core.Instance().DeleteConfigMap(pxVersionsConfigmapName, d.namespace); err != nil {
				return fmt.Errorf("Failed to delete [%s] configmap", pxVersionsConfigmapName)
			}
			deployPxVersionsConfigMap = true
		}

		// Run air-gapped script to pull/push images for the new PX version hop, if custom registry configmap found
		if doesConfigMapExist(pxCustomRegistryConfigmapName, d.namespace) {
			if err := d.pullAndPushImagesToPrivateRegistry(specGenUrl); err != nil {
				return err
			}
		}

		// Create/Update px-version configmap to the new PX version hop only if it previously existed
		if deployPxVersionsConfigMap {
			if err := d.createPxVersionsConfigmap(specGenUrl, d.namespace); err != nil {
				return err
			}
		}

		// Upgrade PX operator, if not skipped
		log.Debugf("Env var SKIP_PX_OPERATOR_UPGRADE is set to [%v]", d.skipPxOperatorUpgrade)
		if !d.skipPxOperatorUpgrade {
			log.InfoD("Will upgrade PX Operator, if new version is availabe for given PX endpoint [%s]", specGenUrl)
			if err := d.upgradePortworxOperator(specGenUrl); err != nil {
				return fmt.Errorf("failed to upgrade PX Operator, Err: %v", err)
			}
		}

		log.InfoD("Will upgrade Portworx StorageCluster")
		if err := d.upgradePortworxStorageCluster(specGenUrl); err != nil {
			return fmt.Errorf("failed to upgrade PX StorageCluster, Err: %v", err)
		}
	} else {
		log.InfoD("Upgrading Portworx DaemonSet")
		if err := d.upgradePortworxDaemonset(specGenUrl); err != nil {
			return err
		}

		log.InfoD("Upgrading Stork")
		if err := d.UpgradeStork(specGenUrl); err != nil {
			return err
		}
	}

	return nil
}

// configurePxReleaseManifestEnvVars configure PX Release Manifest URL for edge and production PX versions, if needed
func configurePxReleaseManifestEnvVars(origEnvVarList []corev1.EnvVar, specGenURL string) ([]corev1.EnvVar, error) {
	var newEnvVarList []corev1.EnvVar

	// Remove release manifest URLs from Env Vars, if any exist
	for _, env := range origEnvVarList {
		if env.Name == pxReleaseManifestURLEnvVarName {
			continue
		}
		newEnvVarList = append(newEnvVarList, env)
	}

	// Set release manifest URL in case of edge
	if strings.Contains(specGenURL, "edge") {
		releaseManifestURL, err := optest.ConstructPxReleaseManifestURL(specGenURL)
		if err != nil {
			return nil, err
		}

		// Add release manifest URL to Env Vars
		newEnvVarList = append(newEnvVarList, corev1.EnvVar{Name: pxReleaseManifestURLEnvVarName, Value: releaseManifestURL})
	}

	return newEnvVarList, nil
}

// doesConfigMapExist returns true or false wether configmap exists or not
func doesConfigMapExist(configmapName, namespace string) bool {
	// Get configmap
	_, err := core.Instance().GetConfigMap(configmapName, namespace)
	if err != nil && k8serrors.IsNotFound(err) {
		return false
	}
	return true
}

func (d *portworx) createPxVersionsConfigmap(specGenUrl, pxNamespace string) error {
	// Get k8s version
	k8sVersion, err := d.schedOps.GetKubernetesVersion()
	if err != nil {
		return err
	}

	// Construct PX versions URL
	pxVersionURL, err := optest.ConstructVersionURL(specGenUrl, k8sVersion.String())
	if err != nil {
		return fmt.Errorf("Failed to construct PX version URL, Err: %v", err)
	}

	// Download PX versions URL content
	pxVersionsFile := path.Join("/versions")
	cmd := fmt.Sprintf("curl -o %s %s", pxVersionsFile, pxVersionURL)
	_, strErr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("Failed to download versions file from URL [%s], Err: %v %v", pxVersionURL, strErr, err)
	}

	// Create px-versions configmap
	log.InfoD("Creating px-versions configmap [%s] in [%s] namespace", pxVersionsConfigmapName, pxNamespace)
	cmd = fmt.Sprintf("kubectl -n %s create configmap %s --from-file=%s", pxNamespace, pxVersionsConfigmapName, pxVersionsFile)
	_, strErr, err = osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("Failed to create [%s] configmap in [%s] namespace, Err: %v", pxVersionsConfigmapName, pxNamespace, err)
	}

	return nil
}

// PpullAndPushImagesToPrivateRegistry will pull and push images to private registry using our air-gapped script
func (d *portworx) pullAndPushImagesToPrivateRegistry(specGenUrl string) error {
	// Get credentials for custom registry from px-custom-registry configmap
	log.Debugf("Get all docker credentials from [%s] configmap in [%s] namespace", pxCustomRegistryConfigmapName, d.namespace)
	pxCustomRegistryConfigmap, err := core.Instance().GetConfigMap(pxCustomRegistryConfigmapName, d.namespace)
	if err != nil || k8serrors.IsNotFound(err) {
		return fmt.Errorf("Failed to get [%s] configmap that should have been created by spawn or manually, Err: %v", pxCustomRegistryConfigmapName, err)
	}

	// Credentials for portworx and custom registry
	portworxDockerRegistryUsername := pxCustomRegistryConfigmap.Data["portworxDockerRegistryUsername"]
	portworxDockerRegistryPassword := pxCustomRegistryConfigmap.Data["portworxDockerRegistryPassword"]
	customDockerRegistryName := pxCustomRegistryConfigmap.Data["customDockerRegistryName"]
	customDockerRegistryUsername := pxCustomRegistryConfigmap.Data["customDockerRegistryUsername"]
	customDockerRegistryPassword := pxCustomRegistryConfigmap.Data["customDockerRegistryPassword"]

	if customDockerRegistryName == "" && customDockerRegistryUsername == "" && customDockerRegistryPassword == "" {
		return fmt.Errorf("Failed to get custom docker registry info from [%s] configmap [%+v]", pxCustomRegistryConfigmapName, pxCustomRegistryConfigmap.Data)
	}

	if portworxDockerRegistryUsername == "" && portworxDockerRegistryPassword == "" {
		return fmt.Errorf("Failed to get portworx docker registry info from [%s] configmap [%+v]", pxCustomRegistryConfigmapName, pxCustomRegistryConfigmap.Data)
	}

	log.InfoD("Pull and push PX images via air-gapped script to a private registry [%s]", customDockerRegistryName)

	// Construct air-gapped URL to use to download script
	pxAirgappedURL, err := d.constructPxAirgappedUrl(specGenUrl)
	if err != nil {
		return fmt.Errorf("Failed to construct air-gapped URL, Err: %v", err)
	}

	pxAgInstallBinary := path.Join("/px-ag-install.sh")
	log.InfoD("Downloading air-gapped script from [%s] to [%s]", pxAirgappedURL, pxAgInstallBinary)
	cmd := fmt.Sprintf("curl -o %s -L %s", pxAgInstallBinary, pxAirgappedURL)
	_, strErr, err := osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("Failed to download [%s] script from URL [%s], Err: %v %v", pxAgInstallBinary, pxAirgappedURL, strErr, err)
	}

	// Docker login to pull images
	log.InfoD("Login to pull images")
	cmd = fmt.Sprintf("docker login -u %s -p %s", portworxDockerRegistryUsername, portworxDockerRegistryPassword)
	_, strErr, err = osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("Failed to execute Docker login, Err: %v %v", strErr, err)
	}

	// Pull images via air-gapped script
	log.InfoD("Pull images using air-gapped script")
	cmd = fmt.Sprintf("sh %s pull", pxAgInstallBinary)
	_, strErr, err = osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("Failed to pull images using [%s] script, Err: %v %v", pxAgInstallBinary, strErr, err)
	}

	// Docker login to push images
	log.InfoD("Login to custom registry [%s] to push images", customDockerRegistryName)
	cmd = fmt.Sprintf("docker login -u %s -p %s %s", customDockerRegistryUsername, customDockerRegistryPassword, customDockerRegistryName)
	_, strErr, err = osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("Failed to execute Docker login, Err: %v %v", strErr, err)
	}

	// Push images to private registry
	log.InfoD("Tag and push images using air-gapped script")
	cmd = fmt.Sprintf("sh %s push %s", pxAgInstallBinary, customDockerRegistryName)
	_, strErr, err = osutils.ExecShell(cmd)
	if err != nil {
		return fmt.Errorf("Failed to push images using [%s] script, Err: %v %v", pxAgInstallBinary, strErr, err)
	}

	log.InfoD("Successfully pulled and pushed PX images via [%s] script to a private registry [%s]", pxAgInstallBinary, customDockerRegistryName)
	return nil
}

// getPxEndpointVersionFromUrl gets PX endpoint from the Spec Generator URL
func getPxEndpointVersionFromUrl(specGenUrl string) (string, error) {
	u, err := url.Parse(specGenUrl)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL [%s], Err: %v", specGenUrl, err)
	}
	return strings.Trim(u.Path, "/"), nil
}

// upgradePortworxDaemonset upgrades Portworx daemonset based on a given Spec Generator URL
func (d *portworx) upgradePortworxDaemonset(specGenUrl string) error {
	upgradeFileName := "/upgrade.sh"

	// Construct PX Upgrade URL
	u, err := url.Parse(specGenUrl)
	if err != nil {
		fmt.Printf("failed to parse URL [%s], Err: %v", specGenUrl, err)
	}
	u.Path = path.Join(u.Path, "upgrade")
	fullEndpointURL := u.String()
	log.Infof("Upgrading Portworx Daemonset using URL [%s]", fullEndpointURL)

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
		return fmt.Errorf("error on getting PX Version on node [%s], Err: %v", pxNode.Name, err)
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
	log.Info("Portworx Daemonset was successfully upgraded")

	// Get PX version from Spec Gen URL
	pxEndpointVersion, err := getPxEndpointVersionFromUrl(specGenUrl)
	if err != nil {
		return err
	}
	log.Infof("Portworx version [%s] from URL [%s]", specGenUrl, pxEndpointVersion)

	for _, n := range node.GetStorageDriverNodes() {
		if err := d.WaitForUpgrade(n, pxEndpointVersion); err != nil {
			return err
		}
	}
	return nil
}

// upgradePortworxStorageCluster upgrades PX StorageCluster based on the given Spec Generator URL
func (d *portworx) upgradePortworxStorageCluster(specGenUrl string) error {
	log.InfoD("Upgrading Portworx StorageCluster")

	// Get k8s version
	k8sVersion, err := k8sCore.GetVersion()
	if err != nil {
		return err
	}

	// Get images from version URL
	imageList, err := optest.GetImagesFromVersionURL(specGenUrl, k8sVersion.String())
	if err != nil {
		return fmt.Errorf("failed to get image list from version URL [%s], Err: %v", specGenUrl, err)
	}

	// Get PX StorageCluster
	cluster, err := d.GetDriver()
	if err != nil {
		return err
	}

	// Update PX Image and Env vars if needed
	updateParamFunc := func(cluster *v1.StorageCluster) *v1.StorageCluster {
		// Set oci-mon version
		cluster.Spec.Image = imageList["version"]
		var envVars []corev1.EnvVar
		// Add Release Manifest URL incase of edge spec
		envVars, err := configurePxReleaseManifestEnvVars(cluster.Spec.Env, specGenUrl)
		if err != nil {
			return nil
		}
		cluster.Spec.Env = envVars
		return cluster
	}

	// Update and validate PX StorageCluster
	if _, err := d.updateAndValidateStorageCluster(cluster, updateParamFunc, specGenUrl, false); err != nil {
		return err
	}

	log.Infof("Successfully upgraded Portworx StorageCluster [%s] to [%s]", cluster.Name, specGenUrl)
	return nil
}

// upgradePortworxOperator will upgrade PX Operator version
func (d *portworx) upgradePortworxOperator(specGenUrl string) error {
	log.InfoD("Upgrading Portworx Operator")

	pxOperatorSpecGenUrl, err := d.constructPxOperatorSpecGenUrl(specGenUrl)
	if err != nil {
		return fmt.Errorf("Failed to construct PX Operator spec gen URL, Err: %v", err)
	}

	resp, err := http.Get(pxOperatorSpecGenUrl)
	if err != nil {
		return fmt.Errorf("Failed to get data from [%s], Err: %v", pxOperatorSpecGenUrl, err)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("Failed to read data from [%s], Err: %v", pxOperatorSpecGenUrl, err)
	}

	// This regex will look for image: and will pull the tag after the colon : which should be match[1]
	regexpString := "image:\\s\\S+:(\\S+)"
	matchRegExp := regexp.MustCompile(regexpString)
	match := matchRegExp.FindStringSubmatch(string(respBytes))
	// Expecting match to contain 2 slices, one is full image and the other is just an image tag
	if len(match) < 2 {
		return fmt.Errorf("Expecting match [%s] to have 2 slices, only got %d ", match, len(match))
	}
	pxOperatorNewImageTag := fmt.Sprintf("%v", match[1])

	// Get PX Operator deployment
	opDep, err := apps.Instance().GetDeployment("portworx-operator", d.namespace)
	if err != nil {
		return fmt.Errorf("Failed to get PX Operator deployment [portworx-operator] in [%s] namespace, Err: %v", d.namespace, err)
	}

	for ind, container := range opDep.Spec.Template.Spec.Containers {
		if container.Name == "portworx-operator" {
			container.Image = strings.Replace(container.Image, strings.Split(container.Image, ":")[1], pxOperatorNewImageTag, -1)
			opDep.Spec.Template.Spec.Containers[ind] = container
		}
	}

	// Update image for PX Operator deployment
	log.InfoD("Updating PX Operator image tag to [%s]", pxOperatorNewImageTag)
	opDep, err = apps.Instance().UpdateDeployment(opDep)
	if err != nil {
		return fmt.Errorf("Failed to update PX Operator deployment [%s] in [%s] namespace, Errr: %v", opDep.Name, opDep.Namespace, err)
	}

	// Validate PX Operator deployment
	log.InfoD("Validating PX Operator deployment [%s] in [%s] namespace", opDep.Name, opDep.Namespace)
	if err := apps.Instance().ValidateDeployment(opDep, validateDeploymentTimeout, validateDeploymentInterval); err != nil {
		return fmt.Errorf("Failed to validate PX Operator deployment [%s] in [%s] namespace, Err: %v", opDep.Name, opDep.Namespace, err)
	}

	log.InfoD("Successfully upgraded Portworx Operator to [%s]", pxOperatorNewImageTag)
	return nil
}

// constructPxOperatorSpecGenUrl constructs PX Operator spec gen URL
func (d *portworx) constructPxOperatorSpecGenUrl(specGenUrl string) (string, error) {
	// Get k8s version
	kubeVersion, err := d.schedOps.GetKubernetesVersion()
	if err != nil {
		return "", err
	}

	// Construct PX Operator spec gen URL
	u, err := url.Parse(specGenUrl)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL [%s], Err: %v", specGenUrl, err)
	}
	q := u.Query()
	q.Set("kbver", kubeVersion.String())
	q.Set("comp", "pxoperator")
	q.Set("ns", d.namespace)
	u.RawQuery = q.Encode()
	pxOperatorSpecGenUrl := u.String()

	return pxOperatorSpecGenUrl, nil
}

// constructPxAirgappedUrl constructs PX air-gapped URL
func (d *portworx) constructPxAirgappedUrl(specGenUrl string) (string, error) {
	// Get k8s version
	kubeVersion, err := d.schedOps.GetKubernetesVersion()
	if err != nil {
		return "", err
	}

	// Convert k8s version into a specific format that is acceptable by air-gapped script
	// NOTE: This is needed due to the bug in the air-gapped script, for more info please see https://portworx.atlassian.net/browse/PWX-30781
	newK8sVersion, err := version.NewVersion(kubeVersion.String())
	if err != nil {
		return "", err
	}
	k8sVersion := newK8sVersion.Core().String()

	// Construct PX air-gapped URL
	u, err := url.Parse(specGenUrl)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL [%s], Err: %v", specGenUrl, err)
	}
	q := u.Query()
	q.Set("kbver", k8sVersion)
	u.Path = path.Join(u.Path, "air-gapped")
	u.RawQuery = q.Encode()
	pxAirgappedUrl := u.String()

	return pxAirgappedUrl, nil
}

// UpgradeStork upgrades Stork based on the given Spec Generator URL
func (d *portworx) UpgradeStork(specGenUrl string) error {
	storkSpecFileName := "/stork.yaml"
	nodeList := node.GetStorageDriverNodes()
	pxNode := nodeList[0]
	pxVersion, err := d.getPxVersionOnNode(pxNode)
	if err != nil {
		return fmt.Errorf("error on getting PX version on node [%s], Err: %v", pxNode.Name, err)
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
		log.Debugf("Skipping Stork upgrade as PX version is less than [%s]", pxMinVersionForStorkUpgrade)
		return nil
	}

	kubeVersion, err := d.schedOps.GetKubernetesVersion()
	if err != nil {
		return err
	}

	// Getting stork spec
	u, err := url.Parse(specGenUrl)
	if err != nil {
		return fmt.Errorf("failed to parse URL [%s], Err: %v", specGenUrl, err)
	}
	q := u.Query()
	q.Set("kbver", kubeVersion.String())
	q.Set("comp", "stork")
	u.RawQuery = q.Encode()
	storkSpecGenUrl := u.String()
	log.Debugf("Getting Stork spec from URL [%s]", storkSpecGenUrl)
	if err := osutils.Wget(storkSpecGenUrl, storkSpecFileName, true); err != nil {
		return err
	}

	// Getting context of the file
	if _, err := osutils.Cat(storkSpecFileName); err != nil {
		return err
	}

	// Apply Stork spec
	cmdArgs := []string{"apply", "-f", storkSpecFileName}
	if err := osutils.Kubectl(cmdArgs); err != nil {
		return err
	}

	log.Infof("Successfully applied Stork spec")
	return nil
}

func (d *portworx) RestartDriver(n node.Node, triggerOpts *driver_api.TriggerOptions) error {
	return driver_api.PerformTask(
		func() error {
			return d.schedOps.RestartPxOnNode(n)
		},
		triggerOpts)
}

// GetClusterPairingInfo returns cluster pair information
func (d *portworx) GetClusterPairingInfo(kubeConfigPath, token string, isPxLBService bool, reversePair bool) (map[string]string, error) {
	pairInfo := make(map[string]string)
	pxNodes, err := d.schedOps.GetRemotePXNodes(kubeConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed retrieving remote PX nodes, Err: %v", err)
	}
	if len(pxNodes) == 0 {
		return nil, fmt.Errorf("No PX Nodes were found")
	}

	// Initially endpoint and IP in cluster pair both default to Node IP
	address, pairIP := pxNodes[0].Addresses[0], pxNodes[0].Addresses[0]
	if isPxLBService {
		// For Loadbalancer change endpoint to service endpoint
		endpoint, err := d.schedOps.GetServiceEndpoint()
		if err != nil {
			return nil, fmt.Errorf("failed to get px service endpoint: %v", err)
		}
		address = endpoint
		if reversePair == true {
			address = pxServiceLocalEndpoint
		}
		// For Loadbalancer set IP in cluster pair to service endpoint
		pairIP = endpoint
	}
	clusterPairManager, err := d.getClusterPairManagerByAddress(address, token)
	if err != nil {
		return nil, err
	}

	var resp *api.SdkClusterPairGetTokenResponse
	if token != "" {
		resp, err = clusterPairManager.GetToken(d.getContextWithToken(context.Background(), token), &api.SdkClusterPairGetTokenRequest{})
	} else {
		resp, err = clusterPairManager.GetToken(d.getContext(), &api.SdkClusterPairGetTokenRequest{})
	}

	log.Infof("Response for token: %v", resp.Result.Token)

	// file up cluster pair info
	pairInfo[clusterIP] = pairIP
	pairInfo[tokenKey] = resp.Result.Token
	pwxServicePort, err := d.getRestContainerPort()
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
			Cause: fmt.Sprintf("Failed to set label on node [%s], Err: %v", n.Name, err),
		}
	}

	err := d.EnterMaintenance(*n)
	//check for storageless node
	if err != nil && len(n.StoragePools) == 0 {
		log.Infof("validating status for storageless node [%s]", n.Name)
		stNode, nodeStatusErr := d.GetDriverNode(n)
		if nodeStatusErr != nil {
			return nodeStatusErr
		}
		if stNode.Status == api.Status_STATUS_OFFLINE {
			//setting nil as OFFLINE status is expected for storageless nodes
			err = nil
		}
	}
	if err != nil {
		return err
	}

	nodeResp, err := d.getNodeManager().Inspect(d.getContext(), &api.SdkNodeInspectRequest{NodeId: n.VolDriverNodeID})
	if err != nil {
		return &ErrFailedToDecommissionNode{
			Node:  n.Name,
			Cause: fmt.Sprintf("Failed to inspect node [%s], Err: %v", nodeResp.Node, err),
		}
	}
	log.Infof("Removing node [%s], Current status: %v", nodeResp.Node.Id, nodeResp.Node.Status)

	t := func() (interface{}, bool, error) {
		stNode, err := d.GetDriverNode(n)
		if err != nil {
			return false, true, fmt.Errorf("failed getting node [%s] status", n.Name)
		}
		if stNode.Status == api.Status_STATUS_MAINTENANCE || stNode.Status == api.Status_STATUS_OFFLINE {
			return true, false, nil
		}
		return false, true, fmt.Errorf("waiting for node [%s] to be in maintenence mode, current Status: %v", n.Name, stNode.Status)
	}
	_, err = task.DoRetryWithTimeout(t, defaultTimeout, defaultRetryInterval)

	cmd := fmt.Sprintf("echo Y | %s cluster delete -f %s", d.getPxctlPath(*n), nodeResp.Node.Id)
	log.Infof("Running command [%s] on node [%s]", cmd, n.Name)

	var cmdNode node.Node
	for _, cn := range node.GetStorageDriverNodes() {
		if cn.Name != n.Name {
			cmdNode = cn
			break
		}
	}

	t = func() (interface{}, bool, error) {
		out, err := d.nodeDriver.RunCommandWithNoRetry(cmdNode, cmd, node.ConnectionOpts{
			Timeout:         2 * time.Minute,
			TimeBeforeRetry: 10 * time.Second,
		})
		if err != nil && strings.Contains(err.Error(), "Node remove is pending") {

			return out, false, nil

		}
		if err != nil {
			return "", true, err
		}
		return out, false, nil
	}

	out, err := task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second)

	if err != nil {
		return err
	}
	outLines := strings.Split(out.(string), "\n")

	log.Infof("Node [%s] remove is pending. Waiting for it to complete,output: %s", nodeResp.Node.Id, outLines)

	// update node in registry
	n.IsStorageDriverInstalled = false
	if err := node.UpdateNode(*n); err != nil {
		return fmt.Errorf("failed to update node [%s], Err: %v", n.Name, err)
	}

	// Force refresh endpoint
	d.refreshEndpoint = true

	t = func() (interface{}, bool, error) {
		latestNodes, err := d.getPxNodes()
		isNodeExist := false

		if err != nil {
			return nil, true, &ErrFailedToDecommissionNode{
				Node:  n.Name,
				Cause: err.Error(),
			}
		}

		for _, latestNode := range latestNodes {
			if latestNode.Hostname == n.Hostname {
				isNodeExist = true
				break
			}
		}

		if isNodeExist {
			return nil, true, &ErrFailedToDecommissionNode{
				Node:  n.Name,
				Cause: fmt.Errorf("node [%s] still exist in the PX cluster", n.Name).Error(),
			}
		}
		return nil, false, nil
	}

	_, err = task.DoRetryWithTimeout(t, 25*time.Minute, 3*time.Minute)

	if err != nil {
		return &ErrFailedToDecommissionNode{
			Node:  n.Name,
			Cause: fmt.Errorf("node [%s] still exist in the PX cluster", n.Name).Error(),
		}
	}

	log.Infof("Successfully removed node [%s/%s]", n.Name, n.VolDriverNodeID)
	return nil
}

func (d *portworx) RejoinNode(n *node.Node) error {
	opts := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: podUpRetryInterval,
		Timeout:         10 * time.Minute,
	}

	if _, err := d.nodeDriver.RunCommand(*n, fmt.Sprintf("%s sv node-wipe --all", d.getPxctlPath(*n)), opts); err != nil {
		return &ErrFailedToRejoinNode{
			Node:  n.Name,
			Cause: fmt.Sprintf("Failed running node wipe on node [%s], Err: %v", n.Name, err),
		}
	}
	log.Info("Node wipe is successfull")

	if err := k8sCore.RemoveLabelOnNode(n.Name, schedops.PXServiceLabelKey); err != nil {
		return &ErrFailedToRejoinNode{
			Node:  n.Name,
			Cause: fmt.Sprintf("Failed to set label on node [%s], Err: %v", n.Name, err),
		}
	}

	if err := k8sCore.RemoveLabelOnNode(n.Name, schedops.PXEnabledLabelKey); err != nil {
		return &ErrFailedToRejoinNode{
			Node:  n.Name,
			Cause: fmt.Sprintf("Failed to set label on node [%s], Err: %v", n.Name, err),
		}
	}

	if err := d.RestartDriver(*n, nil); err != nil {
		return &ErrFailedToRejoinNode{
			Node:  n.Name,
			Cause: err.Error(),
		}
	}

	if err := k8sCore.UnCordonNode(n.Name, defaultTimeout, defaultRetryInterval); err != nil {
		return &ErrFailedToRejoinNode{
			Node:  n.Name,
			Cause: fmt.Sprintf("Failed to uncordon node [%s], Err: %v", n.Name, err),
		}
	}

	return nil
}

func (d *portworx) GetNodeStatus(n node.Node) (*api.Status, error) {

	f := func() (interface{}, bool, error) {
		nodeResponse, err := d.getNodeManager().Inspect(d.getContext(), &api.SdkNodeInspectRequest{NodeId: n.VolDriverNodeID})
		if err != nil {
			if isNodeNotFound(err) {
				return nil, false, err
			}
			return nil, true, err
		}

		return nodeResponse, false, nil
	}
	resp, err := task.DoRetryWithTimeout(f, 5*time.Minute, 1*time.Minute)

	if err != nil {
		if isNodeNotFound(err) {
			apiSt := api.Status_STATUS_NONE
			return &apiSt, nil
		}
		return nil, &ErrFailedToGetNodeStatus{
			Node:  n.Name,
			Cause: fmt.Sprintf("Failed to check status on node [%s], Err: %v", n.Name, err),
		}
	}
	nodeResponse := resp.(*api.SdkNodeInspectResponse)

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

func (d *portworx) getDiagsManager() api.OpenStorageDiagsClient {
	if d.refreshEndpoint {
		d.setDriver()
	}
	return d.diagsManager
}

func (d *portworx) getDiagsJobManager() api.OpenStorageJobClient {
	if d.refreshEndpoint {
		d.setDriver()
	}
	return d.diagsJobManager
}

func (d *portworx) getLicenseManager() pxapi.PortworxLicenseClient {
	if d.refreshEndpoint {
		d.setDriver()
	}
	return d.licenseManager
}

func (d *portworx) getLicenseFeatureManager() pxapi.PortworxLicensedFeatureClient {
	if d.refreshEndpoint {
		d.setDriver()
	}
	return d.licenseFeatureManager
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

func (d *portworx) getClusterPairManagerByAddress(addr, token string) (api.OpenStorageClusterPairClient, error) {
	pxPort, err := d.getSDKContainerPort()
	if err != nil {
		return nil, err
	}
	pxEndpoint := net.JoinHostPort(addr, strconv.Itoa(int(pxPort)))
	conn, err := grpc.Dial(pxEndpoint, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	dClient := api.NewOpenStorageClusterPairClient(conn)
	if token != "" {
		_, err = dClient.Enumerate(d.getContextWithToken(context.Background(), token), &api.SdkClusterPairEnumerateRequest{})
	} else {
		_, err = dClient.Enumerate(d.getContext(), &api.SdkClusterPairEnumerateRequest{})
	}
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
	pxPort, err := d.getSDKContainerPort()
	if err != nil {
		return nil, err
	}
	pxEndpoint := net.JoinHostPort(addr, strconv.Itoa(int(pxPort)))
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

func (d *portworx) getFilesystemTrimManager() api.OpenStorageFilesystemTrimClient {
	if d.refreshEndpoint {
		d.setDriver()
	}
	return d.autoFsTrimManager

}

func (d *portworx) maintenanceOp(n node.Node, op string) error {
	var err error
	// we are removing using service endpoint because services would
	// return a random node endpoint the service chooses and puts it in maintenance.
	// this would fail the status check in `EnterMaintenanc` and `ExitMaintenance`
	pxdRestPort, err := d.getRestContainerPort()
	if err != nil {
		return err
	}
	url := netutil.MakeURL("http://", n.Addresses[0], int(pxdRestPort))

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
		return &node.Node{}, fmt.Errorf("failed to update node ID for node [%s] with ID [%s] in the cluster, Err: %v", n.Name, n.Id, err)
	}
	return n, nil
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
		log.Warnf("Snapshot volume %v not found: %v", snap, err)
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

// DeleteSnapshotsForVolumes deletes snapshots for the specified volumes
func (d *pure) DeleteSnapshotsForVolumes(volumeNames []string, clusterCredential string) error {
	log.Warnf("DeleteSnapshotsForVolumes function has not been implemented for volume driver - %s", d.String())
	return nil
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

// GetKvdbMembers return KVDM member nodes of the PX Cluster
func (d *portworx) GetKvdbMembers(n node.Node) (map[string]*torpedovolume.MetadataNode, error) {
	var err error
	kvdbMembers := make(map[string]*torpedovolume.MetadataNode)
	pxdRestPort, err := d.getRestPort()

	if err != nil {
		return kvdbMembers, err
	}
	var url, endpoint string
	if !d.skipPXSvcEndpoint {
		endpoint, err = d.schedOps.GetServiceEndpoint()
	}

	if err != nil || endpoint == "" {
		log.Warnf("unable to get service endpoint falling back to node addr: err=%v, skipPXSvcEndpoint=%v", err, d.skipPXSvcEndpoint)
		pxdRestPort, err = d.getRestContainerPort()
		if err != nil {
			return kvdbMembers, err
		}
		url = netutil.MakeURL("http://", n.Addresses[0], int(pxdRestPort))
	} else {
		url = netutil.MakeURL("http://", endpoint, int(pxdRestPort))
	}
	// TODO replace by sdk call whenever it is available
	log.Debugf("Url to call %v", url)
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

// GetTimeStamp returns 'readable' timestamp with no spaces 'YYYYMMDDHHMMSS'
func GetTimeStamp() string {
	tnow := time.Now()
	return fmt.Sprintf("%d%02d%02d%02d%02d%02d",
		tnow.Year(), tnow.Month(), tnow.Day(),
		tnow.Hour(), tnow.Minute(), tnow.Second())
}

func (d *portworx) CollectDiags(n node.Node, config *torpedovolume.DiagRequestConfig, diagOps torpedovolume.DiagOps) error {
	if diagOps.Async {
		// Diag collection is done via SDK request
		return collectDiagsSdk(n, config, diagOps, d)
	}
	// Diag collection is done via CLI or API
	return collectDiags(n, config, diagOps, d)
}

func (d *portworx) ValidateDiagsOnS3(n node.Node, diagsFile, pxDir string) error {
	log.Infof("Validating diags got uploaded to the s3 bucket for node [%s]", n.Name)

	// Diag file to look for
	if diagsFile == "" {
		return fmt.Errorf("Empty diag file was passed, cannot continue with validation")
	}
	d.DiagsFile = diagsFile

	opts := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
		Sudo:            true,
	}

	// Get cluster UUID to determine the S3 folder to look for
	clusterUUID, err := d.GetClusterUUID(n, opts, pxDir)
	if err != nil {
		return fmt.Errorf("Failed to get cluster UUID, Err: %v", err)
	}

	// Check S3 bucket for diags
	log.Debugf("Validating diag file [%s] got uploaded to the s3 bucket", d.DiagsFile)
	start := time.Now()
	for {
		if time.Since(start) >= validateDiagsOnS3RetryTimeout {
			return fmt.Errorf("waiting for diags job timed out after [%v], failed to find diag file [%s] on s3 bucket", validateDiagsOnS3RetryTimeout, d.DiagsFile)
		}
		var objects []s3utils.Object
		var err error

		// Check latest s3 folder for diags
		latestObjects, err := s3utils.GetS3Objects(clusterUUID, n.Name, false)
		if err != nil {
			return fmt.Errorf("Failed to get g3 objects from latest folder, Err: %v", err)
		}
		objects = append(objects, latestObjects...)

		// Check previous s3 folder for diags, if exists, due to some race condition of how/when diags can occasionally be uploaded
		previousObjects, err := s3utils.GetS3Objects(clusterUUID, n.Name, true)
		if err != nil {
			return fmt.Errorf("Failed to get g3 objects from previous folder, Err: %v", err)
		}
		objects = append(objects, previousObjects...)

		for _, obj := range objects {
			if strings.Contains(obj.Key, d.DiagsFile) {
				log.Debugf("File validated on s3")
				log.Debugf("Object Name is [%s]", obj.Key)
				log.Debugf("Object Created on [%s]", obj.LastModified.String())
				log.Debugf("Object Size [%d]", obj.Size)
				return nil
			}
		}
		log.Debugf("File [%s] not found in the s3 bucket yet, re-trying in [%v]", d.DiagsFile, validateDiagsOnS3RetryInterval)
		time.Sleep(validateDiagsOnS3RetryInterval)
	}
}

// GetClusterUUID Gets Portworx cluster UUID from cluster_uuid file and returns it
func (d *portworx) GetClusterUUID(n node.Node, opts node.ConnectionOpts, pxDir string) (string, error) {
	out, err := d.nodeDriver.RunCommand(n, fmt.Sprintf("cat %s", path.Join(pxDir, clusterUUIDFile)), opts)
	if err != nil {
		return "", fmt.Errorf("failed to get pxctl status, Err: %v", err)
	}
	return out, nil
}

func collectDiags(n node.Node, config *torpedovolume.DiagRequestConfig, diagOps torpedovolume.DiagOps, d *portworx) error {
	var hostname string
	var status api.Status

	if !diagOps.PxStopped {
		pxNode, err := d.GetDriverNode(&n)
		if err != nil {
			return err
		}
		hostname = pxNode.Hostname
		status = pxNode.Status
	} else {
		hostname = n.Name
		status = api.Status_STATUS_OFFLINE
	}
	log.InfoD("Collecting diags on node [%s]", hostname)

	opts := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
		Sudo:            true,
	}

	if !diagOps.Validate {
		log.Infof("Skip validate on node [%s] during diags collection", hostname)
	}

	// If PX status is OFFLINE, we collect diags via CLI
	if status == api.Status_STATUS_OFFLINE {
		log.Debugf("Node [%s] is offline, collecting diags using pxctl", hostname)

		// Only way to collect diags when PX is offline is using pxctl
		out, err := d.GetPxctlCmdOutputConnectionOpts(n, fmt.Sprintf("sv diags -a -f --output %s", config.OutputFile), opts, true)
		if err != nil {
			return fmt.Errorf("failed to collect diags on node [%s], Err: %v %v", hostname, err, out)
		}
	} else { // Collecting diags via API
		diagsPort := 9014
		// Need to get the diags server port based on the px mgmnt port for OCP its not the standard 9001
		out, err := d.nodeDriver.RunCommand(n, fmt.Sprintf("cat %s", path.Join(diagOps.PxDir, "px_env")), opts)
		if err == nil {
			envVars := map[string]string{}
			for _, line := range strings.Split(out, "\n") {
				splits := strings.Split(line, "=")
				if len(splits) > 1 {
					envVars[splits[0]] = splits[1]
				}
			}
			pxMgmntPort, err := strconv.Atoi(envVars["PWX_MGMT_PORT"])
			if err == nil {
				if pxMgmntPort != 9001 {
					diagsPort = pxMgmntPort + 10
				}
			}
		}

		if len(d.token) > 0 {
			config.Token = d.token
			log.Infof("Added securty token [%s]", config.Token)
		}

		url := netutil.MakeURL("http://", n.Addresses[0], diagsPort)
		log.Infof("Diags server url [%s]", url)

		c, err := client.NewClient(url, "", "")
		if err != nil {
			return err
		}
		req := c.Post().Resource(pxDiagPath).Body(config)

		resp := req.Do()
		if resp.Error() != nil {
			return fmt.Errorf("failed to collect diags on node [%s], Err: %v", hostname, resp.Error())
		}
	}

	// Validate diags, only works for none profile only diags, because OutputFile for the diags.tgz
	if diagOps.Validate && !config.Profile {
		cmd := fmt.Sprintf("test -f %s", config.OutputFile)
		out, err := d.nodeDriver.RunCommand(n, cmd, opts)
		if err != nil {
			return fmt.Errorf("failed to locate diags on node [%s], Err: %v %v", hostname, err, out)
		}

		log.Infof("Found diags file [%s]", config.OutputFile)
		d.DiagsFile = config.OutputFile[strings.LastIndex(config.OutputFile, "/")+1:]
	}

	log.Debugf("Successfully collected diags on node [%s]", hostname)
	return nil
}

func collectDiagsSdk(n node.Node, config *torpedovolume.DiagRequestConfig, diagOps torpedovolume.DiagOps, d *portworx) error {
	diagsMgr := d.getDiagsManager()
	jobMgr := d.getDiagsJobManager()

	pxNode, err := d.GetDriverNode(&n)
	if err != nil {
		return err
	}

	req := &api.SdkDiagsCollectRequest{
		Issuer:      "CLI",
		ProfileOnly: config.Profile,
		Live:        config.Live,
		Filename:    config.OutputFile,
	}

	req.Node = &api.DiagsNodeSelector{
		NodeIds: []string{pxNode.Id},
	}

	// Collect diags via SDK request
	resp, err := diagsMgr.Collect(d.getContext(), req)
	if err != nil {
		return fmt.Errorf("failed to collect diags, Err: %v", err)
	}
	if resp.Job == nil {
		return fmt.Errorf("diags collection SDK request submitted, but did not get a Job ID in response")
	}

	// Wait for diags job to finish
	start := time.Now()
	for {
		if time.Since(start) >= sdkDiagCollectionTimeout {
			return fmt.Errorf("waiting for diags job timed out after [%v]", sdkDiagCollectionTimeout)
		}

		resp, _ := jobMgr.GetStatus(d.getContext(), &api.SdkGetJobStatusRequest{
			Id:   resp.Job.GetId(),
			Type: resp.Job.GetType(),
		})

		state := resp.GetJob().GetState()
		if state == api.Job_DONE || state == api.Job_FAILED || state == api.Job_CANCELLED {
			log.Debugf("Diag job state is [%v]", state)
			break
		}

		// Sleep before checking state of diags job again
		log.Debugf("Diag job state is [%v], waiting for [%v] to check job status again", state, sdkDiagCollectionRetryInterval)
		time.Sleep(sdkDiagCollectionRetryInterval)
	}

	// Validate diags, only works for none profile only diags, because OutputFile for the diags.tgz
	if diagOps.Validate && !config.Profile {
		opts := node.ConnectionOpts{
			IgnoreError:     false,
			TimeBeforeRetry: defaultRetryInterval,
			Timeout:         defaultTimeout,
			Sudo:            true,
		}

		cmd := fmt.Sprintf("test -f %s", config.OutputFile)
		out, err := d.nodeDriver.RunCommand(n, cmd, opts)
		if err != nil {
			return fmt.Errorf("failed to locate diags on node [%s], Err: %v %v", pxNode.Hostname, err, out)
		}

		log.Infof("Found diags file [%s]", config.OutputFile)
		d.DiagsFile = config.OutputFile[strings.LastIndex(config.OutputFile, "/")+1:]
	}

	log.Infof("Successfully collected diags on node [%s]", n.Name)
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
								log.Errorf("Can't parse actionScaleSize: '%d', cause err: %s/%s", actionScaleSize, err, parseErr)
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
						calculatedTotalSize += uint64(requiredScaleSize) * uint64(len(node.Disks))
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
						maxSize, err, parseErr := parseMaxSize(actionMaxSize)
						if err != nil && parseErr != nil {
							log.Errorf("Can't parse maxSize: '%d', cause err: %v/%v", maxSize, err, parseErr)
						}
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
						maxSize, err, parseErr := parseMaxSize(actionMaxSize)
						if err != nil && parseErr != nil {
							log.Errorf("Can't parse maxSize: '%d', cause err: %v/%v", maxSize, err, parseErr)
						}
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

// GetLicenseSummary() returns the activated License
func (d *portworx) GetLicenseSummary() (torpedovolume.LicenseSummary, error) {
	licenseMgr := d.getLicenseManager()
	featureMgr := d.getLicenseFeatureManager()
	licenseSummary := torpedovolume.LicenseSummary{}

	lic, err := licenseMgr.Status(d.getContext(), &pxapi.PxLicenseStatusRequest{})
	if err != nil {
		return licenseSummary, err
	}

	licenseSummary.SKU = lic.GetStatus().GetSku()
	if lic != nil && lic.Status != nil &&
		lic.Status.GetConditions() != nil &&
		len(lic.Status.GetConditions()) > 0 {
		licenseSummary.LicenesConditionMsg = lic.Status.GetConditions()[0].GetMessage()
	}

	features, err := featureMgr.Enumerate(d.getContext(), &pxapi.PxLicensedFeatureEnumerateRequest{})
	if err != nil {
		return licenseSummary, err
	}
	licenseSummary.Features = features.GetFeatures()
	return licenseSummary, nil
}

func (d *portworx) SetClusterRunTimeOpts(n node.Node, rtOpts map[string]string) error {
	var err error

	opts := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
		Sudo:            true,
	}

	var rtopts string
	for k, v := range rtOpts {
		rtopts += k + "=" + v + ","
	}

	rtopts = strings.TrimSuffix(rtopts, ",")
	cmd := fmt.Sprintf("%s cluster options update --runtime-options %s", d.getPxctlPath(n), rtopts)

	out, err := d.nodeDriver.RunCommand(n, cmd, opts)
	if err != nil {
		return fmt.Errorf("failed to set rt_opts, Err: %v %v", err, out)
	}

	log.Debugf("Successfully set rt_opts")
	return nil
}

func (d *portworx) SetClusterOpts(n node.Node, clusterOpts map[string]string) error {
	var err error

	opts := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
		Sudo:            true,
	}

	var clusteropts string
	for k, v := range clusterOpts {
		clusteropts += k + "=" + v + " "
	}
	clusteropts = strings.TrimSuffix(clusteropts, " ")
	cmd := fmt.Sprintf(" cluster options update %s", clusteropts)
	//Create context with admin token if PX security is enabled, later delete the token
	out, err := d.GetPxctlCmdOutputConnectionOpts(n, cmd, opts, true)
	if err != nil {
		return fmt.Errorf("failed to set cluster options, Err: %v %v", err, out)
	}
	log.Debugf("Successfully updated Cluster Options")
	return nil
}

// GetClusterOpts get all cluster options
func (d *portworx) GetClusterOpts(n node.Node, options []string) (map[string]string, error) {
	opts := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
	}
	cmd := fmt.Sprintf("%s cluster options list -j json", d.getPxctlPath(n))
	out, err := d.nodeDriver.RunCommand(n, cmd, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to get pxctl cluster options on node [%s], Err: %v", n.Name, err)
	}
	var data = map[string]interface{}{}
	err = json.Unmarshal([]byte(out), &data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal pxctl cluster option on node [%s], Err: %v", n.Name, err)
	}
	sort.Strings(options)
	var options_map = make(map[string]string)
	//Values can be string, array or map
	for key, val := range data {
		index := sort.SearchStrings(options, key)
		if index < len(options) && options[index] == key {
			options_map[key] = fmt.Sprint(val)
		}
	}
	//Make sure required options are available
	for _, option := range options {
		if _, ok := options_map[option]; !ok {
			return nil, fmt.Errorf("Failed to find option: %v", option)
		}
	}
	return options_map, nil
}

func (d *portworx) SetClusterOptsWithConfirmation(n node.Node, clusterOpts map[string]string) error {
	var err error

	opts := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
		Sudo:            true,
	}

	var clusteropts string
	for k, v := range clusterOpts {
		clusteropts += k + "=" + v + " "
	}

	clusteropts = strings.TrimSuffix(clusteropts, " ")
	cmd := fmt.Sprintf("yes Y | %s cluster options update %s", d.getPxctlPath(n), clusteropts)

	out, err := d.nodeDriver.RunCommand(n, cmd, opts)
	if err != nil {
		return fmt.Errorf("failed to set cluster options on node [%s], Err: %v %v", n.Name, err, out)
	}

	log.Debugf("Successfully updated Cluster Options")
	return nil
}

func (d *portworx) ToggleCallHome(n node.Node, enabled bool) error {
	var err error

	opts := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
		Sudo:            true,
	}

	cmd := fmt.Sprintf("%s sv call-home enable", d.getPxctlPath(n))
	if !enabled {
		cmd = fmt.Sprintf("%s sv call-home disable", d.getPxctlPath(n))
	}

	out, err := d.nodeDriver.RunCommand(n, cmd, opts)
	if err != nil {
		return fmt.Errorf("failed to toggle call-home on node [%s], Err: %v %v", n.Name, err, out)
	}

	log.Debugf("Successfully toggled call-home")
	return nil
}

func (d *portworx) IsOperatorBasedInstall() (bool, error) {
	_, err := apiExtentions.GetCRD("storageclusters.core.libopenstorage.org", metav1.GetOptions{})
	return err == nil, err
}

func (d *portworx) RunSecretsLogin(n node.Node, secretType string) error {
	opts := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
	}

	pxctlPath := d.getPxctlPath(n)

	command := fmt.Sprintf("secrets %s login", secretType)
	cmd := fmt.Sprintf("%s %s", pxctlPath, command)
	_, err := d.nodeDriver.RunCommand(n, cmd, opts)
	if err != nil {
		return fmt.Errorf("failed to run secrets login for %s. cause: %v", secretType, err)
	}

	return nil
}

// GetDriver gets PX cluster and returns it
func (d *portworx) GetDriver() (*v1.StorageCluster, error) {
	// TODO: Need to implement it for Daemonset deployment as well, right now its only for StorageCluster
	stcList, err := pxOperator.ListStorageClusters(d.namespace)
	if err != nil {
		return nil, fmt.Errorf("Failed get StorageCluster list from namespace [%s], Err: %v", d.namespace, err)
	}

	stc, err := pxOperator.GetStorageCluster(stcList.Items[0].Name, stcList.Items[0].Namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get StorageCluster [%s] from namespace [%s], Err: %v", stcList.Items[0].Name, stcList.Items[0].Namespace, err.Error())
	}

	return stc, nil
}

// ValidateDriver takes endpointVersion and validates PX cluster and all components against it
func (d *portworx) ValidateDriver(endpointVersion string, autoUpdateComponents bool) error {
	// TODO: Currently this is only used for PX StorageCluster, need to implement some checks for DaemonSet as well in future
	log.Infof("Validate PX and all components for URL [%s]", endpointVersion)

	// Check if StorageCluster CRD is present, do StorageCluster validation, otherwise PX was deployed using daemonset, we skip this validation
	_, err := apiExtentions.GetCRD("storageclusters.core.libopenstorage.org", metav1.GetOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("failed to get StorageCluster CRD, Err: %v", err)
	} else if k8serrors.IsNotFound(err) {
		return nil
	}
	stcList, err := pxOperator.ListStorageClusters(d.namespace)
	if err != nil {
		return err
	}

	if len(stcList.Items) > 0 {
		stc := stcList.Items[0]
		var newStc *v1.StorageCluster
		if autoUpdateComponents {
			// Auto update components as PX Operator doesn't do it between K8S/OCP version upgrades
			updateStrategy := v1.OnceAutoUpdate
			stc.Spec.AutoUpdateComponents = &updateStrategy
			newStc, err = pxOperator.UpdateStorageCluster(&stc)
			if err != nil {
				return err
			}
		}

		k8sVersion, err := k8sCore.GetVersion()
		if err != nil {
			return err
		}

		imageList, err := optest.GetImagesFromVersionURL(endpointVersion, k8sVersion.String())
		if err != nil {
			return fmt.Errorf("failed to get images from version URL [%s], Err: %v", endpointVersion, err)
		}

		// Validate StorageCluster
		storageClusterValidateTimeout := time.Duration(len(node.GetStorageDriverNodes())*9) * time.Minute
		if err = optest.ValidateStorageCluster(imageList, newStc, storageClusterValidateTimeout, defaultRetryInterval, true); err != nil {
			return err
		}
	}

	log.Infof("Successfully validated PX and all components for URL [%s]", endpointVersion)
	return nil
}

// updateAndValidateStorageCluster updates StorageCluster based on a given new StorageCluster spec and validates PX and all its components
func (d *portworx) updateAndValidateStorageCluster(cluster *v1.StorageCluster, f func(*v1.StorageCluster) *v1.StorageCluster, specGenUrl string, autoUpdateComponents bool) (*v1.StorageCluster, error) {
	liveCluster, err := pxOperator.GetStorageCluster(cluster.Name, cluster.Namespace)
	if err != nil {
		return nil, err
	}

	newCluster := f(liveCluster)

	stc, err := pxOperator.UpdateStorageCluster(newCluster)
	if err != nil {
		return nil, err
	}

	k8sVersion, err := k8sCore.GetVersion()
	if err != nil {
		return nil, err
	}

	imageList, err := optest.GetImagesFromVersionURL(specGenUrl, k8sVersion.String())
	if err != nil {
		return nil, err
	}

	if autoUpdateComponents {
		if autoUpdateComponents {
			// Auto update components as PX Operator doesn't do it between K8S/OCP version upgrades
			updateStrategy := v1.OnceAutoUpdate
			stc.Spec.AutoUpdateComponents = &updateStrategy
			stc, err = pxOperator.UpdateStorageCluster(stc)
			if err != nil {
				return nil, err
			}
		}
	}
	storageClusterValidateTimeout := time.Duration(len(node.GetStorageDriverNodes())*9) * time.Minute
	if err = optest.ValidateStorageCluster(imageList, stc, storageClusterValidateTimeout, defaultRetryInterval, true); err != nil {
		return nil, err
	}
	return stc, nil
}

func (d *portworx) getPxctlPath(n node.Node) string {
	opts := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
		Sudo:            true,
	}
	out, err := d.nodeDriver.RunCommand(n, "which pxctl", opts)
	if err != nil {
		return "sudo /opt/pwx/bin/pxctl"
	}
	out = "sudo " + out
	return strings.TrimSpace(out)
}

// GetPxctlStatus returns the PX status using pxctl
func (d *portworx) GetPxctlStatus(n node.Node) (string, error) {
	opts := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
	}

	pxctlPath := d.getPxctlPath(n)

	// Create context
	if len(d.token) > 0 {
		_, err := d.nodeDriver.RunCommand(n, fmt.Sprintf("%s context create admin --token=%s", pxctlPath, d.token), opts)
		if err != nil {
			return "", fmt.Errorf("failed to create pxctl context. cause: %v", err)
		}
	}

	out, err := d.nodeDriver.RunCommand(n, fmt.Sprintf("%s -j status", pxctlPath), opts)
	if err != nil {
		return "", fmt.Errorf("failed to get pxctl status. cause: %v", err)
	}

	var data interface{}
	err = json.Unmarshal([]byte(out), &data)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal pxctl status. cause: %v", err)
	}

	// Delete context
	if len(d.token) > 0 {
		_, err := d.nodeDriver.RunCommand(n, fmt.Sprintf("%s context delete admin", pxctlPath), opts)
		if err != nil {
			return "", fmt.Errorf("failed to delete pxctl context. cause: %v", err)
		}
	}

	statusMap := data.(map[string]interface{})
	if status, ok := statusMap["status"]; ok {
		return status.(string), nil
	}
	return api.Status_STATUS_NONE.String(), nil
}

// GetPxctlCmdOutputConnectionOpts returns the command output run on the given node with ConnectionOpts and any error
func (d *portworx) GetPxctlCmdOutputConnectionOpts(n node.Node, command string, opts node.ConnectionOpts, retry bool) (string, error) {
	pxctlPath := d.getPxctlPath(n)
	// Create context
	if len(d.token) > 0 {
		_, err := d.nodeDriver.RunCommand(n, fmt.Sprintf("%s context create admin --token=%s", pxctlPath, d.token), opts)
		if err != nil {
			return "", fmt.Errorf("failed to create pxctl context. cause: %v", err)
		}
	}

	var (
		out string
		err error
	)

	cmd := fmt.Sprintf("%s %s", pxctlPath, command)
	if retry {
		out, err = d.nodeDriver.RunCommand(n, cmd, opts)
	} else {
		out, err = d.nodeDriver.RunCommandWithNoRetry(n, cmd, opts)
	}
	if err != nil {
		return "", fmt.Errorf("failed to get pxctl command output. cause: %v", err)
	}

	// Delete context
	if len(d.token) > 0 {
		_, err := d.nodeDriver.RunCommand(n, fmt.Sprintf("%s context delete admin", pxctlPath), opts)
		if err != nil {
			return "", fmt.Errorf("failed to delete pxctl context. cause: %v", err)
		}
	}

	return out, nil
}

// GetPxctlCmdOutput returns the command output run on the given node and any error
func (d *portworx) GetPxctlCmdOutput(n node.Node, command string) (string, error) {
	opts := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
	}

	return d.GetPxctlCmdOutputConnectionOpts(n, command, opts, true)
}

func doesConditionMatch(expectedMetricValue float64, conditionExpression *apapi.LabelSelectorRequirement) bool {
	condExprValue, _ := strconv.ParseFloat(conditionExpression.Values[0], 64)
	return expectedMetricValue < condExprValue && conditionExpression.Operator == apapi.LabelSelectorOpLt ||
		expectedMetricValue > condExprValue && conditionExpression.Operator == apapi.LabelSelectorOpGt
}

func parseMaxSize(maxSize string) (uint64, error, error) {
	mSize, err := strconv.ParseUint(maxSize, 10, 64)
	if err != nil {
		a, parseErr := resource.ParseQuantity(maxSize)
		if parseErr != nil {

			return 0, err, parseErr
		}
		mSize = uint64(a.Value())
	}
	return mSize, nil, nil
}

// getRestPort gets the service port for rest api, required when using service endpoint
func (d *portworx) getRestPort() (int32, error) {
	svc, err := k8sCore.GetService(schedops.PXServiceName, d.namespace)
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
func (d *portworx) getRestContainerPort() (int32, error) {
	svc, err := k8sCore.GetService(schedops.PXServiceName, d.namespace)
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
func (d *portworx) getSDKPort() (int32, error) {
	svc, err := k8sCore.GetService(schedops.PXServiceName, d.namespace)
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
func (d *portworx) getSDKContainerPort() (int32, error) {
	svc, err := k8sCore.GetService(schedops.PXServiceName, d.namespace)
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

// GetNodeStats returns the node stats of the given node and an error if any
func (d *portworx) GetNodeStats(n node.Node) (map[string]map[string]int, error) {
	opts := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
	}

	pxctlPath := d.getPxctlPath(n)

	// Create context
	if len(d.token) > 0 {
		_, err := d.nodeDriver.RunCommand(n, fmt.Sprintf("%s context create admin --token=%s", pxctlPath, d.token), opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create pxctl context. cause: %v", err)
		}
	}

	out, err := d.nodeDriver.RunCommand(n, fmt.Sprintf("%s sv dump --nodestats -j", pxctlPath), opts)
	if err != nil {
		return nil, fmt.Errorf("failed to get pxctl status. cause: %v", err)
	}

	var data interface{}
	err = json.Unmarshal([]byte(out), &data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal pxctl status. cause: %v", err)
	}

	// Delete context
	if len(d.token) > 0 {
		_, err := d.nodeDriver.RunCommand(n, fmt.Sprintf("%s context delete admin", pxctlPath), opts)
		if err != nil {
			return nil, fmt.Errorf("failed to delete pxctl context. cause: %v", err)
		}
	}

	nodeStats := data.(map[string]interface{})
	deleted := 0
	pending := 0
	skipped := 0

	if value, ok := nodeStats["relaxed_reclaim_stats"].(map[string]interface{}); ok {

		if p, ok := value["pending"]; ok {
			pending = int(p.(float64))
		}

		if d, ok := value["deleted"]; ok {
			deleted = int(d.(float64))
		}

		if s, ok := value["skipped"]; ok {
			skipped = int(s.(float64))
		}

	}

	var nodeStatsMap = map[string]map[string]int{}
	nodeStatsMap[n.Name] = map[string]int{}
	nodeStatsMap[n.Name]["deleted"] = deleted
	nodeStatsMap[n.Name]["pending"] = pending
	nodeStatsMap[n.Name]["skipped"] = skipped

	return nodeStatsMap, nil
}

// GetTrashCanVolumeIds returns the volume ids in the trashcan and an error if any
func (d *portworx) GetTrashCanVolumeIds(n node.Node) ([]string, error) {
	opts := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
	}

	pxctlPath := d.getPxctlPath(n)

	// Create context
	if len(d.token) > 0 {
		_, err := d.nodeDriver.RunCommand(n, fmt.Sprintf("%s context create admin --token=%s", pxctlPath, d.token), opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create pxctl context. cause: %v", err)
		}
	}

	out, err := d.nodeDriver.RunCommand(n, fmt.Sprintf("%s v l --trashcan -j", pxctlPath), opts)
	if err != nil {
		return nil, fmt.Errorf("failed to get pxctl status. cause: %v", err)
	}
	log.Info(out)

	var data interface{}
	err = json.Unmarshal([]byte(out), &data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal pxctl status. cause: %v", err)
	}

	// Delete context
	if len(d.token) > 0 {
		_, err := d.nodeDriver.RunCommand(n, fmt.Sprintf("%s context delete admin", pxctlPath), opts)
		if err != nil {
			return nil, fmt.Errorf("failed to delete pxctl context. cause: %v", err)
		}
	}

	res := data.([]interface{})

	trashcanVols := make([]string, 50)

	for _, v := range res {
		var tp map[string]interface{} = v.(map[string]interface{})
		str := fmt.Sprintf("%v", tp["id"])
		trashcanVols = append(trashcanVols, strings.Trim(str, " "))

	}

	log.Infof("trash vols: %v", trashcanVols)

	return trashcanVols, nil
}

// GetNodePureVolumeAttachedCountMap return Map of nodeName and number of pure volume attached on that node
func (d *portworx) GetNodePureVolumeAttachedCountMap() (map[string]int, error) {
	// nodePureVolAttachedCountMap maintains count of attached volume
	nodePureVolAttachedCountMap := make(map[string]int)
	// pureLabelMap is a filter to be used for listing pure volumes
	pureLabelMap := make(map[string]string)
	// nodeIPMap maintains map of IP to node names
	nodeIPMap := make(map[string]string)

	volDriver := d.getVolDriver()
	pureLabelMap[pureKey] = pureBlockValue

	// Initializing the nodePureVolAttachedCountMap
	for key, n := range node.GetNodesByName() {
		nodePureVolAttachedCountMap[key] = 0
		for _, ip := range n.Addresses {
			nodeIPMap[ip] = n.Name
		}
	}

	volumes, err := volDriver.EnumerateWithFilters(d.getContext(), &api.SdkVolumeEnumerateWithFiltersRequest{
		Labels: pureLabelMap,
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to get pure volume list, Err: %v", err)
	}

	for _, volumeID := range volumes.GetVolumeIds() {
		volumeInspectResponse, err := volDriver.Inspect(d.getContext(), &api.SdkVolumeInspectRequest{VolumeId: volumeID})
		if err != nil {
			return nil, err
		}
		pxVol := volumeInspectResponse.Volume
		if pxVol.State == api.VolumeState_VOLUME_STATE_ATTACHED {
			if nodeName, ok := nodeIPMap[pxVol.AttachedOn]; ok {
				nodePureVolAttachedCountMap[nodeName]++
			}

		}
	}
	return nodePureVolAttachedCountMap, nil
}

func (d *portworx) RecoverNode(n *node.Node) error {
	if err := k8sCore.RemoveLabelOnNode(n.Name, schedops.PXServiceLabelKey); err != nil {
		return &ErrFailedToRecoverDriver{
			Node:  *n,
			Cause: fmt.Sprintf("Failed to set label on node [%s], Err: %v", n.Name, err),
		}
	}

	if err := k8sCore.RemoveLabelOnNode(n.Name, schedops.PXEnabledLabelKey); err != nil {
		return &ErrFailedToRecoverDriver{
			Node:  *n,
			Cause: fmt.Sprintf("Failed to set label on node [%s], Err: %v", n.Name, err),
		}
	}

	if err := d.RestartDriver(*n, nil); err != nil {
		return &ErrFailedToRecoverDriver{
			Node:  *n,
			Cause: err.Error(),
		}

	}
	log.Infof("Waiting for PX to trigger restart on node [%s]", n.Name)
	time.Sleep(1 * time.Minute)

	stNode, err := d.GetDriverNode(n)
	if err != nil {
		log.Errorf("failed to get PX storage node [%s]", n.Name) // NOTE: Why are we not returning error here?
	} else {
		if stNode.Status == api.Status_STATUS_MAINTENANCE {
			if err = d.ExitMaintenance(*n); err != nil {
				return &ErrFailedToRecoverDriver{
					Node:  *n,
					Cause: err.Error(),
				}
			}
		}
	}

	if err := k8sCore.UnCordonNode(n.Name, defaultTimeout, defaultRetryInterval); err != nil {
		return &ErrFailedToRecoverDriver{
			Node:  *n,
			Cause: fmt.Sprintf("Failed to uncordon node [%s], Err: %v", n.Name, err),
		}
	}
	return nil
}

// AddBlockDrives add drives to the node using PXCTL
func (d *portworx) AddBlockDrives(n *node.Node, drivePath []string) error {
	systemOpts := node.SystemctlOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         startDriverTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		},
		Action: "start",
	}
	log.Infof("Getting available block drives on node [%s]", n.Name)
	blockDrives, err := d.nodeDriver.GetBlockDrives(*n, systemOpts)

	if err != nil {
		return err
	}

	eligibleDrives := make(map[string]*node.BlockDrive)

	for _, drv := range blockDrives {
		if !strings.Contains(drv.Path, "pxd") && drv.MountPoint == "" && drv.FSType == "" && drv.Type == "disk" {
			isPartitioned, err := isDiskPartitioned(*n, drv.Path, d)
			if err != nil {
				return err
			}
			if !isPartitioned {
				eligibleDrives[drv.Path] = drv
			}
		}
	}

	if len(blockDrives) == 0 || len(eligibleDrives) == 0 {
		return fmt.Errorf("no block drives available to add")
	}

	if drivePath == nil || len(drivePath) == 0 {
		log.Infof("Adding all the available drives")
		for _, drv := range eligibleDrives {
			if err := addDrive(*n, drv.Path, -1, d); err != nil {
				return err
			}
			if err := waitForAddDriveToComplete(*n, drv.Path, d); err != nil {
				return err
			}
		}
	} else {
		for _, drvPath := range drivePath {
			drv, ok := eligibleDrives[drvPath]
			if ok {
				if err := addDrive(*n, drv.Path, -1, d); err != nil {
					return err
				}
				if err := waitForAddDriveToComplete(*n, drv.Path, d); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("block drive path [%s] is not eligible or does not exist to perform drive add", drvPath)
			}
		}
	}
	return nil
}

func isDiskPartitioned(n node.Node, drivePath string, d *portworx) (bool, error) {

	out, err := d.nodeDriver.RunCommandWithNoRetry(n, fmt.Sprintf("ls -l %s*", drivePath), node.ConnectionOpts{
		Timeout:         crashDriverTimeout,
		TimeBeforeRetry: defaultRetryInterval,
	})
	if err != nil {
		return false, fmt.Errorf("error checking drive partition for [%s] in node [%s], Err: %v", drivePath, n.Name, err)
	}

	drvs := strings.Split(strings.TrimSpace(out), "\n")

	if len(drvs) > 1 {
		return true, nil
	}
	return false, nil

}

// GetPoolDrives returns the map of poolID and drive name
func (d *portworx) GetPoolDrives(n *node.Node) (map[string][]string, error) {

	poolDrives := make(map[string][]string, 0)

	connectionOps := node.ConnectionOpts{
		IgnoreError:     false,
		TimeBeforeRetry: defaultRetryInterval,
		Timeout:         defaultTimeout,
		Sudo:            true,
	}
	output, err := d.nodeDriver.RunCommand(*n, "pxctl status", connectionOps)
	if err != nil {
		return poolDrives, err
	}
	log.Infof("got output: %s", output)
	re := regexp.MustCompile(`\b\d+:\d+\b.*`)
	matches := re.FindAllString(output, -1)

	for _, match := range matches {
		log.Debugf("Extracting pool details from [%s]", match)
		pVals := make([]string, 0)
		tempVals := strings.Fields(match)
		for _, tv := range tempVals {
			if strings.Contains(tv, ":") || strings.Contains(tv, "/") {
				pVals = append(pVals, tv)
			}
		}

		if len(pVals) >= 2 {
			tempPoolId := pVals[0]
			poolId := strings.Split(tempPoolId, ":")[0]
			drvPath := pVals[1]

			poolDrives[poolId] = append(poolDrives[poolId], drvPath)
		}

	}
	return poolDrives, nil
}

// AddCloudDrive add cloud drives to the node using PXCTL
func (d *portworx) AddCloudDrive(n *node.Node, deviceSpec string, poolID int32) error {
	log.Infof("Adding Cloud drive on node [%s] with spec [%s] on pool ID [%d]", n.Name, deviceSpec, poolID)
	return addDrive(*n, deviceSpec, poolID, d)
}

func addDrive(n node.Node, drivePath string, poolID int32, d *portworx) error {
	driveAddFlag := fmt.Sprintf("-d %s", drivePath)

	if strings.Contains(drivePath, "size") {
		driveAddFlag = fmt.Sprintf("-s %s", drivePath)
		if poolID != -1 {
			driveAddFlag = fmt.Sprintf("%s -p %d", driveAddFlag, poolID)
		} else {
			stringMatch := false
			matchType := []string{"metadata", "journal"}
			for _, word := range matchType {
				re := regexp.MustCompile(fmt.Sprintf(".*--%s", word))
				match := re.MatchString(drivePath)
				if match {
					stringMatch = true
				}
			}
			if !stringMatch {
				driveAddFlag = fmt.Sprintf("%s %s", driveAddFlag, "--newpool")
			}

		}
	}
	log.Infof("adding cloud drive with params [%v]", driveAddFlag)
	out, err := d.nodeDriver.RunCommandWithNoRetry(n, fmt.Sprintf(pxctlDriveAddStart, d.getPxctlPath(n), driveAddFlag), node.ConnectionOpts{
		Timeout:         crashDriverTimeout,
		TimeBeforeRetry: defaultRetryInterval,
	})
	if err != nil {
		return fmt.Errorf("error when adding drive [%s] in node [%s] using PXCTL, Err: %v", drivePath, n.Name, err)
	}
	output := []byte(out)
	var addDriveStatus statusJSON
	err = json.Unmarshal(output, &addDriveStatus)
	if err != nil {
		return fmt.Errorf("failed to unmarshal adding drive, Err: %v", err)
	}
	if &addDriveStatus == nil {
		return fmt.Errorf("failed to add drive [%s] on node [%s]", drivePath, n.Name)
	}

	if !strings.Contains(addDriveStatus.Status, driveAddSuccessStatus) && !strings.Contains(addDriveStatus.Status, metadataAddSuccessStatus) && !strings.Contains(addDriveStatus.Status, journalAddSuccessStatus) {
		return fmt.Errorf("failed to add drive [%s] on node [%s], AddDrive Status: %+v", drivePath, n.Name, addDriveStatus)

	}
	log.InfoD("Added drive [%s] to node [%s] successfully", drivePath, n.Name)
	return nil
}

func waitForAddDriveToComplete(n node.Node, drivePath string, d *portworx) error {
	driveAddFlag := fmt.Sprintf("-d %s", drivePath)
	var addDriveStatus statusJSON

	if strings.Contains(drivePath, "size") {
		driveAddFlag = fmt.Sprintf("-s %s", drivePath)
	}

	f := func() (interface{}, bool, error) {
		out, err := d.nodeDriver.RunCommandWithNoRetry(n, fmt.Sprintf(pxctlDriveAddStatus, d.getPxctlPath(n), driveAddFlag), node.ConnectionOpts{
			Timeout:         crashDriverTimeout,
			TimeBeforeRetry: defaultRetryInterval,
		})
		if err != nil {
			if strings.Contains(err.Error(), driveExitsStatus) {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("failed to add drive status for path [%s] on node [%s] using PXCTL, Err: %v", drivePath, n.Name, err)
		}
		output := []byte(out)

		err = json.Unmarshal(output, &addDriveStatus)
		if err != nil {
			return nil, true, fmt.Errorf("failed to unmarshal add drive status, Err: %v", err)
		}
		if &addDriveStatus == nil {
			return nil, true, fmt.Errorf("failed to get add drive status for path [%s] on node [%s]", drivePath, n.Name)
		}
		log.Infof("Current add drive for path [%s] status: %+v", drivePath, addDriveStatus)
		if strings.Contains(addDriveStatus.Status, "Drive add: Storage rebalance complete") || strings.Contains(addDriveStatus.Status, "Device already exists") {
			return nil, false, nil
		}
		return nil, true, nil
	}

	_, err := task.DoRetryWithTimeout(f, 3*time.Hour, 2*time.Minute)
	if err == nil {
		log.Infof("Added drive [%s] to node [%s] successfully", drivePath, n.Name)
		return nil
	}
	return fmt.Errorf("failed to  add drive for path [%s] on node [%s], Status: %+v, Err: %v", drivePath, n.Name, addDriveStatus, err)
}

// GetPoolsUsedSize returns map of pool id and current used size
func (d *portworx) GetPoolsUsedSize(n *node.Node) (map[string]string, error) {
	cmd := fmt.Sprintf("%s sv pool show -j | grep -e uuid -e '\"Used\"'", d.getPxctlPath(*n))
	log.Infof("Running command [%s] on node [%s]", cmd, n.Name)

	t := func() (interface{}, bool, error) {
		out, err := d.nodeDriver.RunCommandWithNoRetry(*n, cmd, node.ConnectionOpts{
			Timeout:         2 * time.Minute,
			TimeBeforeRetry: 10 * time.Second,
		})
		if err != nil {
			return "", true, err
		}
		return out, false, nil
	}

	out, err := task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second)

	if err != nil {
		return nil, err
	}
	outLines := strings.Split(out.(string), "\n")

	poolsData := make(map[string]string)
	var poolId string
	var usedSize string
	for _, l := range outLines {
		line := strings.Trim(l, " ")
		line = strings.Trim(line, ",")
		if strings.Contains(line, "uuid") {
			poolId = strings.Split(line, ":")[1]
			poolId = strings.Trim(poolId, " ")
			poolId = strings.ReplaceAll(poolId, "\"", "")
		}
		if strings.Contains(line, "Used") {
			usedSize = strings.Split(line, ":")[1]
			usedSize = strings.Trim(usedSize, " ")
			usedSize = strings.ReplaceAll(usedSize, "\"", "")
		}
		if poolId != "" && usedSize != "" {
			poolsData[poolId] = usedSize
			poolId = ""
			usedSize = ""
		}
	}
	return poolsData, nil
}

// GetJournalDevicePath returns journal device path in the given node
func (d *portworx) GetJournalDevicePath(n *node.Node) (string, error) {
	for _, addr := range n.Addresses {
		if err := d.testAndSetEndpointUsingNodeIP(addr); err != nil {
			log.Warnf("testAndSetEndpoint failed for %v: %v", addr, err)
			continue
		}
	}

	t := func() (interface{}, bool, error) {
		storageSpec, err := d.GetStorageSpec()
		if err != nil {
			return "", true, err
		}
		if storageSpec.GetJournalDev() == "auto" {
			return "", true, fmt.Errorf("journalDev not yet updated")
		}

		return storageSpec.GetJournalDev(), false, nil
	}

	out, err := task.DoRetryWithTimeout(t, upgradeTimeout, defaultRetryInterval)
	if err != nil {
		return "", err
	}
	return out.(string), nil

}

// IsIOsInProgressForTheVolume checks if IOs are happening in the given volume
func (d *portworx) IsIOsInProgressForTheVolume(n *node.Node, volumeNameOrID string) (bool, error) {

	log.Infof("Got vol-id [%s] for checking IOs", volumeNameOrID)
	cmd := fmt.Sprintf("%s v i %s| grep -e 'IOs in progress'", d.getPxctlPath(*n), volumeNameOrID)

	out, err := d.nodeDriver.RunCommandWithNoRetry(*n, cmd, node.ConnectionOpts{
		Timeout:         2 * time.Minute,
		TimeBeforeRetry: 10 * time.Second,
	})

	if err != nil {
		return false, err
	}
	line := strings.Trim(out, " ")
	data := strings.Split(line, ":")[1]
	data = strings.Trim(data, "\n")
	data = strings.Trim(data, " ")
	val, err := strconv.Atoi(data)
	if err != nil {
		return false, err
	}
	if val > 0 {
		return true, nil
	}
	return false, nil
}

// GetRebalanceJobs returns the list of rebalance jobs
func (d *portworx) GetRebalanceJobs() ([]*api.StorageRebalanceJob, error) {
	jobsResp, err := d.storagePoolManager.EnumerateRebalanceJobs(d.getContext(), &api.SdkEnumerateRebalanceJobsRequest{})
	if err != nil {
		return nil, err
	}
	return jobsResp.GetJobs(), nil
}

// GetRebalanceJobStatus returns the rebalance jobs response
func (d *portworx) GetRebalanceJobStatus(jobID string) (*api.SdkGetRebalanceJobStatusResponse, error) {
	return d.storagePoolManager.GetRebalanceJobStatus(d.getContext(), &api.SdkGetRebalanceJobStatusRequest{Id: jobID})
}

func init() {
	torpedovolume.Register(DriverName, provisioners, &portworx{})
	torpedovolume.Register(PureDriverName, csiProvisionerOnly, &pure{portworx: portworx{}})
}

// UpdatePoolLabels updates the pool label for a particular pool id
func (d *portworx) UpdatePoolLabels(n node.Node, poolID string, labels map[string]string) error {

	labelStrings := make([]string, 0)
	for k, v := range labels {
		labelString := fmt.Sprintf("%s=%s", k, v)
		labelStrings = append(labelStrings, labelString)
	}

	labelsStr := strings.Join(labelStrings, ",")
	fmt.Printf("label string is %s\n", labelsStr)
	cmd := fmt.Sprintf("%s sv pool update -u %s --labels=%s", d.getPxctlPath(n), poolID, labelsStr)
	fmt.Printf("cmd: %s\n", cmd)
	_, err := d.nodeDriver.RunCommandWithNoRetry(n, cmd, node.ConnectionOpts{
		Timeout:         2 * time.Minute,
		TimeBeforeRetry: 10 * time.Second,
	})
	if err != nil {
		return err
	}
	return nil

}

// DeleteSnapshotsForVolumes deletes snapshots for the specified volumes in google cloud
func (d *portworx) DeleteSnapshotsForVolumes(volumeNames []string, clusterProviderCredential string) error {
	log.Warnf("DeleteSnapshotsForVolumes function has not been implemented for volume driver - %s", d.String())
	return nil
}

// GetPoolLabelValue returns values of labels
func (d *portworx) GetPoolLabelValue(poolUUID string, label string) (string, error) {
	/* e.x
	1) d.GetPoolLabelValue(poolUUID, "iopriority")
	2) d.GetPoolLabelValue(poolUUID, "beta.kubernetes.io/arch")
	3) d.GetPoolLabelValue(poolUUID, "medium")
	*/
	var PropertyMatch string
	PropertyMatch = ""
	pools, err := d.ListStoragePools(metav1.LabelSelector{})
	if err != nil {
		return "", err
	}

	for _, eachPool := range pools {
		if eachPool.Uuid == poolUUID {
			PropertyMatch = eachPool.Labels[label]
			break
		}
	}
	if PropertyMatch == "" {
		return "", fmt.Errorf(fmt.Sprintf("Failed to Get [%s] for PoolUUID [%v]", label, poolUUID))
	}
	return PropertyMatch, nil
}

// IsNodeInMaintenance returns true if Node in Maintenance
func (d *portworx) IsNodeInMaintenance(n node.Node) (bool, error) {
	stNode, err := d.GetDriverNode(&n)
	if err != nil {
		return false, err
	}

	if stNode.Status == api.Status_STATUS_MAINTENANCE {
		return true, nil
	}

	return false, nil
}

// IsNodeOutOfMaintenance returns true if Node in out of Maintenance
func (d *portworx) IsNodeOutOfMaintenance(n node.Node) (bool, error) {
	stNode, err := d.GetDriverNode(&n)
	if err != nil {
		return false, err
	}

	if stNode.Status == api.Status_STATUS_OK {
		return true, nil
	}

	return false, nil
}

// GetAlertsUsingResourceTypeBySeverity returns all the alerts by resource type filtered by severity
func (d *portworx) GetAlertsUsingResourceTypeBySeverity(resourceType api.ResourceType, severity api.SeverityType) (*api.SdkAlertsEnumerateWithFiltersResponse, error) {
	/*
		resourceType : RESOURCE_TYPE_NONE | RESOURCE_TYPE_VOLUME | RESOURCE_TYPE_NODE | RESOURCE_TYPE_CLUSTER | RESOURCE_TYPE_DRIVE | RESOURCE_TYPE_POOL
		SeverityType : SEVERITY_TYPE_NONE | SEVERITY_TYPE_ALARM | SEVERITY_TYPE_WARNING | SEVERITY_TYPE_NOTIFY
		e.x :
			var resourceType api.ResourceType
			var severity api.SeverityType
			resourceType = api.ResourceType_RESOURCE_TYPE_POOL
			severity = api.SeverityType_SEVERITY_TYPE_ALARM
			alerts, err := Inst().V.GetAlertsUsingResourceTypeBySeverity(resourceType, severity)
	*/
	alerts, err := d.alertsManager.EnumerateWithFilters(d.getContext(), &api.SdkAlertsEnumerateWithFiltersRequest{
		Queries: []*api.SdkAlertsQuery{
			{
				Query: &api.SdkAlertsQuery_ResourceTypeQuery{
					ResourceTypeQuery: &api.SdkAlertsResourceTypeQuery{
						ResourceType: resourceType,
					},
				},
				Opts: []*api.SdkAlertsOption{
					{Opt: &api.SdkAlertsOption_MinSeverityType{
						MinSeverityType: severity,
					}},
				},
			},
		},
	})
	if err != nil {
		return nil, err
	}
	alertsResp, err := alerts.Recv()
	if err != nil {
		return nil, err
	}

	return alertsResp, nil
}

// GetAlertsUsingResourceTypeByTime returns all the alerts by resource type filtered by starttime and endtime
func (d *portworx) GetAlertsUsingResourceTypeByTime(resourceType api.ResourceType, startTime time.Time, endTime time.Time) (*api.SdkAlertsEnumerateWithFiltersResponse, error) {
	/*
		resourceType : RESOURCE_TYPE_NONE | RESOURCE_TYPE_VOLUME | RESOURCE_TYPE_NODE | RESOURCE_TYPE_CLUSTER | RESOURCE_TYPE_DRIVE | RESOURCE_TYPE_POOL
		e.x:
			var resourceType api.ResourceType
			var startTime time.Time
			var endTime time.Time

			startTime = time.Now()
			endTime = time.Now()
			resourceType = api.ResourceType_RESOURCE_TYPE_POOL
			alerts, err := Inst().V.GetAlertsUsingResourceTypeByTime(resourceType, startTime, endTime)
	*/

	alerts, err := d.alertsManager.EnumerateWithFilters(d.getContext(), &api.SdkAlertsEnumerateWithFiltersRequest{
		Queries: []*api.SdkAlertsQuery{
			{
				Query: &api.SdkAlertsQuery_ResourceTypeQuery{
					ResourceTypeQuery: &api.SdkAlertsResourceTypeQuery{
						ResourceType: resourceType,
					},
				},
				Opts: []*api.SdkAlertsOption{
					{Opt: &api.SdkAlertsOption_TimeSpan{
						TimeSpan: &api.SdkAlertsTimeSpan{
							StartTime: timestamppb.New(startTime.UTC()),
							EndTime:   timestamppb.New(endTime.UTC()),
						},
					}},
				},
			},
		},
	})
	if err != nil {
		return nil, err
	}

	alertsResp, err := alerts.Recv()
	if err != nil {
		return nil, err
	}

	return alertsResp, nil
}

func (d *portworx) IsPxReadyOnNode(n node.Node) bool {
	return d.schedOps.IsPXReadyOnNode(n)
}

// EnableSkinnySnap Enables skinnysnap on the cluster
func (d *portworx) EnableSkinnySnap() error {
	for _, eachNode := range node.GetNodes() {
		cmd := fmt.Sprintf("echo Y | %s --skinnysnap on", pxctlClusterOptionsUpdate)
		_, err := d.nodeDriver.RunCommandWithNoRetry(
			eachNode,
			cmd,
			node.ConnectionOpts{
				Timeout:         skinnySnapRetryInterval,
				TimeBeforeRetry: defaultRetryInterval,
			})
		if err != nil {
			return fmt.Errorf("Failed to enable skinny Snap on Cluster, Err: %v", err)
		}
		break
	}
	return nil
}

// UpdateSkinnySnapReplNum update skinnysnap Repl factor
func (d *portworx) UpdateSkinnySnapReplNum(repl string) error {
	for _, eachNode := range node.GetNodes() {
		cmd := fmt.Sprintf("echo Y | %s --skinnysnap-num-repls %s", pxctlClusterOptionsUpdate, repl)
		_, err := d.nodeDriver.RunCommandWithNoRetry(
			eachNode,
			cmd,
			node.ConnectionOpts{
				Timeout:         skinnySnapRetryInterval,
				TimeBeforeRetry: defaultRetryInterval,
			})
		if err != nil {
			return fmt.Errorf("failed to Update SkinnySnap Repl Factor, Err: %v", err)
		}
		break
	}
	return nil
}
