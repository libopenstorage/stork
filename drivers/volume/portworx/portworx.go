package portworx

import (
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/openstorage/api/client"
	clusterclient "github.com/libopenstorage/openstorage/api/client/cluster"
	volumeclient "github.com/libopenstorage/openstorage/api/client/volume"
	"github.com/libopenstorage/openstorage/api/spec"
	"github.com/libopenstorage/openstorage/cluster"
	"github.com/libopenstorage/openstorage/volume"
	"github.com/pborman/uuid"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// DriverName is the name of the portworx driver implementation
	DriverName              = "pxd"
	pxdClientSchedUserAgent = "pxd-sched"
	pxdRestPort             = 9001
	pxVersionLabel          = "PX Version"
	maintenanceOpRetries    = 3
	enterMaintenancePath    = "/entermaintenance"
	exitMaintenancePath     = "/exitmaintenance"
	pxSystemdServiceName    = "portworx.service"
	storageStatusUp         = "Up"
	tokenKey                = "token"
	clusterIP               = "ip"
	clusterPort             = "port"
	remoteKubeConfigPath    = "/tmp/kubeconfig"
)

const (
	defaultRetryInterval             = 10 * time.Second
	maintenanceOpTimeout             = 1 * time.Minute
	maintenanceWaitTimeout           = 2 * time.Minute
	inspectVolumeTimeout             = 10 * time.Second
	inspectVolumeRetryInterval       = 2 * time.Second
	validateDeleteVolumeTimeout      = 3 * time.Minute
	validateReplicationUpdateTimeout = 10 * time.Minute
	validateClusterStartTimeout      = 2 * time.Minute
	validateNodeStartTimeout         = 3 * time.Minute
	validatePXStartTimeout           = 2 * time.Minute
	validateNodeStopTimeout          = 2 * time.Minute
	stopDriverTimeout                = 5 * time.Minute
	crashDriverTimeout               = 2 * time.Minute
	startDriverTimeout               = 2 * time.Minute
	upgradeTimeout                   = 10 * time.Minute
	upgradeRetryInterval             = 30 * time.Second
	waitVolDriverToCrash             = 1 * time.Minute
)

type portworx struct {
	clusterManager  cluster.Cluster
	volDriver       volume.VolumeDriver
	schedOps        schedops.Driver
	nodeDriver      node.Driver
	refreshEndpoint bool
}

func (d *portworx) String() string {
	return DriverName
}

func (d *portworx) Init(sched string, nodeDriver string) error {
	logrus.Infof("Using the Portworx volume driver under scheduler: %v", sched)
	var err error
	if d.nodeDriver, err = node.Get(nodeDriver); err != nil {
		return err
	}

	if d.schedOps, err = schedops.Get(sched); err != nil {
		return fmt.Errorf("failed to get scheduler operator for portworx. Err: %v", err)
	}

	if err = d.setDriver(); err != nil {
		return err
	}

	cluster, err := d.getClusterOnStart()
	if err != nil {
		return err
	}

	if len(cluster.Nodes) == 0 {
		return fmt.Errorf("cluster inspect returned empty nodes")
	}

	err = d.updateNodes(cluster.Nodes)
	if err != nil {
		return err
	}

	for _, n := range node.GetStorageDriverNodes() {
		if err := d.WaitDriverUpOnNode(n); err != nil {
			return err
		}
	}

	logrus.Infof("The following Portworx nodes are in the cluster:")
	for _, n := range cluster.Nodes {
		logrus.Infof(
			"Node UID: %v Node IP: %v Node Status: %v",
			n.Id,
			n.DataIp,
			n.Status,
		)
	}

	return nil
}

func (d *portworx) updateNodes(pxNodes []api.Node) error {
	for _, n := range node.GetWorkerNodes() {
		if err := d.updateNode(n, pxNodes); err != nil {
			return err
		}
	}

	return nil
}

func (d *portworx) updateNode(n node.Node, pxNodes []api.Node) error {
	isPX, err := d.schedOps.IsPXEnabled(n)
	if err != nil {
		return err
	}
	// No need to check in pxNodes if px is not installed
	if !isPX {
		return nil
	}
	for _, address := range n.Addresses {
		for _, pxNode := range pxNodes {
			if address == pxNode.DataIp || address == pxNode.MgmtIp || n.Name == pxNode.Hostname {
				n.VolDriverNodeID = pxNode.Id
				n.IsStorageDriverInstalled = isPX
				node.UpdateNode(n)
				return nil
			}
		}
	}

	// Return error where PX is not explicitly disabled but was not found installed
	return fmt.Errorf("failed to find px node for node: %v PX nodes: %v", n, pxNodes)
}

func (d *portworx) CleanupVolume(name string) error {
	locator := &api.VolumeLocator{}

	volumes, err := d.getVolDriver().Enumerate(locator, nil)
	if err != nil {
		return err
	}

	for _, v := range volumes {
		if v.Locator.Name == name {
			// First unmount this volume at all mount paths...
			for _, path := range v.AttachPath {
				if err = d.getVolDriver().Unmount(v.Id, path, nil); err != nil {
					err = fmt.Errorf(
						"error while unmounting %v at %v because of: %v",
						v.Id,
						path,
						err,
					)
					logrus.Infof("%v", err)
					return err
				}
			}

			if err = d.getVolDriver().Detach(v.Id, nil); err != nil {
				err = fmt.Errorf(
					"error while detaching %v because of: %v",
					v.Id,
					err,
				)
				logrus.Infof("%v", err)
				return err
			}

			if err = d.getVolDriver().Delete(v.Id); err != nil {
				err = fmt.Errorf(
					"error while deleting %v because of: %v",
					v.Id,
					err,
				)
				logrus.Infof("%v", err)
				return err
			}

			logrus.Infof("successfully removed Portworx volume %v", name)

			return nil
		}
	}

	return nil
}

func (d *portworx) GetStorageDevices(n node.Node) ([]string, error) {
	const (
		storageInfoKey = "STORAGE-INFO"
		resourcesKey   = "Resources"
		pathKey        = "path"
	)
	pxNode, err := d.getClusterManager().Inspect(n.VolDriverNodeID)
	if err != nil {
		return nil, err
	}

	storageInfo, ok := pxNode.NodeData[storageInfoKey]
	if !ok {
		return nil, fmt.Errorf("Unable to find storage info for node: %v", n.Name)
	}
	storageInfoMap := storageInfo.(map[string]interface{})

	resourcesMapIntf, ok := storageInfoMap[resourcesKey]
	if !ok || resourcesMapIntf == nil {
		return nil, fmt.Errorf("Unable to find resource info for node: %v", n.Name)
	}
	resourcesMap := resourcesMapIntf.(map[string]interface{})

	devPaths := []string{}
	for _, v := range resourcesMap {
		resource := v.(map[string]interface{})
		path, _ := resource[pathKey]
		if path == "" {
			continue
		}
		devPaths = append(devPaths, path.(string))
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
		apiNode, err := d.getClusterManager().Inspect(n.VolDriverNodeID)
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
		apiNode, err := d.getClusterManager().Inspect(n.VolDriverNodeID)
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

func (d *portworx) ValidateCreateVolume(name string, params map[string]string) error {
	t := func() (interface{}, bool, error) {
		vols, err := d.getVolDriver().Inspect([]string{name})
		if err != nil {
			return nil, true, err
		}

		if len(vols) != 1 {
			errCause := fmt.Sprintf("Volume: %s inspect result has invalid length. Expected:1 Actual:%v", name, len(vols))
			logrus.Warnf(errCause)
			return nil, true, &ErrFailedToInspectVolume{
				ID:    name,
				Cause: errCause,
			}
		}

		return vols[0], false, nil
	}

	out, err := task.DoRetryWithTimeout(t, inspectVolumeTimeout, inspectVolumeRetryInterval)
	if err != nil {
		return &ErrFailedToInspectVolume{
			ID:    name,
			Cause: fmt.Sprintf("Volume inspect returned err: %v", err),
		}
	}

	vol := out.(*api.Volume)

	// Status
	if vol.Status != api.VolumeStatus_VOLUME_STATUS_UP {
		return &ErrFailedToInspectVolume{
			ID: name,
			Cause: fmt.Sprintf("Volume has invalid status. Expected:%v Actual:%v",
				api.VolumeStatus_VOLUME_STATUS_UP, vol.Status),
		}
	}

	// State
	if vol.State == api.VolumeState_VOLUME_STATE_ERROR || vol.State == api.VolumeState_VOLUME_STATE_DELETED {
		return &ErrFailedToInspectVolume{
			ID:    name,
			Cause: fmt.Sprintf("Volume has invalid state. Actual:%v", vol.State),
		}
	}

	// if the volume is a clone or a snap, validate it's parent
	if vol.IsSnapshot() || vol.IsClone() {
		parent, err := d.getVolDriver().Inspect([]string{vol.Source.Parent})
		if err != nil || len(parent) == 0 {
			return &ErrFailedToInspectVolume{
				ID:    name,
				Cause: fmt.Sprintf("Could not get parent with ID [%s]", vol.Source.Parent),
			}
		} else if len(parent) > 1 {
			return &ErrFailedToInspectVolume{
				ID:    name,
				Cause: fmt.Sprintf("Expected:1 Got:%v parents for ID [%s]", len(parent), vol.Source.Parent),
			}
		}
		if err := d.schedOps.ValidateSnapshot(params, parent[0]); err != nil {
			return &ErrFailedToInspectVolume{
				ID:    name,
				Cause: fmt.Sprintf("Snapshot/Clone validation failed. %v", err),
			}
		}
		return nil
	}

	// Labels
	var pxNodes []api.Node
	for _, rs := range vol.ReplicaSets {
		for _, n := range rs.Nodes {
			pxNode, err := d.clusterManager.Inspect(n)
			if err != nil {
				return &ErrFailedToInspectVolume{
					ID:    name,
					Cause: fmt.Sprintf("Failed to inspect replica set node: %s err: %v", n, err),
				}
			}

			pxNodes = append(pxNodes, pxNode)
		}
	}

	// Spec
	requestedSpec, requestedLocator, _, err := spec.NewSpecHandler().SpecFromOpts(params)
	if err != nil {
		return &ErrFailedToInspectVolume{
			ID:    name,
			Cause: fmt.Sprintf("failed to parse requested spec of volume. Err: %v", err),
		}
	}

	delete(vol.Locator.VolumeLabels, "pvc") // special handling for the new pvc label added in k8s

	// Params/Options
	for k, v := range params {
		switch k {
		case api.SpecNodes:
			if !reflect.DeepEqual(v, vol.Spec.ReplicaSet.Nodes) {
				return errFailedToInspectVolume(name, k, v, vol.Spec.ReplicaSet.Nodes)
			}
		case api.SpecParent:
			if v != vol.Source.Parent {
				return errFailedToInspectVolume(name, k, v, vol.Source.Parent)
			}
		case api.SpecEphemeral:
			if requestedSpec.Ephemeral != vol.Spec.Ephemeral {
				return errFailedToInspectVolume(name, k, requestedSpec.Ephemeral, vol.Spec.Ephemeral)
			}
		case api.SpecFilesystem:
			if requestedSpec.Format != vol.Spec.Format {
				return errFailedToInspectVolume(name, k, requestedSpec.Format, vol.Spec.Format)
			}
		case api.SpecBlockSize:
			if requestedSpec.BlockSize != vol.Spec.BlockSize {
				return errFailedToInspectVolume(name, k, requestedSpec.BlockSize, vol.Spec.BlockSize)
			}
		case api.SpecHaLevel:
			if requestedSpec.HaLevel != vol.Spec.HaLevel {
				return errFailedToInspectVolume(name, k, requestedSpec.HaLevel, vol.Spec.HaLevel)
			}
		case api.SpecPriorityAlias:
			// Since IO priority isn't guaranteed, we aren't validating it here.
		case api.SpecSnapshotInterval:
			if requestedSpec.SnapshotInterval != vol.Spec.SnapshotInterval {
				return errFailedToInspectVolume(name, k, requestedSpec.SnapshotInterval, vol.Spec.SnapshotInterval)
			}
		case api.SpecSnapshotSchedule:
			// TODO currently volume spec has a different format than request
			// i.e request "daily=12:00,7" turns into "- freq: daily\n  hour: 12\n  retain: 7\n" in volume spec
			//if requestedSpec.SnapshotSchedule != vol.Spec.SnapshotSchedule {
			//	return errFailedToInspectVolume(name, k, requestedSpec.SnapshotSchedule, vol.Spec.SnapshotSchedule)
			//}
		case api.SpecAggregationLevel:
			if requestedSpec.AggregationLevel != vol.Spec.AggregationLevel {
				return errFailedToInspectVolume(name, k, requestedSpec.AggregationLevel, vol.Spec.AggregationLevel)
			}
		case api.SpecShared:
			if requestedSpec.Shared != vol.Spec.Shared {
				return errFailedToInspectVolume(name, k, requestedSpec.Shared, vol.Spec.Shared)
			}
		case api.SpecSticky:
			if requestedSpec.Sticky != vol.Spec.Sticky {
				return errFailedToInspectVolume(name, k, requestedSpec.Sticky, vol.Spec.Sticky)
			}
		case api.SpecGroup:
			if !reflect.DeepEqual(requestedSpec.Group, vol.Spec.Group) {
				return errFailedToInspectVolume(name, k, requestedSpec.Group, vol.Spec.Group)
			}
		case api.SpecGroupEnforce:
			if requestedSpec.GroupEnforced != vol.Spec.GroupEnforced {
				return errFailedToInspectVolume(name, k, requestedSpec.GroupEnforced, vol.Spec.GroupEnforced)
			}
		// portworx injects pvc name and namespace labels so response object won't be equal to request
		case api.SpecLabels:
			for requestedLabelKey, requestedLabelValue := range requestedLocator.VolumeLabels {
				if labelValue, exists := vol.Locator.VolumeLabels[requestedLabelKey]; !exists || requestedLabelValue != labelValue {
					return errFailedToInspectVolume(name, k, requestedLocator.VolumeLabels, vol.Locator.VolumeLabels)
				}
			}
		case api.SpecIoProfile:
			if requestedSpec.IoProfile != vol.Spec.IoProfile {
				return errFailedToInspectVolume(name, k, requestedSpec.IoProfile, vol.Spec.IoProfile)
			}
		case api.SpecSize:
			if requestedSpec.Size != vol.Spec.Size {
				return errFailedToInspectVolume(name, k, requestedSpec.Size, vol.Spec.Size)
			}
		default:
			logrus.Infof("Warning: Encountered unhandled custom param: %v -> %v", k, v)
		}
	}

	logrus.Infof("Successfully inspected volume: %v (%v)", vol.Locator.Name, vol.Id)
	return nil
}

func (d *portworx) ValidateUpdateVolume(vol *torpedovolume.Volume) error {
	name := d.schedOps.GetVolumeName(vol)
	t := func() (interface{}, bool, error) {
		vols, err := d.getVolDriver().Inspect([]string{name})
		if err != nil {
			return nil, true, err
		}

		if len(vols) != 1 {
			return nil, true, &ErrFailedToInspectVolume{
				ID:    name,
				Cause: fmt.Sprintf("Volume inspect result has invalid length. Expected:1 Actual:%v", len(vols)),
			}
		}

		return vols[0], false, nil
	}

	out, err := task.DoRetryWithTimeout(t, inspectVolumeTimeout, inspectVolumeRetryInterval)
	if err != nil {
		return &ErrFailedToInspectVolume{
			ID:    name,
			Cause: fmt.Sprintf("Volume inspect returned err: %v", err),
		}
	}

	respVol := out.(*api.Volume)

	// Size Update
	if respVol.Spec.Size != vol.Size {
		return &ErrFailedToInspectVolume{
			ID: name,
			Cause: fmt.Sprintf("Volume size differs. Expected:%v Actual:%v",
				vol.Size, respVol.Spec.Size),
		}
	}
	return nil
}

func errIsNotFound(err error) bool {
	statusErr, _ := status.FromError(err)
	return statusErr.Code() == codes.NotFound || strings.Contains(err.Error(), "code = NotFound")
}

func (d *portworx) ValidateDeleteVolume(vol *torpedovolume.Volume) error {
	name := d.schedOps.GetVolumeName(vol)
	t := func() (interface{}, bool, error) {
		vols, err := d.volDriver.Inspect([]string{name})
		if err != nil && (err == volume.ErrEnoEnt || errIsNotFound(err)) {
			return nil, false, nil
		} else if err != nil {
			return nil, true, err
		}
		if len(vols) > 0 {
			return nil, true, fmt.Errorf("Volume %v is not yet removed from the system", name)
		}
		return nil, false, nil
	}

	_, err := task.DoRetryWithTimeout(t, validateDeleteVolumeTimeout, defaultRetryInterval)
	if err != nil {
		return &ErrFailedToDeleteVolume{
			ID:    name,
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

func (d *portworx) StopDriver(nodes []node.Node, force bool) error {
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
		}

	}
	logrus.Infof("Sleeping for %v for volume driver to go down.", waitVolDriverToCrash)
	time.Sleep(waitVolDriverToCrash)
	return nil
}

func (d *portworx) GetNodeForVolume(vol *torpedovolume.Volume) (*node.Node, error) {
	name := d.schedOps.GetVolumeName(vol)
	t := func() (interface{}, bool, error) {
		vols, err := d.getVolDriver().Inspect([]string{name})
		if err != nil {
			logrus.Warnf("failed to inspect volume: %s due to: %v", name, err)
			return nil, true, err
		}
		if len(vols) != 1 {
			err = fmt.Errorf("Incorrect number of volumes (%d) returned for vol: %s", len(vols), name)
			logrus.Warnf(err.Error())
			return nil, true, err
		}

		return vols[0], false, nil
	}

	v, err := task.DoRetryWithTimeout(t, inspectVolumeTimeout, inspectVolumeRetryInterval)
	if err != nil {
		return nil, &ErrFailedToInspectVolume{
			ID:    name,
			Cause: err.Error(),
		}
	}

	pxVol := v.(*api.Volume)
	for _, n := range node.GetStorageDriverNodes() {
		if n.VolDriverNodeID == pxVol.AttachedOn {
			return &n, nil
		}
	}

	// Snapshots may not be attached to a node
	if pxVol.Source.Parent != "" {
		return nil, nil
	}

	return nil, &ErrFailedToInspectVolume{
		ID:    name,
		Cause: "Volume is not attached on any node",
	}
}

func (d *portworx) ExtractVolumeInfo(params string) (string, map[string]string, error) {
	ok, volParams, volName := spec.NewSpecHandler().SpecOptsFromString(params)
	if !ok {
		return params, nil, fmt.Errorf("Unable to parse the volume options")
	}
	return volName, volParams, nil
}

func (d *portworx) RandomizeVolumeName(params string) string {
	re := regexp.MustCompile("(" + api.Name + "=)([0-9A-Za-z_-]+)(,)?")
	return re.ReplaceAllString(params, "${1}${2}_"+uuid.New()+"${3}")
}

func (d *portworx) getClusterOnStart() (*api.Cluster, error) {
	t := func() (interface{}, bool, error) {
		cluster, err := d.getClusterManager().Enumerate()
		if err != nil {
			return nil, true, err
		}
		if cluster.Status != api.Status_STATUS_OK {
			return nil, true, &ErrFailedToWaitForPx{
				Cause: fmt.Sprintf("px cluster is still not up. Status: %v", cluster.Status),
			}
		}

		return &cluster, false, nil
	}

	cluster, err := task.DoRetryWithTimeout(t, validateClusterStartTimeout, defaultRetryInterval)
	if err != nil {
		return nil, err
	}

	return cluster.(*api.Cluster), nil
}

func (d *portworx) WaitDriverUpOnNode(n node.Node) error {
	t := func() (interface{}, bool, error) {
		pxNode, err := d.getClusterManager().Inspect(n.VolDriverNodeID)
		if err != nil {
			return "", true, &ErrFailedToWaitForPx{
				Node:  n,
				Cause: err.Error(),
			}
		}

		if pxNode.Status != api.Status_STATUS_OK {
			return "", true, &ErrFailedToWaitForPx{
				Node: n,
				Cause: fmt.Sprintf("px cluster is usable but node status is not ok. Expected: %v Actual: %v",
					api.Status_STATUS_OK, pxNode.Status),
			}
		}

		storageStatus := d.getStorageStatus(n)
		if storageStatus != storageStatusUp {
			return "", true, &ErrFailedToWaitForPx{
				Node: n,
				Cause: fmt.Sprintf("px cluster is usable but storage status is not ok. Expected: %v Actual: %v",
					storageStatusUp, storageStatus),
			}
		}

		logrus.Infof("px on node %s is now up. status: %v", pxNode.Id, pxNode.Status)

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, validateNodeStartTimeout, defaultRetryInterval); err != nil {
		return err
	}

	// Check if PX pod is up
	t = func() (interface{}, bool, error) {
		if !d.schedOps.IsPXReadyOnNode(n) {
			return "", true, &ErrFailedToWaitForPx{
				Node:  n,
				Cause: fmt.Sprintf("PX is not ready on %s after %v", n.Name, validatePXStartTimeout),
			}
		}
		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, validatePXStartTimeout, defaultRetryInterval); err != nil {
		return err
	}

	return nil
}

func (d *portworx) WaitDriverDownOnNode(n node.Node) error {
	t := func() (interface{}, bool, error) {
		// Check if px is down on all node addresses. We don't want to keep track
		// which was the actual interface px was listening on before it went down
		for _, addr := range n.Addresses {
			cManager, err := d.getClusterManagerByAddress(addr)
			if err != nil {
				return "", true, err
			}

			pxNode, err := cManager.Inspect(n.VolDriverNodeID)
			if err != nil {
				if regexp.MustCompile(`.+timeout|connection refused.*`).MatchString(err.Error()) {
					logrus.Infof("px on node %s addr %s is down as inspect returned: %v",
						n.Name, addr, err.Error())
					continue
				}

				return "", true, &ErrFailedToWaitForPx{
					Node:  n,
					Cause: err.Error(),
				}
			}

			if pxNode.Status != api.Status_STATUS_OFFLINE {
				return "", true, &ErrFailedToWaitForPx{
					Node: n,
					Cause: fmt.Sprintf("px is not yet down on node. Expected: %v Actual: %v",
						api.Status_STATUS_OFFLINE, pxNode.Status),
				}
			}
		}

		logrus.Infof("px on node %s is now down.", n.Name)
		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, validateNodeStopTimeout, defaultRetryInterval); err != nil {
		return err
	}

	return nil
}

func (d *portworx) WaitForUpgrade(n node.Node, image, tag string) error {
	t := func() (interface{}, bool, error) {
		pxNode, err := d.getClusterManager().Inspect(n.VolDriverNodeID)
		if err != nil {
			return nil, true, &ErrFailedToWaitForPx{
				Node:  n,
				Cause: err.Error(),
			}
		}

		if pxNode.Status != api.Status_STATUS_OK {
			return nil, true, &ErrFailedToWaitForPx{
				Node: n,
				Cause: fmt.Sprintf("px cluster is usable but node status is not ok. Expected: %v Actual: %v",
					api.Status_STATUS_OK, pxNode.Status),
			}
		}

		// filter out first 3 octets from the tag
		matches := regexp.MustCompile(`^(\d+\.\d+\.\d+).*`).FindStringSubmatch(tag)
		if len(matches) != 2 {
			return nil, false, &ErrFailedToUpgradeVolumeDriver{
				Version: fmt.Sprintf("%s:%s", image, tag),
				Cause:   fmt.Sprintf("failed to parse first 3 octets of version from new version tag: %s", tag),
			}
		}

		pxVersion := pxNode.NodeLabels[pxVersionLabel]
		if !strings.HasPrefix(pxVersion, matches[1]) {
			return nil, true, &ErrFailedToUpgradeVolumeDriver{
				Version: fmt.Sprintf("%s:%s", image, tag),
				Cause: fmt.Sprintf("version on node %s is still %s. It was expected to begin with: %s",
					n.VolDriverNodeID, pxVersion, matches[1]),
			}
		}
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
		vols, err := d.volDriver.Inspect([]string{name})
		if err != nil && (err == volume.ErrEnoEnt || errIsNotFound(err)) {
			return 0, false, volume.ErrEnoEnt
		} else if err != nil {
			return 0, true, err
		}
		if len(vols) == 1 {
			return vols[0].Spec.HaLevel, false, nil
		}
		return 0, false, fmt.Errorf("Extra volumes with the same volume name/ID seen") //Shouldn't reach this line
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

	return replFactor, nil
}

func (d *portworx) SetReplicationFactor(vol *torpedovolume.Volume, replFactor int64) error {
	name := d.schedOps.GetVolumeName(vol)
	t := func() (interface{}, bool, error) {
		vols, err := d.volDriver.Inspect([]string{name})
		if err != nil && (err == volume.ErrEnoEnt || errIsNotFound(err)) {
			return nil, false, volume.ErrEnoEnt
		} else if err != nil {
			return nil, true, err
		}

		if len(vols) == 1 {
			spec := &api.VolumeSpec{
				HaLevel:          int64(replFactor),
				SnapshotInterval: math.MaxUint32,
				ReplicaSet:       &api.ReplicaSet{},
			}
			locator := &api.VolumeLocator{
				Name:         vols[0].Locator.Name,
				VolumeLabels: vols[0].Locator.VolumeLabels,
			}
			err = d.volDriver.Set(vols[0].Id, locator, spec)
			if err != nil {
				return nil, false, err
			}
			quitFlag := false
			wdt := time.After(validateReplicationUpdateTimeout)
			for !quitFlag && !(areRepSetsFinal(vols[0], replFactor) && isClean(vols[0])) {
				select {
				case <-wdt:
					quitFlag = true
				default:
					vols, err = d.volDriver.Inspect([]string{name})
					if err != nil && (err == volume.ErrEnoEnt || errIsNotFound(err)) {
						return nil, false, volume.ErrEnoEnt
					} else if err != nil {
						return nil, true, err
					}
					time.Sleep(defaultRetryInterval)
				}
			}
			if !(areRepSetsFinal(vols[0], replFactor) && isClean(vols[0])) {
				return 0, false, fmt.Errorf("Volume didn't successfully change to replication factor of %d", replFactor)
			}
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("Extra volumes with the same volume name/ID seen") //Shouldn't reach this line
	}

	if _, err := task.DoRetryWithTimeout(t, validateReplicationUpdateTimeout, defaultRetryInterval); err != nil {
		return &ErrFailedToSetReplicationFactor{
			ID:    name,
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
	name := d.schedOps.GetVolumeName(vol)
	t := func() (interface{}, bool, error) {
		vols, err := d.volDriver.Inspect([]string{name})
		if err != nil && (err == volume.ErrEnoEnt || errIsNotFound(err)) {
			return 0, false, volume.ErrEnoEnt
		} else if err != nil {
			return 0, true, err
		}
		if len(vols) == 1 {
			return vols[0].Spec.AggregationLevel, false, nil
		}
		return 0, false, fmt.Errorf("Extra volumes with the same volume name/ID seen") //Shouldn't reach this line
	}

	iAggrLevel, err := task.DoRetryWithTimeout(t, inspectVolumeTimeout, inspectVolumeRetryInterval)
	if err != nil {
		return 0, &ErrFailedToGetAggregationLevel{
			ID:    name,
			Cause: err.Error(),
		}
	}
	aggrLevel, ok := iAggrLevel.(uint32)
	if !ok {
		return 0, &ErrFailedToGetAggregationLevel{
			ID:    name,
			Cause: fmt.Sprintf("Aggregation level is not of type uint32"),
		}
	}

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
		if err = d.testAndSetEndpoint(endpoint); err == nil {
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
	for _, n := range node.GetWorkerNodes() {
		for _, addr := range n.Addresses {
			if err = d.testAndSetEndpoint(addr); err == nil {
				return nil
			}
			logrus.Infof("testAndSetEndpoint failed for %v: %v", endpoint, err)
		}
	}

	return fmt.Errorf("failed to get endpoint for portworx volume driver")
}

func (d *portworx) testAndSetEndpoint(endpoint string) error {
	pxEndpoint := d.constructURL(endpoint)
	cClient, err := clusterclient.NewClusterClient(pxEndpoint, "v1")
	if err != nil {
		return err
	}

	clusterManager := clusterclient.ClusterManager(cClient)
	_, err = clusterManager.Enumerate()
	if err != nil {
		return err
	}

	dClient, err := volumeclient.NewDriverClient(pxEndpoint, DriverName, "", pxdClientSchedUserAgent)
	if err != nil {
		return err
	}

	d.volDriver = volumeclient.VolumeDriver(dClient)
	d.clusterManager = clusterManager
	logrus.Infof("Using %v as endpoint for portworx volume driver", pxEndpoint)

	return nil
}

func (d *portworx) StartDriver(n node.Node) error {
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

func (d *portworx) UpgradeDriver(version string) error {
	if len(version) == 0 {
		return fmt.Errorf("no version supplied for upgrading portworx")
	}

	parts := strings.Split(version, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid version: %s given to upgrade portworx", version)
	}

	logrus.Infof("upgrading portworx to %s", version)

	image := parts[0]
	tag := parts[1]
	if err := d.schedOps.UpgradePortworx(image, tag); err != nil {
		return &ErrFailedToUpgradeVolumeDriver{
			Version: version,
			Cause:   err.Error(),
		}
	}

	for _, n := range node.GetStorageDriverNodes() {
		if err := d.WaitForUpgrade(n, image, tag); err != nil {
			return err
		}
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

	clusterMgr, err := d.getClusterManagerByAddress(pxNodes[0].Addresses[0])
	if err != nil {
		return nil, err
	}
	resp, err := clusterMgr.GetPairToken(false)
	if err != nil {
		return nil, err
	}
	logrus.Infof("Response for token: %v", resp.Token)

	// file up cluster pair info
	pairInfo[clusterIP] = pxNodes[0].Addresses[0]
	pairInfo[tokenKey] = resp.Token
	pairInfo[clusterPort] = strconv.Itoa(pxdRestPort)

	return pairInfo, nil
}

func (d *portworx) getVolDriver() volume.VolumeDriver {
	if d.refreshEndpoint {
		d.setDriver()
	}
	return d.volDriver
}

func (d *portworx) getClusterManager() cluster.Cluster {
	if d.refreshEndpoint {
		d.setDriver()
	}
	return d.clusterManager

}

func (d *portworx) getClusterManagerByAddress(addr string) (cluster.Cluster, error) {
	pxEndpoint := d.constructURL(addr)
	cClient, err := clusterclient.NewClusterClient(pxEndpoint, "v1")
	if err != nil {
		return nil, err
	}

	return clusterclient.ClusterManager(cClient), nil
}

func (d *portworx) getVolumeDriverByAddress(addr string) (volume.VolumeDriver, error) {
	pxEndpoint := d.constructURL(addr)

	dClient, err := volumeclient.NewDriverClient(pxEndpoint, DriverName, "", pxdClientSchedUserAgent)
	if err != nil {
		return nil, err
	}

	return volumeclient.VolumeDriver(dClient), nil
}

func (d *portworx) maintenanceOp(n node.Node, op string) error {
	url := d.constructURL(n.Addresses[0])
	c, err := client.NewClient(url, "", "")
	if err != nil {
		return err
	}
	req := c.Get().Resource(op)
	resp := req.Do()
	return resp.Error()
}

func (d *portworx) constructURL(ip string) string {
	return fmt.Sprintf("http://%s:%d", ip, pxdRestPort)
}

func (d *portworx) getStorageStatus(n node.Node) string {
	const (
		storageInfoKey = "STORAGE-INFO"
		statusKey      = "Status"
	)
	pxNode, err := d.getClusterManager().Inspect(n.VolDriverNodeID)
	if err != nil {
		return err.Error()
	}

	storageInfo, ok := pxNode.NodeData[storageInfoKey]
	if !ok {
		return fmt.Sprintf("Unable to find storage info for node: %v", n.Name)
	}
	storageInfoMap := storageInfo.(map[string]interface{})

	statusInfo, ok := storageInfoMap[statusKey]
	if !ok || storageInfoMap == nil {
		return fmt.Sprintf("Unable to find status info for node: %v", n.Name)
	}
	status := statusInfo.(string)
	return status
}

func (d *portworx) GetReplicaSetNodes(torpedovol *torpedovolume.Volume) ([]string, error) {
	var pxNodes []string
	volName := d.schedOps.GetVolumeName(torpedovol)
	vols, err := d.getVolDriver().Inspect([]string{volName})
	if err != nil {
		return nil, &ErrFailedToInspectVolume{
			ID:    torpedovol.Name,
			Cause: err.Error(),
		}
	}

	if len(vols) == 0 {
		return nil, &ErrFailedToInspectVolume{
			ID:    torpedovol.ID,
			Cause: fmt.Sprintf("unable to find volume %s [%s]", torpedovol.Name, volName),
		}
	}

	for _, rs := range vols[0].ReplicaSets {
		for _, n := range rs.Nodes {
			pxNode, err := d.clusterManager.Inspect(n)
			if err != nil {
				return nil, &ErrFailedToInspectVolume{
					ID:    torpedovol.Name,
					Cause: fmt.Sprintf("Failed to inspect replica set node: %s err: %v", n, err),
				}
			}
			nodeName := pxNode.SchedulerNodeName
			if nodeName == "" {
				nodeName = pxNode.Hostname
			}
			pxNodes = append(pxNodes, nodeName)
		}
	}
	return pxNodes, nil
}

func init() {
	torpedovolume.Register(DriverName, &portworx{})
}
