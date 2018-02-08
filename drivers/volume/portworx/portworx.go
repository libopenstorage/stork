package portworx

import (
	"fmt"
	"reflect"
	"regexp"
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
	d.updateNodes(cluster.Nodes)

	for _, n := range node.GetWorkerNodes() {
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

func (d *portworx) updateNodes(pxNodes []api.Node) {
	for _, n := range node.GetNodes() {
		d.updateNode(n, pxNodes)
	}
}

func (d *portworx) updateNode(n node.Node, pxNodes []api.Node) {
	for _, address := range n.Addresses {
		for _, pxNode := range pxNodes {
			if address == pxNode.DataIp || address == pxNode.MgmtIp {
				n.VolDriverNodeID = pxNode.Id
				node.UpdateNode(n)
				return
			}
		}
	}
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

	if _, err := task.DoRetryWithTimeout(t, 1*time.Minute, 10*time.Second); err != nil {
		return err
	}
	t = func() (interface{}, bool, error) {
		apiNode, err := d.getClusterManager().Inspect(n.Name)
		if err != nil {
			return nil, true, err
		}
		if apiNode.Status == api.Status_STATUS_MAINTENANCE {
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("Node %v is not in Maintenance mode", n.Name)
	}

	if _, err := task.DoRetryWithTimeout(t, 1*time.Minute, 10*time.Second); err != nil {
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

	if _, err := task.DoRetryWithTimeout(t, 1*time.Minute, 10*time.Second); err != nil {
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
			return nil, true, &ErrFailedToInspectVolume{
				ID:    name,
				Cause: fmt.Sprintf("Volume inspect result has invalid length. Expected:1 Actual:%v", len(vols)),
			}
		}

		return vols[0], false, nil
	}

	out, err := task.DoRetryWithTimeout(t, 10*time.Second, 2*time.Second)
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

	if vol.IsSnapshot() {
		logrus.Infof("Warning: Param/Option testing of snapshots is currently not supported. Skipping")
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

	if err := d.schedOps.ValidateAddLabels(pxNodes, vol); err != nil {
		return &ErrFailedToInspectVolume{
			ID:    name,
			Cause: err.Error(),
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
			if requestedSpec.Group != vol.Spec.Group {
				return errFailedToInspectVolume(name, k, requestedSpec.Group, vol.Spec.Group)
			}
		case api.SpecGroupEnforce:
			if requestedSpec.GroupEnforced != vol.Spec.GroupEnforced {
				return errFailedToInspectVolume(name, k, requestedSpec.GroupEnforced, vol.Spec.GroupEnforced)
			}
		case api.SpecLabels:
			if !reflect.DeepEqual(requestedLocator.VolumeLabels, vol.Locator.VolumeLabels) {
				return errFailedToInspectVolume(name, k, requestedLocator.VolumeLabels, vol.Locator.VolumeLabels)
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

func (d *portworx) ValidateDeleteVolume(vol *torpedovolume.Volume) error {
	name := d.schedOps.GetVolumeName(vol)
	t := func() (interface{}, bool, error) {
		vols, err := d.volDriver.Inspect([]string{name})
		if err != nil && err == volume.ErrEnoEnt {
			return nil, false, nil
		} else if err != nil {
			return nil, true, err
		}
		if len(vols) > 0 {
			return nil, true, fmt.Errorf("Volume %v is not yet removed from the system", name)
		}
		return nil, false, nil
	}

	_, err := task.DoRetryWithTimeout(t, 3*time.Minute, 5*time.Second)
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

func (d *portworx) StopDriver(n node.Node) error {
	return d.nodeDriver.Systemctl(n, pxSystemdServiceName, node.SystemctlOpts{
		Action: "stop",
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         5 * time.Minute,
			TimeBeforeRetry: 10 * time.Second,
		}})
}

func (d *portworx) ExtractVolumeInfo(params string) (string, map[string]string, error) {
	ok, volParams, volName := spec.NewSpecHandler().SpecOptsFromString(params)
	if !ok {
		return params, nil, fmt.Errorf("Unable to parse the volume options")
	}
	return volName, volParams, nil
}

func (d *portworx) RandomizeVolumeName(params string) string {
	re := regexp.MustCompile("(" + api.Name + "=)([0-9A-Za-z_-]+),?")
	return re.ReplaceAllString(params, "${1}${2}_"+uuid.New())
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

	cluster, err := task.DoRetryWithTimeout(t, 2*time.Minute, 10*time.Second)
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

		logrus.Infof("px on node %s is now up. status: %v", pxNode.Id, pxNode.Status)

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, 2*time.Minute, 10*time.Second); err != nil {
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

	if _, err := task.DoRetryWithTimeout(t, 2*time.Minute, 10*time.Second); err != nil {
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

	if _, err := task.DoRetryWithTimeout(t, 10*time.Minute, 30*time.Second); err != nil {
		return err
	}
	return nil
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
	return d.nodeDriver.Systemctl(n, pxSystemdServiceName, node.SystemctlOpts{
		Action: "start",
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         2 * time.Minute,
			TimeBeforeRetry: 10 * time.Second,
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

	for _, n := range node.GetWorkerNodes() {
		if err := d.WaitForUpgrade(n, image, tag); err != nil {
			return err
		}
	}

	return nil
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

func init() {
	torpedovolume.Register(DriverName, &portworx{})
}
