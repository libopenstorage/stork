package portworx

import (
	"fmt"
	"reflect"
	"time"

	dockerclient "github.com/fsouza/go-dockerclient"
	"github.com/libopenstorage/openstorage/api"
	clusterclient "github.com/libopenstorage/openstorage/api/client/cluster"
	volumeclient "github.com/libopenstorage/openstorage/api/client/volume"
	"github.com/libopenstorage/openstorage/api/spec"
	"github.com/libopenstorage/openstorage/cluster"
	"github.com/libopenstorage/openstorage/volume"
	"github.com/portworx/sched-ops/k8s"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	torpedovolume "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/sirupsen/logrus"
)

const (
	// DriverName is the name of the portworx driver implementation
	DriverName = "pxd"
	// PXServiceName is the name of the portworx service
	PXServiceName = "portworx-service"
	// PXNamespace is the kubernetes namespace in which portworx daemon set runs.
	PXNamespace             = "kube-system"
	pxdClientSchedUserAgent = "pxd-sched"
	pxdRestPort             = 9001
)

type portworx struct {
	hostConfig      *dockerclient.HostConfig
	clusterManager  cluster.Cluster
	volDriver       volume.VolumeDriver
	schedDriver     scheduler.Driver
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
	if d.schedDriver, err = scheduler.Get(sched); err != nil {
		return err
	}

	if d.nodeDriver, err = node.Get(nodeDriver); err != nil {
		return err
	}

	if d.schedOps, err = schedops.Get(sched); err != nil {
		return fmt.Errorf("failed to get scheduler operator for portworx. Err: %v", err)
	}

	if err = d.setDriver(); err != nil {
		return err
	}

	cluster, err := d.getClusterManager().Enumerate()
	if err != nil {
		return err
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

	return err
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
				if err = d.getVolDriver().Unmount(v.Id, path); err != nil {
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

			if err = d.getVolDriver().Detach(v.Id, false); err != nil {
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

func (d *portworx) ValidateCreateVolume(name string, params map[string]string) error {
	t := func() (interface{}, error) {
		vols, err := d.getVolDriver().Inspect([]string{name})
		if err != nil {
			return nil, err
		}

		if len(vols) != 1 {
			return nil, &ErrFailedToInspectVolume{
				ID:    name,
				Cause: fmt.Sprintf("Volume inspect result has invalid length. Expected:1 Actual:%v", len(vols)),
			}
		}

		return vols[0], nil
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

	// Size
	actualSizeStr := fmt.Sprintf("%d", vol.Spec.Size)
	if params["size"] != actualSizeStr { // TODO this will fail for docker. Current focus on k8s.
		return &ErrFailedToInspectVolume{
			ID:    name,
			Cause: fmt.Sprintf("Volume has invalid size. Expected:%v Actual:%v", params["size"], actualSizeStr),
		}
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
				return errFailedToInspectVolme(name, k, v, vol.Spec.ReplicaSet.Nodes)
			}
		case api.SpecParent:
			if v != vol.Source.Parent {
				return errFailedToInspectVolme(name, k, v, vol.Source.Parent)
			}
		case api.SpecEphemeral:
			if requestedSpec.Ephemeral != vol.Spec.Ephemeral {
				return errFailedToInspectVolme(name, k, requestedSpec.Ephemeral, vol.Spec.Ephemeral)
			}
		case api.SpecFilesystem:
			if requestedSpec.Format != vol.Spec.Format {
				return errFailedToInspectVolme(name, k, requestedSpec.Format, vol.Spec.Format)
			}
		case api.SpecBlockSize:
			if requestedSpec.BlockSize != vol.Spec.BlockSize {
				return errFailedToInspectVolme(name, k, requestedSpec.BlockSize, vol.Spec.BlockSize)
			}
		case api.SpecHaLevel:
			if requestedSpec.HaLevel != vol.Spec.HaLevel {
				return errFailedToInspectVolme(name, k, requestedSpec.HaLevel, vol.Spec.HaLevel)
			}
		case api.SpecPriorityAlias:
			// Since IO priority isn't guaranteed, we aren't validating it here.
		case api.SpecSnapshotInterval:
			if requestedSpec.SnapshotInterval != vol.Spec.SnapshotInterval {
				return errFailedToInspectVolme(name, k, requestedSpec.SnapshotInterval, vol.Spec.SnapshotInterval)
			}
		case api.SpecAggregationLevel:
			if requestedSpec.AggregationLevel != vol.Spec.AggregationLevel {
				return errFailedToInspectVolme(name, k, requestedSpec.AggregationLevel, vol.Spec.AggregationLevel)
			}
		case api.SpecShared:
			if requestedSpec.Shared != vol.Spec.Shared {
				return errFailedToInspectVolme(name, k, requestedSpec.Shared, vol.Spec.Shared)
			}
		case api.SpecSticky:
			if requestedSpec.Sticky != vol.Spec.Sticky {
				return errFailedToInspectVolme(name, k, requestedSpec.Sticky, vol.Spec.Sticky)
			}
		case api.SpecGroup:
			if requestedSpec.Group != vol.Spec.Group {
				return errFailedToInspectVolme(name, k, requestedSpec.Group, vol.Spec.Group)
			}
		case api.SpecGroupEnforce:
			if requestedSpec.GroupEnforced != vol.Spec.GroupEnforced {
				return errFailedToInspectVolme(name, k, requestedSpec.GroupEnforced, vol.Spec.GroupEnforced)
			}
		case api.SpecLabels:
			if !reflect.DeepEqual(requestedLocator.VolumeLabels, vol.Locator.VolumeLabels) {
				return errFailedToInspectVolme(name, k, requestedLocator.VolumeLabels, vol.Locator.VolumeLabels)
			}
		case api.SpecIoProfile:
			if requestedSpec.IoProfile != vol.Spec.IoProfile {
				return errFailedToInspectVolme(name, k, requestedSpec.IoProfile, vol.Spec.IoProfile)
			}
		case api.SpecSize:
			// pass, we don't validate size here
		default:
			logrus.Infof("Warning: Encountered unhandled custom param: %v -> %v", k, v)
		}
	}

	logrus.Infof("Successfully inspected volume: %v (%v)", vol.Locator.Name, vol.Id)
	return nil
}

func (d *portworx) ValidateDeleteVolume(vol *torpedovolume.Volume) error {
	name := d.schedOps.GetVolumeName(vol)
	t := func() (interface{}, error) {
		vols, err := d.volDriver.Inspect([]string{name})
		if err != nil && err == volume.ErrEnoEnt {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
		if len(vols) > 0 {
			return nil, fmt.Errorf("Volume %v is not yet removed from the system", name)
		}
		return nil, nil
	}

	_, err := task.DoRetryWithTimeout(t, 1*time.Minute, 5*time.Second)
	if err != nil {
		return &ErrFailedToDeleteVolume{
			ID:    name,
			Cause: err.Error(),
		}
	}

	return d.schedOps.ValidateRemoveLabels(vol, d.schedDriver)
}

func (d *portworx) ValidateVolumeCleanup() error {
	return d.schedOps.ValidateVolumeCleanup(d.schedDriver, d.nodeDriver)
}

func (d *portworx) StopDriver(n node.Node) error {
	return d.schedOps.DisableOnNode(n)
}

func (d *portworx) WaitStart(n node.Node) error {
	var err error
	// Wait for Portworx to become usable.
	t := func() (interface{}, error) {
		if status, _ := d.getClusterManager().NodeStatus(); status != api.Status_STATUS_OK {
			return "", &ErrFailedToWaitForPx{
				Node:  n,
				Cause: fmt.Sprintf("px cluster is still not up. Status: %v", status),
			}
		}

		pxNode, err := d.getClusterManager().Inspect(n.Name)
		if err != nil {
			return "", &ErrFailedToWaitForPx{
				Node:  n,
				Cause: err.Error(),
			}
		}

		if pxNode.Status != api.Status_STATUS_OK {
			return "", &ErrFailedToWaitForPx{
				Node: n,
				Cause: fmt.Sprintf("px cluster is usable but not status is not ok. Expected: %v Actual: %v",
					api.Status_STATUS_OK, pxNode.Status),
			}
		}

		return "", nil
	}

	if _, err := task.DoRetryWithTimeout(t, 2*time.Minute, 10*time.Second); err != nil {
		return err
	}

	return err
}

func (d *portworx) setDriver() error {
	var err error
	nodes := d.schedDriver.GetNodes()

	var endpoint string
	// Try portworx-service first
	svc, err := k8s.Instance().GetService(PXServiceName, PXNamespace)
	if err == nil {
		endpoint = svc.Spec.ClusterIP
		if err = d.testAndSetEndpoint(endpoint); err == nil {
			d.refreshEndpoint = false
			return nil
		}
		logrus.Infof("testAndSetEndpoint failed for %v: %v", endpoint, err)
	}

	// Try direct address of cluster nodes
	// Set refresh endpoint to true so that we try and get the new
	// and working driver if the endpoint we are hooked onto goes
	// down
	d.refreshEndpoint = true
	for _, n := range nodes {
		if n.Type == node.TypeWorker {
			for _, addr := range n.Addresses {
				if err = d.testAndSetEndpoint(addr); err == nil {
					return nil
				}
				logrus.Infof("testAndSetEndpoint failed for %v: %v", endpoint, err)
			}
		}
	}

	return fmt.Errorf("failed to get endpoint for portworx volume driver")
}

func (d *portworx) testAndSetEndpoint(endpoint string) error {
	pxEndpoint := fmt.Sprintf("http://%s:%d", endpoint, pxdRestPort)
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
	return d.schedOps.EnableOnNode(n)
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

func init() {
	torpedovolume.Register(DriverName, &portworx{})
}
