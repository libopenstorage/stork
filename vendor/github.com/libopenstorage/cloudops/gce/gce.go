package gce

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/libopenstorage/cloudops"
	"github.com/libopenstorage/cloudops/backoff"
	"github.com/libopenstorage/cloudops/unsupported"
	"github.com/libopenstorage/openstorage/pkg/parser"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	compute "google.golang.org/api/compute/v1"
	container "google.golang.org/api/container/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"k8s.io/apimachinery/pkg/util/wait"
)

var notFoundRegex = regexp.MustCompile(`.*notFound`)

const googleDiskPrefix = "/dev/disk/by-id/google-"
const retrySeconds = 15

// StatusReady ready status
const StatusReady = "ready"

const (
	devicePathMaxRetryCount = 3
	devicePathRetryInterval = 2 * time.Second
	clusterNameKey          = "cluster-name"
	clusterLocationKey      = "cluster-location"
	kubeLabelsKey           = "kube-labels"
	nodePoolKey             = "cloud.google.com/gke-nodepool"
	instanceTemplateKey     = "instance-template"
	doneStatus              = "DONE"
)

type gceOps struct {
	cloudops.Compute
	inst             *instance
	computeService   *compute.Service
	containerService *container.Service
	mutex            sync.Mutex
}

// instance stores the metadata of the running GCE instance
type instance struct {
	name            string
	hostname        string
	zone            string
	region          string
	project         string
	clusterName     string
	clusterLocation string
	nodePoolID      string
}

// IsDevMode checks if the pkg is invoked in developer mode where GCE credentials
// are set as env variables
func IsDevMode() bool {
	var i = new(instance)
	err := gceInfoFromEnv(i)
	return err == nil
}

// NewClient creates a new GCE operations client
func NewClient() (cloudops.Ops, error) {

	var i = new(instance)
	var err error
	if metadata.OnGCE() {
		err = gceInfo(i)
	} else if ok := IsDevMode(); ok {
		err = gceInfoFromEnv(i)
	} else {
		return nil, fmt.Errorf("instance is not running on GCE")
	}

	if err != nil {
		return nil, fmt.Errorf("error fetching instance info. Err: %v", err)
	}

	ctx := context.Background()
	computeService, err := compute.NewService(ctx, option.WithScopes(compute.ComputeScope))
	if err != nil {
		return nil, fmt.Errorf("unable to create Compute service: %v", err)
	}

	containerService, err := container.NewService(ctx, option.WithScopes(compute.CloudPlatformScope))
	if err != nil {
		return nil, fmt.Errorf("unable to create Container service: %v", err)
	}

	return backoff.NewExponentialBackoffOps(
		&gceOps{
			Compute:          unsupported.NewUnsupportedCompute(),
			inst:             i,
			computeService:   computeService,
			containerService: containerService,
		},
		isExponentialError,
		backoff.DefaultExponentialBackoff,
	), nil
}

func (s *gceOps) Name() string { return string(cloudops.GCE) }

func (s *gceOps) InstanceID() string { return s.inst.name }

func (s *gceOps) InspectInstance(instanceID string) (*cloudops.InstanceInfo, error) {
	inst, err := s.computeService.Instances.Get(s.inst.project, s.inst.zone, instanceID).Do()
	if err != nil {
		return nil, err
	}

	instInfo := &cloudops.InstanceInfo{
		CloudResourceInfo: cloudops.CloudResourceInfo{
			Name:   inst.Name,
			ID:     fmt.Sprintf("%d", inst.Id),
			Zone:   inst.Zone,
			Region: s.inst.region,
			Labels: inst.Labels,
		},
		State: mapState(inst.Status),
	}
	return instInfo, nil
}

// https://cloud.google.com/compute/docs/instances/instance-life-cycle
func mapState(status string) cloudops.InstanceState {
	switch status {
	case "PROVISIONING":
		fallthrough
	case "STAGING":
		return cloudops.InstanceStateStarting
	case "RUNNING":
		return cloudops.InstanceStateOnline
	case "SUSPENDING":
		fallthrough
	case "STOPPING":
		fallthrough
	case "REPAIRING":
		return cloudops.InstanceStateTerminating
	case "TERMINATED":
		fallthrough
	case "SUSPENDED":
		fallthrough
	case "STOPPED":
		return cloudops.InstanceStateOffline
	}

	return cloudops.InstanceStateUnknown
}

func (s *gceOps) InspectInstanceGroupForInstance(instanceID string) (*cloudops.InstanceGroupInfo, error) {
	inst, err := s.computeService.Instances.Get(s.inst.project, s.inst.zone, instanceID).Do()
	if err != nil {
		return nil, err
	}

	meta := inst.Metadata
	if meta == nil {
		return nil, fmt.Errorf("instance doesn't have metadata set")
	}

	var (
		gkeClusterName   string
		instanceTemplate string
		clusterLocation  string
		kubeLabels       map[string]string
	)

	for _, item := range meta.Items {
		if item == nil {
			continue
		}

		if item.Key == clusterNameKey {
			if item.Value == nil {
				return nil, fmt.Errorf("instance has %s key in metadata but has invalid value", clusterNameKey)
			}

			gkeClusterName = *item.Value
		}

		if item.Key == instanceTemplateKey {
			if item.Value == nil {
				return nil, fmt.Errorf("instance has %s key in metadata but has invalid value", instanceTemplateKey)
			}

			instanceTemplate = *item.Value
		}

		if item.Key == clusterLocationKey {
			if item.Value == nil {
				return nil, fmt.Errorf("instance has %s key in metadata but has invalid value", clusterLocationKey)
			}

			clusterLocation = *item.Value
		}

		if item.Key == kubeLabelsKey {
			if item.Value == nil {
				return nil, fmt.Errorf("instance has %s key in metadata but has invalid value", kubeLabelsKey)
			}

			kubeLabels, err = parser.LabelsFromString(*item.Value)
			if err != nil {
				return nil, err
			}
		}
	}

	if len(gkeClusterName) == 0 ||
		len(instanceTemplate) == 0 ||
		len(clusterLocation) == 0 ||
		len(kubeLabels) == 0 {
		return nil, &cloudops.ErrNotSupported{
			Operation: "InspectInstanceGroupForInstance",
			Reason:    "API is currently only supported on the GKE platform",
		}
	}

	for labelKey, labelValue := range kubeLabels {
		if labelKey == nodePoolKey {
			nodePoolPath := fmt.Sprintf("projects/%s/locations/%s/clusters/%s/nodePools/%s",
				s.inst.project, clusterLocation, gkeClusterName, labelValue)
			nodePool, err := s.containerService.Projects.Locations.Clusters.NodePools.Get(nodePoolPath).Do()
			if err != nil {
				logrus.Errorf("failed to get node pool at path: %s", nodePoolPath)
				return nil, err
			}

			zones := make([]string, 0)
			for _, igURL := range nodePool.InstanceGroupUrls {
				// e.g https://www.googleapis.com/compute/v1/projects/portworx-eng/zones/us-east1-b/instanceGroupManagers/gke-harsh-regional-asg-t-default-pool-a8750fe9-grp
				parts := strings.Split(igURL, "/")
				if len(parts) < 3 {
					return nil, fmt.Errorf("failed to parse zones for a node pool")
				}

				zones = append(zones, parts[len(parts)-3])
			}

			zonal, err := isZonalCluster(clusterLocation)
			if err != nil {
				return nil, err
			}

			var clusterZone, clusterRegion string
			if zonal {
				clusterZone = clusterLocation
				clusterRegion = clusterLocation[:len(clusterLocation)-2]
			} else {
				// Regional GKE cluster does not have master zone
				clusterZone = ""
				clusterRegion = clusterLocation
			}
			retval := &cloudops.InstanceGroupInfo{
				CloudResourceInfo: cloudops.CloudResourceInfo{
					Name:   nodePool.Name,
					Zone:   clusterZone,
					Region: clusterRegion,
				},
				Zones: zones,
			}

			if nodePool.Autoscaling != nil {
				// to get actual min and max count, need to multiple total number of zones with the count
				// for e.g if MinNodeCount = 1 and we have 3 zones, there will be a minimum of 3 nodes, 1 per zone.
				minCount := nodePool.Autoscaling.MinNodeCount * int64(len(retval.Zones))
				maxCount := nodePool.Autoscaling.MaxNodeCount * int64(len(retval.Zones))
				retval.AutoscalingEnabled = nodePool.Autoscaling.Enabled
				retval.Min = &minCount
				retval.Max = &maxCount
			}

			if nodePool.Config != nil {
				retval.Labels = nodePool.Config.Labels
			}

			return retval, nil
		}
	}

	return nil, fmt.Errorf("instance doesn't belong to a GKE node pool")
}

func (s *gceOps) ApplyTags(
	diskName string,
	labels map[string]string) error {
	d, err := s.computeService.Disks.Get(s.inst.project, s.inst.zone, diskName).Do()
	if err != nil {
		return err
	}

	var currentLabels map[string]string
	if len(d.Labels) == 0 {
		currentLabels = make(map[string]string)
	} else {
		currentLabels = d.Labels
	}

	for k, v := range formatLabels(labels) {
		currentLabels[k] = v
	}

	rb := &compute.ZoneSetLabelsRequest{
		LabelFingerprint: d.LabelFingerprint,
		Labels:           currentLabels,
	}

	operation, err := s.computeService.Disks.SetLabels(s.inst.project, s.inst.zone, d.Name, rb).Do()
	if err != nil {
		return err
	}
	return s.waitForOpCompletion("disk.ApplyTags", s.inst.zone, operation)
}

func (s *gceOps) Attach(diskName string, options map[string]string) (string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var d *compute.Disk
	d, err := s.computeService.Disks.Get(s.inst.project, s.inst.zone, diskName).Do()
	if err != nil {
		return "", err
	}

	if len(d.Users) != 0 {
		return "", fmt.Errorf("disk %s is already in use by %s", diskName, d.Users)
	}

	diskURL := d.SelfLink
	rb := &compute.AttachedDisk{
		DeviceName: d.Name,
		Source:     diskURL,
	}

	operation, err := s.computeService.Instances.AttachDisk(
		s.inst.project,
		s.inst.zone,
		s.inst.name,
		rb).Do()
	if err != nil {
		return "", err
	}

	if opErr := s.waitForOpCompletion("disk.Attach", s.inst.zone, operation); opErr != nil {
		return "", opErr
	}

	devicePath, err := s.waitForAttach(d, time.Minute)
	if err != nil {
		return "", err
	}

	return devicePath, nil
}

func (s *gceOps) Create(
	template interface{},
	labels map[string]string,
) (interface{}, error) {
	v, ok := template.(*compute.Disk)
	if !ok {
		return nil, cloudops.NewStorageError(cloudops.ErrVolInval,
			"Invalid volume template given", "")
	}

	newDisk := &compute.Disk{
		Description:    "Disk created by openstorage",
		Labels:         formatLabels(labels),
		Name:           v.Name,
		SizeGb:         v.SizeGb,
		SourceImage:    v.SourceImage,
		SourceSnapshot: v.SourceSnapshot,
		Type:           v.Type,
		Zone:           path.Base(v.Zone),
	}

	operation, err := s.computeService.Disks.Insert(s.inst.project, newDisk.Zone, newDisk).Do()
	if err != nil {
		return nil, err
	}

	if opErr := s.waitForOpCompletion("disk.Create", newDisk.Zone, operation); opErr != nil {
		return nil, opErr
	}

	if err = s.checkDiskStatus(newDisk.Name, newDisk.Zone, StatusReady); err != nil {
		return nil, s.rollbackCreate(v.Name, err)
	}

	d, err := s.computeService.Disks.Get(s.inst.project, newDisk.Zone, newDisk.Name).Do()
	if err != nil {
		return nil, err
	}

	return d, err
}

func (s *gceOps) DeleteFrom(id, _ string) error {
	return s.Delete(id)
}

func (s *gceOps) DeleteInstance(instanceID string, zone string, timeout time.Duration) error {

	operation, err := s.computeService.Instances.Delete(s.inst.project, zone, instanceID).Do()
	if err != nil {
		return fmt.Errorf("Error occured while deleting instance:[%v] in zone [%s]. Error:[%v]", instanceID, zone, err)
	}

	f := func() (interface{}, bool, error) {

		operation, err := s.computeService.ZoneOperations.Get(s.inst.project, zone, operation.Name).Do()
		if err != nil {
			// Error occured, just retry
			return nil, true, err
		}

		// The operation is done, either cancelled or completed.
		if operation.Status == "DONE" {
			return nil, false, nil
		}

		return nil,
			true,
			fmt.Errorf("instance [%s] delete operation [%v] is still in [%s] state. Waiting to become [DONE]",
				instanceID, operation.Name, operation.Status)
	}

	_, err = task.DoRetryWithTimeout(f, timeout, retrySeconds*time.Second)
	if err != nil {
		return err
	}

	return nil
}

func (s *gceOps) Delete(id string) error {
	ctx := context.Background()
	found := false
	req := s.computeService.Disks.AggregatedList(s.inst.project)
	if err := req.Pages(ctx, func(page *compute.DiskAggregatedList) error {
		for _, diskScopedList := range page.Items {
			for _, disk := range diskScopedList.Disks {
				if disk.Name == id {
					found = true
					operation, err := s.computeService.Disks.Delete(s.inst.project, path.Base(disk.Zone), id).Do()
					if err != nil {
						return err
					}
					return s.waitForOpCompletion("disk.Delete", s.inst.zone, operation)
				}
			}
		}
		return nil
	}); err != nil {
		logrus.Errorf("failed to list disks: %v", err)
		return err
	}

	if !found {
		return fmt.Errorf("failed to delete disk %s: disk not found", id)
	}

	return nil
}

func (s *gceOps) Detach(devicePath string) error {
	return s.detachInternal(devicePath, s.inst.name)
}

func (s *gceOps) DetachFrom(devicePath, instanceName string) error {
	return s.detachInternal(devicePath, instanceName)
}

func (s *gceOps) detachInternal(devicePath, instanceName string) error {
	operation, err := s.computeService.Instances.DetachDisk(
		s.inst.project,
		s.inst.zone,
		instanceName,
		devicePath).Do()
	if err != nil {
		return err
	}

	if opErr := s.waitForOpCompletion("disk.Detach", s.inst.zone, operation); opErr != nil {
		return opErr
	}

	var d *compute.Disk
	d, err = s.computeService.Disks.Get(s.inst.project, s.inst.zone, devicePath).Do()
	if err != nil {
		return err
	}

	err = s.waitForDetach(d.SelfLink, time.Minute)
	if err != nil {
		return err
	}

	return err
}

func (s *gceOps) DeviceMappings() (map[string]string, error) {
	/*
	 * The names of disk devices in GCE are determined by
	 * UDEV rules that must be installed on each host node running
	 * in GCE.
	 *
	 * The UDEV rules can be found in the following files on the host:
	 *
	 *    /lib/udev/rules.d/64-gce-disk-removal.rules
	 *    /lib/udev/rules.d/99-gce.rules
	 *    /lib/udev/rules.d/65-gce-disk-naming.rules
	 *
	 * These rules can also be found at:
	 *  https://github.com/GoogleCloudPlatform/compute-image-packages/tree/master/packages/google-compute-engine/src/lib/udev/rules.d/
	 *
	 * These UDEV rules are installed by default in images supplied by Google.
	 * However, in custom images, these files may not be there, so they must be installed.
	 */
	instance, err := s.describeinstance()
	if err != nil {
		return nil, err
	}
	m := make(map[string]string)
	for _, d := range instance.Disks {
		if d.Boot {
			continue
		}

		pathByID := fmt.Sprintf("%s%s", googleDiskPrefix, d.DeviceName)
		devPath, err := s.diskIDToBlockDevPath(pathByID)
		if err != nil {
			return nil, cloudops.NewStorageError(
				cloudops.ErrInvalidDevicePath,
				fmt.Sprintf("unable to find block dev path for %s. %v", pathByID, err),
				s.inst.name)
		}
		m[devPath] = path.Base(d.Source)
	}

	return m, nil
}

func (s *gceOps) DevicePath(diskName string) (string, error) {
	d, err := s.computeService.Disks.Get(s.inst.project, s.inst.zone, diskName).Do()
	if gerr, ok := err.(*googleapi.Error); ok &&
		gerr.Code == http.StatusNotFound {
		return "", cloudops.NewStorageError(
			cloudops.ErrVolNotFound,
			fmt.Sprintf("Disk: %s not found in zone %s", diskName, s.inst.zone),
			s.inst.name)
	} else if err != nil {
		return "", err
	}

	if len(d.Users) == 0 {
		err = cloudops.NewStorageError(cloudops.ErrVolDetached,
			fmt.Sprintf("Disk: %s is detached", d.Name), s.inst.name)
		return "", err
	}

	var inst *compute.Instance
	inst, err = s.describeinstance()
	if err != nil {
		return "", err
	}

	for _, instDisk := range inst.Disks {
		if instDisk.Source == d.SelfLink {
			pathByID := fmt.Sprintf("%s%s", googleDiskPrefix, instDisk.DeviceName)
			devPath, err := s.diskIDToBlockDevPathWithRetry(pathByID)
			if err == nil {
				return devPath, nil
			}
			return "", cloudops.NewStorageError(
				cloudops.ErrInvalidDevicePath,
				fmt.Sprintf("unable to find block dev path for %s. %v", devPath, err),
				s.inst.name)
		}
	}

	return "", cloudops.NewStorageError(
		cloudops.ErrVolAttachedOnRemoteNode,
		fmt.Sprintf("disk %s is not attached on: %s (Attached on: %v)",
			d.Name, s.inst.name, d.Users),
		s.inst.name)
}

func (s *gceOps) Enumerate(
	volumeIds []*string,
	labels map[string]string,
	setIdentifier string,
) (map[string][]interface{}, error) {
	sets := make(map[string][]interface{})

	allDisks, err := s.getDisksFromAllZones(formatLabels(labels))
	if err != nil {
		return nil, err
	}

	for _, disk := range allDisks {
		if len(setIdentifier) == 0 {
			cloudops.AddElementToMap(sets, disk, cloudops.SetIdentifierNone)
		} else {
			found := false
			for key := range disk.Labels {
				if key == setIdentifier {
					cloudops.AddElementToMap(sets, disk, key)
					found = true
					break
				}
			}

			if !found {
				cloudops.AddElementToMap(sets, disk, cloudops.SetIdentifierNone)
			}
		}
	}

	return sets, nil
}

func (s *gceOps) FreeDevices(
	blockDeviceMappings []interface{},
	rootDeviceName string,
) ([]string, error) {
	return nil, fmt.Errorf("function not implemented")
}

func (s *gceOps) GetDeviceID(disk interface{}) (string, error) {
	if d, ok := disk.(*compute.Disk); ok {
		return d.Name, nil
	} else if d, ok := disk.(*compute.Snapshot); ok {
		return d.Name, nil
	} else {
		return "", fmt.Errorf("invalid type: %v given to GetDeviceID", disk)
	}
}

func (s *gceOps) Expand(
	volumeID string,
	newSizeInGiB uint64,
) (uint64, error) {

	vol, err := s.computeService.Disks.Get(s.inst.project, s.inst.zone, volumeID).Do()
	if err != nil {
		return 0, err
	}
	currentSizeInGiB := uint64(vol.SizeGb)

	if currentSizeInGiB >= newSizeInGiB {
		return currentSizeInGiB, cloudops.NewStorageError(cloudops.ErrDiskGreaterOrEqualToExpandSize,
			fmt.Sprintf("disk is already has a size: %d greater than or equal "+
				"requested size: %d", currentSizeInGiB, newSizeInGiB), "")
	}

	op, err := s.computeService.Disks.Resize(s.inst.project, s.inst.zone, volumeID, &compute.DisksResizeRequest{
		SizeGb:          int64(newSizeInGiB),
		ForceSendFields: nil,
		NullFields:      nil,
	}).Do()
	if err != nil {
		return 0, err
	}

	// Taken from https://github.com/kubernetes/legacy-cloud-providers/blob/cebac2e3367faa71a39050bf5563fa7406006e76/gce/gce.go#L869
	backoff := wait.Backoff{
		// These values will add up to about a minute. See #56293 for background.
		Duration: time.Second,
		Factor:   1.4,
		Steps:    10,
	}

	checkForResize := func() (bool, error) {
		newOp, err := s.computeService.ZoneOperations.Get(s.inst.project, s.inst.zone, fmt.Sprintf("%d", op.Id)).Do()
		if err != nil {
			return false, err
		}
		if newOp.Status == doneStatus {
			return true, nil
		}
		return false, nil
	}
	waitWithErr := wait.ExponentialBackoff(backoff, checkForResize)
	return newSizeInGiB, waitWithErr
}

func (s *gceOps) Inspect(diskNames []*string) ([]interface{}, error) {
	allDisks, err := s.getDisksFromAllZones(nil)
	if err != nil {
		return nil, err
	}

	var disks []interface{}
	for _, id := range diskNames {
		if d, ok := allDisks[*id]; ok {
			disks = append(disks, d)
		} else {
			return nil, fmt.Errorf("disk %s not found", *id)
		}
	}
	return disks, nil
}

func (s *gceOps) RemoveTags(
	diskName string,
	labels map[string]string,
) error {
	d, err := s.computeService.Disks.Get(s.inst.project, s.inst.zone, diskName).Do()
	if err != nil {
		return err
	}

	if len(d.Labels) != 0 {
		currentLabels := d.Labels
		for k := range formatLabels(labels) {
			delete(currentLabels, k)
		}

		rb := &compute.ZoneSetLabelsRequest{
			LabelFingerprint: d.LabelFingerprint,
			Labels:           currentLabels,
		}

		operation, err := s.computeService.Disks.SetLabels(s.inst.project, s.inst.zone, d.Name, rb).Do()
		if err != nil {
			return err
		}
		return s.waitForOpCompletion("disk.SetLabels", s.inst.zone, operation)
	}

	return err
}

// SetClusterVersion sets desired version for the cluster
func (s *gceOps) SetClusterVersion(version string, timeout time.Duration) error {
	clusterPath := fmt.Sprintf("projects/%s/locations/%s/clusters/%s",
		s.inst.project, s.inst.clusterLocation, s.inst.clusterName)

	updateClusterRequest := &container.UpdateClusterRequest{
		Name: clusterPath,
		Update: &container.ClusterUpdate{
			DesiredMasterVersion: version,
		},
	}

	zonalCluster, err := isZonalCluster(s.inst.clusterLocation)
	if err != nil {
		return err
	}

	var operation *container.Operation
	if zonalCluster {
		operation, err = s.containerService.Projects.Zones.Clusters.Update(s.inst.project,
			s.inst.clusterLocation,
			s.inst.clusterName,
			updateClusterRequest).Do()
	} else {
		operation, err = s.containerService.Projects.Locations.Clusters.Update(
			clusterPath,
			updateClusterRequest).Do()
	}

	if err != nil {
		return err
	}

	return s.WaitForOperationCompletion(operation, zonalCluster, timeout)
}

// SetInstanceGroupVersion sets desired version for the node group
func (s *gceOps) SetInstanceGroupVersion(instanceGroupID string,
	version string,
	timeout time.Duration) error {
	clusterPath := fmt.Sprintf("projects/%s/locations/%s/clusters/%s",
		s.inst.project, s.inst.clusterLocation, s.inst.clusterName)

	updateClusterRequest := &container.UpdateClusterRequest{
		Name: clusterPath,
		Update: &container.ClusterUpdate{
			DesiredNodeVersion: version,
			DesiredNodePoolId:  instanceGroupID,
		},
	}

	zonalCluster, err := isZonalCluster(s.inst.clusterLocation)
	if err != nil {
		return err
	}

	var operation *container.Operation
	if zonalCluster {
		operation, err = s.containerService.Projects.Zones.Clusters.Update(s.inst.project,
			s.inst.clusterLocation,
			s.inst.clusterName,
			updateClusterRequest).Do()
	} else {
		operation, err = s.containerService.Projects.Locations.Clusters.Update(
			clusterPath,
			updateClusterRequest).Do()
	}

	if err != nil {
		return err
	}

	return s.WaitForOperationCompletion(operation, zonalCluster, timeout)
}

// SetInstanceGroupSize sets node count for a instance group.
// Count here is per availability zone
func (s *gceOps) SetInstanceGroupSize(instanceGroupID string,
	count int64, timeout time.Duration) error {
	clusterPath := fmt.Sprintf("projects/%s/locations/%s/clusters/%s",
		s.inst.project, s.inst.clusterLocation, s.inst.clusterName)
	nodePoolPath := fmt.Sprintf("%s/nodePools/%s",
		clusterPath, instanceGroupID)

	setSizeRequest := &container.SetNodePoolSizeRequest{
		Name:      nodePoolPath,
		NodeCount: count,
	}

	zonalCluster, err := isZonalCluster(s.inst.clusterLocation)
	if err != nil {
		return err
	}

	var operation *container.Operation
	if zonalCluster {
		operation, err = s.containerService.Projects.Zones.Clusters.NodePools.SetSize(
			s.inst.project,
			s.inst.clusterLocation,
			s.inst.clusterName,
			instanceGroupID,
			setSizeRequest).Do()

	} else {
		operation, err = s.containerService.Projects.Locations.Clusters.NodePools.SetSize(
			nodePoolPath,
			setSizeRequest).Do()

	}

	if err != nil {
		return err
	}

	return s.WaitForOperationCompletion(operation, zonalCluster, timeout)
}

func (s *gceOps) WaitForOperationCompletion(operation *container.Operation,
	zonalCluster bool,
	timeout time.Duration) error {
	var err error
	operationPath := fmt.Sprintf("projects/%s/locations/%s/operations/%s",
		s.inst.project, s.inst.clusterLocation, operation.Name)
	clusterPath := fmt.Sprintf("projects/%s/locations/%s/clusters/%s",
		s.inst.project, s.inst.clusterLocation, s.inst.clusterName)

	if timeout > time.Nanosecond {
		f := func() (interface{}, bool, error) {

			if zonalCluster {
				operation, err = s.containerService.Projects.Zones.Operations.Get(
					s.inst.project,
					s.inst.clusterLocation,
					operation.Name).Do()

			} else {
				operation, err = s.containerService.Projects.Locations.Operations.Get(
					operationPath).Do()
			}

			if err != nil {
				// Error occured, just retry
				return nil, true, err
			}

			// The operation is done, either cancelled or completed.
			if operation.Status == "DONE" {
				return nil, false, nil
			}

			return nil,
				true,
				fmt.Errorf("cluster operation [%v] is in [%s] state. Waiting to become DONE",
					operation.Name, operation.Status)
		}

		_, err = task.DoRetryWithTimeout(f, timeout, retrySeconds*time.Second)
		if err != nil {
			return err
		}
	}
	var cluster *container.Cluster
	if timeout > time.Nanosecond {
		f := func() (interface{}, bool, error) {

			if zonalCluster {
				cluster, err = s.containerService.Projects.Zones.Clusters.Get(
					s.inst.project,
					s.inst.clusterLocation,
					s.inst.clusterName).Do()

			} else {
				cluster, err = s.containerService.Projects.Locations.Clusters.Get(
					clusterPath).Do()

			}

			if err != nil {
				// Error occured, just retry
				return nil, true, err
			}

			if cluster.Status == "RUNNING" {
				return nil, false, nil
			}

			return nil,
				true,
				fmt.Errorf("cluster [%s] state is [%s]. Waiting to become running",
					s.inst.clusterName, cluster.Status)
		}

		_, err = task.DoRetryWithTimeout(f, timeout, retrySeconds*time.Second)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *gceOps) GetInstanceGroupSize(instanceGroupID string) (int64, error) {

	zonalCluster, err := isZonalCluster(s.inst.clusterLocation)
	if err != nil {
		return 0, err
	}

	var nodePool *container.NodePool
	if zonalCluster {
		nodePool, err = s.containerService.Projects.Zones.Clusters.NodePools.Get(
			s.inst.project, s.inst.clusterLocation, s.inst.clusterName, instanceGroupID).Do()
	} else {
		nodePoolPath := fmt.Sprintf("projects/%s/locations/%s/clusters/%s/nodePools/%s",
			s.inst.project, s.inst.clusterLocation, s.inst.clusterName, instanceGroupID)
		nodePool, err = s.containerService.Projects.Locations.Clusters.NodePools.Get(nodePoolPath).Do()
	}

	if err != nil {
		return 0, err
	}

	nodeCount := int64(0)
	for _, instanceGroupURL := range nodePool.InstanceGroupUrls {

		var zoneInfo, zone string
		nodeGrpName := strings.TrimSpace(filepath.Base(instanceGroupURL))

		temp := strings.SplitAfter(instanceGroupURL, "zones")
		if len(temp) > 1 {
			zoneInfo = temp[1]
		} else {
			return int64(0), fmt.Errorf("no zone information found from instance group url")
		}

		temp = strings.Split(zoneInfo, "/")
		if len(temp) > 1 {
			zone = temp[1]
		} else {
			return int64(0), fmt.Errorf("no zone information found from instance group url")
		}

		instGroup, err := s.computeService.InstanceGroups.Get(s.inst.project, zone, nodeGrpName).Do()
		if err != nil {
			return 0, err
		}
		nodeCount = nodeCount + instGroup.Size
	}

	return nodeCount, nil
}

func (s *gceOps) GetClusterSizeForInstance(instanceID string) (int64, error) {
	groupInfo, err := s.InspectInstanceGroupForInstance(instanceID)
	if err != nil {
		return int64(0), nil
	}

	clusterName, err := s.getClusterName()
	if err != nil {
		return int64(0), nil
	}

	var cluster *container.Cluster
	if groupInfo.Zone != "" {
		// Zonal cluster
		cluster, err = s.containerService.Projects.Zones.Clusters.Get(s.inst.project,
			groupInfo.Zone,
			clusterName).Do()
	} else {
		// Regional cluster
		clusterPath := fmt.Sprintf("projects/%s/locations/%s/clusters/%s",
			s.inst.project, groupInfo.Region, clusterName)

		cluster, err = s.containerService.Projects.Locations.Clusters.Get(clusterPath).Do()
	}

	if err != nil {
		return int64(0), err
	}

	return cluster.CurrentNodeCount, nil
}

func (s *gceOps) getClusterName() (string, error) {

	inst, err := s.computeService.Instances.Get(s.inst.project, s.inst.zone, s.inst.name).Do()
	if err != nil {
		return "", err
	}

	meta := inst.Metadata
	if meta == nil {
		return "", fmt.Errorf("instance doesn't have metadata set")
	}

	for _, item := range meta.Items {
		if item == nil {
			continue
		}

		if item.Key == clusterNameKey {
			if item.Value == nil {
				return "", fmt.Errorf("instance has %s key in metadata but has invalid value", clusterNameKey)
			}

			return *item.Value, nil
		}
	}
	return "", fmt.Errorf("cluster name not found for instance [%s]", s.inst.name)
}

func (s *gceOps) Snapshot(
	disk string,
	readonly bool,
) (interface{}, error) {
	rb := &compute.Snapshot{
		Name: fmt.Sprintf("snap-%d%02d%02d", time.Now().Year(), time.Now().Month(), time.Now().Day()),
	}

	operation, err := s.computeService.Disks.CreateSnapshot(s.inst.project, s.inst.zone, disk, rb).Do()
	if err != nil {
		return nil, err
	}

	if opErr := s.waitForOpCompletion("disk.CreateSnapshot", s.inst.zone, operation); opErr != nil {
		return nil, opErr
	}

	if err = s.checkSnapStatus(rb.Name, StatusReady); err != nil {
		return nil, err
	}

	snap, err := s.computeService.Snapshots.Get(s.inst.project, rb.Name).Do()
	if err != nil {
		return nil, err
	}

	return snap, err
}

func (s *gceOps) SnapshotDelete(snapID string) error {
	operation, err := s.computeService.Snapshots.Delete(s.inst.project, snapID).Do()
	if err != nil {
		return err
	}
	return s.waitForOpCompletion("snapshot.Delete", s.inst.zone, operation)
}

func (s *gceOps) Tags(diskName string) (map[string]string, error) {
	d, err := s.computeService.Disks.Get(s.inst.project, s.inst.zone, diskName).Do()
	if err != nil {
		return nil, err
	}

	return d.Labels, nil
}

func (s *gceOps) available(v *compute.Disk) bool {
	return strings.ToLower(v.Status) == StatusReady
}

func (s *gceOps) checkDiskStatus(id string, zone string, desired string) error {
	_, err := task.DoRetryWithTimeout(
		func() (interface{}, bool, error) {
			d, err := s.computeService.Disks.Get(s.inst.project, zone, id).Do()
			if err != nil {
				return nil, true, err
			}

			actual := strings.ToLower(d.Status)
			if len(actual) == 0 {
				return nil, true, fmt.Errorf("nil volume state for %v", id)
			}

			if actual != desired {
				return nil, true,
					fmt.Errorf("invalid status: %s for disk: %s. expected: %s",
						actual, id, desired)
			}

			return nil, false, nil
		},
		cloudops.ProviderOpsTimeout,
		cloudops.ProviderOpsRetryInterval)

	return err
}

func (s *gceOps) checkSnapStatus(id string, desired string) error {
	_, err := task.DoRetryWithTimeout(
		func() (interface{}, bool, error) {
			snap, err := s.computeService.Snapshots.Get(s.inst.project, id).Do()
			if err != nil {
				return nil, true, err
			}

			actual := strings.ToLower(snap.Status)
			if len(actual) == 0 {
				return nil, true, fmt.Errorf("nil snapshot state for %v", id)
			}

			if actual != desired {
				return nil, true,
					fmt.Errorf("invalid status: %s for snapshot: %s. expected: %s",
						actual, id, desired)
			}

			return nil, false, nil
		},
		cloudops.ProviderOpsTimeout,
		cloudops.ProviderOpsRetryInterval)

	return err
}

// Describe current instance.
func (s *gceOps) Describe() (interface{}, error) {
	return s.describeinstance()
}

func (s *gceOps) describeinstance() (*compute.Instance, error) {
	return s.computeService.Instances.Get(s.inst.project, s.inst.zone, s.inst.name).Do()
}

// gceInfo fetches the GCE instance metadata from the metadata server
func gceInfo(inst *instance) error {
	var err error
	inst.zone, err = metadata.Zone()
	if err != nil {
		return err
	}

	inst.region = inst.zone[:len(inst.zone)-2]

	inst.name, err = metadata.InstanceName()
	if err != nil {
		return err
	}

	inst.hostname, err = metadata.Hostname()
	if err != nil {
		return err
	}

	inst.project, err = metadata.ProjectID()
	if err != nil {
		return err
	}

	inst.clusterName, err = metadata.InstanceAttributeValue(clusterNameKey)
	if err != nil {
		// No need to error out for non-GKE compute instances
		logrus.Warnf("no '%s' instance attribute found", clusterNameKey)
	}

	inst.clusterLocation, err = metadata.InstanceAttributeValue(clusterLocationKey)
	if err != nil {
		// No need to error out for non-GKE compute instances
		logrus.Warnf("no '%s' instance attribute found", clusterLocationKey)
	}

	kubeLabels, err := metadata.InstanceAttributeValue(kubeLabelsKey)
	if err != nil {
		// No need to error out for non-GKE compute instances
		logrus.Warnf("no '%s' instance attribute found", kubeLabelsKey)
	} else {
		kubeLabelList, err := parser.LabelsFromString(kubeLabels)
		if err != nil {
			return err
		}

		for labelKey, labelValue := range kubeLabelList {
			if labelKey == nodePoolKey {
				inst.nodePoolID = labelValue
			}
		}
	}
	return nil
}

func gceInfoFromEnv(inst *instance) error {
	var err error
	inst.name, err = cloudops.GetEnvValueStrict("GCE_INSTANCE_NAME")
	if err != nil {
		return err
	}

	inst.zone, err = cloudops.GetEnvValueStrict("GCE_INSTANCE_ZONE")
	if err != nil {
		return err
	}

	inst.region = inst.zone[:len(inst.zone)-2]

	inst.project, err = cloudops.GetEnvValueStrict("GCE_INSTANCE_PROJECT")
	if err != nil {
		return err
	}

	inst.clusterName, _ = cloudops.GetEnvValueStrict("GKE_CLUSTER_NAME")
	inst.clusterLocation, _ = cloudops.GetEnvValueStrict("GKE_CLUSTER_LOCATION")
	inst.nodePoolID, _ = cloudops.GetEnvValueStrict("GKE_NODE_POOL")

	return nil
}

func (s *gceOps) rollbackCreate(id string, createErr error) error {
	logrus.Warnf("Rollback create volume %v, Error %v", id, createErr)
	err := s.Delete(id)
	if err != nil {
		logrus.Warnf("Rollback failed volume %v, Error %v", id, err)
	}
	return createErr
}

// waitForDetach checks if given disk is detached from the local instance
func (s *gceOps) waitForDetach(
	diskURL string,
	timeout time.Duration,
) error {

	_, err := task.DoRetryWithTimeout(
		func() (interface{}, bool, error) {
			inst, err := s.describeinstance()
			if err != nil {
				return nil, true, err
			}

			for _, d := range inst.Disks {
				if d.Source == diskURL {
					return nil, true,
						fmt.Errorf("disk: %s is still attached to instance: %s",
							diskURL, s.inst.name)
				}
			}

			return nil, false, nil

		},
		cloudops.ProviderOpsTimeout,
		cloudops.ProviderOpsRetryInterval)

	return err
}

// waitForAttach checks if given disk is attached to the local instance
func (s *gceOps) waitForAttach(
	disk *compute.Disk,
	timeout time.Duration,
) (string, error) {
	devicePath, err := task.DoRetryWithTimeout(
		func() (interface{}, bool, error) {
			devicePath, err := s.DevicePath(disk.Name)
			if se, ok := err.(*cloudops.StorageError); ok &&
				se.Code == cloudops.ErrVolAttachedOnRemoteNode {
				return "", false, err
			} else if err != nil {
				return "", true, err
			}

			return devicePath, false, nil
		},
		cloudops.ProviderOpsTimeout,
		cloudops.ProviderOpsRetryInterval)
	if err != nil {
		return "", err
	}

	return devicePath.(string), nil
}

// waitForOpCompletion is a blocking function that can be used to check the status
// of any gce service call. It checks the status of the operation object passed
// in as argument. It will keep on retrying until
// 1. gce service returns that the operation has been completed
// 2. the retry timeout is hit
// this code has been inspired from kubernetes cloudprovider for gce
// k8s.io/kubernetes/pkg/cloudprovider/providers/gce/cloud
func (s *gceOps) waitForOpCompletion(
	cloudopsOperationName string,
	opZone string,
	operation *compute.Operation,
) error {
	_, gceOpErr := task.DoRetryWithTimeout(
		func() (interface{}, bool, error) {
			// get the status of the operation
			op, err := s.computeService.ZoneOperations.Get(s.inst.project, opZone, operation.Name).Do()
			if err != nil {
				// failed to get operation status
				// check again later
				if gErr, ok := err.(*googleapi.Error); ok {
					if gErr.Code == int(404) {
						// operation does not exist
						return nil, false, nil
					}
				}
				return nil, true, fmt.Errorf("failed to query gce operation %v for %v: %v", operation.Name, cloudopsOperationName, err)
			}

			if op == nil || op.Status != doneStatus {
				// operation is not done
				// check again later
				return nil, true, fmt.Errorf("gce operation %v for %v not completed", operation.Name, cloudopsOperationName)
			}

			if op.Error != nil && len(op.Error.Errors) > 0 && op.Error.Errors[0] != nil {
				// operation is done
				// and we got an error
				return nil, false, &googleapi.Error{
					Code:    int(op.HttpErrorStatusCode),
					Message: fmt.Sprintf("%v - %v", op.Error.Errors[0].Code, op.Error.Errors[0].Message),
				}
			}
			// operation is done with no error
			logrus.Infof("gce operation %v for %v successfully completed", operation.Name, cloudopsOperationName)
			return nil, false, nil
		},
		cloudops.ProviderOpsTimeout,
		cloudops.ProviderOpsRetryInterval,
	)
	return gceOpErr
}

// generateListFilterFromLabels create a filter string based off --filter documentation at
// https://cloud.google.com/sdk/gcloud/reference/compute/disks/list
func generateListFilterFromLabels(labels map[string]string) string {
	var filter string
	for k, v := range labels {
		filter = fmt.Sprintf("%s(labels.%s eq %s)", filter, k, v)
	}

	return filter
}

func (s *gceOps) getDisksFromAllZones(labels map[string]string) (map[string]*compute.Disk, error) {
	ctx := context.Background()
	response := make(map[string]*compute.Disk)
	var req *compute.DisksAggregatedListCall

	if len(labels) > 0 {
		filter := generateListFilterFromLabels(labels)
		req = s.computeService.Disks.AggregatedList(s.inst.project).Filter(filter)
	} else {
		req = s.computeService.Disks.AggregatedList(s.inst.project)
	}

	if err := req.Pages(ctx, func(page *compute.DiskAggregatedList) error {
		for _, diskScopedList := range page.Items {
			for _, disk := range diskScopedList.Disks {
				response[disk.Name] = disk
			}
		}

		return nil
	}); err != nil {
		logrus.Errorf("failed to list disks: %v", err)
		return nil, err
	}

	return response, nil
}

func (s *gceOps) diskIDToBlockDevPathWithRetry(devPath string) (string, error) {
	var (
		retryCount int
		path       string
		err        error
	)

	for {
		if path, err = s.diskIDToBlockDevPath(devPath); err == nil {
			return path, nil
		}
		logrus.Warnf(err.Error())
		retryCount++
		if retryCount >= devicePathMaxRetryCount {
			break
		}
		time.Sleep(devicePathRetryInterval)
	}
	return "", err
}

func (s *gceOps) diskIDToBlockDevPath(devPath string) (string, error) {
	// check if path is a sym link. If yes, return pointee
	fi, err := os.Lstat(devPath)
	if err != nil {
		return "", err
	}

	if fi.Mode()&os.ModeSymlink != 0 {
		output, err := filepath.EvalSymlinks(devPath)
		if err != nil {
			return "", fmt.Errorf("failed to read symlink due to: %v", err)
		}

		devPath = strings.TrimSpace(string(output))
	} else {
		return "", fmt.Errorf("%s was expected to be a symlink to actual "+
			"device path", devPath)
	}

	return devPath, nil
}

func formatLabels(labels map[string]string) map[string]string {
	newLabels := make(map[string]string)
	for k, v := range labels {
		newLabels[strings.ToLower(k)] = strings.ToLower(v)
	}
	return newLabels
}

func isExponentialError(err error) bool {
	// Got the list of error codes from here
	// https://cloud.google.com/apis/design/errors#handling_errors
	gceCodes := map[int]struct{}{
		int(429): {},
	}
	if err != nil {
		if gceErr, ok := err.(*googleapi.Error); ok {
			if _, exist := gceCodes[gceErr.Code]; exist {
				return true
			}
		}
	}
	return false
}

func isZonalCluster(clusterLocation string) (bool, error) {
	// Zone e.g. us-central1-a
	zoneRegex := "[a-zA-z0-9]+-[a-zA-Z0-9]+-[a-zA-Z]"
	return regexp.MatchString(zoneRegex, clusterLocation)
}
