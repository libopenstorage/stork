package oracle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/libopenstorage/cloudops"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/containerengine"
	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
)

const (
	v1MetadataAPIEndpoint = "http://169.254.169.254/opc/v1/instance/"
	v2MetadataAPIEndpoint = "http://169.254.169.254/opc/v2/instance/"
	metadataInstanceIDkey = "id"
	// MetadataRegionKey key name for region in metadata JSON
	// returned by IMDS service
	MetadataRegionKey = "canonicalRegionName"
	// MetadataAvailabilityDomainKey key name for availability domain
	// in metadata JSON returned by IMDS service
	MetadataAvailabilityDomainKey = "availabilityDomain"
	// MetadataCompartmentIDkey key name for compartmentID
	// in metadata JSON returned by IMDS service
	MetadataCompartmentIDkey = "compartmentId"
	// MetadataKey key name in metadata json for metadata returned by IMDS service
	MetadataKey = "metadata"
	// MetadataUserDataKey key name in metadata json for user data
	MetadataUserDataKey   = "user_data"
	metadataTenancyIDKey  = "oke-tenancy-id"
	metadataPoolIDKey     = "oke-pool-id"
	metadataClusterIDKey  = "oke-cluster-id"
	envPrefix             = "PX_ORACLE"
	envInstanceID         = "INSTANCE_ID"
	envRegion             = "INSTANCE_REGION"
	envAvailabilityDomain = "INSTNACE_AVAILABILITY_DOMAIN"
	envCompartmentID      = "COMPARTMENT_ID"
	envTenancyID          = "TENANCY_ID"
	envPoolID             = "POOL_ID"
	envClusterID          = "CLUSTER_ID"
	defaultTimeout        = 5 * time.Minute
)

type oracleOps struct {
	cloudops.Compute
	cloudops.Storage
	instance                string
	region                  string
	availabilityDomain      string
	compartmentID           string
	tenancyID               string
	poolID                  string
	clusterID               string
	volumeAttachmentMapping map[string]*string
	storage                 core.BlockstorageClient
	compute                 core.ComputeClient
	containerEngine         containerengine.ContainerEngineClient
	mutex                   sync.Mutex
}

// NewClient creates a new cloud operations client for Oracle cloud
func NewClient() (cloudops.Ops, error) {
	oracleOps := &oracleOps{}
	err := getInfoFromMetadata(oracleOps)
	if err != nil {
		err = getInfoFromEnv(oracleOps)
		if err != nil {
			return nil, err
		}
	}
	os.Setenv(fmt.Sprintf("%s_tenancy_ocid", envPrefix), oracleOps.tenancyID)
	os.Setenv(fmt.Sprintf("%s_region", envPrefix), oracleOps.region)
	configProvider := common.ConfigurationProviderEnvironmentVariables(envPrefix, "")
	oracleOps.storage, err = core.NewBlockstorageClientWithConfigurationProvider(configProvider)
	if err != nil {
		return nil, err
	}
	oracleOps.compute, err = core.NewComputeClientWithConfigurationProvider(configProvider)
	if err != nil {
		return nil, err
	}
	oracleOps.containerEngine, err = containerengine.NewContainerEngineClientWithConfigurationProvider(configProvider)
	if err != nil {
		return nil, err
	}

	oracleOps.volumeAttachmentMapping = map[string]*string{}
	// TODO: [PWX-18717] wrap around exponentialBackoffOps
	return oracleOps, nil
}

func getInfoFromEnv(oracleOps *oracleOps) error {
	var err error
	oracleOps.instance, err = cloudops.GetEnvValueStrict(envInstanceID)
	if err != nil {
		return err
	}

	oracleOps.region, err = cloudops.GetEnvValueStrict(envRegion)
	if err != nil {
		return err
	}

	oracleOps.availabilityDomain, err = cloudops.GetEnvValueStrict(envAvailabilityDomain)
	if err != nil {
		return err
	}

	oracleOps.compartmentID, err = cloudops.GetEnvValueStrict(envCompartmentID)
	if err != nil {
		return err
	}

	oracleOps.tenancyID, err = cloudops.GetEnvValueStrict(envTenancyID)
	if err != nil {
		return err
	}

	oracleOps.poolID, err = cloudops.GetEnvValueStrict(envPoolID)
	if err != nil {
		return err
	}

	oracleOps.clusterID, err = cloudops.GetEnvValueStrict(envClusterID)
	if err != nil {
		return err
	}
	return nil
}

func getRequest(endpoint string, headers map[string]string) (map[string]interface{}, int, error) {
	metadata := make(map[string]interface{})
	client := &http.Client{
		Timeout: 15 * time.Second,
	}
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return metadata, 0, err
	}

	for headerKey, headerValue := range headers {
		req.Header.Add(headerKey, headerValue)
	}
	q := req.URL.Query()
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if err != nil {
		errMsg := fmt.Errorf("metadata lookup from [%s] endpoint failed with error:[%v]", endpoint, err)
		if resp != nil {
			return metadata, resp.StatusCode, errMsg
		}
		return metadata, http.StatusNotFound, errMsg
	}
	if resp.StatusCode != http.StatusOK {
		return metadata, resp.StatusCode, nil
	}
	if resp.Body != nil {
		defer resp.Body.Close()
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return metadata, resp.StatusCode,
				fmt.Errorf("error while reading Oracle metadata response: [%v]", err)
		}
		if len(respBody) == 0 {
			return metadata, resp.StatusCode,
				fmt.Errorf("error querying Oracle metadata: Empty response")
		}

		err = json.Unmarshal(respBody, &metadata)
		if err != nil {
			return metadata, resp.StatusCode,
				fmt.Errorf("error parsing Oracle metadata: %v", err)
		}
	}
	return metadata, resp.StatusCode, nil
}

// GetMetadata returns metadata from IMDS
func GetMetadata() (map[string]interface{}, error) {
	httpHeaders := map[string]string{}
	httpHeaders["Authorization"] = "Bearer Oracle"
	var httpStatusCode int
	var err error
	var metadata map[string]interface{}
	metadata, httpStatusCode, err = getRequest(v2MetadataAPIEndpoint, httpHeaders)
	if err != nil {
		return nil, err
	}

	if httpStatusCode != http.StatusOK {
		logrus.Warnf("Trying %s endpoint as got %d http response from %s\n",
			v1MetadataAPIEndpoint, httpStatusCode, v2MetadataAPIEndpoint)
		metadata, httpStatusCode, err = getRequest(v1MetadataAPIEndpoint, map[string]string{})
		if err != nil {
			return nil, err
		}
	}
	if httpStatusCode != http.StatusOK {
		return metadata,
			fmt.Errorf("error:[%v] got HTTP Code %d", err, httpStatusCode)
	}
	return metadata, nil
}

func getInfoFromMetadata(oracleOps *oracleOps) error {
	var tenancyID, poolID, clusterID string
	var ok bool
	metadata, err := GetMetadata()
	if err != nil {
		return err
	}
	if metadata[MetadataKey] != nil {
		if okeMetadata, ok := metadata[MetadataKey].(map[string]interface{}); ok {
			if okeMetadata[metadataTenancyIDKey] != nil {
				if tenancyID, ok = okeMetadata[metadataTenancyIDKey].(string); !ok {
					return fmt.Errorf("can not get tenancy ID from oracle metadata service. error: [%v]", err)
				}
				if poolID, ok = okeMetadata[metadataPoolIDKey].(string); !ok {
					return fmt.Errorf("can not get pool ID from oracle metadata service. error: [%v]", err)
				}
				if clusterID, ok = okeMetadata[metadataClusterIDKey].(string); !ok {
					return fmt.Errorf("can not get cluster ID from oracle metadata service. error: [%v]", err)
				}
			}
		} else {
			return fmt.Errorf("can not get OKE metadata from oracle metadata service. error: [%v]", err)
		}
	}
	oracleOps.tenancyID = tenancyID
	oracleOps.poolID = poolID
	oracleOps.clusterID = clusterID
	if oracleOps.instance, ok = metadata[metadataInstanceIDkey].(string); !ok {
		return fmt.Errorf("can not get instance id from oracle metadata service. error: [%v]", err)
	}
	if oracleOps.region, ok = metadata[MetadataRegionKey].(string); !ok {
		return fmt.Errorf("can not get region from oracle metadata service. error: [%v]", err)
	}
	if oracleOps.availabilityDomain, ok = metadata[MetadataAvailabilityDomainKey].(string); !ok {
		return fmt.Errorf("can not get instance availability domain from oracle metadata service. error: [%v]", err)
	}
	if oracleOps.compartmentID, ok = metadata[MetadataCompartmentIDkey].(string); !ok {
		return fmt.Errorf("can not get compartment ID from oracle metadata service. error: [%v]", err)
	}
	return nil
}

func (o *oracleOps) Name() string { return string(cloudops.Oracle) }

func (o *oracleOps) InstanceID() string { return o.instance }

func (o *oracleOps) InspectInstance(instanceID string) (*cloudops.InstanceInfo, error) {

	instance := core.GetInstanceRequest{
		InstanceId: &instanceID,
	}
	resp, err := o.compute.GetInstance(context.Background(), instance)
	if err != nil {
		return nil, err
	}

	return &cloudops.InstanceInfo{
		CloudResourceInfo: cloudops.CloudResourceInfo{
			Name:   string(*resp.DisplayName),
			ID:     instanceID,
			Region: *resp.Region,
			Zone:   *resp.AvailabilityDomain,
		},
	}, nil
}

func (o *oracleOps) GetInstance(displayName string) (interface{}, error) {
	listInstanceReq := core.ListInstancesRequest{
		DisplayName:   common.String(displayName),
		CompartmentId: common.String(o.compartmentID),
	}

	listInstanceResp, err := o.compute.ListInstances(context.Background(), listInstanceReq)
	if err != nil {
		return nil, err
	}
	if len(listInstanceResp.Items) == 0 {
		return nil, fmt.Errorf("no oracle instance found with display name: %s", displayName)
	}
	// Currently, torpedo uses this API to fetch details of the instance created 
	// by OKE. OKE ensures that all the worker nodes created, have unique display
	// names. In future, if we require to use this API to get details of vanilla
	// compute instances, then we can modify below array indexing.
	return listInstanceResp.Items[0], nil
}

func (o *oracleOps) InspectInstanceGroupForInstance(instanceID string) (*cloudops.InstanceGroupInfo, error) {
	getNodePoolReq := containerengine.GetNodePoolRequest{
		NodePoolId: &o.poolID,
	}

	nodePoolDetails, err := o.containerEngine.GetNodePool(context.Background(), getNodePoolReq)
	if err != nil {
		return nil, err
	}

	zones := []string{}
	for _, placementConfig := range nodePoolDetails.NodePool.NodeConfigDetails.PlacementConfigs {
		zones = append(zones, *placementConfig.AvailabilityDomain)
	}
	size := int64(*nodePoolDetails.NodeConfigDetails.Size)

	return &cloudops.InstanceGroupInfo{
		CloudResourceInfo: cloudops.CloudResourceInfo{
			Name:   *nodePoolDetails.Name,
			ID:     o.poolID,
			Region: o.region,
		},
		Min:   &size,
		Max:   &size,
		Zones: zones,
	}, nil
}

func (o *oracleOps) Describe() (interface{}, error) {
	getInstanceReq := core.GetInstanceRequest{
		InstanceId: &o.instance,
	}
	resp, err := o.compute.GetInstance(context.Background(), getInstanceReq)
	if err != nil {
		return nil, err
	}
	return resp.Instance, nil
}

func (o *oracleOps) DeviceMappings() (map[string]string, error) {
	m := make(map[string]string)
	var devicePath, volID string
	volumeAttachmentReq := core.ListVolumeAttachmentsRequest{
		CompartmentId: common.String(o.compartmentID),
		InstanceId:    common.String(o.instance),
	}
	volumeAttachmentResp, err := o.compute.ListVolumeAttachments(context.Background(), volumeAttachmentReq)
	if err != nil {
		return m, err
	}
	for _, va := range volumeAttachmentResp.Items {
		if va.GetLifecycleState() == core.VolumeAttachmentLifecycleStateAttached {
			if va.GetDevice() != nil && va.GetVolumeId() != nil {
				devicePath = *va.GetDevice()
				volID = *va.GetVolumeId()
			} else {
				logrus.Warnf("Device path or volume id for [%+v] volume attachment not found", va)
				continue
			}
		}
		m[devicePath] = volID
	}
	return m, nil
}

func (o *oracleOps) DevicePath(volumeID string) (string, error) {
	volumeAttachmentReq := core.ListVolumeAttachmentsRequest{
		CompartmentId: common.String(o.compartmentID),
		VolumeId:      common.String(volumeID),
	}

	volumeAttachmentResp, err := o.compute.ListVolumeAttachments(context.Background(), volumeAttachmentReq)
	if err != nil {
		return "", err
	}

	if volumeAttachmentResp.Items == nil ||
		len(volumeAttachmentResp.Items) == 0 ||
		volumeAttachmentResp.Items[0].GetInstanceId() == nil ||
		volumeAttachmentResp.Items[0].GetLifecycleState() == core.VolumeAttachmentLifecycleStateDetached ||
		volumeAttachmentResp.Items[0].GetLifecycleState() == core.VolumeAttachmentLifecycleStateDetaching {
		return "", cloudops.NewStorageError(cloudops.ErrVolDetached,
			"Volume is detached", volumeID)
	}

	volumeAttachment := volumeAttachmentResp.Items[0]

	if o.instance != *volumeAttachment.GetInstanceId() {
		return "", cloudops.NewStorageError(cloudops.ErrVolAttachedOnRemoteNode,
			fmt.Sprintf("Volume attached on %q current instance %q",
				*volumeAttachment.GetInstanceId(), o.instance),
			*volumeAttachment.GetInstanceId())
	}

	if volumeAttachment.GetLifecycleState() != core.VolumeAttachmentLifecycleStateAttached {
		return "", cloudops.NewStorageError(cloudops.ErrVolInval,
			fmt.Sprintf("Invalid state %q, volume is not attached",
				volumeAttachment.GetLifecycleState()), "")
	}

	if volumeAttachment.GetDevice() == nil {
		return "", cloudops.NewStorageError(cloudops.ErrVolInval,
			"Unable to determine volume attachment path", "")
	}

	return *volumeAttachment.GetDevice(), nil
}

// Inspect volumes specified by volumeID
func (o *oracleOps) Inspect(volumeIds []*string) ([]interface{}, error) {
	oracleVols := []interface{}{}
	for _, volID := range volumeIds {
		getVolReq := core.GetVolumeRequest{
			VolumeId: volID,
		}
		getVolResp, err := o.storage.GetVolume(context.Background(), getVolReq)
		if err != nil {
			return nil, err
		}
		oracleVols = append(oracleVols, &getVolResp.Volume)
	}
	return oracleVols, nil
}

// Create volume based on input template volume and also apply given labels.
func (o *oracleOps) Create(template interface{}, labels map[string]string) (interface{}, error) {
	vol, ok := template.(*core.Volume)
	if !ok {
		return nil, cloudops.NewStorageError(cloudops.ErrVolInval,
			"Invalid volume template given", "")
	}

	createVolReq := core.CreateVolumeRequest{
		CreateVolumeDetails: core.CreateVolumeDetails{
			CompartmentId:      &o.compartmentID,
			AvailabilityDomain: &o.availabilityDomain,
			SizeInGBs:          vol.SizeInGBs,
			VpusPerGB:          vol.VpusPerGB,
			DisplayName:        vol.DisplayName,
			FreeformTags:       labels,
		},
	}
	createVolResp, err := o.storage.CreateVolume(context.Background(), createVolReq)
	if err != nil {
		if strings.Contains(err.Error(), "vpusPerGB is invalid") {
			return nil, fmt.Errorf("VPUs must be an integer that is multiple of 10 " +
				"Please refer to oracle cloud block volume documentation for valid values")
		}
		return nil, err
	}

	oracleVol, err := o.waitVolumeStatus(*createVolResp.Id, core.VolumeLifecycleStateAvailable)
	if err != nil {
		return nil, o.rollbackCreate(*createVolResp.Id, err)
	}
	return oracleVol, nil
}

func (o *oracleOps) waitVolumeStatus(volID string, desiredStatus core.VolumeLifecycleStateEnum) (interface{}, error) {
	getVolReq := core.GetVolumeRequest{
		VolumeId: &volID,
	}
	f := func() (interface{}, bool, error) {
		getVolResp, err := o.storage.GetVolume(context.Background(), getVolReq)
		if err != nil {
			return nil, true, err
		}
		if getVolResp.Volume.LifecycleState == desiredStatus {
			return &getVolResp.Volume, false, nil
		}

		logrus.Debugf("volume [%s] is still in [%s] state", volID, getVolResp.Volume.LifecycleState)
		return nil, true, fmt.Errorf("volume [%s] is still in [%s] state", volID, getVolResp.Volume.LifecycleState)
	}
	oracleVol, err := task.DoRetryWithTimeout(f, cloudops.ProviderOpsTimeout, cloudops.ProviderOpsRetryInterval)
	return oracleVol, err
}

func (o *oracleOps) rollbackCreate(id string, createErr error) error {
	logrus.Warnf("Rollback create volume %v, Error %v", id, createErr)
	err := o.Delete(id)
	if err != nil {
		logrus.Warnf("Rollback failed volume %v, Error %v", id, err)
	}
	return createErr
}

// Delete volumeID.
func (o *oracleOps) Delete(volumeID string) error {
	delVolReq := core.DeleteVolumeRequest{
		VolumeId: &volumeID,
	}
	delVolResp, err := o.storage.DeleteVolume(context.Background(), delVolReq)
	if err != nil {
		logrus.Errorf("failed to delete volume [%s]. Response: [%v], Error: [%v]", volumeID, delVolResp, err)
		return err
	}
	return nil
}

func (o *oracleOps) SetInstanceGroupSize(instanceGroupID string, count int64, timeout time.Duration) error {

	if timeout == 0*time.Second {
		timeout = defaultTimeout
	}

	instanceGroupSize := int(count)

	//get nodepool by ID to be updated
	nodePoolReq := containerengine.ListNodePoolsRequest{CompartmentId: &o.compartmentID, Name: &instanceGroupID, ClusterId: &o.clusterID}
	nodePools, err := o.containerEngine.ListNodePools(context.Background(), nodePoolReq)
	if err != nil {
		return err
	}

	if len(nodePools.Items) == 0 {
		return errors.New("No node pool found with name " + instanceGroupID)
	}
	numberOfDomains := len(nodePools.Items[0].NodeConfigDetails.PlacementConfigs)
	totalClusterSize := numberOfDomains * instanceGroupSize
	logrus.Println("Setting instanceGroupSize to ", totalClusterSize, " in total ", numberOfDomains, " regions.")

	//get all availabliity domain
	nodePoolPlacementConfigDetails := make([]containerengine.NodePoolPlacementConfigDetails, numberOfDomains)

	for i, placementConfigs := range nodePools.Items[0].NodeConfigDetails.PlacementConfigs {
		nodePoolPlacementConfigDetails[i].AvailabilityDomain = placementConfigs.AvailabilityDomain
		nodePoolPlacementConfigDetails[i].SubnetId = placementConfigs.SubnetId
	}

	//update node pools
	req := containerengine.UpdateNodePoolRequest{
		NodePoolId: nodePools.Items[0].Id, //get node pool id
		UpdateNodePoolDetails: containerengine.UpdateNodePoolDetails{
			NodeConfigDetails: &containerengine.UpdateNodePoolNodeConfigDetails{
				Size:             &totalClusterSize,
				PlacementConfigs: nodePoolPlacementConfigDetails,
			},
		},
	}

	resp, err := o.containerEngine.UpdateNodePool(context.Background(), req)
	if err != nil {
		return err
	}

	err = o.waitTillWorkStatusIsSucceeded(resp.OpcRequestId, resp.OpcWorkRequestId, timeout)
	if err != nil {
		return err
	}

	return nil
}

func (o *oracleOps) waitTillWorkStatusIsSucceeded(opcRequestID, opcWorkRequestID *string, timeout time.Duration) error {
	workReq := containerengine.GetWorkRequestRequest{OpcRequestId: opcRequestID,
		WorkRequestId: opcWorkRequestID}

	f := func() (interface{}, bool, error) {
		workResp, err := o.containerEngine.GetWorkRequest(context.Background(), workReq)
		if err != nil {
			return nil, true, err
		}

		if workResp.Status == containerengine.WorkRequestStatusSucceeded {
			return workResp.Status, false, nil
		}

		logrus.Debugf("Work status is in [%s] state", workResp.Status)
		return nil, true, fmt.Errorf("Work status is in [%s] state", workResp.Status)
	}
	_, err := task.DoRetryWithTimeout(f, timeout, 10*time.Second)
	return err
}

func (o *oracleOps) GetInstanceGroupSize(instanceGroupID string) (int64, error) {

	var count int64

	nodePoolReq := containerengine.ListNodePoolsRequest{CompartmentId: &o.compartmentID, Name: &instanceGroupID, ClusterId: &o.clusterID}
	nodePools, err := o.containerEngine.ListNodePools(context.Background(), nodePoolReq)
	if err != nil {
		return 0, err
	}

	if len(nodePools.Items) == 0 {
		return 0, errors.New("No node pool found with name " + instanceGroupID)
	}

	req := containerengine.GetNodePoolRequest{NodePoolId: nodePools.Items[0].Id}

	resp, err := o.containerEngine.GetNodePool(context.Background(), req)

	if err != nil {
		return 0, err
	}

	for _, node := range resp.Nodes {
		if node.LifecycleState == containerengine.NodeLifecycleStateActive {
			count++
		}
	}
	return count, nil
}

// Attach volumeID, accepts attachOptions as opaque data
// Return attach path.
func (o *oracleOps) Attach(volumeID string, options map[string]string) (string, error) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	devices, err := o.FreeDevices([]interface{}{}, "")
	if err != nil {
		return "", err
	}

	for _, device := range devices {
		attachVolReq := core.AttachVolumeRequest{
			AttachVolumeDetails: core.AttachParavirtualizedVolumeDetails{
				InstanceId:  common.String(o.instance),
				VolumeId:    common.String(volumeID),
				Device:      common.String(device),
				IsShareable: common.Bool(false),
				IsReadOnly:  common.Bool(false),
			},
		}

		attachVolResp, err := o.compute.AttachVolume(context.Background(), attachVolReq)
		if err != nil {
			if strings.Contains(err.Error(), "is already in use") {
				logrus.Infof("Skipping device: %s as it's in use. Will try next free device", device)
				continue
			}
			return "", err
		}

		var devicePath string
		if attachVolResp.GetLifecycleState() != core.VolumeAttachmentLifecycleStateAttached {
			devicePath, err = o.waitVolumeAttachmentStatus(
				attachVolResp.GetId(),
				core.VolumeAttachmentLifecycleStateAttached,
			)
			if err != nil {
				devicePath, err := o.DevicePath(volumeID)
				if err != nil {
					return "", err
				}
				o.volumeAttachmentMapping[volumeID] = attachVolResp.GetId()
				return devicePath, err
			}
		} else {
			devicePath = *attachVolResp.GetDevice()
		}
		o.volumeAttachmentMapping[volumeID] = attachVolResp.GetId()
		return devicePath, err
	}
	return "", fmt.Errorf("failed to attach any of the free devices. Attempted: %v", devices)
}

func (o *oracleOps) waitVolumeAttachmentStatus(volumeAttachmentID *string, desiredStatus core.VolumeAttachmentLifecycleStateEnum) (string, error) {
	getVolAttachmentReq := core.GetVolumeAttachmentRequest{
		VolumeAttachmentId: volumeAttachmentID,
	}
	f := func() (interface{}, bool, error) {
		getVolAttachmentResp, err := o.compute.GetVolumeAttachment(context.Background(), getVolAttachmentReq)
		if err != nil {
			return nil, true, err
		}
		if getVolAttachmentResp.GetLifecycleState() == desiredStatus {
			return getVolAttachmentResp.GetDevice(), false, nil
		}

		logrus.Debugf("volume [%s] is still in [%s] state", *getVolAttachmentResp.GetVolumeId(), getVolAttachmentResp.GetLifecycleState())
		return nil, true, fmt.Errorf("volume [%s] is still in [%s] state", *getVolAttachmentResp.GetVolumeId(), getVolAttachmentResp.GetLifecycleState())
	}
	devicePathRaw, err := task.DoRetryWithTimeout(f, cloudops.ProviderOpsTimeout, cloudops.ProviderOpsRetryInterval)
	if err != nil {
		return "", err
	}
	devicePath, ok := devicePathRaw.(*string)
	if !ok {
		return "", fmt.Errorf("volume attachment [%s] attached successfully but could not get it's local device path", *volumeAttachmentID)
	}
	return *devicePath, err
}

// Detach volumeID.
func (o *oracleOps) Detach(volumeID string) error {
	return o.detachInternal(volumeID, o.instance)
}

// DetachFrom detaches the disk/volume with given ID from the given instance ID
func (o *oracleOps) DetachFrom(volumeID, instanceID string) error {
	return o.detachInternal(volumeID, instanceID)
}

func (o *oracleOps) detachInternal(volumeID, instanceID string) error {
	attachmentID, ok := o.volumeAttachmentMapping[volumeID]
	if !ok {
		logrus.Warnf("could not find volume attachment ID for volume [%s] locally", volumeID)
		listVolAttachmentReq := core.ListVolumeAttachmentsRequest{
			VolumeId:           common.String(volumeID),
			InstanceId:         common.String(instanceID),
			CompartmentId:      common.String(o.compartmentID),
			AvailabilityDomain: common.String(o.availabilityDomain),
		}
		listVolAttachmentResp, err := o.compute.ListVolumeAttachments(context.Background(), listVolAttachmentReq)
		if err != nil {
			logrus.Errorf("error while getting attachments for volume [%s]. Response: [%+v]. Error: [%v]",
				volumeID, listVolAttachmentResp, err)
			return err
		}
		if len(listVolAttachmentResp.Items) > 0 {
			attachmentID = listVolAttachmentResp.Items[0].GetId()
		} else {
			return fmt.Errorf("volume [%s] is not attached to node [%s]", volumeID, instanceID)
		}
	}
	detachVolReq := core.DetachVolumeRequest{
		VolumeAttachmentId: attachmentID,
	}
	detachVolResp, err := o.compute.DetachVolume(context.Background(), detachVolReq)
	if err != nil {
		logrus.Errorf("error while detaching volume [%s] from instance [%s]. Response: [%+v]. Error: [%v]",
			volumeID, instanceID, detachVolResp, err)
		return err
	}
	_, err = o.waitVolumeAttachmentStatus(
		attachmentID,
		core.VolumeAttachmentLifecycleStateDetached,
	)
	if err == nil {
		o.mutex.Lock()
		delete(o.volumeAttachmentMapping, volumeID)
		o.mutex.Unlock()
	}
	return err
}

// FreeDevices returns free block devices on the instance.
// blockDeviceMappings is a data structure that contains all block devices on
// the instance and where they are mapped to
func (o *oracleOps) FreeDevices(
	blockDeviceMappings []interface{},
	rootDeviceName string) ([]string, error) {
	freeDevices := []string{}
	listDevicesReq := core.ListInstanceDevicesRequest{
		InstanceId:  common.String(o.instance),
		IsAvailable: common.Bool(true),
	}
	respListDevices, err := o.compute.ListInstanceDevices(context.Background(), listDevicesReq)
	if err != nil {
		return freeDevices, err
	}
	for _, d := range respListDevices.Items {
		freeDevices = append(freeDevices, *d.Name)
	}
	return freeDevices, nil
}

func (o *oracleOps) GetDeviceID(vol interface{}) (string, error) {
	if d, ok := vol.(*core.Volume); ok {
		return *d.Id, nil
	}
	return "", fmt.Errorf("invalid type: %v given to GetDeviceID", vol)

}

func (o *oracleOps) DeleteInstance(instanceID string, zone string, timeout time.Duration) error {

	pools, err := o.containerEngine.ListNodePools(context.Background(),
		containerengine.ListNodePoolsRequest{CompartmentId: &o.compartmentID, ClusterId: &o.clusterID})
	if err != nil {
		return err
	}

	var nodePoolID *string

	switch len(pools.Items) {
	case 0:
		return errors.New("No node pool found ")
	case 1:
		nodePoolID = pools.Items[0].Id
	default:
		for _, pool := range pools.Items {
			poolResp, err := o.containerEngine.GetNodePool(context.Background(), containerengine.GetNodePoolRequest{NodePoolId: pool.Id})
			if err != nil {
				return err
			}
			if ok := nodePoolContainsNode(poolResp.Nodes, instanceID); ok {
				logrus.Println("Instance is in pool ", *pool.Name)
				nodePoolID = pool.Id
				break
			}
		}
	}

	if nodePoolID == nil {
		return fmt.Errorf("node pool containing instance [%s] not found", instanceID)
	}

	nodeDeleteReq := containerengine.DeleteNodeRequest{
		NodePoolId:      nodePoolID,
		NodeId:          &instanceID,
		IsDecrementSize: common.Bool(false),
	}
	nodeDeleteResp, err := o.containerEngine.DeleteNode(context.Background(), nodeDeleteReq)

	if err != nil {
		return err
	}

	err = o.waitTillWorkStatusIsSucceeded(nodeDeleteResp.OpcRequestId, nodeDeleteResp.OpcWorkRequestId, timeout)
	if err != nil {
		return err
	}

	return nil
}

func nodePoolContainsNode(s []containerengine.Node, e string) bool {
	for _, v := range s {
		if *v.Id == e {
			return true
		}
	}
	return false
}

func (o *oracleOps) Expand(volumeID string, newSizeInGiB uint64) (uint64, error) {
	logrus.Debug("Expand volume to size ", newSizeInGiB, " GiB")

	volume, err := o.storage.GetVolume(context.Background(), core.GetVolumeRequest{VolumeId: &volumeID})
	if err != nil {
		return 0, err
	}

	currentsize := uint64(*volume.SizeInGBs)

	if (currentsize > newSizeInGiB) || (currentsize == newSizeInGiB) {
		return currentsize, errors.New("Can not change Volume size from " + strconv.Itoa(int(currentsize)) + " GiB to " + strconv.Itoa(int(newSizeInGiB)) + " GiB")
	}

	req := core.UpdateVolumeRequest{
		VolumeId: &volumeID,
		UpdateVolumeDetails: core.UpdateVolumeDetails{
			SizeInGBs: common.Int64(int64(newSizeInGiB)),
		},
	}

	updateVolResp, err := o.storage.UpdateVolume(context.Background(), req)
	if err != nil {
		return 0, err
	}

	oracleVol, err := o.waitVolumeStatus(*updateVolResp.Id, core.VolumeLifecycleStateAvailable)
	if err != nil {
		return 0, err
	}
	updatedVol, ok := oracleVol.(*core.Volume)
	if !ok {
		return 0, errors.New("Marshelling failed for Oracle volume")
	}

	return uint64(*updatedVol.SizeInGBs), nil
}

func (o *oracleOps) SetClusterVersion(version string, timeout time.Duration) error {
	logrus.Println("Setting Cluster version to", version)
	req := containerengine.UpdateClusterRequest{
		ClusterId: &o.clusterID,
		UpdateClusterDetails: containerengine.UpdateClusterDetails{
			KubernetesVersion: &version,
		},
	}

	resp, err := o.containerEngine.UpdateCluster(context.Background(), req)
	if err != nil {
		return err
	}

	return o.waitTillWorkStatusIsSucceeded(resp.OpcRequestId, resp.OpcWorkRequestId, timeout)
}

func (o *oracleOps) SetInstanceGroupVersion(instanceGroupName string, version string, timeout time.Duration) error {
	logrus.Println("Setting Instance group version to", version)
	//get nodepool ID from name
	var instanceGroupID *string
	nodePoolsReq := containerengine.ListNodePoolsRequest{CompartmentId: &o.compartmentID, Name: &instanceGroupName, ClusterId: &o.clusterID}
	nodePools, err := o.containerEngine.ListNodePools(context.Background(), nodePoolsReq)
	if err != nil {
		return err
	}

	if len(nodePools.Items) == 0 {
		return errors.New("No node pool found with name" + instanceGroupName)
	}
	instanceGroupID = nodePools.Items[0].Id

	//update kubernetes version of nodepool
	resp, err := o.containerEngine.UpdateNodePool(context.Background(), containerengine.UpdateNodePoolRequest{
		NodePoolId: instanceGroupID,
		UpdateNodePoolDetails: containerengine.UpdateNodePoolDetails{
			KubernetesVersion: &version,
		},
	})

	if err != nil {
		return err
	}

	err = o.waitTillWorkStatusIsSucceeded(resp.OpcRequestId, resp.OpcWorkRequestId, timeout)
	if err != nil {
		return err
	}

	//Any changes made to worker node properties will only apply to new worker nodes.
	//We cannot change the properties of existing worker nodes.
	//To apply changes on existing worker nodes, first, scale the node pool to 0 .
	//Then scale up to original nodepool size with upgraded version
	//https://docs.oracle.com/en-us/iaas/Content/knownissues.htm#contengworkernodepropertiesoutofsync

	updateResp, err := o.scaleDownToZeroThenScaleUp(instanceGroupName, *instanceGroupID, nodePools, timeout)
	if err != nil {
		return err
	}

	return o.waitTillWorkStatusIsSucceeded(updateResp.OpcRequestId, updateResp.OpcWorkRequestId, timeout)
}

func (o *oracleOps) scaleDownToZeroThenScaleUp(instanceGroupName, instanceGroupID string,
	nodePools containerengine.ListNodePoolsResponse, timeout time.Duration) (containerengine.UpdateNodePoolResponse, error) {

	emptyResponse := containerengine.UpdateNodePoolResponse{}

	//get existing total node count of instanceGroup
	existingClusterSize, err := o.GetInstanceGroupSize(instanceGroupName)
	if err != nil {
		return emptyResponse, err
	}

	numberOfZones := len(nodePools.Items[0].NodeConfigDetails.PlacementConfigs)
	totalClusterSize := int(existingClusterSize)

	//get all zones to be updated
	nodePoolPlacementConfigDetails := make([]containerengine.NodePoolPlacementConfigDetails, numberOfZones)

	for i, placementConfigs := range nodePools.Items[0].NodeConfigDetails.PlacementConfigs {
		nodePoolPlacementConfigDetails[i].AvailabilityDomain = placementConfigs.AvailabilityDomain
		nodePoolPlacementConfigDetails[i].SubnetId = placementConfigs.SubnetId
	}
	//delete all nodes from existing node pool
	if err := o.SetInstanceGroupSize(instanceGroupName, 0, timeout); err != nil {
		return emptyResponse, err
	}

	//create same number of nodes again with updated version
	req := containerengine.UpdateNodePoolRequest{
		NodePoolId: &instanceGroupID, //get node pool id
		UpdateNodePoolDetails: containerengine.UpdateNodePoolDetails{
			NodeConfigDetails: &containerengine.UpdateNodePoolNodeConfigDetails{
				Size:             &totalClusterSize,
				PlacementConfigs: nodePoolPlacementConfigDetails,
			},
		},
	}
	updateResp, err := o.containerEngine.UpdateNodePool(context.Background(), req)
	if err != nil {
		return emptyResponse, err
	}

	return updateResp, nil
}

// Enumerate volumes that match given filters. Organize them into
// sets identified by setIdentifier.
// labels can be nil, setIdentifier can be empty string.
func (o *oracleOps) Enumerate(volumeIds []*string,
	labels map[string]string,
	setIdentifier string,
) (map[string][]interface{}, error) {
	sets := make(map[string][]interface{})
	req := core.ListVolumesRequest{
		CompartmentId: common.String(o.compartmentID),
	}
	resp, err := o.storage.ListVolumes(context.Background(), req)
	if err != nil {
		return nil, err
	}
	volIDsMap := map[string]string{}
	for _, volIds := range volumeIds {
		volIDsMap[*volIds] = *volIds
	}
	for _, vol := range resp.Items {
		_, ok := volIDsMap[*vol.Id]
		if !ok {
			continue
		}

		if o.deleted(vol) {
			continue
		}
		// TODO: [PWX-26616] Check if SDK itself returns list of volumes
		// that have labels OR use volumeGroup for filtering
		if labels != nil && !containsMap(vol.FreeformTags, labels) {
			continue
		}
		if len(setIdentifier) == 0 {
			cloudops.AddElementToMap(sets, vol, cloudops.SetIdentifierNone)
		} else {
			found := false
			for tagKey, tagValue := range vol.FreeformTags {
				if tagKey == setIdentifier {
					cloudops.AddElementToMap(sets, vol, tagValue)
					found = true
					break
				}
			}
			if !found {
				cloudops.AddElementToMap(sets, vol, cloudops.SetIdentifierNone)
			}
		}
	}
	return sets, nil
}

func containsMap(mainMap map[string]string, subMap map[string]string) bool {
	for k, v := range subMap {
		value, ok := mainMap[k]
		if !ok {
			return false
		}
		if value != v {
			return false
		}
	}
	return true
}

func (o *oracleOps) deleted(v core.Volume) bool {
	return v.LifecycleState == core.VolumeLifecycleStateTerminating ||
		v.LifecycleState == core.VolumeLifecycleStateTerminated
}

// ApplyTags will overwrite the existing tags with newly provided tags
func (o *oracleOps) ApplyTags(volumeID string, labels map[string]string) error {
	req := core.UpdateVolumeRequest{
		VolumeId: common.String(volumeID),
		UpdateVolumeDetails: core.UpdateVolumeDetails{
			FreeformTags: labels,
		},
	}
	resp, err := o.storage.UpdateVolume(context.Background(), req)
	if err != nil {
		logrus.Errorf("failed to apply tag to %s. response: %v", volumeID, resp)
	}
	return err
}

// Tags will list the existing labels/tags on the given volume
func (o *oracleOps) Tags(volumeID string) (map[string]string, error) {
	vols, err := o.Inspect([]*string{&volumeID})
	if err != nil {
		return nil, err
	}
	if len(vols) != 1 {
		return nil, fmt.Errorf("incorrect number of volumes [%v] got for volume id: %v",
			len(vols), volumeID)
	}
	oracleVol, ok := vols[0].(*core.Volume)
	if !ok {
		return nil, fmt.Errorf("Invalid oracle volume")
	}
	return oracleVol.FreeformTags, nil
}

// RemoveTags removes labels/tags from the given volume
func (o *oracleOps) RemoveTags(volumeID string, labels map[string]string) error {
	currentTags, err := o.Tags(volumeID)
	if err != nil {
		return nil
	}
	for key := range labels {
		delete(currentTags, key)
	}
	return o.ApplyTags(volumeID, currentTags)
}
