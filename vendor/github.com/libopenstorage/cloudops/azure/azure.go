package azure

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2019-12-01/compute"
	"github.com/Azure/azure-sdk-for-go/services/containerservice/mgmt/2019-02-01/containerservice"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/Azure/go-autorest/autorest/to"
	"github.com/libopenstorage/cloudops"
	"github.com/libopenstorage/cloudops/backoff"
	"github.com/libopenstorage/cloudops/unsupported"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
)

const (
	envInstanceID         = "AZURE_INSTANCE_ID"
	envScaleSetName       = "AZURE_SCALE_SET_NAME"
	envSubscriptionID     = "AZURE_SUBSCRIPTION_ID"
	envResourceGroupName  = "AZURE_RESOURCE_GROUP_NAME"
	envManagedClusterName = "AZURE_MANAGED_CLUSTER_NAME"
	envAgentPoolName      = "AZURE_AGENT_POOL_NAME"
	envUserAgent          = "AZURE_HTTP_USER_AGENT"
	metadataAPIEndpoint   = "http://169.254.169.254/metadata/instance/compute"
	metadataAPIVersion    = "2019-03-11"
	scaleSetNameKey       = "vmScaleSetName"
	resourceGroupNameKey  = "resourceGroupName"
	subscriptionIDKey     = "subscriptionId"
	cloudEnvironmentKey   = "azEnvironment"
	userAgentKey          = "useAgent"
	vmIDKey               = "vmId"
)

const (
	name                                = "azure"
	userAgentExtension                  = "osd"
	azureDiskPrefix                     = "/dev/disk/azure/scsi1/lun"
	snapNameFormat                      = "2006-01-02_15.04.05.999999"
	clientPollingDelay                  = 5 * time.Second
	devicePathMaxRetryCount             = 3
	devicePathRetryInterval             = 2 * time.Second
	errCodeAttachDiskWhileBeingDetached = "AttachDiskWhileBeingDetached"
)

var (
	attachFailureMessageRegex = regexp.MustCompile(`^Cannot attach data disk '(.*)' to VM`)
)

type azureOps struct {
	cloudops.Compute
	instance           string
	resourceGroupName  string
	managedClusterName string
	agentPoolName      string
	disksClient        *compute.DisksClient
	vmsClient          vmsClient
	snapshotsClient    *compute.SnapshotsClient
	agentPoolsClient   *containerservice.AgentPoolsClient
}

// Config contains everything needed to create an Azure client.
// Only instanceID, subscriptionID and resourceGroupName are required, others
// are optional based on the required type of client.
type Config struct {
	InstanceID         string
	ScaleSetName       string
	SubscriptionID     string
	ResourceGroupName  string
	CloudEnvironment   string
	ManagedClusterName string
	AgentPoolName      string
	UserAgent          string
}

// NewClientFromMetadata initializes cloudops driver for azure based on environment
// variables or based on instance metadata info available inside Azure VM
func NewClientFromMetadata() (cloudops.Ops, error) {
	if onAzure, computeMetadata, err := onAzure(); onAzure && err == nil {
		logrus.Info("Running on Azure IaaS VM")
		var config Config

		if resourceGroup, ok := computeMetadata[resourceGroupNameKey]; ok {
			temp := resourceGroup.(string)
			if len(strings.Split(temp, "_")) == 4 {
				config.ResourceGroupName = strings.Split(temp, "_")[1]
				config.ManagedClusterName = strings.Split(temp, "_")[2]
			}
		}

		if scalesetName, exists := computeMetadata[scaleSetNameKey]; exists {
			config.ScaleSetName = scalesetName.(string)
		}

		if vmID, exists := computeMetadata[vmIDKey]; exists {
			config.InstanceID = vmID.(string)
		}

		if subscriptionID, exists := computeMetadata[subscriptionIDKey]; exists {
			config.SubscriptionID = subscriptionID.(string)
		}

		if userAgent, exists := computeMetadata[userAgentKey]; exists {
			config.UserAgent = userAgent.(string)
		}

		if environment, exists := computeMetadata[cloudEnvironmentKey]; exists {
			config.CloudEnvironment = environment.(string)
		}

		return NewClient(config)
	}

	logrus.Info("Not running on Azure IaaS VM")
	return NewEnvClient()
}

// onAzure returns true if process is running inside Azure VM,
// it also additionally returns instance metadata in that case.
func onAzure() (bool, map[string]interface{}, error) {
	metadata := make(map[string]interface{})
	client := &http.Client{}
	req, err := http.NewRequest("GET", metadataAPIEndpoint, nil)
	if err != nil {
		return false, metadata, err
	}
	req.Header.Add("Metadata", "True")
	q := req.URL.Query()
	q.Add("format", "json")
	q.Add("api-version", metadataAPIVersion)
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if err != nil {
		return false, metadata,
			fmt.Errorf("Error occured while getting instance metadata from Azure Metadata API. Error:[%v]", err)
	}
	if resp.StatusCode != 200 {
		return false, metadata,
			fmt.Errorf("Error querying Azure metadata: Code %d returned for url %s", resp.StatusCode, req.URL)
	}

	if resp.Body != nil {
		defer resp.Body.Close()
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return false, metadata,
				fmt.Errorf("Error while reading Azure metadata response: [%v]", err)
		}
		if len(respBody) == 0 {
			return false, metadata,
				fmt.Errorf("Error querying Azure metadata: Empty response")
		}

		err = json.Unmarshal(respBody, &metadata)
		if err != nil {
			return false, metadata,
				fmt.Errorf("Error parsing Azure metadata: %v", err)
		}
	}

	return true, metadata, nil
}

// NewEnvClient make new client from well known environment variables.
func NewEnvClient() (cloudops.Ops, error) {
	instance, err := cloudops.GetEnvValueStrict(envInstanceID)
	if err != nil {
		return nil, err
	}
	subscriptionID, err := cloudops.GetEnvValueStrict(auth.SubscriptionID)
	if err != nil {
		return nil, err
	}
	resourceGroupName, err := cloudops.GetEnvValueStrict(envResourceGroupName)
	if err != nil {
		return nil, err
	}

	config := Config{
		InstanceID:        instance,
		SubscriptionID:    subscriptionID,
		ResourceGroupName: resourceGroupName,
		// For backward compatibility, optional new environment variables
		CloudEnvironment:   os.Getenv(auth.EnvironmentName),
		ScaleSetName:       os.Getenv(envScaleSetName),
		ManagedClusterName: os.Getenv(envManagedClusterName),
		AgentPoolName:      os.Getenv(envAgentPoolName),
		UserAgent:          os.Getenv(envUserAgent),
	}

	return NewClient(config)
}

// NewClient creates new client from specified config.
func NewClient(config Config) (cloudops.Ops, error) {
	authorizer, err := auth.NewAuthorizerFromEnvironment()
	if err != nil {
		return nil, err
	}

	baseURI, err := azureBaseURI(config.CloudEnvironment)
	if err != nil {
		return nil, err
	}

	if len(config.UserAgent) == 0 {
		config.UserAgent = userAgentExtension
	}

	disksClient := compute.NewDisksClientWithBaseURI(baseURI, config.SubscriptionID)
	disksClient.Authorizer = authorizer
	disksClient.PollingDelay = clientPollingDelay
	disksClient.AddToUserAgent(config.UserAgent)

	vmsClient := newVMsClient(config, baseURI, authorizer)

	snapshotsClient := compute.NewSnapshotsClientWithBaseURI(baseURI, config.SubscriptionID)
	snapshotsClient.Authorizer = authorizer
	snapshotsClient.PollingDelay = clientPollingDelay
	snapshotsClient.AddToUserAgent(config.UserAgent)

	agentPoolsClient := containerservice.NewAgentPoolsClientWithBaseURI(baseURI, config.SubscriptionID)
	agentPoolsClient.Authorizer = authorizer
	agentPoolsClient.PollingDelay = clientPollingDelay
	agentPoolsClient.AddToUserAgent(config.UserAgent)

	return backoff.NewExponentialBackoffOps(
		&azureOps{
			Compute:            unsupported.NewUnsupportedCompute(),
			instance:           config.InstanceID,
			resourceGroupName:  config.ResourceGroupName,
			managedClusterName: config.ManagedClusterName,
			agentPoolName:      config.AgentPoolName,
			disksClient:        &disksClient,
			vmsClient:          vmsClient,
			snapshotsClient:    &snapshotsClient,
			agentPoolsClient:   &agentPoolsClient,
		},
		isExponentialError,
		backoff.DefaultExponentialBackoff,
	), nil
}

func (a *azureOps) Name() string {
	return string(cloudops.Azure)
}

func (a *azureOps) InstanceID() string {
	return a.instance
}

func (a *azureOps) InspectInstance(instanceID string) (*cloudops.InstanceInfo, error) {

	instInfo := &cloudops.InstanceInfo{
		CloudResourceInfo: cloudops.CloudResourceInfo{
			Name:   a.instance,
			ID:     a.instance,
			Zone:   "",
			Region: "",
		},
	}
	return instInfo, nil
}

func (a *azureOps) InspectInstanceGroupForInstance(instanceID string) (*cloudops.InstanceGroupInfo, error) {

	ctx := context.Background()
	agentPool, err := a.agentPoolsClient.Get(ctx, a.resourceGroupName, a.managedClusterName, a.agentPoolName)
	if err != nil {
		return nil, fmt.Errorf("Failed to get node pool details for [%s] of cluster [%v] in [%v] resource group."+
			"Error: [%v] ", a.agentPoolName, a.managedClusterName, a.resourceGroupName, err)
	}

	zones := []string{}
	if agentPool.AvailabilityZones == nil {
		// If no availability zone mentioned
		// treat all instances to be in same zone
		zones = append(zones, "")
	} else {
		zones = *agentPool.AvailabilityZones
	}

	var MaxCount, MinCount int64
	if agentPool.MaxCount != nil {
		MaxCount = int64(*agentPool.MaxCount)
	}
	if agentPool.MinCount != nil {
		MinCount = int64(*agentPool.MinCount)
	}

	retval := &cloudops.InstanceGroupInfo{
		CloudResourceInfo: cloudops.CloudResourceInfo{
			ID:   *agentPool.ID,
			Name: *agentPool.Name,
		},
		Zones: zones,
		Max:   &MaxCount,
		Min:   &MinCount,
	}
	return retval, nil
}

// GetInstanceGroupSize
func (a *azureOps) GetInstanceGroupSize(instanceGroupID string) (int64, error) {

	ctx := context.Background()
	agentPool, err := a.agentPoolsClient.Get(ctx, a.resourceGroupName, a.managedClusterName, instanceGroupID)
	if err != nil {
		return 0, err
	}
	if agentPool.Count == nil {
		return 0, fmt.Errorf("got empty agent pool size for [%v] of cluster [%v] in [%v] resource group",
			instanceGroupID, a.managedClusterName, a.resourceGroupName)
	}
	return int64(*agentPool.Count), nil
}

// SetInstanceGroupSize sets desired node count per availability zone
// for given instance group
func (a *azureOps) SetInstanceGroupSize(instanceGroupID string,
	count int64,
	timeout time.Duration) error {

	ctx := context.Background()
	var cancel context.CancelFunc
	if timeout > time.Nanosecond {
		// If timeout is non-zero then
		// add deadline to the operation else
		// wait infinitely for operation to complete
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	instanceGroupSize := int32(count)
	agentPoolProperties := containerservice.ManagedClusterAgentPoolProfileProperties{
		Count: &instanceGroupSize,
	}

	agentPool := containerservice.AgentPool{
		ManagedClusterAgentPoolProfileProperties: &agentPoolProperties,
	}
	future, err := a.agentPoolsClient.CreateOrUpdate(ctx, a.resourceGroupName,
		a.managedClusterName,
		instanceGroupID,
		agentPool)
	if err != nil {
		return fmt.Errorf("Failed to set size for node group [%v] of managed cluster [%v]."+
			"Error:[%v]", instanceGroupID, a.managedClusterName, err)
	}
	err = future.WaitForCompletionRef(ctx, a.agentPoolsClient.Client)
	return err
}

func (a *azureOps) Create(
	template interface{},
	labels map[string]string,
) (interface{}, error) {
	d, ok := template.(*compute.Disk)
	if !ok {
		return nil, cloudops.NewStorageError(
			cloudops.ErrVolInval,
			"Invalid volume template given",
			a.instance,
		)
	}

	// Check if the disk already exists; return err if it does
	_, err := a.disksClient.Get(
		context.Background(),
		a.resourceGroupName,
		*d.Name,
	)
	if err == nil {
		return "", fmt.Errorf("disk with id %v already exists", *d.Name)
	}
	derr, ok := err.(autorest.DetailedError)
	if !ok {
		return "", err
	}
	code, ok := derr.StatusCode.(int)
	if !ok || code != 404 {
		return "", err
	}

	ctx := context.Background()
	future, err := a.disksClient.CreateOrUpdate(
		ctx,
		a.resourceGroupName,
		*d.Name,
		compute.Disk{
			Location: d.Location,
			Type:     d.Type,
			Zones:    d.Zones,
			Tags:     formatTags(labels),
			Sku:      d.Sku,
			DiskProperties: &compute.DiskProperties{
				CreationData: &compute.CreationData{
					CreateOption: compute.Empty,
				},
				DiskSizeGB:                   d.DiskProperties.DiskSizeGB,
				DiskIOPSReadWrite:            d.DiskProperties.DiskIOPSReadWrite,
				DiskMBpsReadWrite:            d.DiskProperties.DiskMBpsReadWrite,
				EncryptionSettingsCollection: d.DiskProperties.EncryptionSettingsCollection,
				Encryption:                   d.DiskProperties.Encryption,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	err = future.WaitForCompletionRef(ctx, a.disksClient.Client)
	if err != nil {
		return nil, err
	}

	dd, err := future.Result(*a.disksClient)
	return &dd, err
}

func (a *azureOps) GetDeviceID(disk interface{}) (string, error) {
	if d, ok := disk.(*compute.Disk); ok {
		return *d.Name, nil
	} else if s, ok := disk.(*compute.Snapshot); ok {
		return *s.Name, nil
	}
	return "", cloudops.NewStorageError(
		cloudops.ErrVolInval,
		"Invalid volume given",
		a.instance,
	)
}

func (a *azureOps) Attach(diskName string, options map[string]string) (string, error) {
	disk, err := a.checkDiskAttachmentStatus(diskName)
	if err == nil {
		// Disk is already attached locally, return device path
		return a.waitForAttach(diskName)
	} else if se, ok := err.(*cloudops.StorageError); !ok ||
		se.Code != cloudops.ErrVolDetached {
		return "", err
	}

	dataDisks, err := a.vmsClient.getDataDisks(a.instance)
	if err != nil {
		return "", err
	}

	nextLun := nextAvailableLun(dataDisks)
	if nextLun < 0 {
		return "", fmt.Errorf("No LUN available to attach the disk. "+
			"%v disks attached to the VM instance", len(dataDisks))
	}

	newDataDisks := append(
		dataDisks,
		compute.DataDisk{
			Lun:          &nextLun,
			Name:         to.StringPtr(diskName),
			DiskSizeGB:   disk.DiskSizeGB,
			CreateOption: compute.DiskCreateOptionTypesAttach,
			ManagedDisk: &compute.ManagedDiskParameters{
				ID: disk.ID,
			},
		},
	)
	if err := a.vmsClient.updateDataDisks(a.instance, newDataDisks); err != nil {
		return "", a.handleAttachError(err)
	}

	return a.waitForAttach(diskName)
}

func (a *azureOps) handleAttachError(err error) error {
	if de, ok := err.(autorest.DetailedError); ok {
		if re, ok := de.Original.(azure.RequestError); ok &&
			re.ServiceError != nil &&
			re.ServiceError.Code == errCodeAttachDiskWhileBeingDetached {
			// Azure sometimes gets stuck on a disk that it previously tried to attach
			// but did not succeed. We need to explicitly remove it to proceed.
			matches := attachFailureMessageRegex.FindStringSubmatch(re.ServiceError.Message)
			if len(matches) == 2 {
				detachErr := a.Detach(matches[1])
				if detachErr != nil {
					logrus.Warnf("Failed to detach disk %v: %v", matches[1], detachErr)
				}
			}
		}
	}
	return err
}

func (a *azureOps) Detach(diskName string) error {
	return a.detachInternal(diskName, a.instance)
}

func (a *azureOps) DetachFrom(diskName, instance string) error {
	return a.detachInternal(diskName, instance)
}

func (a *azureOps) detachInternal(diskName, instance string) error {
	disk, err := a.disksClient.Get(
		context.Background(),
		a.resourceGroupName,
		diskName,
	)
	if derr, ok := err.(autorest.DetailedError); ok {
		if code, ok := derr.StatusCode.(int); !ok {
			return err
		} else if code != 404 {
			return err
		}
	} else if err != nil {
		return err
	}

	// Even if a disk is not found, Azure sometimes thinks that the disk is still
	// attached to it even though it is not present or does not show up in the
	// dataDisks list on the VM. The workaround for this is to update the VM
	// irrespective of whether the disk is present or not.
	// https://github.com/andyzhangx/demo/blob/master/issues/azuredisk-issues.md#18-detach-azure-disk-make-vm-run-into-a-limbo-state

	var diskToDetach string
	if disk.ID != nil {
		diskToDetach = strings.ToLower(*disk.ID)
	}

	dataDisks, err := a.vmsClient.getDataDisks(instance)
	if err != nil {
		return err
	}

	newDataDisks := make([]compute.DataDisk, 0)
	for _, d := range dataDisks {
		if strings.ToLower(*d.ManagedDisk.ID) == diskToDetach {
			continue
		}
		newDataDisks = append(newDataDisks, d)
	}

	if err := a.vmsClient.updateDataDisks(instance, newDataDisks); err != nil {
		return err
	}

	return a.waitForDetach(diskName, instance)
}

func (a *azureOps) Delete(diskName string) error {
	ctx := context.Background()
	future, err := a.disksClient.Delete(ctx, a.resourceGroupName, diskName)
	if err != nil {
		return err
	}

	err = future.WaitForCompletionRef(ctx, a.disksClient.Client)
	if err != nil {
		return err
	}

	_, err = future.Result(*a.disksClient)
	return err
}

func (a *azureOps) DeleteFrom(diskName, _ string) error {
	return a.Delete(diskName)
}

func (a *azureOps) Expand(
	diskName string,
	newSizeInGiB uint64,
) (uint64, error) {
	disk, err := a.disksClient.Get(
		context.Background(),
		a.resourceGroupName,
		diskName,
	)
	if err != nil {
		return 0, err
	}

	if disk.DiskProperties == nil || disk.DiskProperties.DiskSizeGB == nil {
		return 0, fmt.Errorf("disk properties of (%v) is nil", diskName)
	}

	if *disk.DiskProperties.DiskSizeGB >= int32(newSizeInGiB) {
		return uint64(*disk.DiskProperties.DiskSizeGB), cloudops.NewStorageError(cloudops.ErrDiskGreaterOrEqualToExpandSize,
			fmt.Sprintf("disk is already has a size: %d greater than or equal "+
				"requested size: %d", *disk.DiskProperties.DiskSizeGB, newSizeInGiB), "")
	}
	oldSizeInGiB := uint64(*disk.DiskProperties.DiskSizeGB)
	// Azure resizes in chunks of GiB even if the disk properties variable is DiskSizeGB
	newSizeInGiBInt32 := int32(newSizeInGiB)
	disk.DiskProperties.DiskSizeGB = &newSizeInGiBInt32

	ctx := context.Background()
	future, err := a.disksClient.CreateOrUpdate(
		ctx,
		a.resourceGroupName,
		diskName,
		disk,
	)
	if err != nil {
		return oldSizeInGiB, err
	}
	err = future.WaitForCompletionRef(ctx, a.disksClient.Client)
	if err != nil {
		return oldSizeInGiB, err
	}

	dd, err := future.Result(*a.disksClient)
	if err != nil {
		return oldSizeInGiB, err
	}
	if dd.DiskProperties == nil || dd.DiskProperties.DiskSizeGB == nil {
		return oldSizeInGiB, fmt.Errorf("disk properties of (%v) is nil after performing resize", diskName)
	}
	return uint64(*dd.DiskProperties.DiskSizeGB), err
}

func (a *azureOps) Describe() (interface{}, error) {
	return a.vmsClient.describe(a.instance)
}

func (a *azureOps) FreeDevices(
	blockDeviceMappings []interface{},
	rootDeviceName string,
) ([]string, error) {
	return nil, &cloudops.ErrNotSupported{
		Operation: "FreeDevices",
	}
}

func (a *azureOps) Inspect(diskNames []*string) ([]interface{}, error) {
	var disks []interface{}

	for _, diskName := range diskNames {
		if diskName == nil {
			continue
		}
		disk, err := a.disksClient.Get(
			context.Background(),
			a.resourceGroupName,
			*diskName,
		)
		if derr, ok := err.(autorest.DetailedError); ok {
			code, ok := derr.StatusCode.(int)
			if ok && code == 404 {
				return nil, cloudops.NewStorageError(
					cloudops.ErrVolNotFound,
					fmt.Sprintf("disk %s not found", *diskName),
					a.instance,
				)
			}
		}
		if err != nil {
			return nil, err
		}
		disks = append(disks, &disk)
	}

	return disks, nil
}

func (a *azureOps) DeviceMappings() (map[string]string, error) {
	/*
	 * The names of disk devices in Azure are determined by
	 * UDEV rules that must be installed on each host node running
	 * in Azure.
	 *
	 * The UDEV rules can be found in the following files on the host:
	 *
	 *    /etc/udev/rules.d/66-azure-storage.rules
	 *    /etc/udev/rules.d/99-azure-product-uuid.rules
	 *
	 * These rules can also be found at:
	 *  https://github.com/Azure/WALinuxAgent/tree/develop/config/
	 *
	 * These UDEV rules are installed by default in images supplied by Azure.
	 * However, in custom images, these files may not be there, so they must be installed.
	 */
	dataDisks, err := a.vmsClient.getDataDisks(a.instance)
	if err != nil {
		return nil, err
	}

	devMap := make(map[string]string)
	for _, d := range dataDisks {
		devPath, err := lunToBlockDevPath(*d.Lun)
		if err != nil {
			return nil, cloudops.NewStorageError(
				cloudops.ErrInvalidDevicePath,
				fmt.Sprintf("unable to find block dev path for lun%v: %v", *d.Lun, err),
				a.instance,
			)
		}
		devMap[devPath] = *d.Name
	}

	return devMap, nil
}

func (a *azureOps) Enumerate(
	diskNames []*string,
	labels map[string]string,
	setIdentifier string,
) (map[string][]interface{}, error) {
	allDisks, err := a.getDisks(labels)
	if err != nil {
		return nil, err
	}

	sets := make(map[string][]interface{})
	for _, disk := range allDisks {
		if len(setIdentifier) == 0 {
			cloudops.AddElementToMap(sets, disk, cloudops.SetIdentifierNone)
		} else {
			found := false
			for key, value := range disk.Tags {
				if key == setIdentifier && value != nil {
					cloudops.AddElementToMap(sets, disk, *value)
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

func (a *azureOps) DevicePath(diskName string) (string, error) {
	if _, err := a.checkDiskAttachmentStatus(diskName); err != nil {
		return "", err
	}
	return a.devicePath(diskName)
}

// checkDiskAttachmentStatus returns the disk without any error if it is already
// attached to the Ops instance. It will return errors if the disk is not attached
// or attached on remote node.
func (a *azureOps) checkDiskAttachmentStatus(diskName string) (*compute.Disk, error) {
	disk, err := a.disksClient.Get(
		context.Background(),
		a.resourceGroupName,
		diskName,
	)
	if derr, ok := err.(autorest.DetailedError); ok {
		code, ok := derr.StatusCode.(int)
		if ok && code == 404 {
			return nil, cloudops.NewStorageError(
				cloudops.ErrVolNotFound,
				fmt.Sprintf("disk %s not found", diskName),
				a.instance,
			)
		}
	}
	if err != nil {
		return nil, err
	}

	if disk.ManagedBy == nil || len(*disk.ManagedBy) == 0 {
		return &disk, cloudops.NewStorageError(
			cloudops.ErrVolDetached,
			fmt.Sprintf("disk %s is detached", diskName),
			a.instance,
		)
	}
	if !strings.HasSuffix(*disk.ManagedBy, a.vmsClient.name(a.instance)) {
		return &disk, cloudops.NewStorageError(
			cloudops.ErrVolAttachedOnRemoteNode,
			fmt.Sprintf("disk %s is attached on remote node %s", diskName, *disk.ManagedBy),
			a.instance,
		)
	}

	return &disk, nil
}

func (a *azureOps) devicePath(diskName string) (string, error) {
	dataDisks, err := a.vmsClient.getDataDisks(a.instance)
	if err != nil {
		return "", err
	}

	for _, d := range dataDisks {
		if *d.Name == diskName {
			// Retry to get the block dev path as it may take few seconds for the path
			// to be created even after the disk shows attached.
			devPath, err := lunToBlockDevPathWithRetry(*d.Lun)
			if err == nil {
				return devPath, nil
			}
			return "", cloudops.NewStorageError(
				cloudops.ErrInvalidDevicePath,
				fmt.Sprintf("unable to find block dev path for lun%v: %v", *d.Lun, err),
				a.instance,
			)
		}
	}

	return "", cloudops.NewStorageError(
		cloudops.ErrVolDetached,
		fmt.Sprintf("disk %s does not have a device path", diskName),
		a.instance,
	)
}

func (a *azureOps) Snapshot(diskName string, readonly bool) (interface{}, error) {
	if !readonly {
		return nil, fmt.Errorf("read-write snapshots are not supported in Azure")
	}

	disk, err := a.disksClient.Get(context.Background(), a.resourceGroupName, diskName)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	future, err := a.snapshotsClient.CreateOrUpdate(
		ctx,
		a.resourceGroupName,
		fmt.Sprint("snap-", time.Now().Format(snapNameFormat)),
		compute.Snapshot{
			Location: disk.Location,
			SnapshotProperties: &compute.SnapshotProperties{
				CreationData: &compute.CreationData{
					CreateOption:     compute.Copy,
					SourceResourceID: disk.ID,
				},
			},
		},
	)
	if err != nil {
		return nil, err
	}

	err = future.WaitForCompletionRef(ctx, a.snapshotsClient.Client)
	if err != nil {
		return nil, err
	}

	snap, err := future.Result(*a.snapshotsClient)
	return &snap, err
}

func (a *azureOps) SnapshotDelete(snapName string) error {
	ctx := context.Background()
	future, err := a.snapshotsClient.Delete(ctx, a.resourceGroupName, snapName)
	if err != nil {
		return err
	}

	err = future.WaitForCompletionRef(ctx, a.snapshotsClient.Client)
	if err != nil {
		return err
	}

	_, err = future.Result(*a.snapshotsClient)
	return err
}

func (a *azureOps) ApplyTags(diskName string, labels map[string]string) error {
	if len(labels) == 0 {
		return nil
	}

	disk, err := a.disksClient.Get(
		context.Background(),
		a.resourceGroupName,
		diskName,
	)
	if err != nil {
		return err
	}

	if len(disk.Tags) == 0 {
		disk.Tags = make(map[string]*string)
	}

	for k, v := range labels {
		disk.Tags[k] = to.StringPtr(v)
	}

	ctx := context.Background()
	future, err := a.disksClient.Update(
		ctx,
		a.resourceGroupName,
		diskName,
		compute.DiskUpdate{
			Tags: disk.Tags,
		},
	)
	if err != nil {
		return err
	}

	err = future.WaitForCompletionRef(ctx, a.disksClient.Client)
	if err != nil {
		return err
	}

	_, err = future.Result(*a.disksClient)
	return err
}

func (a *azureOps) RemoveTags(diskName string, labels map[string]string) error {
	if len(labels) == 0 {
		return nil
	}

	disk, err := a.disksClient.Get(
		context.Background(),
		a.resourceGroupName,
		diskName,
	)
	if err != nil {
		return err
	}

	if len(disk.Tags) == 0 {
		return nil
	}

	for k := range labels {
		delete(disk.Tags, k)
	}

	ctx := context.Background()
	future, err := a.disksClient.Update(
		ctx,
		a.resourceGroupName,
		diskName,
		compute.DiskUpdate{
			Tags: disk.Tags,
		},
	)
	if err != nil {
		return err
	}

	err = future.WaitForCompletionRef(ctx, a.disksClient.Client)
	if err != nil {
		return err
	}

	_, err = future.Result(*a.disksClient)
	return err
}

func (a *azureOps) Tags(diskName string) (map[string]string, error) {
	disk, err := a.disksClient.Get(context.Background(), a.resourceGroupName, diskName)
	if err != nil {
		return nil, err
	}

	tags := make(map[string]string)
	for k, v := range disk.Tags {
		if v == nil {
			tags[k] = ""
		} else {
			tags[k] = *v
		}
	}
	return tags, nil
}

func (a *azureOps) getDisks(labels map[string]string) (map[string]*compute.Disk, error) {
	response := make(map[string]*compute.Disk)

	it, err := a.disksClient.ListByResourceGroupComplete(
		context.Background(),
		a.resourceGroupName,
	)
	if err != nil {
		return nil, err
	}
	for ; it.NotDone(); err = it.Next() {
		if err != nil {
			return nil, err
		}

		disk := it.Value()
		if labelsMatch(&disk, labels) {
			response[*it.Value().Name] = &disk
		}
	}

	return response, nil
}

func (a *azureOps) waitForAttach(diskName string) (string, error) {
	devicePath, err := task.DoRetryWithTimeout(
		func() (interface{}, bool, error) {
			devicePath, err := a.DevicePath(diskName)
			if se, ok := err.(*cloudops.StorageError); ok &&
				se.Code == cloudops.ErrVolAttachedOnRemoteNode {
				return "", false, err
			} else if err != nil {
				return "", true, err
			}

			return devicePath, false, nil
		},
		cloudops.ProviderOpsTimeout,
		cloudops.ProviderOpsRetryInterval,
	)
	if err != nil {
		return "", err
	}

	return devicePath.(string), nil
}

func (a *azureOps) waitForDetach(diskName, instance string) error {
	_, err := task.DoRetryWithTimeout(
		func() (interface{}, bool, error) {
			dataDisks, err := a.vmsClient.getDataDisks(instance)
			if err != nil {
				return nil, true, err
			}

			for _, d := range dataDisks {
				if *d.Name == diskName {
					return nil, true,
						fmt.Errorf("disk %s is still attached to instance %s",
							diskName, instance)
				}
			}

			return nil, false, nil
		},
		cloudops.ProviderOpsTimeout,
		cloudops.ProviderOpsRetryInterval,
	)

	return err
}

func labelsMatch(disk *compute.Disk, labels map[string]string) bool {
	for key, expected := range labels {
		if actual, exists := disk.Tags[key]; exists {
			// Nil values are not allowed in tags, just safety check
			if actual == nil && expected != "" {
				return false
			} else if actual != nil && *actual != expected {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

func formatTags(labels map[string]string) map[string]*string {
	tags := make(map[string]*string)
	for k, v := range labels {
		value := v
		tags[k] = &value
	}
	return tags
}

func nextAvailableLun(dataDisks []compute.DataDisk) int32 {
	usedLuns := make(map[int32]struct{})
	for _, d := range dataDisks {
		if d.Lun != nil {
			usedLuns[*d.Lun] = struct{}{}
		}
	}
	nextAvailableLun := int32(-1)
	for i := int32(0); i < 64; i++ {
		if _, ok := usedLuns[i]; !ok {
			nextAvailableLun = i
			break
		}
	}
	return nextAvailableLun
}

func lunToBlockDevPathWithRetry(lun int32) (string, error) {
	var (
		retryCount int
		path       string
		err        error
	)

	for {
		if path, err = lunToBlockDevPath(lun); err == nil {
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

func lunToBlockDevPath(lun int32) (string, error) {
	devPath := azureDiskPrefix + strconv.Itoa(int(lun))
	// check if path is a sym link. If yes, return pointee
	fi, err := os.Lstat(devPath)
	if err != nil {
		return "", err
	}

	if fi.Mode()&os.ModeSymlink != 0 {
		output, err := filepath.EvalSymlinks(devPath)
		if err != nil {
			return "", fmt.Errorf("failed to read symlink %s due to: %v", devPath, err)
		}

		devPath = strings.TrimSpace(string(output))
	} else {
		return "", fmt.Errorf("%s was expected to be a symlink to actual "+
			"device path", devPath)
	}

	return devPath, nil
}

func isExponentialError(err error) bool {
	// Got the list of error codes from here
	// https://docs.microsoft.com/en-us/rest/api/storageservices/common-rest-api-error-codes
	// https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-manager-request-limits

	azureCodes := map[int]bool{
		int(429): true,
	}

	serviceErrorCodes := map[string]bool{
		errCodeAttachDiskWhileBeingDetached: true,
	}

	if err != nil {
		if azErr, ok := err.(autorest.DetailedError); ok {
			code, ok := azErr.StatusCode.(int)
			if ok && azureCodes[code] {
				return true
			}
			re, ok := azErr.Original.(azure.RequestError)
			if ok && re.ServiceError != nil && serviceErrorCodes[re.ServiceError.Code] {
				return true
			}
		}
	}
	return false
}

func azureBaseURI(cloudEnvName string) (string, error) {
	if value, ok := os.LookupEnv(auth.EnvironmentName); ok {
		cloudEnvName = value
	}

	if cloudEnvName == "" {
		return azure.PublicCloud.ResourceManagerEndpoint, nil
	}

	cloudEnv, err := azure.EnvironmentFromName(cloudEnvName)
	if err != nil {
		return "", err
	}
	return cloudEnv.ResourceManagerEndpoint, nil
}
