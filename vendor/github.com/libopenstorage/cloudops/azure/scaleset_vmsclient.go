package azure

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2018-06-01/compute"
	"github.com/Azure/go-autorest/autorest"
)

type scaleSetVMsClient struct {
	scaleSetName      string
	resourceGroupName string
	client            *compute.VirtualMachineScaleSetVMsClient
}

func newScaleSetVMsClient(
	config Config,
	baseURI string,
	authorizer autorest.Authorizer,
) vmsClient {
	vmsClient := compute.NewVirtualMachineScaleSetVMsClientWithBaseURI(baseURI, config.SubscriptionID)
	vmsClient.Authorizer = authorizer
	vmsClient.PollingDelay = clientPollingDelay
	vmsClient.AddToUserAgent(config.UserAgent)
	return &scaleSetVMsClient{
		scaleSetName:      config.ScaleSetName,
		resourceGroupName: config.ResourceGroupName,
		client:            &vmsClient,
	}
}

func (s *scaleSetVMsClient) name(instanceID string) string {
	return s.scaleSetName + "_" + instanceID
}

func (s *scaleSetVMsClient) describe(
	instanceID string,
) (interface{}, error) {
	return s.describeInstance(instanceID)
}

func (s *scaleSetVMsClient) getDataDisks(
	instanceID string,
) ([]compute.DataDisk, error) {
	vm, err := s.describeInstance(instanceID)
	if err != nil {
		return nil, err
	}

	if vm.StorageProfile == nil || vm.StorageProfile.DataDisks == nil {
		return nil, fmt.Errorf("vm storage profile is invalid")
	}

	return *vm.StorageProfile.DataDisks, nil
}

func (s *scaleSetVMsClient) updateDataDisks(
	instanceID string,
	dataDisks []compute.DataDisk,
) error {
	vm, err := s.describeInstance(instanceID)
	if err != nil {
		return err
	}

	vm.StorageProfile.DataDisks = &dataDisks

	ctx := context.Background()
	future, err := s.client.Update(
		ctx,
		s.resourceGroupName,
		s.scaleSetName,
		instanceID,
		vm,
	)
	if err != nil {
		return err
	}

	err = future.WaitForCompletionRef(ctx, s.client.Client)
	if err != nil {
		return err
	}
	return nil
}

func (s *scaleSetVMsClient) describeInstance(
	instanceID string,
) (compute.VirtualMachineScaleSetVM, error) {
	return s.client.Get(
		context.Background(),
		s.resourceGroupName,
		s.scaleSetName,
		instanceID,
	)
}
