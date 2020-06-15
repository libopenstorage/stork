package azure

import (
	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2018-06-01/compute"
	"github.com/Azure/go-autorest/autorest"
)

// vmsClient is an interface for azure vm client operations
type vmsClient interface {
	// name returns the name of the instance
	name(instanceID string) string
	// describe returns the VM instance object
	describe(instanceID string) (interface{}, error)
	// getDataDisks returns a list of data disks attached to the given VM
	getDataDisks(instanceID string) ([]compute.DataDisk, error)
	// updateDataDisks update the data disks for the given VM
	updateDataDisks(instanceID string, dataDisks []compute.DataDisk) error
}

func newVMsClient(
	config Config,
	baseURI string,
	authorizer autorest.Authorizer,
) vmsClient {
	if config.ScaleSetName == "" {
		return newBaseVMsClient(config, baseURI, authorizer)
	}
	return newScaleSetVMsClient(config, baseURI, authorizer)
}
