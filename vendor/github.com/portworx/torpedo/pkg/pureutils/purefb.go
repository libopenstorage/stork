package pureutils

import (
	"fmt"
	"github.com/portworx/torpedo/drivers/pure/flashblade"
	"regexp"
)

// PureCreateClientAndConnect Create FB Client and Connect
func PureCreateFbClientAndConnect(fbMgmtEndpoint string, apiToken string) (*flashblade.Client, error) {
	fbClient, err := flashblade.NewClient(fbMgmtEndpoint, "", "", apiToken,
		"", false, false, "", nil)
	if err != nil {
		return nil, err
	}
	return fbClient, nil
}

// GetFAMgmtEndPoints , Get Lists of all management Endpoints from FB Secrets
func GetFBMgmtEndPoints(secret PXPureSecret) []string {
	mgmtEndpoints := []string{}
	for _, fbDetails := range secret.Arrays {
		mgmtEndpoints = append(mgmtEndpoints, fbDetails.MgmtEndPoint)
	}
	return mgmtEndpoints
}

// GetFBClientMapFromPXPureSecret takes a PXPureSecret and returns a map of mgmt endpoints to FB clients
func GetFBClientMapFromPXPureSecret(secret PXPureSecret) (map[string]*flashblade.Client, error) {
	clientMap := make(map[string]*flashblade.Client)
	for _, fb := range secret.Blades {
		fbClient, err := PureCreateFbClientAndConnect(fb.MgmtEndPoint, fb.APIToken)
		if err != nil {
			return nil, fmt.Errorf("failed to create FB client for [%s]. Err: [%v]", fb.MgmtEndPoint, err)
		}
		clientMap[fb.MgmtEndPoint] = fbClient
	}
	return clientMap, nil
}

// GetApiTokenForMgmtEndpoints Returns API token for Mgmt Endpoints
func GetApiTokenForFbMgmtEndpoints(secret PXPureSecret, mgmtEndPoint string) string {
	for _, faDetails := range secret.Arrays {
		if faDetails.MgmtEndPoint == mgmtEndPoint {
			return faDetails.APIToken
		}
	}
	return ""
}

// GetBladeDetails Get Details of all the Blades present in FB
func GetBladeDetails(fbClient *flashblade.Client) ([]flashblade.Blades, error) {
	blades, err := fbClient.Blades.GetBlades(nil, nil)
	if err != nil {
		return nil, err
	}
	return blades, nil
}

// ListAllFileSystems Returns list of all filesystems present in FB Backend
func ListAllFileSystems(fbClient *flashblade.Client) ([]flashblade.FSResponse, error) {
	fileSys, err := fbClient.FileSystem.GetAllFileSystems(nil, nil)
	if err != nil {
		return nil, err
	}
	return fileSys, nil
}

// GetAllPVCNames Returns list of all PVCs present in the FB Cluster
func GetAllPVCNames(fbClient *flashblade.Client) ([]string, error) {
	allPVCs := []string{}
	allFs, err := ListAllFileSystems(fbClient)
	if err != nil {
		return nil, err
	}
	// Define Regex Pattern to fetch only PVCs created by Px
	re, err := regexp.Compile("px_.*-pvc-.*")
	if err != nil {
		return nil, err
	}
	for _, eachPvc := range allFs {
		for _, eachItem := range eachPvc.Items {
			if re.MatchString(fmt.Sprintf("%v", eachItem.Name)) {
				allPVCs = append(allPVCs, eachItem.Name)
			}
		}
	}
	return allPVCs, nil
}

// CreateNewFileSystem Returns list of all filesystems present in FB Backend
func CreateNewFileSystem(fbClient *flashblade.Client, fsName string, data interface{}) ([]flashblade.FsItem, error) {
	queryParams := make(map[string]string)
	queryParams["names"] = fsName
	fileSys, err := fbClient.FileSystem.CreateNewFileSystem(queryParams, data)
	if err != nil {
		return nil, err
	}
	return fileSys, nil
}

// DeleteFileSystem Deletes Filesystem from cluster
func ModifyFileSystemParameters(fbClient *flashblade.Client, fsName string, data interface{}) ([]flashblade.FsItem, error) {
	queryParams := make(map[string]string)
	queryParams["names"] = fsName
	fileParams, err := fbClient.FileSystem.ModifyFilesystemParameters(queryParams, data)
	if err != nil {
		return nil, err
	}
	return fileParams, nil
}

// DeleteFileSystem Deletes Filesystem from cluster
func DeleteFileSystem(fbClient *flashblade.Client, fsName string) error {
	queryParams := make(map[string]string)
	queryParams["names"] = fsName
	err := fbClient.FileSystem.DeleteFilesystem(queryParams)
	if err != nil {
		return err
	}
	return nil
}

// IsFileSystemExists Returns True if Filesystem exists, else Returns False
func IsFileSystemExists(fbClient *flashblade.Client, fsName string) (bool, error) {
	allPvcNames, err := GetAllPVCNames(fbClient)
	if err != nil {
		return false, err
	}
	for _, eachPvc := range allPvcNames {
		if fsName == eachPvc {
			return true, nil
		}
	}
	return false, nil
}

// ListSnapSchedulePolicies Returns list of all FB snapshots schedule policies present
func ListSnapSchedulePolicies(fbClient *flashblade.Client) ([]flashblade.PolicyResponse, error) {
	policies, err := fbClient.FileSystem.GetSnapshotSchedulingPolicies(nil, nil)
	if err != nil {
		return nil, err
	}
	return policies, nil
}

// ListAllNetworkInterfaces Returns list of all Network interfaces from Specific FB
func ListAllNetworkInterfaces(fbClient *flashblade.Client) ([]flashblade.NetResponse, error) {
	netInterface, err := fbClient.NetworkInterface.ListNetworkInterfaces(nil, nil)
	if err != nil {
		return nil, err
	}
	return netInterface, nil
}

// ListAllSubnetInterfaces Returns list of all subnets from FB
func ListAllSubnetInterfaces(fbClient *flashblade.Client) ([]flashblade.SubNetResponse, error) {
	netInterface, err := fbClient.NetworkInterface.ListAllArraySubnets(nil, nil)
	if err != nil {
		return nil, err
	}
	return netInterface, nil
}

// listAllSpecificInterfaces returns list of all specific Interfaces from the available network interface
// interface type can be management, data, replication support
func ListAllSpecificInterfaces(fbClient *flashblade.Client, interfaceType string) ([]flashblade.NetResponse, error) {
	mgmtInterfaces := []flashblade.NetResponse{}
	allInterfaces, err := ListAllNetworkInterfaces(fbClient)
	if err != nil {
		return nil, err
	}
	for _, eachInterfaces := range allInterfaces {
		for _, eachItem := range eachInterfaces.Items {
			for _, eachServices := range eachItem.Services {
				if eachServices == interfaceType {
					mgmtInterfaces = append(mgmtInterfaces, eachInterfaces)
				}
			}
		}
	}
	return mgmtInterfaces, nil
}
