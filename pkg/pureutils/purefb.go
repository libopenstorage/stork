package pureutils

import (
	"fmt"
	"github.com/portworx/torpedo/drivers/pure/flashblade"
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
func GetBladeDetails(faClient *flashblade.Client) ([]flashblade.Blades, error) {
	blades, err := faClient.Blades.GetBlades(nil, nil)
	if err != nil {
		return nil, err
	}
	return blades, nil
}

// ListAllFileSystems Returns list of all filesystems present in FB Backend
func ListAllFileSystems(faClient *flashblade.Client) ([]flashblade.FSResponse, error) {
	fileSys, err := faClient.FileSystem.GetAllFileSystems(nil, nil)
	if err != nil {
		return nil, err
	}
	return fileSys, nil
}

// ListSnapSchedulePolicies Returns list of all FB snapshots schedule policies present
func ListSnapSchedulePolicies(faClient *flashblade.Client) ([]flashblade.PolicyResponse, error) {
	policies, err := faClient.FileSystem.GetSnapshotSchedulingPolicies(nil, nil)
	if err != nil {
		return nil, err
	}
	return policies, nil
}

// ListAllNetworkInterfaces Returns list of all Network interfaces from Specific FB
func ListAllNetworkInterfaces(faClient *flashblade.Client) ([]flashblade.NetResponse, error) {
	netInterface, err := faClient.NetworkInterface.ListNetworkInterfaces(nil, nil)
	if err != nil {
		return nil, err
	}
	return netInterface, nil
}

// ListAllSubnetInterfaces Returns list of all subnets from FB
func ListAllSubnetInterfaces(faClient *flashblade.Client) ([]flashblade.SubNetResponse, error) {
	netInterface, err := faClient.NetworkInterface.ListAllArraySubnets(nil, nil)
	if err != nil {
		return nil, err
	}
	return netInterface, nil
}
