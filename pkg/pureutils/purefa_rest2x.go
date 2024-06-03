package pureutils

import (
	"github.com/portworx/torpedo/drivers/pure/flasharray"
)

const (
	RestAPI = "2.4"
)

// PureCreateClientAndConnect Create FA Client and Connect
func PureCreateClientAndConnectRest2_x(faMgmtEndpoint string, apiToken string) (*flasharray.Client, error) {
	faClient, err := flasharray.NewClient(faMgmtEndpoint, apiToken, "", "",
		RestAPI, false, false, "torpedo", nil)
	if err != nil {
		return nil, err
	}
	return faClient, nil
}

// ListAllVolumesFromFA returns list of all Available Volumes present in FA (Function should be used with RestAPI 2.x)
func ListAllVolumesFromFA(faClient *flasharray.Client) ([]flasharray.VolResponse, error) {
	params := make(map[string]string)
	params["destroyed"] = "false"
	volumes, err := faClient.Volumes.ListAllAvailableVolumes(params, nil)
	if err != nil {
		return nil, err
	}
	return volumes, nil
}

// ListAllDestroyedVolumesFromFA Returns list of all Destroyed FA Volumes (Function should be used with RestAPI 2.x)
func ListAllDestroyedVolumesFromFA(faClient *flasharray.Client) ([]flasharray.VolResponse, error) {
	params := make(map[string]string)
	params["destroyed"] = "true"
	volumes, err := faClient.Volumes.ListAllAvailableVolumes(params, nil)
	if err != nil {
		return nil, err
	}
	return volumes, nil
}
