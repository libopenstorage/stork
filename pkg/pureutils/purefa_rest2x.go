package pureutils

import (
	"github.com/devans10/pugo/flasharray"
	fa "github.com/portworx/torpedo/drivers/pure/flasharray"
)

const (
	RestAPI = "2.26"
)

// PureCreateClientAndConnect Create FA Client and Connect
func PureCreateClientAndConnectRest226(faMgmtEndpoint string, apiToken string) (*flasharray.Client, error) {
	faClient, err := flasharray.NewClient(faMgmtEndpoint, "", "", apiToken,
		RestAPI, false, false, "", nil)
	if err != nil {
		return nil, err
	}
	return faClient, nil
}

// ListAllVolumesFromFA returns list of all Available Volumes present in FA (Function should be used with RestAPI 2.x)
func ListAllVolumesFromFA(faClient *fa.Client) ([]fa.Volumes, error) {
	volumes, err := faClient.Volumes.ListAllAvailableVolumes(nil, nil)
	if err != nil {
		return nil, err
	}
	return volumes, nil
}

// ListAllDestroyedVolumesFromFA Returns list of all Destroyed FA Volumes (Function should be used with RestAPI 2.x)
func ListAllDestroyedVolumesFromFA(faClient *fa.Client) ([]fa.Volumes, error) {
	volumes, err := faClient.Volumes.ListAllDestroyedVolumes(nil, nil)
	if err != nil {
		return nil, err
	}
	return volumes, nil
}
