package pureutils

import (
	"fmt"
	"github.com/pure-px/torpedo/drivers/pure/flasharray"
	"strings"
)

const (
	RestAPI = "2.8"
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
	volumes, err := faClient.Volumes.ListFAVolumes(params, nil)
	if err != nil {
		return nil, err
	}
	return volumes, nil
}

// ListAllDestroyedVolumesFromFA Returns list of all Destroyed FA Volumes (Function should be used with RestAPI 2.x)
func ListAllDestroyedVolumesFromFA(faClient *flasharray.Client) ([]flasharray.VolResponse, error) {
	params := make(map[string]string)
	params["destroyed"] = "true"
	volumes, err := faClient.Volumes.ListFAVolumes(params, nil)
	if err != nil {
		return nil, err
	}
	return volumes, nil
}

// IsVolumeExistsonFA checks if volume exists in FA
func IsVolumeExistsonFA(faClient *flasharray.Client, volumeName string) (bool, error) {
	params := make(map[string]string)
	params["names"] = volumeName
	params["destroyed"] = "false"
	vols, err := faClient.Volumes.ListFAVolumes(params, nil)
	if err != nil {
		return false, err
	}
	for _, eachVolItems := range vols {
		for _, eachVol := range eachVolItems.Items {
			if strings.Contains(eachVol.Name, volumeName) {
				return true, nil
			}
		}
	}
	return false, nil
}

// ListAllRealmsFromFA returns list of all Available Realms present in FA
func ListAllRealmsFromFA(faClient *flasharray.Client) ([]flasharray.RealmResponse, error) {
	params := make(map[string]string)
	params["destroyed"] = "false"
	realms, err := faClient.Realms.ListAllAvailableRealms(params)
	if err != nil {
		return nil, err
	}
	return realms, nil
}

// ListAllPodsFromFA returns list of all Available Pods present in FA
func ListAllPodsFromFA(faClient *flasharray.Client) ([]flasharray.PodResponse, error) {
	params := make(map[string]string)
	params["destroyed"] = "false"
	pods, err := faClient.Pods.ListAllAvailablePods(params)
	if err != nil {
		return nil, err
	}
	return pods, nil
}

// GetCompleteVolumeNameFromFA returns complete volume name from FA
func GetCompleteVolumeNameFromFA(faClient *flasharray.Client, volumeName string) (string, error) {
	vols, err := ListAllVolumesFromFA(faClient)
	if err != nil {
		return "", err

	}
	for _, eachVolItems := range vols {
		for _, eachVol := range eachVolItems.Items {
			if strings.Contains(eachVol.Name, volumeName) {
				return eachVol.Name, nil
			}
		}

	}
	return "", nil
}

// CreatePodinFA creates Pod in FA
func CreatePodinFA(faClient *flasharray.Client, podName string) (*[]flasharray.PodResponse, error) {
	queryParams := make(map[string]string)
	queryParams["names"] = fmt.Sprintf("%s", podName)
	podinfo, err := faClient.Pods.CreatePod(queryParams, nil)
	if err != nil {
		return nil, err
	}
	return podinfo, nil
}

// ListAllInterfaces returns list of all Available Interfaces present in FA
func ListAllInterfaces(faClient *flasharray.Client) ([]flasharray.NetworkInterfaceResponse, error) {
	interfaces, err := faClient.Network.ListNetworkInterfaces()
	if err != nil {
		return nil, err
	}
	return interfaces, nil
}

// SetInterfaceEnabled enables or disables a network interface.
func SetInterfaceEnabled(faClient *flasharray.Client, iface string, enabled bool) ([]flasharray.NetworkInterface, error) {
	interfaces, err := faClient.Network.SetNetworkInterfaceEnabled(iface, enabled)
	if err != nil {
		return nil, err
	}
	return interfaces, nil

}

// DeletePodinFA deletes Pod in FA
func DeletePodinFA(faClient *flasharray.Client, podName string) error {
	queryParams := make(map[string]string)
	queryParams["names"] = fmt.Sprintf("%s", podName)
	queryParams["destroy_contents"] = "true"
	data := map[string]bool{"destroyed": true}

	deleteParams := make(map[string]string)
	deleteParams["names"] = fmt.Sprintf("%s", podName)
	deleteParams["eradicate_contents"] = "true"

	err := faClient.Pods.DeletePod(queryParams, deleteParams, data)
	if err != nil {
		return err
	}
	return nil
}

// GetFARealmFromMgmtEndpoint returns Realm Name from FA Secret
func GetFARealmFromMgmtEndpoint(secret PXPureSecret, mgmtEndPoint string) string {
	for _, faDetails := range secret.Arrays {
		if faDetails.MgmtEndPoint == mgmtEndPoint {
			return faDetails.Realm
		}
	}
	return ""
}

// IsFARealmExistsOnMgmtEndpoint checks if realm is present in FA
func IsFARealmExistsOnMgmtEndpoint(faClient *flasharray.Client, realm string) (bool, error) {
	params := make(map[string]string)
	params["destroyed"] = "false"
	realms, err := faClient.Realms.ListAllAvailableRealms(params)
	if err != nil {
		return false, err
	}
	for _, eachRealmItems := range realms {
		for _, eachRealm := range eachRealmItems.Items {
			if strings.Contains(eachRealm.Name, realm) {
				return true, nil
			}
		}
	}
	return false, nil
}

// IsPodExistsOnMgmtEndpoint checks if pod is present in FA
func IsPodExistsOnMgmtEndpoint(faClient *flasharray.Client, podName string) (bool, error) {
	params := make(map[string]string)
	params["destroyed"] = "false"
	pods, err := faClient.Pods.ListAllAvailablePods(params)
	if err != nil {
		return false, err
	}
	for _, eachPodItems := range pods {
		for _, eachPod := range eachPodItems.Items {
			if strings.Contains(eachPod.Name, podName) {
				return true, nil
			}
		}
	}

	return false, nil
}
