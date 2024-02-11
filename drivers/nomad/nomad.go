package nomad

import (
	"github.com/hashicorp/nomad/api"
)

// NomadClient defines structure for nomad client
type NomadClient struct {
	client *api.Client
}

// NewNomadClient creates and returns a new client for nomad cluster
func NewNomadClient() (*NomadClient, error) {
	config := api.DefaultConfig()
	client, err := api.NewClient(config)
	if err != nil {
		return nil, err
	}
	return &NomadClient{client: client}, nil
}

// CreateVolume creates a new CSI volume
func (n *NomadClient) CreateVolume(volumeID, pluginID string, capacityMin, capacityMax int64, accessMode, attachmentMode, fsType string, mountFlags []string) error {
	volume := &api.CSIVolume{
		ID:                   volumeID,
		Name:                 volumeID,
		PluginID:             pluginID,
		RequestedCapacityMin: capacityMin,
		RequestedCapacityMax: capacityMax,
		MountOptions: &api.CSIMountOptions{
			FSType:     fsType,
			MountFlags: mountFlags,
		},
		RequestedCapabilities: []*api.CSIVolumeCapability{
			{
				AccessMode:     api.CSIVolumeAccessMode(accessMode),
				AttachmentMode: api.CSIVolumeAttachmentMode(attachmentMode),
			},
		},
	}

	_, _, err := n.client.CSIVolumes().Create(volume, nil)
	return err
}

// ListNodes returns all nodes in the cluster
func (n *NomadClient) ListNodes() ([]*api.NodeListStub, error) {
	nodes, _, err := n.client.Nodes().List(nil)
	return nodes, err
}

