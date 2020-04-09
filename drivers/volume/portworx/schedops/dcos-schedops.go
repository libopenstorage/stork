package schedops

import (
	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/errors"
	"k8s.io/apimachinery/pkg/version"
)

type dcosSchedOps struct{}

func (d *dcosSchedOps) GetKubernetesVersion() (*version.Info, error) {
	return nil, nil
}

func (d *dcosSchedOps) StartPxOnNode(n node.Node) error {
	return nil
}

func (d *dcosSchedOps) StopPxOnNode(n node.Node) error {
	return nil
}

func (d *dcosSchedOps) RestartPxOnNode(n node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Portworx DCOS operation",
		Operation: "RestartPxOnNode",
	}
}

func (d *dcosSchedOps) ValidateOnNode(n node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateOnNode",
	}
}

func (d *dcosSchedOps) ValidateAddLabels(replicaNodes []api.StorageNode, vol *api.Volume) error {
	// We do not have labels in DC/OS currently
	return nil
}

func (d *dcosSchedOps) ValidateRemoveLabels(vol *volume.Volume) error {
	// We do not have labels in DC/OS currently
	return nil
}

func (d *dcosSchedOps) GetVolumeName(vol *volume.Volume) string {
	return vol.Name
}

func (d *dcosSchedOps) ValidateVolumeCleanup(n node.Driver) error {
	// TODO: Implement this
	return nil
}

func (d *dcosSchedOps) ValidateVolumeSetup(vol *volume.Volume, driver node.Driver) error {
	// TODO: Implement this
	return nil
}

func (d *dcosSchedOps) ValidateSnapshot(volParams map[string]string, parent *api.Volume) error {
	// TODO: Implement this
	return nil
}

func (d *dcosSchedOps) GetServiceEndpoint() (string, error) {
	// PX driver is accessed directly on agent nodes. There is no DC/OS level
	// service endpoint which can be used to redirect the calls to PX driver
	return "", nil
}

func (d *dcosSchedOps) UpgradePortworx(ociImage, ociTag, pxImage, pxTag string) error {
	// TOOD: Implement this method
	return nil
}

func (d *dcosSchedOps) IsPXReadyOnNode(n node.Node) bool {
	// TODO: Implement this method
	return true
}

// IsPXEnabled should return whether given node has px installed or not
func (d *dcosSchedOps) IsPXEnabled(n node.Node) (bool, error) {
	// TODO: Implement this method
	return true, nil
}

// GetStorageInfo returns cluster pair info from destination clusterrefereced by kubeconfig
func (d *dcosSchedOps) GetRemotePXNodes(destKubeConfig string) ([]node.Node, error) {
	// TODO: Implement this methid
	return nil, nil
}

func (d *dcosSchedOps) CreateAutopilotRule(apRule apapi.AutopilotRule) (*apapi.AutopilotRule, error) {
	// TODO: Implement this methid
	return nil, nil
}

func (d *dcosSchedOps) ListAutopilotRules() (*apapi.AutopilotRuleList, error) {
	// TODO: Implement this methid
	return nil, nil
}

func init() {
	d := &dcosSchedOps{}
	Register("dcos", d)
}
