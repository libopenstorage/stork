package schedops

import (
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/portworx/torpedo/pkg/k8sutils"
)

const (
	// k8sPxRunningLabelKey is the label key used for px state
	k8sPxRunningLabelKey = "px/running"
	// k8sPxNotRunningLabelValue is label value for a not running px state
	k8sPxNotRunningLabelValue = "false"
)

type k8sSchedOps struct{}

func (k *k8sSchedOps) DisableOnNode(n node.Node) error {
	return k8sutils.AddLabelOnNode(n.Name, k8sPxRunningLabelKey, k8sPxNotRunningLabelValue)
}

func (k *k8sSchedOps) ValidateOnNode(n node.Node) error {
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "ValidateOnNode",
	}
}

func (k *k8sSchedOps) EnableOnNode(n node.Node) error {
	return k8sutils.RemoveLabelOnNode(n.Name, k8sPxRunningLabelKey)
}

func init() {
	k := &k8sSchedOps{}
	Register("k8s", k)
}
