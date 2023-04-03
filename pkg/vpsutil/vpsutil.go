package vpsutil

import (
	"fmt"

	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/talisman/pkg/apis/portworx/v1beta1"
	talisman_v1beta2 "github.com/portworx/talisman/pkg/apis/portworx/v1beta2"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/tests"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type VolumePlaceMentStrategyTestCase interface {
	TestName() string
	DeployVPS() error
	DestroyVPSDeployment() error
	ValidateVPSDeployment(contexts []*scheduler.Context) error
}

func VolumeAntiAffinityByMatchExpression(name string, matchExpression []*v1beta1.LabelSelectorRequirement) talisman_v1beta2.VolumePlacementStrategy {
	return talisman_v1beta2.VolumePlacementStrategy{
		ObjectMeta: v1.ObjectMeta{
			Name: name,
		},
		Spec: talisman_v1beta2.VolumePlacementSpec{
			VolumeAntiAffinity: []*talisman_v1beta2.CommonPlacementSpec{
				{
					Enforcement:      v1beta1.EnforcementRequired,
					MatchExpressions: matchExpression,
				},
			},
		},
	}
}

func VolumeAffinityByMatchExpression(name string, matchExpression []*v1beta1.LabelSelectorRequirement) talisman_v1beta2.VolumePlacementStrategy {
	return talisman_v1beta2.VolumePlacementStrategy{
		ObjectMeta: v1.ObjectMeta{
			Name: name,
		},
		Spec: talisman_v1beta2.VolumePlacementSpec{
			VolumeAffinity: []*talisman_v1beta2.CommonPlacementSpec{
				{
					Enforcement:      v1beta1.EnforcementRequired,
					MatchExpressions: matchExpression,
				},
			},
		},
	}
}

func getNodePlacement(vol *api.Volume) []string {
	var nodeList []string
	for _, replica := range vol.ReplicaSets {
		nodeList = append(nodeList, replica.Nodes...)
	}
	return nodeList
}

func getVolumeLabelsValue(vol *api.Volume, key string) string {
	return vol.Locator.VolumeLabels[key]
}

// ValidateVolumeAntiAffinityByNode validates all the volumes with the same volume label key are deployed on different nodes
// in addition, it checks that the expected number of nodes the volumes deploy on
func ValidateVolumeAntiAffinityByNode(vols []*api.Volume, volumeLabelKey string, expectedLength int) error {
	volToNodeMap := make(map[string][]string)
	for _, vol := range vols {
		nodeList := getNodePlacement(vol)
		labelValue := getVolumeLabelsValue(vol, volumeLabelKey)
		for _, node := range nodeList {
			if tests.Contains(volToNodeMap[labelValue], node) {
				return fmt.Errorf("failed to validate vps deployment, expecting vol to be place on unique node but vol %v is duplicated on node %v ... data: %v", labelValue, node, volToNodeMap)
			}
			volToNodeMap[labelValue] = append(volToNodeMap[labelValue], node)
		}

	}

	for _, nodes := range volToNodeMap {
		if len(nodes) != expectedLength {
			return fmt.Errorf("failed to validate vps deployment, expecting label to exist in %v unique nodes, but got length %v and nodes %v .... data: %v", expectedLength, len(nodes), nodes, volToNodeMap)
		}
	}
	return nil
}

// ValidateVolumeAffinityByNode validates all the volumes with the same volume label key are deployed on the same node
func ValidateVolumeAffinityByNode(vols []*api.Volume, label string) error {
	volToNodeMap := make(map[string][]string)
	for _, vol := range vols {
		nodeList := getNodePlacement(vol)
		labelValue := getVolumeLabelsValue(vol, label)
		for _, node := range nodeList {
			if tests.Contains(volToNodeMap[labelValue], node) {
				continue
			}
			volToNodeMap[labelValue] = append(volToNodeMap[labelValue], node)
		}
	}

	for _, nodes := range volToNodeMap {
		if len(nodes) != 1 {
			return fmt.Errorf("failed to validate vps deployment, expecting label to exist in %v unique nodes, but got length %v and nodes %v .... data: %v", 1, len(nodes), nodes, volToNodeMap)
		}
	}
	return nil
}
