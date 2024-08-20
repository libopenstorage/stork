package action

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/libopenstorage/openstorage/api"
	mockVolumeDriver "github.com/libopenstorage/stork/drivers/volume/mock/volume"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetRemoteNodeName(t *testing.T) {
	// Setup
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockVolumeDriver := mockVolumeDriver.NewMockDriver(mockCtrl)

	ac := &ActionController{
		volDriver: mockVolumeDriver,
	}

	action := &storkv1.Action{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-action",
			Namespace: "default",
		},
	}

	// Test getRemoteNodeName with source domain before witness domain
	clusterDomains := &storkv1.ClusterDomains{
		LocalDomain: "dest",
		ClusterDomainInfos: []storkv1.ClusterDomainInfo{
			{
				Name:       "dest",
				State:      storkv1.ClusterDomainActive,
				SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
			},
			{
				Name:       "source",
				State:      storkv1.ClusterDomainActive,
				SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
			},
			{
				Name:       "witness",
				State:      storkv1.ClusterDomainActive,
				SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
			},
		},
	}

	domainToNodesMap := map[string][]*api.Node{
		"source": {
			&api.Node{
				Id:       "node1",
				DomainID: "source",
			},
			&api.Node{
				Id:       "node2",
				DomainID: "source",
			},
			&api.Node{
				Id:       "node3",
				DomainID: "source",
			},
		},
		"dest": {
			&api.Node{
				Id:       "node4",
				DomainID: "dest",
			},
			&api.Node{
				Id:       "node5",
				DomainID: "dest",
			},
			&api.Node{
				Id:       "node6",
				DomainID: "dest",
			},
		},
		"witness": {
			&api.Node{
				Id:       "node7",
				DomainID: "witness",
			},
		},
	}

	remoteNode := ac.getRemoteNodeName(action, clusterDomains, domainToNodesMap)
	expectedRemoteNode := "source"
	if remoteNode != expectedRemoteNode {
		t.Errorf("Expected remote node to be %s, but got %s", expectedRemoteNode, remoteNode)
	}

	// Test getRemoteNodeName with witness domain before source domain
	clusterDomains = &storkv1.ClusterDomains{
		LocalDomain: "dest",
		ClusterDomainInfos: []storkv1.ClusterDomainInfo{
			{
				Name:       "witness",
				State:      storkv1.ClusterDomainActive,
				SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
			},
			{
				Name:       "source",
				State:      storkv1.ClusterDomainActive,
				SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
			},
			{
				Name:       "dest",
				State:      storkv1.ClusterDomainActive,
				SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
			},
		},
	}

	// Test getRemoteNodeName with no witness domain
	clusterDomains = &storkv1.ClusterDomains{
		LocalDomain: "dest",
		ClusterDomainInfos: []storkv1.ClusterDomainInfo{
			{
				Name:       "dest",
				State:      storkv1.ClusterDomainActive,
				SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
			},
			{
				Name:       "source",
				State:      storkv1.ClusterDomainActive,
				SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
			},
		},
	}

	delete(domainToNodesMap, "witness")
	remoteNode = ac.getRemoteNodeName(action, clusterDomains, domainToNodesMap)
	expectedRemoteNode = "source"
	if remoteNode != expectedRemoteNode {
		t.Errorf("Expected remote node to be %s, but got %s", expectedRemoteNode, remoteNode)
	}

	// Test getRemoteNodeName with no domain
	clusterDomains = &storkv1.ClusterDomains{}
	domainToNodesMap = map[string][]*api.Node{}

	remoteNode = ac.getRemoteNodeName(action, clusterDomains, domainToNodesMap)
	expectedRemoteNode = ""
	if remoteNode != expectedRemoteNode {
		t.Errorf("Expected remote node to be %s, but got %s", expectedRemoteNode, remoteNode)
	}
}
