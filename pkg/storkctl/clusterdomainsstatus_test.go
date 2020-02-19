// +build unittest

package storkctl

import (
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func createClusterDomainsStatus(t *testing.T, name string) *storkv1.ClusterDomainsStatus {
	activeDomainInfos := []storkv1.ClusterDomainInfo{
		{
			Name:       "zone1",
			State:      storkv1.ClusterDomainActive,
			SyncStatus: storkv1.ClusterDomainSyncStatusInProgress,
		},
		{
			Name:       "zone2",
			State:      storkv1.ClusterDomainActive,
			SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
		},
	}
	inactiveDomainInfos := []storkv1.ClusterDomainInfo{
		{
			Name:       "zone3",
			State:      storkv1.ClusterDomainInactive,
			SyncStatus: storkv1.ClusterDomainSyncStatusNotInSync,
		},
		{
			Name:       "zone4",
			State:      storkv1.ClusterDomainInactive,
			SyncStatus: storkv1.ClusterDomainSyncStatusNotInSync,
		},
	}
	cds := &storkv1.ClusterDomainsStatus{
		ObjectMeta: meta.ObjectMeta{
			Name: name,
		},
		Status: storkv1.ClusterDomains{
			LocalDomain:        "zone1",
			ClusterDomainInfos: append(activeDomainInfos, inactiveDomainInfos...),
		},
	}
	_, err := storkops.Instance().CreateClusterDomainsStatus(cds)
	require.NoError(t, err, "Error creating ClusterDomainsStatus")

	cds, err = storkops.Instance().GetClusterDomainsStatus(name)
	require.NoError(t, err, "Error getting ClusterDomainsStatus")
	require.Equal(t, name, cds.Name, "ClusterDomainsStatus name mismatch")
	require.Equal(t, len(cds.Status.ClusterDomainInfos), 4, "Number of ClusterDomainInfo objects do not match")

	for _, activeDomainInfo := range activeDomainInfos {
		found := false
		for _, actualDomainInfo := range cds.Status.ClusterDomainInfos {
			if activeDomainInfo.Name == actualDomainInfo.Name {
				require.Equal(t, activeDomainInfo.State, actualDomainInfo.State, "Mismatch in ClusterDomain State")
				require.Equal(t, activeDomainInfo.SyncStatus, actualDomainInfo.SyncStatus, "Mismatch in ClusterDomain SyncState")
				found = true
				break
			}
		}
		require.True(t, found, "Could not find expected ClusterDomain %v", activeDomainInfo.Name)
	}
	for _, inactiveDomainInfo := range inactiveDomainInfos {
		found := false
		for _, actualDomainInfo := range cds.Status.ClusterDomainInfos {
			if inactiveDomainInfo.Name == actualDomainInfo.Name {
				require.Equal(t, inactiveDomainInfo.State, actualDomainInfo.State, "Mismatch in ClusterDomain State")
				require.Equal(t, inactiveDomainInfo.SyncStatus, actualDomainInfo.SyncStatus, "Mismatch in ClusterDomain SyncState")
				found = true
				break
			}
		}
		require.True(t, found, "Could not find expected ClusterDomain %v", inactiveDomainInfo.Name)
	}
	return cds
}

func TestGetClusterDomainsStatusNoStatus(t *testing.T) {
	cmdArgs := []string{"get", "clusterdomainsstatus"}

	expected := "No resources found.\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "cds"}

	expected = "No resources found.\n"
	testCommon(t, cmdArgs, nil, expected, false)

}

func TestGetClusterDomainsStatus(t *testing.T) {
	defer resetTest()
	createClusterDomainsStatus(t, "test1")
	cmdArgs := []string{"get", "clusterdomainsstatus", "test1"}

	expected := "NAME    LOCAL-DOMAIN   ACTIVE                                   INACTIVE                               CREATED\n" +
		"test1   zone1          zone1 (SyncInProgress), zone2 (InSync)   zone3 (NotInSync), zone4 (NotInSync)   \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetClusterDomainsStatusWithChanges(t *testing.T) {
	defer resetTest()
	cds := createClusterDomainsStatus(t, "test1")
	cmdArgs := []string{"get", "clusterdomainsstatus", "test1"}

	expected := "NAME    LOCAL-DOMAIN   ACTIVE                                   INACTIVE                               CREATED\n" +
		"test1   zone1          zone1 (SyncInProgress), zone2 (InSync)   zone3 (NotInSync), zone4 (NotInSync)   \n"
	testCommon(t, cmdArgs, nil, expected, false)

	cds.Status.LocalDomain = "zone1"
	for _, cdInfo := range cds.Status.ClusterDomainInfos {
		if cdInfo.Name == "zone3" {
			cdInfo.State = storkv1.ClusterDomainActive
			cdInfo.SyncStatus = storkv1.ClusterDomainSyncStatusInProgress
			break
		}
	}
	_, err := storkops.Instance().UpdateClusterDomainsStatus(cds)
	require.NoError(t, err, "Error updating cluster domains status")

	cmdArgs = []string{"get", "clusterdomainsstatus", "test1"}

	expected = "NAME    LOCAL-DOMAIN   ACTIVE                                   INACTIVE                               CREATED\n" +
		"test1   zone1          zone1 (SyncInProgress), zone2 (InSync)   zone3 (NotInSync), zone4 (NotInSync)   \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetClusterDomainsStatusNotFound(t *testing.T) {
	defer resetTest()
	createClusterDomainsStatus(t, "test1")
	cmdArgs := []string{"get", "clusterdomainsstatus", "test2"}

	expected := `Error from server (NotFound): clusterdomainsstatuses.stork.libopenstorage.org "test2" not found`
	testCommon(t, cmdArgs, nil, expected, true)
}
