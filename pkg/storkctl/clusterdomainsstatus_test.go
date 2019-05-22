// +build unittest

package storkctl

import (
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func createClusterDomainsStatus(t *testing.T, name string) *storkv1.ClusterDomainsStatus {
	activeZones := []string{"zone1", "zone2"}
	inactiveZones := []string{"zone3", "zone4"}
	cds := &storkv1.ClusterDomainsStatus{
		ObjectMeta: meta.ObjectMeta{
			Name: name,
		},
		Status: storkv1.ClusterDomains{
			LocalDomain: "zone1",
			Active:      activeZones,
			Inactive:    inactiveZones,
		},
	}
	_, err := k8s.Instance().CreateClusterDomainsStatus(cds)
	require.NoError(t, err, "Error creating ClusterDomainsStatus")

	cds, err = k8s.Instance().GetClusterDomainsStatus(name)
	require.NoError(t, err, "Error getting ClusterDomainsStatus")
	require.Equal(t, name, cds.Name, "ClusterDomainsStatus name mismatch")
	require.Equal(t, len(cds.Status.Active), 2, "Active domains count mismatch")
	require.Equal(t, len(cds.Status.Inactive), 2, "InActive domains count mismatch")
	for i, activeZone := range activeZones {
		require.Equal(t, cds.Status.Active[i], activeZone, "Active zone mismatch")
	}
	for i, inactiveZone := range inactiveZones {
		require.Equal(t, cds.Status.Inactive[i], inactiveZone, "Inactive zone mismatch")
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

	expected := "NAME      LOCAL-DOMAIN   ACTIVE          INACTIVE        CREATED\n" +
		"test1     zone1          [zone1 zone2]   [zone3 zone4]   \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetClusterDomainsStatusWithChanges(t *testing.T) {
	defer resetTest()
	cds := createClusterDomainsStatus(t, "test1")
	cmdArgs := []string{"get", "clusterdomainsstatus", "test1"}

	expected := "NAME      LOCAL-DOMAIN   ACTIVE          INACTIVE        CREATED\n" +
		"test1     zone1          [zone1 zone2]   [zone3 zone4]   \n"
	testCommon(t, cmdArgs, nil, expected, false)

	cds.Status.LocalDomain = "zone1"
	cds.Status.Active = []string{"zone1", "zone2", "zone3"}
	cds.Status.Inactive = []string{"zone4"}
	_, err := k8s.Instance().UpdateClusterDomainsStatus(cds)
	require.NoError(t, err, "Error updating cluster domains status")

	cmdArgs = []string{"get", "clusterdomainsstatus", "test1"}

	expected = "NAME      LOCAL-DOMAIN   ACTIVE                INACTIVE   CREATED\n" +
		"test1     zone1          [zone1 zone2 zone3]   [zone4]    \n"
	testCommon(t, cmdArgs, nil, expected, false)

}

func TestGetClusterDomainsStatusNotFound(t *testing.T) {
	defer resetTest()
	createClusterDomainsStatus(t, "test1")
	cmdArgs := []string{"get", "clusterdomainsstatus", "test2"}

	expected := `Error from server (NotFound): clusterdomainsstatuses.stork.libopenstorage.org "test2" not found`
	testCommon(t, cmdArgs, nil, expected, true)
}
