// +build unittest

package storkctl

import (
	"fmt"
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/stretchr/testify/require"
)

func createClusterDomainUpdate(t *testing.T, name string, clusterDomain string, active bool) *storkv1.ClusterDomainUpdate {

	var cmdArgs []string
	var expected string
	if active {
		cmdArgs = []string{"activate", "clusterdomain", "--name", name, clusterDomain}
		expected = fmt.Sprintf("Cluster Domain %v activated successfully\n", clusterDomain)
	} else {
		cmdArgs = []string{"deactivate", "clusterdomain", "--name", name, clusterDomain}
		expected = fmt.Sprintf("Cluster Domain %v deactivated successfully\n", clusterDomain)
	}

	testCommon(t, cmdArgs, nil, expected, false)

	cdu, err := k8s.Instance().GetClusterDomainUpdate(name)
	require.NoError(t, err, "Error getting ClusterDomainUpdate")
	require.Equal(t, name, cdu.Name, "ClusterDomainUpdate name mismatch")
	require.Equal(t, cdu.Spec.ClusterDomain, clusterDomain, "ClusterDomain name mismatch")
	require.Equal(t, cdu.Spec.Active, active, "Active value mismatch")
	return cdu
}

func TestGetClusterDomainUpdateNoUpdates(t *testing.T) {
	cmdArgs := []string{"get", "clusterdomainupdate"}

	expected := "No resources found.\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "cdu"}

	expected = "No resources found.\n"
	testCommon(t, cmdArgs, nil, expected, false)

}

func TestGetClusterDomainUpdate(t *testing.T) {
	defer resetTest()
	createClusterDomainUpdate(t, "test1", "zone1", true)
	cmdArgs := []string{"get", "clusterdomainupdate"}

	expected := "NAME      CLUSTER-DOMAIN   ACTION     STATUS    CREATED\n" +
		"test1     zone1            Activate             \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetClusterDomainUpdateWithChanges(t *testing.T) {
	defer resetTest()
	cdu := createClusterDomainUpdate(t, "test1", "zone1", true)
	cmdArgs := []string{"get", "clusterdomainupdate", "test1"}

	expected := "NAME      CLUSTER-DOMAIN   ACTION     STATUS    CREATED" +
		"\ntest1     zone1            Activate             \n"
	testCommon(t, cmdArgs, nil, expected, false)

	cdu.Status.Status = storkv1.ClusterDomainUpdateStatusFailed
	_, err := k8s.Instance().UpdateClusterDomainUpdate(cdu)
	require.NoError(t, err, "Error updating cluster domain")

	cmdArgs = []string{"get", "clusterdomainupdate", "test1"}

	expected = "NAME      CLUSTER-DOMAIN   ACTION     STATUS    CREATED\n" +
		"test1     zone1            Activate   Failed    \n"
	testCommon(t, cmdArgs, nil, expected, false)

	cdu.Status.Status = storkv1.ClusterDomainUpdateStatusSuccessful
	_, err = k8s.Instance().UpdateClusterDomainUpdate(cdu)
	require.NoError(t, err, "Error updating cluster domain")

	cmdArgs = []string{"get", "clusterdomainupdate", "test1"}

	expected = "NAME      CLUSTER-DOMAIN   ACTION     STATUS       CREATED\n" +
		"test1     zone1            Activate   Successful   \n"
	testCommon(t, cmdArgs, nil, expected, false)

}

func TestGetClusterDomainUpdateNotFound(t *testing.T) {
	defer resetTest()
	createClusterDomainUpdate(t, "test1", "zone1", true)
	cmdArgs := []string{"get", "clusterdomainupdate", "test2"}

	expected := `Error from server (NotFound): clusterdomainupdates.stork.libopenstorage.org "test2" not found`
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestActivateClusterDomain(t *testing.T) {
	defer resetTest()
	name := "zone2"
	cmdArgs := []string{"activate", "clusterdomain", name}

	expected := "Cluster Domain zone2 activated successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cdus, err := k8s.Instance().ListClusterDomainUpdates()
	require.NoError(t, err, "Error listing ClusterDomainUpdate")
	require.Equal(t, len(cdus.Items), 1, "ClusterDomainUpdates count mismatch")
	cdu := cdus.Items[0]
	require.Equal(t, cdu.Spec.ClusterDomain, name, "ClusterDomain name mismatch")
	require.Equal(t, cdu.Spec.Active, true, "Active value mismatch")
}

func TestActivateClusterDomainWithName(t *testing.T) {
	defer resetTest()
	name := "zone2"
	cmdArgs := []string{"activate", "clusterdomain", name, "--name", "testupdate1"}

	expected := "Cluster Domain zone2 activated successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "clusterdomainupdate", "testupdate1"}

	expected = "NAME          CLUSTER-DOMAIN   ACTION     STATUS    CREATED\n" +
		"testupdate1   zone2            Activate             \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestActivateClusterDomainNoDomainSpecified(t *testing.T) {
	defer resetTest()
	cmdArgs := []string{"activate", "clusterdomain"}

	expected := "error: Exactly one cluster domain name needs to be provided to the activate command"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestActivateAllClusterDomain(t *testing.T) {
	defer resetTest()
	createClusterDomainsStatus(t, "test1")

	cmdArgs := []string{"activate", "clusterdomain", "--all"}

	expected := "Cluster Domain zone3 activated successfully\nCluster Domain zone4 activated successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cdus, err := k8s.Instance().ListClusterDomainUpdates()
	require.NoError(t, err, "Error listing ClusterDomainUpdate")
	require.Equal(t, len(cdus.Items), 2, "ClusterDomainUpdates count mismatch")

	for i, cdu := range cdus.Items {
		if i == 0 {
			require.Equal(t, cdu.Spec.ClusterDomain, "zone3", "ClusterDomain name mismatch")
		} else {
			require.Equal(t, cdu.Spec.ClusterDomain, "zone4", "ClusterDomain name mismatch")

		}
		require.Equal(t, cdu.Spec.Active, true, "Active value mismatch")
	}
}

func TestActivateAllClusterDomainWithName(t *testing.T) {
	defer resetTest()
	createClusterDomainsStatus(t, "test1")

	cmdArgs := []string{"activate", "clusterdomain", "--all", "--name", "testupdate1"}

	expected := "Cluster Domain zone3 activated successfully\nCluster Domain zone4 activated successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "cdu", "testupdate1-0"}

	expected = "NAME            CLUSTER-DOMAIN   ACTION     STATUS    CREATED\n" +
		"testupdate1-0   zone3            Activate             \n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "cdu", "testupdate1-1"}

	expected = "NAME            CLUSTER-DOMAIN   ACTION     STATUS    CREATED\n" +
		"testupdate1-1   zone4            Activate             \n"
	testCommon(t, cmdArgs, nil, expected, false)

}

func TestDeactivateClusterDomain(t *testing.T) {
	defer resetTest()
	name := "zone2"
	cmdArgs := []string{"deactivate", "clusterdomain", name}

	expected := "Cluster Domain zone2 deactivated successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cdus, err := k8s.Instance().ListClusterDomainUpdates()
	require.Equal(t, len(cdus.Items), 1, "ClusterDomainUpdates count mismatch")
	cdu := cdus.Items[0]
	require.NoError(t, err, "Error getting ClusterDomainUpdate")
	require.Equal(t, cdu.Spec.ClusterDomain, name, "ClusterDomain name mismatch")
	require.Equal(t, cdu.Spec.Active, false, "Inactive value mismatch")
}

func TestDeactivateClusterDomainWithName(t *testing.T) {
	defer resetTest()
	name := "zone2"
	cmdArgs := []string{"deactivate", "clusterdomain", name, "--name", "testupdate1"}

	expected := "Cluster Domain zone2 deactivated successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "clusterdomainupdate", "testupdate1"}

	expected = "NAME          CLUSTER-DOMAIN   ACTION       STATUS    CREATED\n" +
		"testupdate1   zone2            Deactivate             \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDeactivateClusterDomainNoDomainSpecified(t *testing.T) {
	defer resetTest()
	cmdArgs := []string{"deactivate", "clusterdomain"}

	expected := "error: Exactly one cluster domain name needs to be provided to the deactivate command"
	testCommon(t, cmdArgs, nil, expected, true)
}
