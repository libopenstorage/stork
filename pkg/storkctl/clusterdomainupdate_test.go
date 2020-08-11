// +build unittest

package storkctl

import (
	"fmt"
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/stretchr/testify/require"
)

func createClusterDomainUpdate(t *testing.T, name string, clusterDomain string, active bool) *storkv1.ClusterDomainUpdate {

	var cmdArgs []string
	var expected string
	if active {
		cmdArgs = []string{"activate", "clusterdomain", "--name", name, clusterDomain}
		expected = fmt.Sprintf("Cluster Domain activate operation started successfully for %v\n", clusterDomain)
	} else {
		cmdArgs = []string{"deactivate", "clusterdomain", "--name", name, clusterDomain}
		expected = fmt.Sprintf("Cluster Domain deactivate operation started successfully for %v\n", clusterDomain)
	}

	testCommon(t, cmdArgs, nil, expected, false)

	cdu, err := storkops.Instance().GetClusterDomainUpdate(name)
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

	expected := "NAME    CLUSTER-DOMAIN   ACTION     STATUS   CREATED\n" +
		"test1   zone1            Activate            \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetClusterDomainUpdateWithChanges(t *testing.T) {
	defer resetTest()
	cdu := createClusterDomainUpdate(t, "test1", "zone1", true)
	cmdArgs := []string{"get", "clusterdomainupdate", "test1"}

	expected := "NAME    CLUSTER-DOMAIN   ACTION     STATUS   CREATED\n" +
		"test1   zone1            Activate            \n"
	testCommon(t, cmdArgs, nil, expected, false)

	cdu.Status.Status = storkv1.ClusterDomainUpdateStatusFailed
	_, err := storkops.Instance().UpdateClusterDomainUpdate(cdu)
	require.NoError(t, err, "Error updating cluster domain")

	cmdArgs = []string{"get", "clusterdomainupdate", "test1"}

	expected = "NAME    CLUSTER-DOMAIN   ACTION     STATUS   CREATED\n" +
		"test1   zone1            Activate   Failed   \n"
	testCommon(t, cmdArgs, nil, expected, false)

	cdu.Status.Status = storkv1.ClusterDomainUpdateStatusSuccessful
	_, err = storkops.Instance().UpdateClusterDomainUpdate(cdu)
	require.NoError(t, err, "Error updating cluster domain")

	cmdArgs = []string{"get", "clusterdomainupdate", "test1"}

	expected = "NAME    CLUSTER-DOMAIN   ACTION     STATUS       CREATED\n" +
		"test1   zone1            Activate   Successful   \n"
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

	expected := "Cluster Domain activate operation started successfully for zone2\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cdus, err := storkops.Instance().ListClusterDomainUpdates()
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

	expected := "Cluster Domain activate operation started successfully for zone2\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "clusterdomainupdate", "testupdate1"}

	expected = "NAME          CLUSTER-DOMAIN   ACTION     STATUS   CREATED\n" +
		"testupdate1   zone2            Activate            \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestActivateClusterDomainNoDomainSpecified(t *testing.T) {
	defer resetTest()
	cmdArgs := []string{"activate", "clusterdomain"}

	expected := "error: exactly one cluster domain name needs to be provided to the activate command"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestActivateAllClusterDomain(t *testing.T) {
	defer resetTest()
	createClusterDomainsStatus(t, "test1")

	cmdArgs := []string{"activate", "clusterdomain", "--all"}

	expected := "Cluster Domain activate operation started successfully for zone3\nCluster Domain activate operation started successfully for zone4\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cdus, err := storkops.Instance().ListClusterDomainUpdates()
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

	expected := "Cluster Domain activate operation started successfully for zone3\nCluster Domain activate operation started successfully for zone4\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "cdu", "testupdate1-0"}

	expected = "NAME            CLUSTER-DOMAIN   ACTION     STATUS   CREATED\n" +
		"testupdate1-0   zone3            Activate            \n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "cdu", "testupdate1-1"}

	expected = "NAME            CLUSTER-DOMAIN   ACTION     STATUS   CREATED\n" +
		"testupdate1-1   zone4            Activate            \n"
	testCommon(t, cmdArgs, nil, expected, false)

}

func TestDeactivateClusterDomain(t *testing.T) {
	defer resetTest()
	name := "zone2"
	cmdArgs := []string{"deactivate", "clusterdomain", name}

	expected := "Cluster Domain deactivate operation started successfully for zone2\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cdus, err := storkops.Instance().ListClusterDomainUpdates()
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

	expected := "Cluster Domain deactivate operation started successfully for zone2\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "clusterdomainupdate", "testupdate1"}

	expected = "NAME          CLUSTER-DOMAIN   ACTION       STATUS   CREATED\n" +
		"testupdate1   zone2            Deactivate            \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDeactivateClusterDomainNoDomainSpecified(t *testing.T) {
	defer resetTest()
	cmdArgs := []string{"deactivate", "clusterdomain"}

	expected := "error: exactly one cluster domain name needs to be provided to the deactivate command"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestActivateClusterDomainWaitSuccess(t *testing.T) {
	defer resetTest()

	name := "testzone"
	domainName := "zone2"
	cmdArgs := []string{"activate", "clusterdomain", domainName, "--name", name, "--wait"}

	expected := "Cluster Domain activate operation started successfully for zone2\nActivating..\n" +
		"Cluster Domain zone2 updated successfully\n"
	go setClusterDomainStatus(name, false, t)
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "clusterdomainupdate", name}
	expected = "NAME       CLUSTER-DOMAIN   ACTION     STATUS       CREATED\ntestzone   zone2            Activate   Successful   \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestActivateClusterDomainWaitFailed(t *testing.T) {
	defer resetTest()

	name := "testzone"
	domainName := "zone2"
	cmdArgs := []string{"activate", "clusterdomain", domainName, "--name", name, "--wait"}

	expected := "Cluster Domain activate operation started successfully for zone2\n" +
		"Activating..\nFailed to update ClusterDomain, Reason : Unavailable\n"
	go setClusterDomainStatus(name, true, t)
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "clusterdomainupdate", name}
	expected = "NAME       CLUSTER-DOMAIN   ACTION     STATUS   CREATED\n" + "testzone   zone2            Activate   Failed   \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDeactivateClusterDomainWaitSuccess(t *testing.T) {
	defer resetTest()

	name := "testzone"
	domainName := "zone2"
	cmdArgs := []string{"deactivate", "clusterdomain", domainName, "--name", name, "--wait"}

	expected := "Cluster Domain deactivate operation started successfully for zone2\n" +
		"Deactivating..\nCluster Domain zone2 updated successfully\n"
	go setClusterDomainStatus(name, false, t)
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "clusterdomainupdate", name}
	expected = "NAME       CLUSTER-DOMAIN   ACTION       STATUS       CREATED\ntestzone   zone2            Deactivate   Successful   \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDectivateClusterDomainWaitFailed(t *testing.T) {
	defer resetTest()

	name := "testzone"
	domainName := "zone2"
	cmdArgs := []string{"deactivate", "clusterdomain", domainName, "--name", name, "--wait"}

	expected := "Cluster Domain deactivate operation started successfully for zone2\n" +
		"Deactivating..\nFailed to update ClusterDomain, Reason : Unavailable\n"
	go setClusterDomainStatus(name, true, t)
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "clusterdomainupdate", name}
	expected = "NAME       CLUSTER-DOMAIN   ACTION       STATUS   CREATED\n" +
		"testzone   zone2            Deactivate   Failed   \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func setClusterDomainStatus(name string, isFail bool, t *testing.T) {
	time.Sleep(10 * time.Second)
	cdu, err := storkops.Instance().GetClusterDomainUpdate(name)
	require.NoError(t, err, "Error getting cluster domain")
	require.Equal(t, cdu.Status.Status, storkv1.ClusterDomainUpdateStatusInitial)
	cdu.Status.Status = storkv1.ClusterDomainUpdateStatusSuccessful
	if isFail {
		cdu.Status.Status = storkv1.ClusterDomainUpdateStatusFailed
		cdu.Status.Reason = "Unavailable"
	}

	_, err = storkops.Instance().UpdateClusterDomainUpdate(cdu)
	require.NoError(t, err, "Error updating cluster domain")
}
