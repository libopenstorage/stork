// +build unittest

package storkctl

import (
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func createClusterPairAndVerify(t *testing.T, name string, namespace string) {
	options := map[string]string{
		"option1": "value1",
		"option2": "value2",
	}
	clusterPair := &storkv1.ClusterPair{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},

		Spec: storkv1.ClusterPairSpec{
			Options: options,
		},
	}

	_, err := storkops.Instance().CreateClusterPair(clusterPair)
	require.NoError(t, err, "Error creating Clusterpair")
	// Make sure it was created correctly
	clusterPair, err = storkops.Instance().GetClusterPair(name, namespace)
	require.NoError(t, err, "Error getting Clusterpair")
	require.Equal(t, name, clusterPair.Name, "Clusterpair name mismatch")
	require.Equal(t, namespace, clusterPair.Namespace, "Clusterpair namespace mismatch")
	require.Equal(t, options, clusterPair.Spec.Options, "Clusterpair options mismatch")
}

func TestGetClusterPairNoPair(t *testing.T) {
	cmdArgs := []string{"get", "clusterpair"}

	expected := "No resources found.\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetClusterPairNotFound(t *testing.T) {
	defer resetTest()
	createClusterPairAndVerify(t, "pair2", "test")
	cmdArgs := []string{"get", "clusterpair", "pair1", "-n", "test"}

	expected := `Error from server (NotFound): clusterpairs.stork.libopenstorage.org "pair1" not found`
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestGetClusterPair(t *testing.T) {
	defer resetTest()

	_, err := core.Instance().CreateNamespace("test", nil)
	require.NoError(t, err, "Error creating test namespace")
	_, err = core.Instance().CreateNamespace("test1", nil)
	require.NoError(t, err, "Error creating test1 namespace")

	createClusterPairAndVerify(t, "getclusterpairtest", "test")

	cmdArgs := []string{"get", "clusterpair", "getclusterpairtest", "-n", "test"}
	expected := "NAME                 STORAGE-STATUS   SCHEDULER-STATUS   CREATED\n" +
		"getclusterpairtest                                       \n"
	testCommon(t, cmdArgs, nil, expected, false)

	createClusterPairAndVerify(t, "getclusterpairtest2", "test")
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "clusterpair", "getclusterpairtest", "getclusterpairtest2", "-n", "test"}
	expected = "NAME                  STORAGE-STATUS   SCHEDULER-STATUS   CREATED\n" +
		"getclusterpairtest                                        \n" +
		"getclusterpairtest2                                       \n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "clusterpair", "-n", "test"}
	testCommon(t, cmdArgs, nil, expected, false)

	createClusterPairAndVerify(t, "getclusterpairtest", "test1")
	cmdArgs = []string{"get", "clusterpair", "-n", "test1"}
	expected = "NAME                 STORAGE-STATUS   SCHEDULER-STATUS   CREATED\n" +
		"getclusterpairtest                                       \n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "clusterpair", "--all-namespaces"}
	expected = "NAMESPACE   NAME                  STORAGE-STATUS   SCHEDULER-STATUS   CREATED\n" +
		"test        getclusterpairtest                                        \n" +
		"test        getclusterpairtest2                                       \n" +
		"test1       getclusterpairtest                                        \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetClusterPairsWithStatus(t *testing.T) {
	defer resetTest()
	createClusterPairAndVerify(t, "clusterpairstatustest", "default")
	clusterPair, err := storkops.Instance().GetClusterPair("clusterpairstatustest", "default")
	require.NoError(t, err, "Error getting Clusterpair")
	clusterPair.CreationTimestamp = metav1.Now()
	clusterPair.Status.StorageStatus = storkv1.ClusterPairStatusReady
	clusterPair.Status.SchedulerStatus = storkv1.ClusterPairStatusReady
	_, err = storkops.Instance().UpdateClusterPair(clusterPair)
	require.NoError(t, err, "Error updating Clusterpair")
	cmdArgs := []string{"get", "clusterpair", "clusterpairstatustest"}
	expected := "NAME                    STORAGE-STATUS   SCHEDULER-STATUS   CREATED\n" +
		"clusterpairstatustest   Ready            Ready              " + toTimeString(clusterPair.CreationTimestamp.Time) + "\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGenerateClusterPairInvalidName(t *testing.T) {
	cmdArgs := []string{"generate", "clusterpair", "pair_test", "-n", "test"}

	expected := "error: the Name \"pair_test\" is not valid: [a DNS-1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')]"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestGenerateClusterPairInvalidNamespace(t *testing.T) {
	cmdArgs := []string{"generate", "clusterpair", "pair1", "-n", "test_namespace"}

	expected := "error: the Namespace \"test_namespace\" is not valid: [a DNS-1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')]"
	testCommon(t, cmdArgs, nil, expected, true)
}
