// +build unittest

package storkctl

import (
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func createClusterPairAndVerify(t *testing.T, name string, namespace string) {
	options := map[string]string{
		"option1": "value1",
		"option2": "value2",
	}
	clusterPair := &storkv1.ClusterPair{
		ObjectMeta: meta.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},

		Spec: storkv1.ClusterPairSpec{
			Options: options,
		},
	}

	_, err := k8s.Instance().CreateClusterPair(clusterPair)
	require.NoError(t, err, "Error creating Clusterpair")
	// Make sure it was created correctly
	clusterPair, err = k8s.Instance().GetClusterPair(name, namespace)
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

	_, err := k8s.Instance().CreateNamespace("test", nil)
	require.NoError(t, err, "Error creating test namespace")
	_, err = k8s.Instance().CreateNamespace("test1", nil)
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
	clusterPair, err := k8s.Instance().GetClusterPair("clusterpairstatustest", "default")
	require.NoError(t, err, "Error getting Clusterpair")
	clusterPair.CreationTimestamp = metav1.Now()
	clusterPair.Status.StorageStatus = storkv1.ClusterPairStatusReady
	clusterPair.Status.SchedulerStatus = storkv1.ClusterPairStatusReady
	_, err = k8s.Instance().UpdateClusterPair(clusterPair)
	require.NoError(t, err, "Error updating Clusterpair")
	cmdArgs := []string{"get", "clusterpair", "clusterpairstatustest"}
	expected := "NAME                    STORAGE-STATUS   SCHEDULER-STATUS   CREATED\n" +
		"clusterpairstatustest   Ready            Ready              " + toTimeString(clusterPair.CreationTimestamp.Time) + "\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

/*
func TestGenerateClusterPair(t *testing.T) {
	cmdArgs := []string{"clusterpair", "pair1"}

	var clusterPairs storkv1.ClusterPairList
	clusterPair := &storkv1.ClusterPair{
		TypeMeta: meta.TypeMeta{
			Kind:       reflect.TypeOf(storkv1.ClusterPair{}).Name(),
			APIVersion: storkv1.SchemeGroupVersion.String(),
		},
		ObjectMeta: meta.ObjectMeta{
			Name: "pair1",
		},

		Spec: storkv1.ClusterPairSpec{
			Options: map[string]string{},
		},
	}

	clusterPairs.Items = append(clusterPairs.Items, *clusterPair)

	expected := ""
	testCommon(t, newGenerateCommand, cmdArgs, &clusterPairs, expected, false)
}
*/
