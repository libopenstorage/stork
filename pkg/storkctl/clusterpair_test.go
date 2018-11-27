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

func createClusterPairAndVerify(t *testing.T, name string) {
	options := map[string]string{
		"option1": "value1",
		"option2": "value2",
	}
	clusterPair := &storkv1.ClusterPair{
		ObjectMeta: meta.ObjectMeta{
			Name:      name,
			Namespace: "test",
		},

		Spec: storkv1.ClusterPairSpec{
			Options: options,
		},
	}

	err := k8s.Instance().CreateClusterPair(clusterPair)
	require.NoError(t, err, "Error creating Clusterpair")
	// Make sure it was created correctly
	clusterPair, err = k8s.Instance().GetClusterPair(name, "test")
	require.NoError(t, err, "Error getting Clusterpair")
	require.Equal(t, name, clusterPair.Name, "Clusterpair name mismatch")
	require.Equal(t, "test", clusterPair.Namespace, "Clusterpair namespace mismatch")
	require.Equal(t, options, clusterPair.Spec.Options, "Clusterpair options mismatch")
}

func TestGetClusterPairNoPair(t *testing.T) {
	cmdArgs := []string{"clusterpair"}

	expected := "No resources found.\n"
	testCommon(t, newGetCommand, cmdArgs, nil, expected, false)
}

func TestGetClusterPairNotFound(t *testing.T) {
	defer resetTest()
	createClusterPairAndVerify(t, "pair2")
	cmdArgs := []string{"clusterpair", "pair1"}

	expected := `Error from server (NotFound): clusterpairs.stork.libopenstorage.org "pair1" not found`
	testCommon(t, newGetCommand, cmdArgs, nil, expected, true)
}

func TestGetClusterPair(t *testing.T) {
	defer resetTest()

	createClusterPairAndVerify(t, "getclusterpairtest")

	cmdArgs := []string{"clusterpair", "getclusterpairtest"}
	expected := "NAME                 STORAGE-STATUS   SCHEDULER-STATUS   CREATED\n" +
		"getclusterpairtest                                       \n"
	testCommon(t, newGetCommand, cmdArgs, nil, expected, false)

	createClusterPairAndVerify(t, "getclusterpairtest2")
	testCommon(t, newGetCommand, cmdArgs, nil, expected, false)

	cmdArgs = []string{"clusterpair", "getclusterpairtest", "getclusterpairtest2"}
	expected = "NAME                  STORAGE-STATUS   SCHEDULER-STATUS   CREATED\n" +
		"getclusterpairtest                                        \n" +
		"getclusterpairtest2                                       \n"
	testCommon(t, newGetCommand, cmdArgs, nil, expected, false)

	cmdArgs = []string{"clusterpair"}
	testCommon(t, newGetCommand, cmdArgs, nil, expected, false)
}

func TestGetClusterPairsWithStatus(t *testing.T) {
	defer resetTest()
	createClusterPairAndVerify(t, "clusterpairstatustest")
	clusterPair, err := k8s.Instance().GetClusterPair("clusterpairstatustest", "test")
	require.NoError(t, err, "Error getting Clusterpair")
	clusterPair.CreationTimestamp = metav1.Now()
	clusterPair.Status.StorageStatus = storkv1.ClusterPairStatusReady
	clusterPair.Status.SchedulerStatus = storkv1.ClusterPairStatusReady
	_, err = k8s.Instance().UpdateClusterPair(clusterPair)
	require.NoError(t, err, "Error updating Clusterpair")
	cmdArgs := []string{"clusterpair", "clusterpairstatustest"}
	expected := "NAME                    STORAGE-STATUS   SCHEDULER-STATUS   CREATED\n" +
		"clusterpairstatustest   Ready            Ready              " + toTimeString(clusterPair.CreationTimestamp) + "\n"
	testCommon(t, newGetCommand, cmdArgs, nil, expected, false)
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
