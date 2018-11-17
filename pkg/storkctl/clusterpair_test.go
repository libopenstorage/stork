// +build unittest

package storkctl

import (
	//"reflect"
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	//meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetClusterPairNoPair(t *testing.T) {
	cmdArgs := []string{"clusterpair"}

	var clusterPairs storkv1.ClusterPairList
	expected := "No resources found.\n"
	testCommon(t, newGetCommand, cmdArgs, &clusterPairs, expected, false)
}

func TestGetClusterPairNotFound(t *testing.T) {
	cmdArgs := []string{"clusterpair", "pair1"}

	var clusterPairs storkv1.ClusterPairList
	expected := `Error from server (NotFound): clusterpairs.stork.libopenstorage.org "pair1" not found`
	testCommon(t, newGetCommand, cmdArgs, &clusterPairs, expected, true)
}

// FIXME
//func TestGenerateClusterPair(t *testing.T) {
//	cmdArgs := []string{"clusterpair", "pair1"}
//
//	var clusterPairs storkv1.ClusterPairList
//	clusterPair := &storkv1.ClusterPair{
//		TypeMeta: meta.TypeMeta{
//			Kind:       reflect.TypeOf(storkv1.ClusterPair{}).Name(),
//			APIVersion: storkv1.SchemeGroupVersion.String(),
//		},
//		ObjectMeta: meta.ObjectMeta{
//			Name: "pair1",
//		},
//
//		Spec: storkv1.ClusterPairSpec{
//			Options: map[string]string{},
//		},
//	}
//
//	clusterPairs.Items = append(clusterPairs.Items, *clusterPair)
//
//	expected := ""
//	testCommon(t, newGenerateCommand, cmdArgs, &clusterPairs, expected, false)
//}
