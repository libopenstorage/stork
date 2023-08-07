//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/pkg/asyncdr"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	expectedMongoCrdList = []string{"mongodbcommunity.mongodbcommunity.mongodb.com"}
	kubeconfigDirectory  = "/tmp"
)

func TestOperatorMig(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	logrus.Infof("Using stork volume driver: %s", volumeDriverName)
	t.Run("testoperatormig", testoperatormig)
}

func testoperatormig(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	asyncdr.WriteKubeconfigToFiles()

	var (
		includeVolumesFlag    = true
		includeResourcesFlag  = true
		startApplicationsFlag = true
		nsName                = "mongo"
		repoName              = "mongodb"
		operatorName          = "community-operator"
		appPath               = "/stork-specs/mongocr.yaml"
		clusterPairName       = "mongo-pair" + time.Now().Format("15h03m05s")
		migName               = "mongo-" + time.Now().Format("15h03m05s")
	)

	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	ns, err := core.Instance().GetNamespace(nsName)
	if err != nil {
		logrus.Infof("Creating namespace %v", nsName)
		nsSpec := &v1.Namespace{
			ObjectMeta: meta_v1.ObjectMeta{
				Name: nsName,
			},
		}
		ns, err = core.Instance().CreateNamespace(nsSpec)
		require.NoError(t, err, "Error creating namespace")
	}

	pods_created, err := asyncdr.HelmRepoAddandCrInstall(repoName, "https://mongodb.github.io/helm-charts", ns.Name, operatorName, fmt.Sprintf("%v/%v", repoName, operatorName), appPath)
	require.NoError(t, err, "Error creating pods")
	pods_created_len := len(pods_created.Items)
	logrus.Infof("pods_created_len: %v", pods_created_len)
	sourceClusterConfigPath, err := getSourceClusterConfigPath()
	require.NoError(t, err, "Error getting source config path")
	err = asyncdr.ValidateCRD(expectedMongoCrdList, sourceClusterConfigPath)
	require.NoError(t, err, "Error validating source crds")
	err = scheduleBidirectionalClusterPair(clusterPairName, ns.Name, "", storkv1.BackupLocationType("s3"), "s3secret")
	logrus.Infof("Migration Started")
	setSourceKubeConfig()
	mig, err := asyncdr.CreateMigration(migName, ns.Name, clusterPairName, ns.Name, &includeVolumesFlag, &includeResourcesFlag, &startApplicationsFlag)
	fmt.Printf("Mig out is: %v\nError is: %v", mig, err)
	err = asyncdr.WaitForMigration([]*storkv1.Migration{mig})
	require.NoError(t, err, "Error waiting for migration")
	// As apps take time to come up on destination, putting some wait time here
	time.Sleep(5 * time.Minute)
	setDestinationKubeConfig()
	pods_migrated, err := core.Instance().GetPods(ns.Name, nil)
	require.NoError(t, err, "Error getting migrated pods")
	pods_migrated_len := len(pods_migrated.Items)
	require.Equal(t, pods_created_len, pods_migrated_len, "Pods migration failed as len of pods found on source doesnt match with pods found on destination")
	destClusterConfigPath, err := getDestinationClusterConfigPath()
	require.NoError(t, err, "Error getting destination config path")
	err = asyncdr.ValidateCRD(expectedMongoCrdList, destClusterConfigPath)
	require.NoError(t, err, "Error validating destination crds")
	logrus.Infof("Starting crd deletion")
	asyncdr.DeleteCRAndUninstallCRD(operatorName, appPath, ns.Name)
	setSourceKubeConfig()
	err = asyncdr.DeleteAndWaitForMigrationDeletion(mig.Name, mig.Namespace)
	require.NoError(t, err, "Error deleting migration")
	asyncdr.DeleteCRAndUninstallCRD(operatorName, appPath, ns.Name)
}

// GetSourceClusterConfigPath returns kubeconfig for source
func getSourceClusterConfigPath() (string, error) {
	kubeconfigs := os.Getenv("KUBECONFIGS")
	if kubeconfigs == "" {
		return "", fmt.Errorf("failed to get source config path. Empty KUBECONFIGS environment variable")
	}

	kubeconfigList := strings.Split(kubeconfigs, ",")
	if len(kubeconfigList) < 2 {
		return "", fmt.Errorf(`Failed to get source config path.
				At least minimum two kubeconfigs required but has %d`, len(kubeconfigList))
	}

	log.Infof("Source config path: %s", fmt.Sprintf("%s/%s", kubeconfigDirectory, kubeconfigList[0]))
	return fmt.Sprintf("%s/%s", kubeconfigDirectory, kubeconfigList[0]), nil
}

func getDestinationClusterConfigPath() (string, error) {
	kubeconfigs := os.Getenv("KUBECONFIGS")
	fmt.Printf("kubeconfigs are: %v", kubeconfigs)
	if kubeconfigs == "" {
		return "", fmt.Errorf("failed to get destination config path. Empty KUBECONFIGS environment variable")
	}

	kubeconfigList := strings.Split(kubeconfigs, ",")
	if len(kubeconfigList) < 2 {
		return "", fmt.Errorf(`Failed to get destination config path.
				At least minimum two kubeconfigs required but has %d`, len(kubeconfigList))
	}

	logrus.Infof("Dest config path: %s", fmt.Sprintf("%s/%s", kubeconfigDirectory, kubeconfigList[1]))
	return fmt.Sprintf("%s/%s", kubeconfigDirectory, kubeconfigList[1]), nil
}
