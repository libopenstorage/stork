//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/torpedo/pkg/asyncdr"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

var (
	kubeconfigDirectory   = "/tmp"
	backupLocation        = "s3"
	backupSecret          = "s3secret"
	clusterPairName       = "bid-pair"
	migNamePref           = "operator-"
	includeVolumesFlag    = true
	includeResourcesFlag  = true
	startApplicationsFlag = true
)

func TestOperatorMig(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")
	logrus.Infof("Using stork volume driver: %s", volumeDriverName)
	t.Run("testMongoMig", testMongoMig)
	t.Run("testKafkaMig", testKafkaMig)
	t.Run("testConfluentMig", testConfluentMig)
}

func testMongoMig(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	var (
		appPath = "/stork-specs/mongocr.yaml"
		appName = "mongo"
	)
	require.NoError(t, err, "Error resetting mock time")
	asyncdr.WriteKubeconfigToFiles()
	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	validateAndDestroyCrMigration(t, appName, appPath)
}

func testKafkaMig(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	var (
		appPath = "/stork-specs/kafkacr.yaml"
		appName = "kafka"
	)
	require.NoError(t, err, "Error resetting mock time")
	asyncdr.WriteKubeconfigToFiles()
	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	validateAndDestroyCrMigration(t, appName, appPath)
}

func testConfluentMig(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	var (
		appPath = "/stork-specs/confluentkafkacr.yaml"
		appName = "confluent"
	)
	require.NoError(t, err, "Error resetting mock time")
	asyncdr.WriteKubeconfigToFiles()
	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	validateAndDestroyCrMigration(t, appName, appPath)
}

func validateAndDestroyCrMigration(t *testing.T, appName string, appPath string) {
	appData := asyncdr.GetAppData(appName)
	pods_created, err := asyncdr.PrepareApp(appName, appPath)
	require.NoError(t, err, "Error creating pods")
	pods_created_len := len(pods_created.Items)
	logrus.Infof("pods_created_len: %v", pods_created_len)
	sourceClusterConfigPath, err := getClusterConfigPath(srcConfig)
	require.NoError(t, err, "Error getting source config path")
	destClusterConfigPath, err := getClusterConfigPath(destConfig)
	require.NoError(t, err, "Error getting destination config path")
	// validate CRD on source
	err = asyncdr.ValidateCRD(appData.ExpectedCrdList, sourceClusterConfigPath)
	require.NoError(t, err, "Error validating source crds")
	_, err = storkops.Instance().GetClusterPair(clusterPairName, appData.Ns)
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		require.NoError(t, err, "Error getting clusterpair")
	}
	if k8serrors.IsAlreadyExists(err) {
		err = scheduleBidirectionalClusterPair(clusterPairName, appData.Ns, "", storkv1.BackupLocationType(backupLocation), backupSecret)
		require.NoError(t, err, "Error creating cluster pair")
	}
	logrus.Infof("Migration Started")
	setSourceKubeConfig()
	mig, err := asyncdr.CreateMigration(migNamePref+appName, appData.Ns, clusterPairName, appData.Ns, &includeVolumesFlag, &includeResourcesFlag, &startApplicationsFlag)
	err = asyncdr.WaitForMigration([]*storkv1.Migration{mig})
	require.NoError(t, err, "Error waiting for migration")
	// As apps take time to come up on destination, putting some wait time here
	time.Sleep(5 * time.Minute)
	setDestinationKubeConfig()
	pods_migrated, err := core.Instance().GetPods(appData.Ns, nil)
	require.NoError(t, err, "Error getting migrated pods")
	pods_migrated_len := len(pods_migrated.Items)
	require.Equal(t, pods_created_len, pods_migrated_len, "Pods migration failed as len of pods found on source doesnt match with pods found on destination")
	// validate CRD on destination
	err = asyncdr.ValidateCRD(appData.ExpectedCrdList, destClusterConfigPath)
	require.NoError(t, err, "Error validating destination crds")
	logrus.Infof("Delete Destination and Source Ns")
	err = core.Instance().DeleteNamespace(appData.Ns)
	if err != nil {
		logrus.Infof("Error deleting namespace %s: %v\n", appData.Ns, err)
	}
	setSourceKubeConfig()
	// err = asyncdr.DeleteAndWaitForMigrationDeletion(mig.Name, mig.Namespace)
	// require.NoError(t, err, "Error deleting migration")
	// asyncdr.DeleteCRAndUninstallCRD(appData.OperatorName, appPath, appData.Ns)
	err = core.Instance().DeleteNamespace(appData.Ns)
	if err != nil {
		logrus.Infof("Error deleting namespace %s: %v\n", appData.Ns, err)
	}
}

// GetClusterConfigPath returns kubeconfig path
func getClusterConfigPath(cmName string) (string, error) {
	cm, err := core.Instance().GetConfigMap(cmName, "kube-system")
	if err != nil {
		log.Errorf("Error reading config map: %v", err)
		return "", err
	}
	config := cm.Data["kubeconfig"]
	if len(config) == 0 {
		configErr := fmt.Sprintf("Error reading kubeconfig")
		return "", fmt.Errorf(configErr)
	}
	filePath := fmt.Sprintf("%s/%s", kubeconfigDirectory, cmName)
	log.Infof("Save kubeconfig to %s", filePath)
	err = ioutil.WriteFile(filePath, []byte(config), 0644)
	if err != nil {
		return "", err
	}
	return filePath, nil
}
