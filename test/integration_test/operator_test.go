//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"os"
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/pkg/asyncdr"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
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
	appNameMongo          = "mongo"
	appPathMongo          = "/stork-specs/mongocr.yaml"
	appNameKafka          = "kafka"
	appPathKafka          = "/stork-specs/kafkacr.yaml"
)

func TestOperatorMig(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")

	logrus.Infof("Using stork volume driver: %s", volumeDriverName)
	t.Run("testMongoMig", testMongoMig)
	t.Run("testKafkaMig", testKafkaMig)
}

func testMongoMig(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")

	validateAndDestroyCrMigration(t, appNameMongo, appPathMongo)
}

func testKafkaMig(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")

	validateAndDestroyCrMigration(t, appNameKafka, appPathKafka)
}

func validateAndDestroyCrMigration(t *testing.T, appName string, appPath string) {
	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	appData := asyncdr.GetAppData(appName)
	podsCreated, err := asyncdr.PrepareApp(appName, appPath)
	require.NoError(t, err, "Error creating pods")

	podsCreatedLen := len(podsCreated.Items)
	logrus.Infof("podsCreatedLen: %v", podsCreatedLen)
	sourceClusterConfigPath, err := getClusterConfigPath(srcConfig)
	require.NoError(t, err, "Error getting source config path")

	destClusterConfigPath, err := getClusterConfigPath(destConfig)
	require.NoError(t, err, "Error getting destination config path")

	// validate CRD on source
	err = asyncdr.ValidateCRD(appData.ExpectedCrdList, sourceClusterConfigPath)
	require.NoError(t, err, "Error validating source crds")

	err = scheduleBidirectionalClusterPair(clusterPairName, appData.Ns, "", storkv1.BackupLocationType(backupLocation), backupSecret)
	require.NoError(t, err, "Error creating cluster pair")

	logrus.Infof("Migration Started")
	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	mig, err := asyncdr.CreateMigration(migNamePref+appName, appData.Ns, clusterPairName, appData.Ns, &includeVolumesFlag, &includeResourcesFlag, &startApplicationsFlag)
	err = asyncdr.WaitForMigration([]*storkv1.Migration{mig})
	require.NoError(t, err, "Error waiting for migration")

	// As apps take time to come up on destination, putting some wait time here
	time.Sleep(5 * time.Minute)
	err = setDestinationKubeConfig()
	require.NoError(t, err, "Error setting dest kubeconfig")

	podsMigrated, err := core.Instance().GetPods(appData.Ns, nil)
	require.NoError(t, err, "Error getting migrated pods")

	podsMigratedLen := len(podsMigrated.Items)
	require.Equal(t, podsCreatedLen, podsMigratedLen, "Pods migration failed as len of pods found on source doesnt match with pods found on destination")

	// validate CRD on destination
	err = asyncdr.ValidateCRD(appData.ExpectedCrdList, destClusterConfigPath)
	require.NoError(t, err, "Error validating destination crds")

	// TODO: Need to make more changes for helm uninstall and CR deletion on destination, as of now deleting NS
	logrus.Infof("Delete Destination and Source Ns")
	err = core.Instance().DeleteNamespace(appData.Ns)
	if err != nil {
		logrus.Infof("Error deleting namespace %s: %v\n", appData.Ns, err)
	}
	err = setSourceKubeConfig()
	require.NoError(t, err, "Error setting source kubeconfig")

	err = asyncdr.DeleteAndWaitForMigrationDeletion(mig.Name, mig.Namespace)
	require.NoError(t, err, "Error deleting migration")

	asyncdr.DeleteCRAndUninstallCRD(appData.OperatorName, appPath, appData.Ns)
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
	err = os.WriteFile(filePath, []byte(config), 0644)
	if err != nil {
		return "", err
	}
	return filePath, nil
}
