package asyncdr

import (
	"fmt"
	"github.com/portworx/torpedo/pkg/log"
	"io/ioutil"
	"os"
	"strings"
	"time"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	clusterName            = "tp-cluster"
	restoreNamePrefix      = "tp-restore"
	configMapName          = "kubeconfigs"
	migrationRetryTimeout  = 10 * time.Minute
	migrationRetryInterval = 10 * time.Second

	// DefaultClusterPairDir default directory where kubeconfig files will be generated
	DefaultClusterPairDir = "cluster-pair"

	// DefaultClusterPairName default name of the cluster pair
	DefaultClusterPairName = "remoteclusterpair"

	sourceClusterName      = "source-cluster"
	destinationClusterName = "destination-cluster"
	backupLocationName     = "tp-blocation"

	storkDeploymentName      = "stork"
	storkDeploymentNamespace = "kube-system"

	appReadinessTimeout = 10 * time.Minute
	migrationKey        = "async-dr-"
	kubeconfigDirectory = "/tmp"
)

var (
	orgID      string
	bucketName string
)

// WriteKubeconfigToFiles - writes kubeconfig to files after reading the names from environment variable
func WriteKubeconfigToFiles() error {
	log.Infof("RK=> Writing kubeconfig")
	kubeconfigs := os.Getenv("KUBECONFIGS")
	if kubeconfigs == "" {
		return fmt.Errorf("KUBECONFIGS Environment variable should not be empty")
	}

	kubeconfigList := strings.Split(kubeconfigs, ",")
	// Validate user has provided at least 1 kubeconfig for cluster
	if len(kubeconfigList) < 2 {
		return fmt.Errorf("A minimum of two kubeconfigs required for async DR tests")
	}

	return DumpKubeconfigs(kubeconfigList)

}

// CreateMigration - creates migration CRD based on the options passed
func CreateMigration(
	name string,
	namespace string,
	clusterPair string,
	migrationNamespace string,
	includeResources *bool,
	startApplications *bool,
) (*storkapi.Migration, error) {

	migration := &storkapi.Migration{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: storkapi.MigrationSpec{
			ClusterPair:       clusterPair,
			IncludeResources:  includeResources,
			StartApplications: startApplications,
			Namespaces:        []string{migrationNamespace},
		},
	}
	// TODO figure out a way to check if it's an auth-enabled and add security annotations

	mig, err := storkops.Instance().CreateMigration(migration)
	return mig, err
}

func deleteMigrations(migrations []*storkapi.Migration) error {
	for _, mig := range migrations {
		err := storkops.Instance().DeleteMigration(mig.Name, mig.Namespace)
		if err != nil {
			return fmt.Errorf("Failed to delete migration %s in namespace %s. Error: %v", mig.Name, mig.Namespace, err)
		}
	}
	return nil
}

// WaitForMigration - waits until all migrations in the given list are successful
func WaitForMigration(migrationList []*storkapi.Migration) error {
	checkMigrations := func() (interface{}, bool, error) {
		isComplete := true
		for _, m := range migrationList {
			mig, err := storkops.Instance().GetMigration(m.Name, m.Namespace)
			if err != nil {
				return "", false, err
			}
			if mig.Status.Status != storkapi.MigrationStatusSuccessful {
				log.Infof("Migration %s in namespace %s is pending", m.Name, m.Namespace)
				isComplete = false
			}
		}
		if isComplete {
			return "", false, nil
		}
		return "", true, fmt.Errorf("some migrations are still pending")
	}
	_, err := task.DoRetryWithTimeout(checkMigrations, migrationRetryTimeout, migrationRetryInterval)
	return err
}

// DeleteAndWaitForMigrationDeletion - deletes the given migration and waits until it's deleted
func DeleteAndWaitForMigrationDeletion(name, namespace string) error {
	log.Infof("Deleting migration: %s in namespace: %s", name, namespace)
	err := storkops.Instance().DeleteMigration(name, namespace)
	if err != nil {
		return fmt.Errorf("Failed to delete migration: %s in namespace: %s", name, namespace)
	}
	getMigration := func() (interface{}, bool, error) {
		migration, err := storkops.Instance().GetMigration(name, namespace)
		if err == nil {
			return "", true, fmt.Errorf("Migration %s in %s has not completed yet.Status: %s. Retrying ", name, namespace, migration.Status.Status)
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(getMigration, migrationRetryTimeout, migrationRetryInterval)
	return err
}

// DumpKubeconfigs gets kubeconfigs from configmap and writes the content to kubeconfig files
func DumpKubeconfigs(kubeconfigList []string) error {
	err := dumpKubeConfigs(configMapName, kubeconfigList)
	if err != nil {
		return fmt.Errorf("Failed to get kubeconfigs [%v] from configmap [%s]: err: %v", kubeconfigList, configMapName, err)
	}
	return nil
}

func dumpKubeConfigs(configObject string, kubeconfigList []string) error {
	log.Infof("dump kubeconfigs to file system")
	cm, err := core.Instance().GetConfigMap(configObject, "default")
	if err != nil {
		log.Errorf("Error reading config map: %v", err)
		return err
	}
	log.Infof("Get over kubeconfig list %v", kubeconfigList)
	for _, kubeconfig := range kubeconfigList {
		config := cm.Data[kubeconfig]
		if len(config) == 0 {
			configErr := fmt.Sprintf("Error reading kubeconfig: found empty %s in config map %s",
				kubeconfig, configObject)
			return fmt.Errorf(configErr)
		}
		filePath := fmt.Sprintf("%s/%s", kubeconfigDirectory, kubeconfig)
		log.Infof("Save kubeconfig to %s", filePath)
		err := ioutil.WriteFile(filePath, []byte(config), 0644)
		if err != nil {
			return err
		}
	}
	return nil
}
