package asyncdr

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/osutils"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	v1 "k8s.io/api/core/v1"
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
	includeVolumes *bool,
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
			IncludeVolumes:    includeVolumes,
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

func HelmRepoAddandCrInstall(helm_repo_name string, helm_repo_url string, namespace string, operator_name string, operator_path string, app_yaml_url string) (*v1.PodList, error) {
	cmd := fmt.Sprintf("helm repo add %v %v", helm_repo_name, helm_repo_url)
	log.InfoD("Running command: %v", cmd)
	_, _, err := osutils.ExecShell(cmd)
	if err != nil {
		log.Errorf("Error running command: %v and err is: %v", cmd, err)
		return nil, err
	}
	cmd = "helm repo update"
	_, _, err = osutils.ExecShell(cmd)
	if err != nil {
		log.Errorf("Error running command: %v and err is: %v", cmd, err)
		return nil, err
	}
	if operator_name != "" && operator_path != "" {
		cmd = fmt.Sprintf("helm upgrade --install %v %v -n %v", operator_name, operator_path, namespace)
		log.InfoD("Running command: %v", cmd)
		_, _, err = osutils.ExecShell(cmd)
		if err != nil {
			log.Errorf("Error running command: %v and err is: %v", cmd, err)
			return nil, err
		}
	}
	if app_yaml_url != "" {
		cmd = fmt.Sprintf("kubectl apply -f %v -n %v", app_yaml_url, namespace)
		log.InfoD("Running command: %v", cmd)
		_, _, err = osutils.ExecShell(cmd)
		if err != nil {
			log.Errorf("Error running command: %v and err is: %v", cmd, err)
			return nil, err
		}
		// Sleeping here, as apps deploys one by one, which takes time to collect all pods
		time.Sleep(5 * time.Minute)
		podList, err := core.Instance().GetPods(namespace, nil)
		if err != nil {
			log.Errorf("Error getting podlist: %v and err is: %v", cmd, err)
			return nil, err
		}
		err = WaitForPodToBeRunning(podList)
		if err != nil {
			return nil, err
		}
		return podList, nil
	}
	return nil, err
}

func ValidateCRD(crdList []string, sourceClusterConfigPath string) error {
	//Generate source config path and provide to api extension
	apiExt, err := apiextensions.NewInstanceFromConfigFile(sourceClusterConfigPath)
	if err != nil {
		log.Errorf("Failed to get new config instance")
		return err
	}
	for _, crd := range crdList {
		err = apiExt.ValidateCRD(crd, time.Duration(1)*time.Minute, time.Duration(1)*time.Minute)
		if err != nil {
			log.Errorf("Verifying CRD %s failed on cluster, err is: %s", crd, err)
			return err
		}
	}
	return err
}

func DeleteCRAndUninstallCRD(helm_release_name string, app_yaml_url string, namespace string) {
	cmd := fmt.Sprintf("kubectl delete -f %v -n %v", app_yaml_url, namespace)
	log.InfoD("Running command: %v", cmd)
	_, _, _ = osutils.ExecShell(cmd)
	// TODO: Need to add code for making sure the apps got del
	cmd = fmt.Sprintf("helm uninstall %v -n %v", helm_release_name, namespace)
	log.InfoD("Running command: %v", cmd)
	_, _, _ = osutils.ExecShell(cmd)
}

func WaitForPodToBeRunning(pods *v1.PodList) error {
	checkPods := func() (interface{}, bool, error) {
		isRunning := true
		for _, p := range pods.Items {
			if p.Status.Phase != v1.PodRunning {
				log.Infof("Pod %s in namespace %s is pending", p.Name, p.Namespace)
				isRunning = false
			}
		}
		if isRunning {
			return "", false, nil
		}
		return "", true, fmt.Errorf("some pods are still pending...")
	}
	_, err := task.DoRetryWithTimeout(checkPods, migrationRetryTimeout, migrationRetryInterval)
	return err
}
