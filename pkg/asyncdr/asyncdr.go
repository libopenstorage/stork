package asyncdr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/portworx/torpedo/pkg/aetosutil"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/osutils"
	"github.com/sirupsen/logrus"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var dash *aetosutil.Dashboard

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

	appReadinessTimeout    = 10 * time.Minute
	DeleteNamespaceTimeout = 5 * time.Minute
	migrationKey           = "async-dr-"
	kubeconfigDirectory    = "/tmp"
)

var (
	orgID      string
	bucketName string
)

type MigrationStatsType struct {
	CreatedOn                       string
	TotalNumberOfVolumes            string
	NumOfMigratedVolumes            string
	TotalNumberOfResources          string
	NumOfMigratedResources          string
	TotalBytesMigrated              string
	ElapsedTimeForVolumeMigration   string
	ElapsedTimeForResourceMigration string
	Application                     string
	StorkVersion                    string
	PortworxVersion                 string
}

func CreateStats(name, namespace, pxversion string) (map[string]string, error) {
	migStats := &MigrationStatsType{}
	mig, err := storkops.Instance().GetMigration(name, namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get migration")
	}
	storkVersion, err := GetStorkVersion()
	log.InfoD("Stork Version: %v", storkVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to get strok version")
	}

	migStats.CreatedOn = mig.CreationTimestamp.Format("2006-01-02 15:04:05")
	migStats.TotalNumberOfVolumes = strconv.Itoa(int(mig.Status.Summary.TotalNumberOfVolumes))
	migStats.NumOfMigratedVolumes = strconv.Itoa(int(mig.Status.Summary.NumberOfMigratedVolumes))
	migStats.TotalNumberOfResources = strconv.Itoa(int(mig.Status.Summary.TotalNumberOfResources))
	migStats.NumOfMigratedResources = strconv.Itoa(int(mig.Status.Summary.NumberOfMigratedResources))
	migStats.TotalBytesMigrated = strconv.Itoa(int(mig.Status.Summary.TotalBytesMigrated))
	migStats.ElapsedTimeForVolumeMigration = mig.Status.Summary.ElapsedTimeForVolumeMigration
	migStats.ElapsedTimeForResourceMigration = mig.Status.Summary.ElapsedTimeForResourceMigration
	migStats.Application = getResourceNamesFromMigration(mig)
	migStats.StorkVersion = storkVersion
	migStats.PortworxVersion = pxversion
	data, _ := json.Marshal(migStats)
	migMap := make(map[string]string)
	json.Unmarshal(data, &migMap)
	log.InfoD("Migration Stats are: %v", migMap)
	return migMap, nil
}

func getResourceNamesFromMigration(mig *storkapi.Migration) string {
	var resourceList []string
	for _, resource := range mig.Status.Resources {
		if resource.Kind == "Deployment" || resource.Kind == "StatefulSet" {
			resourceList = append(resourceList, resource.Name)
		}
	}
	if len(resourceList) > 1 {
		// return comma separated list of apps if there are multiple apps
		return strings.Join(resourceList, ",")
	} else if len(resourceList) == 1 {
		return resourceList[0]
	}
	logrus.Info("App name not found for pushing to DB.")
	return ""
}

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

func CreateMigrationSchedule(
	name string,
	namespace string,
	clusterPair string,
	migrationNamespace string,
	includeVolumes *bool,
	includeResources *bool,
	startApplications *bool,
	sp string,
	suspend *bool,
	autoSuspend bool,
	PurgeDeletedResources *bool,
	SkipServiceUpdate *bool,
	IncludeNetworkPolicyWithCIDR *bool,
	Selectors map[string]string,
	ExcludeSelectors map[string]string,
	PreExecRule string,
	PostExecRule string,
	IncludeOptionalResourceTypes []string,
	SkipDeletedNamespaces *bool,
	TransformSpecs []string,
) (*storkapi.MigrationSchedule, error) {

	migrationSpec := storkapi.MigrationSpec{
		ClusterPair:                  clusterPair,
		IncludeVolumes:               includeVolumes,
		IncludeResources:             includeResources,
		StartApplications:            startApplications,
		Namespaces:                   []string{migrationNamespace},
		PurgeDeletedResources:        PurgeDeletedResources,
		SkipServiceUpdate:            SkipServiceUpdate,
		IncludeNetworkPolicyWithCIDR: IncludeNetworkPolicyWithCIDR,
		Selectors:                    Selectors,
		ExcludeSelectors:             ExcludeSelectors,
		PreExecRule:                  PreExecRule,
		PostExecRule:                 PostExecRule,
		IncludeOptionalResourceTypes: IncludeOptionalResourceTypes,
		SkipDeletedNamespaces:        SkipDeletedNamespaces,
		TransformSpecs:               TransformSpecs,
	}

	TemplateSpec := storkapi.MigrationTemplateSpec{
		Spec: migrationSpec,
	}

	migrationSchedule := &storkapi.MigrationSchedule{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: storkapi.MigrationScheduleSpec{
			Template:           TemplateSpec,
			SchedulePolicyName: sp,
			Suspend:            suspend,
			AutoSuspend:        autoSuspend,
		},
	}
	migSched, err := storkops.Instance().CreateMigrationSchedule(migrationSchedule)
	return migSched, err
}

func CreateSchedulePolicy(policyName string, interval int) (pol *storkapi.SchedulePolicy, err error) {
	schedPolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
	if err != nil {
		log.InfoD("Creating a interval schedule policy %v with interval %v minutes", policyName, interval)
		schedPolicy = &storkapi.SchedulePolicy{
			ObjectMeta: meta_v1.ObjectMeta{
				Name: policyName,
			},
			Policy: storkapi.SchedulePolicyItem{
				Interval: &storkapi.IntervalPolicy{
					IntervalMinutes: interval,
				},
			}}
		schedPolicy, err = storkops.Instance().CreateSchedulePolicy(schedPolicy)
	} else {
		log.Infof("schedPolicy %v already exists", schedPolicy.Name)
	}
	return schedPolicy, err
}

// WaitForNumOfMigration waits for a certain number of migrations to complete.
func WaitForNumOfMigration(schedName string, schedNamespace string, count int, miginterval int) (map[string]string, error) {
	migInterval := time.Minute * time.Duration(miginterval)
	migTimeout := time.Minute * time.Duration(count*miginterval)
	expectedMigrations := make(map[string]string)
	checkNumOfMigrations := func() (interface{}, bool, error) {
		migSchedule, err := storkops.Instance().GetMigrationSchedule(schedName, schedNamespace)
		if err != nil {
			return "", true, fmt.Errorf("failed to get migrationschedule: %v", schedName)
		}
		migrations := migSchedule.Status.Items["Interval"]
		for _, mig := range migrations {
			migration, err := storkops.Instance().GetMigration(mig.Name, schedNamespace)
			if err != nil {
				return "", true, fmt.Errorf("failed to get migration: %v", mig.Name)
			}
			err = WaitForMigration([]*storkapi.Migration{migration})
			if err != nil {
				expectedMigrations[migration.Name] = fmt.Sprintf("Migration failed with error: %v", err)
			}
			expectedMigrations[migration.Name] = "Successful"
		}
		log.InfoD("Waiting to complete %v migrations in %v time, %v completed as of now", count, migTimeout, expectedMigrations)
		if len(expectedMigrations) == count {
			return "", false, nil
		}
		return "", true, fmt.Errorf("some migrations are still pending")
	}
	_, err := task.DoRetryWithTimeout(checkNumOfMigrations, migTimeout, migInterval)
	return expectedMigrations, err
}

func DeleteAndWaitForMigrationSchedDeletion(name, namespace string) error {
	log.Infof("Deleting migration: %s in namespace: %s", name, namespace)
	err := storkops.Instance().DeleteMigrationSchedule(name, namespace)
	if err != nil {
		return fmt.Errorf("failed to delete migration schedule: %s in namespace: %s", name, namespace)
	}
	getMigrationSched := func() (interface{}, bool, error) {
		migration, err := storkops.Instance().GetMigration(name, namespace)
		if err == nil {
			return "", true, fmt.Errorf("migration schedule %s in %s has not completed yet.Status: %s. Retrying ", name, namespace, migration.Status.Status)
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(getMigrationSched, migrationRetryTimeout, migrationRetryInterval)
	return err
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

func CollectNsForDeletion(label map[string]string, createdBeforeTime time.Duration) []string {
	var nsToBeDeleted []string
	data, err := core.Instance().ListNamespaces(label)
	if err == nil {
		for _, val := range data.Items {
			if time.Now().Sub(val.CreationTimestamp.Time) > createdBeforeTime*time.Hour {
				nsToBeDeleted = append(nsToBeDeleted, val.Name)
			}
		}
	}
	return nsToBeDeleted
}

func WaitForNamespaceDeletion(namespaces []string) error {
	for _, ns := range namespaces {
		err := core.Instance().DeleteNamespace(ns)
		if err != nil {
			return fmt.Errorf("Failed to delete namespace: %v", ns)
		}
		log.InfoD("deleting ns: [%v] now", ns)
		getNamespace := func() (interface{}, bool, error) {
			_, err := core.Instance().GetNamespace(ns)
			if err == nil {
				msg := fmt.Sprintf("Namespace [%v] is not deleted yet, waiting for it to be deleted", ns)
				log.InfoD(msg)
				return "", true, fmt.Errorf(msg)
			}
			return nil, false, nil
		}
		if _, err := task.DoRetryWithTimeout(getNamespace, DeleteNamespaceTimeout, 10*time.Second); err != nil {
			return err
		}
	}
	return nil
}

func GetStorkVersion() (string, error) {
	storkDeploymentNamespace, err := k8sutils.GetStorkPodNamespace()
	if err != nil {
		return "", err
	}
	storkDeployment, err := apps.Instance().GetDeployment(storkDeploymentName, storkDeploymentNamespace)
	if err != nil {
		return "", err
	}
	storkImage := storkDeployment.Spec.Template.Spec.Containers[0].Image
	storkImageVersion := strings.Split(storkImage, ":")[len(strings.Split(storkImage, ":"))-1]
	return storkImageVersion, nil
}
