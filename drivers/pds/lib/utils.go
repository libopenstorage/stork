package lib

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/portworx/torpedo/pkg/log"

	state "net/http"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	pdsapi "github.com/portworx/torpedo/drivers/pds/api"
	pdscontrolplane "github.com/portworx/torpedo/drivers/pds/controlplane"
	tc "github.com/portworx/torpedo/drivers/pds/targetcluster"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

type Parameter struct {
	DataServiceToTest []struct {
		Name          string `json:"Name"`
		Version       string `json:"Version"`
		Image         string `json:"Image"`
		Replicas      int    `json:"Replicas"`
		ScaleReplicas int    `json:"ScaleReplicas"`
		OldVersion    string `json:"OldVersion"`
		OldImage      string `json:"OldImage"`
	} `json:"DataServiceToTest"`
	InfraToTest struct {
		ControlPlaneURL string `json:"ControlPlaneURL"`
		AccountName     string `json:"AccountName"`
		TenantName      string `json:"TenantName"`
		ProjectName     string `json:"ProjectName"`
		ClusterType     string `json:"ClusterType"`
		Namespace       string `json:"Namespace"`
		PxNamespace     string `json:"PxNamespace"`
	} `json:"InfraToTest"`
}

// ResourceSettingTemplate struct used to store template values
type ResourceSettingTemplate struct {
	Resources struct {
		Limits struct {
			CPU    string `json:"cpu"`
			Memory string `json:"memory"`
		} `json:"limits"`
		Requests struct {
			CPU     string `json:"cpu"`
			Memory  string `json:"memory"`
			Storage string `json:"storage"`
		} `json:"requests"`
	} `json:"resources"`
}

// WorkloadGenerationParams has data service creds
type WorkloadGenerationParams struct {
	Host                         string
	User                         string
	Password                     string
	DataServiceName              string
	DeploymentName               string
	DeploymentID                 string
	ScaleFactor                  string
	Iterations                   string
	Namespace                    string
	UseSSL, VerifyCerts, TimeOut string
}

// StorageOptions struct used to store template values
type StorageOptions struct {
	Filesystem  string
	ForceSpread string
	Replicas    int32
	VolumeGroup bool
}

// StorageClassConfig struct used to unmarshal
type StorageClassConfig struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Annotations struct {
		} `json:"annotations"`
		Labels struct {
			Name              string `json:"name"`
			Namespace         string `json:"namespace"`
			PdsDeploymentID   string `json:"pds/deployment-id"`
			PdsDeploymentName string `json:"pds/deployment-name"`
			PdsEnvironment    string `json:"pds/environment"`
			PdsProjectID      string `json:"pds/project-id"`
		} `json:"labels"`
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	Spec struct {
		DNSZone          string `json:"dnsZone"`
		Image            string `json:"image"`
		ImagePullSecrets []struct {
			Name string `json:"name"`
		} `json:"imagePullSecrets"`
		Initialize string `json:"initialize"`
		Nodes      int32  `json:"nodes"`
		Resources  struct {
			Limits struct {
				CPU    string `json:"cpu"`
				Memory string `json:"memory"`
			} `json:"limits"`
			Requests struct {
				CPU     string `json:"cpu"`
				Memory  string `json:"memory"`
				Storage string `json:"storage"`
			} `json:"requests"`
		} `json:"resources"`
		ServiceType  string `json:"serviceType"`
		StorageClass struct {
			Provisioner string `json:"provisioner"`
		} `json:"storageClass"`
		StorageOptions struct {
			Filesystem  string `json:"filesystem"`
			ForceSpread string `json:"forceSpread"`
			Replicas    string `json:"replicas"`
			Secure      string `json:"secure"`
		} `json:"storageOptions"`
		Version string `json:"version"`
	} `json:"spec"`
}

// PDS const
const (
	storageTemplateName   = "QaDefault"
	resourceTemplateName  = "Small"
	appConfigTemplateName = "QaDefault"
	defaultRetryInterval  = 10 * time.Minute
	duration              = 900
	timeOut               = 30 * time.Minute
	timeInterval          = 10 * time.Second
	maxtimeInterval       = 30 * time.Second
	envDsVersion          = "DS_VERSION"
	envDsBuild            = "DS_BUILD"
	zookeeper             = "ZooKeeper"
	redis                 = "Redis"
	consul                = "Consul"
	cassandraStresImage   = "scylladb/scylla:4.1.11"
	postgresqlStressImage = "portworx/torpedo-pgbench:pdsloadTest"
	consulBenchImage      = "pwxbuild/consul-bench-0.1.1"
	consulAgentImage      = "pwxbuild/consul-agent-0.1.1"
	esRallyImage          = "elastic/rally"
	cbloadImage           = "portworx/pds-loadtests:couchbase-0.0.2"
	pdsTpccImage          = "portworx/torpedo-tpcc-automation:v1"
	redisStressImage      = "redis:latest"
	rmqStressImage        = "pivotalrabbitmq/perf-test:latest"
	postgresql            = "PostgreSQL"
	cassandra             = "Cassandra"
	elasticSearch         = "Elasticsearch"
	couchbase             = "Couchbase"
	rabbitmq              = "RabbitMQ"
	mysql                 = "MySQL"
	pxLabel               = "pds.portworx.com/available"
	defaultParams         = "../drivers/pds/parameters/pds_default_parameters.json"
	pdsParamsConfigmap    = "pds-params"
	configmapNamespace    = "default"
)

// PDS vars
var (
	k8sCore = core.Instance()
	k8sApps = apps.Instance()

	components                            *pdsapi.Components
	deployment                            *pds.ModelsDeployment
	apiClient                             *pds.APIClient
	ns                                    *corev1.Namespace
	pdsAgentpod                           corev1.Pod
	err                                   error
	isavailable                           bool
	isTemplateavailable                   bool
	isVersionAvailable                    bool
	isBuildAvailable                      bool
	currentReplicas                       int32
	deploymentTargetID, storageTemplateID string
	resourceTemplateID                    string
	appConfigTemplateID                   string
	versionID                             string
	imageID                               string
	serviceAccId                          string
	accountID                             string
	projectID                             string
	tenantID                              string
	istargetclusterAvailable              bool
	isAccountAvailable                    bool
	isStorageTemplateAvailable            bool
	serviceType                           = "LoadBalancer"

	dataServiceDefaultResourceTemplateIDMap = make(map[string]string)
	dataServiceNameIDMap                    = make(map[string]string)
	dataServiceNameVersionMap               = make(map[string][]string)
	dataServiceIDImagesMap                  = make(map[string][]string)
	dataServiceNameDefaultAppConfigMap      = make(map[string]string)
	deploymentsMap                          = make(map[string][]*pds.ModelsDeployment)
	namespaceNameIDMap                      = make(map[string]string)
	dataServiceVersionBuildMap              = make(map[string][]string)
	dataServiceImageMap                     = make(map[string][]string)
)

// GetAndExpectStringEnvVar parses a string from env variable.
func GetAndExpectStringEnvVar(varName string) string {
	varValue := os.Getenv(varName)
	return varValue
}

// GetAndExpectIntEnvVar parses an int from env variable.
func GetAndExpectIntEnvVar(varName string) (int, error) {
	varValue := GetAndExpectStringEnvVar(varName)
	varIntValue, err := strconv.Atoi(varValue)
	return varIntValue, err
}

// GetAndExpectBoolEnvVar parses a boolean from env variable.
func GetAndExpectBoolEnvVar(varName string) (bool, error) {
	varValue := GetAndExpectStringEnvVar(varName)
	varBoolValue, err := strconv.ParseBool(varValue)
	return varBoolValue, err
}

func GetDeploymentTargetID(clusterID, tenantID string) (string, error) {
	log.InfoD("Get the Target cluster details")
	targetClusters, err := components.DeploymentTarget.ListDeploymentTargetsBelongsToTenant(tenantID)
	if err != nil {
		return "", fmt.Errorf("error while listing deployments: %v", err)
	}
	if targetClusters == nil {
		return "", fmt.Errorf("target cluster passed is not available to the account/tenant %v", err)
	}
	for i := 0; i < len(targetClusters); i++ {
		if targetClusters[i].GetClusterId() == clusterID {
			deploymentTargetID = targetClusters[i].GetId()
			log.Infof("deploymentTargetID %v", deploymentTargetID)
			log.InfoD("Cluster ID: %v, Name: %v,Status: %v", targetClusters[i].GetClusterId(), targetClusters[i].GetName(), targetClusters[i].GetStatus())
		}
	}
	return deploymentTargetID, nil
}

// ValidateNamespaces validates the namespace is available for pds
func ValidateNamespaces(deploymentTargetID string, ns string, status string) error {
	isavailable = false
	waitErr := wait.Poll(timeOut, timeInterval, func() (bool, error) {
		pdsNamespaces, err := components.Namespace.ListNamespaces(deploymentTargetID)
		if err != nil {
			return false, err
		}
		for _, pdsNamespace := range pdsNamespaces {
			log.Infof("namespace name %v and status %v", *pdsNamespace.Name, *pdsNamespace.Status)
			if *pdsNamespace.Name == ns && *pdsNamespace.Status == status {
				isavailable = true
			}
		}
		if isavailable {
			return true, nil
		}

		return false, nil
	})
	return waitErr
}

// DeletePDSNamespace deletes the given namespace
func DeletePDSNamespace(namespace string) error {
	err := k8sCore.DeleteNamespace(namespace)
	return err
}

// UpdatePDSNamespce updates the namespace
func UpdatePDSNamespce(name string, nsLables map[string]string) (*corev1.Namespace, error) {
	nsSpec := &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Namespace",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: nsLables,
		},
	}
	ns, err := k8sCore.UpdateNamespace(nsSpec)
	if err != nil {
		return nil, err
	}
	return ns, nil
}

// CreatePDSNamespace checks if the namespace is available in the cluster and pds is enabled on it
func CreatePDSNamespace(namespace string) (*corev1.Namespace, bool, error) {
	ns, err = k8sCore.GetNamespace(namespace)
	isavailable = false
	if err != nil {
		log.Warnf("Namespace not found %v", err)
		if strings.Contains(err.Error(), "not found") {
			nsName := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   namespace,
					Labels: map[string]string{pxLabel: "true"},
				},
			}
			log.InfoD("Creating namespace %v", namespace)
			ns, err = k8sCore.CreateNamespace(nsName)
			if err != nil {
				log.Errorf("Error while creating namespace %v", err)
				return nil, false, err
			}
			isavailable = true
		}
		if !isavailable {
			return nil, false, err
		}
	}
	isavailable = false
	for key, value := range ns.Labels {
		log.Infof("key: %v values: %v", key, value)
		if key == pxLabel && value == "true" {
			log.InfoD("key: %v values: %v", key, value)
			isavailable = true
			break
		}
	}
	if !isavailable {
		return nil, false, nil
	}
	return ns, true, nil
}

// ReadParams reads the params from given or default json
func ReadParams(filename string) (*Parameter, error) {
	var jsonPara Parameter

	if filename == "" {
		filename, err = filepath.Abs(defaultParams)
		log.Infof("filename %v", filename)
		if err != nil {
			return nil, err
		}
		log.Infof("Parameter json file is not used, use initial parameters value.")
		log.InfoD("Reading params from %v ", filename)
		file, err := ioutil.ReadFile(filename)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(file, &jsonPara)
		if err != nil {
			return nil, err
		}
	} else {
		cm, err := core.Instance().GetConfigMap(pdsParamsConfigmap, configmapNamespace)
		if err != nil {
			return nil, err
		}
		if len(cm.Data) > 0 {
			configmap := &cm.Data
			for key, data := range *configmap {
				log.InfoD("key %v \n value %v", key, data)
				json_data := []byte(data)
				err = json.Unmarshal(json_data, &jsonPara)
				if err != nil {
					log.FailOnError(err, "Error while unmarshalling json:")
				}
			}
		}
	}
	return &jsonPara, nil
}

// GetPods returns the list of pods in namespace
func GetPods(namespace string) (*corev1.PodList, error) {
	k8sOps := k8sCore
	podList, err := k8sOps.GetPods(namespace, nil)
	if err != nil {
		return nil, err
	}
	return podList, err
}

// ValidatePods returns err if pods are not up
func ValidatePods(namespace string, podName string) error {

	var newPods []corev1.Pod
	newPodList, err := GetPods(namespace)
	if err != nil {
		return err
	}

	if podName != "" {
		for _, pod := range newPodList.Items {
			if strings.Contains(pod.Name, podName) {
				log.Infof("%v", pod.Name)
				newPods = append(newPods, pod)
			}
		}
	} else {
		//reinitializing the pods
		newPods = append(newPods, newPodList.Items...)
	}

	//validate deployment pods are up and running
	for _, pod := range newPods {
		log.Infof("pds system pod name %v", pod.Name)
		err = k8sCore.ValidatePod(&pod, timeOut, timeInterval)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteDeploymentPods deletes the given pods
func DeletePods(podList []corev1.Pod) error {
	err := k8sCore.DeletePods(podList, true)
	if err != nil {
		return err
	}
	return nil
}

// GetStorageTemplate return the storage template id
func GetStorageTemplate(tenantID string) (string, error) {
	log.InfoD("Get the storage template")
	storageTemplates, err := components.StorageSettingsTemplate.ListTemplates(tenantID)
	if err != nil {
		return "", err
	}
	isStorageTemplateAvailable = false
	for i := 0; i < len(storageTemplates); i++ {
		if storageTemplates[i].GetName() == storageTemplateName {
			isStorageTemplateAvailable = true
			log.InfoD("Storage template details -----> Name %v,Repl %v , Fg %v , Fs %v",
				storageTemplates[i].GetName(),
				storageTemplates[i].GetRepl(),
				storageTemplates[i].GetFg(),
				storageTemplates[i].GetFs())
			storageTemplateID = storageTemplates[i].GetId()
		}
	}
	if !isStorageTemplateAvailable {
		log.Fatalf("storage template %v is not available ", storageTemplateName)
	}
	return storageTemplateID, nil
}

// GetResourceTemplate get the resource template id
func GetResourceTemplate(tenantID string, supportedDataService string) (string, error) {
	log.Infof("Get the resource template for each data services")
	resourceTemplates, err := components.ResourceSettingsTemplate.ListTemplates(tenantID)
	if err != nil {
		return "", err
	}
	isavailable = false
	isTemplateavailable = false
	for i := 0; i < len(resourceTemplates); i++ {
		if resourceTemplates[i].GetName() == resourceTemplateName {
			isTemplateavailable = true
			dataService, err := components.DataService.GetDataService(resourceTemplates[i].GetDataServiceId())
			if err != nil {
				return "", err
			}
			if dataService.GetName() == supportedDataService {
				log.Infof("Data service name: %v", dataService.GetName())
				log.Infof("Resource template details ---> Name %v, Id : %v ,DataServiceId %v , StorageReq %v , Memoryrequest %v",
					resourceTemplates[i].GetName(),
					resourceTemplates[i].GetId(),
					resourceTemplates[i].GetDataServiceId(),
					resourceTemplates[i].GetStorageRequest(),
					resourceTemplates[i].GetMemoryRequest())

				isavailable = true
				resourceTemplateID = resourceTemplates[i].GetId()
			}
		}
	}
	if !(isavailable && isTemplateavailable) {
		log.Errorf("Template with Name %v does not exis", resourceTemplateName)
	}
	return resourceTemplateID, nil
}

// GetAllDataserviceResourceTemplate get the resource template id's of supported dataservices and forms supported dataserviceNameIdMap
func GetAllDataserviceResourceTemplate(tenantID string, supportedDataServices []string) (map[string]string, map[string]string, error) {
	log.Infof("Get the resource template for each data services")
	resourceTemplates, err := components.ResourceSettingsTemplate.ListTemplates(tenantID)
	if err != nil {
		return nil, nil, err
	}
	isavailable = false
	isTemplateavailable = false
	for i := 0; i < len(resourceTemplates); i++ {
		if resourceTemplates[i].GetName() == resourceTemplateName {
			isTemplateavailable = true
			dataService, err := components.DataService.GetDataService(resourceTemplates[i].GetDataServiceId())
			if err != nil {
				return nil, nil, err
			}
			for dataKey := range supportedDataServices {
				if dataService.GetName() == supportedDataServices[dataKey] {
					log.Infof("Data service name: %v", dataService.GetName())
					log.Infof("Resource template details ---> Name %v, Id : %v ,DataServiceId %v , StorageReq %v , Memoryrequest %v",
						resourceTemplates[i].GetName(),
						resourceTemplates[i].GetId(),
						resourceTemplates[i].GetDataServiceId(),
						resourceTemplates[i].GetStorageRequest(),
						resourceTemplates[i].GetMemoryRequest())

					dataServiceDefaultResourceTemplateIDMap[dataService.GetName()] =
						resourceTemplates[i].GetId()
					dataServiceNameIDMap[dataService.GetName()] = dataService.GetId()
					isavailable = true
				}
			}
		}
	}
	if !(isavailable && isTemplateavailable) {
		log.Errorf("Template with Name %v does not exis", resourceTemplateName)
	}
	return dataServiceDefaultResourceTemplateIDMap, dataServiceNameIDMap, nil
}

// GetAppConfTemplate returns the app config template id
func GetAppConfTemplate(tenantID string, supportedDataService string) (string, error) {
	appConfigs, err := components.AppConfigTemplate.ListTemplates(tenantID)
	if err != nil {
		return "", err
	}
	isavailable = false
	isTemplateavailable = false
	dataServiceId := GetDataServiceID(supportedDataService)
	for i := 0; i < len(appConfigs); i++ {
		if appConfigs[i].GetName() == appConfigTemplateName {
			isTemplateavailable = true
			if dataServiceId == appConfigs[i].GetDataServiceId() {
				appConfigTemplateID = appConfigs[i].GetId()
				isavailable = true
			}
		}
	}
	if !(isavailable && isTemplateavailable) {
		log.Errorf("App Config Template with name %v does not exist", appConfigTemplateName)
	}
	return appConfigTemplateID, nil
}

// GetAllDataServiceAppConfTemplate returns the supported app config templates
func GetAllDataServiceAppConfTemplate(tenantID string, dataServiceNameIDMap map[string]string) (map[string]string, error) {
	appConfigs, err := components.AppConfigTemplate.ListTemplates(tenantID)
	if err != nil {
		return nil, err
	}
	isavailable = false
	isTemplateavailable = false
	for i := 0; i < len(appConfigs); i++ {
		if appConfigs[i].GetName() == appConfigTemplateName {
			isTemplateavailable = true
			for key := range dataServiceNameIDMap {
				if dataServiceNameIDMap[key] == appConfigs[i].GetDataServiceId() {
					dataServiceNameDefaultAppConfigMap[key] = appConfigs[i].GetId()
					isavailable = true
				}
			}
		}
	}
	if !(isavailable && isTemplateavailable) {
		log.Errorf("App Config Template with name %v does not exist", appConfigTemplateName)
	}
	return dataServiceNameDefaultAppConfigMap, nil
}

// GetnameSpaceID returns the namespace ID
func GetnameSpaceID(namespace string, deploymentTargetID string) (string, error) {
	var namespaceID string

	err = wait.Poll(timeInterval, timeOut, func() (bool, error) {
		namespaces, err := components.Namespace.ListNamespaces(deploymentTargetID)
		for i := 0; i < len(namespaces); i++ {
			if namespaces[i].GetName() == namespace {
				if namespaces[i].GetStatus() == "available" {
					namespaceID = namespaces[i].GetId()
					namespaceNameIDMap[namespaces[i].GetName()] = namespaces[i].GetId()
					log.InfoD("Namespace Status - Name: %v , Id: %v , Status: %v", namespaces[i].GetName(), namespaces[i].GetId(), namespaces[i].GetStatus())
					return true, nil
				}
			}
		}
		if err != nil {
			log.Errorf("An Error Occured while listing namespaces %v", err)
			return false, err
		}
		return false, nil
	})
	return namespaceID, nil
}

// GetVersionsImage returns the required Image of dataservice version
func GetVersionsImage(dsVersion string, dsBuild string, dataServiceID string) (string, string, map[string][]string, error) {
	var versions []pds.ModelsVersion
	var images []pds.ModelsImage

	versions, err = components.Version.ListDataServiceVersions(dataServiceID)
	if err != nil {
		return "", "", nil, err
	}
	isVersionAvailable = false
	isBuildAvailable = false
	for i := 0; i < len(versions); i++ {
		if (*versions[i].Enabled) && (*versions[i].Name == dsVersion) {
			images, _ = components.Image.ListImages(versions[i].GetId())
			for j := 0; j < len(images); j++ {
				if *images[j].Build == dsBuild {
					versionID = versions[i].GetId()
					imageID = images[j].GetId()
					dataServiceVersionBuildMap[versions[i].GetName()] = append(dataServiceVersionBuildMap[versions[i].GetName()], images[j].GetBuild())
					isBuildAvailable = true
					break
				}
			}
			isVersionAvailable = true
			break
		}
	}
	if !(isVersionAvailable && isBuildAvailable) {
		return "", "", nil, fmt.Errorf("version/build passed is not available")
	}
	return versionID, imageID, dataServiceVersionBuildMap, nil
}

// GetAllVersionsImages returns all the versions and Images of dataservice
func GetAllVersionsImages(dataServiceID string) (map[string][]string, map[string][]string, error) {
	var versions []pds.ModelsVersion
	var images []pds.ModelsImage

	versions, err = components.Version.ListDataServiceVersions(dataServiceID)
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < len(versions); i++ {
		if *versions[i].Enabled {
			images, _ = components.Image.ListImages(versions[i].GetId())
			for j := 0; j < len(images); j++ {
				dataServiceIDImagesMap[versions[i].GetId()] = append(dataServiceIDImagesMap[versions[i].GetId()], images[j].GetId())
				dataServiceVersionBuildMap[versions[i].GetName()] = append(dataServiceVersionBuildMap[versions[i].GetName()], images[j].GetBuild())
			}
		}
	}

	for key := range dataServiceVersionBuildMap {
		log.Infof("Version - %v,Build - %v", key, dataServiceVersionBuildMap[key])
	}
	for key := range dataServiceIDImagesMap {
		log.Infof("DS Verion id - %v,DS Image id - %v", key, dataServiceIDImagesMap[key])
	}
	return dataServiceNameVersionMap, dataServiceIDImagesMap, nil
}

func ValidatePDSDeploymentStatus(deployment *pds.ModelsDeployment, healthStatus string, maxtimeInterval time.Duration, timeout time.Duration) error {
	//validate the deployments in pds
	err = wait.Poll(maxtimeInterval, timeOut, func() (bool, error) {
		status, res, err := components.DataServiceDeployment.GetDeploymentStatus(deployment.GetId())
		log.Infof("Health status -  %v", status.GetHealth())
		if err != nil {
			log.Infof("Deployment status %v", err)
			return false, nil
		}
		if res.StatusCode != state.StatusOK {
			log.Infof("Full HTTP response: %v\n", res)
			err = fmt.Errorf("unexpected status code")
			return false, err
		}
		if !strings.Contains(status.GetHealth(), healthStatus) {
			log.Infof("status: %v", status.GetHealth())
			return false, nil
		}
		log.Infof("Deployment details: Health status -  %v,Replicas - %v, Ready replicas - %v", status.GetHealth(), status.GetReplicas(), status.GetReadyReplicas())
		return true, nil
	})
	return err
}

// ValidateDataServiceDeployment checks if deployment is healthy and running
// TODO: Add explicit timeout param and update the relevant tests implementing ValidateDataServiceDeployment func.
// JIRA: PA-401
func ValidateDataServiceDeployment(deployment *pds.ModelsDeployment, namespace string) error {
	var ss *v1.StatefulSet

	err = wait.Poll(maxtimeInterval, timeOut, func() (bool, error) {
		ss, err = k8sApps.GetStatefulSet(deployment.GetClusterResourceName(), namespace)
		if err != nil {
			log.Warnf("An Error Occured while getting statefulsets %v", err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		log.Errorf("An Error Occured while getting statefulsets %v", err)
		return err
	}
	//validate the statefulset deployed in the k8s namespace
	err = k8sApps.ValidateStatefulSet(ss, timeOut)
	if err != nil {
		log.Errorf("An Error Occured while validating statefulsets %v", err)
		return err
	}

	//validate the deployments in pds
	err = wait.Poll(maxtimeInterval, timeOut, func() (bool, error) {
		status, res, err := components.DataServiceDeployment.GetDeploymentStatus(deployment.GetId())
		log.Infof("Health status -  %v", status.GetHealth())
		if err != nil {
			log.Errorf("Error occured while getting deployment status %v", err)
			return false, nil
		}
		if res.StatusCode != state.StatusOK {
			log.Errorf("Error when calling `ApiDeploymentsIdCredentialsGet``: %v\n", err)
			log.Errorf("Full HTTP response: %v\n", res)
			return false, err
		}
		if status.GetHealth() != "Healthy" {
			return false, nil
		}
		log.Infof("Deployment details: Health status -  %v,Replicas - %v, Ready replicas - %v", status.GetHealth(), status.GetReplicas(), status.GetReadyReplicas())
		return true, nil
	})
	return err
}

// DeleteK8sPods deletes the pods in given namespace
func DeleteK8sPods(pod string, namespace string) error {
	err := k8sCore.DeletePod(pod, namespace, true)
	return err
}

// DeleteK8sDeployments deletes the deployments in given namespace
func DeleteK8sDeployments(deployment string, namespace string) error {
	err := k8sApps.DeleteDeployment(deployment, namespace)
	return err
}

// DeleteDeployment deletes the given deployment
func DeleteDeployment(deploymentID string) (*state.Response, error) {
	resp, err := components.DataServiceDeployment.DeleteDeployment(deploymentID)
	if err != nil {
		log.Errorf("An Error Occured while deleting deployment %v", err)
		return nil, err
	}
	return resp, nil
}

// GetDeploymentConnectionInfo returns the dns endpoint
func GetDeploymentConnectionInfo(deploymentID string) (string, error) {
	var isfound bool
	var dnsEndpoint string

	dataServiceDeployment := components.DataServiceDeployment
	deploymentConnectionDetails, clusterDetails, err := dataServiceDeployment.GetConnectionDetails(deploymentID)
	deploymentConnectionDetails.MarshalJSON()
	if err != nil {
		log.Errorf("An Error Occured %v", err)
		return "", err
	}
	deploymentNodes := deploymentConnectionDetails.GetNodes()
	log.Infof("Deployment nodes %v", deploymentNodes)
	isfound = false
	for key, value := range clusterDetails {
		log.Infof("host details key %v value %v", key, value)
		if strings.Contains(key, "host") || strings.Contains(key, "nodes") {
			dnsEndpoint = fmt.Sprint(value)
			isfound = true
		}
	}
	if !isfound {
		log.Errorf("No connection string found")
		return "", err
	}

	return dnsEndpoint, nil
}

// GetDeploymentCredentials returns the password to connect to the dataservice
func GetDeploymentCredentials(deploymentID string) (string, error) {
	dataServiceDeployment := components.DataServiceDeployment
	dataServicePassword, err := dataServiceDeployment.GetDeploymentCredentials(deploymentID)
	if err != nil {
		log.Errorf("An Error Occured %v", err)
		return "", err
	}
	pdsPassword := dataServicePassword.GetPassword()
	return pdsPassword, nil
}

// This module sets up MySQL Database for Running TPCC. There is some specific requirement that needs to be
// done for MySQL before running MySQL.
func SetupMysqlDatabaseForTpcc(dbUser string, pdsPassword string, dnsEndpoint string, namespace string) bool {
	log.InfoD("Trying to configure Mysql deployment for TPCC Workload")
	podSpec := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "configure-mysql-",
			Namespace:    namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:            "configure-mysql",
					Image:           pdsTpccImage,
					Command:         []string{"/bin/sh", "-C", "setup-mysql-for-tpcc.sh", dbUser, pdsPassword, dnsEndpoint},
					WorkingDir:      "/sysbench-tpcc",
					ImagePullPolicy: corev1.PullAlways,
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}
	_, err := k8sCore.CreatePod(podSpec)
	if err != nil {
		log.Errorf("An Error Occured while creating %v", err)
		return false
	}

	//Static sleep to let DB changes settle in
	time.Sleep(20 * time.Second)
	var newPods []corev1.Pod
	newPodList, err := GetPods(namespace)
	if err != nil {
		return false
	}
	//reinitializing the pods
	newPods = append(newPods, newPodList.Items...)

	// Validate if MySQL pod is configured successfully or not for running TPCC
	for _, pod := range newPods {
		if strings.Contains(pod.Name, "configure-mysql") {
			log.InfoD("pds system pod name %v", pod.Name)
			for _, c := range pod.Status.ContainerStatuses {
				if c.State.Terminated.ExitCode == 0 && c.State.Terminated.Reason == "Completed" {
					log.InfoD("Successfully Configured Mysql for TPCC Run. Exiting")
					DeleteK8sPods(pod.Name, namespace)
					return true
				} else {
					DeleteK8sPods(pod.Name, namespace)
				}
			}
		}
	}
	return false
}

// This module creates TPCC Schema for a given Deployment and then Runs TPCC Workload
func RunTpccWorkload(dbUser string, pdsPassword string, dnsEndpoint string, dbName string,
	timeToRun string, numOfThreads string, numOfCustomers string, numOfWarehouses string,
	deploymentName string, namespace string, dataServiceName string) bool {
	var fileToRun string
	if dataServiceName == postgresql {
		dbName = "pds"
		fileToRun = "tpcc-pg-run.sh" // file to run in case of Postgres workload
	}
	if dataServiceName == mysql {
		dbName = "tpcc"
		fileToRun = "tpcc-mysql-run.sh" // File to run in case of MySQL workload
	}
	if dbUser == "" {
		dbUser = "pds"
	}
	if timeToRun == "" {
		timeToRun = "120" // Default time to run is 2 minutes
	}
	if numOfThreads == "" {
		numOfThreads = "64" // Default threads is 64
	}
	if numOfCustomers == "" {
		numOfCustomers = "2" // Default number of customer and districts is 4
	}
	if numOfWarehouses == "" {
		numOfWarehouses = "1" // Default number of warehouses to simulate is 2
	}
	// Create a Deployment to Prepare and Run TPCC Workload
	var replicas int32 = 1
	deploymentSpec := &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: deploymentName + "-",
			Namespace:    namespace,
		},
		Spec: v1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": deploymentName},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": deploymentName},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "tpcc-run",
							Image: pdsTpccImage,
							Command: []string{"/bin/sh", "-C", fileToRun, dbUser, pdsPassword, dnsEndpoint, dbName,
								timeToRun, numOfThreads, numOfCustomers, numOfWarehouses, "run"},
							WorkingDir: "/sysbench-tpcc",
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:  "tpcc-prepare",
							Image: pdsTpccImage,
							Command: []string{"/bin/sh", "-C", fileToRun, dbUser, pdsPassword, dnsEndpoint, dbName,
								timeToRun, numOfThreads, numOfCustomers, numOfWarehouses, "prepare"},
							WorkingDir:      "/sysbench-tpcc",
							ImagePullPolicy: corev1.PullAlways,
						},
					},
					RestartPolicy: corev1.RestartPolicyAlways,
				},
			},
		},
	}
	log.InfoD("Going to Trigger TPCC Workload for the Deployment")
	deployment, err := k8sApps.CreateDeployment(deploymentSpec, metav1.CreateOptions{})
	if err != nil {
		log.Errorf("An Error Occured while creating deployment %v", err)
		return false
	}

	timeAskedToRun, err := strconv.Atoi(timeToRun)
	flag := false
	// Hard sleep for 10 seconds for deployment to come up
	time.Sleep(10 * time.Second)
	var newPods []corev1.Pod
	for i := 1; i <= 200; i++ {
		newPodList, _ := GetPods(namespace)
		newPods = append(newPods, newPodList.Items...)
		for _, pod := range newPods {
			if strings.Contains(pod.Name, deployment.Name) {
				log.InfoD("Will check for status of Init Container Once......")
				for _, c := range pod.Status.InitContainerStatuses {
					if c.State.Terminated != nil {
						flag = true
					}
				}
			}
		}
		if flag {
			log.InfoD("TPCC Schema Prepared successfully. Moving ahead to run the TPCC Workload now.....")
			break
		} else {
			log.InfoD("Init Container is still running means TPCC Schema is being prepared. Will wait for further 30 Seconds.....")
			time.Sleep(30 * time.Second)
		}
	}
	if !flag {
		log.Errorf("TPCC Schema couldn't be prepared in 100 minutes. Timing Out. Please check manually.")
		return false
	}
	flag = false
	for i := 1; i <= int((timeAskedToRun+300)/60); i++ {
		newPodList, _ := GetPods(namespace)
		newPods = append(newPods, newPodList.Items...)
		for _, pod := range newPods {
			if strings.Contains(pod.Name, deployment.Name) {
				log.InfoD("Waiting for TPCC Workload Container to finish")
				for _, c := range pod.Status.ContainerStatuses {
					if int32(c.RestartCount) != 0 {
						flag = true
						if c.State.Terminated != nil && c.State.Terminated.ExitCode != 0 && c.State.Terminated.Reason != "Completed" {
							log.Errorf("Something went wrong and Run Container Exited abruptly. Leaving the TPCC deployment as is - pls check manually")
							log.InfoD("Printing TPCC Deployment Describe Status here .....")
							depStatus, err := k8sApps.DescribeDeployment(deployment.Name, namespace)
							if err != nil {
								log.Errorf("Could not print TPCC Deployment status due to some reason. Please check manually.")
								return false
							}
							log.InfoD("%+v\n", *depStatus)
							return false
						}
						break
					}
				}
			}
		}
		if flag {
			log.InfoD("TPCC Workload run finished. Finishing this Test Case")
			break
		} else {
			log.InfoD("TPCC Workload is still running. Will wait for further 1 minute to check again.....")
			time.Sleep(1 * time.Minute)
		}
	}
	log.InfoD("Will delete TPCC Worklaod Deployment now.....")
	DeleteK8sDeployments(deployment.Name, namespace)
	return flag
}

// This module runs Consul Bench workload from pre-cooked Consul Agent and Consul Bench images
func RunConsulBenchWorkload(deploymentName string, namespace string) (*v1.Deployment, error) {
	var replicas int32 = 1
	benchmarkName := deploymentName + "-bench"
	deploymentSpec := &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      benchmarkName,
			Namespace: namespace,
		},
		Spec: v1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": benchmarkName},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": benchmarkName},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "consul-agent",
							Image:           consulAgentImage,
							ImagePullPolicy: corev1.PullAlways,
							Env: []corev1.EnvVar{
								{
									Name: "AGENT_TOKEN",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: deploymentName + "-creds",
											},
											Key: "master_token",
										},
									},
								},
								{
									Name:  "PDS_CLUSTER",
									Value: deploymentName,
								},
								{
									Name:  "PDS_NS",
									Value: namespace,
								},
							},
						},
						{
							Name:            "consul-kv-workload",
							Image:           consulAgentImage,
							ImagePullPolicy: corev1.PullAlways,
							Command:         []string{"/bin/kv-workload.sh"},
							Env: []corev1.EnvVar{
								{
									Name: "AGENT_TOKEN",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: deploymentName + "-creds",
											},
											Key: "master_token",
										},
									},
								},
							},
						},
						{
							Name:            "consul-bench",
							Image:           consulBenchImage,
							ImagePullPolicy: corev1.PullAlways,
							Env: []corev1.EnvVar{
								{
									Name: "CONSUL_HTTP_TOKEN",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: deploymentName + "-creds",
											},
											Key: "master_token",
										},
									},
								},
								{
									Name: "SERVICE_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.name",
										},
									},
								},
								{
									Name:  "SERVICE_INSTANCES",
									Value: "10",
								},
								{
									Name:  "SERVICE_FLAP_SECONDS",
									Value: "10",
								},
								{
									Name:  "SERVICE_WATCHERS",
									Value: "10",
								},
							},
						},
					},
				},
			},
		},
	}
	log.InfoD("Going to Trigger Consul Bench Workload for the Deployment")
	deployment, err := k8sApps.CreateDeployment(deploymentSpec, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	err = k8sApps.ValidateDeployment(deployment, timeOut, timeInterval)
	if err != nil {
		return nil, err
	}

	// Sleeping for 1 minute to let the Workload run
	time.Sleep(1 * time.Minute)

	return deployment, nil
}

// Returns a randomly generated string of given length
func GetRandomString(length int32) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	rand.Read(b)
	random_string := fmt.Sprintf("%x", b)[:length]
	return random_string
}

// Creates a temporary non PDS namespace of 6 letters length randomly chosen
func CreateTempNS(length int32) (string, error) {
	namespace := GetRandomString(length)
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}
	ns, err = k8sCore.CreateNamespace(ns)
	if err != nil {
		log.Errorf("Error while creating namespace %v", err)
		return "", err
	}
	return namespace, nil
}

func IsReachable(url string) (bool, error) {
	timeout := time.Duration(15 * time.Second)
	client := http.Client{
		Timeout: timeout,
	}
	_, err := client.Get(url)
	if err != nil {
		return false, err
	}
	return true, nil
}

// SetupPDSTest returns few params required to run the test
func SetupPDSTest(ControlPlaneURL, ClusterType, AccountName, TenantName, ProjectName string) (string, string, string, string, string, error) {
	var err error
	apiConf := pds.NewConfiguration()
	endpointURL, err := url.Parse(ControlPlaneURL)
	if err != nil {
		return "", "", "", "", "", err
	}
	apiConf.Host = endpointURL.Host
	apiConf.Scheme = endpointURL.Scheme

	apiClient = pds.NewAPIClient(apiConf)
	components = pdsapi.NewComponents(apiClient)
	controlplane := pdscontrolplane.NewControlPlane(ControlPlaneURL, components)

	if strings.EqualFold(ClusterType, "onprem") || strings.EqualFold(ClusterType, "ocp") {
		serviceType = "ClusterIP"
	}
	log.InfoD("Deployment service type %s", serviceType)

	acc := components.Account
	accounts, err := acc.GetAccountsList()
	if err != nil {
		return "", "", "", "", "", err
	}

	isAccountAvailable = false
	for i := 0; i < len(accounts); i++ {
		log.InfoD("Account Name: %v", accounts[i].GetName())
		if accounts[i].GetName() == AccountName {
			isAccountAvailable = true
			accountID = accounts[i].GetId()
			break
		}
	}
	if !isAccountAvailable {
		return "", "", "", "", "", fmt.Errorf("account %v is not available", AccountName)
	}
	log.InfoD("Account Detail- Name: %s, UUID: %s ", AccountName, accountID)
	tnts := components.Tenant
	tenants, _ := tnts.GetTenantsList(accountID)
	for _, tenant := range tenants {
		if tenant.GetName() == TenantName {
			tenantID = tenant.GetId()
			break
		}

	}
	log.InfoD("Tenant Details- Name: %s, UUID: %s ", TenantName, tenantID)
	dnsZone, err := controlplane.GetDNSZone(tenantID)
	if err != nil {
		return "", "", "", "", "", err
	}
	log.InfoD("DNSZone: %s, tenantName: %s, accountName: %s", dnsZone, TenantName, AccountName)
	projcts := components.Project
	projects, _ := projcts.GetprojectsList(tenantID)
	for _, project := range projects {
		if project.GetName() == ProjectName {
			projectID = project.GetId()
			break
		}
	}
	log.InfoD("Project Details- Name: %s, UUID: %s ", ProjectName, projectID)

	ns, err = k8sCore.GetNamespace("kube-system")
	if err != nil {
		return "", "", "", "", "", err
	}
	clusterID := string(ns.GetObjectMeta().GetUID())
	if len(clusterID) > 0 {
		log.InfoD("clusterID %v", clusterID)
	} else {
		return "", "", "", "", "", fmt.Errorf("unable to get the clusterID")
	}

	return tenantID, dnsZone, projectID, serviceType, clusterID, err
}

// RegisterClusterToControlPlane checks and registers the given target cluster to the controlplane
func RegisterClusterToControlPlane(controlPlaneUrl, tenantId, clusterType string) error {
	log.InfoD("Test control plane url connectivity.")
	_, err := IsReachable(controlPlaneUrl)
	if err != nil {
		return fmt.Errorf("unable to reach the control plane with following error - %v", err)
	}

	helmChartversion, err := components.APIVersion.GetHelmChartVersion()
	if err != nil {
		return fmt.Errorf("error while getting helm version - %v", helmChartversion)
	}

	log.InfoD("Listing service account")
	listServiceAccounts, err := components.ServiceAccount.ListServiceAccounts(tenantId)
	if err != nil {
		return err
	}
	for _, acc := range listServiceAccounts {
		log.Infof(*acc.Name)
		if *acc.Name == "Default-AgentWriter" {
			serviceAccId = *acc.Id
			break
		}
	}

	log.InfoD("Getting service account token")
	serviceAccToken, err := components.ServiceAccount.GetServiceAccountToken(serviceAccId)
	if err != nil {
		return err
	}
	bearerToken := *serviceAccToken.Token

	ctx := GetAndExpectStringEnvVar("TARGET_KUBECONFIG")
	target := tc.NewTargetCluster(ctx)
	err = target.RegisterToControlPlane(controlPlaneUrl, helmChartversion, bearerToken, tenantId, clusterType)
	if err != nil {
		return fmt.Errorf("target cluster registeration failed with the error: %v", err)
	}
	return nil
}

// Check if PV and associated PVC is still present. If yes then delete both of them
func CheckAndDeleteIndependentPV(name string) error {
	pv_check, err := k8sCore.GetPersistentVolume(name)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil
		}
		return err
	}
	log.InfoD("Stranded PV Found by the name - %s. Going ahead to delete this PV and associated entities", name)
	if pv_check.Status.Phase == corev1.VolumeBound {
		if pv_check.Spec.ClaimRef != nil && pv_check.Spec.ClaimRef.Kind == "PersistentVolumeClaim" {
			namespace := pv_check.Spec.ClaimRef.Namespace
			pvc_name := pv_check.Spec.ClaimRef.Name
			// Delete all Pods in this namespace
			var newPods []corev1.Pod
			podList, err := GetPods(namespace)
			if err != nil {
				return err
			}
			for _, pod := range podList.Items {
				newPods = append(newPods, pod)
			}
			err = DeletePods(newPods)
			if err != nil {
				return err
			}
			// Delete PVC from figured out namespace
			err = k8sCore.DeletePersistentVolumeClaim(pvc_name, namespace)
			if err != nil {
				return err
			}
		}
	}
	// Delete PV as it is still available from previous run
	err = k8sCore.DeletePersistentVolume(name)
	if err != nil {
		return err
	}
	return nil
}

// Create a Persistent Vol of 5G manual Storage Class
func CreateIndependentPV(name string) (*corev1.PersistentVolume, error) {
	err := CheckAndDeleteIndependentPV(name)
	if err != nil {
		return nil, err
	}
	pv := &corev1.PersistentVolume{

		TypeMeta: metav1.TypeMeta{Kind: "PersistentVolume"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},

		Spec: corev1.PersistentVolumeSpec{
			StorageClassName: "manual",
			AccessModes: []corev1.PersistentVolumeAccessMode{
				"ReadWriteOnce",
			},
			Capacity: corev1.ResourceList{
				corev1.ResourceName(corev1.ResourceStorage): resource.MustParse("5Gi"),
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/mnt/data",
				},
			},
		},
	}
	pv, err = k8sCore.CreatePersistentVolume(pv)
	if err != nil {
		return pv, err
	}
	return pv, nil
}

// Create a PV Claim of 5G Storage
func CreateIndependentPVC(namespace string, name string) (*corev1.PersistentVolumeClaim, error) {
	ns := namespace
	storageClass := "manual"
	createOpts := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &storageClass,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("5Gi"),
				},
			},
		},
	}
	pvc, err := k8sCore.CreatePersistentVolumeClaim(createOpts)
	if err != nil {
		log.Errorf("PVC Could not be created. Exiting. %v", err)
		return pvc, err
	}
	return pvc, nil
}

// Create an Independant MySQL non PDS App running in a namespace
func CreateIndependentMySqlApp(ns string, podName string, appImage string, pvcName string) (*corev1.Pod, string, error) {
	namespace := ns
	podSpec := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  podName,
					Image: appImage,
					Env:   make([]corev1.EnvVar, 1),
				},
			},
			RestartPolicy: corev1.RestartPolicyOnFailure,
		},
	}
	volumename := "app-persistent-storage"
	var volumes = make([]corev1.Volume, 1)
	volumes[0] = corev1.Volume{Name: volumename, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName, ReadOnly: false}}}
	podSpec.Spec.Volumes = volumes
	env := []string{"MYSQL_ROOT_PASSWORD"}
	var value []string
	value = append(value, "password")
	for index := range env {
		podSpec.Spec.Containers[0].Env[index].Name = env[index]
		podSpec.Spec.Containers[0].Env[index].Value = value[index]
	}

	pod, err := k8sCore.CreatePod(podSpec)
	if err != nil {
		log.Errorf("An Error Occured while creating %v", err)
		return pod, "", err
	}
	return pod, podName, nil
}

// CreatePodWorkloads generate workloads as standalone pods
func CreatePodWorkloads(name string, image string, creds WorkloadGenerationParams, namespace string, count string, env []string) (*corev1.Pod, error) {
	var value []string
	podSpec := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: name + "-",
			Namespace:    namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  name,
					Image: image,
					Env:   make([]corev1.EnvVar, 4),
				},
			},
			RestartPolicy: corev1.RestartPolicyOnFailure,
		},
	}

	value = append(value, creds.Host)
	value = append(value, creds.User)
	value = append(value, creds.Password)
	value = append(value, count)

	for index := range env {
		podSpec.Spec.Containers[0].Env[index].Name = env[index]
		podSpec.Spec.Containers[0].Env[index].Value = value[index]
	}

	pod, err := k8sCore.CreatePod(podSpec)
	if err != nil {
		return nil, fmt.Errorf("failed to create pod [%s], Err: %v", podSpec.Name, err)
	}

	err = k8sCore.ValidatePod(pod, timeOut, timeInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to validate pod [%s], Err: %v", pod.Name, err)
	}

	//TODO: Remove static sleep and verify the injected data
	time.Sleep(1 * time.Minute)

	return pod, nil

}

// CreateDeploymentWorkloads generate workloads as deployment pods
func CreateDeploymentWorkloads(command, deploymentName, stressImage, namespace string) (*v1.Deployment, error) {

	var replicas int32 = 1
	deploymentSpec := &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: deploymentName + "-",
			Namespace:    namespace,
			Labels:       map[string]string{"app": deploymentName},
		},
		Spec: v1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": deploymentName},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": deploymentName},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    deploymentName,
							Image:   stressImage,
							Command: []string{"/bin/bash", "-c"},
							Args:    []string{command},
						},
					},
					RestartPolicy: "Always",
				},
			},
		},
	}
	deployment, err := k8sApps.CreateDeployment(deploymentSpec, metav1.CreateOptions{})
	if err != nil {
		log.Errorf("An Error Occured while creating deployment %v", err)
		return nil, err
	}

	err = k8sApps.ValidateDeployment(deployment, timeOut, timeInterval)
	if err != nil {
		log.Errorf("An Error Occured while validating the pod %v", err)
		return nil, err
	}

	//TODO: Remove static sleep and verify the injected data
	time.Sleep(1 * time.Minute)

	return deployment, nil
}

// CreatepostgresqlWorkload generate workloads on the pg db
func CreatepostgresqlWorkload(dnsEndpoint string, pdsPassword string, scalefactor string, iterations string, deploymentName string, namespace string) (*v1.Deployment, error) {
	var replicas int32 = 1
	deploymentSpec := &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: deploymentName + "-",
			Namespace:    namespace,
		},
		Spec: v1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": deploymentName},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": deploymentName},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "pgbench",
							Image:   postgresqlStressImage,
							Command: []string{"/pgloadgen.sh"},
							Args:    []string{dnsEndpoint, pdsPassword, scalefactor, iterations},
						},
					},
					RestartPolicy: corev1.RestartPolicyAlways,
				},
			},
		},
	}
	deployment, err := k8sApps.CreateDeployment(deploymentSpec, metav1.CreateOptions{})
	if err != nil {
		log.Errorf("An Error Occured while creating deployment %v", err)
		return nil, err
	}
	err = k8sApps.ValidateDeployment(deployment, timeOut, timeInterval)
	if err != nil {
		log.Errorf("An Error Occured while validating the pod %v", err)
		return nil, err
	}

	//TODO: Remove static sleep and verify the injected data
	time.Sleep(2 * time.Minute)

	return deployment, err
}

// CreateRedisWorkload func runs traffic on the Redis deployments
func CreateRedisWorkload(name string, image string, dnsEndpoint string, pdsPassword string, namespace string, env []string, command string) (*corev1.Pod, error) {
	var value []string
	podSpec := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: name + "-",
			Namespace:    namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    name,
					Image:   image,
					Command: []string{"/bin/sh", "-c", command},
					Env:     make([]corev1.EnvVar, 3),
				},
			},
			RestartPolicy: corev1.RestartPolicyOnFailure,
		},
	}

	value = append(value, dnsEndpoint)
	value = append(value, "pds")
	value = append(value, pdsPassword)

	for index := range env {
		podSpec.Spec.Containers[0].Env[index].Name = env[index]
		podSpec.Spec.Containers[0].Env[index].Value = value[index]
	}

	pod, err := k8sCore.CreatePod(podSpec)
	if err != nil {
		log.Errorf("An Error Occured while creating %v", err)
		return nil, err
	}

	err = k8sCore.ValidatePod(pod, timeOut, timeInterval)
	if err != nil {
		log.Errorf("An Error Occured while validating the pod %v", err)
		return nil, err
	}

	//TODO: Remove static sleep and verify the injected data
	time.Sleep(1 * time.Minute)

	return pod, nil
}

// CreateRmqWorkload generate workloads for rmq
func CreateRmqWorkload(dnsEndpoint string, pdsPassword string, namespace string, env []string, command string) (*corev1.Pod, error) {
	var value []string
	podSpec := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "rmq-perf-",
			Namespace:    namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    "rmqperf",
					Image:   rmqStressImage,
					Command: []string{"/bin/sh", "-c"},
					Args:    []string{command},
					Env:     make([]corev1.EnvVar, 3),
				},
			},
			RestartPolicy: corev1.RestartPolicyOnFailure,
		},
	}

	value = append(value, dnsEndpoint)
	value = append(value, "pds")
	value = append(value, pdsPassword)

	for index := range env {
		podSpec.Spec.Containers[0].Env[index].Name = env[index]
		podSpec.Spec.Containers[0].Env[index].Value = value[index]
	}

	pod, err := k8sCore.CreatePod(podSpec)
	if err != nil {
		log.Errorf("An Error Occured while creating %v", err)
		return nil, err
	}

	err = k8sCore.ValidatePod(pod, timeOut, timeInterval)
	if err != nil {
		log.Errorf("An Error Occured while validating the pod %v", err)
		return nil, err
	}

	//TODO: Remove static sleep and verify the injected data
	time.Sleep(1 * time.Minute)

	return pod, nil
}

// This function prepares a deployment for running TPCC Workload
func CreateTpccWorkloads(dataServiceName string, deploymentID string, scalefactor string, iterations string, deploymentName string, namespace string) (bool, error) {
	var dbUser, timeToRun, numOfCustomers, numOfThreads, numOfWarehouses string

	dnsEndpoint, err := GetDeploymentConnectionInfo(deploymentID)
	if err != nil {
		log.Errorf("An Error Occured while getting connection info %v", err)
		return false, err
	}
	log.Infof("Dataservice DNS endpoint %s", dnsEndpoint)
	pdsPassword, err := GetDeploymentCredentials(deploymentID)
	if err != nil {
		log.Errorf("An Error Occured while getting credentials info %v", err)
		return false, err
	}

	switch dataServiceName {
	// For a Postgres workload, simply create schema and run the TPCC Workload for default time
	case postgresql:
		dbName := "pds"
		wasTpccRunSuccessful := RunTpccWorkload(dbUser, pdsPassword, dnsEndpoint, dbName,
			timeToRun, numOfThreads, numOfCustomers, numOfWarehouses,
			deploymentName, namespace, dataServiceName)
		if !wasTpccRunSuccessful {
			return wasTpccRunSuccessful, errors.New("Tpcc run failed. This could be a bug - please check manually")
		} else {
			return wasTpccRunSuccessful, nil
		}
	// For MySQL workload, first setup the deployment to run TPCC, then wait for MySQL to be available,
	// Create TPCC Schema and then run it.
	case mysql:
		dbName := "tpcc"
		var wasMysqlConfigured bool
		// Waiting for approx an hour to check if Mysql deployment comes up
		for i := 1; i <= 80; i++ {
			wasMysqlConfigured := SetupMysqlDatabaseForTpcc(dbUser, pdsPassword, dnsEndpoint, namespace)
			if wasMysqlConfigured {
				log.InfoD("MySQL Deployment is successfully configured to run for TPCC Workload. Starting TPCC Workload Now.")
				break
			} else {
				log.InfoD("MySQL deployment is not yet configured for TPCC. It may still be starting up or there could be some error")
				log.InfoD("Waiting for 30 seconds to retry if MySQL deployment can be configured or not")
				time.Sleep(30 * time.Second)
			}
		}
		if !wasMysqlConfigured {
			log.Errorf("Something went wrong and DB Couldn't be prepared for TPCC workload. Exiting.")
			return wasMysqlConfigured, errors.New("MySQL DB Couldnt be prepared for TPCC as it wasnt reachable. This could be a bug, please check manually.")
		}
		wasTpccRunSuccessful := RunTpccWorkload(dbUser, "password", dnsEndpoint, dbName,
			timeToRun, numOfThreads, numOfCustomers, numOfWarehouses,
			deploymentName, namespace, dataServiceName)
		return wasTpccRunSuccessful, errors.New("TPCC Run failed. This could be a bug - please check manually")
	}
	return false, errors.New("TPCC run failed.")
}

// CreateDataServiceWorkloads generates workloads for the given dataservices
func CreateDataServiceWorkloads(params WorkloadGenerationParams) (*corev1.Pod, *v1.Deployment, error) {
	var dep *v1.Deployment
	var pod *corev1.Pod

	dnsEndpoint, err := GetDeploymentConnectionInfo(params.DeploymentID)
	if err != nil {
		return nil, nil, fmt.Errorf("error occured while getting connection info, Err: %v", err)
	}
	log.Infof("Dataservice DNS endpoint %s", dnsEndpoint)

	pdsPassword, err := GetDeploymentCredentials(params.DeploymentID)
	if err != nil {
		return nil, nil, fmt.Errorf("error occured while getting credentials info, Err: %v", err)
	}

	switch params.DataServiceName {
	case postgresql:
		dep, err = CreatepostgresqlWorkload(dnsEndpoint, pdsPassword, params.ScaleFactor, params.Iterations, params.DeploymentName, params.Namespace)
		if err != nil {
			return nil, nil, fmt.Errorf("error occured while creating postgresql workload, Err: %v", err)
		}

	case rabbitmq:
		env := []string{"AMQP_HOST", "PDS_USER", "PDS_PASS"}
		command := "while true; do java -jar perf-test.jar --uri amqp://${PDS_USER}:${PDS_PASS}@${AMQP_HOST} -jb -s 10240 -z 100 --variable-rate 100:30 --producers 10 --consumers 50; done"
		pod, err = CreateRmqWorkload(dnsEndpoint, pdsPassword, params.Namespace, env, command)
		if err != nil {
			return nil, nil, fmt.Errorf("error occured while creating rabbitmq workload, Err: %v", err)
		}

	case redis:
		env := []string{"REDIS_HOST", "PDS_USER", "PDS_PASS"}
		command := "redis-benchmark -a ${PDS_PASS} -h ${REDIS_HOST} -r 10000 -c 1000 -l -q --cluster --user ${PDS_USER}"
		pod, err = CreateRedisWorkload(params.DeploymentName, redisStressImage, dnsEndpoint, pdsPassword, params.Namespace, env, command)
		if err != nil {
			return nil, nil, fmt.Errorf("error occured while creating redis workload, Err: %v", err)
		}

	case cassandra:
		cassCommand := fmt.Sprintf("%s write no-warmup n=1000000 cl=ONE -mode user=pds password=%s native cql3 -col n=FIXED\\(5\\) size=FIXED\\(64\\)  -pop seq=1..1000000 -node %s -port native=9042 -rate auto -log file=/tmp/%s.load.data -schema \"replication(factor=3)\" -errors ignore; cat /tmp/%s.load.data", params.DeploymentName, pdsPassword, dnsEndpoint, params.DeploymentName, params.DeploymentName)
		dep, err = CreateDeploymentWorkloads(cassCommand, params.DeploymentName, cassandraStresImage, params.Namespace)
		if err != nil {
			return nil, nil, fmt.Errorf("error occured while creating cassandra workload, Err: %v", err)
		}
	case elasticSearch:
		esCommand := fmt.Sprintf("while true; do esrally race --track=geonames --target-hosts=%s --pipeline=benchmark-only --test-mode --kill-running-processes --client-options=\"timeout:%s,use_ssl:%s,verify_certs:%s,basic_auth_user:%s,basic_auth_password:'%s'\"; done", dnsEndpoint, params.TimeOut, params.UseSSL, params.VerifyCerts, params.User, pdsPassword)
		dep, err = CreateDeploymentWorkloads(esCommand, params.DeploymentName, esRallyImage, params.Namespace)
		if err != nil {
			return nil, nil, fmt.Errorf("error occured while creating elasticSearch workload, Err: %v", err)
		}

	case couchbase:
		env := []string{"HOST", "PDS_USER", "PASSWORD", "COUNT"}

		params.Host = dnsEndpoint
		params.User = "pds"
		params.Password = pdsPassword

		pod, err = CreatePodWorkloads(params.DeploymentName, cbloadImage, params, params.Namespace, "1000", env)
		if err != nil {
			return nil, nil, fmt.Errorf("error occured while creating couchbase workload, Err: %v", err)
		}

	case consul:
		dep, err = RunConsulBenchWorkload(params.DeploymentName, params.Namespace)
		if err != nil {
			return nil, nil, fmt.Errorf("error occured while creating Consul workload, Err: %v", err)
		}
	}
	return pod, dep, nil
}

func GetDataServiceID(ds string) string {
	var dataServiceID string
	dsModel, err := components.DataService.ListDataServices()
	if err != nil {
		log.Errorf("An Error Occured while listing dataservices %v", err)
		return ""
	}
	for _, v := range dsModel {
		if *v.Name == ds {
			dataServiceID = *v.Id
		}
	}
	return dataServiceID
}

// DeployDataServices deploys all dataservices, versions and images that are supported
func DeployDataServices(ds, projectID, deploymentTargetID, dnsZone, deploymentName, namespaceID, dataServiceDefaultAppConfigID string,
	replicas int32, serviceType, dataServiceDefaultResourceTemplateID, storageTemplateID, dsVersion,
	dsBuild, namespace string) (*pds.ModelsDeployment, map[string][]string, map[string][]string, error) {

	currentReplicas = replicas

	log.Infof("dataService: %v ", ds)
	id := GetDataServiceID(ds)
	if id == "" {
		log.Errorf("dataservice ID is empty")
		return nil, nil, nil, err
	}
	log.Infof(`Request params:
				projectID- %v deploymentTargetID - %v,
				dnsZone - %v,deploymentName - %v,namespaceID - %v
				App config ID - %v,
				num pods- %v, service-type - %v
				Resource template id - %v, storageTemplateID - %v`,
		projectID, deploymentTargetID, dnsZone, deploymentName, namespaceID, dataServiceDefaultAppConfigID,
		replicas, serviceType, dataServiceDefaultResourceTemplateID, storageTemplateID)

	if ds == zookeeper && replicas != 3 {
		log.Warnf("Zookeeper replicas cannot be %v, it should be 3", replicas)
		currentReplicas = 3
	}
	if ds == redis {
		log.Infof("Replicas passed %v", replicas)
		log.Warnf("Redis deployment replicas should be any one of the following values 1, 6, 8 and 10")
	}

	//clearing up the previous entries of dataServiceImageMap
	for version := range dataServiceImageMap {
		delete(dataServiceImageMap, version)
	}

	for version := range dataServiceVersionBuildMap {
		delete(dataServiceVersionBuildMap, version)
	}

	log.Infof("Getting versionID  for Data service version %s and buildID for %s ", dsVersion, dsBuild)
	versionID, imageID, dataServiceVersionBuildMap, err = GetVersionsImage(dsVersion, dsBuild, id)
	if err != nil {
		return nil, nil, nil, err
	}

	log.Infof("VersionID %v ImageID %v", versionID, imageID)
	components = pdsapi.NewComponents(apiClient)
	deployment, err = components.DataServiceDeployment.CreateDeployment(projectID,
		deploymentTargetID,
		dnsZone,
		deploymentName,
		namespaceID,
		dataServiceDefaultAppConfigID,
		imageID,
		currentReplicas,
		serviceType,
		dataServiceDefaultResourceTemplateID,
		storageTemplateID)

	if err != nil {
		log.Warnf("An Error Occured while creating deployment %v", err)
		return nil, nil, nil, err
	}
	err = ValidateDataServiceDeployment(deployment, namespace)
	if err != nil {
		return deployment, nil, nil, err
	}
	return deployment, dataServiceImageMap, dataServiceVersionBuildMap, nil
}

// DeployAllDataServices deploys all dataservices, versions and images that are supported
func DeployAllDataServices(supportedDataServicesMap map[string]string, projectID, deploymentTargetID, dnsZone, deploymentName, namespaceID string,
	dataServiceNameDefaultAppConfigMap map[string]string, replicas int32, serviceType string, dataServiceDefaultResourceTemplateIDMap map[string]string,
	storageTemplateID string, namespace string) (map[string][]*pds.ModelsDeployment, map[string][]string, map[string][]string, error) {

	currentReplicas = replicas

	for ds, id := range supportedDataServicesMap {
		log.Infof("dataService: %v ", ds)
		log.Infof(`Request params:
				projectID- %v deploymentTargetID - %v,
				dnsZone - %v,deploymentName - %v,namespaceID - %v
				App config ID - %v,
				num pods- %v, service-type - %v
				Resource template id - %v, storageTemplateID - %v`,
			projectID, deploymentTargetID, dnsZone, deploymentName, namespaceID, dataServiceNameDefaultAppConfigMap[ds],
			replicas, serviceType, dataServiceDefaultResourceTemplateIDMap[ds], storageTemplateID)

		if ds == zookeeper && replicas != 3 {
			log.Warnf("Zookeeper replicas cannot be %v, it should be 3", replicas)
			currentReplicas = 3
		}
		if ds == redis {
			log.Infof("Replicas passed %v", replicas)
			log.Warnf("Redis deployment replicas should be any one of the following values 1, 6, 8 and 10")
		}

		//clearing up the previous entries of dataServiceImageMap
		for image := range dataServiceImageMap {
			delete(dataServiceImageMap, image)
		}

		dataServiceVersionBuildMap, dataServiceImageMap, err = GetAllVersionsImages(id)
		if err != nil {
			return nil, nil, nil, err
		}

		for version := range dataServiceImageMap {
			for index := range dataServiceImageMap[version] {
				imageID := dataServiceImageMap[version][index]
				log.Infof("VersionID %v ImageID %v", version, imageID)
				components = pdsapi.NewComponents(apiClient)
				deployment, err = components.DataServiceDeployment.CreateDeployment(projectID,
					deploymentTargetID,
					dnsZone,
					deploymentName,
					namespaceID,
					dataServiceNameDefaultAppConfigMap[ds],
					imageID,
					currentReplicas,
					serviceType,
					dataServiceDefaultResourceTemplateIDMap[ds],
					storageTemplateID)

				if err != nil {
					log.Warnf("An Error Occured while creating deployment %v", err)
					return nil, nil, nil, err
				}
				err = ValidateDataServiceDeployment(deployment, namespace)
				if err != nil {
					return nil, nil, nil, err
				}
				deploymentsMap[ds] = append(deploymentsMap[ds], deployment)
			}
		}
	}
	return deploymentsMap, dataServiceImageMap, dataServiceVersionBuildMap, nil
}

// UpdateDataServiceVerison modifies the existing deployment version/image
func UpdateDataServiceVerison(dataServiceID, deploymentID string, appConfigID string, nodeCount int32, resourceTemplateID, dsImage, namespace, dsVersion string) (*pds.ModelsDeployment, error) {

	//Validate if the passed dsImage is available in the list of images
	var versions []pds.ModelsVersion
	var images []pds.ModelsImage
	var dsImageID string
	versions, err = components.Version.ListDataServiceVersions(dataServiceID)
	if err != nil {
		return nil, err
	}
	isBuildAvailable = false
	for i := 0; i < len(versions); i++ {
		if versions[i].GetName() == dsVersion {
			images, _ = components.Image.ListImages(versions[i].GetId())
			for j := 0; j < len(images); j++ {
				if images[j].GetBuild() == dsImage {
					dsImageID = images[j].GetId()
					isBuildAvailable = true
					break
				}
			}
		}
	}

	if !(isBuildAvailable) {
		log.Fatalf("Version/Build passed is not available")
	}

	deployment, err = components.DataServiceDeployment.UpdateDeployment(deploymentID, appConfigID, dsImageID, nodeCount, resourceTemplateID, nil)
	if err != nil {
		log.Errorf("An Error Occured while updating the deployment %v", err)
		return nil, err
	}

	err = ValidateDataServiceDeployment(deployment, namespace)
	if err != nil {
		return nil, err
	}

	return deployment, nil

}

// GetAllSupportedDataServices get the supported datasservices and returns the map
func GetAllSupportedDataServices() map[string]string {
	dataService, _ := components.DataService.ListDataServices()
	for _, ds := range dataService {
		if !*ds.ComingSoon {
			dataServiceNameIDMap[ds.GetName()] = ds.GetId()
		}
	}
	for key, value := range dataServiceNameIDMap {
		log.Infof("dsKey %v dsValue %v", key, value)
	}
	return dataServiceNameIDMap
}

// UpdateDataServices modifies the existing deployment
func UpdateDataServices(deploymentID string, appConfigID string, imageID string, nodeCount int32, resourceTemplateID, namespace string) (*pds.ModelsDeployment, error) {
	log.Infof("depID %v appConfID %v imageID %v nodeCount %v resourceTemplateID %v", deploymentID, appConfigID, imageID, nodeCount, resourceTemplateID)
	err = wait.Poll(maxtimeInterval, timeOut, func() (bool, error) {
		deployment, err = components.DataServiceDeployment.UpdateDeployment(deploymentID, appConfigID, imageID, nodeCount, resourceTemplateID, nil)
		if err != nil {
			return false, err
		}
		return true, nil
	})

	err = ValidateDataServiceDeployment(deployment, namespace)
	if err != nil {
		return nil, err
	}

	return deployment, nil
}

// ValidateDataServiceVolumes validates the volumes
func ValidateDataServiceVolumes(deployment *pds.ModelsDeployment, dataService string, dataServiceDefaultResourceTemplateID string, storageTemplateID string, namespace string) (ResourceSettingTemplate, StorageOptions, StorageClassConfig, error) {
	var config StorageClassConfig
	var resourceTemp ResourceSettingTemplate
	var storageOp StorageOptions
	ss, err := k8sApps.GetStatefulSet(deployment.GetClusterResourceName(), namespace)
	if err != nil {
		log.Warnf("An Error Occured while getting statefulsets %v", err)
	}
	err = k8sApps.ValidatePVCsForStatefulSet(ss, timeOut, timeInterval)
	if err != nil {
		log.Errorf("An error occured while validating pvcs of statefulsets %v ", err)
	}
	pvcList, err := k8sApps.GetPVCsForStatefulSet(ss)
	if err != nil {
		log.Warnf("An Error Occured while getting pvcs of statefulsets %v", err)
	}

	for _, pvc := range pvcList.Items {
		sc, err := k8sCore.GetStorageClassForPVC(&pvc)
		if err != nil {
			log.Errorf("Error Occured while getting storage class for pvc %v", err)
		}
		scAnnotation := sc.Annotations
		for k, v := range scAnnotation {
			if k == "kubectl.kubernetes.io/last-applied-configuration" {
				log.Infof("Storage Options Values %v", v)
				data := []byte(v)
				err := json.Unmarshal(data, &config)
				if err != nil {
					log.Errorf("Error Occured while getting volume params %v", err)
				}
			}
		}
	}

	rt, err := components.ResourceSettingsTemplate.GetTemplate(dataServiceDefaultResourceTemplateID)
	if err != nil {
		log.Errorf("Error Occured while getting resource setting template %v", err)
	}
	resourceTemp.Resources.Requests.CPU = *rt.CpuRequest
	resourceTemp.Resources.Requests.Memory = *rt.MemoryRequest
	resourceTemp.Resources.Requests.Storage = *rt.StorageRequest
	resourceTemp.Resources.Limits.CPU = *rt.CpuLimit
	resourceTemp.Resources.Limits.Memory = *rt.MemoryLimit

	st, err := components.StorageSettingsTemplate.GetTemplate(storageTemplateID)
	if err != nil {
		log.Errorf("Error Occured while getting storage template %v", err)
		return resourceTemp, storageOp, config, err
	}
	storageOp.Filesystem = st.GetFs()
	storageOp.Replicas = st.GetRepl()
	storageOp.VolumeGroup = st.GetFg()

	return resourceTemp, storageOp, config, nil
}

// ValidateDataServiceVolumes validates the volumes
func ValidateAllDataServiceVolumes(deployment *pds.ModelsDeployment, dataService string, dataServiceDefaultResourceTemplateID map[string]string, storageTemplateID string) (ResourceSettingTemplate, StorageOptions, StorageClassConfig, error) {
	var config StorageClassConfig
	var resourceTemp ResourceSettingTemplate
	var storageOp StorageOptions
	ss, err := k8sApps.GetStatefulSet(deployment.GetClusterResourceName(), GetAndExpectStringEnvVar("NAMESPACE"))
	if err != nil {
		log.Warnf("An Error Occured while getting statefulsets %v", err)
	}
	err = k8sApps.ValidatePVCsForStatefulSet(ss, timeOut, timeInterval)
	if err != nil {
		log.Errorf("An error occured while validating pvcs of statefulsets %v ", err)
	}
	pvcList, err := k8sApps.GetPVCsForStatefulSet(ss)
	if err != nil {
		log.Warnf("An Error Occured while getting pvcs of statefulsets %v", err)
	}

	for _, pvc := range pvcList.Items {
		sc, err := k8sCore.GetStorageClassForPVC(&pvc)
		if err != nil {
			log.Errorf("Error Occured while getting storage class for pvc %v", err)
		}
		scAnnotation := sc.Annotations
		for k, v := range scAnnotation {
			if k == "kubectl.kubernetes.io/last-applied-configuration" {
				log.Infof("Storage Options Values %v", v)
				data := []byte(v)
				err := json.Unmarshal(data, &config)
				if err != nil {
					log.Errorf("Error Occured while getting volume params %v", err)
				}
			}
		}
	}

	rt, err := components.ResourceSettingsTemplate.GetTemplate(dataServiceDefaultResourceTemplateIDMap[dataService])
	if err != nil {
		log.Errorf("Error Occured while getting resource setting template %v", err)
	}
	resourceTemp.Resources.Requests.CPU = *rt.CpuRequest
	resourceTemp.Resources.Requests.Memory = *rt.MemoryRequest
	resourceTemp.Resources.Requests.Storage = *rt.StorageRequest
	resourceTemp.Resources.Limits.CPU = *rt.CpuLimit
	resourceTemp.Resources.Limits.Memory = *rt.MemoryLimit

	st, err := components.StorageSettingsTemplate.GetTemplate(storageTemplateID)
	if err != nil {
		log.Errorf("Error Occured while getting storage template %v", err)
		return resourceTemp, storageOp, config, err
	}
	storageOp.Filesystem = st.GetFs()
	storageOp.Replicas = st.GetRepl()
	storageOp.VolumeGroup = st.GetFg()

	return resourceTemp, storageOp, config, nil

}

// DeleteK8sNamespace deletes the specified namespace
func DeleteK8sNamespace(namespace string) error {
	err := k8sCore.DeleteNamespace(namespace)
	if err != nil {
		log.Errorf("Could not delete the specified namespace %v because %v", namespace, err)
		return err
	}
	return nil
}

// ValidateDataServiceDeploymentNegative checks if deployment is not present
func ValidateDataServiceDeploymentNegative(deployment *pds.ModelsDeployment, namespace string) error {
	var ss *v1.StatefulSet
	err = wait.Poll(10*time.Second, 30*time.Second, func() (bool, error) {
		ss, err = k8sApps.GetStatefulSet(deployment.GetClusterResourceName(), namespace)
		if err != nil {
			log.Warnf("An Error Occured while getting statefulsets %v", err)
			return false, nil
		}
		return true, nil
	})
	if err == nil {
		log.Errorf("Validate DS Deployment negative failed, the StatefulSet still exists %v", ss)
		return fmt.Errorf("the deployment %v has not been deleted", deployment.Name)
	}
	return nil
}

func ValidateK8sNamespaceDeleted(namespace string) error {
	err = wait.Poll(maxtimeInterval, timeOut, func() (bool, error) {
		_, err := k8sCore.GetNamespace(namespace)
		if err == nil {
			log.Warnf("The namespace %v has not been deleted", namespace)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		log.Errorf("The namespace %v has not been deleted", namespace)
		return fmt.Errorf("the namespace %v has not been deleted", namespace)
	}
	log.Infof("The namespace has been successfully deleted")
	return nil

}

// TODO: Consolidate this function with CheckNamespace
func CreateK8sPDSNamespace(nname string) (*corev1.Namespace, error) {
	ns, err := k8sCore.CreateNamespace(&corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Namespace",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   nname,
			Labels: map[string]string{"pds.portworx.com/available": "true"},
		},
	})

	if err != nil {
		return nil, fmt.Errorf("could not create ns %v", nname)
	}

	return ns, nil

}

// DeleteK8sPDSNamespace deletes the pdsnamespace
func DeleteK8sPDSNamespace(nname string) error {
	err := k8sCore.DeleteNamespace(nname)
	return err
}

// GetPDSAgentPods returns the pds agent pod
func GetPDSAgentPods(pdsNamespace string) corev1.Pod {
	log.InfoD("Get agent pod from %v namespace", pdsNamespace)
	podList, err := GetPods(pdsNamespace)
	log.FailOnError(err, "Error while getting pods")
	for _, pod := range podList.Items {
		if strings.Contains(pod.Name, "pds-agent") {
			log.Infof("%v", pod.Name)
			pdsAgentpod = pod
			break
		}
	}
	return pdsAgentpod
}

func GetPodsFromK8sStatefulSet(deployment *pds.ModelsDeployment, namespace string) ([]corev1.Pod, error) {
	var ss *v1.StatefulSet
	err = wait.Poll(maxtimeInterval, timeOut, func() (bool, error) {
		ss, err = k8sApps.GetStatefulSet(deployment.GetClusterResourceName(), namespace)
		if err != nil {
			log.Warnf("An Error Occured while getting statefulsets %v", err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		log.Errorf("An Error Occured while getting statefulsets %v", err)
		return nil, err
	}
	pods, err := k8sApps.GetStatefulSetPods(ss)
	if err != nil {
		log.Errorf("An error occured while getting the pods belonging to this statefulset %v", err)
		return nil, err
	}
	return pods, nil
}

func GetK8sNodeObjectUsingPodName(nodeName string) (*corev1.Node, error) {
	nodeObject, err := k8sCore.GetNodeByName(nodeName)
	if err != nil {
		log.Errorf("Could not get the node object for node %v because %v", nodeName, err)
		return nil, err
	}
	return nodeObject, nil
}

func DrainPxPodOnK8sNode(node *corev1.Node, namespace string) error {
	labelSelector := map[string]string{"name": "portworx"}
	pod, err := k8sCore.GetPodsByNodeAndLabels(node.Name, namespace, labelSelector)
	if err != nil {
		log.Errorf("Could not fetch pods running on the given node %v", err)
		return err
	}
	log.Infof("Portworx pod to be drained %v from node %v", pod.Items[0].Name, node.Name)
	err = k8sCore.DrainPodsFromNode(node.Name, pod.Items, timeOut, maxtimeInterval)
	if err != nil {
		log.Errorf("Could not drain the node %v", err)
		return err
	}

	return nil
}

func LabelK8sNode(node *corev1.Node, label string) error {
	keyval := strings.Split(label, "=")
	err := k8sCore.AddLabelOnNode(node.Name, keyval[0], keyval[1])
	return err
}

func RemoveLabelFromK8sNode(node *corev1.Node, label string) error {
	err := k8sCore.RemoveLabelOnNode(node.Name, label)
	return err
}

func UnCordonK8sNode(node *corev1.Node) error {
	err = wait.Poll(maxtimeInterval, timeOut, func() (bool, error) {
		err = k8sCore.UnCordonNode(node.Name, timeOut, maxtimeInterval)
		if err != nil {
			log.Errorf("Failed uncordon node %v due to %v", node.Name, err)
			return false, nil
		}
		return true, nil
	})
	return err
}

func VerifyPxPodOnNode(nodeName string, namespace string) (bool, error) {
	labelSelector := map[string]string{"name": "portworx"}
	var pods *corev1.PodList
	err = wait.Poll(maxtimeInterval, timeOut, func() (bool, error) {
		pods, err = k8sCore.GetPodsByNodeAndLabels(nodeName, namespace, labelSelector)
		if err != nil {
			log.Errorf("Failed to get pods from node %v due to %v", nodeName, err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		log.Errorf("Could not fetch pods running on the given node %v", err)
		return false, err
	}
	pxPodName := pods.Items[0].Name
	log.Infof("The portworx pod %v from node %v", pxPodName, nodeName)
	return true, nil
}

// Returns nodes on which pods of a given statefulset are running
func GetNodesOfSS(SSName string, namespace string) ([]*corev1.Node, error) {
	var nodes []*corev1.Node
	// Get StatefulSet Object of the given Statefulset
	ss, err := k8sApps.GetStatefulSet(SSName, namespace)
	if err != nil {
		return nil, err
	}
	// Get The pods associated to this statefulset
	pods, err := k8sApps.GetStatefulSetPods(ss)
	if err != nil {
		return nil, err
	}
	// Create Node objects and append them to a list
	if pods != nil && len(pods) > 0 {
		for _, pod := range pods {
			if len(pod.Spec.NodeName) > 0 {
				node, err := k8sCore.GetNodeByName(pod.Spec.NodeName)
				if err != nil {
					return nil, err
				}
				nodes = append(nodes, node)
			}
		}
	}
	return nodes, nil
}

// Returns list of Pods from a given Statefulset running on a given Node
func GetPodsOfSsByNode(SSName string, nodeName string, namespace string) ([]corev1.Pod, error) {
	// Get StatefulSet Object of the given Statefulset
	ss, err := k8sApps.GetStatefulSet(SSName, namespace)
	if err != nil {
		return nil, err
	}
	// Get The pods associated to this statefulset
	pods, err := k8sApps.GetStatefulSetPods(ss)
	if err != nil {
		return nil, err
	}
	var podsList []corev1.Pod

	// Create Node objects and append them to a list
	if pods != nil && len(pods) > 0 {
		for _, pod := range pods {
			if pod.Spec.NodeName == nodeName {
				podsList = append(podsList, pod)
			}
		}
	}
	if podsList != nil && len(podsList) > 0 {
		return podsList, nil
	}
	return nil, errors.New(fmt.Sprintf("There is no pod of the given statefulset running on the given node name %s", nodeName))
}