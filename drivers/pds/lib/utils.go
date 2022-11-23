package lib

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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
	timeOut               = 5 * time.Minute
	timeInterval          = 10 * time.Second
	maxtimeInterval       = 30 * time.Second
	envDsVersion          = "DS_VERSION"
	envDsBuild            = "DS_BUILD"
	zookeeper             = "ZooKeeper"
	redis                 = "Redis"
	cassandraStresImage   = "scylladb/scylla:4.1.11"
	postgresqlStressImage = "portworx/torpedo-pgbench:pdsloadTest"
	redisStressImage      = "redis:latest"
	rmqStressImage        = "pivotalrabbitmq/perf-test:latest"
	postgresql            = "PostgreSQL"
	cassandra             = "Cassandra"
	rabbitmq              = "RabbitMQ"
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
	err                                   error
	isavailable                           bool
	isTemplateavailable                   bool
	isVersionAvailable                    bool
	isBuildAvailable                      bool
	currentReplicas                       int32
	deploymentTargetID, storageTemplateID string
	accountID                             string
	tenantID                              string
	projectID                             string
	resourceTemplateID                    string
	appConfigTemplateID                   string
	versionID                             string
	imageID                               string
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

// SetupPDSTest returns few params required to run the test
func SetupPDSTest(ControlPlaneURL, ClusterType, AccountName string) (string, string, string, string, string, error) {
	var err error
	apiConf := pds.NewConfiguration()
	endpointURL, err := url.Parse(ControlPlaneURL)
	if err != nil {
		log.Errorf("An Error Occured while parsing the URL %v", err)
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
	log.Infof("Deployment service type %s", serviceType)

	acc := components.Account
	accounts, err := acc.GetAccountsList()
	if err != nil {
		log.Errorf("An Error Occured while getting account list %v", err)
		return "", "", "", "", "", err
	}

	for i := 0; i < len(accounts); i++ {
		log.Infof("Account Name: %v", accounts[i].GetName())
		if accounts[i].GetName() == AccountName {
			accountID = accounts[i].GetId()
		}
	}
	log.Infof("Account Detail- Name: %s, UUID: %s ", AccountName, accountID)
	tnts := components.Tenant
	tenants, _ := tnts.GetTenantsList(accountID)
	tenantID = tenants[0].GetId()
	tenantName := tenants[0].GetName()
	log.Infof("Tenant Details- Name: %s, UUID: %s ", tenantName, tenantID)
	dnsZone, err := controlplane.GetDNSZone(tenantID)
	if err != nil {
		log.Errorf("Error while getting DNS Zone %v ", err)
		return "", "", "", "", "", err
	}
	log.Infof("DNSZone info - Name: %s, tenant: %s , account: %s", dnsZone, tenantName, AccountName)
	projcts := components.Project
	projects, _ := projcts.GetprojectsList(tenantID)
	projectID = projects[0].GetId()
	projectName := projects[0].GetName()
	log.Infof("Project Details- Name: %s, UUID: %s ", projectName, projectID)

	ns, err = k8sCore.GetNamespace("kube-system")
	if err != nil {
		log.Errorf("Error while getting k8s namespace %v", err)
		return "", "", "", "", "", err
	}
	clusterID := string(ns.GetObjectMeta().GetUID())
	if len(clusterID) > 0 {
		log.Infof("clusterID %v", clusterID)
	} else {
		log.Errorf("Cluster ID is empty")
		return "", "", "", "", "", err
	}

	log.Info("Get the Target cluster details")
	targetClusters, err := components.DeploymentTarget.ListDeploymentTargetsBelongsToTenant(tenantID)
	if err != nil {
		log.Errorf("Error while listing deployments %v", err)
		return "", "", "", "", "", err
	}
	if targetClusters == nil {
		log.Fatalf("Target cluster passed is not available to the account/tenant %v", err)
	}
	for i := 0; i < len(targetClusters); i++ {
		if targetClusters[i].GetClusterId() == clusterID {
			deploymentTargetID = targetClusters[i].GetId()
			log.Infof("Deployment Target ID %v", deploymentTargetID)
			log.Infof("Cluster ID: %v, Name: %v,Status: %v", targetClusters[i].GetClusterId(), targetClusters[i].GetName(), targetClusters[i].GetStatus())
		}
	}
	return tenantID, dnsZone, projectID, serviceType, deploymentTargetID, err
}

// CheckNamespace checks if the namespace is available in the cluster and pds is enabled on it
func CheckNamespace(namespace string) (bool, error) {
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
			log.Infof("Creating namespace %v", namespace)
			ns, err = k8sCore.CreateNamespace(nsName)
			if err != nil {
				log.Errorf("Error while creating namespace %v", err)
				return false, err
			}
			isavailable = true
		}
		if !isavailable {
			return false, err
		}
	}
	isavailable = false
	for key, value := range ns.Labels {
		log.Infof("key: %v values: %v", key, value)
		if key == pxLabel && value == "true" {
			isavailable = true
			break
		}
	}
	if !isavailable {
		return false, nil
	}
	return true, nil
}

// ReadParams reads the params from given or default json
func ReadParams(filename string) (*Parameter, error) {
	var jsonPara Parameter

	if filename == "" {
		filename, err = filepath.Abs(defaultParams)
		log.Infof("filename %v", filename)
		if err != nil {
			log.Errorf("FilePath err %v", err)
			return nil, err
		}
		log.Infof("Parameter json file is not used, use initial parameters value.")
		log.Infof("Reading params from %v ", filename)
		file, err := ioutil.ReadFile(filename)
		if err != nil {

			log.Errorf("File error: %v\n", err)
			return nil, err
		}
		err = json.Unmarshal(file, &jsonPara)
		if err != nil {
			log.Errorf("Error while unmarshalling json: %v\n", err)
			return nil, err
		}
	} else {
		cm, err := core.Instance().GetConfigMap(pdsParamsConfigmap, configmapNamespace)
		if err != nil {
			log.Errorf("Error reading config map: %v", err)
		}
		if len(cm.Data) > 0 {
			configmap := &cm.Data
			for key, data := range *configmap {
				log.Infof("key %v \n value %v", key, data)
				json_data := []byte(data)
				err = json.Unmarshal(json_data, &jsonPara)
				if err != nil {
					log.Errorf("Error while unmarshalling json: %v\n", err)
					return nil, err
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

// DeleteDeploymentPods deletes the given pods
func DeleteDeploymentPods(podList *corev1.PodList) error {
	var pods, newPods []corev1.Pod
	k8sOps := k8sCore

	pods = append(pods, podList.Items...)
	err := k8sOps.DeletePods(pods, true)
	if err != nil {
		return err
	}

	//get the newly created pods
	time.Sleep(10 * time.Second)
	newPodList, err := GetPods("pds-system")
	if err != nil {
		return err
	}
	//reinitializing the pods
	newPods = append(newPods, newPodList.Items...)

	//validate deployment pods are up and running after deletion
	for _, pod := range newPods {
		log.Infof("pds system pod name %v", pod.Name)
		err = k8sOps.ValidatePod(&pod, timeOut, timeInterval)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetStorageTemplate return the storage template id
func GetStorageTemplate(tenantID string) (string, error) {
	log.Infof("Get the storage template")
	storageTemplates, err := components.StorageSettingsTemplate.ListTemplates(tenantID)
	if err != nil {
		log.Errorf("Error while listing storage template %v", err)
		return "", err
	}
	for i := 0; i < len(storageTemplates); i++ {
		if storageTemplates[i].GetName() == storageTemplateName {
			log.Infof("Storage template details -----> Name %v,Repl %v , Fg %v , Fs %v",
				storageTemplates[i].GetName(),
				storageTemplates[i].GetRepl(),
				storageTemplates[i].GetFg(),
				storageTemplates[i].GetFs())
			storageTemplateID = storageTemplates[i].GetId()
			log.Infof("Storage Id: %v", storageTemplateID)
		}
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
					log.Infof("Namespace Status - Name: %v , Id: %v , Status: %v", namespaces[i].GetName(), namespaces[i].GetId(), namespaces[i].GetStatus())
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
		log.Errorf("Version/Build passed is not available")
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

// ValidateDataServiceDeployment checks if deployment is healthy and running
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
	//validate the statefulset deployed in the namespace
	err = k8sApps.ValidateStatefulSet(ss, defaultRetryInterval)
	if err != nil {
		log.Errorf("An Error Occured while validating statefulsets %v", err)
		return err
	}

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

// CreatecassandraWorkload generate workloads on the cassandra db
func CreatecassandraWorkload(cassCommand string, deploymentName string, namespace string) (*v1.Deployment, error) {

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
							Image:   "scylladb/scylla:4.1.11",
							Command: []string{"/bin/sh", "-c", cassCommand},
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

// CreateDataServiceWorkloads func
func CreateDataServiceWorkloads(dataServiceName string, deploymentID string, scalefactor string, iterations string, deploymentName string, namespace string) (*corev1.Pod, *v1.Deployment, error) {
	var dep *v1.Deployment
	var pod *corev1.Pod

	dnsEndpoint, err := GetDeploymentConnectionInfo(deploymentID)
	if err != nil {
		log.Errorf("An Error Occured while getting connection info %v", err)
		return nil, nil, err
	}
	log.Infof("Dataservice DNS endpoint %s", dnsEndpoint)

	pdsPassword, err := GetDeploymentCredentials(deploymentID)
	if err != nil {
		log.Errorf("An Error Occured while getting credentials info %v", err)
		return nil, nil, err
	}

	switch dataServiceName {
	case postgresql:
		dep, err = CreatepostgresqlWorkload(dnsEndpoint, pdsPassword, scalefactor, iterations, deploymentName, namespace)
		if err != nil {
			log.Errorf("An Error Occured while creating postgresql workload %v", err)
			return nil, nil, err
		}

	case rabbitmq:
		env := []string{"AMQP_HOST", "PDS_USER", "PDS_PASS"}
		command := "while true; do java -jar perf-test.jar --uri amqp://${PDS_USER}:${PDS_PASS}@${AMQP_HOST} -jb -s 10240 -z 100 --variable-rate 100:30 --producers 10 --consumers 50; done"
		pod, err = CreateRmqWorkload(dnsEndpoint, pdsPassword, namespace, env, command)
		if err != nil {
			log.Errorf("An Error Occured while creating rabbitmq workload %v", err)
			return nil, nil, err
		}

	case redis:
		env := []string{"REDIS_HOST", "PDS_USER", "PDS_PASS"}
		command := "redis-benchmark -a ${PDS_PASS} -h ${REDIS_HOST} -r 10000 -c 1000 -l -q --cluster"
		pod, err = CreateRedisWorkload(deploymentName, redisStressImage, dnsEndpoint, pdsPassword, namespace, env, command)
		if err != nil {
			log.Errorf("An Error Occured while creating redis workload %v", err)
			return nil, nil, err
		}

	case cassandra:
		cassCommand := deploymentName + " write no-warmup n=1000000 cl=ONE -mode user=pds password=" + pdsPassword + " native cql3 -col n=FIXED\\(5\\) size=FIXED\\(64\\)  -pop seq=1..1000000 -node " + dnsEndpoint + " -port native=9042 -rate auto -log file=/tmp/" + deploymentName + ".load.data -schema \"replication(factor=3)\" -errors ignore; cat /tmp/" + deploymentName + ".load.data"
		dep, err = CreatecassandraWorkload(cassCommand, deploymentName, namespace)
		if err != nil {
			log.Errorf("An Error Occured while creating cassandra workload %v", err)
			return nil, nil, err
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

	//for ds, id := range supportedDataServicesMap {
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
		return nil, nil, nil, err
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
	deployment, err = components.DataServiceDeployment.UpdateDeployment(deploymentID, appConfigID, imageID, nodeCount, resourceTemplateID, nil)
	if err != nil {
		log.Errorf("An Error Occured while updating deployment %v", err)
		return nil, err
	}

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

func DeleteK8sPDSNamespace(nname string) error {
	err := k8sCore.DeleteNamespace(nname)
	return err
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
