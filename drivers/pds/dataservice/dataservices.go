package dataservice

import (
	"fmt"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	pdsdriver "github.com/portworx/torpedo/drivers/pds"
	pdsapi "github.com/portworx/torpedo/drivers/pds/api"
	pdscontrolplane "github.com/portworx/torpedo/drivers/pds/controlplane"
	"github.com/portworx/torpedo/drivers/pds/parameters"
	"github.com/portworx/torpedo/drivers/pds/targetcluster"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/pkg/log"
	v1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	state "net/http"
	"os"
	"time"
)

// PDS vars
var (
	components    *pdsapi.Components
	deployment    *pds.ModelsDeployment
	targetCluster *targetcluster.TargetCluster

	k8sCore = core.Instance()
	k8sApps = apps.Instance()

	err                                  error
	isVersionAvailable                   bool
	isBuildAvailable                     bool
	currentReplicas                      int32
	deploymentTargetID                   string
	storageTemplateID                    string
	versionID                            string
	imageID                              string
	dataServiceDefaultResourceTemplateID string
	dataServiceDefaultAppConfigID        string

	dataServiceVersionBuildMap = make(map[string][]string)
	dataServiceImageMap        = make(map[string][]string)
)

// PDS const
const (
	zookeeper       = "ZooKeeper"
	redis           = "Redis"
	deploymentName  = "qa"
	driverName      = "pds"
	maxtimeInterval = 30 * time.Second
	timeOut         = 30 * time.Minute
)

// PDS packages
var (
	customparams *parameters.Customparams
	controlplane *pdscontrolplane.ControlPlane
)

type DataserviceType struct{}

// TestParams has the prereqs for deploying pds dataservices
type TestParams struct {
	DeploymentTargetId string
	DnsZone            string
	StorageTemplateId  string
	NamespaceId        string
	TenantId           string
	ProjectId          string
	ServiceType        string
}

type PDSDataService struct {
	Name          string "json:\"Name\""
	Version       string "json:\"Version\""
	Image         string "json:\"Image\""
	Replicas      int    "json:\"Replicas\""
	ScaleReplicas int    "json:\"ScaleReplicas\""
	OldVersion    string "json:\"OldVersion\""
	OldImage      string "json:\"OldImage\""
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
		log.Debugf("version name %s and is enabled=%t", *versions[i].Name, *versions[i].Enabled)
		if *versions[i].Name == dsVersion {
			log.Debugf("DS Version %s is enabled in the control plane", dsVersion)
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

func (d *DataserviceType) GetDataServiceID(ds string) (string, error) {
	var dataServiceID string
	dsModel, err := components.DataService.ListDataServices()
	if err != nil {
		return "", fmt.Errorf("An Error Occured while listing dataservices %v", err)
	}
	for _, v := range dsModel {
		if *v.Name == ds {
			dataServiceID = *v.Id
		}
	}
	return dataServiceID, nil
}

// DeployDS deploys dataservices its internally used function
func (d *DataserviceType) DeployDS(ds, projectID, deploymentTargetID, dnsZone, deploymentName, namespaceID, dataServiceDefaultAppConfigID string,
	replicas int32, serviceType, dataServiceDefaultResourceTemplateID, storageTemplateID, dsVersion,
	dsBuild, namespace string) (*pds.ModelsDeployment, map[string][]string, map[string][]string, error) {

	currentReplicas = replicas

	log.Infof("dataService: %v ", ds)
	id, err := d.GetDataServiceID(ds)
	if id == "" {
		return nil, nil, nil, fmt.Errorf("dataservice ID is empty %v", err)
	}
	log.Infof(`Request params:
				projectID- %v deploymentTargetID - %v,
				dnsZone - %v,deploymentName - %v,namespaceID - %v
				App config ID - %v,
				num pods- %v, service-type - %v
				Resource template id - %v, storageTemplateID - %v`,
		projectID, deploymentTargetID, dnsZone, deploymentName, namespaceID, dataServiceDefaultAppConfigID,
		replicas, serviceType, dataServiceDefaultResourceTemplateID, storageTemplateID)

	//TODO: Fail the tests with replicas are not passed as expected(PA-911)
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
		return nil, nil, nil, fmt.Errorf("An Error Occured while creating deployment %v", err)
	}

	return deployment, dataServiceImageMap, dataServiceVersionBuildMap, nil
}

func (d *DataserviceType) TriggerDeployDataService(ds PDSDataService, namespace, tenantID,
	projectID string, deployOldVersion bool, testParams TestParams) (*pds.ModelsDeployment, map[string][]string, map[string][]string, error) {
	log.InfoD("Going to start %v app deployment", ds.Name)
	var dsVersion string
	var dsImage string

	if deployOldVersion {
		dsVersion = ds.OldVersion
		dsImage = ds.OldImage
		log.Debugf("Deploying old version %s and image %s", dsVersion, dsImage)
	} else {
		dsVersion = ds.Version
		dsImage = ds.Image
		log.Debugf("Deploying latest version %s and image %s", dsVersion, dsImage)
	}

	log.InfoD("Getting Resource Template ID")
	dataServiceDefaultResourceTemplateID, err = controlplane.GetResourceTemplate(tenantID, ds.Name)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error while getting resource template ID %v", err)
	}
	log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)
	log.InfoD("Getting App Template ID")

	dataServiceDefaultAppConfigID, err = controlplane.GetAppConfTemplate(tenantID, ds.Name)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error while getting app configuration template %v", err)
	}
	log.InfoD("dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)

	namespaceID, err := targetCluster.GetnameSpaceID(namespace, testParams.DeploymentTargetId)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error while getting namespace id %v", err)
	}

	log.InfoD("Deploying DataService %v ", ds.Name)
	deployment, dataServiceImageMap, dataServiceVersionBuildMap, err = d.DeployDS(ds.Name, projectID,
		testParams.DeploymentTargetId,
		testParams.DnsZone,
		deploymentName,
		namespaceID,
		dataServiceDefaultAppConfigID,
		int32(ds.Replicas),
		testParams.ServiceType,
		dataServiceDefaultResourceTemplateID,
		testParams.StorageTemplateId,
		dsVersion,
		dsImage,
		namespace,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error while deploying data services %v", err)
	}

	return deployment, dataServiceImageMap, dataServiceVersionBuildMap, err
}

// GetAndExpectStringEnvVar parses a string from env variable.
func GetAndExpectStringEnvVar(varName string) string {
	varValue := os.Getenv(varName)
	return varValue
}

// DeployPDSDataservices method will be used to deploy ds and run common px tests
func (d *DataserviceType) DeployPDSDataservices() ([]*pds.ModelsDeployment, error) {
	log.InfoD("Deployment of pds apps called from schedule applications")
	var deployments = make(map[PDSDataService]*pds.ModelsDeployment)
	var pdsApps []*pds.ModelsDeployment
	var testparams TestParams

	pdsParams := GetAndExpectStringEnvVar("PDS_PARAM_CM")
	params, err := customparams.ReadParams(pdsParams)
	if err != nil {
		return nil, fmt.Errorf("failed to read pds params %v", err)
	}
	infraParams := params.InfraToTest
	namespace := params.InfraToTest.Namespace

	_, isAvailable, err := targetCluster.CreatePDSNamespace(namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to create pds namespace %v", err)
	}
	if !isAvailable {
		return nil, fmt.Errorf("pdsnamespace %v is not available to deploy apps", namespace)
	}
	_, tenantID, dnsZone, projectID, serviceType, clusterID, err := controlplane.SetupPDSTest(infraParams.ControlPlaneURL, infraParams.ClusterType,
		infraParams.AccountName, infraParams.TenantName, infraParams.ProjectName)
	testparams.ServiceType = serviceType
	if err != nil {
		return nil, fmt.Errorf("Failed on SetupPDSTest method %v", err)
	}
	testparams.DnsZone = dnsZone

	err = targetCluster.RegisterClusterToControlPlane(params, tenantID, false)
	log.FailOnError(err, "Target Cluster Registeration failed")

	deploymentTargetID, err = targetCluster.GetDeploymentTargetID(clusterID, tenantID)
	if err != nil {
		return nil, fmt.Errorf("error while getting deployment Target ID %v", err)
	}
	log.InfoD("DeploymentTargetID %s ", deploymentTargetID)
	testparams.DeploymentTargetId = deploymentTargetID

	namespaceId, err := targetCluster.GetnameSpaceID(namespace, deploymentTargetID)
	if err != nil {
		return nil, fmt.Errorf("Failed to get the namespace Id %v", err)
	}
	log.InfoD("NamespaceId %s ", namespaceId)
	testparams.NamespaceId = namespaceId

	storageTemplateID, err = controlplane.GetStorageTemplate(tenantID)
	if err != nil {
		return nil, fmt.Errorf("error while getting storage template ID %v", err)
	}
	log.InfoD("storageTemplateID %v", storageTemplateID)
	testparams.StorageTemplateId = storageTemplateID

	for _, ds := range params.DataServiceToTest {
		deployment, _, _, err := d.TriggerDeployDataService(ds, namespace, tenantID, projectID, false, testparams)
		if err != nil {
			return nil, fmt.Errorf("failed to deploy pds apps %v", err)
		}

		err = d.ValidateDataServiceDeployment(deployment, namespace)
		log.FailOnError(err, fmt.Sprintf("Error while validating dataservice deployment %v", *deployment.ClusterResourceName))

		deployments[ds] = deployment
		pdsApps = append(pdsApps, deployment)
	}

	return pdsApps, nil
}

func (d *DataserviceType) ValidateDataServiceDeployment(deployment *pds.ModelsDeployment, namespace string) error {
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

func (d *DataserviceType) CreateSchedulerContextForPDSApps(pdsApps []*pds.ModelsDeployment) []*scheduler.Context {
	var specObjects []interface{}
	var Contexts []*scheduler.Context
	var ctx *scheduler.Context

	for _, dep := range pdsApps {
		specObjects = append(specObjects, dep)
		ctx = &scheduler.Context{
			UID: dep.GetId(),
			App: &spec.AppSpec{
				Key:      *dep.ClusterResourceName,
				SpecList: specObjects,
			},
		}
		Contexts = append(Contexts, ctx)
	}
	return Contexts
}

func init() {
	err = pdsdriver.Register(driverName, &DataserviceType{})
	if err != nil {
		log.Errorf("Error while Registering pds dataservice type driver: %v", err)
	}
	log.Infof("pds data services drivers got registerd...")
}

func DataserviceInit(ControlPlaneURL string) (*DataserviceType, error) {
	components, controlplane, err = pdsdriver.InitPdsApiComponents(ControlPlaneURL)
	if err != nil {
		return nil, err
	}

	return &DataserviceType{}, nil
}
