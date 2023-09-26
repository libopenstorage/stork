package dataservice

import (
	"fmt"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/pkg/log"
)

type DsWithRBAC struct {
}

var dstype *DataserviceType

func (dsi *DsWithRBAC) TriggerDeployDSWithSiAndTls(ds PDSDataService, namespace,
	projectID string, tlsEnable bool, resourceTemplateID string, appConfigID string, nsId string, dsVersion string, dsImage string, dsID string, testParams TestParams) (*pds.ModelsDeployment, map[string][]string, map[string][]string, error) {
	log.InfoD("Going to start %v app deployment", ds.Name)

	log.InfoD("Deploying DataService %v ", ds.Name)
	deployment, dataServiceImageMap, dataServiceVersionBuildMap, err = dsi.DeployDsWithRBAC(ds.Name, projectID,
		testParams.DeploymentTargetId,
		testParams.DnsZone,
		deploymentName,
		nsId,
		appConfigID,
		int32(ds.Replicas),
		testParams.ServiceType,
		resourceTemplateID,
		testParams.StorageTemplateId,
		dsVersion,
		dsImage,
		namespace,
		dsID,
		tlsEnable,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error while deploying data services %v", err)
	}

	return deployment, dataServiceImageMap, dataServiceVersionBuildMap, err
}

func (dsi *DsWithRBAC) GetDataServiceDeploymentTemplateIDS(tenantID string, ds PDSDataService) (string, string, error) {
	log.InfoD("Getting Resource Template ID")
	resourceTemplateID, err := controlplane.GetResourceTemplate(tenantID, ds.Name)
	if err != nil {
		return "", "", fmt.Errorf("error while getting resource template ID %v", err)
	}
	log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)
	log.InfoD("Getting App Template ID")
	appConfigID, err := controlplane.GetAppConfTemplate(tenantID, ds.Name)
	if err != nil {
		return "", "", fmt.Errorf("error while getting app config ID %v", err)
	}
	return resourceTemplateID, appConfigID, nil
}

func (dsi *DsWithRBAC) GetDSImageVersionToBeDeployed(deployOldVersion bool, ds PDSDataService, dsId string) (string, string, error) {
	var dsVersion string
	var dsImage string
	pdsParams := GetAndExpectStringEnvVar("PDS_PARAM_CM")
	params, err := customparams.ReadParams(pdsParams)
	if err != nil {
		return "", "", fmt.Errorf("failed to read pds params %v", err)
	}

	if params.ForceImageID || deployOldVersion {
		forceImageID = true
	}

	if deployOldVersion {
		dsVersion = ds.OldVersion
		dsImage = ds.OldImage
		log.Debugf("Deploying old version %s and image %s", dsVersion, dsImage)
	} else {
		dsVersion = ds.Version
		dsImage = ds.Image
	}
	if params.ForceImageID {
		log.Infof("Getting versionID  for Data service version %s and buildID for %s ", dsVersion, dsImage)
		versionID, imageID, dataServiceVersionBuildMap, err = GetVersionsImage(dsVersion, dsImage, dsId)
	} else {
		log.Infof("Getting Latest versionID and ImageID for Data service version %s ", dsVersion)
		versionID, imageID, dataServiceVersionBuildMap, err = GetLatestVersionsImage(dsVersion, dsId)
	}

	if err != nil {
		return "", "", nil
	}
	return versionID, imageID, nil
}

func (dsi *DsWithRBAC) DeployDsWithRBAC(ds, projectID, deploymentTargetID, dnsZone, deploymentName, namespaceID, dataServiceDefaultAppConfigID string,
	replicas int32, serviceType, dataServiceDefaultResourceTemplateID, storageTemplateID, dsVersion string,
	dsImageID string, namespace string, dsID string, tlsEnable bool) (*pds.ModelsDeployment, map[string][]string, map[string][]string, error) {

	currentReplicas = replicas

	log.Infof("dataService: %v ", ds)
	if dsID == "" {
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
	log.Infof("VersionID %v ImageID %v", dsVersion, dsImageID)
	deployment, err = components.DataServiceDeployment.CreateDeploymentWithRbac(
		deploymentTargetID,
		dnsZone,
		deploymentName,
		namespaceID,
		dataServiceDefaultAppConfigID,
		dsImageID,
		currentReplicas,
		serviceType,
		dataServiceDefaultResourceTemplateID,
		storageTemplateID,
		tlsEnable)

	if err != nil {
		return nil, nil, nil, fmt.Errorf("An Error Occured while creating deployment %v", err)
	}

	return deployment, dataServiceImageMap, dataServiceVersionBuildMap, nil
}

func (dsi *DsWithRBAC) ValidateDataServiceDeploymentWithRbac() {

}
