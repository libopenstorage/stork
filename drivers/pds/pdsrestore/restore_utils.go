package pdsrestore

import (
	"fmt"
	"github.com/onsi/ginkgo/v2"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdsapi "github.com/portworx/torpedo/drivers/pds/api"
	ds "github.com/portworx/torpedo/drivers/pds/dataservice"
	tc "github.com/portworx/torpedo/drivers/pds/targetcluster"
	"github.com/portworx/torpedo/pkg/log"
	"k8s.io/apimachinery/pkg/util/wait"
	"reflect"
	"strings"
	"time"
)

const (
	restoreTimeInterval    = 20 * time.Second
	restoreMaxTimeInterval = 1 * time.Minute
)

var restoreTimeOut = 30 * time.Minute

type RestoreClient struct {
	TenantId             string
	ProjectId            string
	Components           *pdsapi.Components
	Deployment           *pds.ModelsDeployment
	RestoreTargetCluster *tc.TargetCluster
}

// DSEntity struct contain the current context of data and deployment object. Which could be later utilized as source of truth for validating the restored object
type DSEntity struct {
	Deployment *pds.ModelsDeployment
	// TODO Add datahash for data validation
}

// TriggerAndValidateRestore will trigger restore and validate if flag is true
func (restoreClient *RestoreClient) TriggerAndValidateRestore(backupJobId string, namespace string, bkpDsEntity DSEntity, isRestoreInSameNS, validate bool) (*pds.ModelsRestore, error) {
	var (
		bkpJob                 *pds.ModelsBackupJob
		nsName, pdsNamespaceId string
	)
	k8sClusterId, err := restoreClient.RestoreTargetCluster.GetClusterID()
	if err != nil {
		log.Errorf("Unable to fetch the cluster Id")
		return nil, fmt.Errorf("unable to fetch the cluster Id")
	}
	pdsRestoreTargetClusterId, err := restoreClient.RestoreTargetCluster.GetDeploymentTargetID(k8sClusterId, restoreClient.TenantId)
	if err != nil {
		log.Errorf("Unable to fetch the cluster details from the control plane")
		return nil, fmt.Errorf("unable to fetch the cluster details from the control plane")
	}
	if !isRestoreInSameNS {
		nsName, pdsNamespaceId, err = restoreClient.getNameSpaceId(pdsRestoreTargetClusterId)
		if err != nil {
			return nil, err
		}
	} else {
		bkpJob, err = restoreClient.Components.BackupJob.GetBackupJob(backupJobId)
		if err != nil {
			return nil, err
		}
		nsName, pdsNamespaceId = namespace, bkpJob.GetNamespaceId()
	}
	log.Infof("backup Job - %v,Restore Target Cluster Id - %v, NamespaceId - %v", backupJobId, pdsRestoreTargetClusterId, pdsNamespaceId)
	restoredModel, err := restoreClient.Components.Restore.RestoreToNewDeployment(backupJobId, "autom-res", pdsRestoreTargetClusterId, pdsNamespaceId)
	if err != nil {
		log.Errorf("Failed during restore.")
		return nil, fmt.Errorf("failed during restore")
	}

	if validate {
		err = restoreClient.WaitForRestoreAndValidate(restoredModel, bkpDsEntity, nsName)
		if err != nil {
			log.Errorf("Failed to validate restored dataservice")
			return nil, fmt.Errorf("failed to validate restored dataservice")
		}
	}
	return restoredModel, nil
}

// WaitForRestoreAndValidate will wait for the restore to complete and validate its configuration
func (restoreClient *RestoreClient) WaitForRestoreAndValidate(restoredModel *pds.ModelsRestore, bkpDsEntity DSEntity, nsName string) error {

	currentSpecReport := ginkgo.CurrentSpecReport()
	testName := strings.Split(currentSpecReport.FullText(), " ")[0]
	log.Debugf("Testcase Name %v", testName)

	if testName == "{ValidateDSHealthStatusOnNodeFailures}" {
		log.Debugf("Updating the restoreTimeout to 2min")
		restoreTimeOut = 2 * time.Minute
	}

	err := wait.Poll(restoreTimeInterval, restoreTimeOut, func() (bool, error) {
		restore, err := restoreClient.Components.Restore.GetRestore(restoredModel.GetId())
		state := restore.GetStatus()
		if err != nil {
			log.Errorf("failed during fetching the restore object, %v", err)
			return false, err
		}
		log.Infof("Restore status -  %v", state)
		if strings.ToLower(state) != strings.ToLower("Successful") {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return err
	}

	newDeploymentId := restoredModel.GetDeploymentId()
	newDeploymentModel, err := restoreClient.Components.DataServiceDeployment.GetDeployment(newDeploymentId)
	if err != nil {
		return fmt.Errorf("error while fetching restored deployment object")
	}
	dsType := ds.DataserviceType{}
	err = dsType.ValidateDataServiceDeployment(newDeploymentModel, nsName)
	if err != nil {
		return fmt.Errorf("error while validating the restored deployment")
	}
	restoreDsEntity := DSEntity{Deployment: newDeploymentModel}
	err = restoreClient.ValidateRestore(bkpDsEntity, restoreDsEntity)
	if err != nil {
		return fmt.Errorf("error while validation data service entities(i.e App config, resource etc). Err: %v", err)
	}
	return nil
}

func (restoreClient *RestoreClient) RestoreDataServiceWithRbac(pdsRestoreTargetClusterID string, backupJobId string, namespace string, bkpDsEntity DSEntity, pdsNamespaceId string, validate bool) (*pds.ModelsRestore, error) {

	restoredModel, err := restoreClient.Components.Restore.RestoreToNewDeployment(backupJobId, "autom-res", pdsRestoreTargetClusterID, pdsNamespaceId)
	if err != nil {
		log.Errorf("Failed during restore.")
		return nil, fmt.Errorf("failed during restore")
	}

	if validate {
		err = restoreClient.WaitForRestoreAndValidate(restoredModel, bkpDsEntity, namespace)
		if err != nil {
			log.Errorf("Failed to validate restored dataservice")
			return nil, fmt.Errorf("failed to validate restored dataservice")
		}
	}
	return restoredModel, nil
}

func (restoreClient *RestoreClient) getNameSpaceId(pdsClusterId string) (string, string, error) {
	randomName := generateRandomName("restore")
	_, err := restoreClient.RestoreTargetCluster.CreateNamespace(randomName)
	if err != nil {
		return "", "", fmt.Errorf("unable to create namespace %v", randomName)
	}
	nsId, err := restoreClient.RestoreTargetCluster.GetnameSpaceID(randomName, pdsClusterId)
	if err != nil {
		return "", "", fmt.Errorf("unable to fetch  %v", randomName)
	}
	return randomName, nsId, nil
}
func (restoreClient *RestoreClient) getNameSpaceIdCustomNs(pdsClusterId string, nsName string) (string, string, error) {
	nsName, nsId, err := restoreClient.getNameSpaceId(pdsClusterId)
	if err != nil {
		return "", "", fmt.Errorf("unable to fetch  %v", nsName)
	}
	return nsName, nsId, nil
}

func (restoreClient *RestoreClient) GetNameSpaceNameToRestore(backupJobId string, pdsRestoreTargetClusterID string, namespace string, isRestoreInSameNS bool) (string, string, error) {
	var bkpJob *pds.ModelsBackupJob
	var nsName string
	var pdsNamespaceId string
	var err error
	if !isRestoreInSameNS {
		nsName, pdsNamespaceId, err = restoreClient.getNameSpaceIdCustomNs(pdsRestoreTargetClusterID, namespace)
		if err != nil {
			return "", "", nil
		}
	} else {
		bkpJob, err = restoreClient.Components.BackupJob.GetBackupJob(backupJobId)
		if err != nil {
			return "", "", nil
		}
		nsName, pdsNamespaceId = namespace, bkpJob.GetNamespaceId()
	}
	log.Infof("backup Job - %v,Restore Target Cluster Id - %v, NamespaceId - %v", backupJobId, pdsRestoreTargetClusterID, pdsNamespaceId)
	return nsName, pdsNamespaceId, nil
}

func (restoreClient *RestoreClient) ValidateRestore(bkpDsEntity, restoreDsEntity DSEntity) error {
	// Validate the Application configuration
	log.Info("Validating application config post restore.")
	bkpAppConfig := bkpDsEntity.Deployment.Configuration
	restoreAppConfig := restoreDsEntity.Deployment.Configuration
	log.Infof("Backed up application configuration- %v", bkpAppConfig)
	log.Infof("Restored application configuration- %v", restoreAppConfig)
	if !reflect.DeepEqual(bkpAppConfig, restoreAppConfig) {
		return fmt.Errorf("restored application configuration are not same as backed up application config")
	}

	// Validate the Resource configuration
	log.Info("Validating Resource config post restore.")
	bkpResourceConfig := resourceStructToMap(bkpDsEntity.Deployment.Resources)
	restoreResourceConfig := resourceStructToMap(restoreDsEntity.Deployment.Resources)
	log.Infof("Backed up resource configuration- %v", bkpResourceConfig)
	log.Infof("Restored resource configuration- %v", restoreResourceConfig)
	if !reflect.DeepEqual(bkpResourceConfig, restoreResourceConfig) {
		return fmt.Errorf("restored resource configuration are not same as backed up resource config")
	}

	// Validate the StorageOption configuration
	log.Info("Validating Storage Option config post restore.")
	bkpStorageOptionConfig := storageOptionsStructToMap(bkpDsEntity.Deployment.StorageOptions)
	restoreStorageOptionConfig := storageOptionsStructToMap(restoreDsEntity.Deployment.StorageOptions)
	log.Infof("Backed up storage options configuration- %v", bkpStorageOptionConfig)
	log.Infof("Restored storage options configuration- %v", restoreStorageOptionConfig)
	if !reflect.DeepEqual(bkpStorageOptionConfig, restoreStorageOptionConfig) {
		return fmt.Errorf("restored storage options configuration are not same as backed up resource storage options config")
	}

	// NodeCount,DeploymentTarget, Image and data service version
	log.Info("Validating node counts and image id post restore.")
	log.Infof("Backed up data- node count: %v, image id: %v", bkpDsEntity.Deployment.GetNodeCount(), bkpDsEntity.Deployment.GetImageId())
	log.Infof("Restored data- node count: %v, image id: %v", restoreDsEntity.Deployment.GetNodeCount(), restoreDsEntity.Deployment.GetImageId())
	if bkpDsEntity.Deployment.GetNodeCount() != restoreDsEntity.Deployment.GetNodeCount() &&
		bkpDsEntity.Deployment.GetImageId() != restoreDsEntity.Deployment.GetImageId() {
		return fmt.Errorf("validation for nodeCount, image and data service version failed")
	}
	// TODO implement for data consistency validation.

	// TODO implement for Service Type validation.

	return nil
}
