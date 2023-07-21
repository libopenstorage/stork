package pdsrestore

import (
	"fmt"
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
	restoreTimeOut         = 30 * time.Minute
	restoreTimeInterval    = 20 * time.Second
	restoreMaxTimeInterval = 1 * time.Minute
)

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
	restoredModel, err := restoreClient.Components.Restore.RestoreToNewDeployment(backupJobId, "autom", pdsRestoreTargetClusterId, pdsNamespaceId)
	if err != nil {
		log.Errorf("Failed during restore.")
		return nil, fmt.Errorf("failed during restore")
	}
	err = wait.Poll(restoreTimeInterval, restoreTimeOut, func() (bool, error) {
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
		return nil, err
	}

	if validate {
		newDeploymentId := restoredModel.GetDeploymentId()
		newDeploymentModel, err := restoreClient.Components.DataServiceDeployment.GetDeployment(newDeploymentId)
		if err != nil {
			return nil, fmt.Errorf("error while fetching restored deployment object")
		}
		dsType := ds.DataserviceType{}
		err = dsType.ValidateDataServiceDeployment(newDeploymentModel, nsName)
		if err != nil {
			return nil, fmt.Errorf("error while validating the restored deployment")
		}
		restoreDsEntity := DSEntity{Deployment: newDeploymentModel}
		err = restoreClient.ValidateRestore(bkpDsEntity, restoreDsEntity)
		if err != nil {
			return nil, fmt.Errorf("error while validation data service entities(i.e App confoig, resource etc). Err: %v", err)
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

func (restoreClient *RestoreClient) ValidateRestore(bkpDsEntity, restoreDsEntity DSEntity) error {
	// Validate the Application configuration
	log.Info("Validating application config post restore.")
	bkpAppConfig := bkpDsEntity.Deployment.Configuration
	restoreAppConfig := restoreDsEntity.Deployment.Configuration
	log.Infof("Backed up resource configuration- %v", bkpAppConfig)
	log.Infof("Restored resource configuration- %v", restoreAppConfig)
	if !reflect.DeepEqual(bkpAppConfig, restoreAppConfig) {
		return fmt.Errorf("restored Application configuration are not same as as backed up app config")
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
	log.Infof("Backed up resource configuration- %v", bkpStorageOptionConfig)
	log.Infof("Restored resource configuration- %v", restoreStorageOptionConfig)
	if !reflect.DeepEqual(bkpStorageOptionConfig, restoreStorageOptionConfig) {
		return fmt.Errorf("restored resource configuration are not same as backed up resource config")
	}

	// NodeCount,DeploymentTarget, Image and data service version
	log.Info("Validating application config post restore.")
	log.Infof("Backed up data- node count: %v, image id: %v", &bkpDsEntity.Deployment.NodeCount, &bkpDsEntity.Deployment.ImageId)
	log.Infof("Restored data- node count: %v, image id: %v", &restoreDsEntity.Deployment.NodeCount, &restoreDsEntity.Deployment.ImageId)
	if bkpDsEntity.Deployment.GetNodeCount() != restoreDsEntity.Deployment.GetNodeCount() &&
		bkpDsEntity.Deployment.GetImageId() != restoreDsEntity.Deployment.GetImageId() {
		return fmt.Errorf("validation for nodeCount, image and data service version failed")
	}
	// TODO implement for data consistency validation.

	// TODO implement for Service Type validation.

	return nil
}
