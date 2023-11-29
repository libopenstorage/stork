package tests

import (
	"fmt"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/torpedo/drivers/node"
	pdsbkp "github.com/portworx/torpedo/drivers/pds/pdsbackup"
	restoreBkp "github.com/portworx/torpedo/drivers/pds/pdsrestore"
	"github.com/portworx/torpedo/drivers/volume"
	"k8s.io/apimachinery/pkg/api/resource"
	"net/http"
	"strconv"
	"strings"
	"time"

	pdsdriver "github.com/portworx/torpedo/drivers/pds"
	"github.com/portworx/torpedo/drivers/pds/api"
	"github.com/portworx/torpedo/drivers/pds/controlplane"
	dataservices "github.com/portworx/torpedo/drivers/pds/dataservice"
	"github.com/portworx/torpedo/drivers/pds/targetcluster"
	"k8s.io/apimachinery/pkg/util/wait"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	"github.com/portworx/torpedo/drivers/pds/parameters"
	tc "github.com/portworx/torpedo/drivers/pds/targetcluster"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/aetosutil"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/units"
	. "github.com/portworx/torpedo/tests"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

type PDSDataService struct {
	Name          string "json:\"Name\""
	Version       string "json:\"Version\""
	Image         string "json:\"Image\""
	Replicas      int    "json:\"Replicas\""
	ScaleReplicas int    "json:\"ScaleReplicas\""
	OldVersion    string "json:\"OldVersion\""
	OldImage      string "json:\"OldImage\""
}

type TestParams struct {
	DeploymentTargetId string
	DnsZone            string
	StorageTemplateId  string
	NamespaceId        string
	TenantId           string
	ProjectId          string
	ServiceType        string
}

const (
	pdsNamespace                        = "pds-system"
	deploymentName                      = "qa"
	envDeployAllDataService             = "DEPLOY_ALL_DATASERVICE"
	postgresql                          = "PostgreSQL"
	cassandra                           = "Cassandra"
	elasticSearch                       = "Elasticsearch"
	couchbase                           = "Couchbase"
	redis                               = "Redis"
	rabbitmq                            = "RabbitMQ"
	mongodb                             = "MongoDB"
	mysql                               = "MySQL"
	kafka                               = "Kafka"
	zookeeper                           = "ZooKeeper"
	consul                              = "Consul"
	pdsNamespaceLabel                   = "pds.portworx.com/available"
	timeOut                             = 30 * time.Minute
	maxtimeInterval                     = 30 * time.Second
	timeInterval                        = 1 * time.Second
	ActiveNodeRebootDuringDeployment    = "active-node-reboot-during-deployment"
	RebootNodeDuringAppVersionUpdate    = "reboot-node-during-app-version-update"
	KillDeploymentControllerPod         = "kill-deployment-controller-pod-during-deployment"
	RestartPxDuringDSScaleUp            = "restart-portworx-during-ds-scaleup"
	RestartAppDuringResourceUpdate      = "restart-app-during-resource-update"
	BackUpCRD                           = "backups.pds.io"
	DeploymentCRD                       = "deployments.pds.io"
	RebootNodesDuringDeployment         = "reboot-multiple-nodes-during-deployment"
	KillAgentPodDuringDeployment        = "kill-agent-pod-during-deployment"
	KillTeleportPodDuringDeployment     = "kill-teleport-pod-during-deployment"
	RestoreDSDuringPXPoolExpansion      = "restore-ds-during-px-pool-expansion"
	RestoreDSDuringKVDBFailOver         = "restore-ds-during-kvdb-fail-over"
	StopPXDuringStorageResize           = "stop-px-during-storage-resize"
	KillDbMasterNodeDuringStorageResize = "kill-db-master-node-during-storage-resize"
	RestoreDuringAllNodesReboot         = "restore-ds-during-node-reboot"
)

var (
	namespace                               string
	pxnamespace                             string
	tenantID                                string
	dnsZone                                 string
	clusterID                               string
	projectID                               string
	serviceType                             string
	deploymentTargetID                      string
	replicas                                int32
	err                                     error
	supportedDataServices                   []string
	dataServiceNameDefaultAppConfigMap      map[string]string
	namespaceID                             string
	storageTemplateID                       string
	dataServiceDefaultResourceTemplateIDMap map[string]string
	dataServiceNameIDMap                    map[string]string
	supportedDataServicesNameIDMap          map[string]string
	backupSupportedDataServiceNameIDMap     map[string]string
	DeployAllVersions                       bool
	DataService                             string
	DeployAllImages                         bool
	dataServiceDefaultResourceTemplateID    string
	dataServiceDefaultAppConfigID           string
	dataServiceVersionBuildMap              map[string][]string
	dataServiceImageMap                     map[string][]string
	dep                                     *v1.Deployment
	pod                                     *corev1.Pod
	params                                  *parameters.Parameter
	podList                                 *corev1.PodList
	isDeploymentsDeleted                    bool
	isNamespacesDeleted                     bool
	dash                                    *aetosutil.Dashboard
	deployment                              *pds.ModelsDeployment
	k8sCore                                 = core.Instance()
	k8sApps                                 = apps.Instance()
	pdsLabels                               = make(map[string]string)
	accountID                               string
)

// imports based on functionalities
var (
	dsTest        *dataservices.DataserviceType
	dsWithRbac    *dataservices.DsWithRBAC
	customParams  *parameters.Customparams
	targetCluster *targetcluster.TargetCluster
	controlPlane  *controlplane.ControlPlane
	components    *api.Components
	wkloadParams  pdsdriver.LoadGenParams
)

var (
	k8sStorage    = storage.Instance()
	apiExtentions = apiextensions.Instance()
)

var dataServiceDeploymentWorkloads = []string{cassandra, elasticSearch, postgresql, consul, mysql}
var dataServicePodWorkloads = []string{redis, rabbitmq, couchbase}

func DeployandValidateDataServicesWithSiAndTls(ds PDSDataService, namespaceName string, namespaceid, projectID string, resourceTemplateID string, appConfigID string, dsVersion string, dsImage string, dsID string, enableTls bool) (*pds.ModelsDeployment, map[string][]string, map[string][]string, error) {

	log.InfoD("Data Service Deployment Triggered")
	log.InfoD("Deploying ds in namespace %v and servicetype is %v", namespaceName, serviceType)

	deployment, dataServiceImageMap, dataServiceVersionBuildMap, err := dsWithRbac.TriggerDeployDSWithSiAndTls(dataservices.PDSDataService(ds), namespaceName, projectID, enableTls, resourceTemplateID, appConfigID, namespaceid, dsVersion, dsImage, dsID, dataservices.TestParams(TestParams{StorageTemplateId: storageTemplateID, DeploymentTargetId: deploymentTargetID, DnsZone: dnsZone, ServiceType: serviceType}))
	log.FailOnError(err, "Error occured while deploying data service %s", ds.Name)

	Step("Validate Data Service Deployments", func() {
		err = dsTest.ValidateDataServiceDeployment(deployment, namespaceName)
		log.FailOnError(err, fmt.Sprintf("Error while validating dataservice deployment %v", *deployment.ClusterResourceName))
	})
	return deployment, dataServiceImageMap, dataServiceVersionBuildMap, err
}

func RunWorkloads(params pdslib.WorkloadGenerationParams, ds PDSDataService, deployment *pds.ModelsDeployment, namespace string) (*corev1.Pod, *v1.Deployment, error) {
	params.DataServiceName = ds.Name
	params.DeploymentID = deployment.GetId()
	params.Namespace = namespace
	log.Infof("Dataservice Name : %s", ds.Name)

	if ds.Name == postgresql {
		params.DeploymentName = "pgload"
		params.ScaleFactor = "100"
		params.Iterations = "1"

		log.Infof("Running Workloads on DataService %v ", ds.Name)
		pod, dep, err = pdslib.CreateDataServiceWorkloads(params)

	}
	if ds.Name == rabbitmq {
		params.DeploymentName = "rmq"
		log.Infof("Running Workloads on DataService %v ", ds.Name)
		pod, dep, err = pdslib.CreateDataServiceWorkloads(params)

	}
	if ds.Name == redis {
		params.DeploymentName = "redisbench"
		params.Replicas = ds.Replicas
		log.Infof("Running Workloads on DataService %v ", ds.Name)

		pod, dep, err = pdslib.CreateDataServiceWorkloads(params)

	}
	if ds.Name == cassandra {
		params.DeploymentName = "cassandra-stress"
		log.Infof("Running Workloads on DataService %v ", ds.Name)
		pod, dep, err = pdslib.CreateDataServiceWorkloads(params)

	}
	if ds.Name == elasticSearch {
		params.DeploymentName = "es-rally"
		params.User = "elastic"
		params.UseSSL = "false"
		params.VerifyCerts = "false"
		params.TimeOut = "60"
		log.Infof("Running Workloads on DataService %v ", ds.Name)
		pod, dep, err = pdslib.CreateDataServiceWorkloads(params)

	}
	if ds.Name == couchbase {
		params.DeploymentName = "cb-load"
		log.Infof("Running Workloads on DataService %v ", ds.Name)
		pod, dep, err = pdslib.CreateDataServiceWorkloads(params)

	}
	if ds.Name == consul {
		params.DeploymentName = *deployment.ClusterResourceName
		log.Infof("Running Workloads on DataService %v ", ds.Name)
		pod, dep, err = pdslib.CreateDataServiceWorkloads(params)
	}
	if ds.Name == mysql {
		params.DeploymentName = *deployment.ClusterResourceName
		log.Infof("Running Workloads on DataService %v ", ds.Name)
		pod, dep, err = pdslib.CreateDataServiceWorkloads(params)
	}

	return pod, dep, err

}

// Check the DS related PV usage and resize in case of 90% full
func CheckPVCtoFullCondition(context []*scheduler.Context) error {
	log.Infof("Check PVC Usage")
	f := func() (interface{}, bool, error) {
		for _, ctx := range context {
			vols, err := Inst().S.GetVolumes(ctx)
			if err != nil {
				return nil, true, err
			}
			for _, vol := range vols {
				appVol, err := Inst().V.InspectVolume(vol.ID)
				if err != nil {
					return nil, true, err
				}
				pvcCapacity := appVol.Spec.Size / units.GiB
				usedGiB := appVol.GetUsage() / units.GiB
				threshold := pvcCapacity - 1
				if usedGiB >= threshold {
					log.Infof("The PVC capacity was %vGB , the consumed PVC is %vGB", pvcCapacity, usedGiB)
					return nil, false, nil
				}
			}
		}
		return nil, true, fmt.Errorf("threshold not achieved for the PVC, ")
	}
	_, err := task.DoRetryWithTimeout(f, 30*time.Minute, 15*time.Second)

	return err
}

func ScaleUpDeployments(tenantID string, deployments map[PDSDataService]*pds.ModelsDeployment) {
	for ds, deployment := range deployments {
		log.InfoD("Scaling up DataService %v ", ds.Name)

		dataServiceDefaultAppConfigID, err = controlPlane.GetAppConfTemplate(tenantID, ds.Name)
		log.FailOnError(err, "Error while getting app configuration template")
		dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

		dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
		log.FailOnError(err, "Error while getting resource setting template")
		dash.VerifyFatal(dataServiceDefaultResourceTemplateID != "", true, "Validating dataServiceDefaultAppConfigID")

		updatedDeployment, err := dsTest.UpdateDataServices(deployment.GetId(),
			dataServiceDefaultAppConfigID, deployment.GetImageId(),
			int32(ds.ScaleReplicas), dataServiceDefaultResourceTemplateID, namespace)
		log.FailOnError(err, "Error while updating dataservices")

		err = dsTest.ValidateDataServiceDeployment(updatedDeployment, namespace)
		log.FailOnError(err, "Error while validating data service deployment")

		_, _, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
		log.FailOnError(err, "error on ValidateDataServiceVolumes method")
		dash.VerifyFatal(int32(ds.ScaleReplicas), config.Replicas, "Validating replicas after scaling up of dataservice")
	}
}

func CleanMapEntries(deleteMapEntries map[string]string) {
	for hash := range deleteMapEntries {
		delete(deleteMapEntries, hash)
	}
	log.Debugf("size of map post deletion %d", len(deleteMapEntries))
}

// CleanupWorkloadDeployments will clean up the wldeployment based on the kubeconfigs
func CleanupWorkloadDeployments(wlDeploymentsToBeCleaned []*v1.Deployment, isSrc bool) error {
	if isSrc {
		SetSourceKubeConfig()
	} else {
		SetDestinationKubeConfig()
	}
	for _, wlDep := range wlDeploymentsToBeCleaned {
		err := k8sApps.DeleteDeployment(wlDep.Name, wlDep.Namespace)
		if err != nil {
			return err
		}
	}
	return nil
}

func GetPvsAndPVCsfromDeployment(namespace string, deployment *pds.ModelsDeployment) (*corev1.PersistentVolumeClaimList, []*volume.Volume) {
	log.Infof("Get PVC List based on namespace and deployment")
	k8sOps := k8sCore
	var vols []*volume.Volume
	labelSelector := make(map[string]string)
	labelSelector["name"] = deployment.GetClusterResourceName()
	pvcList, _ := k8sOps.GetPersistentVolumeClaims(namespace, labelSelector)
	for _, pvc := range pvcList.Items {
		vols = append(vols, &volume.Volume{
			ID: pvc.Spec.VolumeName,
		})
	}
	return pvcList, vols
}

// Check the DS related PV usage and resize in case of 90% full
func CheckStorageFullCondition(namespace string, deployment *pds.ModelsDeployment) error {
	log.Infof("Check PVC Usage")
	f := func() (interface{}, bool, error) {
		_, vols := GetPvsAndPVCsfromDeployment(namespace, deployment)
		for _, vol := range vols {
			appVol, err := Inst().V.InspectVolume(vol.ID)
			if err != nil {
				return nil, true, err
			}
			pvcCapacity := appVol.Spec.Size / units.GiB
			usedGiB := appVol.GetUsage() / units.GiB
			threshold := pvcCapacity - 1
			if usedGiB >= threshold {
				log.Infof("The PVC is consumed upto threshold . PVC capacity was %vGB , the consumed PVC is %vGB", pvcCapacity, usedGiB)
				return nil, false, nil
			}
		}
		return nil, true, fmt.Errorf("threshold not achieved for the PVC, ")
	}
	_, err := task.DoRetryWithTimeout(f, 30*time.Minute, 15*time.Second)

	return err
}

// Increase PVC by 1 gb
func IncreasePVCby1Gig(namespace string, deployment *pds.ModelsDeployment, sizeInGb uint64) (*volume.Volume, error) {
	log.Info("Resizing of the PVC begins")
	var vol *volume.Volume
	pvcList, _ := GetPvsAndPVCsfromDeployment(namespace, deployment)
	initialCapacity, err := GetVolumeCapacityInGB(namespace, deployment)
	log.Debugf("Initial volume storage size is : %v", initialCapacity)
	if err != nil {
		return nil, err
	}
	for _, pvc := range pvcList.Items {
		k8sOps := k8sCore
		storageSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		extraAmount, _ := resource.ParseQuantity(fmt.Sprintf("%dGi", sizeInGb))
		storageSize.Add(extraAmount)
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = storageSize
		_, err := k8sOps.UpdatePersistentVolumeClaim(&pvc)
		if err != nil {
			return nil, err
		}
		sizeInt64, _ := storageSize.AsInt64()
		vol = &volume.Volume{
			Name:          pvc.Name,
			RequestedSize: uint64(sizeInt64),
		}
	}

	// wait for the resize to take effect
	time.Sleep(30 * time.Second)
	newcapacity, err := GetVolumeCapacityInGB(namespace, deployment)
	log.Infof("Resized volume storage size is : %v", newcapacity)
	if err != nil {
		return nil, err
	}

	if newcapacity > initialCapacity {
		log.InfoD("Successfully resized the pvc by 1gb")
		return vol, nil
	} else {
		return vol, err
	}
}

// Get volume capacity
func GetVolumeCapacityInGB(namespace string, deployment *pds.ModelsDeployment) (uint64, error) {
	var pvcCapacity uint64
	_, vols := GetPvsAndPVCsfromDeployment(namespace, deployment)
	for _, vol := range vols {
		appVol, err := Inst().V.InspectVolume(vol.ID)
		if err != nil {
			return 0, err
		}
		pvcCapacity = appVol.Spec.Size / units.GiB
	}
	return pvcCapacity, err
}

func CleanUpBackUpTargets(projectID, objectStore, prefix string) error {
	bkpClient, err := pdsbkp.InitializePdsBackup()
	if err != nil {
		return err
	}
	bkpTargets, err := bkpClient.GetAllBackUpTargets(projectID, prefix)
	for _, bkpTarget := range bkpTargets {
		log.Debugf("Deleting bkptarget %s", bkpTarget.GetName())
		if objectStore == "s3" {
			err = bkpClient.DeleteAwsS3BackupCredsAndTarget(bkpTarget.GetId())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidateDataIntegrityPostRestore validates the md5hash for the given deployments and returns the workload pods
func ValidateDataIntegrityPostRestore(dataServiceDeployments []*pds.ModelsDeployment,
	pdsdeploymentsmd5Hash map[string]string) []*v1.Deployment {
	var (
		wlDeploymentsToBeCleanedinDest []*v1.Deployment
		restoredDeploymentsmd5Hash     = make(map[string]string)
	)
	for _, pdsDeployment := range dataServiceDeployments {
		ckSum, wlDep, err := dsTest.ReadDataAndReturnChecksum(pdsDeployment, wkloadParams)
		wlDeploymentsToBeCleanedinDest = append(wlDeploymentsToBeCleanedinDest, wlDep)
		log.FailOnError(err, "Error while Running workloads")
		log.Debugf("Checksum for the deployment %s is %s", *pdsDeployment.ClusterResourceName, ckSum)
		restoredDeploymentsmd5Hash[*pdsDeployment.ClusterResourceName] = ckSum
	}

	dash.VerifyFatal(dsTest.ValidateDataMd5Hash(pdsdeploymentsmd5Hash, restoredDeploymentsmd5Hash),
		true, "Validate md5 hash after restore")

	return wlDeploymentsToBeCleanedinDest
}

func PerformRestore(restoreClient restoreBkp.RestoreClient, dsEntity restoreBkp.DSEntity, projectID string, deployment *pds.ModelsDeployment) []*pds.ModelsDeployment {
	var restoredDeployments []*pds.ModelsDeployment
	backupJobs, err := restoreClient.Components.BackupJob.ListBackupJobsBelongToDeployment(projectID, deployment.GetId())
	log.FailOnError(err, "Error while fetching the backup jobs for the deployment: %v", deployment.GetClusterResourceName())
	for _, backupJob := range backupJobs {
		log.Infof("[Restoring] Details Backup job name- %v, Id- %v", backupJob.GetName(), backupJob.GetId())
		restoredModel, err := restoreClient.TriggerAndValidateRestore(backupJob.GetId(), params.InfraToTest.Namespace, dsEntity, true, true)
		log.FailOnError(err, "Failed during restore.")
		restoredDeployment, err := restoreClient.Components.DataServiceDeployment.GetDeployment(restoredModel.GetDeploymentId())
		log.FailOnError(err, fmt.Sprintf("Failed while fetching the restore data service instance: %v", restoredModel.GetClusterResourceName()))
		restoredDeployments = append(restoredDeployments, restoredDeployment)
		log.InfoD("Restored successfully. Details: Deployment- %v, Status - %v", restoredModel.GetClusterResourceName(), restoredModel.GetStatus())
	}
	return restoredDeployments
}

func CleanupDeployments(dsInstances []*pds.ModelsDeployment) {
	if len(dsInstances) < 1 {
		log.Info("No DS left for deletion as part of this test run.")
	}
	log.InfoD("Deleting all the ds instances.")
	for _, dsInstance := range dsInstances {
		log.InfoD("Deleting Deployment %v ", *dsInstance.ClusterResourceName)
		dsId := *dsInstance.Id
		components.DataServiceDeployment.GetDeployment(dsId)
		log.Infof("Deleting Deployment %v ", dsInstance.GetClusterResourceName())
		resp, err := pdslib.DeleteDeployment(dsInstance.GetId())
		if err != nil {
			log.Infof("The deployment %v is associated with the backup jobs.", dsInstance.GetClusterResourceName())
			err = DeleteAllDsBackupEntities(dsInstance)
			log.FailOnError(err, "Failed during deleting the backup entities for deployment %v",
				dsInstance.GetClusterResourceName())
			resp, err = pdslib.DeleteDeployment(dsInstance.GetId())
			log.FailOnError(err, "Error while deleting deployment.")
		}
		log.FailOnError(err, "Error while deleting deployment.")
		dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")

		log.InfoD("Getting all PV and associated PVCs and deleting them")
		err = pdslib.DeletePvandPVCs(*dsInstance.ClusterResourceName, false)
		log.FailOnError(err, "Error while deleting PV and PVCs")
	}
}

// GetReplicaNodes return the volume replicated nodes and its pool id's
func GetReplicaNodes(appVolume *volume.Volume) ([]string, []string, error) {
	replicaSets, err := Inst().V.GetReplicaSets(appVolume)
	if err != nil {
		return nil, nil, err
	}
	log.FailOnError(err, "error while getting replicasets")
	replicaNodes := replicaSets[0].Nodes
	for _, node := range replicaNodes {
		log.Debugf("replica node [%s] of volume [%s]", node, appVolume.Name)
	}
	replPools := replicaSets[0].PoolUuids

	return replPools, replicaNodes, nil
}

// GetVolumeNodesOnWhichPxIsRunning fetches the lit of Volnodes on which PX is running
func GetVolumeNodesOnWhichPxIsRunning() []node.Node {
	var (
		nodesToStopPx []node.Node
		stopPxNode    []node.Node
	)
	stopPxNode = node.GetStorageNodes()
	if err != nil {
		log.FailOnError(err, "Error while getting PX Node to Restart")
	}
	log.InfoD("PX the node with vol running found is-  %v ", stopPxNode)
	nodesToStopPx = append(nodesToStopPx, stopPxNode[0])
	return nodesToStopPx
}

// StopPxOnReplicaVolumeNode is used to STOP PX on the given list of nodes
func StopPxOnReplicaVolumeNode(nodesToStopPx []node.Node) error {
	err = Inst().V.StopDriver(nodesToStopPx, true, nil)
	if err != nil {
		log.FailOnError(err, "Error while trying to STOP PX on the volNode- [%v]", nodesToStopPx)
	}
	log.InfoD("PX stopped successfully on node %v", nodesToStopPx)
	return nil
}

// StartPxOnReplicaVolumeNode is used to START PX on the given list of nodes
func StartPxOnReplicaVolumeNode(nodesToStartPx []node.Node) error {
	for _, nodeName := range nodesToStartPx {
		log.InfoD("Going ahead and re-starting PX the node %v as there is an ", nodeName)
		err = Inst().V.StartDriver(nodeName)
		if err != nil {
			log.FailOnError(err, "Error while trying to Start PX on the volNode- [%v]", nodeName)
			return err
		}
		log.InfoD("PX ReStarted successfully on node %v", nodeName)
	}
	return nil
}

func CleanupServiceIdentitiesAndIamRoles(siToBeCleaned []string, iamRolesToBeCleaned []string, actorID string) {
	log.InfoD("Starting to delete the Iam Roles first...")
	for _, iam := range iamRolesToBeCleaned {
		resp, err := components.IamRoleBindings.DeleteIamRoleBinding(accountID, actorID)
		if err != nil {
			log.FailOnError(err, "Error while deleting IamRoles")
		}
		log.InfoD("Successfully deleted IAMRoles with ID- %v and ResponseStatus is- %v", iam, resp.StatusCode)
	}
	log.InfoD("Starting to delete the Service Identities...")
	for _, si := range siToBeCleaned {
		resp, err := components.ServiceIdentity.DeleteServiceIdentity(si)
		if err != nil {
			log.FailOnError(err, "Error while deleting ServiceIdentities")
		}
		log.InfoD("Successfully deleted ServiceIdentities- %v", resp.StatusCode)
	}

}

func DeleteAllDsBackupEntities(dsInstance *pds.ModelsDeployment) error {
	log.Infof("Fetch backups associated to the deployment %v ",
		dsInstance.GetClusterResourceName())

	//ListBackup api uses older way of finding the backups
	//we need to delete backup jobs
	backups, err := components.Backup.ListBackup(dsInstance.GetId())
	if err != nil {
		return fmt.Errorf("failed while fetching the backup objects.Err - %v", err)
	}
	for _, backup := range backups {
		log.Infof("Delete backup.Details: Name - %v, Id - %v", backup.GetClusterResourceName(), backup.GetId())
		backupId := backup.GetId()
		resp, err := components.Backup.DeleteBackup(backupId)
		waitErr := wait.Poll(maxtimeInterval, timeOut, func() (bool, error) {
			model, bkpErr := components.Backup.GetBackup(backupId)
			if model != nil {
				log.Info(model.GetId())
				return false, bkpErr
			}
			if bkpErr != nil && strings.Contains(bkpErr.Error(), "not found") {
				return true, nil
			}
			return false, bkpErr
		})
		if waitErr != nil {
			return fmt.Errorf("error occured while polling for deleting backup : %v", err)
		}
		if err != nil {
			return fmt.Errorf("backup object %v deletion failed.Err - %v, Response status - %v",
				backup.GetClusterResourceName(), err, resp.StatusCode)
		}
	}
	return nil
}

func GetDbMasterNode(namespace string, dsName string, deployment *pds.ModelsDeployment, targetCluster *tc.TargetCluster) (string, bool) {
	var command, dbMaster string
	switch dsName {
	case dataservices.Postgresql:
		command = fmt.Sprintf("patronictl list | grep -i leader | awk '{print $2}'")
		dbMaster, err = targetCluster.ExecuteCommandInStatefulSetPod(deployment.GetClusterResourceName(), namespace, command)
		log.FailOnError(err, "Failed while fetching db master pods=.")
		log.Infof("Deployment %v of type %v have the master "+
			"running at %v pod.", deployment.GetClusterResourceName(), dsName, dbMaster)
	case dataservices.Mysql:
		_, connectionDetails, err := pdslib.ApiComponents.DataServiceDeployment.GetConnectionDetails(deployment.GetId())
		log.FailOnError(err, "Failed while fetching connection details.")
		cred, err := pdslib.ApiComponents.DataServiceDeployment.GetDeploymentCredentials(deployment.GetId())
		log.FailOnError(err, "Failed while fetching credentials.")
		command = fmt.Sprintf("mysqlsh --host=%v --port %v --user=innodb-config "+
			" --password=%v -- cluster status", connectionDetails["host"], connectionDetails["port"], cred.GetPassword())
		dbMaster, err = targetCluster.ExecuteCommandInStatefulSetPod(deployment.GetClusterResourceName(), namespace, command)
		log.Infof("Deployment %v of type %v have the master "+
			"running at %v pod.", deployment.GetClusterResourceName(), dsName, dbMaster)
	default:
		return "", false
	}
	return dbMaster, true
}

func KillDbMasterNodeDuringStorageIncrease(dsName string, nsName string, deployment *pds.ModelsDeployment, sourceTarget *targetcluster.TargetCluster) error {
	log.Debugf("I HAVE ENTERED INTO KILL FUNC")
	dbMaster, _ := GetDbMasterNode(nsName, dsName, deployment, sourceTarget)
	log.InfoD("dbMaster Node is %v-", dbMaster)
	log.FailOnError(err, "Failed while fetching db master node.")

	err = sourceTarget.DeleteK8sPods(dbMaster, nsName)
	log.FailOnError(err, "Failed while deleting db master pod.")
	err = dsTest.ValidateDataServiceDeployment(deployment, nsName)
	log.FailOnError(err, "Failed while validating the deployment pods, post pod deletion.")
	log.InfoD("DB MasterNode- [%v] Successfully killed", dbMaster)
	newDbMaster, _ := GetDbMasterNode(nsName, dsName, deployment, sourceTarget)
	if dbMaster == newDbMaster {
		log.FailOnError(fmt.Errorf("leader node is not reassigned"), fmt.Sprintf("Leader pod %v", dbMaster))
	}
	log.InfoD("New DB MasterNode- [%v] is created.", newDbMaster)
	return nil
}

// ValidateDepConfigPostStorageIncrease Verifies the storage and other config values after storage resize
func ValidateDepConfigPostStorageIncrease(ds PDSDataService, updatedDeployment *pds.ModelsDeployment, stConfigUpdated *pds.ModelsStorageOptionsTemplate, resConfigUpdated *pds.ModelsResourceSettingsTemplate, initialCapacity, updatedPvcSize uint64) error {
	log.InfoD("Get updated template ids from the storageModel and the resourceModel")
	newResourceTemplateID := resConfigUpdated.GetId()
	newStorageTemplateID := stConfigUpdated.GetId()
	_, _, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, ds.Name, newResourceTemplateID, newStorageTemplateID, params.InfraToTest.Namespace)
	log.FailOnError(err, "error on ValidateDataServiceVolumes method")
	log.InfoD("resConfigModel.StorageRequest val is- %v and updated config val is- %v", *resConfigUpdated.StorageRequest, config.Resources.Requests.EphemeralStorage)
	dash.VerifyFatal(config.Resources.Requests.EphemeralStorage, *resConfigUpdated.StorageRequest, "Validating the storage size is updated in the config post resize (STS-LEVEL)")
	dash.VerifyFatal(config.Parameters.Fs, *stConfigUpdated.Fs, "Validating the File System Type post storage resize (FileSystem-LEVEL)")
	stringRelFactor := strconv.Itoa(int(*stConfigUpdated.Repl))
	dash.VerifyFatal(config.Parameters.Repl, stringRelFactor, "Validating the Replication Factor count post storage resize (RepelFactor-LEVEL)")
	if updatedPvcSize > initialCapacity {
		flag := true
		dash.VerifyFatal(flag, true, "Validating the storage size is updated in the config post resize (PV/PVC-LEVEL)")
		log.InfoD("Initial PVC Capacity is- %v and Updated PVC Capacity is- %v", initialCapacity, updatedPvcSize)
	} else {
		log.FailOnError(err, "Failed to verify Storage Resize at PV/PVC level")
	}
	return nil
}
