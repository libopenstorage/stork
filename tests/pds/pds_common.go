package tests

import (
	"fmt"
	pdsdriver "github.com/portworx/torpedo/drivers/pds"
	"github.com/portworx/torpedo/drivers/pds/api"
	"github.com/portworx/torpedo/drivers/pds/controlplane"
	dataservices "github.com/portworx/torpedo/drivers/pds/dataservice"
	"github.com/portworx/torpedo/drivers/pds/targetcluster"
	"net/http"
	"time"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	"github.com/portworx/torpedo/drivers/pds/parameters"
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

const (
	pdsNamespace                     = "pds-system"
	deploymentName                   = "qa"
	envDeployAllDataService          = "DEPLOY_ALL_DATASERVICE"
	postgresql                       = "PostgreSQL"
	cassandra                        = "Cassandra"
	elasticSearch                    = "Elasticsearch"
	couchbase                        = "Couchbase"
	redis                            = "Redis"
	rabbitmq                         = "RabbitMQ"
	mongodb                          = "MongoDB"
	mysql                            = "MySQL"
	kafka                            = "Kafka"
	zookeeper                        = "ZooKeeper"
	consul                           = "Consul"
	pdsNamespaceLabel                = "pds.portworx.com/available"
	timeOut                          = 30 * time.Minute
	maxtimeInterval                  = 30 * time.Second
	timeInterval                     = 1 * time.Second
	ActiveNodeRebootDuringDeployment = "active-node-reboot-during-deployment"
	RebootNodeDuringAppVersionUpdate = "reboot-node-during-app-version-update"
	KillDeploymentControllerPod      = "kill-deployment-controller-pod-during-deployment"
	RestartPxDuringDSScaleUp         = "restart-portworx-during-ds-scaleup"
	RestartAppDuringResourceUpdate   = "restart-app-during-resource-update"
	BackUpCRD                        = "backups.pds.io"
	DeploymentCRD                    = "deployments.pds.io"
	RebootNodesDuringDeployment      = "reboot-multiple-nodes-during-deployment"
	KillAgentPodDuringDeployment     = "kill-agent-pod-during-deployment"
	KillTeleportPodDuringDeployment  = "kill-teleport-pod-during-deployment"
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
	customParams  *parameters.Customparams
	targetCluster *targetcluster.TargetCluster
	controlPlane  *controlplane.ControlPlane
	components    *api.Components
	wkloadParams  pdsdriver.LoadGenParams
)

var dataServiceDeploymentWorkloads = []string{cassandra, elasticSearch, postgresql, consul, mysql}
var dataServicePodWorkloads = []string{redis, rabbitmq, couchbase}

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

// Increase PVC by 1 gb
func IncreasePVCby1Gig(context []*scheduler.Context) error {
	log.Info("Resizing of the PVC begins")
	initialCapacity, err := GetVolumeCapacityInGB(context)
	log.Debugf("Initial volume storage size is : %v", initialCapacity)
	if err != nil {
		return err
	}
	for _, ctx := range context {
		appVolumes, err := Inst().S.ResizeVolume(ctx, "")
		log.FailOnError(err, "Volume resize successful ?")
		log.InfoD(fmt.Sprintf("validating successful volume size increase on app %s's volumes: %v",
			ctx.App.Key, appVolumes))
	}
	// wait for the resize to take effect
	time.Sleep(30 * time.Second)
	newcapacity, err := GetVolumeCapacityInGB(context)
	log.Infof("Resized volume storage size is : %v", newcapacity)
	if err != nil {
		return err
	}

	if newcapacity > initialCapacity {
		log.InfoD("Successfully resized the pvc by 1gb")
		return nil
	}
	return nil
}

// Get volume capacity
func GetVolumeCapacityInGB(context []*scheduler.Context) (uint64, error) {
	var pvcCapacity uint64
	for _, ctx := range context {
		vols, err := Inst().S.GetVolumes(ctx)
		if err != nil {
			return 0, err
		}
		for _, vol := range vols {
			appVol, err := Inst().V.InspectVolume(vol.ID)
			if err != nil {
				return 0, err
			}
			pvcCapacity = appVol.Spec.Size / units.GiB
		}
	}
	return pvcCapacity, err
}

// CleanupDeployments used to clean up deployment from pds and all other stale resources in the cluster.
func CleanupDeployments(dsInstances []*pds.ModelsDeployment) {
	for _, dsInstance := range dsInstances {
		log.InfoD("Deleting Deployment %v ", *dsInstance.ClusterResourceName)
		resp, err := pdslib.DeleteDeployment(dsInstance.GetId())
		log.FailOnError(err, "Error while deleting data services")
		dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
		log.InfoD("Getting all PV and associated PVCs and deleting them")
		err = pdslib.DeletePvandPVCs(*dsInstance.ClusterResourceName, false)
		log.FailOnError(err, "Error while deleting PV and PVCs")
	}
}
