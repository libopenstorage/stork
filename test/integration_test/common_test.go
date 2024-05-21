//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	oputils "github.com/libopenstorage/operator/drivers/storage/portworx/util"
	opcorev1 "github.com/libopenstorage/operator/pkg/apis/core/v1"
	"github.com/libopenstorage/stork/pkg/aetosutils"
	"github.com/libopenstorage/stork/pkg/log"
	aetosLogger "github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/dynamic"
	"github.com/portworx/sched-ops/k8s/externalstorage"
	"github.com/portworx/sched-ops/k8s/openshift"
	"github.com/portworx/sched-ops/k8s/operator"
	"github.com/portworx/sched-ops/k8s/rbac"
	"github.com/portworx/sched-ops/k8s/storage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	_ "github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/portworx/torpedo/drivers/objectstore"
	"github.com/portworx/torpedo/drivers/scheduler"
	_ "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/volume"
	_ "github.com/portworx/torpedo/drivers/volume/aws"
	_ "github.com/portworx/torpedo/drivers/volume/azure"
	_ "github.com/portworx/torpedo/drivers/volume/gce"
	_ "github.com/portworx/torpedo/drivers/volume/generic_csi"
	_ "github.com/portworx/torpedo/drivers/volume/linstor"
	_ "github.com/portworx/torpedo/drivers/volume/portworx"
	aetosutil "github.com/portworx/torpedo/pkg/aetosutil"
	"github.com/portworx/torpedo/pkg/stats"
	testrailutils "github.com/portworx/torpedo/pkg/testrailuttils"
	"github.com/sirupsen/logrus"
	"github.com/skyrings/skyring-common/tools/uuid"
	appsapi "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storage_v1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	storkdriver "github.com/libopenstorage/stork/drivers/volume"
	_ "github.com/libopenstorage/stork/drivers/volume/aws"
	_ "github.com/libopenstorage/stork/drivers/volume/azure"
	_ "github.com/libopenstorage/stork/drivers/volume/csi"
	_ "github.com/libopenstorage/stork/drivers/volume/gcp"
	_ "github.com/libopenstorage/stork/drivers/volume/linstor"
	_ "github.com/libopenstorage/stork/drivers/volume/portworx"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/schedule"
	"github.com/libopenstorage/stork/pkg/storkctl"
	"github.com/libopenstorage/stork/pkg/version"
)

const (
	nodeDriverName               = "ssh"
	cmName                       = "stork-version"
	defaultAdminNamespace        = "kube-system"
	schedulerDriverName          = "k8s"
	remotePairName               = "remoteclusterpair"
	srcConfig                    = "sourceconfigmap"
	destConfig                   = "destinationconfigmap"
	specDir                      = "./specs"
	tempDir                      = "/tmp"
	defaultClusterPairDir        = "cluster-pair"
	bidirectionalClusterPairDir  = "bidirectional-cluster-pair"
	unidirectionalClusterPairDir = "unidirectionalClusterPairDir"
	pairFileName                 = "cluster-pair.yaml"
	remoteFilePath               = "/tmp/kubeconfig"
	configMapSyncWaitTime        = 3 * time.Second
	defaultSchedulerName         = "default-scheduler"
	bucketPrefix                 = "stork-test"
	adminTokenSecretName         = "px-admin-token"
	pxStcServiceTypeKey          = "portworx.io/service-type"
	stcLoadBalancerValue         = "LoadBalancer"
	pxStcServiceKey              = "service/portworx-service"
	awsInternalLBKey             = "service.beta.kubernetes.io/aws-load-balancer-internal"
	awsInternalLBValue           = "true"
	awsLBTypeKey                 = "service.beta.kubernetes.io/aws-load-balancer-type"
	awsLBTypeVal                 = "nlb"
	awsNLBTargetTypeKey          = "service.beta.kubernetes.io/aws-load-balancer-nlb-target-type"
	awsNLBTargetTypeVal          = "ip"
	awsLBSubnetKey               = "service.beta.kubernetes.io/aws-load-balancer-subnets"
	awsLBSubnetVal               = "subnet-0758f16bc3ca384e0"

	pxServiceName = "portworx-service"

	// TODO: Figure out a way to communicate with PX nodes from other cluster
	nodeScore   = 100
	rackScore   = 50
	zoneScore   = 25
	regionScore = 10

	defaultWaitTimeout       time.Duration = 10 * time.Minute
	clusterDomainWaitTimeout time.Duration = 10 * time.Minute
	groupSnapshotWaitTimeout time.Duration = 15 * time.Minute
	defaultWaitInterval      time.Duration = 10 * time.Second
	backupWaitInterval       time.Duration = 2 * time.Second

	enableClusterDomainTests = "ENABLE_CLUSTER_DOMAIN_TESTS"
	storageProvisioner       = "STORAGE_PROVISIONER"
	authSecretConfigMap      = "AUTH_SECRET_CONFIGMAP"
	backupPathVar            = "BACKUP_LOCATION_PATH"
	externalTestCluster      = "EXTERNAL_TEST_CLUSTER"
	cloudDeletionValidation  = "CLOUD_DELETION_VALIDATION"
	internalLBAws            = "INTERNAL_AWS_LB"
	portworxNamespace        = "PX_NAMESPACE"
	enableDashStats          = "ENABLE_DASH"

	tokenKey    = "token"
	clusterIP   = "ip"
	clusterPort = "port"

	testResultPass = "Pass"
	testResultFail = "Fail"

	testrailRunNameVar         = "TESTRAIL_RUN_NAME"
	testrailRunIDVar           = "TESTRAIL_RUN_ID"
	testrailJenkinsBuildURLVar = "TESTRAIL_JENKINS_BUILD_URL"
	testrailHostVar            = "TESTRAIL_HOST"
	testrailUserNameVar        = "TESTRAIL_USERNAME"
	testrailPasswordVar        = "TESTRAIL_PASSWORD"
	testrailMilestoneVar       = "TESTRAIL_MILESTONE"
)

var (
	enableDashboard = true
	aetosLog        *logrus.Logger
	testLogFile     *os.File
	Dash            = aetosutils.Get()
)

var nodeDriver node.Driver
var schedulerDriver scheduler.Driver
var volumeDriver volume.Driver

var storkVolumeDriver storkdriver.Driver
var objectStoreDriver objectstore.Driver
var tpDash *aetosutil.Dashboard

var snapshotScaleCount int
var migrationScaleCount int
var backupScaleCount int
var authToken string
var authTokenConfigMap string
var volumeDriverName string
var schedulerName string
var backupLocationPath string
var genericCsiConfigMap string
var externalTest bool
var storkVersionCheck bool
var cloudDeletionValidate bool
var isInternalLBAws bool
var pxNamespace string
var storkVersion string
var testrailHostname string
var testrailUsername string
var testrailPassword string
var testrailSetupSuccessful bool
var bidirectionalClusterpair bool
var unidirectionalClusterpair bool
var currentTestSuite string
var kubevirtScale int

func TestSnapshot(t *testing.T) {
	currentTestSuite = t.Name()
	t.Run("testSnapshot", testSnapshot)
	t.Run("testSnapshotRestore", testSnapshotRestore)
	t.Run("testWebhook", testWebhook)
}

func TestStorkCbt(t *testing.T) {
	t.Run("deploymentTest", deploymentMigrationTest)
	t.Run("dataExportTest", TestDataExportRsync)
	t.Run("testMigrationFailoverFailback", testMigrationFailoverFailback)
	t.Run("stopDriverTest", stopDriverTest)
	t.Run("simpleSnapshotTest", simpleSnapshotTest)
	t.Run("pvcOwnershipTest", pvcOwnershipTest)
	t.Run("cmdExecutorTest", cmdExecutorTest)
}

func TestStorkCbtBackup(t *testing.T) {
	setDefaultsForBackup(t)

	log.InfoD("Using stork volume driver: %s", volumeDriverName)
	log.InfoD("Backup path being used: %s", backupLocationPath)

	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	t.Run("applicationBackupRestoreTest", applicationBackupRestoreTest)
}

func doDashboardSetup() {
	aetosLog = aetosLogger.GetLogInstance()
	Dash = aetosutils.Get()
	tpDash = aetosutil.Get()
	Dash.TestLog = aetosLog
	log.InfoD("Getting dashboard utils in dosetup")
	aetosLog.Out = io.MultiWriter(aetosLog.Out)
	logLevel := os.Getenv("LOG_LEVEL")
	aetosLogger.SetLoglevel(aetosLog, logLevel)
	logDir := os.Getenv("LOG_DIR")
	testLogPath := aetosLogger.GetLogDir(logDir)
	fmt.Printf("testLogPath is %s //n", testLogPath)
	testLogFile = aetosLogger.CreateLogFile(testLogPath)
	if testLogFile != nil {
		aetosLogger.SetStorkTestFileOutput(aetosLog, testLogFile)
	}
	if enableDashboard && !aetosLogger.IsDashboardReachable() {
		enableDashboard = false
		aetosLogger.Warn("Aetos Dashboard is not reachable. Disabling dashboard reporting.")
	}
	Dash.IsEnabled = enableDashboard
	tagsProvided := make(map[string]string)
	aetosUser := os.Getenv("TEST_USER")
	testProduct := os.Getenv("PRODUCT")
	testDescription := os.Getenv("TEST_DESCRIPTION")
	testBranch := os.Getenv("TEST_BRANCH")
	testType := os.Getenv("TEST_TYPE")
	testTags := os.Getenv("TEST_TAG")
	testSetId, err := strconv.Atoi(os.Getenv("TESTSET_ID"))
	if err != nil {
		testSetId = 0
	}
	tagsProvided["stork"] = "true"
	if testTags != "" {
		tags := strings.Split(testTags, ",")
		for _, tag := range tags {
			var key, val string
			if !strings.Contains(tag, ":") {
				log.InfoD("Invalid tag %s. Please provide tag in key:value format skipping provided tag", tag)
			} else {
				key = strings.SplitN(tag, ":", 2)[0]
				val = strings.SplitN(tag, ":", 2)[1]
				tagsProvided[key] = val
			}
		}
	}
	if testSetId != 0 {
		Dash.TestSetID = testSetId
	}

	testSet := aetosutils.TestSet{
		User:        aetosUser,
		Product:     testProduct,
		Description: testDescription,
		Branch:      testBranch,
		TestType:    testType,
		Tags:        tagsProvided,
		Status:      aetosutils.NOTSTARTED,
	}

	Dash.TestSet = &testSet
}

// TODO: Take driver name from input
// TODO: Parse storageclass specs based on driver name
func setup() error {
	var err error
	Dash.TestSetBegin(Dash.TestSet)
	tags := make(map[string]string)
	defer Dash.TestCaseEnd()
	Dash.TestCaseBegin("SetupTest", "Setup for stork-test", "", tags)
	log.Info("Setup test")
	externalTest, err = strconv.ParseBool(os.Getenv(externalTestCluster))
	if err == nil {
		log.InfoD("Three cluster config mode has been activated for test: %t", externalTest)
	}

	err = setSourceKubeConfig()
	if err != nil {
		return fmt.Errorf("setting kubeconfig to source failed in setup: %v", err)
	}

	log.InfoD("Using stork volume driver: %s", volumeDriverName)
	provisioner := os.Getenv(storageProvisioner)
	backupLocationPath = addTimestampSuffix(os.Getenv(backupPathVar))

	if storkVolumeDriver, err = storkdriver.Get(volumeDriverName); err != nil {
		return fmt.Errorf("Error getting stork volume driver %s: %v", volumeDriverName, err)
	}

	if err = storkVolumeDriver.Init(nil); err != nil {
		return fmt.Errorf("Error initializing stork volume driver %v: %v", volumeDriverName, err)
	}

	if nodeDriver, err = node.Get(nodeDriverName); err != nil {
		return fmt.Errorf("Error getting node driver %v: %v", nodeDriverName, err)
	}

	log.InfoD("Initializing node driver")
	if err = nodeDriver.Init(node.InitOptions{}); err != nil {
		return fmt.Errorf("Error initializing node driver %v: %v", nodeDriverName, err)
	}

	if schedulerDriver, err = scheduler.Get(schedulerDriverName); err != nil {
		return fmt.Errorf("Error getting scheduler driver %v: %v", schedulerDriverName, err)
	}

	if volumeDriver, err = volume.Get(volumeDriverName); err != nil {
		return fmt.Errorf("Error getting volume driver %v: %v", volumeDriverName, err)
	}

	cloudDeletionValidate, err = strconv.ParseBool(os.Getenv(cloudDeletionValidation))
	if err == nil {
		log.InfoD("cloud deletion validation flag has been set to: %t", cloudDeletionValidate)
	}
	if objectStoreDriver, err = objectstore.Get(); err != nil {
		return fmt.Errorf("Error getting volume driver %v: %v", volumeDriverName, err)
	}

	authTokenConfigMap = os.Getenv(authSecretConfigMap)
	if authTokenConfigMap != "" {
		if authToken, err = schedulerDriver.GetTokenFromConfigMap(authTokenConfigMap); err != nil {
			return fmt.Errorf("Failed to get config map for token when running on an auth-enabled cluster %v", err)
		}
		log.InfoD("Auth token used for initializing scheduler/volume driver: %s ", authToken)

	}
	log.InfoD("Using provisioner: %s", provisioner)
	var customAppConfig map[string]scheduler.AppConfig
	// For non-PX backups use default-scheduler in apps instead of stork
	if provisioner != "pxd" {
		schedulerName = defaultSchedulerName
	}
	if err = schedulerDriver.Init(scheduler.InitOptions{SpecDir: "specs",
		VolDriverName:       volumeDriverName,
		NodeDriverName:      nodeDriverName,
		SecretConfigMapName: authTokenConfigMap,
		CustomAppConfig:     customAppConfig,
	}); err != nil {
		return fmt.Errorf("Error initializing scheduler driver %v: %v", schedulerDriverName, err)
	}

	if err = volumeDriver.Init(schedulerDriverName, nodeDriverName, authToken, provisioner, genericCsiConfigMap); err != nil {
		return fmt.Errorf("Error initializing volume driver %v: %v", volumeDriverName, err)
	}
	cm, err := core.Instance().GetConfigMap(cmName, defaultAdminNamespace)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("Unable to get stork version configmap: %v", err)
	}
	if cm != nil {
		ver, ok := cm.Data["version"]
		if !ok {
			return fmt.Errorf("stork version not found in configmap: %s", cmName)
		}
		storkVersion = getStorkVersion(ver)
		if storkVersionCheck == true {
			if getStorkVersion(ver) != getStorkVersion(version.Version) {
				return fmt.Errorf("stork version mismatch, found: %s, expected: %s", getStorkVersion(ver), getStorkVersion(version.Version))
			}
		}
	}

	isInternalLBAws, err = strconv.ParseBool(os.Getenv(internalLBAws))
	if err == nil {
		log.InfoD("Internal AWS Load Balancer is being used: %t", isInternalLBAws)
	}
	// On source cluster, change PX service to type LoadBalancer if it is an EKS/AKS/GKE cluster
	pxNamespace = os.Getenv(portworxNamespace)
	log.InfoD("PX has been deployed in namespace: %s", pxNamespace)
	if volumeDriverName == storkdriver.PortworxDriverName && IsCloud() {
		if err = changePxServiceToLoadBalancer(isInternalLBAws); err != nil {
			return fmt.Errorf("failed to change PX service to LoadBalancer on source cluster: %v", err)
		}
	}
	err = setDestinationKubeConfig()
	if err != nil {
		return fmt.Errorf("while PX service to LoadBalancer, setting kubeconfig to destination failed %v", err)
	}

	err = addTestModeEnvironmentVar()
	if err != nil {
		return fmt.Errorf("TEST_MODE environment variable not set for stork in destination cluster: %v", err)
	}

	// On destination cluster, change PX service to type LoadBalancer if it is an EKS/AKS/GKE cluster
	if volumeDriverName == storkdriver.PortworxDriverName && IsCloud() {
		if err = changePxServiceToLoadBalancer(isInternalLBAws); err != nil {
			return fmt.Errorf("failed to change PX service to LoadBalancer on destination cluster: %v", err)
		}
	}

	err = setSourceKubeConfig()
	if err != nil {
		return fmt.Errorf("at the end of setup, setting kubeconfig to source failed in setup: %v", err)
	}

	err = addTestModeEnvironmentVar()
	if err != nil {
		return fmt.Errorf("TEST_MODE environment variable not set for stork in source cluster: %v", err)
	}
	SetupTestRail()

	return nil
}

func generateInstanceID(t *testing.T, testName string) string {
	id, err := uuid.New()
	log.FailOnError(t, err, "Error generating uuid for task")
	return testName + "-" + id.String()
}

func destroyAndWait(t *testing.T, ctxs []*scheduler.Context) {
	for _, ctx := range ctxs {
		err := schedulerDriver.Destroy(ctx, nil)
		log.FailOnError(t, err, "Error destroying ctx: %+v", ctx)
	}
	for _, ctx := range ctxs {
		err := schedulerDriver.WaitForDestroy(ctx, defaultWaitTimeout)
		log.FailOnError(t, err, "Error waiting for destroy of ctx: %+v", ctx)
		_, err = schedulerDriver.DeleteVolumes(ctx, nil)
		log.FailOnError(t, err, "Error deleting volumes in ctx: %+v", ctx)
	}
}

func getVolumeNames(t *testing.T, ctx *scheduler.Context) []string {
	volumeParams, err := schedulerDriver.GetVolumeParameters(ctx)
	log.FailOnError(t, err, "Error getting volume Parameters")

	var volumes []string
	for vol := range volumeParams {
		volumes = append(volumes, vol)
	}
	return volumes
}

func verifyScheduledNode(t *testing.T, appNode node.Node, volumes []string) {
	if externalTest {
		// TODO: Figure out a way to communicate with PX nodes from other cluster
		return
	}
	if schedulerName == defaultSchedulerName {
		return
	}

	driverNodes, err := storkVolumeDriver.GetNodes()
	log.FailOnError(t, err, "Error getting nodes from stork driver")

	found := false
	for _, dNode := range driverNodes {
		if dNode.Hostname == appNode.Name {
			found = true
			break
		}
		for _, address := range appNode.Addresses {
			for _, ip := range dNode.IPs {
				if ip == address {
					dNode.Hostname = appNode.Name
					found = true
					break
				}
			}
			if found {
				break
			}
		}
	}
	Dash.VerifyFatal(t, found, true, fmt.Sprintf("Scheduled node not found in driver node list. DriverNodes: %v ScheduledNode: %v", driverNodes, appNode))

	scores := getScoringBasedOnHyperconvergence(t, driverNodes, volumes)

	highScore := 0
	for _, score := range scores {
		if score > highScore {
			highScore = score
		}
	}

	log.InfoD("Scores: %v", scores)
	Dash.VerifyFatal(t, highScore, scores[appNode.Name], "Scheduled node has the highest score")
}

// Helper function to get scoring of driverNodes based on hyper-convergence
func getScoringBasedOnHyperconvergence(t *testing.T, driverNodes []*storkdriver.NodeInfo, volumes []string) map[string]int {
	scores := make(map[string]int)
	idMap := make(map[string]*storkdriver.NodeInfo)
	rackMap := make(map[string][]string)
	zoneMap := make(map[string][]string)
	regionMap := make(map[string][]string)
	for _, dNode := range driverNodes {
		scores[dNode.Hostname] = 0
		idMap[dNode.StorageID] = dNode
		if dNode.Status == storkdriver.NodeOnline {
			if dNode.Rack != "" {
				rackMap[dNode.Rack] = append(rackMap[dNode.Rack], dNode.Hostname)
			}
			if dNode.Zone != "" {
				zoneMap[dNode.Zone] = append(rackMap[dNode.Zone], dNode.Hostname)
			}
			if dNode.Region != "" {
				regionMap[dNode.Region] = append(rackMap[dNode.Region], dNode.Hostname)
			}
		}
	}

	// Calculate scores for each node
	for _, vol := range volumes {
		volInfo, err := storkVolumeDriver.InspectVolume(vol)
		log.FailOnError(t, err, "Error inspecting volume %v", vol)

		for _, dataNode := range volInfo.DataNodes {
			hostname := idMap[dataNode].Hostname
			scores[hostname] += nodeScore

			if idMap[dataNode].Rack != "" {
				for _, node := range rackMap[idMap[dataNode].Rack] {
					if dataNode != node {
						scores[node] += rackScore
					}
				}
			}
			if idMap[dataNode].Zone != "" {
				for _, node := range zoneMap[idMap[dataNode].Zone] {
					if dataNode != node {
						scores[node] += zoneScore
					}
				}
			}
			if idMap[dataNode].Rack != "" {
				for _, node := range regionMap[idMap[dataNode].Region] {
					if dataNode != node {
						scores[node] += regionScore
					}
				}
			}
		}
	}
	return scores
}

// Verify pods are scheduled on appropriate nodes based on hyperconvergence scoring
// This method assumes we have pod AntiAffinities set in the deployment to deploy each replica on a different node.
func verifyScheduledNodesMultipleReplicas(t *testing.T, appNodes []node.Node, volumes []string) {
	driverNodes, err := storkVolumeDriver.GetNodes()
	log.FailOnError(t, err, "Error getting nodes from stork driver")

	//The scores are stored in a map where the keys are node names, and the values are the corresponding scores.
	scores := getScoringBasedOnHyperconvergence(t, driverNodes, volumes)
	replicaCount := len(appNodes)

	// The nodes are sorted based on their scores, from highest to lowest.
	// From the nodeScores slice we want to select the top nodes with the highest scores.
	// The actual nodes which the pods get scheduled on maybe different in case of multiple nodes having the same scores.
	// However, the scheduled nodes should have the same scores as the top n highest scores where n is number of replicas.
	nodeScores := descendingSortBasedOnValue(scores)
	expectedNodeScores := nodeScores[0:replicaCount]
	log.InfoD("Expected nodescores are %v", expectedNodeScores)

	//appNodeScores is a map of nodeName:score, in which we store the calculated scores of the set of scheduled nodes.
	appNodeScores := make(map[string]int)
	for _, appNode := range appNodes {
		appNodeScores[appNode.Name] = scores[appNode.Name]
	}
	actualNodeScores := descendingSortBasedOnValue(appNodeScores)

	//since we have sorted both the expectedNodeScores and actualNodeScores
	//for scheduling to be correct these two arrays should be equal
	Dash.VerifyFatal(t, len(expectedNodeScores) == len(actualNodeScores), true, "Scheduled nodes count does not match the number of replicas")
}

// helper function to sort a map of nodeScores in descending order of scores
func descendingSortBasedOnValue(scores map[string]int) []int {
	var nodeScores []int

	for _, value := range scores {
		nodeScores = append(nodeScores, value)
	}

	// Sort the slice in descending order
	sort.Sort(sort.Reverse(sort.IntSlice(nodeScores)))
	return nodeScores
}

// write kubbeconfig file to /tmp/kubeconfig
func dumpRemoteKubeConfig(configObject string) error {
	cm, err := core.Instance().GetConfigMap(configObject, "kube-system")
	if err != nil {
		log.Error("Error reading config map: %v", err)
		return err
	}
	config := cm.Data["kubeconfig"]
	if len(config) == 0 {
		configErr := "Error reading kubeconfig: found empty remoteConfig in config map"
		return fmt.Errorf(configErr)
	}
	// dump to remoteFilePath
	return os.WriteFile(remoteFilePath, []byte(config), 0644)
}

func dumpKubeConfigPath(configObject string, path string) error {
	cm, err := core.Instance().GetConfigMap(configObject, "kube-system")
	if err != nil {
		log.Error("Error reading config map: %v", err)
		return err
	}
	config := cm.Data["kubeconfig"]
	if len(config) == 0 {
		configErr := "Error reading kubeconfig: found empty remoteConfig in config map"
		return fmt.Errorf(configErr)
	}
	// dump to remoteFilePath
	return os.WriteFile(path, []byte(config), 0644)
}

func setRemoteConfig(kubeConfig string) error {

	var config *rest.Config
	var err error

	if kubeConfig == "" {
		config = nil
	} else {
		config, err = clientcmd.BuildConfigFromFlags("", kubeConfig)
		if err != nil {
			return err
		}
	}

	k8sOps := core.Instance()
	k8sOps.SetConfig(config)

	storkOps := storkops.Instance()
	storkOps.SetConfig(config)

	appsOps := apps.Instance()
	appsOps.SetConfig(config)

	storageOps := storage.Instance()
	storageOps.SetConfig(config)

	dynamicOps := dynamic.Instance()
	dynamicOps.SetConfig(config)

	extOps := externalstorage.Instance()
	extOps.SetConfig(config)

	ocpOps := openshift.Instance()
	ocpOps.SetConfig(config)

	rbacOps := rbac.Instance()
	rbacOps.SetConfig(config)

	operatorOps := operator.Instance()
	operatorOps.SetConfig(config)

	k8sBatchOps := batch.Instance()
	k8sBatchOps.SetConfig(config)

	return nil
}

func setKubeConfig(config string) error {
	// setting kubeconfig to default cluster first since all configmaps are created there
	err := setRemoteConfig("")
	if err != nil {
		return fmt.Errorf("setting kubeconfig to default failed: %v", err)
	}

	err = dumpRemoteKubeConfig(config)
	if err != nil {
		return fmt.Errorf("unable to dump config %v to remoteFilePath: %v", config, err)
	}

	err = setRemoteConfig(remoteFilePath)
	if err != nil {
		return fmt.Errorf("unable to set config to %v: %v", config, err)
	}

	if schedulerDriver != nil {
		err = schedulerDriver.RefreshNodeRegistry()
		if err != nil {
			return fmt.Errorf(
				"unable to refresh node registry after setting config to %v: %v", config, err)
		}

		err = volumeDriver.RefreshDriverEndpoints()
		if err != nil {
			// return fmt.Errorf(
			log.Error(
				"unable to refresh driver endpoints after setting config to %v: %v", config, err)
		}
	}
	return nil
}

func setSourceKubeConfig() error {
	log.Info("Set kubeConfig to Source")
	return setKubeConfig(srcConfig)
}

func setDestinationKubeConfig() error {
	log.Info("Set kubeConfig to Destination")
	return setKubeConfig(destConfig)
}

func createClusterPair(pairInfo map[string]string, skipStorage, resetConfig bool, clusterPairDir, projectIDMappings string) error {
	err := os.MkdirAll(path.Join(specDir, clusterPairDir), 0777)
	if err != nil {
		log.Error("Unable to make directory (%v) for cluster pair spec: %v", specDir+"/"+clusterPairDir, err)
		return err
	}
	clusterPairFileName := path.Join(specDir, clusterPairDir, pairFileName)
	pairFile, err := os.Create(clusterPairFileName)
	if err != nil {
		log.Error("Unable to create clusterPair.yaml: %v", err)
		return err
	}
	defer func() {
		err := pairFile.Close()
		if err != nil {
			log.Error("Error closing pair file: %v", err)
		}
	}()

	factory := storkctl.NewFactory()
	cmd := storkctl.NewCommand(factory, os.Stdin, pairFile, os.Stderr)

	if !skipStorage {
		var storageOptionsStr string
		if len(pairInfo) > 0 {
			for k, v := range pairInfo {
				if len(storageOptionsStr) > 0 {
					storageOptionsStr = storageOptionsStr + "," + k + "=" + v
				} else {
					storageOptionsStr = k + "=" + v
				}
			}
		}
		cmd.SetArgs([]string{"generate", "clusterpair", remotePairName, "--kubeconfig", remoteFilePath, "--project-mappings", projectIDMappings, "--storageoptions", storageOptionsStr})
	} else {
		cmd.SetArgs([]string{"generate", "clusterpair", remotePairName, "--kubeconfig", remoteFilePath, "--project-mappings", projectIDMappings})
	}
	if err := cmd.Execute(); err != nil {
		log.Error("Execute storkctl failed: %v", err)
		return err
	}

	if resetConfig {
		// storkctl generate command sets sched-ops to source cluster config
		err = setSourceKubeConfig()
		if err != nil {
			log.Error("during cluster pair setting kubeconfig to source failed %v", err)
			return err
		}
	} else {
		// Change kubeconfig to destination cluster config
		err = setDestinationKubeConfig()
		if err != nil {
			log.Error("during cluster pair setting kubeconfig to destination failed %v", err)
			return err
		}
	}

	log.Info("cluster-pair.yml created")
	return nil

}

func scheduleClusterPair(ctx *scheduler.Context, skipStorage, resetConfig bool, clusterPairDir, projectIDMappings string, reverse bool) error {
	var token string
	var err error

	if reverse {
		// For auth-enabled clusters first get admin token for destination cluster
		err := setSourceKubeConfig()
		if err != nil {
			return fmt.Errorf("during cluster pair setting kubeconfig to source failed %v", err)
		}
	} else {
		err := setDestinationKubeConfig()
		if err != nil {
			return fmt.Errorf("during cluster pair setting kubeconfig to destination failed %v", err)
		}
	}

	// For auth-enabled clusters, get token for the current cluster, which will be used to generate cluster pair
	if authTokenConfigMap != "" {
		token, err = getTokenFromSecret(adminTokenSecretName, defaultAdminNamespace)
		if err != nil {
			return err
		}
	}

	info, err := volumeDriver.GetClusterPairingInfo(remoteFilePath, token, IsCloud(), reverse)
	if err != nil {
		log.Error("Error writing to clusterpair.yml: %v", err)
		return err
	}

	err = createClusterPair(info, skipStorage, resetConfig, clusterPairDir, projectIDMappings)
	if err != nil {
		log.Error("Error creating cluster Spec: %v", err)
		return err
	}

	err = schedulerDriver.RescanSpecs(specDir, volumeDriverName)
	if err != nil {
		log.Error("Unable to parse spec dir: %v", err)
		return err
	}

	err = schedulerDriver.AddTasks(ctx,
		scheduler.ScheduleOptions{AppKeys: []string{clusterPairDir}})
	if err != nil {
		log.Error("Failed to schedule Cluster Pair Specs: %v", err)
		return err
	}

	err = schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
	if err != nil {
		log.Error("Error waiting to get cluster pair in ready state: %v", err)
		return err
	}

	return nil
}

// Create a cluster pair from source to destination and another cluster pair from destination to source
func scheduleBidirectionalClusterPair(cpName, cpNamespace, projectMappings string, objectStoreType storkv1.BackupLocationType, secretName string, isMetroDR bool) error {
	//var token string
	// Setting kubeconfig to source because we will create bidirectional cluster pair based on source as reference
	err := setSourceKubeConfig()
	if err != nil {
		return fmt.Errorf("during cluster pair setting kubeconfig to source failed %v", err)
	}

	// Create namespace for the cluster pair on source cluster
	_, err = core.Instance().CreateNamespace(&v1.Namespace{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: cpNamespace,
			Labels: map[string]string{
				"creator": "stork-test",
			},
		},
	})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("Failed to create namespace %s on source cluster", cpNamespace)
	}

	// Create directory to store kubeconfig files
	err = os.MkdirAll(path.Join(tempDir, bidirectionalClusterPairDir), 0777)
	if err != nil {
		log.Error("Unable to make directory (%v) for cluster pair spec: %v", tempDir+"/"+bidirectionalClusterPairDir, err)
		return err
	}
	srcKubeconfigPath := path.Join(tempDir, bidirectionalClusterPairDir, "src_kubeconfig")
	srcKubeConfig, err := os.Create(srcKubeconfigPath)
	if err != nil {
		log.Error("Unable to write source kubeconfig file: %v", err)
		return err
	}

	defer func() {
		err := srcKubeConfig.Close()
		if err != nil {
			log.Error("Error closing source kubeconfig file: %v", err)
		}
	}()

	// Dump source config to the directory created before
	err = dumpKubeConfigPath(srcConfig, srcKubeconfigPath)
	if err != nil {
		return fmt.Errorf("unable to dump remote config while setting source config: %v", err)
	}

	destKubeconfigPath := path.Join(tempDir, bidirectionalClusterPairDir, "dest_kubeconfig")
	destKubeConfig, err := os.Create(destKubeconfigPath)
	if err != nil {
		log.Error("Unable to write source kubeconfig file: %v", err)
		return err
	}

	defer func() {
		err := destKubeConfig.Close()
		if err != nil {
			log.Error("Error closing destination kubeconfig file: %v", err)
		}
	}()

	// Dump destination config to the directory created before
	err = dumpKubeConfigPath(destConfig, destKubeconfigPath)
	if err != nil {
		return fmt.Errorf("unable to dump remote config while setting destination config: %v", err)
	}

	err = setDestinationKubeConfig()
	if err != nil {
		return fmt.Errorf("during cluster pair setting kubeconfig to source failed %v", err)
	}

	// Create namespace for the cluster pair on destination cluster
	_, err = core.Instance().CreateNamespace(&v1.Namespace{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: cpNamespace,
			Labels: map[string]string{
				"creator": "stork-test",
			},
		},
	})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("Failed to create namespace %s on destination cluster", cpNamespace)
	}

	err = setSourceKubeConfig()
	if err != nil {
		return fmt.Errorf("during cluster pair setting kubeconfig to source failed %v", err)
	}

	// Create source --> destination and destination --> cluster pairs using storkctl
	factory := storkctl.NewFactory()
	cmd := storkctl.NewCommand(factory, os.Stdin, os.Stdout, os.Stderr)
	cmdArgs := []string{"create", "clusterpair", "-n", cpNamespace, cpName,
		"--src-kube-file", srcKubeconfigPath,
		"--dest-kube-file", destKubeconfigPath,
	}

	// Append the sync-dr mode arg in case of metro DR.
	if isMetroDR {
		cmdArgs = append(cmdArgs, "--mode", "sync-dr")
	}

	if projectMappings != "" {
		cmdArgs = append(cmdArgs, "--project-mappings")
		cmdArgs = append(cmdArgs, projectMappings)
	}

	// Get external object store details and append to the command accordingily
	objectStoreArgs := make([]string, 0)
	if !isMetroDR {
		objectStoreArgs, err = getObjectStoreArgs(objectStoreType, secretName)
		if err != nil {
			return fmt.Errorf("failed to get  %s secret in configmap secret-config in default namespace", objectStoreType)
		}
	}

	cmdArgs = append(cmdArgs, objectStoreArgs...)
	cmd.SetArgs(cmdArgs)
	log.InfoD("Following is the bidirectional command: %v", cmdArgs)
	if err := cmd.Execute(); err != nil {
		return fmt.Errorf("Creation of bidirectional cluster pair using storkctl failed: %v", err)
	}
	return nil
}

// Create a cluster pair from source to destination using unidirectional flag
func scheduleUnidirectionalClusterPair(cpName, cpNamespace, projectMappings string, objectStoreType storkv1.BackupLocationType, secretName string, resetConfig bool, reverse bool) error {
	//var token string
	// Setting kubeconfig to source because we will create unidirectional cluster pair based on source as reference
	err := setSourceKubeConfig()
	if err != nil {
		return fmt.Errorf("during cluster pair setting kubeconfig to source failed %v", err)
	}

	// Create namespace for the cluster pair on source cluster
	_, err = core.Instance().CreateNamespace(&v1.Namespace{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: cpNamespace,
			Labels: map[string]string{
				"creator": "stork-test",
			},
		},
	})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("Failed to create namespace %s on source cluster", cpNamespace)
	}

	// Create directory to store kubeconfig files
	err = os.MkdirAll(path.Join(tempDir, unidirectionalClusterPairDir), 0777)
	if err != nil {
		log.Error("Unable to make directory (%v) for cluster pair spec: %v", tempDir+"/"+unidirectionalClusterPairDir, err)
		return err
	}
	srcKubeconfigPath := path.Join(tempDir, unidirectionalClusterPairDir, "src_kubeconfig")
	srcKubeConfig, err := os.Create(srcKubeconfigPath)
	if err != nil {
		log.Error("Unable to write source kubeconfig file: %v", err)
		return err
	}

	defer func() {
		err := srcKubeConfig.Close()
		if err != nil {
			log.Error("Error closing source kubeconfig file: %v", err)
		}
	}()

	// Dump source config to the directory created before
	err = dumpKubeConfigPath(srcConfig, srcKubeconfigPath)
	if err != nil {
		return fmt.Errorf("unable to dump remote config while setting source config: %v", err)
	}

	destKubeconfigPath := path.Join(tempDir, unidirectionalClusterPairDir, "dest_kubeconfig")
	destKubeConfig, err := os.Create(destKubeconfigPath)
	if err != nil {
		log.Error("Unable to write destination kubeconfig file: %v", err)
		return err
	}

	defer func() {
		err := destKubeConfig.Close()
		if err != nil {
			log.Error("Error closing destination kubeconfig file: %v", err)
		}
	}()

	// Dump destination config to the directory created before
	err = dumpKubeConfigPath(destConfig, destKubeconfigPath)
	if err != nil {
		return fmt.Errorf("unable to dump remote config while setting destination config: %v", err)
	}

	err = setDestinationKubeConfig()
	if err != nil {
		return fmt.Errorf("during cluster pair setting kubeconfig to destination failed %v", err)
	}

	// Create namespace for the cluster pair on destination cluster
	_, err = core.Instance().CreateNamespace(&v1.Namespace{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: cpNamespace,
			Labels: map[string]string{
				"creator": "stork-test",
			},
		},
	})
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("Failed to create namespace %s on destination cluster", cpNamespace)
	}

	err = setSourceKubeConfig()
	if err != nil {
		return fmt.Errorf("during cluster pair setting kubeconfig to source failed %v", err)
	}

	// Create source --> destination and destination --> cluster pairs using storkctl
	factory := storkctl.NewFactory()
	cmd := storkctl.NewCommand(factory, os.Stdin, os.Stdout, os.Stderr)
	cmdArgs := []string{"create", "clusterpair", "-n", cpNamespace, cpName,
		"--src-kube-file", srcKubeconfigPath,
		"--dest-kube-file", destKubeconfigPath,
		"--unidirectional",
	}
	if reverse {
		cmdArgs = []string{"create", "clusterpair", "-n", cpNamespace, cpName,
			"--src-kube-file", destKubeconfigPath,
			"--dest-kube-file", srcKubeconfigPath,
			"--unidirectional",
		}
	}

	if projectMappings != "" {
		cmdArgs = append(cmdArgs, "--project-mappings")
		cmdArgs = append(cmdArgs, projectMappings)
	}

	// Get external object store details and append to the command accordingily
	objectStoreArgs, err := getObjectStoreArgs(objectStoreType, secretName)
	if err != nil {
		return fmt.Errorf("failed to get  %s secret in configmap secret-config in default namespace", objectStoreType)
	}

	cmdArgs = append(cmdArgs, objectStoreArgs...)
	cmd.SetArgs(cmdArgs)
	log.InfoD("Following is the unidirectional command: %v", cmdArgs)
	if err := cmd.Execute(); err != nil {
		return fmt.Errorf("Creation of unidirectional cluster pair using storkctl failed: %v", err)
	}

	if resetConfig {
		err = setSourceKubeConfig()
		if err != nil {
			log.Error("during cluster pair setting kubeconfig to source failed %v", err)
			return err
		}
	} else {
		err = setDestinationKubeConfig()
		if err != nil {
			log.Error("during cluster pair setting kubeconfig to destination failed %v", err)
			return err
		}
	}

	return nil
}

func activateAppUsingStorkctl(namespace string, runInSource bool) error {
	factory := storkctl.NewFactory()
	cmd := storkctl.NewCommand(factory, os.Stdin, os.Stdout, os.Stderr)
	cmdArgs := []string{"activate", "migrations", "-n", namespace}
	err := setSourceKubeConfig()
	if err != nil {
		return fmt.Errorf("setting kubeconfig to source failed during activate migrations %v", err)
	}
	if runInSource {
		srcKubeconfigPath := path.Join(tempDir, "src_kubeconfig")
		srcKubeConfig, err := os.Create(srcKubeconfigPath)
		if err != nil {
			log.Error("Unable to write source kubeconfig file: %v", err)
			return err
		}

		defer func() {
			err := srcKubeConfig.Close()
			if err != nil {
				log.Error("Error closing source kubeconfig file: %v", err)
			}
		}()
		err = dumpKubeConfigPath(srcConfig, srcKubeconfigPath)
		if err != nil {
			return fmt.Errorf("unable to dump remote config while setting source config during activate migrations: %v", err)
		}
		cmdArgs = append(cmdArgs, []string{"--kubeconfig", srcKubeconfigPath}...)
	} else {
		destKubeconfigPath := path.Join(tempDir, "dest_kubeconfig")
		destKubeConfig, err := os.Create(destKubeconfigPath)
		if err != nil {
			log.Error("Unable to write destination kubeconfig file: %v", err)
			return err
		}

		defer func() {
			err := destKubeConfig.Close()
			if err != nil {
				log.Error("Error closing destination kubeconfig file: %v", err)
			}
		}()

		err = dumpKubeConfigPath(destConfig, destKubeconfigPath)
		if err != nil {
			return fmt.Errorf("unable to dump remote config while setting destination config during activate migrations: %v", err)
		}
		cmdArgs = append(cmdArgs, []string{"--kubeconfig", destKubeconfigPath}...)
	}

	cmd.SetArgs(cmdArgs)
	log.InfoD("Activating apps in namespace with command: %v", cmdArgs)
	if err := cmd.Execute(); err != nil {
		return fmt.Errorf("activating apps using storkctl failed: %v", err)
	}
	return nil
}

func deactivateAppUsingStorkctl(namespace string, runInSource bool) error {
	factory := storkctl.NewFactory()
	cmd := storkctl.NewCommand(factory, os.Stdin, os.Stdout, os.Stderr)
	cmdArgs := []string{"deactivate", "migrations", "-n", namespace}

	err := setSourceKubeConfig()
	if runInSource {
		if err != nil {
			return fmt.Errorf("setting kubeconfig to source failed during deactivate migrations %v", err)
		}
		srcKubeconfigPath := path.Join(tempDir, "src_kubeconfig")
		srcKubeConfig, err := os.Create(srcKubeconfigPath)
		if err != nil {
			log.Error("Unable to write source kubeconfig file: %v", err)
			return err
		}

		defer func() {
			err := srcKubeConfig.Close()
			if err != nil {
				log.Error("Error closing source kubeconfig file: %v", err)
			}
		}()
		err = dumpKubeConfigPath(srcConfig, srcKubeconfigPath)
		if err != nil {
			return fmt.Errorf("unable to dump remote config while setting source config during deactivate migrations: %v", err)
		}
		cmdArgs = append(cmdArgs, []string{"--kubeconfig", srcKubeconfigPath}...)
	} else {
		if err != nil {
			return fmt.Errorf("setting kubeconfig to destination failed during deactivate migrations %v", err)
		}
		destKubeconfigPath := path.Join(tempDir, "dest_kubeconfig")
		destKubeConfig, err := os.Create(destKubeconfigPath)
		if err != nil {
			log.Error("Unable to write destination kubeconfig file: %v", err)
			return err
		}

		defer func() {
			err := destKubeConfig.Close()
			if err != nil {
				log.Error("Error closing destination kubeconfig file: %v", err)
			}
		}()
		err = dumpKubeConfigPath(destConfig, destKubeconfigPath)
		if err != nil {
			return fmt.Errorf("unable to dump remote config while setting destination config during deactivate migrations: %v", err)
		}
		cmdArgs = append(cmdArgs, []string{"--kubeconfig", destKubeconfigPath}...)
	}

	cmd.SetArgs(cmdArgs)
	log.InfoD("Deactivating apps in namespace with command: %v", cmdArgs)
	if err := cmd.Execute(); err != nil {
		return fmt.Errorf("deactivating apps using storkctl failed: %v", err)
	}
	return nil
}

func getObjectStoreArgs(objectStoreType storkv1.BackupLocationType, secretName string) ([]string, error) {
	var objectStoreArgs []string
	secretData, err := core.Instance().GetSecret(secretName, "default")
	if err != nil {
		return objectStoreArgs, fmt.Errorf("error getting secret %s in default namespace: %v", secretName, err)
	}
	if objectStoreType == storkv1.BackupLocationS3 {
		objectStoreArgs = append(objectStoreArgs,
			[]string{"--provider", "s3",
				"--s3-access-key", string(secretData.Data["accessKeyID"]),
				"--s3-secret-key", string(secretData.Data["secretAccessKey"]),
				"--s3-region", string(secretData.Data["region"]),
				"--s3-endpoint", string(secretData.Data["endpoint"]),
			}...)
		if val, ok := secretData.Data["disableSSL"]; ok && string(val) == "true" {
			objectStoreArgs = append(objectStoreArgs, "--disable-ssl")
		}
		if val, ok := secretData.Data["encryptionKey"]; ok && len(val) > 0 {
			objectStoreArgs = append(objectStoreArgs, "--encryption-key")
			objectStoreArgs = append(objectStoreArgs, string(val))
		}
	} else if objectStoreType == storkv1.BackupLocationAzure {
		objectStoreArgs = append(objectStoreArgs,
			[]string{"--provider", "azure", "--azure-account-name", string(secretData.Data["storageAccountName"]),
				"--azure-account-key", string(secretData.Data["storageAccountKey"])}...)
		if val, ok := secretData.Data["encryptionKey"]; ok && len(val) > 0 {
			objectStoreArgs = append(objectStoreArgs, "--encryption-key")
			objectStoreArgs = append(objectStoreArgs, string(val))
		}
	} else if objectStoreType == storkv1.BackupLocationGoogle {
		objectStoreArgs = append(objectStoreArgs,
			[]string{"--provider", "google", "--google-project-id", string(secretData.Data["projectID"]), "--google-key-file-path", string(secretData.Data["accountKey"])}...)
		if val, ok := secretData.Data["encryptionKey"]; ok && len(val) > 0 {
			objectStoreArgs = append(objectStoreArgs, "--encryption-key")
			objectStoreArgs = append(objectStoreArgs, string(val))
		}
	}

	return objectStoreArgs, nil
}

func setMockTime(t *time.Time) error {
	timeString := ""
	if t != nil {
		timeString = t.Format(time.RFC1123)
	}

	cm, err := core.Instance().GetConfigMap(schedule.MockTimeConfigMapName, schedule.MockTimeConfigMapNamespace)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}

		// create new config map
		data := map[string]string{
			schedule.MockTimeConfigMapKey: timeString,
		}

		cm := &v1.ConfigMap{
			ObjectMeta: meta_v1.ObjectMeta{
				Name:      schedule.MockTimeConfigMapName,
				Namespace: schedule.MockTimeConfigMapNamespace,
			},
			Data: data,
		}
		_, err = core.Instance().CreateConfigMap(cm)
		return err
	}

	// update existing config map
	cmCopy := cm.DeepCopy()
	if cmCopy.Data == nil {
		cmCopy.Data = make(map[string]string)
	}

	cmCopy.Data[schedule.MockTimeConfigMapKey] = timeString
	_, err = core.Instance().UpdateConfigMap(cmCopy)
	if err != nil {
		return err
	}

	time.Sleep(configMapSyncWaitTime)
	return nil
}

func createApp(t *testing.T, testID string) *scheduler.Context {

	ctxs, err := schedulerDriver.Schedule(testID,
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-1-pvc"}, Scheduler: schedulerName})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running state")

	scheduledNodes, err := schedulerDriver.GetNodesForApp(ctxs[0])
	log.FailOnError(t, err, "Error getting node for app")
	Dash.VerifyFatal(t, len(scheduledNodes), 1, "App should be scheduled on one node")

	volumeNames := getVolumeNames(t, ctxs[0])
	Dash.VerifyFatal(t, len(volumeNames), 1, "Should only have one volume")

	verifyScheduledNode(t, scheduledNodes[0], volumeNames)
	return ctxs[0]
}

func addTimestampSuffix(path string) string {
	t := time.Now()
	timeStampSuffix := t.Format("20060102150405")
	return fmt.Sprintf("%s-%s-%s", bucketPrefix, path, timeStampSuffix)
}

func addSecurityAnnotation(spec interface{}) error {
	// Adds annotations required for auth enabled runs
	configMap, err := core.Instance().GetConfigMap(authTokenConfigMap, "default")
	if err != nil {
		log.Error("Error reading config map: %v", err)
		return err
	}
	log.Debugf("Config Map details: %v", configMap.Data)
	if _, ok := configMap.Data[secretNameKey]; !ok {
		return fmt.Errorf("failed to get secret name from config map")
	}
	if _, ok := configMap.Data[secretNamespaceKey]; !ok {
		return fmt.Errorf("failed to get secret namespace from config map")
	}
	if obj, ok := spec.(*storage_v1.StorageClass); ok {
		if obj.Parameters == nil {
			obj.Parameters = make(map[string]string)
		}
		obj.Parameters[secretName] = configMap.Data[secretNameKey]
		obj.Parameters[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*v1.PersistentVolumeClaim); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*snapv1.VolumeSnapshot); ok {
		if obj.Metadata.Annotations == nil {
			obj.Metadata.Annotations = make(map[string]string)
		}
		obj.Metadata.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Metadata.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*appsapi.StatefulSet); ok {
		var pvcList []v1.PersistentVolumeClaim
		for _, pvc := range obj.Spec.VolumeClaimTemplates {
			if pvc.Annotations == nil {
				pvc.Annotations = make(map[string]string)
			}
			pvc.Annotations[secretName] = configMap.Data[secretNameKey]
			pvc.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
			pvcList = append(pvcList, pvc)
		}
		obj.Spec.VolumeClaimTemplates = pvcList
	} else if obj, ok := spec.(*storkv1.ApplicationBackup); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*storkv1.ApplicationClone); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*storkv1.ApplicationRestore); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*storkv1.Migration); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*storkv1.VolumeSnapshotRestore); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*storkv1.GroupVolumeSnapshot); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*storkv1.ApplicationBackupSchedule); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*storkv1.SchedulePolicy); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	} else if obj, ok := spec.(*storkv1.VolumeSnapshotSchedule); ok {
		if obj.Annotations == nil {
			obj.Annotations = make(map[string]string)
		}
		obj.Annotations[secretName] = configMap.Data[secretNameKey]
		obj.Annotations[secretNamespace] = configMap.Data[secretNamespaceKey]
	}
	return nil
}

func getStorkVersion(fullVersion string) string {
	noHash := strings.Split(fullVersion, "-")[0]
	majorVersion := strings.Split(noHash, ".")
	return strings.Join([]string{majorVersion[0], majorVersion[1]}, ".")
}

func getTokenFromSecret(secretName, secretNamespace string) (string, error) {
	var token string
	secret, err := core.Instance().GetSecret(secretName, secretNamespace)
	if err != nil {
		return "", fmt.Errorf("Failed to get secret %s: %v", secretName, err)
	}
	if tk, ok := secret.Data["auth-token"]; ok {
		token = string(tk)
		return token, nil
	}
	return "", fmt.Errorf("secret does not contain key 'auth-token'")
}

func createSecret(t *testing.T, secret_name string, secret_map map[string]string) *v1.Secret {
	secret := &v1.Secret{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      secret_name,
			Namespace: "kube-system",
		},
		StringData: secret_map,
	}
	secretObj, err := core.Instance().CreateSecret(secret)
	if !errors.IsAlreadyExists(err) {
		log.FailOnError(t, err, "failed to create secret for volumes")
	}
	return secretObj
}

func cleanup(t *testing.T, namespace string, storageClass string) {
	funcCleanup := func() {
		err := core.Instance().DeleteNamespace(namespace)
		if err != nil {
			log.InfoD("Error deleting namespace %s: %v\n", namespace, err)
		}
		if storageClass != "" {
			err = storage.Instance().DeleteStorageClass(storageClass)
			if err != nil {
				log.InfoD("Error deleting storage class %s: %v\n", namespace, err)
			}
		}

	}
	funcCleanup()
	executeOnDestination(t, funcCleanup)
	// time to let deletion finish
	time.Sleep(time.Second * 20)
}

func executeOnDestination(t *testing.T, funcToExecute func()) {
	err := setDestinationKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to destination cluster: %v", err)

	defer func() {
		err := setSourceKubeConfig()
		log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	}()

	funcToExecute()
}

func scheduleAppAndWait(t *testing.T, instanceIDs []string, appKey string) []*scheduler.Context {
	var ctxs []*scheduler.Context

	// creates the namespace (appKey-instanceID) and schedules the app (appKey)
	for _, instanceID := range instanceIDs {
		newCtxs, err := schedulerDriver.Schedule(
			instanceID,
			scheduler.ScheduleOptions{
				AppKeys: []string{appKey},
				Labels:  nil,
			})
		log.FailOnError(t, err, "Error scheduling task")
		Dash.VerifyFatal(t, len(newCtxs), 1, "Task started")
		ctxs = append(ctxs, newCtxs[0])
	}

	// wait for all apps to get to running state
	for _, ctx := range ctxs {
		err := schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error waiting for app to get to running state")
	}
	return ctxs
}

func addTasksAndWait(t *testing.T, ctx *scheduler.Context, appKeys []string) {
	err := schedulerDriver.AddTasks(
		ctx,
		scheduler.ScheduleOptions{
			AppKeys: appKeys,
		})
	log.FailOnError(t, err, "Error scheduling app")

	err = schedulerDriver.WaitForRunning(ctx, defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for app to get to running state")
}

func triggerMigrationMultiple(
	t *testing.T,
	ctxs []*scheduler.Context,
	migrationName string,
	namespaces []string,
	includeResources bool,
	includeVolumes bool,
	startApplications bool,
) ([]*scheduler.Context, []*scheduler.Context, []*storkv1.Migration) {
	var preMigrationCtxs []*scheduler.Context
	var migrations []*storkv1.Migration

	for idx, ctx := range ctxs {
		preMigrationCtxs = append(preMigrationCtxs, ctx.DeepCopy())

		// create, apply and validate cluster pair specs
		err := scheduleClusterPair(
			ctx, true, true, defaultClusterPairDir, "", false)
		log.FailOnError(t, err, "Error scheduling cluster pair")

		// apply migration specs
		migration, err := createMigration(
			t, migrationName, namespaces[idx], "remoteclusterpair",
			namespaces[idx], &includeResources, &includeVolumes, &startApplications)
		log.FailOnError(t, err, "Error scheduling migration")
		migrations = append(migrations, migration)
	}
	return preMigrationCtxs, ctxs, migrations
}

func createActionCR(
	t *testing.T,
	actionName,
	namespace string,
) (*storkv1.Action, error) {
	actionSpec := storkv1.Action{
		ObjectMeta: meta_v1.ObjectMeta{
			Name:      actionName,
			Namespace: namespace,
		},
		Spec: storkv1.ActionSpec{
			ActionType: storkv1.ActionTypeFailover,
		},
		Status: storkv1.ActionStatus{
			Status: storkv1.ActionStatusScheduled,
		},
	}
	return storkops.Instance().CreateAction(&actionSpec)
}

func validateActionCR(t *testing.T, actionName, namespace string, isSuccessful bool) {
	action, err := storkops.Instance().GetAction(actionName, namespace)
	log.FailOnError(t, err, "error fetching Action CR")
	if isSuccessful {
		Dash.VerifyFatal(t, action.Status, storkv1.ActionStatusSuccessful, "Action status Successful")
	} else {
		Dash.VerifyFatal(t, action.Status, storkv1.ActionStatusFailed, "Action status Failed")
	}
}

func scaleDownApps(
	t *testing.T,
	ctxs []*scheduler.Context,
) []map[string]int32 {
	var scaleFactors []map[string]int32

	for _, ctx := range ctxs {
		scaleFactor, err := schedulerDriver.GetScaleFactorMap(ctx)
		log.FailOnError(t, err, "unexpected error on GetScaleFactorMap")
		scaleFactors = append(scaleFactors, scaleFactor)

		zeroScaleFactor := make(map[string]int32) // scale down
		for k := range scaleFactor {
			zeroScaleFactor[k] = 0
		}

		err = schedulerDriver.ScaleApplication(ctx, zeroScaleFactor)
		log.FailOnError(t, err, "unexpected error on ScaleApplication")

		// check if the app is scaled down
		_, err = task.DoRetryWithTimeout(
			func() (interface{}, bool, error) {
				updatedScaleFactor, err := schedulerDriver.GetScaleFactorMap(ctx)
				if err != nil {
					return "", true, err
				}
				for k := range updatedScaleFactor {
					if int(updatedScaleFactor[k]) != 0 {
						return "", true, fmt.Errorf("expected scale to be 0")
					}
				}
				return "", false, nil
			},
			defaultWaitTimeout,
			defaultWaitInterval)
		log.FailOnError(t, err, "unexpected error on scaling down application.")
	}
	return scaleFactors
}

func validateMigrationOnSrc(
	t *testing.T,
	migrationName string,
	namespaces []string,
) {
	for _, namespace := range namespaces {
		err := storkops.Instance().ValidateMigration(migrationName, namespace, defaultWaitTimeout, defaultWaitInterval)
		log.FailOnError(t, err, "Error validating migration")
		log.InfoD("Validated migration on src: %v", migrationName)
	}
}

func changePxServiceToLoadBalancer(internalLB bool) error {
	// Check if service is already of type loadbalancer
	pxService, err := core.Instance().GetService(pxServiceName, pxNamespace)
	if err != nil {
		return fmt.Errorf("failed to get portworx service before changing it to load balancer: %v", err)
	}
	if pxService.Spec.Type == v1.ServiceTypeLoadBalancer {
		log.InfoD("portworx service is already of type LoadBalancer")
		return nil
	}

	if volumeDriverName == storkdriver.PortworxDriverName {
		stc, err := operator.Instance().ListStorageClusters(pxNamespace)
		if err != nil {
			return fmt.Errorf("failed to list PX storage cluster on EKS/AKS/GKE: %v", err)
		}
		if len(stc.Items) > 0 {
			pxStc := (*stc).Items[0]
			// Change portworx service to LoadBalancer, since this is an EKS/AKS/GKE with PX operator install for Portworx
			if pxStc.ObjectMeta.Annotations == nil {
				pxStc.ObjectMeta.Annotations = make(map[string]string)
			}
			pxStc.ObjectMeta.Annotations[pxStcServiceTypeKey] = stcLoadBalancerValue

			if internalLB {
				// Add LoadBalancer annotations specific to EKS ELB
				if pxStc.Spec.Metadata == nil {
					pxStc.Spec.Metadata = &opcorev1.Metadata{}
					pxStc.Spec.Metadata.Annotations = make(map[string]map[string]string)
					pxStc.Spec.Metadata.Annotations[pxStcServiceKey] = make(map[string]string)
				}
				if pxStc.Spec.Metadata.Annotations == nil {
					pxStc.Spec.Metadata.Annotations = make(map[string]map[string]string)
				}
				pxStc.Spec.Metadata.Annotations[pxStcServiceKey] = make(map[string]string)
				pxStc.Spec.Metadata.Annotations[pxStcServiceKey][awsInternalLBKey] = awsInternalLBValue
				pxStc.Spec.Metadata.Annotations[pxStcServiceKey][awsLBTypeKey] = awsLBTypeVal
				pxStc.Spec.Metadata.Annotations[pxStcServiceKey][awsNLBTargetTypeKey] = awsNLBTargetTypeVal
				pxStc.Spec.Metadata.Annotations[pxStcServiceKey][awsLBSubnetKey] = awsLBSubnetVal
			}
			_, err = operator.Instance().UpdateStorageCluster(&pxStc)
			if err != nil {
				return fmt.Errorf("failed to update PX service type to LoadBalancer on EKS: %v", err)
			}

		} else {
			return fmt.Errorf("No storage clusters found")
		}

		time.Sleep(1 * time.Minute)

		// Check if service has been changed to type loadbalancer
		pxService, err := core.Instance().GetService(pxServiceName, pxNamespace)
		if err != nil {
			return fmt.Errorf("failed to get portworx service after changing it to load balancer: %v", err)
		}

		if pxService.Spec.Type != v1.ServiceTypeLoadBalancer {
			return fmt.Errorf("failed to set portworx service to type %s", v1.ServiceTypeLoadBalancer)
		}

	}
	return nil
}

// Set environment variable for TEST_MODE=true required for running some stork integration tests
func addTestModeEnvironmentVar() error {
	stc, err := operator.Instance().ListStorageClusters(pxNamespace)
	if err != nil {
		log.InfoD("failed to list PX storage cluster: %v, won't add TEST_MODE environment variable to stork", err)
		return nil
	}
	if len(stc.Items) > 0 {
		pxStc := (*stc).Items[0]
		storkEnvVars := v1.EnvVar{
			Name:  "TEST_MODE",
			Value: "true",
		}
		if len(pxStc.Spec.Stork.Env) == 0 {
			pxStc.Spec.Stork.Env = []v1.EnvVar{storkEnvVars}
		} else {
			pxStc.Spec.Stork.Env = append(pxStc.Spec.Stork.Env, storkEnvVars)
		}
		_, err = operator.Instance().UpdateStorageCluster(&pxStc)
		if err != nil {
			return fmt.Errorf("failed to update PX service type to LoadBalancer on EKS: %v", err)
		}
		log.InfoD("Successfully added TEST_MODE environment variable to stork spec in storage cluster")
	}
	return nil
}

func IsCloud() bool {
	stc, err := operator.Instance().ListStorageClusters(defaultAdminNamespace)
	if len(stc.Items) > 0 && err == nil {
		log.InfoD("Storage cluster name: %s", stc.Items[0].Name)
		if len(stc.Items) > 0 {
			if oputils.IsEKS(&stc.Items[0]) {
				log.InfoD("EKS installation detected.")
				return true
			} else if oputils.IsAKS(&stc.Items[0]) {
				log.InfoD("AKS installation detected.")
				return true
			} else if oputils.IsGKE(&stc.Items[0]) {
				log.InfoD("GKE installation detected.")
				return true
			}
		}
	}
	return false
}

// SetupTestRail checks if the required parameters for testrail are passed, verifies connectivity and creates milestone if it does not exist
func SetupTestRail() {
	if testrailutils.RunName = os.Getenv(testrailRunNameVar); testrailutils.RunName != "" {
		log.InfoD("Testrail Run name: %s", testrailutils.RunName)
	}
	if testrailutils.JobRunID = os.Getenv(testrailRunIDVar); testrailutils.JobRunID != "" {
		log.InfoD("Testrail Run ID: %s", testrailutils.JobRunID)
	}
	if testrailutils.MilestoneName = os.Getenv(testrailMilestoneVar); testrailutils.MilestoneName != "" {
		log.InfoD("Testrail Milestone  %s", testrailutils.MilestoneName)
	}
	if testrailutils.JenkinsBuildURL = os.Getenv(testrailJenkinsBuildURLVar); testrailutils.JenkinsBuildURL != "" {
		log.InfoD("Testrail Jenkins Build URL: %s", testrailutils.JenkinsBuildURL)
	}
	if testrailHostname = os.Getenv(testrailHostVar); testrailHostname != "" {
		log.InfoD("Testrail Host: %s", testrailHostname)
	}
	if testrailUsername = os.Getenv(testrailUserNameVar); testrailUsername != "" {
		log.InfoD("Testrail Host: %s", testrailUsername)
	}
	if testrailPassword = os.Getenv(testrailPasswordVar); testrailPassword != "" {
		log.InfoD("Testrail Password: %s", testrailPassword)
	}
	if testrailHostname != "" && testrailUsername != "" && testrailPassword != "" {
		err := testrailutils.Init(testrailHostname, testrailUsername, testrailPassword)
		if err == nil {
			if testrailutils.MilestoneName == "" || testrailutils.RunName == "" || testrailutils.JobRunID == "" {
				log.Warn("not all details provided to update testrail")
				return
			}
			testrailutils.CreateMilestone()
			testrailSetupSuccessful = true
		}
		log.InfoD("Testrail setup is successful, will log results to testrail automatically. Details:\nMilestone: %s, Testrun: %s",
			testrailutils.MilestoneName, testrailutils.RunName)
	} else {
		log.Warn("Not all information to connect to testrail is provided, skipping updates to testrail")
	}
}

func updateTestRail(testStatus *string, ids ...int) {
	if ids[0] != 0 && ids[1] != 0 {
		testrailObject := testrailutils.Testrail{
			Status:        *testStatus,
			TestID:        ids[0],
			RunID:         ids[1],
			DriverVersion: storkVersion,
		}
		testrailutils.AddTestEntry(testrailObject)
		log.InfoD("Testrail testrun url: %s/index.php?/runs/view/%d&group_by=cases:custom_automated&group_order=asc&group_id=%d", testrailHostname, ids[1], testrailutils.PwxProjectID)
	} else {
		log.Warn("Skipping testrail update for this case, testID: %d, testrun: %d", ids[0], ids[1])
	}
}

func testrailSetupForTest(testrailID int, testResult *string, testName string) int {
	tags := make(map[string]string)
	tags["storkTest"] = "true"
	Dash.TestCaseBegin(testName, fmt.Sprintf("Stork test - %s", testName), "", tags)
	runID, err := addRunToMilestone(testrailID, testResult)
	if err != nil {
		log.Warn("For current case: %d, not adding this run to testrail", testrailID)
		return 0
	}
	return runID
}

func addRunToMilestone(testrailID int, testResult *string) (int, error) {
	var runID int
	var err error
	if testrailutils.JobRunID != "" {
		if testrailID == 0 {
			return 0, fmt.Errorf("invalid testcase ID: %v", testrailID)
		}
		runID, err = strconv.Atoi(testrailutils.JobRunID)
		if err != nil {
			return 0, fmt.Errorf("invalid testrail run ID: %v", testrailutils.JobRunID)
		}
	}
	runID = testrailutils.AddRunsToMilestone(testrailID)
	return runID, nil
}

func getPodsForApp(ctx *scheduler.Context) ([]v1.Pod, error) {
	var pods []v1.Pod

	for _, specObj := range ctx.App.SpecList {
		if obj, ok := specObj.(*appsapi.Deployment); ok {
			depPods, err := apps.Instance().GetDeploymentPods(obj)
			if err != nil {
				return nil, err
			}
			pods = append(pods, depPods...)
		} else if obj, ok := specObj.(*appsapi.StatefulSet); ok {
			ssPods, err := apps.Instance().GetStatefulSetPods(obj)
			if err != nil {
				return nil, err
			}
			pods = append(pods, ssPods...)
		}
	}

	return pods, nil
}

type StorageClass struct {
	name        string
	provisioner string
	// parameters
	repl                          int
	nearsync                      bool
	nearsync_replication_strategy string
}

func createStorageClass(storageClass StorageClass) (*storage_v1.StorageClass, error) {
	parameters := make(map[string]string)
	if storageClass.repl != 0 {
		parameters["repl"] = strconv.Itoa(storageClass.repl)
	} else {
		parameters["repl"] = "1"
	}
	if storageClass.nearsync {
		parameters["nearsync"] = "true"
	}
	if storageClass.nearsync_replication_strategy != "" {
		parameters["near_sync_replication_strategy"] = storageClass.nearsync_replication_strategy
	}
	return storage.Instance().CreateStorageClass(&storage_v1.StorageClass{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: storageClass.name,
		},
		Provisioner: storageClass.provisioner,
		Parameters:  parameters,
	})
}

func TestMain(m *testing.M) {
	flag.IntVar(&snapshotScaleCount,
		"snapshot-scale-count",
		10,
		"Number of volumes to use for scale snapshot test")
	flag.IntVar(&migrationScaleCount,
		"migration-scale-count",
		10,
		"Number of migrations to use for migration test")
	flag.IntVar(&backupScaleCount,
		"backup-scale-count",
		10,
		"Number of different backups per app for scaled backup test")
	flag.StringVar(&volumeDriverName,
		"volume-driver",
		"pxd",
		"Stork volume driver to be used for stork integration tests")
	flag.StringVar(&genericCsiConfigMap,
		"generic-csi-config",
		"",
		"Config map name that contains details of csi driver to be used for provisioning")
	flag.BoolVar(&storkVersionCheck,
		"stork-version-check",
		false,
		"Turn on/off stork version check before running tests. Default off.")
	flag.BoolVar(&bidirectionalClusterpair,
		"bidirectional-cluster-pair",
		false,
		"Turn on/off bidirectional cluster pair creation for all migrations. Default off.")
	flag.BoolVar(&unidirectionalClusterpair,
		"unidirectional-cluster-pair",
		false,
		"Turn on/off unidirectional cluster pair creation for all migrations. Default off.")
	flag.IntVar(&kubevirtScale,
		"kubevirt-scale-count",
		1,
		"Number of different kubevirt VMs deployed and validated in kubevirt tests")
	flag.Parse()
	var once sync.Once
	once.Do(func() {
		doDashboardSetup()
	})

	if err := setup(); err != nil {
		log.Error("Setup failed with error: %v", err)
		os.Exit(1)
	}
	exitCode := m.Run()
	Dash.TestSetEnd()
	os.Exit(exitCode)
}

// activates/deactivate the source cluster domain
func updateClusterDomain(t *testing.T, clusterDomains *storkv1.ClusterDomains, activate bool, wait bool) {
	var err error
	op := "activate"
	if !activate {
		op = "deactivate"
	}
	executeOnDestination(t, func() {
		destNode := node.GetStorageDriverNodes()[0]
		out, err := volumeDriver.GetPxctlCmdOutput(
			destNode, fmt.Sprintf("cluster domains %v --name %v", op, clusterDomains.LocalDomain))
		log.FailOnError(t, err, "Failed to %v cluster domain %v", op, clusterDomains.LocalDomain)
		log.InfoD(out)
	})
	if wait {
		srcNodes := node.GetStorageDriverNodes()
		if activate {
			for _, srcNode := range srcNodes {
				err = volumeDriver.WaitDriverUpOnNode(srcNode, defaultWaitTimeout)
				log.FailOnError(t, err, "Failed to wait for driver to come up on node %v", srcNode)
			}
		} else {
			for _, srcNode := range srcNodes {
				err = volumeDriver.WaitDriverDownOnNode(srcNode)
				log.FailOnError(t, err, "Failed to wait for driver to go down on node %v", srcNode)
			}
		}
	}
}

func updatePXVolumeLabel(t *testing.T, volume string, labelsMap map[string]string) {
	labels := make([]string, 0)
	for k, v := range labelsMap {
		labels = append(labels, fmt.Sprintf("%s=%s", k, v))
	}
	labelString := strings.Join(labels, ",")
	pxNode := node.GetStorageDriverNodes()[0]
	out, err := volumeDriver.GetPxctlCmdOutput(
		pxNode, fmt.Sprintf("volume update %s --label %s", volume, labelString))
	log.FailOnError(t, err, "Failed to update volume %v with labels %v", volume, labelsMap)
	log.InfoD(out)
}

func getSupportedOperatorCRMapping() map[string][]meta_v1.APIResource {
	operatorAppToCRMap := make(map[string][]meta_v1.APIResource)
	// mongodbcommunity CR
	operatorAppToCRMap[appNameMongo] = []meta_v1.APIResource{
		{
			Kind:       "MongoDBCommunity",
			Version:    "v1",
			Group:      "mongodbcommunity.mongodb.com",
			Name:       "mongodbcommunity",
			Namespaced: true,
		},
	}

	// kafka CR
	operatorAppToCRMap[appNameKafka] = []meta_v1.APIResource{
		{
			Kind:       "Kafka",
			Version:    "v1beta2",
			Group:      "kafka.strimzi.io",
			Name:       "kafkas",
			Namespaced: true,
		},
	}

	return operatorAppToCRMap
}

// extract data from a file in []byte format
func getByteDataFromFile(filePath string) ([]byte, error) {
	if filePath == "" {
		return nil, fmt.Errorf("empty file path")
	}
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file %v: %v", filePath, err)
	}
	data, err := io.ReadAll(bufio.NewReader(file))
	if err != nil {
		return nil, fmt.Errorf("error reading file %v: %v", filePath, err)
	}
	return data, nil
}

func updateDashStats(testName string, testResult *string) {
	var err error
	tpDash.IsEnabled, err = strconv.ParseBool(os.Getenv(enableDashStats))
	if tpDash.IsEnabled && err == nil {
		log.InfoD("Dash is not enabled, stork integration tests will NOT push stats from this run to Aetos.")
	}
	dashStats := make(map[string]string)
	dashStats["test_suite"] = currentTestSuite
	dashStats["test_name"] = testName
	dashStats["stork_version"] = storkVersion
	dashStats["test_result"] = *testResult
	dashStats["node_driver"] = nodeDriver.String()
	dashStats["volume_driver"] = volumeDriver.String()
	provisioner := os.Getenv(storageProvisioner)
	dashStats["storage_provisioner"] = provisioner
	dashStats["scheduler_driver"] = schedulerDriver.String()
	pxVersion, err := volumeDriver.GetDriverVersion()
	if err != nil {
		log.Error("error getting px version. err: %+v", err)
	}
	eventStat := &stats.EventStat{
		EventName: "Automated Stork Integration Tests",
		EventTime: time.Now().Format(time.RFC1123),
		Version:   pxVersion,
		DashStats: dashStats,
	}

	stats.PushStatsToAetos(tpDash, testName, dashProductName, dashStatsType, eventStat)
	Dash.TestCaseEnd()
}
