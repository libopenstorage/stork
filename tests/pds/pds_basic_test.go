package tests

import (
	"os"
	"testing"

	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/pkg/log"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	"github.com/portworx/torpedo/pkg/aetosutil"
	. "github.com/portworx/torpedo/tests"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	pdsNamespace            = "pds-system"
	deploymentName          = "qa"
	envDeployAllDataService = "DEPLOY_ALL_DATASERVICE"
	postgresql              = "PostgreSQL"
	cassandra               = "Cassandra"
	redis                   = "Redis"
	rabbitmq                = "RabbitMQ"
	mongodb                 = "MongoDB"
	mysql                   = "MySQL"
	kafka                   = "Kafka"
	zookeeper               = "ZooKeeper"
	pdsNamespaceLabel       = "pds.portworx.com/available"
)

var (
	namespace                               string
	pxnamespace                             string
	tenantID                                string
	dnsZone                                 string
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
	DeployAllVersions                       bool
	DataService                             string
	DeployAllImages                         bool
	dataServiceDefaultResourceTemplateID    string
	dataServiceDefaultAppConfigID           string
	dataServiceVersionBuildMap              map[string][]string
	dep                                     *v1.Deployment
	pod                                     *corev1.Pod
	params                                  *pdslib.Parameter
	isDeploymentsDeleted                    bool
	isNamespacesDeleted                     bool
	dash                                    *aetosutil.Dashboard
	deployment                              *pds.ModelsDeployment
	k8sCore                                 = core.Instance()
)

func TestDataService(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_basic.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : pds", specReporters)

}

var _ = BeforeSuite(func() {
	Step("get prerequisite params to run the pds tests", func() {
		//InitInstance()
		dash = Inst().Dash
		dash.TestSetBegin(dash.TestSet)
		pdsparams := pdslib.GetAndExpectStringEnvVar("PDS_PARAM_CM")
		params, err = pdslib.ReadParams(pdsparams)
		log.FailOnError(err, "Failed to read params from json file")
		infraParams := params.InfraToTest

		tenantID, dnsZone, projectID, serviceType, deploymentTargetID, err = pdslib.SetupPDSTest(infraParams.ControlPlaneURL, infraParams.ClusterType, infraParams.AccountName)
		log.InfoD("DeploymentTargetID %v ", deploymentTargetID)
		log.FailOnError(err, "Failed on SetupPDSTest method")
	})

	Step("Get StorageTemplateID and Replicas", func() {
		storageTemplateID, err = pdslib.GetStorageTemplate(tenantID)
		log.FailOnError(err, "Failed while getting storage template ID")
		log.InfoD("storageTemplateID %v", storageTemplateID)
	})

	Step("Create/Get Namespace and NamespaceID", func() {
		namespace = params.InfraToTest.Namespace
		_, isavailable, err := pdslib.CreatePDSNamespace(namespace)
		log.FailOnError(err, "Error while Create/Get Namespaces")
		dash.VerifyFatal(bool(true), isavailable, "Namespace is not available for pds to deploy data services")
		namespaceID, err = pdslib.GetnameSpaceID(namespace, deploymentTargetID)
		log.FailOnError(err, "Error while getting namespace id")
		dash.VerifyFatal(namespaceID != "", true, "validating namespace ID")
	})
})

var _ = AfterSuite(func() {
	defer dash.TestSetEnd()
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
