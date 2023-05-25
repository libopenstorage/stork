package tests

import (
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	pdsdriver "github.com/portworx/torpedo/drivers/pds"
	"github.com/portworx/torpedo/drivers/pds/api"
	"github.com/portworx/torpedo/drivers/pds/controlplane"
	dataservices "github.com/portworx/torpedo/drivers/pds/dataservice"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	"github.com/portworx/torpedo/drivers/pds/parameters"
	"github.com/portworx/torpedo/drivers/pds/targetcluster"
	"os"
	"testing"

	"github.com/portworx/torpedo/pkg/log"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	. "github.com/portworx/torpedo/tests"
)

func TestDataService(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_basic.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : pds", specReporters)

}

// imports based on functionalities
var (
	dsTest        *dataservices.DataserviceType
	customParams  *parameters.Customparams
	targetCluster *targetcluster.TargetCluster
	controlPlane  *controlplane.ControlPlane
	components    *api.Components
	apiClient     *pds.APIClient
)

var _ = BeforeSuite(func() {
	steplog := "Get prerequisite params to run the pds tests"
	log.InfoD(steplog)
	Step(steplog, func() {
		log.InfoD(steplog)
		dash = Inst().Dash
		dash.TestSet.Product = "pds"
		dash.TestSetBegin(dash.TestSet)
		pdsLabels["pds"] = "true"

		pdsparams := pdslib.GetAndExpectStringEnvVar("PDS_PARAM_CM")
		params, err = customParams.ReadParams(pdsparams)
		log.FailOnError(err, "Failed to read params from json file")
		infraParams := params.InfraToTest
		pdsLabels["clusterType"] = infraParams.ClusterType

		//Initialize pds components in pdslib
		err = pdslib.InitPdsComponents(params.InfraToTest.ControlPlaneURL)
		log.FailOnError(err, "Error while initializing pds components in pdslib")

		components, controlPlane, err = pdsdriver.InitPdsApiComponents(params.InfraToTest.ControlPlaneURL)
		log.FailOnError(err, "Error while initializing pds components in pds test")

		dsTest, err = dataservices.DataserviceInit(params.InfraToTest.ControlPlaneURL)
		log.FailOnError(err, "Error while initializing dataservice package")

		accountID, tenantID, dnsZone, projectID, serviceType, clusterID, err = controlPlane.SetupPDSTest(
			infraParams.ControlPlaneURL, infraParams.ClusterType, infraParams.AccountName, infraParams.TenantName, infraParams.ProjectName)
		log.FailOnError(err, "Failed on SetupPDSTest method")

	})

	steplog = "Check and Register Target Cluster to ControlPlane"
	Step(steplog, func() {
		log.InfoD(steplog)
		err = targetCluster.RegisterClusterToControlPlane(params, tenantID, false)
		log.FailOnError(err, "Target Cluster Registeration failed")
	})

	steplog = "Get Deployment TargetID"
	Step(steplog, func() {
		log.InfoD(steplog)
		log.Infof("cluster id %v and tenant id %v", clusterID, tenantID)
		deploymentTargetID, err = targetCluster.GetDeploymentTargetID(clusterID, tenantID)
		log.FailOnError(err, "Failed to get the deployment TargetID")
		dash.VerifyFatal(deploymentTargetID != "", true, "Verifying deployment target is registerd to control plane")
		log.InfoD("DeploymentTargetID %s ", deploymentTargetID)

	})

	steplog = "Get StorageTemplateID and Replicas"
	Step(steplog, func() {
		log.InfoD(steplog)
		storageTemplateID, err = controlPlane.GetStorageTemplate(tenantID)
		log.FailOnError(err, "Failed while getting storage template ID")
		log.InfoD("storageTemplateID %v", storageTemplateID)
	})

	Step(steplog, func() {
		log.InfoD(steplog)
		namespace = params.InfraToTest.Namespace
		_, isavailable, err := targetCluster.CreatePDSNamespace(namespace)
		log.FailOnError(err, "Error while Create/Get Namespaces")
		dash.VerifyFatal(bool(true), isavailable, "Verifying if Namespace available for pds to deploy data services")
		namespaceID, err = targetCluster.GetnameSpaceID(namespace, deploymentTargetID)
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
