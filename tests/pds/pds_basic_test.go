package tests

import (
	"fmt"
	"os"
	"strings"
	"testing"

	pdsdriver "github.com/portworx/torpedo/drivers/pds"
	dataservices "github.com/portworx/torpedo/drivers/pds/dataservice"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
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

var _ = BeforeSuite(func() {
	steplog := "Get prerequisite params to run the pds tests"
	log.InfoD(steplog)
	Step(steplog, func() {
		log.InfoD(steplog)
		InitInstance()
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
	if params.CleanUpParams.SkipTargetClusterCheck {
		log.InfoD("Skipping Target Cluster Check")
	} else {
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

		steplog = "Update deployment target with cluster issuer"
		Step(steplog, func() {
			log.InfoD(steplog)
			if params.TLS.EnableTLS {
				err = targetCluster.UpdateTargetClusterWithClusterIssuer(deploymentTargetID, params.TLS.ClusterIssuerName, false)
				log.FailOnError(err, "Error while updating the target cluster with cluster issuer")
			}
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
	}
	kubeconfigs := os.Getenv("KUBECONFIGS")
	if kubeconfigs != "" {
		kubeconfigList := strings.Split(kubeconfigs, ",")
		if len(kubeconfigList) < 2 {
			log.FailOnError(fmt.Errorf("At least minimum two kubeconfigs required but has"),
				"Failed to get k8s config path.At least minimum two kubeconfigs required")
		}
		DumpKubeconfigs(kubeconfigList)
	}

})

var _ = AfterSuite(func() {
	defer dash.TestSetEnd()
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
