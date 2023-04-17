package tests

import (
	"fmt"
	"net/http"

	. "github.com/onsi/ginkgo"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

var _ = Describe("{RebootActiveNodeDuringDeployment}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RebootActiveNodeDuringDeployment", "Reboots a Node onto which a pod is coming up", pdsLabels, 0)
	})

	It("deploy Dataservices", func() {
		Step("Deploy Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				Step("Start deployment, Reboot a node on which deployment is coming up and validate data service", func() {
					isDeploymentsDeleted = false
					// Global Resiliency TC marker
					pdslib.MarkResiliencyTC(true, true)
					// Type of failure that this TC needs to cover
					failuretype := pdslib.TypeOfFailure{
						Type: ActiveNodeRebootDuringDeployment,
						Method: func() error {
							return pdslib.RebootActiveNodeDuringDeployment(params.InfraToTest.Namespace)
						},
					}
					pdslib.DefineFailureType(failuretype)
					// Deploy and Validate this Data service after injecting the type of failure we want to catch
					deployment, _, _, err = TriggerDeployDataService(ds, params.InfraToTest.Namespace, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")
					err = pdslib.InduceFailureAfterWaitingForCondition(deployment, namespace, params.ResiliencyTest.CheckTillReplica)
					log.FailOnError(err, fmt.Sprintf("Error happened while executing Reboot test for data service %v", *deployment.ClusterResourceName))
				})
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		pdslib.CloseResiliencyChannel()
		if !isDeploymentsDeleted {
			Step("Delete created deployments")
			resp, err := pdslib.DeleteDeployment(deployment.GetId())
			log.FailOnError(err, "Error while deleting data services")
			dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
		}
	})
})

var _ = Describe("{KillDeploymentControllerDuringDeployment}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("KillDeploymentControllerDuringDeployment", "Kill Deployment Controller Pod when a DS Deployment is happening", pdsLabels, 0)
	})

	It("Deploy Dataservices", func() {
		Step("Deploy Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				if ds.Name != postgresql {
					continue
				}
				Step("Start deployment, Kill Deployment Controller Pod while deployment is ongoing and validate data service", func() {
					isDeploymentsDeleted = false
					// Global Resiliency TC marker
					pdslib.MarkResiliencyTC(true, false)
					// Type of failure that this TC needs to cover
					failuretype := pdslib.TypeOfFailure{
						Type: KillDeploymentControllerPod,
						Method: func() error {
							return pdslib.KillDeploymentPodDuringDeployment(params.InfraToTest.PDSNamespace)
						},
					}
					pdslib.DefineFailureType(failuretype)
					// Deploy and Validate this Data service after injecting the type of failure we want to catch
					deployment, _, _, err = TriggerDeployDataService(ds, params.InfraToTest.Namespace, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")
					err = pdslib.InduceFailureAfterWaitingForCondition(deployment, namespace, params.ResiliencyTest.CheckTillReplica)
					log.FailOnError(err, fmt.Sprintf("Error happened while executing Reboot test for data service %v", *deployment.ClusterResourceName))
				})
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		pdslib.CloseResiliencyChannel()
		if !isDeploymentsDeleted {
			Step("Delete created deployments")
			resp, err := pdslib.DeleteDeployment(deployment.GetId())
			log.FailOnError(err, "Error while deleting data services")
			dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
		}
	})
})
