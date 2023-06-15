package tests

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/portworx/torpedo/drivers/pds/dataservice"

	tc "github.com/portworx/torpedo/drivers/pds/targetcluster"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/torpedo/drivers/node"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	corev1 "k8s.io/api/core/v1"
)

const (
	defaultWaitRebootRetry       = 10 * time.Second
	defaultCommandRetry          = 5 * time.Second
	defaultCommandTimeout        = 1 * time.Minute
	defaultTestConnectionTimeout = 15 * time.Minute
)

var _ = Describe("{ValidateDNSEndpoint}", func() {
	steplog := "Deploy dataservice, delete and validate pds pods"
	JustBeforeEach(func() {
		StartTorpedoTest("ValidateDNSEndpoint", "validate dns endpoitns", pdsLabels, 0)
	})

	Step(steplog, func() {
		log.InfoD(steplog)
		It("validate dns endpoints", func() {
			var deployments = make(map[PDSDataService]*pds.ModelsDeployment)
			var dsVersions = make(map[string]map[string][]string)

			for _, ds := range params.DataServiceToTest {
				Step("Deploy and validate data service", func() {
					isDeploymentsDeleted = false
					deployment, _, dataServiceVersionBuildMap, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")
					deployments[ds] = deployment
					dsVersions[ds.Name] = dataServiceVersionBuildMap
				})
			}

			defer func() {
				for _, newDeployment := range deployments {
					Step("Delete created deployments")
					resp, err := pdslib.DeleteDeployment(newDeployment.GetId())
					log.FailOnError(err, "Error while deleting data services")
					dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
				}
			}()

			steplog = "Validate Dns Endpoints of Data services"
			Step(steplog, func() {
				log.InfoD(steplog)
				for ds, deployment := range deployments {
					dnsEndpoint, port, err := pdslib.GetDeploymentConnectionInfo(deployment.GetId(), ds.Name)
					log.FailOnError(err, "Failed Getting DNS endpoints")
					err = controlPlane.ValidateDNSEndpoint(dnsEndpoint + ":" + port)
					log.FailOnError(err, "Failed Validating DNS endpoints")
					log.InfoD("DNS endpoint is reachable and ready to accept connections")
				}
			})
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

var _ = Describe("{DeletePDSPods}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("DeletePDSPods", "delete pds pods and validate if its coming back online and dataserices are not affected", pdsLabels, 0)
	})

	It("Delete pds pods and validate if its coming back online and dataserices are not affected", func() {
		Step("Deploy dataservice, delete and validate pds pods", func() {
			for _, ds := range params.DataServiceToTest {
				Step("Deploy and validate data service", func() {
					isDeploymentsDeleted = false
					deployment, _, _, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")
				})

				pdsPods := make([]corev1.Pod, 0)

				Step("get pods from pds-system namespace", func() {

					podList, err = pdslib.GetPods(pdsNamespace)
					log.FailOnError(err, "Error while getting pods")
					log.Infof("PDS System Pods")
					for _, pod := range podList.Items {
						log.Infof("%v", pod.Name)
						pdsPods = append(pdsPods, pod)
					}
				})

				Step("delete pods from pds-system namespace", func() {
					log.InfoD("Deleting PDS System Pods")
					err = pdslib.DeletePods(pdsPods)
					log.FailOnError(err, "Error while deleting pods")
					log.InfoD("Validating PDS System Pods")
					err = pdslib.ValidatePods(pdsNamespace, "")
					log.FailOnError(err, "Error while validating pods")

				})

				Step("Validate Deployments after pods are up", func() {
					log.InfoD("Validate Deployments after pds pods are up")
					err = dsTest.ValidateDataServiceDeployment(deployment, namespace)
					log.FailOnError(err, "Error while validating data services")
					log.InfoD("Deployments pods are up and healthy")
				})

				Step("Delete Deployments", func() {
					log.InfoD("Deleting Deployment %v ", *deployment.ClusterResourceName)
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					log.FailOnError(err, "Error while deleting data services")
					dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
					log.InfoD("Getting all PV and associated PVCs and deleting them")
					err = pdslib.DeletePvandPVCs(*deployment.ClusterResourceName, false)
					log.FailOnError(err, "Error while deleting PV and PVCs")
				})
			}

		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

var _ = Describe("{UpdatePDSHelmVersion}", func() {
	steplog := "Validate Data service pods post helm upgrade"
	var deps []*pds.ModelsDeployment
	var wlparams pdslib.WorkloadGenerationParams
	var dataservices []PDSDataService
	var workloadPods []string

	JustBeforeEach(func() {
		StartTorpedoTest("UpdatePDSHelmVersion", steplog, pdsLabels, 0)
	})

	It(steplog, func() {
		steplog = "Install older helm verison"
		Step(steplog, func() {
			log.InfoD(steplog)
			ctx := pdslib.GetAndExpectStringEnvVar("TARGET_KUBECONFIG")
			target := tc.NewTargetCluster(ctx)
			isOldversion, err := target.IsLatestPDSHelm(params.PDSHelmVersions.PreviousHelmVersion)
			if !isOldversion {
				err = target.DeRegisterFromControlPlane()
				log.FailOnError(err, "Target Cluster DeRegisteration failed")

				err = targetCluster.RegisterClusterToControlPlane(params, tenantID, true)
				log.FailOnError(err, "Target Cluster Registeration failed")
			} else {
				log.InfoD("Target Cluster is with old pds helm version %s", params.PDSHelmVersions.PreviousHelmVersion)
			}
		})

		steplog = "Deploy and validate data service"
		Step(steplog, func() {
			log.InfoD(steplog)
			deps = DeployInANamespaceAndVerify(params.InfraToTest.Namespace)
		})

		defer func() {
			steplog = "Delete created dataservice deployments"
			Step(steplog, func() {
				log.InfoD(steplog)
				for _, dep := range deps {
					_, err := pdslib.DeleteDeployment(*dep.Id)
					log.FailOnError(err, "error while deleting deployments")
				}
				isDeploymentsDeleted = true
			})
		}()

		log.InfoD("List of created deployments")
		for _, deployment := range deps {
			log.InfoD("%v ", *deployment.ClusterResourceName)
		}

		steplog = "Running Workloads before scaling up of dataservices"
		Step(steplog, func() {

			for _, ds := range params.DataServiceToTest {
				dataservices = append(dataservices, ds)
			}
			for index, deployment := range deps {
				log.InfoD("Running Workloads on deployment %v ", *deployment.ClusterResourceName)
				pod, dep, err = RunWorkloads(wlparams, dataservices[index], deployment, namespace)
				if dep == nil {
					workloadPods = append(workloadPods, pod.Name)
				} else {
					workloadPods = append(workloadPods, dep.Name)
				}
				log.FailOnError(err, fmt.Sprintf("Error while genearating workloads for dataservice [%s]", dataservices[index].Name))
			}
		})

		defer func() {
			//delete all the workload generating pods
			Step("Delete the workload generating deployments", func() {
				for _, workloadGeneratorName := range workloadPods {
					for _, ds := range params.DataServiceToTest {
						if Contains(dataServiceDeploymentWorkloads, ds.Name) {
							log.InfoD("Deleting Workload Generating pods %v ", workloadGeneratorName)
							err = pdslib.DeleteK8sDeployments(dep.Name, namespace)
						} else if Contains(dataServicePodWorkloads, ds.Name) {
							log.InfoD("Deleting Workload Generating pods %v ", workloadGeneratorName)
							err = pdslib.DeleteK8sPods(pod.Name, namespace)
						}
						log.FailOnError(err, "error deleting workload generating pods")
					}
				}
			})
		}()

		steplog = "Upgrade to latest pds helm verison"
		Step(steplog, func() {
			log.InfoD(steplog)
			err = targetCluster.RegisterClusterToControlPlane(params, tenantID, false)
			log.FailOnError(err, "Target Cluster Registeration failed")
		})

		steplog = "Validate Deployments after pds-system pods are up"
		Step(steplog, func() {
			log.InfoD(steplog)
			for _, dep := range deps {
				err = dsTest.ValidateDataServiceDeployment(dep, namespace)
				log.FailOnError(err, "Error while validating data services")
				log.InfoD("Deployments pods are up and healthy")
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()

		if !isDeploymentsDeleted {
			Step("Delete created deployments")
			for _, dep := range deps {
				resp, err := pdslib.DeleteDeployment(*dep.Id)
				log.FailOnError(err, "error while deleting deployments")
				dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
			}
		}
	})
})

var _ = Describe("{DeregisterTargetCluster}", func() {
	steplog := "Deregister target cluster"
	var deps []*pds.ModelsDeployment

	JustBeforeEach(func() {
		StartTorpedoTest("DeregisterTargetCluster", steplog, pdsLabels, 0)
	})

	It(steplog, func() {
		steplog = "Deploy and validate data service"
		Step(steplog, func() {
			log.InfoD(steplog)
			deps = DeployInANamespaceAndVerify(params.InfraToTest.Namespace)
		})

		log.InfoD("List of created deployments")
		for _, deployment := range deps {
			log.InfoD("%v ", *deployment.ClusterResourceName)
		}

		steplog = "Delete created dataservice deployments"
		Step(steplog, func() {
			log.InfoD(steplog)
			for _, dep := range deps {
				_, err := pdslib.DeleteDeployment(*dep.Id)
				log.FailOnError(err, "error while deleting deployments")
				log.InfoD("Getting all PV and associated PVCs and deleting them")
				err = pdslib.DeletePvandPVCs(*dep.ClusterResourceName, false)
				log.FailOnError(err, "Error while deleting PV and PVCs")
			}
			isDeploymentsDeleted = true
		})

		steplog = "Remove backup and deployment CRD's"
		Step(steplog, func() {
			log.InfoD(steplog)
			apiGroups := []string{BackUpCRD, DeploymentCRD}
			err = pdslib.DeletePDSCRDs(apiGroups)
			log.FailOnError(err, "Error while getting crd information")
		})

		steplog = "De-Register Target Cluster"
		Step(steplog, func() {
			log.InfoD(steplog)
			ctx := pdslib.GetAndExpectStringEnvVar("TARGET_KUBECONFIG")
			target := tc.NewTargetCluster(ctx)
			err = target.DeRegisterFromControlPlane()
			log.FailOnError(err, "Error occurred while de-registering target cluster")
		})

		defer func() {
			steplog = "Check and Register Target Cluster to ControlPlane"
			Step(steplog, func() {
				log.InfoD(steplog)
				err = targetCluster.RegisterClusterToControlPlane(params, tenantID, false)
				log.FailOnError(err, "Target Cluster Registeration failed")

				deploymentTarget, err := pdslib.ValidatePDSDeploymentTargetHealthStatus(deploymentTargetID, "healthy")
				log.FailOnError(err, "Error while getting deployment target status")
				dash.VerifyFatal(deploymentTarget.GetStatus(), "healthy", "Validating Deployment Target health status")
			})
		}()

		steplog = "Validate the health status of target cluster"
		Step(steplog, func() {
			log.InfoD("Getting status for Deployment Target ID: %v", deploymentTargetID)
			deploymentTarget, err := pdslib.ValidatePDSDeploymentTargetHealthStatus(deploymentTargetID, "unhealthy")
			log.FailOnError(err, "Error while getting deployment target status")

			dash.VerifyFatal(deploymentTarget.GetStatus(), "unhealthy", "Validating Deployment Target health status")
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()

		if !isDeploymentsDeleted {
			Step("Delete created deployments")
			for _, dep := range deps {
				resp, err := pdslib.DeleteDeployment(*dep.Id)
				log.FailOnError(err, "error while deleting deployments")
				dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
			}
		}

	})
})

var _ = Describe("{ValidatePDSHealthInCaseOfFailures}", func() {
	steplog := "Validate Health of PDS services in case of failures"

	JustBeforeEach(func() {
		StartTorpedoTest("ValidatePDSHealthInCaseOfFailures", steplog, pdsLabels, 0)
	})

	It(steplog, func() {
		tesTimeInterval := 50 * time.Millisecond
		for _, ds := range params.DataServiceToTest {
			Step("Deploy and validate data service", func() {
				isDeploymentsDeleted = false
				deployment, _, _, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
				log.FailOnError(err, "Error while deploying data services")
			})

			pdsPods := make([]corev1.Pod, 0)

			Step("Delete dataservice pods and Check health of data service in PDS Controlplane", func() {
				podList, err := pdslib.GetPods(params.InfraToTest.Namespace)
				log.FailOnError(err, "Error while getting pods")

				for _, pod := range podList.Items {
					if strings.Contains(pod.Name, *deployment.ClusterResourceName) && !strings.Contains(pod.Name, "init") {
						log.Infof("Deployment pod: %v", pod.Name)
						pdsPods = append(pdsPods, pod)
					}
				}

				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					defer GinkgoRecover()
					log.InfoD("Deleting the first data service pod %s", pdsPods[0].GetName())
					err = k8sCore.DeletePod(pdsPods[0].GetName(), params.InfraToTest.Namespace, true)
					log.FailOnError(err, "Error while deleting pod %s", pdsPods[0].GetName())

				}()

				go func() {
					defer wg.Done()
					defer GinkgoRecover()
					log.InfoD("Validating the data service pod status in PDS Control Plane")
					err = pdslib.WaitForPDSDeploymentToBeDown(deployment, tesTimeInterval, timeOut)
					log.FailOnError(err, "Error while validating the pds pods")

				}()
				wg.Wait()

				log.InfoD("Validating if the data service pods are back to healthy state")
				err = pdslib.WaitForPDSDeploymentToBeUp(deployment, tesTimeInterval, timeOut)
				log.FailOnError(err, "Error while validating the pds deployment pods")

				Step("Delete Deployments", func() {
					log.InfoD("Deleting Deployment %v ", *deployment.ClusterResourceName)
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					log.FailOnError(err, "Error while deleting data services")
					dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
					log.InfoD("Getting all PV and associated PVCs and deleting them")
					err = pdslib.DeletePvandPVCs(*deployment.ClusterResourceName, false)
					log.FailOnError(err, "Error while deleting PV and PVCs")
				})

			})
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

var _ = Describe("{RestartPDSagentPod}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RestartPDSagentPod", "Restart pds agent pods and validate if its coming back online and dataserices are not affected", pdsLabels, 0)
	})

	It("Restart pds pods and validate if its coming back online and dataserices are not affected", func() {
		Step("Deploy Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				Step("Deploy and validate data service", func() {
					deployment, _, _, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")
				})

				Step("Delete pods from pds-system namespace", func() {
					log.InfoD("Getting PDS System Pods")
					agentPod := pdslib.GetPDSAgentPods(pdsNamespace)

					var wg sync.WaitGroup
					wg.Add(2)
					go func() {
						defer wg.Done()
						log.InfoD("Deleting PDS agent Pods")
						err = pdslib.DeleteK8sPods(agentPod.Name, pdsNamespace)
						log.FailOnError(err, "Error while deleting pods")
					}()

					go func() {
						defer wg.Done()
						log.InfoD("Validating the dataservice deployment during pds-agent pod downtime")
						err = dsTest.ValidateDataServiceDeployment(deployment, namespace)
						log.FailOnError(err, "Error while validating dataservice during pds-agent downtime")
					}()

					wg.Wait()

					log.Infof("Getting new PDS agent Pod")
					agentPod = pdslib.GetPDSAgentPods(pdsNamespace)

					log.InfoD("Validating new PDS agent Pod")
					err = k8sCore.ValidatePod(&agentPod, 5*time.Minute, 10*time.Second)
					log.FailOnError(err, "pds agent pod failed to comeup")
				})

				Step("Validate Deployments after pods are up", func() {
					log.InfoD("Validate Deployments after pds pods are up")
					err = dsTest.ValidateDataServiceDeployment(deployment, namespace)
					log.FailOnError(err, "Error while validating data services")
					log.InfoD("Deployments pods are up and healthy")
				})

				Step("Delete Deployments", func() {
					log.InfoD("Deleting DataService %v ", ds.Name)
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					log.FailOnError(err, "Error while deleting data services")
					dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
					log.InfoD("Getting all PV and associated PVCs and deleting them")
					err = pdslib.DeletePvandPVCs(*deployment.ClusterResourceName, false)
					log.FailOnError(err, "Error while deleting PV and PVCs")
				})
			}
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

var _ = Describe("{EnableandDisableNamespace}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("EnableandDisableNamespace", "Enables and Disables pds on a namespace multiple times", pdsLabels, 0)
	})

	It("enable/disable namespace multiple times by giving labels to the namespace", func() {
		Step("Enable/Disable PDS Namespace", func() {
			pdsNamespace := "pds" + strconv.Itoa(rand.Int())
			testns, _, err := targetCluster.CreatePDSNamespace(pdsNamespace)
			log.FailOnError(err, "Error while creating pds namespace")
			log.InfoD("PDS Namespace created %v", testns)

			defer func() {
				err := pdslib.DeletePDSNamespace(pdsNamespace)
				log.FailOnError(err, "Error while deleting namespace")
			}()

			// Modifies the namespace multiple times
			for index := 0; index < 5; index++ {
				nsLables := map[string]string{
					pdsNamespaceLabel: "true",
				}
				testns, err = pdslib.UpdatePDSNamespce(pdsNamespace, nsLables)
				log.FailOnError(err, "Error while updating pds namespace")
				log.Infof("PDS Namespace Updated %v", testns)

				//Validate Namespace is available for pds
				err = pdslib.ValidateNamespaces(deploymentTargetID, pdsNamespace, "available")
				log.FailOnError(err, "Error while validating pds namespace")

				nsLables = map[string]string{
					pdsNamespaceLabel: "false",
				}
				testns, err = pdslib.UpdatePDSNamespce(pdsNamespace, nsLables)
				log.FailOnError(err, "Error while updating pds namespace")
				log.Infof("PDS Namespace Updated %v", testns)

				//Validate Namespace is available for pds
				err = pdslib.ValidateNamespaces(deploymentTargetID, pdsNamespace, "unavailable")
				log.FailOnError(err, "Error while validating pds namespace")

			}

		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})

})

var _ = Describe("{ScaleUPDataServices}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("ScaleUPDataServices", "Deploys and Scales Up the dataservices", pdsLabels, 0)
	})

	It("Deploy Dataservices", func() {
		var generateWorkloads = make(map[string]string)
		var deployments = make(map[PDSDataService]*pds.ModelsDeployment)
		var dsVersions = make(map[string]map[string][]string)
		Step("Deploy Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				if ds.Name == zookeeper {
					log.Warnf("Scaling of nodes is not supported for %v dataservice ", ds.Name)
					continue
				}
				Step("Deploy and validate data service", func() {
					isDeploymentsDeleted = false
					deployment, _, dataServiceVersionBuildMap, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")
					deployments[ds] = deployment
					dsVersions[ds.Name] = dataServiceVersionBuildMap
				})
			}

			defer func() {
				for _, newDeployment := range deployments {
					Step("Delete created deployments")
					resp, err := pdslib.DeleteDeployment(newDeployment.GetId())
					log.FailOnError(err, "Error while deleting data services")
					dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
				}
			}()

			Step("Running Workloads before scaling up of dataservices ", func() {
				for ds, deployment := range deployments {
					if Contains(dataServicePodWorkloads, ds.Name) || Contains(dataServiceDeploymentWorkloads, ds.Name) {
						log.InfoD("Running Workloads on DataService %v ", ds.Name)
						var params pdslib.WorkloadGenerationParams
						pod, dep, err = RunWorkloads(params, ds, deployment, namespace)
						log.FailOnError(err, fmt.Sprintf("Error while genearating workloads for dataservice [%s]", ds.Name))
						if dep == nil {
							generateWorkloads[ds.Name] = pod.Name
						} else {
							generateWorkloads[ds.Name] = dep.Name
						}
						for dsName, workloadContainer := range generateWorkloads {
							log.Debugf("dsName %s, workloadContainer %s", dsName, workloadContainer)
						}
					}
				}
			})

			defer func() {
				for dsName, workloadContainer := range generateWorkloads {
					Step("Delete the workload generating deployments", func() {
						if Contains(dataServiceDeploymentWorkloads, dsName) {
							log.InfoD("Deleting Workload Generating deployment %v ", workloadContainer)
							err = pdslib.DeleteK8sDeployments(workloadContainer, namespace)
						} else if Contains(dataServicePodWorkloads, dsName) {
							log.InfoD("Deleting Workload Generating pod %v ", workloadContainer)
							err = pdslib.DeleteK8sPods(workloadContainer, namespace)
						}
						log.FailOnError(err, "error deleting workload generating pods")
					})
				}
			}()

			Step("Validate Deployments before scale up", func() {
				for ds, deployment := range deployments {
					err = dsTest.ValidateDataServiceDeployment(deployment, namespace)
					log.FailOnError(err, "Error while validating dataservices")
					log.InfoD("Data-service: %v is up and healthy", ds.Name)
				}
			})

			Step("Scaling up the dataservice replicas", func() {
				for ds, deployment := range deployments {
					log.InfoD("Scaling up DataService %v ", ds.Name)

					dataServiceDefaultAppConfigID, err = controlPlane.GetAppConfTemplate(tenantID, ds.Name)
					log.FailOnError(err, "Error while getting app configuration template")
					dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

					dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
					log.FailOnError(err, "Error while getting resource setting template")
					dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

					updatedDeployment, err := pdslib.UpdateDataServices(deployment.GetId(),
						dataServiceDefaultAppConfigID, deployment.GetImageId(),
						int32(ds.ScaleReplicas), dataServiceDefaultResourceTemplateID, namespace)
					log.FailOnError(err, "Error while updating dataservices")

					err = dsTest.ValidateDataServiceDeployment(updatedDeployment, namespace)
					log.FailOnError(err, "Error while validating data service deployment")

					_, _, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, "error on ValidateDataServiceVolumes method")
					dash.VerifyFatal(int32(ds.ScaleReplicas), config.Spec.Nodes, "Validating replicas after scaling up of dataservice")
				}
			})
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

var _ = Describe("{RunIndependentAppNonPdsNS}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RunIndependentAppNonPdsNS", "Runs an independent app on a non-PDS namespace and then enables PDS on this namespace", pdsLabels, 0)
	})
	ns := ""
	var err error
	var podName string

	It("Create an independent app in a non PDS namespace and then enable PDS on this namespace", func() {
		Step("Create a temporary namespace on the cluster for creating an independent app", func() {
			ns, err = pdslib.CreateTempNS(6)
			log.FailOnError(err, "Failure in creating namespace on the target cluster. Exiting the Test case with failure")
			log.InfoD("Namespace %s created for creating a Non-PDS App", ns)
		})
		Step("Create an Independent app in a non-PDS namespace", func() {
			_, pv_creation_err := pdslib.CreateIndependentPV("mysql-pv-" + ns)
			if pv_creation_err == nil {
				_, pvc_creation_err := pdslib.CreateIndependentPVC(ns, "mysql-pvc-"+ns)
				if pvc_creation_err == nil {
					_, podName, err = pdslib.CreateIndependentMySqlApp(ns, "mysql-app-"+ns, "mysql:8.0", "mysql-pvc-"+ns)
					log.FailOnError(err, "Failure in creating the application in non-pds namespace")
					log.InfoD("Non PDS MySQL App with name : %s is created", podName)
				} else {
					log.FailOnError(pvc_creation_err, "Failure in creating PVC on Target Cluster. Exiting the Test case with failure")
				}
			} else {
				log.FailOnError(pv_creation_err, "Failure in creating Persistent Volume on target cluster. Exiting the Test case with failure")
			}
		})
		Step("Add PDS Label to Non-PDS Namespace running a DB Service already", func() {
			nsLables := map[string]string{
				pdsNamespaceLabel: "true",
			}
			testns, err := pdslib.UpdatePDSNamespce(ns, nsLables)
			log.FailOnError(err, "Error while updating pds namespace")
			log.InfoD("PDS Namespace Updated with PDS Label %v", testns)
		})
		Step("Deploy, Validate and Delete Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				log.InfoD("Deploying DataService %v ", ds.Name)
				isDeploymentsDeleted = false
				deployment, _, _, err = DeployandValidateDataServices(ds, ns, tenantID, projectID)
				log.FailOnError(err, fmt.Sprintf("Error while deploying data services %s", ds.Name))
				log.InfoD("Data service %v deployed successfully", ds.Name)

				Step("Delete Deployments", func() {
					log.InfoD("Deleting DataService %v ", ds.Name)
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					log.FailOnError(err, "Error while deleting data services")
					dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
					log.InfoD("Getting all PV and associated PVCs and deleting them")
					err = pdslib.DeletePvandPVCs(*deployment.ClusterResourceName, false)
					log.FailOnError(err, "Error while deleting PV and PVCs")
				})
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()

		log.InfoD("Trying to Delete Independent App pod now : %s", podName)
		err = pdslib.DeleteK8sPods(podName, ns)
		log.FailOnError(err, "Error while deleting K8s pods")
		log.InfoD("Trying to Delete Independent PVC now from ns : %s", ns)
		err = k8sCore.DeletePersistentVolumeClaim("mysql-pvc-"+ns, ns)
		log.FailOnError(err, "Error while deleting Independent PVC")
		log.InfoD("Trying to delete Independent PV now")
		err = k8sCore.DeletePersistentVolume("mysql-pv-" + ns)
		log.FailOnError(err, "Error while deleting Independent PV")
		log.InfoD("Trying to delete NS now: %s", ns)
		err = pdslib.DeletePDSNamespace(ns)
		log.FailOnError(err, "Error while deleting Independent Namespace")

	})
})

var _ = Describe("{RunTpccWorkloadOnDataServices}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RunTpccWorkloadOnDataServices", "Runs TPC-C Workload on Postgres and MySQL Deployment", pdsLabels, 0)
	})

	It("Deploy , Validate and start TPCC Workload", func() {
		for _, ds := range params.DataServiceToTest {
			if ds.Name == postgresql {
				log.InfoD("Deploying, Validating and Running TPCC Workload on %v Data Service ", ds.Name)
				deployAndTriggerTpcc(ds.Name, ds.Version, ds.Image, ds.Version, ds.Image, int32(ds.Replicas))
			}
			if ds.Name == mysql {
				log.InfoD("Deploying, Validating and Running TPCC Workload on %v Data Service ", ds.Name)
				deployAndTriggerTpcc(ds.Name, ds.Version, ds.Image, ds.Version, ds.Image, int32(ds.Replicas))
			}
		}
	})
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

// This module deploys a data service, validates it, Prepares the data service with 4 districts and 2 warehouses
// and runs TPCC Workload for a default time of 2 minutes
func deployAndTriggerTpcc(dataservice, Version, Image, dsVersion, dsBuild string, replicas int32) {
	Step("Deploy and Validate Data Service and run TPCC Workload", func() {
		isDeploymentsDeleted = false
		dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, dataservice)
		log.FailOnError(err, "Error while getting resource template")
		log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

		dataServiceDefaultAppConfigID, err = controlPlane.GetAppConfTemplate(tenantID, dataservice)
		dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

		log.InfoD(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)
		log.InfoD("Deploying DataService %v ", dataservice)
		deployment, _, dataServiceVersionBuildMap, err = dsTest.DeployDS(dataservice, projectID,
			deploymentTargetID,
			dnsZone,
			deploymentName,
			namespaceID,
			dataServiceDefaultAppConfigID,
			replicas,
			serviceType,
			dataServiceDefaultResourceTemplateID,
			storageTemplateID,
			Version,
			Image,
			namespace,
		)
		log.FailOnError(err, "Error while deploying data services")
		err = dsTest.ValidateDataServiceDeployment(deployment, namespace)
		log.FailOnError(err, fmt.Sprintf("Error while validating dataservice deployment %v", *deployment.ClusterResourceName))

		Step("Validate Storage Configurations", func() {
			resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, dataservice, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
			log.FailOnError(err, "error on ValidateDataServiceVolumes method")
			ValidateDeployments(resourceTemp, storageOp, config, int(replicas), dataServiceVersionBuildMap)
		})

		Step("Running TPCC Workloads - ", func() {
			if dataservice == postgresql {
				deploymentName := "pg-tpcc"
				tpccRunResult, _ := pdslib.CreateTpccWorkloads(dataservice, deployment.GetId(), "100", "1", deploymentName, namespace)
				if !tpccRunResult {
					dash.VerifyFatal(tpccRunResult, true, "Validating if Postgres TPCC Run was successful or not")
				}
			}
			if dataservice == mysql {
				deploymentName := "my-tpcc"
				tpccRunResult, _ := pdslib.CreateTpccWorkloads(dataservice, deployment.GetId(), "100", "1", deploymentName, namespace)
				if !tpccRunResult {
					dash.VerifyFatal(tpccRunResult, true, "Validating if MySQL TPCC Run was successful or not")
				}
			}
		})
		Step("Delete Deployments", func() {
			log.InfoD("Deleting DataService")
			resp, err := pdslib.DeleteDeployment(deployment.GetId())
			log.FailOnError(err, "Error while deleting data services")
			dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
			log.InfoD("Getting all PV and associated PVCs and deleting them")
			err = pdslib.DeletePvandPVCs(*deployment.ClusterResourceName, false)
			log.FailOnError(err, "Error while deleting PV and PVCs")
		})
	})
}

var _ = Describe("{UpgradeDataServiceVersion}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("UpgradeDataServiceVersion", "Upgrades the dataservice version", pdsLabels, 0)
	})

	It("runs the dataservice version upgrade test", func() {
		for _, ds := range params.DataServiceToTest {
			log.InfoD("Running UpgradeDataServiceVersion test for DataService %v ", ds.Name)
			UpgradeDataService(ds.Name, ds.OldVersion, ds.OldImage, ds.Version, ds.Image, int32(ds.Replicas), ds)
		}
	})
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe("{UpgradeDataServiceImage}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("UpgradeDataServiceImage", "Upgrades the dataservice image", pdsLabels, 0)
	})

	It("runs the dataservice build image upgrade test", func() {
		for _, ds := range params.DataServiceToTest {
			log.InfoD("Running UpgradeDataServiceImage test for DataService %v ", ds.Name)
			UpgradeDataService(ds.Name, ds.Version, ds.OldImage, ds.Version, ds.Image, int32(ds.Replicas), ds)
		}
	})
	JustAfterEach(func() {
		defer func() {
			if !isDeploymentsDeleted {
				Step("Delete created deployments")
				resp, err := pdslib.DeleteDeployment(deployment.GetId())
				log.FailOnError(err, "Error while deleting data services")
				dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
			}
		}()

		defer EndTorpedoTest()
	})
})

var _ = Describe("{DrainAndDecommissionNode}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("DrainAndDecommissionNode", "Deploys a data service, drains one selected node, decommissions that node", pdsLabels, 0)
	})

	It("Drain and Decommission a node", func() {
		Step("Deploy, Validate, Drain Node, Cordon Node, Validate Data Service, Run Workload on Data Service, Uncordon Node", func() {
			var nodeName string
			var old_deployment *pds.ModelsDeployment
			for _, ds := range params.DataServiceToTest {
				isDeploymentsDeleted = false
				deployment, _, _, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
				log.FailOnError(err, fmt.Sprintf("Error while deploying data services %s", ds.Name))
				old_deployment = deployment
				log.InfoD("Running Check Node DataService Func %v ", ds.Name)
				nodes, err := pdslib.GetNodesOfSS(*deployment.ClusterResourceName, namespace)
				log.FailOnError(err, "Cannot fetch nodes of the running Data Service")
				nodeName = nodes[0].Name // Selecting the 1st node in the list to cordon
				Step("Drain Pods from a node", func() {
					podsList, err := pdslib.GetPodsOfSsByNode(*deployment.ClusterResourceName, nodeName, namespace)
					log.FailOnError(err, fmt.Sprintf("Pod not found on this Node : %s", nodeName))
					log.InfoD("Pods found on %v node. Trying to Drain pods from this node now.", nodeName)
					err = k8sCore.DrainPodsFromNode(nodeName, podsList, timeOut, maxtimeInterval)
					log.FailOnError(err, fmt.Sprintf("Draining pod from the node %s failed", nodeName))
					log.InfoD("Pods successfully drained from the node %s", nodeName)
				})
				Step("Validate Data Service to see if Pods have rescheduled on another node", func() {
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, fmt.Sprintf("error on ValidateDataServiceVolumes method for %v", *deployment.ClusterResourceName))
					ValidateDeployments(resourceTemp, storageOp, config, ds.Replicas, dataServiceVersionBuildMap)
				})
				Step("Validate no pods are on the cordoned node anymore", func() {
					nodes, err := pdslib.GetNodesOfSS(*deployment.ClusterResourceName, namespace)
					log.FailOnError(err, fmt.Sprintf("Cannot fetch nodes of the running Data Service %v", *deployment.ClusterResourceName))
					for _, nodeObj := range nodes {
						if nodeObj.Name == nodeName {
							log.FailOnError(errors.New("New Pod came up on the node that was cordoned."), "Unexpected error")
						}
					}
					log.InfoD("The pods of the Stateful Set %v are not on the cordoned node. Moving ahead now.", *deployment.ClusterResourceName)
				})
				Step("Create a new Data Service Deployment and validate this doesn't get created on the Cordoned Node. Delete it as well.", func() {
					newDeployment, _, _, err := DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
					log.FailOnError(err, fmt.Sprintf("Error while deploying data services %s", ds.Name))
					nodes, err := pdslib.GetNodesOfSS(*newDeployment.ClusterResourceName, namespace)
					log.FailOnError(err, fmt.Sprintf("Cannot fetch nodes of the running Data Service %v", *newDeployment.ClusterResourceName))
					for _, nodeObj := range nodes {
						if nodeObj.Name == nodeName {
							log.FailOnError(errors.New("New Pod came up on the node that was cordoned."), "Unexpected error")
						}
					}
					log.InfoD("The pods of the Stateful Set %v are not on the cordoned node.", *newDeployment.ClusterResourceName)
					resp, err := pdslib.DeleteDeployment(newDeployment.GetId())
					log.FailOnError(err, "Error while deleting data services")
					dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
					deployment = old_deployment
					log.InfoD("Getting all PV and associated PVCs and deleting them")
					err = pdslib.DeletePvandPVCs(*deployment.ClusterResourceName, false)
					log.FailOnError(err, "Error while deleting PV and PVCs")
				})
				Step("Running Workloads before scaling up of dataservices ", func() {
					log.InfoD("Running Workloads on DataService %v ", ds.Name)
					var params pdslib.WorkloadGenerationParams
					pod, dep, err = RunWorkloads(params, ds, deployment, namespace)
					log.FailOnError(err, fmt.Sprintf("Error while generating workloads for dataservice [%s]", ds.Name))
					defer func() {
						Step("Delete the workload generating deployments", func() {
							if Contains(dataServiceDeploymentWorkloads, ds.Name) {
								log.Infof("Deleting Workload Generating pods %v ", dep.Name)
								err = pdslib.DeleteK8sDeployments(dep.Name, namespace)
							} else if Contains(dataServicePodWorkloads, ds.Name) {
								log.Infof("Deleting Workload Generating pods %v ", pod.Name)
								err = pdslib.DeleteK8sPods(pod.Name, namespace)
							}
							log.FailOnError(err, "error deleting workload generating pods for ds %s", ds.Name)
							log.Infof("Workload generating deployments deleted successfully")
						})
					}()
				})
				Step("UnCordon Selected Node", func() {
					err = k8sCore.UnCordonNode(nodeName, timeOut, maxtimeInterval)
					log.FailOnError(err, fmt.Sprintf("UnCordoning the node %s Failed", nodeName))
					log.InfoD("Node %s successfully UnCordoned", nodeName)
				})
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		if !isDeploymentsDeleted {
			Step("Delete created deployments")
			resp, err := pdslib.DeleteDeployment(deployment.GetId())
			log.FailOnError(err, "Error while deleting data services")
			dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
		}
	})
})

var _ = Describe("{CordonNodeAndDeletePod}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("CordonNodeAndDeletePod", "Deploys a data service, cordons one selected node, deletes the pod from that node", pdsLabels, 0)
	})

	It("Cordon a Node and Delete Pods from that node", func() {
		Step("Deploy, Validate, Cordon Node and Delete Pods, Validate new Pod, Validate Storage, Run Workload on Data Service", func() {
			for _, ds := range params.DataServiceToTest {
				isDeploymentsDeleted = false
				deployment, _, _, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
				log.FailOnError(err, fmt.Sprintf("Error while deploying data services %s", ds.Name))
				log.InfoD("Running Check Node DataService Func %v ", ds.Name)
				nodes, err := pdslib.GetNodesOfSS(*deployment.ClusterResourceName, namespace)
				log.FailOnError(err, "Cannot fetch nodes of the running Data Service")
				var nodeName string
				Step("Delete Pods and Cordon a node", func() {
					nodeName = nodes[0].Name // Selecting the 1st node in the list to cordon
					podsList, err := pdslib.GetPodsOfSsByNode(*deployment.ClusterResourceName, nodeName, namespace)
					log.FailOnError(err, fmt.Sprintf("Pod not found on this Node : %s", nodeName))
					log.InfoD("Pods found on %v node. Trying to Cordon and Drain pods from this node now.", nodeName)
					err = k8sCore.DrainPodsFromNode(nodeName, podsList, timeOut, maxtimeInterval)
					log.FailOnError(err, fmt.Sprintf("Draining pod from the node %s failed", nodeName))
				})
				Step("Validate Data Service Again", func() {
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, fmt.Sprintf("error on ValidateDataServiceVolumes method for %v", *deployment.ClusterResourceName))
					ValidateDeployments(resourceTemp, storageOp, config, ds.Replicas, dataServiceVersionBuildMap)
				})
				Step("Validate no pods are on the cordoned node anymore", func() {
					nodes, err := pdslib.GetNodesOfSS(*deployment.ClusterResourceName, namespace)
					log.FailOnError(err, fmt.Sprintf("Cannot fetch nodes of the running Data Service %v", *deployment.ClusterResourceName))
					for _, nodeObj := range nodes {
						if nodeObj.Name == nodeName {
							log.FailOnError(errors.New("New Pod came up on the node that was cordoned."), "Unexpected error")
						}
					}
					log.InfoD("The pods of the Stateful Set %v are not on the cordoned node. Moving ahead now.", *deployment.ClusterResourceName)
				})
				Step("Running Workloads before scaling up of dataservices ", func() {
					log.InfoD("Running Workloads on DataService %v ", ds.Name)
					var params pdslib.WorkloadGenerationParams
					pod, dep, err = RunWorkloads(params, ds, deployment, namespace)
					log.FailOnError(err, fmt.Sprintf("Error while generating workloads for dataservice [%s]", ds.Name))
					defer func() {
						Step("Delete the workload generating deployments", func() {
							if Contains(dataServiceDeploymentWorkloads, ds.Name) {
								log.InfoD("Deleting Workload Generating pods %v ", dep.Name)
								err = pdslib.DeleteK8sDeployments(dep.Name, namespace)
							} else if Contains(dataServicePodWorkloads, ds.Name) {
								log.InfoD("Deleting Workload Generating pods %v ", pod.Name)
								err = pdslib.DeleteK8sPods(pod.Name, namespace)
							}
							log.FailOnError(err, "error deleting workload generating pods for ds %s", ds.Name)
						})
					}()
				})
				Step("UnCordon Selected Node", func() {
					err = k8sCore.UnCordonNode(nodeName, timeOut, maxtimeInterval)
					log.FailOnError(err, fmt.Sprintf("UnCordoning the node %s Failed", nodeName))
					log.InfoD("Node %s successfully UnCordoned", nodeName)
				})
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()

		defer func() {
			if !isDeploymentsDeleted {
				Step("Delete created deployments")
				resp, err := pdslib.DeleteDeployment(deployment.GetId())
				log.FailOnError(err, "Error while deleting data services")
				dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
			}
		}()
	})
})

var _ = Describe("{DeployDataServicesOnDemand}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("DeployDataServicesOnDemand", "Deploys DataServices", pdsLabels, 0)
	})

	It("Deploy DataservicesOnDemand", func() {
		log.Info("Create dataservices without backup.")
		Step("Deploy, Validate and Delete Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				Step("Deploy and validate data service", func() {
					isDeploymentsDeleted = false
					deployment, _, _, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")
				})

				Step("Delete Deployments", func() {
					log.InfoD("Deleting DataService %v ", ds.Name)
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					log.FailOnError(err, "Error while deleting data services")
					dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
					log.InfoD("Getting all PV and associated PVCs and deleting them")
					err = pdslib.DeletePvandPVCs(*deployment.ClusterResourceName, false)
					log.FailOnError(err, "Error while deleting PV and PVCs")
				})
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

var _ = Describe("{DeployAllDataServices}", func() {

	JustBeforeEach(func() {
		Step("Get All Supported Dataservices and Versions", func() {
			supportedDataServicesNameIDMap = pdslib.GetAllSupportedDataServices()
			for dsName := range supportedDataServicesNameIDMap {
				supportedDataServices = append(supportedDataServices, dsName)
			}
			for index := range supportedDataServices {
				log.Infof("supported data service %v ", supportedDataServices[index])
			}
			Step("Get the resource and app config template for supported dataservice", func() {
				dataServiceDefaultResourceTemplateIDMap, dataServiceNameIDMap, err = pdslib.GetAllDataserviceResourceTemplate(tenantID, supportedDataServices)
				Expect(err).NotTo(HaveOccurred())
				Expect(dataServiceDefaultResourceTemplateIDMap).NotTo(BeEmpty())
				Expect(dataServiceNameIDMap).NotTo(BeEmpty())

				dataServiceNameDefaultAppConfigMap, err = pdslib.GetAllDataServiceAppConfTemplate(tenantID, dataServiceNameIDMap)
				Expect(err).NotTo(HaveOccurred())
				Expect(dataServiceNameDefaultAppConfigMap).NotTo(BeEmpty())
			})
		})
	})

	It("Deploy All SupportedDataServices", func() {
		Step("Deploy All Supported Data Services", func() {
			replicas = 3
			log.InfoD("Deploying All Supported DataService")
			deployments, _, _, err := pdslib.DeployAllDataServices(supportedDataServicesNameIDMap, projectID,
				deploymentTargetID,
				dnsZone,
				deploymentName,
				namespaceID,
				dataServiceNameDefaultAppConfigMap,
				replicas,
				serviceType,
				dataServiceDefaultResourceTemplateIDMap,
				storageTemplateID,
				namespace,
			)
			Expect(err).NotTo(HaveOccurred())
			Step("Validate Storage Configurations", func() {
				for ds, deployment := range deployments {
					for index := range deployment {
						log.Infof("data service deployed %v ", ds)
						resourceTemp, storageOp, config, err := pdslib.ValidateAllDataServiceVolumes(deployment[index], ds, dataServiceDefaultResourceTemplateIDMap, storageTemplateID)
						Expect(err).NotTo(HaveOccurred())
						log.Infof("filesystem used %v ", config.Spec.StorageOptions.Filesystem)
						log.Infof("storage replicas used %v ", config.Spec.StorageOptions.Replicas)
						log.Infof("cpu requests used %v ", config.Spec.Resources.Requests.CPU)
						log.Infof("memory requests used %v ", config.Spec.Resources.Requests.Memory)
						log.Infof("storage requests used %v ", config.Spec.Resources.Requests.Storage)
						log.Infof("No of nodes requested %v ", config.Spec.Nodes)
						log.Infof("volume group %v ", storageOp.VolumeGroup)

						Expect(resourceTemp.Resources.Requests.CPU).Should(Equal(config.Spec.Resources.Requests.CPU))
						Expect(resourceTemp.Resources.Requests.Memory).Should(Equal(config.Spec.Resources.Requests.Memory))
						Expect(resourceTemp.Resources.Requests.Storage).Should(Equal(config.Spec.Resources.Requests.Storage))
						Expect(resourceTemp.Resources.Limits.CPU).Should(Equal(config.Spec.Resources.Limits.CPU))
						Expect(resourceTemp.Resources.Limits.Memory).Should(Equal(config.Spec.Resources.Limits.Memory))
						repl, err := strconv.Atoi(config.Spec.StorageOptions.Replicas)
						Expect(err).NotTo(HaveOccurred())
						Expect(storageOp.Replicas).Should(Equal(int32(repl)))
						Expect(storageOp.Filesystem).Should(Equal(config.Spec.StorageOptions.Filesystem))
						Expect(config.Spec.Nodes).Should(Equal(replicas))
					}
				}
			})
			defer func() {
				Step("Delete created deployments")
				for _, dep := range deployments {
					for index := range dep {
						_, err := pdslib.DeleteDeployment(dep[index].GetId())
						Expect(err).NotTo(HaveOccurred())
					}
				}
			}()
		})
	})
})

func DeployandValidateDataServices(ds dataservice.PDSDataService, namespace, tenantID, projectID string) (*pds.ModelsDeployment, map[string][]string, map[string][]string, error) {
	log.InfoD("Data Service Deployment Triggered")
	log.InfoD("Deploying ds in namespace %v and servicetype is %v", namespace, serviceType)
	deployment, dataServiceImageMap, dataServiceVersionBuildMap, err := dsTest.TriggerDeployDataService(ds, namespace, tenantID, projectID, false,
		dataservice.TestParams{StorageTemplateId: storageTemplateID, DeploymentTargetId: deploymentTargetID, DnsZone: dnsZone, ServiceType: serviceType})
	log.FailOnError(err, "Error occured while deploying data service %s", ds.Name)
	Step("Validate Data Service Deployments", func() {
		err = dsTest.ValidateDataServiceDeployment(deployment, namespace)
		log.FailOnError(err, fmt.Sprintf("Error while validating dataservice deployment %v", *deployment.ClusterResourceName))
	})
	Step("Validate Storage Configurations", func() {
		dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
		log.FailOnError(err, "Error while getting resource template")
		log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)
		resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
		log.FailOnError(err, "error on ValidateDataServiceVolumes method")
		ValidateDeployments(resourceTemp, storageOp, config, ds.Replicas, dataServiceVersionBuildMap)
	})
	return deployment, dataServiceImageMap, dataServiceVersionBuildMap, err
}

func UpgradeDataService(dataservice, oldVersion, oldImage, dsVersion, dsBuild string, replicas int32, ds PDSDataService) {
	Step("Deploy, Validate and Update Data Services", func() {
		isDeploymentsDeleted = false
		dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, dataservice)
		log.FailOnError(err, "Error while getting resource template")
		log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

		dataServiceDefaultAppConfigID, err = controlPlane.GetAppConfTemplate(tenantID, dataservice)
		log.FailOnError(err, "Error while getting app configuration template")
		dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

		log.InfoD(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)
		log.InfoD("Deploying DataService %v ", dataservice)
		deployment, _, dataServiceVersionBuildMap, err = dsTest.DeployDS(dataservice, projectID,
			deploymentTargetID,
			dnsZone,
			deploymentName,
			namespaceID,
			dataServiceDefaultAppConfigID,
			replicas,
			serviceType,
			dataServiceDefaultResourceTemplateID,
			storageTemplateID,
			oldVersion,
			oldImage,
			namespace,
		)
		log.FailOnError(err, "Error while deploying data services")
		err = dsTest.ValidateDataServiceDeployment(deployment, namespace)
		log.FailOnError(err, fmt.Sprintf("Error while validating dataservice deployment %v", *deployment.ClusterResourceName))
	})

	log.Debugf("map before deletion %v", dataServiceVersionBuildMap)

	Step("Validate Storage Configurations", func() {
		resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, dataservice, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
		log.FailOnError(err, "error on ValidateDataServiceVolumes method")
		ValidateDeployments(resourceTemp, storageOp, config, int(replicas), dataServiceVersionBuildMap)
	})

	for version := range dataServiceVersionBuildMap {
		delete(dataServiceVersionBuildMap, version)
	}

	log.Debugf("map post deletion %v", dataServiceVersionBuildMap)

	for version, build := range dataServiceVersionBuildMap {
		log.Debugf("version :%s   build:%s", version, build)
	}

	Step("Running Workloads before scaling up of dataservices ", func() {
		var params pdslib.WorkloadGenerationParams
		if Contains(dataServicePodWorkloads, ds.Name) || Contains(dataServiceDeploymentWorkloads, ds.Name) {
			pod, dep, err = RunWorkloads(params, ds, deployment, namespace)
			log.FailOnError(err, "Error while genearating workloads")
		}

	})

	defer func() {
		Step("Delete the workload generating deployments", func() {
			if Contains(dataServiceDeploymentWorkloads, ds.Name) {
				log.InfoD("Deleting Workload Generating pods %v ", dep.Name)
				err = pdslib.DeleteK8sDeployments(dep.Name, namespace)
			} else if Contains(dataServicePodWorkloads, ds.Name) {
				log.InfoD("Deleting Workload Generating pods %v ", pod.Name)
				err = pdslib.DeleteK8sPods(pod.Name, namespace)
			}
			log.FailOnError(err, "error deleting workload generating pods for ds %s", dataservice)
		})
	}()

	Step("Update the data service patch versions", func() {
		log.Infof("Version/Build: %v %v", dsVersion, dsBuild)
		updatedDeployment, err := pdslib.UpdateDataServiceVerison(deployment.GetDataServiceId(), deployment.GetId(),
			dataServiceDefaultAppConfigID,
			replicas, dataServiceDefaultResourceTemplateID, dsBuild, dsVersion)
		log.FailOnError(err, "Error while updating data services")
		log.InfoD("data service deployed %v ", dataservice)

		err = dsTest.ValidateDataServiceDeployment(updatedDeployment, namespace)
		log.FailOnError(err, "error while Validating DataService Deployment")

		resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, dataservice, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
		log.FailOnError(err, "error on ValidateDataServiceVolumes method")

		id := pdslib.GetDataServiceID(dataservice)
		dash.VerifyFatal(id != "", true, "Validating dataservice id")
		log.Infof("Getting versionID  for Data service version %s and buildID for %s ", dsVersion, dsBuild)

		_, _, dsVersionBuildMap, err := pdslib.GetVersionsImage(dsVersion, dsBuild, id)
		log.FailOnError(err, "Error while fetching versions/image information")

		log.Debugf("Newly generated map %v", dsVersionBuildMap)

		for version, build := range dsVersionBuildMap {
			log.Debugf("New version :%s   build:%s", version, build)
		}

		ValidateDeployments(resourceTemp, storageOp, config, int(replicas), dsVersionBuildMap)
		dash.VerifyFatal(config.Spec.Version, dsVersion+"-"+dsBuild, "validating ds build and version")
	})

	Step("Delete Deployments", func() {
		resp, err := pdslib.DeleteDeployment(deployment.GetId())
		log.FailOnError(err, "Error while deleting data services")
		dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
		log.InfoD("Getting all PV and associated PVCs and deleting them")
		err = pdslib.DeletePvandPVCs(*deployment.ClusterResourceName, false)
		log.FailOnError(err, "Error while deleting PV and PVCs")
	})

}

var _ = Describe("{DeployMultipleNamespaces}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("DeployMultipleNamespaces", "Create multiple namespaces and deploy all dataservices", pdsLabels, 0)
	})

	It("Creates multiple namespaces, deploys in each namespace", func() {

		var namespaces []*corev1.Namespace
		// create k8s namespaces
		for i := 0; i < 2; i++ {
			nname := "mulnamespace-" + strconv.Itoa(rand.Int())
			ns, err := pdslib.CreateK8sPDSNamespace(nname)
			log.InfoD("Created namespace: %v", nname)
			log.FailOnError(err, "error while creating namespace")
			namespaces = append(namespaces, ns)
		}
		isNamespacesDeleted = false

		defer func() {
			if !isNamespacesDeleted {
				Step("Delete created namespaces", func() {
					for _, namespace := range namespaces {
						log.InfoD("Cleanup: Deleting created namespace %v", namespace.Name)
						err := pdslib.DeleteK8sPDSNamespace(namespace.Name)
						log.FailOnError(err, "error while deleting namespaces")
					}
				})
			}
		}()
		log.InfoD("Waiting for created namespaces to be available in PDS")
		time.Sleep(10 * time.Second)

		Step("Deploy All Supported Data Services", func() {
			var cleanupall []*pds.ModelsDeployment
			for _, namespace := range namespaces {
				log.InfoD("Deploying dataservices in namespace: %v", namespace.Name)
				deps := DeployInANamespaceAndVerify(namespace.Name)
				cleanupall = append(cleanupall, deps...)
			}

			log.InfoD("List of created deployments")
			for _, dep := range cleanupall {
				log.InfoD("%v ", *dep.ClusterResourceName)
			}

			Step("Delete created deployments", func() {
				for _, dep := range cleanupall {
					_, err := pdslib.DeleteDeployment(*dep.Id)
					log.FailOnError(err, "error while deleting deployments")
					log.InfoD("Getting all PV and associated PVCs and deleting them")
					err = pdslib.DeletePvandPVCs(*dep.ClusterResourceName, false)
					log.FailOnError(err, "Error while deleting PV and PVCs")
				}
			})

			Step("Delete created namespaces", func() {
				for _, namespace := range namespaces {
					log.InfoD("Cleanup: Deleting created namespace %v", namespace.Name)
					err := pdslib.DeleteK8sPDSNamespace(namespace.Name)
					log.FailOnError(err, "error while deleting pds namespace")
				}
				isNamespacesDeleted = true
			})

		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

var _ = Describe("{DeletePDSEnabledNamespace}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("DeletePDSEnabledNamespace", "Create a namespace, deploy dataservices, delete the namespace and validate", pdsLabels, 0)
	})

	It("Deploy Dataservices and delete namespace", func() {

		nname := "namespace-" + strconv.Itoa(rand.Int())
		_, err := pdslib.CreateK8sPDSNamespace(nname)
		log.FailOnError(err, "error while creating pds namespace")
		isNamespacesDeleted = false
		log.InfoD("Created namespace: %v", nname)
		log.InfoD("Waiting for created namespaces to be available in PDS")
		time.Sleep(10 * time.Second)
		log.InfoD("Create dataservices")

		Step("Deploy All Supported Data Services in the namespace", func() {

			log.InfoD("Deploying deployment %v in namespace: %v", deploymentTargetID, nname)
			newNamespaceID, err := targetCluster.GetnameSpaceID(nname, deploymentTargetID)
			log.FailOnError(err, "error while getting namespaceid")
			Expect(newNamespaceID).NotTo(BeEmpty())

			var cleanup []*pds.ModelsDeployment
			for _, ds := range params.DataServiceToTest {
				Step("Deploy and validate data service", func() {
					isDeploymentsDeleted = false
					deployment, _, _, err = DeployandValidateDataServices(ds, nname, tenantID, projectID)
					log.FailOnError(err, fmt.Sprintf("Error while deploying data services - %v", ds.Name))
					cleanup = append(cleanup, deployment)
				})
			}
			log.InfoD("List of created deployments")
			for _, deployment := range cleanup {
				log.InfoD("%s ", deployment.GetClusterResourceName())
			}

			Step("Delete created namespace", func() {
				log.InfoD("Cleanup: Deleting created namespace %v", nname)
				err := pdslib.DeleteK8sPDSNamespace(nname)
				log.FailOnError(err, "Error while deleting namespace")
			})

			Step("Verify that the namespace was deleted", func() {
				err := pdslib.ValidateK8sNamespaceDeleted(nname)
				log.FailOnError(err, "Error while validating namespace deletion")
				isNamespacesDeleted = true
			})

			Step("Verify created deployments have been deleted", func() {
				for _, dep := range cleanup {
					err := pdslib.ValidateDataServiceDeploymentNegative(dep, nname)
					log.FailOnError(err, "Error while cleaning up data services")
					isDeploymentsDeleted = true
				}
			})

		})

	})

	JustAfterEach(func() {
		defer func() {
			if !isDeploymentsDeleted {
				Step("Delete created deployments")
				resp, err := pdslib.DeleteDeployment(deployment.GetId())
				log.FailOnError(err, "Error while deleting data services")
				dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
			}
		}()

		defer func() {
			if !isNamespacesDeleted {
				err := pdslib.DeleteK8sPDSNamespace("test-namespace-0")
				log.FailOnError(err, "Error while deleting namespace")
			}
		}()

		defer EndTorpedoTest()
	})
})

var _ = Describe("{RestartPXPods}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RestartPXPods", "Deploy dataservice, stop px service on the nodes where the dataservice is deployed, validate", pdsLabels, 0)
	})

	It("Deploy Dataservices", func() {
		log.Info("Create dataservices without backup.")
		Step("Deploy PDS Data Service", func() {
			for _, ds := range params.DataServiceToTest {
				Step("Deploy and validate data service", func() {
					isDeploymentsDeleted = false
					deployment, _, _, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")
				})

				Step("Running Workloads before scaling up of dataservices ", func() {
					var params pdslib.WorkloadGenerationParams
					pod, dep, err = RunWorkloads(params, ds, deployment, namespace)
					log.FailOnError(err, "Error while genearating workloads")
				})

				defer func() {
					Step("Delete the workload generating deployments", func() {
						if !(ds.Name == mysql || ds.Name == kafka || ds.Name == zookeeper || ds.Name == mongodb) {
							if ds.Name == cassandra || ds.Name == postgresql {
								err = pdslib.DeleteK8sDeployments(dep.Name, namespace)
							} else {
								err = pdslib.DeleteK8sPods(pod.Name, namespace)
							}
							log.FailOnError(err, "Error while deleting workloads")
						}
					})
				}()

				var deploymentPods []corev1.Pod
				Step("Get a list of pod names that belong to the deployment", func() {
					deploymentPods, err = pdslib.GetPodsFromK8sStatefulSet(deployment, namespace)
					log.FailOnError(err, "Error while getting deployment pods")
					Expect(deploymentPods).NotTo(BeEmpty())
				})

				var nodeList []*corev1.Node
				Step("Get the node that the PV of the pod resides on", func() {
					for _, pod := range deploymentPods {
						log.InfoD("The pod spec node name: %v", pod.Spec.NodeName)
						nodeObject, err := pdslib.GetK8sNodeObjectUsingPodName(pod.Spec.NodeName)
						log.FailOnError(err, "error while getting node object")
						nodeList = append(nodeList, nodeObject)
					}
				})

				Step("For each node in the nodelist, stop px service on it", func() {

					for _, node := range nodeList {
						err := pdslib.LabelK8sNode(node, "px/service=stop")
						log.FailOnError(err, "error while labelling node")
					}

					log.InfoD("Finished labeling the nodes...")
					time.Sleep(30 * time.Second)

				})

				Step("Validate that the deployment is healthy", func() {
					err := dsTest.ValidateDataServiceDeployment(deployment, namespace)
					log.FailOnError(err, "error while validating dataservice")
				})

				Step("Cleanup: Start px on the node and uncordon the node", func() {
					for _, node := range nodeList {
						err := pdslib.RemoveLabelFromK8sNode(node, "px/service")
						log.FailOnError(err, "error while removing label from k8s node")
					}

					log.InfoD("Finished removing labels from the nodes...")

					for _, node := range nodeList {
						err := pdslib.DrainPxPodOnK8sNode(node, pxnamespace)
						log.FailOnError(err, "error while draining node")
					}

					log.InfoD("Finished draining px pods from the nodes...")

					for _, node := range nodeList {
						err := pdslib.UnCordonK8sNode(node)
						log.FailOnError(err, "error while uncordoning node")
					}

					log.InfoD("Finished uncordoning the node...")
					log.InfoD("Verify that the px pod has started on node...")
					// Read log lines of the px pod on the node to see if the service is running
					for _, node := range nodeList {
						rc, err := pdslib.VerifyPxPodOnNode(node.Name, pxnamespace)
						dash.VerifyFatal(rc, bool(true), "validating px pod on node")
						log.FailOnError(err, "error while validating px pod")
					}

				})

				Step("Delete Deployments", func() {
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					log.FailOnError(err, "error deleting deployment")
					dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
					log.InfoD("Getting all PV and associated PVCs and deleting them")
					err = pdslib.DeletePvandPVCs(*deployment.ClusterResourceName, false)
					log.FailOnError(err, "Error while deleting PV and PVCs")
				})
			}
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
	})

})

func ValidateDeployments(resourceTemp pdslib.ResourceSettingTemplate, storageOp pdslib.StorageOptions, config pdslib.StorageClassConfig, replicas int, dataServiceVersionBuildMap map[string][]string) {
	log.InfoD("filesystem used %v ", config.Spec.StorageOptions.Filesystem)
	log.InfoD("storage replicas used %v ", config.Spec.StorageOptions.Replicas)
	log.InfoD("cpu requests used %v ", config.Spec.Resources.Requests.CPU)
	log.InfoD("memory requests used %v ", config.Spec.Resources.Requests.Memory)
	log.InfoD("storage requests used %v ", config.Spec.Resources.Requests.Storage)
	log.InfoD("No of nodes requested %v ", config.Spec.Nodes)
	log.InfoD("volume group %v ", storageOp.VolumeGroup)

	dash.VerifyFatal(resourceTemp.Resources.Requests.CPU, config.Spec.Resources.Requests.CPU, "Validating CPU Request")
	dash.VerifyFatal(resourceTemp.Resources.Requests.Memory, config.Spec.Resources.Requests.Memory, "Validating Memory Request")
	dash.VerifyFatal(resourceTemp.Resources.Requests.Storage, config.Spec.Resources.Requests.Storage, "Validating storage")
	dash.VerifyFatal(resourceTemp.Resources.Limits.CPU, config.Spec.Resources.Limits.CPU, "Validating CPU Limits")
	dash.VerifyFatal(resourceTemp.Resources.Limits.Memory, config.Spec.Resources.Limits.Memory, "Validating Memory Limits")
	repl, err := strconv.Atoi(config.Spec.StorageOptions.Replicas)
	log.FailOnError(err, "failed on atoi method")
	dash.VerifyFatal(storageOp.Replicas, int32(repl), "Validating storage replicas")
	dash.VerifyFatal(storageOp.Filesystem, config.Spec.StorageOptions.Filesystem, "Validating filesystems")
	dash.VerifyFatal(config.Spec.Nodes, int32(replicas), "Validating node replicas")

	for version, build := range dataServiceVersionBuildMap {
		dash.VerifyFatal(config.Spec.Version, version+"-"+build[0], "validating ds build and version")
	}
}

func DeployInANamespaceAndVerify(nname string) []*pds.ModelsDeployment {
	var cleanup []*pds.ModelsDeployment
	for _, ds := range params.DataServiceToTest {
		Step("Deploy and validate data service", func() {
			isDeploymentsDeleted = false
			deployment, _, _, err = DeployandValidateDataServices(ds, nname, tenantID, projectID)
			log.FailOnError(err, "Error while deploying data services")
			cleanup = append(cleanup, deployment)
		})
	}
	return cleanup
}

var _ = Describe("{RollingRebootNodes}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PDS: RollingRebootNodes", "Reboot node(s) while the data services will be running", pdsLabels, 0)
	})

	It("has to deploy data service and reboot node(s) while the data services will be running.", func() {
		Step("Deploy Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				Step("Deploy and validate data service", func() {
					isDeploymentsDeleted = false
					deployment, _, _, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")
				})

				// TODO: Running workload for all datasevices
				// JIRA: PA-403

				Step("Reboot nodes", func() {
					dash.Info("Rebooting all the nodes in rolling fashion.")
					nodesToReboot := node.GetWorkerNodes()
					for _, n := range nodesToReboot {
						log.InfoD("reboot node: %s", n.Name)
						err = Inst().N.RebootNode(n, node.RebootNodeOpts{
							Force: true,
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         defaultCommandTimeout,
								TimeBeforeRetry: defaultCommandRetry,
							},
						})
						log.FailOnError(err, "Error while rebooting nodes")

						log.Infof("wait for node: %s to be back up", n.Name)
						err = Inst().N.TestConnection(n, node.ConnectionOpts{
							Timeout:         defaultTestConnectionTimeout,
							TimeBeforeRetry: defaultWaitRebootRetry,
						})
						if err != nil {
							log.FailOnError(err, "Error while testing node status %v, err: %v", n.Name, err.Error())
						}
						log.FailOnError(err, "Error while testing connection")
					}

				})

				Step("Validate Deployments after nodes are up", func() {
					log.Info("Validate Deployments after nodes are up")
					err = dsTest.ValidateDataServiceDeployment(deployment, namespace)
					log.FailOnError(err, "error validating deployment")
					log.Info("Deployments pods are up and healthy")
				})

				Step("Delete the deployment", func() {
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					log.FailOnError(err, "error deleting deployment")
					dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
					log.InfoD("Getting all PV and associated PVCs and deleting them")
					err = pdslib.DeletePvandPVCs(*deployment.ClusterResourceName, false)
					log.FailOnError(err, "Error while deleting PV and PVCs")
					isDeploymentsDeleted = true
				})
			}
		})
	})
	JustAfterEach(func() {

		defer func() {
			if !isDeploymentsDeleted {
				Step("Delete created deployments")
				resp, err := pdslib.DeleteDeployment(deployment.GetId())
				log.FailOnError(err, "error deleting deployment")
				dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
			}
		}()

		defer EndTorpedoTest()
	})
})

var _ = Describe("{AddAndValidateUserRoles}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("AddAndValidateUser", "Add users with admin or non admin privileges and validate the same.", nil, 0)
	})

	It("add users with admin or non admin privileges and validate.", func() {
		usersParam := params.Users
		defaultAdmin := os.Getenv("PDS_USERNAME")
		defaultAdminPassword := os.Getenv("PDS_PASSWORD")

		Step("Adding users having admin privileges and validating for same.", func() {
			log.Info("Adding user with admin privileges.")
			username := usersParam.AdminUsername
			password := usersParam.AdminPassword
			log.InfoD("Adding the user - %v", username)
			err := pdslib.ApiComponents.AccountRoleBinding.AddUser(accountID, username, true)
			log.FailOnError(err, fmt.Sprintf("Error while adding the user %s", username))
			os.Setenv("PDS_USERNAME", username)
			os.Setenv("PDS_PASSWORD", password)
			log.Info("Validating the admin role by listing users.")
			_, err = pdslib.ApiComponents.AccountRoleBinding.ListAccountsRoleBindings(accountID)
			log.FailOnError(err, fmt.Sprintf("User - %v is unable to fetch all the users belong to this account which is unexpected since it has the admin privileges.", usersParam.AdminUsername))
			log.InfoD("User - %v is able to fetch all the users belong to this account as expected since it has the admin privileges.", usersParam.AdminUsername)

		})

		Step("Adding users having non-admin privileges and validating it.", func() {
			log.InfoD("Adding user with non admin privileges.")
			username := usersParam.NonAdminUsername
			password := usersParam.NonAdminPassword
			err := pdslib.ApiComponents.AccountRoleBinding.AddUser(accountID, username, false)
			log.FailOnError(err, fmt.Sprintf("Error while adding the user %s", username))
			os.Setenv("PDS_USERNAME", username)
			os.Setenv("PDS_PASSWORD", password)
			log.InfoD("Validating the non-admin role by trying to list users belong to this account.")
			_, err = pdslib.ApiComponents.AccountRoleBinding.ListAccountsRoleBindings(accountID)
			log.FailOnError(err, fmt.Sprintf("User - %v is unable to fetch all the users belong to this account as expected since it doesn't have the admin privileges.", usersParam.NonAdminUsername))

		})
		log.InfoD("Resetting to default admin user and validating by listing users.")
		os.Setenv("PDS_USERNAME", defaultAdmin)
		os.Setenv("PDS_PASSWORD", defaultAdminPassword)
		_, err := pdslib.ApiComponents.AccountRoleBinding.ListAccountsRoleBindings(accountID)
		log.FailOnError(err, fmt.Sprintf("User - %v is unable to fetch all the users belong to this account which is unexpected since it has the admin privileges.", defaultAdmin))
	})

	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe("{GetPvcToFullCondition}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("GetPvcToFullCondition", "Deploys and increases the pvc size of DS once trhreshold is met", pdsLabels, 0)
	})

	It("Deploy Dataservices", func() {
		var generateWorkloads = make(map[string]string)
		var deployments = make(map[PDSDataService]*pds.ModelsDeployment)
		var dsVersions = make(map[string]map[string][]string)
		var depList []*pds.ModelsDeployment
		var dsName string

		Step("Deploy Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				if ds.Name == postgresql {
					Step("Deploy and validate data service", func() {
						isDeploymentsDeleted = false
						controlPlane.UpdateResourceTemplateName("pds-auto-pvcFullCondition")
						deployment, _, dataServiceVersionBuildMap, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
						log.FailOnError(err, "Error while deploying data services")
						deployments[ds] = deployment
						dsVersions[ds.Name] = dataServiceVersionBuildMap
						depList = append(depList, deployment)
						dsName = ds.Name

					})
				}
			}
			defer func() {
				for _, newDeployment := range deployments {
					Step("Delete created deployments")
					resp, err := pdslib.DeleteDeployment(newDeployment.GetId())
					log.FailOnError(err, "Error while deleting data services")
					dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
				}
			}()

			// This testcase is currently applicable only for postgresql ds deployments
			if dsName == postgresql {
				Step("Running Workloads before scaling up PVC ", func() {
					for ds, deployment := range deployments {
						if Contains(dataServicePodWorkloads, ds.Name) || Contains(dataServiceDeploymentWorkloads, ds.Name) {
							log.InfoD("Running Workloads on DataService %v ", ds.Name)
							var params pdslib.WorkloadGenerationParams
							pod, dep, err = RunWorkloads(params, ds, deployment, namespace)
							log.FailOnError(err, fmt.Sprintf("Error while genearating workloads for dataservice [%s]", ds.Name))
							generateWorkloads[ds.Name] = dep.Name
							for dsName, workloadContainer := range generateWorkloads {
								log.Debugf("dsName %s, workloadContainer %s", dsName, workloadContainer)
							}
						}
					}
				})

				defer func() {
					for dsName, workloadContainer := range generateWorkloads {
						Step("Delete the workload generating deployments", func() {
							if Contains(dataServiceDeploymentWorkloads, dsName) {
								log.InfoD("Deleting Workload Generating deployment %v ", workloadContainer)
								err = pdslib.DeleteK8sDeployments(workloadContainer, namespace)
							} else if Contains(dataServicePodWorkloads, dsName) {
								log.InfoD("Deleting Workload Generating pod %v ", workloadContainer)
								err = pdslib.DeleteK8sPods(workloadContainer, namespace)
							}
							log.FailOnError(err, "error deleting workload generating pods")
						})
					}
				}()

				Step("Checking the PVC usage", func() {
					ctx, err := dsTest.CreateSchedulerContextForPDSApps(depList)
					log.FailOnError(err, "Unable to create scheduler context")
					err = CheckPVCtoFullCondition(ctx)
					log.FailOnError(err, "Failing while filling the PVC to 90 percentage of its capacity due to ...")
					err = IncreasePVCby1Gig(ctx)
					log.FailOnError(err, "Failing while Increasing the PVC name...")
				})

				Step("Validate Deployments after PVC Resize", func() {
					for ds, deployment := range deployments {
						err = dsTest.ValidateDataServiceDeployment(deployment, namespace)
						log.FailOnError(err, "Error while validating dataservices")
						log.InfoD("Data-service: %v is up and healthy", ds.Name)
					}
				})
			}

		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})
