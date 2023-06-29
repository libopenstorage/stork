package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	dss "github.com/portworx/torpedo/drivers/pds/dataservice"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	"net/http"
)

var _ = Describe("{RestartPXDuringAppScaleUp}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RestartPXDuringAppScaleUp", "Restart PX on a node during application is scaled up", pdsLabels, 0)
		pdslib.MarkResiliencyTC(true)
	})

	It("Deploy Dataservices and Restart PX During App scaleup", func() {
		var deployments = make(map[PDSDataService]*pds.ModelsDeployment)
		var generateWorkloads = make(map[string]string)

		Step("Deploy Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				Step("Deploy and validate data service", func() {
					isDeploymentsDeleted = false
					deployment, _, _, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")
					deployments[ds] = deployment
				})
			}
		})

		defer func() {
			for _, newDeployment := range deployments {
				Step("Delete created deployments")
				resp, err := pdslib.DeleteDeployment(newDeployment.GetId())
				log.FailOnError(err, "Error while deleting data services")
				dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
				err = pdslib.DeletePvandPVCs(*newDeployment.ClusterResourceName, false)
				log.FailOnError(err, "Error while deleting PV and PVCs")
			}
		}()

		Step("Scale Up Data Services and Restart Portworx", func() {
			for ds, deployment := range deployments {

				failuretype := pdslib.TypeOfFailure{
					Type: RestartPxDuringDSScaleUp,
					Method: func() error {
						return pdslib.RestartPXDuringDSScaleUp(params.InfraToTest.Namespace, deployment)
					},
				}
				pdslib.DefineFailureType(failuretype)

				log.InfoD("Scaling up DataService %v ", ds.Name)
				dataServiceDefaultAppConfigID, err = controlPlane.GetAppConfTemplate(tenantID, ds.Name)
				log.FailOnError(err, "Error while getting app configuration template")
				dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

				dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
				log.FailOnError(err, "Error while getting resource setting template")
				dash.VerifyFatal(dataServiceDefaultResourceTemplateID != "", true, "Validating dataServiceDefaultResourceTemplateID")

				updatedDeployment, err := pdslib.UpdateDataServices(deployment.GetId(),
					dataServiceDefaultAppConfigID, deployment.GetImageId(),
					int32(ds.ScaleReplicas), dataServiceDefaultResourceTemplateID, namespace)
				log.FailOnError(err, "Error while updating dataservices")

				//wait for the scaled up data service and restart px
				err = pdslib.InduceFailureAfterWaitingForCondition(deployment, namespace, int32(ds.ScaleReplicas))
				log.FailOnError(err, fmt.Sprintf("Error happened while restarting px for data service %v", *deployment.ClusterResourceName))

				id := pdslib.GetDataServiceID(ds.Name)
				dash.VerifyFatal(id != "", true, "Validating dataservice id")
				log.Infof("Getting versionID  for Data service version %s and buildID for %s ", ds.Version, ds.Image)

				_, _, dsVersionBuildMap, err := pdslib.GetVersionsImage(ds.Version, ds.Image, id)
				log.FailOnError(err, "Error while fetching versions/image information")

				//TODO: Rename the method ValidateDataServiceVolumes
				resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
				log.FailOnError(err, "error on ValidateDataServiceVolumes method")
				ValidateDeployments(resourceTemp, storageOp, config, int(ds.ScaleReplicas), dsVersionBuildMap)
				dash.VerifyFatal(int32(ds.ScaleReplicas), config.Spec.Nodes, "Validating replicas after scaling up of dataservice")

			}
		})
		Step("Running Workloads", func() {
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
				} else {
					log.InfoD("Workload script not available for ds %v", ds.Name)
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
	})
	JustAfterEach(func() {
		EndTorpedoTest()

	})
})

var _ = Describe("{RebootActiveNodeDuringDeployment}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RebootActiveNodeDuringDeployment", "Reboots a Node onto which a pod is coming up", pdsLabels, 0)
	})

	It("deploy Dataservices", func() {
		Step("Deploy Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				var dsVersionBuildMap = make(map[string][]string)
				var num_reboots int
				num_reboots = 1
				Step("Start deployment, Reboot a node on which deployment is coming up and validate data service", func() {
					isDeploymentsDeleted = false
					// Global Resiliency TC marker
					pdslib.MarkResiliencyTC(true)

					// Deploy and Validate this Data service after injecting the type of failure we want to catch
					deployment, _, dsVersionBuildMap, err = dsTest.TriggerDeployDataService(ds, params.InfraToTest.Namespace, tenantID, projectID, false,
						dss.TestParams{NamespaceId: namespaceID, StorageTemplateId: storageTemplateID, DeploymentTargetId: deploymentTargetID, DnsZone: dnsZone, ServiceType: serviceType})
					log.FailOnError(err, "Error while deploying data services")

					// Type of failure that this TC needs to cover
					failuretype := pdslib.TypeOfFailure{
						Type: ActiveNodeRebootDuringDeployment,
						Method: func() error {
							return pdslib.RebootActiveNodeDuringDeployment(params.InfraToTest.Namespace, deployment, num_reboots)
						},
					}
					pdslib.DefineFailureType(failuretype)

					err = pdslib.InduceFailureAfterWaitingForCondition(deployment, namespace, params.ResiliencyTest.CheckTillReplica)
					log.FailOnError(err, fmt.Sprintf("Error happened while executing Reboot test for data service %v", *deployment.ClusterResourceName))

					dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
					log.FailOnError(err, "Error while getting resource setting template")
					dash.VerifyFatal(dataServiceDefaultResourceTemplateID != "", true, "Validating dataServiceDefaultResourceTemplateID")

					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, "error on ValidateDataServiceVolumes method")
					ValidateDeployments(resourceTemp, storageOp, config, int(ds.Replicas), dsVersionBuildMap)
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

var _ = Describe("{RebootNodeDuringAppVersionUpdate}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RebootNodeDuringAppVersionUpdate", "Reboot node while app version update is going on", pdsLabels, 0)
		// Global Resiliency TC marker
		pdslib.MarkResiliencyTC(true)
	})

	It("Reboot Node While App Version update is going on", func() {
		var deployments = make(map[PDSDataService]*pds.ModelsDeployment)
		var generateWorkloads = make(map[string]string)
		var dsVersionBuildMap = make(map[string][]string)

		Step("Deploy and Validate Data services", func() {
			for _, ds := range params.DataServiceToTest {
				deployment, _, dsVersionBuildMap, err = dsTest.TriggerDeployDataService(ds, params.InfraToTest.Namespace, tenantID, projectID, true,
					dss.TestParams{NamespaceId: namespaceID, StorageTemplateId: storageTemplateID, DeploymentTargetId: deploymentTargetID, DnsZone: dnsZone, ServiceType: serviceType})
				log.FailOnError(err, "Error while deploying data services")

				err = dsTest.ValidateDataServiceDeployment(deployment, params.InfraToTest.Namespace)
				log.FailOnError(err, "Error while validating data service deployment")
				deployments[ds] = deployment

				dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
				log.FailOnError(err, "Error while getting resource template")
				log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

				resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
				log.FailOnError(err, "error on ValidateDataServiceVolumes method")
				ValidateDeployments(resourceTemp, storageOp, config, ds.Replicas, dsVersionBuildMap)
			}
		})

		defer func() {
			for _, newDeployment := range deployments {
				Step("Delete created deployments")
				resp, err := pdslib.DeleteDeployment(newDeployment.GetId())
				log.FailOnError(err, "Error while deleting data services")
				dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
				err = pdslib.DeletePvandPVCs(*newDeployment.ClusterResourceName, false)
				log.FailOnError(err, "Error while deleting PV and PVCs")
			}
		}()

		Step("Update Data Service Version and reboot node", func() {
			for ds, deployment := range deployments {

				log.Infof("Version/Build: %v %v", ds.Version, ds.Image)
				updatedDeployment, err := pdslib.UpdateDataServiceVerison(deployment.GetDataServiceId(), deployment.GetId(),
					dataServiceDefaultAppConfigID,
					int32(ds.Replicas), dataServiceDefaultResourceTemplateID, ds.Image, ds.Version)
				log.FailOnError(err, "Error while updating data services")
				log.InfoD("data service %v update triggered", ds.Name)

				// Type of failure that this TC needs to cover
				failuretype := pdslib.TypeOfFailure{
					Type: RebootNodeDuringAppVersionUpdate,
					Method: func() error {
						return pdslib.NodeRebootDurinAppVersionUpdate(params.InfraToTest.Namespace, deployment)
					},
				}
				pdslib.DefineFailureType(failuretype)

				err = pdslib.InduceFailureAfterWaitingForCondition(updatedDeployment, namespace, params.ResiliencyTest.CheckTillReplica)
				log.FailOnError(err, fmt.Sprintf("Error happened while executing Reboot test for data service %v", *deployment.ClusterResourceName))

				dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
				log.FailOnError(err, "Error while getting resource template")
				log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

				resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
				log.FailOnError(err, "error on ValidateDataServiceVolumes method")

				id := pdslib.GetDataServiceID(ds.Name)
				dash.VerifyFatal(id != "", true, "Validating dataservice id")
				log.Infof("Getting versionID  for Data service version %s and buildID for %s ", ds.Version, ds.Image)
				for version := range dsVersionBuildMap {
					delete(dsVersionBuildMap, version)
				}
				_, _, dsVersionBuildMap, err = pdslib.GetVersionsImage(ds.Version, ds.Image, id)
				log.FailOnError(err, "Error while fetching versions/image information")

				ValidateDeployments(resourceTemp, storageOp, config, int(ds.Replicas), dsVersionBuildMap)
				dash.VerifyFatal(config.Spec.Version, ds.Version+"-"+ds.Image, "validating ds build and version")
			}

		})

		Step("Running Workloads", func() {
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
				} else {
					log.InfoD("Workload script not available for ds %v", ds.Name)
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
	})
	JustAfterEach(func() {
		EndTorpedoTest()
	})

})

var _ = Describe("{KillDeploymentControllerDuringDeployment}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("KillDeploymentControllerDuringDeployment", "Kill Deployment Controller Pod when a DS Deployment is happening", pdsLabels, 0)
	})

	It("Deploy Data Services", func() {
		Step("Deploy Data Services", func() {
			var dsVersionBuildMap = make(map[string][]string)
			for _, ds := range params.DataServiceToTest {
				Step("Start deployment, Kill Deployment Controller Pod while deployment is ongoing and validate data service", func() {
					isDeploymentsDeleted = false
					// Global Resiliency TC marker
					pdslib.MarkResiliencyTC(true)
					// Type of failure that this TC needs to cover
					failuretype := pdslib.TypeOfFailure{
						Type: KillDeploymentControllerPod,
						Method: func() error {
							return pdslib.KillPodsInNamespace(params.InfraToTest.PDSNamespace, pdslib.PdsDeploymentControllerManagerPod)
						},
					}
					pdslib.DefineFailureType(failuretype)
					// Deploy and Validate this Data service after injecting the type of failure we want to catch
					deployment, _, dsVersionBuildMap, err = dsTest.TriggerDeployDataService(ds, params.InfraToTest.Namespace, tenantID, projectID, false,
						dss.TestParams{NamespaceId: namespaceID, StorageTemplateId: storageTemplateID, DeploymentTargetId: deploymentTargetID, DnsZone: dnsZone, ServiceType: serviceType})
					log.FailOnError(err, "Error while deploying data services")

					err = pdslib.InduceFailureAfterWaitingForCondition(deployment, namespace, params.ResiliencyTest.CheckTillReplica)
					log.FailOnError(err, fmt.Sprintf("Error happened while executing Kill Deployment Controller test for data service %v", *deployment.ClusterResourceName))

					dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
					log.FailOnError(err, "Error while getting resource template")
					log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, "error on ValidateDataServiceVolumes method")
					ValidateDeployments(resourceTemp, storageOp, config, int(ds.Replicas), dsVersionBuildMap)
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

var _ = Describe("{RebootAllWorkerNodesDuringDeployment}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RebootAllWorkerNodesDuringDeployment", "Reboots all worker nodes while a data service pod is coming up", pdsLabels, 0)
	})

	It("deploy Dataservices", func() {
		Step("Deploy Data Services", func() {
			var dsVersionBuildMap = make(map[string][]string)
			for _, ds := range params.DataServiceToTest {
				Step("Start deployment, Reboot multiple nodes on which deployment is coming up and validate data service", func() {
					isDeploymentsDeleted = false
					// Global Resiliency TC marker
					pdslib.MarkResiliencyTC(true)

					// Deploy and Validate this Data service after injecting the type of failure we want to catch
					deployment, _, dsVersionBuildMap, err = dsTest.TriggerDeployDataService(ds, params.InfraToTest.Namespace, tenantID, projectID, false,
						dss.TestParams{NamespaceId: namespaceID, StorageTemplateId: storageTemplateID, DeploymentTargetId: deploymentTargetID, DnsZone: dnsZone, ServiceType: serviceType})
					log.FailOnError(err, "Error while deploying data services")

					// Type of failure that this TC needs to cover
					failuretype := pdslib.TypeOfFailure{
						Type: RebootNodesDuringDeployment,
						Method: func() error {
							return pdslib.RebootWorkerNodesDuringDeployment(params.InfraToTest.Namespace, deployment, "all")
						},
					}
					pdslib.DefineFailureType(failuretype)

					err = pdslib.InduceFailureAfterWaitingForCondition(deployment, namespace, params.ResiliencyTest.CheckTillReplica)
					log.FailOnError(err, fmt.Sprintf("Error happened while executing Reboot all worker nodes test for data service %v", *deployment.ClusterResourceName))

					dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
					log.FailOnError(err, "Error while getting resource setting template")
					dash.VerifyFatal(dataServiceDefaultResourceTemplateID != "", true, "Validating dataServiceDefaultResourceTemplateID")

					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, "error on ValidateDataServiceVolumes method")
					ValidateDeployments(resourceTemp, storageOp, config, int(ds.Replicas), dsVersionBuildMap)
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

var _ = Describe("{KillAgentDuringDeployment}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("KillAgentDuringDeployment", "Kill Agent Pod when a DS Deployment is happening", pdsLabels, 0)
	})

	It("Deploy Dataservices", func() {
		Step("Deploy Data Services", func() {
			var dsVersionBuildMap = make(map[string][]string)
			for _, ds := range params.DataServiceToTest {
				Step("Start deployment, Kill Agent Pod while deployment is ongoing and validate data service", func() {
					isDeploymentsDeleted = false
					// Global Resiliency TC marker
					pdslib.MarkResiliencyTC(true)
					// Type of failure that this TC needs to cover
					failuretype := pdslib.TypeOfFailure{
						Type: KillAgentPodDuringDeployment,
						Method: func() error {
							return pdslib.KillPodsInNamespace(params.InfraToTest.PDSNamespace, pdslib.PdsAgentPod)
						},
					}
					pdslib.DefineFailureType(failuretype)
					// Deploy and Validate this Data service after injecting the type of failure we want to catch
					deployment, _, dsVersionBuildMap, err = dsTest.TriggerDeployDataService(ds, params.InfraToTest.Namespace, tenantID, projectID, false,
						dss.TestParams{NamespaceId: namespaceID, StorageTemplateId: storageTemplateID, DeploymentTargetId: deploymentTargetID, DnsZone: dnsZone, ServiceType: serviceType})
					log.FailOnError(err, "Error while deploying data services")

					err = pdslib.InduceFailureAfterWaitingForCondition(deployment, namespace, params.ResiliencyTest.CheckTillReplica)
					log.FailOnError(err, fmt.Sprintf("Error happened while executing Kill Agent Pod test for data service %v", *deployment.ClusterResourceName))

					dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
					log.FailOnError(err, "Error while getting resource template")
					log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, "error on ValidateDataServiceVolumes method")

					ValidateDeployments(resourceTemp, storageOp, config, int(ds.Replicas), dsVersionBuildMap)
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

var _ = Describe("{RestartAppDuringResourceUpdate}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RestartAppDuringResourceUpdate", "Restart application pod during resource update", pdsLabels, 0)
		pdslib.MarkResiliencyTC(true)
	})

	It("Deploy Data Services", func() {
		var deployments = make(map[PDSDataService]*pds.ModelsDeployment)
		Step("Deploy Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				Step("Deploy and validate data service", func() {
					deployment, _, _, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")
					deployments[ds] = deployment
				})
			}
		})

		defer func() {
			for _, newDeployment := range deployments {
				Step("Delete created deployments")
				resp, err := pdslib.DeleteDeployment(newDeployment.GetId())
				log.FailOnError(err, "Error while deleting data services")
				dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
				err = pdslib.DeletePvandPVCs(*newDeployment.ClusterResourceName, false)
				log.FailOnError(err, "Error while deleting PV and PVCs")
			}
		}()

		Step("Update the resource and Restart application pods", func() {
			for _, deployment := range deployments {
				failureType := pdslib.TypeOfFailure{
					Type: RestartAppDuringResourceUpdate,
					Method: func() error {
						return pdslib.RestartApplicationDuringResourceUpdate(params.InfraToTest.Namespace, deployment)
					},
				}
				pdslib.DefineFailureType(failureType)

				err = pdslib.InduceFailureAfterWaitingForCondition(deployment, namespace, 0)
				log.FailOnError(err, fmt.Sprintf("Error while pod restart during Resource update %v", *deployment.ClusterResourceName))

				err = dsTest.ValidateDataServiceDeployment(deployment, namespace)
				log.FailOnError(err, "error on ValidateDataServiceDeployment")
			}
		})
	})
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe("{KillTeleportDuringDeployment}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("KillTeleportDuringDeployment", "Kill Teleport Pod when a DS Deployment is happening", pdsLabels, 0)
	})

	It("Deploy Dataservices", func() {
		Step("Deploy Data Services", func() {
			var dsVersionBuildMap = make(map[string][]string)
			for _, ds := range params.DataServiceToTest {
				Step("Start deployment, Kill Agent Pod while deployment is ongoing and validate data service", func() {
					isDeploymentsDeleted = false
					// Global Resiliency TC marker
					pdslib.MarkResiliencyTC(true)
					// Type of failure that this TC needs to cover
					failuretype := pdslib.TypeOfFailure{
						Type: KillTeleportPodDuringDeployment,
						Method: func() error {
							return pdslib.KillPodsInNamespace(params.InfraToTest.PDSNamespace, pdslib.PdsTeleportPod)
						},
					}
					pdslib.DefineFailureType(failuretype)
					// Deploy and Validate this Data service after injecting the type of failure we want to catch
					deployment, _, dsVersionBuildMap, err = dsTest.TriggerDeployDataService(ds, params.InfraToTest.Namespace, tenantID, projectID, false,
						dss.TestParams{NamespaceId: namespaceID, StorageTemplateId: storageTemplateID, DeploymentTargetId: deploymentTargetID, DnsZone: dnsZone, ServiceType: serviceType})
					log.FailOnError(err, "Error while deploying data services")

					err = pdslib.InduceFailureAfterWaitingForCondition(deployment, namespace, params.ResiliencyTest.CheckTillReplica)
					log.FailOnError(err, fmt.Sprintf("Error happened while executing Kill Teleport Pod test for data service %v", *deployment.ClusterResourceName))

					dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
					log.FailOnError(err, "Error while getting resource template")
					log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, "error on ValidateDataServiceVolumes method")

					ValidateDeployments(resourceTemp, storageOp, config, int(ds.Replicas), dsVersionBuildMap)
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

var _ = Describe("{RebootActiveNodeMultipleTimesDuringDeployment}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RebootActiveNodeMultipleTimesDuringDeployment", "Reboots a Node multiple times onto which a pod is coming up", pdsLabels, 0)
	})

	It("deploy Dataservices", func() {
		Step("Deploy Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				var dsVersionBuildMap = make(map[string][]string)
				var num_reboots int
				num_reboots = 3
				Step("Start deployment, Reboot a node on which deployment is coming up and validate data service", func() {
					isDeploymentsDeleted = false
					// Global Resiliency TC marker
					pdslib.MarkResiliencyTC(true)

					// Deploy and Validate this Data service after injecting the type of failure we want to catch
					deployment, _, dsVersionBuildMap, err = dsTest.TriggerDeployDataService(ds, params.InfraToTest.Namespace, tenantID, projectID, false,
						dss.TestParams{NamespaceId: namespaceID, StorageTemplateId: storageTemplateID, DeploymentTargetId: deploymentTargetID, DnsZone: dnsZone, ServiceType: serviceType})
					log.FailOnError(err, "Error while deploying data services")

					// Type of failure that this TC needs to cover
					failuretype := pdslib.TypeOfFailure{
						Type: ActiveNodeRebootDuringDeployment,
						Method: func() error {
							return pdslib.RebootActiveNodeDuringDeployment(params.InfraToTest.Namespace, deployment, num_reboots)
						},
					}
					pdslib.DefineFailureType(failuretype)

					err = pdslib.InduceFailureAfterWaitingForCondition(deployment, namespace, params.ResiliencyTest.CheckTillReplica)
					log.FailOnError(err, fmt.Sprintf("Error happened while executing Reboot test for data service %v", *deployment.ClusterResourceName))

					dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
					log.FailOnError(err, "Error while getting resource setting template")
					dash.VerifyFatal(dataServiceDefaultResourceTemplateID != "", true, "Validating dataServiceDefaultResourceTemplateID")

					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, "error on ValidateDataServiceVolumes method")
					ValidateDeployments(resourceTemp, storageOp, config, int(ds.Replicas), dsVersionBuildMap)
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

var _ = Describe("{KillPdsAgentDuringWorkloadRun}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("KillPdsAgentDuringWorkloadRun", "Kill Pds Agent Pods while Workload is running", pdsLabels, 0)
	})
	It("Deploy Dataservices and Restart PX During App scaleup", func() {
		var deployments = make(map[PDSDataService]*pds.ModelsDeployment)
		var generateWorkloads = make(map[string]string)
		Step("Deploy Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				if ds.Name != postgresql {
					continue
				}
				Step("Deploy and validate data service", func() {
					isDeploymentsDeleted = false
					deployment, _, _, err = DeployandValidateDataServices(ds, params.InfraToTest.Namespace, tenantID, projectID)
					log.FailOnError(err, "Error while deploying data services")
					deployments[ds] = deployment
				})
			}
		})

		defer func() {
			for _, newDeployment := range deployments {
				Step("Delete created deployments")
				resp, err := pdslib.DeleteDeployment(newDeployment.GetId())
				log.FailOnError(err, "Error while deleting data services")
				dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
				err = pdslib.DeletePvandPVCs(*newDeployment.ClusterResourceName, false)
				log.FailOnError(err, "Error while deleting PV and PVCs")
			}
		}()
		Step("Running Workloads", func() {
			for ds, deployment := range deployments {
				if Contains(dataServicePodWorkloads, ds.Name) || Contains(dataServiceDeploymentWorkloads, ds.Name) {
					log.InfoD("Running Workloads on DataService %v ", ds.Name)
					var params pdslib.WorkloadGenerationParams
					pod, dep, err = RunWorkloads(params, ds, deployment, namespace)
					log.FailOnError(err, fmt.Sprintf("Error while generating workloads for dataservice [%s]", ds.Name))
					if dep == nil {
						generateWorkloads[ds.Name] = pod.Name
					} else {
						generateWorkloads[ds.Name] = dep.Name
					}
					for dsName, workloadContainer := range generateWorkloads {
						log.Debugf("dsName %s, workloadContainer %s", dsName, workloadContainer)
					}
				} else {
					log.InfoD("Workload script not available for ds %v", ds.Name)
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

		Step("Killing PDS Agent Pods", func() {
			err = pdslib.KillPodsInNamespace(params.InfraToTest.PDSNamespace, pdslib.PdsAgentPod)
			log.FailOnError(err, "Failed while deleting PDS Agent Pods")
		})

		// TODO : Once Workload Validation Module is ready, we will add that here. AI: @jyoti

	})
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe("{RebootMoreThanQuorumWorkerNodesDuringDeployment}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RebootMoreThanQuorumWorkerNodesDuringDeployment", "Reboots more worker nodes than required for Px Quorum while a data service pod is coming up", pdsLabels, 0)
	})

	It("Deploy DS, Reboot Nodes, Validate DS, Run Workload", func() {
		var deployments = make(map[PDSDataService]*pds.ModelsDeployment)
		var generateWorkloads = make(map[string]string)
		Step("Deploy DS, Reboot Nodes, Validate DS, Run Workload", func() {
			var dsVersionBuildMap = make(map[string][]string)
			for _, ds := range params.DataServiceToTest {
				Step("Start deployment, Reboot multiple nodes on which deployment is coming up, validate data service and run workload", func() {
					isDeploymentsDeleted = false
					// Global Resiliency TC marker
					pdslib.MarkResiliencyTC(true)

					// Deploy and Validate this Data service after injecting the type of failure we want to catch
					deployment, _, dsVersionBuildMap, err = dsTest.TriggerDeployDataService(ds, params.InfraToTest.Namespace, tenantID, projectID, false,
						dss.TestParams{NamespaceId: namespaceID, StorageTemplateId: storageTemplateID, DeploymentTargetId: deploymentTargetID, DnsZone: dnsZone, ServiceType: serviceType})
					log.FailOnError(err, "Error while deploying data services")

					deployments[ds] = deployment
					// Type of failure that this TC needs to cover
					failuretype := pdslib.TypeOfFailure{
						Type: RebootNodesDuringDeployment,
						Method: func() error {
							return pdslib.RebootWorkerNodesDuringDeployment(params.InfraToTest.Namespace, deployment, "quorum")
						},
					}
					pdslib.DefineFailureType(failuretype)

					err = pdslib.InduceFailureAfterWaitingForCondition(deployment, namespace, params.ResiliencyTest.CheckTillReplica)
					log.FailOnError(err, fmt.Sprintf("Error happened while executing Reboot all worker nodes test for data service %v", *deployment.ClusterResourceName))

					dataServiceDefaultResourceTemplateID, err = controlPlane.GetResourceTemplate(tenantID, ds.Name)
					log.FailOnError(err, "Error while getting resource setting template")
					dash.VerifyFatal(dataServiceDefaultResourceTemplateID != "", true, "Validating dataServiceDefaultResourceTemplateID")

					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, "error on ValidateDataServiceVolumes method")
					ValidateDeployments(resourceTemp, storageOp, config, int(ds.Replicas), dsVersionBuildMap)

					Step("Running Workloads", func() {
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
							} else {
								log.InfoD("Workload script not available for ds %v", ds.Name)
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
