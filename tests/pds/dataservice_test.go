package tests

import (
	"net/http"
	"strconv"
	"time"

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
	defaultWaitRebootTimeout     = 5 * time.Minute
	defaultWaitRebootRetry       = 10 * time.Second
	defaultCommandRetry          = 5 * time.Second
	defaultCommandTimeout        = 1 * time.Minute
	defaultTestConnectionTimeout = 15 * time.Minute
	defaultRebootTimeRange       = 5 * time.Minute
)

var _ = Describe("{DeletePDSPods}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("DeletePDSPods", "delete pds pods and validate if its coming back online and dataserices are not affected", nil, 0)
	})

	It("delete pds pods and validate if its coming back online and dataserices are not affected", func() {
		Step("Deploy Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				log.InfoD("Deploying DataService %v ", ds.Name)
				isDeploymentsDeleted = false
				dataServiceDefaultResourceTemplateID, err = pdslib.GetResourceTemplate(tenantID, ds.Name)
				log.FailOnError(err, "Error while getting resource template")
				log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

				dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, ds.Name)
				log.FailOnError(err, "Error while getting app configuration template")
				dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")
				log.InfoD(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)

				deployment, _, dataServiceVersionBuildMap, err = pdslib.DeployDataServices(ds.Name, projectID,
					deploymentTargetID,
					dnsZone,
					deploymentName,
					namespaceID,
					dataServiceDefaultAppConfigID,
					int32(ds.Replicas),
					serviceType,
					dataServiceDefaultResourceTemplateID,
					storageTemplateID,
					ds.Version,
					ds.Image,
					namespace,
				)
				log.FailOnError(err, "Error while deploying data services")

				Step("Validate Storage Configurations", func() {
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, "error on ValidateDataServiceVolumes method")
					ValidateDeployments(resourceTemp, storageOp, config, ds.Replicas, dataServiceVersionBuildMap)
				})

				Step("get pods from pds-system namespace")
				podList, err := pdslib.GetPods("pds-system")
				log.FailOnError(err, "Error while getting pods")

				log.Info("PDS System Pods")
				for _, pod := range podList.Items {
					log.Infof("%v", pod.Name)
				}

				Step("delete pods from pds-system namespace")
				log.InfoD("Deleting PDS System Pods")
				err = pdslib.DeleteDeploymentPods(podList)
				log.FailOnError(err, "Error while deleting pods")

				Step("Validate Deployments after pods are up", func() {
					log.InfoD("Validate Deployments after pds pods are up")
					err = pdslib.ValidateDataServiceDeployment(deployment, namespace)
					log.FailOnError(err, "Error while validating data services")
					log.InfoD("Deployments pods are up and healthy")
				})

				Step("Delete Deployments", func() {
					log.InfoD("Deleting DataService %v ", ds.Name)
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					log.FailOnError(err, "Error while deleting data services")
					dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
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
				log.FailOnError(err, "Error while deleting data services")
				dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
			}
		}()

		defer EndTorpedoTest()
	})
})

var _ = Describe("{ScaleUPDataServices}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("ScaleUPDataServices", "Deploys and Scales Up the dataservices", nil, 0)
	})

	It("deploy Dataservices", func() {
		Step("Deploy Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				if ds.Name == zookeeper {
					log.Warnf("Scaling of nodes is not supported for %v dataservice ", ds.Name)
					continue
				}
				log.InfoD("Deploying DataService %v ", ds.Name)
				isDeploymentsDeleted = false
				dataServiceDefaultResourceTemplateID, err = pdslib.GetResourceTemplate(tenantID, ds.Name)
				log.FailOnError(err, "Error while getting resource template id")
				log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

				dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, ds.Name)
				log.FailOnError(err, "Error while getting app config template id")
				dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "validating dataServiceDefaultAppConfigID not to be empty")
				log.InfoD("dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)

				deployment, _, dataServiceVersionBuildMap, err = pdslib.DeployDataServices(ds.Name, projectID,
					deploymentTargetID,
					dnsZone,
					deploymentName,
					namespaceID,
					dataServiceDefaultAppConfigID,
					int32(ds.Replicas),
					serviceType,
					dataServiceDefaultResourceTemplateID,
					storageTemplateID,
					ds.Version,
					ds.Image,
					namespace,
				)
				log.FailOnError(err, "Error while deploying data services")

				Step("Validate Storage Configurations", func() {
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, "error on ValidateDataServiceVolumes method")
					ValidateDeployments(resourceTemp, storageOp, config, ds.Replicas, dataServiceVersionBuildMap)
				})

				Step("Running Workloads before scaling up of dataservices ", func() {
					log.InfoD("Running Workloads on DataService %v ", ds.Name)
					if ds.Name == postgresql {
						deploymentName := "pgload"
						log.Infof("Running Workloads on DataService %v ", ds.Name)
						pod, dep, err = pdslib.CreateDataServiceWorkloads(ds.Name, deployment.GetId(), "100", "1", deploymentName, namespace)
						log.FailOnError(err, "Error while genearating workloads")
					}
					if ds.Name == rabbitmq {
						deploymentName := "rmq"
						log.Infof("Running Workloads on DataService %v ", ds.Name)
						pod, dep, err = pdslib.CreateDataServiceWorkloads(ds.Name, deployment.GetId(), "", "", deploymentName, namespace)
						log.FailOnError(err, "Error while genearating workloads")
					}
					if ds.Name == redis {
						deploymentName := "redisbench"
						log.Infof("Running Workloads on DataService %v ", ds.Name)
						pod, dep, err = pdslib.CreateDataServiceWorkloads(ds.Name, deployment.GetId(), "", "", deploymentName, namespace)
						log.FailOnError(err, "Error while genearating workloads")
					}
					if ds.Name == cassandra {
						deploymentName := "cassandra-stress"
						log.Infof("Running Workloads on DataService %v ", ds.Name)
						pod, dep, err = pdslib.CreateDataServiceWorkloads(ds.Name, deployment.GetId(), "", "", deploymentName, namespace)
						log.FailOnError(err, "Error while genearating workloads")
					}
				})

				Step("Validate Deployments before scale up", func() {
					err = pdslib.ValidateDataServiceDeployment(deployment, namespace)
					log.FailOnError(err, "Error while validating dataservices")
					log.InfoD("Deployments pods are up and healthy")
				})

				Step("Scaling up the dataservice replicas", func() {
					updatedDeployment, err := pdslib.UpdateDataServices(deployment.GetId(),
						dataServiceDefaultAppConfigID, deployment.GetImageId(),
						int32(ds.ScaleReplicas), dataServiceDefaultResourceTemplateID, namespace)

					log.FailOnError(err, "Error while updating dataservices")
					log.InfoD("Scaling up DataService %v ", ds.Name)

					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, "error on ValidateDataServiceVolumes method")
					ValidateDeployments(resourceTemp, storageOp, config, ds.ScaleReplicas, dataServiceVersionBuildMap)
					for version, build := range dataServiceVersionBuildMap {
						dash.VerifyFatal(config.Spec.Version, version+"-"+build[0], "validating ds build and version")
					}
				})

				Step("Delete Deployments", func() {
					log.InfoD("Deleting DataService %v ", ds.Name)
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					log.FailOnError(err, "Error while deleting data services")
					dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
					isDeploymentsDeleted = true
				})

				Step("Delete the workload generating deployments", func() {
					if !(ds.Name == mysql || ds.Name == kafka || ds.Name == zookeeper || ds.Name == mongodb) {
						if ds.Name == cassandra || ds.Name == postgresql {
							log.InfoD("Deleting Workload Generating pods %v ", dep.Name)
							err = pdslib.DeleteK8sDeployments(dep.Name, namespace)
						} else {
							log.InfoD("Deleting Workload Generating pods %v ", pod.Name)
							err = pdslib.DeleteK8sPods(pod.Name, namespace)
						}
						log.FailOnError(err, "error deleting workload generating pods")
					}
				})
			}
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

		defer EndTorpedoTest()
	})
})

var _ = Describe("{UpgradeDataServiceVersion}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("UpgradeDataServiceVersion", "Upgrades the dataservice version", nil, 0)
	})

	It("runs the dataservice version upgrade test", func() {
		for _, ds := range params.DataServiceToTest {
			log.InfoD("Running UpgradeDataServiceVersion test for DataService %v ", ds.Name)
			UpgradeDataService(ds.Name, ds.OldVersion, ds.OldImage, ds.Version, ds.Image, int32(ds.Replicas))
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

var _ = Describe("{UpgradeDataServiceImage}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("UpgradeDataServiceImage", "Upgrades the dataservice image", nil, 0)
	})

	It("runs the dataservice build image upgrade test", func() {
		for _, ds := range params.DataServiceToTest {
			log.InfoD("Running UpgradeDataServiceImage test for DataService %v ", ds.Name)
			UpgradeDataService(ds.Name, ds.Version, ds.OldImage, ds.Version, ds.Image, int32(ds.Replicas))
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

var _ = Describe("{DeployDataServicesOnDemand}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("DeployDataServicesOnDemand", "Deploys DataServices", nil, 0)
	})

	It("Deploy DataservicesOnDemand", func() {
		log.Info("Create dataservices without backup.")
		Step("Deploy, Validate and Delete Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				log.InfoD("Deploying DataService %v ", ds.Name)
				isDeploymentsDeleted = false
				dataServiceDefaultResourceTemplateID, err = pdslib.GetResourceTemplate(tenantID, ds.Name)
				log.FailOnError(err, "Error while getting resource template")
				log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

				dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, ds.Name)
				log.FailOnError(err, "Error while getting app configuration template")
				dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")
				log.InfoD(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)

				deployment, _, dataServiceVersionBuildMap, err = pdslib.DeployDataServices(ds.Name, projectID,
					deploymentTargetID,
					dnsZone,
					deploymentName,
					namespaceID,
					dataServiceDefaultAppConfigID,
					int32(ds.Replicas),
					serviceType,
					dataServiceDefaultResourceTemplateID,
					storageTemplateID,
					ds.Version,
					ds.Image,
					namespace,
				)
				log.FailOnError(err, "Error while deploying data services")

				Step("Validate Storage Configurations", func() {
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, "error on ValidateDataServiceVolumes method")
					ValidateDeployments(resourceTemp, storageOp, config, ds.Replicas, dataServiceVersionBuildMap)
				})

				Step("Delete Deployments", func() {
					log.InfoD("Deleting DataService %v ", ds.Name)
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					log.FailOnError(err, "Error while deleting data services")
					dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
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
				log.FailOnError(err, "Error while deleting data services")
				dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
			}
		}()

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

func UpgradeDataService(dataservice, oldVersion, oldImage, dsVersion, dsBuild string, replicas int32) {
	Step("Deploy, Validate and Update Data Services", func() {
		isDeploymentsDeleted = false
		dataServiceDefaultResourceTemplateID, err = pdslib.GetResourceTemplate(tenantID, dataservice)
		log.FailOnError(err, "Error while getting resource template")
		log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

		dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, dataservice)
		log.FailOnError(err, "Error while getting app configuration template")
		dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

		log.InfoD(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)
		log.InfoD("Deploying DataService %v ", dataservice)
		deployment, _, dataServiceVersionBuildMap, err = pdslib.DeployDataServices(dataservice, projectID,
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

		Step("Validate Storage Configurations", func() {
			resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, dataservice, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
			log.FailOnError(err, "error on ValidateDataServiceVolumes method")
			ValidateDeployments(resourceTemp, storageOp, config, int(replicas), dataServiceVersionBuildMap)
		})

		Step("Running Workloads before scaling up of dataservices ", func() {
			if dataservice == postgresql {
				deploymentName := "pgload"
				pod, dep, err = pdslib.CreateDataServiceWorkloads(dataservice, deployment.GetId(), "100", "1", deploymentName, namespace)
				log.FailOnError(err, "Error while genearating workloads")
			}
			if dataservice == rabbitmq {
				deploymentName := "rmq"
				pod, dep, err = pdslib.CreateDataServiceWorkloads(dataservice, deployment.GetId(), "", "", deploymentName, namespace)
				log.FailOnError(err, "Error while genearating workloads")
			}
			if dataservice == redis {
				deploymentName := "redisbench"
				pod, dep, err = pdslib.CreateDataServiceWorkloads(dataservice, deployment.GetId(), "", "", deploymentName, namespace)
				log.FailOnError(err, "Error while genearating workloads")
			}
			if dataservice == cassandra {
				deploymentName := "cassandra-stress"
				pod, dep, err = pdslib.CreateDataServiceWorkloads(dataservice, deployment.GetId(), "", "", deploymentName, namespace)
				log.FailOnError(err, "Error while genearating workloads")
			}
		})

		Step("Update the data service patch versions", func() {
			log.Infof("Version/Build: %v %v", dsVersion, dsBuild)
			updatedDeployment, err := pdslib.UpdateDataServiceVerison(deployment.GetDataServiceId(), deployment.GetId(),
				dataServiceDefaultAppConfigID,
				replicas, dataServiceDefaultResourceTemplateID, dsBuild, namespace, dsVersion)
			log.FailOnError(err, "Error while updating data services")
			log.InfoD("data service deployed %v ", dataservice)

			resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, dataservice, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
			log.FailOnError(err, "error on ValidateDataServiceVolumes method")
			ValidateDeployments(resourceTemp, storageOp, config, int(replicas), dataServiceVersionBuildMap)
			dash.VerifyFatal(config.Spec.Version, dsVersion+"-"+dsBuild, "validating ds build and version")
		})

		Step("Delete Deployments", func() {
			resp, err := pdslib.DeleteDeployment(deployment.GetId())
			log.FailOnError(err, "Error while deleting data services")
			dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
			isDeploymentsDeleted = true
		})

		defer func() {
			Step("Delete the workload generating deployments", func() {
				if !(dataservice == mysql || dataservice == kafka || dataservice == zookeeper || dataservice == mongodb) {
					if dataservice == cassandra || dataservice == postgresql {
						err = pdslib.DeleteK8sDeployments(dep.Name, namespace)
					} else {
						err = pdslib.DeleteK8sPods(pod.Name, namespace)
					}
					log.FailOnError(err, "error while deleting workload deployments")
				}
			})
		}()
	})
}

var _ = Describe("{DeployMultipleNamespaces}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("DeployMultipleNamespaces", "Create multiple namespaces and deploy all dataservices", nil, 0)
	})

	It("creates multiple namespaces, deploys in each namespace", func() {

		var namespaces []*corev1.Namespace
		// create k8s namespaces
		for i := 0; i < 2; i++ {
			nname := "namespace-" + strconv.Itoa(i)
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
			var cleanupall []string
			for _, namespace := range namespaces {
				log.InfoD("Deploying deployment %v in namespace: %v", deploymentTargetID, namespace.Name)
				newNamespaceID, err := pdslib.GetnameSpaceID(namespace.Name, deploymentTargetID)
				log.FailOnError(err, "error while getting namespaceid")
				Expect(newNamespaceID).NotTo(BeEmpty())

				deps := DeployInANamespaceAndVerify(namespace.Name, newNamespaceID)
				cleanupall = append(cleanupall, deps...)
			}

			log.InfoD("List of created deployments: %v ", cleanupall)
			Step("Delete created deployments", func() {
				for _, dep := range cleanupall {
					_, err := pdslib.DeleteDeployment(dep)
					log.FailOnError(err, "error while deleting deployments")
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
		StartTorpedoTest("DeletePDSEnabledNamespace", "Create a namespace, deploy dataservices, delete the namespace and validate", nil, 0)
	})

	It("Deploy Dataservices and delete namespace", func() {

		nname := "test-namespace-0"
		_, err := pdslib.CreateK8sPDSNamespace(nname)
		log.FailOnError(err, "error while creating pds namespace")
		isNamespacesDeleted = false
		log.InfoD("Created namespace: %v", nname)
		log.InfoD("Waiting for created namespaces to be available in PDS")
		time.Sleep(10 * time.Second)
		log.InfoD("Create dataservices")

		Step("Deploy All Supported Data Services in the namespace", func() {

			log.InfoD("Deploying deployment %v in namespace: %v", deploymentTargetID, nname)
			newNamespaceID, err := pdslib.GetnameSpaceID(nname, deploymentTargetID)
			log.FailOnError(err, "error while getting namespaceid")
			Expect(newNamespaceID).NotTo(BeEmpty())

			var cleanup []*pds.ModelsDeployment
			for _, ds := range params.DataServiceToTest {
				isDeploymentsDeleted = false
				dataServiceDefaultResourceTemplateID, err = pdslib.GetResourceTemplate(tenantID, ds.Name)
				log.FailOnError(err, "Error while getting resource template")
				log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

				dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, ds.Name)
				log.FailOnError(err, "Error while getting app configuration template")
				dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")
				log.InfoD(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)

				deployment, _, _, err := pdslib.DeployDataServices(ds.Name, projectID,
					deploymentTargetID,
					dnsZone,
					deploymentName,
					newNamespaceID,
					dataServiceDefaultAppConfigID,
					int32(ds.Replicas),
					serviceType,
					dataServiceDefaultResourceTemplateID,
					storageTemplateID,
					ds.Version,
					ds.Image,
					nname,
				)
				log.FailOnError(err, "error while creating deployment")

				Step("Validate Storage Configurations", func() {
					log.Infof("data service deployed %v ", ds)
					log.InfoD("data service deployed %v ", ds)
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, nname)
					log.FailOnError(err, "error on ValidateDataServiceVolumes method")
					ValidateDeployments(resourceTemp, storageOp, config, ds.Replicas, dataServiceVersionBuildMap)
					cleanup = append(cleanup, deployment)

				})

			}
			log.InfoD("List of created deployments: %v ", cleanup)

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
				}
				isDeploymentsDeleted = true
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
		StartTorpedoTest("RestartPXPods", "Deploy dataservice, stop px service on the nodes where the dataservice is deployed, validate", nil, 0)
	})

	It("Deploy Dataservices", func() {
		log.Info("Create dataservices without backup.")
		Step("Deploy PDS Data Service", func() {
			for _, ds := range params.DataServiceToTest {
				isDeploymentsDeleted = false
				dataServiceDefaultResourceTemplateID, err = pdslib.GetResourceTemplate(tenantID, ds.Name)
				log.FailOnError(err, "Error while getting resource template id")
				log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

				dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, ds.Name)
				log.FailOnError(err, "Error while getting app config template id")
				dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

				deployment, _, _, err := pdslib.DeployDataServices(ds.Name, projectID,
					deploymentTargetID,
					dnsZone,
					deploymentName,
					namespaceID,
					dataServiceDefaultAppConfigID,
					int32(ds.Replicas),
					serviceType,
					dataServiceDefaultResourceTemplateID,
					storageTemplateID,
					ds.Version,
					ds.Image,
					namespace,
				)
				log.FailOnError(err, "Error while deploying data services")

				Step("Validate Storage Configurations", func() {
					log.Infof("data service deployed %v ", ds.Name)
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, "error on ValidateDataServiceVolumes method")
					ValidateDeployments(resourceTemp, storageOp, config, ds.Replicas, dataServiceVersionBuildMap)
				})

				Step("Running Workloads before scaling up of dataservices ", func() {
					if ds.Name == postgresql {
						deploymentName := "pgload"
						pod, dep, err = pdslib.CreateDataServiceWorkloads(ds.Name, deployment.GetId(), "100", "1", deploymentName, namespace)
						log.FailOnError(err, "Error while genearating workloads")
					}
					if ds.Name == rabbitmq {
						deploymentName := "rmq"
						pod, dep, err = pdslib.CreateDataServiceWorkloads(ds.Name, deployment.GetId(), "", "", deploymentName, namespace)
						log.FailOnError(err, "Error while genearating workloads")
					}
					if ds.Name == redis {
						deploymentName := "redisbench"
						pod, dep, err = pdslib.CreateDataServiceWorkloads(ds.Name, deployment.GetId(), "", "", deploymentName, namespace)
						log.FailOnError(err, "Error while genearating workloads")
					}
					if ds.Name == cassandra {
						deploymentName := "cassandra-stress"
						pod, dep, err = pdslib.CreateDataServiceWorkloads(ds.Name, deployment.GetId(), "", "", deploymentName, namespace)
						log.FailOnError(err, "Error while genearating workloads")
					}
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
					err := pdslib.ValidateDataServiceDeployment(deployment, namespace)
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
					isDeploymentsDeleted = true
				})
			}
		})
	})

	JustAfterEach(func() {
		//TODO: Write a wrapper method for defer func
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

func DeployInANamespaceAndVerify(nname string, namespaceID string) []string {

	var cleanup []string
	for _, ds := range params.DataServiceToTest {
		dataServiceDefaultResourceTemplateID, err = pdslib.GetResourceTemplate(tenantID, ds.Name)
		Expect(err).NotTo(HaveOccurred())

		log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

		dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, ds.Name)
		Expect(err).NotTo(HaveOccurred())
		dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")

		log.InfoD(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)

		deployment, _, dataServiceVersionBuildMap, err := pdslib.DeployDataServices(ds.Name, projectID,
			deploymentTargetID,
			dnsZone,
			deploymentName,
			namespaceID,
			dataServiceDefaultAppConfigID,
			int32(ds.Replicas),
			serviceType,
			dataServiceDefaultResourceTemplateID,
			storageTemplateID,
			ds.Version,
			ds.Image,
			nname,
		)
		Expect(err).NotTo(HaveOccurred())

		Step("Validate Storage Configurations", func() {
			resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, nname)
			log.FailOnError(err, "error on ValidateDataServiceVolumes method")
			ValidateDeployments(resourceTemp, storageOp, config, ds.Replicas, dataServiceVersionBuildMap)
			cleanup = append(cleanup, deployment.GetId())

		})
	}
	return cleanup
}

var _ = Describe("{RollingRebootNodes}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PDS: RollingRebootNodes", "Reboot node(s) while the data services will be running", nil, 0)
	})

	It("has to deploy data service and reboot node(s) while the data services will be running.", func() {
		Step("Deploy Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				isDeploymentsDeleted = false
				log.InfoD("Deploying DataService %v ", ds.Name)
				dataServiceDefaultResourceTemplateID, err = pdslib.GetResourceTemplate(tenantID, ds.Name)
				dash.VerifyFatal(err, nil, "Verifying data service deployment.")
				log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

				dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, ds.Name)
				log.FailOnError(err, "Error while getting app config template id")
				dash.VerifyFatal(dataServiceDefaultAppConfigID != "", true, "Validating dataServiceDefaultAppConfigID")
				log.InfoD(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)

				deployment, _, _, err := pdslib.DeployDataServices(ds.Name, projectID,
					deploymentTargetID,
					dnsZone,
					deploymentName,
					namespaceID,
					dataServiceDefaultAppConfigID,
					int32(ds.Replicas),
					serviceType,
					dataServiceDefaultResourceTemplateID,
					storageTemplateID,
					ds.Version,
					ds.Image,
					namespace,
				)
				log.FailOnError(err, "Error while deploying data services")

				Step("Validate Storage Configurations", func() {
					log.InfoD("data service deployed %v ", ds.Name)
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					log.FailOnError(err, "error on ValidateDataServiceVolumes method")
					ValidateDeployments(resourceTemp, storageOp, config, ds.Replicas, dataServiceVersionBuildMap)
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
					err = pdslib.ValidateDataServiceDeployment(deployment, namespace)
					log.FailOnError(err, "error validating deployment")
					log.Info("Deployments pods are up and healthy")
				})

				Step("Delete the deployment", func() {
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					log.FailOnError(err, "error deleting deployment")
					dash.VerifyFatal(resp.StatusCode, http.StatusAccepted, "validating the status response")
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
