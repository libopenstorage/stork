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
				Expect(err).NotTo(HaveOccurred())

				log.Infof("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

				dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, ds.Name)
				Expect(err).NotTo(HaveOccurred())
				Expect(dataServiceDefaultAppConfigID).NotTo(BeEmpty())

				log.Infof(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)

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
				Expect(err).NotTo(HaveOccurred())

				defer func() {
					if !isDeploymentsDeleted {
						Step("Delete created deployments")
						resp, err := pdslib.DeleteDeployment(deployment.GetId())
						Expect(err).NotTo(HaveOccurred())
						Expect(resp.StatusCode).Should(BeEquivalentTo(http.StatusAccepted))
					}
				}()

				Step("Validate Storage Configurations", func() {
					log.Infof("data service deployed %v ", ds.Name)
					log.InfoD("Validating DataService %v ", ds.Name)
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
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
					Expect(config.Spec.Nodes).Should(Equal(int32(ds.Replicas)))
				})

				Step("get pods from pds-system namespace")
				podList, err := pdslib.GetPods("pds-system")
				Expect(err).NotTo(HaveOccurred())

				log.Info("PDS System Pods")
				for _, pod := range podList.Items {
					log.Infof("%v", pod.Name)
				}

				Step("delete pods from pds-system namespace")
				log.InfoD("Deleting PDS System Pods")
				err = pdslib.DeleteDeploymentPods(podList)
				Expect(err).NotTo(HaveOccurred())

				Step("Validate Deployments after pods are up", func() {
					log.InfoD("Validate Deployments after pds pods are up")
					err = pdslib.ValidateDataServiceDeployment(deployment, namespace)
					Expect(err).NotTo(HaveOccurred())
					log.InfoD("Deployments pods are up and healthy")
				})
			}
		})
	})
	JustAfterEach(func() {
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
				if ds.Name == mysql || ds.Name == kafka || ds.Name == zookeeper {
					continue
				}
				log.InfoD("Deploying DataService %v ", ds.Name)
				isDeploymentsDeleted = false
				dataServiceDefaultResourceTemplateID, err = pdslib.GetResourceTemplate(tenantID, ds.Name)
				Expect(err).NotTo(HaveOccurred())

				log.Infof("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

				dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, ds.Name)
				Expect(err).NotTo(HaveOccurred())
				Expect(dataServiceDefaultAppConfigID).NotTo(BeEmpty())

				log.Infof(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)

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
				Expect(err).NotTo(HaveOccurred())

				defer func() {
					if !isDeploymentsDeleted {
						Step("Delete created deployments")
						resp, err := pdslib.DeleteDeployment(deployment.GetId())
						Expect(err).NotTo(HaveOccurred())
						Expect(resp.StatusCode).Should(BeEquivalentTo(http.StatusAccepted))
					}
				}()

				Step("Validate Storage Configurations", func() {
					log.Infof("data service deployed %v ", ds.Name)
					log.InfoD("Validating DataService %v ", ds.Name)
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
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
					Expect(config.Spec.Nodes).Should(Equal(int32(ds.Replicas)))
				})

				Step("Running Workloads before scaling up of dataservices ", func() {
					log.InfoD("Running Workloads on DataService %v ", ds.Name)
					if ds.Name == postgresql {
						deploymentName := "pgload"
						log.Infof("Running Workloads on DataService %v ", ds.Name)
						pod, dep, err = pdslib.CreateDataServiceWorkloads(ds.Name, deployment.GetId(), "100", "1", deploymentName, namespace)
						Expect(err).NotTo(HaveOccurred())
					}
					if ds.Name == rabbitmq {
						deploymentName := "rmq"
						log.Infof("Running Workloads on DataService %v ", ds.Name)
						pod, dep, err = pdslib.CreateDataServiceWorkloads(ds.Name, deployment.GetId(), "", "", deploymentName, namespace)
						Expect(err).NotTo(HaveOccurred())
					}
					if ds.Name == redis {
						deploymentName := "redisbench"
						log.Infof("Running Workloads on DataService %v ", ds.Name)
						pod, dep, err = pdslib.CreateDataServiceWorkloads(ds.Name, deployment.GetId(), "", "", deploymentName, namespace)
						Expect(err).NotTo(HaveOccurred())
					}
					if ds.Name == cassandra {
						deploymentName := "cassandra-stress"
						log.Infof("Running Workloads on DataService %v ", ds.Name)
						pod, dep, err = pdslib.CreateDataServiceWorkloads(ds.Name, deployment.GetId(), "", "", deploymentName, namespace)
						Expect(err).NotTo(HaveOccurred())
					}
				})

				Step("Scaling up the dataservice replicas", func() {
					updatedDeployment, err := pdslib.UpdateDataServices(deployment.GetId(),
						dataServiceDefaultAppConfigID, deployment.GetImageId(),
						int32(ds.ScaleReplicas), dataServiceDefaultResourceTemplateID, namespace)

					Expect(err).NotTo(HaveOccurred())

					log.Infof("Scaling up DataService %v ", ds.Name)
					log.InfoD("Scaling up DataService %v ", ds.Name)
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					Expect(err).NotTo(HaveOccurred())

					log.Infof("filesystem used %v ", config.Spec.StorageOptions.Filesystem)
					log.Infof("storage replicas used %v ", config.Spec.StorageOptions.Replicas)
					log.Infof("cpu requests used %v ", config.Spec.Resources.Requests.CPU)
					log.Infof("memory requests used %v ", config.Spec.Resources.Requests.Memory)
					log.Infof("storage requests used %v ", config.Spec.Resources.Requests.Storage)
					log.Infof("No of nodes requested %v ", config.Spec.Nodes)
					log.Infof("volume group %v ", storageOp.VolumeGroup)
					log.Infof("version/images used %v ", config.Spec.Version)

					Expect(resourceTemp.Resources.Requests.CPU).Should(Equal(config.Spec.Resources.Requests.CPU))
					Expect(resourceTemp.Resources.Requests.Memory).Should(Equal(config.Spec.Resources.Requests.Memory))
					Expect(resourceTemp.Resources.Requests.Storage).Should(Equal(config.Spec.Resources.Requests.Storage))
					Expect(resourceTemp.Resources.Limits.CPU).Should(Equal(config.Spec.Resources.Limits.CPU))
					Expect(resourceTemp.Resources.Limits.Memory).Should(Equal(config.Spec.Resources.Limits.Memory))
					repl, err := strconv.Atoi(config.Spec.StorageOptions.Replicas)
					Expect(err).NotTo(HaveOccurred())
					Expect(storageOp.Replicas).Should(Equal(int32(repl)))
					Expect(storageOp.Filesystem).Should(Equal(config.Spec.StorageOptions.Filesystem))
					Expect(config.Spec.Nodes).Should(Equal(int32(ds.ScaleReplicas)))
					for version, build := range dataServiceVersionBuildMap {
						Expect(config.Spec.Version).Should(Equal(version + "-" + build[0]))
					}
				})
				Step("Delete Deployments", func() {
					log.InfoD("Deleting DataService %v ", ds.Name)
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					Expect(err).NotTo(HaveOccurred())
					Expect(resp.StatusCode).Should(BeEquivalentTo(http.StatusAccepted))
					isDeploymentsDeleted = true
				})

				Step("Delete the worload generating deployments", func() {
					if ds.Name == "Cassandra" || ds.Name == "PostgreSQL" {
						log.InfoD("Deleting Workload Generating pods %v ", dep.Name)
						err = pdslib.DeleteK8sDeployments(dep.Name, namespace)
					} else {
						log.InfoD("Deleting Workload Generating pods %v ", pod.Name)
						err = pdslib.DeleteK8sPods(pod.Name, namespace)
					}
					Expect(err).NotTo(HaveOccurred())
				})
			}
		})
	})
	JustAfterEach(func() {
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
				Expect(err).NotTo(HaveOccurred())

				log.Infof("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

				dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, ds.Name)
				Expect(err).NotTo(HaveOccurred())
				Expect(dataServiceDefaultAppConfigID).NotTo(BeEmpty())

				log.Infof(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)

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
				Expect(err).NotTo(HaveOccurred())

				defer func() {
					if !isDeploymentsDeleted {
						Step("Delete created deployments")
						resp, err := pdslib.DeleteDeployment(deployment.GetId())
						Expect(err).NotTo(HaveOccurred())
						Expect(resp.StatusCode).Should(BeEquivalentTo(http.StatusAccepted))
					}
				}()

				Step("Validate Storage Configurations", func() {
					log.Infof("data service deployed %v ", ds.Name)
					log.InfoD("Validating DataService %v ", ds.Name)
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
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
					Expect(config.Spec.Nodes).Should(Equal(int32(ds.Replicas)))
				})

				Step("Delete Deployments", func() {
					log.InfoD("Deleting DataService %v ", ds.Name)
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					Expect(err).NotTo(HaveOccurred())
					Expect(resp.StatusCode).Should(BeEquivalentTo(http.StatusAccepted))
					isDeploymentsDeleted = true
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

func UpgradeDataService(dataservice, oldVersion, oldImage, dsVersion, dsBuild string, replicas int32) {
	Step("Deploy, Validate and Update Data Services", func() {
		isDeploymentsDeleted = false
		dataServiceDefaultResourceTemplateID, err = pdslib.GetResourceTemplate(tenantID, dataservice)
		Expect(err).NotTo(HaveOccurred())

		log.Infof("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

		dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, dataservice)
		Expect(err).NotTo(HaveOccurred())
		Expect(dataServiceDefaultAppConfigID).NotTo(BeEmpty())

		log.Infof(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)
		log.InfoD("Deploying DataService %v ", dataservice)
		deployment, _, dataServiceVersionBuildMap, err := pdslib.DeployDataServices(dataservice, projectID,
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
		Expect(err).NotTo(HaveOccurred())

		defer func() {
			if !isDeploymentsDeleted {
				Step("Delete created deployments")
				log.InfoD("Deleting DataService %v ", dataservice)
				resp, err := pdslib.DeleteDeployment(deployment.GetId())
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).Should(BeEquivalentTo(http.StatusAccepted))
			}
		}()

		Step("Validate Storage Configurations", func() {
			log.Infof("data service deployed %v ", dataservice)
			resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, dataservice, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
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
		})

		Step("Running Workloads before scaling up of dataservices ", func() {
			if dataservice == postgresql {
				deploymentName := "pgload"
				pod, dep, err = pdslib.CreateDataServiceWorkloads(dataservice, deployment.GetId(), "100", "1", deploymentName, namespace)
				Expect(err).NotTo(HaveOccurred())
			}
			if dataservice == rabbitmq {
				deploymentName := "rmq"
				pod, dep, err = pdslib.CreateDataServiceWorkloads(dataservice, deployment.GetId(), "", "", deploymentName, namespace)
				Expect(err).NotTo(HaveOccurred())
			}
			if dataservice == redis {
				deploymentName := "redisbench"
				pod, dep, err = pdslib.CreateDataServiceWorkloads(dataservice, deployment.GetId(), "", "", deploymentName, namespace)
				Expect(err).NotTo(HaveOccurred())
			}
			if dataservice == cassandra {
				deploymentName := "cassandra-stress"
				pod, dep, err = pdslib.CreateDataServiceWorkloads(dataservice, deployment.GetId(), "", "", deploymentName, namespace)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		Step("Update the data service patch versions", func() {
			log.Infof("Version/Build: %v %v", dsVersion, dsBuild)
			updatedDeployment, err := pdslib.UpdateDataServiceVerison(deployment.GetDataServiceId(), deployment.GetId(),
				dataServiceDefaultAppConfigID,
				replicas, dataServiceDefaultResourceTemplateID, dsBuild, namespace, dsVersion)

			Expect(err).NotTo(HaveOccurred())
			log.Infof("data service deployed %v ", dataservice)
			resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, dataservice, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
			Expect(err).NotTo(HaveOccurred())

			log.Infof("filesystem used %v ", config.Spec.StorageOptions.Filesystem)
			log.Infof("storage replicas used %v ", config.Spec.StorageOptions.Replicas)
			log.Infof("cpu requests used %v ", config.Spec.Resources.Requests.CPU)
			log.Infof("memory requests used %v ", config.Spec.Resources.Requests.Memory)
			log.Infof("storage requests used %v ", config.Spec.Resources.Requests.Storage)
			log.Infof("No of nodes requested %v ", config.Spec.Nodes)
			log.Infof("volume group %v ", storageOp.VolumeGroup)
			log.Infof("version/images used %v ", config.Spec.Version)

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
			for version, build := range dataServiceVersionBuildMap {
				Expect(config.Spec.Version).Should(Equal(version + "-" + build[0]))
			}
		})

		Step("Delete Deployments", func() {
			resp, err := pdslib.DeleteDeployment(deployment.GetId())
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).Should(BeEquivalentTo(http.StatusAccepted))
			isDeploymentsDeleted = true
		})

		defer func() {
			Step("Delete the worload generating deployments", func() {
				if dataservice == "Cassandra" || dataservice == "PostgreSQL" {
					err = pdslib.DeleteK8sDeployments(dep.Name, namespace)
				} else {
					err = pdslib.DeleteK8sPods(pod.Name, namespace)
				}
				Expect(err).NotTo(HaveOccurred())
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
			log.Infof("Created namespace: %v", nname)
			log.InfoD("Created namespace: %v", nname)
			Expect(err).NotTo(HaveOccurred())
			namespaces = append(namespaces, ns)
		}

		defer func() {
			for _, namespace := range namespaces {
				log.Infof("Cleanup: Deleting created namespace %v", namespace.Name)
				log.InfoD("Cleanup: Deleting created namespace %v", namespace.Name)
				err := pdslib.DeleteK8sPDSNamespace(namespace.Name)
				Expect(err).NotTo(HaveOccurred())
			}
		}()

		log.Info("Waiting for created namespaces to be available in PDS")
		log.InfoD("Waiting for created namespaces to be available in PDS")
		time.Sleep(10 * time.Second)

		Step("Deploy All Supported Data Services", func() {
			var cleanupall []string
			for _, namespace := range namespaces {
				log.Infof("Deploying deployment %v in namespace: %v", deploymentTargetID, namespace.Name)
				log.InfoD("Deploying deployment %v in namespace: %v", deploymentTargetID, namespace.Name)
				newNamespaceID, err := pdslib.GetnameSpaceID(namespace.Name, deploymentTargetID)
				Expect(err).NotTo(HaveOccurred())
				Expect(newNamespaceID).NotTo(BeEmpty())

				deps := DeployInANamespaceAndVerify(namespace.Name, newNamespaceID)
				cleanupall = append(cleanupall, deps...)
			}

			log.Infof("List of created deployments: %v ", cleanupall)
			log.InfoD("List of created deployments: %v ", cleanupall)
			Step("Delete created deployments", func() {
				for _, dep := range cleanupall {
					_, err := pdslib.DeleteDeployment(dep)
					Expect(err).NotTo(HaveOccurred())
				}
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
		Expect(err).NotTo(HaveOccurred())
		log.Infof("Created namespace: %v", nname)
		log.InfoD("Created namespace: %v", nname)

		log.Info("Waiting for created namespaces to be available in PDS")
		log.InfoD("Waiting for created namespaces to be available in PDS")
		time.Sleep(10 * time.Second)

		log.Info("Create dataservices")
		log.InfoD("Create dataservices")

		Step("Deploy All Supported Data Services in the namespace", func() {

			log.Infof("Deploying deployment %v in namespace: %v", deploymentTargetID, nname)
			newNamespaceID, err := pdslib.GetnameSpaceID(nname, deploymentTargetID)
			Expect(err).NotTo(HaveOccurred())
			Expect(newNamespaceID).NotTo(BeEmpty())

			var cleanup []*pds.ModelsDeployment
			for _, ds := range params.DataServiceToTest {
				isDeploymentsDeleted = false
				dataServiceDefaultResourceTemplateID, err = pdslib.GetResourceTemplate(tenantID, ds.Name)
				Expect(err).NotTo(HaveOccurred())

				log.Infof("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)
				log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

				dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, ds.Name)
				Expect(err).NotTo(HaveOccurred())
				Expect(dataServiceDefaultAppConfigID).NotTo(BeEmpty())

				log.Infof(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)
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
				Expect(err).NotTo(HaveOccurred())

				defer func() {
					if !isDeploymentsDeleted {
						Step("Delete created deployments")
						resp, err := pdslib.DeleteDeployment(deployment.GetId())
						Expect(err).NotTo(HaveOccurred())
						Expect(resp.StatusCode).Should(BeEquivalentTo(http.StatusAccepted))
					}
				}()

				Step("Validate Storage Configurations", func() {

					log.Infof("data service deployed %v ", ds)
					log.InfoD("data service deployed %v ", ds)
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, nname)
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
					Expect(config.Spec.Nodes).Should(Equal(int32(ds.Replicas)))
					cleanup = append(cleanup, deployment)

				})

			}

			log.Infof("List of created deployments: %v ", cleanup)
			log.InfoD("List of created deployments: %v ", cleanup)

			Step("Delete created namespace", func() {
				log.Infof("Cleanup: Deleting created namespace %v", nname)
				log.InfoD("Cleanup: Deleting created namespace %v", nname)
				err := pdslib.DeleteK8sPDSNamespace(nname)
				Expect(err).NotTo(HaveOccurred())
			})

			Step("Verify that the namespace was deleted", func() {
				err := pdslib.ValidateK8sNamespaceDeleted(nname)
				Expect(err).NotTo(HaveOccurred())
			})

			Step("Verify created deployments have been deleted", func() {
				for _, dep := range cleanup {
					err := pdslib.ValidateDataServiceDeploymentNegative(dep, nname)
					Expect(err).NotTo(HaveOccurred())
				}
				isDeploymentsDeleted = true
			})

		})

	})

	JustAfterEach(func() {
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
				Expect(err).NotTo(HaveOccurred())

				log.Infof("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

				dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, ds.Name)
				Expect(err).NotTo(HaveOccurred())
				Expect(dataServiceDefaultAppConfigID).NotTo(BeEmpty())

				log.Infof(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)

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
				Expect(err).NotTo(HaveOccurred())

				defer func() {
					if !isDeploymentsDeleted {
						Step("Delete created deployments")
						resp, err := pdslib.DeleteDeployment(deployment.GetId())
						Expect(err).NotTo(HaveOccurred())
						Expect(resp.StatusCode).Should(BeEquivalentTo(http.StatusAccepted))
					}
				}()

				Step("Validate Storage Configurations", func() {
					log.Infof("data service deployed %v ", ds.Name)
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
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
					Expect(config.Spec.Nodes).Should(Equal(int32(ds.Replicas)))
				})

				Step("Running Workloads before scaling up of dataservices ", func() {
					if ds.Name == postgresql {
						deploymentName := "pgload"
						pod, dep, err = pdslib.CreateDataServiceWorkloads(ds.Name, deployment.GetId(), "100", "1", deploymentName, namespace)
						Expect(err).NotTo(HaveOccurred())
					}
					if ds.Name == rabbitmq {
						deploymentName := "rmq"
						pod, dep, err = pdslib.CreateDataServiceWorkloads(ds.Name, deployment.GetId(), "", "", deploymentName, namespace)
						Expect(err).NotTo(HaveOccurred())
					}
					if ds.Name == redis {
						deploymentName := "redisbench"
						pod, dep, err = pdslib.CreateDataServiceWorkloads(ds.Name, deployment.GetId(), "", "", deploymentName, namespace)
						Expect(err).NotTo(HaveOccurred())
					}
					if ds.Name == cassandra {
						deploymentName := "cassandra-stress"
						pod, dep, err = pdslib.CreateDataServiceWorkloads(ds.Name, deployment.GetId(), "", "", deploymentName, namespace)
						Expect(err).NotTo(HaveOccurred())
					}
				})

				defer func() {
					Step("Delete the workload generating deployments", func() {
						if ds.Name == "Cassandra" || ds.Name == "PostgreSQL" {
							err = pdslib.DeleteK8sDeployments(dep.Name, namespace)
						} else {
							err = pdslib.DeleteK8sPods(pod.Name, namespace)
						}
						Expect(err).NotTo(HaveOccurred())
					})
				}()

				var deploymentPods []corev1.Pod
				Step("Get a list of pod names that belong to the deployment", func() {
					deploymentPods, err = pdslib.GetPodsFromK8sStatefulSet(deployment, namespace)
					Expect(err).NotTo(HaveOccurred())
					Expect(deploymentPods).NotTo(BeEmpty())
				})

				var nodeList []*corev1.Node
				Step("Get the node that the PV of the pod resides on", func() {
					for _, pod := range deploymentPods {
						log.Infof("The pod spec node name: %v", pod.Spec.NodeName)
						log.InfoD("The pod spec node name: %v", pod.Spec.NodeName)
						nodeObject, err := pdslib.GetK8sNodeObjectUsingPodName(pod.Spec.NodeName)
						Expect(err).NotTo(HaveOccurred())
						nodeList = append(nodeList, nodeObject)
					}
				})

				Step("For each node in the nodelist, stop px service on it", func() {

					for _, node := range nodeList {
						err := pdslib.LabelK8sNode(node, "px/service=stop")
						Expect(err).NotTo(HaveOccurred())
					}

					log.Info("Finished labeling the nodes...")
					log.InfoD("Finished labeling the nodes...")
					time.Sleep(30 * time.Second)

				})

				Step("Validate that the deployment is healthy", func() {
					err := pdslib.ValidateDataServiceDeployment(deployment, namespace)
					Expect(err).NotTo(HaveOccurred())
				})

				Step("Cleanup: Start px on the node and uncordon the node", func() {
					for _, node := range nodeList {
						err := pdslib.RemoveLabelFromK8sNode(node, "px/service")
						Expect(err).NotTo(HaveOccurred())
					}

					log.Info("Finished removing labels from the nodes...")
					log.InfoD("Finished removing labels from the nodes...")

					for _, node := range nodeList {
						err := pdslib.DrainPxPodOnK8sNode(node, pxnamespace)
						Expect(err).NotTo(HaveOccurred())
					}

					log.Info("Finished draining px pods from the nodes...")
					log.InfoD("Finished draining px pods from the nodes...")

					for _, node := range nodeList {
						err := pdslib.UnCordonK8sNode(node)
						Expect(err).NotTo(HaveOccurred())
					}

					log.Infof("Finished uncordoning the node...")
					log.InfoD("Finished uncordoning the node...")

					log.Info("Verify that the px pod has started on node...")
					log.InfoD("Verify that the px pod has started on node...")
					// Read log lines of the px pod on the node to see if the service is running
					for _, node := range nodeList {
						rc, err := pdslib.VerifyPxPodOnNode(node.Name, pxnamespace)
						Expect(rc).To(BeTrue())
						Expect(err).NotTo(HaveOccurred())
					}

				})

				Step("Delete Deployments", func() {
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					Expect(err).NotTo(HaveOccurred())
					Expect(resp.StatusCode).Should(BeEquivalentTo(http.StatusAccepted))
					isDeploymentsDeleted = true
				})
			}
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
	})

})

func DeployInANamespaceAndVerify(nname string, namespaceID string) []string {

	var cleanup []string
	for _, ds := range params.DataServiceToTest {
		dataServiceDefaultResourceTemplateID, err = pdslib.GetResourceTemplate(tenantID, ds.Name)
		Expect(err).NotTo(HaveOccurred())

		log.Infof("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)
		log.InfoD("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

		dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, ds.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(dataServiceDefaultAppConfigID).NotTo(BeEmpty())

		log.Infof(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)
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
			nname,
		)
		Expect(err).NotTo(HaveOccurred())

		Step("Validate Storage Configurations", func() {

			log.Infof("data service deployed %v ", ds)
			log.InfoD("data service deployed %v ", ds)
			resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, nname)
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
			Expect(config.Spec.Nodes).Should(Equal(int32(ds.Replicas)))
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
				log.Infof("Deploying DataService %v ", ds.Name)
				isDeploymentsDeleted = false
				dataServiceDefaultResourceTemplateID, err = pdslib.GetResourceTemplate(tenantID, ds.Name)
				dash.VerifyFatal(err, nil, "Verifying data service deployment.")

				log.Infof("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

				dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, ds.Name)
				Expect(err).NotTo(HaveOccurred())
				Expect(dataServiceDefaultAppConfigID).NotTo(BeEmpty())

				log.Infof(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)

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
				Expect(err).NotTo(HaveOccurred())

				Step("Validate Storage Configurations", func() {
					log.Infof("data service deployed %v ", ds.Name)
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
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
					Expect(config.Spec.Nodes).Should(Equal(int32(ds.Replicas)))
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
						Expect(err).NotTo(HaveOccurred())

						log.Infof("wait for node: %s to be back up", n.Name)
						err = Inst().N.TestConnection(n, node.ConnectionOpts{
							Timeout:         defaultTestConnectionTimeout,
							TimeBeforeRetry: defaultWaitRebootRetry,
						})
						if err != nil {
							log.FailOnError(err, "Error while testing node status %v, err: %v", n.Name, err.Error())
						}
						Expect(err).NotTo(HaveOccurred())
					}

				})

				Step("Validate Deployments after nodes are up", func() {
					log.Info("Validate Deployments after nodes are up")
					err = pdslib.ValidateDataServiceDeployment(deployment, namespace)
					Expect(err).NotTo(HaveOccurred())
					log.Info("Deployments pods are up and healthy")
				})

				Step("Delete the deployment", func() {
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					Expect(err).NotTo(HaveOccurred())
					Expect(resp.StatusCode).Should(BeEquivalentTo(http.StatusAccepted))
				})
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})
