package tests

import (
	"net/http"
	"os"
	"strconv"
	"testing"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	"github.com/portworx/torpedo/pkg/aetosutil"
	. "github.com/portworx/torpedo/tests"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	deploymentName          = "qa"
	envDeployAllDataService = "DEPLOY_ALL_DATASERVICE"
	postgresql              = "PostgreSQL"
	cassandra               = "Cassandra"
	redis                   = "Redis"
	rabbitmq                = "RabbitMQ"
)

var (
	namespace                               string
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
	dash                                    *aetosutil.Dashboard
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
		dash = Inst().Dash
		dash.TestSetBegin(dash.TestSet)
		pdsparams := Inst().PDSParams
		params, err = pdslib.ReadParams(pdsparams)
		Expect(err).NotTo(HaveOccurred())
		infraParams := params.InfraToTest

		tenantID, dnsZone, projectID, serviceType, deploymentTargetID, err = pdslib.SetupPDSTest(infraParams.ControlPlaneURL, infraParams.ClusterType, infraParams.AccountName)
		Expect(err).NotTo(HaveOccurred())
	})

	Step("Get StorageTemplateID and Replicas", func() {
		storageTemplateID, err = pdslib.GetStorageTemplate(tenantID)
		Expect(err).NotTo(HaveOccurred())
		logrus.Infof("storageTemplateID %v", storageTemplateID)
	})

	Step("Create/Get Namespace and NamespaceID", func() {
		namespace = params.InfraToTest.Namespace
		isavailabbe, err := pdslib.CheckNamespace(namespace)
		Expect(err).NotTo(HaveOccurred())
		Expect(isavailabbe).To(BeTrue())
		namespaceID, err = pdslib.GetnameSpaceID(namespace, deploymentTargetID)
		Expect(err).NotTo(HaveOccurred())
		Expect(namespaceID).NotTo(BeEmpty())
	})
})

var _ = AfterSuite(func() {
	defer dash.TestSetEnd()
	defer dash.TestCaseEnd()
})

var _ = Describe("{ScaleUPDataServices}", func() {

	It("deploy Dataservices", func() {
		logrus.Info("Create dataservices without backup.")
		Step("Deploy Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				if ds.Name == "MySQL" {
					continue
				}
				isDeploymentsDeleted = false
				dataServiceDefaultResourceTemplateID, err = pdslib.GetResourceTemplate(tenantID, ds.Name)
				Expect(err).NotTo(HaveOccurred())

				logrus.Infof("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

				dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, ds.Name)
				Expect(err).NotTo(HaveOccurred())
				Expect(dataServiceDefaultAppConfigID).NotTo(BeEmpty())

				logrus.Infof(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)

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
					logrus.Infof("data service deployed %v ", ds.Name)
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					Expect(err).NotTo(HaveOccurred())
					logrus.Infof("filesystem used %v ", config.Spec.StorageOptions.Filesystem)
					logrus.Infof("storage replicas used %v ", config.Spec.StorageOptions.Replicas)
					logrus.Infof("cpu requests used %v ", config.Spec.Resources.Requests.CPU)
					logrus.Infof("memory requests used %v ", config.Spec.Resources.Requests.Memory)
					logrus.Infof("storage requests used %v ", config.Spec.Resources.Requests.Storage)
					logrus.Infof("No of nodes requested %v ", config.Spec.Nodes)
					logrus.Infof("volume group %v ", storageOp.VolumeGroup)

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

				Step("Scaling up the dataservice replicas", func() {

					updatedDeployment, err := pdslib.UpdateDataServices(deployment.GetId(),
						dataServiceDefaultAppConfigID, deployment.GetImageId(),
						int32(ds.ScaleReplicas), dataServiceDefaultResourceTemplateID, namespace)

					Expect(err).NotTo(HaveOccurred())

					logrus.Infof("data service deployed %v ", ds.Name)
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					Expect(err).NotTo(HaveOccurred())

					logrus.Infof("filesystem used %v ", config.Spec.StorageOptions.Filesystem)
					logrus.Infof("storage replicas used %v ", config.Spec.StorageOptions.Replicas)
					logrus.Infof("cpu requests used %v ", config.Spec.Resources.Requests.CPU)
					logrus.Infof("memory requests used %v ", config.Spec.Resources.Requests.Memory)
					logrus.Infof("storage requests used %v ", config.Spec.Resources.Requests.Storage)
					logrus.Infof("No of nodes requested %v ", config.Spec.Nodes)
					logrus.Infof("volume group %v ", storageOp.VolumeGroup)
					logrus.Infof("version/images used %v ", config.Spec.Version)

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
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					Expect(err).NotTo(HaveOccurred())
					Expect(resp.StatusCode).Should(BeEquivalentTo(http.StatusAccepted))
					isDeploymentsDeleted = true
				})

				Step("Delete the worload generating deployments", func() {
					if ds.Name == "Cassandra" || ds.Name == "PostgreSQL" {
						err = pdslib.DeleteK8sDeployments(dep.Name, namespace)
					} else {
						err = pdslib.DeleteK8sPods(pod.Name, namespace)
					}
					Expect(err).NotTo(HaveOccurred())
				})
			}
		})
	})
})

var _ = Describe("{UpgradeDataServiceVersion}", func() {
	It("runs the dataservice version upgrade test", func() {
		for _, ds := range params.DataServiceToTest {
			UpgradeDataService(ds.Name, ds.OldVersion, ds.OldImage, ds.Version, ds.Image, int32(ds.Replicas))
		}
	})
})

var _ = Describe("{UpgradeDataServiceImage}", func() {
	It("runs the dataservice build image upgrade test", func() {
		for _, ds := range params.DataServiceToTest {
			UpgradeDataService(ds.Name, ds.Version, ds.OldImage, ds.Version, ds.Image, int32(ds.Replicas))
		}
	})
})

var _ = Describe("{DeployDataServicesOnDemand}", func() {
	It("Deploy DataservicesOnDemand", func() {
		logrus.Info("Create dataservices without backup.")
		Step("Deploy, Validate and Delete Data Services", func() {
			for _, ds := range params.DataServiceToTest {
				isDeploymentsDeleted = false
				dataServiceDefaultResourceTemplateID, err = pdslib.GetResourceTemplate(tenantID, ds.Name)
				Expect(err).NotTo(HaveOccurred())

				logrus.Infof("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

				dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, ds.Name)
				Expect(err).NotTo(HaveOccurred())
				Expect(dataServiceDefaultAppConfigID).NotTo(BeEmpty())

				logrus.Infof(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)

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
					logrus.Infof("data service deployed %v ", ds.Name)
					resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, ds.Name, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
					Expect(err).NotTo(HaveOccurred())
					logrus.Infof("filesystem used %v ", config.Spec.StorageOptions.Filesystem)
					logrus.Infof("storage replicas used %v ", config.Spec.StorageOptions.Replicas)
					logrus.Infof("cpu requests used %v ", config.Spec.Resources.Requests.CPU)
					logrus.Infof("memory requests used %v ", config.Spec.Resources.Requests.Memory)
					logrus.Infof("storage requests used %v ", config.Spec.Resources.Requests.Storage)
					logrus.Infof("No of nodes requested %v ", config.Spec.Nodes)
					logrus.Infof("volume group %v ", storageOp.VolumeGroup)

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
					resp, err := pdslib.DeleteDeployment(deployment.GetId())
					Expect(err).NotTo(HaveOccurred())
					Expect(resp.StatusCode).Should(BeEquivalentTo(http.StatusAccepted))
					isDeploymentsDeleted = true
				})
			}
		})
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
				logrus.Infof("supported data service %v ", supportedDataServices[index])
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
						logrus.Infof("data service deployed %v ", ds)
						resourceTemp, storageOp, config, err := pdslib.ValidateAllDataServiceVolumes(deployment[index], ds, dataServiceDefaultResourceTemplateIDMap, storageTemplateID)
						Expect(err).NotTo(HaveOccurred())
						logrus.Infof("filesystem used %v ", config.Spec.StorageOptions.Filesystem)
						logrus.Infof("storage replicas used %v ", config.Spec.StorageOptions.Replicas)
						logrus.Infof("cpu requests used %v ", config.Spec.Resources.Requests.CPU)
						logrus.Infof("memory requests used %v ", config.Spec.Resources.Requests.Memory)
						logrus.Infof("storage requests used %v ", config.Spec.Resources.Requests.Storage)
						logrus.Infof("No of nodes requested %v ", config.Spec.Nodes)
						logrus.Infof("volume group %v ", storageOp.VolumeGroup)

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

		logrus.Infof("dataServiceDefaultResourceTemplateID %v ", dataServiceDefaultResourceTemplateID)

		dataServiceDefaultAppConfigID, err = pdslib.GetAppConfTemplate(tenantID, dataservice)
		Expect(err).NotTo(HaveOccurred())
		Expect(dataServiceDefaultAppConfigID).NotTo(BeEmpty())

		logrus.Infof(" dataServiceDefaultAppConfigID %v ", dataServiceDefaultAppConfigID)

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
				resp, err := pdslib.DeleteDeployment(deployment.GetId())
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).Should(BeEquivalentTo(http.StatusAccepted))
			}
		}()

		Step("Validate Storage Configurations", func() {
			logrus.Infof("data service deployed %v ", dataservice)
			resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment, dataservice, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
			Expect(err).NotTo(HaveOccurred())
			logrus.Infof("filesystem used %v ", config.Spec.StorageOptions.Filesystem)
			logrus.Infof("storage replicas used %v ", config.Spec.StorageOptions.Replicas)
			logrus.Infof("cpu requests used %v ", config.Spec.Resources.Requests.CPU)
			logrus.Infof("memory requests used %v ", config.Spec.Resources.Requests.Memory)
			logrus.Infof("storage requests used %v ", config.Spec.Resources.Requests.Storage)
			logrus.Infof("No of nodes requested %v ", config.Spec.Nodes)
			logrus.Infof("volume group %v ", storageOp.VolumeGroup)

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
			logrus.Infof("Version/Build: %v %v", dsVersion, dsBuild)
			updatedDeployment, err := pdslib.UpdateDataServiceVerison(deployment.GetDataServiceId(), deployment.GetId(),
				dataServiceDefaultAppConfigID,
				replicas, dataServiceDefaultResourceTemplateID, dsBuild, namespace, dsVersion)

			Expect(err).NotTo(HaveOccurred())
			logrus.Infof("data service deployed %v ", dataservice)
			resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(updatedDeployment, dataservice, dataServiceDefaultResourceTemplateID, storageTemplateID, namespace)
			Expect(err).NotTo(HaveOccurred())

			logrus.Infof("filesystem used %v ", config.Spec.StorageOptions.Filesystem)
			logrus.Infof("storage replicas used %v ", config.Spec.StorageOptions.Replicas)
			logrus.Infof("cpu requests used %v ", config.Spec.Resources.Requests.CPU)
			logrus.Infof("memory requests used %v ", config.Spec.Resources.Requests.Memory)
			logrus.Infof("storage requests used %v ", config.Spec.Resources.Requests.Storage)
			logrus.Infof("No of nodes requested %v ", config.Spec.Nodes)
			logrus.Infof("volume group %v ", storageOp.VolumeGroup)
			logrus.Infof("version/images used %v ", config.Spec.Version)

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

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
