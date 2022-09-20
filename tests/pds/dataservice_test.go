package tests

import (
	"net/http"
	"strconv"
	"testing"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	pdslib "github.com/portworx/torpedo/drivers/pds/lib"
	. "github.com/portworx/torpedo/tests"
	"github.com/sirupsen/logrus"
)

const (
	deploymentName          = "qa"
	envDsVersion            = "DS_VERSION"
	envDsBuild              = "DS_BUILD"
	envReplicas             = "NO_OF_NODES"
	envNamespace            = "NAMESPACE"
	envDataService          = "DATA_SERVICE"
	envDeployAllVersions    = "DEPLOY_ALL_VERSIONS"
	envDeployAllDataService = "DEPLOY_ALL_DATASERVICE"
	envControlPlaneURL      = "CONTROL_PLANE_URL"
	envClusterType          = "CLUSTER_TYPE"
	envTargetClusterName    = "TARGET_CLUSTER_NAME"
	envDeployAllImages      = "DEPLOY_ALL_IMAGES"
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
	DeployAllDataService                    bool
	DeployAllVersions                       bool
	DeployAllImages                         bool
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
		ControlPlaneURL := pdslib.GetAndExpectStringEnvVar(envControlPlaneURL)
		Expect(ControlPlaneURL).NotTo(BeEmpty(), "ENV "+envControlPlaneURL+" is not set")

		ClusterType := pdslib.GetAndExpectStringEnvVar(envClusterType)
		Expect(ClusterType).NotTo(BeEmpty(), "ENV "+envClusterType+" is not set")

		TargetClusterName := pdslib.GetAndExpectStringEnvVar(envTargetClusterName)
		Expect(TargetClusterName).NotTo(BeEmpty(), "ENV "+envTargetClusterName+" is not set")

		DeployAllDataService, err = pdslib.GetAndExpectBoolEnvVar(envDeployAllDataService)
		Expect(err).NotTo(HaveOccurred())

		DeployAllVersions, err = pdslib.GetAndExpectBoolEnvVar(envDeployAllVersions)
		Expect(err).NotTo(HaveOccurred())

		DeployAllImages, err = pdslib.GetAndExpectBoolEnvVar(envDeployAllImages)
		Expect(err).NotTo(HaveOccurred())

		tenantID, dnsZone, projectID, serviceType, deploymentTargetID, err = pdslib.SetupPDSTest(ControlPlaneURL, ClusterType, TargetClusterName)
		Expect(err).NotTo(HaveOccurred())

		DataService := pdslib.GetAndExpectStringEnvVar(envDataService)
		Expect(DataService).NotTo(BeEmpty(), "ENV "+envDataService+" is not set")

	})

	Step("Get StorageTemplateID and Replicas", func() {
		storageTemplateID = pdslib.GetStorageTemplate(tenantID)
		logrus.Infof("storageTemplateID %v", storageTemplateID)
		rep, err := pdslib.GetAndExpectIntEnvVar(envReplicas)
		Expect(err).NotTo(HaveOccurred())
		replicas = int32(rep)

	})

	Step("Create/Get Namespace and NamespaceID", func() {
		namespace = pdslib.GetAndExpectStringEnvVar(envNamespace)
		Expect(namespace).NotTo(BeEmpty(), "ENV "+envNamespace+" is not set")
		namespaceID, err = pdslib.GetnameSpaceID(namespace, deploymentTargetID)
		Expect(err).NotTo(HaveOccurred())
		Expect(namespaceID).NotTo(BeEmpty())
	})
})

var _ = Describe("{DeployDataServicesOnDemand}", func() {

	JustBeforeEach(func() {
		if !DeployAllDataService {
			supportedDataServices = append(supportedDataServices, pdslib.GetAndExpectStringEnvVar(envDataService))
			for _, ds := range supportedDataServices {
				logrus.Infof("supported dataservices %v", ds)
			}
			Step("Get the resource and app config template for supported dataservice", func() {
				dataServiceDefaultResourceTemplateIDMap, dataServiceNameIDMap, err = pdslib.GetResourceTemplate(tenantID, supportedDataServices)
				Expect(err).NotTo(HaveOccurred())

				dataServiceNameDefaultAppConfigMap, err = pdslib.GetAppConfTemplate(tenantID, dataServiceNameIDMap)
				Expect(err).NotTo(HaveOccurred())
				Expect(dataServiceNameDefaultAppConfigMap).NotTo(BeEmpty())
			})
		} else {
			Expect(DeployAllDataService).To(Equal(true))
		}
	})

	It("deploy Dataservices", func() {
		logrus.Info("Create dataservices without backup.")
		Step("Deploy Data Services", func() {
			deployements, _, err := pdslib.DeployDataServices(dataServiceNameIDMap, projectID,
				deploymentTargetID,
				dnsZone,
				deploymentName,
				namespaceID,
				dataServiceNameDefaultAppConfigMap,
				replicas,
				serviceType,
				dataServiceDefaultResourceTemplateIDMap,
				storageTemplateID,
				DeployAllVersions,
				DeployAllImages,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(deployements).NotTo(BeEmpty())
			Step("Validate Storage Configurations", func() {
				for ds, deployment := range deployements {
					for index := range deployment {
						logrus.Infof("data service deployed %v ", ds)
						resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment[index], ds, dataServiceDefaultResourceTemplateIDMap, storageTemplateID)
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
				for _, dep := range deployements {
					for index := range dep {
						resp, err := pdslib.DeleteDeployment(dep[index].GetId())
						Expect(err).NotTo(HaveOccurred())
						Expect(resp.StatusCode).Should(BeEquivalentTo(http.StatusAccepted))
					}
				}
			}()
		})
	})
})

var _ = Describe("{DeployAllDataServices}", func() {

	JustBeforeEach(func() {
		Step("Check the required env param is available to run this test", func() {
			if !DeployAllDataService && DeployAllVersions {
				logrus.Fatal("Env Var are not set as expected")
			}
		})
		Step("Get All Supported Dataservices and Versions", func() {
			supportedDataServicesNameIDMap = pdslib.GetAllSupportedDataServices()
			for dsName := range supportedDataServicesNameIDMap {
				supportedDataServices = append(supportedDataServices, dsName)
			}
			for index := range supportedDataServices {
				logrus.Infof("supported data service %v ", supportedDataServices[index])
			}
			Step("Get the resource and app config template for supported dataservice", func() {
				dataServiceDefaultResourceTemplateIDMap, dataServiceNameIDMap, err = pdslib.GetResourceTemplate(tenantID, supportedDataServices)
				Expect(err).NotTo(HaveOccurred())
				Expect(dataServiceDefaultResourceTemplateIDMap).NotTo(BeEmpty())
				Expect(dataServiceNameIDMap).NotTo(BeEmpty())

				dataServiceNameDefaultAppConfigMap, err = pdslib.GetAppConfTemplate(tenantID, dataServiceNameIDMap)
				Expect(err).NotTo(HaveOccurred())
				Expect(dataServiceNameDefaultAppConfigMap).NotTo(BeEmpty())

			})

		})

	})

	It("Deploy All SupportedDataServices", func() {
		Step("Deploy All Supported Data Services", func() {
			deployements, _, err := pdslib.DeployDataServices(supportedDataServicesNameIDMap, projectID,
				deploymentTargetID,
				dnsZone,
				deploymentName,
				namespaceID,
				dataServiceNameDefaultAppConfigMap,
				replicas,
				serviceType,
				dataServiceDefaultResourceTemplateIDMap,
				storageTemplateID,
				DeployAllVersions,
				DeployAllImages,
			)
			Expect(err).NotTo(HaveOccurred())
			Step("Validate Storage Configurations", func() {
				for ds, deployment := range deployements {
					for index := range deployment {
						logrus.Infof("data service deployed %v ", ds)
						resourceTemp, storageOp, config, err := pdslib.ValidateDataServiceVolumes(deployment[index], ds, dataServiceDefaultResourceTemplateIDMap, storageTemplateID)
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
				for _, dep := range deployements {
					for index := range dep {
						_, err := pdslib.DeleteDeployment(dep[index].GetId())
						Expect(err).NotTo(HaveOccurred())
					}
				}
			}()
		})
	})
})

// func TestMain(m *testing.M) {
// 	// call flag.Parse() here if TestMain uses flags
// 	ParseFlags()
// 	os.Exit(m.Run())
// }
