package tests

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/vcluster"
	"github.com/portworx/torpedo/pkg/aututils"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/snapshotutils"
	. "github.com/portworx/torpedo/tests"

	v1 "k8s.io/api/core/v1"
	storageApi "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("{CreateAndRunFioOnVcluster}", func() {
	vc := &vcluster.VCluster{}
	var scName string
	var pvcName string
	var appNS string
	fioOptions := vcluster.FIOOptions{
		Name:      "mytest",
		IOEngine:  "libaio",
		RW:        "randwrite",
		BS:        "4k",
		NumJobs:   1,
		Size:      "500m",
		TimeBased: true,
		Runtime:   "600s",
		EndFsync:  1,
	}
	JustBeforeEach(func() {
		StartTorpedoTest("CreateAndRunFioOnVcluster", "Create, Connect and run FIO Application on Vcluster", nil, 0)
		vc, err = vcluster.NewVCluster("my-vcluster1")
		log.FailOnError(err, "Failed to initialise VCluster")
		err = vc.CreateAndWaitVCluster()
		log.FailOnError(err, "Failed to create VCluster")
	})
	It("Create FIO app on VCluster and run it for 10 minutes", func() {
		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("fio-app-sc-%v", time.Now().Unix())
		err = CreateStorageClass(scName)
		log.FailOnError(err, "Error creating Storageclass")
		log.Infof("Successfully created StorageClass with name: %v", scName)
		// Create PVC on VCluster
		appNS = scName + "-ns"
		pvcName, err = vc.CreatePVC("", scName, appNS, "")
		log.FailOnError(err, fmt.Sprintf("Error creating PVC with Storageclass name %v", scName))
		log.Infof("Successfully created PVC with name: %v", pvcName)
		jobName := "fio-job"
		// Create FIO Deployment on VCluster using the above PVC
		err = vc.CreateFIODeployment(pvcName, appNS, fioOptions, jobName, 12*time.Minute)
		log.FailOnError(err, "Error in creating FIO Application")
		log.Infof("Successfully ran FIO on Vcluster")
	})
	JustAfterEach(func() {
		// VCluster, StorageClass and Namespace cleanup
		err := vc.VClusterCleanup(scName)
		if err != nil {
			log.Errorf("Problem in Cleanup: %v", err)
		} else {
			log.Infof("Cleanup successfully done.")
		}
	})
})

var _ = Describe("{CreateAndRunMultipleFioOnVcluster}", func() {
	vc := &vcluster.VCluster{}
	var scName string
	var appNS string
	envValueIterations := vcluster.ReadEnvVariable("VCLUSTER_TOTAL_ITERATIONS")
	envValueBatch := vcluster.ReadEnvVariable("VCLUSTER_PARALLEL_APPS")
	batchCount := 2
	totalIterations := 1
	if envValueIterations != "" {
		var err error
		totalIterations, err = strconv.Atoi(envValueIterations)
		if err != nil {
			log.Errorf("Failed to convert value %v to int with error: %v", envValueIterations, err)
			totalIterations = 1
		}
	}
	if envValueBatch != "" {
		var err error
		batchCount, err = strconv.Atoi(envValueBatch)
		if err != nil {
			log.Errorf("Failed to convert value %v to int with error: %v", envValueBatch, err)
			batchCount = 2
		}
	}
	fioOptions := vcluster.FIOOptions{
		Name:      "mytest",
		IOEngine:  "libaio",
		RW:        "randwrite",
		BS:        "4k",
		NumJobs:   1,
		Size:      "500m",
		TimeBased: true,
		Runtime:   "100s",
		EndFsync:  1,
	}
	JustBeforeEach(func() {
		StartTorpedoTest("CreateAndRunMultipleFioOnVcluster", "Create, Connect and run Multiple FIO Applications on Same Vcluster", nil, 0)
		vc, err = vcluster.NewVCluster("my-vcluster1")
		log.FailOnError(err, "Failed to initialise VCluster")
		err = vc.CreateAndWaitVCluster()
		log.FailOnError(err, "Failed to create VCluster")
	})
	It("Create Multiple FIO apps on VCluster and run it for 10 minutes", func() {
		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("fio-app-sc-%v", time.Now().Unix())
		err = CreateStorageClass(scName)
		log.FailOnError(err, "Error creating Storageclass")
		log.Infof("Successfully created StorageClass with name: %v", scName)
		appNS = scName + "-ns"
		for i := 0; i < totalIterations; i++ {
			var wg sync.WaitGroup
			var jobNames []string
			for j := 0; j < batchCount; j++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					pvcNameSuffix := fmt.Sprintf("-pvc-%d-%d-%d", i, j, idx)
					jobName := fmt.Sprintf("fio-job-%d-%d-%d", i, j, idx)
					jobNames = append(jobNames, jobName)
					pvcName, err := vc.CreatePVC(scName+pvcNameSuffix, scName, appNS, "")
					log.FailOnError(err, fmt.Sprintf("Error creating PVC %v with Storageclass name %v", pvcName, scName))
					log.Infof("Successfully created PVC with name: %v", pvcName)
					// Create FIO Deployment on VCluster using the above PVC
					err = vc.CreateFIODeployment(pvcName, appNS, fioOptions, jobName, 10*time.Minute)
					log.FailOnError(err, "Error in creating FIO Application for PVC "+pvcName)
				}(i + j)
			}
			wg.Wait()
			log.Infof("Successfully ran FIO on Vcluster for batch starting at %d", i)
			for _, jobName := range jobNames {
				err := vc.DeleteJobOnVcluster(appNS, jobName)
				log.FailOnError(err, fmt.Sprintf("Error deleting FIO Job: %v", jobName))
				log.Infof("Removed FIO Job : %v successfully from vcluster %v", jobName, vc.Name)
			}
			log.Infof("Successfully removed all FIO Jobs from Vcluster")
		}
	})
	JustAfterEach(func() {
		// VCluster, StorageClass and Namespace cleanup
		err := vc.VClusterCleanup(scName)
		if err != nil {
			log.Errorf("Problem in Cleanup: %v", err)
		} else {
			log.Infof("Cleanup successfully done.")
		}
	})
})

var _ = Describe("{ScaleUpScaleDownAppOnVcluster}", func() {
	vc := &vcluster.VCluster{}
	var scName string
	var pvcName string
	var appNS string
	JustBeforeEach(func() {
		StartTorpedoTest("ScaleUpScaleDownAppOnVcluster", "Creates Nginx Deployment on Vcluster, Scales it up and then scale it down", nil, 0)
		vc, err = vcluster.NewVCluster("my-vcluster1")
		log.FailOnError(err, "Failed to initialise VCluster")
		err = vc.CreateAndWaitVCluster()
		log.FailOnError(err, "Failed to create VCluster")
	})
	It("Create Nginx Deployment on VCluster, Sclae it up, Scale it down and Delete it from Vcluster", func() {
		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("nginx-app-sc-%v", time.Now().Unix())
		err = CreateStorageClass(scName)
		log.FailOnError(err, "Error creating Storageclass")
		log.Infof("Successfully created StorageClass with name: %v", scName)
		// Create PVC on VCluster
		appNS = scName + "-ns"
		pvcName, err = vc.CreatePVC("", scName, appNS, "RWX")
		log.FailOnError(err, fmt.Sprintf("Error creating PVC with Storageclass name %v", scName))
		log.Infof("Successfully created PVC with name: %v", pvcName)
		deploymentName := "nginx-deployment"
		// Create Nginx Deployment on VCluster using the above PVC
		err = vc.CreateNginxDeployment(pvcName, appNS, deploymentName)
		log.FailOnError(err, "Error in creating Nginx Application")
		log.Infof("Successfully created Nginx App on Vcluster")
		log.Infof("Hard Sleep for 10 seconds after creation of Nginx Deployment")
		time.Sleep(10 * time.Second)
		// Trying to Scale up the Deployment
		log.Infof("Trying to Scale up this Nginx Deployment to 3 replicas")
		err = vc.ScaleVclusterDeployment(appNS, deploymentName, 3)
		log.FailOnError(err, "Failed to Scale up the Nginx Deployment")
		log.Infof("Successfully triggered scale up of Nginx Deployment %v Running on Vcluster %v to 3 replicas", deploymentName, vc.Name)
		// Validating if Nginx App has really scaled up
		err = vc.ValidateDeploymentScaling(appNS, deploymentName, 3)
		log.FailOnError(err, "Failed to Scale up the Nginx Deployment")
		log.Infof("Successfully Scaled Nginx Deployment %v Running on Vcluster %v to 3 replicas", deploymentName, vc.Name)
		// Trying to Scale Down the Deployment
		log.Infof("Trying to Scale up this Nginx Deployment to 1 replicas")
		err = vc.ScaleVclusterDeployment(appNS, deploymentName, 1)
		log.FailOnError(err, "Failed to Scale down the Nginx Deployment")
		log.Infof("Successfully triggered scale down of Nginx Deployment %v Running on Vcluster %v to 1 replicas", deploymentName, vc.Name)
		// Validating if Nginx App has really scaled down
		err = vc.ValidateDeploymentScaling(appNS, deploymentName, 1)
		log.FailOnError(err, "Failed to Scale down the Nginx Deployment")
		log.Infof("Successfully Scaled down Nginx Deployment %v Running on Vcluster %v to 1 replicas", deploymentName, vc.Name)
		// Trying to Delete NGinx Deployment now
		err = vc.DeleteDeploymentOnVCluster(appNS, deploymentName)
		log.FailOnError(err, "Failed to delete Nginx Deployment name %v on Vcluster %v", deploymentName, vc.Name)
		log.Infof("Successfully deleted Nginx deployment %v on Vcluster %v", deploymentName, vc.Name)
	})
	JustAfterEach(func() {
		// VCluster, StorageClass and Namespace cleanup
		err := vc.VClusterCleanup(scName)
		if err != nil {
			log.Errorf("Problem in Cleanup: %v", err)
		} else {
			log.Infof("Cleanup successfully done.")
		}
	})
})

var _ = Describe("{CreateAndRunFioOnVclusterRWX}", func() {
	vc := &vcluster.VCluster{}
	var scName string
	var pvcName string
	var appNS string
	fioOptions := vcluster.FIOOptions{
		Name:      "mytest",
		IOEngine:  "libaio",
		RW:        "randwrite",
		BS:        "4k",
		NumJobs:   1,
		Size:      "500m",
		TimeBased: true,
		Runtime:   "600s",
		EndFsync:  1,
	}
	JustBeforeEach(func() {
		StartTorpedoTest("CreateAndRunFioOnVclusterRWX", "Create, Connect and run 2 FIO Applications on Vcluster on RWX PVC", nil, 0)
		vc, err = vcluster.NewVCluster("my-vcluster1")
		log.FailOnError(err, "Failed to initialise VCluster")
		err = vc.CreateAndWaitVCluster()
		log.FailOnError(err, "Failed to create VCluster")
	})
	It("Create FIO app on VCluster and run it for 10 minutes", func() {
		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("fio-app-sc-%v", time.Now().Unix())
		err = CreateStorageClass(scName)
		log.FailOnError(err, "Error creating Storageclass")
		log.Infof("Successfully created StorageClass with name: %v", scName)
		// Create PVC on VCluster
		appNS = scName + "-ns"
		pvcName, err = vc.CreatePVC("", scName, appNS, "RWX")
		log.FailOnError(err, fmt.Sprintf("Error creating RWX PVC with Storageclass name %v", scName))
		log.Infof("Successfully created RWX PVC with name: %v", pvcName)
		jobName1 := "fio-job-1"
		jobName2 := "fio-job-2"

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			err = vc.CreateFIODeployment(pvcName, appNS, fioOptions, jobName1, 12*time.Minute)
			log.FailOnError(err, "Error in creating first FIO Application")
		}()
		go func() {
			defer wg.Done()
			err = vc.CreateFIODeployment(pvcName, appNS, fioOptions, jobName2, 12*time.Minute)
			log.FailOnError(err, "Error in creating second FIO Application")
		}()
		wg.Wait()
		log.Infof("Successfully ran 2 FIO jobs on Vcluster using a single RWX PVC")
	})
	JustAfterEach(func() {
		// VCluster, StorageClass and Namespace cleanup
		err := vc.VClusterCleanup(scName)
		if err != nil {
			log.Errorf("Problem in Cleanup: %v", err)
		} else {
			log.Infof("Cleanup successfully done.")
		}
	})
})

var _ = Describe("{CreateAndRunMultipleFioOnManyVclusters}", func() {
	envValueVcluster := vcluster.ReadEnvVariable("NUM_VCLUSTERS")
	envValueBatch := vcluster.ReadEnvVariable("VCLUSTER_PARALLEL_APPS")
	envValueIterations := vcluster.ReadEnvVariable("VCLUSTER_TOTAL_ITERATIONS")
	totalVclusters := 1
	batchCount := 2
	totalIterations := 1
	if envValueVcluster != "" {
		var err error
		totalVclusters, err = strconv.Atoi(envValueVcluster)
		if err != nil {
			log.Errorf("Failed to convert value %v to int with error: %v", envValueVcluster, err)
			totalVclusters = 1
		}
	}
	if envValueBatch != "" {
		var err error
		batchCount, err = strconv.Atoi(envValueBatch)
		if err != nil {
			log.Errorf("Failed to convert value %v to int with error: %v", envValueBatch, err)
			batchCount = 2
		}
	}
	if envValueIterations != "" {
		var err error
		totalIterations, err = strconv.Atoi(envValueIterations)
		if err != nil {
			log.Errorf("Failed to convert value %v to int with error: %v", envValueIterations, err)
			totalIterations = 1
		}
	}
	var vClusters []*vcluster.VCluster
	var scName string
	var appNS string
	fioOptions := vcluster.FIOOptions{
		Name:      "mytest",
		IOEngine:  "libaio",
		RW:        "randwrite",
		BS:        "4k",
		NumJobs:   1,
		Size:      "500m",
		TimeBased: true,
		Runtime:   "100s",
		EndFsync:  1,
	}
	JustBeforeEach(func() {
		StartTorpedoTest("CreateAndRunMultipleFioOnManyVclusters", "Create, Connect and run Multiple FIO Applications on Many Vclusters in Parallel", nil, 0)
		for i := 0; i < totalVclusters; i++ {
			vClusterName := fmt.Sprintf("my-vcluster%d", i+1)
			vc, err := vcluster.NewVCluster(vClusterName)
			log.FailOnError(err, "Failed to initialise VCluster")
			vClusters = append(vClusters, vc)
			err = vc.CreateAndWaitVCluster()
			log.FailOnError(err, fmt.Sprintf("Failed to create VCluster %s", vClusterName))
		}
	})
	It("Create Multiple FIO apps on VCluster and run it for 10 minutes", func() {
		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("fio-app-sc-%v", time.Now().Unix())
		err = CreateStorageClass(scName)
		log.FailOnError(err, "Error creating Storageclass")
		log.Infof("Successfully created StorageClass with name: %v", scName)
		appNS = scName + "-ns"

		var wgVClusters sync.WaitGroup
		wgVClusters.Add(totalVclusters)

		for _, vc := range vClusters {
			go func(vc *vcluster.VCluster) {
				defer wgVClusters.Done()
				for i := 0; i < totalIterations; i++ {
					var wg sync.WaitGroup
					var jobNames []string
					for j := 0; j < batchCount; j++ {
						wg.Add(1)
						go func(idx int) {
							defer wg.Done()
							pvcNameSuffix := fmt.Sprintf("-pvc-%d-%d-%d", i, j, idx)
							jobName := fmt.Sprintf("fio-job-%d-%d-%d", i, j, idx)
							jobNames = append(jobNames, jobName)
							pvcName, err := vc.CreatePVC(scName+pvcNameSuffix, scName, appNS, "")
							log.FailOnError(err, fmt.Sprintf("Error creating PVC %v with Storageclass name %v", pvcName, scName))
							log.Infof("Successfully created PVC with name: %v", pvcName)
							// Create FIO Deployment on VCluster using the above PVC
							err = vc.CreateFIODeployment(pvcName, appNS, fioOptions, jobName, 10*time.Minute)
							log.FailOnError(err, "Error in creating FIO Application for PVC "+pvcName)
						}(i + j)
					}
					wg.Wait()
					log.Infof("Successfully ran FIO on Vcluster for batch starting at %d", i)
					for _, jobName := range jobNames {
						err := vc.DeleteJobOnVcluster(appNS, jobName)
						log.FailOnError(err, fmt.Sprintf("Error deleting FIO Job: %v", jobName))
						log.Infof("Removed FIO Job : %v successfully from vcluster %v", jobName, vc.Name)
					}
					log.Infof("Successfully removed all FIO Jobs from Vcluster")
				}
			}(vc)
		}
		wgVClusters.Wait()
	})
	JustAfterEach(func() {
		// VCluster, StorageClass and Namespace cleanup
		for _, vc := range vClusters {
			vc.TerminateVCluster()
			vcluster.DeleteNSFromHost(vc.Namespace)
		}
		vcluster.DeleteStorageclassFromHost(scName)
	})
})

var _ = Describe("{VolumeDriverDownVCluster}", func() {
	vc := &vcluster.VCluster{}
	var scName string
	var pvcName string
	var appNS string
	JustBeforeEach(func() {
		StartTorpedoTest("VolumeDriverDownVCluster", "Creates Nginx Deployment on Vcluster, Brings Down Portworx on All nodes and then brings it up, Validates Nginx Deployment", nil, 0)
		vc, err = vcluster.NewVCluster("my-vcluster1")
		log.FailOnError(err, "Failed to initialise VCluster")
		err = vc.CreateAndWaitVCluster()
		log.FailOnError(err, "Failed to create VCluster")
	})
	It("Create Nginx Deployment on VCluster, bring down Px on all nodes and once it is up, validate Nginx again", func() {
		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("nginx-app-sc-%v", time.Now().Unix())
		err = CreateStorageClass(scName)
		log.FailOnError(err, "Error creating Storageclass")
		log.Infof("Successfully created StorageClass with name: %v", scName)
		// Create PVC on VCluster
		appNS = scName + "-ns"
		pvcName, err = vc.CreatePVC("", scName, appNS, "")
		log.FailOnError(err, fmt.Sprintf("Error creating PVC with Storageclass name %v", scName))
		log.Infof("Successfully created PVC with name: %v", pvcName)
		deploymentName := "nginx-deployment"
		// Create Nginx Deployment on VCluster using the above PVC
		err = vc.CreateNginxDeployment(pvcName, appNS, deploymentName)
		log.FailOnError(err, "Error in creating Nginx Application")
		log.Infof("Successfully created Nginx App on Vcluster")
		log.Infof("Hard Sleep for 10 seconds after creation of Nginx Deployment")
		time.Sleep(10 * time.Second)
		// Validate if Nginx Deployment is healthy or not
		err = vc.IsDeploymentHealthy(appNS, deploymentName, 1)
		log.FailOnError(err, "Looks like Nginx Deployment is not healthy")
		log.Infof("Nginx Deployment %s is healthy. Will kill Px and wait for its restart on all nodes now", deploymentName)
		Step("get nodes bounce volume driver", func() {
			for _, appNode := range node.GetStorageDriverNodes() {
				stepLog = fmt.Sprintf("stop volume driver %s on node: %s",
					Inst().V.String(), appNode.Name)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						StopVolDriverAndWait([]node.Node{appNode})
					})

				stepLog = fmt.Sprintf("starting volume %s driver on node %s",
					Inst().V.String(), appNode.Name)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						StartVolDriverAndWait([]node.Node{appNode})
					})

				stepLog = "Giving few seconds for volume driver to stabilize"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					time.Sleep(20 * time.Second)
				})
				// Validate if Nginx Deployment is healthy or not
				err = vc.IsDeploymentHealthy(appNS, deploymentName, 1)
				log.FailOnError(err, "Looks like Nginx Deployment is not healthy")
				log.Infof("Nginx Deployment %s is healthy. Will kill Px and wait for its restart on all nodes now", deploymentName)
			}
		})
	})
	JustAfterEach(func() {
		// VCluster, StorageClass and Namespace cleanup
		err := vc.VClusterCleanup(scName)
		if err != nil {
			log.Errorf("Problem in Cleanup: %v", err)
		} else {
			log.Infof("Cleanup successfully done.")
		}
	})
})

var _ = Describe("{VolumeDriverCrashVCluster}", func() {
	vc := &vcluster.VCluster{}
	var scName string
	var pvcName string
	var appNS string
	JustBeforeEach(func() {
		StartTorpedoTest("VolumeDriverCrashVCluster", "Creates Nginx Deployment on Vcluster, Crashes Portworx on All nodes and then brings it up, Validates Nginx Deployment", nil, 0)
		vc, err = vcluster.NewVCluster("my-vcluster1")
		log.FailOnError(err, "Failed to initialise VCluster")
		err = vc.CreateAndWaitVCluster()
		log.FailOnError(err, "Failed to create VCluster")
	})
	It("Create Nginx Deployment on VCluster, crashes Px on all nodes and once it is up, validate Nginx again", func() {
		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("nginx-app-sc-%v", time.Now().Unix())
		err = CreateStorageClass(scName)
		log.FailOnError(err, "Error creating Storageclass")
		log.Infof("Successfully created StorageClass with name: %v", scName)
		// Create PVC on VCluster
		appNS = scName + "-ns"
		pvcName, err = vc.CreatePVC("", scName, appNS, "")
		log.FailOnError(err, fmt.Sprintf("Error creating PVC with Storageclass name %v", scName))
		log.Infof("Successfully created PVC with name: %v", pvcName)
		deploymentName := "nginx-deployment"
		// Create Nginx Deployment on VCluster using the above PVC
		err = vc.CreateNginxDeployment(pvcName, appNS, deploymentName)
		log.FailOnError(err, "Error in creating Nginx Application")
		log.Infof("Successfully created Nginx App on Vcluster")
		log.Infof("Hard Sleep for 10 seconds after creation of Nginx Deployment")
		time.Sleep(10 * time.Second)
		// Validate if Nginx Deployment is healthy or not
		err = vc.IsDeploymentHealthy(appNS, deploymentName, 1)
		log.FailOnError(err, "Looks like Nginx Deployment is not healthy")
		log.Infof("Nginx Deployment %s is healthy. Will kill Px and wait for its restart on all nodes now", deploymentName)
		stepLog = "crash volume driver in all nodes"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appNode := range node.GetStorageDriverNodes() {
				stepLog = fmt.Sprintf("crash volume driver %s on node: %v",
					Inst().V.String(), appNode.Name)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						CrashVolDriverAndWait([]node.Node{appNode})
					})
			}
		})
		// Validate if Nginx Deployment is healthy or not
		err = vc.IsDeploymentHealthy(appNS, deploymentName, 1)
		log.FailOnError(err, "Looks like Nginx Deployment is not healthy")
		log.Infof("Nginx Deployment %s is healthy. Will kill Px and wait for its restart on all nodes now", deploymentName)
	})
	JustAfterEach(func() {
		// VCluster, StorageClass and Namespace cleanup
		err := vc.VClusterCleanup(scName)
		if err != nil {
			log.FailOnError(err, "Cleanup Failed")
		} else {
			log.Infof("Cleanup successfully done.")
		}
	})
})

// This test case is failing due to : https://portworx.atlassian.net/browse/PWX-34762
var _ = Describe("{VolumeDriverAppDownVCluster}", func() {
	vc := &vcluster.VCluster{}
	var scName string
	var pvcName string
	var appNS string
	JustBeforeEach(func() {
		StartTorpedoTest("VolumeDriverAppDownVCluster", "Creates Nginx Deployment on Vcluster, Brings Down Portworx on node running Nginx and then deletes Nginx deployment. Brings up Px again", nil, 0)
		vc, err = vcluster.NewVCluster("my-vcluster1")
		log.FailOnError(err, "Failed to initialise VCluster")
		err = vc.CreateAndWaitVCluster()
		log.FailOnError(err, "Failed to create VCluster")
	})
	It("Creates Nginx Deployment on Vcluster, Brings Down Portworx on node running Nginx and then deletes Nginx deployment. Brings up Px again", func() {
		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("nginx-app-sc-%v", time.Now().Unix())
		err = CreateStorageClass(scName)
		log.FailOnError(err, "Error creating Storageclass")
		log.Infof("Successfully created StorageClass with name: %v", scName)
		// Create PVC on VCluster
		appNS = scName + "-ns"
		pvcName, err = vc.CreatePVC("", scName, appNS, "")
		log.FailOnError(err, fmt.Sprintf("Error creating PVC with Storageclass name %v", scName))
		log.Infof("Successfully created PVC with name: %v", pvcName)
		deploymentName := "nginx-deployment"
		// Create Nginx Deployment on VCluster using the above PVC
		err = vc.CreateNginxDeployment(pvcName, appNS, deploymentName)
		log.FailOnError(err, "Error in creating Nginx Application")
		log.Infof("Successfully created Nginx App on Vcluster")
		log.Infof("Hard Sleep for 10 seconds after creation of Nginx Deployment")
		time.Sleep(10 * time.Second)
		// Validate if Nginx Deployment is healthy or not
		err = vc.IsDeploymentHealthy(appNS, deploymentName, 1)
		log.FailOnError(err, "Looks like Nginx Deployment is not healthy")
		log.Infof("Nginx Deployment %s is healthy. Will kill Px and delete Nginx deployment now", deploymentName)
		podNodes, err := vc.GetDeploymentPodNodes(appNS, deploymentName)
		log.FailOnError(err, "Failed to get nodes of pods of deployment")
		var nodesToReboot []string
		for _, appNode := range node.GetWorkerNodes() {
			for _, podNode := range podNodes {
				if appNode.Name == podNode {
					nodesToReboot = append(nodesToReboot, appNode.Name)
				}
			}
		}
		Step("get nodes bounce volume driver", func() {
			for _, appNode := range node.GetWorkerNodes() {
				for _, nodes := range nodesToReboot {
					if appNode.Name == nodes {
						stepLog = fmt.Sprintf("stop volume driver %s on node: %s",
							Inst().V.String(), appNode.Name)
						Step(stepLog,
							func() {
								log.InfoD(stepLog)
								StopVolDriverAndWait([]node.Node{appNode})
							})
					}
				}
			}
			// Validates if VCluster is still accessible or not by listing namespaces within it
			err = vc.WaitForVClusterAccess()
			log.FailOnError(err, "Even after waiting for 5 minutes, Could not access objects within vcluster after Px shutdown")
			log.Infof("Vcluster has become responsive - going ahead with the test")
			// Deleting Deployment from Vcluster now once it becomes accessible
			err = vc.DeleteDeploymentOnVCluster(appNS, deploymentName)
			log.FailOnError(err, "Failed to delete Nginx Deployment name %v on Vcluster %v", deploymentName, vc.Name)
			log.Infof("Successfully deleted Nginx deployment %v on Vcluster %v", deploymentName, vc.Name)

			for _, appNode := range node.GetWorkerNodes() {
				for _, nodes := range nodesToReboot {
					if appNode.Name == nodes {
						stepLog = fmt.Sprintf("start volume driver %s on node: %s",
							Inst().V.String(), appNode.Name)
						Step(stepLog,
							func() {
								log.InfoD(stepLog)
								StartVolDriverAndWait([]node.Node{appNode})
							})
					}
				}
			}
			stepLog = "Giving few seconds for volume driver to stabilize"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				time.Sleep(20 * time.Second)
			})
		})
	})
	JustAfterEach(func() {
		// VCluster, StorageClass and Namespace cleanup
		err := vc.VClusterCleanup(scName)
		if err != nil {
			log.Errorf("Problem in Cleanup: %v", err)
		} else {
			log.Infof("Cleanup successfully done.")
		}
	})
})

// This test case is failing due to : https://portworx.atlassian.net/browse/PWX-34762
var _ = Describe("{VolumeDriverDownVClusterOps}", func() {
	vc := &vcluster.VCluster{}
	var scName string
	var pvcName string
	var appNS string
	JustBeforeEach(func() {
		StartTorpedoTest("VolumeDriverAppDownVCluster", "Brings Down Portworx on one node, Creates Nginx Deployment on Vcluster, Validate Nginx deployment, Brings up Px again", nil, 0)
		vc, err = vcluster.NewVCluster("my-vcluster1")
		log.FailOnError(err, "Failed to initialise VCluster")
		err = vc.CreateAndWaitVCluster()
		log.FailOnError(err, "Failed to create VCluster")
	})
	It("Brings Down Portworx on one node, Creates Nginx Deployment on Vcluster, Validate Nginx deployment, Brings up Px again", func() {
		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("nginx-app-sc-%v", time.Now().Unix())
		err = CreateStorageClass(scName)
		log.FailOnError(err, "Error creating Storageclass")
		log.Infof("Successfully created StorageClass with name: %v", scName)
		nodes := node.GetWorkerNodes()
		Step("bounce volume driver on one node", func() {
			stepLog = fmt.Sprintf("stop volume driver %s on node: %s",
				Inst().V.String(), nodes[0].Name)
			Step(stepLog,
				func() {
					log.InfoD(stepLog)
					StopVolDriverAndWait([]node.Node{nodes[0]})
				})
		})
		// Validates if VCluster is still accessible or not by listing namespaces within it
		err = vc.WaitForVClusterAccess()
		log.FailOnError(err, "Even after waiting for 5 minutes, Could not access objects within vcluster after Px shutdown")
		log.Infof("Vcluster has become responsive - going ahead with the test")
		// Create PVC on VCluster
		appNS = scName + "-ns"
		pvcName, err = vc.CreatePVC("", scName, appNS, "")
		log.FailOnError(err, fmt.Sprintf("Error creating PVC with Storageclass name %v", scName))
		log.Infof("Successfully created PVC with name: %v", pvcName)
		deploymentName := "nginx-deployment"
		// Create Nginx Deployment on VCluster using the above PVC
		err = vc.CreateNginxDeployment(pvcName, appNS, deploymentName)
		log.FailOnError(err, "Error in creating Nginx Application")
		log.Infof("Successfully created Nginx App on Vcluster")
		log.Infof("Hard Sleep for 10 seconds after creation of Nginx Deployment")
		time.Sleep(10 * time.Second)
		// Validate if Nginx Deployment is healthy or not
		err = vc.IsDeploymentHealthy(appNS, deploymentName, 1)
		log.FailOnError(err, "Looks like Nginx Deployment is not healthy")
		log.Infof("Nginx Deployment %s is healthy. Will kill Px and delete Nginx deployment now", deploymentName)
		StartVolDriverAndWait([]node.Node{nodes[0]})
		stepLog = "Giving few seconds for volume driver to stabilize"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			time.Sleep(20 * time.Second)
		})
	})
	JustAfterEach(func() {
		// VCluster, StorageClass and Namespace cleanup
		err := vc.VClusterCleanup(scName)
		if err != nil {
			log.Errorf("Problem in Cleanup: %v", err)
		} else {
			log.Infof("Cleanup successfully done.")
		}
	})
})

var _ = Describe("{CreateEncryptedVolVCluster}", func() {
	vc := &vcluster.VCluster{}
	var scName string
	var pvcName string
	var appNS string
	var secretName string
	fioOptions := vcluster.FIOOptions{
		Name:      "mytest",
		IOEngine:  "libaio",
		RW:        "randwrite",
		BS:        "4k",
		NumJobs:   1,
		Size:      "500m",
		TimeBased: true,
		Runtime:   "100s",
		EndFsync:  1,
	}
	JustBeforeEach(func() {
		StartTorpedoTest("CreateEncryptedVolVCluster", "Create app on encrypted vol in vcluster, validate app, cleanup", nil, 0)
		vc, err = vcluster.NewVCluster("my-vcluster1")
		log.FailOnError(err, "Failed to initialise VCluster")
		err = vc.CreateAndWaitVCluster()
		log.FailOnError(err, "Failed to create VCluster")
	})
	It("Create app on encrypted vol in vcluster, validate app, cleanup", func() {
		secretName = fmt.Sprintf("px-vol-encryption")
		err := vcluster.CreateClusterWideSecret(secretName)
		log.FailOnError(err, "Failed to create a Cluster Wide Secret")
		log.Infof("Cluster wide secret successfully created")
		nodes := node.GetWorkerNodes()
		cmd := fmt.Sprintf("yes | /opt/pwx/bin/pxctl secrets set-cluster-key --secret %v --overwrite", vcluster.ClusterWideSecretKey)
		_, err = runCmd(cmd, nodes[0])
		log.FailOnError(err, "Failed to set cluster key via Pxctl")
		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("fio-app-sc-%v", time.Now().Unix())
		err = CreateStorageClass(scName, WithSecureParameter(true))
		log.FailOnError(err, "Error creating Storageclass")
		log.Infof("Successfully created Secure StorageClass with name: %v", scName)
		// Create PVC on VCluster
		appNS = scName + "-ns"
		pvcName, err = vc.CreatePVC("", scName, appNS, "")
		log.FailOnError(err, fmt.Sprintf("Error creating PVC with Storageclass name %v", scName))
		log.Infof("Successfully created PVC with name: %v", pvcName)
		jobName := "fio-job"
		// Create FIO Deployment on VCluster using the above PVC
		err = vc.CreateFIODeployment(pvcName, appNS, fioOptions, jobName, 10*time.Minute)
		log.FailOnError(err, "Error in creating FIO Application")
		log.Infof("Successfully ran FIO on Vcluster")
	})
	JustAfterEach(func() {
		// VCluster, StorageClass, Namespace and Cluster Wide cleanup
		err := vc.VClusterCleanup(scName)
		if err != nil {
			log.Errorf("Problem in Cleanup: %v", err)
		}
		err = vcluster.DeleteSecret(secretName, vcluster.PxNamespace)
		if err != nil {
			log.Errorf("Problem in Cleaning up secret: %v", err)
		} else {
			log.Infof("Entire Cleanup successfully done.")
		}
	})
})

var _ = Describe("{NodeRebootVCluster}", func() {
	vc := &vcluster.VCluster{}
	var scName string
	var pvcName string
	var appNS string
	JustBeforeEach(func() {
		StartTorpedoTest("NodeRebootVCluster", "Creates Nginx Deployment on Vcluster, Reboots a Px Cluster node, Validates Nginx Deployment", nil, 0)
		vc, err = vcluster.NewVCluster("my-vcluster1")
		log.FailOnError(err, "Failed to initialise VCluster")
		err = vc.CreateAndWaitVCluster()
		log.FailOnError(err, "Failed to create VCluster")
	})
	It("Creates Nginx Deployment on Vcluster, Reboots a Px Cluster node, Validates Nginx Deployment", func() {
		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("nginx-app-sc-%v", time.Now().Unix())
		err = CreateStorageClass(scName)
		log.FailOnError(err, "Error creating Storageclass")
		log.Infof("Successfully created StorageClass with name: %v", scName)
		// Create PVC on VCluster
		appNS = scName + "-ns"
		pvcName, err = vc.CreatePVC("", scName, appNS, "")
		log.FailOnError(err, fmt.Sprintf("Error creating PVC with Storageclass name %v", scName))
		log.Infof("Successfully created PVC with name: %v", pvcName)
		deploymentName := "nginx-deployment"
		// Create Nginx Deployment on VCluster using the above PVC
		err = vc.CreateNginxDeployment(pvcName, appNS, deploymentName)
		log.FailOnError(err, "Error in creating Nginx Application")
		log.Infof("Successfully created Nginx App on Vcluster")
		log.Infof("Hard Sleep for 10 seconds after creation of Nginx Deployment")
		time.Sleep(10 * time.Second)
		// Validate if Nginx Deployment is healthy or not
		err = vc.IsDeploymentHealthy(appNS, deploymentName, 1)
		log.FailOnError(err, "Looks like Nginx Deployment is not healthy")
		log.Infof("Nginx Deployment %s is healthy. Will kill Px and wait for its restart on all nodes now", deploymentName)
		stepLog = "Reboot All Nodes one by one in rolling fashion"
		Step(stepLog, func() {
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
		// Validate if Nginx Deployment is healthy or not
		err = vc.IsDeploymentHealthy(appNS, deploymentName, 1)
		log.FailOnError(err, "Looks like Nginx Deployment is not healthy")
		log.Infof("Nginx Deployment %s is healthy. Will kill Px and wait for its restart on all nodes now", deploymentName)
	})
	JustAfterEach(func() {
		// VCluster, StorageClass and Namespace cleanup
		err := vc.VClusterCleanup(scName)
		if err != nil {
			log.Errorf("Problem in Cleanup: %v", err)
		} else {
			log.Infof("Cleanup successfully done.")
		}
	})
})

// This Test Case is failing due to : https://portworx.atlassian.net/browse/PWX-34792
var _ = Describe("{VolumeSnapshotAndRestoreVcluster}", func() {
	vc := &vcluster.VCluster{}
	var scName string
	var pvcName string
	var appNS string
	do_verify := 1
	verify := "crc32c"
	fioOptions := vcluster.FIOOptions{
		Name:      "mytest",
		IOEngine:  "libaio",
		RW:        "write",
		BS:        "4k",
		NumJobs:   1,
		Size:      "500m",
		TimeBased: true,
		Runtime:   "100s",
		EndFsync:  1,
		DoVerify:  &do_verify,
		Verify:    &verify,
	}
	JustBeforeEach(func() {
		StartTorpedoTest("VolumeSnapshotAndRestoreVcluster", "Create, Connect and run FIO Application on Vcluster, Create a Volume Snapshot, Restore it and Read from FIO the data previously written", nil, 0)
		vc, err = vcluster.NewVCluster("my-vcluster1")
		log.FailOnError(err, "Failed to initialise VCluster")
		err = vc.CreateAndWaitVCluster()
		log.FailOnError(err, "Failed to create VCluster")
	})
	It("Create, Connect and run FIO Application on Vcluster, Create a Volume Snapshot, Restore it and Read from FIO the data previously written", func() {
		// Create Snapshot Schedule Policy
		snapSchedulePolicy := fmt.Sprintf("snap-schedule-%v", time.Now().Unix())
		err := snapshotutils.SchedulePolicyInDefaultNamespace(snapSchedulePolicy, 1, 5)
		log.FailOnError(err, "Failed to create Snapshot Schedule Policy")
		// Create Storage Class with snapshot schedule policy
		scName = fmt.Sprintf("fio-app-sc-%v", time.Now().Unix())
		err = CreateStorageClass(scName, WithSnapshotSchedule(snapSchedulePolicy, "local"))
		log.FailOnError(err, "Error creating Storageclass")
		log.Infof("Successfully created StorageClass with name: %v", scName)
		// Create PVC on VCluster
		appNS = scName + "-ns"
		pvcName, err = vc.CreatePVC("", scName, appNS, "")
		log.FailOnError(err, fmt.Sprintf("Error creating PVC with Storageclass name %v", scName))
		log.Infof("Successfully created PVC with name: %v", pvcName)
		jobName := "fio-job"
		// Create FIO Deployment on VCluster using the above PVC
		err = vc.CreateFIODeployment(pvcName, appNS, fioOptions, jobName, 10*time.Minute)
		log.FailOnError(err, "Error in creating FIO Application")
		log.Infof("Successfully ran FIO on Vcluster")
		log.Infof("Waiting for 60 seconds as that is frequency to take one snapshot")
		time.Sleep(1 * time.Minute)
		snapList, err := vc.ListSnapshots()
		log.FailOnError(err, "Failed to list snapshots")
		// Finding the most recent snapshot, but not older than 1 minute
		var chosenSnapshot *snapv1.VolumeSnapshot
		var minAge time.Duration = 1 * time.Minute
		for i, snap := range snapList.Items {
			if snap.Metadata.CreationTimestamp.Time.After(time.Now().Add(-minAge)) {
				// Choosing the most recent snapshot
				if chosenSnapshot == nil || snap.Metadata.CreationTimestamp.Time.After(chosenSnapshot.Metadata.CreationTimestamp.Time) {
					chosenSnapshot = &snapList.Items[i]
				}
			}
		}
		if chosenSnapshot == nil {
			err = fmt.Errorf("No recent snapshot found for PVC: %s within the past %v", pvcName, minAge)
			log.FailOnError(err, "Exiting as no recent snapshot found")
		} else {
			log.Infof("Selected snapshot: %v with creation time: %v", chosenSnapshot.Metadata.Name, chosenSnapshot.Metadata.CreationTimestamp)
		}
		// Creqte a Restored PVC from this Snapshot
		restoredPvcName := "restored-" + pvcName
		err = vc.RestorePVCFromSnapshot(restoredPvcName, chosenSnapshot.Metadata.Name, appNS, scName, "")
		log.FailOnError(err, "Failed to restore a PVC from the snapshot")
		// Create Read only FIO Options and create FIO Job from those
		fioOptions.RW = "read"
		fioOptions.VerifyOnly = true
		jobName = "fio-restored-job"
		err = vc.CreateFIODeployment(restoredPvcName, appNS, fioOptions, jobName, 10*time.Minute)
		log.FailOnError(err, "Error in creating FIO Application")
		log.Infof("Successfully ran FIO on Vcluster")
	})
	JustAfterEach(func() {
		// VCluster, StorageClass and Namespace cleanup
		err := vc.VClusterCleanup(scName)
		if err != nil {
			log.Errorf("Problem in Cleanup: %v", err)
		} else {
			log.Infof("Cleanup successfully done.")
		}
	})
})

var _ = Describe("{AutopilotPvcResizeTestVCluster}", func() {
	vc := &vcluster.VCluster{}
	var scName string
	var pvcName string
	var appNS string
	var apRule apapi.AutopilotRule
	fioOptions := vcluster.FIOOptions{
		Name:      "mytest",
		IOEngine:  "libaio",
		RW:        "randwrite",
		BS:        "4k",
		NumJobs:   1,
		Size:      "1000m",
		TimeBased: true,
		Runtime:   "300s",
		EndFsync:  1,
	}
	JustBeforeEach(func() {
		StartTorpedoTest("AutopilotPvcResizeTestVCluster", "Create a PVC Resize Autopilot Rule, Create Connect and run FIO Application on Vcluster and validate Autopilot rule triggers successfully", nil, 0)
		vc, err = vcluster.NewVCluster("my-vcluster1")
		log.FailOnError(err, "Failed to initialise VCluster")
		err = vc.CreateAndWaitVCluster()
		log.FailOnError(err, "Failed to create VCluster")
	})
	It("Create a PVC Expand Autopilot Rule, Create Connect and run FIO Application on Vcluster and validate Autopilot rule triggers successfully", func() {
		apRule = vcluster.PVCRuleByUsageCapacityForVcluster(5, 50, "20Gi")
		_, err := Inst().S.CreateAutopilotRule(apRule)
		log.FailOnError(err, "Failed to set Autopilot rule")
		log.Infof("Autopilot rule %v successfully set", apRule.Name)
		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("fio-app-sc-%v", time.Now().Unix())
		err = CreateStorageClass(scName, WithVolumeExpansion(true))
		log.FailOnError(err, "Error creating Storageclass")
		log.Infof("Successfully created StorageClass with name: %v", scName)
		// Create PVC on VCluster
		appNS = scName + "-ns"
		pvcName, err = vc.CreatePVC("", scName, appNS, "")
		log.FailOnError(err, fmt.Sprintf("Error creating PVC with Storageclass name %v", scName))
		log.Infof("Successfully created PVC with name: %v", pvcName)
		jobName := "fio-job"
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			err = vc.CreateFIODeployment(pvcName, appNS, fioOptions, jobName, 10*time.Minute)
			log.FailOnError(err, "Error in creating first FIO Application")
		}()
		go func() {
			defer wg.Done()
			err := aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.AnyToTriggeredEvent})
			log.FailOnError(err, "Failed to trigger Autopilot event of PVC Expansion")
			log.Infof("Autopilot event to trigger PVC Expansion successfully triggered")
			err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActiveActionsPendingToActiveActionsInProgress})
			log.FailOnError(err, "Failed to bring autopilot event to In Progress State")
			log.Infof("Autopilot event of PVC Expansion is in progress")
			err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActiveActionsInProgressToActiveActionsTaken})
			log.FailOnError(err, "Failed to bring In Progress Autopilot action to Completed state")
			log.Infof("Successfully resized PVC as per set Autopilot rule")
			pvc, err := vc.GetPVC(pvcName, appNS)
			log.FailOnError(err, "Failed to get PVC Object of PVC in Vcluster context")
			originalSize := vcluster.GetPvcOriginalSize(pvc)
			expandedSize := vcluster.GetPvcCapacitySize(pvc)
			if expandedSize > originalSize {
				log.Infof("New Size of PVC %v is %v GiB, whereas earlier size was %v GiB", pvcName, expandedSize, originalSize)
				log.Infof("Successfully expanded PVC using Autopilot on vCluster")
			} else {
				err = fmt.Errorf("Something went wrong as expanded size of PVC %v is coming out to be %v and original size is %v", pvcName, expandedSize, originalSize)
				log.FailOnError(err, "Autopilot failed to resize PVC")
			}

		}()
		wg.Wait()

	})
	JustAfterEach(func() {
		// VCluster, StorageClass and Namespace cleanup
		err := vc.VClusterCleanup(scName)
		if err != nil {
			log.Errorf("Problem in Cleanup: %v", err)
		} else {
			log.Infof("Cleanup successfully done.")
		}
		err = Inst().S.DeleteAutopilotRule(apRule.ObjectMeta.Name)
		log.FailOnError(err, fmt.Sprintf("Failed during deletion of Autopilot Rule %s", apRule.ObjectMeta.Name))
	})
})

var _ = Describe("{AutopilotMultiplePvcResizeTestVCluster}", func() {
	vc := &vcluster.VCluster{}
	var scName string
	var appNS string
	var apRule apapi.AutopilotRule
	fioOptions := vcluster.FIOOptions{
		Name:      "mytest",
		IOEngine:  "libaio",
		RW:        "randwrite",
		BS:        "4k",
		NumJobs:   1,
		Size:      "1000m",
		TimeBased: true,
		Runtime:   "300s",
		EndFsync:  1,
	}
	JustBeforeEach(func() {
		StartTorpedoTest("AutopilotMultiplePvcResizeTestVCluster", "Create a PVC Resize Autopilot Rule, Create Connect and run FIO Application on Vcluster that uses multiple PVCs and validate Autopilot rule triggers successfully", nil, 0)
		vc, err = vcluster.NewVCluster("my-vcluster1")
		log.FailOnError(err, "Failed to initialise VCluster")
		err = vc.CreateAndWaitVCluster()
		log.FailOnError(err, "Failed to create VCluster")
	})
	It("Create a PVC Expand Autopilot Rule, Create Connect and run FIO Application on Vcluster and validate Autopilot rule triggers successfully", func() {
		apRule = vcluster.PVCRuleByUsageCapacityForVcluster(5, 50, "20Gi")
		_, err := Inst().S.CreateAutopilotRule(apRule)
		log.FailOnError(err, "Failed to set Autopilot rule")
		log.Infof("Autopilot rule %v successfully set", apRule.Name)
		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("fio-app-sc-%v", time.Now().Unix())
		err = CreateStorageClass(scName, WithVolumeExpansion(true))
		log.FailOnError(err, "Error creating Storageclass")
		log.Infof("Successfully created StorageClass with name: %v", scName)
		// Create PVC on VCluster
		appNS = scName + "-ns"
		pvcNames := make([]string, 2)
		for i := 0; i < len(pvcNames); i++ {
			pvcName := fmt.Sprintf(vc.Name+"-"+scName+"-pvc-%d", i)
			pvcName, err = vc.CreatePVC(pvcName, scName, appNS, "")
			log.FailOnError(err, fmt.Sprintf("Error creating PVC with Storageclass name %v", scName))
			log.Infof("Successfully created PVC with name: %v", pvcName)
			pvcNames[i] = pvcName
		}
		jobName := "fio-job"
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			err = vc.CreateFIOMultiPvcDeployment(pvcNames, appNS, fioOptions, jobName, 10*time.Minute)
			log.FailOnError(err, "Error in creating first FIO Application")
		}()
		go func() {
			defer wg.Done()
			err := aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.AnyToTriggeredEvent})
			log.FailOnError(err, "Failed to trigger Autopilot event of PVC Expansion")
			log.Infof("Autopilot event to trigger PVC Expansion successfully triggered")
			err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActiveActionsPendingToActiveActionsInProgress})
			log.FailOnError(err, "Failed to bring autopilot event to In Progress State")
			log.Infof("Autopilot event of PVC Expansion is in progress")
			err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActiveActionsInProgressToActiveActionsTaken})
			log.FailOnError(err, "Failed to bring In Progress Autopilot action to Completed state")
			log.Infof("Successfully resized PVC as per set Autopilot rule")
			for i := 0; i < len(pvcNames); i++ {
				pvc, err := vc.GetPVC(pvcNames[i], appNS)
				log.FailOnError(err, "Failed to get PVC Object of PVC in Vcluster context")
				originalSize := vcluster.GetPvcOriginalSize(pvc)
				expandedSize := vcluster.GetPvcCapacitySize(pvc)
				if expandedSize > originalSize {
					log.Infof("New Size of PVC %v is %v GiB, whereas earlier size was %v GiB", pvcNames[i], expandedSize, originalSize)
					log.Infof("Successfully expanded PVC using Autopilot on vCluster")
				} else {
					err = fmt.Errorf("Something went wrong as expanded size of PVC %v is coming out to be %v and original size is %v", pvcNames[i], expandedSize, originalSize)
					log.FailOnError(err, "Autopilot failed to resize PVC")
				}
			}
		}()
		wg.Wait()

	})
	JustAfterEach(func() {
		// VCluster, StorageClass and Namespace cleanup
		err := vc.VClusterCleanup(scName)
		if err != nil {
			log.Errorf("Problem in Cleanup: %v", err)
		} else {
			log.Infof("Cleanup successfully done.")
		}
		err = Inst().S.DeleteAutopilotRule(apRule.ObjectMeta.Name)
		log.FailOnError(err, fmt.Sprintf("Failed during deletion of Autopilot Rule %s", apRule.ObjectMeta.Name))
	})
})

var _ = Describe("{AutopilotMultipleFioOnManyVclusters}", func() {
	envValueVcluster := vcluster.ReadEnvVariable("NUM_VCLUSTERS")
	envValueBatch := vcluster.ReadEnvVariable("VCLUSTER_PARALLEL_APPS")
	envValueIterations := vcluster.ReadEnvVariable("VCLUSTER_TOTAL_ITERATIONS")
	totalVclusters := 1
	batchCount := 2
	totalIterations := 1
	if envValueVcluster != "" {
		var err error
		totalVclusters, err = strconv.Atoi(envValueVcluster)
		if err != nil {
			log.Errorf("Failed to convert value %v to int with error: %v", envValueVcluster, err)
			totalVclusters = 1
		}
	}
	if envValueBatch != "" {
		var err error
		batchCount, err = strconv.Atoi(envValueBatch)
		if err != nil {
			log.Errorf("Failed to convert value %v to int with error: %v", envValueBatch, err)
			batchCount = 2
		}
	}
	if envValueIterations != "" {
		var err error
		totalIterations, err = strconv.Atoi(envValueIterations)
		if err != nil {
			log.Errorf("Failed to convert value %v to int with error: %v", envValueIterations, err)
			totalIterations = 1
		}
	}
	var vClusters []*vcluster.VCluster
	var scName string
	var appNS string
	var apRules []apapi.AutopilotRule
	fioOptions := vcluster.FIOOptions{
		Name:      "mytest",
		IOEngine:  "libaio",
		RW:        "randwrite",
		BS:        "4k",
		NumJobs:   1,
		Size:      "600m",
		TimeBased: true,
		Runtime:   "300s",
		EndFsync:  1,
	}
	JustBeforeEach(func() {
		StartTorpedoTest("AutopilotMultipleFioOnManyVclusters", "Create, Connect and run Multiple FIO Applications on Many Vclusters in Parallel", nil, 0)
		for i := 0; i < totalVclusters; i++ {
			vClusterName := fmt.Sprintf("my-vcluster%d", i+1)
			vc, err := vcluster.NewVCluster(vClusterName)
			log.FailOnError(err, "Failed to initialise VCluster")
			vClusters = append(vClusters, vc)
			err = vc.CreateAndWaitVCluster()
			log.FailOnError(err, fmt.Sprintf("Failed to create VCluster %s", vClusterName))
		}
	})
	It("Create Multiple FIO apps on VCluster and run it for 10 minutes", func() {
		// Create Storage Class on Host Cluster
		scName = fmt.Sprintf("fio-app-sc-%v", time.Now().Unix())
		err = CreateStorageClass(scName, WithVolumeExpansion(true))
		log.FailOnError(err, "Error creating Storageclass")
		log.Infof("Successfully created StorageClass with name: %v", scName)
		appNS = scName + "-ns"

		var wgVClusters sync.WaitGroup
		wgVClusters.Add(totalVclusters)

		for _, vc := range vClusters {
			go func(vc *vcluster.VCluster) {
				defer wgVClusters.Done()
				for i := 0; i < totalIterations; i++ {
					var wg sync.WaitGroup
					var jobNames []string
					for j := 0; j < batchCount; j++ {
						wg.Add(1)
						go func(idx int) {
							defer wg.Done()
							var wginternal sync.WaitGroup
							wginternal.Add(2)
							var apRule apapi.AutopilotRule
							apRule = vcluster.PVCRuleByUsageCapacityForVcluster(5, 50, "20Gi")
							apRules = append(apRules, apRule)
							_, err := Inst().S.CreateAutopilotRule(apRule)
							log.FailOnError(err, "Failed to set Autopilot rule")
							log.Infof("Autopilot rule %v successfully set", apRule.Name)
							pvcNameSuffix := fmt.Sprintf("-pvc-%d-%d-%d", i, j, idx)
							jobName := fmt.Sprintf("fio-job-%d-%d-%d", i, j, idx)
							jobNames = append(jobNames, jobName)
							pvcName, err := vc.CreatePVC(scName+pvcNameSuffix, scName, appNS, "")
							log.FailOnError(err, fmt.Sprintf("Error creating PVC %v with Storageclass name %v", pvcName, scName))
							log.Infof("Successfully created PVC with name: %v", pvcName)
							go func() {
								defer wginternal.Done()
								// Create FIO Deployment on VCluster using the above PVC
								err = vc.CreateFIODeployment(pvcName, appNS, fioOptions, jobName, 10*time.Minute)
								log.FailOnError(err, "Error in creating FIO Application for PVC "+pvcName)
							}()
							go func() {
								defer wginternal.Done()
								err := aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.AnyToTriggeredEvent})
								log.FailOnError(err, "Failed to trigger Autopilot event of PVC Expansion")
								log.Infof("Autopilot event to trigger PVC Expansion successfully triggered")
								err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActiveActionTakenToNormalEvent})
								log.FailOnError(err, "Failed to bring autopilot event to Final State")
								log.Infof("Autopilot event of PVC Expansion is successful")
								pvc, err := vc.GetPVC(pvcName, appNS)
								log.FailOnError(err, "Failed to get PVC Object of PVC in Vcluster context")
								originalSize := vcluster.GetPvcOriginalSize(pvc)
								expandedSize := vcluster.GetPvcCapacitySize(pvc)
								if expandedSize > originalSize {
									log.Infof("New Size of PVC %v is %v GiB, whereas earlier size was %v GiB", pvcName, expandedSize, originalSize)
									log.Infof("Successfully expanded PVC using Autopilot on vCluster")
								} else {
									err = fmt.Errorf("Something went wrong as expanded size of PVC %v is coming out to be %v and original size is %v", pvcName, expandedSize, originalSize)
									log.FailOnError(err, "Autopilot failed to resize PVC")
								}

							}()
							wginternal.Wait()
						}(i + j)
					}
					wg.Wait()
					log.Infof("Successfully ran FIO on Vcluster for batch starting at %d", i)
					for _, jobName := range jobNames {
						err := vc.DeleteJobOnVcluster(appNS, jobName)
						log.FailOnError(err, fmt.Sprintf("Error deleting FIO Job: %v", jobName))
						log.Infof("Removed FIO Job : %v successfully from vcluster %v", jobName, vc.Name)
					}
					log.Infof("Successfully removed all FIO Jobs from Vcluster")
				}
			}(vc)
		}
		wgVClusters.Wait()
	})
	JustAfterEach(func() {
		// VCluster, StorageClass and Namespace cleanup
		for _, vc := range vClusters {
			vc.TerminateVCluster()
			vcluster.DeleteNSFromHost(vc.Namespace)
		}
		vcluster.DeleteStorageclassFromHost(scName)
		for _, apRule := range apRules {
			Inst().S.DeleteAutopilotRule(apRule.ObjectMeta.Name)
			log.Errorf("Failed to delete Rule %v", apRule.ObjectMeta.Name)
		}
	})
})

var _ = Describe("{DeployMultipleKubevirtApps}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("DeployMultipleKubevirtApps", "Create, Connect and run Multiple Kubevirt VMs", nil, 0)
	})
	It("Create Multiple FIO apps on VCluster and run it for 10 minutes", func() {
		Inst().AppList = []string{"kubevirt-cloudsnap", "kubevirt-localsnap", "kubevirt-multi-disk", "kubevirt-ssie-vm", "kubevirt-ssie-io", "kubevirt-vm-pvc"}

		for i := 1; i <= 20; i++ {
			taskName := fmt.Sprintf("ssie-load-%d", i)
			appNamespace := fmt.Sprintf("%v-%v", "multi-kubevirt-app", time.Now().Unix())
			_ = ScheduleApplicationsOnNamespace(appNamespace, taskName)
		}
	})
})

// CreateStorageClass method creates a storageclass using host's k8s clientset on host cluster
func CreateStorageClass(scName string, opts ...StorageClassOption) error {
	params := make(map[string]string)
	params["repl"] = "2"
	params["priority_io"] = "high"
	params["io_profile"] = "auto"
	v1obj := metav1.ObjectMeta{
		Name: scName,
	}
	reclaimPolicyDelete := v1.PersistentVolumeReclaimDelete
	bindMode := storageApi.VolumeBindingImmediate
	scObj := storageApi.StorageClass{
		ObjectMeta:        v1obj,
		Provisioner:       k8s.CsiProvisioner,
		Parameters:        params,
		ReclaimPolicy:     &reclaimPolicyDelete,
		VolumeBindingMode: &bindMode,
	}
	// Applying each extra option to Storage class definition
	for _, opt := range opts {
		opt(&scObj)
	}
	k8sStorage := storage.Instance()
	if _, err := k8sStorage.CreateStorageClass(&scObj); err != nil {
		return err
	}
	return nil
}

// Generic definition to keep on adding new params to storageclass definition
type StorageClassOption func(*storageApi.StorageClass)

// WithSecureParameter Method add secure param to existing StorageClass definitions
func WithSecureParameter(secure bool) StorageClassOption {
	return func(sc *storageApi.StorageClass) {
		if secure {
			sc.Parameters["secure"] = "true"
		}
	}
}

// WithSnapshotSchedule adds a snapshot schedule to the StorageClass parameters
func WithSnapshotSchedule(scheduleName, snapshotType string) StorageClassOption {
	return func(sc *storageApi.StorageClass) {
		yamlSnippet := fmt.Sprintf("schedulePolicyName: %s\nannotations:\n  portworx/snapshot-type: %s", scheduleName, snapshotType)
		sc.Parameters["snapshotschedule.stork.libopenstorage.org/interval-schedule"] = yamlSnippet
	}
}

// WithVolumeExpansion sets allowVolumeExpansion parameter to storageclass
func WithVolumeExpansion(allowExpansion bool) StorageClassOption {
	return func(sc *storageApi.StorageClass) {
		sc.AllowVolumeExpansion = &allowExpansion
	}
}
