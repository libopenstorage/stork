package tests

import (
	"fmt"
	"github.com/portworx/torpedo/drivers/node"
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/vcluster"
	"github.com/portworx/torpedo/pkg/log"
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
		Filename:  "/data/fiotest",
		EndFsync:  1,
	}
	JustBeforeEach(func() {
		StartTorpedoTest("CreateAndRunFioOnVcluster", "Create, Connect and run FIO Application on Vcluster", nil, 0)
		vc, err := vcluster.NewVCluster("my-vcluster1")
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
		err = vc.CreateFIODeployment(pvcName, appNS, fioOptions, jobName)
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
	const totalIterations = 2 // Number of Iterations we want to run the FIO Pods for
	const batchCount = 5      // Number of FIO Pods to run in parallel in a single iteration
	fioOptions := vcluster.FIOOptions{
		Name:      "mytest",
		IOEngine:  "libaio",
		RW:        "randwrite",
		BS:        "4k",
		NumJobs:   1,
		Size:      "500m",
		TimeBased: true,
		Runtime:   "100s",
		Filename:  "/data/fiotest",
		EndFsync:  1,
	}
	JustBeforeEach(func() {
		StartTorpedoTest("CreateAndRunMultipleFioOnVcluster", "Create, Connect and run Multiple FIO Applications on Same Vcluster", nil, 0)
		vc, err := vcluster.NewVCluster("my-vcluster1")
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
					err = vc.CreateFIODeployment(pvcName, appNS, fioOptions, jobName)
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
		vc, err := vcluster.NewVCluster("my-vcluster1")
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
		Runtime:   "100s",
		Filename:  "/data/fiotest",
		EndFsync:  1,
	}
	JustBeforeEach(func() {
		StartTorpedoTest("CreateAndRunFioOnVclusterRWX", "Create, Connect and run 2 FIO Applications on Vcluster on RWX PVC", nil, 0)
		vc, err := vcluster.NewVCluster("my-vcluster1")
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
			err = vc.CreateFIODeployment(pvcName, appNS, fioOptions, jobName1)
			log.FailOnError(err, "Error in creating first FIO Application")
		}()
		go func() {
			defer wg.Done()
			err = vc.CreateFIODeployment(pvcName, appNS, fioOptions, jobName2)
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
	const totalVclusters = 3
	var vClusters []*vcluster.VCluster
	var scName string
	var appNS string
	const totalIterations = 2 // Number of Iterations we want to run the FIO Pods for
	const batchCount = 2      // Number of FIO Pods to run in parallel in a single iteration
	fioOptions := vcluster.FIOOptions{
		Name:      "mytest",
		IOEngine:  "libaio",
		RW:        "randwrite",
		BS:        "4k",
		NumJobs:   1,
		Size:      "500m",
		TimeBased: true,
		Runtime:   "100s",
		Filename:  "/data/fiotest",
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
							err = vc.CreateFIODeployment(pvcName, appNS, fioOptions, jobName)
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
		vc, err := vcluster.NewVCluster("my-vcluster1")
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

// CreateStorageClass method creates a storageclass using host's k8s clientset on host cluster
func CreateStorageClass(scName string) error {
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
	k8sStorage := storage.Instance()
	if _, err := k8sStorage.CreateStorageClass(&scObj); err != nil {
		return err
	}
	return nil
}
