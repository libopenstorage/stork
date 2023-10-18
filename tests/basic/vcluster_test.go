package tests

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/vcluster"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"

	v1 "k8s.io/api/core/v1"
	storageApi "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("CreateAndRunFioOnVcluster", func() {
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
		vc = vcluster.NewVCluster("my-vcluster1")
		err := vc.CreateAndWaitVCluster()
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
		pvcName, err = vc.CreatePVC(scName, appNS)
		log.FailOnError(err, fmt.Sprintf("Error creating PVC with Storageclass name %v", scName))
		log.Infof("Successfully created PVC with name: %v", pvcName)
		// Create FIO Deployment on VCluster using the above PVC
		err = vc.CreateFIODeployment(pvcName, appNS, fioOptions)
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

var _ = Describe("CreateNginxAppOnVcluster", func() {
	const vclusterCount = 1
	vclusterNames := make([]string, vclusterCount)
	JustBeforeEach(func() {
		StartTorpedoTest("VclusterOperations", "Create, Connect and execute a method on Vcluster", nil, 0)
		vclusterNames, err = vcluster.CreateAndWaitForVCluster(vclusterCount)
		log.FailOnError(err, "Failed in vcluster prep")
	})
	It("Bring up Nginx Application", func() {
		for _, name := range vclusterNames {
			log.Infof("Trying to connect to %v", name)
			err := ConnectVClusterAndExecute(name, RunNginxAppOnVcluster, name, vcluster.NginxApp)
			log.FailOnError(err, fmt.Sprintf("Failed to connect and execute method on cluster %v", name))
		}
	})
	JustAfterEach(func() {
		for _, name := range vclusterNames {
			vcluster.DeleteVCluster(name)
		}
	})
})

// This method connects to a vcluster and executes a test function that a testcase wants to run
func ConnectVClusterAndExecute(vclusterName string, testFunc func([]interface{}) []interface{}, args ...interface{}) error {
	killChan := make(chan bool)
	// Running the vcluster connect in the background
	go func() {
		cmd := exec.Command("vcluster", "connect", vclusterName)
		cmd.Start()
		<-killChan
		cmd.Process.Signal(os.Interrupt)
		cmd.Wait()
	}()
	f := func() (interface{}, bool, error) {
		cmd := exec.Command("kubectl", "config", "current-context")
		out, err := cmd.Output()
		if err != nil {
			return nil, true, fmt.Errorf("Failed to get current context: %v", err)
		}
		prefix := fmt.Sprintf("vcluster_%s_", vclusterName)
		curContext := strings.TrimSpace(string(out))
		log.Infof("current context is: %v", curContext)
		log.Infof("prefix is: %v", prefix)
		if !strings.Contains(curContext, prefix) {
			return nil, true, fmt.Errorf("Context not yet switched to %v. Retrying", curContext)
		} else {
			log.Infof("Successfully switched to context: %v", strings.TrimSpace(string(out)))
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("Context not yet switched")
	}
	_, err := task.DoRetryWithTimeout(f, vcluster.VclusterConnectionTimeout, vcluster.VClusterRetryInterval)
	if err != nil {
		killChan <- true
		return err
	}

	results := testFunc(args)
	killChan <- true
	if resError, ok := results[0].(error); ok && resError != nil {
		return resError
	}
	return nil
}

// This method tries to run nginx app within a vcluster
func RunNginxAppOnVcluster(args []interface{}) []interface{} {
	clusterName, ok := args[0].(string)
	if !ok {
		return []interface{}{fmt.Errorf("Expected the first argument type to be string")}
	}
	appToRun, ok := args[1].(string)
	if !ok {
		return []interface{}{fmt.Errorf("Expected the second argument type to be string")}
	}
	log.Infof("Trying to Run App %v within the vCluster: %v", appToRun, clusterName)
	Inst().AppList = []string{appToRun}
	vcluster.ContextChange = true
	vcluster.CurrentClusterContext = clusterName
	vcluster.UpdatedClusterContext = "host"
	context := ScheduleApplications("vcluster-test-app")
	errChan := make(chan error, 100)
	for _, ctx := range context {
		ValidateContext(ctx, &errChan)
	}
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}
	errStrings := make([]string, 0)
	for _, err := range errors {
		if err != nil {
			errStrings = append(errStrings, err.Error())
		}
	}
	if len(errStrings) > 0 {
		return []interface{}{errStrings}
	}
	opts := make(map[string]bool)
	opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

	for _, ctx := range context {
		TearDownContext(ctx, opts)
	}
	return []interface{}{nil}
}

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
