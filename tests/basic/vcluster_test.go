package tests

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/vcluster"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

var _ = Describe("CreateNginxAppOnVcluster", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("VclusterOperations", "Create, Connect and execute a method on Vcluster", nil, 0)
	})
	var vclusterNames = []string{"my-vcluster1"}
	It("Create and connect to vclusters and run a sample method", func() {
		steplog := "Create vClusters"
		log.InfoD(steplog)
		Step(steplog, func() {
			for _, name := range vclusterNames {
				currentDir, err := os.Getwd()
				log.FailOnError(err, "Could not get absolute path to current Dir")
				vClusterPath := filepath.Join(currentDir, "..", "deployments", "customconfigs", "vcluster.yaml")
				absPath, err := filepath.Abs(vClusterPath)
				log.FailOnError(err, "Could not get absolute path to vcluster.yaml")
				err = vcluster.CreateVCluster(name, absPath)
				log.FailOnError(err, "Failed to create vCluster")
			}
		})
		steplog = "Wait for all vClusters to come up in Running State"
		log.InfoD(steplog)
		Step(steplog, func() {
			for _, name := range vclusterNames {
				err := vcluster.WaitForVClusterRunning(name, 10*time.Minute)
				log.FailOnError(err, "Vcluster did not come up in time")
			}
		})
		steplog = "Connect to each vCluster and execute a method"
		log.InfoD(steplog)
		Step(steplog, func() {
			for _, name := range vclusterNames {
				log.Infof("Trying to connect to %v", name)
				err := ConnectVClusterAndExecute(name, RunNginxAppOnVcluster, name, vcluster.NginxApp)
				log.FailOnError(err, fmt.Sprintf("Failed to connect and execute method on cluster %v", name))
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
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
	return []interface{}{nil}
}
