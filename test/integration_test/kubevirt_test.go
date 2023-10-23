//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/kubevirt"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

var templatePVCSpecs = map[string]string{
	"fedora": "kubevirt-templates",
}

const (
	importerPodPrefix            = "importer"
	importerPodStartTimeOut      = 2 * time.Minute
	importerPodCompletionTimeout = 20 * time.Minute
	importerPodRetryInterval     = 10 * time.Second
)

func TestKubevirt(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")

	err = createTemplatePVC()
	require.NoError(t, err, "Error creating template")

	//	t.Run("kubevirtDeployFedoraVMWithClonePVC", kubevirtDeployFedoraVMWithClonePVC)
	t.Run("kubeVirtHyperConvOneLiveMigration", kubeVirtHyperConvOneLiveMigration)
}

func kubevirtDeployFedoraVMWithClonePVC(t *testing.T) {
	var testrailID, testResult = 50803, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "vm"
	appKey := "kubevirt-fedora"
	deployedVMName := "test-vm-csi"

	kubevirtVMDeployAndValidate(
		t,
		instanceID,
		appKey,
		deployedVMName,
	)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func kubevirtVMDeployAndValidate(
	t *testing.T,
	instanceID string,
	appKey string,
	deployedVMName string,
) []*scheduler.Context {
	ctxs, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{
			AppKeys: []string{appKey},
		})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctxs), "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	require.NoError(t, err, "Error waiting for app to get to running state")

	namespace := appKey + "-" + instanceID
	vms, err := kubevirt.Instance().ListVirtualMachines(namespace)
	require.NoError(t, err, "Error listing virtual machines")

	for _, vm := range vms.Items {
		validateVM(t, vm, deployedVMName)
	}
	return ctxs
}

func validateVM(t *testing.T, virtualMachine kubevirtv1.VirtualMachine, vmName string) {
	require.Equal(t, virtualMachine.Name, vmName, "VM %s has not been deployed", vmName)
	require.Equal(t, virtualMachine.Status.Created, true, "VM %s created status is: %t", virtualMachine.Name, virtualMachine.Status.Created)
	require.Equal(t, virtualMachine.Status.Ready, true, "VM %s ready status is: %t", virtualMachine.Name, virtualMachine.Status.Ready)

	// TODO add more validations here if required
}

func createTemplatePVC() error {
	_, err := schedulerDriver.Schedule("",
		scheduler.ScheduleOptions{
			AppKeys:   []string{"kubevirt-templates"},
			Namespace: "openshift-virtualization-os-images",
		})
	if err != nil {
		return fmt.Errorf("error deploying kubevirt templates")
	}
	// TODO: wait for imported pod to complete
	waitForImporterPodStart := func() (interface{}, bool, error) {
		allPods, err := core.Instance().GetPods("openshift-virtualization-os-images", nil)
		if err != nil || len(allPods.Items) == 0 {
			return nil, true, fmt.Errorf("No importer pod not started yet. Retrying")
		}
		for _, pod := range allPods.Items {
			if strings.Contains(pod.Name, importerPodPrefix) {
				logrus.Infof("Importer pod found: %s. Status: %s", pod.Name, pod.Status.Phase)
				return nil, false, nil
			}
		}
		return nil, false, nil
	}
	_, err = task.DoRetryWithTimeout(waitForImporterPodStart, importerPodStartTimeOut, importerPodRetryInterval)
	if err != nil {
		return err
	}

	// Importer pod has begun, now wait for it to get completed and to disappear
	waitForImporterPodCompletion := func() (interface{}, bool, error) {
		allPods, err := core.Instance().GetPods("openshift-virtualization-os-images", nil)
		if err != nil {
			return "", false, nil
		}
		for _, pod := range allPods.Items {
			if strings.Contains(pod.Name, importerPodPrefix) {
				if pod.Status.Phase == corev1.PodRunning {
					return "", true, fmt.Errorf("importer pod %s found but not completed yet. Status: %s", pod.Name, pod.Status.Phase)
				} else {
					logrus.Infof("Importer pod: %s completed successfully", pod.Name)
					return "", false, nil
				}
			}
		}
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(waitForImporterPodCompletion, importerPodCompletionTimeout, importerPodRetryInterval)
	if err != nil {
		return err
	}
	return err
}
