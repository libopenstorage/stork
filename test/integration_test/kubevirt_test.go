//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/kubevirt"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

var templatePVCSpecs = map[string]string{
	"fedora": "kubevirt-templates",
}

const (
	importerPodPrefix            = "importer"
	importerPodStartTimeout      = 2 * time.Minute
	importerPodCompletionTimeout = 20 * time.Minute
	importerPodRetryInterval     = 10 * time.Second

	kubevirtTemplates                     = "kubevirt-templates"
	kubevirtDatadiskTemplates             = "kubevirt-datadisk-templates"
	kubevirtTemplateNamespace             = "openshift-virtualization-os-images"
	kubevirtDatadiskNamespace             = "openshift-virtualization-datadisk-templates"
	kubevirtCDIStorageConditionAnnotation = "cdi.kubevirt.io/storage.condition.running.reason"
	kubevirtCDIStoragePodPhaseAnnotation  = "cdi.kubevirt.io/storage.pod.phase"

	volumeBindingImmediate = "kubevirt-templates"
)

func TestKubevirt(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	require.NoError(t, err, "Error resetting mock time")

	err = createImageTemplates(t)
	require.NoError(t, err, "Error creating kubevirt templates")

	err = createDatadiskTemplates(t)
	require.NoError(t, err, "Error creating kubevirt templates")

	t.Run("kubevirtDeployFedoraVMWithClonePVC", kubevirtDeployFedoraVMWithClonePVC)
	t.Run("kubevirtDeployWindowsServerWithClonePVC", kubevirtDeployWindowsServerWithClonePVC)
	t.Run("kubevirtDeployFedoraVMWithClonePVCWaitFirstConsumer", kubevirtDeployFedoraVMWithClonePVCWaitFirstConsumer)
	t.Run("kubevirtDeployWindowsServerWithClonePVCWaitFirstConsumer", kubevirtDeployWindowsServerWithClonePVCWaitFirstConsumer)
	t.Run("kubevirtDeployFedoraVMMultiVolume", kubevirtDeployFedoraVMMultiVolume)
}

func kubevirtDeployFedoraVMWithClonePVC(t *testing.T) {
	var testrailID, testResult = 50803, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "vm"
	appKey := "kubevirt-fedora"
	deployedVMName := "fedora-test-vm"

	ctxs := kubevirtVMDeployAndValidate(
		t,
		instanceID,
		appKey,
		deployedVMName,
		false,
	)

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func kubevirtDeployWindowsServerWithClonePVC(t *testing.T) {
	var testrailID, testResult = 50804, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "vm"
	appKey := "kubevirt-windows-22k-server"
	deployedVMName := "windows-test-vm"

	ctxs := kubevirtVMDeployAndValidate(
		t,
		instanceID,
		appKey,
		deployedVMName,
		false,
	)

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func kubevirtVMDeployAndValidate(
	t *testing.T,
	instanceID string,
	appKey string,
	deployedVMName string,
	multiVolume bool,
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
		validateVM(t, vm, deployedVMName, multiVolume)
	}
	return ctxs
}

func kubevirtDeployFedoraVMWithClonePVCWaitFirstConsumer(t *testing.T) {
	var testrailID, testResult = 50803, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "vm"
	appKey := "kubevirt-fedora-wait-first-consumer"
	deployedVMName := "fedora-test-vm-wait-first-consumer"

	ctxs := kubevirtVMDeployAndValidate(
		t,
		instanceID,
		appKey,
		deployedVMName,
		false,
	)

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func kubevirtDeployWindowsServerWithClonePVCWaitFirstConsumer(t *testing.T) {
	var testrailID, testResult = 50804, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "vm"
	appKey := "kubevirt-windows-22k-server-wait-first-consumer"
	deployedVMName := "windows-test-vm-wait-first-consumer"

	ctxs := kubevirtVMDeployAndValidate(
		t,
		instanceID,
		appKey,
		deployedVMName,
		false,
	)

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func kubevirtDeployFedoraVMMultiVolume(t *testing.T) {
	var testrailID, testResult = 50803, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "vm"
	appKey := "kubevirt-fedora-multiple-disks"
	deployedVMName := "fedora-vm-multidisk"

	ctxs := kubevirtVMDeployAndValidate(
		t,
		instanceID,
		appKey,
		deployedVMName,
		true,
	)

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func validateVM(t *testing.T, virtualMachine kubevirtv1.VirtualMachine, vmName string, multiVol bool) {
	require.Equal(t, virtualMachine.Name, vmName, "VM %s has not been deployed", vmName)
	require.Equal(t, virtualMachine.Status.Created, true, "VM %s created status is: %t", virtualMachine.Name, virtualMachine.Status.Created)
	require.Equal(t, virtualMachine.Status.Ready, true, "VM %s ready status is: %t", virtualMachine.Name, virtualMachine.Status.Ready)

	if multiVol {
		// verify there are multiple volumes mounted by the virtual machine in case of multi volume config
		require.Greater(t, len(virtualMachine.Spec.Template.Spec.Volumes), 2, "VM %s does not have the required data disks", virtualMachine.Name)
	}
	// TODO add more validations here if required
}

func createImageTemplates(t *testing.T) error {
	_, err := schedulerDriver.Schedule("",
		scheduler.ScheduleOptions{
			AppKeys:   []string{kubevirtTemplates},
			Namespace: kubevirtTemplateNamespace,
		})
	require.NoErrorf(t, err, "error deploying kubevirt templates")

	// if new templates are deployed, this function will wait for them to get imported else it will exit
	waitForCompletedAnnotations := func() (interface{}, bool, error) {
		// Loop through all PVCs and check for annotations that signify existing downloaded templates
		pvcTemplates, err := core.Instance().GetPersistentVolumeClaims(kubevirtTemplateNamespace, nil)
		require.NoErrorf(t, err, "error getting PVCs in %s namespace", kubevirtTemplateNamespace)
		for _, pvc := range pvcTemplates.Items {
			if pvc.ObjectMeta.Annotations[kubevirtCDIStorageConditionAnnotation] != "Completed" {
				return nil, true, fmt.Errorf("storage condition is not completed on pvc %s. Status: %s. Retrying.",
					pvc.Name, pvc.ObjectMeta.Annotations[kubevirtCDIStorageConditionAnnotation])
			}
			if pvc.ObjectMeta.Annotations[kubevirtCDIStoragePodPhaseAnnotation] != "Succeeded" {
				return nil, true, fmt.Errorf("pod phase has not succeeded on pvc %s. Phase: %s. Retrying.",
					pvc.Name, pvc.ObjectMeta.Annotations[kubevirtCDIStoragePodPhaseAnnotation])
			}
		}
		logrus.Infof("All templates are downloaded.")
		return "", false, nil
	}
	_, err = task.DoRetryWithTimeout(waitForCompletedAnnotations, importerPodCompletionTimeout, importerPodRetryInterval)
	if err != nil {
		return err
	}

	return err
}

func createDatadiskTemplates(t *testing.T) error {
	ctxs, err := schedulerDriver.Schedule("",
		scheduler.ScheduleOptions{
			AppKeys:   []string{kubevirtDatadiskTemplates},
			Namespace: kubevirtDatadiskNamespace,
		})
	if err != nil {
		return fmt.Errorf("error deploying kubevirt datadisk templates")
	}

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	if err != nil {
		return fmt.Errorf("error waiting to provision kubevirt data disk PVCs")
	}
	return nil
}
