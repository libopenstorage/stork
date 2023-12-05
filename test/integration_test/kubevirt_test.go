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
	"github.com/portworx/torpedo/pkg/log"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
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
	t.Run("kubeVirtHypercOneLiveMigration", kubeVirtHypercOneLiveMigration)
	t.Run("kubeVirtHypercTwoLiveMigrations", kubeVirtHypercTwoLiveMigrations)
}

func kubevirtDeployFedoraVMWithClonePVC(t *testing.T) {
	var testrailID, testResult = 50803, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "vm"
	appKey := "kubevirt-fedora"

	ctxs := kubevirtVMsDeployAndValidate(
		t,
		instanceID,
		[]string{appKey},
	)

	log.Infof("Destroying apps")
	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func kubevirtDeployWindowsServerWithClonePVC(t *testing.T) {
	var testrailID, testResult = 50804, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "vm"
	appKey := "kubevirt-windows-22k-server"

	ctxs := kubevirtVMsDeployAndValidate(
		t,
		instanceID,
		[]string{appKey},
	)

	log.Infof("Destroying apps")
	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func kubevirtVMsDeployAndValidate(
	t *testing.T,
	instanceID string,
	appKeys []string,
) []*scheduler.Context {
	ctxs, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{
			AppKeys: appKeys,
		})
	require.NoError(t, err, "Error scheduling tasks")
	require.Equal(t, len(appKeys), len(ctxs), "wrong number of tasks started")

	for _, ctx := range ctxs {
		err = schedulerDriver.WaitForRunning(ctx, 30*time.Minute, defaultWaitInterval)
		require.NoError(t, err, "Error waiting for app %s to get to running state", ctx.App.Key)

		namespace := appKey + "-" + instanceID
		vms, err := kubevirt.Instance().ListVirtualMachines(namespace)
		require.NoError(t, err, "Error listing virtual machines")

		for _, vm := range vms.Items {
			require.Equal(t, vm.Status.Created, true, "VM %s not created yet", vm.Name)
			require.Equal(t, vm.Status.Ready, true, "VM %s not ready yet", vm.Name)
			logrus.Infof("VM %s has %d disks", vm.Name, len(vm.Spec.Template.Spec.Volumes))
		}
	}
	return ctxs
}

func kubevirtDeployFedoraVMWithClonePVCWaitFirstConsumer(t *testing.T) {
	var testrailID, testResult = 50803, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	instanceID := "vm"
	appKey := "kubevirt-fedora-wait-first-consumer"

	ctxs := kubevirtVMsDeployAndValidate(
		t,
		instanceID,
		[]string{appKey},
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

	ctxs := kubevirtVMsDeployAndValidate(
		t,
		instanceID,
		[]string{appKey},
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

	ctxs := kubevirtVMsDeployAndValidate(
		t,
		instanceID,
		[]string{appKey},
	)

	destroyAndWait(t, ctxs)

	// If we are here then the test has passed
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
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
		log.Infof("All templates are downloaded.")
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
