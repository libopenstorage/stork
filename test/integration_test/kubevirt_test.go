//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/core"
	kubevirt "github.com/portworx/sched-ops/k8s/kubevirt"
	"github.com/portworx/sched-ops/k8s/rbac"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var templatePVCSpecs = map[string]string{
	"fedora": "kubevirt-templates",
}

const (
	importerPodPrefix            = "importer"
	importerPodStartTimeout      = 2 * time.Minute
	importerPodCompletionTimeout = 60 * time.Minute
	importerPodRetryInterval     = 30 * time.Second

	kubevirtTemplates                     = "kubevirt-templates"
	kubevirtDatadiskTemplates             = "kubevirt-datadisk-templates"
	kubevirtTemplateNamespace             = "openshift-virtualization-os-images"
	kubevirtDatadiskNamespace             = "openshift-virtualization-datadisk-templates"
	kubevirtCDIStorageConditionAnnotation = "cdi.kubevirt.io/storage.condition.running.reason"
	kubevirtCDIStoragePodPhaseAnnotation  = "cdi.kubevirt.io/storage.pod.phase"

	volumeBindingImmediate       = "kubevirt-templates"
	dataVolumeClusterrole        = "datavol-clusterrole"
	dataVolumeClusterroleBinding = "datavol-clusterrolebinding"
	dataDiskClusterroleBinding   = "datadisk-clusterrolebinding"
)

func TestKubevirt(t *testing.T) {
	// reset mock time before running any tests
	err := setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")
	currentTestSuite = t.Name()

	err = createImageTemplates(t)
	log.FailOnError(t, err, "Error creating kubevirt templates")

	err = createDatadiskTemplates(t)
	log.FailOnError(t, err, "Error creating kubevirt templates")

	err = createClusterroleDataVolume(dataVolumeClusterrole)
	log.FailOnError(t, err, "Error creating cluste role for datavolume clone")

	err = createClusterroleBindingDatavolume(dataVolumeClusterrole, kubevirtDatadiskNamespace)
	log.FailOnError(t, err, "Error creating/updating data volume clusterrole binding %s with namespace: %s",
		dataVolumeClusterroleBinding, kubevirtDatadiskNamespace)

	t.Run("kubevirtDeployFedoraVMWithClonePVC", kubevirtDeployFedoraVMWithClonePVC)
	t.Run("kubevirtDeployWindowsServerWithClonePVC", kubevirtDeployWindowsServerWithClonePVC)
	t.Run("kubevirtDeployFedoraVMWithClonePVCWaitFirstConsumer", kubevirtDeployFedoraVMWithClonePVCWaitFirstConsumer)
	t.Run("kubevirtDeployWindowsServerWithClonePVCWaitFirstConsumer", kubevirtDeployWindowsServerWithClonePVCWaitFirstConsumer)
	t.Run("kubevirtDeployFedoraVMMultiVolume", kubevirtDeployFedoraVMMultiVolume)
	t.Run("kubeVirtSimulateOCPUpgrade", kubeVirtSimulateOCPUpgrade)
	t.Run("kubeVirtHypercTwoLiveMigrations", kubeVirtHypercTwoLiveMigrations)
	t.Run("kubeVirtHypercHotPlugDiskCollocation", kubeVirtHypercHotPlugDiskCollocation)
	t.Run("kubeVirtHypercVPSFixJob", kubeVirtHypercVPSFixJob)
	t.Run("kubeVirtUpdatePX", kubeVirtUpdatePX)
	t.Run("kubeVirtUpdatePXBlocked", kubeVirtUpdatePXBlocked)
}

func kubevirtDeployFedoraVMWithClonePVC(t *testing.T) {
	var testrailID, testResult = 50803, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	instanceID := "vm"
	appKeys := []string{"kubevirt-fedora"}

	allCtxs := kubevirtVMScaledDeployAndValidate(t, instanceID, appKeys, kubevirtScale)

	log.InfoD("Destroying apps")
	destroyAndWait(t, allCtxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func kubevirtDeployWindowsServerWithClonePVC(t *testing.T) {
	var testrailID, testResult = 50804, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	instanceID := "vm"
	appKeys := []string{"kubevirt-windows-22k-server"}

	allCtxs := kubevirtVMScaledDeployAndValidate(t, instanceID, appKeys, kubevirtScale)

	log.InfoD("Destroying apps")
	destroyAndWait(t, allCtxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func kubevirtDeployFedoraVMWithClonePVCWaitFirstConsumer(t *testing.T) {
	var testrailID, testResult = 50803, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	instanceID := "vm"
	appKeys := []string{"kubevirt-fedora-wait-first-consumer"}

	allCtxs := kubevirtVMScaledDeployAndValidate(t, instanceID, appKeys, kubevirtScale)

	log.InfoD("Destroying apps")
	destroyAndWait(t, allCtxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func kubevirtDeployWindowsServerWithClonePVCWaitFirstConsumer(t *testing.T) {
	var testrailID, testResult = 50804, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	instanceID := "vm"
	appKeys := []string{"kubevirt-windows-22k-server-wait-first-consumer"}

	allCtxs := kubevirtVMScaledDeployAndValidate(t, instanceID, appKeys, kubevirtScale)

	log.InfoD("Destroying apps")
	destroyAndWait(t, allCtxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func kubevirtDeployFedoraVMMultiVolume(t *testing.T) {
	var testrailID, testResult = 50803, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)
	instanceID := "vm"
	appKeys := []string{"kubevirt-fedora-multiple-disks"}

	allCtxs := kubevirtVMScaledDeployAndValidate(t, instanceID, appKeys, kubevirtScale)

	log.InfoD("Destroying apps")
	destroyAndWait(t, allCtxs)

	// If we are here then the test has passed
	testResult = testResultPass
	log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}

func createImageTemplates(t *testing.T) error {
	_, err := schedulerDriver.Schedule("",
		scheduler.ScheduleOptions{
			AppKeys:   []string{kubevirtTemplates},
			Namespace: kubevirtTemplateNamespace,
		})
	log.FailOnError(t, err, "error deploying kubevirt templates")

	// if new templates are deployed, this function will wait for them to get imported else it will exit
	waitForCompletedAnnotations := func() (interface{}, bool, error) {
		// Loop through all PVCs and check for annotations that signify existing downloaded templates
		pvcTemplates, err := core.Instance().GetPersistentVolumeClaims(kubevirtTemplateNamespace, nil)
		log.FailOnError(t, err, "error getting PVCs in %s namespace", kubevirtTemplateNamespace)
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
		log.InfoD("All templates are downloaded.")
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

func kubevirtVMScaledDeployAndValidate(t *testing.T, instanceID string, appKeys []string, scale int) []*scheduler.Context {
	var allCtxs []*scheduler.Context
	log.InfoD("Deploying %d VMs and validating them.", len(appKeys)*scale)
	for i := 1; i <= scale; i++ {
		log.InfoD("Deploying VM set: %d", i)
		// Before validating the VM create required clusterrolebindings for datavolume cloning in the VM namespace
		var testNamespaces []string
		for _, app := range appKeys {
			testNamespace := app + "-" + instanceID + "-" + strconv.Itoa(i)
			testNamespaces = append(testNamespaces, testNamespace)
			err := createClusterroleBindingDatavolume(dataVolumeClusterrole, testNamespace)
			log.FailOnError(t, err, "Error creating/updating data volumeclusterrole binding %s with namespace: %s",
				dataVolumeClusterroleBinding, testNamespace)
		}
		ctxs, err := schedulerDriver.Schedule(instanceID+"-"+strconv.Itoa(i),
			scheduler.ScheduleOptions{
				AppKeys: appKeys,
			})
		log.FailOnError(t, err, "Error scheduling tasks")
		Dash.VerifyFatal(t, len(appKeys), len(ctxs), "wrong number of tasks started")

		for _, ctx := range ctxs {
			err = schedulerDriver.WaitForRunning(ctx, 30*time.Minute, defaultWaitInterval)
			log.FailOnError(t, err, "Error waiting for app %s to get to running state", ctx.App.Key)

			vms, err := kubevirt.Instance().ListVirtualMachines(ctx.App.NameSpace)
			log.FailOnError(t, err, "Error listing virtual machines")
			for _, vm := range vms.Items {
				Dash.VerifyFatal(t, vm.Status.Created, true, fmt.Sprintf("VM %s created yet", vm.Name))
				Dash.VerifyFatal(t, vm.Status.Ready, true, fmt.Sprintf("VM %s not ready yet", vm.Name))
				log.InfoD("VM %s in namespace %s, has %d disks", vm.Name, vm.Namespace, len(vm.Spec.Template.Spec.Volumes))
				log.InfoD("Validated VM in iteration: %d, name: %s, namespace: %s", i, vm.Name, vm.Namespace)
			}

		}
		allCtxs = append(allCtxs, ctxs...)
	}
	return allCtxs
}

func createClusterroleDataVolume(clusterroleName string) error {
	clusterRoleDatavolume := &rbacv1.ClusterRole{
		ObjectMeta: meta.ObjectMeta{
			Name: clusterroleName,
		},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{"cdi.kubevirt.io"},
			Resources: []string{"datavolumes/source"},
			Verbs:     []string{"*"},
		},
		},
	}
	_, err := rbac.Instance().CreateClusterRole(clusterRoleDatavolume)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// creates/updates clusterrolebindings by adding default ServiceAccount in the given namespace
func createClusterroleBindingDatavolume(clusterRoleName, namespace string) error {
	newSubject := rbacv1.Subject{Kind: "ServiceAccount", Name: "default", Namespace: namespace}

	// append subjects if clusterrolebinding already exists else create new cluster role binding
	cB, err := rbac.Instance().GetClusterRoleBinding(dataVolumeClusterroleBinding)
	if err == nil && cB != nil {
		cB.Subjects = append(cB.Subjects, newSubject)
		_, err = rbac.Instance().UpdateClusterRoleBinding(cB)
	} else {
		clusterRoleBindingDatavolume := &rbacv1.ClusterRoleBinding{
			ObjectMeta: meta.ObjectMeta{
				Name: dataVolumeClusterroleBinding,
			},
			Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "default", Namespace: namespace}},
			RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: clusterRoleName},
		}
		_, err = rbac.Instance().CreateClusterRoleBinding(clusterRoleBindingDatavolume)
	}
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}
	return nil
}
