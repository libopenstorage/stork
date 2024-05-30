//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	crdv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/storkctl"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/torpedo/drivers/scheduler"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestStorkCtlSnapshotSchedule(t *testing.T) {
	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	currentTestSuite = t.Name()
	t.Run("testSnapShotScheduleModifyPVCName", testSnapShotScheduleModifyPVCName)

	err = setRemoteConfig("")
	log.FailOnError(t, err, "setting kubeconfig to default failed")
}

/*
testSnapShotScheduleModifyPVCName tests the `storkctl update` utility to update the volumesnapshotschedule
with a certain PVC name.

Steps:
1. Deploy a test mysql application with PVC.
2. Deploy a schedulepolicy and volumesnapshotschedule corresponding to the app's PVC.
3. Use storkctl to update the volumesnapshotschedule's PVC.
4. Verify if the change got reflected in the actual volumesnapshotschedule.
*/
func testSnapShotScheduleModifyPVCName(t *testing.T) {
	// TODO: Add testrail id here for this test.
	// var testrailID, testResult = 50797, testResultFail
	// runID := testrailSetupForTest(testrailID, &testResult, t.Name())
	// defer updateTestRail(&testResult, testrailID, runID)
	// defer updateDashStats(t.Name(), &testResult)

	/////////////////////////////////////////////////
	// Deploy test application and schedulepolicy //
	///////////////////////////////////////////////
	ctx := createApp(t, "storkctl-snapshot-schedule-pvc-update")
	policyName := "intervalpolicy"
	retain := 2
	interval := 2
	schedPolicy := &storkv1.SchedulePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Interval: &storkv1.IntervalPolicy{
				Retain:          storkv1.Retain(retain),
				IntervalMinutes: interval,
			},
		}}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(schedPolicy)
		log.FailOnError(t, err, "Error adding annotations to interval schedule policy")
	}

	_, err := storkops.Instance().CreateSchedulePolicy(schedPolicy)
	log.FailOnError(t, err, "Error creating interval schedule policy")
	log.InfoD("Created schedulepolicy %v with %v minute interval and retain at %v", policyName, interval, retain)

	////////////////////////////////////////////////////////////////////////////////
	// Deploy volumesnapshotschedule corresponding to the test application's PVC //
	//////////////////////////////////////////////////////////////////////////////
	scheduleName := "snapshotschedule-pvc-update-test"
	namespace := ctx.GetID()
	snapSched := &storkv1.VolumeSnapshotSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scheduleName,
			Namespace: namespace,
		},
		Spec: storkv1.VolumeSnapshotScheduleSpec{
			Template: storkv1.VolumeSnapshotTemplateSpec{
				Spec: crdv1.VolumeSnapshotSpec{
					PersistentVolumeClaimName: "mysql-data"},
			},
			SchedulePolicyName: policyName,
		},
	}

	if authTokenConfigMap != "" {
		err := addSecurityAnnotation(snapSched)
		log.FailOnError(t, err, "Error adding annotations to interval schedule policy")
	}

	_, err = storkops.Instance().CreateSnapshotSchedule(snapSched)
	log.FailOnError(t, err, "Error creating snapshot schedule")
	sleepTime := time.Duration((retain+1)*interval) * time.Minute
	log.InfoD("Created snapshotschedule %v in namespace %v, sleeping for %v for schedule to trigger",
		scheduleName, namespace, sleepTime)
	time.Sleep(sleepTime)

	_, err = storkops.Instance().ValidateSnapshotSchedule(scheduleName,
		namespace,
		snapshotScheduleRetryTimeout,
		snapshotScheduleRetryInterval)
	log.FailOnError(t, err, "Error validating snapshot schedule")
	log.InfoD("Validated snapshotschedule %v", scheduleName)

	/////////////////////////////////////////////////////////////////
	// Update PVC name of volumesnapshotschedule through storkctl //
	////////////////////////////////////////////////////////////////

	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	cmdArgs := []string{"update", "volumesnapshotschedule", scheduleName, "--new-pvc-name", "new-pvc", "-n", namespace}
	cmd.SetArgs(cmdArgs)

	// Execute the command.
	log.InfoD("The storkctl command being executed is %v", cmdArgs)
	err = cmd.Execute()
	log.FailOnError(t, err, "Storkctl execution failed: %v", err)

	////////////////////////////////////////////
	// Assert the command output and results //
	//////////////////////////////////////////

	// Get the captured output as a string and validate it with the expected output.
	actualOutput := outputBuffer.String()
	log.InfoD("Actual output is: %s\n", actualOutput)
	expectedOutput := fmt.Sprintf("VolumeSnapshotSchedule %v updated successfully\n", scheduleName)
	Dash.VerifyFatal(t, expectedOutput, actualOutput, "Error validating the output of the command")

	// Validate the snapshotschedule CR modification through the command.
	snapshotSchedule, err := storkops.Instance().GetSnapshotSchedule(scheduleName, namespace)
	log.FailOnError(t, err, "failed to get snapshotschedule %s from %s namespace : %v", scheduleName, namespace, err)
	Dash.VerifyFatal(t, snapshotSchedule.Spec.Template.Spec.PersistentVolumeClaimName, "new-pvc", "snapshotschedule pvc name should be updated")

	//////////////
	// Cleanup //
	////////////

	deletePolicyAndSnapshotSchedule(t, namespace, policyName, scheduleName)
	destroyAndWait(t, []*scheduler.Context{ctx})

	// If we are here then the test has passed
	// testResult = testResultPass
	// log.InfoD("Test status at end of %s test: %s", t.Name(), testResult)
}
