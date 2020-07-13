// +build unittest

package storkctl

import (
	"testing"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetClonesNoClone(t *testing.T) {
	cmdArgs := []string{"get", "applicationclones"}

	var cloneList storkv1.ApplicationCloneList
	expected := "No resources found.\n"
	testCommon(t, cmdArgs, &cloneList, expected, false)
}

func createApplicationCloneAndVerify(
	t *testing.T,
	name string,
	namespace string,
	sourceNamespace string,
	destinationNamespace string,
	preExecRule string,
	postExecRule string,
) {
	cmdArgs := []string{"create", "clones", "-n", namespace, "--sourceNamespace", sourceNamespace, "--destinationNamespace", destinationNamespace, name}
	if preExecRule != "" {
		cmdArgs = append(cmdArgs, "--preExecRule", preExecRule)
	}
	if postExecRule != "" {
		cmdArgs = append(cmdArgs, "--postExecRule", postExecRule)
	}

	expected := "ApplicationClone " + name + " started successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// Make sure it was created correctly
	clone, err := storkops.Instance().GetApplicationClone(name, namespace)
	require.NoError(t, err, "Error getting clone")
	require.Equal(t, name, clone.Name, "ApplicationClone name mismatch")
	require.Equal(t, namespace, clone.Namespace, "ApplicationClone namespace mismatch")
	require.Equal(t, sourceNamespace, clone.Spec.SourceNamespace, "ApplicationClone source namespace mismatch")
	require.Equal(t, destinationNamespace, clone.Spec.DestinationNamespace, "ApplicationClone destination namespace mismatch")
	require.Equal(t, preExecRule, clone.Spec.PreExecRule, "ApplicationClone preExecRule mismatch")
	require.Equal(t, postExecRule, clone.Spec.PostExecRule, "ApplicationClone postExecRule mismatch")
}

func TestGetApplicationClonesOneApplicationClone(t *testing.T) {
	defer resetTest()
	createApplicationCloneAndVerify(t, "getclonetest", "test", "src", "dest", "preExec", "postExec")

	expected := "NAME           SOURCE   DESTINATION   STAGE   STATUS   VOLUMES   RESOURCES   CREATED   ELAPSED\n" +
		"getclonetest   src      dest                           0/0       0                     \n"

	cmdArgs := []string{"get", "clones", "-n", "test"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetApplicationClonesMultiple(t *testing.T) {
	defer resetTest()
	_, err := core.Instance().CreateNamespace(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
	require.NoError(t, err, "Error creating default namespace")

	createApplicationCloneAndVerify(t, "getclonetest1", "default", "src", "dest", "", "")
	createApplicationCloneAndVerify(t, "getclonetest2", "default", "src", "dest", "", "")

	expected := "NAME            SOURCE   DESTINATION   STAGE   STATUS   VOLUMES   RESOURCES   CREATED   ELAPSED\n" +
		"getclonetest1   src      dest                           0/0       0                     \n" +
		"getclonetest2   src      dest                           0/0       0                     \n"

	cmdArgs := []string{"get", "clones", "getclonetest1", "getclonetest2"}
	testCommon(t, cmdArgs, nil, expected, false)

	// Should get all clones if no name given
	cmdArgs = []string{"get", "clones"}
	testCommon(t, cmdArgs, nil, expected, false)

	expected = "NAME            SOURCE   DESTINATION   STAGE   STATUS   VOLUMES   RESOURCES   CREATED   ELAPSED\n" +
		"getclonetest1   src      dest                           0/0       0                     \n"
	// Should get only one clone if name given
	cmdArgs = []string{"get", "clones", "getclonetest1"}
	testCommon(t, cmdArgs, nil, expected, false)

	_, err = core.Instance().CreateNamespace(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns1"}})
	require.NoError(t, err, "Error creating ns1 namespace")
	createApplicationCloneAndVerify(t, "getclonetest21", "ns1", "src", "dest", "", "")
	cmdArgs = []string{"get", "clones", "--all-namespaces"}
	expected = "NAMESPACE   NAME             SOURCE   DESTINATION   STAGE   STATUS   VOLUMES   RESOURCES   CREATED   ELAPSED\n" +
		"default     getclonetest1    src      dest                           0/0       0                     \n" +
		"default     getclonetest2    src      dest                           0/0       0                     \n" +
		"ns1         getclonetest21   src      dest                           0/0       0                     \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetApplicationClonesWithStatusAndProgress(t *testing.T) {
	defer resetTest()
	createApplicationCloneAndVerify(t, "getclonestatustest", "default", "src", "dest", "", "")
	clone, err := storkops.Instance().GetApplicationClone("getclonestatustest", "default")
	require.NoError(t, err, "Error getting clone")

	// Update the status of the clone
	clone.Status.FinishTimestamp = metav1.Now()
	clone.CreationTimestamp = metav1.NewTime(clone.Status.FinishTimestamp.Add(-5 * time.Minute))
	clone.Status.Stage = storkv1.ApplicationCloneStageFinal
	clone.Status.Status = storkv1.ApplicationCloneStatusSuccessful
	clone.Status.Volumes = []*storkv1.ApplicationCloneVolumeInfo{}
	_, err = storkops.Instance().UpdateApplicationClone(clone)
	require.NoError(t, err, "Error updating clone")

	expected := "NAME                 SOURCE   DESTINATION   STAGE   STATUS       VOLUMES   RESOURCES   CREATED               ELAPSED\n" +
		"getclonestatustest   src      dest          Final   Successful   0/0       0           " + toTimeString(clone.CreationTimestamp.Time) + "   5m0s\n"
	cmdArgs := []string{"get", "clones", "getclonestatustest"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreateApplicationClonesNoName(t *testing.T) {
	cmdArgs := []string{"create", "clones"}

	expected := "error: exactly one name needs to be provided for applicationclone name"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateApplicationClones(t *testing.T) {
	defer resetTest()
	createApplicationCloneAndVerify(t, "createclone", "default", "src", "dest", "", "")
}

func TestCreateDuplicateApplicationClones(t *testing.T) {
	defer resetTest()
	createApplicationCloneAndVerify(t, "createclone", "default", "src", "dest", "", "")
	cmdArgs := []string{"create", "clones", "createclone", "--sourceNamespace", "src", "--destinationNamespace", "dest"}

	expected := "Error from server (AlreadyExists): applicationclones.stork.libopenstorage.org \"createclone\" already exists"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestDeleteApplicationClonesNoApplicationCloneName(t *testing.T) {
	cmdArgs := []string{"delete", "clones"}

	var cloneList storkv1.ApplicationCloneList
	expected := "error: at least one argument needs to be provided for applicationclone name"
	testCommon(t, cmdArgs, &cloneList, expected, true)
}

func TestDeleteApplicationClones(t *testing.T) {
	defer resetTest()
	createApplicationCloneAndVerify(t, "deleteclone", "default", "src", "dest", "", "")

	cmdArgs := []string{"delete", "clones", "deleteclone"}
	expected := "ApplicationClone deleteclone deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"delete", "clones", "deleteclone"}
	expected = "Error from server (NotFound): applicationclones.stork.libopenstorage.org \"deleteclone\" not found"
	testCommon(t, cmdArgs, nil, expected, true)

	createApplicationCloneAndVerify(t, "deleteclone1", "default", "src", "dest", "", "")
	createApplicationCloneAndVerify(t, "deleteclone2", "default", "src", "dest", "", "")

	cmdArgs = []string{"delete", "clones", "deleteclone1", "deleteclone2"}
	expected = "ApplicationClone deleteclone1 deleted successfully\n"
	expected += "ApplicationClone deleteclone2 deleted successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	createApplicationCloneAndVerify(t, "deleteclone1", "default", "src", "dest", "", "")
	createApplicationCloneAndVerify(t, "deleteclone2", "default", "src", "dest", "", "")
}

func TestCreateApplicationCloneWaitSuccess(t *testing.T) {
	cloneStatusRetryInterval = 10 * time.Second
	defer resetTest()

	srcNamespace := "dummy-src-namespace"
	dstNamespace := "dummy-dst-namespace"
	name := "dummy-name"
	cmdArgs := []string{"create", "clones", "-n", srcNamespace, "--sourceNamespace", srcNamespace,
		"--destinationNamespace", dstNamespace, name, "--wait"}

	expected := "ApplicationClone dummy-name started successfully\n" +
		"STAGE\t\tSTATUS              \n" +
		"\t\t                    \n" +
		"Volumes\t\tSuccessful          \n" +
		"ApplicationClone dummy-name completed successfully\n"
	go setApplicationCloneStatus(name, srcNamespace, false, t)
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreateApplicationCloneWaitFailed(t *testing.T) {
	cloneStatusRetryInterval = 10 * time.Second
	defer resetTest()

	srcNamespace := "dummy-src-namespace"
	dstNamespace := "dummy-dst-namespace"
	name := "dummy-name"
	cmdArgs := []string{"create", "clones", "-n", srcNamespace, "--sourceNamespace", srcNamespace,
		"--destinationNamespace", dstNamespace, name, "--wait"}

	expected := "ApplicationClone dummy-name started successfully\n" +
		"STAGE\t\tSTATUS              \n" +
		"\t\t                    \n" +
		"Volumes\t\tFailed              \n" +
		"ApplicationClone dummy-name failed\n"
	go setApplicationCloneStatus(name, srcNamespace, true, t)
	testCommon(t, cmdArgs, nil, expected, false)
}

func setApplicationCloneStatus(name, namespace string, isFail bool, t *testing.T) {
	time.Sleep(10 * time.Second)
	clone, err := storkops.Instance().GetApplicationClone(name, namespace)
	require.NoError(t, err, "Error getting ApplicationClone details")
	require.Equal(t, clone.Status.Status, storkv1.ApplicationCloneStatusInitial)
	require.Equal(t, clone.Status.Stage, storkv1.ApplicationCloneStageInitial)
	clone.Status.Status = storkv1.ApplicationCloneStatusSuccessful
	clone.Status.Stage = storkv1.ApplicationCloneStageVolumes
	if isFail {
		clone.Status.Status = storkv1.ApplicationCloneStatusFailed
	}

	_, err = storkops.Instance().UpdateApplicationClone(clone)
	require.NoError(t, err, "Error updating ApplicationClones")
}
