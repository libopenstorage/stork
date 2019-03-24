// +build unittest

package storkctl

import (
	"fmt"
	"strings"
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/stretchr/testify/require"
)

func TestOneGroupSnapshot(t *testing.T) {
	defer resetTest()
	name := "test-group-snap"
	namespace := "test"
	selectors := map[string]string{"app": "mysql"}
	preRuleName := "mysql-pre-snap"
	postRuleName := "mysql-post-snap"
	restoreNamespaces := []string{namespace, "prod"}

	createGroupSnapshotAndVerify(t, name, namespace,
		selectors, preRuleName, postRuleName, restoreNamespaces, nil, 99)

	expected := fmt.Sprintf("NAME              STATUS    STAGE     SNAPSHOTS   CREATED\n"+
		"%s                       0           \n", name)
	cmdArgs := []string{"get", "groupsnapshots", "-n", namespace, name}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGroupSnapshotWithStatus(t *testing.T) {
	defer resetTest()

	name := "test-group-snap-status"
	namespace := "default"
	selectors := map[string]string{"app": "mysql"}

	createGroupSnapshotAndVerify(t, name, namespace, selectors, "", "", nil, nil, 0)

	groupSnap, err := k8s.Instance().GetGroupSnapshot(name, namespace)
	require.NoError(t, err, "failed to get group snapshot")
	require.NotNil(t, groupSnap, "got nil group snapshot after get call")

	groupSnap.Status.Status = storkv1.GroupSnapshotSuccessful
	groupSnap.Status.Stage = storkv1.GroupSnapshotStageFinal
	groupSnap.Status.VolumeSnapshots = []*storkv1.VolumeSnapshotStatus{
		{
			VolumeSnapshotName: fmt.Sprintf("%s-child-1", name),
			TaskID:             "123",
			ParentVolumeID:     "mysql-data-1",
			// rest of fields not relevant for unit test
		},
		{
			VolumeSnapshotName: fmt.Sprintf("%s-child-2", name),
			TaskID:             "456",
			ParentVolumeID:     "mysql-data-2",
			// rest of fields not relevant for unit test
		},
	}

	_, err = k8s.Instance().UpdateGroupSnapshot(groupSnap)
	require.NoError(t, err, "failed to update group snapshot")

	expected := fmt.Sprintf("NAME                     STATUS       STAGE     SNAPSHOTS   CREATED\n"+
		"%s   Successful   Final     2           \n", name)
	cmdArgs := []string{"get", "groupsnapshots", "-n", namespace, name}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGroupSnapshotWithNoSelector(t *testing.T) {
	name := "test-group-snap-no-selector"
	expected := "error: PVC label selectors must be provided"
	cmdArgs := []string{"create", "groupsnapshots", name}
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestGroupSnapshotWithNoName(t *testing.T) {
	expected := "error: Exactly one name needs to be provided for groupsnapshot name"
	cmdArgs := []string{"create", "groupsnapshots"}
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestDuplicateGroupSnapshots(t *testing.T) {
	defer resetTest()

	name := "test-group-snap-duplicate"
	namespace := "default"
	selectors, err := parseKeyValueList([]string{"app=mysql"})
	require.NoError(t, err, "failed to parse selectors")

	createGroupSnapshotAndVerify(t, name, namespace, selectors, "", "", nil, nil, 0)

	// create another with the same name. should fail
	cmdArgs := []string{"create", "groupsnapshots", "-n", namespace, "--pvcSelectors", "app=mysql", name}
	expected := fmt.Sprintf("Error from server (AlreadyExists): groupvolumesnapshots.stork.libopenstorage.org \"%s\" already exists", name)
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestMultipleGroupSnapshots(t *testing.T) {
	defer resetTest()

	name1 := "test-group-snap-1"
	name2 := "test-group-snap-2"
	namespace := "default"
	selectors := map[string]string{"app": "mysql"}

	_, err := k8s.Instance().CreateNamespace(namespace, nil)
	require.NoError(t, err, "Error creating namespace")

	createGroupSnapshotAndVerify(t, name1, namespace, selectors, "", "", nil, nil, 0)
	createGroupSnapshotAndVerify(t, name2, namespace, selectors, "", "", nil, nil, 0)

	expected := fmt.Sprintf("NAME                STATUS    STAGE     SNAPSHOTS   CREATED\n"+
		"%s                       0           \n%s                       0           \n",
		name1, name2)
	cmdArgs := []string{"get", "groupsnapshots", "-n", namespace, name1, name2}
	testCommon(t, cmdArgs, nil, expected, false)

	// Should get all group snapshots if no name given
	cmdArgs = []string{"get", "groupsnapshots"}
	testCommon(t, cmdArgs, nil, expected, false)

	name3 := "test-group-snap-3"
	customNamespace := "ns1"
	_, err = k8s.Instance().CreateNamespace(customNamespace, nil)
	require.NoError(t, err, "Error creating namespace")

	createGroupSnapshotAndVerify(t, name3, customNamespace, selectors, "", "", nil, nil, 0)

	// get from all namespaces
	expected = fmt.Sprintf("NAMESPACE   NAME                STATUS    STAGE     SNAPSHOTS   CREATED\n"+
		"%s     %s                       0           \n%s     %s                       0           \n"+
		"%s         %s                       0           \n",
		namespace, name1, namespace, name2, customNamespace, name3)
	cmdArgs = []string{"get", "groupsnapshots", "--all-namespaces"}
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDeleteGroupSnapshots(t *testing.T) {
	defer resetTest()

	name := "test-group-snap-delete"
	namespace := "default"
	selectors := map[string]string{"app": "mysql"}
	createGroupSnapshotAndVerify(t, name, namespace, selectors, "", "", nil, nil, 0)

	cmdArgs := []string{"delete", "groupsnapshots", name}
	expected := fmt.Sprintf("GroupVolumeSnapshot %s deleted successfully\n", name)
	testCommon(t, cmdArgs, nil, expected, false)

	// delete again. should fail
	cmdArgs = []string{"delete", "groupsnapshots", name}
	expected = fmt.Sprintf("Error from server (NotFound): groupvolumesnapshots.stork.libopenstorage.org \"%s\" not found", name)
	testCommon(t, cmdArgs, nil, expected, true)

	// delete multiple
	name1 := "test-group-snap-delete-1"
	name2 := "test-group-snap-delete-2"
	createGroupSnapshotAndVerify(t, name1, namespace, selectors, "", "", nil, nil, 0)
	createGroupSnapshotAndVerify(t, name2, namespace, selectors, "", "", nil, nil, 0)
	cmdArgs = []string{"delete", "groupsnapshots", name1, name2}
	expected = fmt.Sprintf("GroupVolumeSnapshot %s deleted successfully\n", name1)
	expected += fmt.Sprintf("GroupVolumeSnapshot %s deleted successfully\n", name2)
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestDeleteNoName(t *testing.T) {
	cmdArgs := []string{"delete", "groupsnapshots"}
	expected := "error: At least one argument needs to be provided for groupsnapshot name"
	testCommon(t, cmdArgs, nil, expected, true)
}

func createGroupSnapshotAndVerify(
	t *testing.T,
	name string,
	namespace string,
	pvcSelectors map[string]string,
	preExecRule string,
	postExecRule string,
	restoreNamespaces []string,
	opts map[string]string,
	maxRetries int,
) {
	selectorList := make([]string, 0)
	for k, v := range pvcSelectors {
		selectorList = append(selectorList, fmt.Sprintf("%s=%s", k, v))
	}

	cmdArgs := []string{"create", "groupsnapshots", "-n", namespace, "--pvcSelectors", strings.Join(selectorList, ","), name}

	if preExecRule != "" {
		cmdArgs = append(cmdArgs, "--preExecRule", preExecRule)
	}
	if postExecRule != "" {
		cmdArgs = append(cmdArgs, "--postExecRule", postExecRule)
	}

	if len(restoreNamespaces) > 0 {
		cmdArgs = append(cmdArgs, "--restoreNamespaces", strings.Join(restoreNamespaces, ","))
	}

	if len(opts) > 0 {
		optsList := make([]string, 0)
		for k, v := range opts {
			optsList = append(optsList, fmt.Sprintf("%s=%s", k, v))
		}
		cmdArgs = append(cmdArgs, "--opts", strings.Join(optsList, ","))
	}

	if maxRetries > 0 {
		cmdArgs = append(cmdArgs, "--maxRetries", fmt.Sprintf("%d", maxRetries))
	}

	expected := "GroupVolumeSnapshot " + name + " created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)

	// Make sure it's created correctly
	groupSnap, err := k8s.Instance().GetGroupSnapshot(name, namespace)
	require.NoError(t, err, "failed to get group snapshot")
	require.NotNil(t, groupSnap, "got nil group snapshot after get call")

	require.Equal(t, name, groupSnap.Name, "name mismatch")
	require.Equal(t, namespace, groupSnap.Namespace, "namespace mismatch")
	require.Equal(t, pvcSelectors, groupSnap.Spec.PVCSelector.MatchLabels, "selectors mismatch")
	require.Equal(t, preExecRule, groupSnap.Spec.PreExecRule, "preRuleName mismatch")
	require.Equal(t, postExecRule, groupSnap.Spec.PostExecRule, "postRuleName mismatch")
	require.Equal(t, restoreNamespaces, groupSnap.Spec.RestoreNamespaces, "restoreNamespaces mismatch")
	require.Equal(t, maxRetries, groupSnap.Spec.MaxRetries, "maxRetries mismatch")
	require.Equal(t, opts, groupSnap.Spec.Options, "restoreNamespaces mismatch")
}
