// +build unittest

package storkctl

import (
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/stretchr/testify/require"
)

func createApplicationRegistrationAndVerify(
	t *testing.T,
	name string,
	kind string,
	crdName string,
	version string,
	keepStatus bool,
	path string,
	pathType string,
) {
	res := storkv1.ApplicationResource{
		KeepStatus: keepStatus,
		SuspendOptions: storkv1.SuspendOptions{
			Path: path,
			Type: pathType,
		},
	}
	res.Kind = kind
	res.Group = crdName
	res.Version = version
	appReg := &storkv1.ApplicationRegistration{Resources: []storkv1.ApplicationResource{res}}

	appReg.Name = name
	// Make sure it was created correctly
	resp, err := storkops.Instance().CreateApplicationRegistration(appReg)
	require.NoError(t, err, "Error getting clone")
	require.NotNil(t, resp)
}
func TestGetApplicationRegistrations(t *testing.T) {

	createApplicationRegistrationAndVerify(t, "cassandra-app-reg", "CassandraDatacenter",
		"cassandradatacenters.cassandra.datastax.com", "", false, "spec.stopped", "bool")

	expected := "NAME                KIND                  CRD_NAME                                      SUSPEND_OPTIONS     KEEP_STATUS\n" +
		"cassandra-app-reg   CassandraDatacenter   cassandradatacenters.cassandra.datastax.com   spec.stopped,bool   false\n"
	cmdArgs := []string{"get", "appreg", "cassandra-app-reg"}
	testCommon(t, cmdArgs, nil, expected, false)

	createApplicationRegistrationAndVerify(t, "test-app-reg1", "TestCustomCRD1",
		"CRD_NAME1", "Version", true, "spec.path", "spec.path.type")
	createApplicationRegistrationAndVerify(t, "test-app-reg2", "TestCustomCRD2",
		"CRD_NAME2", "Version", true, "spec.path", "spec.path.type")
	createApplicationRegistrationAndVerify(t, "test-app-reg3", "TestCustomCRD3",
		"CRD_NAME3", "Version", true, "spec.path", "spec.path.type")

	expected = "NAME                KIND                  CRD_NAME                                      SUSPEND_OPTIONS            KEEP_STATUS\n" +
		"cassandra-app-reg   CassandraDatacenter   cassandradatacenters.cassandra.datastax.com   spec.stopped,bool          false\n" +
		"test-app-reg1       TestCustomCRD1        CRD_NAME1                                     spec.path,spec.path.type   true\n" +
		"test-app-reg2       TestCustomCRD2        CRD_NAME2                                     spec.path,spec.path.type   true\n" +
		"test-app-reg3       TestCustomCRD3        CRD_NAME3                                     spec.path,spec.path.type   true\n"
	cmdArgs = []string{"get", "appreg"}
	testCommon(t, cmdArgs, nil, expected, false)

}
