//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"testing"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
)

func testWebhook(t *testing.T) {
	var testResult = testResultFail
	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	defer updateDashStats(t.Name(), &testResult)

	t.Run("simpleDryRunTest", simpleDryRunTest)
	err = setRemoteConfig("")
	require.NoError(t, err, "setting kubeconfig to default failed")
}

func simpleDryRunTest(t *testing.T) {
	ctx, err := schedulerDriver.Schedule(generateInstanceID(t, ""),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-2-pvc"}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctx), "Only one task should have started")
}
