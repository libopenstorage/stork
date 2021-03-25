// +build integrationtest

package integrationtest

import (
	"testing"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func testWebhook(t *testing.T) {
	err := setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)

	t.Run("simpleDryRunTest", simpleDryRunTest)
	err = setRemoteConfig("")
	require.NoError(t, err, "setting kubeconfig to default failed")
}

func simpleDryRunTest(t *testing.T) {
	ctx, err := schedulerDriver.Schedule(generateInstanceID(t, ""),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-2-pvc"}, DryRun: []string{metav1.DryRunAll}})
	require.NoError(t, err, "Error scheduling task")
	require.Equal(t, 1, len(ctx), "Only one task should have started")
}
