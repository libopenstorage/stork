//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"github.com/libopenstorage/stork/pkg/log"
	"testing"

	"github.com/portworx/torpedo/drivers/scheduler"
)

func testWebhook(t *testing.T) {
	var testResult = testResultFail
	err := setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	defer updateDashStats(t.Name(), &testResult)

	t.Run("simpleDryRunTest", simpleDryRunTest)
	err = setRemoteConfig("")
	log.FailOnError(t, err, "setting kubeconfig to default failed")
}

func simpleDryRunTest(t *testing.T) {
	ctx, err := schedulerDriver.Schedule(generateInstanceID(t, ""),
		scheduler.ScheduleOptions{AppKeys: []string{"mysql-2-pvc"}})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctx), 1, "Only one task should have started")
}
