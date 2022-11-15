package tests

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	. "github.com/portworx/torpedo/tests"
)

var _ = Describe("{StopScheduler}", func() {
	var testrailID = 35268
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35268
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("StopScheduler", "Validate stop scheduler and apps", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	testName := "stopscheduler"
	stepLog := "has to stop scheduler service and check if applications are fine"
	It(stepLog, func() {
		dash.Info(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", testName, i))...)
		}

		ValidateApplications(contexts)
		stepLog = "get nodes and induce scheduler service to stop on the node"
		Step(stepLog, func() {
			dash.Info(stepLog)
			for _, storageNode := range node.GetStorageDriverNodes() {
				stepLog = fmt.Sprintf("stop scheduler service on node %s", storageNode.Name)
				Step(stepLog, func() {
					dash.Infof(stepLog)
					err := Inst().S.StopSchedOnNode(storageNode)
					dash.VerifyFatal(err, nil, "validate scheduler is stopped")
				})

				stepLog = "wait for the service to stop and reschedule apps"
				Step(stepLog, func() {
					dash.Info(stepLog)
					time.Sleep(6 * time.Minute)
				})

				Step("validate apps", func() {
					for _, ctx := range contexts {
						ValidateContext(ctx)
					}
				})

				stepLog = fmt.Sprintf("start scheduler service on node %s", storageNode.Name)
				Step(stepLog, func() {
					dash.Info(stepLog)
					err := Inst().S.StartSchedOnNode(storageNode)
					dash.VerifyFatal(err, nil, "Validate start scheduler on the node")
				})

				Step("validate apps", func() {
					for _, ctx := range contexts {
						ValidateContext(ctx)
					}
				})

			}
		})

		ValidateAndDestroy(contexts, nil)
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})
