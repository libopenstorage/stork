package tests

import (
	"fmt"

	"github.com/portworx/torpedo/pkg/testrailuttils"

	. "github.com/onsi/ginkgo"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

var _ = Describe("{CrashOneNode}", func() {
	var testrailID = 35255
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35255
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("CrashOneNode", "Validate Crash one node", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context

	stepLog := "has to schedule apps and crash node(s) with volumes"
	It(stepLog, func() {
		dash.Info(stepLog)
		var err error
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("crashonenode-%d", i))...)
		}

		ValidateApplications(contexts)
		stepLog = "get all nodes and crash one by one"
		Step(stepLog, func() {
			dash.Info(stepLog)
			nodesToCrash := node.GetWorkerNodes()

			// Crash node and check driver status
			stepLog = fmt.Sprintf("crash node one at a time from the node(s): %v", nodesToCrash)
			Step(stepLog, func() {
				dash.Info(fmt.Sprintf("crash node one at a time from the node(s): %v", nodesToCrash))
				for _, n := range nodesToCrash {
					if n.IsStorageDriverInstalled {
						stepLog = fmt.Sprintf("crash node: %s", n.Name)
						Step(stepLog, func() {
							dash.Info(stepLog)
							err = Inst().N.CrashNode(n, node.CrashNodeOpts{
								Force: true,
								ConnectionOpts: node.ConnectionOpts{
									Timeout:         defaultCommandTimeout,
									TimeBeforeRetry: defaultCommandRetry,
								},
							})
							dash.VerifySafely(err, nil, "Validate node is crashed")

						})

						stepLog = fmt.Sprintf("wait for node: %s to be back up", n.Name)
						Step(stepLog, func() {
							dash.Info(stepLog)
							err = Inst().N.TestConnection(n, node.ConnectionOpts{
								Timeout:         defaultTestConnectionTimeout,
								TimeBeforeRetry: defaultWaitRebootRetry,
							})
							dash.VerifyFatal(err, nil, "Validate node is back up")
						})

						stepLog = fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
							Inst().S.String(), Inst().V.String())
						Step(stepLog, func() {
							dash.Info(stepLog)
							err = Inst().S.IsNodeReady(n)
							dash.VerifyFatal(err, nil, "Validate node is ready")
							err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
							dash.VerifyFatal(err, nil, "Validate volume is driver up")
						})

						Step("validate apps", func() {
							for _, ctx := range contexts {
								ValidateContext(ctx)
							}
						})
					}
				}
			})
		})

		Step("destroy apps", func() {
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
			for _, ctx := range contexts {
				TearDownContext(ctx, opts)
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})
