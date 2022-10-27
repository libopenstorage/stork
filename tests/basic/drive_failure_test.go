package tests

import (
	"fmt"
	"time"

	"github.com/portworx/torpedo/pkg/testrailuttils"

	. "github.com/onsi/ginkgo"

	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

const (
	dfDefaultTimeout       = 1 * time.Minute
	driveFailTimeout       = 2 * time.Minute
	dfDefaultRetryInterval = 5 * time.Second
)

var _ = Describe("{DriveFailure}", func() {
	var testrailID = 35265
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35265
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("DriveFailure", "Validate PX after drive failure", nil)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	testName := "drivefailure"
	stepLog := "has to schedule apps and induce a drive failure on one of the nodes"
	It(stepLog, func() {
		dash.Info(stepLog)
		var err error
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", testName, i))...)
		}

		ValidateApplications(contexts)

		stepLog = "get nodes for all apps in test and induce drive failure on one of the nodes"

		Step(stepLog, func() {
			dash.Infof(stepLog)
			for _, ctx := range contexts {
				var (
					drives        []string
					appNodes      []node.Node
					nodeWithDrive node.Node
				)

				stepLog = fmt.Sprintf("get nodes where %s app is running", ctx.App.Key)
				Step(stepLog, func() {
					dash.Infof(stepLog)
					appNodes, err = Inst().S.GetNodesForApp(ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verify Get nodes for the app %s", ctx.App.Key))
					dash.VerifyFatal(len(appNodes) > 0, true, fmt.Sprintf("Verify apps length %d", len(appNodes)))

					nodeWithDrive = appNodes[0]
				})

				stepLog = fmt.Sprintf("get drive from node %v", nodeWithDrive)
				Step(stepLog, func() {
					dash.Infof(stepLog)
					drives, err = Inst().V.GetStorageDevices(nodeWithDrive)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verify Get storage devices for the node %s", nodeWithDrive.Name))
					dash.VerifyFatal(len(drives) > 0, true, fmt.Sprintf("Verify drives length %d", len(appNodes)))
				})

				busInfoMap := make(map[string]string)
				stepLog := fmt.Sprintf("induce a failure on all drives on the node %v", nodeWithDrive)
				Step(stepLog, func() {
					dash.Infof(stepLog)
					for _, driveToFail := range drives {
						busID, err := Inst().N.YankDrive(nodeWithDrive, driveToFail, node.ConnectionOpts{
							Timeout:         dfDefaultTimeout,
							TimeBeforeRetry: dfDefaultRetryInterval,
						})
						busInfoMap[driveToFail] = busID
						dash.VerifyFatal(err, nil, fmt.Sprintf("Verify yank drive %s", driveToFail))

					}
					stepLog = "wait for the drives to fail"
					Step(stepLog, func() {
						dash.Info(stepLog)
						time.Sleep(30 * time.Second)
					})

					Step(fmt.Sprintf("check if apps are running"), func() {
						ValidateContext(ctx)
					})

				})

				stepLog = "recover all drives and the storage driver"
				Step(stepLog, func() {
					dash.Infof(stepLog)
					for _, driveToFail := range drives {
						err = Inst().N.RecoverDrive(nodeWithDrive, driveToFail, busInfoMap[driveToFail], node.ConnectionOpts{
							Timeout:         driveFailTimeout,
							TimeBeforeRetry: dfDefaultRetryInterval,
						})
						dash.VerifyFatal(err, nil, fmt.Sprintf("Verify drive %s recovery init", driveToFail))

					}
					stepLog = "wait for the drives to recover"
					Step(stepLog, func() {
						dash.Info(stepLog)
						time.Sleep(30 * time.Second)
					})

					err = Inst().V.RecoverDriver(nodeWithDrive)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verify drive recovery in node %s", nodeWithDrive.Name))

				})

				stepLog = "check if volume driver is up"
				Step(stepLog, func() {
					err = Inst().V.WaitDriverUpOnNode(nodeWithDrive, Inst().DriverStartTimeout)
					dash.VerifyFatal(err, nil, "Validate volume driver is up")
				})
			}
		})

		ValidateAndDestroy(contexts, nil)
	})
	JustAfterEach(func() {
		EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})
