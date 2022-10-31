package tests

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	. "github.com/portworx/torpedo/tests"
	// https://github.com/kubernetes/client-go/issues/242
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

var (
	upgradeTimeoutMins = 90 * time.Minute
)

var _ = Describe("{UpgradeScheduler}", func() {
	var testrailID = 58849
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/58849
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("UpgradeScheduler", "Validate scheduler upgrade", nil)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "upgrade scheduler and ensure everything is running fine"
	It(stepLog, func() {
		dash.Info(stepLog)
		contexts = make([]*scheduler.Context, 0)

		intitialNodeCount, err := Inst().N.GetASGClusterSize()
		Expect(err).NotTo(HaveOccurred())

		dash.Infof("Validating cluster size before upgrade. Initial Node Count: [%v]", intitialNodeCount)
		ValidateClusterSize(intitialNodeCount)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradescheduler-%d", i))...)
		}

		ValidateApplications(contexts)

		upgradeHops := strings.Split(Inst().SchedUpgradeHops, ",")
		dash.VerifyFatal(len(upgradeHops) > 0, true, "Validate upgrade hops are provided")

		for _, schedVersion := range upgradeHops {
			schedVersion = strings.TrimSpace(schedVersion)
			stepLog = fmt.Sprintf("start the upgrade of scheduler to version [%v]", schedVersion)
			Step(stepLog, func() {
				dash.Info(stepLog)
				err := Inst().N.SetClusterVersion(schedVersion, upgradeTimeoutMins)
				dash.VerifyFatal(err, nil, "Validate set cluster version")
			})

			stepLog = fmt.Sprintf("wait for %s minutes for auto recovery of storage nodes",
				Inst().AutoStorageNodeRecoveryTimeout.String())
			Step(stepLog, func() {
				dash.Info(fmt.Sprintf("wait for %s minutes for auto recovery of storage nodes",
					Inst().AutoStorageNodeRecoveryTimeout.String()))
				time.Sleep(Inst().AutoStorageNodeRecoveryTimeout)
			})

			err = Inst().S.RefreshNodeRegistry()
			dash.VerifyFatal(err, nil, "Validate node registry refresh")

			err = Inst().V.RefreshDriverEndpoints()
			dash.VerifyFatal(err, nil, "Validate driver end points refresh")
			stepLog = fmt.Sprintf("validate number of storage nodes after scheduler upgrade to [%s]",
				schedVersion)
			Step(stepLog, func() {
				dash.Info(stepLog)
				ValidateClusterSize(intitialNodeCount)
			})

			Step("validate all apps after upgrade", func() {
				for _, ctx := range contexts {
					ValidateContext(ctx)
				}
			})
			PerformSystemCheck()
		}
	})
	Step("teardown all apps", func() {
		for _, ctx := range contexts {
			TearDownContext(ctx, nil)
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})
