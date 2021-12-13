package tests

import (
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	. "github.com/portworx/torpedo/tests"
)

func TestStopScheduler(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_StopScheduler.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : StopScheduler", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
})

var _ = Describe("{StopScheduler}", func() {
	var testrailID = 35268
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35268
	var runID int
	JustBeforeEach(func() {
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	testName := "stopscheduler"
	It("has to stop scheduler service and check if applications are fine", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", testName, i))...)
		}

		ValidateApplications(contexts)

		Step("get nodes and induce scheduler service to stop on the node", func() {
			for _, storageNode := range node.GetStorageDriverNodes() {

				Step(fmt.Sprintf("stop scheduler service on node %s", storageNode.Name), func() {
					err := Inst().S.StopSchedOnNode(storageNode)
					Expect(err).NotTo(HaveOccurred())
				})

				Step("wait for the service to stop and reschedule apps", func() {
					time.Sleep(6 * time.Minute)
				})

				Step("validate apps", func() {
					for _, ctx := range contexts {
						ValidateContext(ctx)
					}
				})

				Step(fmt.Sprintf("start scheduler service on node %s", storageNode.Name), func() {
					err := Inst().S.StartSchedOnNode(storageNode)
					Expect(err).NotTo(HaveOccurred())
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
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
