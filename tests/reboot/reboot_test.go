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
	. "github.com/portworx/torpedo/tests"
)

func TestReboot(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_Reboot.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : Reboot", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
})

var _ = Describe("{RebootOneNode}", func() {
	var contexts []*scheduler.Context

	It("has to schedule apps and reboot node(s) with volumes", func() {
		var err error
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rebootonenode-%d", i))...)
		}

		ValidateApplications(contexts)

		Step("get all nodes and reboot one by one", func() {
			nodesToReboot := node.GetWorkerNodes()

			// Reboot node and check driver status
			Step(fmt.Sprintf("reboot node one at a time from the node(s): %v", nodesToReboot), func() {
				for _, n := range nodesToReboot {
					if n.IsStorageDriverInstalled {
						Step(fmt.Sprintf("reboot node: %s", n.Name), func() {
							err = Inst().N.RebootNode(n, node.RebootNodeOpts{
								Force: true,
								ConnectionOpts: node.ConnectionOpts{
									Timeout:         1 * time.Minute,
									TimeBeforeRetry: 5 * time.Second,
								},
							})
							Expect(err).NotTo(HaveOccurred())
						})

						Step(fmt.Sprintf("wait for node: %s to be back up", n.Name), func() {
							err = Inst().N.TestConnection(n, node.ConnectionOpts{
								Timeout:         15 * time.Minute,
								TimeBeforeRetry: 10 * time.Second,
							})
							Expect(err).NotTo(HaveOccurred())
						})

						Step(fmt.Sprintf("wait for volume driver to stop on node: %v", n.Name), func() {
							err := Inst().V.WaitDriverDownOnNode(n)
							Expect(err).NotTo(HaveOccurred())
						})

						Step(fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
							Inst().S.String(), Inst().V.String()), func() {

							err = Inst().S.IsNodeReady(n)
							Expect(err).NotTo(HaveOccurred())

							err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
							Expect(err).NotTo(HaveOccurred())
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
		AfterEachTest(contexts)
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
