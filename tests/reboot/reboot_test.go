package tests

import (
	"fmt"
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

		Step("get nodes for all apps in test and reboot their nodes", func() {
			nodesToReboot := node.GetWorkerNodes()

			// Reboot node and check driver status
			Step(fmt.Sprintf("reboot node one at a time from the node(s): %v", nodesToReboot), func() {
				for _, n := range nodesToReboot {
					err = Inst().N.RebootNode(n, node.RebootNodeOpts{
						Force: true,
						ConnectionOpts: node.ConnectionOpts{
							Timeout:         1 * time.Minute,
							TimeBeforeRetry: 5 * time.Second,
						},
					})
					Expect(err).NotTo(HaveOccurred())

					Step("wait for node to go down", func() {
						time.Sleep(20 * time.Second)
					})

					Step("wait for node to be back up", func() {
						err = Inst().N.TestConnection(n, node.ConnectionOpts{
							Timeout:         15 * time.Minute,
							TimeBeforeRetry: 10 * time.Second,
						})
						Expect(err).NotTo(HaveOccurred())
					})

					Step(fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
						Inst().S.String(), Inst().V.String()), func() {

						err = Inst().S.IsNodeReady(n)
						Expect(err).NotTo(HaveOccurred())

						err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
						Expect(err).NotTo(HaveOccurred())
					})
				}
			})
		})

		ValidateAndDestroy(contexts, nil)
	})
	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

var _ = Describe("{RebootAllNodes}", func() {
	var contexts []*scheduler.Context

	It("has to scheduler apps and reboot app node(s)", func() {
		var err error
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rebootallnodes-%d", i))...)
		}

		ValidateApplications(contexts)

		Step("get nodes for all apps in test and reboot their nodes", func() {
			for _, ctx := range contexts {
				var nodesToReboot []node.Node

				Step(fmt.Sprintf("get nodes for %s app", ctx.App.Key), func() {
					nodesToReboot, err = Inst().S.GetNodesForApp(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(nodesToReboot).NotTo(BeEmpty())
				})

				Step(fmt.Sprintf("reboot app %s's node(s): %v", ctx.App.Key, nodesToReboot), func() {
					for _, n := range nodesToReboot {
						err = Inst().N.RebootNode(n, node.RebootNodeOpts{
							Force: true,
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         1 * time.Minute,
								TimeBeforeRetry: 5 * time.Second,
							},
						})
						Expect(err).NotTo(HaveOccurred())

						Step("wait for node to go down", func() {
							time.Sleep(20 * time.Second)
						})

						Step("wait for node to be back up", func() {
							err = Inst().N.TestConnection(n, node.ConnectionOpts{
								Timeout:         15 * time.Minute,
								TimeBeforeRetry: 10 * time.Second,
							})
							Expect(err).NotTo(HaveOccurred())
						})

						Step(fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
							Inst().S.String(), Inst().V.String()), func() {

							err = Inst().S.IsNodeReady(n)
							Expect(err).NotTo(HaveOccurred())

							err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
							Expect(err).NotTo(HaveOccurred())
						})
					}
				})
			}
		})

		ValidateAndDestroy(contexts, nil)
	})
	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
