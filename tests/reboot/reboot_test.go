package tests

import (
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	. "github.com/portworx/torpedo/tests"
)

func TestReboot(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Torpedo : Reboot")
}

var _ = BeforeSuite(func() {
	InitInstance()
})

func rebootNodesTest(testName string, allNodes bool) {
	It("has to scheduler apps and reboot app node(s)", func() {
		var err error
		contexts := ScheduleAndValidate(testName)

		Step("get nodes for all apps in test and reboot their nodes", func() {
			for _, ctx := range contexts {
				var appNodes []node.Node
				var nodesToReboot []node.Node

				Step(fmt.Sprintf("get nodes for %s app", ctx.App.Key), func() {
					appNodes, err = Inst().S.GetNodesForApp(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(appNodes).NotTo(BeEmpty())
				})

				Step(fmt.Sprintf("find node(s) to reboot for %s app", ctx.App.Key), func() {
					if allNodes {
						nodesToReboot = appNodes
					} else {
						nodesToReboot = append(nodesToReboot, appNodes[0])
					}
				})

				Step(fmt.Sprintf("reboot app %s's node(s): %v", ctx.App.Key, nodesToReboot), func() {
					for _, n := range nodesToReboot {
						err = Inst().N.RebootNode(n, node.RebootNodeOpts{
							Force: false,
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

							err = Inst().V.WaitDriverUpOnNode(n)
							Expect(err).NotTo(HaveOccurred())
						})
					}
				})
			}
		})

		Step("validate and destroy apps", func() {
			for _, ctx := range contexts {
				ValidateAndDestroy(ctx, nil)
			}
		})

	})
}

var _ = Describe("Reboot one node test", func() {
	rebootNodesTest("rebootonenode", false)
})

var _ = Describe("Reboot all nodes test", func() {
	rebootNodesTest("rebootallnodes", false)
})

var _ = AfterSuite(func() {
	CollectSupport()
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
