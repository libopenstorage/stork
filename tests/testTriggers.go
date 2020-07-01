package tests

import (
	"fmt"
	"time"

	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
)

// TriggerCrashVolDriver crashes vol driver
func TriggerCrashVolDriver(contexts []*scheduler.Context) {
	Step("crash volume driver in all nodes", func() {
		for _, appNode := range node.GetStorageDriverNodes() {
			Step(
				fmt.Sprintf("crash volume driver %s on node: %v",
					Inst().V.String(), appNode.Name),
				func() {
					CrashVolDriverAndWait([]node.Node{appNode})
				})
		}
	})
}

// TriggerRestartVolDriver restarts volume driver and validates app
func TriggerRestartVolDriver(contexts []*scheduler.Context) {
	Step("get nodes bounce volume driver", func() {
		for _, appNode := range node.GetStorageDriverNodes() {
			Step(
				fmt.Sprintf("stop volume driver %s on node: %s",
					Inst().V.String(), appNode.Name),
				func() {
					StopVolDriverAndWait([]node.Node{appNode})
				})

			Step(
				fmt.Sprintf("starting volume %s driver on node %s",
					Inst().V.String(), appNode.Name),
				func() {
					StartVolDriverAndWait([]node.Node{appNode})
				})

			Step("Giving few seconds for volume driver to stabilize", func() {
				time.Sleep(20 * time.Second)
			})

			Step("validate apps", func() {
				for _, ctx := range contexts {
					ValidateContext(ctx)
				}
			})
		}
	})
}

// TriggerDeleteApps deletes app and verifies if those are rescheduled properly
func TriggerDeleteApps(contexts []*scheduler.Context) {
	Step("delete all application tasks", func() {
		for _, ctx := range contexts {
			Step(fmt.Sprintf("delete tasks for app: %s", ctx.App.Key), func() {
				err := Inst().S.DeleteTasks(ctx, nil)
				expect(err).NotTo(haveOccurred())
			})
			ValidateContext(ctx)
		}
	})
}

// TriggerRebootNodes reboots node on which apps are running
func TriggerRebootNodes(contexts []*scheduler.Context) {
	Step("get all nodes and reboot one by one", func() {
		nodesToReboot := node.GetWorkerNodes()

		// Reboot node and check driver status
		Step(fmt.Sprintf("reboot node one at a time from the node(s): %v", nodesToReboot), func() {
			// TODO: Below is the same code from existing nodeReboot test
			for _, n := range nodesToReboot {
				if n.IsStorageDriverInstalled {
					Step(fmt.Sprintf("reboot node: %s", n.Name), func() {
						err := Inst().N.RebootNode(n, node.RebootNodeOpts{
							Force: true,
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         1 * time.Minute,
								TimeBeforeRetry: 5 * time.Second,
							},
						})
						expect(err).NotTo(haveOccurred())
					})

					Step(fmt.Sprintf("wait for node: %s to be back up", n.Name), func() {
						err := Inst().N.TestConnection(n, node.ConnectionOpts{
							Timeout:         15 * time.Minute,
							TimeBeforeRetry: 10 * time.Second,
						})
						expect(err).NotTo(haveOccurred())
					})

					Step(fmt.Sprintf("wait for volume driver to stop on node: %v", n.Name), func() {
						err := Inst().V.WaitDriverDownOnNode(n)
						expect(err).NotTo(haveOccurred())
					})

					Step(fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
						Inst().S.String(), Inst().V.String()), func() {

						err := Inst().S.IsNodeReady(n)
						expect(err).NotTo(haveOccurred())

						err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
						expect(err).NotTo(haveOccurred())
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
}
