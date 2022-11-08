package tests

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	. "github.com/portworx/torpedo/tests"
)

const (
	defaultWaitRebootTimeout     = 5 * time.Minute
	defaultWaitRebootRetry       = 10 * time.Second
	defaultCommandRetry          = 5 * time.Second
	defaultCommandTimeout        = 1 * time.Minute
	defaultTestConnectionTimeout = 15 * time.Minute
	defaultRebootTimeRange       = 5 * time.Minute
)

var _ = Describe("{RebootOneNode}", func() {
	var testrailID = 35266
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35266
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("RebootOneNode", "Reboot one storage node at a time and validate apps and px", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	It("has to schedule apps and reboot node(s) with volumes", func() {
		dash.Info("has to schedule apps and reboot node(s) with volumes")
		var err error
		contexts = make([]*scheduler.Context, 0)

		dash.Info("Scheduling Applications")

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rebootonenode-%d", i))...)
		}

		ValidateApplications(contexts)

		Step("get all nodes and reboot one by one", func() {
			dash.Info("get all nodes and reboot one by one")
			nodesToReboot := node.GetWorkerNodes()

			// Reboot node and check driver status
			Step(fmt.Sprintf("reboot node one at a time from the node(s): %v", nodesToReboot), func() {
				dash.Infof("reboot node one at a time from the node(s): %v", nodesToReboot)
				for _, n := range nodesToReboot {
					if n.IsStorageDriverInstalled {
						Step(fmt.Sprintf("reboot node: %s", n.Name), func() {
							dash.Infof("reboot node: %s", n.Name)
							err = Inst().N.RebootNode(n, node.RebootNodeOpts{
								Force: true,
								ConnectionOpts: node.ConnectionOpts{
									Timeout:         defaultCommandTimeout,
									TimeBeforeRetry: defaultCommandRetry,
								},
							})
							dash.VerifyFatal(err, nil, fmt.Sprintf("Reboot node %s. Err: %v", n.Name, err))
						})

						Step(fmt.Sprintf("wait for node: %s to be back up", n.Name), func() {
							dash.Infof("wait for node: %s to be back up", n.Name)
							err = Inst().N.TestConnection(n, node.ConnectionOpts{
								Timeout:         defaultTestConnectionTimeout,
								TimeBeforeRetry: defaultWaitRebootRetry,
							})
							dash.VerifyFatal(err, nil, fmt.Sprintf("node %s is up. Err: %v", n.Name, err))
						})

						Step(fmt.Sprintf("Check if node: %s rebooted in last 3 minutes", n.Name), func() {
							dash.Infof("Check if node: %s rebooted in last 3 minutes", n.Name)
							isNodeRebootedAndUp, err := Inst().N.IsNodeRebootedInGivenTimeRange(n, defaultRebootTimeRange)
							dash.VerifyFatal(err, nil, fmt.Sprintf("check for node: %s rebooted in last 3 minutes. Err: %v", n.Name, err))
							if !isNodeRebootedAndUp {
								Step(fmt.Sprintf("wait for volume driver to stop on node: %v", n.Name), func() {
									dash.Infof("wait for volume driver to stop on node: %v", n.Name)
									err := Inst().V.WaitDriverDownOnNode(n)
									dash.VerifyFatal(err, nil, fmt.Sprintf("node %s is PX stopped. Err: %v", n.Name, err))
								})
							}
						})

						Step(fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
							Inst().S.String(), Inst().V.String()), func() {
							dash.Infof("wait to scheduler: %s and volume driver: %s to start",
								Inst().S.String(), Inst().V.String())

							err = Inst().S.IsNodeReady(n)
							dash.VerifyFatal(err, nil, fmt.Sprintf("node %s is ready. Err: %v", n.Name, err))
							err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
							dash.VerifyFatal(err, nil, fmt.Sprintf("node %s volume driver is up. Err: %v", n.Name, err))

						})

						Step(fmt.Sprintf("validate apps"), func() {
							dash.Info("Validate Apps")
							for _, ctx := range contexts {
								ValidateContext(ctx)
							}
						})
					}
				}
			})
		})

		Step(fmt.Sprintf("Destroying apps"), func() {
			dash.Info("Destroying Apps")
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

var _ = Describe("{ReallocateSharedMount}", func() {

	var testrailID = 58844
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/58844
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("ReallocateSharedMount", "Validating Px and apps after reallocating shared mounts", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	It("has to schedule apps and reboot node(s) with shared volume mounts", func() {
		dash.Info("has to schedule apps and reboot node(s) with shared volume mounts")

		//var err error
		contexts = make([]*scheduler.Context, 0)
		dash.Info("Scheduling Applications")

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("reallocate-mount-%d", i))...)
		}

		dash.Info("Validating Applications")

		ValidateApplications(contexts)

		Step(fmt.Sprintf("get nodes with shared mount and reboot them"), func() {
			dash.Infof("get nodes with shared mount and reboot them")
			for _, ctx := range contexts {
				vols, err := Inst().S.GetVolumes(ctx)
				Expect(err).NotTo(HaveOccurred())
				for _, vol := range vols {
					if vol.Shared {

						n, err := Inst().V.GetNodeForVolume(vol, defaultCommandTimeout, defaultCommandRetry)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Get node for volume: %s", vol.ID))

						dash.Infof("volume %s is attached on node %s [%s]", vol.ID, n.SchedulerNodeName, n.Addresses[0])

						// Workaround to avoid PWX-24277 for now.
						Step(fmt.Sprintf("wait until volume %v status is Up", vol.ID), func() {
							dash.Infof("wait until volume %v status is Up", vol.ID)
							prevStatus := ""
							Eventually(func() (string, error) {
								connOpts := node.ConnectionOpts{
									Timeout:         defaultCommandTimeout,
									TimeBeforeRetry: defaultCommandRetry,
									Sudo:            true,
								}
								cmd := fmt.Sprintf("pxctl volume inspect %s | grep \"Replication Status\"", vol.ID)
								volStatus, err := Inst().N.RunCommandWithNoRetry(*n, cmd, connOpts)
								if err != nil {
									dash.Warnf("failed to get replication state of volume %v: %v", vol.ID, err)
									return "", err
								}
								if volStatus != prevStatus {
									dash.Infof("volume %v: %v", vol.ID, volStatus)
									prevStatus = volStatus
								}
								return volStatus, nil
							}, 30*time.Minute, 10*time.Second).Should(ContainSubstring("Up"),
								"volume %v status is not Up for app %v", vol.ID, ctx.App.Key)
						})

						err = Inst().S.DisableSchedulingOnNode(*n)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Disable sceduling on node : %s", n.Name))

						err = Inst().V.StopDriver([]node.Node{*n}, false, nil)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Stop volume driver on node : %s", n.Name))

						err = Inst().N.RebootNode(*n, node.RebootNodeOpts{
							Force: true,
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         defaultCommandTimeout,
								TimeBeforeRetry: defaultCommandRetry,
							},
						})
						dash.VerifyFatal(err, nil, fmt.Sprintf("Rebooting node : %s", n.Name))

						// as we keep the storage driver down on node until we check if the volume, we wait a minute for
						// reboot to occur then we force driver to refresh endpoint to pick another storage node which is up
						dash.Infof("wait for %v for node reboot", defaultCommandTimeout)
						time.Sleep(defaultCommandTimeout)

						// Start NFS server to avoid pods stuck in terminating state (PWX-24274)
						err = Inst().N.Systemctl(*n, "nfs-server.service", node.SystemctlOpts{
							Action: "start",
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         5 * time.Minute,
								TimeBeforeRetry: 10 * time.Second,
							}})
						dash.VerifyFatal(err, nil, fmt.Sprintf("Starting nfs service on node : %s", n.Name))

						ctx.RefreshStorageEndpoint = true
						ValidateContext(ctx)
						n2, err := Inst().V.GetNodeForVolume(vol, defaultCommandTimeout, defaultCommandRetry)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Get node for volume : %s", vol.ID))

						// the mount should move to another node otherwise fail
						dash.Infof("volume %s is now attached on node %s [%s]", vol.ID, n2.SchedulerNodeName, n2.Addresses[0])
						dash.VerifyFatal(n.SchedulerNodeName != n2.SchedulerNodeName, true, fmt.Sprintf("Verfiy volume is scheduled on differt nodes"))

						StartVolDriverAndWait([]node.Node{*n})
						dash.VerifyFatal(err, nil, fmt.Sprintf("Staet volume driver on node: %s", n.Name))
						err = Inst().S.EnableSchedulingOnNode(*n)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Enable scheduling on node: %s", n.Name))

						dash.Info("validating applications")
						ValidateApplications(contexts)
					}
				}
			}
		})

		Step(fmt.Sprintf("Destroy apps"), func() {
			dash.Info("Destroy apps")
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
