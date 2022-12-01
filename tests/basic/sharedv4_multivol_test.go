package tests

import (
	"fmt"
	"github.com/portworx/torpedo/pkg/log"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	. "github.com/portworx/torpedo/tests"
)

const (
	nodeDeleteTimeoutMins = 7 * time.Minute
)

// This test performs multi volume mounts to a single deployment
var _ = Describe("{MultiVolumeMountsForSharedV4}", func() {
	var testrailID = 58846
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/58846
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("MultiVolumeMountsForSharedV4", "Validate mounting multiple SV4 volumes for one app", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "has to create multiple sharedv4 volumes and mount to single pod"
	It(stepLog, func() {
		log.InfoD(stepLog)
		// set frequency mins depending on the chaos level
		var frequency int
		var timeout time.Duration

		chaosLevel := Inst().ChaosLevel
		if chaosLevel != 0 {
			frequency = 10 * chaosLevel
			timeout = (15 * time.Duration(chaosLevel) * time.Minute) / 10
		} else {
			frequency = 10
			timeout = 1 * time.Minute
		}
		log.InfoD("setting number of volumes=%v and app readiness timeout=%v for chaos level %v",
			frequency, timeout, chaosLevel)

		customAppConfig := scheduler.AppConfig{
			ClaimsCount: frequency,
		}

		provider := Inst().V.String()
		contexts = []*scheduler.Context{}
		// there should be only 1 app
		Expect(len(Inst().AppList)).To(Equal(1))
		appName := Inst().AppList[0]

		Inst().CustomAppConfig[appName] = customAppConfig
		err := Inst().S.RescanSpecs(Inst().SpecDir, provider)
		log.FailOnError(err, "Failed to rescan specs from %s for storage provider %s", Inst().SpecDir, provider)

		stepLog = "schedule application with multiple sharedv4 volumes attached"

		Step(stepLog, func() {
			log.InfoD(stepLog)
			log.InfoD("Number of Volumes to be mounted: %v", frequency)

			taskName := "sharedv4-multivol"

			log.Infof("Task name %s\n", taskName)

			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				newContexts := ScheduleApplications(taskName)
				contexts = append(contexts, newContexts...)
			}

			for _, ctx := range contexts {
				ctx.ReadinessTimeout = timeout
				ctx.SkipVolumeValidation = false
				ValidateContext(ctx)
			}
		})
		stepLog = "get nodes where volume is attached and restart volume driver"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, ctx := range contexts {
				appVolumes, err := Inst().S.GetVolumes(ctx)
				Expect(err).NotTo(HaveOccurred())
				for _, appVolume := range appVolumes {
					attachedNode, err := Inst().V.GetNodeForVolume(appVolume, defaultCommandTimeout, defaultCommandRetry)
					log.FailOnError(err, "Failed to get volume %s from node", appVolume.Name)
					stepLog = fmt.Sprintf("stop volume driver %s on app %s's node: %s",
						Inst().V.String(), ctx.App.Key, attachedNode.Name)
					Step(stepLog,
						func() {
							log.InfoD(stepLog)
							StopVolDriverAndWait([]node.Node{*attachedNode})
						})
					stepLog = fmt.Sprintf("starting volume %s driver on app %s's node %s",
						Inst().V.String(), ctx.App.Key, attachedNode.Name)
					Step(stepLog,
						func() {
							log.InfoD(stepLog)
							StartVolDriverAndWait([]node.Node{*attachedNode})
						})
					stepLog = "Giving few seconds for volume driver to stabilize"
					Step(stepLog, func() {
						log.InfoD(stepLog)
						time.Sleep(20 * time.Second)
					})
					stepLog = fmt.Sprintf("validate app %s", attachedNode.Name)
					Step(stepLog, func() {
						ctx.ReadinessTimeout = timeout
						ctx.SkipVolumeValidation = true
						ValidateContext(ctx)
					})
				}
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This test performs sharedv4 nfs server pod termination failover use case
var _ = Describe("{NFSServerNodeDelete}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("NFSServerNodeDelete", "Vslidate NFS server delete", nil, 0)
	})

	var contexts []*scheduler.Context
	stepLog := "has to validate that the new pods started successfully after nfs server node is terminated"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		var err error

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("nodekill-%d", i))...)
		}

		ValidateApplications(contexts)
		for _, ctx := range contexts {
			var appVolumes []*volume.Volume
			stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				appVolumes, err = Inst().S.GetVolumes(ctx)
				log.FailOnError(err, "Failed to get volumes")
				dash.VerifyFatal(len(appVolumes) > 0, 0, " App volumes are empty?")
			})
			for _, v := range appVolumes {
				stepLog = "get attached node and stop the instance"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					currNodes := node.GetStorageDriverNodes()
					countOfCurrNodes := len(currNodes)

					attachedNode, err := Inst().V.GetNodeForVolume(v, defaultCommandTimeout, defaultCommandRetry)

					stepLog = fmt.Sprintf("delete node : %v having volume: %v attached", attachedNode.Name, v.Name)
					// Delete node and check Apps status
					Step(stepLog, func() {
						log.InfoD(stepLog)
						sv4KillANodeAndValidate(*attachedNode)
						stepLog = fmt.Sprintf("validate node: %v is deleted", attachedNode.Name)
						Step(stepLog, func() {
							log.InfoD(stepLog)
							currNodes = node.GetStorageDriverNodes()
							for _, currNode := range currNodes {
								if currNode.Name == attachedNode.Name {
									dash.VerifyFatal(currNode.Name, attachedNode.Name, fmt.Sprintf("Node: %v still exists?",
										attachedNode.Name))
									break
								}
							}
						})

						stepLog = fmt.Sprintf("validate applications after node [%v] deletion", attachedNode.Name)
						Step(stepLog, func() {
							log.InfoD(stepLog)
							for _, ctx := range contexts {
								ValidateContext(ctx)
							}
						})
						stepLog = fmt.Sprintf("wait to new instance to start scheduler: %s and volume driver: %s",
							Inst().S.String(), Inst().V.String())
						Step(stepLog, func() {
							log.InfoD(stepLog)
							time.Sleep(2 * time.Minute)
							currNodes = node.GetStorageDriverNodes()
							dash.VerifyFatal(countOfCurrNodes, len(currNodes), "Create new instance successful?")
							Expect(countOfCurrNodes).To(Equal(len(currNodes)))
							log.InfoD("Validating Node and Volume driver for all nodes")
							for _, n := range currNodes {

								err = Inst().S.IsNodeReady(n)
								log.FailOnError(err, "Node %s not ready", n.Name)

								err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
								log.FailOnError(err, "Failed to wait for volume driver %s to be up", n.Name)
							}
						})

						Step("validate apps after new node is ready", func() {
							for _, ctx := range contexts {
								ValidateContext(ctx)
							}
						})

					})
				})
			}

		}

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

func sv4KillANodeAndValidate(nodeToKill node.Node) {
	steplog := fmt.Sprintf("Deleting node [%v]", nodeToKill.Name)
	Step(steplog, func() {
		log.InfoD(steplog)
		log.Infof("Instance is of %v ", Inst().N.String())
		err := Inst().N.DeleteNode(nodeToKill, nodeDeleteTimeoutMins)
		dash.VerifyFatal(err, nil, "Validate node delete init")
	})
	steplog = fmt.Sprintf("Wait for node: %v to be deleted", nodeToKill.Name)
	Step(steplog, func() {
		log.InfoD(steplog)
		maxWait := 10
	OUTER:
		for maxWait > 0 {
			for _, currNode := range node.GetStorageDriverNodes() {
				if currNode.Name == nodeToKill.Name {
					log.Infof("Node %v still exists. Waiting for a minute to check again", nodeToKill.Name)
					maxWait--
					time.Sleep(1 * time.Minute)
					continue OUTER
				}
			}
			break
		}
	})

	err := Inst().S.RefreshNodeRegistry()
	dash.VerifyFatal(err, nil, "Validate node registry refresh")

	err = Inst().V.RefreshDriverEndpoints()
	dash.VerifyFatal(err, nil, "Validate volume driver end points refresh")
}
