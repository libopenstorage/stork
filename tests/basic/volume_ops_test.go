package tests

import (
	"fmt"
	"github.com/portworx/torpedo/pkg/log"
	"math"
	"reflect"
	"time"

	"github.com/portworx/torpedo/pkg/testrailuttils"

	. "github.com/onsi/ginkgo"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	. "github.com/portworx/torpedo/tests"
)

const (
	bandwidthMBps = 1
	// buffered BW = 1 MBps with 10% buffer speed in KBps
	bufferedBW = 1130
)
const (
	fio = "fio-throttle-io"
)

// Volume replication change
var _ = Describe("{VolumeUpdate}", func() {
	var testrailID = 35271
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35271
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("VolumeUpdate", "Validate Volume update", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "has to schedule apps and update replication factor and size on all volumes of the apps"
	It(stepLog, func() {
		log.InfoD(stepLog)
		var err error
		contexts = make([]*scheduler.Context, 0)
		expReplMap := make(map[*volume.Volume]int64)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("volupdate-%d", i))...)
		}

		ValidateApplications(contexts)

		stepLog = "get volumes for all apps in test and update replication factor and size"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					appVolumes, err = Inst().S.GetVolumes(ctx)
					log.FailOnError(err, "Failed to get volumes for app %s", ctx.App.Key)
					dash.VerifyFatal(len(appVolumes) > 0, true, "App volumes exist?")
				})
				for _, v := range appVolumes {
					MaxRF := Inst().V.GetMaxReplicationFactor()
					MinRF := Inst().V.GetMinReplicationFactor()
					stepLog = fmt.Sprintf("repl decrease volume driver %s on app %s's volume: %v",
						Inst().V.String(), ctx.App.Key, v)
					Step(stepLog,
						func() {
							log.InfoD(stepLog)
							currRep, err := Inst().V.GetReplicationFactor(v)
							log.FailOnError(err, "Failed to get volume  %s repl factor", v.Name)
							expReplMap[v] = int64(math.Max(float64(MinRF), float64(currRep)-1))
							err = Inst().V.SetReplicationFactor(v, currRep-1, nil, nil, true)
							log.FailOnError(err, "Failed to set volume  %s repl factor", v.Name)
							dash.VerifyFatal(err == nil, true, fmt.Sprintf("Set volume  %s repl factor successful ?", v.Name))
						})
					stepLog = fmt.Sprintf("validate successful repl decrease on app %s's volume: %v",
						ctx.App.Key, v)
					Step(stepLog,
						func() {
							log.InfoD(stepLog)
							newRepl, err := Inst().V.GetReplicationFactor(v)
							log.FailOnError(err, "Failed to get volume  %s repl factor", v.Name)
							dash.VerifyFatal(newRepl, expReplMap[v], "Repl factor is as expected ?")
						})
					stepLog = fmt.Sprintf("repl increase volume driver %s on app %s's volume: %v",
						Inst().V.String(), ctx.App.Key, v)
					Step(stepLog,
						func() {
							currRep, err := Inst().V.GetReplicationFactor(v)
							log.FailOnError(err, "Failed to get volume  %s repl factor", v.Name)
							// GetMaxReplicationFactory is hardcoded to 3
							// if it increases repl 3 to an aggregated 2 volume, it will fail
							// because it would require 6 worker nodes, since
							// number of nodes required = aggregation level * replication factor
							currAggr, err := Inst().V.GetAggregationLevel(v)
							log.FailOnError(err, "Failed to get volume  %s aggregate level", v.Name)
							if currAggr > 1 {
								MaxRF = int64(len(node.GetWorkerNodes())) / currAggr
							}
							expReplMap[v] = int64(math.Min(float64(MaxRF), float64(currRep)+1))
							err = Inst().V.SetReplicationFactor(v, currRep+1, nil, nil, true)
							log.FailOnError(err, "Failed to set volume  %s repl factor", v.Name)
							dash.VerifyFatal(err == nil, true, fmt.Sprintf("Repl factor set succesfully on volume  %s repl", v.Name))
						})
					stepLog = fmt.Sprintf("validate successful repl increase on app %s's volume: %v",
						ctx.App.Key, v)
					Step(stepLog,
						func() {
							newRepl, err := Inst().V.GetReplicationFactor(v)
							log.FailOnError(err, "Failed to get volume  %s repl factor", v.Name)
							dash.VerifyFatal(newRepl, expReplMap[v], "Repl factor is as expected?")
						})
				}
				var requestedVols []*volume.Volume
				stepLog = fmt.Sprintf("increase volume size %s on app %s's volumes: %v",
					Inst().V.String(), ctx.App.Key, appVolumes)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						requestedVols, err = Inst().S.ResizeVolume(ctx, Inst().ConfigMap)
						log.FailOnError(err, "Volume resize successful ?")
					})
				stepLog = fmt.Sprintf("validate successful volume size increase on app %s's volumes: %v",
					ctx.App.Key, appVolumes)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						for _, v := range requestedVols {
							// Need to pass token before validating volume
							params := make(map[string]string)
							if Inst().ConfigMap != "" {
								params["auth-token"], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
								log.FailOnError(err, "Failed to get token from configMap")
							}
							err := Inst().V.ValidateUpdateVolume(v, params)
							dash.VerifyFatal(err, nil, "Validate volume update successful?")
						}
					})

			}
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

// Volume IO Throttle change
var _ = Describe("{VolumeIOThrottle}", func() {
	var contexts []*scheduler.Context
	var namespace string
	var speedBeforeUpdate, speedAfterUpdate int
	var testrailID = 58504
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/58504
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("VolumeIOThrottle", "Validate volume IO throttle", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	stepLog := "has to schedule IOPs and limit them to a max bandwidth"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		var err error
		taskNamePrefix := "io-throttle"
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			log.Debugf("Task name %s\n", taskName)
			appContexts := ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			namespace = appContexts[0].ScheduleOptions.Namespace
		}
		ValidateApplications(contexts)
		stepLog = "get the BW for volume without limiting bandwidth"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			log.Infof("waiting for 5 sec for the pod to stablize")
			time.Sleep(5 * time.Second)
			speedBeforeUpdate, err = Inst().S.GetIOBandwidth(fio, namespace)
			log.FailOnError(err, "Failed to get IO Bandwidth")
		})
		log.InfoD("BW before update %d", speedBeforeUpdate)

		stepLog = "updating the BW"
		Step(stepLog, func() {
			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
				Step(stepLog, func() {
					appVolumes, err = Inst().S.GetVolumes(ctx)
					log.FailOnError(err, "Failed to get volumes for app %s", ctx.App.Key)
					dash.VerifyFatal(len(appVolumes) > 0, true, "App volumes exist?")
				})
				log.InfoD("Volumes to be updated %s", appVolumes)
				for _, v := range appVolumes {
					err := Inst().V.SetIoBandwidth(v, bandwidthMBps, bandwidthMBps)
					log.FailOnError(err, "Failed to set IO bandwidth")
				}
			}
		})
		log.InfoD("waiting for the FIO to reduce the speed to take into account the IO Throttle")
		time.Sleep(60 * time.Second)
		stepLog = "get the BW for volume after limiting bandwidth"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			speedAfterUpdate, err = Inst().S.GetIOBandwidth(fio, namespace)
			log.FailOnError(err, "Failed to get IO bandwidth after update")
		})
		log.InfoD("BW after update %d", speedAfterUpdate)
		stepLog = "Validate speed reduction"
		Step(stepLog, func() {
			// We are setting the BW to 1 MBps so expecting the returned value to be in 10% buffer
			log.InfoD(stepLog)
			dash.VerifyFatal(speedAfterUpdate < bufferedBW, true, "Speed reduced below the buffer?")
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

// Volume replication change
var _ = Describe("{VolumeUpdateForAttachedNode}", func() {
	var testrailID = 58838
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/58838
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("VolumeUpdateForAttachedNode", "Validate volume update for the attached node", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "has to schedule apps and update replication factor for attached node"

	It(stepLog, func() {
		log.InfoD(stepLog)
		var err error
		contexts = make([]*scheduler.Context, 0)
		expReplMap := make(map[*volume.Volume]int64)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("volupdate-%d", i))...)
		}

		ValidateApplications(contexts)
		stepLog = "get volumes for all apps in test and update replication factor and size"
		Step(stepLog, func() {
			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
					appVolumes, err = Inst().S.GetVolumes(ctx)
					log.FailOnError(err, "Failed to get volumes for app %s", ctx.App.Key)
					dash.VerifyFatal(len(appVolumes) > 0, true, "App volumes exist ?")
				})
				for _, v := range appVolumes {
					MaxRF := Inst().V.GetMaxReplicationFactor()
					MinRF := Inst().V.GetMinReplicationFactor()
					currReplicaSet := []string{}
					updateReplicaSet := []string{}
					expectedReplicaSet := []string{}
					stepLog = fmt.Sprintf("repl decrease volume driver %s on app %s's volume: %v",
						Inst().V.String(), ctx.App.Key, v)
					Step(stepLog,
						func() {
							log.InfoD(stepLog)
							currRep, err := Inst().V.GetReplicationFactor(v)
							log.FailOnError(err, "Failed to get vol %s repl factor", v.Name)
							attachedNode, err := Inst().V.GetNodeForVolume(v, defaultCommandTimeout, defaultCommandRetry)

							replicaSets, err := Inst().V.GetReplicaSets(v)
							log.FailOnError(err, "Failed to get vol %s replica sets", v.Name)
							dash.VerifyFatal(len(replicaSets) > 0, true, fmt.Sprintf("Validate vol %s has replica sets", v.Name))

							for _, nID := range replicaSets[0].Nodes {
								currReplicaSet = append(currReplicaSet, nID)
							}

							log.InfoD("ReplicaSet of volume %v is: %v", v.Name, currReplicaSet)
							log.InfoD("Volume %v is attached to : %v", v.Name, attachedNode.Id)

							for _, n := range currReplicaSet {
								if n == attachedNode.Id {
									updateReplicaSet = append(updateReplicaSet, n)
								} else {
									expectedReplicaSet = append(expectedReplicaSet, n)
								}
							}

							if len(updateReplicaSet) == 0 {
								log.InfoD("Attached node in not part of ReplicatSet, choosing a random node part of set for setting replication factor")
								updateReplicaSet = append(updateReplicaSet, expectedReplicaSet[0])
								expectedReplicaSet = expectedReplicaSet[1:]
							}

							expReplMap[v] = int64(math.Max(float64(MinRF), float64(currRep)-1))
							err = Inst().V.SetReplicationFactor(v, currRep-1, updateReplicaSet, nil, true)
							log.FailOnError(err, "Failed to set repl factor")
							dash.VerifyFatal(err == nil, true, "Repl factor set successfully?")
						})
					stepLog = fmt.Sprintf("validate successful repl decrease on app %s's volume: %v",
						ctx.App.Key, v)
					Step(stepLog,
						func() {
							log.InfoD(stepLog)
							newRepl, err := Inst().V.GetReplicationFactor(v)
							log.InfoD("Got repl factor after update: %v", newRepl)
							log.FailOnError(err, "Failed to get vol %s repl factor", v.Name)
							dash.VerifyFatal(newRepl, expReplMap[v], "New repl factor is as expected?")
							currReplicaSets, err := Inst().V.GetReplicaSets(v)
							log.FailOnError(err, "Failed to get vol %s replica sets", v.Name)
							dash.VerifyFatal(len(currReplicaSet) > 0, true, fmt.Sprintf(" Vol %s repl sets exist?", v.Name))
							reducedReplicaSet := []string{}
							for _, nID := range currReplicaSets[0].Nodes {
								reducedReplicaSet = append(reducedReplicaSet, nID)
							}

							log.InfoD("ReplicaSet of volume %v is: %v", v.Name, reducedReplicaSet)
							log.InfoD("Expected ReplicaSet of volume %v is: %v", v.Name, expectedReplicaSet)
							res := reflect.DeepEqual(reducedReplicaSet, expectedReplicaSet)
							dash.VerifyFatal(res, true, "Reduced replica set is as expected?")
						})
					for _, ctx := range contexts {
						ctx.SkipVolumeValidation = true
					}
					ValidateApplications(contexts)
					for _, ctx := range contexts {
						ctx.SkipVolumeValidation = false
					}
					stepLog = fmt.Sprintf("repl increase volume driver %s on app %s's volume: %v",
						Inst().V.String(), ctx.App.Key, v)
					Step(stepLog,
						func() {
							log.InfoD(stepLog)
							currRep, err := Inst().V.GetReplicationFactor(v)
							log.FailOnError(err, "Failed to get vol %s repl factor", v.Name)
							// GetMaxReplicationFactory is hardcoded to 3
							// if it increases repl 3 to an aggregated 2 volume, it will fail
							// because it would require 6 worker nodes, since
							// number of nodes required = aggregation level * replication factor
							currAggr, err := Inst().V.GetAggregationLevel(v)
							log.FailOnError(err, "Failed to get vol %s aggregation level", v.Name)
							if currAggr > 1 {
								MaxRF = int64(len(node.GetWorkerNodes())) / currAggr
							}
							expReplMap[v] = int64(math.Min(float64(MaxRF), float64(currRep)+1))
							err = Inst().V.SetReplicationFactor(v, currRep+1, updateReplicaSet, nil, true)
							log.FailOnError(err, "Failed to set vol %s repl factor", v.Name)
							dash.VerifyFatal(err == nil, true, fmt.Sprintf("Vol %s repl factor set as expected?", v.Name))
						})
					stepLog = fmt.Sprintf("validate successful repl increase on app %s's volume: %v",
						ctx.App.Key, v)
					Step(stepLog,
						func() {
							log.InfoD(stepLog)
							newRepl, err := Inst().V.GetReplicationFactor(v)
							log.FailOnError(err, "Failed to get vol %s repl factor", v.Name)
							dash.VerifyFatal(newRepl, expReplMap[v], "New repl factor is as expected?")
							currReplicaSets, err := Inst().V.GetReplicaSets(v)
							log.FailOnError(err, "Failed to get vol %s replica sets", v.Name)
							dash.VerifyFatal(len(currReplicaSets) > 0, true, "New repl factor is as expected?")
							dash.VerifyFatal(len(currReplicaSet) > 0, true, fmt.Sprintf("Vol %s repl sets exist?", v.Name))
							increasedReplicaSet := []string{}
							for _, nID := range currReplicaSets[0].Nodes {
								increasedReplicaSet = append(increasedReplicaSet, nID)
							}

							log.InfoD("ReplicaSet of volume %v is: %v", v.Name, increasedReplicaSet)
							res := reflect.DeepEqual(increasedReplicaSet, currReplicaSet)
							dash.VerifyFatal(res, true, "Validate increased replica set is as expected")
						})
					ValidateApplications(contexts)
				}

			}
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
