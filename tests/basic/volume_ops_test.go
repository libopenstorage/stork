package tests

import (
	"fmt"
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
		dash.Info(stepLog)
		var err error
		contexts = make([]*scheduler.Context, 0)
		expReplMap := make(map[*volume.Volume]int64)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("volupdate-%d", i))...)
		}

		ValidateApplications(contexts)

		stepLog = "get volumes for all apps in test and update replication factor and size"
		Step(stepLog, func() {
			dash.Info(stepLog)
			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
				Step(stepLog, func() {
					dash.Info(stepLog)
					appVolumes, err = Inst().S.GetVolumes(ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get volumes for app %s", ctx.App.Key))
					dash.VerifyFatal(len(appVolumes) > 0, true, "Validate app volumes exist")
				})
				for _, v := range appVolumes {
					MaxRF := Inst().V.GetMaxReplicationFactor()
					MinRF := Inst().V.GetMinReplicationFactor()
					stepLog = fmt.Sprintf("repl decrease volume driver %s on app %s's volume: %v",
						Inst().V.String(), ctx.App.Key, v)
					Step(stepLog,
						func() {
							dash.Info(stepLog)
							errExpected := false
							currRep, err := Inst().V.GetReplicationFactor(v)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get volume  %s repl factor", v.Name))
							if currRep == MinRF {
								errExpected = true
							}
							expReplMap[v] = int64(math.Max(float64(MinRF), float64(currRep)-1))
							err = Inst().V.SetReplicationFactor(v, currRep-1, nil, true)
							if !errExpected {
								dash.VerifyFatal(err, nil, fmt.Sprintf("Validate set volume  %s repl factor", v.Name))
							} else {
								dash.VerifyFatal(err != nil, true, fmt.Sprintf("Validate error occured whiel setting volume  %s repl factor, Err: %v", v.Name, err))
							}

						})
					stepLog = fmt.Sprintf("validate successful repl decrease on app %s's volume: %v",
						ctx.App.Key, v)
					Step(stepLog,
						func() {
							dash.Info(stepLog)
							newRepl, err := Inst().V.GetReplicationFactor(v)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get volume  %s repl factor", v.Name))
							dash.VerifyFatal(newRepl, expReplMap[v], "Validate repl factor is as expected")
						})
					stepLog = fmt.Sprintf("repl increase volume driver %s on app %s's volume: %v",
						Inst().V.String(), ctx.App.Key, v)
					Step(stepLog,
						func() {
							errExpected := false
							currRep, err := Inst().V.GetReplicationFactor(v)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get volume  %s repl factor", v.Name))
							// GetMaxReplicationFactory is hardcoded to 3
							// if it increases repl 3 to an aggregated 2 volume, it will fail
							// because it would require 6 worker nodes, since
							// number of nodes required = aggregation level * replication factor
							currAggr, err := Inst().V.GetAggregationLevel(v)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get volume  %s aggregate level", v.Name))
							if currAggr > 1 {
								MaxRF = int64(len(node.GetWorkerNodes())) / currAggr
							}
							if currRep == MaxRF {
								errExpected = true
							}
							expReplMap[v] = int64(math.Min(float64(MaxRF), float64(currRep)+1))
							err = Inst().V.SetReplicationFactor(v, currRep+1, nil, true)
							if !errExpected {
								dash.VerifyFatal(err, nil, fmt.Sprintf("Validate set volume  %s repl factor", v.Name))
							} else {
								dash.VerifyFatal(err != nil, true, fmt.Sprintf("Validate error occured whiel setting volume  %s repl factor, Err: %v", v.Name, err))
							}
						})
					stepLog = fmt.Sprintf("validate successful repl increase on app %s's volume: %v",
						ctx.App.Key, v)
					Step(stepLog,
						func() {
							newRepl, err := Inst().V.GetReplicationFactor(v)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get volume  %s repl factor", v.Name))
							dash.VerifyFatal(newRepl, expReplMap[v], "Validate repl factor is as expected")
						})
				}
				var requestedVols []*volume.Volume
				stepLog = fmt.Sprintf("increase volume size %s on app %s's volumes: %v",
					Inst().V.String(), ctx.App.Key, appVolumes)
				Step(stepLog,
					func() {
						dash.Info(stepLog)
						requestedVols, err = Inst().S.ResizeVolume(ctx, Inst().ConfigMap)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Validate volume resize"))
					})
				stepLog = fmt.Sprintf("validate successful volume size increase on app %s's volumes: %v",
					ctx.App.Key, appVolumes)
				Step(stepLog,
					func() {
						dash.Info(stepLog)
						for _, v := range requestedVols {
							// Need to pass token before validating volume
							params := make(map[string]string)
							if Inst().ConfigMap != "" {
								params["auth-token"], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
								dash.VerifyFatal(err, nil, "Validate get token from config map")
							}
							err := Inst().V.ValidateUpdateVolume(v, params)
							dash.VerifyFatal(err, nil, "Validate volume update")
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
		dash.Info(stepLog)
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
			dash.Info(stepLog)
			log.Infof("waiting for 5 sec for the pod to stablize")
			time.Sleep(5 * time.Second)
			speedBeforeUpdate, err = Inst().S.GetIOBandwidth(fio, namespace)
			dash.VerifyFatal(err, nil, "validate get IO bandwidth")
		})
		dash.Infof("BW before update %d", speedBeforeUpdate)

		stepLog = "updating the BW"
		Step(stepLog, func() {
			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
				Step(stepLog, func() {
					appVolumes, err = Inst().S.GetVolumes(ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get volumes for app %s", ctx.App.Key))
					dash.VerifyFatal(len(appVolumes) > 0, true, "Validate app volumes exist")
				})
				dash.Infof("Volumes to be updated %s", appVolumes)
				for _, v := range appVolumes {
					err := Inst().V.SetIoBandwidth(v, bandwidthMBps, bandwidthMBps)
					dash.VerifyFatal(err, nil, "validate set IO bandwidth")
				}
			}
		})
		dash.Infof("waiting for the FIO to reduce the speed to take into account the IO Throttle")
		time.Sleep(60 * time.Second)
		stepLog = "get the BW for volume after limiting bandwidth"
		Step(stepLog, func() {
			dash.Info(stepLog)
			speedAfterUpdate, err = Inst().S.GetIOBandwidth(fio, namespace)
			dash.VerifyFatal(err, nil, "validate get IO bandwidth after update")
		})
		dash.Infof("BW after update %d", speedAfterUpdate)
		stepLog = "Validate speed reduction"
		Step(stepLog, func() {
			// We are setting the BW to 1 MBps so expecting the returned value to be in 10% buffer
			dash.Info(stepLog)
			dash.VerifyFatal(speedAfterUpdate < bufferedBW, true, "Validate speed reduced below the buffer")
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
		dash.Info(stepLog)
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
					dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get volumes for app %s", ctx.App.Key))
					dash.VerifyFatal(len(appVolumes) > 0, true, "Validate app volumes exist")
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
							dash.Info(stepLog)
							errExpected := false
							currRep, err := Inst().V.GetReplicationFactor(v)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get vol %s repl factor", v.Name))
							if currRep == MinRF {
								errExpected = true
							}
							attachedNode, err := Inst().V.GetNodeForVolume(v, defaultCommandTimeout, defaultCommandRetry)

							replicaSets, err := Inst().V.GetReplicaSets(v)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get vol %s replica sets", v.Name))
							dash.VerifyFatal(len(replicaSets) > 0, true, fmt.Sprintf("Validate vol %s has replica sets", v.Name))

							for _, nID := range replicaSets[0].Nodes {
								currReplicaSet = append(currReplicaSet, nID)
							}

							dash.Infof("ReplicaSet of volume %v is: %v", v.Name, currReplicaSet)
							dash.Infof("Volume %v is attached to : %v", v.Name, attachedNode.Id)

							for _, n := range currReplicaSet {
								if n == attachedNode.Id {
									updateReplicaSet = append(updateReplicaSet, n)
								} else {
									expectedReplicaSet = append(expectedReplicaSet, n)
								}
							}

							if len(updateReplicaSet) == 0 {
								dash.Info("Attached node in not part of ReplicatSet, choosing a random node part of set for setting replication factor")
								updateReplicaSet = append(updateReplicaSet, expectedReplicaSet[0])
								expectedReplicaSet = expectedReplicaSet[1:]
							}

							expReplMap[v] = int64(math.Max(float64(MinRF), float64(currRep)-1))
							err = Inst().V.SetReplicationFactor(v, currRep-1, updateReplicaSet, true)
							if !errExpected {
								dash.VerifyFatal(err, nil, "Validate set repl factor")
							} else {
								dash.VerifyFatal(err != nil, true, fmt.Sprintf("Validate err occured while setting repl factor,Err: %v ", err))
							}

						})
					stepLog = fmt.Sprintf("validate successful repl decrease on app %s's volume: %v",
						ctx.App.Key, v)
					Step(stepLog,
						func() {
							dash.Info(stepLog)
							newRepl, err := Inst().V.GetReplicationFactor(v)
							dash.Infof("Got repl factor after update: %v", newRepl)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get vol %s repl factor", v.Name))
							dash.VerifyFatal(newRepl, expReplMap[v], "validate new repl factor is as expected")
							currReplicaSets, err := Inst().V.GetReplicaSets(v)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get vol %s replica setss", v.Name))
							dash.VerifyFatal(len(currReplicaSet) > 0, true, fmt.Sprintf("Validate vol %s repl sets exist", v.Name))
							reducedReplicaSet := []string{}
							for _, nID := range currReplicaSets[0].Nodes {
								reducedReplicaSet = append(reducedReplicaSet, nID)
							}

							dash.Infof("ReplicaSet of volume %v is: %v", v.Name, reducedReplicaSet)
							dash.Infof("Expected ReplicaSet of volume %v is: %v", v.Name, expectedReplicaSet)
							res := reflect.DeepEqual(reducedReplicaSet, expectedReplicaSet)
							dash.VerifyFatal(res, true, "Validate reduced replica set is as expected")
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
							dash.Info(stepLog)
							errExpected := false
							currRep, err := Inst().V.GetReplicationFactor(v)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get vol %s repl factor", v.Name))
							// GetMaxReplicationFactory is hardcoded to 3
							// if it increases repl 3 to an aggregated 2 volume, it will fail
							// because it would require 6 worker nodes, since
							// number of nodes required = aggregation level * replication factor
							currAggr, err := Inst().V.GetAggregationLevel(v)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get vol %s aggregation level", v.Name))
							if currAggr > 1 {
								MaxRF = int64(len(node.GetWorkerNodes())) / currAggr
							}
							if currRep == MaxRF {
								errExpected = true
							}
							expReplMap[v] = int64(math.Min(float64(MaxRF), float64(currRep)+1))
							err = Inst().V.SetReplicationFactor(v, currRep+1, updateReplicaSet, true)
							if !errExpected {
								dash.VerifyFatal(err, nil, fmt.Sprintf("Validate set vol %s repl factor", v.Name))
							} else {
								dash.VerifyFatal(err != nil, true, fmt.Sprintf("Validate error occured while setting vol %s repl factor, Err: %v", v.Name, err))
							}
						})
					stepLog = fmt.Sprintf("validate successful repl increase on app %s's volume: %v",
						ctx.App.Key, v)
					Step(stepLog,
						func() {
							dash.Info(stepLog)
							newRepl, err := Inst().V.GetReplicationFactor(v)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get vol %s repl factor", v.Name))
							dash.VerifyFatal(newRepl, expReplMap[v], "Validate new repl factor is as expected")
							currReplicaSets, err := Inst().V.GetReplicaSets(v)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get vol %s replica sets", v.Name))
							dash.VerifyFatal(len(currReplicaSets) > 0, true, "Validate new repl factor is as expected")
							dash.VerifyFatal(len(currReplicaSet) > 0, true, fmt.Sprintf("Validate vol %s repl sets exist", v.Name))
							increasedReplicaSet := []string{}
							for _, nID := range currReplicaSets[0].Nodes {
								increasedReplicaSet = append(increasedReplicaSet, nID)
							}

							dash.Infof("ReplicaSet of volume %v is: %v", v.Name, increasedReplicaSet)
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
