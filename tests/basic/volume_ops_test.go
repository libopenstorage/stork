package tests

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/portworx/torpedo/pkg/log"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/portworx/torpedo/pkg/testrailuttils"

	. "github.com/onsi/ginkgo"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/units"
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
							log.InfoD(stepLog)
							currRep, err := Inst().V.GetReplicationFactor(v)
							log.FailOnError(err, "Failed to get volume  %s repl factor", v.Name)
							// GetMaxReplicationFactory is hardcoded to 3
							// if it increases repl 3 to an aggregated 2 volume, it will fail
							// because it would require 6 worker nodes, since
							// number of nodes required = aggregation level * replication factor
							currAggr, err := Inst().V.GetAggregationLevel(v)
							log.FailOnError(err, "Failed to get volume  %s aggregate level", v.Name)
							if currAggr > 1 {
								MaxRF = int64(len(node.GetStorageDriverNodes())) / currAggr
							}
							expReplMap[v] = int64(math.Min(float64(MaxRF), float64(currRep)+1))
							opts := volume.Options{
								ValidateReplicationUpdateTimeout: validateReplicationUpdateTimeout,
							}
							err = Inst().V.SetReplicationFactor(v, currRep+1, nil, nil, true, opts)
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
								MaxRF = int64(len(node.GetStorageDriverNodes())) / currAggr
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

// Volume replication change
var _ = Describe("{CreateLargeNumberOfVolumes}", func() {
	var testrailID = 0
	// JIRA ID :https://portworx.atlassian.net/browse/PWX-26820
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("CreateLargeNumberOfVolumes", "Volumes more than 684 went into down state after creation", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context
	var totalVolumesToCreate = 700
	var maxVolumesToAttach = 100
	var volumesCurrentlyAttached = 0
	var newVolumeIDs []string
	var attachedVolumes []string
	terminate := false

	stepLog := "has to schedule apps and update replication factor for attached node"
	It(stepLog, func() {
		log.InfoD(stepLog)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("createmaxvolume-%d", i))...)
		}
		deleteVolumes := func() {
			terminate = true
			for _, each := range newVolumeIDs {
				log.InfoD(fmt.Sprintf("delete volume [%v]", each))
				log.FailOnError(Inst().V.DetachVolume(each), fmt.Sprintf("Failed to detach volume [%v]", each))
				time.Sleep(500 * time.Millisecond)
				log.FailOnError(Inst().V.DeleteVolume(each), fmt.Sprintf("Delete volume with ID [%v] failed", each))
			}
		}

		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)
		defer deleteVolumes()

		// Get list of all volumes present in the cluster
		log.InfoD("Listing all the volumes present in the cluster")
		allVolumeIds, err := Inst().V.ListAllVolumes()

		log.FailOnError(err, "failed to list all the volume")
		log.Info(fmt.Sprintf("total number of volumes present in the cluster [%v]", len(allVolumeIds)))

		if len(allVolumeIds) >= totalVolumesToCreate {
			log.FailOnError(fmt.Errorf("exceeded total volume count limit.. exiting [%d]", len(allVolumeIds)),
				"Total volume count exceeded ")
		}

		// Get Total number of already attached volumes
		for _, each := range allVolumeIds {
			vol, err := Inst().V.InspectVolume(each)
			log.FailOnError(err, "inspect returned error ?")
			if vol.State.String() == "VOLUME_STATE_ATTACHED" {
				volumesCurrentlyAttached = volumesCurrentlyAttached + 1
			}
		}

		// Run inspect continuously in the background
		log.InfoD("start attach volume in the backend while more than 100 volumes got created")
		go func(volumeIds []string) {
			defer GinkgoRecover()
			attachedCount := 0
			for {
				if terminate == true {
					break
				}
				if len(newVolumeIDs) > 100 {
					for _, each := range newVolumeIDs {
						if attachedCount < (maxVolumesToAttach - volumesCurrentlyAttached) {
							_, err := Inst().V.AttachVolume(each)
							log.FailOnError(err, "attaching volume failed")
							attachedCount += 1
							attachedVolumes = append(attachedVolumes, each)
							time.Sleep(2 * time.Second)
						}
					}
				}
			}
		}(newVolumeIDs)

		volumesToBeCreated := totalVolumesToCreate - len(allVolumeIds)
		log.InfoD(fmt.Sprintf("Total number of new volumes to be created in the cluster [%v]", volumesToBeCreated))

		// Create volumes in the cluster till it reaches maximum count
		for initVol := 0; initVol < volumesToBeCreated; initVol++ {
			id := uuid.New()
			volName := fmt.Sprintf("volume_%s", id.String()[:8])
			log.InfoD(fmt.Sprintf("Volume [%v] will be created with name [%v]", initVol, volName))

			// get size of the volume from size 1GiB till 100GiB
			minSize := 1
			maxSize := 100
			randSize := uint64(rand.Intn(maxSize-minSize) + minSize)

			// Pick HA Update from 1 to 3
			haUpdate := int64(rand.Intn(3-1) + 1)

			volId, err := Inst().V.CreateVolume(volName, randSize, haUpdate)
			if err != nil {
				terminate = true
				log.FailOnError(err, fmt.Sprintf("Failed to create volume with vol Name [%v]", volName))
			}
			log.InfoD("Volume Created with ID [%v]", volId)
			newVolumeIDs = append(newVolumeIDs, volId)
		}

		// Validate Volume Attached status
		for _, eachVol := range attachedVolumes {
			vol, err := Inst().V.InspectVolume(eachVol)
			log.FailOnError(err, fmt.Sprintf("Inspect volume failed on volume [%v]", eachVol))
			dash.VerifyFatal(vol.State.String() == "VOLUME_STATE_ATTACHED", true,
				fmt.Sprintf(" volume [%v] state is [%v]", eachVol, vol.State.String()))
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})

// Volume replication change
var _ = Describe("{CreateDeleteVolumeKillKVDBMaster}", func() {
	var testrailID = 0
	// JIRA ID :https://portworx.atlassian.net/browse/PTX-17728
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("CreateDeleteVolumeKillKVDBMaster",
			"Create Delete volume in loop kill kvdb master node in random intervals", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "has to schedule apps and update replication factor for attached node"
	It(stepLog, func() {

		var wg sync.WaitGroup
		numGoroutines := 2

		wg.Add(numGoroutines)
		terminate := false

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("createmaxvolume-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)
		
		// Kill KVDB Master in regular interval
		kvdbMaster, err := GetKvdbMasterNode()
		log.FailOnError(err, "Getting KVDB Master Node details failed")
		log.InfoD("KVDB Master Node is [%v]", kvdbMaster.Name)

		// Go Routine to create volume continuously
		volumesCreated := []string{}

		stopRoutine := func() {
			if !terminate {
				terminate = true
				for _, each := range volumesCreated {
					log.FailOnError(Inst().V.DeleteVolume(each), "volume deletion failed on the cluster with volume ID [%s]", each)
				}
			}
		}
		defer stopRoutine()

		go func() {
			defer wg.Done()
			defer GinkgoRecover()
			for {
				if terminate {
					break
				}
				// Volume create continuously
				uuidObj := uuid.New()
				VolName := fmt.Sprintf("volume_%s", uuidObj.String())
				Size := uint64(rand.Intn(100) + 1)  // Size of the Volume between 1G to 100G
				haUpdate := int64(rand.Intn(2) + 1) // Size of the HA between 1 and 3

				volId, err := Inst().V.CreateVolume(VolName, Size, haUpdate)
				if err != nil {
					stopRoutine()
					log.FailOnError(err, "volume creation failed on the cluster with volume name [%s]", VolName)
				}

				volumesCreated = append(volumesCreated, volId)
			}
		}()

		// Go Routine to delete volume continuously in parallel to volume create
		go func() {
			defer wg.Done()
			defer GinkgoRecover()
			for {
				if terminate {
					break
				}
				if len(volumesCreated) > 5 {
					deleteVolume := volumesCreated[0]

					err := Inst().V.DeleteVolume(deleteVolume)
					if err != nil {
						stopRoutine()
						log.FailOnError(err,
							"volume deletion failed on the cluster with volume ID [%s]", deleteVolume)
					}

					// Remove the first element
					for i := 0; i < len(volumesCreated)-1; i++ {
						volumesCreated[i] = volumesCreated[i+1]
					}
					// Resize the array by truncating the last element
					volumesCreated = volumesCreated[:len(volumesCreated)-1]
				}
			}

		}()

		// Run KVDB Master Terminate / Volume Create / Delete continuously in parallel for latest one hour
		for i := 0; i < 10; i++ {
			// Wait for KVDB Members to be online
			log.FailOnError(WaitForKVDBMembers(), "failed waiting for KVDB members to be active")

			// Kill KVDB Master Node
			masterNode, err := GetKvdbMasterNode()
			log.FailOnError(err, "failed getting details of KVDB master node")

			// Get KVDB Master PID
			pid, err := GetKvdbMasterPID(*masterNode)
			log.FailOnError(err, "failed getting PID of KVDB master node")

			log.InfoD("KVDB Master is [%v] and PID is [%v]", masterNode.Name, pid)

			// Kill kvdb master PID for regular intervals
			log.FailOnError(KillKvdbMemberUsingPid(*masterNode), "failed to kill KVDB Node")

			// Wait for some time after killing kvdb master Node
			time.Sleep(5 * time.Minute)
		}
		terminate = true
		wg.Wait()

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})

var _ = Describe("{VolumeMultipleHAIncreaseVolResize}", func() {
	var testrailID = 0
	/*  Try Volume resize to 5 GB every time
	    Try HA Refactor of the volume
	    Try one HA node Reboot

		all the above 3 operations are done in parallel
	*/
	// JIRA ID :https://portworx.atlassian.net/browse/PWX-27123
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("VolumeMultipleHAIncreaseVolResize",
			"Px crashes when we perform multiple HAUpdate in a loop", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "Px crashes when we perform multiple HAUpdate in a loop"
	It(stepLog, func() {
		var wg sync.WaitGroup
		var driverNode *node.Node

		var volReplMap map[string]int64

		driverNode = nil

		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("volmulhaupvolr-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get a pool with running IO
		poolUUID, err := GetPoolIDWithIOs(contexts)
		log.FailOnError(err, "Failed to get pool running with IO")
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		// Get Node Details of the Pool with IO
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)
		log.InfoD("Pool with UUID [%v] present in Node [%v]", poolUUID, nodeDetail.Name)

		// Get All Volumes from the pool
		volumes, err := GetVolumesFromPoolID(contexts, poolUUID)
		log.FailOnError(err, "Failed to get list of volumes from the poolIDs")

		for _, each := range volumes {
			replFactor, err := Inst().V.GetReplicationFactor(each)
			log.FailOnError(err, "failed to get replication factor for volume [%v]", each.Name)
			volReplMap[each.ID] = replFactor
		}

		revertReplica := func() {
			for _, each := range volumes {
				for vID, replCount := range volReplMap {
					if each.ID == vID {
						replFactor, err := Inst().V.GetReplicationFactor(each)
						log.FailOnError(err, "failed to get replication factor for volume [%v]", each.Name)
						if replFactor != replCount {
							err = Inst().V.SetReplicationFactor(each, replCount, nil, nil, true)
							log.FailOnError(err, "failed to set replication factor for volume [%v]", each.Name)
						}
					}
				}
			}
		}

		defer revertReplica()

		numGoroutines := len(volumes)
		wg.Add(numGoroutines)

		terminate := false
		terminateflow := func() {
			terminate = true
		}

		waitTillDriverUp := func() {
			if driverNode != nil {
				err = Inst().V.WaitDriverUpOnNode(*nodeDetail, 10*time.Minute)
				if err != nil {
					terminateflow()
					log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", nodeDetail.Name))
				}
			}
		}
		defer waitTillDriverUp()

		log.InfoD("Initiate Volume resize continuously")
		volumeResize := func(vol *volume.Volume) error {

			apiVol, err := Inst().V.InspectVolume(vol.ID)
			if err != nil {
				terminateflow()
				return err
			}

			curSize := apiVol.Spec.Size
			newSize := curSize + (uint64(5) * units.GiB)
			log.Infof("Initiating volume size increase on volume [%v] by size [%v] to [%v]",
				vol.ID, curSize/units.GiB, newSize/units.GiB)

			err = Inst().V.ResizeVolume(vol.ID, newSize)
			if err != nil {
				terminateflow()
				return err
			}

			// Wait for 2 seconds for Volume to update stats
			time.Sleep(2 * time.Second)
			volumeInspect, err := Inst().V.InspectVolume(vol.ID)
			if err != nil {
				terminateflow()
				return err
			}

			updatedSize := volumeInspect.Spec.Size
			if updatedSize <= curSize {
				terminateflow()
				return fmt.Errorf("volume did not update from [%v] to [%v] ",
					curSize/units.GiB, updatedSize/units.GiB)
			}

			return nil
		}

		go func() {
			defer wg.Done()
			defer GinkgoRecover()
			for {
				if terminate == true {
					break
				}
				for _, eachVol := range volumes {
					err := volumeResize(eachVol)
					if err != nil {
						if strings.Contains(fmt.Sprintf("%v", err), "Resource has not been initialized") {
							waitTillDriverUp()
						} else {
							terminateflow()
							log.FailOnError(err, "failed to resize Volume  [%v]", eachVol.Name)
						}
					}
				}
			}
		}()

		log.InfoD("Trigger test to change replication factor of the volume continuously")
		previousReplFactor := int64(1)
		go func(vol []*volume.Volume) {
			defer wg.Done()
			defer GinkgoRecover()
			for {
				if terminate == true {
					break
				}

				// Change replication factor of the volume continuously once volume is resized
				for _, each := range vol {
					log.Infof("Changing replication factor of volume [%v]", each.Name)
					setReplFactor := int64(1)
					currRepFactor, err := Inst().V.GetReplicationFactor(each)
					if err != nil {
						terminateflow()
						log.FailOnError(err, "failed to get replication factor for volume [%v]", each.Name)
					}
					// Do HA Update based on current replication factor for the node
					// if repl factor is 3 reduce it by 1
					// if previous repl factor is 3 and current repl factor is 2 reduce it by 1
					// else increase repl factor by 1
					if currRepFactor == 3 {
						setReplFactor = currRepFactor - 1
					} else if currRepFactor == 2 || previousReplFactor == 3 {
						setReplFactor = setReplFactor
					} else {
						setReplFactor = currRepFactor + 1
					}
					previousReplFactor = currRepFactor

					log.Infof("Setting replication factor on volume [%v] from [%v] to [%v]", each.Name, currRepFactor, setReplFactor)
					err = Inst().V.SetReplicationFactor(each, setReplFactor, nil, nil, true)
					if err != nil {
						if strings.Contains(fmt.Sprintf("%v", err), "Another HA increase operation is in progress") {
							continue
						} else {
							terminateflow()
							log.FailOnError(err, "failed to set replication factor for volume [%v]", each.Name)
						}
					}
				}
			}
		}(volumes)

		for i := 0; i < 2; i++ {
			// Pick a random volume
			randomIndex := rand.Intn(len(volumes))
			volPicked := volumes[randomIndex]

			// Pick a Node on which volume is placed and start rebooting the node
			poolIds, err := GetPoolIDsFromVolName(volPicked.ID)
			if err != nil {
				terminateflow()
				log.FailOnError(err, "failed to get pool details from the volume")
			}

			// select random pool and get the node associated with that pool
			randomIndex = rand.Intn(len(poolIds))
			poolPicked := poolIds[randomIndex]

			nodeDetail, err := GetNodeWithGivenPoolID(poolPicked)
			if err != nil {
				terminateflow()
				log.FailOnError(err, "error while fetching node details from pool ID")
			}
			log.InfoD("Restarting Px on Node [%v] and waiting for the Px to come back online", nodeDetail.Name)

			driverNode = nodeDetail
			err = Inst().V.RestartDriver(*nodeDetail, nil)
			if err != nil {
				terminateflow()
				log.FailOnError(err, fmt.Sprintf("error restarting px on node %s", nodeDetail.Name))
			}

			waitTillDriverUp()

			// flag is to make sure to wait for driver to be up and running when because
			// of some other process test terminates in middle
			driverNode = nil
		}
		terminate = true
		wg.Wait()

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})
