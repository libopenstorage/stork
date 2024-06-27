package tests

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/portworx/torpedo/drivers/volume/portworx"
	"github.com/portworx/torpedo/pkg/restutil"
	"math"
	"math/rand"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/libopenstorage/openstorage/api"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/pkg/log"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	opsapi "github.com/libopenstorage/openstorage/api"
	"github.com/portworx/torpedo/pkg/testrailuttils"

	. "github.com/onsi/ginkgo/v2"
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
	fio                     = "fio-throttle-io"
	fastpathAppName         = "fastpath"
	fioPVScheduleName       = "tc-cs-volsnapsched"
	fioOutputPVScheduleName = "tc-cs-volsnapsched-2"
)

type volumeDataMap struct {
	UsedSize         float64 `json:"UsedSize"`
	PoolID           int     `json:"PoolID"`
	ClusterID        string  `json:"ClusterID"`
	TotalRestoreSize float64 `json:"TotalRestoreSize"`
}

type CloudBackupSizeAPI struct {
	Size                  string `json:"size"`
	TotalDownloadBytes    string `json:"total_download_bytes"`
	CompressedObjectBytes string `json:"compressed_object_bytes"`
	Capacity              string `json:"capacity_required_for_restore"`
}

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
					if _, ok := v.Labels[k8s.PureDAVolumeLabel]; ok {
						// This is a Pure Direct Access volume, which will not support repl updates.
						// Ensure the command fails.
						Step(fmt.Sprintf("ensure repl update fails on Pure Direct Access on app %s's volume: %v", ctx.App.Key, v), func() {
							appNodes, err := Inst().S.GetNodesForApp(ctx)
							log.FailOnError(err, "Failed to get nodes for app %s", ctx.App.Key)
							dash.VerifyFatal(len(appNodes) > 0, true, "App nodes exist ?")

							err = Inst().V.SetReplicationFactor(v, 3, []string{appNodes[0].VolDriverNodeID}, nil, true)
							dash.VerifyFatal(err != nil, true, "Repl update failed (as expected) for Pure DA volumes?")
							dash.VerifyFatal(strings.Contains(err.Error(), "not supported for Pure"), true, "Repl update error for Pure DA volumes contains proper string?")
						})
						continue
					}

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
							log.FailOnError(err, fmt.Sprintf("Failed to get node for vol %s", v.Name))

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
							opts := volume.Options{
								ValidateReplicationUpdateTimeout: validateReplicationUpdateTimeout,
							}
							err = Inst().V.SetReplicationFactor(v, currRep+1, updateReplicaSet, nil, true, opts)
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
				if IsVolumeExits(each) {
					log.InfoD(fmt.Sprintf("delete volume [%v]", each))
					log.FailOnError(Inst().V.DetachVolume(each), fmt.Sprintf("Failed to detach volume [%v]", each))
					time.Sleep(500 * time.Millisecond)
					log.FailOnError(Inst().V.DeleteVolume(each), fmt.Sprintf("Delete volume with ID [%v] failed", each))
				}
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

	stepLog := "Continuously creates and deletes volume while killing kvdb master node"
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
					if IsVolumeExits(each) {
						log.FailOnError(Inst().V.DeleteVolume(each), "volume deletion failed on the cluster with volume ID [%s]", each)
					}
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

					if IsVolumeExits(deleteVolume) {
						err := Inst().V.DeleteVolume(deleteVolume)
						if err != nil {
							stopRoutine()
							log.FailOnError(err,
								"volume deletion failed on the cluster with volume ID [%s]", deleteVolume)
						}
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

	stepLog := "Px crashes when we perform multiple HAUpdate in a loop"
	It(stepLog, func() {
		var wg sync.WaitGroup
		var driverNode *node.Node

		volReplMap := make(map[string]int64)

		driverNode = nil

		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("volmulhaupvolr-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get a pool with running IO
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_AUTO, 0)
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

		// Function to Set replication factor on Volume
		setReplOnVolume := func(volName *volume.Volume, replCount int64, waitToFinish bool) error {
			err = Inst().V.SetReplicationFactor(volName, replCount, nil, nil, waitToFinish)
			if err != nil {
				if strings.Contains(fmt.Sprintf("%v", err), "Another HA increase operation is in progress") {
					return nil
				} else if strings.Contains(fmt.Sprintf("%v", err), "Resource has not been initialized") {
					waitTillDriverUp()
					err = Inst().V.SetReplicationFactor(volName, replCount, nil, nil, waitToFinish)
					if err != nil {
						return err
					}
				} else {
					return err
				}

			}
			return nil
		}

		revertReplica := func() {
			log.Info("Reverting Replica on the volumes")
			waitTillDriverUp()
			for _, each := range volumes {
				for vID, replCount := range volReplMap {
					if each.ID == vID {
						replFactor, err := Inst().V.GetReplicationFactor(each)
						log.FailOnError(err, "failed to get replication factor for volume [%v]", each.Name)
						if replFactor != replCount {
							err := setReplOnVolume(each, replCount, true)
							log.FailOnError(err, "failed to set replication factor for volume [%v]", each.Name)
						}
					}
				}
			}
		}

		defer revertReplica()

		wg.Add(2)
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
				if terminate {
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
				if terminate {
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
					err = setReplOnVolume(each, setReplFactor, true)
					log.FailOnError(err, "failed to set replication factor for volume after waiting for Px online [%v]", each.Name)
				}
			}

		}(volumes)

		for i := 0; i < 5; i++ {
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
		terminateflow()
		wg.Wait()

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{CloudsnapAndRestore}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("CloudsnapAndRestore", "Validate cloudsnap creation and restore", nil, 0)
	})

	var contexts []*scheduler.Context
	stepLog := "has to schedule apps, create scheduled cloud snap and restore it"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		retain := 8
		interval := 4

		err := CreatePXCloudCredential()
		log.FailOnError(err, "failed to create cloud credential")

		n := node.GetStorageDriverNodes()[0]
		uuidCmd := "pxctl cred list -j | grep uuid"
		output, err := runCmd(uuidCmd, n)
		log.FailOnError(err, "error getting uuid for cloudsnap credential")
		if output == "" {
			log.FailOnError(fmt.Errorf("cloud cred is not created"), "Check for cloud cred exists?")
		}

		credUUID := strings.Split(strings.TrimSpace(output), " ")[1]
		credUUID = strings.ReplaceAll(credUUID, "\"", "")
		log.Infof("Got Cred UUID: %s", credUUID)
		contexts = make([]*scheduler.Context, 0)
		policyName := "intervalpolicy"
		stepLog = fmt.Sprintf("create schedule policy %s", policyName)

		Step(stepLog, func() {
			log.InfoD(stepLog)

			schedPolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
			if err != nil {

				log.InfoD("Creating a interval schedule policy %v with interval %v minutes", policyName, interval)
				schedPolicy = &storkv1.SchedulePolicy{
					ObjectMeta: meta_v1.ObjectMeta{
						Name: policyName,
					},
					Policy: storkv1.SchedulePolicyItem{
						Interval: &storkv1.IntervalPolicy{
							Retain:          storkv1.Retain(retain),
							IntervalMinutes: interval,
						},
					}}

				_, err = storkops.Instance().CreateSchedulePolicy(schedPolicy)
				log.FailOnError(err, fmt.Sprintf("error creating a SchedulePolicy [%s]", policyName))
			}

			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("cloudsnaprestore-%d", i))...)
			}

			ValidateApplications(contexts)

		})

		defer func() {
			err := storkops.Instance().DeleteSchedulePolicy(policyName)
			log.FailOnError(err, fmt.Sprintf("error deleting a SchedulePolicy [%s]", policyName))
		}()

		stepLog = "Verify that cloud snap status"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			for _, ctx := range contexts {
				if !strings.Contains(ctx.App.Key, "cloudsnap") {
					continue
				}
				var appVolumes []*volume.Volume
				var err error
				appNamespace := ctx.App.Key + "-" + ctx.UID
				log.Infof("Namespace: %v", appNamespace)
				stepLog = fmt.Sprintf("Getting app volumes for volume %s", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					appVolumes, err = Inst().S.GetVolumes(ctx)
					log.FailOnError(err, "error getting volumes for [%s]", ctx.App.Key)

					if len(appVolumes) == 0 {
						log.FailOnError(fmt.Errorf("no volumes found for [%s]", ctx.App.Key), "error getting volumes for [%s]", ctx.App.Key)
					}
				})
				log.Infof("Got volume count : %v", len(appVolumes))
				scaleFactor := time.Duration(Inst().GlobalScaleFactor * len(appVolumes))
				err = Inst().S.ValidateVolumes(ctx, scaleFactor*4*time.Minute, defaultRetryInterval, nil)
				log.FailOnError(err, "error validating volumes for [%s]", ctx.App.Key)
				for _, v := range appVolumes {

					isPureVol, err := Inst().V.IsPureVolume(v)
					log.FailOnError(err, "error checking if volume is pure volume")
					if isPureVol {
						log.Warnf("Cloud snapshot is not supported for Pure DA volumes: [%s],Skipping cloud snapshot trigger for pure volume.", v.Name)
						continue
					}

					snapshotScheduleName := v.Name + "-interval-schedule"
					log.InfoD("snapshotScheduleName : %v for volume: %s", snapshotScheduleName, v.Name)

					resp, err := storkops.Instance().GetSnapshotSchedule(snapshotScheduleName, appNamespace)
					log.FailOnError(err, fmt.Sprintf("error getting snapshot schedule for [%s], volume:[%s] in namespace [%s]", snapshotScheduleName, v.Name, v.Namespace))
					dash.VerifyFatal(len(resp.Status.Items) > 0, true, fmt.Sprintf("verify snapshots exists for [%s]", snapshotScheduleName))
					for _, snapshotStatuses := range resp.Status.Items {
						if len(snapshotStatuses) > 0 {
							status := snapshotStatuses[len(snapshotStatuses)-1]
							if status == nil {
								log.FailOnError(fmt.Errorf("SnapshotSchedule has an empty migration in it's most recent status"), fmt.Sprintf("error getting latest snapshot status for [%s]", snapshotScheduleName))
							}
							status, err = WaitForSnapShotToReady(snapshotScheduleName, status.Name, appNamespace)
							log.Infof("Snapshot [%s] has status [%v]", status.Name, status.Status)
							if status.Status == snapv1.VolumeSnapshotConditionError {
								resp, _ := storkops.Instance().GetSnapshotSchedule(snapshotScheduleName, appNamespace)
								log.Infof("SnapshotSchedule resp: %v", resp)
								snapData, _ := Inst().S.GetSnapShotData(ctx, status.Name, appNamespace)
								log.Infof("snapData : %v", snapData)
								log.FailOnError(fmt.Errorf("snapshot: %s failed. status: [%v]", status.Name, status.Status), fmt.Sprintf("cloud snapshot for [%s] failed", snapshotScheduleName))
							}
							if status.Status == snapv1.VolumeSnapshotConditionPending {
								log.FailOnError(fmt.Errorf("snapshot: %s not completed. status: [%v]", status.Name, status.Status), fmt.Sprintf("cloud snapshot for [%s] stuck in pending state", snapshotScheduleName))
							}
							if status.Status == snapv1.VolumeSnapshotConditionReady {
								snapData, err := Inst().S.GetSnapShotData(ctx, status.Name, appNamespace)
								log.FailOnError(err, fmt.Sprintf("error getting snapshot data for [%s/%s]", appNamespace, status.Name))

								snapType := snapData.Spec.PortworxSnapshot.SnapshotType
								log.Infof("Snapshot Type: %v", snapType)
								if snapType != "cloud" {
									err = &scheduler.ErrFailedToGetVolumeParameters{
										App:   ctx.App,
										Cause: fmt.Sprintf("Snapshot Type: [%s] does not match", snapType),
									}
									log.FailOnError(err, fmt.Sprintf("error validating snapshot data for [%s/%s]", appNamespace, status.Name))
								}
								condition := snapData.Status.Conditions[0]
								dash.VerifyFatal(condition.Type == snapv1.VolumeSnapshotDataConditionReady, true, fmt.Sprintf("validate volume snapshot condition data for [%s] expteced: [%v], actual [%v]", status.Name, snapv1.VolumeSnapshotDataConditionReady, condition.Type))

								snapID := snapData.Spec.PortworxSnapshot.SnapshotID
								log.Infof("Snapshot ID: %v", snapID)
								if snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot == nil ||
									len(snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot.SnapshotID) == 0 {
									err = &scheduler.ErrFailedToGetVolumeParameters{
										App:   ctx.App,
										Cause: fmt.Sprintf("volumesnapshotdata: %s does not have portworx volume source set", snapData.Metadata.Name),
									}
									log.FailOnError(err, fmt.Sprintf("error validating snapshot data for [%s/%s]", appNamespace, status.Name))
								}
							}
						}
					}
				}
			}
		})

		stepLog = "Validating cloud snapshot backup size values"
		Step(stepLog, func() {
			for _, ctx := range contexts {
				if !strings.Contains(ctx.App.Key, "cloudsnap") {
					continue
				}
				// Validate the cloud snapshot backup size values [PTX-17342]
				log.Infof("Validating cloud snapshot backup size values for app [%s]", ctx.App.Key)
				vols, err := Inst().S.GetVolumeParameters(ctx)
				log.FailOnError(err, fmt.Sprintf("error getting volume params for [%s]", ctx.App.Key))
				for vol, params := range vols {
					dash.VerifyFatal(validateCloudSnapValues(credUUID, vol, params), true, fmt.Sprintf("validate cloud snap values for volume [%s]", vol))
				}
			}
		})

		stepLog = "Update volume io_profiles on all volumes"
		Step(stepLog, func() {
			for _, ctx := range contexts {

				appVols, err := Inst().S.GetVolumes(ctx)
				log.FailOnError(err, "error getting volumes for [%s]", ctx.App.Key)

				for _, v := range appVols {
					var volumeSpec *api.VolumeSpecUpdate
					inspectVolume, err := Inst().V.InspectVolume(v.ID)
					log.FailOnError(err, fmt.Sprintf("error inspecting volume %s", v.ID))
					newIOProfile := api.IoProfile_IO_PROFILE_JOURNAL
					if inspectVolume.DerivedIoProfile != api.IoProfile_IO_PROFILE_JOURNAL {
						volumeSpec = &api.VolumeSpecUpdate{IoProfileOpt: &api.VolumeSpecUpdate_IoProfile{IoProfile: newIOProfile}}
					} else {
						newIOProfile = api.IoProfile_IO_PROFILE_AUTO
						volumeSpec = &api.VolumeSpecUpdate{IoProfileOpt: &api.VolumeSpecUpdate_IoProfile{IoProfile: newIOProfile}}
					}
					err = Inst().V.UpdateVolumeSpec(v, volumeSpec)
					log.FailOnError(err, fmt.Sprintf("failed to update io profile to %v for volume %s", newIOProfile, v.ID))
				}

				ctx.SkipVolumeValidation = true
				ValidateContext(ctx)

			}

		})

		stepLog = "Verify cloud snap restore"
		Step(stepLog, func() {
			for _, ctx := range contexts {

				if strings.Contains(ctx.App.Key, "cloudsnap") {

					appNamespace := ctx.App.Key + "-" + ctx.UID
					snapSchedList, err := storkops.Instance().ListSnapshotSchedules(appNamespace)
					log.FailOnError(err, "error getting snapshot list")

					vols, err := Inst().S.GetVolumes(ctx)
					log.FailOnError(err, "error getting volumes")

					for _, vol := range vols {
						var snapshotScheduleName string
						for _, snap := range snapSchedList.Items {
							snapshotScheduleName = snap.Name
							if strings.Contains(snapshotScheduleName, vol.Name) {
								break
							}
						}
						resp, err := storkops.Instance().GetSnapshotSchedule(snapshotScheduleName, appNamespace)
						log.FailOnError(err, "error getting snapshot schedule for [%s] in namespace [%s]", snapshotScheduleName, appNamespace)
						var volumeSnapshotStatus *storkv1.ScheduledVolumeSnapshotStatus
					outer:
						for _, snapshotStatuses := range resp.Status.Items {
							for _, vsStatus := range snapshotStatuses {
								if vsStatus.Status == snapv1.VolumeSnapshotConditionReady {
									volumeSnapshotStatus = vsStatus
									break outer
								}
							}
						}
						if volumeSnapshotStatus != nil {
							restoreSpec := &storkv1.VolumeSnapshotRestore{ObjectMeta: meta_v1.ObjectMeta{
								Name:      vol.Name,
								Namespace: vol.Namespace,
							}, Spec: storkv1.VolumeSnapshotRestoreSpec{SourceName: volumeSnapshotStatus.Name, SourceNamespace: appNamespace, GroupSnapshot: false}}
							restore, err := storkops.Instance().CreateVolumeSnapshotRestore(restoreSpec)
							log.FailOnError(err, "error CreateVolumeSnapshotRestore")
							err = storkops.Instance().ValidateVolumeSnapshotRestore(restore.Name, restore.Namespace, snapshotScheduleRetryTimeout, snapshotScheduleRetryInterval)
							dash.VerifySafely(err, nil, fmt.Sprintf("validate snapshot restore source: %s , destnation: %s in namespace %s", restore.Name, vol.Name, vol.Namespace))
							if err == nil {
								err = storkops.Instance().DeleteVolumeSnapshotRestore(restore.Name, restore.Namespace)
								log.FailOnError(err, "error deleting volume snapshot restore object")
							}
						} else {
							log.FailOnError(fmt.Errorf("no snapshot with Ready status found for vol[%s] in namespace[%s]", vol.Name, vol.Namespace), "error getting volume snapshot")
						}
					}
				}
			}
		})

		stepLog = "Validating apps after cloudsnap restore"
		Step(stepLog, func() {

			for _, ctx := range contexts {
				ctx.ReadinessTimeout = 15 * time.Minute
				//skipping volume validation as ip_profiles are updated
				ctx.SkipVolumeValidation = true
				ValidateContext(ctx)
			}
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		bucketName, err := GetCloudsnapBucketName(contexts)
		log.FailOnError(err, "error getting cloud snap bucket name")
		opts := make(map[string]bool)
		DestroyApps(contexts, opts)
		err = DeleteCloudSnapBucket(bucketName)
		log.FailOnError(err, "error deleting cloud snap bucket")
		AfterEachTest(contexts)
	})
})

var _ = Describe("{LocalsnapAndRestore}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("LocalsnapAndRestore", "Validate localsnap creation and restore", nil, 0)
	})

	var contexts []*scheduler.Context
	stepLog := "has to schedule apps, create scheduled local snap and restore it"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		retain := 8
		interval := 3

		contexts = make([]*scheduler.Context, 0)
		policyName := "localintervalpolicy"
		stepLog = fmt.Sprintf("create schedule policy %s for local snapshots", policyName)

		Step(stepLog, func() {
			log.InfoD(stepLog)

			schedPolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
			if err != nil {

				log.InfoD("Creating a interval schedule policy %v with interval %v minutes", policyName, interval)
				schedPolicy = &storkv1.SchedulePolicy{
					ObjectMeta: meta_v1.ObjectMeta{
						Name: policyName,
					},
					Policy: storkv1.SchedulePolicyItem{
						Interval: &storkv1.IntervalPolicy{
							Retain:          storkv1.Retain(retain),
							IntervalMinutes: interval,
						},
					}}

				_, err = storkops.Instance().CreateSchedulePolicy(schedPolicy)
				log.FailOnError(err, fmt.Sprintf("error creating a SchedulePolicy [%s]", policyName))
			}

			appList := Inst().AppList

			defer func() {

				Inst().AppList = appList

			}()

			Inst().AppList = []string{"fio-localsnap"}

			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("localsnaprestore-%d", i))...)
			}

			ValidateApplications(contexts)

		})
		volSnapMap := make(map[string]map[*volume.Volume]*storkv1.ScheduledVolumeSnapshotStatus)

		stepLog = "Verify that local snap status"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				var err error
				appNamespace := ctx.App.Key + "-" + ctx.UID
				log.Infof("Namespace: %v", appNamespace)
				stepLog = fmt.Sprintf("Getting app volumes for volume %s", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					appVolumes, err = Inst().S.GetVolumes(ctx)
					log.FailOnError(err, "error getting volumes for [%s]", ctx.App.Key)

					if len(appVolumes) == 0 {
						log.FailOnError(fmt.Errorf("no volumes found for [%s]", ctx.App.Key), "error getting volumes for [%s]", ctx.App.Key)
					}
				})
				log.Infof("Got volume count : %v", len(appVolumes))
				scaleFactor := time.Duration(Inst().GlobalScaleFactor * len(appVolumes))
				err = Inst().S.ValidateVolumes(ctx, scaleFactor*4*time.Minute, defaultRetryInterval, nil)
				log.FailOnError(err, "error validating volumes for [%s]", ctx.App.Key)
				snapMap := make(map[*volume.Volume]*storkv1.ScheduledVolumeSnapshotStatus)
				for _, v := range appVolumes {

					isPureVol, err := Inst().V.IsPureVolume(v)
					log.FailOnError(err, "error checking if volume is pure volume")
					if isPureVol {
						log.Warnf("Cloud snapshot is not supported for Pure DA volumes: [%s],Skipping cloud snapshot trigger for pure volume.", v.Name)
						continue
					}

					snapshotScheduleName := v.Name + "-interval-schedule"
					log.InfoD("snapshotScheduleName : %v for volume: %s", snapshotScheduleName, v.Name)

					var volumeSnapshotStatus *storkv1.ScheduledVolumeSnapshotStatus
					checkSnapshotSchedules := func() (interface{}, bool, error) {
						resp, err := storkops.Instance().GetSnapshotSchedule(snapshotScheduleName, appNamespace)
						if err != nil {
							return "", false, fmt.Errorf("error getting snapshot schedule for %s, volume:%s in namespace %s", snapshotScheduleName, v.Name, v.Namespace)
						}
						if len(resp.Status.Items) == 0 {
							return "", false, fmt.Errorf("no snapshot schedules found for %s, volume:%s in namespace %s", snapshotScheduleName, v.Name, v.Namespace)
						}

						for _, snapshotStatuses := range resp.Status.Items {
							if len(snapshotStatuses) > 0 {
								volumeSnapshotStatus = snapshotStatuses[len(snapshotStatuses)-1]
								if volumeSnapshotStatus == nil {
									return "", true, fmt.Errorf("SnapshotSchedule has an empty migration in it's most recent status")
								}
								if volumeSnapshotStatus.Status == snapv1.VolumeSnapshotConditionReady {
									return nil, false, nil
								}
								if volumeSnapshotStatus.Status == snapv1.VolumeSnapshotConditionError {
									return nil, false, fmt.Errorf("volume snapshot: %s failed. status: %v", volumeSnapshotStatus.Name, volumeSnapshotStatus.Status)
								}
								if volumeSnapshotStatus.Status == snapv1.VolumeSnapshotConditionPending {
									return nil, true, fmt.Errorf("volume Sanpshot %s is still pending", volumeSnapshotStatus.Name)
								}
							}
						}
						return nil, true, fmt.Errorf("volume Sanpshots for %s is not found", v.Name)
					}
					_, err = task.DoRetryWithTimeout(checkSnapshotSchedules, time.Duration(5*15)*defaultCommandTimeout, defaultReadynessTimeout)
					log.FailOnError(err, "error validating volume snapshot for %s", v.Name)

					snapMap[v] = volumeSnapshotStatus

					snapData, err := Inst().S.GetSnapShotData(ctx, volumeSnapshotStatus.Name, appNamespace)
					log.FailOnError(err, fmt.Sprintf("error getting snapshot data for [%s/%s]", appNamespace, volumeSnapshotStatus.Name))

					snapType := snapData.Spec.PortworxSnapshot.SnapshotType
					log.Infof("Snapshot Type: %v", snapType)
					if snapType != "local" {
						err = &scheduler.ErrFailedToGetVolumeParameters{
							App:   ctx.App,
							Cause: fmt.Sprintf("Snapshot Type: %s does not match", snapType),
						}
						log.FailOnError(err, fmt.Sprintf("error validating snapshot data for [%s/%s]", appNamespace, volumeSnapshotStatus.Name))
					}
					condition := snapData.Status.Conditions[0]
					dash.VerifyFatal(condition.Type == snapv1.VolumeSnapshotDataConditionReady, true, fmt.Sprintf("validate volume snapshot condition data for %s expteced: %v, actual %v", volumeSnapshotStatus.Name, snapv1.VolumeSnapshotDataConditionReady, condition.Type))

					snapID := snapData.Spec.PortworxSnapshot.SnapshotID
					log.Infof("Snapshot ID: %v", snapID)
					if snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot == nil ||
						len(snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot.SnapshotID) == 0 {
						err = &scheduler.ErrFailedToGetVolumeParameters{
							App:   ctx.App,
							Cause: fmt.Sprintf("volumesnapshotdata: %s does not have portworx volume source set", snapData.Metadata.Name),
						}
						log.FailOnError(err, fmt.Sprintf("error validating snapshot data for [%s/%s]", appNamespace, volumeSnapshotStatus.Name))
					}

				}
				volSnapMap[appNamespace] = snapMap
			}
			log.Infof("waiting for 10 mins to create multiple local snaps")
			time.Sleep(10 * time.Minute)
		})

		stepLog = "Update volume io_profiles on all volumes"
		Step(stepLog, func() {
			for _, ctx := range contexts {

				appVols, err := Inst().S.GetVolumes(ctx)
				log.FailOnError(err, "error getting volumes for [%s]", ctx.App.Key)

				for _, v := range appVols {
					var volumeSpec *api.VolumeSpecUpdate
					inspectVolume, err := Inst().V.InspectVolume(v.ID)
					log.FailOnError(err, fmt.Sprintf("error inspecting volume %s", v.ID))
					newIOProfile := api.IoProfile_IO_PROFILE_JOURNAL
					if inspectVolume.DerivedIoProfile != api.IoProfile_IO_PROFILE_JOURNAL {
						volumeSpec = &api.VolumeSpecUpdate{IoProfileOpt: &api.VolumeSpecUpdate_IoProfile{IoProfile: newIOProfile}}
					} else {
						newIOProfile = api.IoProfile_IO_PROFILE_AUTO
						volumeSpec = &api.VolumeSpecUpdate{IoProfileOpt: &api.VolumeSpecUpdate_IoProfile{IoProfile: newIOProfile}}
					}
					err = Inst().V.UpdateVolumeSpec(v, volumeSpec)
					log.FailOnError(err, fmt.Sprintf("failed to update io profile to %v for volume %s", newIOProfile, v.ID))
				}

				ctx.SkipVolumeValidation = true
				ValidateContext(ctx)

			}

		})

		stepLog = "Verify local snap restore"
		Step(stepLog, func() {
			for ns, volSnap := range volSnapMap {
				for vol, snap := range volSnap {
					restoreSpec := &storkv1.VolumeSnapshotRestore{ObjectMeta: meta_v1.ObjectMeta{
						Name:      vol.Name,
						Namespace: vol.Namespace,
					}, Spec: storkv1.VolumeSnapshotRestoreSpec{SourceName: snap.Name, SourceNamespace: ns, GroupSnapshot: false}}
					restore, err := storkops.Instance().CreateVolumeSnapshotRestore(restoreSpec)
					log.FailOnError(err, fmt.Sprintf("error creating volume snapshot restore for %s", snap.Name))
					err = storkops.Instance().ValidateVolumeSnapshotRestore(restore.Name, restore.Namespace, time.Duration(5*15)*defaultCommandTimeout, defaultReadynessTimeout)
					dash.VerifyFatal(err, nil, fmt.Sprintf("validate snapshot restore source: %s , destination: %s in namespace %s", restore.Name, vol.Name, vol.Namespace))
				}
			}

		})
		stepLog = "Validating and Destroying apps"
		Step(stepLog, func() {
			for _, ctx := range contexts {
				ctx.SkipVolumeValidation = false
				ctx.ReadinessTimeout = 15 * time.Minute
				ValidateContext(ctx)
				opts := make(map[string]bool)
				opts[SkipClusterScopedObjects] = true
				DestroyApps(contexts, opts)
			}
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{ResizeVolumeAfterFull}", func() {
	/*
		https://portworx.atlassian.net/browse/PTX-18927
		Fill volumes completely , then resize volume by 50%, verify IO on volumes in Longevity
	*/
	JustBeforeEach(func() {
		StartTorpedoTest("ResizeVolumeAfterFull",
			"Fill volumes completely , then resize volume by 50%, verify IO on volumes in Longrun script",
			nil, 0)
	})

	var contexts []*scheduler.Context
	stepLog := "Fill volumes completely , then resize volume by 50%, verify IO on volumes in Longrun script"
	It(stepLog, func() {
		contexts = make([]*scheduler.Context, 0)
		currAppList := Inst().AppList

		revertAppList := func() {
			Inst().AppList = currAppList
		}
		defer revertAppList()

		Inst().AppList = []string{}
		var ioIntensiveApp = []string{"vdbench-heavyload"}

		for _, eachApp := range ioIntensiveApp {
			Inst().AppList = append(Inst().AppList, eachApp)
		}

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("resizepoolfiftyper-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		log.Infof("Get all the list of available volumes with IO running")
		allVolumes, err := GetAllVolumesWithIO(contexts)
		log.FailOnError(err, "Failed to get volumes with IO Running")
		log.InfoD("List of all volumes with IO Running [%v]", allVolumes)

		// All Data volumes for Resize
		volumesToResize := []*volume.Volume{}
		for _, eachVol := range allVolumes {
			log.Infof("Checking volume with name [%v]", eachVol.Name)
			if eachVol.Name != "vdbench-pvc-output" {
				volumesToResize = append(volumesToResize, eachVol)
			}
		}
		dash.VerifyFatal(len(volumesToResize) > 0, true, "no volumes with IO for resize operations to continue")

		// Select Random Volumes for pool Expand
		randomIndex := rand.Intn(len(volumesToResize))
		randomVol := volumesToResize[randomIndex]

		waitForVolumeFull := func(volName *volume.Volume) error {
			waitTillVolume := func() (interface{}, bool, error) {
				volumeFull, err := IsVolumeFull(*volName)
				if err != nil {
					return nil, true, err
				}
				if volumeFull {
					return nil, false, nil
				}
				return nil, true, fmt.Errorf("Volume is still not full waiting.")
			}
			_, err := task.DoRetryWithTimeout(waitTillVolume, 2*time.Hour, 10*time.Second)
			return err
		}

		// Wait for Volume Full on the Node
		err = waitForVolumeFull(randomVol)
		log.FailOnError(err, "waiting for volume full on the node")

		// Expand Volume Size by 50%
		expectedSize := randomVol.Size + (randomVol.Size / 2)
		log.InfoD("Volume will be resized from [%v] to [%v]", randomVol.Size, expectedSize)
		log.FailOnError(Inst().V.ResizeVolume(randomVol.ID, expectedSize), "failed to Resize Volume")

		// Verify after Resize volume if IO is running
		isIOsInProgress, err := Inst().V.IsIOsInProgressForTheVolume(&node.GetStorageNodes()[0], randomVol.ID)
		log.FailOnError(err, "is io running on the volume?")
		dash.VerifyFatal(isIOsInProgress, true, fmt.Sprintf("no io running on the volume [%v] after resize", randomVol.Name))
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})

})

var _ = Describe("{CreateFastpathVolumeRebootNode}", func() {
	var testrailID = 0
	// JIRA ID : https://portworx.atlassian.net/browse/PTX-15700
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("CreateFastpathVolumeRebootNode",
			"Create fast path volume, reboot the node, check fastpath is active", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		log.Infof("The runID  %v ", runID)
	})

	var pxNode node.Node
	var contexts []*scheduler.Context
	var volumrlidttr []*api.Volume

	stepLog := "Create fastpath Volume reboot node and check if fastpath is active"
	It(stepLog, func() {
		applist := Inst().AppList
		log.InfoD(stepLog)
		revertAppList := func() {
			Inst().AppList = applist
		}
		defer revertAppList()
		stepLog = "Step 1: Get all the Storage nodes and select a node for test"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			// Get all the Nodes
			pxNodes, err := GetStorageNodes()
			log.FailOnError(err, "Unable to get the storage nodes")

			// Select random Storage node for the test
			if len(pxNodes) > 0 {
				pxNode = GetRandomNode(pxNodes)
			} else {
				log.FailOnError(errors.New("No Storage Node Availiable"), "Error occured while selecting StorageNode")
			}

			log.Infof("The Selected node for Fast path label is %v : ", pxNode.Name)

			// Remove if node-type label is set before the test
			err = RemoveLabelsAllNodes(k8s.NodeType, true, false)
			log.FailOnError(err, "error removing label on node ")
		})

		stepLog = "Step 2: Schedule application and Add label on the selected storage node"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			var err error

			// Add label on the selected node
			err = Inst().S.AddLabelOnNode(pxNode, k8s.NodeType, k8s.FastpathNodeType)
			log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", pxNode.Name))
			Inst().AppList = []string{"fio-fastpath-repl1"}
			contexts = make([]*scheduler.Context, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("fastpath-%d", i))...)
			}
			ValidateApplications(contexts)
		})

		stepLog = " Step 3: Get app volumes and Check fast path is active on the node"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			var err error
			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					appVolumes, err = Inst().S.GetVolumes(ctx)

					// Get the app volume details
					log.FailOnError(err, "Failed to get volumes for app %s", ctx.App.Key)
					dash.VerifyFatal(len(appVolumes) > 0, true, "App volumes exist?")
					log.Infof("App volumes details: %v ", appVolumes)

					// Store the volume details
					for _, vol := range appVolumes {
						apivol, err := Inst().V.InspectVolume(vol.ID)
						log.FailOnError(err, "failed to inspect volume %v", vol.ID)
						volumrlidttr = append(volumrlidttr, apivol)
					}

					// Loop through the apps and check if the volumes are fastpath active before reboot
					for _, appvolume := range appVolumes {
						log.Infof("current volume : %v", appvolume.Name)
						if strings.Contains(ctx.App.Key, fastpathAppName) {
							err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_ACTIVE)
							log.FailOnError(err, "fastpath volume validation failed")
						}
					}
				})
			}
		})
		stepLog = " Step 4: Reboot the node wait for it to complete"
		Step(stepLog, func() {
			var err error
			var volExists bool
			log.Infof(" Before reboot check if the volumes are attached on the local node")
			for _, volumePtr := range volumrlidttr {
				volExists, err = Inst().V.IsVolumeAttachedOnNode(volumePtr, pxNode)
				log.FailOnError(err, "Volume attached on local node validation failed")
				if !volExists {
					log.FailOnError(errors.New("Error occured while inspecting volume "), " Volume attached on local node validation failed")
				}
			}
			log.Infof("The volumes were found attached on local node: %v", pxNode.Name)
			log.InfoD(stepLog)
			rebootNodeAndWaitForReady(&pxNode)
		})

		stepLog = " Step 5: Get app volumes and Check fast path is active on the node"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			var err error
			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					appVolumes, err = Inst().S.GetVolumes(ctx)

					// Get the app volume details
					log.FailOnError(err, "Failed to get volumes for app %s", ctx.App.Key)
					dash.VerifyFatal(len(appVolumes) > 0, true, "App volumes exist?")
					log.Infof("App volumes details: %v ", appVolumes)

					// Loop through the apps and check if the volumes are fastpath active after the reboot
					for _, appvolume := range appVolumes {
						log.Infof("current volume : %s", appvolume.Name)
						if strings.Contains(ctx.App.Key, fastpathAppName) {
							err := ValidateFastpathVolume(ctx, opsapi.FastpathStatus_FASTPATH_ACTIVE)
							log.FailOnError(err, "fastpath volume validation failed")
						}
					}

				})
			}

		})
		stepLog = " Step 6: Remove the Label from the selected node"
		Step(stepLog, func() {
			var err error
			log.InfoD(stepLog)
			err = Inst().S.RemoveLabelOnNode(pxNode, k8s.NodeType)
			log.FailOnError(err, "error removing label on node [%s]", pxNode.Name)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{TrashcanRecoveryWithCloudsnap}", func() {
	/*
		1) Create volumes
		2) Put the volume in resync state
		3) While some volumes are in resync, delete all volumes.
		4) Make sure all the volumes are in the trashcan.
		5) Recover all volumes from trashcan and Verify the volumes are restored correctly.
		6) Validate the data and verify cloudsnap schedules continues after restore

	*/
	JustBeforeEach(func() {
		StartTorpedoTest("TrashcanRecoveryWithCloudsnap", "Validate the successful restore from Trashcan when volumes got deleted in resync state", nil, 0)
	})

	stepLog := "Validate the successful restore from Trashcan of volume in resync"
	It(stepLog, func() {
		log.InfoD(stepLog)
		err := CreatePXCloudCredential()
		log.FailOnError(err, "failed to create cloud credential")

		stepLog = "Enable Trashcan"
		Step(stepLog,
			func() {
				log.InfoD(stepLog)
				currNode := node.GetStorageDriverNodes()[0]
				err := Inst().V.SetClusterOptsWithConfirmation(currNode, map[string]string{
					"--volume-expiration-minutes": "600",
				})
				log.FailOnError(err, "error while enabling trashcan")
				log.InfoD("Trashcan is successfully enabled")
			})
		stepLog = "Create schedule policy"
		policyName := "intervalpolicy"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			schedPolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
			if err != nil {
				retain := 3
				interval := 3
				log.InfoD("Creating a interval schedule policy %v with interval %v minutes", policyName, interval)
				schedPolicy = &storkv1.SchedulePolicy{
					ObjectMeta: meta_v1.ObjectMeta{
						Name: policyName,
					},
					Policy: storkv1.SchedulePolicyItem{
						Interval: &storkv1.IntervalPolicy{
							Retain:          storkv1.Retain(retain),
							IntervalMinutes: interval,
						},
					}}

				_, err = storkops.Instance().CreateSchedulePolicy(schedPolicy)
				log.FailOnError(err, fmt.Sprintf("error creating a SchedulePolicy [%s]", policyName))
			}
		})

		defer func() {
			err := storkops.Instance().DeleteSchedulePolicy(policyName)
			log.FailOnError(err, fmt.Sprintf("error deleting a SchedulePolicy [%s]", policyName))

		}()
		fioPVC := "fio-pvc"
		fioPVName := "fio-pv"
		fioOutputPVC := "fio-output-pvc"
		fioOutputPVName := "fio-output-pv"

		appNamespace := fmt.Sprintf("tc-cs-%s", Inst().InstanceID)

		stepLog = fmt.Sprintf("create volumes %s and %s using volume request", fioPVName, fioOutputPVName)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			log.Infof("Creating volume : %s", fioPVName)
			pxctlCmdFull := fmt.Sprintf("v c %s -s 500 -r 2", fioPVName)
			output, err := Inst().V.GetPxctlCmdOutput(node.GetStorageNodes()[0], pxctlCmdFull)
			log.FailOnError(err, fmt.Sprintf("error creating volume %s", fioPVName))
			log.Infof(output)

			log.Infof("Creating volume : %s", fioOutputPVName)
			pxctlCmdFull = fmt.Sprintf("v c %s -s 50 -r 2", fioOutputPVName)
			output, err = Inst().V.GetPxctlCmdOutput(node.GetStorageNodes()[0], pxctlCmdFull)
			log.FailOnError(err, fmt.Sprintf("error creating volume %s", fioOutputPVName))
			log.Infof(output)
		})
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		Inst().AppList = []string{"fio-pod"}

		contexts = make([]*scheduler.Context, 0)
		actRepls := make(map[*volume.Volume]int64)

		log.InfoD("scheduling apps ")
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplicationsOnNamespace(appNamespace, fmt.Sprintf("trashrec-%d", i))...)
		}
		stepLog = fmt.Sprintf("Create volume snapshot schedule %s and %s", fioPVScheduleName, fioOutputPVScheduleName)
		Step(stepLog, func() {
			annotations := make(map[string]string)
			annotations["portworx/snapshot-type"] = "cloud"
			suspend := false
			log.InfoD(stepLog)

			tcVolSnapSched := &storkv1.VolumeSnapshotSchedule{
				ObjectMeta: meta_v1.ObjectMeta{
					Name:        fioPVScheduleName,
					Namespace:   appNamespace,
					Annotations: annotations,
				},
				Spec: storkv1.VolumeSnapshotScheduleSpec{
					SchedulePolicyName: policyName,
					Suspend:            &suspend,
					ReclaimPolicy:      storkv1.ReclaimPolicyDelete,
					Template: storkv1.VolumeSnapshotTemplateSpec{
						Spec: snapv1.VolumeSnapshotSpec{
							PersistentVolumeClaimName: fioPVC,
						},
					},
				},
			}

			volSnapshotSchedule, err := storkops.Instance().CreateSnapshotSchedule(tcVolSnapSched)
			log.FailOnError(err, fmt.Sprintf("error creating volume snapshot schedule %s", fioPVScheduleName))
			dash.VerifyFatal(volSnapshotSchedule.Name, fioPVScheduleName, "verify volume snapshot schedule is created successfully")

			tcVolSnapSched = &storkv1.VolumeSnapshotSchedule{
				ObjectMeta: meta_v1.ObjectMeta{
					Name:        fioOutputPVScheduleName,
					Namespace:   appNamespace,
					Annotations: annotations,
				},
				Spec: storkv1.VolumeSnapshotScheduleSpec{
					SchedulePolicyName: policyName,
					Suspend:            &suspend,
					ReclaimPolicy:      storkv1.ReclaimPolicyDelete,
					Template: storkv1.VolumeSnapshotTemplateSpec{
						Spec: snapv1.VolumeSnapshotSpec{
							PersistentVolumeClaimName: fioOutputPVC,
						},
					},
				},
			}

			volSnapshotSchedule, err = storkops.Instance().CreateSnapshotSchedule(tcVolSnapSched)
			log.FailOnError(err, fmt.Sprintf("error creating volume snapshot schedule %s", fioOutputPVScheduleName))
			dash.VerifyFatal(volSnapshotSchedule.Name, fioOutputPVScheduleName, "verify volume snapshot schedule is created successfully")
		})

		for _, ctx := range contexts {
			ctx.SkipVolumeValidation = true
			ValidateContext(ctx)
		}

		log.Infof("waiting for 10 mins for enough cloud snaps to be created.")
		time.Sleep(10 * time.Minute)
		initalSnaps, err := validateCloudSnaps(appNamespace)
		log.FailOnError(err, "error validating cloudsnaps")

		stepLog := "Scenario: Get volumes for cloudsnap apps in test,update replication factor,delete volumes then restore volumes from trashcan and validate cloudsnaps"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			for _, ctx := range contexts {

				//waiting for the data to be written before performing ha-update
				_, err := GetVolumeWithMinimumSize([]*scheduler.Context{ctx}, 50)
				log.FailOnError(err, "error selecting volumes for resync")

				testVolumes, err := Inst().S.GetVolumes(ctx)
				log.FailOnError(err, "Failed to get volumes for app %s", ctx.App.Key)
				dash.VerifyFatal(len(testVolumes) > 0, true, "App volumes exist?")

				appVolumes := make([]*volume.Volume, 0)
				//Getting volumes for repl update having minimum size
				for _, vol := range testVolumes {
					appVol, err := Inst().V.InspectVolume(vol.ID)
					log.FailOnError(err, fmt.Sprintf("error inspecting volume %s", vol.Name))
					usedBytes := appVol.GetUsage()
					usedGiB := usedBytes / units.GiB
					if usedGiB >= 50 {
						appVolumes = append(appVolumes, vol)
					}
				}

				//Reducing the repl factor if volume as max replication factor enabled
				stepLog = fmt.Sprintf("Adjusting the volume replications before increasing the repls for %s", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					err = replAdjust(appVolumes, actRepls)
					log.FailOnError(err, "Failed to adjust the repls for volumes of the app %s", ctx.App.Key)
				})

				stepLog = fmt.Sprintf("Increasing the volume repls for %s", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					newRepls := make(map[*volume.Volume]int64)
					for _, v := range appVolumes {
						currRep, err := Inst().V.GetReplicationFactor(v)
						log.FailOnError(err, "Failed to get volume  %s repl factor", v.Name)
						currAggr, err := Inst().V.GetAggregationLevel(v)
						log.FailOnError(err, "Failed to get volume  %s aggregate level", v.Name)
						numStorageNodes := len(node.GetStorageNodes())
						numStorageNodesRequired := int(currAggr * (currRep + 1))

						if numStorageNodes < numStorageNodesRequired {
							log.Warnf("skipping volume %s repl increase as numStorageNodesRequired is %d where as numStorageNodes is %d", v.Name, numStorageNodesRequired, numStorageNodes)
							continue
						}
						newRepls[v] = currRep + 1
					}
					err = setVolumeRepl(newRepls, false)
					dash.VerifyFatal(err, nil, fmt.Sprintf("validate successful repl increase for %s", ctx.App.Key))
				})

				log.InfoD("waiting for volumes to be in resync state")
				time.Sleep(2 * time.Minute)

				//waiting for all the volume is resync state
				for _, v := range appVolumes {

					checkVolumeState := func() (interface{}, bool, error) {
						runTimeState, err := GetVolumeReplicationStatus(v)
						if err != nil {
							return "", false, fmt.Errorf("error getting run time state for volume:%s. App : %s", v.Name, ctx.App.Key)
						}
						if strings.ToLower(runTimeState) == "resync" {
							return "", false, nil
						}

						return nil, true, fmt.Errorf("waiting for volume %s run time state to change to resync, current state: %s", v.Name, runTimeState)
					}
					_, err = task.DoRetryWithTimeout(checkVolumeState, time.Duration(5)*defaultCommandTimeout, 1*time.Minute)
					log.FailOnError(err, "error validating volume runtime state for %s", v.Name)

				}

				stepLog = fmt.Sprintf("Deleting app %s", ctx.App.Key)
				Step(stepLog, func() {
					DestroyApps(contexts, nil)
					log.FailOnError(deletePXVolume(fioPVName), fmt.Sprintf("error deleting portworx volume %s", fioPVName))
					log.FailOnError(deletePXVolume(fioOutputPVName), fmt.Sprintf("error deleting portworx volume %s", fioOutputPVName))
				})

				var trashcanVols []string
				stepLog = "validate volumes in trashcan"
				Step(stepLog, func() {
					// wait for few seconds for pvc to get deleted and volume to get detached
					time.Sleep(10 * time.Second)
					node := node.GetStorageDriverNodes()[0]
					log.InfoD(stepLog)
					trashcanVols, err = Inst().V.GetTrashCanVolumeIds(node)
					log.FailOnError(err, "error While getting trashcan volumes")
					log.Infof("trashcan len: %d", len(trashcanVols))
					dash.VerifyFatal(len(trashcanVols) > 0, true, "validate volumes exist in trashcan")

				})

				stepLog = "Validating trashcan restore"
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						for _, tID := range trashcanVols {
							if tID != "" {
								vol, err := Inst().V.InspectVolume(tID)
								log.FailOnError(err, fmt.Sprintf("error inspecting volume %s", tID))
								if strings.Contains(vol.Locator.Name, "fio-output-pv") {
									err = trashcanRestore(vol.Id, "fio-output-pv")
									log.FailOnError(err, fmt.Sprintf("error restoring volume %s from trashcan", vol.Id))
								}
								if strings.Contains(vol.Locator.Name, "fio-pv") {
									err = trashcanRestore(vol.Id, "fio-pv")
									log.FailOnError(err, fmt.Sprintf("error restoring volume %s from trashcan", vol.Id))
								}
							}
						}

					})
				log.InfoD("scheduling apps ")
				for i := 0; i < Inst().GlobalScaleFactor; i++ {
					contexts = append(contexts, ScheduleApplicationsOnNamespace(appNamespace, fmt.Sprintf("trashrec-%d", i))...)
				}
				for _, ctx := range contexts {
					ctx.SkipVolumeValidation = true
					ValidateContext(ctx)
				}

				log.Infof("waiting for 10 mins for enough cloud snaps to be created.")
				time.Sleep(10 * time.Minute)
				restoredSnaps, err := validateCloudSnaps(appNamespace)
				log.FailOnError(err, "error validating cloudsnaps")
				dash.VerifyFatal(len(restoredSnaps) > 0, true, "verify cloudsnaps are created.")
				for k := range restoredSnaps {
					dash.VerifySafely(initalSnaps[k] != restoredSnaps[k], true, fmt.Sprintf("verfiy new snaps are created for schedule %s", k))
				}

			}

		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		bucketName, err := GetCloudsnapBucketName(contexts)
		log.FailOnError(err, "error getting cloud snap bucket name")
		opts := make(map[string]bool)
		DestroyApps(contexts, opts)
		err = DeleteCloudSnapBucket(bucketName)
		log.FailOnError(err, "error deleting cloud snap bucket")
		AfterEachTest(contexts)
	})
})

func replAdjust(appVolumes []*volume.Volume, actRepls map[*volume.Volume]int64) error {
	setRepls := make(map[*volume.Volume]int64)

	for _, v := range appVolumes {
		currRep, err := Inst().V.GetReplicationFactor(v)
		if err != nil {
			return err
		}
		actRepls[v] = currRep
		if currRep == 3 {
			setRepls[v] = currRep - 1
		}
	}
	err := setVolumeRepl(setRepls, true)
	return err
}

func setVolumeRepl(setRepls map[*volume.Volume]int64, waitToFinish bool) error {

	for v, r := range setRepls {
		log.InfoD("setting repl for volume %s to %d", v.ID, r)
		err := Inst().V.SetReplicationFactor(v, r, nil, nil, waitToFinish)
		if err != nil {
			return err
		}

	}

	return nil
}

func validateCloudSnaps(appNamespace string) (map[string]string, error) {

	log.Infof("Verify that cloud snap status")
	snapsMap := make(map[string]string, 0)

	for _, ctx := range contexts {
		if strings.Contains(ctx.App.Key, "cloudsnap") || strings.Contains(ctx.App.Key, "fastpath") {

			var appVolumes []*volume.Volume
			var err error

			appVolumes, err = Inst().S.GetVolumes(ctx)
			log.FailOnError(err, "error getting volumes for [%s]", ctx.App.Key)
			if err != nil {
				return snapsMap, err
			}

			if len(appVolumes) == 0 {
				return snapsMap, fmt.Errorf("no volumes found for [%s]", ctx.App.Key)
			}

			log.Infof("Got volume count : %v", len(appVolumes))

			err = Inst().S.ValidateVolumes(ctx, 4*time.Minute, defaultRetryInterval, nil)
			log.FailOnError(err, "error validating volumes for [%s]", ctx.App.Key)
			if err != nil {
				return snapsMap, err
			}

			for _, v := range appVolumes {
				isPureVol, err := Inst().V.IsPureVolume(v)
				if err != nil {
					return snapsMap, err
				}

				if isPureVol {
					log.Warnf("Cloud snapshot is not supported for Pure DA volumes: [%s],Skipping cloud snapshot trigger for pure volume.", v.Name)
					continue
				}
				snapshotScheduleName := ""
				if v.Name == "fio-pvc" {
					snapshotScheduleName = fioPVScheduleName
				} else if v.Name == "fio-output-pvc" {
					snapshotScheduleName = fioOutputPVScheduleName
				} else {
					snapshotScheduleName = v.Name + "-interval-schedule"
				}

				log.InfoD("snapshotScheduleName : %v for volume: %s", snapshotScheduleName, v.Name)
				resp, err := storkops.Instance().GetSnapshotSchedule(snapshotScheduleName, appNamespace)
				if err != nil {
					return snapsMap, err
				}

				dash.VerifyFatal(len(resp.Status.Items) > 0, true, fmt.Sprintf("verify snapshots exists for %s", snapshotScheduleName))
				for _, snapshotStatuses := range resp.Status.Items {
					if len(snapshotStatuses) > 0 {
						status := snapshotStatuses[len(snapshotStatuses)-1]
						if status == nil {
							return snapsMap, fmt.Errorf("snapshotSchedule has an empty migration in it's most recent status,Err: %v", err)
						}
						status, err = WaitForSnapShotToReady(snapshotScheduleName, status.Name, appNamespace)
						if err != nil {
							return snapsMap, err
						}
						log.Infof("Snapshot %s has status %v", status.Name, status.Status)

						if status.Status == snapv1.VolumeSnapshotConditionError {
							return snapsMap, fmt.Errorf("snapshot: %s failed. status: %v", status.Name, status.Status)
						}

						if status.Status == snapv1.VolumeSnapshotConditionPending {
							return snapsMap, fmt.Errorf("snapshot: %s not completed. status: %v", status.Name, status.Status)
						}

						if status.Status == snapv1.VolumeSnapshotConditionReady {
							snapData, err := Inst().S.GetSnapShotData(ctx, status.Name, appNamespace)

							if err != nil {
								return snapsMap, err
							}

							snapType := snapData.Spec.PortworxSnapshot.SnapshotType
							log.Infof("Snapshot Type: %v", snapType)
							if snapType != "cloud" {
								err = &scheduler.ErrFailedToGetVolumeParameters{
									App:   ctx.App,
									Cause: fmt.Sprintf("Snapshot Type: %s does not match", snapType),
								}
								return snapsMap, err
							}

							snapID := snapData.Spec.PortworxSnapshot.SnapshotID
							log.Infof("Snapshot ID: %v", snapID)
							if snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot == nil ||
								len(snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot.SnapshotID) == 0 {
								err = &scheduler.ErrFailedToGetVolumeParameters{
									App:   ctx.App,
									Cause: fmt.Sprintf("volumesnapshotdata: %s does not have portworx volume source set", snapData.Metadata.Name),
								}
								return snapsMap, err
							}

						}
						snapsMap[snapshotScheduleName] = status.Name

					}
				}

			}
		}
	}
	return snapsMap, nil
}

func validateCloudSnapValues(credUUID string, volName string, params map[string]string) bool {
	log.InfoD("Validating snapshot values")
	cSnaps, err := Inst().V.GetCloudsnaps(volName, params)
	if err != nil || len(cSnaps) == 0 {
		log.FailOnError(err, "error getting cloudsnaps or no cloudsnaps found!")
		return false
	}
	for _, cSnap := range cSnaps {
		volData := cSnap.Metadata["volume"]
		log.Infof("Volume Data from SDK: %v", volData)
		var volumeData volumeDataMap
		err := json.Unmarshal([]byte(volData), &volumeData)
		if err != nil {
			log.FailOnError(err, "Error while unmarshalling volume data")
			return false
		}
		totalRestoreSize := strconv.FormatFloat(volumeData.TotalRestoreSize, 'f', -1, 64)
		compressedSizeBytes := cSnap.Metadata["compressedSizeBytes"]
		capacityRequiredForRestore := strconv.FormatFloat(volumeData.UsedSize, 'f', -1, 64)
		log.Infof("TotalRestoreSize: %v, CompressedObjectBytes: %v, CapacityRequiredForRestore: %v",
			totalRestoreSize, compressedSizeBytes, capacityRequiredForRestore)

		// API GET values from v1/cloudbackups/size
		url := fmt.Sprintf("http://%s:9021/v1/cloudbackups/size?credential_id=%s&backup_id=%s",
			node.GetStorageDriverNodes()[0].MgmtIp, credUUID, cSnap.Id)
		resp, respStatusCode, err := restutil.GET(url, nil, nil)
		if err != nil || respStatusCode != http.StatusOK || len(resp) == 0 {
			log.FailOnError(err, "Error in fetching cloud backup size, Cause: %v; Status code: %v "+
				"\n Or the data is empty", err, respStatusCode)
			return false
		}
		log.InfoD("Parsing output from cloud backup size API")
		var cloudBackupSize CloudBackupSizeAPI
		err = json.Unmarshal(resp, &cloudBackupSize)
		log.Infof("CloudBackupSize: %v", cloudBackupSize)

		if err != nil || cloudBackupSize.Size != totalRestoreSize ||
			cloudBackupSize.TotalDownloadBytes != totalRestoreSize ||
			cloudBackupSize.CompressedObjectBytes != compressedSizeBytes ||
			cloudBackupSize.Capacity != capacityRequiredForRestore {
			log.FailOnError(err, "Cloudsnap size mismatch: %v", cSnap.Id)
			return false
		}
	}

	return true
}

func trashcanRestore(volId, volName string) error {
	log.InfoD("Restoring vol [%v] from trashcan", volId)
	pxctlCmdFull := fmt.Sprintf("v r %s --trashcan %s", volName, volId)
	output, err := Inst().V.GetPxctlCmdOutput(node.GetStorageNodes()[0], pxctlCmdFull)
	if err != nil {
		return err
	}
	log.Infof("output: %v", output)
	if !strings.Contains(output, fmt.Sprintf("Successfully restored: %s", volName)) {
		err = fmt.Errorf("volume %v, restore from trashcan failed, Err: %v", volId, output)
		return err
	}
	return nil
}

func deletePXVolume(volName string) error {
	delVol := func() (interface{}, bool, error) {
		err := Inst().V.DeleteVolume(volName)
		if err != nil {
			return "", true, err
		}
		return nil, false, nil
	}
	_, err := task.DoRetryWithTimeout(delVol, time.Duration(60)*defaultCommandTimeout, 2*time.Minute)
	return err
}

var _ = Describe("{CloudSnapWithPXEvents}", func() {
	var testrailID = 0
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("CloudSnapWithPXEvents", "Validate cloudsnap during PX events", nil, 0)
		runID = testrailuttils.AddRunsToMilestone(0)

	})

	stepLog := "has to schedule apps with cloudsnaps and perform PX events"
	It(stepLog, func() {
		log.InfoD(stepLog)

		contexts = make([]*scheduler.Context, 0)

		stepLog = "validate cloud cred and create schedule policy"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err := CreatePXCloudCredential()
			log.FailOnError(err, "failed to create cloud credential")
			contexts = make([]*scheduler.Context, 0)
			policyName := "intervalpolicy"

			stepLog = fmt.Sprintf("create schedule policy %s", policyName)
			Step(stepLog, func() {
				log.InfoD(stepLog)

				schedPolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
				if err != nil {
					retain := 5
					interval := 1
					log.InfoD("Creating a interval schedule policy %v with interval %v minutes", policyName, interval)
					schedPolicy = &storkv1.SchedulePolicy{
						ObjectMeta: meta_v1.ObjectMeta{
							Name: policyName,
						},
						Policy: storkv1.SchedulePolicyItem{
							Interval: &storkv1.IntervalPolicy{
								Retain:          storkv1.Retain(retain),
								IntervalMinutes: interval,
							},
						}}

					_, err = storkops.Instance().CreateSchedulePolicy(schedPolicy)
					log.FailOnError(err, fmt.Sprintf("error creating a SchedulePolicy [%s]", policyName))
				}
			})

			defer func() {
				err := storkops.Instance().DeleteSchedulePolicy(policyName)
				log.FailOnError(err, fmt.Sprintf("error deleting a SchedulePolicy [%s]", policyName))
			}()

			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("cspxevents-%d", i))...)
			}

			areCloudsnapEnabledAppsDeployed := false

			for _, ctx := range contexts {
				if strings.Contains(ctx.App.Key, "cloudsnap") {
					areCloudsnapEnabledAppsDeployed = true
					break
				}
			}

			if !areCloudsnapEnabledAppsDeployed {
				log.FailOnError(fmt.Errorf("no cloudsnap enabled apps deployed"), "error validating apps for cloudsnaps events test")
			}

			ValidateApplications(contexts)
			nsList, err := core.Instance().ListNamespaces(map[string]string{"creator": "torpedo"})
			log.FailOnError(err, "error getting all namespaces")
			log.Infof("%v", nsList)
			appNamespaces := make([]string, 0)
			for _, ns := range nsList.Items {
				if strings.Contains(ns.Name, "cspxevents") {
					appNamespaces = append(appNamespaces, ns.Name)
				}
			}

			if len(appNamespaces) == 0 {
				log.FailOnError(fmt.Errorf("no namespaces found to validate cloudsnaps"), "error getting cloudsnap namespaces")
			}

			stepLog = "validate cloudsnaps"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				for _, ns := range appNamespaces {
					_, err = validateCloudSnaps(ns)
					log.FailOnError(err, fmt.Sprintf("error validating cloudsnaps in namespace [%s]", ns))
				}
			})
			var csAppVolumes []*volume.Volume
			for _, ctx := range contexts {
				if strings.Contains(ctx.App.Key, "cloudsnap") {
					appVolumes, err := Inst().S.GetVolumes(ctx)
					log.FailOnError(err, "Failed to get volumes for app %s", ctx.App.Key)
					csAppVolumes = append(csAppVolumes, appVolumes...)
				}
			}

			stepLog = "trigger px restart event during cloudsnaps and validate cloudsnaps"
			Step(stepLog, func() {
				log.InfoD(stepLog)

				for _, csAppVol := range csAppVolumes {
					attachedNode, err := Inst().V.GetNodeForVolume(csAppVol, defaultCommandTimeout, defaultCommandRetry)
					dash.VerifySafely(err, nil, fmt.Sprintf("Verify Get nodes for vol %s", csAppVol.Name))
					stepLog = fmt.Sprintf("stop volume driver %s on node: %s",
						Inst().V.String(), attachedNode.Name)
					Step(stepLog,
						func() {
							StopVolDriverAndWait([]node.Node{*attachedNode})
						})

					log.Infof("wait for 10 mins for volumes to reallocate")
					time.Sleep(10 * time.Minute)

					stepLog = fmt.Sprintf("starting volume %s driver on node %s",
						Inst().V.String(), attachedNode.Name)
					Step(stepLog,
						func() {
							StartVolDriverAndWait([]node.Node{*attachedNode})
						})

					stepLog = "Giving few seconds for volume driver to stabilize"
					Step(stepLog, func() {
						log.InfoD("Giving few seconds for volume driver to stabilize")
						time.Sleep(20 * time.Second)
					})

				}
				for _, ctx := range contexts {
					ValidateContext(ctx)
				}
				stepLog = "validate cloudsnaps"
				Step(stepLog, func() {
					log.InfoD(stepLog)

					for _, ns := range appNamespaces {
						_, err = validateCloudSnaps(ns)
						log.FailOnError(err, fmt.Sprintf("error validating cloudsnaps in namespace [%s]", ns))
					}
				})
			})

			stepLog = "validate repl update during cloudsnaps"

			Step(stepLog, func() {
				stopValidation := make(chan bool, 1)

				log.InfoD(stepLog)

				actRepls := make(map[*volume.Volume]int64)
				//Reducing the repl factor if volume as max replication factor enabled
				stepLog = fmt.Sprintf("Adjusting the volume replications before increasing the repls for cloudsnap volumes")
				Step(stepLog, func() {
					log.InfoD(stepLog)
					err = replAdjust(csAppVolumes, actRepls)
					log.FailOnError(err, "Failed to adjust the repls for cloudsnap volumes")
				})

				stepLog = fmt.Sprintf("Increasing the repls for cloudsnap volumes")
				Step(stepLog, func() {
					log.InfoD(stepLog)
					newRepls := make(map[*volume.Volume]int64)
					for _, v := range csAppVolumes {
						currRep, err := Inst().V.GetReplicationFactor(v)
						log.FailOnError(err, "Failed to get volume  %s repl factor", v.Name)
						currAggr, err := Inst().V.GetAggregationLevel(v)
						log.FailOnError(err, "Failed to get volume  %s aggregate level", v.Name)
						numStorageNodes := len(node.GetStorageNodes())
						numStorageNodesRequired := int(currAggr * (currRep + 1))

						if numStorageNodes < numStorageNodesRequired {
							log.Warnf("skipping volume %s repl increase as numStorageNodesRequired is %d where as numStorageNodes is %d", v.Name, numStorageNodesRequired, numStorageNodes)
							continue
						}
						newRepls[v] = currRep + 1
					}
					// Create a channel to signal an error.
					errorChan := make(chan error, 2)
					// Create a WaitGroup to wait for both functions to finish.
					var wg sync.WaitGroup
					for v, r := range newRepls {
						log.InfoD("setting repl for volume %s to %d", v.Name, r)
						if err := Inst().V.SetReplicationFactor(v, r, nil, nil, false); err != nil {
							log.Errorf(fmt.Sprintf("got error while repl increase %v", err))

						}
					}

					//Go routine for cloudsnap validate
					wg.Add(1)
					go func() {
						defer GinkgoRecover()
						defer wg.Done()

						for {
							select {
							case <-errorChan:
								close(stopValidation)
								close(errorChan)
								return
							case <-stopValidation:
								close(stopValidation)
								close(errorChan)
								return
							default:
								for _, ns := range appNamespaces {
									if _, err := validateCloudSnaps(ns); err != nil {
										errorChan <- err
										break
									}
								}
								log.Infof("waiting for 3 mins for next validation")
								time.Sleep(3 * time.Minute)
							}
						}

					}()

					for v, r := range newRepls {
						log.InfoD(fmt.Sprintf("validating repl for %s shoulbe be %d", v.ID, r))
						if err = ValidateReplFactorUpdate(v, r); err != nil {
							errorChan <- err
							break
						}
					}
					stopValidation <- true

					wg.Wait()
					for e := range errorChan {
						dash.VerifySafely(e, nil, "validate cloudsnaps while repl increase")
					}

					for v, r := range actRepls {
						if newRepls[v] != r {
							log.InfoD("setting repl for volume %s to %d", v.Name, r)
							err = Inst().V.SetReplicationFactor(v, r, nil, nil, true)
							log.FailOnError(err, fmt.Sprintf("error setting repl for volume %s with value %d", v.ID, r))
						}

					}

				})

			})

			stepLog = "node de-comm and rejoin while cloudsnap in progress"
			Step(stepLog, func() {
				attachedNodes := make(map[string]bool, 0)
				for _, v := range csAppVolumes {
					attachedNode, err := Inst().V.GetNodeForVolume(v, 1*time.Minute, 5*time.Second)
					log.FailOnError(err, fmt.Sprintf("error getting attahced node for volume %s", v.Name))
					if attachedNode != nil {
						if _, ok := attachedNodes[attachedNode.Name]; !ok {
							attachedNodes[attachedNode.Name] = true
						}
					}
				}

				for attachedNode := range attachedNodes {
					nodeToDecommission, err := node.GetNodeByName(attachedNode)
					stepLog = fmt.Sprintf("decommission node %s", nodeToDecommission.Name)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						err := Inst().S.PrepareNodeToDecommission(nodeToDecommission, Inst().Provisioner)
						dash.VerifyFatal(err, nil, "Validate node decommission preparation")
						err = Inst().V.DecommissionNode(&nodeToDecommission)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Validate node [%s] decommission init", nodeToDecommission.Name))
						stepLog = fmt.Sprintf("check if node %s was decommissioned", nodeToDecommission.Name)
						Step(stepLog, func() {
							log.InfoD(stepLog)
							t := func() (interface{}, bool, error) {
								status, err := Inst().V.GetNodeStatus(nodeToDecommission)
								if err != nil {
									return false, true, err
								}
								if *status == api.Status_STATUS_NONE {
									return true, false, nil
								}
								return false, true, fmt.Errorf("node %s not decomissioned yet", nodeToDecommission.Name)
							}
							decommissioned, err := task.DoRetryWithTimeout(t, 15*time.Minute, defaultRetryInterval)
							log.FailOnError(err, "Failed to get decommissioned node status")
							dash.VerifyFatal(decommissioned.(bool), true, fmt.Sprintf("Validate node [%s] is decommissioned", nodeToDecommission.Name))
						})
					})
					stepLog = "validate cloudsnaps after node decomm"
					Step(stepLog, func() {
						log.InfoD(stepLog)
						for _, ns := range appNamespaces {
							_, err = validateCloudSnaps(ns)
							dash.VerifySafely(err, nil, fmt.Sprintf("validating cloudsnaps in namespace [%s]", ns))
						}
					})
					stepLog = fmt.Sprintf("Rejoin node %s", nodeToDecommission.Name)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						//reboot required to remove encrypted dm devices if any
						err := Inst().N.RebootNode(nodeToDecommission, node.RebootNodeOpts{
							Force: true,
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         defaultCommandTimeout,
								TimeBeforeRetry: defaultRetryInterval,
							},
						})
						log.FailOnError(err, fmt.Sprintf("error rebooting node %s", nodeToDecommission.Name))
						err = Inst().V.RejoinNode(&nodeToDecommission)
						dash.VerifyFatal(err, nil, "Validate node rejoin init")
						var rejoinedNode *api.StorageNode
						t := func() (interface{}, bool, error) {
							drvNodes, err := Inst().V.GetDriverNodes()
							if err != nil {
								return false, true, err
							}

							for _, n := range drvNodes {
								if n.Hostname == nodeToDecommission.Hostname {
									rejoinedNode = n
									return true, false, nil
								}
							}

							return false, true, fmt.Errorf("node %s not joined yet", nodeToDecommission.Name)
						}
						_, err = task.DoRetryWithTimeout(t, 15*time.Minute, defaultRetryInterval)
						log.FailOnError(err, fmt.Sprintf("error joining the node [%s]", nodeToDecommission.Name))
						dash.VerifyFatal(rejoinedNode != nil, true, fmt.Sprintf("verify node [%s] rejoined PX cluster", nodeToDecommission.Name))
						err = Inst().S.RefreshNodeRegistry()
						log.FailOnError(err, "error refreshing node registry")
						err = Inst().V.RefreshDriverEndpoints()
						log.FailOnError(err, "error refreshing storage drive endpoints")
						decommissionedNode := node.Node{}
						for _, n := range node.GetStorageDriverNodes() {
							if n.Name == rejoinedNode.Hostname {
								decommissionedNode = n
								break
							}
						}
						if decommissionedNode.Name == "" {
							log.FailOnError(fmt.Errorf("rejoined node not found"), fmt.Sprintf("node [%s] not found in the node registry", rejoinedNode.Hostname))
						}
						err = Inst().V.WaitDriverUpOnNode(decommissionedNode, Inst().DriverStartTimeout)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Validate driver up on rejoined node [%s] after rejoining", decommissionedNode.Name))
					})

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
		defer EndTorpedoTest()
		bucketName, err := GetCloudsnapBucketName(contexts)
		log.FailOnError(err, "error getting cloud snap bucket name")
		opts := make(map[string]bool)
		DestroyApps(contexts, opts)
		err = DeleteCloudSnapBucket(bucketName)
		log.FailOnError(err, "failed to delete cloud snap bucket")
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{PoolFullCloudsnap}", func() {

	/*
			Priority: P1
		1. Selected a node and a pool
		2. Deploy cloudsnap apps and make sure volumes are attached to the pool selected above
		2. Fill up the pool and validate cloudsnaps
		3. Do pool expansion and validate cloudsnaps
	*/

	var testrailID = 0
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("PoolFullCloudsnap",
			"Make pool full and validate cloudsnaps",
			nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "Make pool full and validate cloudsnaps"
	It(stepLog, func() {

		stepLog = "Create cloudsnap schedule and validate cloud cred"
		policyName := "intervalpolicy"
		Step(stepLog, func() {

			log.InfoD(stepLog)
			err := CreatePXCloudCredential()
			log.FailOnError(err, "failed to create cloud credential")
			contexts = make([]*scheduler.Context, 0)

			stepLog = fmt.Sprintf("create schedule policy %s", policyName)
			Step(stepLog, func() {
				log.InfoD(stepLog)

				schedPolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
				if err != nil {
					retain := 4
					interval := 2
					log.InfoD("Creating a interval schedule policy %v with interval %v minutes", policyName, interval)
					schedPolicy = &storkv1.SchedulePolicy{
						ObjectMeta: meta_v1.ObjectMeta{
							Name: policyName,
						},
						Policy: storkv1.SchedulePolicyItem{
							Interval: &storkv1.IntervalPolicy{
								Retain:          storkv1.Retain(retain),
								IntervalMinutes: interval,
							},
						}}

					_, err = storkops.Instance().CreateSchedulePolicy(schedPolicy)
					log.FailOnError(err, fmt.Sprintf("error creating a SchedulePolicy [%s]", policyName))
				}
			})

		})

		defer func() {
			err := storkops.Instance().DeleteSchedulePolicy(policyName)
			log.FailOnError(err, fmt.Sprintf("error deleting a SchedulePolicy [%s]", policyName))
		}()

		log.InfoD(stepLog)
		existingAppList := Inst().AppList

		selectedNode := GetNodeWithLeastSize()

		stNodes := node.GetStorageNodes()
		var secondReplNode node.Node
		for _, stNode := range stNodes {
			if stNode.Name != selectedNode.Name {
				secondReplNode = stNode
			}
		}

		if selectedNode.Name == "" {
			log.FailOnError(fmt.Errorf("no node with multiple pools exists"), "error identifying node with more than one pool")

		}
		log.Infof("Identified node [%s] for pool expansion", selectedNode.Name)

		repl1PoolUUID := selectedNode.Pools[0].Uuid

		repl1Pool, err := GetStoragePoolByUUID(repl1PoolUUID)
		log.FailOnError(err, "error getting storage pool with UUID [%s]", repl1PoolUUID)

		repl2Pool := secondReplNode.Pools[0]
		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "Failed to check if Journal enabled")

		//expanding to repl2 pool so that it won't go to storage down state
		if (repl2Pool.TotalSize / units.GiB) <= (repl1Pool.TotalSize/units.GiB)*2 {
			expectedSize := (repl2Pool.TotalSize / units.GiB) * 2
			log.InfoD("Current Size of the pool %s is %d", repl2Pool.Uuid, repl2Pool.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(repl2Pool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			resizeErr := waitForPoolToBeResized(expectedSize, repl2Pool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", repl2Pool.Uuid, secondReplNode.Name))
		}

		stepLog = fmt.Sprintf("Fill up  pool [%s] in node [%s] and validate cloudsnaps", repl1Pool.Uuid, selectedNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)

			poolLabelToUpdate := make(map[string]string)
			nodesToDisableProvisioning := make([]string, 0)
			poolsToDisableProvisioning := make([]string, 0)

			defer func() {
				//Reverting the provisioning changes done for the test
				Inst().AppList = existingAppList
				err = Inst().V.SetClusterOpts(*selectedNode, map[string]string{
					"--disable-provisioning-labels": ""})
				log.FailOnError(err, fmt.Sprintf("error removing cluster options disable-provisioning-labels"))
				err = Inst().S.RemoveLabelOnNode(*selectedNode, k8s.NodeType)
				log.FailOnError(err, "error removing label on node [%s]", selectedNode.Name)
				err = Inst().S.RemoveLabelOnNode(secondReplNode, k8s.NodeType)
				log.FailOnError(err, "error removing label on node [%s]", secondReplNode.Name)

				poolLabelToUpdate[k8s.NodeType] = ""
				poolLabelToUpdate["provision"] = ""
				// Update the pool label
				for _, p := range selectedNode.Pools {
					err = Inst().V.UpdatePoolLabels(*selectedNode, p.Uuid, poolLabelToUpdate)
					log.FailOnError(err, "Failed to update the label [%v] on the pool [%s] on node [%s]", poolLabelToUpdate, repl1Pool.Uuid, selectedNode.Name)
				}

			}()

			//Disabling provisioning on the other nodes/pools  and enabling only on selected pools for making sure the metadata node is full
			err = Inst().S.AddLabelOnNode(*selectedNode, k8s.NodeType, k8s.FastpathNodeType)
			log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", selectedNode.Name))
			err = Inst().S.AddLabelOnNode(secondReplNode, k8s.NodeType, k8s.FastpathNodeType)
			log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", secondReplNode.Name))

			for _, n := range stNodes {
				if n.VolDriverNodeID != selectedNode.VolDriverNodeID && n.VolDriverNodeID != secondReplNode.VolDriverNodeID {
					nodesToDisableProvisioning = append(nodesToDisableProvisioning, n.VolDriverNodeID)
				}
			}

			for _, p := range selectedNode.Pools {
				if p.Uuid != repl1Pool.Uuid {
					poolsToDisableProvisioning = append(poolsToDisableProvisioning, p.Uuid)
				}

			}
			for _, p := range secondReplNode.Pools {
				if p.Uuid != repl2Pool.Uuid {
					poolsToDisableProvisioning = append(poolsToDisableProvisioning, p.Uuid)
				}

			}

			poolLabelToUpdate[k8s.NodeType] = ""
			poolLabelToUpdate["provision"] = "disable"
			for _, p := range selectedNode.Pools {
				if p.Uuid != repl1Pool.Uuid {
					err = Inst().V.UpdatePoolLabels(*selectedNode, p.Uuid, poolLabelToUpdate)
					log.FailOnError(err, "Failed to update the label [%v] on the pool [%s] on node [%s]", poolLabelToUpdate, repl1Pool.Uuid, selectedNode.Name)

				}
			}

			clusterOptsVal := fmt.Sprintf("\"node=%s;provision=disable\"", strings.Join(nodesToDisableProvisioning, ","))
			err = Inst().V.SetClusterOpts(*selectedNode, map[string]string{
				"--disable-provisioning-labels": clusterOptsVal})
			log.FailOnError(err, fmt.Sprintf("error update cluster options disable-provisioning-labels with value [%s]", clusterOptsVal))

			Inst().AppList = []string{"fio-fastpath"}
			contexts = make([]*scheduler.Context, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("plflcs-%d", i))...)
			}
			ValidateApplications(contexts)
			defer appsValidateAndDestroy(contexts)
			nsList, err := core.Instance().ListNamespaces(map[string]string{"creator": "torpedo"})
			log.FailOnError(err, "error getting all namespaces")
			log.Infof("%v", nsList)
			appNamespaces := make([]string, 0)
			for _, ns := range nsList.Items {
				if strings.Contains(ns.Name, "plflcs") {
					appNamespaces = append(appNamespaces, ns.Name)
				}
			}

			if len(appNamespaces) == 0 {
				log.FailOnError(fmt.Errorf("no namespaces found to validate cloudsnaps"), "error getting cloudsnap namespaces")
			}

			stepLog = "validate cloudsnaps"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				for _, ns := range appNamespaces {
					_, err = validateCloudSnaps(ns)
					log.FailOnError(err, fmt.Sprintf("error validating cloudsnaps in namespace [%s]", ns))
				}
			})

			err = WaitForPoolOffline(*selectedNode)
			log.FailOnError(err, fmt.Sprintf("Failed to make pool [%s] offline", repl1Pool.Uuid))

			stepLog = "validate cloudsnaps after pool full"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				log.Infof("waiting for 2 mins to create new cloudsnaps")
				time.Sleep(2 * time.Minute)
				for _, ns := range appNamespaces {
					_, err = validateCloudSnaps(ns)
					log.FailOnError(err, fmt.Sprintf("error validating cloudsnaps in namespace [%s] after pool full", ns))
				}
			})

			expectedSize := (repl1Pool.TotalSize / units.GiB) * 2

			log.InfoD("Current Size of the pool %s is %d", repl1Pool.Uuid, repl1Pool.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(repl1Pool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			resizeErr := waitForPoolToBeResized(expectedSize, repl1Pool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", repl1Pool.Uuid, selectedNode.Name))
			status, err := Inst().V.GetNodeStatus(*selectedNode)
			log.FailOnError(err, fmt.Sprintf("Error getting PX status of node %s", selectedNode.Name))
			dash.VerifySafely(*status, api.Status_STATUS_OK, fmt.Sprintf("validate PX status on node %s. Current status: [%s]", selectedNode.Name, status.String()))

			stepLog = "validate cloudsnaps after pool resize"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				log.Infof("waiting for 2 mins to create new cloudsnaps")
				time.Sleep(2 * time.Minute)
				for _, ns := range appNamespaces {
					_, err = validateCloudSnaps(ns)
					log.FailOnError(err, fmt.Sprintf("error validating cloudsnaps in namespace [%s] after pool resize", ns))
				}
			})

		})

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		bucketName, err := GetCloudsnapBucketName(contexts)
		log.FailOnError(err, "error getting cloud snap bucket name")
		opts := make(map[string]bool)
		DestroyApps(contexts, opts)
		err = DeleteCloudSnapBucket(bucketName)
		log.FailOnError(err, "failed to delete cloud snap bucket")
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{NFSProxyVolumeValidation}", func() {
	var contexts []*scheduler.Context
	JustBeforeEach(func() {
		StartTorpedoTest("NFSProxyVolumeValidation", "Validate PX operations with NFS proxy volumes", nil, 0)
	})

	It("schedule proxy volumes on applications, run CRUD, tear down", func() {
		var masterNode node.Node
		stepLog = "setup proxy server necessary for proxy volume"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			masterNodes := node.GetMasterNodes()
			if len(masterNodes) == 0 {
				log.FailOnError(fmt.Errorf("no master nodes found"), "Identifying master node of proxy server failed")
			}

			masterNode = masterNodes[0]
			err = SetupProxyServer(masterNode)
			log.FailOnError(err, fmt.Sprintf("error setting up proxy server on master node %s", masterNode.Name))

		})
		stepLog = "create storage class for proxy volumes"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			addresses := masterNode.Addresses
			if len(addresses) == 0 {
				log.FailOnError(fmt.Errorf("no addresses found for node [%s]", masterNode.Name), "error getting ip addresses ")
			}
			err = CreateNFSProxyStorageClass("portworx-proxy-volume-volume", addresses[0], "/exports/testnfsexportdir")
			log.FailOnError(err, "error creating storage class for proxy volume")
		})

		stepLog = "create apps with proxy volumes"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			appList := Inst().AppList

			defer func() {
				Inst().AppList = appList
			}()

			Inst().AppList = []string{"nginx-proxy-deployment"}
			contexts = make([]*scheduler.Context, 0)

			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("nfsproxytest-%d", i))...)
			}

			for _, ctx := range contexts {
				log.InfoD("Validating application [%s]", ctx.App.Key)
				ctx.SkipVolumeValidation = true //skipping as volume does not have the mount path inside the pod
				ValidateContext(ctx)
			}
		})

		stepLog = "restart PX on all nodes one by one"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appNode := range node.GetStorageDriverNodes() {
				stepLog = fmt.Sprintf("stop volume driver %s on node: %s",
					Inst().V.String(), appNode.Name)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						StopVolDriverAndWait([]node.Node{appNode})
					})
				time.Sleep(20 * time.Second)

				stepLog = fmt.Sprintf("starting volume %s driver on node %s",
					Inst().V.String(), appNode.Name)
				Step(stepLog,
					func() {
						log.InfoD(stepLog)
						StartVolDriverAndWait([]node.Node{appNode})
					})

				stepLog = "Giving few seconds for volume driver to stabilize"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					time.Sleep(20 * time.Second)
				})
			}
			Step("validate apps after PX restart", func() {
				for _, ctx := range contexts {
					log.InfoD("Validating application [%s]", ctx.App.Key)
					ctx.SkipVolumeValidation = true //skipping as volume does not have the mount path inside the pod
					ValidateContext(ctx)
				}
			})
			PerformSystemCheck()

		})

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{SharedVolFuseTest}", func() {
	/*
					https://portworx.atlassian.net/browse/PWX-35639
				   https://portworx.atlassian.net/browse/PTX-21805



			  		1.Get the list of storage nodes where sv4 service and sv4 volumes are attached
					2.Stop/Start PX on each storage node filtered in step 1
					3.Validate PX on the node
			   		4.Repeat this in a loop for 10 iterations
		            5. Validate the applications
	*/
	var testrailID = 12133434
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35259
	var runID int
	var contexts []*scheduler.Context
	JustBeforeEach(func() {
		StartTorpedoTest("SharedVolFuseTest", "Validate PX operations after sharedv4 and sharedv4 svc volumes  failover multiple times", nil, 0)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	It("schedule sharedv4 and sharedv4_svc volumes and perform failover of the coordinator node", func() {

		appList := make([]string, 0)
		stepLog = "create sharedv4 and sharedv4_svc apps "
		Step(stepLog, func() {
			log.InfoD(stepLog)

			for _, appName := range Inst().AppList {

				if strings.Contains(appName, "shared") || strings.Contains(appName, "svc") {
					appList = append(appList, appName)
				}
			}

			if len(appList) == 0 {
				log.FailOnError(fmt.Errorf("sharedv4 or sharedv4 svc apps are mandatory for the test"), "no sharedv4 or sharedv4 svc apps found to deploy")
			}

			contexts = make([]*scheduler.Context, 0)

			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pxfusetest-%d", i))...)
			}

			for _, ctx := range contexts {
				log.InfoD("Validating application [%s]", ctx.App.Key)
				ValidateContext(ctx)
			}
		})

		stNodes := node.GetStorageNodes()

		nodesToRestart := make(map[string]bool)
		sharedVols := make([]*volume.Volume, 0)

		for _, stNode := range stNodes {
			nodesToRestart[stNode.Name] = false
		}

		stepLog = "restart PX on storage nodes"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			numIter := 10

			//Getting the volumes of sharedv4 and sharedv4 svc apps
			for _, ctx := range contexts {
				if Contains(appList, ctx.App.Key) {
					appVols, err := Inst().S.GetVolumes(ctx)
					log.FailOnError(err, fmt.Sprintf("error getting volumes for app [%s]", ctx.App.Key))
					sharedVols = append(sharedVols, appVols...)
				}
			}

			for i := 1; i <= numIter; i++ {

				log.Infof("Running Iteration: #%d", i)

				//Getting the coordinator nodes of sharedv4 and sharedv4 svc volumes
				for _, appVol := range sharedVols {
					attachedNode, err := Inst().V.GetNodeForVolume(appVol, 1*time.Minute, 5*time.Second)
					log.FailOnError(err, fmt.Sprintf("error getting attached node for volume [%s]", appVol.Name))
					nodesToRestart[attachedNode.Name] = true
				}

				for _, stNode := range stNodes {
					//Restarting Px only if sharedv4 or sharedv4 svc volume is attached to the provided node
					if nodesToRestart[stNode.Name] {
						StopVolDriverAndWait([]node.Node{stNode})
						log.Infof("waiting for 1 min before starting PX for volumes corordinator to failover")
						time.Sleep(1 * time.Second)
						StartVolDriverAndWait([]node.Node{stNode})
						status, err := IsPxRunningOnNode(&stNode)
						log.FailOnError(err, "error checking px status on node [%s]", stNode.Name)
						dash.VerifyFatal(status, true, fmt.Sprintf("verfiy px is running on node [%s]", stNode.Name))
					}
				}

				//Setting it false to obtain refreshed nodes once volumes failover
				for _, stNode := range stNodes {
					nodesToRestart[stNode.Name] = false
				}

			}

			Step("validate apps after all failovers", func() {
				for _, ctx := range contexts {
					log.InfoD("Validating application [%s]", ctx.App.Key)
					ValidateContext(ctx)
				}
			})
			PerformSystemCheck()

		})

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{FioClonedVolumeFaultInjection}", func() {
	/*
		https://portworx.atlassian.net/browse/PTX-15687
			1. Create 1 Volume,  Run fio
			2. Create the Clone of Volume in step 1 , Run Fio on the cloned volume
			3. Perform HA increase/Decrease
			4. Perform Volume resize
			4. Inject faults ( like portworx restart)
			6. Repeat step 1-5 for 10 iterations
	*/
	JustBeforeEach(func() {
		StartTorpedoTest("FioClonedVolumeFaultInjectoin", "Create fio clone volume and inject faults,HA increase and volume resize", nil, 0)
	})

	itLog := "FioClonedVolumeFaultInjectoin"
	It(itLog, func() {
		log.InfoD(itLog)

		var secretId = "secret"
		var secretValue = "password"
		selectedNode := node.GetStorageDriverNodes()[0]

		numberOfTotalVolumes := 10
		numberofVolumeCreationInParallel := 5

		numberofIterations := numberOfTotalVolumes / numberofVolumeCreationInParallel
		var Wg sync.WaitGroup

		for j := 0; j < numberofIterations; j++ {
			var once sync.Once
			stepLog := "Create a secret using pxctl secrets kvdb"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				cmd := fmt.Sprintf("pxctl secrets kvdb login | pxctl secrets kvdb put-secret --secret_id %v --secret_value %v", secretId, secretValue)

				out, err := Inst().N.RunCommandWithNoRetry(selectedNode, cmd, node.ConnectionOpts{
					Timeout:         2 * time.Minute,
					TimeBeforeRetry: 10 * time.Second,
				})
				log.FailOnError(err, "Unable to execute the pxctl show command")
				log.InfoD("Succesfully created secrets for secure volume: %v", out)
			})

			for i := 0; i < numberofVolumeCreationInParallel; i++ {
				// Create a px secure volume using pxctl.
				volName := fmt.Sprintf("fio-clone-fault-injection-%d", i)
				var cloneVol string
				Wg.Add(1)
				go func(i int) {
					defer Wg.Done()
					defer GinkgoRecover()

					stepLog = "Create 1 Volume,  Run fio"
					Step(stepLog, func() {
						log.InfoD(stepLog)

						pxctlCreateVolumeCmd := fmt.Sprintf("volume create --secure --size 10 %v --secret_key %v", volName, secretId)
						output, err := runPxctlCommand(pxctlCreateVolumeCmd, selectedNode, nil)
						log.FailOnError(err, "Failed to create volume using pxctl")
						log.InfoD("Successfully created volume: %v", output)

						//attach volume to host
						attachCmd := fmt.Sprintf("pxctl host attach %s --secret_key %v", volName, secretId)
						cmdConnectionOpts := node.ConnectionOpts{
							Timeout:         15 * time.Second,
							TimeBeforeRetry: 5 * time.Second,
							Sudo:            true,
						}

						_, err = Inst().N.RunCommandWithNoRetry(selectedNode, attachCmd, cmdConnectionOpts)
						log.FailOnError(err, "Failed to attach volume to host")

						err = writeFioDataToVolume(volName, selectedNode, 5)
						log.FailOnError(err, "Failed to write data to volume")

					})

					stepLog = "Create a clone of volume and run fio on the cloned volume"
					Step(stepLog, func() {
						log.InfoD(stepLog)
						cloneVol, err = Inst().V.CloneVolume(volName)
						log.FailOnError(err, "Failed to clone volume")
						log.InfoD("successfully create clone of volume :%v -> %v", volName, cloneVol)

						//attach volume to host
						attachCmd := fmt.Sprintf("pxctl host attach %s --secret_key %v", cloneVol, secretId)
						cmdConnectionOpts := node.ConnectionOpts{
							Timeout:         15 * time.Second,
							TimeBeforeRetry: 5 * time.Second,
							Sudo:            true,
						}

						_, err = Inst().N.RunCommandWithNoRetry(selectedNode, attachCmd, cmdConnectionOpts)
						log.FailOnError(err, "Failed to attach volume to host")

						err = writeFioDataToVolume(cloneVol, selectedNode, 2)
						log.FailOnError(err, "Failed to write data to volume")
					})

					stepLog = "Perform HA Increase/Decrease"
					Step(stepLog, func() {
						log.InfoD(stepLog)

						// Increase replication factor to 2
						pxctlHAUpdateCmd := fmt.Sprintf("v ha-update --repl 2 %v", volName)
						_, err = runPxctlCommand(pxctlHAUpdateCmd, selectedNode, nil)
						log.FailOnError(err, "Failed to increase replication factor to 2")
						log.InfoD("Successfully increase replication factor to 2")

						time.Sleep(2 * time.Minute)

						// Decrease replication factor to 1
						pxctlHAUpdateCmd = fmt.Sprintf("v ha-update --repl 1 %v", volName)
						_, err = runPxctlCommand(pxctlHAUpdateCmd, selectedNode, nil)
						log.FailOnError(err, "Failed to increase replication factor to 1")
						log.InfoD("Successfully increase replication factor to 1")

						time.Sleep(30 * time.Second)
					})

					stepLog = "Perform volume resize"
					Step(stepLog, func() {
						log.InfoD(stepLog)

						pxctlVolResizeCmd := fmt.Sprintf("v update %v --size %v", volName, 20)
						_, err = runPxctlCommand(pxctlVolResizeCmd, selectedNode, nil)
						log.FailOnError(err, "Failed to resize volume: %v", volName)
						log.InfoD("Succesfully resized volume: %v", volName)

						volInspect, err := Inst().V.InspectVolume(volName)
						log.FailOnError(err, "Failed to inspect volume")
						log.InfoD("Volume size: %v", volInspect.Spec.Size)

						time.Sleep(30 * time.Second)

					})
					stepLog = "Delete the volume and clone of the volume"
					Step(stepLog, func() {
						//unmount volume
						pxctlUnmountCmd := fmt.Sprintf("host unmount --path /var/lib/osd/mounts/%s %s", volName, volName)
						_, err = runPxctlCommand(pxctlUnmountCmd, selectedNode, nil)
						log.FailOnError(err, "Failed to unmount volume: %v", volName)
						log.InfoD("Succesfully unmounted volume: %v", volName)

						err = Inst().V.DeleteVolume(volName)
						log.FailOnError(err, "Failed to delete volume:%v", volName)

						pxctlUnmountCmd = fmt.Sprintf("host unmount --path /var/lib/osd/mounts/%s %s", cloneVol, cloneVol)
						_, err = runPxctlCommand(pxctlUnmountCmd, selectedNode, nil)
						log.FailOnError(err, "Failed to unmount volume: %v", volName)
						log.InfoD("Succesfully unmounted volume: %v", volName)

						//Delete the clone volume
						err = Inst().V.DeleteVolume(cloneVol)
						log.FailOnError(err, "Failed to delete volume:%v", cloneVol)
					})
				}(i)
			}
			Wg.Wait()

			once.Do(func() {
				stepLog = "Restart portworx where the volume is attached"
				Step(stepLog, func() {
					log.InfoD(stepLog)
					log.Infof("Stop volume driver [%s] on node: [%s]", Inst().V.String(), selectedNode.Name)
					StopVolDriverAndWait([]node.Node{selectedNode})
					log.Infof("Starting volume driver [%s] on node [%s]", Inst().V.String(), selectedNode.Name)
					StartVolDriverAndWait([]node.Node{selectedNode})
					time.Sleep(30 * time.Second)
				})
			})
		}
	})
})
var _ = Describe("{VolumePreCheck}", func() {
	/*
			https://portworx.atlassian.net/browse/PTX-20557
			1. Deploy a basic volume on the cluster
			2. Now we need to test the ha-update of the volume with different cases
			scenarios :
		    1. Test the ha-update sources option with an uuid of other nodes on which node is not present
		    2. Test the ha-update sources option with an ip of nodes
			3. Test the ha-update sources option with an invalid uuid of the node on which the repl of the volume is present
		    4. Test the ha-update sources option with a valid uuid of the node on which the repl of the volume is present

	*/
	JustBeforeEach(func() {
		StartTorpedoTest("volumeprecheck", "Precheck for volume operations", nil, 0)
	})

	It("volumeprecheck", func() {
		log.InfoD("volumeprecheck")
		stepLog := "Create a volume and perform a precheck for sources options"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			stNodes := node.GetStorageDriverNodes()
			nodesuuidWithoutReplica := make([]string, 0)
			nodesIP := make([]string, 0)

			index := rand.Intn(len(stNodes))
			selectedNode := &stNodes[index]

			var aggr_level int
			var repl_level int
			storageNodes := node.GetStorageNodes()
			if len(storageNodes) >= 4 && len(storageNodes) < 9 {
				aggr_level = 2
				repl_level = 2
			} else if len(storageNodes) >= 9 {
				aggr_level = 3
				repl_level = 3
			} else {
				aggr_level = 2
				repl_level = 1
			}
			log.InfoD("Setting the aggr_level to %d and repl_level to %d as storage nodes in the cluster are %d", aggr_level, repl_level, len(storageNodes))

			id := uuid.New()
			volName := fmt.Sprintf("volume_%s", id.String()[:8])
			log.InfoD("Create a volume with a min size on node [%s]", selectedNode.Name)
			basicVolumeCreate := fmt.Sprintf("volume create -a %d --repl %d  %s", aggr_level, repl_level, volName)
			_, err := runPxctlCommand(basicVolumeCreate, *selectedNode, nil)
			log.FailOnError(err, "volume creation failed on the cluster with volume name [%s] ", volName)
			log.InfoD("Base Volume creation with volume name %s successful", volName)
			//find on which node volume got created
			volInspect, err := Inst().V.InspectVolume(volName)
			log.FailOnError(err, "Failed to inspect volume")
			log.InfoD("Volume created on node: %s", volInspect.ReplicaSets[0].Nodes[0])

			selectedNodeId := volInspect.ReplicaSets[0].Nodes[0]
			listofNodesVolumePlaced := volInspect.ReplicaSets[0].Nodes

			log.InfoD("List of nodes on which volume is placed: %v", listofNodesVolumePlaced)
			for _, stNode := range stNodes {
				if !Contains(listofNodesVolumePlaced, stNode.VolDriverNodeID) {
					nodesuuidWithoutReplica = append(nodesuuidWithoutReplica, stNode.Id)
					nodesIP = append(nodesIP, stNode.Addresses[0])
				}
			}
			if len(nodesuuidWithoutReplica) == 0 && len(listofNodesVolumePlaced) == len(storageNodes) {
				log.InfoD("Volume Cannot be placed on other nodes as all the nodes are already used for the volume creation")
				return
			}

			log.InfoD("Test the ha-update sources option with a uuid of other nodes on which node is not present")
			wrongUuidcmd := fmt.Sprintf("v ha-update %s --repl %d --sources %s", volName, repl_level+1, nodesuuidWithoutReplica[rand.Intn(len(nodesuuidWithoutReplica))])
			_, err = runPxctlCommand(wrongUuidcmd, node.GetStorageDriverNodes()[0], nil)
			if err != nil {
				isExpectedError := strings.Contains(err.Error(), "does not belong to volume's replication set")
				dash.VerifyFatal(isExpectedError, true, fmt.Sprintf("Expected error: %v", err))

			}

			log.InfoD("Test the ha-update sources option with a ip of nodes ")
			wrongIPcmd := fmt.Sprintf("v ha-update %s --repl %d --sources %s", volName, repl_level+1, nodesIP[rand.Intn(len(nodesIP))])
			_, err = runPxctlCommand(wrongIPcmd, node.GetStorageDriverNodes()[0], nil)
			if err != nil {
				isExpectedError := strings.Contains(err.Error(), "could not find any node with id")
				dash.VerifyFatal(isExpectedError, true, fmt.Sprintf("Expected error: %v", err))

			}

			log.InfoD("Test the ha-update sources option with an invalid uuid of the node on which the repl of the volume is present")
			randomUUID := uuid.New()
			invalidUuidcmd := fmt.Sprintf("v ha-update %s --repl %d --sources %s", volName, repl_level+1, randomUUID)
			_, err = runPxctlCommand(invalidUuidcmd, node.GetStorageDriverNodes()[0], nil)
			if err != nil {
				isExpectedError := strings.Contains(err.Error(), "Failed to update volume: could not find any node with id")
				dash.VerifyFatal(isExpectedError, true, fmt.Sprintf("Expected error: %v", err))

			}

			log.InfoD("Test the ha-update sources option with a valid uuid of the node on which the repl of the volume is present")
			validUuidcmd := fmt.Sprintf("v ha-update %s --repl %d --sources %s", volName, repl_level+1, selectedNodeId)
			_, err = runPxctlCommand(validUuidcmd, node.GetStorageDriverNodes()[0], nil)
			log.FailOnError(err, "Failed to update volume: %v", volName)
			log.InfoD("Successfully updated volume: %v", volName)

			log.InfoD("Delete the volume that is created for the test")
			deleteVolumeCmd := fmt.Sprintf("volume delete %s", volName)
			_, err = runPxctlCommand(deleteVolumeCmd, *selectedNode, nil)
			log.FailOnError(err, "Failed to delete volume: %v", volName)
		})
	})
})

func writeFioDataToVolume(volName string, n node.Node, size int64) error {
	mountPath := fmt.Sprintf("/var/lib/osd/mounts/%s", volName)
	creatDir := fmt.Sprintf("mkdir %s", mountPath)

	cmdConnectionOpts := node.ConnectionOpts{
		Timeout:         15 * time.Second,
		TimeBeforeRetry: 5 * time.Second,
		Sudo:            true,
	}

	log.Infof("Running command %s on %s", creatDir, n.Name)
	_, err := Inst().N.RunCommandWithNoRetry(n, creatDir, cmdConnectionOpts)

	if err != nil {
		return err
	}

	mountCmd := fmt.Sprintf("pxctl host mount --path %s %s", mountPath, volName)
	log.Infof("Running command %s on %s", mountCmd, n.Name)
	_, err = Inst().N.RunCommandWithNoRetry(n, mountCmd, cmdConnectionOpts)

	if err != nil {
		return err
	}

	writeCmd := fmt.Sprintf("fio --name=%s --ioengine=libaio --rw=write --bs=4k --numjobs=1 --size=%vG --iodepth=256 --directory=%s --output=/tmp/vol_write.log --verify=meta --direct=1 --randrepeat=1 --verify_pattern=0xbeddacef --end_fsync=1", volName, size, mountPath)

	log.Infof("Running command %s on %s", writeCmd, n.Name)
	_, err = Inst().N.RunCommandWithNoRetry(n, writeCmd, cmdConnectionOpts)

	if err != nil {
		return err
	}

	return nil

}

var _ = Describe("{OverCommitVolumeTest}", func() {
	/*
						    https://portworx.atlassian.net/browse/PTX-19103
							Total 5 scenarios Tested
							1. Verify Thick Provisioning on Specific Nodes when resizing the volume are honoured
							2. Verify Thick Provisioning (Global) OverCommitPercent when resizing the volume are honoured
							3. Update the pxctl cluster with cluster option OverCommitPercent with 200(Enabeling Thick Provisioning) on a specific Node and thin provisioning on the other nodes
							4. Thin Provisioning with Global and Node Specific Settings [300% on a certain node and 200% over commit on the other nodes]
							5. Disable all imposed cluster options and try creating thin provisioned volumes
		                    Process:
							1. Update the pxctl cluster with cluster option OverCommitPercent
						    2. Check the overall storage pool capacity of a particular node
						    3. create a volume with max of target size of the pool
						    4. Now update the volume size more than size of the storage pool
						    5. Check vol size is successful or not
						    6. Also validate the creation of volume with size more than available capacity on the node


	*/
	JustBeforeEach(func() {
		StartTorpedoTest("OverCommitVolumeTest", "Validate Overcommit volume size", nil, 0)
	})
	// check the size left in the node
	itLog := "honor OverCommitPercent when resizing the volume"
	It(itLog, func() {
		log.InfoD(itLog)
		getRandomPoolandCalculateSize := func(snapshotPercent uint64) (selectedNode *node.Node, targetSizeGiB uint64) {
			stNodes := node.GetStorageDriverNodes()
			index := rand.Intn(len(stNodes))
			selectedNode = &stNodes[index]
			pools := selectedNode.Pools
			poolToResize := pools[rand.Intn(len(pools))]
			poolIDToResize := poolToResize.Uuid
			originalSizeInBytes := poolToResize.TotalSize
			log.InfoD("Original size of the pool %s is %d of node %s ", poolIDToResize, originalSizeInBytes, selectedNode.Name)
			SnapshotPercent := snapshotPercent
			SubtractSize := (SnapshotPercent * originalSizeInBytes) / 100
			targetSizeInBytes := originalSizeInBytes - SubtractSize
			targetSizeGiB = targetSizeInBytes / units.GiB
			log.InfoD("Target size of the pool %s is %d", poolIDToResize, targetSizeGiB)
			return selectedNode, targetSizeGiB
		}
		CreateVolumeandValidate := func(selectedNode *node.Node, multiple uint64, targetSizeGiB uint64) {
			id := uuid.New()
			VolName := fmt.Sprintf("volume_%s", id.String()[:8])
			log.InfoD("Create a volume with a min size on node [%s]", selectedNode.Name)
			basicVolumeCreate := fmt.Sprintf("volume create --nodes %s %s", selectedNode.Id, VolName)
			_, err := runPxctlCommand(basicVolumeCreate, *selectedNode, nil)
			log.FailOnError(err, "volume creation failed on the cluster with volume name [%s]", VolName)
			log.InfoD("Base Volume creation with volume name %s successful", VolName)
			log.InfoD("Now Resize volume with a size of %d %d time of targetsize of pool on node [%s] as %d overcommit percent imposed", multiple*targetSizeGiB, multiple, selectedNode.Name, multiple*100)
			resizeVolumeCmd := fmt.Sprintf("volume update --size %d %s", multiple*targetSizeGiB, VolName)
			_, volCreateErr := runPxctlCommand(resizeVolumeCmd, *selectedNode, nil)
			log.FailOnError(volCreateErr, "volume resize failed  on the cluster with volume name [%s]", VolName)
			log.InfoD("Volume [%s] resized to %d GiB", VolName, multiple*targetSizeGiB)
			log.InfoD("Resize the volume more than %d times available capacity on the node [%s]", multiple, selectedNode.Name)
			resizeVolumeGreaterThanPoolSizeCmd := fmt.Sprintf("volume update --size %d %s", (multiple+1)*targetSizeGiB, VolName)
			_, err = runPxctlCommand(resizeVolumeGreaterThanPoolSizeCmd, *selectedNode, nil)
			if err != nil {
				IsExpectederr := strings.Contains(err.Error(), "Failed to resize volume")
				dash.VerifyFatal(IsExpectederr, true, err.Error())
			}

			err = Inst().V.DeleteVolume(VolName)
			log.FailOnError(err, "Failed to delete volume [%s]", VolName)
			log.InfoD("Successfully deleted volume [%s]", VolName)
			log.InfoD("Try Creating a New Volume with size more than %d times available capacity on the node [%s]", multiple, selectedNode.Name)
			id = uuid.New()
			VolName = fmt.Sprintf("volume_%s", id.String()[:8])
			volCreatecmd := fmt.Sprintf("volume create --size %d --nodes %s %s", (multiple+1)*targetSizeGiB, selectedNode.Id, VolName)
			_, volerr := runPxctlCommand(volCreatecmd, *selectedNode, nil)
			IsExpectederr := strings.Contains(volerr.Error(), "pools must not over-commit provisioning space")
			if volerr != nil {
				dash.VerifyFatal(IsExpectederr, true, volerr.Error())
			} else {
				PrintInspectVolume(VolName)
				dash.VerifyFatal(volerr, fmt.Errorf("Volume Creation should be failed"), "Volume should not be created as we have imposed the cluster options")
			}
			DisableClusterOptionscmd := "cluster options update  --provisioning-commit-labels '[]'"
			_, disable_err := runPxctlCommand(DisableClusterOptionscmd, *selectedNode, nil)
			log.FailOnError(disable_err, "Failed to set cluster options")
			log.InfoD("Successfully set cluster options")

		}

		//Case 1: Verify Thick Provisioning on Specific Nodes when resizing the volume are honoured
		stepLog = "Verify Thick Provisioning on Specific Nodes when resizing the volume are honoured "
		Step(stepLog, func() {
			log.InfoD(stepLog)
			selectedNode, targetSizeGiB := getRandomPoolandCalculateSize(30)
			SetClusterOptionscmdOnNode := fmt.Sprintf("cluster options update  --provisioning-commit-labels '[{\"OverCommitPercent\": 100, \"SnapReservePercent\": 30,\"LabelSelector\": {\"node\": \"%s\"}} ]'", selectedNode.Id)
			_, err := runPxctlCommand(SetClusterOptionscmdOnNode, *selectedNode, nil)
			log.FailOnError(err, "Failed to set cluster options")
			log.InfoD("Successfully set cluster options")
			ClusterOptionsValidationcmd := "cluster options list -j | jq -r '.ProvisionCommitRule'"
			output, err := runPxctlCommand(ClusterOptionsValidationcmd, *selectedNode, nil)
			log.InfoD("The Current Cluster options: %v", output)
			CreateVolumeandValidate(selectedNode, 1, targetSizeGiB)
		})

		// Case 2: Verify Thick Provisioning (Global) OverCommitPercent when resizing the volume are honoured
		stepLog = "Verify Thick Provisioning (Global) OverCommitPercent when resizing the volume are honoured "
		Step(stepLog, func() {
			log.InfoD(stepLog)
			selectedNode, targetSizeGiB := getRandomPoolandCalculateSize(15)
			SetClusterOptionscmd := "cluster options update  --provisioning-commit-labels '[{\"OverCommitPercent\": 100, \"SnapReservePercent\": 15}]'"
			_, err := runPxctlCommand(SetClusterOptionscmd, *selectedNode, nil)
			log.FailOnError(err, "Failed to set cluster options")
			log.InfoD("Successfully set cluster options")
			ClusterOptionsValidationcmd := "cluster options list -j | jq -r '.ProvisionCommitRule'"
			output, err := runPxctlCommand(ClusterOptionsValidationcmd, *selectedNode, nil)
			log.InfoD("The Current Cluster options: %v", output)
			CreateVolumeandValidate(selectedNode, 1, targetSizeGiB)

		})

		//Case 3 : Verify Thin Provisioning 200% (Global)  when resizing the volume are honoured
		stepLog = "Update the pxctl cluster with cluster option OverCommitPercent with 200(Enabeling Thick Provisioning)"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			selectedNode, targetSizeGiB := getRandomPoolandCalculateSize(15)
			SetClusterOptionscmd := "cluster options update  --provisioning-commit-labels '[{\"OverCommitPercent\": 200, \"SnapReservePercent\": 15}]'"
			_, err := runPxctlCommand(SetClusterOptionscmd, *selectedNode, nil)
			log.FailOnError(err, "Failed to set cluster options")
			log.InfoD("Successfully set cluster options")
			ClusterOptionsValidationcmd := "cluster options list -j | jq -r '.ProvisionCommitRule'"
			output, err := runPxctlCommand(ClusterOptionsValidationcmd, *selectedNode, nil)
			log.InfoD("The Current Cluster options: %v", output)
			CreateVolumeandValidate(selectedNode, 2, targetSizeGiB)

		})

		//Case 4: Thin Provisioning with Global and Node Specific Settings
		stepLog = "Update the pxctl cluster with cluster option OverCommitPercent with 200(Enabeling Thick Provisioning) on a specific Node and thin provisioning on the other nodes"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			selectedNode, targetSizeGiB := getRandomPoolandCalculateSize(30)
			SetClusterOptionscmd := fmt.Sprintf("cluster options update  --provisioning-commit-labels '[{\"OverCommitPercent\": 300, \"SnapReservePercent\": 30,\"LabelSelector\": {\"node\": \"%s\"}}, {\"OverCommitPercent\": 200, \"SnapReservePercent\": 15}]'", selectedNode.Id)
			_, err := runPxctlCommand(SetClusterOptionscmd, *selectedNode, nil)
			log.FailOnError(err, "Failed to set cluster options")
			log.InfoD("Successfully set cluster options")
			ClusterOptionsValidationcmd := "cluster options list -j | jq -r '.ProvisionCommitRule'"
			output, err := runPxctlCommand(ClusterOptionsValidationcmd, *selectedNode, nil)
			log.InfoD("The Current Cluster options: %v", output)
			CreateVolumeandValidate(selectedNode, 3, targetSizeGiB)

			log.Info("Try Creating a New Volume with size more than 200% available capacity on any other node ")
			stNodes := node.GetStorageDriverNodes()
			for _, node := range stNodes {
				if node.Name != selectedNode.Name {
					SetClusterOptionscmd := fmt.Sprintf("cluster options update  --provisioning-commit-labels '[{\"OverCommitPercent\": 300, \"SnapReservePercent\": 30,\"LabelSelector\": {\"node\": \"%s\"}}, {\"OverCommitPercent\": 200, \"SnapReservePercent\": 15}]'", selectedNode.Id)
					_, err := runPxctlCommand(SetClusterOptionscmd, node, nil)
					log.FailOnError(err, "Failed to set cluster options")
					id := uuid.New()
					VolName := fmt.Sprintf("volume_%s", id.String()[:8])
					volCreatecmd := fmt.Sprintf("volume create --size %d --nodes %s %s", 3*targetSizeGiB, node.Id, VolName)
					_, volerr := runPxctlCommand(volCreatecmd, node, nil)
					if volerr != nil {
						IsExpectederr := strings.Contains(volerr.Error(), "pools must not over-commit provisioning space")
						dash.VerifyFatal(IsExpectederr, true, volerr.Error())

					}
					break
				}

			}

		})
		//Case 5 : Disable all imposed cluster options and try creating thin provisioned volumes
		stepLog = "Create a volume again with size greater than Storage pool size as we have disabled the thick provisioning (Should Be created)"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			selectedNode, targetSizeGiB := getRandomPoolandCalculateSize(0)
			SetClusterOptionscmd := "cluster options update  --provisioning-commit-labels '[]'"
			_, err := runPxctlCommand(SetClusterOptionscmd, *selectedNode, nil)
			log.FailOnError(err, "Failed to set cluster options")
			log.InfoD("Successfully set cluster options")
			ClusterOptionsValidationcmd := "cluster options list -j | jq -r '.ProvisionCommitRule'"
			output, err := runPxctlCommand(ClusterOptionsValidationcmd, *selectedNode, nil)
			log.InfoD("The Current Cluster options: %v", output)
			id := uuid.New()
			VolName := fmt.Sprintf("volume_%s", id.String()[:8])
			volerr := Inst().V.CreateVolumeUsingPxctlCmd(*selectedNode, VolName, 3*targetSizeGiB, 1)
			log.FailOnError(volerr, "volume creation failed on the cluster with volume name [%s]", VolName)
			log.InfoD("Volume created with name [%s]", VolName)
			//Delete the Volume , As we have created it only for Validation Purpose
			err = Inst().V.DeleteVolume(VolName)
			log.FailOnError(err, "Failed to delete volume [%s]", VolName)

		})

	})
})
var _ = Describe("{RestartPxandRestartNode}", func() {
	/*
	   https://purestorage.atlassian.net/browse/PTX-24483
	   1.Deploy Applications
	   2.Validate Applications are Deployed
	   3.Restart Portworx service on few nodes
	   4.once portworx is up on the node,immediately reboot the node
	   5.Make sure Both portworx and node are up.
	   6.Validate the Applications are running
	*/
	var contexts []*scheduler.Context
	JustBeforeEach(func() {
		StartTorpedoTest("RestartPxandRestartNode",
			"Restart Portworx and Restart Node", nil, 0)
	})
	itLog := "RestartPxandRestartNode"
	It(itLog, func() {
		log.InfoD(itLog)
		pxNodes := node.GetStorageDriverNodes()
		selectedNodesForReboot := pxNodes[:len(pxNodes)/2]
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := "restartpxandrebootnode"
			Provisioner := fmt.Sprintf("%v", portworx.PortworxCsi)
			context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
				AppKeys:            Inst().AppList,
				StorageProvisioner: Provisioner,
				Namespace:          taskName,
			})
			log.FailOnError(err, "Failed to schedule application of %v namespace", taskName)
			contexts = append(contexts, context...)
		}
		ValidateApplications(contexts)
		defer DestroyApps(contexts, nil)
		stepLog := "Restart Portworx Service on few nodes and once portworx is up, immediately reboot the node"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, nodeToReboot := range selectedNodesForReboot {
				log.InfoD("Restarting portworx  Service on Node [%v]", nodeToReboot.Name)
				err := Inst().V.RestartDriver(nodeToReboot, nil)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Failed to restart portworx on node [%v]", nodeToReboot.Name))
				log.InfoD("Restarted portworx on  node %s", nodeToReboot.Name)
				err = Inst().N.RebootNode(nodeToReboot,
					node.RebootNodeOpts{
						Force: true,
						ConnectionOpts: node.ConnectionOpts{
							Timeout:         defaultCommandTimeout,
							TimeBeforeRetry: defaultCommandRetry,
						},
					})
				log.FailOnError(err, "Failed to reboot node %v", nodeToReboot.Name)
				nodeReadyStatus := func() (interface{}, bool, error) {
					err := Inst().S.IsNodeReady(nodeToReboot)
					if err != nil {
						return "", true, err
					}
					return "", false, nil
				}
				log.InfoD("wait for node: %s to be back up", nodeToReboot.Name)
				_, err = DoRetryWithTimeoutWithGinkgoRecover(nodeReadyStatus, 10*time.Minute, 35*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the status of rebooted node %s", nodeToReboot.Name))
				err = Inst().V.WaitDriverUpOnNode(nodeToReboot, Inst().DriverStartTimeout)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the node driver status of rebooted node %s", nodeToReboot.Name))
			}

		})
		stepLog = "Validate the applications are in running state"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			ValidateApplications(contexts)
		})
	})
	JustAfterEach(func() {
		EndTorpedoTest()
		AfterEachTest(contexts)
	})
})
