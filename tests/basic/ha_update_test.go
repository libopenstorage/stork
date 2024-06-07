package tests

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/pkg/units"
	corev1 "k8s.io/api/core/v1"
	"strings"
	"sync"
	"time"

	"github.com/portworx/torpedo/pkg/log"

	. "github.com/onsi/ginkgo/v2"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	. "github.com/portworx/torpedo/tests"
)

const (
	validateReplicationUpdateTimeout = 4 * time.Hour
)

var _ = Describe("{HaIncreaseRebootTarget}", func() {
	testName := "ha-inc-reboot-tgt"
	performHaIncreaseRebootTest(testName)
})

var _ = Describe("{HaIncreaseRebootSource}", func() {
	testName := "ha-inc-reboot-src"
	performHaIncreaseRebootTest(testName)
})
var _ = Describe("{HaIncreaseRestartPXSource}", func() {
	testName := "ha-inc-restartpx-src"
	performHaIncreaseRebootTest(testName)
})

var _ = Describe("{HaIncreaseRestartPXTarget}", func() {
	testName := "ha-inc-restartpx-tgt"
	performHaIncreaseRebootTest(testName)
})

func performHaIncreaseRebootTest(testName string) {
	var contexts []*scheduler.Context
	nodeRebootType := "target"
	testDesc := "HaIncreaseRebootTarget"

	if testName == "ha-inc-reboot-tgt" {
		nodeRebootType = "target"
		testDesc = "HaIncreaseRebootTarget"
	}
	if testName == "ha-inc-reboot-src" {
		nodeRebootType = "source"
		testDesc = "HaIncreaseRebootSource"
	}

	if testName == "ha-inc-restartpx-src" {
		nodeRebootType = "source"
		testDesc = "HaIncreaseRestartPX on Source"
	}
	if testName == "ha-inc-restartpx-tgt" {
		nodeRebootType = "target"
		testDesc = "HaIncreaseRestartPX on Target"
	}
	JustBeforeEach(func() {
		StartTorpedoTest(testDesc, fmt.Sprintf("Validate HA increase and reboot %s", nodeRebootType), nil, 0)

	})
	stepLog := fmt.Sprintf("has to perform repl increase and reboot %s node", nodeRebootType)
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", testName, i))...)
		}

		ValidateApplications(contexts)

		//Reboot target node and source node while repl increase is in progress
		stepLog = fmt.Sprintf("get a volume to  increase replication factor and reboot %s node", nodeRebootType)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			storageNodeMap := make(map[string]node.Node)
			storageNodes, err := GetStorageNodes()
			log.FailOnError(err, "Failed to get storage nodes")

			for _, n := range storageNodes {
				storageNodeMap[n.Id] = n
			}

			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				var err error
				stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
				Step(stepLog, func() {
					log.InfoD(stepLog)
					appVolumes, err = Inst().S.GetVolumes(ctx)
					log.FailOnError(err, "Failed to get volumes")
					dash.VerifyFatal(len(appVolumes) > 0, true, fmt.Sprintf("Found %d app volmues", len(appVolumes)))
				})

				for _, v := range appVolumes {
					// Check if volumes are Pure FA/FB DA volumes
					isPureVol, err := Inst().V.IsPureVolume(v)
					log.FailOnError(err, "Failed to check is PURE volume")
					if isPureVol {
						log.Warnf("Repl increase on Pure DA Volume [%s] not supported.Skiping this operation", v.Name)
						continue
					}

					currRep, err := Inst().V.GetReplicationFactor(v)
					log.FailOnError(err, "Failed to get Repl factor for vil %s", v.Name)

					if currRep != 0 {
						//Reduce replication factor
						if currRep == 3 {
							log.Infof("Current replication is  3, reducing before proceeding")
							opts := volume.Options{
								ValidateReplicationUpdateTimeout: validateReplicationUpdateTimeout,
							}
							err = Inst().V.SetReplicationFactor(v, currRep-1, nil, nil, true, opts)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Validate set repl factor to %d", currRep-1))
						}
					}

					if testName == "ha-inc-reboot-src" {
						err = HaIncreaseErrorInjectSourceNode(nil, ctx, v, storageNodeMap, REBOOT)
						dash.VerifyFatal(err, nil, "Validate HA increase and reboot source node")

					} else if testName == "ha-inc-restartpx-src" {
						err = HaIncreaseErrorInjectSourceNode(nil, ctx, v, storageNodeMap, PX_RESTART)
						dash.VerifyFatal(err, nil, "Validate HA increase and restart source node")
					} else if testName == "ha-inc-restartpx-tgt" {
						err = HaIncreaseErrorInjectionTargetNode(nil, ctx, v, storageNodeMap, PX_RESTART)
						dash.VerifyFatal(err, nil, "Validate HA increase and restart target node")
					} else {
						err = HaIncreaseErrorInjectionTargetNode(nil, ctx, v, storageNodeMap, REBOOT)
						dash.VerifyFatal(err, nil, "Validate HA increase and reboot target node")
					}
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
		AfterEachTest(contexts)
	})

}

var _ = Describe("{VolResizeAllVolumes}", func() {

	/*
		PTX-23576
		Trigger vol resize on all volumes at once
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("VolResizeAllVolumes", "Trigger vol resize on all volumes at once", nil, 0)
	})

	itLog := "VolResizeAllVolumes"
	It(itLog, func() {
		var contexts []*scheduler.Context
		var k8sCore = core.Instance()

		type VolumeDetails struct {
			vol *volume.Volume
			pvc *corev1.PersistentVolumeClaim
			ctx *scheduler.Context
		}
		volSizeMap := []*VolumeDetails{}

		stepLog = "Enable Trashcan on the cluster"
		Step(stepLog,
			func() {
				log.InfoD(stepLog)
				err := EnableTrashcanOnCluster("90")
				log.FailOnError(err, "failed to enable trashcan on the cluster")
			})

		stepLog = "Schedule Applications on the cluster and get details of Volumes"
		Step(stepLog, func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("volresizeallvol-%d", i))...)
			}
		})
		appsDeleted := false
		deleteApps := func() {
			if !appsDeleted {
				appsValidateAndDestroy(contexts)
			}
		}

		ValidateApplications(contexts)
		defer deleteApps()

		stepLog = "Verify parallel resize of all the volumes "
		Step(stepLog, func() {
			for _, eachCtx := range contexts {
				vols, err := Inst().S.GetVolumes(eachCtx)
				log.FailOnError(err, "Failed to get list of Volumes in the cluster")
				for _, eachVol := range vols {
					volDetails := VolumeDetails{}

					// Get details on the Volume
					volDetails.vol = eachVol

					// Get PVC object from the Volume
					pvc, err := GetPVCObjFromVol(eachVol)
					log.FailOnError(err, "Failed to get PVC Details from Volume [%v]", eachVol.Name)

					// Update volSizeMap to perform Parallel Resize of all the Volumes present in the cluster
					volDetails.pvc = pvc
					volSizeMap = append(volSizeMap, &volDetails)
				}
			}
		})

		for _, eachVol := range volSizeMap {
			// Pod details after blocking IP
			podsAfterblk, err := k8sCore.GetPodsUsingPVC(eachVol.vol.Name, eachVol.vol.Namespace)
			log.FailOnError(err, "unable to find the node from the pod")
			for _, eachPodAfter := range podsAfterblk {
				log.Infof(fmt.Sprintf("Current state of the Pod [%v] is [%v]", eachPodAfter.Name, eachPodAfter.Status.Phase))
				if eachPodAfter.Status.Phase != "Running" {
					log.Infof(fmt.Sprintf("State of the Pod before Resize volume [%v] is [%v]", eachPodAfter.Name, eachPodAfter.Status.Phase))
					log.FailOnError(fmt.Errorf("Pod [%v] Consuming Volume [%v] is not in Running State ",
						eachPodAfter.Name, eachVol.vol.Name), "Pod not in Running State")
				}
			}
		}

		// Volume resize routine resizes specific Volume
		log.InfoD("start Volume resize on each Volume present in the cluster")
		ResizePvcInParallel := func(newVolumeIDs *VolumeDetails) {
			defer GinkgoRecover()
			volCurSize := newVolumeIDs.vol.Size / units.GiB
			volNewSize := volCurSize + 10
			log.Infof("Resizing Volume [%v] from size [%v]/[%v] to [%v]/[%v]",
				newVolumeIDs.vol.Name, newVolumeIDs.vol.Size, newVolumeIDs.vol.Size/units.GB,
				volNewSize*units.GB, volNewSize)

			log.Infof("Resizing PVC [%v] to [%v]", newVolumeIDs.pvc.Name, volNewSize)
			vol, err := Inst().S.ResizePVC(newVolumeIDs.ctx, newVolumeIDs.pvc, 10)
			log.FailOnError(err, "Failed to resize PVC [%v]", vol.Name)

			time.Sleep(30 * time.Second)
			volReSize, err := Inst().V.InspectVolume(vol.ID)
			log.FailOnError(err, "inspect returned error ?")

			log.Infof("Verify volume size after resizing the volume [%v]", vol.Name)
			dash.VerifyFatal(volReSize.Spec.Size > newVolumeIDs.vol.Size, true,
				fmt.Sprintf("Resize of Volume didnot happen? current size is [%v]", volReSize.Spec.Size/units.GiB))

			log.Infof(fmt.Sprintf("Volume [%v] resized from [%v] to [%v]",
				newVolumeIDs.vol.Name, newVolumeIDs.vol.Size, volReSize.Spec.Size))
			dash.VerifyFatal((volReSize.Spec.Size/units.GiB) == volNewSize, true,
				fmt.Sprintf("Resize of Volume didnot happen? current size is [%v]!", volReSize.Spec.Size/units.GiB))

		}

		// Resize Volumes in parallel
		ResizeVolumes := func(volId string) {
			defer GinkgoRecover()
			sizeBeforeResize, err := Inst().V.InspectVolume(volId)
			log.FailOnError(err, "inspect returned error ?")
			toResize := sizeBeforeResize.Spec.Size + (10 * units.GiB)
			log.Infof("Resizing Volume [%v] from [%v] to [%v]",
				volId, sizeBeforeResize.Spec.Size, toResize/units.GiB)

			// Resize Volume and verify if volume resized
			log.FailOnError(Inst().V.ResizeVolume(volId, toResize), "Failed to resize volume")

			//Size after resize
			sizeAfterResize, err := Inst().V.InspectVolume(volId)
			log.FailOnError(err, "inspect returned error ?")

			dash.VerifyFatal(sizeBeforeResize.Spec.Size < sizeAfterResize.Spec.Size, true,
				"Volume resize did not happen")

			log.Infof("Volume [%v] resized from [%v] to [%v] and expected is [%v]",
				volId, sizeBeforeResize.Spec.Size, toResize, sizeAfterResize.Spec.Size)

			dash.VerifyFatal((toResize/units.GiB) == (sizeAfterResize.Spec.Size/units.GiB), true,
				fmt.Sprintf("Resize of Volume didnot happen? current size is [%v]!", sizeAfterResize.Spec.Size))

		}

		for _, eachVol := range volSizeMap {
			Step(fmt.Sprintf("Do Resize on Volume [%v]", eachVol.vol.Name), func() {
				go ResizePvcInParallel(eachVol)
			})
		}

		// wait for some time before checking all the applications are Up and Running
		time.Sleep(60 * time.Second)

		for _, eachVol := range volSizeMap {
			// Pod details after blocking IP
			podsAfterblk, err := k8sCore.GetPodsUsingPVC(eachVol.vol.Name, eachVol.vol.Namespace)
			log.FailOnError(err, fmt.Sprintf("failed to get details of Pods assigned to volume [%v]", eachVol.vol.Name))
			for _, eachPodAfter := range podsAfterblk {
				log.Infof(fmt.Sprintf("State of the pod after resize [%v] is [%v]",
					eachPodAfter.Name, eachPodAfter.Status.Phase))
				if eachPodAfter.Status.Phase != "Running" {
					log.FailOnError(fmt.Errorf("Pod [%v] Consuming Volume [%v] is not in Running State ",
						eachPodAfter.Name, eachVol.vol.Name), "Pod not in Running State")
				}
			}
		}

		// Destroy all apps
		DestroyApps(contexts, nil)
		appsDeleted = true

		// Restore all the Volumes from trashcan
		var trashcanVols []string
		stepLog = "validate volumes in trashcan"
		Step(stepLog, func() {
			// wait for few seconds for pvc to get deleted and volume to get detached
			time.Sleep(60 * time.Second)
			node := node.GetStorageDriverNodes()[0]
			log.InfoD(stepLog)
			trashcanVols, err = Inst().V.GetTrashCanVolumeIds(node)
			log.FailOnError(err, "error While getting trashcan volumes")
			log.Infof("trashcan len: %v", trashcanVols)
			dash.VerifyFatal(len(trashcanVols) > 0, true, "validate volumes exist in trashcan")

			for _, volsInTrash := range trashcanVols {
				if volsInTrash != "" {
					volReSize, err := Inst().V.InspectVolume(volsInTrash)
					log.FailOnError(err, "inspect returned error for volume [%v]?", volsInTrash)
					if !volReSize.InTrashcan {
						log.FailOnError(fmt.Errorf("Volume [%v] is still not in trashcan", volsInTrash),
							"is volume in trashcan after delete ?")
					}
				}
			}
		})

		log.Infof("list of volumes in trashcan [%v]", trashcanVols)

		// Get List of volumes present in the cluster before Restoring volumes
		allVols, err := Inst().V.ListAllVolumes()
		log.FailOnError(err, "failed to get list of volumes")
		log.Infof("List of all Volumes present in the cluster [%v]", allVols)

		stepLog = "validate restore volumes from trashcan"
		restoredVol := []string{}
		newUUID, err := uuid.NewRandom()
		log.FailOnError(err, "failed to get random UUID")

		Step(stepLog, func() {
			// Restore Volumes from trashcan
			for _, eachVol := range trashcanVols {
				if eachVol != "" {
					err = trashcanRestore(eachVol, fmt.Sprintf("restore_%v_%v", newUUID, eachVol))
					log.FailOnError(err, fmt.Sprintf("Failed restoring volume [%v] from trashcan", eachVol))
					restoredVol = append(restoredVol, fmt.Sprintf("restore_%v_%v", newUUID, eachVol))
				}
			}
		})

		// Get List of volumes present in the cluster before Restoring volumes
		allVolsAfterRestore, err := Inst().V.ListAllVolumes()
		log.FailOnError(err, "failed to get list of volumes")
		log.Infof("List of all Volumes present in the cluster after restore [%v]", allVolsAfterRestore)

		for _, eachVolume := range restoredVol {
			volReSize, err := Inst().V.InspectVolume(eachVolume)
			log.FailOnError(err, "inspect returned error for volume [%v]?", volReSize.Id)
			if strings.Contains(eachVolume, fmt.Sprintf("restore_%v", newUUID)) {
				log.Infof("Resizing Volume [%v]", eachVolume)
				// Resize Volume in parallel
				go ResizeVolumes(eachVolume)
			}
		}

		time.Sleep(60 * time.Second)
		// Sleep for some time and try deleting Volumes which were restore from trashcan
		for _, eachVolume := range restoredVol {
			if strings.Contains(eachVolume, fmt.Sprintf("restore_%v", newUUID)) {
				log.Infof("Deleting the volume [%v]", eachVolume)
				// Delete Volumes which are restored / Resized
				log.FailOnError(Inst().V.DeleteVolume(eachVolume), "failed to delete volumes")
			}
		}
		stepLog = "Disable trashcan on the cluster"
		Step(stepLog,
			func() {
				log.InfoD(stepLog)
				err := EnableTrashcanOnCluster("0")
				log.FailOnError(err, "failed to disable trashcan on the cluster")
			})

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})

})

var _ = Describe("{VolHAIncreaseAllVolumes}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("VolHAIncreaseAllVolumes", "Trigger vol HA Increase on all volumes at once", nil, 0)
	})

	itLog := "VolHAIncreaseAllVolumes"
	It(itLog, func() {
		var contexts []*scheduler.Context
		var wg sync.WaitGroup

		stepLog = "Schedule Applications on the cluster and get details of Volumes"
		Step(stepLog, func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("volresizeallvol-%d", i))...)
			}
		})
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		type volMap struct {
			ReplSet int64
			volObj  *volume.Volume
		}
		volHAMap := []*volMap{}

		revertReplica := func() {
			for _, eachvol := range volHAMap {
				getReplicaSets, err := Inst().V.GetReplicaSets(eachvol.volObj)
				log.FailOnError(err, "Failed to get replication factor on the volume")
				if len(getReplicaSets[0].Nodes) != int(eachvol.ReplSet) {
					log.Infof("Reverting Replication factor on Volume [%v] with ID [%v] to [%v]",
						eachvol.volObj.Name, eachvol.volObj.ID, eachvol.ReplSet)
					err := Inst().V.SetReplicationFactor(eachvol.volObj, eachvol.ReplSet,
						nil, nil, true)
					log.FailOnError(err, "failed to set replicaiton value of Volume [%v]", eachvol.volObj.Name)
				}
			}
		}

		setReplOnVolumes := func(vol *volume.Volume, curReplSet int64, wait bool, wg *sync.WaitGroup) {
			defer wg.Done()
			defer GinkgoRecover()
			var setRepl int64
			if curReplSet == 1 || curReplSet == 3 {
				setRepl = 2
			} else {
				setRepl = 3
			}
			opts := volume.Options{
				ValidateReplicationUpdateTimeout: replicationUpdateTimeout,
			}
			log.Infof("Setting Replication factor on Volume [%v] with ID [%v] to [%v]", vol.Name, vol.ID, setRepl)
			err = Inst().V.SetReplicationFactor(vol, setRepl, nil, nil, wait, opts)
			log.FailOnError(err, fmt.Sprintf("err setting repl factor  to %d for  vol : %s", setRepl, vol.Name))

		}
		defer revertReplica()

		// Wait for some time so that IO's will generate some data on all the volumes created so that
		// HA Update will take some time to finish
		time.Sleep(10 * time.Minute)

		getReplFactors := func(vol *volume.Volume) {
			defer wg.Done()
			defer GinkgoRecover()
			volDet := volMap{}
			curReplSet, err := Inst().V.GetReplicationFactor(vol)
			log.FailOnError(err, "failed to get replication factor of the volume")
			volDet.volObj = vol
			volDet.ReplSet = curReplSet
			log.Infof("Volume [%v] is with HA [%v]", volDet.volObj.Name, volDet.ReplSet)
			volHAMap = append(volHAMap, &volDet)
		}

		for _, eachCtx := range contexts {
			vols, err := Inst().S.GetVolumes(eachCtx)
			log.FailOnError(err, "Failed to get list of Volumes in the cluster")

			for _, eachVol := range vols {
				wg.Add(1)
				log.Infof("Get Repl factor for Volume [%v]", eachVol.Name)
				go getReplFactors(eachVol)
			}
		}
		wg.Wait()

		// Wait for all the Volumes in Clean State
		for _, eachVol := range volHAMap {
			log.FailOnError(WaitForVolumeClean(eachVol.volObj), "is Volume in clean state ?")
		}
		log.Infof("All Volumes are in clean state, proceeding with HA Update")

		// Set Repl Factor on all the volumes at ones
		for _, eachVol := range volHAMap {
			wg.Add(1)
			log.Infof("Set Repl on Volume [%v] to [%v]", eachVol.volObj.Name, eachVol.ReplSet)
			go setReplOnVolumes(eachVol.volObj, eachVol.ReplSet, false, &wg)
		}
		wg.Wait()

		// Wait for 2 min before validating the volume
		time.Sleep(2 * time.Minute)

		log.Infof("Waiting for all volumes in clean state")
		// Wait for all the Volumes in Clean State after starting Resync of the volume
		for _, eachVol := range volHAMap {
			log.FailOnError(WaitForVolumeClean(eachVol.volObj), "is Volume in clean state ?")
		}

		// Verify Repl Resync Completed after all volumes are in Clean state
		for _, eachVol := range volHAMap {
			curReplSet, err := Inst().V.GetReplicationFactor(eachVol.volObj)
			log.FailOnError(err, "failed to get replication factor of the volume")

			if eachVol.ReplSet == 3 || eachVol.ReplSet == 1 {
				dash.VerifyFatal(curReplSet == 2, true, fmt.Sprintf("Verify if HA Value is 2 for Volume [%v]", eachVol.volObj.Name))
			} else {
				dash.VerifyFatal(curReplSet == 3, true, fmt.Sprintf("Verify if HA Value is 3 for Volume [%v]", eachVol.volObj.Name))
			}
		}

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})