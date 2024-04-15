package tests

import (
	"fmt"
	"github.com/portworx/torpedo/pkg/log"
	"math/rand"
	"time"

	"github.com/libopenstorage/openstorage/api"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"k8s.io/apimachinery/pkg/types"

	"github.com/portworx/torpedo/pkg/testrailuttils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/portworx/torpedo/tests"
)

const (
	numApps = 30
)

// Legacy Shared Volume Create
// Automatically it should get created as Sharedv4 service volume.

var _ = Describe("{LegacySharedVolumeCreate}", func() {
	var testrailID = 296369
	// https://portworx.testrail.net/index.php?/cases/view/296369

	JustBeforeEach(func() {
		StartTorpedoTest("LegacySharedVolumeAppCreateVolume", "Legacy Shared to Sharedv4 Service CreateVolume", nil, testrailID)
		setCreateLegacySharedAsSharedv4Service(true)
	})

	volumeName := "legacy-shared-volume"
	stepLog := "Create legacy shared volume and check it got created as sharedv4 service volume"
	It(stepLog, func() {
		pxNodes, err := GetStorageNodes()
		log.FailOnError(err, "Unable to get the storage nodes")
		pxNode := GetRandomNode(pxNodes)
		log.Infof("Creating legacy shared volume: %s", volumeName)
		pxctlCmdFull := fmt.Sprintf("v c --shared=true %s", volumeName)
		output, err := Inst().V.GetPxctlCmdOutput(pxNode, pxctlCmdFull)
		log.FailOnError(err, fmt.Sprintf("error creating legacy shared volume %s", volumeName))
		log.Infof(output)
		vol, err := Inst().V.InspectVolume(volumeName)
		log.FailOnError(err, fmt.Sprintf("Inspect volume failed on volume {%v}", volumeName))
		dash.VerifyFatal(vol.Spec.Sharedv4, true, "sharedv4 volume was not created")
		dash.VerifyFatal(vol.Spec.Shared, false, "shared volume was created unexpectedly")
		pxctlCmdFull = fmt.Sprintf("v d %s --force", volumeName)
		output, _ = Inst().V.GetPxctlCmdOutput(pxNode, pxctlCmdFull)
		log.Infof(output)
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

func setCreateLegacySharedAsSharedv4Service(on bool) {
	pxNodes, err := GetStorageNodes()
	log.FailOnError(err, "Unable to get storage nodes")
	pxNode := GetRandomNode(pxNodes)
	log.Infof("Setting Creation of Legacy shared volumes")
	var pxctlCmdFull string
	pxctlCmdFull = fmt.Sprintf("cluster options update --create-legacy-shared-as-sharedv4-service=%t", on)
	_, err = Inst().V.GetPxctlCmdOutput(pxNode, pxctlCmdFull)
	log.FailOnError(err, fmt.Sprintf("error updating cluster option"))
	// Sleep so that the config variable can be updated on all nodes.
	time.Sleep(20 * time.Second)
}

func setMigrateLegacySharedToSharedv4Service(on bool) {
	pxNodes, err := GetStorageNodes()
	log.FailOnError(err, "Unable to get storage nodes")
	pxNode := GetRandomNode(pxNodes)
	log.Infof("Turning on Migration of Legacy shared volumes")
	var pxctlCmdFull string
	pxctlCmdFull = fmt.Sprintf("cluster options update --migrate-legacy-shared-to-sharedv4-service=%t", on)
	_, err = Inst().V.GetPxctlCmdOutput(pxNode, pxctlCmdFull)
	log.FailOnError(err, fmt.Sprintf("error updating cluster option"))
	// Sleep so that the config variable can be updated on all nodes.
	time.Sleep(20 * time.Second)

}

func getLegacySharedVolumeCount(contexts []*scheduler.Context) int {
	count := 0
	for _, ctx := range contexts {
		vols, err := Inst().S.GetVolumes(ctx)
		log.FailOnError(err, "error geting volumes used by app")
		for _, v := range vols {
			vol, err := Inst().V.InspectVolume(v.ID)
			log.FailOnError(err, "Failed to inspect volume %v", v.ID)
			if vol.Spec.Shared {
				count++
			}
		}
	}
	return count
}

func getLegacySharedTestAppVol(ctx *scheduler.Context) (*volume.Volume, *api.Volume, *node.Node) {
	vols, err := Inst().S.GetVolumes(ctx)
	log.FailOnError(err, "Failed to get volumes for app %s", ctx.App.Key)
	vol := vols[0]
	apiVol, err := Inst().V.InspectVolume(vol.ID)
	log.FailOnError(err, "Failed to Inspect volume [%v]", vol.ID)
	attachedNode, err := Inst().V.GetNodeForVolume(vol, 1*time.Minute, 5*time.Second)
	log.FailOnError(err, "Failed to Get Attached node for volume [%v]", vol.ID)
	log.Infof("volume %v {%v} is attached to node %v", vol.ID, apiVol.Id, attachedNode.Name)
	return vol, apiVol, attachedNode
}

func returnMapOfPodsUsingApiSharedVolumes(sharedVolPods map[types.UID]bool, sharedVols map[string]bool, ctx *scheduler.Context) {
	vols, err := Inst().S.GetVolumes(ctx)
	log.FailOnError(err, "Failed to get volumes for app %s", ctx.App.Key)
	for _, vol := range vols {
		apiVol, err := Inst().V.InspectVolume(vol.ID)
		log.FailOnError(err, "Failed to Inspect volume [%v]", vol.ID)
		if apiVol.Spec.Shared {
			sharedVols[vol.ID] = true
			pods, err := core.Instance().GetPodsUsingPV(vol.ID)
			log.FailOnError(err, "Failed to Pods using volume [%v]", vol.ID)
			for _, pod := range pods {
				sharedVolPods[pod.UID] = true
			}
		}
	}
	return
}

func checkVolsConvertedtoSharedv4Service(sharedVols map[string]bool) {
	for v := range sharedVols {
		apiVol, err := Inst().V.InspectVolume(v)
		log.FailOnError(err, "Failed to Inspect Volume [%v]", v)
		dash.VerifyFatal(apiVol.Spec.Shared, false, "legacy shared volume exists post migration")
		dash.VerifyFatal(apiVol.Spec.Sharedv4, true, "legacy shared volume not migrated to sharedv4 service")
	}
	return
}

func checkMapOfPods(sharedVolPods map[types.UID]bool, ctx *scheduler.Context) {
	vols, err := Inst().S.GetVolumes(ctx)
	log.FailOnError(err, "Failed to get volumes for app %s", ctx.App.Key)
	for _, vol := range vols {
		apiVol, err := Inst().V.InspectVolume(vol.ID)
		log.FailOnError(err, "Failed to Inspect Volume %v", vol.ID)
		if apiVol.Spec.Shared {
			pods, err := core.Instance().GetPodsUsingPV(vol.ID)
			log.FailOnError(err, "Failed to get pods using Volume %v", vol.ID)
			for _, pod := range pods {
				_, ok := sharedVolPods[pod.UID]
				dash.VerifyFatal(ok, false, fmt.Sprintf("pod using shared volume prior to migration remains after migration [%v]", pod.Name))
			}
		}
	}
	return
}

func waitAllSharedVolumesToGetMigrated(contexts []*scheduler.Context, maxWaitTime int) {
	i := 0
	for i < maxWaitTime {
		count := getLegacySharedVolumeCount(contexts)
		if count != 0 {
			time.Sleep(time.Minute)
			i++
			log.Infof("There are [%d] Legacy Shared Volumes. Waiting for them to be migrated", count)
		} else {
			break
		}
	}
	return
}

func createSnapshotsAndClones(volMap map[string]bool, snapshotSuffix, cloneSuffix string) {
	storageNodes, err := GetStorageNodes()
	log.FailOnError(err, "Unable to get the storage nodes")
	pxNode := storageNodes[rand.Intn(len(storageNodes))]
	for vol := range volMap {
		apiVol, err := Inst().V.InspectVolume(vol)
		log.FailOnError(err, "Failed to Inspect volume [%v]", vol)
		cloneName := fmt.Sprintf("%s-%s", vol, cloneSuffix)
		snapshotName := fmt.Sprintf("%s-%s", vol, snapshotSuffix)
		pxctlCloneCmd := fmt.Sprintf("volume clone %s --name %s", apiVol.Id, cloneName)
		pxctlSnapshotCmd := fmt.Sprintf("volume snapshot create %s --name %s", apiVol.Id, snapshotName)
		output, err := Inst().V.GetPxctlCmdOutput(pxNode, pxctlCloneCmd)
		log.FailOnError(err, fmt.Sprintf("error creating clone for volumes %s", apiVol.Id))
		log.Infof(output)
		output, err = Inst().V.GetPxctlCmdOutput(pxNode, pxctlSnapshotCmd)
		log.FailOnError(err, fmt.Sprintf("error creating snapshot for volumes %s", apiVol.Id))
		log.Infof(output)
	}
	return
}

func deleteSnapshotsAndClones(volMap map[string]bool, snapshotSuffix, cloneSuffix string) {
	storageNodes, err := GetStorageNodes()
	log.FailOnError(err, "Unable to get the storage nodes")
	pxNode := storageNodes[rand.Intn(len(storageNodes))]
	log.Infof("Deleting Snapshots and Clones that were created")
	for vol := range volMap {
		_, err := Inst().V.InspectVolume(vol)
		if err == nil {
			//Delete Volumes should not fail, even if there are errors.
			cloneName := fmt.Sprintf("%s-%s", vol, cloneSuffix)
			snapshotName := fmt.Sprintf("%s-%s", vol, snapshotSuffix)
			pxctlCloneCmd := fmt.Sprintf("volume delete %s --force", cloneName)
			pxctlSnapshotCmd := fmt.Sprintf("volume delete %s --force", snapshotName)
			log.Infof("Deleting clone %s", cloneName)
			output, _ := Inst().V.GetPxctlCmdOutput(pxNode, pxctlCloneCmd)
			log.Infof(output)
			log.Infof("Deleting snapshot %s", snapshotName)
			output, _ = Inst().V.GetPxctlCmdOutput(pxNode, pxctlSnapshotCmd)
			log.Infof(output)
		}
	}
	return
}

// Create Legacy Shared Volumes.
// Turn on Migration, no Apps required, volumes should get converted to sharedv4 service volume.

var _ = Describe("{LegacySharedVolumeMigrate_CreateIdle}", func() {
	var testrailID = 296370
	volumeName := "legacy-shared-volume-idle"
	JustBeforeEach(func() {
		StartTorpedoTest("LegacySharedVolumeIdleVolume", "Legacy Shared to Sharedv4 Service Idle Volume", nil, testrailID)
		setCreateLegacySharedAsSharedv4Service(false)
		setMigrateLegacySharedToSharedv4Service(false)
	})
	stepLog := "Create legacy shared volume and check it is created as shared volume. Then enable migration"
	It(stepLog, func() {
		pxctlCmdFull := fmt.Sprintf("v c --shared=true %s", volumeName)
		pxNodes, err := GetStorageNodes()
		log.FailOnError(err, "Unable to get the storage nodes")
		pxNode := GetRandomNode(pxNodes)
		log.Infof("Creating legacy shared volume: %s", volumeName)
		pxctlCmdFull = fmt.Sprintf("v c --shared=true %s", volumeName)
		output, err := Inst().V.GetPxctlCmdOutput(pxNode, pxctlCmdFull)
		log.FailOnError(err, fmt.Sprintf("error creating legacy shared volume %s", volumeName))
		log.Infof(output)

		vol, err := Inst().V.InspectVolume(volumeName)
		log.FailOnError(err, fmt.Sprintf("Inspect volume failed on volume {%v}", volumeName))
		dash.VerifyFatal(vol.Spec.Shared, true, "non-shared volume created unexpectedly")
		setMigrateLegacySharedToSharedv4Service(true)
		migrated := false
		for i := 0; i < 6; i++ {
			vol, err := Inst().V.InspectVolume(volumeName)
			log.FailOnError(err, fmt.Sprintf("Inspect volume failed on volume {%v}", volumeName))
			if !vol.Spec.Shared && vol.Spec.Sharedv4 {
				migrated = true
				break
			}
			time.Sleep(1 * time.Minute)
		}
		dash.VerifyFatal(migrated, true, fmt.Sprintf("migration failed on volume [%v]", volumeName))
		pxctlCmdFull = fmt.Sprintf("v d %s --force", volumeName)
		Inst().V.GetPxctlCmdOutput(pxNode, pxctlCmdFull)
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
	})
})

// Basic migration Test case:
// Create apps, start migration.
// apps should restart, shared volume should be
var _ = Describe("{LegacySharedVolumeAppMigrateBasic}", func() {
	var testrailID = 296374
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("LegacySharedVolumeAppMigrateBasic", "Legacy Shared to Sharedv4 Service Functional Test", nil, testrailID)
		namespacePrefix := "lstsv4mbasic"
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		setCreateLegacySharedAsSharedv4Service(false)
		setMigrateLegacySharedToSharedv4Service(false)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", namespacePrefix, i))...)
		}
		// TODO: Skip non legacy shared tests
		ValidateApplications(contexts)
	})

	It("has to verify migration is successful and pods are restarted", func() {
		// podMap is a map of the pods using shared volumes.
		// volMap is the list of shared volumes.

		podMap := make(map[types.UID]bool)
		volMap := make(map[string]bool)
		for _, ctx := range contexts {
			returnMapOfPodsUsingApiSharedVolumes(podMap, volMap, ctx)
		}
		totalSharedVolumes := getLegacySharedVolumeCount(contexts)
		timeForMigration := ((totalSharedVolumes + 30) / 30) * 10
		setMigrateLegacySharedToSharedv4Service(true)
		waitAllSharedVolumesToGetMigrated(contexts, timeForMigration)
		countPostTimeout := getLegacySharedVolumeCount(contexts)
		dash.VerifyFatal(countPostTimeout == 0, true, fmt.Sprintf("Expected legacy shared volume to be 0 but is %d", countPostTimeout))
		checkVolsConvertedtoSharedv4Service(volMap)
		for _, ctx := range contexts {
			checkMapOfPods(podMap, ctx)
		}
		ValidateApplications(contexts)
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
		DestroyApps(contexts, nil)
	})
})

var _ = Describe("{LegacySharedToSharedv4ServiceMigrationBasicMany}", func() {
	var testrailID = 296728
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("LegacySharedVolumeAppMigrateMany", "Legacy Shared to Sharedv4 Service Functional Test with Many Volumes", nil, testrailID)
		namespacePrefix := "lstsv4mbasic2"
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		setCreateLegacySharedAsSharedv4Service(false)
		setMigrateLegacySharedToSharedv4Service(false)
		contexts = make([]*scheduler.Context, 0)
		numberNameSpaces := Inst().GlobalScaleFactor
		if numberNameSpaces < numApps {
			numberNameSpaces = numApps
		}
		for i := 0; i < numberNameSpaces; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", namespacePrefix, i))...)
		}
		// TODO: Skip non legacy shared tests
		ValidateApplications(contexts)
	})

	stepLog := "Start Migration and wait till all volumes have migrated"
	It(stepLog, func() {
		podMap := make(map[types.UID]bool)
		volMap := make(map[string]bool)
		for _, ctx := range contexts {
			returnMapOfPodsUsingApiSharedVolumes(podMap, volMap, ctx)
		}
		timeForMigration := ((len(volMap) + 30) / 30) * 10
		setMigrateLegacySharedToSharedv4Service(true)
		waitAllSharedVolumesToGetMigrated(contexts, timeForMigration)
		countPostTimeout := getLegacySharedVolumeCount(contexts)
		dash.VerifyFatal(countPostTimeout == 0, true, fmt.Sprintf("Expected legacy shared volume to be 0 but is %d", countPostTimeout))
		checkVolsConvertedtoSharedv4Service(volMap)
		for _, ctx := range contexts {
			checkMapOfPods(podMap, ctx)
		}
		ValidateApplications(contexts)
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
		DestroyApps(contexts, nil)
	})
})

var _ = Describe("{LegacySharedToSharedv4ServiceMigrationRestart}", func() {
	var testrailID = 296736
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("LegacySharedVolumeAppMigrationRestart", "Legacy Shared to Sharedv4 Service Functional Test with Many Volumes", nil, testrailID)
		namespacePrefix := "lstsv4m-re"
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		setCreateLegacySharedAsSharedv4Service(false)
		setMigrateLegacySharedToSharedv4Service(false)
		contexts = make([]*scheduler.Context, 0)
		numberNameSpaces := Inst().GlobalScaleFactor
		if numberNameSpaces < numApps {
			numberNameSpaces = numApps
		}
		for i := 0; i < numberNameSpaces; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", namespacePrefix, i))...)
		}
		// TODO: Skip non legacy shared tests
		ValidateApplications(contexts)
	})

	ItLog := "Start Migration and after 3 minutes stop migration"
	It(ItLog, func() {
		podMap := make(map[types.UID]bool)
		volMap := make(map[string]bool)
		for _, ctx := range contexts {
			returnMapOfPodsUsingApiSharedVolumes(podMap, volMap, ctx)
		}
		setMigrateLegacySharedToSharedv4Service(true)
		time.Sleep(210 * time.Second) // sleep 3.5 minutes.
		totalSharedVolumes := getLegacySharedVolumeCount(contexts)
		timeForMigration := ((totalSharedVolumes + 30) / 30) * 10

		stepLog := "Pause Migration and let all Apps come up and restart Migration"
		Step(stepLog, func() {
			setMigrateLegacySharedToSharedv4Service(false)
			ValidateApplications(contexts)
			time.Sleep(time.Minute)
			setMigrateLegacySharedToSharedv4Service(true)
			waitAllSharedVolumesToGetMigrated(contexts, timeForMigration)
			countPostTimeout := getLegacySharedVolumeCount(contexts)
			dash.VerifyFatal(countPostTimeout == 0, true, fmt.Sprintf("Post migration count is [%d] instead of 0", countPostTimeout))
			checkVolsConvertedtoSharedv4Service(volMap)
			for _, ctx := range contexts {
				checkMapOfPods(podMap, ctx)
			}
			ValidateApplications(contexts)
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
		DestroyApps(contexts, nil)
	})
})

var _ = Describe("{LegacySharedToSharedv4ServicePxRestart}", func() {
	var testrailID = 296732
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("LegacySharedVolumeAppMigrationRestart", "Legacy Shared to Sharedv4 Service Functional Test with Many Volumes", nil, testrailID)
		namespacePrefix := "lstsv4m-px-mig-res"
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		setCreateLegacySharedAsSharedv4Service(false)
		setMigrateLegacySharedToSharedv4Service(false)
		contexts = make([]*scheduler.Context, 0)
		numberNameSpaces := Inst().GlobalScaleFactor
		if numberNameSpaces < numApps {
			numberNameSpaces = numApps
		}
		for i := 0; i < numberNameSpaces; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", namespacePrefix, i))...)
		}
		// TODO: Skip non legacy shared tests
		ValidateApplications(contexts)
	})

	ItLog := "Start Migration and after 3 minutes restart px on a random Storage Node"
	It(ItLog, func() {
		podMap := make(map[types.UID]bool)
		volMap := make(map[string]bool)
		for _, ctx := range contexts {
			returnMapOfPodsUsingApiSharedVolumes(podMap, volMap, ctx)
		}
		setMigrateLegacySharedToSharedv4Service(true)
		time.Sleep(210 * time.Second) // sleep 3.5 minutes.

		stepLog := "Restart px and let all Apps come up and restart Migration"
		Step(stepLog, func() {
			storageNodes, err := GetStorageNodes()
			log.FailOnError(err, "Unable to get the storage nodes")
			pxNode := storageNodes[rand.Intn(len(storageNodes))]
			err = Inst().V.RestartDriver(pxNode, nil)
			log.FailOnError(err, fmt.Sprintf("error restarting px on node %s", pxNode.Name))
			err = Inst().V.WaitDriverUpOnNode(pxNode, 5*time.Minute)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", pxNode.Name))
		})

		totalSharedVolumes := getLegacySharedVolumeCount(contexts)
		timeForMigration := ((totalSharedVolumes + 30) / 30) * 10
		waitAllSharedVolumesToGetMigrated(contexts, timeForMigration)
		countPostTimeout := getLegacySharedVolumeCount(contexts)
		dash.VerifyFatal(countPostTimeout == 0, true, fmt.Sprintf("Post migration count is [%d] instead of 0", countPostTimeout))
		checkVolsConvertedtoSharedv4Service(volMap)
		for _, ctx := range contexts {
			checkMapOfPods(podMap, ctx)
		}
		ValidateApplications(contexts)
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
		DestroyApps(contexts, nil)
	})
})

var _ = Describe("{LegacySharedToSharedv4ServiceNodeDecommission}", func() {
	var testrailID = 296732
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("LegacySharedServiceNodeDecomssion", "Legacy Shared to Sharedv4 Service Functional Test with Node Decommission", nil, testrailID)
		namespacePrefix := "lstsv4m-node-decom"
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		setCreateLegacySharedAsSharedv4Service(false)
		setMigrateLegacySharedToSharedv4Service(false)
		contexts = make([]*scheduler.Context, 0)
		numberNameSpaces := Inst().GlobalScaleFactor
		if numberNameSpaces < numApps {
			numberNameSpaces = numApps
		}
		for i := 0; i < numberNameSpaces; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", namespacePrefix, i))...)
		}
		// TODO: Skip non legacy shared tests
		ValidateApplications(contexts)
	})

	ItLog := "Start Migration and after 3 minutes Decommission a random Storage Node"
	It(ItLog, func() {
		podMap := make(map[types.UID]bool)
		volMap := make(map[string]bool)
		for _, ctx := range contexts {
			returnMapOfPodsUsingApiSharedVolumes(podMap, volMap, ctx)
		}
		setMigrateLegacySharedToSharedv4Service(true)
		time.Sleep(210 * time.Second) // sleep 3.5 minutes.

		storageNodes, err := GetStorageNodes()
		log.FailOnError(err, "Unable to get the storage nodes")
		pxNode := storageNodes[rand.Intn(len(storageNodes))]
		err = Inst().S.PrepareNodeToDecommission(pxNode, Inst().Provisioner)
		log.FailOnError(err, fmt.Sprintf("error preparing node %s for decommision", pxNode.Name))
		stepLog := "Decommission Node while Migration is in Progress"
		Step(stepLog, func() {
			err = Inst().V.DecommissionNode(&pxNode)
			log.FailOnError(err, fmt.Sprintf("error in decommision of node %s ", pxNode.Name))
		})

		stepLog = "Validate migration process after node Decommission"
		Step(stepLog, func() {
			totalSharedVolumes := getLegacySharedVolumeCount(contexts)
			timeForMigration := ((totalSharedVolumes + 30) / 30) * 10
			waitAllSharedVolumesToGetMigrated(contexts, timeForMigration)
			countPostTimeout := getLegacySharedVolumeCount(contexts)
			dash.VerifyFatal(countPostTimeout == 0, true, fmt.Sprintf("Post migration count is [%d] instead of 0", countPostTimeout))
			checkVolsConvertedtoSharedv4Service(volMap)
			for _, ctx := range contexts {
				checkMapOfPods(podMap, ctx)
			}
			ValidateApplications(contexts)
		})
		stepLog = fmt.Sprintf("Rejoin node %s", pxNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			//reboot required to remove encrypted dm devices if any
			err := Inst().N.RebootNode(pxNode, node.RebootNodeOpts{
				Force: true,
				ConnectionOpts: node.ConnectionOpts{
					Timeout:         defaultCommandTimeout,
					TimeBeforeRetry: defaultRetryInterval,
				},
			})
			log.FailOnError(err, fmt.Sprintf("error rebooting node %s", pxNode.Name))
			err = Inst().V.RejoinNode(&pxNode)
			dash.VerifyFatal(err, nil, "Validate node rejoin init")
			var rejoinedNode *api.StorageNode
			t := func() (interface{}, bool, error) {
				drvNodes, err := Inst().V.GetDriverNodes()
				if err != nil {
					return false, true, err
				}

				for _, n := range drvNodes {
					if n.Hostname == pxNode.Hostname {
						rejoinedNode = n
						return true, false, nil
					}
				}

				return false, true, fmt.Errorf("node %s not joined yet", pxNode.Name)
			}
			_, err = task.DoRetryWithTimeout(t, 15*time.Minute, defaultRetryInterval)
			log.FailOnError(err, fmt.Sprintf("error joining the node [%s]", pxNode.Name))
			dash.VerifyFatal(rejoinedNode != nil, true, fmt.Sprintf("verify node [%s] rejoined PX cluster", pxNode.Name))
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
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
		DestroyApps(contexts, nil)
	})
})

var _ = Describe("{LegacySharedToSharedv4ServiceRestartCoordinator}", func() {
	var testrailID = 296732
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("LegacySharedVolumeAppRestartCoordinator", "Legacy Shared to Sharedv4 Service Migration and coordinator restart", nil, testrailID)
		namespacePrefix := "lstsv4m-px-res-cd"
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		setCreateLegacySharedAsSharedv4Service(false)
		setMigrateLegacySharedToSharedv4Service(false)
		contexts = make([]*scheduler.Context, 0)
		numberNameSpaces := Inst().GlobalScaleFactor
		if numberNameSpaces < numApps {
			numberNameSpaces = numApps
		}
		for i := 0; i < numberNameSpaces; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", namespacePrefix, i))...)
		}
		// TODO: Skip non legacy shared tests
		ValidateApplications(contexts)
	})

	ItLog := "Start Migration and after 2 minutes restart volume coordinator node"
	It(ItLog, func() {
		podMap := make(map[types.UID]bool)
		volMap := make(map[string]bool)
		for _, ctx := range contexts {
			returnMapOfPodsUsingApiSharedVolumes(podMap, volMap, ctx)
		}
		var nodeForPxRestart *node.Node
		for _, ctx := range contexts {
			_, apiVol, attachedNode := getLegacySharedTestAppVol(ctx)
			if apiVol.Spec.Shared {
				nodeForPxRestart = attachedNode
				break
			}
		}
		setMigrateLegacySharedToSharedv4Service(true)
		time.Sleep(120 * time.Second) // sleep 2 minutes.

		stepLog := "Decommission Node while Migration is in Progress"
		Step(stepLog, func() {
			err := Inst().V.RestartDriver(*nodeForPxRestart, nil)
			log.FailOnError(err, fmt.Sprintf("error in Restart PX Driver of node %s ", nodeForPxRestart.Name))
			err = Inst().V.WaitDriverUpOnNode(*nodeForPxRestart, 5*time.Minute)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", nodeForPxRestart.Name))
		})

		totalSharedVolumes := getLegacySharedVolumeCount(contexts)
		timeForMigration := ((totalSharedVolumes + 30) / 30) * 10
		waitAllSharedVolumesToGetMigrated(contexts, timeForMigration)
		countPostTimeout := getLegacySharedVolumeCount(contexts)
		dash.VerifyFatal(countPostTimeout == 0, true, fmt.Sprintf("Post migration count is [%d] instead of 0", countPostTimeout))
		checkVolsConvertedtoSharedv4Service(volMap)
		for _, ctx := range contexts {
			checkMapOfPods(podMap, ctx)
		}
		ValidateApplications(contexts)
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
		DestroyApps(contexts, nil)
	})
})

var _ = Describe("{LegacySharedToSharedv4ServiceCreateSnapshotsClones}", func() {
	var testrailID = 0
	var runID int
	podMap := make(map[types.UID]bool)
	volMap := make(map[string]bool)
	JustBeforeEach(func() {
		StartTorpedoTest("LegacySharedVolumeAppRestartCoordinator", "Legacy Shared to Sharedv4 Service Migration with creation of snapshots and clones", nil, testrailID)
		namespacePrefix := "lstsv4m-snap-clone"
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		setCreateLegacySharedAsSharedv4Service(false)
		setMigrateLegacySharedToSharedv4Service(false)
		contexts = make([]*scheduler.Context, 0)
		numberNameSpaces := Inst().GlobalScaleFactor
		if numberNameSpaces < 5 {
			numberNameSpaces = 5
		}
		for i := 0; i < numberNameSpaces; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", namespacePrefix, i))...)
		}
		// TODO: Skip non legacy shared tests
		ValidateApplications(contexts)
	})

	ItLog := "Start Migration "
	It(ItLog, func() {
		for _, ctx := range contexts {
			returnMapOfPodsUsingApiSharedVolumes(podMap, volMap, ctx)
		}
		createSnapshotsAndClones(volMap, "snapshot-1", "clone-1")
		setMigrateLegacySharedToSharedv4Service(true)
		time.Sleep(120 * time.Second) // sleep 2 minutes.

		createSnapshotsAndClones(volMap, "snapshot-2", "clone-2")

		totalSharedVolumes := getLegacySharedVolumeCount(contexts)
		timeForMigration := ((totalSharedVolumes + 30) / 30) * 10
		waitAllSharedVolumesToGetMigrated(contexts, timeForMigration)
		countPostTimeout := getLegacySharedVolumeCount(contexts)
		dash.VerifyFatal(countPostTimeout == 0, true, fmt.Sprintf("Post migration count is [%d] instead of 0", countPostTimeout))
		checkVolsConvertedtoSharedv4Service(volMap)
		for _, ctx := range contexts {
			checkMapOfPods(podMap, ctx)
		}
		ValidateApplications(contexts)
	})
	JustAfterEach(func() {
		// Delete even if there are failures.
		defer EndTorpedoTest()
		deleteSnapshotsAndClones(volMap, "snapshot-1", "clone-1")
		deleteSnapshotsAndClones(volMap, "snapshot-2", "clone-2")
		AfterEachTest(contexts, testrailID, runID)
		DestroyApps(contexts, nil)
	})
})

var _ = Describe("{LegacySharedToSharedv4ServicePxRestartAll}", func() {
	var testrailID = 296732
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("LegacySharedVolumePxRestartAll", "Legacy Shared to Sharedv4 Service Functional Test with restart px on all nodes", nil, testrailID)
		namespacePrefix := "lstsv4m-px-res-all"
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		setCreateLegacySharedAsSharedv4Service(false)
		setMigrateLegacySharedToSharedv4Service(false)
		contexts = make([]*scheduler.Context, 0)
		numberNameSpaces := Inst().GlobalScaleFactor
		if numberNameSpaces < numApps {
			numberNameSpaces = numApps
		}
		for i := 0; i < numberNameSpaces; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", namespacePrefix, i))...)
		}
		// TODO: Skip non legacy shared tests
		ValidateApplications(contexts)
	})

	ItLog := "Start Migration and after 3 minutes restart px on a random Storage Node"
	It(ItLog, func() {
		podMap := make(map[types.UID]bool)
		volMap := make(map[string]bool)
		for _, ctx := range contexts {
			returnMapOfPodsUsingApiSharedVolumes(podMap, volMap, ctx)
		}
		setMigrateLegacySharedToSharedv4Service(true)
		time.Sleep(210 * time.Second) // sleep 3.5 minutes.

		stepLog := "Restart px on all nodes and let all Apps come up and restart Migration"
		Step(stepLog, func() {
			storageNodes, err := GetStorageNodes()
			log.FailOnError(err, "Unable to get the storage nodes")
			for i := 0; i < len(storageNodes); i++ {
				err = Inst().V.RestartDriver(storageNodes[i], nil)
				log.FailOnError(err, fmt.Sprintf("error restarting px on node %s", storageNodes[i].Name))
			}
			for i := 0; i < len(storageNodes); i++ {
				err = Inst().V.WaitDriverUpOnNode(storageNodes[i], 10*time.Minute)
				log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", storageNodes[i].Name))
			}
		})

		totalSharedVolumes := getLegacySharedVolumeCount(contexts)
		timeForMigration := ((totalSharedVolumes + 30) / 30) * 10
		waitAllSharedVolumesToGetMigrated(contexts, timeForMigration)
		countPostTimeout := getLegacySharedVolumeCount(contexts)
		dash.VerifyFatal(countPostTimeout == 0, true, fmt.Sprintf("Post migration count is [%d] instead of 0", countPostTimeout))
		checkVolsConvertedtoSharedv4Service(volMap)
		for _, ctx := range contexts {
			checkMapOfPods(podMap, ctx)
		}
		ValidateApplications(contexts)
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
		DestroyApps(contexts, nil)
	})
})

var _ = Describe("{LegacySharedToSharedv4ServicePxKill}", func() {
	var testrailID = 296732
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("LegacySharedVolumePxkill", "Legacy Shared to Sharedv4 Service Functional Test with restart px kill on one nodes", nil, testrailID)
		namespacePrefix := "lstsv4m-px-kill"
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		setCreateLegacySharedAsSharedv4Service(false)
		setMigrateLegacySharedToSharedv4Service(false)
		contexts = make([]*scheduler.Context, 0)
		numberNameSpaces := Inst().GlobalScaleFactor
		if numberNameSpaces < numApps {
			numberNameSpaces = numApps
		}
		for i := 0; i < numberNameSpaces; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", namespacePrefix, i))...)
		}
		// TODO: Skip non legacy shared tests
		ValidateApplications(contexts)
	})

	ItLog := "Start Migration and after 3 minutes restart px on a random Storage Node"
	It(ItLog, func() {
		podMap := make(map[types.UID]bool)
		volMap := make(map[string]bool)
		for _, ctx := range contexts {
			returnMapOfPodsUsingApiSharedVolumes(podMap, volMap, ctx)
		}
		setMigrateLegacySharedToSharedv4Service(true)
		time.Sleep(180 * time.Second) // sleep 3 minutes.

		stepLog := "kill px on one nodes and let all Apps come"
		Step(stepLog, func() {
			storageNodes, err := GetStorageNodes()
			log.FailOnError(err, "Unable to get the storage nodes")
			pxNode := GetRandomNode(storageNodes)
			err = Inst().V.KillPXDaemon([]node.Node{pxNode}, nil)
			log.FailOnError(err, fmt.Sprintf("error restarting px on node %s", pxNode.Name))
			err = Inst().V.WaitDriverUpOnNode(pxNode, 6*time.Minute)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", pxNode.Name))
		})

		totalSharedVolumes := getLegacySharedVolumeCount(contexts)
		timeForMigration := ((totalSharedVolumes + 30) / 30) * 10
		waitAllSharedVolumesToGetMigrated(contexts, timeForMigration)
		countPostTimeout := getLegacySharedVolumeCount(contexts)
		dash.VerifyFatal(countPostTimeout == 0, true, fmt.Sprintf("Post migration count is [%d] instead of 0", countPostTimeout))
		checkVolsConvertedtoSharedv4Service(volMap)
		for _, ctx := range contexts {
			checkMapOfPods(podMap, ctx)
		}
		ValidateApplications(contexts)
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
		DestroyApps(contexts, nil)
	})
})
