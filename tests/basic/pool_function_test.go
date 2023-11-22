package tests

import (
	"fmt"
	"reflect"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/libopenstorage/openstorage/api"
	. "github.com/onsi/ginkgo"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	"github.com/portworx/torpedo/pkg/units"
	. "github.com/portworx/torpedo/tests"
)

var (
	stepLog       string
	runID         int
	testrailID    int
	targetSizeGiB uint64
	storageNode   *node.Node
	err           error
)
var _ = Describe("{PoolExpandMultipleTimes}", func() {
	BeforeEach(func() {
		contexts = scheduleApps()
	})

	JustBeforeEach(func() {
		poolIDToResize = pickPoolToResize()
		log.Infof("Picked pool %s to resize", poolIDToResize)
		poolToResize = getStoragePool(poolIDToResize)
	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})

	AfterEach(func() {
		appsValidateAndDestroy(contexts)
		EndTorpedoTest()
	})

	It("Select a pool and expand it by 100 GiB 3 time with add-disk type. ", func() {
		StartTorpedoTest("PoolExpandDiskAdd3Times",
			"Validate storage pool expansion 3 times with type=add-disk", nil, 0)
		for i := 0; i < 3; i++ {
			poolToResize = getStoragePool(poolIDToResize)
			originalSizeInBytes = poolToResize.TotalSize
			targetSizeInBytes = originalSizeInBytes + 100*units.GiB
			targetSizeGiB = targetSizeInBytes / units.GiB

			log.InfoD("Current Size of pool %s is %d GiB. Expand to %v GiB with type add-disk...",
				poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
			triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK)
			resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
			dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
		}
	})

	It("Select a pool and expand it by 100 GiB 3 times with resize-disk type. ", func() {
		StartTorpedoTest("PoolExpandDiskResize3Times",
			"Validate storage pool expansion with type=resize-disk", nil, 0)
		for i := 0; i < 3; i++ {
			originalSizeInBytes = poolToResize.TotalSize
			targetSizeInBytes = originalSizeInBytes + 100*units.GiB
			targetSizeGiB = targetSizeInBytes / units.GiB

			log.InfoD("Current Size of pool %s is %d GiB. Expand to %v GiB with type resize-disk...",
				poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
			triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK)
			resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
			dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
		}
	})
})

var _ = Describe("{PoolExpandSmoky}", func() {
	BeforeEach(func() {
		contexts = scheduleApps()
	})

	JustBeforeEach(func() {
		poolIDToResize = pickPoolToResize()
		log.Infof("Picked pool %s to resize", poolIDToResize)
		poolToResize = getStoragePool(poolIDToResize)
	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})

	AfterEach(func() {
		appsValidateAndDestroy(contexts)
		EndTorpedoTest()
	})

	It("Select a pool and expand it by 100 GiB with add-disk type. ", func() {
		StartTorpedoTest("PoolExpandDiskAdd",
			"Validate storage pool expansion with type=add-disk", nil, 0)
		originalSizeInBytes = poolToResize.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB = targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type add-disk",
			poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK)
		resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
		verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
	})

	It("Select a pool and expand it by 100 GiB with resize-disk type. ", func() {
		StartTorpedoTest("PoolExpandDiskResize",
			"Validate storage pool expansion with type=resize-disk", nil, 0)
		originalSizeInBytes = poolToResize.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB = targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type resize-disk",
			poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK)
		resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
		verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
	})

	It("Select a pool and expand it by 100 GiB with auto type. ", func() {
		StartTorpedoTest("PoolExpandDiskAuto",
			"Validate storage pool expansion with type=auto ", nil, 0)
		originalSizeInBytes = poolToResize.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB = targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type auto",
			poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_AUTO)
		resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
		verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
	})

})

var _ = Describe("{PoolExpandRejectConcurrent}", func() {
	BeforeEach(func() {
		contexts = scheduleApps()
	})

	JustBeforeEach(func() {
		poolIDToResize = pickPoolToResize()
		log.Infof("Picked pool %s to resize", poolIDToResize)
		poolToResize = getStoragePool(poolIDToResize)
		resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(resizeErr, nil, "Previous pool expansion(s) should not result in error")
		storageNode, err = GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "Failed to get node with given pool ID")
	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})

	AfterEach(func() {
		appsValidateAndDestroy(contexts)
		EndTorpedoTest()
	})

	// test resizing all pools on one storage node concurrently and ensure only the first one makes progress
	It("Select all pools on a storage node and expand them concurrently. ", func() {
		// TestRail:https://portworx.testrail.net/index.php?/tests/view/34542836&group_by=cases:custom_automated&group_order=desc&group_id=2
		StartTorpedoTest("PoolExpandRejectConcurrent",
			"Validate storage pool expansion rejects concurrent requests", nil, 34542836)
		var pools []*api.StoragePool
		Step("Verify multiple pools are present on this node", func() {
			// collect all pools available
			for _, p := range storageNode.Pools {
				pools = append(pools, p)
			}
			dash.VerifyFatal(len(pools) > 1, true, "This test requires more than 1 pool.")
		})

		Step("Expand all pools concurrently. ", func() {
			expandType := api.SdkStoragePool_RESIZE_TYPE_ADD_DISK
			var wg sync.WaitGroup
			for _, p := range pools {
				wg.Add(1)
				go func(p *api.StoragePool) {
					defer wg.Done()
					err = Inst().V.ExpandPool(p.Uuid, expandType, p.TotalSize/units.GiB+100, true)
				}(p)
			}
			wg.Wait()
		})

		Step("Verify only one expansion is making progress at any given time", func() {
			inProgressCount := 0
			startTime := time.Now()
			for time.Since(startTime) < 1*time.Minute {
				inProgressCount = 0
				time.Sleep(5)
				storageNode, err = GetNodeWithGivenPoolID(poolIDToResize)
				for _, p := range storageNode.Pools {
					if p.LastOperation.Status == api.SdkStoragePool_OPERATION_IN_PROGRESS {
						inProgressCount++
					}
					dash.VerifyFatal(inProgressCount <= 1, true, "Only one pool expansion should be in progress at any given time.")
				}
			}
		})
	})

	// test expansion request on a pool while a previous expansion is in progress is rejected
	It("Expand a pool while a previous expansion is in progress", func() {
		expandType := api.SdkStoragePool_RESIZE_TYPE_ADD_DISK
		targetSize := poolToResize.TotalSize/units.GiB + 100
		err = Inst().V.ExpandPool(poolIDToResize, expandType, targetSize, true)
		// wait for expansion to start
		// TODO: this is a hack to wait for expansion to start. The existing WaitForExpansionToStart() risks returning
		// when the expansion has already completed.
		time.Sleep(1)
		// verify pool expansion is in progress
		isExpandInProgress, expandErr := poolResizeIsInProgress(poolToResize)
		if expandErr != nil {
			log.Fatalf("Error checking if pool expansion is in progress: %v", expandErr)
		}
		if !isExpandInProgress {
			log.Warnf("Pool expansion already finished. Skipping this test. Using a testing app that writes " +
				"more data which may slow down add-disk type expansion. ")
			return
		}
		expandResponse := Inst().V.ExpandPoolUsingPxctlCmd(*storageNode, poolToResize.Uuid, expandType, targetSize+100, true)
		dash.VerifyFatal(expandResponse != nil, true, "Pool expansion should fail when expansion is in progress")
		dash.VerifyFatal(strings.Contains(expandResponse.Error(), "is already in progress"), true,
			"Pool expansion failure reason should be communicated to the user	")
	})
})

var _ = Describe("{PoolExpandWithReboot}", func() {
	BeforeEach(func() {
		contexts = scheduleApps()
	})

	JustBeforeEach(func() {
		poolIDToResize = pickPoolToResize()
		log.Infof("Picked pool %s to resize", poolIDToResize)
		poolToResize = getStoragePool(poolIDToResize)
		storageNode, err = GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "Failed to get node with given pool ID")
	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})

	AfterEach(func() {
		appsValidateAndDestroy(contexts)
		EndTorpedoTest()
	})

	It("Initiate pool expansion using add-disk and reboot node", func() {
		StartTorpedoTest("PoolExpandDiskAddWithReboot", "Initiate pool expansion using add-disk and reboot node", nil, 51309)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		Step("Select a pool that has I/O and expand it by 100 GiB with add-disk type. ", func() {
			originalSizeInBytes = poolToResize.TotalSize
			targetSizeInBytes = originalSizeInBytes + 100*units.GiB
			targetSizeGiB = targetSizeInBytes / units.GiB
			log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type add-disk",
				poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
			triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK)
		})

		Step("Wait for expansion to start and reboot node", func() {
			err := WaitForExpansionToStart(poolIDToResize)
			log.FailOnError(err, "Timed out waiting for expansion to start")
			err = RebootNodeAndWait(*storageNode)
			log.FailOnError(err, "Failed to reboot node and wait till it is up")
		})

		Step("Ensure pool has been expanded to the expected size", func() {
			err = waitForOngoingPoolExpansionToComplete(poolIDToResize)
			dash.VerifyFatal(err, nil, "Pool expansion does not result in error")
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
		})
	})
})

var _ = Describe("{PoolExpandWithPXRestart}", func() {
	BeforeEach(func() {
		contexts = scheduleApps()
	})

	JustBeforeEach(func() {
		poolIDToResize = pickPoolToResize()
		log.Infof("Picked pool %s to resize", poolIDToResize)
		poolToResize = getStoragePool(poolIDToResize)
		storageNode, err = GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "Failed to get node with given pool ID")
	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})

	AfterEach(func() {
		appsValidateAndDestroy(contexts)
		EndTorpedoTest()
	})

	It("Restart PX after pool expansion", func() {
		StartTorpedoTest("RestartAfterPoolExpansion",
			"Restart PX after pool expansion", nil, testrailID)

		Step("Select a pool that has I/O and expand it by 100 GiB with add-disk type. ", func() {
			originalSizeInBytes = poolToResize.TotalSize
			targetSizeInBytes = originalSizeInBytes + 100*units.GiB
			targetSizeGiB = targetSizeInBytes / units.GiB
			log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type add-disk",
				poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
			triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK)
		})

		Step("Wait for expansion to finish and restart PX", func() {
			resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
			dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
			log.FailOnError(Inst().V.RestartDriver(*storageNode, nil),
				fmt.Sprintf("Error restarting px on node [%s]", storageNode.Name))
			log.FailOnError(Inst().V.WaitDriverUpOnNode(*storageNode, addDriveUpTimeOut),
				fmt.Sprintf("Timed out waiting for px to come up on node [%s]", storageNode.Name))
		})

		Step("Ensure pool is up and running", func() {
			// Ensure pool is up and running
			poolToResize = getStoragePool(poolIDToResize)
			// Ensure poolToResize is not nil
			dash.VerifyFatal(poolToResize != nil, true, "Pool is up and running after restart")
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
		})
	})

	It("Initiate pool expansion using add-drive and restart PX", func() {
		StartTorpedoTest("PoolExpandAddDiskAndPXRestart",
			"Initiate pool expansion using add-drive and restart PX", nil, testrailID)

		Step("Select a pool that has I/O and expand it by 100 GiB with add-disk type. ", func() {
			originalSizeInBytes = poolToResize.TotalSize
			targetSizeInBytes = originalSizeInBytes + 100*units.GiB
			targetSizeGiB = targetSizeInBytes / units.GiB
			log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type add-disk",
				poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
			triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK)
		})

		Step("Wait for expansion to start and reboot node", func() {
			err := WaitForExpansionToStart(poolIDToResize)
			log.FailOnError(err, "Timed out waiting for expansion to start")
			err = Inst().V.RestartDriver(*storageNode, nil)
			log.FailOnError(err, fmt.Sprintf("Error restarting px on node [%s]", storageNode.Name))
			err = Inst().V.WaitDriverUpOnNode(*storageNode, addDriveUpTimeOut)
			log.FailOnError(err, fmt.Sprintf("Timed out waiting for px to come up on node [%s]", storageNode.Name))
		})

		Step("Ensure pool has been expanded to the expected size", func() {
			resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
			dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
		})
	})
})

var _ = Describe("{PoolExpandResizeInvalidPoolID}", func() {

	var testrailID = 34542946
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/34542946

	BeforeEach(func() {
		StartTorpedoTest("PoolExpandResizeInvalidPoolID",
			"Initiate pool expansion using invalid Id", nil, testrailID)
	})

	AfterEach(func() {
		EndTorpedoTest()
	})

	stepLog := "Resize with invalid pool ID"
	It(stepLog, func() {
		log.InfoD(stepLog)
		// invalidPoolUUID Generation
		invalidPoolUUID := uuid.New().String()

		// Resize Pool with Invalid Pool ID
		stepLog = fmt.Sprintf("Expanding pool on Node UUID [%s] using auto", invalidPoolUUID)
		Step(stepLog, func() {
			resizeErr := Inst().V.ExpandPool(invalidPoolUUID, api.SdkStoragePool_RESIZE_TYPE_AUTO, 100, true)
			dash.VerifyFatal(resizeErr != nil, true, "Verify error occurs with invalid Pool UUID")
			// Verify error on pool expansion failure
			var errMatch error
			re := regexp.MustCompile(fmt.Sprintf(".*failed to find storage pool with UID.*%s.*",
				invalidPoolUUID))
			if !re.MatchString(fmt.Sprintf("%v", resizeErr)) {
				errMatch = fmt.Errorf("failed to verify failure using invalid PoolUUID [%v]", invalidPoolUUID)
			}
			dash.VerifyFatal(errMatch, nil, "Pool expand with invalid PoolUUID failed as expected.")
		})
	})

})

var _ = Describe("{PoolExpandDiskAddAndVerifyFromOtherNode}", func() {

	var testrailID = 34542840
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/34542840

	BeforeEach(func() {
		StartTorpedoTest("PoolExpandDiskAddAndVerifyFromOtherNode",
			"Initiate pool expansion and verify from other node", nil, testrailID)
		contexts = scheduleApps()
	})

	JustBeforeEach(func() {
		poolIDToResize = pickPoolToResize()
		log.Infof("Picked pool %s to resize", poolIDToResize)
		poolToResize = getStoragePool(poolIDToResize)
		storageNode, err = GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "Failed to get node with given pool ID")
	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})

	AfterEach(func() {
		appsValidateAndDestroy(contexts)
		EndTorpedoTest()
	})

	stepLog := "should get the existing pool and expand it by adding a disk and verify from other node"
	It(stepLog, func() {
		log.InfoD(stepLog)
		// get original total size
		provisionStatus, err := GetClusterProvisionStatusOnSpecificNode(*storageNode)
		var orignalTotalSize float64
		for _, pstatus := range provisionStatus {
			if pstatus.NodeUUID == storageNode.Id {
				orignalTotalSize += pstatus.TotalSize
			}
		}

		originalSizeInBytes = poolToResize.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB = targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type add-disk",
			poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK)

		Step("Ensure pool has been expanded to the expected size", func() {
			err = waitForOngoingPoolExpansionToComplete(poolIDToResize)
			dash.VerifyFatal(err, nil, "Pool expansion does not result in error")
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
		})

		stNodes, err := GetStorageNodes()
		log.FailOnError(err, "Unable to get the storage nodes")
		var verifyNode node.Node
		for _, node := range stNodes {
			status, _ := IsPxRunningOnNode(&node)
			if node.Id != storageNode.Id && status {
				verifyNode = node
				break
			}
		}

		// get final total size
		provisionStatus, err = GetClusterProvisionStatusOnSpecificNode(verifyNode)
		var finalTotalSize float64
		for _, pstatus := range provisionStatus {
			if pstatus.NodeUUID == storageNode.Id {
				finalTotalSize += pstatus.TotalSize
			}
		}
		dash.VerifyFatal(finalTotalSize > orignalTotalSize, true, "Pool expansion failed, pool size is not greater than pool size before expansion")

	})

})

var _ = Describe("{PoolExpansionDiskResizeInvalidSize}", func() {

	var testrailID = 34542945
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/34542945

	BeforeEach(func() {
		StartTorpedoTest("PoolExpansionDiskResizeInvalidSize",
			"Initiate pool expansion using invalid expansion size", nil, testrailID)
	})

	AfterEach(func() {
		EndTorpedoTest()
	})

	stepLog := "select a pool and expand it by 30000000 GiB with resize-disk type"
	It(stepLog, func() {
		log.InfoD(stepLog)
		// pick pool to resize
		pools, err := GetAllPoolsPresent()
		log.FailOnError(err, "Unable to get the storage Pools")
		pooltoPick := pools[0]

		resizeErr := Inst().V.ExpandPool(pooltoPick, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 30000000, true)
		dash.VerifyFatal(resizeErr != nil, true, "Verify error occurs with invalid Pool expansion size")

		// Verify error on pool expansion failure
		var errMatch error
		re := regexp.MustCompile(`.*cannot be expanded beyond maximum size.*`)
		if !re.MatchString(fmt.Sprintf("%v", resizeErr)) {
			errMatch = fmt.Errorf("failed to verify failure using invalid Pool size")
		}
		dash.VerifyFatal(errMatch, nil, "Pool expand with invalid PoolUUID failed as expected.")
	})

})

var _ = Describe("{PoolExpandResizeWithSameSize}", func() {

	var testrailID = 34542944
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/34542944

	BeforeEach(func() {
		StartTorpedoTest("PoolExpandResizeWithSameSize",
			"Initiate pool expansion using same size", nil, testrailID)
	})

	AfterEach(func() {
		EndTorpedoTest()
	})

	stepLog := "select a pool and expand it by same pool size with resize-disk type"
	It(stepLog, func() {
		log.InfoD(stepLog)
		// pick pool to resize
		pools, err := GetAllPoolsPresent()
		log.FailOnError(err, "Unable to get the storage Pools")
		pooltoPick := pools[0]
		poolToResize = getStoragePool(pooltoPick)

		originalSizeGiB := poolToResize.TotalSize / units.GiB
		targetSizeGiB = originalSizeGiB
		resizeErr := Inst().V.ExpandPool(pooltoPick, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, targetSizeGiB, true)
		dash.VerifyFatal(resizeErr != nil, true, "Verify error occurs with same pool size")

		// Verify error on pool expansion failure
		var errMatch error
		re := regexp.MustCompile(`.*already at a size.*`)
		if !re.MatchString(fmt.Sprintf("%v", resizeErr)) {
			errMatch = fmt.Errorf("failed to verify failure using same Pool size")
		}
		dash.VerifyFatal(errMatch, nil, "Pool expand with Same Pool Size failed as expected.")
	})
})

var _ = Describe("{PoolExpandWhileResizeDiskInProgress}", func() {

	var testrailID = 34542896
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/34542896

	BeforeEach(func() {
		StartTorpedoTest("PoolExpandWhileResizeDiskInProgress",
			"Initiate pool expansion on a pool where one pool expansion is already in progress", nil, testrailID)
		contexts = scheduleApps()
	})

	JustBeforeEach(func() {
		poolIDToResize = pickPoolToResize()
		log.Infof("Picked pool %s to resize", poolIDToResize)
		poolToResize = getStoragePool(poolIDToResize)
		storageNode, err = GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "Failed to get node with given pool ID")
	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})

	AfterEach(func() {
		appsValidateAndDestroy(contexts)
		EndTorpedoTest()
	})

	stepLog := "should get the existing pool and expand it by initiating a resize-disk and again trigger pool expand on same pool"
	It(stepLog, func() {
		log.InfoD(stepLog)

		originalSizeInBytes = poolToResize.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB = targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type resize-disk",
			poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK)

		// we are using pxctl command direclty as we dont want retries and Inst().V.ExpandPool does not returns required error
		pxctlCmdFull := fmt.Sprintf("pxctl sv pool expand -u %s -s %d -o resize-disk ", poolIDToResize, targetSizeGiB)

		// Execute the command and check the alerts of type POOL
		_, err := Inst().N.RunCommandWithNoRetry(*storageNode, pxctlCmdFull, node.ConnectionOpts{
			Timeout:         1 * time.Minute,
			TimeBeforeRetry: 10 * time.Second,
			IgnoreError:     false,
		})

		// Verify error on pool expansion failure
		var errMatch error
		re := regexp.MustCompile(`.*already in progress.*`)
		if !re.MatchString(fmt.Sprintf("%v", err)) {
			errMatch = fmt.Errorf("failed to verify pool expand when one already in progress")
		}
		dash.VerifyFatal(errMatch, nil, "Pool expand with one resize already in Porgress failed as expected.")

		Step("Ensure pool has been expanded to the expected size", func() {
			err = waitForOngoingPoolExpansionToComplete(poolIDToResize)
			dash.VerifyFatal(err, nil, "Pool expansion does not result in error")
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
		})

	})

})

var _ = Describe("{PoolExpandResizePoolMaintenanceCycle}", func() {
	var testrailID = 34542842
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/34542842

	BeforeEach(func() {
		StartTorpedoTest("PoolExpandResizePoolMaintenanceCycle",
			"Initiate pool expansion and do a maintenance cycle after resize", nil, testrailID)
		contexts = scheduleApps()
	})

	JustBeforeEach(func() {
		poolIDToResize = pickPoolToResize()
		log.Infof("Picked pool %s to resize", poolIDToResize)
		poolToResize = getStoragePool(poolIDToResize)
		storageNode, err = GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "Failed to get node with given pool ID")
	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})

	AfterEach(func() {
		appsValidateAndDestroy(contexts)
		EndTorpedoTest()
	})

	stepLog := "cycle through maintenance mode after pool expand is complete"
	It(stepLog, func() {
		log.InfoD(stepLog)

		originalSizeInBytes = poolToResize.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB = targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type add-disk",
			poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK)

		err = waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(err, nil, "Pool expansion does not result in error")
		verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)

		// Enter Maintenance Mode
		err = Inst().V.EnterMaintenance(*storageNode)
		log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenance mode", storageNode.Name))
		status, err := Inst().V.GetNodeStatus(*storageNode)
		log.FailOnError(err, fmt.Sprintf("Error getting PX status of node %s", storageNode.Name))
		dash.VerifyFatal(*status, api.Status_STATUS_MAINTENANCE, fmt.Sprintf("Node %s Status not Online", storageNode.Name))

		// Exit Maintenance Mode
		err = Inst().V.ExitMaintenance(*storageNode)
		log.FailOnError(err, fmt.Sprintf("fail to exit node %s in maintenance mode", storageNode.Name))
		status, err = Inst().V.GetNodeStatus(*storageNode)
		log.FailOnError(err, fmt.Sprintf("Error getting PX status of node %s", storageNode.Name))
		dash.VerifyFatal(*status, api.Status_STATUS_OK, fmt.Sprintf("Node %s Status not Online", storageNode.Name))

		// verify pool size after maintenance cycle
		verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)

		// check pool status is healthy after maintenance cycle
		poolsStatus, err := Inst().V.GetNodePoolsStatus(*storageNode)
		log.FailOnError(err, "error getting pool status on node %s", storageNode.Name)
		dash.VerifyFatal(poolsStatus[poolIDToResize], "Online", fmt.Sprintf("Pool %s Status not Online", poolIDToResize))
	})
})

var _ = Describe("{PoolExpandResizeDiskInMaintenanceMode}", func() {
	var testrailID = 34542861
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/34542861

	/*
		Steps:
			1. Move a node to maintenance mode.
			2. Initiate pool expand with resize-disk operation.
			3. Exit out of maintenance mode (PX only performs pool expand in normal mode, not in maintenance mode)
			4. Verify pool expand operation goes to completion.
	*/

	BeforeEach(func() {
		StartTorpedoTest("PoolExpandResizeDiskInMaintenanceMode",
			"Initiate pool expand with resize-disk when node is already in maintenance mode", nil, testrailID)
		contexts = scheduleApps()
	})

	JustBeforeEach(func() {
		poolIDToResize = pickPoolToResize()
		log.Infof("Picked pool %s to resize", poolIDToResize)
		poolToResize = getStoragePool(poolIDToResize)
	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})

	AfterEach(func() {
		appsValidateAndDestroy(contexts)
		EndTorpedoTest()
	})

	stepLog := "Start pool expand with resize-disk on node which is already in maintenance mode "
	It(stepLog, func() {
		log.InfoD(stepLog)
		var nodeDetail *node.Node
		var err error
		stepLog = "Move node to maintenance mode"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			nodeDetail, err = GetNodeWithGivenPoolID(poolToResize.Uuid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Failed to get Node Details using PoolUUID [%v]", poolToResize.Uuid))

			log.InfoD("Bring Node to Maintenance Mode")
			err = Inst().V.EnterMaintenance(*nodeDetail)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Failed to shift Node [%s] to Mainteinance Mode", nodeDetail.Name))
		})

		stepLog = "Initiate pool expand with resize-disk operation"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			originalSizeInBytes = poolToResize.TotalSize
			targetSizeInBytes = originalSizeInBytes + 100*units.GiB
			targetSizeGiB = targetSizeInBytes / units.GiB

			log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type resize-disk",
				poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
			err := Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, targetSizeGiB, true)
			dash.VerifyFatal(err, nil, "pool expansion requested successfully")
		})

		stepLog = "Exit node out of maintenance mode"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			log.InfoD("Bring Node out of Maintenance Mode")
			err = Inst().V.ExitMaintenance(*nodeDetail)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Failed to shift Node [%s] out of Mainteinance Mode", nodeDetail.Name))
		})

		stepLog = "Verify pool expand completes successfully"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
			dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
		})
	})
})

var _ = Describe("{PoolExpandAddDiskInMaintenanceMode}", func() {
	var testrailID = 34542888
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/34542888

	/*
		Steps:
			1. Move a node to maintenance mode.
			2. Initiate pool expand with add-disk operation.
			3. Exit out of maintenance mode (PX only performs pool expand in normal mode, not in maintenance mode)
			4. Verify pool expand operation goes to completion.
	*/

	BeforeEach(func() {
		StartTorpedoTest("PoolExpandAddDiskInMaintenanceMode",
			"Initiate pool expand with add-disk when node is already in maintenance mode", nil, testrailID)
		contexts = scheduleApps()
	})

	JustBeforeEach(func() {
		poolIDToResize = pickPoolToResize()
		log.Infof("Picked pool %s to resize", poolIDToResize)
		poolToResize = getStoragePool(poolIDToResize)
	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})

	AfterEach(func() {
		appsValidateAndDestroy(contexts)
		EndTorpedoTest()
	})

	stepLog := "Start pool expand with add-disk on node which is already in maintenance mode "
	It(stepLog, func() {
		log.InfoD(stepLog)
		var nodeDetail *node.Node
		var err error
		stepLog = "Move node to maintenance mode"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			nodeDetail, err = GetNodeWithGivenPoolID(poolToResize.Uuid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Failed to get Node Details using PoolUUID [%v]", poolToResize.Uuid))

			log.InfoD("Bring Node to Maintenance Mode")
			err = Inst().V.EnterMaintenance(*nodeDetail)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Failed to shift Node [%s] to Mainteinance Mode", nodeDetail.Name))
		})

		stepLog = "Initiate pool expand with add-disk operation"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			originalSizeInBytes = poolToResize.TotalSize
			targetSizeInBytes = originalSizeInBytes + 100*units.GiB
			targetSizeGiB = targetSizeInBytes / units.GiB

			log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type add-disk",
				poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
			err := Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, targetSizeGiB, true)
			dash.VerifyFatal(err, nil, "pool expansion requested successfully")
		})

		stepLog = "Exit node out of maintenance mode"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			log.InfoD("Bring Node out of Maintenance Mode")
			err = Inst().V.ExitMaintenance(*nodeDetail)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Failed to shift Node [%s] out of Mainteinance Mode", nodeDetail.Name))
		})

		stepLog = "Verify pool expand completes successfully"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
			dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
		})
	})
})

var _ = Describe("{StorageFullPoolExpansion}", func() {
	var (
		appList      []string
		selectedNode *node.Node
	)

	BeforeEach(func() {
		Inst().AppList = []string{"fio-fastpath-repl1"}
		contexts = ScheduleApplications("storagefull-resize")
		appList = Inst().AppList
	})

	JustBeforeEach(func() {
		selectedNode = GetNodeWithLeastSize()
		_ = Inst().S.AddLabelOnNode(*selectedNode, k8s.NodeType, k8s.FastpathNodeType)
		log.FailOnError(err, fmt.Sprintf("Failed to add fastpath label on node %v", selectedNode.Name))
	})

	AfterEach(func() {
		Inst().AppList = appList
		appsValidateAndDestroy(contexts)
		_ = Inst().S.RemoveLabelOnNode(*selectedNode, k8s.NodeType)
	})

	It("Expand pool with resize-disk type after pool is down due to storage full", func() {
		// https://portworx.testrail.net/index.php?/cases/view/51280
		StartTorpedoTest("StorageFullPoolResize", "Feed a pool full, then expand the pool in type resize-disk", nil, 51280)
		Step("Prepare a full pool to expand", func() {
			err = WaitForPoolOffline(*selectedNode)
			log.FailOnError(err, fmt.Sprintf("Timed out waiting to load a pool and bring node %s storage down", selectedNode.Name))
			poolsStatus, err := Inst().V.GetNodePoolsStatus(*selectedNode)
			log.FailOnError(err, "error getting pool status on node %s", selectedNode.Name)
			for i, s := range poolsStatus {
				if s == "Offline" {
					poolIDToResize = i
					poolToResize, err = GetStoragePoolByUUID(poolIDToResize)
					log.FailOnError(err, "error getting pool with UUID [%s]", poolIDToResize)
					break
				}
			}
		})

		Step("Expand the full pool in type resize-disk", func() {
			targetSizeGiB = (poolToResize.TotalSize / units.GiB) * 2
			log.InfoD("Current Size of the pool %s is %d, trying to expand it to double the size", poolToResize.Uuid, poolToResize.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToResize.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, targetSizeGiB, true)
			dash.VerifyFatal(err, nil, "Pool expansion init should be successful.")
		})

		Step("Verify that pool expansion is successful", func() {
			err = waitForOngoingPoolExpansionToComplete(poolToResize.Uuid)
			log.FailOnError(err, fmt.Sprintf("Error waiting for pool %s resize", poolToResize.Uuid))
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
			status, err := Inst().V.GetNodeStatus(*selectedNode)
			log.FailOnError(err, fmt.Sprintf("Error getting PX status of node %s", selectedNode.Name))
			dash.VerifySafely(*status, api.Status_STATUS_OK, fmt.Sprintf("validate PX status on node %s", selectedNode.Name))
		})
  })
})
     

var _ = Describe("{PoolExpandTestLimits}", func() {
	BeforeEach(func() {
		contexts = scheduleApps()
	})

	JustBeforeEach(func() {
		poolIDToResize = pickPoolToResize()
		log.Infof("Picked pool %s to resize", poolIDToResize)
		poolToResize = getStoragePool(poolIDToResize)
	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})

	AfterEach(func() {
		appsValidateAndDestroy(contexts)
		EndTorpedoTest()
	})

	It("Initiate pool expansion (DMThin) to its limits (15 TiB)", func() {
		var testrailID = 51292
		// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51292

		StartTorpedoTest("PoolExpandTestWithin15TiBLimit",
			"Initiate pool expansion using resize-disk to 15 TiB target size", nil, testrailID)

		// To achieve total pool size of 15 TiB:
		// 1. Add another drive of same size for pool to have 2 drives.
		// 2. Perform resize-disk operation which is faster than pool rebalance
		//    due to adding a new 7 TiB drive.
		targetSizeGiB := (poolToResize.TotalSize / units.GiB) * 2

		log.InfoD("Next trying to expand the pool %s to %v GiB with type add-disk",
			poolIDToResize, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK)
		resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(resizeErr, nil, "Pool expansion should not result in error")
		verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)

		targetSizeTiB := uint64(15)
		targetSizeInBytes = targetSizeTiB * units.TiB
		targetSizeGiB = targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v TiB with type resize-disk",
			poolIDToResize, targetSizeGiB, targetSizeTiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK)
		resizeErr = waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(resizeErr, nil, "Pool expansion should not result in error")
		verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)

	})

	It("Expand pool to 20 TiB (beyond max supported capacity for DMThin) with add-disk type. ", func() {
		var testrailID = 50643
		// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/50643

		StartTorpedoTest("DMThinPoolExpandBeyond15TiBLimit",
			"Initiate pool expansion using add-disk to 20 TiB target size", nil, testrailID)
		isDMthin, err := IsDMthin()
		dash.VerifyFatal(err, nil, "error verifying if set up is DMTHIN enabled")
		dash.VerifyFatal(isDMthin, true, "DMThin/PX-Storev2 is not enabled on underlaying PX cluster. Skipping `PoolExpandTestBeyond15TiBLimit` test.")

		targetSizeTiB := uint64(20)
		targetSizeInBytes = targetSizeTiB * units.TiB
		targetSizeGiB = targetSizeInBytes / units.GiB
		log.InfoD("Trying to expand pool %s to %v TiB with type add-disk",
			poolIDToResize, targetSizeTiB)
		err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, targetSizeGiB, true)
		dash.VerifyFatal(err != nil, true, "DMThin pool expansion to 20 TB should result in error")
	})
})


var _ = Describe("{PoolExpandAndCheckAlertsUsingResizeDisk}", func() {

	var testrailID = 34542894
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/34542894

	BeforeEach(func() {
		StartTorpedoTest("PoolExpandAndCheckAlertsUsingResizeDisk", "pool expansion using resize-disk and check alerts after each operation", nil, testrailID)
		contexts = scheduleApps()
	})
	JustBeforeEach(func() {
		poolIDToResize = pickPoolToResize()
		log.Infof("Picked pool %s to resize", poolIDToResize)
		storageNode, err = GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "Failed to get node with given pool ID")
	})
	JustAfterEach(func() {
		AfterEachTest(contexts)
	})

	AfterEach(func() {
		appsValidateAndDestroy(contexts)
		EndTorpedoTest()
	})

	It("pool expansion using resize-disk and check alerts after each operation", func() {
		log.InfoD("Initiate pool expansion using resize-disk")
		poolToResize = getStoragePool(poolIDToResize)
		originalSizeInBytes = poolToResize.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB = targetSizeInBytes / units.GiB
		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type resize-disk", poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK)

		resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")

		log.Infof("Check the alert for pool expand for pool uuid %s", poolIDToResize)
		alertExists, _ := checkAlertsForPoolExpansion(poolIDToResize, targetSizeGiB)
		dash.VerifyFatal(alertExists, true, "Verify Alert is Present")
	})

})

func checkAlertsForPoolExpansion(poolIDToResize string, targetSizeGiB uint64) (bool, error) {
	// Get the node to check the pool show output
	n := node.GetStorageDriverNodes()[0]
	// Below command to change when PWX-28484 is fixed
	cmd := "pxctl alerts show| grep -e POOL"
	// Execute the command and check the alerts of type POOL
	out, err := Inst().N.RunCommandWithNoRetry(n, cmd, node.ConnectionOpts{
		Timeout:         2 * time.Minute,
		TimeBeforeRetry: 10 * time.Second,
	})
	log.FailOnError(err, "Unable to execute the alerts show command")
	outLines := strings.Split(out, "\n")
	substr := "[0-9]+ GiB"
	re := regexp.MustCompile(substr)

	for _, l := range outLines {
		line := strings.Trim(l, " ")
		if strings.Contains(line, "PoolExpandSuccessful") && strings.Contains(line, poolIDToResize) {
			if re.MatchString(line) {
				matchedSize := re.FindStringSubmatch(line)[0]
				poolSize := matchedSize[:len(matchedSize)-4]
				poolSizeUint, _ := strconv.ParseUint(poolSize, 10, 64)
				if poolSizeUint >= targetSizeGiB {
					log.Infof("The Alert generated is %s", line)
					return true, nil
				}
			}
		}
	}
	return false, fmt.Errorf("Alert not found")

}


var _ = Describe("{CheckPoolLabelsAfterResizeDisk}", func() {

	var testrailID = 34542904
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/34542904

	BeforeEach(func() {
		StartTorpedoTest("CheckPoolLabelsAfterResizeDisk",
			"Initiate pool expansion and Newly set pool labels should persist post pool expand resize-disk operation", nil, testrailID)
		contexts = scheduleApps()
	})

	JustBeforeEach(func() {
		poolIDToResize = pickPoolToResize()
		log.Infof("Picked pool %s to resize", poolIDToResize)
		poolToResize = getStoragePool(poolIDToResize)
		storageNode, err = GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "Failed to get node with given pool ID")

	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})

	AfterEach(func() {
		appsValidateAndDestroy(contexts)
		EndTorpedoTest()
	})

	It("Initiate pool expansion and Newly set pool labels should persist post pool expand resize-disk operation", func() {
		log.InfoD("set pool label, before pool expand")
		labelBeforeExpand := poolToResize.Labels
		poolLabelToUpdate := make(map[string]string)
		poolLabelToUpdate["cust-type"] = "test-label"
		// Update the pool label
		err = Inst().V.UpdatePoolLabels(*storageNode, poolIDToResize, poolLabelToUpdate)
		dash.VerifyFatal(err, nil, "Check if able to update the label on the pool")

		log.InfoD("expand pool using resize-disk")
		originalSizeInBytes = poolToResize.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB = targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type resize-disk",
			poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK)

		err = waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(err, nil, "Pool expansion does not result in error")
		verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)

		log.InfoD("check pool label, after pool expand")
		poolToResize = getStoragePool(poolIDToResize)
		labelAfterExpand := poolToResize.Labels
		result := reflect.DeepEqual(labelBeforeExpand, labelAfterExpand)
		dash.VerifyFatal(result, true, "Check if labels changed after pool expand")
	})

})
