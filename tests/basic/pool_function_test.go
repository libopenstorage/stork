package tests

import (
	"fmt"
	"regexp"

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
		poolToBeResized = getStoragePool(poolIDToResize)
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
			poolToBeResized = getStoragePool(poolIDToResize)
			originalSizeInBytes = poolToBeResized.TotalSize
			targetSizeInBytes = originalSizeInBytes + 100*units.GiB
			targetSizeGiB = targetSizeInBytes / units.GiB

			log.InfoD("Current Size of pool %s is %d GiB. Expand to %v GiB with type add-disk...",
				poolIDToResize, poolToBeResized.TotalSize/units.GiB, targetSizeGiB)
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
			originalSizeInBytes = poolToBeResized.TotalSize
			targetSizeInBytes = originalSizeInBytes + 100*units.GiB
			targetSizeGiB = targetSizeInBytes / units.GiB

			log.InfoD("Current Size of pool %s is %d GiB. Expand to %v GiB with type resize-disk...",
				poolIDToResize, poolToBeResized.TotalSize/units.GiB, targetSizeGiB)
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
		poolToBeResized = getStoragePool(poolIDToResize)
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
		originalSizeInBytes = poolToBeResized.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB = targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type add-disk",
			poolIDToResize, poolToBeResized.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK)
		resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
		verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
	})

	It("Select a pool and expand it by 100 GiB with resize-disk type. ", func() {
		StartTorpedoTest("PoolExpandDiskResize",
			"Validate storage pool expansion with type=resize-disk", nil, 0)
		originalSizeInBytes = poolToBeResized.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB = targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type resize-disk",
			poolIDToResize, poolToBeResized.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK)
		resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
		verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
	})

	It("Select a pool and expand it by 100 GiB with auto type. ", func() {
		StartTorpedoTest("PoolExpandDiskAuto",
			"Validate storage pool expansion with type=auto ", nil, 0)
		originalSizeInBytes = poolToBeResized.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB = targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type auto",
			poolIDToResize, poolToBeResized.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_AUTO)
		resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
		verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
	})
})

var _ = Describe("{PoolExpandWithReboot}", func() {
	BeforeEach(func() {
		contexts = scheduleApps()
	})

	JustBeforeEach(func() {
		poolIDToResize = pickPoolToResize()
		log.Infof("Picked pool %s to resize", poolIDToResize)
		poolToBeResized = getStoragePool(poolIDToResize)
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
			originalSizeInBytes = poolToBeResized.TotalSize
			targetSizeInBytes = originalSizeInBytes + 100*units.GiB
			targetSizeGiB = targetSizeInBytes / units.GiB
			log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type add-disk",
				poolIDToResize, poolToBeResized.TotalSize/units.GiB, targetSizeGiB)
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
		poolToBeResized = getStoragePool(poolIDToResize)
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

	It("Initiate pool expansion using add-drive and restart PX", func() {
		StartTorpedoTest("PoolExpandAddDiskAndPXRestart",
			"Initiate pool expansion using add-drive and restart PX", nil, testrailID)

		Step("Select a pool that has I/O and expand it by 100 GiB with add-disk type. ", func() {
			originalSizeInBytes = poolToBeResized.TotalSize
			targetSizeInBytes = originalSizeInBytes + 100*units.GiB
			targetSizeGiB = targetSizeInBytes / units.GiB
			log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type add-disk",
				poolIDToResize, poolToBeResized.TotalSize/units.GiB, targetSizeGiB)
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

	BeforeEach(func() {
		StartTorpedoTest("PoolExpandResizeInvalidPoolID",
			"Initiate pool expansion using invalid Id", nil, testrailID)
	})

	AfterEach(func() {
		EndTorpedoTest()
	})

	stepLog := "Resize with invalid pool ID"
	log.InfoD(stepLog)
	It(stepLog, func() {
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
			dash.VerifyFatal(errMatch, nil, "Pool expand with invalid PoolUUID completed?")
		})
	})

})

var _ = Describe("{PoolExpandDiskAddAndVerifyFromOtherNode}", func() {

	BeforeEach(func() {
		StartTorpedoTest("PoolExpandDiskAddAndVerifyFromOtherNode",
			"Initiate pool expansion and verify from other node", nil, testrailID)
		contexts = scheduleApps()
	})

	JustBeforeEach(func() {
		poolIDToResize = pickPoolToResize()
		log.Infof("Picked pool %s to resize", poolIDToResize)
		poolToBeResized = getStoragePool(poolIDToResize)
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
	log.InfoD(stepLog)
	It(stepLog, func() {
		// get original total size
		provisionStatus, err := GetClusterProvisionStatusOnSpecificNode(*storageNode)
		var orignalTotalSize float64
		for _, pstatus := range provisionStatus {
			if pstatus.NodeUUID == storageNode.Id {
				orignalTotalSize += pstatus.TotalSize
			}
		}

		originalSizeInBytes = poolToBeResized.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB = targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type add-disk",
			poolIDToResize, poolToBeResized.TotalSize/units.GiB, targetSizeGiB)
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

	BeforeEach(func() {
		StartTorpedoTest("PoolExpansionDiskResizeInvalidSize",
			"Initiate pool expansion using invalid expansion size", nil, testrailID)
	})

	AfterEach(func() {
		EndTorpedoTest()
	})

	stepLog := "select a pool and expand it by 30000000 GiB with resize-disk type"
	log.InfoD(stepLog)
	It(stepLog, func() {
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
		dash.VerifyFatal(errMatch, nil, "Pool expand with invalid PoolUUID completed?")
	})

})
