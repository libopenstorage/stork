package tests

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/google/uuid"
	"github.com/libopenstorage/openstorage/api"
	. "github.com/onsi/ginkgo/v2"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
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
		// TestRail:https://portworx.testrail.net/index.php?/tests/view/86355092
		StartTorpedoTest("PoolExpandDiskAdd3Times",
			"Validate storage pool expansion 3 times with type=add-disk", nil, 86355092)
		for i := 0; i < 3; i++ {
			poolToResize = getStoragePool(poolIDToResize)
			originalSizeInBytes = poolToResize.TotalSize
			targetSizeInBytes = originalSizeInBytes + 100*units.GiB
			targetSizeGiB = targetSizeInBytes / units.GiB

			log.InfoD("Current Size of pool %s is %d GiB. Expand to %v GiB with type add-disk...",
				poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
			triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK)
			resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)

			if isDMthin, _ := IsDMthin(); isDMthin {
				dash.VerifyFatal(resizeErr != nil, true,
					"Pool expansion request of add-disk type should be rejected with dmthin")
			} else {
				dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
				verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
			}
		}
	})

	It("Select a pool and expand it by 100 GiB 3 times with resize-disk type. ", func() {
		// TestRail:https://portworx.testrail.net/index.php?/tests/view/86355067
		StartTorpedoTest("PoolExpandDiskResize3Times",
			"Validate storage pool expansion with type=resize-disk", nil, 86355067)
		for i := 0; i < 3; i++ {
			poolToResize = getStoragePool(poolIDToResize)
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

var _ = Describe("{PoolExpandSmoke}", func() {
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

	It("Verify expanding pool with add-disk type is rejected with dmthin. ", func() {
		StartTorpedoTest("PoolExpandDiskAdd",
			"Validate storage pool expansion with type=add-disk", nil, 0)
		originalSizeInBytes = poolToResize.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB = targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type add-disk",
			poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK)
		if isDMthin, _ := IsDMthin(); !isDMthin {
			resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
			dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
		}
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

var _ = Describe("{PoolExpandRejectConcurrentDiskResize}", func() {
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

	// test resizing all pools on one storage node concurrently and ensure only the first one makes progress
	It("Select all pools on a storage node and expand them concurrently. ", func() {
		// TestRail:https://portworx.testrail.net/index.php?/tests/view/86355074
		StartTorpedoTest("PoolExpandRejectConcurrentDiskResize",
			"Validate storage pool expansion rejects concurrent requests", nil, 86355074)
		var pools []*api.StoragePool
		Step("Verify multiple pools are present on this node", func() {
			// collect all pools available
			pools = append(pools, storageNode.Pools...)
			dash.VerifyFatal(len(pools) > 1, true, "This test requires more than 1 pool.")
		})

		Step("Expand all pools concurrently. ", func() {
			expandType := api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK
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
				time.Sleep(1 * time.Second)
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
		expandType := api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK
		targetSize := poolToResize.TotalSize/units.GiB + 100
		err = Inst().V.ExpandPool(poolIDToResize, expandType, targetSize, true)
		isExpandInProgress, expandErr := poolResizeIsInProgress(poolToResize)
		if expandErr != nil {
			log.Fatalf("Error checking if pool expansion is in progress: %v", expandErr)
		}
		if !isExpandInProgress {
			log.Warnf("Pool expansion already finished. Skipping this test. Use a testing app that writes " +
				"more data which may slow down resize-disk type expansion. ")
			return
		}
		expandResponse := Inst().V.ExpandPoolUsingPxctlCmd(*storageNode, poolToResize.Uuid, expandType, targetSize+100, true)
		dash.VerifyFatal(expandResponse != nil, true, "Pool expansion should fail when expansion is in progress")
		dash.VerifyFatal(strings.Contains(expandResponse.Error(), "is already in progress"), true,
			"Pool expansion failure reason should be communicated to the user	")
	})
})

var _ = Describe("{PoolExpandRejectConcurrentDiskAdd}", func() {
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

	// test resizing all pools on one storage node concurrently and ensure only the first one makes progress
	It("Select all pools on a storage node and expand them concurrently. ", func() {
		// TestRail:https://portworx.testrail.net/index.php?/tests/view/34542836
		StartTorpedoTest("PoolExpandRejectConcurrent",
			"Validate storage pool expansion rejects concurrent requests", nil, 34542836)
		var pools []*api.StoragePool
		Step("Verify multiple pools are present on this node", func() {
			// collect all pools available
			pools = append(pools, storageNode.Pools...)
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
				time.Sleep(5 * time.Second)
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
		time.Sleep(5 * time.Second)
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

var _ = Describe("{PoolExpandDiskResizeWithReboot}", func() {
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

	It("Initiate pool expansion using resize-disk and reboot node", func() {
		StartTorpedoTest("PoolExpandDiskResizeWithReboot", "Initiate pool expansion using resize-disk and reboot node", nil, 51309)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
		Step("Select a pool that has I/O and expand it by 100 GiB with resize-disk type. ", func() {
			originalSizeInBytes = poolToResize.TotalSize
			targetSizeInBytes = originalSizeInBytes + 100*units.GiB
			targetSizeGiB = targetSizeInBytes / units.GiB
			log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type resize-disk",
				poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
			triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK)
		})

		Step("Wait for expansion to start and reboot node", func() {
			err := WaitForExpansionToStart(poolIDToResize)
			log.FailOnError(err, "Timed out waiting for expansion to start")
			err = RebootNodeAndWaitForPxUp(*storageNode)
			log.FailOnError(err, "Failed to reboot node and wait till it is up")
		})

		log.Infof("Debug Pool %s", poolIDToResize)
		Step("Ensure pool has been expanded to the expected size", func() {
			_ = waitForOngoingPoolExpansionToComplete(poolIDToResize)
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
		})
		log.Infof("Debug Pool %s", poolIDToResize)
		Step("Pool has expanded. Reboot the node", func() {
			err = RebootNodeAndWaitForPxUp(*storageNode)
			log.FailOnError(err, "Failed to reboot node and wait till it is up")
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
		})
	})
})

var _ = Describe("{PoolExpandDiskAddWithReboot}", func() {
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
			err = RebootNodeAndWaitForPxUp(*storageNode)
			log.FailOnError(err, "Failed to reboot node and wait till it is up")
		})

		Step("Ensure pool has been expanded to the expected size", func() {
			err = waitForOngoingPoolExpansionToComplete(poolIDToResize)
			dash.VerifyFatal(err, nil, "Pool expansion does not result in error")
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
		})

		Step("Pool has expanded. Reboot the node", func() {
			err = RebootNodeAndWaitForPxUp(*storageNode)
			log.FailOnError(err, "Failed to reboot node and wait till it is up")
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
		})
	})
})

var _ = Describe("{PoolExpandDiskResizePXRestart}", func() {
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

		Step("Select a pool that has I/O and expand it by 100 GiB with resize-disk type. ", func() {
			originalSizeInBytes = poolToResize.TotalSize
			targetSizeInBytes = originalSizeInBytes + 100*units.GiB
			targetSizeGiB = targetSizeInBytes / units.GiB
			log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type resize-disk",
				poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
			triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK)
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

		Step("Expansioin successful. Restart PX", func() {
			err = Inst().V.RestartDriver(*storageNode, nil)
			log.FailOnError(err, fmt.Sprintf("Error restarting px on node [%s]", storageNode.Name))
			err = Inst().V.WaitDriverUpOnNode(*storageNode, addDriveUpTimeOut)
			log.FailOnError(err, fmt.Sprintf("Timed out waiting for px to come up on node [%s]", storageNode.Name))
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
		})
	})
})

var _ = Describe("{PoolExpandDiskAddPXRestart}", func() {
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

		Step("Expansioin successful. Restart PX", func() {
			err = Inst().V.RestartDriver(*storageNode, nil)
			log.FailOnError(err, fmt.Sprintf("Error restarting px on node [%s]", storageNode.Name))
			err = Inst().V.WaitDriverUpOnNode(*storageNode, addDriveUpTimeOut)
			log.FailOnError(err, fmt.Sprintf("Timed out waiting for px to come up on node [%s]", storageNode.Name))
			verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
		})
	})
})

var _ = Describe("{PoolExpandInvalidSize}", func() {
	// TestrailId: https://portworx.testrail.net/index.php?/tests/view/34542945
	BeforeEach(func() {
		StartTorpedoTest("PoolExpansionDiskResizeInvalidSize",
			"Initiate pool expansion using invalid expansion size", nil, 34542945)
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
		storageNode, err = GetNodeWithGivenPoolID(pooltoPick)
		log.FailOnError(err, "Failed to get node with given pool ID")

		resizeErr := Inst().V.ExpandPool(pooltoPick, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 30000000, true)
		dash.VerifyFatal(resizeErr, nil, "Verify error occurs with invalid Pool expansion size")
	})

})

var _ = Describe("{PoolExpandResizeInvalidPoolID}", func() {
	// TestrailID: https://portworx.testrail.net/index.php?/tests/view/34542946
	BeforeEach(func() {
		StartTorpedoTest("PoolExpandResizeInvalidPoolID",
			"Initiate pool expansion using invalid Id", nil, 34542946)
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
			resizeErr := Inst().V.ExpandPool(invalidPoolUUID, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 100, true)
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

var _ = Describe("{PoolExpandDiskResizeAndVerifyFromOtherNode}", func() {

	BeforeEach(func() {
		StartTorpedoTest("PoolExpandDiskResizeAndVerifyFromOtherNode",
			"Initiate pool expansion and verify from other node", nil, 34542840)
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

	stepLog := "should get the existing pool and expand it by resizing a disk and verify from other node"
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

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type resize-disk",
			poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK)

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

var _ = Describe("{PoolExpandDiskAddAndVerifyFromOtherNode}", func() {
	// TestrailID: https://portworx.testrail.net/index.php?/tests/view/34542840
	BeforeEach(func() {
		StartTorpedoTest("PoolExpandDiskAddAndVerifyFromOtherNode",
			"Initiate pool expansion and verify from other node", nil, 34542840)
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

var _ = Describe("{PoolExpandResizeWithSameSize}", func() {
	// TestrailId: https://portworx.testrail.net/index.php?/tests/view/34542944

	BeforeEach(func() {
		StartTorpedoTest("PoolExpandResizeWithSameSize",
			"Initiate pool expansion using same size", nil, 34542944)
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
	// TestrailId: https://portworx.testrail.net/index.php?/tests/view/34542896

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
		dash.VerifyFatal(errMatch, nil, "Pool expand with one resize already in progress failed as expected.")

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

var _ = Describe("{MaintenanceCycleDuringPoolExpandResizeDisk}", func() {
	var testrailID = 34542902
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/34542902

	/*
	   Steps:
	       1. Initiate pool expand with resize-disk operation.
	       2. Cycle the node through maintenance mode.
	       3. Verify pool expand operation goes to completion.
	*/

	BeforeEach(func() {
		StartTorpedoTest("MaintenanceCycleDuringPoolExpandResizeDisk",
			"Perform maintenance cycle during pool expand with resize-disk operation", nil, testrailID)
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

	stepLog := "cycle through maintenance mode during pool expand with resize-disk"
	It(stepLog, func() {
		log.InfoD(stepLog)

		originalSizeInBytes = poolToResize.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB = targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type resize-disk",
			poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK)

		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "Failed to check is journal enabled")
		if isjournal {
			targetSizeGiB = targetSizeGiB - 3
		}

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

		err = waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(err, nil, "Pool expansion does not result in error")

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
		stepLog = "Move Pool to maintenance mode"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			nodeDetail, err = GetNodeWithGivenPoolID(poolToResize.Uuid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Failed to get Node Details using PoolUUID [%v]", poolToResize.Uuid))

			log.InfoD("Bring Pool to Maintenance Mode")
			err = Inst().V.EnterPoolMaintenance(*nodeDetail)
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
			log.InfoD("Bring Pool out of Maintenance Mode")
			err = Inst().V.ExitPoolMaintenance(*nodeDetail)
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
		stepLog = "Move pool to maintenance mode"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			nodeDetail, err = GetNodeWithGivenPoolID(poolToResize.Uuid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Failed to get Node Details using PoolUUID [%v]", poolToResize.Uuid))

			log.InfoD("Bring Pool to Maintenance Mode")
			err = Inst().V.EnterPoolMaintenance(*nodeDetail)
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

			if isDMthin, _ := IsDMthin(); isDMthin {
				dash.VerifyFatal(err != nil, true,
					"Pool expansion request of add-disk type should be rejected with dmthin")
				dash.VerifyFatal(strings.Contains(err.Error(), "add-drive type expansion is not supported with px-storev2"), true, fmt.Sprintf("check error message: %v", err.Error()))
			} else {
				dash.VerifyFatal(err, nil, "pool expansion requested successfully")
			}
		})

		if isDMthin, _ := IsDMthin(); !isDMthin {
			stepLog = "Exit Pool out of maintenance mode"
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
		}
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

		targetSizeTiB := uint64(15)
		targetSizeInBytes = targetSizeTiB * units.TiB
		targetSizeGiB = targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v TiB with type resize-disk",
			poolIDToResize, targetSizeGiB, targetSizeTiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK)
		resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(resizeErr, nil, "Pool expansion should not result in error")
		verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)

	})

	It("Expand pool to 20 TiB (beyond max supported capacity for DMThin) with resize-disk type. ", func() {
		// TestrailId: https://portworx.testrail.net/index.php?/cases/view/50643

		StartTorpedoTest("DMThinPoolExpandBeyond15TiBLimit",
			"Initiate pool expansion using resize-disk to 20 TiB target size", nil, testrailID)
		isDMthin, err := IsDMthin()
		dash.VerifyFatal(err, nil, "error verifying if set up is DMTHIN enabled")
		dash.VerifyFatal(isDMthin, true, "DMThin/PX-Storev2 is not enabled on underlaying PX cluster. Skipping `PoolExpandTestBeyond15TiBLimit` test.")

		targetSizeTiB := uint64(20)
		targetSizeInBytes = targetSizeTiB * units.TiB
		targetSizeGiB = targetSizeInBytes / units.GiB
		log.InfoD("Trying to expand pool %s to %v TiB with type resize-disk",
			poolIDToResize, targetSizeTiB)
		err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, targetSizeGiB, true)
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

func checkPoolShowMessageOutput(n *node.Node) bool {
	// Get the node to check the pool show output and grep for Message
	cmd := "pxctl sv pool show | grep -e Message"
	// Execute the command and check the alerts of type POOL
	out, err := Inst().N.RunCommandWithNoRetry(*n, cmd, node.ConnectionOpts{
		Timeout:         2 * time.Minute,
		TimeBeforeRetry: 10 * time.Second,
	})
	log.FailOnError(err, "Unable to execute the pxctl show command")
	outLines := strings.Split(out, "\n")
	for _, l := range outLines {
		line := strings.Trim(l, " ")
		// the following error is expected cause we are trying to expand the pool beyond the limit
		if strings.Contains(line, "cannot be expanded beyond maximum size") {
			return true
		}
	}
	return false
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

var _ = Describe("{CheckPoolLabelsAfterAddDisk}", func() {

	var testrailID = 34542906
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/34542906

	BeforeEach(func() {
		StartTorpedoTest("CheckPoolLabelsAfterAddDisk",
			"Initiate pool expansion and Newly set pool labels should persist post pool expand add-disk operation", nil, testrailID)
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

	It("Initiate pool expansion and Newly set pool labels should persist post pool expand add-disk operation", func() {

		labelBeforeExpand := poolToResize.Labels

		stepLog = "set pool label, before pool expand"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolLabelToUpdate := make(map[string]string)
			poolLabelToUpdate["cust-type1"] = "add-disk-test-label"
			err = Inst().V.UpdatePoolLabels(*storageNode, poolIDToResize, poolLabelToUpdate)
			log.FailOnError(err, "Failed to update the label on the pool %s", poolIDToResize)
		})

		stepLog = "Move pool to maintenance mode"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			expectedStatus := "In Maintenance"
			log.InfoD(fmt.Sprintf("Entering pool maintenance mode on node %s", storageNode.Name))
			err = Inst().V.EnterPoolMaintenance(*storageNode)
			log.FailOnError(err, fmt.Sprintf("failed to enter node %s in maintenance mode", storageNode.Name))
			status, _ := Inst().V.GetNodeStatus(*storageNode)
			log.InfoD(fmt.Sprintf("Node %s status %s", storageNode.Name, status.String()))
			err := WaitForPoolStatusToUpdate(*storageNode, expectedStatus)
			dash.VerifyFatal(err, nil, "Pool now in maintenance mode")
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
			resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
			dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
		})

		stepLog = "Exit pool out of maintenance mode"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			expectedStatus := "Online"
			statusMap, err := Inst().V.GetNodePoolsStatus(*storageNode)
			log.FailOnError(err, "Failed to get map of pool UUID and status")
			log.InfoD(fmt.Sprintf("pool %s has status %s", storageNode.Name, statusMap[poolToResize.Uuid]))
			if statusMap[poolToResize.Uuid] == "In Maintenance" {
				log.InfoD(fmt.Sprintf("Exiting pool maintenance mode on node %s", storageNode.Name))
				err := Inst().V.ExitPoolMaintenance(*storageNode)
				log.FailOnError(err, "failed to exit pool maintenance mode")
			} else {
				dash.VerifyFatal(statusMap[poolToResize.Uuid], "In Maintenance", "Pool is not in Maintenance mode")
			}
			status, err := Inst().V.GetNodeStatus(*storageNode)
			log.FailOnError(err, "err getting node [%s] status", storageNode.Name)
			log.Infof(fmt.Sprintf("Node %s status %s after exit", storageNode.Name, status.String()))
			exitErr := WaitForPoolStatusToUpdate(*storageNode, expectedStatus)
			dash.VerifyFatal(exitErr, nil, "Pool is now online")
		})

		stepLog = "check pool label, after pool expand"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			labelAfterExpand := poolToResize.Labels
			result := reflect.DeepEqual(labelBeforeExpand, labelAfterExpand)
			dash.VerifyFatal(result, true, "Check if labels changed after pool expand")
		})
	})

})

var _ = Describe("{PoolExpandAndCheckAlertsUsingAddDisk}", func() {

	var testrailID = 34542894
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/34542894

	BeforeEach(func() {
		StartTorpedoTest("PoolExpandAndCheckAlertsUsingAddDisk", "pool expansion using add-disk and check alerts after each operation", nil, testrailID)
		contexts = scheduleApps()
		ValidateApplications(contexts)
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

	It("pool expansion using add-disk and check alerts after each operation", func() {
		var err error

		stepLog = "Move pool to maintenance mode"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			expectedStatus := "In Maintenance"
			log.InfoD(fmt.Sprintf("Entering pool maintenance mode on node %s", storageNode.Name))
			err = Inst().V.EnterPoolMaintenance(*storageNode)
			log.FailOnError(err, fmt.Sprintf("failed to enter node %s in maintenance mode", storageNode.Name))
			status, _ := Inst().V.GetNodeStatus(*storageNode)
			log.InfoD(fmt.Sprintf("Node %s status %s", storageNode.Name, status.String()))
			err := WaitForPoolStatusToUpdate(*storageNode, expectedStatus)
			dash.VerifyFatal(err, nil, "Pool now in maintenance mode")
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
			resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
			dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")

		})

		stepLog = "Exit pool out of maintenance mode"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			expectedStatus := "Online"
			statusMap, err := Inst().V.GetNodePoolsStatus(*storageNode)
			log.FailOnError(err, "Failed to get map of pool UUID and status")
			log.InfoD(fmt.Sprintf("pool %s has status %s", storageNode.Name, statusMap[poolToResize.Uuid]))
			if statusMap[poolToResize.Uuid] == "In Maintenance" {
				log.InfoD(fmt.Sprintf("Exiting pool maintenance mode on node %s", storageNode.Name))
				err := Inst().V.ExitPoolMaintenance(*storageNode)
				log.FailOnError(err, "failed to exit pool maintenance mode")
			} else {
				dash.VerifyFatal(statusMap[poolToResize.Uuid], "In Maintenance", "Pool is not in Maintenance mode")
			}
			status, err := Inst().V.GetNodeStatus(*storageNode)
			log.FailOnError(err, "err getting node [%s] status", storageNode.Name)
			log.Infof(fmt.Sprintf("Node %s status %s after exit", storageNode.Name, status.String()))
			exitErr := WaitForPoolStatusToUpdate(*storageNode, expectedStatus)
			dash.VerifyFatal(exitErr, nil, "Pool is now online")
		})

		stepLog = "Check the alerts for pool expand"
		Step(stepLog, func() {
			log.Infof("Check the alert for pool expand for pool uuid %s", poolIDToResize)
			alertExists, _ := checkAlertsForPoolExpansion(poolIDToResize, targetSizeGiB)
			dash.VerifyFatal(alertExists, true, "Verify Alert is Present")
		})
	})

})

var _ = Describe("{PoolVolUpdateResizeDisk}", func() {

	// 1.The volumes originally have a HA of 2. We are adding a check at first to see if the HA is 3.
	// 2.If it is, we decrease it to 2 and then increase it back to 3 again.
	// 3.If not, then we are good to increase it by 1.
	// 4.While this increase is happening, we do the pool resize operation.

	var testrailID = 34542876
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/34542876

	BeforeEach(func() {
		StartTorpedoTest("PoolVolUpdateResizeDisk", "Increase the HA replica of the volume and expand pool using resize-disk during the increase", nil, testrailID)
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
	It("Increase the HA replica of the volume and expand pool using resize-disk during the increase", func() {
		var newRep int64
		var currRep int64
		volSelected, err := GetVolumeWithMinimumSize(contexts, 10)
		log.FailOnError(err, "error identifying volume")
		opts := volume.Options{
			ValidateReplicationUpdateTimeout: replicationUpdateTimeout,
		}
		Step("Increase the HA replica of the volume", func() {
			currRep, err = Inst().V.GetReplicationFactor(volSelected)
			log.FailOnError(err, fmt.Sprintf("err getting repl factor for  vol : %s", volSelected.Name))
			newRep = currRep
			// If the HA is 3, reduce it by 1, so that we can increase it later
			if currRep == 3 {
				newRep = currRep - 1
				err = Inst().V.SetReplicationFactor(volSelected, newRep, nil, nil, true, opts)
				log.FailOnError(err, fmt.Sprintf("err setting repl factor  to %d for  vol : %s", newRep, volSelected.Name))

				decreasedRep, err := Inst().V.GetReplicationFactor(volSelected)
				log.FailOnError(err, fmt.Sprintf("err getting repl factor for  vol : %s", volSelected.Name))
				dash.VerifyFatal(decreasedRep == newRep, true, fmt.Sprintf("repl factor successfully decreased to %d for the vol : %s ", decreasedRep, volSelected.Name))
			}
			// Increase the HA by 1
			log.InfoD(fmt.Sprintf("setting repl factor to %d for vol : %s", newRep+1, volSelected.Name))
			// err = Inst().V.SetReplicationFactor(volSelected, newRep+1, []string{storageNode.Id}, []string{poolToResize.Uuid}, false, opts)
			err = Inst().V.SetReplicationFactor(volSelected, newRep+1, nil, nil, true, opts)
			log.FailOnError(err, fmt.Sprintf("err setting repl factor  to %d for  vol : %s", newRep+1, volSelected.Name))
			dash.VerifyFatal(err == nil, true, fmt.Sprintf("vol %s expansion triggered successfully on node %s", volSelected.Name, storageNode.Name))
		})

		Step("Initiate pool expansion using resize-disk while repl increase is in progress", func() {
			originalSizeInBytes = poolToResize.TotalSize
			targetSizeInBytes = originalSizeInBytes + 100*units.GiB
			targetSizeGiB = targetSizeInBytes / units.GiB
			log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type resize-disk", poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
			triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK)

			log.InfoD("Wait for expansion to finish")
			resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
			dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
			err = ValidateReplFactorUpdate(volSelected, newRep+1)
			log.FailOnError(err, "error validating repl factor for vol [%s]", volSelected.Name)

			//reverting the replication for volume validation
			if currRep < 3 {
				err = Inst().V.SetReplicationFactor(volSelected, currRep, nil, nil, true, opts)
				log.FailOnError(err, fmt.Sprintf("err setting repl factor to %d for vol : %s", newRep, volSelected.Name))
			}

		})
	})
})

var _ = Describe("{PoolExpandStorageFullPoolResize}", func() {

	//step1: feed p1 size GB I/O on the volume
	//step2: After I/O done p1 should be offline and full, expand the pool p1 using resize-disk
	//step4: validate the pool and the data

	var testrailID = 34542835
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/34542835

	JustBeforeEach(func() {
		StartTorpedoTest("PoolExpandStorageFullPoolResize", "Feed a pool full, then expand the pool using resize-disk", nil, testrailID)
	})

	var contexts []*scheduler.Context

	stepLog := "Create vols and make pool full"
	It(stepLog, func() {
		log.InfoD(stepLog)
		selectedNode := GetNodeWithLeastSize()

		stNodes := node.GetStorageNodes()
		var secondReplNode node.Node
		for _, stNode := range stNodes {
			if stNode.Name != selectedNode.Name {
				secondReplNode = stNode
			}
		}

		applist := Inst().AppList
		var err error
		defer func() {
			Inst().AppList = applist
			err = Inst().S.RemoveLabelOnNode(*selectedNode, k8s.NodeType)
			log.FailOnError(err, "error removing label on node [%s]", selectedNode.Name)
			err = Inst().S.RemoveLabelOnNode(secondReplNode, k8s.NodeType)
			log.FailOnError(err, "error removing label on node [%s]", secondReplNode.Name)
		}()
		err = Inst().S.AddLabelOnNode(*selectedNode, k8s.NodeType, k8s.FastpathNodeType)
		log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", selectedNode.Name))
		err = Inst().S.AddLabelOnNode(secondReplNode, k8s.NodeType, k8s.FastpathNodeType)
		log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", secondReplNode.Name))

		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "is journal enabled check failed")

		err = adjustReplPools(*selectedNode, secondReplNode, isjournal)
		log.FailOnError(err, "Error setting pools for clean volumes")

		Inst().AppList = []string{"fio-fastpath"}
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("sfullrz-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		err = WaitForPoolOffline(*selectedNode)
		log.FailOnError(err, fmt.Sprintf("Failed to make node %s storage down", selectedNode.Name))

		poolsStatus, err := Inst().V.GetNodePoolsStatus(*selectedNode)
		log.FailOnError(err, "error getting pool status on node %s", selectedNode.Name)

		var offlinePoolUUID string
		for i, s := range poolsStatus {
			if s == "Offline" {
				offlinePoolUUID = i
				break
			}
		}
		selectedPool, err := GetStoragePoolByUUID(offlinePoolUUID)
		log.FailOnError(err, "error getting pool with UUID [%s]", offlinePoolUUID)

		var expandedExpectedPoolSize uint64
		Step(stepLog, func() {
			log.InfoD(stepLog)
			expandedExpectedPoolSize = (selectedPool.TotalSize / units.GiB) * 2
			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, selectedPool.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expandedExpectedPoolSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
		})
		stepLog = fmt.Sprintf("Ensure that pool %s expansion is successful", selectedPool.Uuid)
		Step(stepLog, func() {
			log.InfoD(stepLog)

			err = waitForPoolToBeResized(expandedExpectedPoolSize, selectedPool.Uuid, isjournal)
			log.FailOnError(err, fmt.Sprintf("Error waiting for poor %s resize", selectedPool.Uuid))
			resizedPool, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("error get pool using UUID %s", selectedPool.Uuid))
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			expectedSizeWithJournal := expandedExpectedPoolSize - 3

			if newPoolSize >= expectedSizeWithJournal {
				isExpansionSuccess = true
			}
			dash.VerifyFatal(isExpansionSuccess, true, fmt.Sprintf("expected new pool size to be %v or %v, got %v", expandedExpectedPoolSize, expectedSizeWithJournal, newPoolSize))
			status, err := Inst().V.GetNodeStatus(*selectedNode)
			log.FailOnError(err, fmt.Sprintf("Error getting PX status of node %s", selectedNode.Name))
			dash.VerifySafely(*status, api.Status_STATUS_OK, fmt.Sprintf("validate PX status on node %s", selectedNode.Name))
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		// Cores are expected with StoragePool full.
		// AfterEachTest will fail due to cores found during the test.
		// AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{DriveAddDifferentTypesAndResize}", func() {

	var testrailID = 34542903
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/34542903

	BeforeEach(func() {
		StartTorpedoTest("DriveAddDifferentTypesAndResize",
			"Create pools with different types of drive and pool expand using resize-disk", nil, testrailID)
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

	It("creating pools with different drive types and resizing them", func() {
		var driveTypes []string

		driveSize := "100"
		driveTypes, err = Inst().N.GetSupportedDriveTypes()
		log.FailOnError(err, "Error getting drive types for the provider")
		for i := 0; i < len(driveTypes); i++ {

			poolsBfr, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
			log.FailOnError(err, "Failed to list storage pools")

			newDriveSpec := fmt.Sprintf("size=%s,type=%s", driveSize, driveTypes[i])
			err = Inst().V.AddCloudDrive(storageNode, newDriveSpec, -1)
			log.FailOnError(err, fmt.Sprintf("Add cloud drive failed on node %s", storageNode.Name))

			log.InfoD("Validate pool rebalance after drive add to the node %s", storageNode.Name)
			err = ValidateDriveRebalance(*storageNode)
			log.FailOnError(err, "pool re-balance failed on node %s", storageNode.Name)

			err = Inst().V.WaitDriverUpOnNode(*storageNode, addDriveUpTimeOut)
			log.FailOnError(err, "volume driver down on node %s", storageNode.Name)

			poolsAfr, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
			log.FailOnError(err, "Failed to list storage pools")
			dash.VerifyFatal(len(poolsBfr)+1, len(poolsAfr), "verify new pool is created")

		}

		allPools, err := GetAllPoolsOnNode(storageNode.Id)
		log.FailOnError(err, "Unable to get pool drives on node %s", storageNode.Name)

		for i := 0; i < len(allPools); i++ {
			poolToResize = getStoragePool(allPools[i])
			originalSizeInBytes = poolToResize.TotalSize
			targetSizeInBytes = originalSizeInBytes + 100*units.GiB
			targetSizeGiB = targetSizeInBytes / units.GiB
			log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type resize-disk", allPools[i], poolToResize.TotalSize/units.GiB, targetSizeGiB)
			triggerPoolExpansion(allPools[i], 1000, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK)
			resizeErr := waitForOngoingPoolExpansionToComplete(allPools[i])
			dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
		}

	})
})
