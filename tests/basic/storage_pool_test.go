package tests

import (
	"errors"
	"fmt"
	"github.com/Masterminds/semver/v3"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/portworx/torpedo/drivers/node/vsphere"

	"github.com/google/uuid"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/volume"

	"github.com/portworx/torpedo/pkg/log"

	"github.com/libopenstorage/openstorage/api"
	. "github.com/onsi/ginkgo/v2"

	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	"github.com/portworx/torpedo/pkg/units"
	. "github.com/portworx/torpedo/tests"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("{StoragePoolExpandDiskResize}", func() {
	BeforeEach(func() {
		StartTorpedoTest(testName, testDescription, nil, 0)
		contexts = scheduleApps()
	})

	JustBeforeEach(func() {

		isJournalEnabled, _ = IsJournalEnabled()
		bufferSizeInGB = uint64(0)
		if isJournalEnabled {
			bufferSizeInGB = JournalDeviceSizeInGB
		}
	})

	testName = "StoragePoolExpandDiskResize"
	testDescription = "Validate storage pool expansion using resize-disk option"
	It("select a pool that has I/O and expand it by 100 GiB with resize-disk type. ", func() {
		poolIDToResize = pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 100)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))
		poolToResize = getStoragePool(poolIDToResize)
		originalSizeInBytes = poolToResize.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB // getDesiredSize(originalSizeInBytes)
		targetSizeGiB := targetSizeInBytes / units.GiB

		log.InfoD("Current size of pool %s is %d GiB. Trying to expand to %v GiB",
			poolIDToResize, poolToResize.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB+bufferSizeInGB, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK)
		resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
		verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})

	AfterEach(func() {
		appsValidateAndDestroy(contexts)
		EndTorpedoTest()
	})
})

var _ = Describe("{StoragePoolExpandDiskAdd}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("StoragePoolExpandDiskAdd", "Validate storage pool expansion using add-disk option", nil, 0)
	})

	stepLog := "should get the existing pool and expand it by adding a disk"
	It(stepLog, func() {
		log.InfoD(stepLog)
		if !IsPoolAddDiskSupported() {
			Skip("Pool Add Disk is not supported on DMthin Cluster")
		}
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pooladddisk-%d", i))...)
		}

		ValidateApplications(contexts)

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, "Storage pools exist ?")

		// pick a pool from a pools list and resize it
		poolIDToResize = pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 100)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {

			log.InfoD(stepLog)
			if val, err := poolResizeIsInProgress(poolToBeResized); val {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			} else {
				log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		stepLog = "Calculate expected pool size and trigger pool resize"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			expectedSize = (poolToBeResized.TotalSize / units.GiB) + 100
			expectedSize = roundUpValue(expectedSize)
			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check is Journal enabled")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}

			log.InfoD("Current Size of the pool %s is %d", poolIDToResize, poolToBeResized.TotalSize/units.GiB)
			enterPoolMaintenanceAddDisk(poolIDToResize)
			defer exitPoolMaintenance(poolIDToResize)

			if IsPoolAddDiskSupported() {
				err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, false)
				log.FailOnError(err, "is Pool Expand using Add disk successful ?")
				resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
				dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Expected new size to be '%d' or '%d' if pool has journal", expectedSize, expectedSizeWithJournal))
			}
		})
		Step("Ensure that new pool has been expanded to the expected size", func() {
			if IsPoolAddDiskSupported() {
				ValidateApplications(contexts)
				resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
				newPoolSize := resizedPool.TotalSize / units.GiB
				isExpansionSuccess := false
				if newPoolSize >= expectedSizeWithJournal {
					isExpansionSuccess = true
				}
				dash.VerifyFatal(isExpansionSuccess, true,
					fmt.Sprintf("expected new pool size to be %v or %v if pool has journal, got %v", expectedSize, expectedSizeWithJournal, newPoolSize))
				appsValidateAndDestroy(contexts)
			}

		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{StoragePoolExpandDiskAuto}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("StoragePoolExpandDiskAuto", "Validate storage pool expansion using auto option", nil, 0)
	})

	stepLog := "has to schedule apps, and expand it by resizing a disk"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("poolexpandauto-%d", i))...)
		}

		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, " Storage pools exist?")

		// pick a pool from a pools list and resize it
		poolIDToResize = pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_AUTO, 0)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		time.Sleep(time.Second * 60)
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if val, err := poolResizeIsInProgress(poolToBeResized); val {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			} else {

				log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64
		stepLog = "Calculate expected pool size and trigger pool resize"
		Step(stepLog, func() {
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}
			log.InfoD("Current Size of the pool %s is %d", poolIDToResize, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize, false)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Expected new size to be '%d' or '%d'", expectedSize, expectedSizeWithJournal))
		})

		stepLog = "Ensure that new pool has been expanded to the expected size"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			ValidateApplications(contexts)

			resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			if newPoolSize >= expectedSizeWithJournal {
				isExpansionSuccess = true
			}
			dash.VerifyFatal(isExpansionSuccess, true, fmt.Sprintf("Expected new pool size to be %v or %v, got %v", expectedSize, expectedSizeWithJournal, newPoolSize))

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{PoolResizeDiskReboot}", func() {

	/*
		1. Initiate pool expansion using resize-disk
		2. Reboot the node where pool is present
		3.Validate pool expansion
	*/

	var testrailID = 51309
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51309
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("PoolResizeDiskReboot", "Initiate pool expansion using resize-disk and reboot node", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "has to schedule apps, and expand it by resizing a disk"
	It(stepLog, func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("poolresizediskreboot-%d", i))...)
		}

		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		dash.VerifyFatal(err, nil, "Validate list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, "Validate storage pools exist")

		// pick a pool from a pools list and resize it
		poolIDToResize = pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 0)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		time.Sleep(time.Second * 60)
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if val, err := poolResizeIsInProgress(poolToBeResized); val {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			} else {
				log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		stepLog = "Calculate expected pool size and trigger pool resize"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize = (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check is journal enabled")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}
			log.InfoD("Current Size of the pool %s is %d", poolIDToResize, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful ?")

			err = WaitForExpansionToStart(poolIDToResize)
			log.FailOnError(err, "Expansion is not started")

			storageNode, err := GetNodeWithGivenPoolID(poolIDToResize)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			err = RebootNodeAndWaitForPxUp(*storageNode)
			log.FailOnError(err, "Failed to reboot node and wait till it is up")
			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Expected new size to be '%d' or '%d'", expectedSize, expectedSizeWithJournal))
		})

		stepLog = "Ensure that new pool has been expanded to the expected size"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			ValidateApplications(contexts)

			resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			if newPoolSize >= expectedSizeWithJournal {
				isExpansionSuccess = true
			}
			dash.VerifyFatal(isExpansionSuccess, true,
				fmt.Sprintf("Expected new pool size to be %v or %v, got %v", expectedSize, expectedSizeWithJournal, newPoolSize))
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{PoolAddDiskReboot}", func() {
	/*
		1. Initiate pool expansion using add-disk
		2. Trigger node reboot while expansion is in-progress
		3. Validate pool expansion once node and PX are up
	*/
	var testrailID = 51440
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51440
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("PoolAddDiskReboot", "Initiate pool expansion using add-disk and reboot node", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "should get the existing pool and expand it by adding a disk"

	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pooladddiskreboot-%d", i))...)
		}

		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, "Storage pools exist?")

		// pick a pool from a pools list and resize it
		poolIDToResize = pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if val, err := poolResizeIsInProgress(poolToBeResized); val {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			} else {
				log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		stepLog = "Calculate expected pool size and trigger pool resize"
		Step(stepLog, func() {
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize = (poolToBeResized.TotalSize / units.GiB) + drvSize
			expectedSize = roundUpValue(expectedSize)
			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check is journal enabled")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}
			log.InfoD("Current Size of the pool %s is %d", poolIDToResize, poolToBeResized.TotalSize/units.GiB)
			enterPoolMaintenanceAddDisk(poolIDToResize)
			defer exitPoolMaintenance(poolIDToResize)
			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			err = WaitForExpansionToStart(poolIDToResize)
			log.FailOnError(err, "Failed while waiting for expansion to start")

			storageNode, err := GetNodeWithGivenPoolID(poolIDToResize)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			err = RebootNodeAndWaitForPxUp(*storageNode)
			if err != nil {
				if !strings.Contains(err.Error(), "Actual: STATUS_POOLMAINTENANCE") {
					log.FailOnError(err, "Failed to reboot node and wait till it is up")
				}
			}
			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Expected new size to be '%d' or '%d' if pool has journal", expectedSize, expectedSizeWithJournal))
		})

		stepLog = "Ensure that new pool has been expanded to the expected size"
		Step(stepLog, func() {
			ValidateApplications(contexts)

			resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			if newPoolSize >= expectedSizeWithJournal {
				isExpansionSuccess = true
			}
			dash.VerifyFatal(isExpansionSuccess, true,
				fmt.Sprintf("Expected new pool size to be %v or %v if pool has journal, got %v", expectedSize, expectedSizeWithJournal, newPoolSize))
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{NodePoolsResizeDisk}", func() {

	/*
		1. Initiate pool expansion on multiple pools in the same node using resize-disk
		2. Validate pool expansion in all the pools
	*/
	nodePoolsExpansion("NodePoolsResizeDisk")

})

var _ = Describe("{NodePoolsAddDisk}", func() {

	/*
		1. Initiate pool expansion on multiple pools in the same node using add-disk
		2. Validate pool expansion in all the pools
	*/
	nodePoolsExpansion("NodePoolsAddDisk")

})

func nodePoolsExpansion(testName string) {

	var operation api.SdkStoragePool_ResizeOperationType
	var poolsExpectedSizeMap map[string]uint64
	var option string
	if testName == "NodePoolsResizeDisk" {
		operation = api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK
		option = "resize-disk"
	} else {
		operation = api.SdkStoragePool_RESIZE_TYPE_ADD_DISK
		option = "add-disk"
	}

	JustBeforeEach(func() {
		StartTorpedoTest(testName, fmt.Sprintf("Validate multi storage pools on the same node expansion  using %s option", option), nil, 0)
		poolsExpectedSizeMap = make(map[string]uint64)
	})

	var (
		err                error
		pools              map[string]*api.StoragePool
		poolsToBeResized   []*api.StoragePool
		nodePoolToExpanded node.Node
		nodePools          []*api.StoragePool
		eligibility        map[string]bool
	)

	stepLog := fmt.Sprintf("has to schedule apps, and expand it by %s", option)
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("nodepools-%s-%d", option, i))...)
		}

		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		pools, err = Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")

		stNodes := node.GetStorageNodes()

		//getting the eligible pools of the node to initiate expansion
		for _, stNode := range stNodes {
			nodePools = stNode.Pools
			nodePoolToExpanded = stNode
			eligibility, err = GetPoolExpansionEligibility(&stNode, operation, 0)
			log.FailOnError(err, "error checking node [%s] expansion criteria", stNode.Name)
			if len(nodePools) > 1 && eligibility[stNode.Id] {
				for _, p := range nodePools {
					if eligibility[p.Uuid] {
						poolsToBeResized = append(poolsToBeResized, pools[p.Uuid])
					}
				}
				if len(poolsToBeResized) > 1 {
					break
				}
			}
		}
		dash.VerifyFatal(len(poolsToBeResized) > 1, true, fmt.Sprintf("verify Node [%s] has multiple storage pools to initiate expansion", nodePoolToExpanded.Name))

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wait until the ongoing operation is completed
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, poolToBeResized := range poolsToBeResized {
				poolIDToResize := poolToBeResized.Uuid
				if val, err := poolResizeIsInProgress(poolToBeResized); val {
					// wait until resize is completed and get the updated pool again
					poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
					log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
				} else {
					log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
				}
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "Failed to check is Journal Enabled")
		stepLog = fmt.Sprintf("Calculate expected pool size and trigger pool resize for %s", nodePoolToExpanded.Name)
		Step(stepLog, func() {

			if operation == api.SdkStoragePool_RESIZE_TYPE_ADD_DISK {
				enterPoolMaintenanceAddDisk(poolsToBeResized[0].Uuid)
			}

			for _, poolToBeResized := range poolsToBeResized {
				drvSize, err := getPoolDiskSize(poolToBeResized)
				log.FailOnError(err, fmt.Sprintf("error getting drive size for pool [%s]", poolToBeResized.Uuid))
				expectedSize = (poolToBeResized.TotalSize / units.GiB) + drvSize
				poolsExpectedSizeMap[poolToBeResized.Uuid] = expectedSize

				//To-Do Need to handle the case for multiple pools
				expectedSizeWithJournal = expectedSize
				if isjournal {
					expectedSizeWithJournal = expectedSizeWithJournal - 3
				}
				log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)

				err = Inst().V.ExpandPool(poolToBeResized.Uuid, operation, expectedSize, true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Pool %s expansion init succesful?", poolToBeResized.Uuid))
				err = WaitForExpansionToStart(poolToBeResized.Uuid)
				//this condition is skip error where drive is size is small and resize completes very fast
				if err != nil {
					expandedPool, err := GetStoragePoolByUUID(poolToBeResized.Uuid)
					log.FailOnError(err, fmt.Sprintf("error getting pool using uuid [%s]", poolToBeResized.Uuid))
					if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_SUCCESSFUL {
						// storage pool resize expansion completed
						err = nil
					}
				}
				log.FailOnError(err, "pool expansion not started")
			}

			exitPoolMaintenance(poolsToBeResized[0].Uuid)

			for poolUUID, expectedSize := range poolsExpectedSizeMap {
				resizeErr := waitForPoolToBeResized(expectedSize, poolUUID, isjournal)
				expectedSizeWithJournal = expectedSize
				if isjournal {
					expectedSizeWithJournal = expectedSizeWithJournal - 3
				}
				log.FailOnError(resizeErr, fmt.Sprintf("Expected new size to be '%d' or '%d'", expectedSize, expectedSizeWithJournal))
			}

		})

		stepLog = "Ensure that pools have been expanded to the expected size"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			ValidateApplications(contexts)
			for poolUUID, expectedSize := range poolsExpectedSizeMap {
				resizedPool, err := GetStoragePoolByUUID(poolUUID)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID  %s", poolUUID))
				newPoolSize := resizedPool.TotalSize / units.GiB
				isExpansionSuccess := false
				expectedSizeWithJournal = expectedSize
				if isjournal {
					expectedSizeWithJournal = expectedSizeWithJournal - 3
				}
				if newPoolSize >= expectedSizeWithJournal {
					isExpansionSuccess = true
				}
				dash.VerifyFatal(isExpansionSuccess, true, fmt.Sprintf("Expected new pool size to be %v or %v, got %v", expectedSize, expectedSizeWithJournal, newPoolSize))
			}

		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		for poolUUID := range poolsExpectedSizeMap {
			exitPoolMaintenance(poolUUID)
		}

		AfterEachTest(contexts)
	})
}

var _ = Describe("{AddNewPoolWhileRebalance}", func() {
	//AddNewPoolWhileRebalance:
	//
	//step1: create volume repl=2, and get its pool P1 on n1 and p2 on n2
	//
	//step2: feed 10GB I/O on the volume
	//
	//step3: After I/O expand the pool p1 when p1 is rebalancing add a new drive with different size
	//so that a new pool would be created
	//
	//step4: validate the pool and the data
	var testrailID = 51441
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51441
	var (
		runID                int
		currentTotalPoolSize uint64
		err                  error
		nodeSelected         node.Node
		pools                map[string]*api.StoragePool
		volSelected          *volume.Volume
	)

	JustBeforeEach(func() {
		StartTorpedoTest("AddNewPoolWhileRebalance", "Validate adding new storage pool while another pool rebalancing", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "has to schedule apps, and expand it by resizing a disk"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("addnewpoolrebal-%d", i))...)
		}

		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()

		volSelected, err = GetVolumeWithMinimumSize(contexts, 10)
		log.FailOnError(err, "error identifying volume")
		log.Infof("%+v", volSelected)
		rs, err := Inst().V.GetReplicaSets(volSelected)
		log.FailOnError(err, fmt.Sprintf("error getting replica sets for vol %s", volSelected.Name))
		attachedNodeID := rs[0].Nodes[0]
		volumePools := rs[0].PoolUuids
		for _, stNode := range stNodes {
			if stNode.Id == attachedNodeID {
				eligibility, err := GetPoolExpansionEligibility(&stNode, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
				log.FailOnError(err, "error checking node [%s] expansion criteria", stNode.Name)

				if eligibility[stNode.Id] {
					nodeSelected = stNode
					break
				}
			}
		}

		if &nodeSelected == nil {
			dash.VerifyFatal(false, true, "unable to identify the node for add new pool")
		}
	poolloop:
		for _, volPool := range volumePools {
			for _, nodePool := range nodeSelected.Pools {
				eligibility, err := GetPoolExpansionEligibility(&nodeSelected, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
				log.FailOnError(err, "error checking node [%s] expansion criteria", nodeSelected.Name)
				if nodePool.Uuid == volPool && eligibility[nodePool.Uuid] {
					poolIDToResize = nodePool.Uuid
					break poolloop
				}
			}
		}
		log.Infof("selected node %s, pool %s", nodeSelected.Name, poolIDToResize)
		poolToResize, err = GetStoragePoolByUUID(poolIDToResize)
		log.FailOnError(err, fmt.Sprintf("unable to get pool using UUID and vol %+v", volSelected))
		currentTotalPoolSize = poolToResize.TotalSize / units.GiB
		pools, err = Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "error getting storage pools")
		existingPoolsCount := len(pools)
		///creating a spec to perform add  drive
		driveSpecs, err := GetCloudDriveDeviceSpecs()
		log.FailOnError(err, "Error getting cloud drive specs")

		minSpecSize := uint64(math.MaxUint64)
		var specSize uint64
		for _, s := range driveSpecs {
			specParams := strings.Split(s, ",")
			for _, param := range specParams {
				if strings.Contains(param, "size") {
					val := strings.Split(param, "=")[1]
					specSize, err = strconv.ParseUint(val, 10, 64)
					log.FailOnError(err, "Error converting size to uint64")
					if specSize < minSpecSize {
						minSpecSize = specSize
					}
				}
			}
		}

		deviceSpec := driveSpecs[0]
		deviceSpecParams := strings.Split(deviceSpec, ",")
		paramsArr := make([]string, 0)
		for _, param := range deviceSpecParams {
			if strings.Contains(param, "size") {
				paramsArr = append(paramsArr, fmt.Sprintf("size=%d,", minSpecSize/2))
			} else {
				paramsArr = append(paramsArr, param)
			}
		}
		newSpec := strings.Join(paramsArr, ",")
		expandedExpectedPoolSize := currentTotalPoolSize + specSize

		stepLog = fmt.Sprintf("Verify that pool %s can be expanded", poolIDToResize)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			isPoolHealthy, err := poolResizeIsInProgress(poolToResize)
			log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
			dash.VerifyFatal(isPoolHealthy, true, "Verify pool before expansion")
		})

		stepLog = fmt.Sprintf("Trigger pool %s resize by add-disk", poolIDToResize)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			enterPoolMaintenanceAddDisk(poolIDToResize)
			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expandedExpectedPoolSize, true)
			log.FailOnError(err, "failed to initiate pool expansion")
		})

		stepLog = fmt.Sprintf("Ensure that pool %s rebalance started and add new pool to the node %s", poolIDToResize, nodeSelected.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			t := func() (interface{}, bool, error) {
				expandedPool, err := GetStoragePoolByUUID(poolIDToResize)
				if err != nil {
					return nil, true, fmt.Errorf("error getting pool by using id %s", poolIDToResize)
				}

				if expandedPool == nil {
					return nil, false, fmt.Errorf("expanded pool value is nil")
				}
				if expandedPool.LastOperation != nil {
					log.Infof("Pool Resize Status : %v, Message : %s", expandedPool.LastOperation.Status, expandedPool.LastOperation.Msg)
					if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_IN_PROGRESS &&
						(strings.Contains(expandedPool.LastOperation.Msg, "Storage rebalance is running") || strings.Contains(expandedPool.LastOperation.Msg, "Rebalance in progress")) {
						return nil, false, nil
					}
					if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_FAILED {
						return nil, false, fmt.Errorf("PoolResize has failed. Error: %s", expandedPool.LastOperation)
					}

				}
				return nil, true, fmt.Errorf("pool status not updated")
			}
			_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second)
			log.FailOnError(err, "Error checking pool rebalance")

			nodeName := nodeSelected.Name
			nodeSelected, err = node.GetNodeByName(nodeSelected.Name)
			log.FailOnError(err, "error getting node using name [%s]", nodeName)
			err = Inst().V.AddCloudDrive(&nodeSelected, newSpec, -1)
			log.FailOnError(err, fmt.Sprintf("Add cloud drive failed on node %s", nodeSelected.Name))
			err = Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "error refreshing driver end points")
			//validating add-disk rebalance
			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "is journal enabled check failed")
			err = waitForPoolToBeResized(expandedExpectedPoolSize, poolIDToResize, isjournal)
			log.FailOnError(err, "Error waiting for pool resize")
			exitPoolMaintenance(poolIDToResize)

			//validating new pool rebalance
			log.InfoD("Validate pool rebalance after drive add")
			err = ValidateDriveRebalance(nodeSelected)
			if err != nil && strings.Contains(err.Error(), "Device already exists") {
				log.Infof("new pool with spec [%s] created.", newSpec)
				err = nil
			}
			log.FailOnError(err, fmt.Sprintf("pool %s rebalance failed", poolIDToResize))

			resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
			log.FailOnError(err, fmt.Sprintf("error get pool using UUID %s", poolIDToResize))
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			expectedSizeWithJournal := expandedExpectedPoolSize - 3

			if newPoolSize >= expectedSizeWithJournal {
				isExpansionSuccess = true
			}
			dash.VerifyFatal(isExpansionSuccess, true, fmt.Sprintf("expected new pool size to be %v or %v, got %v", expandedExpectedPoolSize, expectedSizeWithJournal, newPoolSize))
			pools, err = Inst().V.ListStoragePools(metav1.LabelSelector{})
			log.FailOnError(err, "error getting storage pools")

			dash.VerifyFatal(len(pools), existingPoolsCount+1, "Validate new pool is created")
			ValidateApplications(contexts)
			for _, stNode := range stNodes {
				status, err := Inst().V.GetNodeStatus(stNode)
				log.FailOnError(err, fmt.Sprintf("Error getting PX status of node %s", stNode.Name))
				dash.VerifySafely(*status, api.Status_STATUS_OK, fmt.Sprintf("validate PX status on node %s", stNode.Name))
			}
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

func roundUpValue(toRound uint64) uint64 {

	if toRound%10 == 0 {
		return toRound
	}
	rs := (10 - toRound%10) + toRound
	return rs

}

func poolResizeIsInProgress(poolToBeResized *api.StoragePool) (bool, error) {
	if poolToBeResized.LastOperation != nil {
		f := func() (interface{}, bool, error) {
			pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
			if err != nil || len(pools) == 0 {
				return nil, true, fmt.Errorf("error getting pools list, err %v", err)
			}

			updatedPoolToBeResized := pools[poolToBeResized.Uuid]
			if updatedPoolToBeResized == nil {
				return nil, false, fmt.Errorf("error getting pool with given pool id %s", poolToBeResized.Uuid)
			}

			if updatedPoolToBeResized.LastOperation.Status != api.SdkStoragePool_OPERATION_SUCCESSFUL {
				log.Infof("Current pool status : %v", updatedPoolToBeResized.LastOperation)
				if updatedPoolToBeResized.LastOperation.Status == api.SdkStoragePool_OPERATION_FAILED {
					return nil, false, fmt.Errorf("PoolResize has failed. Error: %s", updatedPoolToBeResized.LastOperation)
				}
				log.Infof("Pool Resize is already in progress: %v", updatedPoolToBeResized.LastOperation)
				return nil, true, nil
			}
			return nil, false, nil
		}

		_, err := task.DoRetryWithTimeout(f, poolResizeTimeout, retryTimeout)
		if err != nil {
			return false, err
		}
	}

	stNode, err := GetNodeWithGivenPoolID(poolToBeResized.Uuid)
	if err != nil {
		return false, err
	}

	t := func() (interface{}, bool, error) {
		status, err := Inst().V.GetNodePoolsStatus(*stNode)
		if err != nil {
			return "", false, err
		}
		currStatus := status[poolToBeResized.Uuid]

		if currStatus == "Offline" {
			return "", true, fmt.Errorf("pool [%s] has current status [%s].Waiting rebalance to complete if in-progress", poolToBeResized.Uuid, currStatus)
		}
		return "", false, nil
	}

	_, err = task.DoRetryWithTimeout(t, 120*time.Minute, 2*time.Second)
	if err != nil {
		return false, err
	}

	return true, nil
}

func waitForPoolToBeResized(expectedSize uint64, poolIDToResize string, isJournalEnabled bool) error {

	cnt := 0
	currentLastMsg := ""
	f := func() (interface{}, bool, error) {
		expandedPool, err := GetStoragePoolByUUID(poolIDToResize)
		if err != nil {
			return nil, true, fmt.Errorf("error getting pool by using id %s", poolIDToResize)
		}

		if expandedPool == nil {
			return nil, false, fmt.Errorf("expanded pool value is nil")
		}
		if expandedPool.LastOperation != nil {
			log.Infof("Pool Resize Status : %v, Message : %s", expandedPool.LastOperation.Status, expandedPool.LastOperation.Msg)
			if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_FAILED {
				return nil, false, fmt.Errorf("pool %s expansion has failed. Error: %s", poolIDToResize, expandedPool.LastOperation)
			}
			if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_PENDING {
				return nil, true, fmt.Errorf("pool %s is in pending state, waiting to start", poolIDToResize)
			}
			if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_IN_PROGRESS {
				if strings.Contains(expandedPool.LastOperation.Msg, "Rebalance in progress") {
					if currentLastMsg == expandedPool.LastOperation.Msg {
						cnt += 1
					} else {
						cnt = 0
					}
					if cnt == 5 {
						return nil, false, fmt.Errorf("pool rebalance stuck at %s", currentLastMsg)
					}
					currentLastMsg = expandedPool.LastOperation.Msg

					return nil, true, fmt.Errorf("wait for pool rebalance to complete")
				}

				if strings.Contains(expandedPool.LastOperation.Msg, "No pending operation pool status: Maintenance") ||
					strings.Contains(expandedPool.LastOperation.Msg, "Storage rebalance complete pool status: Maintenance") {
					return nil, false, nil
				}

				return nil, true, fmt.Errorf("waiting for pool status to update")
			}
		}
		newPoolSize := expandedPool.TotalSize / units.GiB

		expectedSizeWithJournal := expectedSize
		if isJournalEnabled {
			expectedSizeWithJournal = expectedSizeWithJournal - 3
		}
		if newPoolSize >= expectedSizeWithJournal {
			// storage pool resize has been completed
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("pool has not been resized to %d or %d yet. Waiting...Current size is %d", expectedSize, expectedSizeWithJournal, newPoolSize)
	}

	_, err := task.DoRetryWithTimeout(f, poolResizeTimeout, retryTimeout)
	n, terr := GetNodeWithGivenPoolID(poolIDToResize)
	if terr == nil {
		PrintSvPoolStatus(*n)
	} else {
		log.Warnf("error getting node for pool uuid [%s]. Cause: %v", poolIDToResize, terr)
	}
	return err
}

func getPoolLastOperation(poolID string) (*api.StoragePoolOperation, error) {
	log.Infof(fmt.Sprintf("Getting pool status for %s", poolID))
	f := func() (interface{}, bool, error) {
		pool, err := GetStoragePoolByUUID(poolID)
		if err != nil {
			return nil, true, fmt.Errorf("error getting pool by using id %s", poolID)
		}

		if pool == nil {
			return nil, false, fmt.Errorf("pool value is nil")
		}
		if pool.LastOperation != nil {
			return pool.LastOperation, false, nil
		}
		return nil, true, fmt.Errorf("pool status not updated")
	}

	var poolLastOperation *api.StoragePoolOperation
	poolStatus, err := task.DoRetryWithTimeout(f, poolResizeTimeout, retryTimeout)
	if err != nil {
		return nil, err
	}
	poolLastOperation = poolStatus.(*api.StoragePoolOperation)
	return poolLastOperation, err
}

var _ = Describe("{PoolAddDrive}", func() {

	/*
		Add Drive using legacy add drive feature
	*/
	var testrailID = 2017
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/2017
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("PoolAddDrive", "Initiate pool expansion using add-drive", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "should get the existing storage node and expand the pool by adding a drive"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pooladddrive-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNode, err := getRandomNodeWithPoolIOs(contexts)
		log.FailOnError(err, "error identifying node to run test")
		err = AddCloudDrive(stNode, -1)
		log.FailOnError(err, "error adding cloud drive")

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{AddDriveAndPXRestart}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) Expand pool by adding cloud drives.
	//4) Restart px service where the pool is present.
	var testrailID = 2014
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/2014
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("AddDriveAndPXRestart", "Initiate pool expansion using add-drive and restart PX", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "should get the existing storage node and expand the pool by adding a drive"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pladddrvrestrt-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNode, err := getRandomNodeWithPoolIOs(contexts)
		log.FailOnError(err, "error identifying node to run test")
		err = AddCloudDrive(stNode, -1)
		log.FailOnError(err, "error adding cloud drive")
		stepLog = fmt.Sprintf("Restart PX on node %s", stNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err := Inst().V.RestartDriver(stNode, nil)
			log.FailOnError(err, fmt.Sprintf("error restarting px on node %s", stNode.Name))
			err = Inst().V.WaitDriverUpOnNode(stNode, 5*time.Minute)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", stNode.Name))
			dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after restarting on node %s", stNode.Name))
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})

var _ = Describe("{AddDriveWithPXRestart}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) Expand pool by adding cloud drives.
	//4) Restart px service where the pool expansion is in-progress
	//5) Verify total pool count after addition of cloud drive of same spec with PX restart

	var testrailID = 50632
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/50632
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("AddDriveWithPXRestart", "Initiate pool expansion using add-drive and restart PX while it is in progress", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool by adding a drive"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pladddrvwrst-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		var initialPoolCount int
		stNode, err := getRandomNodeWithPoolIOs(contexts)
		log.FailOnError(err, "error identifying node to run test")
		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "error getting pools list")
		dash.VerifyFatal(len(pools) > 0, true, "Verify pools exist")
		initialPoolCount = len(pools)

		var currentTotalPoolSize uint64
		var specSize uint64
		for _, pool := range pools {
			currentTotalPoolSize += pool.GetTotalSize() / units.GiB
		}

		driveSpecs, err := GetCloudDriveDeviceSpecs()
		log.FailOnError(err, "Error getting cloud drive specs")
		deviceSpec := driveSpecs[0]
		deviceSpecParams := strings.Split(deviceSpec, ",")

		for _, param := range deviceSpecParams {
			if strings.Contains(param, "size") {
				val := strings.Split(param, "=")[1]
				specSize, err = strconv.ParseUint(val, 10, 64)
				log.FailOnError(err, "Error converting size to uint64")
			}
		}
		expectedTotalPoolSize := currentTotalPoolSize + specSize

		stepLog := "Initiate add cloud drive and restart PX"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err = Inst().V.AddCloudDrive(&stNode, deviceSpec, -1)
			log.FailOnError(err, fmt.Sprintf("Add cloud drive failed on node %s", stNode.Name))
			time.Sleep(5 * time.Second)
			log.Infof(fmt.Sprintf("Restarting volume drive on node [%s]", stNode.Name))
			err = Inst().V.RestartDriver(stNode, nil)
			log.FailOnError(err, fmt.Sprintf("error restarting px on node %s", stNode.Name))
			err = Inst().V.WaitDriverUpOnNode(stNode, addDriveUpTimeOut)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", stNode.Name))
			log.InfoD("Validate pool rebalance after drive add and px restart")
			err = ValidateDriveRebalance(stNode)
			log.FailOnError(err, "Pool re-balance failed")
			dash.VerifyFatal(err == nil, true, "PX is up after add drive with vol driver restart")

			var finalPoolCount int
			var newTotalPoolSize uint64
			pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
			log.FailOnError(err, "error getting pools list")
			dash.VerifyFatal(len(pools) > 0, true, "Verify pools exist")
			for _, pool := range pools {
				newTotalPoolSize += pool.GetTotalSize() / units.GiB
			}
			finalPoolCount = len(pools)
			dash.VerifyFatal(newTotalPoolSize, expectedTotalPoolSize, fmt.Sprintf("Validate total pool size after add cloud drive on node %s", stNode.Name))
			dash.VerifyFatal(initialPoolCount+1 == finalPoolCount, true, fmt.Sprintf("Total pool count after cloud drive add with PX restart Expected:[%d] Got:[%d]", initialPoolCount+1, finalPoolCount))
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})

var _ = Describe("{PoolAddDriveVolResize}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) Expand pool by adding cloud drives.
	//4) expand the volume to the resized pool
	var testrailID = 2018
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/2018
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("PoolAddDriveVolResize", "pool expansion using add-drive and expand volume to the pool", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool by adding a drive"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pooladdvolrz-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		volSelected, err := GetVolumeWithMinimumSize(contexts, 2)
		log.FailOnError(err, "error identifying volume")
		appVol, err := Inst().V.InspectVolume(volSelected.ID)
		log.FailOnError(err, fmt.Sprintf("err inspecting vol : %s", volSelected.ID))
		volNodes := appVol.ReplicaSets[0].Nodes
		var stNode node.Node
		for _, n := range stNodes {
			nodeExist := false
			for _, vn := range volNodes {
				if n.Id == vn {
					nodeExist = true
				}
			}
			if !nodeExist {
				stNode = n
				break
			}
		}
		nodePools, err := GetPoolsDetailsOnNode(&stNode)
		log.FailOnError(err, fmt.Sprintf("error getting pools on node %s", stNode.Name))

		var nodePoolsIds []string
		for _, pool := range nodePools {
			nodePoolsIds = append(nodePoolsIds, pool.Uuid)
		}

		err = AddCloudDrive(stNode, -1)
		log.FailOnError(err, "error adding cloud drive")

		nodePools, err = GetPoolsDetailsOnNode(&stNode)

		log.FailOnError(err, fmt.Sprintf("error getting pools on node %s", stNode.Name))
		var nodePoolsIdsAfter []string
		for _, pool := range nodePools {
			nodePoolsIdsAfter = append(nodePoolsIdsAfter, pool.Uuid)
		}

		var selectedPool *api.StoragePool
		for _, pool := range nodePools {
			if !Contains(nodePoolsIds, pool.Uuid) {
				selectedPool = pool
				break
			}
		}
		if selectedPool == nil {
			log.FailOnError(fmt.Errorf("error getting new pool on node %s", stNode.Name), "Pool not found after add cloud drive for repl increase")

		}
		stepLog = "Expand volume to the expanded pool"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			currRep, err := Inst().V.GetReplicationFactor(volSelected)
			log.FailOnError(err, fmt.Sprintf("err getting repl factor for  vol : %s", volSelected.Name))
			opts := volume.Options{
				ValidateReplicationUpdateTimeout: replicationUpdateTimeout,
			}
			newRep := currRep
			if currRep == 3 {
				newRep = currRep - 1
				err = Inst().V.SetReplicationFactor(volSelected, newRep, nil, nil, true, opts)
				log.FailOnError(err, fmt.Sprintf("err setting repl factor  to %d for  vol : %s", newRep, volSelected.Name))
			}
			log.InfoD(fmt.Sprintf("setting repl factor  to %d for  vol : %s", newRep+1, volSelected.Name))
			err = Inst().V.SetReplicationFactor(volSelected, newRep+1, []string{stNode.Id}, []string{selectedPool.Uuid}, true, opts)
			log.FailOnError(err, fmt.Sprintf("err setting repl factor  to %d for  vol : %s", newRep+1, volSelected.Name))
			dash.VerifyFatal(err == nil, true, fmt.Sprintf("vol %s expanded successfully on node %s", volSelected.Name, stNode.Name))
			//Reverting to original rep for volume validation
			if currRep < 3 {
				err = Inst().V.SetReplicationFactor(volSelected, currRep, nil, nil, true, opts)
				log.FailOnError(err, fmt.Sprintf("err setting repl factor to %d for vol : %s", newRep, volSelected.Name))
			}
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{AddDriveMaintenanceMode}", func() {
	/*
		1.Put node in maintenance mode
		2. Perform add drive operatiom
		3. Validate add drive failed
		4.Exit node from maintenance mode
	*/
	var testrailID = 2013
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/2013
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("AddDriveMaintenanceMode", "pool expansion using add-drive when node is in maintenance mode", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and put it in maintenance mode"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("adddrvmnt-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNode, err := getRandomNodeWithPoolIOs(contexts)
		log.FailOnError(err, "error identifying node to run test")
		err = Inst().V.EnterMaintenance(stNode)
		log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenance mode", stNode.Name))
		status, err := Inst().V.GetNodeStatus(stNode)
		log.Infof(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))
		defer func() {
			status, err := Inst().V.GetNodeStatus(stNode)
			log.FailOnError(err, fmt.Sprintf("error getting node %s status", stNode.Name))
			log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))
			if *status == api.Status_STATUS_MAINTENANCE {
				log.InfoD(fmt.Sprintf("Exiting maintenance mode on node %s", stNode.Name))
				err = Inst().V.ExitMaintenance(stNode)
				log.FailOnError(err, fmt.Sprintf("fail to exit node %s in maintenance mode", stNode.Name))
				status, err = Inst().V.GetNodeStatus(stNode)
				log.FailOnError(err, fmt.Sprintf("err getting node [%s] status", stNode.Name))
				log.Infof(fmt.Sprintf("Node %s status %s after exit", stNode.Name, status.String()))
			}
		}()
		stepLog = fmt.Sprintf("add cloud drive to the node %s", stNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err = AddCloudDrive(stNode, -1)
			if err != nil {
				errStr := err.Error()
				res := strings.Contains(errStr, "node in maintenance mode") || strings.Contains(errStr, "couldn't get: /adddrive")
				dash.VerifySafely(res, true, fmt.Sprintf("Add drive failed when node [%s] is in maintenance mode. Error: %s", stNode.Name, errStr))
			} else {
				dash.VerifyFatal(err == nil, false, fmt.Sprintf("Add drive succeeded whien node [%s] is in maintenance mode", stNode.Name))
			}
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{AddDriveStoragelessAndResize}", func() {
	var testrailID = 50617
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/2017
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("AddDriveStorageless", "Initiate add-drive to storageless node and pool expansion", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the storageless node and add a drive"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("adddrvsl-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		slNodes := node.GetStorageLessNodes()
		if len(slNodes) == 0 {
			dash.VerifyFatal(len(slNodes) > 0, true, "Storage less nodes found?")
		}

		slNode := GetRandomStorageLessNode(slNodes)

		isDMthin, err := IsDMthin()
		log.FailOnError(err, "error verifying if set up is DMTHIN enabled")

		if isDMthin {
			err = AddMetadataDisk(slNode)
			log.FailOnError(err, "error while adding metadata disk")
		}

		err = AddCloudDrive(slNode, -1)
		log.FailOnError(err, "error adding cloud drive")
		stNodes := node.GetStorageNodes()
		var stNode node.Node
		for _, n := range stNodes {
			if n.Id == slNode.Id {
				stNode = n
				break
			}
		}
		dash.VerifyFatal(stNode.Name != "", true, fmt.Sprintf("Verify node %s is converted to storage node", slNode.Name))

		poolToResize := stNode.Pools[0]

		dash.VerifyFatal(poolToResize != nil, true, fmt.Sprintf("Is pool identified from stroage node %s?", stNode.Name))

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "error getting pools list")

		poolToBeResized := pools[poolToResize.Uuid]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		time.Sleep(time.Second * 60)
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			_, err := poolResizeIsInProgress(poolToBeResized)
			log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolToBeResized.Uuid, err))
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		stepLog = "Calculate expected pool size and trigger pool expansion by resize-disk "
		Step(stepLog, func() {
			log.InfoD(stepLog)
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check is journal enabled")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}
			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, false)
			log.FailOnError(err, fmt.Sprintf("Pool %s expansion init failed", poolToResize.Uuid))

			resizeErr := waitForPoolToBeResized(expectedSize, poolToResize.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Expected new size to be '%d' or '%d'", expectedSize, expectedSizeWithJournal))
		})

		pools, err = Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "error getting pools list")

		poolToBeResized = pools[poolToResize.Uuid]

		stepLog = "Calculate expected pool size and trigger pool expansion by add-disk"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check is journal enabled")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}
			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			enterPoolMaintenanceAddDisk(poolToBeResized.Uuid)
			poolResizeType := api.SdkStoragePool_RESIZE_TYPE_AUTO

			//Remove it once SdkStoragePool_RESIZE_TYPE_ADD_DISK is supported in dmthin
			if dmthin, err := IsDMthin(); err == nil {
				if !dmthin {
					poolResizeType = api.SdkStoragePool_RESIZE_TYPE_ADD_DISK
				}
			}
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, poolResizeType, expectedSize, false)
			log.FailOnError(err, fmt.Sprintf("Pool %s expansion init failed", poolToResize.Uuid))

			resizeErr := waitForPoolToBeResized(expectedSize, poolToResize.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Expected new size to be '%d' or '%d'", expectedSize, expectedSizeWithJournal))
			exitPoolMaintenance(poolToBeResized.Uuid)
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

func GetVolumeWithMinimumSize(contexts []*scheduler.Context, size uint64) (*volume.Volume, error) {
	var volSelected *volume.Volume
	//waiting till one of the volume has enough IO and selecting pool and node  using the volume to run the test
	f := func() (interface{}, bool, error) {
		for _, ctx := range contexts {
			vols, err := Inst().S.GetVolumes(ctx)
			if err != nil {
				return nil, true, err
			}
			for _, vol := range vols {
				log.Infof("checking vol %s", vol.ID)
				appVol, err := Inst().V.InspectVolume(vol.ID)
				if err != nil {
					return nil, true, err
				}
				usedBytes := appVol.GetUsage()
				log.Infof("usedBytes %d", usedBytes)
				usedGiB := usedBytes / units.GiB
				log.Infof("usedGiB %d", usedGiB)
				if usedGiB > size {
					volSelected = vol
					return nil, false, nil
				}
			}
		}
		return nil, true, fmt.Errorf("error getting volume with size atleast %d GiB used", size)
	}
	_, err := task.DoRetryWithTimeout(f, 120*time.Minute, retryTimeout)
	return volSelected, err
}

func getVolumeWithMinRepl(contexts []*scheduler.Context, repl int) (*volume.Volume, error) {
	var volSelected *volume.Volume

	f := func() (interface{}, bool, error) {
		for _, ctx := range contexts {
			vols, err := Inst().S.GetVolumes(ctx)
			if err != nil {
				return nil, true, err
			}
			for _, vol := range vols {
				appVol, err := Inst().V.InspectVolume(vol.ID)
				if err != nil {
					return nil, true, err
				}
				replNodes := appVol.ReplicaSets[0].Nodes

				if len(replNodes) >= repl {
					volSelected = vol
					return nil, false, nil
				}
			}
		}
		return nil, true, fmt.Errorf("error getting volume with minimum repl %d", repl)
	}
	_, err := task.DoRetryWithTimeout(f, 2*time.Minute, 10*time.Second)
	return volSelected, err
}

func getPoolWithLeastSize() *api.StoragePool {

	pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
	log.FailOnError(err, "error getting pools list")
	var currentSize uint64
	currentSize = 54975581388800 / units.GiB
	var selectedPool *api.StoragePool
	for _, pool := range pools {
		poolSize := pool.TotalSize / units.GiB
		if poolSize < currentSize {
			currentSize = poolSize
			selectedPool = pool
		}
	}
	log.Infof(fmt.Sprintf("Pool %s has least size %d", selectedPool.Uuid, currentSize))
	return selectedPool
}

func GetNodeWithLeastSize() *node.Node {
	stNodes := node.GetStorageNodes()
	var selectedNode node.Node
	var currLowestSize uint64
	currLowestSize = 54975581388800 / units.GiB
	for _, n := range stNodes {
		plSize := getTotalPoolSize(n) / units.GiB
		log.Debugf(fmt.Sprintf("Total pool size on node [%s] is [%d]Gib", n.Name, plSize))
		if plSize < currLowestSize {
			currLowestSize = plSize
			selectedNode = n
		}
	}
	log.Infof(fmt.Sprintf("Node %s has least total size %d", selectedNode.Name, currLowestSize))
	return &selectedNode
}

func waitForVolMinimumSize(volID string, size uint64) (bool, error) {

	//waiting till given volume has enough IO to run the test
	f := func() (interface{}, bool, error) {
		appVol, err := Inst().V.InspectVolume(volID)
		if err != nil {
			return nil, true, err
		}
		usedBytes := appVol.GetUsage()
		usedGiB := usedBytes / units.GiB
		if usedGiB >= size {
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("vol %s is not having required IO", volID)
	}
	_, err := task.DoRetryWithTimeout(f, 30*time.Minute, retryTimeout)
	if err != nil {
		return false, err
	}
	return true, nil
}

var _ = Describe("{AddNewDrivesMultipleTimes}", func() {
	//1) Deploy px with cloud drive.
	//2) Select a pool with iops happening.
	//3) Expand pool by adding cloud drives.
	//4) Expand pool again by adding cloud drives.
	//4) Expand pool again by pool expand auto.
	var testrailID = 2019
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/2019
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("PoolResizeMul", "Initiate pool resize multiple times", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool multiple times"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("poolresizemul-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		var selectedNode node.Node
		var err error
		var selectedPool *api.StoragePool
		for _, stNode := range stNodes {
			selectedPool, err = GetPoolWithIOsInGivenNode(stNode, contexts, api.SdkStoragePool_RESIZE_TYPE_AUTO, 0)
			if selectedPool != nil {
				drvMap, err := Inst().V.GetPoolDrives(&stNode)
				log.FailOnError(err, "error getting pool drives from node [%s]", stNode.Name)
				drvs := drvMap[fmt.Sprintf("%d", selectedPool.ID)]
				if len(drvs) > (POOL_MAX_CLOUD_DRIVES - 2) {
					continue
				}
				selectedNode = stNode
				break
			}
		}
		log.FailOnError(err, "error identifying node to run test")
		stepLog = fmt.Sprintf("Adding drive to the node %s and pool UUID: %s, Id:%d", selectedNode.Name, selectedPool.Uuid, selectedPool.ID)
		Step(stepLog, func() {
			err = AddCloudDrive(selectedNode, -1)
			log.FailOnError(err, "error adding cloud drive")
		})
		stepLog = fmt.Sprintf("Adding drive again to the node %s and pool UUID: %s, Id:%d", selectedNode.Name, selectedPool.Uuid, selectedPool.ID)
		Step(stepLog, func() {
			err = AddCloudDrive(selectedNode, -1)
			log.FailOnError(err, "error adding cloud drive")
		})

		stepLog = fmt.Sprintf("Expanding pool  on node %s and pool UUID: %s using auto", selectedNode.Name, selectedPool.Uuid)
		Step(stepLog, func() {
			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using auto", selectedPool.Uuid, selectedNode.Name))
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{PoolResizeDiskDiff}", func() {
	//1) Deploy px with cloud drive.
	//2) Select a pool with iops happening.
	//3) Expand pool by resize-disk
	//4) Expand pool again by resize-disk with different size multiple.
	//4) Expand pool again by resize-disk with different size multiple.
	var testrailID = 51311
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51311
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("PoolResizeDiskDiff", "Initiate pool resize multiple times with different size multiples using resize-disk", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool multiple times"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("plrszediff-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		var selectedNode node.Node
		var err error
		var selectedPool *api.StoragePool
		for _, stNode := range stNodes {
			selectedPool, err = GetPoolWithIOsInGivenNode(stNode, contexts, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 0)
			if selectedPool != nil {
				selectedNode = stNode
				break
			}
		}
		log.FailOnError(err, "error identifying node to run test")
		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "Failed to check if Journal enabled")

		stepLog = fmt.Sprintf("Expanding pool on node %s and pool UUID: %s using resize-disk", selectedNode.Name, selectedPool.Uuid)
		var drvSize uint64
		Step(stepLog, func() {
			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))
			drvSize, err = getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", selectedPool.Uuid, selectedNode.Name))
		})

		stepLog = fmt.Sprintf("Expanding pool  2nd time on node %s and pool UUID: %s using resize-disk", selectedNode.Name, selectedPool.Uuid)
		Step(stepLog, func() {
			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + 50 + drvSize

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", selectedPool.Uuid, selectedNode.Name))
		})

		stepLog = fmt.Sprintf("Expanding pool 3rd time on node %s and pool UUID: %s using resize-disk", selectedNode.Name, selectedPool.Uuid)
		Step(stepLog, func() {
			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + 150 + drvSize

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", selectedPool.Uuid, selectedNode.Name))
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{PoolAddDiskDiff}", func() {
	//1) Deploy px with cloud drive.
	//2) Select a pool with iops happening.
	//3) Expand pool by add-disk
	//4) Expand pool again by add-disk with different size multiple.
	//4) Expand pool again by add-disk with different size multiple.
	var testrailID = 51184
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51184
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("PoolAddDiskDiff", "Initiate pool resize multiple times with different size multiples using add-disk", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool multiple times"

	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("plradddiff-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		var selectedNode node.Node
		var err error
		var selectedPool *api.StoragePool
		for _, stNode := range stNodes {
			selectedPool, err = GetPoolWithIOsInGivenNode(stNode, contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
			if selectedPool != nil {
				selectedNode = stNode
				break
			}
		}
		log.FailOnError(err, "error identifying node to run test")
		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "Failed to check if Journal enabled")

		stepLog = fmt.Sprintf("Expanding pool on node %s and pool UUID: %s using add-disk", selectedNode.Name, selectedPool.Uuid)
		var drvSize uint64
		Step(stepLog, func() {
			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))
			drvSize, err = getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			enterPoolMaintenanceAddDisk(selectedPool.Uuid)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", selectedPool.Uuid, selectedNode.Name))
		})

		stepLog = fmt.Sprintf("Expanding pool 2nd time on node %s and pool UUID: %s using add-disk", selectedNode.Name, selectedPool.Uuid)
		Step(stepLog, func() {
			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + 50 + drvSize

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", selectedPool.Uuid, selectedNode.Name))
			exitPoolMaintenance(selectedPool.Uuid)
		})

		stepLog = fmt.Sprintf("Expanding pool 3rd time on node %s and pool UUID: %s using add-disk", selectedNode.Name, selectedPool.Uuid)
		Step(stepLog, func() {
			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + 100 + drvSize

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", selectedPool.Uuid, selectedNode.Name))
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{MultiDriveResizeDisk}", func() {
	//Select Pool with multiple drives
	//While IO is going onto repl=3 vols on all the pools on that system, expand the pool using ""pxctl sv pool expand-u <uuid> -s <size> -o resize-disk"
	var testrailID = 51266
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51266
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("MultiDriveResizeDisk", "Initiate pool resize multiple drive", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node with multi drives and resize-disk"

	It(stepLog, func() {
		log.InfoD(stepLog)
		var err error
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("muldrvresize-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "Failed to check if Journal enabled")
		minDiskCount := 1
		if isjournal {
			minDiskCount = 2
		}

		nodesWithMultiDrives := make([]node.Node, 0)
		for _, n := range stNodes {
			pxNode, err := Inst().V.GetDriverNode(&n)
			log.FailOnError(err, "Error getting PX node")
			if len(pxNode.Disks) > minDiskCount {
				nodesWithMultiDrives = append(nodesWithMultiDrives, n)
			}
		}
		dash.VerifyFatal(len(nodesWithMultiDrives) > 0, true, "nodes with multiple disks exist?")
		var selectedNode node.Node

		var selectedPool *api.StoragePool
		for _, stNode := range nodesWithMultiDrives {
			selectedPool, err = GetPoolWithIOsInGivenNode(stNode, contexts, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 0)
			if selectedPool != nil {
				selectedNode = stNode
				break
			}
		}
		log.FailOnError(err, "error identifying node to run test")

		stepLog = fmt.Sprintf("Expanding pool  on node %s and pool UUID: %s using resize-disk", selectedNode.Name, selectedPool.Uuid)
		Step(stepLog, func() {
			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, false)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", selectedPool.Uuid, selectedNode.Name))
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{ResizeWithPXRestart}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) Expand pool by resize-disk
	//4) Restart px service where the pool expansion is in-progress
	var testrailID = 51281
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51281
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("ResizeWithPXRestart", "Initiate pool expansion using resize-disk and restart PX while it is in progress", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool by resize-disk"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rsizedskrst-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNode, err := getRandomNodeWithPoolIOs(contexts)
		log.FailOnError(err, "error identifying node to run test")
		selectedPool, err := GetPoolWithIOsInGivenNode(stNode, contexts, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 0)
		log.FailOnError(err, "error identifying pool to run test")

		stepLog := "Initiate pool expansion drive and restart PX"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			err = WaitForExpansionToStart(poolToBeResized.Uuid)
			log.FailOnError(err, "pool expansion not started")
			err = Inst().V.RestartDriver(stNode, nil)
			log.FailOnError(err, fmt.Sprintf("error restarting px on node %s", stNode.Name))

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", selectedPool.Uuid, stNode.Name))

		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})

var _ = Describe("{AddWithPXRestart}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) Expand pool by add-disk
	//4) Restart px service where the pool expansion is in-progress

	JustBeforeEach(func() {
		StartTorpedoTest("AddWithPXRestart", "Initiate pool expansion using add-disk and restart PX while it is in progress", nil, 0)

	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool by resize-disk"

	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("adddskwrst-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNode, err := getRandomNodeWithPoolIOs(contexts)
		log.FailOnError(err, "error identifying node to run test")
		selectedPool, err := GetPoolWithIOsInGivenNode(stNode, contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		log.FailOnError(err, "error identifying pool to run test")

		stepLog := "Initiate pool expansion drive and restart PX"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			enterPoolMaintenanceAddDisk(selectedPool.Uuid)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			err = WaitForExpansionToStart(poolToBeResized.Uuid)
			log.FailOnError(err, "pool expansion not started")
			err = Inst().V.RestartDriver(stNode, nil)
			log.FailOnError(err, fmt.Sprintf("error restarting px on node %s", stNode.Name))

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", selectedPool.Uuid, stNode.Name))
			exitPoolMaintenance(selectedPool.Uuid)

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})

})

var _ = Describe("{ResizeDiskVolUpdate}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) Expand pool by resize-disk.
	//4) expand the volume to the resized pool
	var testrailID = 51290
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51290
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("ResizeDiskVolUpdate", "pool expansion using resize-disk and expand volume to the pool", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool by resize-disk"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("plrszvolupdt-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		volSelected, err := GetVolumeWithMinimumSize(contexts, 10)
		log.FailOnError(err, "error identifying volume")
		appVol, err := Inst().V.InspectVolume(volSelected.ID)
		log.FailOnError(err, fmt.Sprintf("err inspecting vol : %s", volSelected.ID))
		volNodes := appVol.ReplicaSets[0].Nodes
		var stNode node.Node
		for _, n := range stNodes {
			nodeExist := false
			for _, vn := range volNodes {
				if n.Id == vn {
					nodeExist = true
				}
			}
			if !nodeExist {
				stNode = n
				break
			}
		}
		selectedPool := stNode.Pools[0]
		var poolToBeResized *api.StoragePool
		stepLog := "Initiate pool expansion using resize-disk"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			poolToBeResized, err = GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))

			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", selectedPool.Uuid, stNode.Name))

		})
		stepLog = "Expand volume to the expanded pool"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			currRep, err := Inst().V.GetReplicationFactor(volSelected)
			log.FailOnError(err, fmt.Sprintf("err getting repl factor for  vol : %s", volSelected.Name))
			opts := volume.Options{
				ValidateReplicationUpdateTimeout: replicationUpdateTimeout,
			}
			newRep := currRep
			if currRep == 3 {
				newRep = currRep - 1
				err = Inst().V.SetReplicationFactor(volSelected, newRep, nil, nil, true, opts)
				log.FailOnError(err, fmt.Sprintf("err setting repl factor  to %d for  vol : %s", newRep, volSelected.Name))
			}
			log.InfoD(fmt.Sprintf("setting repl factor  to %d for  vol : %s", newRep+1, volSelected.Name))
			err = Inst().V.SetReplicationFactor(volSelected, newRep+1, []string{stNode.Id}, []string{poolToBeResized.Uuid}, true, opts)
			log.FailOnError(err, fmt.Sprintf("err setting repl factor  to %d for  vol : %s", newRep+1, volSelected.Name))
			dash.VerifyFatal(err == nil, true, fmt.Sprintf("vol %s expanded successfully on node %s", volSelected.Name, stNode.Name))
			//reverting the replication to volume validation to pass
			if currRep < 3 {
				err = Inst().V.SetReplicationFactor(volSelected, currRep, nil, nil, true, opts)
				log.FailOnError(err, fmt.Sprintf("err setting repl factor to %d for vol : %s", newRep, volSelected.Name))
			}
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{VolUpdateResizeDisk}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) expand the volume to the pool
	//4) perform resize disk operation on the pool while volume update is in-progress
	var testrailID = 51284
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51284
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("VolUpdateResizeDisk", "expand volume to the pool and pool expansion using resize-disk", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool by resize-disk"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("volupdtplrsz-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		volSelected, err := GetVolumeWithMinimumSize(contexts, 10)
		log.FailOnError(err, "error identifying volume")
		appVol, err := Inst().V.InspectVolume(volSelected.ID)
		log.FailOnError(err, fmt.Sprintf("err inspecting vol : %s", volSelected.ID))
		volNodes := appVol.ReplicaSets[0].Nodes
		var stNode node.Node
		for _, n := range stNodes {
			nodeExist := false
			for _, vn := range volNodes {
				if n.Id == vn {
					nodeExist = true
				}
			}
			if !nodeExist {
				stNode = n
				break
			}
		}
		selectedPool := stNode.Pools[0]
		var poolToBeResized *api.StoragePool
		poolToBeResized, err = GetStoragePoolByUUID(selectedPool.Uuid)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))

		stepLog = "Expand volume to the expanded pool"
		var newRep int64
		opts := volume.Options{
			ValidateReplicationUpdateTimeout: replicationUpdateTimeout,
		}
		var currRep int64
		Step(stepLog, func() {
			log.InfoD(stepLog)
			currRep, err = Inst().V.GetReplicationFactor(volSelected)
			log.FailOnError(err, fmt.Sprintf("err getting repl factor for  vol : %s", volSelected.Name))

			newRep = currRep
			if currRep == 3 {
				newRep = currRep - 1
				err = Inst().V.SetReplicationFactor(volSelected, newRep, nil, nil, true, opts)
				log.FailOnError(err, fmt.Sprintf("err setting repl factor  to %d for  vol : %s", newRep, volSelected.Name))
			}
			log.InfoD(fmt.Sprintf("setting repl factor to %d for vol : %s", newRep+1, volSelected.Name))
			err = Inst().V.SetReplicationFactor(volSelected, newRep+1, []string{stNode.Id}, []string{poolToBeResized.Uuid}, false, opts)
			log.FailOnError(err, fmt.Sprintf("err setting repl factor  to %d for  vol : %s", newRep+1, volSelected.Name))
			dash.VerifyFatal(err == nil, true, fmt.Sprintf("vol %s expansion triggered successfully on node %s", volSelected.Name, stNode.Name))
		})
		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "Failed to check if Journal enabled")

		stepLog := "Initiate pool expansion using resize-disk while repl increase is in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, false)
			if err != nil {
				if strings.Contains(fmt.Sprintf("%v", err), "Please re-issue expand with force") {
					err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
				}
			}
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", selectedPool.Uuid, stNode.Name))

		})
		err = ValidateReplFactorUpdate(volSelected, newRep+1)
		log.FailOnError(err, "error validating repl factor for vol [%s]", volSelected.Name)

		stepLog = "Initiate pool expansion using resize-disk after rsync is successfull"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolToBeResized, err = GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("error getting pool using UUID [%s]", selectedPool.Uuid))

			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)

			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", selectedPool.Uuid, stNode.Name))
		})

		//reverting the replication for volume validation
		if currRep < 3 {
			err = Inst().V.SetReplicationFactor(volSelected, currRep, nil, nil, true, opts)
			log.FailOnError(err, fmt.Sprintf("err setting repl factor to %d for vol : %s", newRep, volSelected.Name))
		}

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{VolUpdateAddDisk}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) expand the volume to the pool using add-disk
	//4) perform resize disk operation on the pool while volume update is in-progress

	JustBeforeEach(func() {
		StartTorpedoTest("VolUpdateAddDisk", "expand volume to the pool and pool expansion using add-disk", nil, 0)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool by resize-disk"

	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("volupdtplrsz-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		volSelected, err := GetVolumeWithMinimumSize(contexts, 10)
		log.FailOnError(err, "error identifying volume")
		appVol, err := Inst().V.InspectVolume(volSelected.ID)
		log.FailOnError(err, fmt.Sprintf("error inspecting vol : %s", volSelected.ID))
		volNodes := appVol.ReplicaSets[0].Nodes
		var stNode node.Node
		for _, n := range stNodes {
			eligibilityMap, err := GetPoolExpansionEligibility(&n, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
			if err != nil || !eligibilityMap[n.Id] {
				continue
			}

			nodeExist := false
			for _, vn := range volNodes {
				if n.Id == vn {
					nodeExist = true
				}
			}
			if !nodeExist {
				stNode = n
				break
			}
		}
		if stNode.VolDriverNodeID == "" {
			log.FailOnError(errors.New("no eligible node found for pool expansion"), "error identifying node to run test")

		}
		selectedPool := stNode.Pools[0]
		var poolToBeResized *api.StoragePool
		poolToBeResized, err = GetStoragePoolByUUID(selectedPool.Uuid)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))

		stepLog = "Expand volume to the expanded pool"
		var newRep int64
		opts := volume.Options{
			ValidateReplicationUpdateTimeout: replicationUpdateTimeout,
		}
		var currRep int64
		Step(stepLog, func() {
			log.InfoD(stepLog)
			currRep, err = Inst().V.GetReplicationFactor(volSelected)
			log.FailOnError(err, fmt.Sprintf("err getting repl factor for  vol : %s", volSelected.Name))

			newRep = currRep
			if currRep == 3 {
				newRep = currRep - 1
				err = Inst().V.SetReplicationFactor(volSelected, newRep, nil, nil, true, opts)
				log.FailOnError(err, fmt.Sprintf("error setting repl factor to %d for vol : %s", newRep, volSelected.Name))
			}
			log.InfoD(fmt.Sprintf("setting repl factor to %d for vol : %s", newRep+1, volSelected.Name))
			err = Inst().V.SetReplicationFactor(volSelected, newRep+1, []string{stNode.Id}, []string{poolToBeResized.Uuid}, false, opts)
			log.FailOnError(err, fmt.Sprintf("error setting repl factor to %d for vol : %s", newRep+1, volSelected.Name))
			dash.VerifyFatal(err == nil, true, fmt.Sprintf("vol %s expansion triggered successfully on node %s", volSelected.Name, stNode.Name))
		})

		stepLog := "Initiate pool expansion using add-disk"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			enterPoolMaintenanceAddDisk(selectedPool.Uuid)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", selectedPool.Uuid, stNode.Name))
			exitPoolMaintenance(selectedPool.Uuid)

		})
		err = ValidateReplFactorUpdate(volSelected, newRep+1)
		log.FailOnError(err, "error validating repl factor for vol [%s]", volSelected.Name)
		//reverting the replication for volume validation
		if currRep < 3 {
			err = Inst().V.SetReplicationFactor(volSelected, currRep, nil, nil, true, opts)
			log.FailOnError(err, fmt.Sprintf("err setting repl factor to %d for vol : %s", newRep, volSelected.Name))
		}

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{VolUpdateAddDrive}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) expand the volume to the pool
	//4) perform add drive on the node where volume update is in-progress
	var testrailID = 50635
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/50635
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("VolUpdateAddDrive", "expand volume to the pool and pool expansion using add drive", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool by resize-disk"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("plrszvolupdt-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		volSelected, err := GetVolumeWithMinimumSize(contexts, 10)
		log.FailOnError(err, "error identifying volume")
		appVol, err := Inst().V.InspectVolume(volSelected.ID)
		log.FailOnError(err, fmt.Sprintf("err inspecting vol : %s", volSelected.ID))
		volNodes := appVol.ReplicaSets[0].Nodes
		var stNode node.Node
		for _, n := range stNodes {
			nodeExist := false
			for _, vn := range volNodes {
				if n.Id == vn {
					nodeExist = true
				}
			}
			if !nodeExist {
				stNode = n
				break
			}
		}
		selectedPool := stNode.Pools[0]
		var poolToBeResized *api.StoragePool
		poolToBeResized, err = GetStoragePoolByUUID(selectedPool.Uuid)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))

		stepLog = "Expand volume to the expanded pool"
		var newRep int64
		var currRep int64
		opts := volume.Options{
			ValidateReplicationUpdateTimeout: replicationUpdateTimeout,
		}
		Step(stepLog, func() {
			log.InfoD(stepLog)
			currRep, err = Inst().V.GetReplicationFactor(volSelected)
			log.FailOnError(err, fmt.Sprintf("err getting repl factor for  vol : %s", volSelected.Name))

			newRep = currRep
			if currRep == 3 {
				newRep = currRep - 1
				err = Inst().V.SetReplicationFactor(volSelected, newRep, nil, nil, true, opts)
				log.FailOnError(err, fmt.Sprintf("err setting repl factor  to %d for  vol : %s", newRep, volSelected.Name))
			}
			log.InfoD(fmt.Sprintf("setting repl factor  to %d for  vol : %s", newRep+1, volSelected.Name))
			err = Inst().V.SetReplicationFactor(volSelected, newRep+1, []string{stNode.Id}, []string{poolToBeResized.Uuid}, false, opts)
			log.FailOnError(err, fmt.Sprintf("err setting repl factor  to %d for  vol : %s", newRep+1, volSelected.Name))
			dash.VerifyFatal(err == nil, true, fmt.Sprintf("vol %s expansion triggered successfully on node %s", volSelected.Name, stNode.Name))
		})

		stepLog := "Initiate pool expansion using add drive"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err = AddCloudDrive(stNode, -1)
			log.FailOnError(err, "error adding cloud drive")
			dash.VerifyFatal(err == nil, true, fmt.Sprintf("Verify pool %s on node %s expansion using add drive", poolToBeResized.Uuid, stNode.Name))

		})
		err = ValidateReplFactorUpdate(volSelected, newRep+1)
		log.FailOnError(err, "error validating repl factor for vol [%s]", volSelected.Name)
		//Reverting to original repl for volume validation
		if currRep < 3 {
			err = Inst().V.SetReplicationFactor(volSelected, currRep, nil, nil, true, opts)
			log.FailOnError(err, fmt.Sprintf("err setting repl factor to %d for vol : %s", newRep, volSelected.Name))
		}

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{AddDriveWithNodeReboot}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on o that pool and write some data on the volume.
	//3) Expand pool by adding cloud drives.
	//4) reboot the node where the pool is present and while pool expand is in progress.
	var testrailID = 50944
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/50944
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("AddDriveAndNodeReboot", "Initiate pool expansion using add-drive and reboot node", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool by adding a drive and reboot node"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pladddrvwrbt-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNode, err := getRandomNodeWithPoolIOs(contexts)
		log.FailOnError(err, "error identifying node to run test")
		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "error getting pools list")
		dash.VerifyFatal(len(pools) > 0, true, "Verify pools exist")

		var currentTotalPoolSize uint64
		var specSize uint64
		for _, pool := range pools {
			currentTotalPoolSize += pool.GetTotalSize() / units.GiB
		}

		driveSpecs, err := GetCloudDriveDeviceSpecs()
		log.FailOnError(err, "Error getting cloud drive specs")
		deviceSpec := driveSpecs[0]
		deviceSpecParams := strings.Split(deviceSpec, ",")

		for _, param := range deviceSpecParams {
			if strings.Contains(param, "size") {
				val := strings.Split(param, "=")[1]
				specSize, err = strconv.ParseUint(val, 10, 64)
				log.FailOnError(err, "Error converting size to uint64")
			}
		}
		expectedTotalPoolSize := currentTotalPoolSize + specSize

		stepLog := "Initiate add cloud drive and reboot node"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err = Inst().V.AddCloudDrive(&stNode, deviceSpec, -1)
			log.FailOnError(err, fmt.Sprintf("Add cloud drive failed on node %s", stNode.Name))
			err = Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "error refreshing driver end points")
			time.Sleep(3 * time.Second)
			err = RebootNodeAndWaitForPxUp(stNode)
			log.FailOnError(err, fmt.Sprintf("error rebooting node %s", stNode.Name))
			log.InfoD("Validate pool rebalance after drive add")
			err = ValidateDriveRebalance(stNode)
			log.FailOnError(err, "Pool re-balance failed")
			dash.VerifyFatal(err == nil, true, "PX is up after add drive")

			var newTotalPoolSize uint64
			pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
			log.FailOnError(err, "error getting pools list")
			dash.VerifyFatal(len(pools) > 0, true, "Verify pools exist")
			for _, pool := range pools {
				newTotalPoolSize += pool.GetTotalSize() / units.GiB
			}
			dash.VerifyFatal(newTotalPoolSize, expectedTotalPoolSize, fmt.Sprintf("Validate total pool size after add cloud drive on node %s", stNode.Name))
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})

var _ = Describe("{MulPoolsResize}", func() {
	//1) Deploy px with cloud drive.
	//2) Select multiple pools
	//3) Expand multiple pools by resize-disk same time.

	var testrailID = 51291
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51291
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("MulPoolsResize", "Initiate multiple pool resize on same node in parallel", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node with multiple pools and expand pools at same time using resize-disk"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("mulpoolsresiz-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		var poolsToBeResized []*api.StoragePool

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")

		numPoolsToResize := len(pools) / 3
		i := 1
		for _, v := range pools {
			if i == numPoolsToResize {
				break
			}
			poolsToBeResized = append(poolsToBeResized, v)
			i += 1
		}

		stepLog = fmt.Sprintf("Expanding multiple pools on node and pool using resize-disk")
		Step(stepLog, func() {

			resizedPoolsMap := make(map[string]uint64)
			for _, selPool := range poolsToBeResized {
				poolToBeResized, err := GetStoragePoolByUUID(selPool.Uuid)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selPool.Uuid))
				drvSize, err := getPoolDiskSize(poolToBeResized)
				log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
				expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize
				resizedPoolsMap[poolToBeResized.Uuid] = expectedSize
				log.InfoD("Current Size of the pool %s is %d", selPool.Uuid, poolToBeResized.TotalSize/units.GiB)
				err = Inst().V.ExpandPool(selPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
				dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			}

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")
			for selPoolID, expectedPoolSize := range resizedPoolsMap {

				resizeErr := waitForPoolToBeResized(expectedPoolSize, selPoolID, isjournal)
				dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on expansion using resize-disk", selPoolID))

			}

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{MulPoolsAddDisk}", func() {
	//1) Deploy px with cloud drive.
	//2) Select multiple pools
	//3) Expand multiple pools by add-disk same time.

	var testrailID = 50642
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/50642
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("MulPoolsAddDisk", "Initiate multiple pool add-disk on same node in parallel", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node with multiple pools and expand pools at same time using add-disk"

	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("mulpooladd-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		var poolsToBeResized []*api.StoragePool

		stNodes := node.GetStorageNodes()

		elMap := make(map[string]bool, 0)
		for _, stNode := range stNodes {
			el, err := GetPoolExpansionEligibility(&stNode, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
			log.FailOnError(err, "error getting pool expansion criteria for node [%s]", stNode.Name)
			for k, v := range el {
				elMap[k] = v
			}
		}

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")
		numPoolsToResize := len(pools) / 3
		i := 1
		for _, v := range pools {
			if i > numPoolsToResize {
				break
			}
			//checking if pool can be expanded using add-disk
			if elMap[v.Uuid] {
				poolsToBeResized = append(poolsToBeResized, v)
				i += 1
			}
		}
		stepLog = fmt.Sprintf("Expanding multiple pools on node and pool using add-disk")
		Step(stepLog, func() {
			log.InfoD(stepLog)
			resizedPoolsMap := make(map[string]uint64)
			for _, selPool := range poolsToBeResized {
				poolToBeResized, err := GetStoragePoolByUUID(selPool.Uuid)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selPool.Uuid))
				drvSize, err := getPoolDiskSize(poolToBeResized)
				log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
				expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize
				resizedPoolsMap[poolToBeResized.Uuid] = expectedSize

				log.FailOnError(err, "Failed to check if Journal enabled")

				log.InfoD("Current Size of the pool %s is %d", selPool.Uuid, poolToBeResized.TotalSize/units.GiB)
				err = Inst().V.ExpandPool(selPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
				dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			}

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")
			for selPoolID, expectedPoolSize := range resizedPoolsMap {
				resizeErr := waitForPoolToBeResized(expectedPoolSize, selPoolID, isjournal)
				dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on expansion using add-disk", selPoolID))
			}

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{ResizeWithJrnlAndMeta}", func() {
	//1) Deploy px with cloud drive and journal enabled.
	//2) Create a volume on that pool and write some data on the volume.
	//3) Get the metadata node
	//4) Expand the pool with journal device
	var testrailID = 51289
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51289
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("ResizeWithJrnlAndMeta", "Initiate pool expansion using resize-disk for "+
			"the pool the with journal and metadata devices", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the metadata node and expand the pool by resize-disk"

	It(stepLog, func() {
		log.InfoD(stepLog)
		journalStatus, err := IsJournalEnabled()
		log.FailOnError(err, "err getting journal status")
		dash.VerifyFatal(journalStatus, true, "verify journal device is enabled")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rsizedrvmeta-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNode, err := getRandomNodeWithPoolIOs(contexts)
		log.FailOnError(err, "error identifying node to run test")
		stNodePools := stNode.Pools

		var selectedPool *api.StoragePool
		for _, pool := range stNodePools {
			if pool.ID == int32(len(stNodePools)-1) {
				selectedPool = pool
				break
			}
		}

		stepLog := "Initiate pool expansion drive and restart PX"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, journalStatus)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", selectedPool.Uuid, stNode.Name))

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})

var _ = Describe("{PoolExpandWhileIOAndPXRestart}", func() {
	//step1: create volume repl node n1 and n2 and start IO
	//step2: during I/O restart px on n1 and at the same time expand the pool on n2
	//step3: after n1 is back operational validate that n2 pool size is the new size
	//step4: read/validate I/O after expansion
	var testrailID = 51445
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51445
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("PoolExpandWhileIOAndPXRestart", "Initiate pool expansion and restart px on n1 and at the same time expand the pool on n2 where vol repl exists", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the volume with IOs, expand the pool by resize-disk and restart PX on one the repl node"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rsizerepl-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		volSelected, err := getVolumeWithMinRepl(contexts, 2)
		log.FailOnError(err, "error identifying volume")
		appVol, err := Inst().V.InspectVolume(volSelected.ID)
		log.FailOnError(err, fmt.Sprintf("err inspecting vol : %s", volSelected.ID))
		replPools := appVol.ReplicaSets[0].PoolUuids
		storageNode1, err := GetNodeWithGivenPoolID(replPools[0])
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", replPools[0]))
		selectedPool := replPools[1]
		storageNode2, err := GetNodeWithGivenPoolID(selectedPool)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool))

		var poolToBeResized *api.StoragePool
		poolToBeResized, err = GetStoragePoolByUUID(selectedPool)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool))

		stepLog := "Initiate pool expansion drive on n2 and restart PX on n1"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			expectedSize := poolToBeResized.TotalSize * 2 / units.GiB

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			time.Sleep(3 * time.Second)
			err = Inst().V.RestartDriver(*storageNode1, nil)
			log.FailOnError(err, fmt.Sprintf("error restarting px on node %s", storageNode1.Name))

			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", poolToBeResized.Uuid, storageNode2.Name))

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})

var _ = Describe("{ResizeNodeMaintenanceCycle}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) Expand pool by resize-disk
	//4) Enter and Exit node maintenance
	//5) Validate PX and applications
	var testrailID = 51297
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51297
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("ResizeNodeMaintenanceCycle", "Initiate pool expansion using resize-disk and perform node maintenance cycle", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the volume with IOs, expand the pool by resize-disk and perform node maintenance cycle"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rsizenodem-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}

		var selectedNode node.Node
		var err error
		var selectedPool *api.StoragePool
		for _, stNode := range stNodes {
			selectedPool, err = GetPoolWithIOsInGivenNode(stNode, contexts, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 0)
			if selectedPool != nil {
				selectedNode = stNode
				break
			}
		}
		log.FailOnError(err, "error identifying node to run test")

		var poolToBeResized *api.StoragePool
		poolToBeResized, err = GetStoragePoolByUUID(selectedPool.Uuid)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))

		stepLog := "Initiate pool expansion drive start node maintenance"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", poolToBeResized.Uuid, selectedNode.Name))

			log.InfoD(fmt.Sprintf("Performing node maintenance cycle on node %s", selectedNode.Name))
			err = Inst().V.RecoverDriver(selectedNode)
			log.FailOnError(err, fmt.Sprintf("error performing maintenance cycle on node %s", selectedNode.Name))

			err = Inst().V.WaitDriverUpOnNode(selectedNode, 5*time.Minute)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", selectedNode.Name))
			dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after maintenance cycle on node %s", selectedNode.Name))

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})

var _ = Describe("{AddDiskNodeMaintenanceCycle}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) Expand pool by resize-disk
	//4) Enter and Exit node maintenance
	//5) Validate PX and applications
	var testrailID = 50647
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/50647
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("AddDiskNodeMaintenanceCycle", "Initiate pool expansion using add-disk and perform node maintenance cycle", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the volume with IOs, expand the pool by add-disk and perform node maintenance cycle"

	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Pool Add disk is not supported on DMThin Cluster")
		}
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("addnodem-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}

		var selectedNode node.Node
		var err error
		var selectedPool *api.StoragePool
		for _, stNode := range stNodes {
			selectedPool, err = GetPoolWithIOsInGivenNode(stNode, contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
			if selectedPool != nil {
				selectedNode = stNode
				break
			}
		}
		log.FailOnError(err, "error identifying node to run test")

		var poolToBeResized *api.StoragePool
		poolToBeResized, err = GetStoragePoolByUUID(selectedPool.Uuid)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))

		stepLog := "Initiate pool expansion drive start node maintenance"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", poolToBeResized.Uuid, selectedNode.Name))

			log.InfoD(fmt.Sprintf("Performing node maintenance cycle on node %s", selectedNode.Name))
			err = Inst().V.RecoverDriver(selectedNode)
			log.FailOnError(err, fmt.Sprintf("error performing maintenance cycle on node %s", selectedNode.Name))

			err = Inst().V.WaitDriverUpOnNode(selectedNode, 5*time.Minute)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", selectedNode.Name))
			dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after maintenance cycle on node %s", selectedNode.Name))

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})

var _ = Describe("{ResizePoolMaintenanceCycle}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) Expand pool by resize-disk
	//4) Enter and Exit pool maintenance
	//5) Validate PX and applications

	JustBeforeEach(func() {
		StartTorpedoTest("ResizePoolMaintenanceCycle", "Initiate pool expansion using resize-disk and perform pool maintenance cycle", nil, 0)

	})
	var contexts []*scheduler.Context

	stepLog := "should get the volume with IOs, expand the pool by resize-disk and perform pool maintenance cycle"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rsizepoolm-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}

		var selectedNode node.Node
		var err error
		var selectedPool *api.StoragePool
		for _, stNode := range stNodes {
			selectedPool, err = GetPoolWithIOsInGivenNode(stNode, contexts, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 0)
			if selectedPool != nil {
				selectedNode = stNode
				break
			}
		}
		log.FailOnError(err, "error identifying node to run test")

		var poolToBeResized *api.StoragePool
		poolToBeResized, err = GetStoragePoolByUUID(selectedPool.Uuid)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))

		stepLog := "Initiate pool expansion drive start pool maintenance"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", poolToBeResized.Uuid, selectedNode.Name))

			log.InfoD(fmt.Sprintf("Performing pool maintenance cycle on node %s", selectedNode.Name))
			err = Inst().V.RecoverPool(selectedNode)
			log.FailOnError(err, fmt.Sprintf("error performing pool maintenance cycle on node %s", selectedNode.Name))

			err = Inst().V.WaitDriverUpOnNode(selectedNode, 5*time.Minute)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", selectedNode.Name))
			dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after maintenance cycle on node %s", selectedNode.Name))
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})

})

var _ = Describe("{AddDiskPoolMaintenanceCycle}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) Expand pool by resize-disk
	//4) Enter and Exit pool maintenance
	//5) Validate PX and applications

	JustBeforeEach(func() {
		StartTorpedoTest("AddDiskPoolMaintenanceCycle", "Initiate pool expansion using add-disk and perform pool maintenance cycle", nil, 0)

	})
	var contexts []*scheduler.Context

	stepLog := "should get the volume with IOs, expand the pool by add-disk and perform pool maintenance cycle"

	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("addpoolm-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}

		var selectedNode node.Node
		var err error
		var selectedPool *api.StoragePool
		for _, stNode := range stNodes {
			selectedPool, err = GetPoolWithIOsInGivenNode(stNode, contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
			if selectedPool != nil {
				selectedNode = stNode
				break
			}
		}
		log.FailOnError(err, "error identifying node to run test")

		var poolToBeResized *api.StoragePool
		poolToBeResized, err = GetStoragePoolByUUID(selectedPool.Uuid)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))

		stepLog := "Initiate pool expansion drive start pool maintenance"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", poolToBeResized.Uuid, selectedNode.Name))

			log.InfoD(fmt.Sprintf("Performing pool maintenance cycle on node %s", selectedNode.Name))
			err = Inst().V.RecoverPool(selectedNode)
			log.FailOnError(err, fmt.Sprintf("error performing pool maintenance cycle on node %s", selectedNode.Name))

			err = Inst().V.WaitDriverUpOnNode(selectedNode, 5*time.Minute)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", selectedNode.Name))
			dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after maintenance cycle on node %s", selectedNode.Name))

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})

})

var _ = Describe("{NodeMaintenanceResize}", func() {
	/*
		1. Put node in maintenance mode
		2. Trigger pool expansion using resize-disk
		3. Exit maintenance mode
		4. Validate pool expansion
	*/
	var testrailID = 51269
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51269
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("NodeMaintenanceResize", "pool expansion using resize-disk when node is in maintenance mode", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and put it in maintenance mode"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rszedskmnt-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}

		// pick a pool from a pools list and resize it
		poolIDToResize = pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 0)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "error getting storage pools")
		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if val, err := poolResizeIsInProgress(poolToBeResized); val {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			} else {
				log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
			}
		})

		stNode, err := GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "error identifying node to run test")
		log.InfoD(fmt.Sprintf("Entering maintenance mode on node %s", stNode.Name))
		err = Inst().V.EnterMaintenance(*stNode)
		log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenance mode", stNode.Name))
		status, err := Inst().V.GetNodeStatus(*stNode)
		log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))
		stepLog = fmt.Sprintf("pool expansion to the node %s", stNode.Name)
		var expectedSize uint64
		Step(stepLog, func() {
			log.InfoD(stepLog)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize = (poolToBeResized.TotalSize / units.GiB) + drvSize

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
		})

		log.InfoD(fmt.Sprintf("Exiting maintenance mode on node %s", stNode.Name))
		t := func() (interface{}, bool, error) {

			status, err := Inst().V.GetNodeStatus(*stNode)
			if err != nil {
				return nil, true, err
			}
			log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))
			if *status == api.Status_STATUS_MAINTENANCE {
				log.InfoD(fmt.Sprintf("Exiting maintenance mode on node %s", stNode.Name))
				if err := Inst().V.ExitMaintenance(*stNode); err != nil {
					return nil, true, err
				}
			}

			return nil, false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 15*time.Minute, 2*time.Minute)
		log.FailOnError(err, fmt.Sprintf("fail to exit maintenance mode in node %s", stNode.Name))
		err = Inst().V.WaitDriverUpOnNode(*stNode, 5*time.Minute)
		log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", stNode.Name))
		dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after exiting maintenance on node %s", stNode.Name))
		status, err = Inst().V.GetNodeStatus(*stNode)
		log.FailOnError(err, fmt.Sprintf("Error getting status on node %s", stNode.Name))
		log.Infof(fmt.Sprintf("Node %s status %s after exit", stNode.Name, status.String()))

		stepLog = fmt.Sprintf("validating pool [%s] expansion", poolToBeResized.Uuid)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", poolToBeResized.Uuid, stNode.Name))
		})

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{NodeMaintenanceModeAddDisk}", func() {
	/*
		1. Put node in maintenance mode
		2. Trigger pool expansion using add-disk
		3. Exit maintenance mode
		4. Validate pool expansion
	*/
	var testrailID = 2013
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/2013
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("NodeMaintenanceModeAddDisk", "pool expansion using add-disk when node is in maintenance mode", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and put it in maintenance mode"

	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("adddskmnt-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}

		// pick a pool from a pools list and resize it
		poolIDToResize = pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "error getting storage pools")
		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if val, err := poolResizeIsInProgress(poolToBeResized); val {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			} else {
				log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
			}
		})

		stNode, err := GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "error identifying node to run test")
		log.InfoD(fmt.Sprintf("Entering maintenance mode on node %s", stNode.Name))
		err = Inst().V.EnterMaintenance(*stNode)
		log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenance mode", stNode.Name))
		status, err := Inst().V.GetNodeStatus(*stNode)
		log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))
		stepLog = fmt.Sprintf("pool expansion to the node %s", stNode.Name)
		var expectedSize uint64
		Step(stepLog, func() {
			log.InfoD(stepLog)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize = (poolToBeResized.TotalSize / units.GiB) + drvSize

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

		})
		log.InfoD(fmt.Sprintf("Exiting maintenance mode on node %s", stNode.Name))
		t := func() (interface{}, bool, error) {

			status, err := Inst().V.GetNodeStatus(*stNode)
			if err != nil {
				return nil, true, err
			}
			log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))
			if *status == api.Status_STATUS_MAINTENANCE {
				log.InfoD(fmt.Sprintf("Exiting maintenance mode on node %s", stNode.Name))
				if err := Inst().V.ExitMaintenance(*stNode); err != nil {
					return nil, true, err
				}
			}

			return nil, false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 15*time.Minute, 2*time.Minute)
		log.FailOnError(err, fmt.Sprintf("fail to exit maintenance mode in node %s", stNode.Name))
		err = Inst().V.WaitDriverUpOnNode(*stNode, 5*time.Minute)
		log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", stNode.Name))
		dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after exiting maintenance on node %s", stNode.Name))
		status, err = Inst().V.GetNodeStatus(*stNode)
		log.FailOnError(err, fmt.Sprintf("Error getting status on node %s", stNode.Name))
		log.Infof(fmt.Sprintf("Node %s status %s after exit", stNode.Name, status.String()))

		stepLog = fmt.Sprintf("validating pool [%s] expansion", poolToBeResized.Uuid)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", poolToBeResized.Uuid, stNode.Name))
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{PoolMaintenanceModeResize}", func() {

	/*
		1. Put pool in maintenance mode
		2. Trigger pool expansion using resize-disk
		3. Validate pool expansion
		4. Exit pool maintenance mode
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("PoolMaintenanceModeResize", "pool expansion using resize-disk when pool is in maintenance mode", nil, 0)

	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and put it in maintenance mode"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rszedskmnt-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}

		// pick a pool from a pools list and resize it
		poolIDToResize = pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 0)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "error getting storage pools")
		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if val, err := poolResizeIsInProgress(poolToBeResized); val {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			} else {
				log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
			}
		})

		stNode, err := GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "error identifying node to run test")
		log.InfoD(fmt.Sprintf("Entering pool maintenance mode on node %s", stNode.Name))
		err = Inst().V.EnterPoolMaintenance(*stNode)
		log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenance mode", stNode.Name))
		status, err := Inst().V.GetNodeStatus(*stNode)
		log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))
		stepLog = fmt.Sprintf("pool expansion to the node %s", stNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", poolToBeResized.Uuid, stNode.Name))

		})
		log.InfoD(fmt.Sprintf("Exiting pool maintenance mode on node %s", stNode.Name))

		t := func() (interface{}, bool, error) {

			status, err := Inst().V.GetNodePoolsStatus(*stNode)
			if err != nil {
				return nil, true, err
			}
			log.InfoD(fmt.Sprintf("pool %s has status %s", stNode.Name, status[poolToBeResized.Uuid]))
			if status[poolToBeResized.Uuid] == "In Maintenance" {
				log.InfoD(fmt.Sprintf("Exiting pool maintenance mode on node %s", stNode.Name))
				if err := Inst().V.ExitPoolMaintenance(*stNode); err != nil {
					return nil, true, err
				}
			}
			return nil, false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 1*time.Minute)
		err = Inst().V.WaitDriverUpOnNode(*stNode, 5*time.Minute)
		log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", stNode.Name))
		dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after maintenance cycle on node %s", stNode.Name))
		status, err = Inst().V.GetNodeStatus(*stNode)
		log.FailOnError(err, "err getting node [%s] status", stNode.Name)
		log.Infof(fmt.Sprintf("Node %s status %s after exit", stNode.Name, status.String()))
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{PoolMaintenanceModeAddDisk}", func() {
	/*
		1. Put pool in maintenance mode
		2. Trigger pool expansion using add-disk
		3. Validate pool expansion
		4. Exit pool maintenance mode
	*/

	var (
		contexts = make([]*scheduler.Context, 0)
	)

	JustBeforeEach(func() {
		StartTorpedoTest("PoolMaintenanceModeAddDisk", "pool expansion using add-disk when pool is in maintenance mode", nil, 0)
	})

	stepLog := "should get the existing storage node and put it in maintenance mode"

	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Pool Add disk is not supported on DMThin Cluster")
		}
		log.InfoD(stepLog)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("adddskmnt-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}

		// pick a pool from a pools list and resize it
		poolIDToResize = pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "error getting storage pools")
		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if val, err := poolResizeIsInProgress(poolToBeResized); val {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			} else {
				log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
			}
		})

		stNode, err := GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "error identifying node to run test")
		log.InfoD(fmt.Sprintf("Entering maintenance mode on node %s", stNode.Name))
		err = Inst().V.EnterPoolMaintenance(*stNode)
		log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenance mode", stNode.Name))
		status, err := Inst().V.GetNodeStatus(*stNode)
		log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))
		stepLog = fmt.Sprintf("pool expansion to the node %s", stNode.Name)
		Step(stepLog, func() {
			defer func() {
				log.InfoD(fmt.Sprintf("Exiting pool maintenance mode on node %s", stNode.Name))
				t := func() (interface{}, bool, error) {

					status, err := Inst().V.GetNodePoolsStatus(*stNode)
					if err != nil {
						return nil, true, err
					}
					log.InfoD(fmt.Sprintf("pool %s has status %s", stNode.Name, status[poolToBeResized.Uuid]))
					if status[poolToBeResized.Uuid] == "In Maintenance" {
						log.InfoD(fmt.Sprintf("Exiting pool maintenance mode on node %s", stNode.Name))
						if err := Inst().V.ExitPoolMaintenance(*stNode); err != nil {
							return nil, true, err
						}
					}

					return nil, false, nil
				}
				_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 1*time.Minute)
				err = Inst().V.WaitDriverUpOnNode(*stNode, 5*time.Minute)
				log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", stNode.Name))
				dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after maintenance cycle on node %s", stNode.Name))
			}()
			log.InfoD(stepLog)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			if IsPoolAddDiskSupported() {
				log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
				err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
				log.FailOnError(err, "Pool Expand using Add Disk Failed ")
				resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
				dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", poolToBeResized.Uuid, stNode.Name))
			}
			status, err = Inst().V.GetNodeStatus(*stNode)
			log.FailOnError(err, "err getting node [%s] status", stNode.Name)
			log.Infof(fmt.Sprintf("Node %s status %s after exit", stNode.Name, status.String()))
		})

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{AddDiskNodeMaintenanceMode}", func() {
	/*
		1. Trigger pool expansion using add-disk
		2. Place node in maintenance mode once expansion starts
		3. Exit maintenance mode
		4. Validate pool expansion
	*/

	var (
		contexts = make([]*scheduler.Context, 0)
	)

	JustBeforeEach(func() {
		StartTorpedoTest("AddDiskMaintenanceMode", "pool expansion using add-disk then put node is in maintenance mode", nil, 0)
	})

	stepLog := "should get the existing storage node,trigger add-disk and put it in maintenance mode"

	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("mntadddsk-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}

		// pick a pool from a pools list and resize it
		poolIDToResize = pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "error getting storage pools")
		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if val, err := poolResizeIsInProgress(poolToBeResized); val {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			} else {
				log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
			}
		})

		stNode, err := GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "error identifying node to run test")

		stepLog = fmt.Sprintf("pool expansion to the node %s and put it in maintenance mode", stNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			err = WaitForExpansionToStart(poolToBeResized.Uuid)
			log.FailOnError(err, "pool expansion not started")
			log.InfoD(fmt.Sprintf("Entering maintenance mode on node %s", stNode.Name))
			err = Inst().V.EnterMaintenance(*stNode)
			log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenance mode", stNode.Name))
			status, err := Inst().V.GetNodeStatus(*stNode)
			log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))

			//Waiting for 5 mins before exiting node maintenance
			time.Sleep(5 * time.Minute)

			log.InfoD(fmt.Sprintf("Exiting maintenance mode on node %s", stNode.Name))
			t := func() (interface{}, bool, error) {

				status, err := Inst().V.GetNodeStatus(*stNode)
				if err != nil {
					return nil, true, err
				}
				log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))
				if *status == api.Status_STATUS_MAINTENANCE {
					log.InfoD(fmt.Sprintf("Exiting maintenance mode on node %s", stNode.Name))
					if err := Inst().V.ExitMaintenance(*stNode); err != nil {
						return nil, true, err
					}
				}

				return nil, false, nil
			}
			_, err = task.DoRetryWithTimeout(t, 15*time.Minute, 2*time.Minute)
			log.FailOnError(err, fmt.Sprintf("fail to exit maintenance mode in node %s", stNode.Name))
			err = Inst().V.WaitDriverUpOnNode(*stNode, 5*time.Minute)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", stNode.Name))
			dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after exiting maintenance on node %s", stNode.Name))
			status, err = Inst().V.GetNodeStatus(*stNode)
			log.FailOnError(err, "error get node [%s] status", stNode.Name)
			log.Infof(fmt.Sprintf("Node %s status %s after exit", stNode.Name, status.String()))

			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", poolToBeResized.Uuid, stNode.Name))

		})

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{ResizeNodeMaintenanceMode}", func() {
	/*
		1. Trigger pool expansion using resize-disk
		2. Place node in maintenance mode once expansion starts
		3. Wait for some time and exit maintenance mode
		4. Validate pool expansion
	*/

	var (
		contexts = make([]*scheduler.Context, 0)
	)

	JustBeforeEach(func() {
		StartTorpedoTest("ResizeNodeMaintenanceMode", "pool expansion using resize-disk then put node is in maintenance mode", nil, 0)
	})

	stepLog := "should get the existing storage node,trigger resize-disk and put it in maintenance mode"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("mntrsze-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}

		// pick a pool from a pools list and resize it
		poolIDToResize = pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 0)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "error getting storage pools")
		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if val, err := poolResizeIsInProgress(poolToBeResized); val {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			} else {
				log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
			}
		})

		stNode, err := GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "error identifying node to run test")

		stepLog = fmt.Sprintf("pool expansion to the node %s and put it in maintenance mode", stNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			err = WaitForExpansionToStart(poolToBeResized.Uuid)
			log.FailOnError(err, "pool expansion not started")
			log.InfoD(fmt.Sprintf("Entering maintenance mode on node %s", stNode.Name))
			err = Inst().V.EnterMaintenance(*stNode)
			log.FailOnError(err, fmt.Sprintf("fail to enter node %s into maintenance mode", stNode.Name))
			status, err := Inst().V.GetNodeStatus(*stNode)
			log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))
			//wait for 1 minute before existing maintenance
			time.Sleep(1 * time.Minute)
			log.InfoD(fmt.Sprintf("Exiting maintenance mode on node %s", stNode.Name))
			t := func() (interface{}, bool, error) {

				status, err := Inst().V.GetNodeStatus(*stNode)
				if err != nil {
					return nil, true, err
				}
				log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))
				if *status == api.Status_STATUS_MAINTENANCE {
					log.InfoD(fmt.Sprintf("Exiting maintenance mode on node %s", stNode.Name))
					if err := Inst().V.ExitMaintenance(*stNode); err != nil {
						return nil, true, err
					}
				}

				return nil, false, nil
			}
			_, err = task.DoRetryWithTimeout(t, 15*time.Minute, 2*time.Minute)
			log.FailOnError(err, fmt.Sprintf("fail to exit maintenance mode on node %s", stNode.Name))
			err = Inst().V.WaitDriverUpOnNode(*stNode, 5*time.Minute)
			dash.VerifyFatal(err, nil, fmt.Sprintf("verify PX is up after exiting maintenance on node %s", stNode.Name))
			status, err = Inst().V.GetNodeStatus(*stNode)
			log.FailOnError(err, "error getting node [%s] status", stNode.Name)
			log.Infof(fmt.Sprintf("Node %s status %s after exit", stNode.Name, status.String()))
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", poolToBeResized.Uuid, stNode.Name))

		})

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{ResizePoolMaintenanceMode}", func() {
	/*
		1. Trigger pool expansion using resize-disk
		2. Put pool in maintenance mode
		3. Validate pool expansion
		4. Exit pool maintenance mode
	*/

	var (
		contexts = make([]*scheduler.Context, 0)
	)

	JustBeforeEach(func() {
		StartTorpedoTest("ResizePoolMaintenanceMode", "pool expansion using resize-disk then put pool in maintenance mode", nil, 0)
	})

	stepLog := "should get the existing storage node and put it in maintenance mode"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("plmntrsze-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}

		// pick a pool from a pools list and resize it
		poolIDToResize = pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 0)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "error getting storage pools")
		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if val, err := poolResizeIsInProgress(poolToBeResized); val {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			} else {
				log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
			}
		})

		stNode, err := GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "error identifying node to run test")
		stepLog = fmt.Sprintf("pool expansion to the node %s and trigger pool maintenance", stNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			err = WaitForExpansionToStart(poolToBeResized.Uuid)
			log.FailOnError(err, "pool expansion not started")
			log.InfoD(fmt.Sprintf("Entering pool maintenance mode on node %s", stNode.Name))
			err = Inst().V.EnterPoolMaintenance(*stNode)
			log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenance mode", stNode.Name))
			status, err := Inst().V.GetNodeStatus(*stNode)
			log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))

			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", poolToBeResized.Uuid, stNode.Name))

		})
		log.InfoD(fmt.Sprintf("Exiting pool maintenance mode on node %s", stNode.Name))
		t := func() (interface{}, bool, error) {
			status, err := Inst().V.GetNodePoolsStatus(*stNode)
			if err != nil {
				return nil, true, err
			}
			log.InfoD(fmt.Sprintf("pool %s has status %s", stNode.Name, status[poolToBeResized.Uuid]))
			if status[poolToBeResized.Uuid] == "In Maintenance" {
				log.InfoD(fmt.Sprintf("Exiting pool maintenance mode on node %s", stNode.Name))
				if err := Inst().V.ExitPoolMaintenance(*stNode); err != nil {
					return nil, true, err
				}
			}
			return nil, false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 1*time.Minute)
		err = Inst().V.WaitDriverUpOnNode(*stNode, 5*time.Minute)
		log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", stNode.Name))
		dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after maintenance cycle on node %s", stNode.Name))
		status, err := Inst().V.GetNodeStatus(*stNode)
		log.FailOnError(err, "error getting node [%s] status", stNode.Name)
		log.Infof(fmt.Sprintf("Node %s status %s after exit", stNode.Name, status.String()))
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{AddDiskPoolMaintenanceMode}", func() {

	/*
		1. Trigger pool expansion using add-disk
		2. Put pool in maintenance mode
		3. Validate pool expansion
		4. Exit pool maintenance mode
	*/

	var (
		contexts = make([]*scheduler.Context, 0)
	)

	JustBeforeEach(func() {
		StartTorpedoTest("AddDiskPoolMaintenanceMode", "pool expansion using add-disk then put pool in maintenance mode", nil, 0)
	})

	stepLog := "should get the existing storage node and put it in maintenance mode"

	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Pool Add disk is not supported on DMThin Cluster")
		}
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("plmntadddsk-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}

		// pick a pool from a pools list and resize it
		poolIDToResize = pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "error getting storage pools")
		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if val, err := poolResizeIsInProgress(poolToBeResized); val {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			} else {
				log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
			}
		})

		stNode, err := GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "error identifying node to run test")
		stepLog = fmt.Sprintf("pool expansion to the node %s and trigger pool maintenance", stNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			err = WaitForExpansionToStart(poolToBeResized.Uuid)
			log.FailOnError(err, "pool expansion not started")
			log.InfoD(fmt.Sprintf("Entering pool maintenance mode on node %s", stNode.Name))
			err = Inst().V.EnterPoolMaintenance(*stNode)
			log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenance mode", stNode.Name))
			status, err := Inst().V.GetNodeStatus(*stNode)
			log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))

			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", poolToBeResized.Uuid, stNode.Name))

		})
		log.InfoD(fmt.Sprintf("Exiting pool maintenance mode on node %s", stNode.Name))
		t := func() (interface{}, bool, error) {
			status, err := Inst().V.GetNodePoolsStatus(*stNode)
			if err != nil {
				return nil, true, err
			}
			log.InfoD(fmt.Sprintf("pool %s has status %s", stNode.Name, status[poolToBeResized.Uuid]))
			if status[poolToBeResized.Uuid] == "In Maintenance" {
				log.InfoD(fmt.Sprintf("Exiting pool maintenance mode on node %s", stNode.Name))
				if err := Inst().V.ExitPoolMaintenance(*stNode); err != nil {
					return nil, true, err
				}
			}
			return nil, false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 1*time.Minute)
		err = Inst().V.WaitDriverUpOnNode(*stNode, 5*time.Minute)
		log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", stNode.Name))
		dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after maintenance cycle on node %s", stNode.Name))
		status, err := Inst().V.GetNodeStatus(*stNode)
		log.FailOnError(err, "error getting node [%s] status", stNode.Name)
		log.Infof(fmt.Sprintf("Node %s status %s after exit", stNode.Name, status.String()))
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{PXRestartResize}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) Restart px service
	//4) Expand pool by resize-disk

	JustBeforeEach(func() {
		StartTorpedoTest("PXRestartResize", "Restart PX and initiate pool expansion using resize-disk", nil, 0)

	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node,restart PX and expand the pool by resize-disk"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rstrszedsk-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNode, err := getRandomNodeWithPoolIOs(contexts)
		log.FailOnError(err, "error identifying node to run test")
		selectedPool, err := GetPoolWithIOsInGivenNode(stNode, contexts, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 0)
		log.FailOnError(err, "error identifying pool to run test")

		err = Inst().V.RestartDriver(stNode, nil)
		log.FailOnError(err, fmt.Sprintf("error restarting px on node %s", stNode.Name))

		stepLog := "Initiate pool expansion drive while PX is restarting"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))

			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", selectedPool.Uuid, stNode.Name))

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})

})

var _ = Describe("{PXRestartAddDisk}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) Restart px service
	//4)Expand pool by add-disk

	JustBeforeEach(func() {
		StartTorpedoTest("PXRestartAddDisk", "Restart PX and Initiate pool expansion using add-disk", nil, 0)

	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool by add-disk"

	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rstadddsk-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNode, err := getRandomNodeWithPoolIOs(contexts)
		log.FailOnError(err, "error identifying node to run test")
		selectedPool, err := GetPoolWithIOsInGivenNode(stNode, contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		log.FailOnError(err, "error identifying pool to run test")

		err = Inst().V.RestartDriver(stNode, nil)
		log.FailOnError(err, fmt.Sprintf("error restarting px on node %s", stNode.Name))

		stepLog := "Initiate pool expansion drive while PX is restarting"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", selectedPool.Uuid, stNode.Name))

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})

})

var _ = Describe("{PoolExpandPendingUntilVolClean}", func() {
	/*
		step1: create volume repl=2 n1 and n2, bring down n1
		step2: feed data to volume then bring back n1 and when volume is resync bring down n2, so n1 is pending for resync
		step3: expand pool p1 on n1 and check the operation status should be pending
		step4: bring back n2 and wait until volume is clean and validate p1 size and n1 capacity
	*/

	var testrailID = 51442
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51442
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("PoolExpandPendingUntilVolClean", "Expand pool should wait until volume gets clean", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the volume with IOs and resync pending, expand the pool by resize-disk"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rsizecln-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		volSelected, err := getVolumeWithMinRepl(contexts, 2)
		log.FailOnError(err, "error identifying volume")
		appVol, err := Inst().V.InspectVolume(volSelected.ID)
		log.FailOnError(err, fmt.Sprintf("err inspecting vol : %s", volSelected.ID))
		replPools := appVol.ReplicaSets[0].PoolUuids
		selectedPool := replPools[0]
		storageNode1, err := GetNodeWithGivenPoolID(selectedPool)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", replPools[0]))
		storageNode2, err := GetNodeWithGivenPoolID(replPools[1])
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool))

		var poolToBeResized *api.StoragePool
		poolToBeResized, err = GetStoragePoolByUUID(selectedPool)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool))

		stepLog := "Stop PX on n1 and validate volume data and start PX on n1"
		Step(stepLog, func() {

			log.InfoD(stepLog)
			usedBytes := appVol.GetUsage()
			currUsedGiB := usedBytes / units.GiB
			log.Infof("Curr GiB %d", currUsedGiB)
			err = Inst().V.StopDriver([]node.Node{*storageNode1}, false, nil)
			log.FailOnError(err, "error stopping vol driver on node [%s]", storageNode1.Name)
			_, err = waitForVolMinimumSize(appVol.Id, currUsedGiB+10)
			log.FailOnError(err, fmt.Sprintf("Volume %s has not enough IO", appVol.Id))

			err = Inst().V.StartDriver(*storageNode1)
			log.FailOnError(err, "error starting vol driver on node [%s]", storageNode1.Name)
			err = Inst().V.WaitDriverUpOnNode(*storageNode1, 5*time.Minute)
			log.FailOnError(err, "error waiting for vol driver to be up on node [%s]", storageNode1.Name)

			time.Sleep(5 * time.Second)
			appVol, err = Inst().V.InspectVolume(appVol.Id)
			log.FailOnError(err, fmt.Sprintf("err inspecting vol : %s", appVol.Id))
			err = Inst().V.StopDriver([]node.Node{*storageNode2}, false, nil)
			log.FailOnError(err, "error stopping vol driver on node [%s]", storageNode2.Name)
			time.Sleep(5 * time.Second)
			appVol, err = Inst().V.InspectVolume(appVol.Id)
			log.FailOnError(err, fmt.Sprintf("err inspecting vol : %s", appVol.Id))

			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, false)
			if err != nil {
				if strings.Contains(fmt.Sprintf("%v", err), "Please re-issue expand with force") {
					err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
				}
			}
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			err = Inst().V.StartDriver(*storageNode2)
			log.FailOnError(err, "error starting vol driver on node [%s]", storageNode2.Name)
			err = Inst().V.WaitDriverUpOnNode(*storageNode2, 5*time.Minute)
			log.FailOnError(err, "error waiting for vol driver to be up on node [%s]", storageNode2.Name)
			poolStatus, err := getPoolLastOperation(poolToBeResized.Uuid)
			log.FailOnError(err, "error getting pool status")
			dash.VerifySafely(poolStatus.Status, api.SdkStoragePool_OPERATION_PENDING, "Verify pool status")
			dash.VerifySafely(strings.Contains(poolStatus.Msg, "to be clean before starting pool expansion"), true, fmt.Sprintf("verify pool expansion message %s", poolStatus.Msg))
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", poolToBeResized.Uuid, storageNode2.Name))

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})

var _ = Describe("{AddNewPoolWhileFullPoolExpanding}", func() {
	/*
		step1: create volume repl=2, and get its pool P1 on n1 and p2 on n2, expand p2 by increasing P1's size
		step2: feed p1 size GB I/O on the volume
		step3: After I/O done p1 should be offline and full, expand the pool p1 when p1 is rebalancing add a new drive with different size so that a new pool would be created
		step4: validate the pool and the data
	*/
	var testrailID = 51443
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51443
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("AddNewPoolWhileFullPoolExpanding", "Feed a pool full, then expand the pool when it is rebalancing add another pool", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context

	stepLog := "Create vols and make pool full"
	It(stepLog, func() {
		log.InfoD(stepLog)
		selectedNode := GetNodeWithLeastSize()
		if selectedNode.Name == "" {
			log.FailOnError(fmt.Errorf("unable get node with least size"), "error identifying the node with least size")
		}
		log.Infof(fmt.Sprintf("Node %s is marked for repl 1", selectedNode.Name))
		stNodes := node.GetStorageNodes()
		var secondReplNode node.Node
		for _, stNode := range stNodes {
			if stNode.Name != selectedNode.Name {
				secondReplNode = stNode
			}
		}

		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "is journal enabled check failed")
		err = adjustReplPools(*selectedNode, secondReplNode, isjournal)
		log.FailOnError(err, fmt.Sprintf("error increasing pool size on node %s", secondReplNode.Name))

		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
			err = Inst().S.RemoveLabelOnNode(*selectedNode, k8s.NodeType)
			log.FailOnError(err, "error removing label on node [%s]", selectedNode.Name)
			err = Inst().S.RemoveLabelOnNode(secondReplNode, k8s.NodeType)
			log.FailOnError(err, "error removing label on node [%s]", secondReplNode.Name)
		}()

		err = Inst().S.AddLabelOnNode(*selectedNode, k8s.NodeType, k8s.FastpathNodeType)
		log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", selectedNode.Name))
		err = Inst().S.AddLabelOnNode(secondReplNode, k8s.NodeType, k8s.FastpathNodeType)
		log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", secondReplNode.Name))

		Inst().AppList = []string{"fio-fastpath"}
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("nwplfullad-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)
		//creating a spec to perform add  drive
		driveSpecs, err := GetCloudDriveDeviceSpecs()
		log.FailOnError(err, "Error getting cloud drive specs")

		deviceSpec := driveSpecs[0]
		deviceSpecParams := strings.Split(deviceSpec, ",")
		var specSize uint64
		paramsArr := make([]string, 0)
		for _, param := range deviceSpecParams {
			if strings.Contains(param, "size") {
				val := strings.Split(param, "=")[1]
				specSize, err = strconv.ParseUint(val, 10, 64)
				log.FailOnError(err, "Error converting size to uint64")
				paramsArr = append(paramsArr, fmt.Sprintf("size=%d,", specSize/2))
			} else {
				paramsArr = append(paramsArr, param)
			}
		}
		newSpec := strings.Join(paramsArr, ",")
		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "error getting storage pools")
		existingPoolsCount := len(pools)

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

		defer func() {
			status, err := Inst().V.GetNodePoolsStatus(*selectedNode)
			log.FailOnError(err, fmt.Sprintf("error getting node %s pool status", selectedNode.Name))
			log.InfoD(fmt.Sprintf("Pool %s has status %s", selectedNode.Name, status[selectedPool.Uuid]))
			if status[selectedPool.Uuid] == "In Maintenance" {
				log.InfoD(fmt.Sprintf("Exiting pool maintenance mode on node %s", selectedNode.Name))
				err = Inst().V.ExitPoolMaintenance(*selectedNode)
				log.FailOnError(err, fmt.Sprintf("fail to exit pool maintenance mode ib node %s", selectedNode.Name))
			}
		}()

		log.InfoD(fmt.Sprintf("Entering pool maintenance mode on node %s", selectedNode.Name))
		err = Inst().V.EnterPoolMaintenance(*selectedNode)
		log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenance mode", selectedNode.Name))
		status, err := Inst().V.GetNodePoolsStatus(*selectedNode)
		log.FailOnError(err, fmt.Sprintf("error getting node %s pool status", selectedNode.Name))
		log.InfoD(fmt.Sprintf("pool %s status %s", selectedNode.Name, status[selectedPool.Uuid]))

		stepLog = fmt.Sprintf("expand pool %s using add-disk", selectedPool.Uuid)
		var expandedExpectedPoolSize uint64
		Step("", func() {
			expandedExpectedPoolSize = (selectedPool.TotalSize / units.GiB) * 2

			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, selectedPool.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expandedExpectedPoolSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
		})
		stepLog = fmt.Sprintf("Ensure that pool %s rebalance started and add new pool to the node %s", selectedPool.Uuid, selectedNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			t := func() (interface{}, bool, error) {
				expandedPool, err := GetStoragePoolByUUID(selectedPool.Uuid)
				if err != nil {
					return nil, true, fmt.Errorf("error getting pool by using id %s", selectedPool.Uuid)
				}

				if expandedPool == nil {
					return nil, false, fmt.Errorf("expanded pool value is nil")
				}
				if expandedPool.LastOperation != nil {
					log.Infof("Pool Resize Status : %v, Message : %s", expandedPool.LastOperation.Status, expandedPool.LastOperation.Msg)
					if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_IN_PROGRESS &&
						(strings.Contains(expandedPool.LastOperation.Msg, "Storage rebalance is running") || strings.Contains(expandedPool.LastOperation.Msg, "Rebalance in progress")) {
						return nil, false, nil
					}
					if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_FAILED {
						return nil, false, fmt.Errorf("PoolResize has failed. Error: %s", expandedPool.LastOperation)
					}

				}
				return nil, true, fmt.Errorf("pool status not updated")
			}
			_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second)
			log.FailOnError(err, "Error checking pool rebalance")

			err = Inst().V.AddCloudDrive(selectedNode, newSpec, -1)
			log.FailOnError(err, fmt.Sprintf("Add cloud drive failed on node %s", selectedNode.Name))
			err = Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "error refreshing driver end points")

			err = waitForPoolToBeResized(expandedExpectedPoolSize, selectedPool.Uuid, isjournal)
			log.FailOnError(err, fmt.Sprintf("Error waiting for poor %s resize", selectedPool.Uuid))

			status, err = Inst().V.GetNodePoolsStatus(*selectedNode)
			log.FailOnError(err, fmt.Sprintf("error getting node %s pool status", selectedNode.Name))
			log.InfoD(fmt.Sprintf("Pool %s has status %s", selectedNode.Name, status[selectedPool.Uuid]))
			if status[selectedPool.Uuid] == "In Maintenance" {
				log.InfoD(fmt.Sprintf("Exiting pool maintenance mode on node %s", selectedNode.Name))
				err = Inst().V.ExitPoolMaintenance(*selectedNode)
				log.FailOnError(err, fmt.Sprintf("fail to exit pool maintenance mode ib node %s", selectedNode.Name))
			}

			log.InfoD("Validate pool rebalance after drive add")
			err = ValidateDriveRebalance(*selectedNode)
			if err != nil {
				if strings.Contains(err.Error(), "current status: Offline") {
					poolsStatus, err := Inst().V.GetNodePoolsStatus(*selectedNode)
					log.FailOnError(err, fmt.Sprintf("error getting pool status on node %s after rebalance", selectedNode.Name))
					for k, v := range poolsStatus {
						if v == "Offline" {
							additionalPoolToExpand, err := GetStoragePoolByUUID(selectedPool.Uuid)
							log.FailOnError(err, fmt.Sprintf("error getting pool with UUID %s", k))
							additionalPoolExpectedPoolSize := (selectedPool.TotalSize / units.GiB) * 2
							log.InfoD("Current Size of the pool %s is %d", k, additionalPoolToExpand.TotalSize/units.GiB)
							err = Inst().V.ExpandPool(k, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, additionalPoolExpectedPoolSize, true)
							log.FailOnError(err, fmt.Sprintf("error initiating expansion for pool %s", k))
							err = waitForPoolToBeResized(additionalPoolExpectedPoolSize, k, isjournal)
							log.FailOnError(err, fmt.Sprintf("Error waiting for poor %s resize", k))
						}
					}
				} else {
					log.FailOnError(err, fmt.Sprintf("pool %s rebalance failed", selectedPool.Uuid))
				}
			}

			resizedPool, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("error get pool using UUID %s", selectedPool.Uuid))
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			expectedSizeWithJournal := expandedExpectedPoolSize - 3

			if newPoolSize >= expectedSizeWithJournal {
				isExpansionSuccess = true
			}
			dash.VerifyFatal(isExpansionSuccess, true, fmt.Sprintf("expected new pool size to be %v or %v, got %v", expandedExpectedPoolSize, expectedSizeWithJournal, newPoolSize))
			pools, err = Inst().V.ListStoragePools(metav1.LabelSelector{})
			log.FailOnError(err, "error getting storage pools")

			dash.VerifyFatal(len(pools), existingPoolsCount+1, "Validate new pool is created")
			nodeStatus, err := Inst().V.GetNodeStatus(*selectedNode)
			log.FailOnError(err, fmt.Sprintf("Error getting PX status of node %s", selectedNode.Name))
			dash.VerifySafely(*nodeStatus, api.Status_STATUS_OK, fmt.Sprintf("validate PX status on node %s", selectedNode.Name))
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})

func adjustReplPools(firstNode, replNode node.Node, isjournal bool) error {

	selectedNodeSize := getTotalPoolSize(firstNode)
	secondReplSize := getTotalPoolSize(replNode)
	if secondReplSize <= selectedNodeSize*3 {
		secondPool := replNode.StoragePools[0]
		maxSize := secondPool.TotalSize / units.GiB
		for _, p := range replNode.StoragePools {
			currSize := p.TotalSize / units.GiB
			if currSize > maxSize {
				maxSize = currSize
				secondPool = p
			}
		}

		expandSize := maxSize * 3
		log.InfoD("Current Size of the pool %s is %d", secondPool.Uuid, secondPool.TotalSize/units.GiB)
		if err := Inst().V.ExpandPool(secondPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expandSize, false); err != nil {
			return fmt.Errorf("pool expansion init failed for %s. Err : %v", secondPool.Uuid, err)
		}

		log.InfoD("expand pool %s using resize-disk", secondPool.Uuid)
		if err := waitForPoolToBeResized(expandSize, secondPool.Uuid, isjournal); err != nil {
			return fmt.Errorf("error waiting for poor %s resize", secondPool.Uuid)
		}
	}
	return nil
}

var _ = Describe("{StorageFullPoolResize}", func() {

	//step1: feed p1 size GB I/O on the volume
	//step2: After I/O done p1 should be offline and full, expand the pool p1 using resize-disk
	//step4: validate the pool and the data

	var testrailID = 51280
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51280
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("StorageFullPoolResize", "Feed a pool full, then expand the pool using resize-disk", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
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

			log.FailOnError(err, "Failed to check if Journal enabled")

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
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{StorageFullPoolAddDisk}", func() {

	//step1: feed p1 size GB I/O on the volume
	//step2: After I/O done p1 should be offline and full, expand the pool p1 using add-disk
	//step4: validate the pool and the data

	var testrailID = 50631
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/50631
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("StorageFullPoolAddDisk", "Feed a pool full, then expand the pool using add-disk", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context

	stepLog := "Create vols and make pool full"
	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
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
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("sfullad-%d", i))...)
		}
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

		defer func() {
			status, err := Inst().V.GetNodePoolsStatus(*selectedNode)
			log.FailOnError(err, fmt.Sprintf("error getting node %s pool status", selectedNode.Name))
			log.InfoD(fmt.Sprintf("Pool %s has status %s", selectedNode.Name, status[selectedPool.Uuid]))
			if status[selectedPool.Uuid] == "In Maintenance" {
				log.InfoD(fmt.Sprintf("Exiting pool maintenance mode on node %s", selectedNode.Name))
				err = Inst().V.ExitPoolMaintenance(*selectedNode)
				log.FailOnError(err, fmt.Sprintf("fail to exit pool maintenance mode ib node %s", selectedNode.Name))
			}
		}()

		log.InfoD(fmt.Sprintf("Entering pool maintenance mode on node %s", selectedNode.Name))
		err = Inst().V.EnterPoolMaintenance(*selectedNode)
		log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenance mode", selectedNode.Name))
		status, err := Inst().V.GetNodePoolsStatus(*selectedNode)
		log.FailOnError(err, fmt.Sprintf("error getting node %s pool status", selectedNode.Name))
		log.InfoD(fmt.Sprintf("pool %s status %s", selectedNode.Name, status[selectedPool.Uuid]))

		stepLog = fmt.Sprintf("expand pool %s using add-disk", selectedPool.Uuid)
		var expandedExpectedPoolSize uint64
		Step(stepLog, func() {
			log.InfoD(stepLog)
			expandedExpectedPoolSize = (selectedPool.TotalSize / units.GiB) * 2

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, selectedPool.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expandedExpectedPoolSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
		})
		stepLog = fmt.Sprintf("Ensure that pool %s expansion is successful", selectedPool.Uuid)
		Step(stepLog, func() {
			log.InfoD(stepLog)

			err = waitForPoolToBeResized(expandedExpectedPoolSize, selectedPool.Uuid, isjournal)
			log.FailOnError(err, "Error waiting for poor resize")
			status, err = Inst().V.GetNodePoolsStatus(*selectedNode)
			log.FailOnError(err, fmt.Sprintf("error getting node %s pool status", selectedNode.Name))
			log.InfoD(fmt.Sprintf("Pool %s has status %s", selectedNode.Name, status[selectedPool.Uuid]))
			if status[selectedPool.Uuid] == "In Maintenance" {
				log.InfoD(fmt.Sprintf("Exiting pool maintenance mode on node %s", selectedNode.Name))
				err = Inst().V.ExitPoolMaintenance(*selectedNode)
				log.FailOnError(err, fmt.Sprintf("failed to exit pool maintenance mode on node %s", selectedNode.Name))
				expectedStatus := "Online"
				err = WaitForPoolStatusToUpdate(*selectedNode, expectedStatus)
				log.FailOnError(err, fmt.Sprintf("node %s pools are not in status %s", selectedNode.Name, expectedStatus))

			}

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
		AfterEachTest(contexts, testrailID, runID)
	})

})

func waitForStorageDown(n node.Node) error {

	t := func() (interface{}, bool, error) {
		status, err := Inst().V.GetNodeStatus(n)

		if err != nil {
			return nil, true, err
		}

		if *status == api.Status_STATUS_STORAGE_DOWN || *status == api.Status_STATUS_OFFLINE {
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("node %s status is not down yet, current status: %s", n.Name, status.String())
	}
	if _, err := task.DoRetryWithTimeout(t, poolResizeTimeout, retryTimeout); err != nil {
		return err
	}

	return nil
}

var _ = Describe("{ResizeKvdbNoQuorum}", func() {
	//1) Deploy px with cloud drive.
	//2) Make KVDB out of quorum
	//3) Expand a healthy pools by resize-disk

	var testrailID = 51300
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51300
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("ResizeKvdbNoQuorum", "Initiate pool expansion by resize-disk when kvdb is out quorum", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should make kvdb out of quorum, and expand healthy pool using resize-disk"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("resiznoqr-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stoageDriverNodes := node.GetStorageNodes()

		nonKvdbNodes := make([]node.Node, 0)
		kvdbNodes := make([]node.Node, 0)
		driverDownNodes := make([]node.Node, 0)

		kvdbNodesIDs := make([]string, 0)
		kvdbMembers, err := Inst().V.GetKvdbMembers(stoageDriverNodes[0])
		log.FailOnError(err, "Error getting KVDB members")

		for _, n := range kvdbMembers {
			kvdbNodesIDs = append(kvdbNodesIDs, n.Name)
		}
		log.Infof("kvdbNodesIDs %v", kvdbNodesIDs)
		for _, n := range stoageDriverNodes {
			if Contains(kvdbNodesIDs, n.Id) {
				kvdbNodes = append(kvdbNodes, n)
			} else {
				nonKvdbNodes = append(nonKvdbNodes, n)
			}
		}

		if len(nonKvdbNodes) == 0 {
			log.FailOnError(fmt.Errorf("No non kvdb nodes found"), "non kvdb nodes doesnt not exist in the cluster")
		}

		selPool := nonKvdbNodes[0].Pools[0]
		poolToBeResized, err := GetStoragePoolByUUID(selPool.Uuid)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selPool.Uuid))

		stepLog = "Make kvdb out of quorum"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			i := 0
			for _, n := range kvdbNodes {
				if i == len(kvdbNodes)-1 {
					break
				}
				err := Inst().V.StopDriver([]node.Node{n}, false, nil)
				log.FailOnError(err, "error stopping driver on node %s", n.Name)

				err = Inst().V.WaitDriverDownOnNode(n)
				log.FailOnError(err, "error while waiting for driver down on node %s", n.Name)
				driverDownNodes = append(driverDownNodes, n)
				i += 1
			}

		})

		defer func() {
			Step("set cluster to running", func() {
				log.InfoD("set cluster to running")
				for _, n := range driverDownNodes {
					err := Inst().V.StartDriver(n)
					log.FailOnError(err, "error starting driver on node %s", n.Name)
					err = Inst().V.WaitDriverUpOnNode(n, 10*time.Minute)
					log.FailOnError(err, "error while waiting for driver up on node %s", n.Name)
				}
			})

		}()

		stepLog = fmt.Sprintf("Expanding pool on non kvdb node using resize-disk")
		Step(stepLog, func() {

			expectedSize := poolToBeResized.TotalSize * 2 / units.GiB

			log.InfoD("Current Size of the pool %s is %d", selPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err != nil, true, "Pool expansion init failed?")

			validErr := strings.Contains(err.Error(), "failed to find storage pool")
			dash.VerifyFatal(validErr, true, fmt.Sprintf("verify err is valid %s", err.Error()))

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{StoPoolExpMulPools}", func() {
	/*
		Having multiple pools and resize only one pool
	*/
	var testrailID = 51298
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51298
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("StoPoolExpMulPools", "Validate storage pool expansion using resize-disk option when multiple pools are present on the cluster", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context

	stepLog := "Has to schedule apps, and expand it by resizing a pool"
	It(stepLog, func() {
		log.InfoD(stepLog)

		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("poolexpand-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get all the storage Nodes present in the system
		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found!")
		}
		log.InfoD("All Storage Nodes present on the kubernetes cluster [%s]", stNodes)

		/* Validate if the Node with Multiple pools are available ,
		   if, any node has multiple pools present , then use that Node for expanding
		   else, Fail the test case
		*/
		var selectedNode node.Node
		isMultiPoolNode := false
		for _, selNode := range stNodes {
			log.InfoD("Validating Node [%s] for multipool configuraitons", selNode.Name)
			if len(selNode.StoragePools) > 1 {
				isMultiPoolNode = true
				selectedNode = selNode
				break
			}
		}

		dash.VerifyFatal(isMultiPoolNode, true, "Failed as Multipool configuration doesnot exists!")

		// Selecting Storage pool based on Pools present on the Node with IO running
		selectedPool, err := GetPoolWithIOsInGivenNode(selectedNode, contexts, api.SdkStoragePool_RESIZE_TYPE_AUTO, 0)
		log.FailOnError(err, "error while selecting the pool [%s]", selectedPool)

		stepLog := fmt.Sprintf("Expanding pool on node [%s] and pool UUID: [%s] using auto", selectedNode.Name, selectedPool.Uuid)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, "Failed to get pool using UUID [%s]", selectedPool.Uuid)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool [%s] on node [%s] expansion using auto", selectedPool.Uuid, selectedNode.Name))
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{CreateSnapshotsPoolResize}", func() {
	/*
		Try pool resize when a lot of snapshots are created on the volume
	*/
	var testrailID = 50652
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/50652
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("CreateSnapshotsPoolResize", "Validate storage pool expansion when lots of snapshots present on the system", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context
	totalSnapshotsPerVol := 60

	snapshotList := make(map[string][]string)
	var selectedNode node.Node

	// Try pool resize when ot of snapshots are created on the volume
	stepLog := "should get the existing storage node and expand the pool by resize-disk"
	It(stepLog, func() {

		log.InfoD(stepLog)

		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("snapcreateresizepool-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		var stNode node.Node
		var err error

		// Get List of Volumes present in the Node
		stNode, err = getRandomNodeWithPoolIOs(contexts)
		log.FailOnError(err, "error getting node having pool with IOs")

		// Selecting Storage pool based on Pools present on the Node
		selectedPool, err := GetPoolWithIOsInGivenNode(stNode, contexts, api.SdkStoragePool_RESIZE_TYPE_AUTO, 0)
		log.FailOnError(err, "error identifying pool running IO [%s]", stNode.Name)

		var selectedVol *volume.Volume
		for _, each := range contexts {
			log.InfoD("Getting context Info [%v]", each)
			Volumes, err := Inst().S.GetVolumes(each)
			log.FailOnError(err, "Listing Volumes Failed")

			log.InfoD("Get all the details of Volumes Present")
		outer:
			for _, vol := range Volumes {
				log.InfoD("List of Volumes to inspect [%T] , [%s]", vol, vol.ID)
				volInspect, err := Inst().V.InspectVolume(vol.ID)
				log.FailOnError(err, "Failed to Inpect volumes present Err : [%s]", volInspect)
				replicaNodes := volInspect.ReplicaSets[0].Nodes

				for _, nID := range replicaNodes {
					if nID == stNode.Id {
						selectedVol = vol
						break outer
					}
				}
			}
			if selectedVol != nil {
				break
			}
		}
		dash.VerifyFatal(selectedVol != nil, true, fmt.Sprintf("Identify volume for snapshots on the node [%v]", stNode.Name))

		for snap := 0; snap < totalSnapshotsPerVol; snap++ {
			uuidCreated := uuid.New()
			snapshotName := fmt.Sprintf("snapshot_%s_%s", selectedVol.ID, uuidCreated.String())
			snapshotResponse, err := Inst().V.CreateSnapshot(selectedVol.ID, snapshotName)
			log.FailOnError(err, "error identifying volume [%s]", selectedVol.ID)
			snapshotList[selectedVol.ID] = append(snapshotList[selectedVol.ID], snapshotName)
			log.InfoD("Snapshot [%s] created with ID [%s]", snapshotName, snapshotResponse.GetSnapshotId())
		}
		stepLog = fmt.Sprintf("Expanding pool on node [%s] and pool UUID: [%s] using auto", selectedNode.Name, selectedPool.Uuid)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, "Failed to get pool using UUID [%s]", selectedPool.Uuid)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool [%s] is [%d]", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool [%s] on node [%s] expansion using auto", selectedPool.Uuid, selectedNode.Name))
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

func inResync(vol string) bool {
	volDetails, err := Inst().V.InspectVolume(vol)
	if err != nil {
		log.Error("not in Resync State")
		return false
	}
	for _, v := range volDetails.RuntimeState {
		log.InfoD("RuntimeState is in state %s", v.GetRuntimeState()["RuntimeState"])
		if v.GetRuntimeState()["RuntimeState"] == "resync" ||
			v.GetRuntimeState()["RuntimeState"] == "clean" {
			return true
		}
	}
	return false
}

func WaitTillVolumeInResync(vol string) bool {
	now := time.Now()
	targetTime := now.Add(30 * time.Minute)

	for {
		if now.After(targetTime) {
			log.Error("Failed as the timeout of 0 Min is reached before resync triggered")
			return false
		} else {
			if inResync(vol) {
				return true
			}
		}
	}
}

var _ = Describe("{PoolResizeVolumesResync}", func() {
	/*
		Try pool resize when a lot of volumes are in resync state
	*/
	var testrailID = 51301
	// Testrail Corresponds : https://portworx.testrail.net/index.php?/cases/view/51301
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("PoolResizeVolumesResync", "Validate Pool resize when lots of volumes are in resync state", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context
	var volIds []string

	stepLog := "should get the existing storage node and expand the pool by resize-disk"
	It(stepLog, func() {
		log.InfoD(stepLog)

		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("snapcreateresizepool-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		time.Sleep(5 * time.Second)
		for _, each := range contexts {
			Volumes, err := Inst().S.GetVolumes(each)
			log.FailOnError(err, "Failed while listing the volume with error")

			// Appending all the volume IDs to array so that one random volume can be picked for resizeing
			for _, vol := range Volumes {
				volIds = append(volIds, vol.ID)
			}

			// Select Random Volumes for pool Expand
			randomIndex := rand.Intn(len(volIds))
			randomVolIDs := volIds[randomIndex]

			// From each volume pick the random pool and restart pxdriver
			poolUUIDs, err := GetPoolIDsFromVolName(randomVolIDs)
			log.InfoD("List of pool IDs %v", poolUUIDs)
			log.FailOnError(err, "Failed to get Pool IDs from the volume [%s]", poolUUIDs)

			// Select the random pools from UUIDs for PxDriver Restart
			randomIndex = rand.Intn(len(poolUUIDs))
			rebootPoolID := poolUUIDs[randomIndex]

			// Rebooting Node
			log.InfoD("Get the Node for Restart %v", rebootPoolID)
			restartDriver, err := GetNodeWithGivenPoolID(rebootPoolID)
			log.FailOnError(err, "Geting Node Driver for restart failed")

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			poolToBeResized, err := GetStoragePoolByUUID(rebootPoolID)
			log.InfoD("Pool to be resized %v", poolToBeResized)
			log.FailOnError(err, "Failed to get pool using UUID [%s]", rebootPoolID)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			log.InfoD("setting replication on the volumes")
			setRepl := func(vol *volume.Volume) error {
				log.InfoD("setting replication factor of the volume [%v] with ID [%v]", vol.Name, vol.ID)
				currRepFactor, err := Inst().V.GetReplicationFactor(vol)
				log.FailOnError(err, "Failed to get replication factor on the volume")
				log.Infof("Replication factor on the volume [%v] is [%v]", vol.Name, currRepFactor)
				opts := volume.Options{
					ValidateReplicationUpdateTimeout: replicationUpdateTimeout,
				}
				if currRepFactor == 3 {
					newRepl := currRepFactor - 1
					err = Inst().V.SetReplicationFactor(vol, newRepl, nil, nil, true, opts)
					if err != nil {
						return err
					}
				}
				// Change Replica sets of each volumes created to 3
				var (
					maxReplicaFactor int64
					nodesToBeUpdated []string
					poolsToBeUpdated []string
				)
				maxReplicaFactor = 3
				nodesToBeUpdated = nil
				poolsToBeUpdated = nil
				err = Inst().V.SetReplicationFactor(vol, maxReplicaFactor,
					nodesToBeUpdated, poolsToBeUpdated, true, opts)
				if err != nil {
					return err
				}

				return nil
			}

			// Set replicaiton on all volumes in parallel so that multiple volumes will be in resync
			var wg sync.WaitGroup
			var m sync.Mutex
			error_array := []error{}
			for _, eachVol := range Volumes {
				log.InfoD("Set replication on the volume [%v]", eachVol.ID)
				wg.Add(1)
				go func(eachVol *volume.Volume) {
					defer wg.Done()
					err := setRepl(eachVol)
					if err != nil {
						m.Lock()
						error_array = append(error_array, err)
						m.Unlock()
					}
				}(eachVol)
			}
			wg.Wait()
			dash.VerifyFatal(len(error_array) == 0, true, fmt.Sprintf("errored while setting replication on volumes [%v]", error_array))

			log.InfoD("Waiting till Volume is In Resync Mode ")
			if WaitTillVolumeInResync(randomVolIDs) == false {
				log.InfoD("Failed to get Volume in Resync state [%s]", randomVolIDs)
			}

			log.InfoD("Current Size of the pool %s is %d", rebootPoolID, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(rebootPoolID, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, rebootPoolID, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool [%s] on node [%s] expansion using auto", rebootPoolID, restartDriver.Name))
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{PoolIncreaseSize20TB}", func() {
	/*
		Resize a pool of capacity of 100GB to 20TB
	*/
	var testrailID = 51292
	// Testrail Corresponds : https://portworx.testrail.net/index.php?/cases/view/51292
	var runID int

	var (
		contexts = make([]*scheduler.Context, 0)
	)

	JustBeforeEach(func() {
		StartTorpedoTest("PoolIncreaseSize20TB", "Resize a pool of capacity of 100GB to 20TB", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	//var vol_ids []string
	stepLog := "should get the existing storage node and expand the pool by resize-disk"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("snapcreateresizepool-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, "Storage pools exist?")

		// pick a pool from a pools list and resize it
		poolIDToResize = pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 0)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize [%s]", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if val, err := poolResizeIsInProgress(poolToBeResized); val {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			} else {
				log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		// Marking the expected size to be 20TB
		expectedSize = (2048 * 1024 * 1024 * 1024 * 1024 * 1024) / units.TiB
		if IsEksCluster() {
			expectedSize = (15 * 1024 * 1024 * 1024 * 1024) / units.TiB
		}

		stepLog = "Calculate expected pool size and trigger pool resize"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check is Journal enabled")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}
			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Expected new size to be [%d] or [%d] if pool has journal", expectedSize, expectedSizeWithJournal))
		})

		stepLog = "Ensure that new pool has been expanded to the expected size"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			ValidateApplications(contexts)

			resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
			log.FailOnError(err, "Failed to get pool using UUID [%s]", poolIDToResize)
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			if newPoolSize >= expectedSizeWithJournal {
				isExpansionSuccess = true
			}
			dash.VerifyFatal(isExpansionSuccess, true,
				fmt.Sprintf("expected new pool size to be [%v] or [%v] if pool has journal, got [%v]", expectedSize, expectedSizeWithJournal, newPoolSize))
		})

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})

func addDiskToSpecificPool(node node.Node, sizeOfDisk uint64, poolID int32) bool {
	// Get the Spec to add the disk to the Node
	//  if the diskSize ( sizeOfDisK ) is 0 , then Disk of default spec size will be picked
	driveSpecs, err := GetCloudDriveDeviceSpecs()
	log.FailOnError(err, "Error getting cloud drive specs")
	log.InfoD("Cloud Drive Spec %s", driveSpecs)

	// Update the device spec to update the disk size
	deviceSpec := driveSpecs[0]
	deviceSpecParams := strings.Split(deviceSpec, ",")
	paramsArr := make([]string, 0)
	for _, param := range deviceSpecParams {
		if strings.Contains(param, "size") {
			if sizeOfDisk == 0 {
				var specSize uint64
				val := strings.Split(param, "=")[1]
				specSize, err = strconv.ParseUint(val, 10, 64)
				log.FailOnError(err, "Error converting size [%v] to uint64", val)
				paramsArr = append(paramsArr, fmt.Sprintf("size=%d,", specSize))
			} else {
				paramsArr = append(paramsArr, fmt.Sprintf("size=%d", sizeOfDisk))
			}
		} else {
			paramsArr = append(paramsArr, param)
		}
	}
	newSpec := strings.Join(paramsArr, ",")
	log.InfoD("New Spec Details %v", newSpec)

	// Add Drive to the Volume
	err = Inst().V.AddCloudDrive(&node, newSpec, poolID)
	if err != nil {
		// Regex to check if the error message is reported
		re := regexp.MustCompile(`Drive not compatible with specified pool.*`)
		if re.MatchString(fmt.Sprintf("%v", err)) {
			log.InfoD("Error while adding Disk %v", err)
			return false
		}
	}
	err = Inst().V.RefreshDriverEndpoints()
	log.FailOnError(err, "error refreshing driver end points")
	return true
}

var _ = Describe("{ResizePoolDrivesInDifferentSize}", func() {
	/*
		Resizing the pool should fail when drives in the pool have been resized to different size
	*/
	var testrailID = 51320
	// Testrail Corresponds : https://portworx.testrail.net/index.php?/cases/view/51320
	var runID int

	var (
		contexts = make([]*scheduler.Context, 0)
	)

	JustBeforeEach(func() {
		StartTorpedoTest("ResizePoolDrivesInDifferentSize",
			"Resizing the pool should fail when drives in the pool have been resized to different size",
			nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "should get the existing storage node and expand the pool by resize-disk"
	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("resizepooldrivesdiffsize-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Select a Pool with IO Runing poolID returns UUID ( String )
		var poolID int32

		// Add disk to the Node
		var diskSize uint64
		minDiskSize := 50
		maxDiskSize := 150
		size := rand.Intn(maxDiskSize-minDiskSize) + minDiskSize
		diskSize = (uint64(size) * 1024 * 1024 * 1024) / units.GiB

		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, diskSize)

		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		allPools, _ := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.InfoD("List of all the Pools present in the system [%s]", allPools)

		// Get Pool ID of pool selected for Resize
		for uuid, each := range allPools {
			if uuid == poolUUID {
				poolID = each.ID
				break
			}

		}
		log.InfoD("Getting Pool with ID [%v] and UUID [%v] for Drive Addition", poolID, poolUUID)

		// Get the Node from the PoolID (nodeDetails returns node.Node)
		nodeDetails, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Getting NodeID from the given poolUUID [%v] Failed", poolUUID)
		log.InfoD("Node Details %v", nodeDetails)

		log.InfoD("Adding New Disk with Size [%v]", diskSize)
		response := addDiskToSpecificPool(*nodeDetails, diskSize, poolID)
		dash.VerifyFatal(response, false,
			fmt.Sprintf("Pool expansion with Disk Resize with Disk size [%v GiB] Succeeded?", diskSize))

		log.InfoD("Attempt Adding Disk with size same as pool size")
		response = addDiskToSpecificPool(*nodeDetails, 0, poolID)
		dash.VerifyFatal(response, true,
			fmt.Sprintf("Pool expansion with Disk size same as pool size [%v GiB] Succeeded?", diskSize))
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})
var _ = Describe("{PoolDelete}", func() {
	/*
		1) Place pool on maintenance mode
		2) Delete the pool
		3) Add new pool
		4) expand newly added pool
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("PoolDelete", "Initiate pool deletion", nil, 0)

	})
	var contexts []*scheduler.Context

	stepLog := "Initiate pool delete, then add a new pool and expand the pool"

	It(stepLog, func() {
		log.InfoD(stepLog)

		stNodes := node.GetStorageNodes()
		var nodeSelected node.Node
		var nodePools []node.StoragePool

		randomIndex := rand.Intn(len(stNodes))
		nodeSelected = stNodes[randomIndex]
		nodePools = nodeSelected.StoragePools

		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "Failed to check if Journal enabled")
		var jrnlPartPoolID string

		if isjournal && len(nodePools) > 1 {

			jDev, err := Inst().V.GetJournalDevicePath(&nodeSelected)
			log.FailOnError(err, fmt.Sprintf("error getting journal device path from node %s", nodeSelected.Name))

			log.Infof("JournalDev: %s", jDev)
			if jDev == "" {
				log.FailOnError(fmt.Errorf("no journal device path found"), "error getting journal device path from storage spec")
			}

			drivesMap, err := Inst().V.GetPoolDrives(&nodeSelected)
			jPath := jDev[:len(jDev)-1]
		outer:
			for k, v := range drivesMap {
				for _, dv := range v {
					if strings.Contains(dv.Device, jPath) {
						jrnlPartPoolID = k
						break outer
					}
				}

			}
			if jrnlPartPoolID != "" {
				err = DeleteGivenPoolInNode(nodeSelected, jrnlPartPoolID, false)
				isValidError := strings.Contains(err.Error(), "pool with autojournal partition cannot be deleted when there are multiple pools")
				dash.VerifyFatal(isValidError, true, fmt.Sprintf("pool %s deletion failed with err : %s", jrnlPartPoolID, err.Error()))
			} else {
				log.Infof("No pool is partitioned with journal device")
			}

		}

		var poolToDelete node.StoragePool
		for _, pl := range nodePools {
			if strconv.Itoa(int(pl.ID)) != jrnlPartPoolID {
				poolToDelete = pl
				break
			}
		}
		poolIDToDelete := fmt.Sprintf("%d", poolToDelete.ID)
		poolsMap, err := Inst().V.GetPoolDrives(&nodeSelected)
		log.FailOnError(err, "error getting pool drive from the node [%s]", nodeSelected.Name)
		poolsCount := len(poolsMap)
		if _, ok := poolsMap[poolIDToDelete]; !ok {
			log.FailOnError(fmt.Errorf("error idetifying pool drive"), "poolID %s not found in the node %s", poolIDToDelete, nodeSelected.Name)
		}
		poolsBfr, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")

		deletePoolAndValidate(nodeSelected, poolIDToDelete)

		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("resiznoqr-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		newSpecSize := (poolToDelete.TotalSize / units.GiB) / 2
		///creating a spec to perform add  drive
		driveSpecs, err := GetCloudDriveDeviceSpecs()
		log.FailOnError(err, "Error getting cloud drive specs")

		deviceSpec := driveSpecs[0]
		deviceSpecParams := strings.Split(deviceSpec, ",")

		paramsArr := make([]string, 0)
		for _, param := range deviceSpecParams {
			if strings.Contains(param, "size") {
				paramsArr = append(paramsArr, fmt.Sprintf("size=%d,", newSpecSize))
			} else {
				paramsArr = append(paramsArr, param)
			}
		}
		newSpec := strings.Join(paramsArr, ",")
		stepLog = fmt.Sprintf("Adding cloud drive to node %s with size %s", nodeSelected.Name, newSpec)

		Step(stepLog, func() {
			log.InfoD(stepLog)
			err = Inst().V.AddCloudDrive(&nodeSelected, newSpec, -1)
			log.FailOnError(err, "error adding new drive to node %s", nodeSelected.Name)
			err = Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "error refreshing driver end points")
			log.InfoD("Validate pool rebalance after drive add to the node %s", nodeSelected.Name)
			err = ValidateDriveRebalance(nodeSelected)
			log.FailOnError(err, "pool re-balance failed on node %s", nodeSelected.Name)
			err = Inst().V.WaitDriverUpOnNode(nodeSelected, addDriveUpTimeOut)
			log.FailOnError(err, "volume drive down on node %s", nodeSelected.Name)

			poolsAfr, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
			log.FailOnError(err, "Failed to list storage pools")
			dash.VerifyFatal(len(poolsBfr) == len(poolsAfr), true, "verify new pool is created")
			newPoolsMap, err := Inst().V.GetPoolDrives(&nodeSelected)
			log.FailOnError(err, "error getting pool drive from the node [%s]", nodeSelected.Name)
			dash.VerifyFatal(poolsCount == len(newPoolsMap), true, "verify new drive is created")
		})
		stepLog = fmt.Sprintf("Expand newly added pool on node [%s]", nodeSelected.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolsAfr, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
			log.FailOnError(err, "Failed to list storage pools")
			var poolIDSelected string
			for k := range poolsAfr {
				if _, ok := poolsBfr[k]; !ok {
					poolIDSelected = k
					break
				}
			}
			poolToBeResized, err := GetStoragePoolByUUID(poolIDSelected)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDSelected))
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + 100

			log.InfoD("Current Size of the pool %s is %d", poolIDSelected, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolIDSelected, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize, false)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, poolIDSelected, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on expansion using auto option", poolIDSelected))
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

func appsValidateAndDestroy(contexts []*scheduler.Context) {
	opts := make(map[string]bool)
	opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

	Step("validate apps", func() {
		log.InfoD("Validating apps")
		for _, ctx := range contexts {
			ctx.ReadinessTimeout = 15 * time.Minute
			ValidateContext(ctx)
		}
	})

	Step("destroy apps", func() {
		log.InfoD("Destroying apps")
		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
	})
}

var _ = Describe("{VolDeletePoolExpand}", func() {
	/*
		1) Deploy px with cloud drive.
		2) Create a large volume on that pool and write 200G on the volume.
		3) Update the label for the pool before expand
		4) perform volume delete
		5) Expand by resize the pool when delete is in progress
		6) Check the alert for the pool expand
		7) check the labels after pool expand
	*/
	var testrailID = 51285
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/tests/view/51285

	var runID int
	JustBeforeEach(func() {

		StartTorpedoTest("VolDeletePoolExpand", "Delete volume which has ~200G data and do an expansion of pool by resize", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)

	})
	var contexts []*scheduler.Context
	var newContexts []*scheduler.Context

	stepLog := "should get the existing storage node and write ~200G data to a volume"

	It(stepLog, func() {

		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		existingAppList := Inst().AppList

		defer func() {
			Inst().AppList = existingAppList
		}()

		Inst().AppList = []string{"bonnie-sharedv4"}

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("voldeletepoolexpand-%d", i))...)
		}

		ValidateApplications(contexts)

		log.Infof("Need to check if volume is close to 200G occupied")
		vol, err := GetVolumeWithMinimumSize(contexts, 200)

		dash.VerifyFatal(err, nil, "Checking if the desired volume is obtained")
		volID := vol.ID
		volName := vol.Name

		log.Infof("The volume that is having size used around 200G is %s with name %s", volID, volName)

		var poolIDToResize string
		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, " Storage pools exist?")

		// Pick a pool from a pools list and resize it
		appVol, err := Inst().V.InspectVolume(volID)
		dash.VerifyFatal(err, nil, fmt.Sprintf("Checking if the Volume inspect is success for the desired volume %s", volID))
		// Get the pool UUID on which the volume which is ~190G exist
		poolIDToResize = appVol.ReplicaSets[0].PoolUuids[0]

		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))
		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if val, err := poolResizeIsInProgress(poolToBeResized); val {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			} else {
				log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolIDToResize, err))
			}
		})
		stepLog = "set pool label, before pool expand"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolLabelToUpdate := make(map[string]string)
			poolLabelToUpdate["cust-type"] = "test-label"
			storageNode, err := GetNodeWithGivenPoolID(poolIDToResize)
			log.FailOnError(err, "Failed to get the storagenode using pool UUID %s", poolIDToResize)
			// Update the pool label
			err = Inst().V.UpdatePoolLabels(*storageNode, poolIDToResize, poolLabelToUpdate)
			log.FailOnError(err, "Failed to update the label on the pool %s", poolIDToResize)
			// store the new label that is updated
		})

		// Let the expansion complete
		var expectedSize uint64
		var expectedSizeWithJournal uint64
		var contextToDel *scheduler.Context

		labelBeforeExpand := poolToBeResized.Labels
		stepLog = "Calculate expected pool size and trigger pool resize"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize = (poolToBeResized.TotalSize / units.GiB) + drvSize
			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")
			// To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}
			log.InfoD("Current Size of the pool %s is %d", poolIDToResize, poolToBeResized.TotalSize/units.GiB)
			// Delete the Volume that was ~190G before the pool expand begins
			// Iterate through the contexts, get the volumes and then get the matching ID
		gotContext:
			for _, l := range contexts {
				vols, err := Inst().S.GetVolumes(l)
				dash.VerifyFatal(err, nil, "Verify if able to get the app for the volume that is filled approx 200G")
				for _, vol := range vols {
					if vol.ID == volID {
						contextToDel = l
						break gotContext
					}
				}
			}
			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, false)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			// Destroy the context
			err = Inst().S.Destroy(contextToDel, nil)
			dash.VerifyFatal(err, nil, "Verify the successful delete context of the volume which had ~190 G usage")
			log.InfoD("Going to delete the volume, by deletion of Namespace")
			TearDownContext(contextToDel, map[string]bool{
				SkipClusterScopedObjects:                    false,
				scheduler.OptionsWaitForResourceLeakCleanup: true,
				scheduler.OptionsWaitForDestroy:             true,
			})
			dash.VerifyFatal(err, nil, "Verify the successful delete of the volume which had ~190 G usage")
			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Expected new size to be '%d' or '%d'", expectedSize, expectedSizeWithJournal))
		})
		// Make sure to remove the deleted context and validate the other apps
		for _, l := range contexts {
			if l.App.Key != contextToDel.App.Key {
				newContexts = append(newContexts, l)
			}
		}
		stepLog = "Ensure that new pool has been expanded to the expected size and also check the pool expand alert"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			ValidateApplications(newContexts)
			resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
			log.FailOnError(err, fmt.Sprintf(" Failed to get pool using UUID %s", poolIDToResize))
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			if newPoolSize == expectedSize || newPoolSize >= expectedSizeWithJournal {
				isExpansionSuccess = true
			}
			dash.VerifyFatal(isExpansionSuccess, true, fmt.Sprintf("Expected new pool size to be %v or %v, got %v", expectedSize, expectedSizeWithJournal, newPoolSize))
			log.Infof("Check the alert for pool expand for pool uuid %s", poolIDToResize)
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
			var alertExist bool
			alertExist = false
			for _, l := range outLines {
				line := strings.Trim(l, " ")
				if strings.Contains(line, "PoolExpandSuccessful") && strings.Contains(line, poolIDToResize) {
					if strings.Contains(line, fmt.Sprintf("%d", expectedSize)) || strings.Contains(line, fmt.Sprintf("%d", expectedSizeWithJournal)) {
						alertExist = true
						log.Infof("The Alert generated is %s", line)
						break
					}
				}
			}
			dash.VerifyFatal(alertExist, true, "Verify Alert is Present")
		})
		stepLog = "Ensure Label is not changed after expand"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			ValidateApplications(newContexts)
			labelAfterExpand := poolToBeResized.Labels
			result := reflect.DeepEqual(labelBeforeExpand, labelAfterExpand)
			dash.VerifyFatal(result, true, "Check if labels changed after pool expand")
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(newContexts, testrailID, runID)
	})
})

var _ = Describe("{PoolResizeSameSize}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PoolResizeSameSize", "Validate storage pool expansion using resize-disk with same size should fail", nil, 0)
	})

	var contexts []*scheduler.Context
	stepLog := "add multiple pools and do resize on a pool with same size"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("plrszsame-%d", i))...)
		}

		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		err := Inst().V.RefreshDriverEndpoints()
		log.FailOnError(err, "error refreshing end points")

		stNode, err := getRandomNodeWithPoolIOs(contexts)
		log.FailOnError(err, "err getting node with IOs running")

		if len(stNode.StoragePools) < 3 {
			poolsToAdd := 3 - len(stNode.StoragePools)

			stepLog = fmt.Sprintf("Adding %d new pools to the node %s", poolsToAdd, stNode.Name)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				err = addNewPools(stNode, poolsToAdd)
				log.FailOnError(err, "error adding new pool on node [%s]", stNode.Name)
			})
		}

		err = Inst().V.RefreshDriverEndpoints()
		log.FailOnError(err, "error refreshing end points")
		stNodes := node.GetStorageNodes()
		for _, n := range stNodes {
			if n.Name == stNode.Name {
				stNode = n
				break
			}
		}

		selectedNodePool := stNode.StoragePools[0]
		minSize := selectedNodePool.TotalSize / units.GiB
		for _, p := range stNode.StoragePools {
			currSize := p.TotalSize / units.GiB
			if currSize < minSize {
				minSize = currSize
				selectedNodePool = p
			}
		}

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, " Storage pools exist?")

		poolToBeResized := pools[selectedNodePool.Uuid]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if val, err := poolResizeIsInProgress(poolToBeResized); val {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(selectedNodePool.Uuid)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedNodePool.Uuid))
			} else {
				log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", selectedNodePool.Uuid, err))
			}
		})

		var expectedSize uint64

		stepLog = "trigger pool resize with the same size"
		Step(stepLog, func() {
			expectedSize = (poolToBeResized.TotalSize / units.GiB) + 2

			log.InfoD("Current Size of the pool %s is %d", selectedNodePool.Uuid, poolToBeResized.TotalSize/units.GiB)

			// expand pool should error when trying to expand pool of 2 GiB size when minimum expansion size is 4.0 GiB
			err = Inst().V.ExpandPool(selectedNodePool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, false)
			dash.VerifyFatal(err != nil, true,
				fmt.Sprintf("verify pool expansion using resize-disk with same size failed on pool [%s] in node [%s]",
					selectedNodePool.Uuid, stNode.Name))

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

func addNewPools(n node.Node, numPools int) error {

	if numPools == 0 {
		return nil
	}

	i := 0
	pools := n.StoragePools

	var currentSize uint64
	currentSize = 54975581388800 / units.GiB

	for _, pool := range pools {
		poolSize := pool.TotalSize / units.GiB
		if poolSize < currentSize {
			currentSize = poolSize
		}
	}
	driveSpecs, err := GetCloudDriveDeviceSpecs()
	log.FailOnError(err, "Error getting cloud drive specs")
	deviceSpec := driveSpecs[0]
	deviceSpecParams := strings.Split(deviceSpec, ",")
	paramsArr := make([]string, 0)
	for _, param := range deviceSpecParams {
		if !strings.Contains(param, "size") {
			paramsArr = append(paramsArr, param)
		}
	}

	for i < numPools {
		newParams := make([]string, 0)
		newParams = append(newParams, paramsArr...)
		newSize := currentSize + 4
		currentSize = newSize
		newParams = append(newParams, fmt.Sprintf("size=%d,", newSize))
		newSpec := strings.Join(newParams, ",")

		if err := Inst().V.AddCloudDrive(&n, newSpec, -1); err != nil {
			return fmt.Errorf("add cloud drive failed on node %s, err: %v", n.Name, err)
		}
		err = Inst().V.RefreshDriverEndpoints()
		log.FailOnError(err, "error refreshing driver end points")

		log.InfoD("Validate pool rebalance after drive add on node %s", n.Name)
		if err = ValidateDriveRebalance(n); err != nil {
			return fmt.Errorf("pool re-balance failed on node %s, err: %v", n.Name, err)
		}

		if err = Inst().V.WaitDriverUpOnNode(n, addDriveUpTimeOut); err != nil {
			return fmt.Errorf("volume driver is down on node %s, err: %v", n.Name, err)
		}
		i += 1
	}
	return nil
}

func getPoolDiskSize(poolToBeResized *api.StoragePool) (uint64, error) {

	var driveSize uint64
	systemOpts := node.SystemctlOpts{
		ConnectionOpts: node.ConnectionOpts{
			Timeout:         2 * time.Minute,
			TimeBeforeRetry: defaultRetryInterval,
		},
		Action: "start",
	}

	stNode, err := GetNodeWithGivenPoolID(poolToBeResized.Uuid)
	if err != nil {
		return driveSize, err
	}

	drivesMap, err := Inst().N.GetBlockDrives(*stNode, systemOpts)
	if err != nil {
		return driveSize, fmt.Errorf("error getting block drives from node %s, Err :%v", stNode.Name, err)
	}

	var drvSize string
outer:
	for _, drv := range drivesMap {
		labels := drv.Labels
		for k, v := range labels {
			if k == "pxpool" && v == fmt.Sprintf("%d", poolToBeResized.ID) {
				drvSize = drv.Size
				sizeString := []string{"G", "T"}
				indexChecked := false
				for _, eachString := range sizeString {
					i := strings.Index(drvSize, eachString)
					if i != -1 {
						indexChecked = true
						if eachString == "T" {
							num, err := strconv.ParseFloat(drvSize[:i], 64)
							if err != nil {
								return 0, fmt.Errorf("converting string to int failed for value [%v]", drv)
							}
							drvSize = strconv.FormatFloat(num*1000, 'f', 0, 64)
						} else {
							drvSize = drvSize[:i]
						}
					}
					if indexChecked {
						break outer
					}
				}
				return 0, fmt.Errorf("unable to determine drive size with info [%v]", drv)
			}
		}
	}

	driveSize, err = strconv.ParseUint(drvSize, 10, 64)

	if err != nil {
		return driveSize, err
	}
	return driveSize, nil

}

var _ = Describe("{ChangedIOPriorityPersistPoolExpand}", func() {
	var testrailID = 79487
	// Testrail Description : Changed pool IO_priority should persist post pool expand
	// Testrail Corresponds : https://portworx.testrail.net/index.php?/cases/view/79487
	var runID int

	var (
		contexts = make([]*scheduler.Context, 0)
	)

	JustBeforeEach(func() {
		StartTorpedoTest("ChangedIOPriorityPersistPoolExpand",
			"Changed pool IO_priority should persist post pool expand",
			nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "Changed pool IO_priority should persist post pool expand"
	It(stepLog, func() {
		log.InfoD(stepLog)

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("chgpriopoolex-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get the Pool UUID on which IO is running
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_AUTO, 0)

		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		// Get IO Priority of Pool before running the test
		ioPriorityBefore, err := Inst().V.GetPoolLabelValue(poolUUID, "iopriority")
		log.FailOnError(err, "Failed to get IO Priority for Pool with UUID [%v]", poolUUID)
		log.InfoD("IO Priority of Pool [%s] before Pool expand is [%s]", poolUUID, ioPriorityBefore)

		// Change IO Priority of the Pool
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details using PoolUUID [%v]", poolUUID)

		log.InfoD("Bring Node to Maintenance Mode")
		log.FailOnError(Inst().V.EnterMaintenance(*nodeDetail), fmt.Sprintf("Failed to bring Pool [%s] to Mainteinance Mode on Node [%s]", poolUUID, nodeDetail.Name))

		// Wait for some time before verifying Maintenance state
		time.Sleep(2 * time.Minute)

		// Set IO Priority on the Pool
		var ioPriorities = []string{"low", "medium", "high"}
		var setIOPriority string

		// Selecting Pool IO Priority Value different that the one already set
		for _, eachIOPriority := range ioPriorities {
			if eachIOPriority != strings.ToLower(ioPriorityBefore) {
				setIOPriority = eachIOPriority
				break
			}
		}

		log.InfoD("Setting Pool [%s] with IO Priority [%s]", poolUUID, setIOPriority)
		log.FailOnError(Inst().V.UpdatePoolIOPriority(*nodeDetail, poolUUID, setIOPriority), fmt.Sprintf("Failed to set IO Priority of Pool [%s]", poolUUID))

		log.InfoD("Bring Node out of Maintenance Mode")
		log.FailOnError(ExitFromMaintenanceMode(*nodeDetail), fmt.Sprintf("Failed to bring up node [%v] back from maintenance mode", nodeDetail.Name))

		// Do Pool Expand on the Node
		stepLog = fmt.Sprintf("Expanding pool on node [%s] and pool UUID: [%s] using auto", nodeDetail.Name, poolUUID)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolToBeResized, err := GetStoragePoolByUUID(poolUUID)
			log.FailOnError(err, "Failed to get pool using UUID [%s]", poolUUID)

			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool [%s] is [%d]", poolUUID, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolUUID, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, poolUUID, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool [%s] on node [%s] expansion using auto", poolUUID, nodeDetail.Name))
		})

		// Validate if PoolIO Priority is not changed after pool Expansion
		ioPriorityAfter, err := Inst().V.GetPoolLabelValue(poolUUID, "iopriority")
		log.FailOnError(err, "Failed to get IO Priority for Pool with UUID [%v]", poolUUID)

		log.InfoD(fmt.Sprintf("Priority Before [%s] was set to [%s] and Priority after Pool Expansion [%s]", ioPriorityBefore, setIOPriority, ioPriorityAfter))
		dash.VerifyFatal(strings.ToLower(setIOPriority) == strings.ToLower(ioPriorityAfter), true, "IO Priority mismatch after pool expansion")

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Exit from Maintenance mode if Pool is still in Maintenance")
		log.FailOnError(ExitNodesFromMaintenanceMode(), "exit from maintenance mode failed?")
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{VerifyPoolDeleteInvalidPoolID}", func() {
	var testrailID = 79487
	// Testrail Description : Verify deletion of invalid pool ids
	// Testrail Corresponds : https://portworx.testrail.net/index.php?/cases/view/55349

	// Testrail Corresponds : https://portworx.testrail.net/index.php?/cases/view/55330
	// Testrail Description : Delete pool when PX/Pool (2.6.0+) is not in maintenance mode and verify the error message

	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("VerifyPoolDeleteInvalidPoolID",
			"Verify deletion of invalid pool ids",
			nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "Verify deletion of invalid pool ids"
	It(stepLog, func() {
		log.InfoD(stepLog)

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("deleteinvalidpoolid-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		kvdbNodes, err := GetAllKvdbNodes()
		log.FailOnError(err, "failed to get kvdb nodes")

		kvdbNodeIDs := make([]string, 0)
		for _, kvdbNode := range kvdbNodes {
			poolIDs, err := GetAllPoolsOnNode(kvdbNode.ID)
			log.FailOnError(err, "failed to get all pools on node [%s]", kvdbNode.ID)
			if len(poolIDs) < 2 {
				log.Infof("Excluding KVDB node [%s] as it has less than 2 pools", kvdbNode.ID)
				kvdbNodeIDs = append(kvdbNodeIDs, kvdbNode.ID)
			}
		}

		// Get the Pool UUID on which IO is running
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_AUTO, 0, kvdbNodeIDs...)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)

		PoolDetail, err := GetPoolsDetailsOnNode(nodeDetail)
		log.FailOnError(err, "Fetching all pool details from the node [%v] failed ", nodeDetail.Name)

		if IsLocalCluster(*nodeDetail) == true || IsIksCluster() == true {
			// Delete Pool without entering Maintenance Mode [ PTX-15157 ]
			err = Inst().V.DeletePool(*nodeDetail, "0", true)
			dash.VerifyFatal(err == nil, false, fmt.Sprintf("Expected Failure as pool not in maintenance mode : Node Detail [%v]", nodeDetail.Name))

		}

		re := regexp.MustCompile("Requires pool maintenance mode")
		if re.MatchString(fmt.Sprintf("%v", err)) == true {
			err = nil
		}
		log.FailOnError(err, "pool delete successful?")

		// invalidPoolID is total Pools present on the node + 1
		invalidPoolID := fmt.Sprintf("%d", len(PoolDetail)+1)

		// Enter maintenance mode before deleting the pools from the cluster
		log.InfoD("Setting pools to maintenance on node [%s]", nodeDetail.Name)
		log.FailOnError(Inst().V.EnterPoolMaintenance(*nodeDetail),
			"failed to set pool maintenance mode on node [%s]", nodeDetail.Name)

		// Wait for some time before verifying Maintenance state
		time.Sleep(2 * time.Minute)
		expectedStatus := "In Maintenance"
		log.FailOnError(WaitForPoolStatusToUpdate(*nodeDetail, expectedStatus),
			fmt.Sprintf("node %s pools are not in status %s", nodeDetail.Name, expectedStatus))

		// Delete the Pool with Invalid Pool ID
		err = Inst().V.DeletePool(*nodeDetail, invalidPoolID, false)
		dash.VerifyFatal(err != nil, true,
			fmt.Sprintf("Expected Failure? : Node Detail [%v]", nodeDetail.Name))
		log.InfoD("Deleting Pool with InvalidID Errored as expected [%v]", err)

		// Exit pool maintenance and see if px becomes operational
		err = Inst().V.ExitPoolMaintenance(*nodeDetail)
		log.FailOnError(err, "failed to exit pool maintenance mode on node %s", nodeDetail.Name)

		nodeDetail, err = GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)

		err = Inst().V.WaitDriverUpOnNode(*nodeDetail, addDriveUpTimeOut)
		log.FailOnError(err, "volume driver down on node %s", nodeDetail.Name)

		expectedStatus = "Online"
		err = WaitForPoolStatusToUpdate(*nodeDetail, expectedStatus)
		log.FailOnError(err, fmt.Sprintf("node %s pools are not in status %s", nodeDetail.Name, expectedStatus))

		// Verify Alerts generated after Pool Expansion [PWX-28484]
		var severityType = []api.SeverityType{api.SeverityType_SEVERITY_TYPE_ALARM,
			api.SeverityType_SEVERITY_TYPE_NOTIFY,
			api.SeverityType_SEVERITY_TYPE_WARNING}
		for _, eachAlert := range severityType {
			alerts, err := Inst().V.GetAlertsUsingResourceTypeBySeverity(api.ResourceType_RESOURCE_TYPE_POOL,
				eachAlert)
			log.Infof("alerts generated: %v for resource type: %v", alerts, eachAlert)
			if err != nil && strings.Contains(err.Error(), "EOF") == true {
				log.Infof("Alerts not generated for severity type [%v] of resource Type [%v]", eachAlert, "Pool")
				continue
			}
			log.FailOnError(err, "Failed to fetch alerts using severity type [%v] of resource Type [%v]",
				eachAlert,
				api.ResourceType_RESOURCE_TYPE_POOL)

			dash.VerifyFatal(len(alerts.Alerts) > 0,
				true,
				fmt.Sprintf("did alert generated for resource type [%v] and severity [%v]?",
					api.ResourceType_RESOURCE_TYPE_POOL,
					eachAlert))
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Exit from Maintenance mode if Pool is still in Maintenance")
		log.FailOnError(ExitNodesFromMaintenanceMode(), "exit from maintenance mode failed?")
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{PoolResizeInvalidPoolID}", func() {
	var testrailID = 79487
	// Testrail Description : Resize with invalid pool ID
	// Testrail Corresponds : https://portworx.testrail.net/index.php?/cases/view/84470
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("PoolResizeInvalidPoolID",
			"Resize with invalid pool ID",
			nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "Resize with invalid pool ID"
	It(stepLog, func() {
		log.InfoD(stepLog)

		startTime := time.Now()

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("invalidpoolid-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get the Pool UUID on which IO is running
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_AUTO, 0)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)

		// invalidPoolUUID Generation
		id := uuid.New()
		invalidPoolUUID := id.String()

		// Resize Pool with Invalid Pool ID
		// Do Pool Expand on the Node
		stepLog = fmt.Sprintf("Expanding pool on node [%s] and pool UUID: [%s] using auto",
			nodeDetail.Name,
			poolUUID)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolToBeResized, err := GetStoragePoolByUUID(poolUUID)
			log.FailOnError(err, "Failed to get pool using UUID [%s]", poolUUID)

			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool [%s] is [%d]", poolUUID, poolToBeResized.TotalSize/units.GiB)

			alertType := api.SdkStoragePool_RESIZE_TYPE_AUTO
			// Now trying to Expand Pool with Invalid Pool UUID
			err = Inst().V.ExpandPoolUsingPxctlCmd(*nodeDetail, invalidPoolUUID,
				alertType, expectedSize, false)
			if err != nil && strings.Contains(fmt.Sprintf("%v", err), "Please re-issue expand with force") {
				err = Inst().V.ExpandPoolUsingPxctlCmd(*nodeDetail, invalidPoolUUID,
					alertType, expectedSize, true)
			}

			// Verify error on pool expansion failure
			var errMatch error
			errMatch = nil
			re := regexp.MustCompile(fmt.Sprintf(".*failed to find storage pool with UID.*%s.*",
				invalidPoolUUID))
			if re.MatchString(fmt.Sprintf("%v", err)) == false {
				errMatch = fmt.Errorf("failed to verify failure using invalid PoolUUID [%v]", invalidPoolUUID)
			}
			dash.VerifyFatal(errMatch, nil, "Pool expand with invalid PoolUUID completed?")

			// retry pool resize but with valid pool UUID
			// Now trying to Expand Pool with Invalid Pool UUID
			err = Inst().V.ExpandPoolUsingPxctlCmd(*nodeDetail, poolUUID,
				api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize, false)
			if err != nil && strings.Contains(fmt.Sprintf("%v", err), "Please re-issue expand with force") {
				err = Inst().V.ExpandPoolUsingPxctlCmd(*nodeDetail, poolUUID,
					api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize, true)
			}
			log.FailOnError(err, "Failed to resize pool with UUID [%s]", poolToBeResized.Uuid)
			resizeErr := waitForPoolToBeResized(expectedSize, poolUUID, isjournal)
			dash.VerifyFatal(resizeErr, nil,
				fmt.Sprintf("Verify pool [%s] on node [%s] expansion using auto", poolUUID, nodeDetail.Name))

			// Sleep for 1 minute to check if there is some alerts generated
			time.Sleep(60 * time.Second)

			endTime := time.Now()

			// Get alerts from the cluster between startTime till endTime [ PWX-28484 ]
			log.InfoD("Getting alerts generated by Pool between startTime : [%v] and endTime : [%v]",
				startTime, endTime)
			alerts, err := Inst().V.GetAlertsUsingResourceTypeByTime(api.ResourceType_RESOURCE_TYPE_POOL,
				startTime, endTime)

			// Failing as no alerts seen , as we are running some negative scenarios it is expected to have some
			// alerts generated for resource type pool
			log.FailOnError(err, "Failed to fetch alerts between startTime [%v] and endTime [%v]",
				startTime, endTime)
			log.InfoD("Lists of alerts generated [%v]", alerts)

			alertErrorMessage := fmt.Sprintf("did alert generated for resource type [%v] with time specified?",
				api.ResourceType_RESOURCE_TYPE_POOL)
			dash.VerifyFatal(len(alerts.Alerts) > 0, true, alertErrorMessage)

		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Exit from Maintenance mode if Pool is still in Maintenance")
		log.FailOnError(ExitNodesFromMaintenanceMode(), "exit from maintenance mode failed?")
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{ResizePoolReduceErrorcheck}", func() {
	// Testrail Description : Resize to lower size than existing pool size,should fail with proper error statement

	JustBeforeEach(func() {
		StartTorpedoTest("ResizePoolReduceErrorcheck",
			"Resize to lower size than existing pool size,should fail with proper error statement",
			nil, 0)
	})

	stepLog := "Resize to lower size than existing"
	It(stepLog, func() {
		log.InfoD(stepLog)

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("reducesize-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get the Pool UUID on which IO is running
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_AUTO, 0)
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)

		// Resize Pool with lower pool size than existing
		stepLog = fmt.Sprintf("Resizing pool on node [%s] and pool UUID: [%s] using auto", nodeDetail.Name, poolUUID)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolToBeResized, err := GetStoragePoolByUUID(poolUUID)
			log.FailOnError(err, "Failed to get pool using UUID [%s]", poolUUID)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) - 1
			log.InfoD("Current Size of the pool [%s] is [%d]", poolUUID, poolToBeResized.TotalSize/units.GiB)

			// Now trying to Expand Pool with reduced Pool size
			err = Inst().V.ExpandPoolUsingPxctlCmd(*nodeDetail, poolUUID, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize, false)

			// Verify error on pool expansion failure
			var errMatch error
			errMatch = nil
			re := regexp.MustCompile(fmt.Sprintf("service pool expand: pool: %s is already at a size..*", poolUUID))
			if re.MatchString(fmt.Sprintf("%v", err)) == false {
				errMatch = fmt.Errorf("Failed to verify failure to lower pool size PoolUUID [%v]", poolUUID)
			}
			dash.VerifyFatal(errMatch, nil, "Pool expand to lower size than existing pool size completed?")

		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{PoolDeleteRebalancePxState}", func() {
	/*
		1. Create 4 Pools  say  0, 1 ,2 3, using disk of different size
		2. Delete Pool 1 and 3 ( after adding additional  disk with some rebalance in progress )
		3. Create Pool 5  with a disk -
			# pxctl sv drive add -s "type=gp3,size=20"
		4. Add the new disk to the pool created in step 3
		5. let rebalance continue.
		6. Exit pool maintenance and see if px becomes operational
	*/

	var testrailID = 0
	// Testrail Description : Delete Pool while Rebalance and verify Px comes up
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("PoolDeleteRebalancePxState",
			"Get Px State after pool delete",
			nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "Get Px State after pool delete"
	It(stepLog, func() {
		log.InfoD(stepLog)

		if IsEksCluster() != true {
			log.FailOnError(fmt.Errorf("DeletePool is currently supported for EKS and LocalDrives"), "Pool deletion supported?")
		}

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pooldeleterebalanceid-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		poolsBfr, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")

		// Get Pool with running IO on the cluster
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 0)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		// Get Node Details of the Pool with IO
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)
		log.InfoD("Pool with UUID [%v] present in Node [%v]", poolUUID, nodeDetail.Name)

		// Get Total Pools present on the Node present
		poolDetails, err := GetPoolsDetailsOnNode(nodeDetail)
		log.FailOnError(err, "Failed to get Pool Details from Node [%v]", nodeDetail.Name)
		log.InfoD("List of Pools present in the node [%v]", poolDetails)
		n1, err := node.GetNodeByName(nodeDetail.Name)
		log.FailOnError(err, "Failed to get Node Details from Node Name [%v]", nodeDetail.Name)
		nodeDetail = &n1

		// Test Needs minimum of 4 Pools to be present on the Node
		if len(poolDetails) < 4 {
			log.FailOnError(addNewPools(*nodeDetail, 4-len(poolDetails)),
				fmt.Sprintf("Adding New Pools failed on Node [%v]", nodeDetail.Name))
		}

		// Enter maintenance mode before deleting the pools from the cluster
		log.InfoD("Setting pools to maintenance on node [%s]", nodeDetail.Name)
		log.FailOnError(Inst().V.EnterPoolMaintenance(*nodeDetail),
			"failed to set pool maintenance mode on node [%s]", nodeDetail.Name)

		time.Sleep(1 * time.Minute)
		expectedStatus := "In Maintenance"
		log.FailOnError(WaitForPoolStatusToUpdate(*nodeDetail, expectedStatus),
			fmt.Sprintf("node %s pools are not in status %s", nodeDetail.Name, expectedStatus))

		//Wait for 5 min to bring up the portworx daemon before trying cloud drive add
		time.Sleep(5 * time.Minute)

		// Once 4 Pools are added Delete Pool 1 and Pool 3 from the Node
		for _, poolID := range []string{"1", "3"} {
			log.FailOnError(Inst().V.DeletePool(*nodeDetail, poolID, true),
				fmt.Sprintf("Deleting Pool with ID [%s] from Node [%v] failed", poolID, nodeDetail.Name))
		}

		// Exit pool maintenance and see if px becomes operational
		err = Inst().V.ExitPoolMaintenance(*nodeDetail)
		log.FailOnError(err, "failed to exit pool maintenance mode on node %s", nodeDetail.Name)

		err = Inst().V.WaitDriverUpOnNode(*nodeDetail, addDriveUpTimeOut)
		log.FailOnError(err, "volume driver down on node %s", nodeDetail.Name)

		expectedStatus = "Online"
		err = WaitForPoolStatusToUpdate(*nodeDetail, expectedStatus)
		log.FailOnError(err, fmt.Sprintf("node %s pools are not in status %s", nodeDetail.Name, expectedStatus))

		poolsAfr, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")

		dash.VerifySafely(len(poolsBfr) > len(poolsAfr),
			true,
			"verify pools count is updated after pools deletion")

		stepLog = fmt.Sprintf("Ensure that pool %s rebalance started and add new pool to the node %s", poolUUID, nodeDetail.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			t := func() (interface{}, bool, error) {
				expandedPool, err := GetStoragePoolByUUID(poolUUID)
				if err != nil {
					return nil, true, fmt.Errorf("error getting pool by using id %s", poolUUID)
				}

				if expandedPool == nil {
					return nil, false, fmt.Errorf("expanded pool value is nil")
				}
				if expandedPool.LastOperation != nil {
					log.Infof("Pool Resize Status: %v, Message : %s", expandedPool.LastOperation.Status, expandedPool.LastOperation.Msg)
					if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_IN_PROGRESS &&
						(strings.Contains(expandedPool.LastOperation.Msg, "Storage rebalance is running") || strings.Contains(expandedPool.LastOperation.Msg, "Rebalance in progress")) {
						return nil, false, nil
					}
					if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_FAILED {
						return nil, false, fmt.Errorf("PoolResize has failed. Error: %s", expandedPool.LastOperation)
					}

				}
				return nil, true, fmt.Errorf("pool status not updated")
			}
			_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second)
			log.FailOnError(err, "Error checking pool rebalance")
		})

		log.FailOnError(addNewPools(*nodeDetail, -1),
			fmt.Sprintf("Adding New Pools failed on Node [%v]", nodeDetail.Name))

		// Verify New Pool added successfully
		poolsAfrAdding, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")

		isAvailable := func(element *api.StoragePool, storagePools map[string]*api.StoragePool) bool {
			// iterate using the for loop
			for _, each := range storagePools {
				if each.Uuid == element.Uuid {
					return true
				}
			}
			return false
		}

		// Add newDisk for the pool created
		newPoolAdded := make([]*api.StoragePool, 0)
		for _, eachPool := range poolsAfrAdding {
			if isAvailable(eachPool, poolsAfr) == false {
				newPoolAdded = append(newPoolAdded, eachPool)
			}
		}

		dash.VerifySafely(len(poolsAfr) < len(poolsAfrAdding), true,
			fmt.Sprintf("New Pool added successfully on the node [%v]", newPoolAdded))

		dash.VerifyFatal(len(newPoolAdded) > 0, true, "New Pool Addition successful ?")
		log.InfoD("New Pool Added [%v]", newPoolAdded[0].Uuid)

		// Try resize the pool after addition
		poolUUID = newPoolAdded[0].Uuid
		expectedSize := (newPoolAdded[0].TotalSize / units.GiB) + 100

		log.InfoD("Current Size of the pool %s is %d", poolUUID, newPoolAdded[0].TotalSize/units.GiB)
		err = Inst().V.ExpandPool(poolUUID, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize, true)
		dash.VerifyFatal(err, nil, "Pool expansion init successful?")

		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "Failed to check if Journal enabled")

		resizeErr := waitForPoolToBeResized(expectedSize, poolUUID, isjournal)
		dash.VerifyFatal(resizeErr, nil,
			fmt.Sprintf("Verify pool %s on expansion using auto option", poolUUID))

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Exit from Maintenance mode if Pool is still in Maintenance")
		log.FailOnError(ExitNodesFromMaintenanceMode(), "exit from maintenance mode failed?")
		AfterEachTest(contexts, testrailID, runID)
	})

})

var _ = Describe("{AddMultipleDriveStorageLessNodeResizeDisk}", func() {
	/*
		Pool Resize after adding drives to storage less node
		https://portworx.testrail.net/index.php?/cases/view/51329
		https://portworx.testrail.net/index.php?/cases/view/51330
	*/

	var testrailID = 0
	// Testrail Description : Pool Resize after adding drives to storage less node
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("AddMultipleDriveStorageLessNodeResizeDisk",
			"Add Drive to storage less node and resize Disk",
			nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "Add Drives to storage less node and resize after adding the node"
	It(stepLog, func() {
		log.InfoD(stepLog)

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("storagelessresizedisk-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get Pool with running IO on the cluster
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_AUTO, 0)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		// Get Node Details of the Pool with IO
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)
		log.InfoD("Pool with UUID [%v] present in Node [%v]", poolUUID, nodeDetail.Name)

		storageLessNode := node.GetStorageLessNodes()
		// Get random storage less node present in the cluster
		var pickNode node.Node
		if len(storageLessNode) == 0 {
			err := MakeStoragetoStoragelessNode(*nodeDetail)
			log.FailOnError(err, "failed to mark storage Node to Storage less Node")
			storageLessNode = node.GetStorageLessNodes()
		}
		randomIndex := rand.Intn(len(storageLessNode))
		pickNode = storageLessNode[randomIndex]
		log.InfoD("Storage Less node is [%v]", pickNode.Name)

		isDMthin, err := IsDMthin()
		log.FailOnError(err, "error verifying if set up is DMTHIN enabled")

		if isDMthin {
			err = AddMetadataDisk(pickNode)
			log.FailOnError(err, "err adding metadata disk")
		}

		// Add multiple Drives to Storage less node
		maxDrivesToAdd := 6
		for i := 1; i < maxDrivesToAdd+1; i++ {
			log.InfoD("Adding [%d/%d] disks to the Node [%v]", i, maxDrivesToAdd, pickNode.Name)
			log.FailOnError(AddCloudDrive(pickNode, -1), "error adding cloud drive on Node [%v]", pickNode.Name)
		}
		log.Infof("Adding disks to the node completed")

		// Refresh endpoints
		log.FailOnError(Inst().V.RefreshDriverEndpoints(), "Failed to refresh end points")

		// Resize the cloud drive added on the Node
		poolList, err := GetAllPoolsOnNode(pickNode.Id)
		log.FailOnError(err, "failed to get pool details from Node [%v]", pickNode)

		for _, eachPool := range poolList {
			poolToBeResized, err := GetStoragePoolByUUID(eachPool)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", eachPool))
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + 100

			// Resize the Pool with either one of the allowed resize type
			log.InfoD("Current Size of the pool %s is %d", eachPool, poolToBeResized.TotalSize/units.GiB)

			poolResizeType := []api.SdkStoragePool_ResizeOperationType{api.SdkStoragePool_RESIZE_TYPE_AUTO,
				api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK}

			//Remove it once SdkStoragePool_RESIZE_TYPE_ADD_DISK is supported in dmthin
			if dmthin, err := IsDMthin(); err == nil {
				if !dmthin {
					poolResizeType = append(poolResizeType, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK)
				}
			}
			randomIndex := rand.Intn(len(poolResizeType))
			pickType := poolResizeType[randomIndex]
			log.InfoD("Expanding Pool [%v] using resize type [%v]", eachPool, pickType)
			err = Inst().V.ExpandPool(eachPool, pickType, expectedSize, false)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			resizeErr := waitForPoolToBeResized(expectedSize, eachPool, isjournal)
			dash.VerifyFatal(resizeErr, nil,
				fmt.Sprintf("Verify pool %s on expansion using auto option", eachPool))
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Exit from Maintenance mode if Pool is still in Maintenance")
		log.FailOnError(ExitNodesFromMaintenanceMode(), "exit from maintenance mode failed?")
		AfterEachTest(contexts, testrailID, runID)
	})

})

var _ = Describe("{DriveAddPXDown}", func() {
	/*
		Add drive when Px is down
	*/

	var testrailID = 0
	// Testrail Description : add drive when px is down
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("DriveAddPXDown",
			"Add Drive when Px is down",
			nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "Add Drive when Px is down"
	It(stepLog, func() {
		log.InfoD(stepLog)

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("adddrivepxdownid-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get Pool with running IO on the cluster
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		// Get Node Details of the Pool with IO
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)
		log.InfoD("Pool with UUID [%v] present in Node [%v]", poolUUID, nodeDetail.Name)

		startDriver := func() {
			// Start Px Back on the Node
			log.InfoD("Start Px Driver and wait for driver to come up on node [%v]", nodeDetail.Name)
			log.FailOnError(Inst().V.StartDriver(*nodeDetail),
				fmt.Sprintf("Failed to Bring back the Px on Node [%v]", nodeDetail.Name))
			log.FailOnError(Inst().V.WaitDriverUpOnNode(*nodeDetail, addDriveUpTimeOut),
				fmt.Sprintf("Driver is still down on node [%v] after waiting", nodeDetail.Name))
		}
		// Bring Px Down on the Node selected
		var nodeToPxDown []node.Node
		nodeToPxDown = append(nodeToPxDown, *nodeDetail)
		log.FailOnError(Inst().V.StopDriver(nodeToPxDown,
			false,
			nil),
			"Errored while stopping Px Driver")

		// wait for some time for driver to go down completly
		log.FailOnError(Inst().V.WaitDriverDownOnNode(*nodeDetail), "Failed waiting for driver to come up")

		// Start PxDriver after attempting add cloud drive
		defer startDriver()

		// Add Drive on the Node [ PTX-15856 ]
		err = AddCloudDrive(*nodeDetail, -1)
		if err != nil {
			// Check if the errMessage contains the phrases
			containsCloudDriveError := strings.Contains(err.Error(), "add cloud drive failed on node")
			containsPXError := strings.Contains(err.Error(), "PX is not running since the systemd service portworx.service is installed but not active")
			poolExpandCondition := containsCloudDriveError && containsPXError
			dash.VerifyFatal(poolExpandCondition, true, "adding new pool on the node failed?")
			log.InfoD(err.Error(), "Pool expand failed")
		} else {
			log.FailOnError(err, "Expect the addition of a new pool on the node to fail.")
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Exit from Maintenance mode if Pool is still in Maintenance")
		log.FailOnError(ExitNodesFromMaintenanceMode(), "exit from maintenance mode failed?")
		AfterEachTest(contexts, testrailID, runID)
	})

})

var _ = Describe("{ExpandUsingAddDriveAndPXRestart}", func() {
	/*
		Expand Using Add drive and restart Px
	*/
	var testrailID = 0
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("ExpandUsingAddDriveAndPXRestart",
			"Initiate pool expansion using add-drive and restart PX", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "Initiate pool expansion using add-drive and restart PX"

	It(stepLog, func() {
		log.InfoD(stepLog)
		if !IsPoolAddDiskSupported() {
			Skip("Add disk is not supported in DMThin")
		}

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pladddrvrestrt-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get Pool with running IO on the cluster
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 100)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		// Get Node Details of the Pool with IO
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)
		log.InfoD("Pool with UUID [%v] present in Node [%v]", poolUUID, nodeDetail.Name)

		poolToBeResized, err := GetStoragePoolByUUID(poolUUID)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID [%s]", poolUUID))
		expectedSize := (poolToBeResized.TotalSize / units.GiB) + 100

		log.InfoD("Current Size of the pool %s is %d", poolUUID, poolToBeResized.TotalSize/units.GiB)
		err = Inst().V.ExpandPool(poolUUID, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
		dash.VerifyFatal(err, nil, "Pool expansion init successful?")

		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "Failed to check if Journal enabled")

		resizeErr := waitForPoolToBeResized(expectedSize, poolUUID, isjournal)
		dash.VerifyFatal(resizeErr, nil,
			fmt.Sprintf("Verify pool %s on expansion using auto option", poolUUID))

		stepLog = fmt.Sprintf("Restart PX on node %s", nodeDetail.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err := Inst().V.RestartDriver(*nodeDetail, nil)
			log.FailOnError(err, fmt.Sprintf("error restarting px on node [%s]", nodeDetail.Name))
			err = Inst().V.WaitDriverUpOnNode(*nodeDetail, addDriveUpTimeOut)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node [%s]", nodeDetail.Name))
			dash.VerifyFatal(err == nil, true,
				fmt.Sprintf("PX is up after restarting on node [%s]", nodeDetail.Name))
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{ExpandUsingAddDriveAndNodeRestart}", func() {
	/*
		Expand Using Add drive and restart Node and verify if Px will be up after restart
	*/
	var testrailID = 0
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("ExpandUsingAddDriveAndNodeRestart",
			"Initiate pool expansion using add-drive and Reboot Node", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "Initiate pool expansion using add-drive and Reboot Node"

	It(stepLog, func() {
		log.InfoD(stepLog)

		if !IsPoolAddDiskSupported() {
			Skip("Add disk is not supported on DMThin Cluster")
		}

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("expanddiskadddrive-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get Pool with running IO on the cluster
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		// Get Node Details of the Pool with IO
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)
		log.InfoD("Pool with UUID [%v] present in Node [%v]", poolUUID, nodeDetail.Name)

		poolToBeResized, err := GetStoragePoolByUUID(poolUUID)
		log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)

		drvSize, err := getPoolDiskSize(poolToBeResized)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID [%s]", poolUUID))

		expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize
		expectedSize = roundUpValue(expectedSize)
		expectedSizeWithJournal := expectedSize

		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "Failed to check is journal enabled")

		if isjournal {
			expectedSizeWithJournal = expectedSizeWithJournal - 3
		}
		log.InfoD("Current Size of the pool [%s] is [%d]",
			poolToBeResized.Uuid,
			poolToBeResized.TotalSize/units.GiB)

		err = Inst().V.ExpandPool(poolToBeResized.Uuid,
			api.SdkStoragePool_RESIZE_TYPE_ADD_DISK,
			expectedSize, true)
		dash.VerifyFatal(err,
			nil,
			"Pool expansion init successful?")

		storageNode, err := GetNodeWithGivenPoolID(poolToBeResized.Uuid)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID [%s]", poolToBeResized.Uuid))
		err = RebootNodeAndWaitForPxUp(*storageNode)
		log.FailOnError(err, "Failed to reboot node [%v] and wait till it is up", storageNode.Name)

		log.FailOnError(Inst().V.WaitDriverUpOnNode(*storageNode, addDriveUpTimeOut), fmt.Sprintf("Driver is down on node [%s]", storageNode.Name))

		resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
		dash.VerifyFatal(resizeErr,
			nil,
			fmt.Sprintf("Expected new size to be [%d] or [%d] if pool has journal",
				expectedSize,
				expectedSizeWithJournal))

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{ResizeDiskAddDiskSamePool}", func() {
	/*
		Resize Disk Followed by adddisk should not create a new pool
	*/
	var testrailID = 0
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("ResizeDiskAddDiskSamePool",
			"Resize Disk Followed by adddisk should not create a new pool", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "Resize Disk Followed by adddisk should not create a new pool"

	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("resizediskadddisk-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get Pool with running IO on the cluster
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		// Get Node Details of the Pool with IO
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)
		log.InfoD("Pool with UUID [%v] present in Node [%v]", poolUUID, nodeDetail.Name)

		poolToBeResized, err := GetStoragePoolByUUID(poolUUID)
		log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)

		allPoolsOnNode, err := GetPoolsDetailsOnNode(nodeDetail)
		log.FailOnError(err, fmt.Sprintf("Failed to get all Pools present in Node [%s]", nodeDetail.Name))

		drvSize, err := getPoolDiskSize(poolToBeResized)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID [%s]", poolUUID))

		expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize
		expectedSize = roundUpValue(expectedSize)
		expectedSizeWithJournal := expectedSize

		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "Failed to check is journal enabled")

		if isjournal {
			expectedSizeWithJournal = expectedSizeWithJournal - 3
		}
		log.InfoD("Current Size of the pool %s is %d",
			poolToBeResized.Uuid,
			poolToBeResized.TotalSize/units.GiB)

		err = Inst().V.ExpandPool(poolToBeResized.Uuid,
			api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK,
			expectedSize, false)
		dash.VerifyFatal(err,
			nil,
			"Pool expansion init successful?")

		resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
		dash.VerifyFatal(resizeErr, nil,
			fmt.Sprintf("Verify pool [%s] on expansion using auto option", poolToBeResized.Uuid))

		if IsPoolAddDiskSupported() {
			expectedSize += drvSize

			// Expand Pool using Add Drive and verify if the Pool is expanded successfully
			err = Inst().V.ExpandPool(poolToBeResized.Uuid,
				api.SdkStoragePool_RESIZE_TYPE_ADD_DISK,
				expectedSize, true)
			dash.VerifyFatal(err,
				nil,
				"Pool expansion init successful?")

			resizeErr = waitForPoolToBeResized(expectedSize, poolUUID, isjournal)
			dash.VerifyFatal(resizeErr, nil,
				fmt.Sprintf("Verify pool [%s] on expansion using auto option", poolUUID))

			allPoolsOnNodeAfterResize, err := GetPoolsDetailsOnNode(nodeDetail)
			log.FailOnError(err, fmt.Sprintf("Failed to get all Pools present in Node [%s]", nodeDetail.Name))
			dash.VerifyFatal(len(allPoolsOnNode) <= len(allPoolsOnNodeAfterResize), true,
				"New pool is created on trying to expand pool using add disk option")
		}

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})

var _ = Describe("{DriveAddRebalanceInMaintenance}", func() {
	/*
		Rebalance taking long time during drive add in pool maintenance mode [PTX-15691] -> [PWX-26629]
	*/
	var testrailID = 0
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("DriveAddRebalanceInMaintenance",
			"Rebalance taking long time during drive add in pool maintenance mode", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "Rebalance taking long time during drive add in pool maintenance mode"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("resizediskadddisk-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get Pool with running IO on the cluster
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		// Get Node Details of the Pool with IO
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)
		log.InfoD("Pool with UUID [%v] present in Node [%v]", poolUUID, nodeDetail.Name)

		poolToBeResized, err := GetStoragePoolByUUID(poolUUID)
		log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)

		// Enter maintenance mode before deleting the pools from the cluster
		log.InfoD("Setting pools to maintenance on node [%s]", nodeDetail.Name)
		log.FailOnError(Inst().V.EnterPoolMaintenance(*nodeDetail),
			"failed to set pool maintenance mode on node [%s]", nodeDetail.Name)

		expectedStatus := "In Maintenance"
		log.FailOnError(WaitForPoolStatusToUpdate(*nodeDetail, expectedStatus),
			fmt.Sprintf("node %s pools are not in status %s", nodeDetail.Name, expectedStatus))

		//Wait for 2 min to bring up the portworx daemon before trying cloud drive add
		time.Sleep(2 * time.Minute)

		// Add cloud drive on the node selected and wait for rebalance to happen
		driveSpecs, err := GetCloudDriveDeviceSpecs()
		log.FailOnError(err, "Error getting cloud drive specs")

		deviceSpec := driveSpecs[0]
		deviceSpecParams := strings.Split(deviceSpec, ",")
		var specSize uint64
		paramsArr := make([]string, 0)
		for _, param := range deviceSpecParams {
			if strings.Contains(param, "size") {
				val := strings.Split(param, "=")[1]
				specSize, err = strconv.ParseUint(val, 10, 64)
				log.FailOnError(err, "Error converting size to uint64")
				paramsArr = append(paramsArr, fmt.Sprintf("size=%d,", specSize/2))
			} else {
				paramsArr = append(paramsArr, param)
			}
		}
		newSpec := strings.Join(paramsArr, ",")
		cloudAdderr := Inst().V.AddCloudDrive(nodeDetail, newSpec, -1)
		// NOTE: Will be validating error after bringing up the pool out of maintenance mode
		// this is to make sure that Pool is out of maintenance and other tests which runs after this
		// would not fail because of pool maintenance

		// Exit pool maintenance and see if px becomes operational
		err = Inst().V.ExitPoolMaintenance(*nodeDetail)
		log.FailOnError(err, "failed to exit pool maintenance mode on node %s", nodeDetail.Name)

		err = Inst().V.WaitDriverUpOnNode(*nodeDetail, addDriveUpTimeOut)
		log.FailOnError(err, "volume driver down on node %s", nodeDetail.Name)

		log.FailOnError(cloudAdderr, fmt.Sprintf("Add cloud drive failed on node %s", nodeDetail.Name))

		log.FailOnError(ValidateDriveRebalance(*nodeDetail),
			fmt.Sprintf("pool %v rebalance failed", poolUUID))
		err = Inst().V.RefreshDriverEndpoints()
		log.FailOnError(err, "error refreshing driver end points")

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{ResizePoolReduceErrorcheck}", func() {
	// Testrail Description : Resize to lower size than existing pool size,should fail with proper error statement

	JustBeforeEach(func() {
		StartTorpedoTest("ResizePoolReduceErrorcheck",
			"Resize to lower size than existing pool size,should fail with proper error statement",
			nil, 0)

	})

	stepLog := "Resize to lower size than existing"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("reducesize-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get the Pool UUID on which IO is running
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_AUTO, 0)
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)

		// Resize Pool with lower pool size than existing
		stepLog = fmt.Sprintf("Resizing pool on node [%s] and pool UUID: [%s] using auto", nodeDetail.Name, poolUUID)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolToBeResized, err := GetStoragePoolByUUID(poolUUID)
			log.FailOnError(err, "Failed to get pool using UUID [%s]", poolUUID)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) - 1
			log.InfoD("Current Size of the pool [%s] is [%d]", poolUUID, poolToBeResized.TotalSize/units.GiB)

			// Now trying to Expand Pool with reduced Pool size
			err = Inst().V.ExpandPoolUsingPxctlCmd(*nodeDetail, poolUUID, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize, false)

			// Verify error on pool expansion failure
			var errMatch error
			errMatch = nil
			re := regexp.MustCompile(fmt.Sprintf("service pool expand: pool: %s is already at a size..*", poolUUID))
			if re.MatchString(fmt.Sprintf("%v", err)) == false {
				errMatch = fmt.Errorf("Failed to verify failure to lower pool size PoolUUID [%v]", poolUUID)
			}
			dash.VerifyFatal(errMatch, nil, "Pool expand to lower size than existing pool size completed?")
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{AllPoolsDeleteAndCreateAndDelete}", func() {
	/*
	   1. Delete all the pools in a node
	   2. Verify it becomes a storageless node
	   3. Create a new pool on the node
	   4. Validate volume upadate and apps deployment it the new pool
	   5. Delete newly created pool
	   6. Validate node becomes storage less node
	   7. validate apps
	*/

	var testrailID = 57293
	// Testrail Corresponds : https://portworx.testrail.net/index.php?/cases/view/57293
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("AllPoolsDeleteAndCreateAndDelete",
			"Delete all the pools in a node, create a new pool and delete again",
			nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context
	stepLog := "Delete all the pools in a node, create a new pool and delete again"
	It(stepLog, func() {
		log.InfoD(stepLog)

		stNodes := node.GetStorageNodes()
		kvdbNodesIDs := make([]string, 0)
		kvdbMembers, err := Inst().V.GetKvdbMembers(stNodes[0])
		log.FailOnError(err, "Error getting KVDB members")

		var stNode node.Node
		for _, k := range kvdbMembers {
			kvdbNodesIDs = append(kvdbNodesIDs, k.Name)
		}
		for _, n := range stNodes {
			if !Contains(kvdbNodesIDs, n.Id) {
				stNode = n
			}
		}

		stepLog = fmt.Sprintf("Deleting all the pools from the node [%s]", stNode.Name)
		Step(stepLog, func() {

			nodePools := stNode.StoragePools
			for _, nodePool := range nodePools {
				poolIDToDelete := fmt.Sprintf("%d", nodePool.ID)
				deletePoolAndValidate(stNode, poolIDToDelete)
			}
			stepLog := fmt.Sprintf("validate node [%s] changed to storageless node", stNode.Name)
			Step(stepLog, func() {
				err := Inst().V.RefreshDriverEndpoints()
				log.FailOnError(err, "error refreshing end points")
				slNodes := node.GetStorageLessNodes()
				isStorageless := false
				for _, n := range slNodes {
					if n.Name == stNode.Name {
						isStorageless = true
						break
					}
				}

				dash.VerifyFatal(isStorageless, true, fmt.Sprintf("Verify node %s is converted to storageless node", stNode.Name))
			})

		})

		stepLog = fmt.Sprintf("Creating a new pool on node [%v]", stNode.Name)

		Step(stepLog, func() {
			log.InfoD(stepLog)

			err := AddCloudDrive(stNode, -1)
			log.FailOnError(err, "error adding cloud drive")
			stNodes := node.GetStorageNodes()
			isStorageNode := false

			for _, n := range stNodes {
				if n.Name == stNode.Name {
					isStorageNode = true
					stNode = n
					break
				}
			}
			dash.VerifyFatal(isStorageNode, true, fmt.Sprintf("Verify node %s is converted to storage node", stNode.Name))

		})
		stepLog = "Deploying Apps and validate"

		Step(stepLog, func() {
			log.InfoD(stepLog)
			contexts = make([]*scheduler.Context, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("alpldel2-%d", i))...)
			}
			ValidateApplications(contexts)
			defer appsValidateAndDestroy(contexts)

			isReplExists := false

		outer:
			for _, ctx := range contexts {
				vols, err := Inst().S.GetVolumes(ctx)
				log.FailOnError(err, "error getting volumes for the context [%s]", ctx.App.Key)
				for _, vol := range vols {
					appVol, err := Inst().V.InspectVolume(vol.ID)
					log.FailOnError(err, "error inspecting volume [%v]", vol.ID)
					replNodes := appVol.ReplicaSets[0].Nodes
					if Contains(replNodes, stNode.Id) {
						isReplExists = true
						break outer
					}
				}
			}

			dash.VerifySafely(isReplExists, true, fmt.Sprintf("Verify volume is created on the node [%s]", stNode.Name))
		})

		stepLog = fmt.Sprintf("Delete pool from the node [%s]", stNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			nodePool := stNode.StoragePools[0]
			deletePoolAndValidate(stNode, fmt.Sprintf("%d", nodePool.ID))
			err := Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "error refreshing end points")
			slNodes := node.GetStorageLessNodes()
			isStorageless := false

			for _, n := range slNodes {
				if n.Name == stNode.Name {
					isStorageless = true
					break
				}
			}
			dash.VerifyFatal(isStorageless, true, fmt.Sprintf("Verify node %s is converted to storageless node again after deleting pool %d", stNode.Name, nodePool.ID))
		})

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

func deletePoolAndValidate(stNode node.Node, poolIDToDelete string) {
	isPureBackend := false
	validateMultipath := []string{}
	if IsPureCluster() {
		isPureBackend = true
	}

	if isPureBackend {
		// if pure backend , we get the list of all multipath devices used while creating the pool
		// later check if those multipath devices are still exist post deleting the pool
		multipathDevBeforeDelete, err := GetMultipathDeviceOnPool(&stNode)
		log.FailOnError(err, fmt.Sprintf("Failed to get list of Multipath devices on Node [%v]", stNode.Name))
		validateMultipath = multipathDevBeforeDelete[poolIDToDelete]
	}

	poolsBfr, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
	log.FailOnError(err, "Failed to list storage pools")

	poolsMap, err := Inst().V.GetPoolDrives(&stNode)
	log.FailOnError(err, "error getting pool drive from the node [%s]", stNode.Name)

	stepLog := fmt.Sprintf("Delete poolID %s on node %s", poolIDToDelete, stNode.Name)

	Step(stepLog, func() {
		log.InfoD(stepLog)
		err = DeleteGivenPoolInNode(stNode, poolIDToDelete, true)
		dash.VerifyFatal(err, nil, fmt.Sprintf("verify deleting pool [%s] in the node [%s]", poolIDToDelete, stNode.Name))

		poolsAfr, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")

		dash.VerifySafely(len(poolsBfr) > len(poolsAfr), true, "verify pools count is updated after pools deletion")

		poolsMap, err = Inst().V.GetPoolDrives(&stNode)
		log.FailOnError(err, "error getting pool drive from the node [%s]", stNode.Name)
		_, ok := poolsMap[poolIDToDelete]
		dash.VerifyFatal(ok, false, "verify drive is deleted from the node")

		if isPureBackend {
			// Get list of all Multipath devices after deleting the pool
			allMultipathDev, err := GetMultipathDeviceIDsOnNode(&stNode)
			log.FailOnError(err, fmt.Sprintf("failed to get multipath devices on Node [%v]", stNode.Name))
			for _, eachMultipath := range allMultipathDev {
				for _, validateEach := range validateMultipath {
					dash.VerifyFatal(validateEach == eachMultipath, false, fmt.Sprintf("Multipath device [%v] did not delete on Deleting Pool", validateEach))
				}
			}
		}
	})
}

var _ = Describe("{NodeAddDiskWhileAddDiskInProgress}", func() {
	/*
	   1.Add disk using add-disk option
	   2. Add disk again while initial expansion is in-progress
	*/
	var testrailID = 51356
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51356
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("NodeAddDriveWhileAddDriveInProgress", "Initiate pool expansion using add-drive while one already in progress", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "should get the existing storage node and expand the pool by adding a drive while one already in progress"

	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pladddskinp-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		poolUUIDToBeResized := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)

		node, err := GetNodeWithGivenPoolID(poolUUIDToBeResized)
		log.FailOnError(err, "error finding node with pool uuid [%s]", poolUUIDToBeResized)

		stepLog = fmt.Sprintf("Add disk in pool [%s] from node [%s]", poolUUIDToBeResized, node.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
			log.FailOnError(err, "Failed to list storage pools")
			dash.VerifyFatal(len(pools) > 0, true, "Storage pools exist ?")

			poolToBeResized := pools[poolUUIDToBeResized]
			dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

			// px will put a new request in a queue, but in this case we can't calculate the expected size,
			// so need to wain until the ongoing operation is completed
			stepLog = "Verify that pool resize is not in progress"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				if val, err := poolResizeIsInProgress(poolToBeResized); val {
					// wait until resize is completed and get the updated pool again
					poolToBeResized, err = GetStoragePoolByUUID(poolUUIDToBeResized)
					log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolUUIDToBeResized))
				} else {
					log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolUUIDToBeResized, err))
				}
			})

			var expectedSize uint64
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check is Journal enabled")

			stepLog = "Calculate expected pool size and trigger pool resize using add-disk"
			Step(stepLog, func() {
				log.InfoD(stepLog)

				expectedSize = (poolToBeResized.TotalSize / units.GiB) + drvSize

				log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)

				err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
				dash.VerifyFatal(err, nil, "Pool expansion init successful?")

				err = WaitForExpansionToStart(poolToBeResized.Uuid)
				log.FailOnError(err, "error waiting for expansion to start on the pool [%s]", poolToBeResized.Uuid)

			})

			stepLog = fmt.Sprintf("trigger pool resize using add-disk again on pool [%s] while previous one is in-progress", poolToBeResized.Uuid)
			Step(stepLog, func() {
				log.InfoD(stepLog)

				newExpectedSize := expectedSize + drvSize

				//To-Do Need to handle the case for multiple pools
				newExpectedSizeWithJournal := newExpectedSize
				if isjournal {
					newExpectedSizeWithJournal = newExpectedSizeWithJournal - 3
				}

				log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)

				poolNode, err := GetNodeWithGivenPoolID(poolToBeResized.Uuid)
				log.FailOnError(err, "error getting node with pool uuid [%s]", poolToBeResized.Uuid)
				err = Inst().V.ExpandPoolUsingPxctlCmd(*poolNode, poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, newExpectedSize, true)
				expectedErr := false
				expectedErrStr := fmt.Sprintf("resize for pool %s is already in progress", poolToBeResized.Uuid)
				if err != nil && strings.Contains(err.Error(), expectedErrStr) {
					expectedErr = true

				}
				dash.VerifyFatal(expectedErr, true, fmt.Sprintf("verify pool expansion failed with expected error. Error. %v", err))

			})

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{NodeAddDiskWhileResizeDiskInProgress}", func() {
	/*
	   1.Add disk using resize-disk option
	   2. Add disk again while initial expansion is in-progress
	*/
	var testrailID = 50939
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/50939
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("NodeAddDiskWhileResizeDiskInProgress", "Initiate pool expansion using add-disk while one already in progress with resize-disk", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "should get the existing storage node and expand the pool by adding a drive while one already in progress"

	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("plrszdskinp-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		poolUUIDToBeResized := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)

		node, err := GetNodeWithGivenPoolID(poolUUIDToBeResized)
		log.FailOnError(err, "error finding node with pool uuid [%s]", poolUUIDToBeResized)

		stepLog = fmt.Sprintf("Resize disk in pool [%s] from node [%s]", poolUUIDToBeResized, node.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
			log.FailOnError(err, "Failed to list storage pools")
			dash.VerifyFatal(len(pools) > 0, true, "Storage pools exist ?")

			poolToBeResized := pools[poolUUIDToBeResized]
			dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

			// px will put a new request in a queue, but in this case we can't calculate the expected size,
			// so need to wain until the ongoing operation is completed
			stepLog = "Verify that pool resize is not in progress"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				if val, err := poolResizeIsInProgress(poolToBeResized); val {
					// wait until resize is completed and get the updated pool again
					poolToBeResized, err = GetStoragePoolByUUID(poolUUIDToBeResized)
					log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolUUIDToBeResized))
				} else {
					log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolUUIDToBeResized, err))
				}
			})

			var expectedSize uint64
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check is Journal enabled")

			stepLog = "Calculate expected pool size and trigger pool resize using add-disk"
			Step(stepLog, func() {
				log.InfoD(stepLog)

				expectedSize = (poolToBeResized.TotalSize / units.GiB) * 2

				log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)

				err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
				dash.VerifyFatal(err, nil, "Pool expansion init successful?")

				err = WaitForExpansionToStart(poolToBeResized.Uuid)
				log.FailOnError(err, "error waiting for expansion to start on the pool [%s]", poolToBeResized.Uuid)

			})

			stepLog = fmt.Sprintf("trigger pool resize using resize-disk on pool [%s] while previous one is in-progress", poolToBeResized.Uuid)
			Step(stepLog, func() {
				log.InfoD(stepLog)

				newExpectedSize := expectedSize + drvSize

				//To-Do Need to handle the case for multiple pools
				newExpectedSizeWithJournal := newExpectedSize
				if isjournal {
					newExpectedSizeWithJournal = newExpectedSizeWithJournal - 3
				}

				log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)

				poolNode, err := GetNodeWithGivenPoolID(poolToBeResized.Uuid)
				log.FailOnError(err, "error getting node with pool uuid [%s]", poolToBeResized.Uuid)
				err = Inst().V.ExpandPoolUsingPxctlCmd(*poolNode, poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, newExpectedSize, true)
				expectedErr := false
				expectedErrStr := fmt.Sprintf("resize for pool %s is already in progress", poolToBeResized.Uuid)
				if err != nil && strings.Contains(err.Error(), expectedErrStr) {
					expectedErr = true

				}
				dash.VerifyFatal(expectedErr, true, fmt.Sprintf("verify pool expansion failed with expected error. Error. %v", err))
			})

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{MulVolPoolResize}", func() {
	var testrailID = 51345
	/*
			Priority: P0
		1. Create a spec and deploy multiple volumes on a pool
		2. Perform pool resize on the pool
		3. Validate the applications
	*/

	// Testrail Corresponds : https://portworx.testrail.net/index.php?/cases/view/51345
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("MulVolPoolResize",
			"Resize pool with multiples volumes attached",
			nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context
	stepLog := "Deploy multiple volumes"
	It(stepLog, func() {
		log.InfoD(stepLog)

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("nwplfullad-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stepLog = "Get pool with multiple volumes"
		var selectedPoolID string
		Step(stepLog, func() {
			log.InfoD(stepLog)

			poolsVolsMap := make(map[string]int, 0)
			for _, ctx := range contexts {
				vols, err := Inst().S.GetVolumes(ctx)
				log.FailOnError(err, "error getting volumes for context [%s]", ctx.App.Key)
				for _, vol := range vols {
					apiVol, err := Inst().V.InspectVolume(vol.ID)
					log.FailOnError(err, "error inspecting volume [%s]", vol.ID)
					volPools := apiVol.ReplicaSets[0].PoolUuids

					for _, volPool := range volPools {
						poolsVolsMap[volPool] += 1
					}
				}
			}
			currMaxVols := math.MinInt
			for k, v := range poolsVolsMap {

				if v > currMaxVols {
					selectedPoolID = k
					currMaxVols = v
				}
			}
		})

		stepLog = "Initiate pool expansion using resize-disk"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolToBeResized, err := GetStoragePoolByUUID(selectedPoolID)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPoolID))
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, false)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on expansion using resize-disk", poolToBeResized.Uuid))
		})

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Exit from Maintenance mode if Pool is still in Maintenance")
		log.FailOnError(ExitNodesFromMaintenanceMode(), "exit from maintenance mode failed?")
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{MulPoolsUpMetaPoolFullAndResize}", func() {
	var testrailID = 51350
	/*
			Priority: P0
		1. Selected a node with single pool and has metadata
		2. deploy apps and add new pool
		2. Fill up the pool with the metadata
		3. Resize the pool and recover the node
	*/

	// Testrail Corresponds : https://portworx.testrail.net/index.php?/cases/view/51350
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("MulPoolsUpMetaPoolFullAndResize",
			"Resize pool with multiples volumes attached",
			nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context
	stepLog := "Get node with multiple pools and deploy volumes"
	It(stepLog, func() {
		log.InfoD(stepLog)
		existingAppList := Inst().AppList

		stNodes := node.GetStorageNodes()
		var selectedNode, secondReplNode node.Node

		for _, n := range stNodes {
			if len(n.Pools) > 1 {
				selectedNode = n
				break
			}
		}
		if selectedNode.Name == "" {
			log.FailOnError(fmt.Errorf("no node with multiple pools exists"), "error identifying node with more than one pool")

		}
		log.Infof("Identified node [%s] for pool expansion", selectedNode.Name)

		poolWithMetadataDisk, err := GetPoolUUIDWithMetadataDisk(selectedNode)
		log.FailOnError(err, "error identifying pool with metadata disk from the node [%v]", selectedNode.Name)
		repl1Pool, err := GetStoragePoolByUUID(poolWithMetadataDisk)
		fmt.Printf("repl1 UUID is [%s]\n", repl1Pool.Uuid)
		log.FailOnError(err, "error getting storage pool with UUID [%s]", poolWithMetadataDisk)
		for _, n := range stNodes {
			if n.Name != selectedNode.Name {
				secondReplNode = n
				break
			}
		}
		fmt.Printf("seconde repl node: %s\n", secondReplNode.Name)
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

		stepLog = fmt.Sprintf("Fill up metadata pool [%s] in node [%s] and initiate pool expansion", repl1Pool.Uuid, selectedNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)

			poolLabelToUpdate := make(map[string]string)
			nodesToDisableProvisioning := make([]string, 0)
			poolsToDisableProvisioning := make([]string, 0)

			defer func() {
				//Reverting the provisioning changes done for the test
				Inst().AppList = existingAppList
				err = Inst().V.SetClusterOpts(selectedNode, map[string]string{
					"--disable-provisioning-labels": ""})
				log.FailOnError(err, fmt.Sprintf("error removing cluster options disable-provisioning-labels"))
				err = Inst().S.RemoveLabelOnNode(selectedNode, k8s.NodeType)
				log.FailOnError(err, "error removing label on node [%s]", selectedNode.Name)
				err = Inst().S.RemoveLabelOnNode(secondReplNode, k8s.NodeType)
				log.FailOnError(err, "error removing label on node [%s]", secondReplNode.Name)

				poolLabelToUpdate[k8s.NodeType] = ""
				poolLabelToUpdate["provision"] = ""
				// Update the pool label
				for _, p := range selectedNode.Pools {
					err = Inst().V.UpdatePoolLabels(selectedNode, p.Uuid, poolLabelToUpdate)
					log.FailOnError(err, "Failed to update the label [%v] on the pool [%s] on node [%s]", poolLabelToUpdate, repl1Pool.Uuid, selectedNode.Name)
				}

			}()

			//Disabling provisioning on the other nodes/pools  and enabling only on selected pools for making sure the metadata node is full
			err = Inst().S.AddLabelOnNode(selectedNode, k8s.NodeType, k8s.FastpathNodeType)
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
					err = Inst().V.UpdatePoolLabels(selectedNode, p.Uuid, poolLabelToUpdate)
					log.FailOnError(err, "Failed to update the label [%v] on the pool [%s] on node [%s]", poolLabelToUpdate, repl1Pool.Uuid, selectedNode.Name)

				}
			}

			clusterOptsVal := fmt.Sprintf("\"node=%s;provision=disable\"", strings.Join(nodesToDisableProvisioning, ","))
			err = Inst().V.SetClusterOpts(selectedNode, map[string]string{
				"--disable-provisioning-labels": clusterOptsVal})
			log.FailOnError(err, fmt.Sprintf("error update cluster options disable-provisioning-labels with value [%s]", clusterOptsVal))

			Inst().AppList = []string{"fio-fastpath"}
			contexts = make([]*scheduler.Context, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("mtplfullrz-%d", i))...)
			}
			ValidateApplications(contexts)
			defer appsValidateAndDestroy(contexts)

			err = waitForStorageDown(selectedNode)
			log.FailOnError(err, fmt.Sprintf("Failed to make node %s storage down", selectedNode.Name))
			t := func() (interface{}, bool, error) {
				poolsStatus, err := Inst().V.GetNodePoolsStatus(selectedNode)
				log.FailOnError(err, "error getting pool status on node %s", selectedNode.Name)

				for i, s := range poolsStatus {
					log.Infof("pool [%s] has status [%s]", i, s)
					if i == repl1Pool.Uuid && s == "Offline" {
						return nil, false, nil
					}

				}

				return nil, true, fmt.Errorf("pool status not updated")
			}
			_, err = task.DoRetryWithTimeout(t, 15*time.Minute, 10*time.Second)
			log.FailOnError(err, "metadata pool is not offline")

			expectedSize := (repl1Pool.TotalSize / units.GiB) * 2

			log.InfoD("Current Size of the pool %s is %d", repl1Pool.Uuid, repl1Pool.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(repl1Pool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			resizeErr := waitForPoolToBeResized(expectedSize, repl1Pool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", repl1Pool.Uuid, selectedNode.Name))
			status, err := Inst().V.GetNodeStatus(selectedNode)
			log.FailOnError(err, fmt.Sprintf("Error getting PX status of node %s", selectedNode.Name))
			dash.VerifySafely(*status, api.Status_STATUS_OK, fmt.Sprintf("validate PX status on node %s. Current status: [%s]", selectedNode.Name, status.String()))

		})

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{DiffPoolExpansionFromMaintenanceNode}", func() {
	/*
			Priority: P2
		1. Put a node in maintenance mode
		2. Pick a pool from another node and trigger expansion from node in maintenance mode
		3. Validate the applications
	*/

	var (
		contexts = make([]*scheduler.Context, 0)
	)

	JustBeforeEach(func() {
		StartTorpedoTest("DiffPoolExpansionFromMaintenanceNode",
			"Trigger pool expansion of node 2 from node 1 while node 1 is in maintenance mode",
			nil, 0)
	})

	stepLog := "Deploy multiple volumes"
	It(stepLog, func() {
		log.InfoD(stepLog)

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("nwplfullad-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stepLog = "Get pool with IOs"
		var selectedPoolID string
		var err error
		Step(stepLog, func() {
			log.InfoD(stepLog)
			selectedPoolID = pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, 0)
		})

		stepLog = "Pick node 2 and place it in maintenance mode"
		var selectedNode *node.Node
		var maintenanceNode node.Node
		Step(stepLog, func() {
			log.InfoD(stepLog)
			selectedNode, err = GetNodeWithGivenPoolID(selectedPoolID)
			log.FailOnError(err, "error getting node with pool UUID [%s]", selectedPoolID)
			stNodes := node.GetStorageNodes()
			for _, n := range stNodes {
				if n.Name != selectedNode.Name {
					maintenanceNode = n
					break
				}
			}

			err = Inst().V.EnterMaintenance(maintenanceNode)
			log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenance mode", maintenanceNode.Name))
			//maintenance mode takes few seconds to be updated even though node has returned maintenance status,hence the wait
			time.Sleep(1 * time.Minute)
			status, err := Inst().V.GetNodeStatus(maintenanceNode)
			log.FailOnError(err, "error getting status of node [%s]", maintenanceNode.Name)
			log.InfoD(fmt.Sprintf("Node %s status %s", maintenanceNode.Name, status.String()))
		})

		stepLog = "Initiate pool expansion of node 1 using resize-disk from maintenance node"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolToBeResized, err := GetStoragePoolByUUID(selectedPoolID)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPoolID))
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPoolUsingPxctlCmd(maintenanceNode, poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on expansion using resize-disk", poolToBeResized.Uuid))
		})

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Exit from Maintenance mode if Pool is still in Maintenance")
		log.FailOnError(ExitNodesFromMaintenanceMode(), "exit from maintenance mode failed?")
		AfterEachTest(contexts)
	})
})

var _ = Describe("{ResyncFailedPoolOutOfRebalance}", func() {
	// Testrail Description : Resync failed for a volume after pool came out of rebalance PTX-15696 -> PWX-26967
	/*
		Deployed systemtest sysbench spec with 1TB volume
		Pod come up and started writing
		Added the drive in node 10.13.166.216
		Observed the volume status to be degraded
		Waited for pool to come online
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("ResyncFailedPoolOutOfRebalance",
			"Resync failed for a volume after pool came out of rebalance",
			nil, 0)
	})

	stepLog := "Resync volume after rebalance"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("reducesize-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get Pool with running IO on the cluster
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		// Get Node Details of the Pool with IO
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)
		log.InfoD("Pool with UUID [%v] present in Node [%v]", poolUUID, nodeDetail.Name)

		// Resize the Pool few times expanding drives

		poolToBeResized, err := GetStoragePoolByUUID(poolUUID)
		for count := 0; count < 1; count++ {
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + 50

			// Resize the Pool with either one of the allowed resize type

			log.InfoD("Current Size of the pool %s is %d", poolUUID, poolToBeResized.TotalSize/units.GiB)
			log.InfoD("Expanding Pool [%v] using resize type [%v]", poolUUID, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK)
			err = Inst().V.ExpandPool(poolUUID, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			resizeErr := waitForPoolToBeResized(expectedSize, poolUUID, isjournal)
			dash.VerifyFatal(resizeErr, nil,
				fmt.Sprintf("Verify pool %s on expansion using auto option", poolUUID))
		}

		// Validate Volume resync if any volume got in to resync mode
		for _, eachContext := range contexts {
			vols, err := Inst().S.GetVolumes(eachContext)
			log.FailOnError(err, "Failed to get volumes from context")
			for _, eachVol := range vols {
				curReplSet, err := Inst().V.GetReplicationFactor(eachVol)
				log.FailOnError(err, "failed to get replication factor of the volume")

				var poolID []string
				poolID, err = GetPoolIDsFromVolName(eachVol.ID)
				log.FailOnError(err, "failed to get PoolID from volume Name [%s]", eachVol.Name)

				for _, eachPoolUUID := range poolID {
					if eachPoolUUID == poolUUID {
						// Check if Replication factor is 3. if so, then reduce the repl factor and then set repl factor to 3
						if curReplSet == 3 {
							newRepl := int64(curReplSet - 1)
							log.FailOnError(Inst().V.SetReplicationFactor(eachVol, newRepl,
								nil, nil, true),
								"Failed to set Replicaiton factor")
						}
						// Change Replica sets of each volumes created to 3
						var maxReplicaFactor int64
						var nodesToBeUpdated []string
						var poolsToBeUpdated []string
						maxReplicaFactor = 3
						nodesToBeUpdated = nil
						poolsToBeUpdated = nil
						log.FailOnError(Inst().V.SetReplicationFactor(eachVol, maxReplicaFactor,
							nodesToBeUpdated, poolsToBeUpdated, true),
							"Failed to set Replicaiton factor")

						// Sleep for some time before checking if any resync to start
						time.Sleep(2 * time.Minute)
						if inResync(eachVol.Name) {
							WaitTillVolumeInResync(eachVol.Name)
						}
					}
				}
			}
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{AddDiskAddDriveAndDeleteInstance}", func() {
	/*
	   1.Add disk using add-disk option
	   2. Create a new pool
	   3. Delete the instance
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("AddDiskAddDriveAndDeleteInstance", "Initiate pool expand using add-disk and create new pool and delete instance", nil, 0)

	})

	stepLog := "should get the existing pool, expand the pool by adding disk and create a new pool and then delete the instance"

	It(stepLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("plrszdskinp-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		poolUUIDToBeResized := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)

		stNode, err := GetNodeWithGivenPoolID(poolUUIDToBeResized)
		log.FailOnError(err, "error finding stNode with pool uuid [%s]", poolUUIDToBeResized)

		stepLog = fmt.Sprintf("add-disk to the pool [%s] in the stNode [%s]", poolUUIDToBeResized, stNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
			log.FailOnError(err, "Failed to list storage pools")
			dash.VerifyFatal(len(pools) > 0, true, "Storage pools exist ?")

			poolToBeResized := pools[poolUUIDToBeResized]
			dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

			stepLog = "Verify that pool resize is not in progress"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				if val, err := poolResizeIsInProgress(poolToBeResized); val {
					// wait until resize is completed and get the updated pool again
					poolToBeResized, err = GetStoragePoolByUUID(poolUUIDToBeResized)
					log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolUUIDToBeResized))
				} else {
					log.FailOnError(err, fmt.Sprintf("pool [%s] cannot be expanded due to error: %v", poolUUIDToBeResized, err))
				}
			})

			var expectedSize uint64
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check is Journal enabled")

			stepLog = "Calculate expected pool size and trigger pool resize using add-disk"
			Step(stepLog, func() {
				log.InfoD(stepLog)

				expectedSize = (poolToBeResized.TotalSize / units.GiB) + drvSize

				log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)

				err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, false)
				dash.VerifyFatal(err, nil, "Pool expansion init successful?")
				resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
				dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Expected new size to be '%d' or '%d'", expectedSize, expectedSize-3))
			})

		})
		poolsBfr, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")
		stepLog = fmt.Sprintf("create new pool in the stNode [%v]", stNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			///creating a spec to perform add  drive
			driveSpecs, err := GetCloudDriveDeviceSpecs()
			log.FailOnError(err, "Error getting cloud drive specs")

			minSpecSize := uint64(math.MaxUint64)

			for _, p := range stNode.Pools {

				diskSize, err := getPoolDiskSize(p)
				log.FailOnError(err, "error getting disk size from pool [%s] in the node [%s]", p.Uuid, stNode.Name)
				if diskSize < minSpecSize {
					minSpecSize = diskSize
				}
			}

			deviceSpec := driveSpecs[0]
			deviceSpecParams := strings.Split(deviceSpec, ",")
			paramsArr := make([]string, 0)
			for _, param := range deviceSpecParams {
				if strings.Contains(param, "size") {
					paramsArr = append(paramsArr, fmt.Sprintf("size=%d,", minSpecSize/2))
				} else {
					paramsArr = append(paramsArr, param)
				}
			}
			newSpec := strings.Join(paramsArr, ",")

			stepLog = fmt.Sprintf("Adding new pool to node [%s] with spec size [%s]", stNode.Name, newSpec)
			Step(stepLog, func() {
				log.InfoD(stepLog)
				err = Inst().V.AddCloudDrive(stNode, newSpec, -1)
				log.FailOnError(err, "error adding new drive to node %s", stNode.Name)
				log.InfoD("Validate pool rebalance after drive add to the node %s", stNode.Name)
				err = ValidateDriveRebalance(*stNode)
				log.FailOnError(err, "pool re-balance failed on node %s", stNode.Name)
				err = Inst().V.WaitDriverUpOnNode(*stNode, addDriveUpTimeOut)
				log.FailOnError(err, "volume drive down on node %s", stNode.Name)

				poolsAfr, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
				log.FailOnError(err, "Failed to list storage pools")
				dash.VerifyFatal(len(poolsBfr)+1, len(poolsAfr), "verify new pool is created")
				err = Inst().V.RefreshDriverEndpoints()
				log.FailOnError(err, "error refreshing driver end points")

			})

		})

		stNode, err = GetNodeWithGivenPoolID(poolUUIDToBeResized)
		log.FailOnError(err, "error finding stNode with pool uuid [%s]", poolUUIDToBeResized)

		initDisks := stNode.Disks
		initPools := stNode.Pools

		systemOpts := node.SystemctlOpts{
			ConnectionOpts: node.ConnectionOpts{
				Timeout:         2 * time.Minute,
				TimeBeforeRetry: defaultRetryInterval,
			},
			Action: "start",
		}
		drivesMap, err := Inst().N.GetBlockDrives(*stNode, systemOpts)
		log.FailOnError(err, "error getting block drives from node [%s]", stNode.Name)

		stepLog = fmt.Sprintf("killing node [%s]", stNode.Name)
		Step(stepLog, func() {
			//Storing existing node details before terminating an instance
			storageDriverNodes := node.GetStorageDriverNodes()
			stDrvNodesNames := make([]string, len(storageDriverNodes))
			for _, sn := range storageDriverNodes {
				stDrvNodesNames = append(stDrvNodesNames, sn.Name)
			}
			slNodes := node.GetStorageLessNodes()
			slNodesNames := make([]string, len(slNodes))
			for _, sn := range slNodes {
				slNodesNames = append(slNodesNames, sn.Name)
			}
			stNodes := node.GetStorageNodes()
			stNodesNames := make([]string, len(stNodes))
			for _, sn := range stNodes {
				stNodesNames = append(stNodesNames, sn.Name)
			}

			err = AsgKillNode(*stNode)
			dash.VerifyFatal(err, nil, fmt.Sprintf("verify terminating node [%s]", stNode.Name))
			newStorageDriverNodes := node.GetStorageDriverNodes()
			dash.VerifyFatal(len(storageDriverNodes), len(newStorageDriverNodes), "verify new storage driver node is created")
			dash.VerifyFatal(len(slNodes), len(node.GetStorageLessNodes()), "verify storageless nodes count is same")

			var newNode node.Node
			var nodeToValidate node.Node
			for _, ns := range newStorageDriverNodes {
				if !Contains(stDrvNodesNames, ns.Name) {
					newNode = ns
					break
				}
			}
			if len(newNode.Pools) > 0 {
				log.InfoD("new node [%s] created as storage node", newNode.Name)
				nodeToValidate = newNode
			} else {
				log.InfoD("new node [%s] created as storageless node", newNode.Name)
				for _, n := range node.GetStorageNodes() {
					if Contains(slNodesNames, n.Name) {
						log.InfoD("node [%s] is converted to storage node", n.Name)
						nodeToValidate = n
						break
					}
				}
			}

			//validating if in-build metadata disk exists
			isInitMetadataDiskExist := false
			var initTotalDiskSize uint64
			for _, n := range initDisks {
				if n.Metadata {
					isInitMetadataDiskExist = true
				}
				initTotalDiskSize = initTotalDiskSize + (n.Size / units.GiB)

			}

			isnewMetadataDiskExist := false
			var newTotalDiskSize uint64
			nNodeDisks := nodeToValidate.Disks
			for k, n := range nNodeDisks {
				dash.VerifySafely(n.Online, true, fmt.Sprintf("verify disk [%s] is online", k))
				if n.Metadata {
					isnewMetadataDiskExist = true
				}
				newTotalDiskSize = newTotalDiskSize + (n.Size / units.GiB)

			}
			dash.VerifySafely(isInitMetadataDiskExist, isnewMetadataDiskExist, "Verify metadata disk status")

			dash.VerifySafely(len(nodeToValidate.Pools), len(initPools), fmt.Sprintf("verify node [%s] pools count matching with deleted node", nodeToValidate.Name))
			var initTotalSize uint64
			for _, p := range initPools {
				initTotalSize = initTotalSize + (p.TotalSize / units.GiB)
			}

			var newTotalSize uint64
			for _, p := range nodeToValidate.Pools {
				newTotalSize = newTotalSize + (p.TotalSize / units.GiB)
			}
			dash.VerifySafely(initTotalSize, newTotalSize, fmt.Sprintf("verify node [%s] total size matching with deleted node", nodeToValidate.Name))

			//validating if dedicated metadat disk exists
			newDrivesMap, err := Inst().N.GetBlockDrives(nodeToValidate, systemOpts)
			log.FailOnError(err, "error getting block drives from node [%s]", nodeToValidate.Name)

			isInitDedicatedMetadataDiskExist := false
			isNewDedicatedMetadataDiskExist := false
			for _, v := range drivesMap {
				for lk := range v.Labels {
					if lk == "mdvol" {
						isInitDedicatedMetadataDiskExist = true
					}
				}
			}

			for _, v := range newDrivesMap {
				for lk := range v.Labels {
					if lk == "mdvol" {
						isNewDedicatedMetadataDiskExist = true
					}
				}
			}

			dash.VerifySafely(isInitDedicatedMetadataDiskExist, isNewDedicatedMetadataDiskExist, "Verify dedicated metadisk status")
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{DriveAddAsJournal}", func() {
	/*
		Add drive when as journal
		case1:if dmthin journal is not supported so it should fail with  error message
		case2:if it is btrfs and journal drive exists so it should have failed with error message jounral drive exists
		case3:if it is btrfs and journal drive does not exists so it add journal drive successfully

	*/
	var testrailID = 0
	// Testrail Description : Add drive when as journal
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("DriveAddAsJournal",
			"Add drive when as journal",
			nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "Add drive when as journal"
	It(stepLog, func() {
		log.InfoD(stepLog)

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("adddriveasjournal-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get Pool with running IO on the cluster
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_AUTO, 0)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		// Get Node Details of the Pool with IO
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)
		log.InfoD("Pool with UUID [%v] present in Node [%v]", poolUUID, nodeDetail.Name)

		exitPoolMaintenance := func() {
			err = Inst().V.ExitPoolMaintenance(*nodeDetail)
			log.FailOnError(err, "Exiting maintenance mode failed")
			log.InfoD("Exiting pool Maintenance mode successful")

			expectedStatus := "Online"
			err = WaitForPoolStatusToUpdate(*nodeDetail, expectedStatus)
			log.FailOnError(err,
				fmt.Sprintf("node %s pools are not in status %s", nodeDetail.Name, expectedStatus))
		}

		dmthinEnabled, err := IsDMthin()
		log.FailOnError(err, "error checking if set up is DMTHIN enabled")

		// Add cloud drive on the node selected and wait for rebalance to happen
		driveSpecs, err := GetCloudDriveDeviceSpecs()
		log.FailOnError(err, "Error getting cloud drive specs")

		deviceSpec := driveSpecs[0]
		devicespecjournal := deviceSpec + " --journal"
		if dmthinEnabled {
			err := Inst().V.AddCloudDrive(nodeDetail, devicespecjournal, -1)
			dash.VerifyFatal(err != nil, true, "Did not Error out when adding cloud drive as expected")
			re := regexp.MustCompile(".*Journal parameter is not supported with PX-StoreV2*")
			dash.VerifyFatal(re.MatchString(fmt.Sprintf("%v", err)),
				true,
				fmt.Sprintf("Errored while adding Pool as expected on Node [%v] with err: %v", nodeDetail.Name, err))
		} else {

			err = Inst().V.EnterPoolMaintenance(*nodeDetail)
			log.FailOnError(err, "Error Entering Maintenance mode on Node[%v]", nodeDetail.Name)
			log.InfoD("Enter pool Maintenance mode ")
			expectedStatus := "In Maintenance"

			defer exitPoolMaintenance()

			log.FailOnError(WaitForPoolStatusToUpdate(*nodeDetail, expectedStatus),
				fmt.Sprintf("node %s pools are not in status %s", nodeDetail.Name, expectedStatus))

			//Wait for 7 min to bring up the portworx daemon before trying cloud drive add
			time.Sleep(7 * time.Minute)
			isjournal, err := Inst().V.GetJournalDevicePath(nodeDetail)
			log.FailOnError(err, "Error getting journal status")
			if isjournal != "" {
				devicespecjournal := deviceSpec + " --journal"
				err = Inst().V.AddCloudDrive(nodeDetail, devicespecjournal, -1)
				if err == nil {
					log.FailOnError(fmt.Errorf("adding cloud drive with journal expected ? Error: [%v]", err),
						"adding cloud drive with journal failed ?")
				}
				log.InfoD("adding journal failed as expected. verifying the error")
				re := regexp.MustCompile(".*journal exists*")
				re1 := regexp.MustCompile(".*Journal device.*is already configured*")
				dash.VerifyFatal(re.MatchString(fmt.Sprintf("%v", err)) || re1.MatchString(fmt.Sprintf("%v", err)),
					true,
					fmt.Sprintf("Errored while adding Pool as expected on Node [%v]", nodeDetail.Name))
			} else {
				systemOpts := node.SystemctlOpts{
					ConnectionOpts: node.ConnectionOpts{
						Timeout:         2 * time.Minute,
						TimeBeforeRetry: defaultRetryInterval,
					},
					Action: "start",
				}
				drivesMap, err := Inst().N.GetBlockDrives(*nodeDetail, systemOpts)
				log.FailOnError(err, "error getting block drives from node %s", nodeDetail.Name)
				blockDeviceBefore := len(drivesMap)
				devicespecjournal := deviceSpec + " --journal"
				err = Inst().V.AddCloudDrive(nodeDetail, devicespecjournal, -1)
				log.FailOnError(err, "journal add failed")
				drivesMap, err = Inst().N.GetBlockDrives(*nodeDetail, systemOpts)
				log.FailOnError(err, "error getting block drives from node %s", nodeDetail.Name)
				blockDeviceAfter := len(drivesMap)
				dash.VerifyFatal(blockDeviceBefore+1 == blockDeviceAfter, true, "adding cloud drive as journal successful")
				isjournal, err := IsJournalEnabled()
				log.FailOnError(err, "Error getting journal status")
				dash.VerifyFatal(isjournal, true, "journal device added successfully")
				err = Inst().V.RefreshDriverEndpoints()
				log.FailOnError(err, "error refreshing driver end points")
			}
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Exit from Maintenance mode if Pool is still in Maintenance")
		log.FailOnError(ExitNodesFromMaintenanceMode(), "exit from maintenance mode failed?")
		AfterEachTest(contexts, testrailID, runID)
	})

})

func waitTillVolumeStatusUp(vol *volume.Volume) error {
	now := 20 * time.Minute
	targetTime := time.After(now)
	for {
		select {
		case <-targetTime:
			return fmt.Errorf("timeout reached waiting for volume status")
		default:
			log.InfoD("Validating Volume Status of Volume [%v]", vol.ID)
			status, err := IsVolumeStatusUP(vol)
			if err != nil {
				return err
			}
			if status == true {
				return nil
			}
		}
	}
}

var _ = Describe("{ReplResyncOnPoolExpand}", func() {
	/*
		PTX-15696 -> PWX-26967
		Deploy IO aggressive application using repl-2 volumes
		Identify the pools of this volume
		Invoke pool expand is one pool of this volume and wait for pool expand to be completed
		Volume status will be degraded when pool expand is going on for one of the pool
		Volume repl resync should not fail after pool expand is done (This behavior after fix)
	*/
	JustBeforeEach(func() {
		StartTorpedoTest("ReplResyncOnPoolExpand",
			"Resync failed for a volume after pool came out of rebalance",
			nil, 0)
	})

	stepLog := "Resync volume after rebalance"
	It(stepLog, func() {

		contexts = make([]*scheduler.Context, 0)
		currAppList := Inst().AppList

		revertAppList := func() {
			Inst().AppList = currAppList
		}
		defer revertAppList()

		Inst().AppList = []string{}
		var ioIntensiveApp = []string{"fio", "fio-writes"}

		for _, eachApp := range ioIntensiveApp {
			Inst().AppList = append(Inst().AppList, eachApp)
		}
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("replresyncpoolexpand-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get a pool with running IO
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		// Get Node Details of the Pool with IO
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)
		log.InfoD("Pool with UUID [%v] present in Node [%v]", poolUUID, nodeDetail.Name)

		// Get All Volumes from the pool
		volumes, err := GetVolumesFromPoolID(contexts, poolUUID)
		log.FailOnError(err, "Failed to get list of volumes from the poolIDs")

		// Change replication factor to 2 on all the volumes
		volumeReplicaMap := make(map[string]int)
		revertReplica := func() {
			for _, eachvol := range volumes {
				for volName, replcount := range volumeReplicaMap {
					if eachvol.Name == volName {
						getReplicaSets, err := Inst().V.GetReplicaSets(eachvol)
						log.FailOnError(err, "Failed to get replication factor on the volume")
						if len(getReplicaSets[0].Nodes) != replcount {
							err := Inst().V.SetReplicationFactor(eachvol, 2, nil, nil, true)
							log.FailOnError(err, "failed to set replication value of Volume [%v]", volName)
						}
					}
				}
			}
		}

		defer revertReplica()
		for _, eachVol := range volumes {
			getReplicaSets, err := Inst().V.GetReplicaSets(eachVol)
			log.FailOnError(err, "Failed to get replication factor on the volume")
			volumeReplicaMap[eachVol.Name] = len(getReplicaSets[0].Nodes)

			if len(getReplicaSets[0].Nodes) != 2 {
				err := Inst().V.SetReplicationFactor(eachVol, 2, nil, nil, true)
				if err != nil {
					log.FailOnError(err, "failed to set replicaiton for Volume [%v]", eachVol.Name)
				}
			}
		}

		// Wait for some time for ingest to continue and add up some more data to it
		time.Sleep(10 * time.Minute)

		// Invoke pool expand in one pool of this volume and wait for pool expand to be completed
		poolToBeResized, err := GetStoragePoolByUUID(poolUUID)
		log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID [%s]", poolUUID))
		expectedSize := (poolToBeResized.TotalSize / units.GiB) + 100

		expandType := api.SdkStoragePool_RESIZE_TYPE_ADD_DISK
		if !IsPoolAddDiskSupported() {
			expandType = api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK
		}

		log.InfoD("Current Size of the pool %s is %d", poolUUID, poolToBeResized.TotalSize/units.GiB)
		err = Inst().V.ExpandPool(poolUUID, expandType, expectedSize, true)
		dash.VerifyFatal(err, nil, "Pool expansion init successful?")

		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "Failed to check if Journal enabled")

		resizeErr := waitForPoolToBeResized(expectedSize, poolUUID, isjournal)
		dash.VerifyFatal(resizeErr, nil,
			fmt.Sprintf("Verify pool %s on expansion using auto option", poolUUID))

		log.Info("Checking for each volumes status is up")
		for _, eachVol := range volumes {
			log.FailOnError(waitTillVolumeStatusUp(eachVol), "failed to get volume status UP")
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

// Volume replication change
var _ = Describe("{VolumeHAPoolOpsNoKVDBleaderDown}", func() {
	var testrailID = 0
	// Do multiple pool operations on the pool and volume and make sure kvdb leader is up and running
	// JIRA ID :https://portworx.atlassian.net/browse/PTX-17728
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("VolumeHAPoolOpsNoKVDBleaderDown",
			"Test Volume HA Pool Operations should not make KVDB node down", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "has to schedule apps and update replication factor for attached node"
	It(stepLog, func() {
		var wg sync.WaitGroup
		numGoroutines := 2

		wg.Add(numGoroutines)

		volumesCreated := []string{}

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("volumepooloperations-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get Pool with running IO on the cluster
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		terminate := false
		stopRoutine := func() {
			if !terminate {
				terminate = true
				time.Sleep(1 * time.Minute) // Wait for 1 min to settle down all other go routines to terminate
				for _, each := range volumesCreated {
					if IsVolumeExits(each) {
						log.FailOnError(Inst().V.DeleteVolume(each), "volume deletion failed on the cluster with volume ID [%s]", each)
					}

				}

			}
		}

		defer stopRoutine()

		// Wait for KVDB Nodes up and running and in healthy state
		// Go routine to kill kvdb master in regular intervals
		go func() {
			defer wg.Done()
			defer GinkgoRecover()
			for {
				if terminate {
					break
				}
				err := WaitForKVDBMembers()
				if err != nil {
					stopRoutine()
					log.FailOnError(err, "not all kvdb members in healthy state")
				}
				// Wait for some time after killing kvdb master Node
				time.Sleep(5 * time.Minute)
			}
		}()

		doPoolOperations := func() error {

			poolToBeResized, err := GetStoragePoolByUUID(poolUUID)
			if err != nil {
				return err
			}

			expectedSize := (poolToBeResized.TotalSize / units.GiB) + 10
			log.InfoD("Current Size of the pool %s is %d", poolUUID, poolToBeResized.TotalSize/units.GiB)

			poolResizeType := []api.SdkStoragePool_ResizeOperationType{api.SdkStoragePool_RESIZE_TYPE_AUTO,
				api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK}

			//Remove it once SdkStoragePool_RESIZE_TYPE_ADD_DISK is supported in dmthin
			if dmthin, err := IsDMthin(); err == nil {
				if !dmthin {
					poolResizeType = append(poolResizeType, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK)
				}
			}
			randomIndex := rand.Intn(len(poolResizeType))
			pickType := poolResizeType[randomIndex]
			log.InfoD("Current Size of the pool %s is %d", poolUUID, poolToBeResized.TotalSize/units.GiB)
			log.InfoD("Expanding Pool [%v] using resize type [%v]", poolUUID, pickType)
			err = Inst().V.ExpandPool(poolUUID, pickType, expectedSize, true)
			if err != nil {
				return err
			}

			isjournal, err := IsJournalEnabled()
			if err != nil {
				return err
			}

			resizeErr := waitForPoolToBeResized(expectedSize, poolUUID, isjournal)
			if resizeErr != nil {
				return resizeErr
			}

			return nil
		}

		doVolumeOperations := func() {
			defer wg.Done()
			defer GinkgoRecover()
			for {
				if terminate {
					break
				}
				uuidObj := uuid.New()
				VolName := fmt.Sprintf("volume_%s", uuidObj.String())
				Size := uint64(rand.Intn(10) + 1)   // Size of the Volume between 1G to 10G
				haUpdate := int64(rand.Intn(3) + 1) // Size of the HA between 1 and 3

				volId, err := Inst().V.CreateVolume(VolName, Size, int64(haUpdate))
				log.FailOnError(err, "volume creation failed on the cluster with volume name [%s]", VolName)
				log.InfoD("Volume created with name [%s] having id [%s]", VolName, volId)

				volumesCreated = append(volumesCreated, volId)

				// HA Update on the volume
				_, err = Inst().V.InspectVolume(volId)
				log.FailOnError(err, "Failed to inspect volume [%s]", VolName)

				for _, eachVol := range volumesCreated {
					if len(volumesCreated) > 5 {
						_, err = Inst().V.AttachVolume(eachVol)
						if err != nil {
							stopRoutine()
							log.FailOnError(err, "attach volume with volume ID failed [%s]", eachVol)
						}

						err = Inst().V.DetachVolume(eachVol)
						if err != nil {
							stopRoutine()
							log.FailOnError(err, "detach volume with volume ID failed [%s]", eachVol)
						}

						time.Sleep(5 * time.Second)
						// Delete the Volume
						err = Inst().V.DeleteVolume(eachVol)
						if err != nil {
							stopRoutine()
							log.FailOnError(err, "failed to delete volume with volume ID [%s]", eachVol)
						}

						// Remove the first element
						for i := 0; i < len(volumesCreated)-1; i++ {
							volumesCreated[i] = volumesCreated[i+1]
						}
						// Resize the array by truncating the last element
						volumesCreated = volumesCreated[:len(volumesCreated)-1]
					}
					if terminate {
						break
					}
				}

			}
		}

		go doVolumeOperations()
		// Do pool resize continuously for 20 times when volume operation in progress
		for iteration := 0; iteration <= 5; iteration++ {
			err := doPoolOperations()
			if err != nil {
				stopRoutine()
				wg.Wait()
				log.FailOnError(err, "error seen during pool operations")
			}
			if terminate {
				break
			}
		}
		stopRoutine()
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})

// Volume replication change
var _ = Describe("{KvdbFailoverDuringPoolExpand}", func() {

	var (
		testrailID = 0
		// JIRA ID :https://portworx.atlassian.net/browse/PTX-17728
		runID    int
		contexts = make([]*scheduler.Context, 0)
	)

	JustBeforeEach(func() {
		StartTorpedoTest("KvdbFailoverDuringPoolExpand",
			"KVDB failover during pool expand", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "KVDB failover during pool expand"
	It(stepLog, func() {
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("volumepooloperations-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get a pool with running IO
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		// Get Node Details of the Pool with IO
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)
		log.InfoD("Pool with UUID [%v] present in Node [%v]", poolUUID, nodeDetail.Name)

		poolResizeType := []api.SdkStoragePool_ResizeOperationType{api.SdkStoragePool_RESIZE_TYPE_AUTO,
			api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK}

		//Remove it once SdkStoragePool_RESIZE_TYPE_ADD_DISK is supported in dmthin
		if dmthin, err := IsDMthin(); err == nil {
			if !dmthin {
				poolResizeType = append(poolResizeType, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK)
			}
		}

		poolToBeResized, err := GetStoragePoolByUUID(poolUUID)
		if err != nil {
			log.FailOnError(err, "Failed to pool details to be resized from pool uuid [%s]", poolUUID)
		}

		randomIndex := rand.Intn(len(poolResizeType))
		pickType := poolResizeType[randomIndex]

		expandPoolWithKVDBFailover := func(poolUUID string) error {

			expectedSize := (poolToBeResized.TotalSize / units.GiB) + 200
			log.InfoD("Current Size of the pool %s is %d", poolUUID, poolToBeResized.TotalSize/units.GiB)

			err = Inst().V.ExpandPool(poolUUID, pickType, expectedSize, true)
			if err != nil {
				return err
			}

			err = WaitForExpansionToStart(poolUUID)
			if err != nil {
				return err
			}

			isjournal, err := IsJournalEnabled()
			if err != nil {
				return err
			}

			err = KillKvdbMasterNodeAndFailover()
			if err != nil {
				return err
			}

			resizeErr := waitForPoolToBeResized(expectedSize, poolUUID, isjournal)
			if resizeErr != nil {
				return resizeErr
			}

			return nil
		}
		log.FailOnError(expandPoolWithKVDBFailover(poolUUID), "pool expand with kvdb failover failed")

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})
var _ = Describe("{KvdbRestartNewNodeAcquired}", func() {
	/*
		PTX-15696 -> PWX-26967
		Deploy IO aggressive application using repl-2 volumes
		Identify the pools of this volume
		Invoke pool expand is one pool of this volume and wait for pool expand to be completed
		Volume status will be degraded when pool expand is going on for one of the pool
		Volume repl resync should not fail after pool expand is done (This behavior after fix)
	*/
	JustBeforeEach(func() {
		StartTorpedoTest("KvdbRestartNewNodeAcquired",
			"Shutdown the KVDB leader node and wait for third copy to be created",
			nil, 0)
	})

	var contexts []*scheduler.Context
	stepLog := "Resync volume after rebalance"
	It(stepLog, func() {
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("kvdbrestartnewnodeacquired-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		killType := []string{"reboot", "kill"}

		if len(node.GetStorageNodes()) <= 3 {
			log.FailOnError(fmt.Errorf("test needs minimum of 4 storage nodes for kvdb failover"), "required nodes present?")
		}

		for _, eachType := range killType {
			allKvdbNodes, err := GetAllKvdbNodes()
			log.FailOnError(err, "failed to get list of kvdb nodes")

			dash.VerifyFatal(len(allKvdbNodes) == 3, true,
				fmt.Sprintf("all kvdb nodes are not up available total kvdb nodes [%v]", len(allKvdbNodes)))

			masterNode, err := GetKvdbMasterNode()
			log.FailOnError(err, "failed to get the master node ip")
			log.Infof("kvdb master node is [%v]", masterNode.Name)

			if eachType == "kill" {
				log.FailOnError(KillKvdbMemberUsingPid(*masterNode), "failed to kill kvdb master node")
			} else {
				err = RebootNodeAndWaitForPxUp(*masterNode)
				log.FailOnError(err, "Failed to reboot node and wait till it is up")
			}
			masterNodeAfterKill, err := GetKvdbMasterNode()
			log.FailOnError(err, "failed to get the master node ip")

			log.Infof("kvdb master node is [%v]", masterNodeAfterKill.Name)
			dash.VerifyFatal(masterNode.Name == masterNodeAfterKill.Name, false,
				"master node ip is same before and after masternode kill?")

			allKvdbNodes, err = GetAllKvdbNodes()
			log.FailOnError(err, "failed to get list of kvdb nodes")
			dash.VerifyFatal(len(allKvdbNodes) == 3, true,
				fmt.Sprintf("all kvdb nodes are not up available total kvdb nodes [%v]", len(allKvdbNodes)))

		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

// ExpandMultiplePoolsInParallel expands provided poolIDs in parallel based on the expandType provided
// E.x : poolIds := [f724fb7f-9a43-4df2-bc38-550841fc3bfc, 492a3d03-cc47-4a8c-a8f0-d1d92dfdf25f]
//
//	size := 10
//	expandType := [api.SdkStoragePool_RESIZE_TYPE_AUTO]
//			     or  [api.SdkStoragePool_RESIZE_TYPE_ADD_DISK]
//			     or  [api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK]
func ExpandMultiplePoolsInParallel(poolIds []string, expandSize uint64, expandType []api.SdkStoragePool_ResizeOperationType) (*sync.WaitGroup, error) {
	var wg sync.WaitGroup
	numGoroutines := len(poolIds)

	wg.Add(numGoroutines)
	for _, eachPool := range poolIds {
		poolResizeType := expandType

		randomIndex := rand.Intn(len(poolResizeType))
		pickType := poolResizeType[randomIndex]
		go func(poolUUID string, expandSize uint64) {
			defer wg.Done()
			defer GinkgoRecover()
			poolToBeResized, err := GetStoragePoolByUUID(poolUUID)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID [%s]", poolUUID))

			expectedSize := (poolToBeResized.TotalSize / units.GiB) + expandSize
			log.InfoD("Current Size of the pool %s is %d", poolUUID, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolUUID, pickType, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			resizeErr := waitForPoolToBeResized(expectedSize, poolUUID, isjournal)
			dash.VerifyFatal(resizeErr, nil,
				fmt.Sprintf("Verify pool %s on expansion using auto option", poolUUID))

		}(eachPool, expandSize)

	}
	return &wg, nil
}

var _ = Describe("{ExpandMultiplePoolWithIOsInClusterAtOnce}", func() {
	/*
			test to expand multiple pool at once in parallel
		    Pick a Pool from each Storage Node and expand all the node in parallel
	*/
	JustBeforeEach(func() {
		StartTorpedoTest("ExpandMultiplePoolWithIOsInClusterAtOnce",
			"Expand multiple pool in the cluster at once in parallel",
			nil, 0)
	})

	var contexts []*scheduler.Context
	stepLog := "Expand multiple pool in the cluster at once in parallel"
	It(stepLog, func() {
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("expandmultiplepoolparallel-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		poolIdsToExpand := []string{}
		for _, eachNodes := range node.GetStorageNodes() {
			poolsPresent, err := GetPoolWithIOsInGivenNode(eachNodes, contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
			if err == nil {
				poolIdsToExpand = append(poolIdsToExpand, poolsPresent.Uuid)
			} else {
				log.InfoD("Errored while getting Pool IDs , ignoring for now ...")
			}
		}
		dash.VerifyFatal(len(poolIdsToExpand) > 0, true,
			fmt.Sprintf("No pools with IO present ?"))

		expandType := []api.SdkStoragePool_ResizeOperationType{api.SdkStoragePool_RESIZE_TYPE_ADD_DISK}
		if !IsPoolAddDiskSupported() {
			expandType = []api.SdkStoragePool_ResizeOperationType{api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK}
		}
		wg, err := ExpandMultiplePoolsInParallel(poolIdsToExpand, 100, expandType)
		dash.VerifyFatal(err, nil, "Pool expansion in parallel failed")

		wg.Wait()
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

// CreateNewPoolsOnMultipleNodesInParallel Create New Pools in parallel on the cluster
func CreateNewPoolsOnMultipleNodesInParallel(nodes []node.Node) error {
	var wg sync.WaitGroup

	poolList := make(map[string]int)
	poolListAfterCreate := make(map[string]int)

	isDmthin, err := IsDMthin()
	log.FailOnError(err, "failed to check if Node is DMThin ")

	for _, eachNode := range nodes {
		pools, _ := GetPoolsDetailsOnNode(&eachNode)
		log.InfoD("Length of pools present on Node [%v] =  [%v]", eachNode.Name, len(pools))
		poolList[eachNode.Name] = len(pools)
	}

	log.InfoD("Pool Details and total pools present [%v]", poolList)

	wg.Add(len(nodes))
	for _, eachNode := range nodes {
		go func(eachNode node.Node) {
			defer wg.Done()
			defer GinkgoRecover()
			log.InfoD("Adding cloud drive on Node [%v]", eachNode.Name)

			// Add Metadata Node if the provided node is not Storage Node
			if isDmthin && !node.IsStorageNode(eachNode) {
				log.FailOnError(AddMetadataDisk(eachNode), "Failed to add metadata disk to the node ")
			}

			err := AddCloudDrive(eachNode, -1)
			log.FailOnError(err, "adding cloud drive failed on Node [%v]", eachNode)
		}(eachNode)
	}
	wg.Wait()

	err = Inst().V.RefreshDriverEndpoints()
	log.FailOnError(err, "error refreshing driver end points")

	for _, eachNode := range nodes {
		pools, _ := GetPoolsDetailsOnNode(&eachNode)
		log.InfoD("Length of pools present on Node [%v] =  [%v]", eachNode.Name, len(pools))
		poolListAfterCreate[eachNode.Name] = len(pools)
	}
	log.InfoD("Pool Details and total pools present [%v]", poolListAfterCreate)

	for pool, poolCount := range poolList {
		if poolListAfterCreate[pool] <= poolList[pool] {
			return fmt.Errorf("NewPool didnot create on Node. Available pool length is [%v]", poolCount)
		}
	}
	return nil
}

var _ = Describe("{CreateNewPoolsOnClusterInParallel}", func() {
	/*
				Create new pools on the cluster in parallel
			    https://portworx.atlassian.net/browse/PTX-17614

				Priority : P0

		        Test legacy Drive Add to multiple pools at the same time
				for Automation : Trying to add Drives using legacy method to create new pools on all the nodes in the cluster
	*/
	JustBeforeEach(func() {
		StartTorpedoTest("CreateNewPoolsOnClusterInParallel",
			"create new pools on the cluster in parallel",
			nil, 0)
	})

	var contexts []*scheduler.Context
	stepLog := "create new pools on the cluster in parallel"
	It(stepLog, func() {

		var nodesToUse []node.Node

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("createnewpoolsinparallel-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		getNodes := node.GetNodes()
		for _, each := range getNodes {
			if node.IsMasterNode(each) == false {
				sPools, err := GetPoolsDetailsOnNode(&each)
				if err != nil {
					fmt.Printf("[%v]", err)
				}
				if len(sPools) < 8 {
					nodesToUse = append(nodesToUse, each)
				}
			}
		}
		err := CreateNewPoolsOnMultipleNodesInParallel(nodesToUse)
		log.FailOnError(err, "error adding cloud drives in parallel")

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{AddDriveMetadataPool}", func() {
	/*
				Create new pools on the cluster in parallel
			    https://portworx.atlassian.net/browse/PTX-17616

				Priority : P0

		        Test Add Drive to Metadata Pool
				for Automation : for automation we try only expand using add-disk option on the pool
	*/
	JustBeforeEach(func() {
		StartTorpedoTest("AddDriveMetadataPool",
			"Test Add Drive to Metadata Pool",
			nil, 0)
	})

	stepLog := "Test Add Drive to Metadata Pool"
	It(stepLog, func() {

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("adddrivemetadatapool-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get Pool with running IO on the cluster
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		// Get Node Details of the Pool with IO
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)
		log.InfoD("Pool with UUID [%v] present in Node [%v]", poolUUID, nodeDetail.Name)

		// Get metadata poolUUID from the Node
		poolUUID, err = GetPoolUUIDWithMetadataDisk(*nodeDetail)
		log.FailOnError(err, "Failed to get metadata pool uuid on Node [%v]", nodeDetail.Name)

		poolToBeResized, err := GetStoragePoolByUUID(poolUUID)
		log.FailOnError(err, "Failed to get pool using UUID [%s]", poolUUID)

		drvSize, err := getPoolDiskSize(poolToBeResized)
		log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
		expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "Failed to check if Journal enabled")
		log.InfoD("Current Size of the pool [%s] is [%d]", poolUUID, expectedSize)

		alertType := api.SdkStoragePool_RESIZE_TYPE_AUTO
		// Now trying to Expand Pool with Invalid Pool UUID
		err = Inst().V.ExpandPoolUsingPxctlCmd(*nodeDetail, poolUUID,
			alertType, expectedSize, false)
		if err != nil && strings.Contains(fmt.Sprintf("%v", err), "Please re-issue expand with force") {
			err = Inst().V.ExpandPoolUsingPxctlCmd(*nodeDetail, poolUUID,
				alertType, expectedSize, true)
		}
		resizeErr := waitForPoolToBeResized(expectedSize, poolUUID, isjournal)
		dash.VerifyFatal(resizeErr, nil,
			fmt.Sprintf("Verify pool %s on expansion using auto option", poolUUID))

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

func scheduleApps() []*scheduler.Context {
	contexts := make([]*scheduler.Context, 0)
	for i := 0; i < Inst().GlobalScaleFactor; i++ {
		log.Infof("Deploy app %v", i)
		contexts = append(contexts, ScheduleApplications(
			fmt.Sprintf("pooltest-%d", i))...)
	}
	ValidateApplications(contexts)
	return contexts
}

func pickPoolToResize(contexts []*scheduler.Context, expandType api.SdkStoragePool_ResizeOperationType, targetIncrementInGiB uint64, excludeNodeIDs ...string) string {
	poolsWithIO, err := GetPoolIDWithIOs(contexts)

	if err != nil {
		log.Warnf("Error identifying pool with IOs, Errot: %v", err)
	}

	for _, poolID := range poolsWithIO {
		log.Infof("checking pool expansion eliginblity of pool [%s] with IOs", poolID)
		n, err := GetNodeWithGivenPoolID(poolID)
		if err != nil {
			continue
		}
		eligibilityMap, err := GetPoolExpansionEligibility(n, expandType, targetIncrementInGiB)
		if err != nil {
			continue
		}
		if eligibilityMap[n.Id] && eligibilityMap[poolID] {
			return poolID
		}

	}
	log.Warnf("No pool with IO found, picking a random pool in use to resize")
	poolIDsInUseByTestingApp, err := GetPoolsInUse()
	failOnError(err, "Error identifying pool to run test")
	verifyArrayNotEmpty(poolIDsInUseByTestingApp, "Found no pool used by persistent volumes. ")
	rand.Seed(time.Now().UnixNano())
	updatedPoolIDs := make([]string, 0)
	if len(excludeNodeIDs) > 0 {
		log.Infof("Filtering out pools belonging to nodes [%v]", excludeNodeIDs)
		for _, poolID := range poolIDsInUseByTestingApp {
			n, err := GetNodeWithGivenPoolID(poolID)
			failOnError(err, "failed to get node details from PoolUUID [%v]", poolID)
			if !slices.Contains(excludeNodeIDs, n.Id) {
				updatedPoolIDs = append(updatedPoolIDs, poolID)
			} else {
				log.Infof("Excluding pool [%s] from resize as it is on node [%s]", poolID, n.Id)
			}
		}
	}
	if len(updatedPoolIDs) == 0 {
		updatedPoolIDs = append(updatedPoolIDs, poolIDsInUseByTestingApp...)
	}
	poolsForExpand := make([]string, 0)
	for _, poolID := range updatedPoolIDs {
		n, err := GetNodeWithGivenPoolID(poolID)
		failOnError(err, "failed to get node details from PoolUUID [%v]", poolID)
		eligibilityMap, err := GetPoolExpansionEligibility(n, expandType, targetIncrementInGiB)
		if err != nil {
			log.Warnf("Error identifying pool expansion eligibility, Error: %v", err)
			continue
		}
		if eligibilityMap[n.Id] && eligibilityMap[poolID] {
			poolsForExpand = append(poolsForExpand, poolID)
		} else {
			log.Infof("Excluding pool [%s] from resize as it is on node [%s] as it is not eligible for expansion", poolID, n.Id)
		}

	}
	var poolIDToResize string
	if len(poolsForExpand) > 0 {
		poolIDToResize = poolsForExpand[rand.Intn(len(poolsForExpand))]
	}
	return poolIDToResize
}

func getRandomNodeWithPoolIOs(contexts []*scheduler.Context) (node.Node, error) {
	// pick a storage node with pool having IOs
	poolID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
	n, err := GetNodeWithGivenPoolID(poolID)
	return *n, err
}

func getStoragePool(poolIDToResize string) *api.StoragePool {
	pool, err := GetStoragePoolByUUID(poolIDToResize)
	failOnError(err, "Failed to get pool using UUID %s", poolIDToResize)
	dash.VerifyFatal(pool != nil, true, "found pool to resize")
	return pool
}

func failOnError(err error, message string, args ...interface{}) {
	log.FailOnError(err, message, args...)
}

func verifyNonEmpty(value string, message string) {
	dash.VerifyFatal(len(value) > 0, true, message)
}

func verifyArrayNotEmpty(values []string, message string) {
	dash.VerifyFatal(len(values) > 0, true, message)
}
func triggerPoolExpansion(poolIDToResize string, targetSizeGiB uint64, expandType api.SdkStoragePool_ResizeOperationType) {
	stepLog := "Trigger pool expansion"
	Step(stepLog, func() {
		log.InfoD(stepLog)
		isDMthin, _ := IsDMthin()
		err := Inst().V.ExpandPool(poolIDToResize, expandType, targetSizeGiB, true)
		if isDMthin && expandType == api.SdkStoragePool_RESIZE_TYPE_ADD_DISK {
			dash.VerifyFatal(err != nil, true,
				"Pool expansion request of add-disk type should be rejected with dmthin")
			dash.VerifyFatal(strings.Contains(err.Error(), "add-drive type expansion is not supported with px-storev2"), true, fmt.Sprintf("check error message: %v", err.Error()))
		} else {
			dash.VerifyFatal(err, nil, "pool expansion requested successfully")
		}
	})
}
func waitForOngoingPoolExpansionToComplete(poolIDToResize string) error {
	currentLastMsg := ""
	f := func() (interface{}, bool, error) {
		expandedPool, err := GetStoragePoolByUUID(poolIDToResize)
		if err != nil {
			return nil, true, fmt.Errorf("error getting pool by using id %s", poolIDToResize)
		}
		if expandedPool == nil {
			return nil, false, fmt.Errorf("pool to expand not found")
		}
		if expandedPool.LastOperation == nil {
			return nil, false, fmt.Errorf("no pool resize operation in progress")
		}
		log.Infof("Pool Resize Status: %v, Message : %s", expandedPool.LastOperation.Status, expandedPool.LastOperation.Msg)
		switch expandedPool.LastOperation.Status {
		case api.SdkStoragePool_OPERATION_SUCCESSFUL:
			return nil, false, nil
		case api.SdkStoragePool_OPERATION_FAILED:
			return nil, false, fmt.Errorf("pool %s expansion failed: %s", poolIDToResize, expandedPool.LastOperation)
		case api.SdkStoragePool_OPERATION_PENDING:
			return nil, true, fmt.Errorf("pool %s expansion is pending", poolIDToResize)
		case api.SdkStoragePool_OPERATION_IN_PROGRESS:
			if strings.Contains(expandedPool.LastOperation.Msg, "Rebalance in progress") {
				if currentLastMsg == expandedPool.LastOperation.Msg {
					return nil, true, fmt.Errorf("pool rebalance is not progressing")
				}
				currentLastMsg = expandedPool.LastOperation.Msg
				return nil, true, fmt.Errorf("wait for pool rebalance to complete")
			}
			fallthrough
		default:
			return nil, true, fmt.Errorf("waiting for pool status to update")
		}
	}

	_, err := task.DoRetryWithTimeout(f, poolResizeTimeout, poolExpansionStatusCheckInterval)
	n, terr := GetNodeWithGivenPoolID(poolIDToResize)
	if terr == nil {
		PrintSvPoolStatus(*n)
	} else {
		log.Warnf("error getting node for pool uuid [%s]", poolIDToResize)
	}

	return err
}

func verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize string, targetSizeGiB uint64) {
	Step("Verify that pool has been expanded to the expected size", func() {
		resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
		failOnError(err, "Failed to get pool using UUID %s", poolIDToResize)
		newPoolSizeGiB := resizedPool.TotalSize / units.GiB
		dash.VerifyFatal(newPoolSizeGiB >= targetSizeGiB, true,
			fmt.Sprintf("Expected pool to have been expanded to %v GiB, but got %v GiB", targetSizeGiB, newPoolSizeGiB))
	})
}

var _ = Describe("{PoolExpandRebalanceShutdownNode}", func() {
	/*
		1. create one pool
		2. run ios
		3. expand pool to higher size
		4. while it is in progress shutdown and restart the node
		5. let rebalance continue.
		6. check if pending operation continues
	*/

	var testrailID = 0
	// Testrail Description : while pool expand Rebalance is in progress ShutdownNode and check operation resumes
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("PoolExpandRebalanceShutdownNode",
			"while pool is expanding shutdown and poweron and check operation resumes",
			nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "while pool is expanding shutdown and poweron and check operation resumes"
	It(stepLog, func() {
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rebalanceshutdown-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get Pool with running IO on the cluster
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		// Get Node Details of the Pool with IO
		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)
		log.InfoD("Pool with UUID [%v] present in Node [%v]", poolUUID, nodeDetail.Name)

		if IsLocalCluster(*nodeDetail) != true {
			log.FailOnError(fmt.Errorf("This test will only support onprem vms"), "is this onprem?")
		}
		// Get Total Pools present on the Node present
		poolDetails, err := GetPoolsDetailsOnNode(nodeDetail)
		n1, err := node.GetNodeByName(nodeDetail.Name)
		log.FailOnError(err, "Failed to get Pool Details from Node [%v]", nodeDetail.Name)
		log.InfoD("List of Pools present in the node [%v]", poolDetails)
		nodeDetail = &n1
		log.FailOnError(err, "Failed to get Node Details using name [%v]", nodeDetail.Name)

		poolToBeResized, err := GetStoragePoolByUUID(poolDetails[0].Uuid)
		drvSize, err := getPoolDiskSize(poolToBeResized)
		log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized)
		//getting the eligible pools of the node to initiate expansion
		eligibility, err := GetPoolExpansionEligibility(nodeDetail, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		log.FailOnError(err, "error checking node [%s] expansion criteria", nodeDetail.Name)
		if !eligibility[poolUUID] {
			log.FailOnError(fmt.Errorf("cannot add drive to the pool selected %s as it is full and not eligible for expansion through drive addition", poolUUID), "cannot add drive into the pool")
		}

		expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize
		err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
		log.FailOnError(err, "error while doing pool expand using add drive on pool [%s]", poolToBeResized)
		expandedPool, err := GetStoragePoolByUUID(poolToBeResized.Uuid)
		log.FailOnError(err, "error getting pool by using uuid %s", poolToBeResized.Uuid)
		dash.VerifyFatal(expandedPool == nil, false, fmt.Sprintf("Pool selected for expansion is %s(UUID of pool)", expandedPool.Uuid))
		err = WaitForExpansionToStart(poolToBeResized.Uuid)
		log.FailOnError(err, "error when waiting for pool expansion on pool %s", poolToBeResized.Uuid)
		var connect node.ConnectionOpts
		connect.Timeout = 60
		connect.TimeBeforeRetry = 10
		err = Inst().N.ShutdownNode(*nodeDetail, node.ShutdownNodeOpts{
			Force:          true,
			ConnectionOpts: connect,
		})
		log.FailOnError(err, "failed to shutdown the node %s", nodeDetail.Name)
		time.Sleep(300 * time.Second)
		log.InfoD("sleeping for 5 mins to wait for shutdown to be completed")
		t := func() (interface{}, bool, error) {
			err = Inst().N.PowerOnVM(*nodeDetail)
			if err != nil {
				return nil, false, err
			}
			return nil, true, err
		}

		_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second)
		log.FailOnError(err, "Failed to powered on the vm on Node %s", nodeDetail.Name)
		isjournal, err := IsJournalEnabled()
		log.FailOnError(err, "Failed to check if Journal enabled")
		validatePXStartTimeout := 15 * time.Minute
		err = Inst().V.WaitDriverUpOnNode(*nodeDetail, validatePXStartTimeout)
		log.FailOnError(err, "timedout when waiting for node %s to be up", nodeDetail.Name)
		poolStatus, err := getPoolLastOperation(expandedPool.Uuid)
		log.FailOnError(err, "Failed to get last operation on pool %s", expandedPool.Uuid)
		if poolStatus.Status == api.SdkStoragePool_OPERATION_FAILED {
			log.FailOnError(fmt.Errorf("Failed last operation with msg %s", poolStatus.Msg), "Failed on operation after reboot")
		}
		log.InfoD("after poweron the operation status is %v", poolStatus.Status)

		dash.VerifyFatal(poolStatus.Status != api.SdkStoragePool_OPERATION_FAILED, true, fmt.Sprintf("PoolResize is successful on pool %s", expandedPool.Uuid))
		resizeErr := waitForPoolToBeResized(expectedSize, poolUUID, isjournal)
		dash.VerifyFatal(resizeErr, nil,
			fmt.Sprintf("waiting for pool expansion to complete failed on pool %s", poolUUID))
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Exit from Maintenance mode if Pool is still in Maintenance")
		log.FailOnError(ExitNodesFromMaintenanceMode(), "exit from maintenance mode failed?")
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{AddDriveWithKernelPanic}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) Expand pool by adding cloud drives.
	//4) Inject kernel panic where the pool expansion is in-progress
	//5) Verify total pool count after addition of cloud drive with kernel panic

	var testrailID = 0
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("AddDriveWithKernelPanic", "Initiate pool expansion using add-drive and do kernel panic while it is in progress", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	stepLog := "should get the existing storage node and expand the pool by adding a drive"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pladddrvwrst-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get Pool with running IO on the cluster
		poolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 200)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		poolToBeResized, err := GetStoragePoolByUUID(poolUUID)
		log.FailOnError(err, "Failed to get Pool from Pool uuid [%v]", poolUUID)

		stNode, stNodeerr := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(stNodeerr, "Failed to get Node Details from PoolUUID [%v]", poolUUID)

		expectedSize := (poolToBeResized.TotalSize / units.GiB) + 200
		log.InfoD("Current Size of the Pool %s is %d", poolUUID, poolToBeResized.TotalSize/units.GiB)

		expanderr := Inst().V.ExpandPool(poolUUID, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
		log.FailOnError(expanderr, "Failed to initiate Expand on Pool [%v]", poolUUID)

		isjournal, journalerr := IsJournalEnabled()
		log.FailOnError(journalerr, "Failed to get Journal Disk Details")

		err = WaitForExpansionToStart(poolUUID)
		log.FailOnError(err, "Expansion is not started")

		cmd := "echo c > /proc/sysrq-trigger"

		// Execute the command to generate kernel panic
		log.Infof("Executing command on node, [%v]", stNode.Name)
		_, err = Inst().N.RunCommandWithNoRetry(*stNode, cmd, node.ConnectionOpts{
			Timeout:         2 * time.Minute,
			TimeBeforeRetry: 10 * time.Second,
		})

		re, _ := regexp.Compile(".*remote command exited without exit status or exit signal")
		regMatch := re.MatchString(fmt.Sprintf("%v", err))
		dash.VerifyFatal(regMatch, true, " force panic the node successful?")

		err = Inst().N.TestConnection(*stNode, node.ConnectionOpts{
			Timeout:         addDriveUpTimeOut,
			TimeBeforeRetry: 10 * time.Second,
		})
		log.FailOnError(err, fmt.Sprintf("Verify the Node %s connection is up?", stNode.Name))

		err = Inst().V.WaitDriverDownOnNode(*stNode)
		log.FailOnError(err, fmt.Sprintf("Verify the Node %s driver down and up?", stNode.Name))

		err = Inst().S.IsNodeReady(*stNode)
		log.FailOnError(err, fmt.Sprintf("Verify the Node %s is ready?", stNode.Name))

		err = Inst().V.WaitDriverUpOnNode(*stNode, addDriveUpTimeOut)
		log.FailOnError(err, fmt.Sprintf("Kernel Panic on Node %s", stNode.Name))
		log.InfoD("Validate pool rebalance after drive add and Kernel panic")

		resizeErr := waitForPoolToBeResized(expectedSize, poolUUID, isjournal)
		log.FailOnError(resizeErr, "Failed waiting for Pool Resize")

		err = ValidateDriveRebalance(*stNode)
		log.FailOnError(err, "Pool re-balance failed")
		dash.VerifyFatal(err == nil, true, "PX is up after add drive with kernel panic")
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})

})

func isMaintenanceModeRequiredForAddDisk() bool {
	isDmthin, err := IsDMthin()
	if err != nil {
		log.FailOnError(err, "error checking if dmthin is enabled")
	}
	if isDmthin {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
		return true

	}
	if Inst().N.String() == ssh.DriverName || Inst().N.String() == vsphere.DriverName {
		cmd := "uname -r"

		stNode := node.GetStorageDriverNodes()[0]
		// Execute the command to generate kernel panic
		log.Infof("Executing command on node, [%v]", stNode.Name)
		versionOutput, err := Inst().N.RunCommandWithNoRetry(stNode, cmd, node.ConnectionOpts{
			Timeout:         2 * time.Minute,
			TimeBeforeRetry: 10 * time.Second,
		})
		log.FailOnError(err, "error getting kernel version")

		parts := strings.Split(versionOutput, "-")

		if len(parts) >= 1 {
			kernelVersion := parts[0]
			parsedVersion, err := semver.NewVersion(kernelVersion)
			log.FailOnError(err, fmt.Sprintf("error parsion kernel version [%s]", kernelVersion))
			compareVersion, err := semver.NewVersion("5.9.0")
			log.FailOnError(err, fmt.Sprintf("error parsion kernel version [%s]", "5.9.0"))
			if parsedVersion.LessThan(compareVersion) {
				return true
			}
		} else {
			log.FailOnError(fmt.Errorf("unable for extract major kernel version using version: %s", versionOutput), "error in validating kernal version")
		}
	}

	return false
}

func enterPoolMaintenanceAddDisk(poolId string) {
	if ok := isMaintenanceModeRequiredForAddDisk(); ok {
		n, err := GetNodeWithGivenPoolID(poolId)
		log.FailOnError(err, fmt.Sprintf("error getting node with pool uid [%s]", poolId))
		err = Inst().V.EnterPoolMaintenance(*n)
		log.FailOnError(err, fmt.Sprintf("error entering pool maintenance pool uid [%s]", poolId))
	}
}

func exitPoolMaintenance(poolId string) {
	n, err := GetNodeWithGivenPoolID(poolId)
	log.FailOnError(err, fmt.Sprintf("error getting node with pool uid [%s]", poolId))
	status, err := Inst().V.GetNodePoolsStatus(*n)
	log.FailOnError(err, fmt.Sprintf("error getting node %s pool status", n.Name))
	log.InfoD(fmt.Sprintf("Pool %s has status %s", n.Name, status[poolId]))
	if status[poolId] == "In Maintenance" {
		log.InfoD(fmt.Sprintf("Exiting pool maintenance mode on node %s", n.Name))
		err = Inst().V.ExitPoolMaintenance(*n)
		log.FailOnError(err, fmt.Sprintf("fail to exit pool maintenance mode ib node %s", n.Name))
	}

}

func findNodeForReplAdd(vol *volume.Volume) (*node.Node, error) {
	poolUUIDs, err := GetPoolIDsFromVolName(vol.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pool uuids for volume %v: %v", vol.ID, err)
	}

	// list of nodes
	nodes := make([]*node.Node, 0)
	for _, p := range poolUUIDs {
		n, err := GetNodeFromPoolUUID(p)
		if err != nil {
			return nil, fmt.Errorf("failed to find node for pool UUID %v: %v", p, err)
		}
		nodes = append(nodes, n)
	}

	// find node to repl add
	for _, new := range node.GetStorageNodes() {
		found := false
		for _, old := range nodes {
			if new.Id == old.Id {
				found = true
				break
			}
		}
		if !found {
			return &new, nil
		}
	}
	return nil, fmt.Errorf("failed to find a node for repl add for volume %v", vol.ID)
}

// return a list of pool deletable nodes: prioritize for non-kvdb nodes
func selectPoolDeletableNodes() []node.Node {
	testNodes := []node.Node{}
	log.Info("Select non-kvdb node or node with > 1 pools)")
	stNodes := node.GetStorageNodes()
	log.InfoD("storage nodes %+v", stNodes)

	kvdbNodesIDs := []string{}
	kvdbMembers, err := Inst().V.GetKvdbMembers(stNodes[0])
	log.FailOnError(err, "Error getting KVDB members")
	log.InfoD("kvdb members %+v", kvdbMembers)

	for _, n := range kvdbMembers {
		kvdbNodesIDs = append(kvdbNodesIDs, n.Name)
	}

	// testNodes for pool deletable: [non-kvdb-nodes] + [kvdb-nodes with >= 2 pools]
	// collect non-kvdb firsts
	for _, n := range stNodes {
		if !Contains(kvdbNodesIDs, n.Id) { // non kvdb node
			log.InfoD("get non-kvdb node %v", n.Name)
			poolsMap, err := Inst().V.GetPoolDrives(&n)
			log.FailOnError(err, "cannot get pool drives")
			log.InfoD("non-kvdb node %v has %v pools %+v", n.Name, len(poolsMap), poolsMap)
			if len(poolsMap) > 0 {
				testNodes = append(testNodes, n)
			}
		}
	}

	// kvdb nodes, need at least 2 pools
	for _, n := range stNodes {
		if Contains(kvdbNodesIDs, n.Id) {
			log.InfoD("get kvdb node %v", n.Name)
			poolsMap, err := Inst().V.GetPoolDrives(&n)
			log.FailOnError(err, "cannot get pool drives")
			log.InfoD("kvdb node %v has %v pools %+v", n.Name, len(poolsMap), poolsMap)
			if len(poolsMap) > 1 {
				testNodes = append(testNodes, n)
			}
		}
	}

	if len(testNodes) == 0 { // it means all nodes are kvdb-nodes with 1 pools
		testNode := stNodes[0]
		log.InfoD("select kvdb node %v for test, the node need to have at least 2 pools", testNode.Name)
		poolsMap, err := Inst().V.GetPoolDrives(&testNode)
		log.FailOnError(err, "cannot get pool drives")
		log.InfoD("the kvdb node %v has pools %+v", testNode.Name, poolsMap)
		if len(poolsMap) <= 1 {
			log.InfoD("try create new pool for test")
			err = AddCloudDrive(testNode, -1)
			log.FailOnError(err, "drive add failed")
		}
		testNodes = append(testNodes, testNode)
	}

	for _, n := range testNodes {
		log.InfoD("found pool deletable node %v", n.Name)
	}

	dash.VerifyFatal(len(testNodes) > 0, true, "select test node")
	return testNodes
}

func selectPoolDeletableNode() *node.Node {
	poolDeletableNodes := selectPoolDeletableNodes()
	return &poolDeletableNodes[0]
}

var _ = Describe("{PoolDeleteFunctionality}", func() {
	/*
		Migrated from px-test: PoolDeleteFunctionality
			1. Delete pools till total availble pool is 1 and verify for each delete that the right pool was deleted.
			2. Delete the last pool and verify node transitions to storageless.
			3. Randomly pick a drive which got freed up because of pool delete and add it back.
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("PoolDeleteFunctionality", "Initiate pool deletion", nil, 0)
	})
	var contexts []*scheduler.Context

	ItLog := "Initiate pool delete, then add a new pool and expand the pool"
	It(ItLog, func() {
		testNode := selectPoolDeletableNode()
		dash.VerifyFatal(testNode != nil, true, "verify if select test node ok")
		selectedNode := *testNode
		nodePools := selectedNode.Pools
		log.Infof("node %v has pools %+v", testNode.Name, nodePools)
		poolToAddBack := nodePools[rand.Intn(len(nodePools))]
		stepLog = fmt.Sprintf("Delete [%v/%v] pools on node [%v], so that 1 pool remains", len(nodePools)-1, len(nodePools), selectedNode.Name)
		Step(stepLog, func() {
			for _, pool := range nodePools[:len(nodePools)-1] {
				poolID := fmt.Sprintf("%v", pool.GetID())
				log.Infof("delete pool %v", poolID)
				deletePoolAndValidate(selectedNode, poolID)
			}
		})

		stepLog = fmt.Sprintf("Delete the last pool on node [%v] and verify it transitions to storageless ", selectedNode.Name)
		Step(stepLog, func() {
			// no need to exit pool maintenance mode
			err = EnterPoolMaintenance(selectedNode)
			log.FailOnError(err, "")
			poolID := strconv.Itoa(int(nodePools[len(nodePools)-1].ID))
			err = Inst().V.DeletePool(selectedNode, poolID, true)
			log.FailOnError(err, "")

			poolsMap, err := Inst().V.GetPoolDrives(&selectedNode)
			log.FailOnError(err, "error getting pool drive from the node [%s]", selectedNode.Name)
			log.Infof("poolMap %+v", poolsMap)
			dash.VerifyFatal(len(poolsMap) == 0, true, "verify all pools deleted")

			err = Inst().V.RefreshDriverEndpoints()
			log.FailOnError(err, "error refreshing driver end points")
			found := false
			for _, n := range node.GetStorageLessNodes() {
				if n.Id == selectedNode.Id {
					found = true
					break
				}
			}
			dash.VerifyFatal(found, true, "expect node to be a storageless node")
		})

		// deploy applications
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pooldeletefunc-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stepLog = fmt.Sprintf("Adding cloud drive to node [%v] with size [%v]", selectedNode.Name, poolToAddBack.TotalSize/units.GiB)
		Step(stepLog, func() {
			// create spec to perform add drive
			newSpecSize := poolToAddBack.TotalSize / units.GiB
			driveSpecs, err := GetCloudDriveDeviceSpecs()
			log.FailOnError(err, "Error getting cloud drive specs")
			deviceSpec := driveSpecs[0]
			deviceSpecParams := strings.Split(deviceSpec, ",")
			paramsArr := make([]string, 0)
			for _, param := range deviceSpecParams {
				if strings.Contains(param, "size") {
					paramsArr = append(paramsArr, fmt.Sprintf("size=%d,", newSpecSize))
				} else {
					paramsArr = append(paramsArr, param)
				}
			}
			newSpec := strings.Join(paramsArr, ",")

			err = Inst().V.AddCloudDrive(&selectedNode, newSpec, -1)
			log.FailOnError(err, "error adding new drive to node %s", selectedNode.Name)
			log.InfoD("Validate pool rebalance after drive add to the node %s", selectedNode.Name)
			err = ValidateDriveRebalance(selectedNode)
			log.FailOnError(err, "pool re-balance failed on node %s", selectedNode.Name)
			err = Inst().V.WaitDriverUpOnNode(selectedNode, addDriveUpTimeOut)
			log.FailOnError(err, "volume drive down on node %s", selectedNode.Name)

			newPoolsMap, err := Inst().V.GetPoolDrives(&selectedNode)
			log.FailOnError(err, "error getting pool drive from the node [%s]", selectedNode.Name)
			dash.VerifyFatal(len(newPoolsMap) == 1, true, "verify new drive is created")
		})

		stepLog = fmt.Sprintf("Expand newly added pool on node [%s]", selectedNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolsAfr, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
			log.FailOnError(err, "Failed to list storage pools")
			var poolIDSelected string
			for k := range poolsAfr {
				poolIDSelected = k
				break
			}
			poolToBeResized, err := GetStoragePoolByUUID(poolIDSelected)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDSelected))
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + 100

			log.InfoD("Current Size of the pool %s is %d", poolIDSelected, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolIDSelected, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize, false)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, poolIDSelected, false)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on expansion using auto option", poolIDSelected))
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{PoolDeleteNegative}", func() {

	/*
				migrated from px-test: PoolDeleteNegative
		 		1. Delete pool with invalid pool IDs
				2. Delete pool with volumes on it
				3. Delete pool after it was added to volume through ha-incr
	*/

	deletePoolAndValidateFaliure := func(poolID string, selectedNode *node.Node, errRegExp *regexp.Regexp) {
		log.InfoD("Delete pool with ID [%v] on node [%v]", poolID, selectedNode.Name)
		err = Inst().V.DeletePool(*selectedNode, poolID, false)
		dash.VerifyFatal(err != nil, true, fmt.Sprintf("Expect pool delete to fail: got [%v]", err))
		dash.VerifyFatal(errRegExp.MatchString(err.Error()), true, fmt.Sprintf("Expect error to contain message: [%v]", errRegExp))
	}

	JustBeforeEach(func() {
		StartTorpedoTest("PoolDeleteNegative", "RunPoolDeleteNegativeTests tests cases where pool deletion should not happen", nil, 0)
	})

	var selectedNode *node.Node
	BeforeEach(func() {
		selectedNode = selectPoolDeletableNode()
		dash.VerifyFatal(selectedNode != nil, true, "very if select test node ok")
	})

	ItLog := "Delete pool using invalid pool ids"
	It(ItLog, func() {
		log.InfoD(stepLog)

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pooldeleteinvalidid-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// test pool delete without entering pool maintenance mode - should fail
		// TODO (do we need this check?) if IsLocalCluster(*selectedNode) || IsIksCluster() {
		log.InfoD("Delete pool without entering pool maintenance mode")
		errRegExp := regexp.MustCompile("Requires pool maintenance mode")
		deletePoolAndValidateFaliure("0", selectedNode, errRegExp)

		log.FailOnError(EnterPoolMaintenance(*selectedNode), "failed to enter pool maintenance mode")

		// test pool delete with invalid pool IDs - should fail
		invalidPoolIDs := []int{math.MaxInt64, len(selectedNode.StoragePools) + 1}
		for _, poolID := range invalidPoolIDs {
			invalidPoolID := fmt.Sprintf("%d", poolID)
			errRegExp := regexp.MustCompile("Pool ID [0-9]+ is invalid")
			deletePoolAndValidateFaliure(invalidPoolID, selectedNode, errRegExp)
		}

		errRegExp = regexp.MustCompile("unknown shorthand flag: '1' in -1")
		deletePoolAndValidateFaliure("-1", selectedNode, errRegExp)

		log.FailOnError(ExitPoolMaintenance(*selectedNode), "failed to exit pool maintenance mode")
	})

	ItLog = "Delete pool that has a volume on it"
	It(ItLog, func() {
		log.InfoD(stepLog)

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pooldeletewithvol-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		selectedPoolUUID := pickPoolToResize(contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 0)
		log.InfoD("Pool UUID on which IO is running [%s]", selectedPoolUUID)
		selectedPool, err := GetStoragePoolByUUID(selectedPoolUUID)
		log.FailOnError(err, "error getting storage pool with UUID [%s]", selectedPoolUUID)
		selectedNode, err := GetNodeWithGivenPoolID(selectedPoolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", selectedPoolUUID)

		log.FailOnError(EnterPoolMaintenance(*selectedNode), "failed to enter pool maintenance mode")
		errRegExp := regexp.MustCompile("Cannot delete pool: Following volumes have data on pool")
		deletePoolAndValidateFaliure(fmt.Sprintf("%v", selectedPool.ID), selectedNode, errRegExp)
		log.FailOnError(ExitPoolMaintenance(*selectedNode), "failed to exit pool maintenance mode")
	})

	ItLog = "Delete pool while a volume on the pool is in resync"
	It(ItLog, func() {
		log.InfoD(stepLog)
		// don't schedule the appList so that any pool we pick for repl-add
		// and delete doesn't have any other volumes

		// schedule the fio app
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		Inst().AppList = []string{"fio-storagepool"}
		log.InfoD("scheduling apps ")
		appNamespace := fmt.Sprintf("pooldeletewithresync-%s", Inst().InstanceID)
		contexts := ScheduleApplicationsOnNamespace(appNamespace, "pooldeletewithresync")
		defer appsValidateAndDestroy(contexts)

		// waiting for the data to be written before performing ha-update
		testVolume, err := GetVolumeWithMinimumSize(contexts, 50)
		log.FailOnError(err, "error selecting volumes for resync")

		contextAppKey := contexts[0].App.Key

		// find pool to repl add and then delete
		replAddNode, err := findNodeForReplAdd(testVolume)
		log.FailOnError(err, "failed to find node for repl add")
		dash.VerifyFatal(replAddNode != nil, true, "expect to find a node to do repl add")
		replAddPool := replAddNode.StoragePools[0]
		// TODO add check that pool doesn't contain metadata

		stepLog = fmt.Sprintf("Increasing the volume repls for %s", contextAppKey)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			log.Infof("Increase repl for volume: %v", testVolume.ID)
			pxctlCmdFull := fmt.Sprintf("v ha-update -r 3 --node %v %v", replAddPool.Uuid, testVolume.ID)
			output, err := Inst().V.GetPxctlCmdOutput(node.GetStorageNodes()[0], pxctlCmdFull)
			log.FailOnError(err, fmt.Sprintf("error update ha for volume %v", testVolume.ID))
			log.Infof(output)
		})

		// waiting for all the volume is resync state
		checkVolumeStateIsResync := func(v *volume.Volume) (interface{}, bool, error) {
			runTimeState, err := GetVolumeReplicationStatus(v)
			if err != nil {
				return "", false, fmt.Errorf("error getting run time state for volume:%s. App : %s", v.Name, contextAppKey)
			}
			if strings.ToLower(runTimeState) == "resync" {
				return "", false, nil
			}
			return nil, true, fmt.Errorf("waiting for volume %s run time state to change to resync, current state: %s", v.Name, runTimeState)
		}

		f := func() (interface{}, bool, error) { return checkVolumeStateIsResync(testVolume) }
		_, err = task.DoRetryWithTimeout(f, time.Duration(5)*defaultCommandTimeout, 1*time.Minute)
		log.FailOnError(err, "expect volume to enter resync state")

		log.FailOnError(EnterPoolMaintenance(*replAddNode), "failed to enter pool maintenance mode")
		errRegExp := regexp.MustCompile("Cannot delete pool: Following volumes have data on pool")
		deletePoolAndValidateFaliure(fmt.Sprintf("%v", replAddPool.ID), replAddNode, errRegExp)
		log.FailOnError(ExitPoolMaintenance(*replAddNode), "failed to exit pool maintenance mode")
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{PoolDeleteVariations}", func() {

	/*
				migrated from px-test: PoolDeleteVariations
		 		1. Verify pool deletion after creating 50 volumes
				2. Verify pool deletion after creating 75 snaps
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("PoolDeleteVariations", "Pool delete with variations", nil, 0)
	})
	var contexts []*scheduler.Context

	itLog := fmt.Sprintf("Verify pool delete variations")
	It(itLog, func() {
		numVolCreate := 50
		stepLog := fmt.Sprintf("1. Verify pool deletion after creating %v volumes", numVolCreate)
		Step(stepLog, func() {
			log.InfoD(itLog)
			poolDeletableNodes := selectPoolDeletableNodes()
			poolDeletableNodesMap := map[string]*node.Node{}
			for _, p := range poolDeletableNodes {
				poolDeletableNodesMap[p.Id] = &p
			}
			log.Infof("Deletable pools %+v", poolDeletableNodesMap)

			volumesCreated := []string{}
			for i := 0; i < numVolCreate; i++ {
				uuidObj := uuid.New()
				volName := fmt.Sprintf("volume_%s", uuidObj.String())
				size := uint64(rand.Intn(5) + 1) // Size of the Volume between 1G to 5G
				ha := int64(rand.Intn(3) + 1)    // Size of the HA between 1 and 3

				volId, err := Inst().V.CreateVolume(volName, size, int64(ha))
				log.FailOnError(err, "volume creation failed on the cluster with volume name [%s]", volName)
				volumesCreated = append(volumesCreated, volId)
			}

			// select a pool with volume on it to delete
			testNode := (*node.Node)(nil)
			poolIDToDelete := ""
			for _, vol := range volumesCreated {
				appVol, err := Inst().V.InspectVolume(vol)
				log.FailOnError(err, fmt.Sprintf("err inspecting vol : %s", vol))
				replNodes := appVol.ReplicaSets[0].Nodes
				for _, n := range replNodes {
					// the node can do pool delete
					if t, ok := poolDeletableNodesMap[n]; ok {
						testNode = t
						// get poolUUID to ID in the node
						deletablePools := make(map[string]string)
						for _, p := range testNode.Pools {
							deletablePools[p.Uuid] = fmt.Sprintf("%v", p.GetID())
						}
						replPools := appVol.ReplicaSets[0].PoolUuids
						// find the pool id to delete
						for _, p := range replPools {
							if id, ok := deletablePools[p]; ok {
								poolIDToDelete = id
								break
							}
						}
						break
					}
				}

				if poolIDToDelete != "" {
					break
				}
			}
			log.InfoD("select testNode %v poolID %v to delete", testNode.Addresses, poolIDToDelete)

			// delete should fail since the pool has volume replicas
			err = DeleteGivenPoolInNode(*testNode, poolIDToDelete, false)
			dash.VerifyFatal(err != nil, true, "do not expect pool delete success since pool has volume replicas")
			dash.VerifyFatal(strings.Contains(err.Error(), "have data on pool"), true, fmt.Sprintf("check error message: %v", err.Error()))

			log.InfoD("deleting all volumes")

			for _, volID := range volumesCreated {
				// Delete the Volume
				err = Inst().V.DeleteVolume(volID)
				log.FailOnError(err, "failed to delete volume with volume ID [%s]", volID)
			}

			vols, err := Inst().V.ListAllVolumes()
			log.FailOnError(err, "cannot list all volumes")

			dash.VerifyFatal(len(vols) == 0, true, fmt.Sprintf("expect all volumes deleted: %+v", vols))

			deletePoolAndValidate(*testNode, poolIDToDelete)

			// add a pool back
			err = AddCloudDrive(*testNode, -1)
			log.FailOnError(err, "drive add failed")
		})

		numVolSnaps := 3
		numVols := 25
		stepLog = fmt.Sprintf("2. Verify pool deletion after creating vols and each with %v snaps %v", numVols, numVolSnaps)

		Step(stepLog, func() {
			poolDeletableNodes := selectPoolDeletableNodes()
			poolDeletableNodesMap := map[string]*node.Node{}
			for _, p := range poolDeletableNodes {
				poolDeletableNodesMap[p.Id] = &p
			}
			log.Infof("Deletable pools %+v", poolDeletableNodesMap)

			volIDs := []string{}
			for i := 0; i < numVols; i++ {
				uuidObj := uuid.New()
				volName := fmt.Sprintf("volume_%s", uuidObj.String())
				size := uint64(rand.Intn(5) + 1) // Size of the Volume between 1G to 5G
				ha := int64(3)
				volID, err := Inst().V.CreateVolume(volName, size, int64(ha))
				log.FailOnError(err, "volume creation failed on the cluster with volume name [%s]", volName)
				volIDs = append(volIDs, volID)
			}

			snapshotList := []string{}
			for _, volID := range volIDs {
				for i := 0; i < numVolSnaps; i++ {
					uuidCreated := uuid.New()
					snapshotName := fmt.Sprintf("snapshot_%s_%s", volID, uuidCreated.String())
					snapshotResponse, err := Inst().V.CreateSnapshot(volID, snapshotName)
					log.FailOnError(err, "error creating snapshot [%s]", volID)
					snapID := snapshotResponse.GetSnapshotId()
					snapshotList = append(snapshotList, snapID)
					log.InfoD("Snapshot [%s] created with ID [%s]", snapshotName, snapID)
				}
			}

			// select a pool with volume on it to delete
			testNode := (*node.Node)(nil)
			poolIDToDelete := ""
			for _, vol := range snapshotList {
				appVol, err := Inst().V.InspectVolume(vol)
				log.FailOnError(err, fmt.Sprintf("err inspecting vol : %s", vol))
				replNodes := appVol.ReplicaSets[0].Nodes
				for _, n := range replNodes {
					// the node can do pool delete
					if t, ok := poolDeletableNodesMap[n]; ok {
						testNode = t
						// get poolUUID to ID in the node
						deletablePools := make(map[string]string)
						for _, p := range testNode.Pools {
							deletablePools[p.Uuid] = fmt.Sprintf("%v", p.GetID())
						}
						replPools := appVol.ReplicaSets[0].PoolUuids
						// find the pool id to delete
						for _, p := range replPools {
							if id, ok := deletablePools[p]; ok {
								poolIDToDelete = id
								break
							}
						}
						break
					}
				}

				if poolIDToDelete != "" {
					break
				}
			}
			log.InfoD("select testNode %v poolID %v to delete", testNode.Addresses, poolIDToDelete)
			dash.VerifyFatal(poolIDToDelete != "", true, fmt.Sprintf("target pool deletion ID: %v", poolIDToDelete))
			log.InfoD("deleting all vols")
			for _, volID := range volIDs {
				err = Inst().V.DeleteVolume(volID)
				log.FailOnError(err, "failed to delete volume with volume ID [%s]", volID)
			}

			// delete should fail
			err = DeleteGivenPoolInNode(*testNode, poolIDToDelete, false)
			dash.VerifyFatal(err != nil, true, "do not expect pool delete success since pool has snap replicas")
			dash.VerifyFatal(strings.Contains(err.Error(), "have data on pool"), true, fmt.Sprintf("check error message: %v", err.Error()))

			log.InfoD("deleting all snaps")
			for _, snapID := range snapshotList {
				// Delete the Volume
				err = Inst().V.DeleteVolume(snapID)
				log.FailOnError(err, "failed to delete snap with volume ID [%s]", snapID)
			}

			deletePoolAndValidate(*testNode, poolIDToDelete)

			// add a pool back
			err = AddCloudDrive(*testNode, -1)
			log.FailOnError(err, "drive add failed")
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{PoolDeleteServiceDisruption}", func() {

	/*
		migrated from px-test: RunPoolServiceDisruptiveTests
		after pool deletion, after the followin cases px should be up
		1. entering and exiting maintenance
		2. restart
		3. reboot
		4. add a drive
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("PoolDeleteServiceDisruption", "Pool delete with service disruption", nil, 0)
	})

	var contexts []*scheduler.Context

	itLog := "PoolDeleteServiceDisruption"
	It(itLog, func() {
		testNode := selectPoolDeletableNode()
		poolIDToDelete := ""

		drvMap, err := Inst().V.GetPoolDrives(testNode)
		log.FailOnError(err, "error getting pool drives from node [%s]", testNode.Name)
		for poolID := range drvMap {
			poolIDToDelete = poolID
			break
		}

		dash.VerifyFatal(poolIDToDelete != "", true, fmt.Sprintf("check deltable pool on node %s: poolIDToDelete %v", testNode.Name, poolIDToDelete))

		deletePoolAndValidate(*testNode, poolIDToDelete)

		stepLog := "1. Verify enter and exit maintenance mode"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			err = Inst().V.EnterMaintenance(*testNode)
			log.FailOnError(err, "fail to enter maintenace mode")

			err = Inst().V.ExitMaintenance(*testNode)
			log.FailOnError(err, "fail to exit maintenace mode")
		})

		stepLog = "2. Verify restart pxc"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err := Inst().V.RestartDriver(*testNode, nil)
			log.FailOnError(err, fmt.Sprintf("error restarting px on node %s", testNode.Name))
			err = Inst().V.WaitDriverUpOnNode(*testNode, 5*time.Minute)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", testNode.Name))
			dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after restarting on node %s", testNode.Name))
		})

		stepLog = "3. Verify reboot"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err = RebootNodeAndWaitForPxUp(*testNode)
			log.FailOnError(err, "Failed to reboot node and wait till it is up")
			log.Info("Verify reboot succeed")
		})

		// add a pool back
		stepLog = "4. Add a drive back"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			log.Info("Wait 1 min for stabling everything after reboot")
			time.Sleep(time.Second * 60)
			err = AddCloudDrive(*testNode, -1)
			log.FailOnError(err, "drive add failed")
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{HAIncreasePoolresizeAndAdddisk}", func() {
	/*
		PTX:
		https://portworx.atlassian.net/browse/PTX-15465

		TestRail:
		https://portworx.testrail.net/index.php?/cases/view/57783

		 1. Trigger HA increase for volumes
		 2. Pool resize trigger
		 3. Add disk to the pool
		All three steps should occur parallel and should not cause any issue
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("HAIncreasePoolresizeAndAdddisk", "HA increase, pool resize and add disk run all this parallely", nil, 57783)
	})
	var contexts []*scheduler.Context
	var poolToBeUpdated string
	var wg sync.WaitGroup

	itLog := "HAIncreasePoolresizeAndAdddisk"
	It(itLog, func() {
		isPoolAddDiskSupported := IsPoolAddDiskSupported()
		if !isPoolAddDiskSupported {
			Skip("Add disk operation is not supported for DMThin Setup")
		}
		stepLog := "schedule Application"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				for _, app := range Inst().AppList {
					contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%s", app, "ha-increase-pool-add-resize"))...)
				}
			}

		})
		ValidateApplications(contexts)
		defer DestroyApps(contexts, nil)

		for _, eachContext := range contexts {
			stepLog = "HA increase for volumes of app: " + eachContext.App.Key
			Step(stepLog, func() {
				// First get volumes of the application

				vols, err := Inst().S.GetVolumes(eachContext)
				log.FailOnError(err, "Failed to get volumes from context")

				vol := vols[rand.Intn(len(vols))]
				curReplSet, err := Inst().V.GetReplicationFactor(vol)
				log.InfoD("Node selected for repl increase")

				var nodesToBeUpdated []string
				var poolsToBeUpdated []string

				// Check if Replication factor is 3. if so, then reduce the repl factor and then set repl factor to 2
				if curReplSet == 3 {
					inspectVol, err := Inst().V.InspectVolume(vol.ID)
					log.FailOnError(err, "Failed to inspect volume: %v", vol.ID)
					replicaSets := inspectVol.ReplicaSets
					replicaset := replicaSets[len(replicaSets)-1]
					nodeToBeUpdated, err := GetNodeWithGivenPoolID(replicaset.PoolUuids[0])
					poolToBeUpdated = replicaset.PoolUuids[0]

					log.InfoD("Node selected for pool expand: %v", nodeToBeUpdated.Name)
					log.InfoD("pool selected for pool expand: %v", poolToBeUpdated)
					nodesToBeUpdated = append(nodesToBeUpdated, nodeToBeUpdated.Id)
					poolsToBeUpdated = append(poolsToBeUpdated, poolToBeUpdated)

					newRepl := int64(curReplSet - 1)
					log.FailOnError(Inst().V.SetReplicationFactor(vol, newRepl,
						nodesToBeUpdated, poolsToBeUpdated, true),
						"Failed to set Replicaiton factor")
				} else {
					// pick nodes which are not in replicaset
					inspectVol, err := Inst().V.InspectVolume(vol.ID)
					log.FailOnError(err, "Failed to inspect volume: %v", vol.ID)
					replicaSets := inspectVol.ReplicaSets
					found := false
					nodeToBeUpdated := node.Node{}
					//pick a node which is not present in replicaset
					for _, n := range replicaSets {
						for _, storageNode := range node.GetStorageNodes() {
							log.Infof("Storage node: %v", storageNode.Id)
							for _, node := range n.Nodes {
								log.InfoD("replica set: %v", node)
								if storageNode.Id == node {
									found = true
									break
								}
							}
							if !found {
								nodeToBeUpdated = storageNode
								break
							}
							found = false
						}
					}
					log.InfoD("Node selected: %v", nodeToBeUpdated.Id)

					poolsUuid, err := GetAllPoolsOnNode(nodeToBeUpdated.Id)
					log.FailOnError(err, "Failed to get pool using node %s", nodeToBeUpdated.Id)

					poolToBeUpdated = poolsUuid[0]
					log.InfoD("pool selected for pool expand: %v", poolToBeUpdated)

					nodesToBeUpdated = append(nodesToBeUpdated, nodeToBeUpdated.Id)
					poolsToBeUpdated = append(poolsToBeUpdated, poolsUuid[0])

				}

				var maxReplicaFactor int64
				maxReplicaFactor = 3

				log.FailOnError(Inst().V.SetReplicationFactor(vol, maxReplicaFactor,
					nodesToBeUpdated, poolsToBeUpdated, false),
					"Failed to set Replicaiton factor")
				t := func() (interface{}, bool, error) {
					volDetails, err := Inst().V.InspectVolume(vol.ID)
					if err != nil {
						return nil, true, fmt.Errorf("error getting volume by using id %s", vol.ID)
					}
					resync := false
					for _, v := range volDetails.RuntimeState {
						log.InfoD("RuntimeState is in state %s", v.GetRuntimeState()["RuntimeState"])
						if v.GetRuntimeState()["RuntimeState"] == "resync" ||
							v.GetRuntimeState()["RuntimeState"] == "clean" {
							resync = true
						}
					}
					if resync {
						return fmt.Sprintf("Volume resync has started"), false, nil

					}
					return nil, true, fmt.Errorf("volume resync hasn't started")
				}
				_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 10*time.Second)
				log.FailOnError(err, "Error checking volume resync")

			})
			stepLog = "Pool resize trigger"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				//initiate pool expand on the pool where ha increase is happening.
				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					pool, err := GetStoragePoolByUUID(poolToBeUpdated)
					log.FailOnError(err, "Failed to get pool using UUID %s", poolToBeUpdated)

					expectedSize := (pool.TotalSize / units.GiB) + 100
					err = Inst().V.ExpandPool(poolToBeUpdated, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
					log.FailOnError(err, "Failed to initiate pool resize")

					//wait for pool expand to complete
					err = waitForPoolToBeResized(expectedSize, pool.Uuid, false)
					log.FailOnError(err, "Failed to wait for pool to be resized")
					log.InfoD("Successfully expanded the pool with resize disk: %s", pool.Uuid)
				}()
			})

			stepLog = "Pool expand add disk trigger"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer GinkgoRecover()
					pool, err := GetStoragePoolByUUID(poolToBeUpdated)
					log.FailOnError(err, "Failed to get pool using UUID %s", poolToBeUpdated)

					expectedSize := (pool.TotalSize / units.GiB) + 100
					err = Inst().V.ExpandPool(poolToBeUpdated, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
					log.FailOnError(err, "Failed to initiate pool resize")

					//wait for pool expand to complete
					err = waitForPoolToBeResized(expectedSize, pool.Uuid, false)
					log.FailOnError(err, "Failed to wait for pool to be resized")
					log.InfoD("Successfully expanded the pool with add disk pool id: %s", pool.Uuid)
				}()

			})
			wg.Wait()
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{PoolResizeInTrashCanNode}", func() {
	/*
	  1. Deploy apps
	  2. Pick a volume and locate the node where this is attached
	  3. Delete the volume and let it be placed in trashcan
	  4. Trigger pool expand in the node where the trashcan volume is present

	*/

	JustBeforeEach(func() {
		StartTorpedoTest("PoolResizeInTrashCanNode", "Pool resize with volumes in trashcan", nil, 0)
	})

	var contexts []*scheduler.Context

	var trashcanVolsBeforePoolExpand []string

	itLog := "PoolResizeInTrashCanNode"
	It(itLog, func() {
		log.InfoD(itLog)
		stepLog := "Enable trashCan for the cluster"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			currNode := node.GetStorageDriverNodes()[0]
			err := Inst().V.SetClusterOptsWithConfirmation(currNode, map[string]string{
				"--volume-expiration-minutes": "600",
			})
			log.FailOnError(err, "Failed to enable trashCan")
		})

		// Deploy apps
		stepLog = "Schedule application"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("trash-can-pool-exopand-%d", i))...)
			}
		})
		ValidateApplications(contexts)

		stepLog = "Enable trashcan"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			//Enable trashcan in the node
			err = Inst().V.SetClusterOptsWithConfirmation(node.GetStorageNodes()[0], map[string]string{
				"--volume-expiration-minutes": "600",
			})
			log.FailOnError(err, "error while enabling trashcan")
			log.InfoD("Trashcan is successfully enabled")

		})

		poolsWithVolsAttachedCount := make(map[string]int, 0)

		for _, eachContext := range contexts {

			vols, err := Inst().S.GetVolumes(eachContext)
			log.FailOnError(err, "Failed to get volumes from context")

			for _, vol := range vols {
				volDetails, err := Inst().V.InspectVolume(vol.ID)
				log.FailOnError(err, "Failed to inspect volume: %v", vol.Name)
				for _, replSet := range volDetails.ReplicaSets {
					for _, poolUUID := range replSet.PoolUuids {
						poolsWithVolsAttachedCount[poolUUID] += 1
					}
				}
				poolsWithVolsAttachedCount[volDetails.AttachedOn] += 1
			}
		}

		var poolForExpansion string
		maxAttached := 0

		for poolWithVolsAttached, count := range poolsWithVolsAttachedCount {
			if count > maxAttached {
				maxAttached = count
				poolForExpansion = poolWithVolsAttached
			}
		}

		nodeForExpansion, err := GetNodeWithGivenPoolID(poolForExpansion)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolForExpansion)

		stepLog = "Destroy apps and let it's volumes be placed in trashcan"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			DestroyApps(contexts, nil)
		})

		stepLog = "Check if volumes are in trashcan before pool expand"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			// wait for few seconds for pvc to get deleted and volume to get detached
			time.Sleep(30 * time.Second)

			log.InfoD(stepLog)
			trashcanVolsBeforePoolExpand, err = Inst().V.GetTrashCanVolumeIds(*nodeForExpansion)
			log.FailOnError(err, "error While getting trashcan volumes")
			log.Infof("trashcan len before pool expand: %d", len(trashcanVolsBeforePoolExpand))
			dash.VerifyFatal(len(trashcanVolsBeforePoolExpand) > 0, true, "validate volumes exist in trashcan")

		})

		stepLog = "Expand pool using resize"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			pool, err := GetStoragePoolByUUID(poolForExpansion)
			log.FailOnError(err, "Failed to get pool using UUID %s", poolForExpansion)

			expectedSize := (pool.TotalSize / units.GiB) + 200
			err = Inst().V.ExpandPool(pool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			log.FailOnError(err, "Failed to initiate pool resize")

			//wait for pool expand to complete
			err = waitForPoolToBeResized(expectedSize, pool.Uuid, true)
		})

		stepLog = "Check trashcan after pool expand"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			trashcanVolsAfterPoolExpand, err := Inst().V.GetTrashCanVolumeIds(*nodeForExpansion)
			log.FailOnError(err, "error While getting trashcan volumes")

			log.Infof("trashcan len after pool expand: %d", len(trashcanVolsAfterPoolExpand))

			dash.VerifyFatal(len(trashcanVolsAfterPoolExpand) > 0, true, "validate volumes exist in trashcan")
			dash.VerifyFatal(len(trashcanVolsAfterPoolExpand) == len(trashcanVolsBeforePoolExpand), true, "trashcan size same before and after pool expand")
			// Create a exist map to check if all the volumes in trashcan are present
			trashCanMap := map[string]bool{}
			for _, vol := range trashcanVolsAfterPoolExpand {
				if vol != "" {
					trashCanMap[vol] = true
				}
			}
			// check if the values are same before and after pool expand
			for _, vol := range trashcanVolsBeforePoolExpand {
				if vol != "" {
					if trashCanMap[vol] == false {
						log.Errorf("Volume not present in trashcan after pool expand: %v", vol)
					}
				}
			}
			log.InfoD("Succesfully verified all the volumes in trashcan after pool expand")
		})

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

// PX pool went offline even when 30% of storage still available [PWX-26872]

var _ = Describe("{CheckPoolOffline}", func() {
	/*
		    1. Deploy fio apps
		    2. when capacity 70 percent is reached, check if pool is offline (Pool shouldn't be offline)
			3. when capacity 80 percent is reached, check if pool is offline (Pool should be offline)

			https://portworx.atlassian.net/browse/PTX-15695
	*/
	itLog := "CheckPoolOffline"
	JustBeforeEach(func() {
		StartTorpedoTest(itLog, "Check if pool is offline when capacity is reached", nil, 0)
	})
	var contexts []*scheduler.Context
	var selectedNode *node.Node

	It(itLog, func() {
		log.InfoD(itLog)
		stepLog := "Label nodes and Deploy fio apps"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			selectedNode = GetNodeWithLeastSize()

			AppList := Inst().AppList
			Inst().AppList = []string{"fio-fastpath-repl1"}

			var err error
			defer func() {
				Inst().AppList = AppList
				err = Inst().S.RemoveLabelOnNode(*selectedNode, k8s.NodeType)
				log.FailOnError(err, "error removing label on node [%s]", selectedNode.Name)
			}()
			err = Inst().S.AddLabelOnNode(*selectedNode, k8s.NodeType, k8s.FastpathNodeType)
			log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", selectedNode.Name))

			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("storagefull-check-%d", i))...)
			}
		})
		ValidateApplications(contexts)
		defer DestroyApps(contexts, nil)

		// Pool should go offline after using 80% of the storage capacity
		stepLog = "Keep checking when the pool goes offline"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			t := func() (interface{}, bool, error) {
				poolsStatus, err := Inst().V.GetNodePoolsStatus(*selectedNode)
				if err != nil {
					return nil, true, err
				}

				for poolUUID, v := range poolsStatus {
					if v == "Offline" {
						offlinePool, err := GetStoragePoolByUUID(poolUUID)
						log.FailOnError(err, "Failed to get uuid of offline pool")
						log.Infof("Pool %s is offline", offlinePool.Uuid)
						// Used storage should be greater than 80 percentage
						if (offlinePool.Used) >= (offlinePool.TotalSize*80)/100 {
							log.Infof("Pool %s is offline and used storage is greater than 80 percentage", offlinePool.Uuid)
							return nil, false, nil
						}
						return nil, true, fmt.Errorf("Pool %s is offline but used storage is less than 80 percentage", offlinePool.Uuid)
					}
				}
				return nil, true, fmt.Errorf("no pool is offline is node %s", selectedNode.Name)
			}
			_, err := task.DoRetryWithTimeout(t, time.Minute*360, time.Minute*2)
			log.FailOnError(err, "Failed to wait for pool offline")

		})

	})

})

var _ = Describe("{FACDPoolIOPriorityCheck}", func() {

	/* This test is created to provide functional testing coverage for ticket PWX-35590
	1. Create a cluster with FACD backend
	2. Check if the IO Priority for the storagepools is HIGH
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("FACDPoolIOPriorityCheck", "Verify the Priority of the pools with FACD is High", nil, 0)
	})

	var contexts []*scheduler.Context

	itLog := "FACDPoolIOPriorityCheck"
	It(itLog, func() {
		provisions, err := GetClusterProvisionStatus()
		log.FailOnError(err, "failed to get cluster provision status")
		for _, provision := range provisions {
			if isCloudDriveTypePureBlock(provision.NodeUUID, provision.PoolUUID) {
				log.Infof("the iopriority is %v\n", provision.IoPriority)
				dash.VerifyFatal(provision.IoPriority == "HIGH", true, fmt.Sprintf("Expected the IOPriority to be HIGH for pool with ID: %s on the node: %s but found to be %s", provision.PoolUUID, provision.NodeUUID, provision.IoPriority))
			} else {
				dash.VerifyFatal(false, true, fmt.Sprintf("Expected the CloudDrive type to be pure-block, found something else."))
			}
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

func isCloudDriveTypePureBlock(nodeUUID, poolUUID string) bool {
	n, err := GetNodeFromPoolUUID(poolUUID)
	log.FailOnError(err, "Failed to get node with given pool ID")
	cmd := fmt.Sprintf("pxctl clouddrive inspect --node %v | grep Type | uniq", nodeUUID)
	out, err := Inst().N.RunCommandWithNoRetry(*n, cmd, node.ConnectionOpts{
		Timeout:         2 * time.Minute,
		TimeBeforeRetry: 10 * time.Second,
	})
	log.FailOnError(err, "Unable to execute the clouddrive inspect command")
	driveType := strings.TrimSpace(strings.Split(out, ":")[1])
	return driveType == "pure-block"
}

var _ = Describe("{OnlineJournalAddCheck}", func() {
	/*
			https://portworx.atlassian.net/browse/PTX-15712
			1. Deploy apps
			2. Check lsblk state before addition of drive add of journal device
		    3. Add a cloud drive as a journal device without pool in maintenance mode
		    4. The request must be rejected and the journal device should not be added in lsblk
		    5. Now put pool in maintenance mode and add a cloud drive as a journal device this should go through successfully
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("OnlineJournalAddCheck", "Online journal add check", nil, 0)
	})

	var contexts []*scheduler.Context
	var selectedNode = node.Node{}
	diskMapBeforeDriveAdd := make(map[string]string)

	itLog := "OnlineJournalAddCheck"
	It(itLog, func() {
		stepLog := "Check if file system is dm-thin if so then skip the test"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if isDMthin, _ := IsDMthin(); isDMthin {
				log.FailOnError(fmt.Errorf("File system is dm-thin"), "dm-thin doesn't support journal device")
			}
		})

		log.InfoD(itLog)
		stepLog = "Schedule application"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("online-journal-add-check-%d", i))...)
			}
		})
		ValidateApplications(contexts)
		defer DestroyApps(contexts, nil)

		stepLog = "select a node where there is no journal device"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			storageNodes := node.GetStorageDriverNodes()
			for _, node := range storageNodes {
				output, err := runCmd("pxctl status", node)
				log.FailOnError(err, "Failed to run pxctl status on node: %v", node.Name)
				log.Infof("pxctl status output: %v", output)

				if !strings.Contains(output, "Journal Device") {
					selectedNode = node
					break
				}
			}
			if selectedNode.Name == "" {
				log.FailOnError(fmt.Errorf("No node found without journal device"), "No node found without journal device")
			}
			log.Infof("Selected Node for add journal device: %v", selectedNode.Name)
		})
		stepLog = "Check lsblk state before addition of drive add of journal device"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			drivesMap, err := Inst().N.GetBlockDrives(selectedNode, node.SystemctlOpts{
				ConnectionOpts: node.ConnectionOpts{
					Timeout:         2 * time.Minute,
					TimeBeforeRetry: defaultRetryInterval,
				},
				Action: "start",
			})
			log.FailOnError(err, "Failed to get block drives on node %s", selectedNode.Name)
			// store map of drives
			for _, v := range drivesMap {
				log.InfoD("Drive path: %v , Drive size: %v", v.Path, v.Size)
				diskMapBeforeDriveAdd[v.Path] = v.Size
			}
		})

		stepLog = "Add a cloud drive as a journal device without pool in maintenance mode"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			//Add a cloud drive as a journal device without pool in maintenance mode
			driveSpecs, err := GetCloudDriveDeviceSpecs()
			log.FailOnError(err, "Error getting cloud drive specs")

			deviceSpec := driveSpecs[0]
			devicespecjournal := deviceSpec + " --journal"

			err = Inst().V.AddCloudDrive(&selectedNode, devicespecjournal, -1)
			if err == nil {
				log.FailOnError(fmt.Errorf("adding cloud drive with journal expected ? Error: [%v]", err),
					"adding cloud drive with journal failed ?")
			}
			log.InfoD("adding journal failed as expected. verifying the error: %v", err.Error())

			re := regexp.MustCompile(".*Requires pool maintenance mode*")
			if !re.MatchString(err.Error()) {
				log.FailOnError(err, "Requires pool maintenance mode alert is not received")
			}

			// Wait for lsblk to catch up [BUG PWX-36300] and verify if the journal device is not added on lsblk
			time.Sleep(30 * time.Second)

			// verify if the journal device is not added on lsblk
			drivesMap, err := Inst().N.GetBlockDrives(selectedNode, node.SystemctlOpts{
				ConnectionOpts: node.ConnectionOpts{
					Timeout:         2 * time.Minute,
					TimeBeforeRetry: defaultRetryInterval,
				},
			})
			log.FailOnError(err, "Failed to get block drives on node %s", selectedNode.Name)
			// store map of drives
			for _, v := range drivesMap {
				log.InfoD("Drive path: %v , Drive size: %v", v.Path, v.Size)
				if _, ok := diskMapBeforeDriveAdd[v.Path]; !ok {
					log.FailOnError(fmt.Errorf("Journal device added at path: %v", v.Path), "Journal device added at path this should have been blocked")
				}
			}
		})

		stepLog = "Put pool in maintenance mode and add a cloud drive as a journal device"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			//Put pool in maintenance mode
			defer func() {
				err := ExitPoolMaintenance(selectedNode)
				log.FailOnError(err, "Failed to exit pool maintenance mode")
			}()
			err := EnterPoolMaintenance(selectedNode)
			log.FailOnError(err, "Failed to enter pool maintenance mode")

			//Add a cloud drive as a journal device
			driveSpecs, err := GetCloudDriveDeviceSpecs()
			log.FailOnError(err, "Error getting cloud drive specs")

			deviceSpec := driveSpecs[0]
			devicespecjournal := deviceSpec + " --journal"

			err = Inst().V.AddCloudDrive(&selectedNode, devicespecjournal, -1)
			log.FailOnError(err, "Failed to add cloud drive as a journal device")
		})
		stepLog = "validate on pxctl and lsblk if metadata device has been added successfully"
		Step(stepLog, func() {
			//get metadata device path from pxctl status
			journalDevicePath := getJournalDevicePath(selectedNode)
			log.InfoD("Metadata device path from pxctl : %v", journalDevicePath)

			//After drive add run the command to check if metadata drive has been added or not.
			drivesMap, err := Inst().N.GetBlockDrives(selectedNode, node.SystemctlOpts{
				ConnectionOpts: node.ConnectionOpts{
					Timeout:         2 * time.Minute,
					TimeBeforeRetry: defaultRetryInterval,
				},
				Action: "start",
			})
			log.FailOnError(err, "Failed to get block drives on node %s", selectedNode.Name)

			for _, v := range drivesMap {
				log.InfoD("Drive path: %v , Drive size: %v", v.Path, v.Size)
				if _, ok := diskMapBeforeDriveAdd[v.Path]; !ok {
					log.Infof("New drive added at path: %v", v.Path)
					if strings.Contains(journalDevicePath, v.Path) {
						log.InfoD("Metadata device added at path: %v", v.Path)
					} else {
						log.FailOnError(fmt.Errorf("Journal device not added at path: %v", v.Path), "Journal device not added at path")
					}
				}
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

// Returns path and size of metadata device of node n
func getJournalDevicePath(n node.Node) string {
	output, err := runPxctlCommand("status | grep -A 1 'Journal Device:' | tail -n 1", n, nil)
	log.FailOnError(err, "Failed to run pxctl command on node: %v", n.Name)
	log.Infof("Output of pxctl status: %v", output)
	components := strings.Fields(output)

	// Extract path and size
	path := components[1]
	path = strings.TrimSuffix(path, "-part1")

	log.InfoD("Metadata device path: %v", path)
	return path
}

var _ = Describe("{DriveAddMetaDataDiskStatusCheck}", func() {

	/*
		https://portworx.atlassian.net/browse/PTX-15169
		1. Add 2 disk of same size/type to create a new Pool
		2. While disks are getting attached and after check status of metadata disk (metadata disk shouldn't be in rebalance)
		3. After adding the disk, check the status of metadata disk (metadata disk shouldn't be in rebalance)
		4. Add a disk of same size/type
		5. While disk is getting attached and after check status of metadata disk (metadata disk shouldn't be in rebalance)

	*/

	itLog := "DriveAddMetaDataDiskStatusCheck"
	JustBeforeEach(func() {
		StartTorpedoTest(itLog, "Drive add and check metadata disk status", nil, 0)
	})
	var contexts []*scheduler.Context
	var selectedNode node.Node

	It(itLog, func() {
		log.InfoD(itLog)
		stepLog := "Schedule apps"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("drive-add-metadata-disk-%d", i))...)
			}
		})
		defer DestroyApps(contexts, nil)
		ValidateApplications(contexts)

		stepLog = "Check which node has a metadata disk if not add one"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			storageNodes, err := GetStorageNodes()
			log.FailOnError(err, "Failed to get storage nodes")

			isDedicatedMetadataDiskExist := false

			//Check which node has metadata disk if not add one
			for _, storageNode := range storageNodes {
				path, err := getMetaDataDiskPath(storageNode)
				log.FailOnError(err, "Failed to get metadata disk")
				if path != "" {
					log.InfoD("Metadata disk path: %v", path)
					isDedicatedMetadataDiskExist = true
					selectedNode = storageNode
					break
				}
			}
			if !isDedicatedMetadataDiskExist {
				for _, storageNode := range storageNodes {
					deviceSpec := fmt.Sprintf("size=100 --metadata")
					log.InfoD("Initiate add cloud drive and validate")
					// enter pool maintenance mode
					err = Inst().V.EnterPoolMaintenance(storageNode)
					log.FailOnError(err, "node: %v failed to transition to pool maintenance mode", storageNode.Name)

					err := Inst().V.AddCloudDrive(&storageNode, deviceSpec, -1)
					if err != nil {
						if strings.Contains(err.Error(), "Cannot add metadata device when internal kvdb is running on this node") {
							log.Infof("Cannot add metadata device when internal kvdb is running on this node")
							err = nil
							continue
						}
					}
					log.FailOnError(err, "Failed to add metadata device on node : %s", selectedNode.Name)

					// exit pool maintenance
					err = Inst().V.ExitPoolMaintenance(storageNode)
					log.FailOnError(err, "Node: %v Failed to exit out of maintenance mode", storageNode.Name)

					selectedNode = storageNode
					break
				}
			}
			//check if selecteNode is empty or not
			if selectedNode.Name == "" {
				log.FailOnError(fmt.Errorf("No node found with metadata disk or metadata disks cannot be added to any nodes"), "No node found with metadata disk ")
			}
		})
		var wg sync.WaitGroup
		var devicePath string
		done := make(chan struct{})
		devicePath, err = getMetaDataDiskPath(selectedNode)
		log.FailOnError(err, "Failed to get metadata device path")

		stepLog = "Add 2 disk of same size/type to create a new Pool"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				defer func() {
					log.InfoD("Signalling metadatachecker to exit")
					close(done)
				}()
				drivePath := "size=57"
				for i := 0; i < 2; i++ {
					err = Inst().V.WaitDriverUpOnNode(selectedNode, Inst().DriverStartTimeout)
					log.FailOnError(err, "Failed to wait for driver to come up on node: %v", selectedNode.Name)

					err := Inst().V.AddCloudDrive(&selectedNode, drivePath, -1)
					log.FailOnError(err, "Failed to add a drive to the pool")
					time.Sleep(60 * time.Second)
				}
			}()
		})
		stepLog = "Keep checking if metadata disk goes into rebalance"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			wg.Add(1)
			go func() {
				defer wg.Done()
				waitForMetadataRebalance(done, devicePath, selectedNode)
			}()
		})
		wg.Wait()
		stepLog = "Again add a drive to an existing pool and check the metadatadevice"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			done := make(chan struct{})
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				defer close(done)
				err := AddCloudDrive(selectedNode, 0)
				log.FailOnError(err, "Failed to add a drive to the pool")
			}()
			go waitForMetadataRebalance(done, devicePath, selectedNode)
		})
		wg.Wait()
	})
})

// This function checks for metadata disk to be in rebalance state
func waitForMetadataRebalance(done <-chan struct{}, devicePath string, selectedNode node.Node) {
	defer GinkgoRecover()
	for {
		select {
		case <-done:
			// Received signal from the first goroutine
			fmt.Println("Metadata rebalance status checker is exiting")
			return
		default:
			err = Inst().V.WaitDriverUpOnNode(selectedNode, Inst().DriverStartTimeout)
			log.FailOnError(err, "Failed to wait for driver to come up on node: %v", selectedNode.Name)

			log.InfoD("Checking metadata disk status with device path: %v", devicePath)
			cmd := fmt.Sprintf("-j service drive add -d %s -o status", devicePath)
			// Simulated runPxctlCommand, replace with actual implementation
			_, err := runPxctlCommand(cmd, selectedNode, nil)
			log.InfoD("metadata status: %v", err.Error())

			if strings.Contains(err.Error(), "rebalance") {
				log.Warnf("Metadata device in rebalance")
				log.FailOnError(err, "Metadata device in rebalance")
				return
			}
			time.Sleep(5 * time.Second)
		}
	}
}

func getMetaDataDiskPath(n node.Node) (string, error) {
	output, err := runPxctlCommand("status | grep -A 1 'Metadata Device:' | tail -n 1", n, nil)
	if err != nil {
		return "", err
	}
	log.Infof("Output of pxctl status: %v", output)
	path := ""
	if output != "" {
		components := strings.Fields(output)
		// Extract path and size
		path = components[1]

		log.InfoD("Metadata device path: %v", path)
	}
	return path, nil
}

var _ = Describe("{NetworkDelayWhilePoolExpand}", func() {
	/*
	   https://portworx.atlassian.net/browse/PTX-15473
	   1. Deploy apps
	   2. Trigger pool expand using add drive/ resize
	   3. Introduce network delay with the cloud provider while pool expand is in progress
	   4. Check if the pool expand is successful
	*/
	JustBeforeEach(func() {
		StartTorpedoTest("NetworkDelayWhilePoolExpand", "Network delay while pool expand", nil, 0)
	})

	var contexts []*scheduler.Context
	var selectedNode node.Node

	itLog := "NetworkDelayWhilePoolExpand"
	It(itLog, func() {
		log.InfoD(itLog)
		stepLog := "schedule Application"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			//pick a random storage node
			storageNodes := node.GetStorageNodes()
			selectedNode = storageNodes[rand.Intn(len(storageNodes))]

			AppList := Inst().AppList
			Inst().AppList = []string{"fio-fastpath-repl1"}

			var err error
			defer func() {
				Inst().AppList = AppList
				err = Inst().S.RemoveLabelOnNode(selectedNode, k8s.NodeType)
				log.FailOnError(err, "error removing label on node [%s]", selectedNode.Name)
			}()
			err = Inst().S.AddLabelOnNode(selectedNode, k8s.NodeType, k8s.FastpathNodeType)
			log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", selectedNode.Name))

			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("network-delay-pool-expand-%d", i))...)
			}
		})
		ValidateApplications(contexts)
		defer DestroyApps(contexts, nil)

		done := make(chan bool)

		stepLog = "Trigger pool expand using add drive/ resize"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			go func() {
				defer GinkgoRecover()
				defer func() {
					done <- true
				}()
				selectedPool, err := GetPoolWithIOsInGivenNode(selectedNode, contexts, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, 100)
				log.FailOnError(err, "Failed to get pool with IOs in given node")
				log.InfoD("Selected pool for pool expand: %v", selectedPool.Uuid)

				isJournalEnabled, _ = IsJournalEnabled()

				expectedSize := selectedPool.TotalSize/units.GiB + 100

				err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize, true)
				log.FailOnError(err, "Failed to expand pool with add disk")

				err = waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isJournalEnabled)
				log.FailOnError(err, "Failed to wait for pool to be resized")

				poolToResize = getStoragePool(selectedPool.Uuid)
				expectedSize = selectedPool.TotalSize/units.GiB + 150

				err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
				log.FailOnError(err, "Failed to expand pool with add disk")
				err = waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isJournalEnabled)
				log.FailOnError(err, "Failed to wait for pool to be resized")

			}()

		})

		stepLog = "Introduce network delay with the cloud provider while pool expand is in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			// Introduce network delay
			Delay := 10
			defer func() {
				log.Infof(fmt.Sprintf("Deleting the delay of %dms from the node: %v", Delay, selectedNode.Name))
				err := Inst().N.InjectNetworkErrorWithRebootFallback([]node.Node{selectedNode}, "delay", "del", 0, Delay)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Removing delay of %dms from nodes", Delay))
			}()

			log.Infof(fmt.Sprintf("Adding a delay of %dms to a node: %v ", Delay, selectedNode.Name))
			err := Inst().N.InjectNetworkErrorWithRebootFallback([]node.Node{selectedNode}, "delay", "add", 0, Delay)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Adding a delay of %dms to nodes", Delay))

			//Wait for pool expand to complete
			<-done
			log.InfoD("Successfully expanded the pool and removing network delay for the node: %v", selectedNode.Name)

		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{PoolResizeWhenReplOneVolinPool}", func() {

	/*
			PTX:https://portworx.atlassian.net/browse/PTX-15464
		    1. Create px volumes with repl factor 1.
		    2. Resize pool with resize disk.

	*/

	JustBeforeEach(func() {
		StartTorpedoTest("PoolResizeWhenReplOneVolinPool", "Pool resize when repl factor is 1", nil, 0)
	})

	itLog := "PoolResizeWhenReplOneVolinPool"
	It(itLog, func() {
		// Create px volumes with repl factor 1
		volName := "pool-resize-repl1"

		var Wg sync.WaitGroup
		stepLog := "Create px volumes with repl factor 1"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			// Create 100 volumes with repl 1
			Wg.Add(1)
			go func() {
				defer Wg.Done()
				defer GinkgoRecover()
				for i := 0; i < 100; i++ {
					name := fmt.Sprintf("%s-%d", volName, i)
					vol, err := Inst().V.CreateVolume(name, 5, 1)
					log.FailOnError(err, "Failed to create volume")
					log.Infof("Volume created with ID: %s", vol)

				}
			}()
			// Wait for 30s for some volumes to be created
			time.Sleep(30 * time.Second)

		})

		stepLog = "Resize pool with resize disk"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			// Resize pool with resize disk
			name := "pool-resize-repl1-0"
			pool, err := GetPoolIDsFromVolName(name)
			log.FailOnError(err, "Failed to get pool id's from volume name: %v", name)

			isjournal, err := IsJournalEnabled()
			log.FailOnError(err, "Failed to check is Journal enabled")
			//TODO Need to handle the case for multiple pools

			poolID := pool[0]
			poolToResize, err = GetStoragePoolByUUID(poolID)
			log.FailOnError(err, "Failed to get pool using UUID %s", poolID)

			// Resize pool with resize disk
			expectedSize := (poolToResize.TotalSize / units.GiB) * 2
			expectedSizeWithJournal := expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}

			err = Inst().V.ExpandPool(poolID, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize, true)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, poolToResize.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Expected new size to be '%d' or '%d' if pool has journal", expectedSize, expectedSizeWithJournal))

		})
		Wg.Wait()

		//Delete all the volumes created
		stepLog = "Delete all the volumes created"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for i := 0; i < 100; i++ {
				Wg.Add(1)
				go func(i int) {
					defer Wg.Done()
					defer GinkgoRecover()
					name := fmt.Sprintf("%s-%d", volName, i)
					err := Inst().V.DeleteVolume(name)
					log.FailOnError(err, "Failed to delete volume with name: %v", name)
					log.InfoD("Successfully deleted volume with name: %v", name)
				}(i)
			}
		})
		Wg.Wait()
	})
})

var _ = Describe("{PoolDeleteMultiplePools}", func() {

	/*
		1. Have multiple pools
		2. Delete one of the middle pools like pool 1
		3. Do pool maintenance operations like pool expand
		4. Do pool delete which is greater than pool id deleted in step 2
		5. Restart the node to make sure things remain the same
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("PoolDeleteMultiplePools", "Pool delete with multiple pools", nil, 0)
	})

	var contexts []*scheduler.Context

	itLog := "PoolDeleteMultiplePools"
	It(itLog, func() {
		testNode := selectPoolDeletableNode()

		drvMap, err := Inst().V.GetPoolDrives(testNode)
		log.FailOnError(err, "error getting pool drives from node [%s]", testNode.Name)

		numPools := len(drvMap)
		targetNumPools := 3

		for i := numPools; i < targetNumPools; i++ {
			err = AddCloudDrive(*testNode, -1)
			log.FailOnError(err, "drive add failed")
		}

		drvMap, err = Inst().V.GetPoolDrives(testNode)
		log.FailOnError(err, "error getting pool drives from node [%s]", testNode.Name)

		poolIDs := []string{}

		for poolID := range drvMap {
			poolIDs = append(poolIDs, poolID)
		}

		sort.Strings(poolIDs)

		dash.VerifyFatal(len(poolIDs) >= targetNumPools, true, fmt.Sprintf("requires %v pools, but only has %+v", targetNumPools, drvMap))

		log.Info("test node %v, pools %+v", testNode.Name, poolIDs)

		stepLog := fmt.Sprintf("1. delete second pool, id %v", poolIDs[1])
		Step(stepLog, func() {
			log.InfoD(stepLog)
			deletePoolAndValidate(*testNode, poolIDs[1])
		})

		// add a pool back
		stepLog = "2. Add a drive back"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			log.Info("Wait 1 min for stabling everything after reboot")
			err = AddCloudDrive(*testNode, -1)
		})

		stepLog = fmt.Sprintf("3. delete third pool, id %v", poolIDs[2])
		Step(stepLog, func() {
			log.InfoD(stepLog)
			deletePoolAndValidate(*testNode, poolIDs[2])
		})
		stepLog = "4. Verify reboot"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err = RebootNodeAndWaitForPxUp(*testNode)
			log.FailOnError(err, "Failed to reboot node and wait till it is up")
			log.Info("Verify reboot succeed")
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})

})