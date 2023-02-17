package tests

import (
	"fmt"
	"math/rand"
	"reflect"
	"regexp"

	"github.com/google/uuid"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/volume"

	"github.com/portworx/torpedo/pkg/log"

	"strconv"
	"strings"
	"time"

	"github.com/portworx/torpedo/pkg/testrailuttils"

	"github.com/libopenstorage/openstorage/api"
	. "github.com/onsi/ginkgo"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/units"
	. "github.com/portworx/torpedo/tests"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	replicationUpdateTimeout = 4 * time.Hour
	poolResizeTimeout        = time.Minute * 360
	retryTimeout             = time.Minute * 2
	addDriveUpTimeOut        = time.Minute * 15
)

var _ = Describe("{StoragePoolExpandDiskResize}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("StoragePoolExpandDiskResize", "Validate storage pool expansion using resize-disk option", nil, 0)
	})

	var contexts []*scheduler.Context
	stepLog := "has to schedule apps, and expand it by resizing a disk"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("poolexpand-%d", i))...)
		}

		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, " Storage pools exist?")

		// pick a pool from a pools list and resize it
		poolIDToResize, err = GetPoolIDWithIOs()
		log.FailOnError(err, "error identifying pool to run test")
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		time.Sleep(time.Second * 60)
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64
		stepLog = "Calculate expected pool size and trigger pool resize"
		Step(stepLog, func() {

			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}

			log.InfoD("Current Size of the pool %s is %d", poolIDToResize, poolToBeResized.TotalSize/units.GiB)

			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
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

var _ = Describe("{StoragePoolExpandDiskAdd}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("StoragePoolExpandDiskAdd", "Validate storage pool expansion using add-disk option", nil, 0)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing pool and expand it by adding a disk"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pooladddisk-%d", i))...)
		}

		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, "Storage pools exist ?")

		// pick a pool from a pools list and resize it
		poolIDToResize, err = GetPoolIDWithIOs()
		log.FailOnError(err, "error identifying pool to run test")
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		stepLog = "Calculate expected pool size and trigger pool resize"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB
			expectedSize = roundUpValue(expectedSize)
			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check is Journal enabled")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}

			log.InfoD("Current Size of the pool %s is %d", poolIDToResize, poolToBeResized.TotalSize/units.GiB)

			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Expected new size to be '%d' or '%d' if pool has journal", expectedSize, expectedSizeWithJournal))
		})

		Step("Ensure that new pool has been expanded to the expected size", func() {
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

	var contexts []*scheduler.Context
	stepLog := "has to schedule apps, and expand it by resizing a disk"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("poolexpandauto-%d", i))...)
		}

		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, " Storage pools exist?")

		// pick a pool from a pools list and resize it
		poolIDToResize, err = GetPoolIDWithIOs()
		log.FailOnError(err, "error identifying pool to run test")
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		time.Sleep(time.Second * 60)
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64
		stepLog = "Calculate expected pool size and trigger pool resize"
		Step(stepLog, func() {
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}
			log.InfoD("Current Size of the pool %s is %d", poolIDToResize, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize)
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

	var contexts []*scheduler.Context

	stepLog := "has to schedule apps, and expand it by resizing a disk"
	It(stepLog, func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("poolresizediskreboot-%d", i))...)
		}

		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		dash.VerifyFatal(err, nil, "Validate list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, "Validate storage pools exist")

		// pick a pool from a pools list and resize it
		poolIDToResize, err = GetPoolIDWithIOs()
		log.FailOnError(err, "error identifying pool to run test")
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		time.Sleep(time.Second * 60)
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check is journal enabled")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}
			log.InfoD("Current Size of the pool %s is %d", poolIDToResize, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful ?")

			err = WaitForExpansionToStart(poolIDToResize)
			log.FailOnError(err, "Expansion is not started")

			storageNode, err := GetNodeWithGivenPoolID(poolIDToResize)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			err = RebootNodeAndWait(*storageNode)
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
	var contexts []*scheduler.Context

	stepLog := "should get the existing pool and expand it by adding a disk"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pooladddiskreboot-%d", i))...)
		}

		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, "Storage pools exist?")

		// pick a pool from a pools list and resize it
		poolIDToResize, err = GetPoolIDWithIOs()
		log.FailOnError(err, "error identifying pool to run test")
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
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
			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check is journal enabled")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}
			log.InfoD("Current Size of the pool %s is %d", poolIDToResize, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			err = WaitForExpansionToStart(poolIDToResize)
			log.FailOnError(err, "Failed while waiting for expansion to start")

			storageNode, err := GetNodeWithGivenPoolID(poolIDToResize)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			err = RebootNodeAndWait(*storageNode)
			log.FailOnError(err, "Failed to reboot node and wait till it is up")
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
	})

	var (
		contexts           []*scheduler.Context
		err                error
		pools              map[string]*api.StoragePool
		poolsToBeResized   []*api.StoragePool
		nodePoolToExpanded node.Node
		nodePools          []node.StoragePool
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
			nodePools = stNode.StoragePools
			nodePoolToExpanded = stNode
			eligibility, err = GetPoolExpansionEligibility(&stNode)
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
				if poolResizeIsInProgress(poolToBeResized) {
					// wait until resize is completed and get the updated pool again
					poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
					log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID  %s", poolIDToResize))
				}
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64
		poolsExpectedSizeMap := make(map[string]uint64)
		isjournal, err := isJournalEnabled()
		log.FailOnError(err, "Failed to check is Journal Enabled")
		stepLog = fmt.Sprintf("Calculate expected pool size and trigger pool resize for %s", nodePoolToExpanded.Name)
		Step(stepLog, func() {

			for _, poolToBeResized := range poolsToBeResized {
				drvSize, err := getPoolDiskSize(poolToBeResized)
				log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
				expectedSize = (poolToBeResized.TotalSize / units.GiB) + drvSize
				poolsExpectedSizeMap[poolToBeResized.Uuid] = expectedSize

				//To-Do Need to handle the case for multiple pools
				expectedSizeWithJournal = expectedSize
				if isjournal {
					expectedSizeWithJournal = expectedSizeWithJournal - 3
				}
				log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
				err = Inst().V.ExpandPool(poolToBeResized.Uuid, operation, expectedSize)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Pool %s expansion init succesful?", poolToBeResized.Uuid))
				err = WaitForExpansionToStart(poolToBeResized.Uuid)
				log.FailOnError(err, "pool expansion not started")
			}

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
		contexts             []*scheduler.Context
		poolIDToResize       string
		poolToBeResized      *api.StoragePool
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

		volSelected, err = getVolumeWithMinimumSize(contexts, 10)
		log.FailOnError(err, "error identifying volume")
		log.Infof("%+v", volSelected)
		rs, err := Inst().V.GetReplicaSets(volSelected)
		log.FailOnError(err, fmt.Sprintf("error getting replica sets for vol %s", volSelected.Name))
		attachedNodeID := rs[0].Nodes[0]
		volumePools := rs[0].PoolUuids
		for _, stNode := range stNodes {
			if stNode.Id == attachedNodeID {
				nodeSelected = stNode
			}
		}

		if &nodeSelected == nil {
			dash.VerifyFatal(false, true, "unable to identify the node for add new pool")
		}
	poolloop:
		for _, volPool := range volumePools {
			for _, nodePool := range nodeSelected.Pools {
				if nodePool.Uuid == volPool {
					poolIDToResize = nodePool.Uuid
					break poolloop
				}
			}
		}
		dash.Infof("selected node %s, pool %s", nodeSelected.Name, poolIDToResize)
		poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
		log.FailOnError(err, "unable to get pool using UUID")
		currentTotalPoolSize = poolToBeResized.TotalSize / units.GiB
		pools, err = Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "error getting storage pools")
		existingPoolsCount := len(pools)
		///creating a spec to perform add  drive
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
		expandedExpectedPoolSize := currentTotalPoolSize + specSize

		stepLog = fmt.Sprintf("Verify that pool %s can be expanded", poolIDToResize)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			isPoolHealthy := poolResizeIsInProgress(poolToBeResized)
			dash.VerifyFatal(isPoolHealthy, true, "Verfiy pool before expansion")
		})

		stepLog = fmt.Sprintf("Trigger pool %s resize by add-disk", poolIDToResize)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			dash.VerifyFatal(err, nil, "Validate is journal enabled check")
			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expandedExpectedPoolSize)
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

			err = Inst().V.AddCloudDrive(&nodeSelected, newSpec, -1)
			log.FailOnError(err, fmt.Sprintf("Add cloud drive failed on node %s", nodeSelected.Name))

			log.InfoD("Validate pool rebalance after drive add")
			err = ValidatePoolRebalance()
			log.FailOnError(err, fmt.Sprintf("pool %s rebalance failed", poolIDToResize))
			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "is journal enabled check failed")
			err = waitForPoolToBeResized(expandedExpectedPoolSize, poolIDToResize, isjournal)
			log.FailOnError(err, "Error waiting for poor resize")
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

func poolResizeIsInProgress(poolToBeResized *api.StoragePool) bool {
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
				err = ValidatePoolRebalance()
				if err != nil {
					return nil, true, fmt.Errorf("errorvalidatng  err %v", err)
				}
				log.Infof("Pool Resize is already in progress: %v", updatedPoolToBeResized.LastOperation)
				return nil, true, nil
			}
			return nil, false, nil
		}

		_, err := task.DoRetryWithTimeout(f, poolResizeTimeout, retryTimeout)
		if err != nil {
			dash.VerifyFatal(err, nil, "Verify pool status before expansion")
		}
		return true
	}
	return true
}

func waitForPoolToBeResized(expectedSize uint64, poolIDToResize string, isJournalEnabled bool) error {

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
				return nil, true, fmt.Errorf("pool %s is in pending state, waiting to to start", poolIDToResize)
			}
			if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_IN_PROGRESS {
				if strings.Contains(expandedPool.LastOperation.Msg, "Rebalance in progress") {
					if currentLastMsg == expandedPool.LastOperation.Msg {
						return nil, false, fmt.Errorf("pool reblance is not progressing")
					}
					currentLastMsg = expandedPool.LastOperation.Msg
					return nil, true, fmt.Errorf("wait for pool rebalance to complete")
				}

				if strings.Contains(expandedPool.LastOperation.Msg, "No pending operation pool status: Maintenance") {
					return nil, false, nil
				}

				return nil, true, fmt.Errorf("waiting for pool status to update")
			}
		}
		newPoolSize := expandedPool.TotalSize / units.GiB
		err = ValidatePoolRebalance()
		if err != nil {
			return nil, true, fmt.Errorf("pool %s not been resized .Current size is %d,Error while pool rebalance: %v", poolIDToResize, newPoolSize, err)
		}
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
	return err
}

func getPoolLastOperation(poolID string) (*api.StoragePoolOperation, error) {
	log.Infof(fmt.Sprintf("Gettting pool status for %s", poolID))
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

func isJournalEnabled() (bool, error) {
	storageSpec, err := Inst().V.GetStorageSpec()
	if err != nil {
		return false, err
	}
	jDev := storageSpec.GetJournalDev()
	if jDev != "" {
		log.Infof("JournalDev: %s", jDev)
		return true, nil
	}
	return false, nil
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
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool by adding a drive"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pooladddrive-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		stNode, err := GetRandomNodeWithPoolIOs(stNodes)
		log.FailOnError(err, "error identifying node to run test")
		err = addCloudDrive(stNode, -1)
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
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool by adding a drive"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pladddrvrestrt-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		stNode, err := GetRandomNodeWithPoolIOs(stNodes)
		log.FailOnError(err, "error identifying node to run test")
		err = addCloudDrive(stNode, -1)
		log.FailOnError(err, "error adding cloud drive")
		stepLog = fmt.Sprintf("Restart PX on node %s", stNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err := Inst().V.RestartDriver(stNode, nil)
			log.FailOnError(err, fmt.Sprintf("error restarting px on node %s", stNode.Name))
			err = Inst().V.WaitDriverUpOnNode(stNode, 2*time.Minute)
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

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		stNode, err := GetRandomNodeWithPoolIOs(stNodes)
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
			err = ValidatePoolRebalance()
			log.FailOnError(err, "Pool re-balance failed")
			dash.VerifyFatal(err == nil, true, "PX is up after add drive with vol driver restart")

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
		volSelected, err := getVolumeWithMinimumSize(contexts, 10)
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
		selectedPool := stNode.StoragePools[0]
		err = addCloudDrive(stNode, selectedPool.ID)
		log.FailOnError(err, "error adding cloud drive")
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

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		stNode, err := GetRandomNodeWithPoolIOs(stNodes)
		log.FailOnError(err, "error identifying node to run test")
		err = Inst().V.EnterMaintenance(stNode)
		log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenence mode", stNode.Name))
		status, err := Inst().V.GetNodeStatus(stNode)
		log.Infof(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))
		stepLog = fmt.Sprintf("add cloud drive to the node %s", stNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err = addCloudDrive(stNode, -1)
			if err != nil {
				errStr := err.Error()
				res := strings.Contains(errStr, "node in maintenance mode") || strings.Contains(errStr, "couldn't get: /adddrive")
				dash.VerifySafely(res, true, fmt.Sprintf("Add drive failed when node [%s] is in maintenance mode. Error: %s", stNode.Name, errStr))
			} else {
				dash.VerifyFatal(err == nil, false, fmt.Sprintf("Add drive succeeded whien node [%s] is in maintenance mode", stNode.Name))
			}
		})
		t := func() (interface{}, bool, error) {
			if err := Inst().V.ExitMaintenance(stNode); err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}

		_, err = task.DoRetryWithTimeout(t, 15*time.Minute, 2*time.Minute)
		log.FailOnError(err, fmt.Sprintf("fail to exit maintenence mode in node %s", stNode.Name))
		status, err = Inst().V.GetNodeStatus(stNode)
		log.FailOnError(err, fmt.Sprintf("err getting node [%s] status", stNode.Name))
		log.Infof(fmt.Sprintf("Node %s status %s after exit", stNode.Name, status.String()))

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
		err := addCloudDrive(slNode, -1)
		log.FailOnError(err, "error adding cloud drive")
		err = Inst().V.RefreshDriverEndpoints()
		log.FailOnError(err, "error refreshing end points")
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
			dash.VerifyFatal(poolResizeIsInProgress(poolToBeResized), true, fmt.Sprintf("can pool %s expansion start?", poolToBeResized.Uuid))
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		stepLog = "Calculate expected pool size and trigger pool expansion by resize-disk "
		Step(stepLog, func() {
			log.InfoD(stepLog)
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check is journal enabled")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}
			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check is journal enabled")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}
			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			log.FailOnError(err, fmt.Sprintf("Pool %s expansion init failed", poolToResize.Uuid))

			resizeErr := waitForPoolToBeResized(expectedSize, poolToResize.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Expected new size to be '%d' or '%d'", expectedSize, expectedSizeWithJournal))
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

func addCloudDrive(stNode node.Node, poolID int32) error {
	driveSpecs, err := GetCloudDriveDeviceSpecs()
	if err != nil {
		return fmt.Errorf("error getting cloud drive specs, err: %v", err)
	}
	deviceSpec := driveSpecs[0]
	deviceSpecParams := strings.Split(deviceSpec, ",")
	var specSize uint64
	var driveSize string

	if poolID != -1 {
		systemOpts := node.SystemctlOpts{
			ConnectionOpts: node.ConnectionOpts{
				Timeout:         2 * time.Minute,
				TimeBeforeRetry: defaultRetryInterval,
			},
			Action: "start",
		}
		drivesMap, err := Inst().N.GetBlockDrives(stNode, systemOpts)
		if err != nil {
			return fmt.Errorf("error getting block drives from node %s, Err :%v", stNode.Name, err)
		}

	outer:
		for _, v := range drivesMap {
			labels := v.Labels
			for _, pID := range labels {
				if pID == fmt.Sprintf("%d", poolID) {
					driveSize = v.Size
					i := strings.Index(driveSize, "G")
					driveSize = driveSize[:i]
					break outer
				}
			}
		}
	}

	if driveSize != "" {
		paramsArr := make([]string, 0)
		for _, param := range deviceSpecParams {
			if strings.Contains(param, "size") {
				paramsArr = append(paramsArr, fmt.Sprintf("size=%s,", driveSize))
			} else {
				paramsArr = append(paramsArr, param)
			}
		}
		deviceSpec = strings.Join(paramsArr, ",")
		specSize, err = strconv.ParseUint(driveSize, 10, 64)
		if err != nil {
			return fmt.Errorf("error converting size to uint64, err: %v", err)
		}
	}

	pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
	if err != nil {
		return fmt.Errorf("error getting pools list, err: %v", err)
	}
	dash.VerifyFatal(len(pools) > 0, true, "Verify pools exist")

	var currentTotalPoolSize uint64

	for _, pool := range pools {
		currentTotalPoolSize += pool.GetTotalSize() / units.GiB
	}

	log.Info(fmt.Sprintf("current pool size: %d GiB", currentTotalPoolSize))

	expectedTotalPoolSize := currentTotalPoolSize + specSize

	log.InfoD("Initiate add cloud drive and validate")
	err = Inst().V.AddCloudDrive(&stNode, deviceSpec, poolID)
	if err != nil {
		return fmt.Errorf("add cloud drive failed on node %s, err: %v", stNode.Name, err)
	}
	log.InfoD("Validate pool rebalance after drive add")
	err = ValidatePoolRebalance()
	if err != nil {
		return fmt.Errorf("pool re-balance failed, err: %v", err)
	}
	err = Inst().V.WaitDriverUpOnNode(stNode, addDriveUpTimeOut)
	if err != nil {
		return fmt.Errorf("volume driver is down on node %s, err: %v", stNode.Name, err)
	}
	dash.VerifyFatal(err == nil, true, "PX is up after add drive")

	var newTotalPoolSize uint64

	pools, err = Inst().V.ListStoragePools(metav1.LabelSelector{})
	if err != nil {
		return fmt.Errorf("error getting pools list, err: %v", err)
	}
	dash.VerifyFatal(len(pools) > 0, true, "Verify pools exist")
	for _, pool := range pools {
		newTotalPoolSize += pool.GetTotalSize() / units.GiB
	}
	isPoolSizeUpdated := false

	if newTotalPoolSize == expectedTotalPoolSize || newTotalPoolSize == (expectedTotalPoolSize-3) {
		isPoolSizeUpdated = true
	}
	log.Info(fmt.Sprintf("updated pool size: %d GiB", newTotalPoolSize))
	dash.VerifyFatal(isPoolSizeUpdated, true, fmt.Sprintf("Validate total pool size after add cloud drive on node %s", stNode.Name))
	return nil
}
func getVolumeWithMinimumSize(contexts []*scheduler.Context, size uint64) (*volume.Volume, error) {
	var volSelected *volume.Volume
	//waiting till one of the volume has enough IO and selecting pool and node  using the volume to run the test
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
				usedBytes := appVol.GetUsage()
				usedGiB := usedBytes / units.GiB
				if usedGiB > size {
					volSelected = vol
					return nil, false, nil
				}
			}
		}
		return nil, true, fmt.Errorf("error getting volume with size atleast %d GiB used", size)
	}
	_, err := task.DoRetryWithTimeout(f, 60*time.Minute, retryTimeout)
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

var _ = Describe("{PoolResizeMul}", func() {
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
			selectedPool, err = GetPoolWithIOsInGivenNode(stNode)
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
			err = addCloudDrive(selectedNode, selectedPool.ID)
			log.FailOnError(err, "error adding cloud drive")
		})
		stepLog = fmt.Sprintf("Adding drive again to the node %s and pool UUID: %s, Id:%d", selectedNode.Name, selectedPool.Uuid, selectedPool.ID)
		Step(stepLog, func() {
			err = addCloudDrive(selectedNode, selectedPool.ID)
			log.FailOnError(err, "error adding cloud drive")
		})

		stepLog = fmt.Sprintf("Expanding pool  on node %s and pool UUID: %s using auto", selectedNode.Name, selectedPool.Uuid)
		Step(stepLog, func() {
			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize)
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
			selectedPool, err = GetPoolWithIOsInGivenNode(stNode)
			if selectedPool != nil {
				selectedNode = stNode
				break
			}
		}
		log.FailOnError(err, "error identifying node to run test")
		isjournal, err := isJournalEnabled()
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
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
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
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
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
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
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
	//4) Expand pool again by radd-disk with different size multiple.
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
			selectedPool, err = GetPoolWithIOsInGivenNode(stNode)
			if selectedPool != nil {
				selectedNode = stNode
				break
			}
		}
		log.FailOnError(err, "error identifying node to run test")
		isjournal, err := isJournalEnabled()
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
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
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
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", selectedPool.Uuid, selectedNode.Name))
		})

		stepLog = fmt.Sprintf("Expanding pool 3rd time on node %s and pool UUID: %s using add-disk", selectedNode.Name, selectedPool.Uuid)
		Step(stepLog, func() {
			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + 100 + drvSize

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
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
		isjournal, err := isJournalEnabled()
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
			selectedPool, err = GetPoolWithIOsInGivenNode(stNode)
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
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
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

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		stNode, err := GetRandomNodeWithPoolIOs(stNodes)
		log.FailOnError(err, "error identifying node to run test")
		selectedPool, err := GetPoolWithIOsInGivenNode(stNode)
		log.FailOnError(err, "error identifying pool to run test")

		stepLog := "Initiate pool expansion drive and restart PX"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
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
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("adddskwrst-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		stNode, err := GetRandomNodeWithPoolIOs(stNodes)
		log.FailOnError(err, "error identifying node to run test")
		selectedPool, err := GetPoolWithIOsInGivenNode(stNode)
		log.FailOnError(err, "error identifying pool to run test")

		stepLog := "Initiate pool expansion drive and restart PX"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedPool.Uuid))
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			err = WaitForExpansionToStart(poolToBeResized.Uuid)
			log.FailOnError(err, "pool expansion not started")
			err = Inst().V.RestartDriver(stNode, nil)
			log.FailOnError(err, fmt.Sprintf("error restarting px on node %s", stNode.Name))

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", selectedPool.Uuid, stNode.Name))

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
		volSelected, err := getVolumeWithMinimumSize(contexts, 10)
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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
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
		volSelected, err := getVolumeWithMinimumSize(contexts, 10)
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

		stepLog := "Initiate pool expansion using resize-disk"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", selectedPool.Uuid, stNode.Name))

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
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{VolUpdateAddDisk}", func() {
	//1) Deploy px with cloud drive.
	//2) Create a volume on that pool and write some data on the volume.
	//3) expand the volume to the pool using add-disk
	//4) perform resize disk operation on the pool while volume update is in-progress

	JustBeforeEach(func() {
		StartTorpedoTest("VolUpdateAddDisk", "expand volume to the pool and pool expansion using resize-disk", nil, 0)
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
		volSelected, err := getVolumeWithMinimumSize(contexts, 10)
		log.FailOnError(err, "error identifying volume")
		appVol, err := Inst().V.InspectVolume(volSelected.ID)
		log.FailOnError(err, fmt.Sprintf("error inspecting vol : %s", volSelected.ID))
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
				log.FailOnError(err, fmt.Sprintf("error setting repl factor to %d for vol : %s", newRep, volSelected.Name))
			}
			log.InfoD(fmt.Sprintf("setting repl factor to %d for vol : %s", newRep+1, volSelected.Name))
			err = Inst().V.SetReplicationFactor(volSelected, newRep+1, []string{stNode.Id}, []string{poolToBeResized.Uuid}, false, opts)
			log.FailOnError(err, fmt.Sprintf("error setting repl factor to %d for vol : %s", newRep+1, volSelected.Name))
			dash.VerifyFatal(err == nil, true, fmt.Sprintf("vol %s expansion triggered successfully on node %s", volSelected.Name, stNode.Name))
		})

		stepLog := "Initiate pool expansion using resize-disk"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", selectedPool.Uuid, stNode.Name))

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
	//4) perform add drive on the pool while volume update is in-progress
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
		volSelected, err := getVolumeWithMinimumSize(contexts, 10)
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
			err = addCloudDrive(stNode, poolToBeResized.ID)
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

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		stNode, err := GetRandomNodeWithPoolIOs(stNodes)
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
			time.Sleep(3 * time.Second)
			err = RebootNodeAndWait(stNode)
			log.FailOnError(err, fmt.Sprintf("error rebooting node %s", stNode.Name))
			log.InfoD("Validate pool rebalance after drive add")
			err = ValidatePoolRebalance()
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
		StartTorpedoTest("MulPoolsResize", "Initiate multiple pool resize on same node in parallel ", nil, testrailID)
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

				log.FailOnError(err, "Failed to check if Journal enabled")

				log.InfoD("Current Size of the pool %s is %d", selPool.Uuid, poolToBeResized.TotalSize/units.GiB)
				err = Inst().V.ExpandPool(selPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
				dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			}

			isjournal, err := isJournalEnabled()
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
		StartTorpedoTest("MulPoolsAddDisk", "Initiate multiple pool add-disk on same node in parallel ", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node with multiple pools and expand pools at same time using add-disk"

	It(stepLog, func() {
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
			el, err := GetPoolExpansionEligibility(&stNode)
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
				err = Inst().V.ExpandPool(selPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
				dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			}

			isjournal, err := isJournalEnabled()
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
		StartTorpedoTest("ResizeWithJrnlAndMeta", "Initiate pool expansion using resize-disk for the pool the with journal and metadata devices", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the metadata node and expand the pool by resize-disk"

	It(stepLog, func() {
		log.InfoD(stepLog)
		journalStatus, err := isJournalEnabled()
		log.FailOnError(err, "err getting journal status")
		dash.VerifyFatal(journalStatus, true, "verify journal device is enabled")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rsizedrvmeta-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetMetadataNodes()

		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Metadata nodes found?")
		}
		stNode, err := GetRandomNodeWithPoolIOs(stNodes)
		log.FailOnError(err, "error identifying node to run test")
		stNodePools := stNode.Pools

		var selectedPool *api.StoragePool
		for _, pool := range stNodePools {
			if pool.ID == 0 {
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
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
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
			selectedPool, err = GetPoolWithIOsInGivenNode(stNode)
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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", poolToBeResized.Uuid, selectedNode.Name))

			log.InfoD(fmt.Sprintf("Performing node maintenance cycle on node %s", selectedNode.Name))
			err = Inst().V.RecoverDriver(selectedNode)
			log.FailOnError(err, fmt.Sprintf("error performing maintenance cycle on node %s", selectedNode.Name))

			err = Inst().V.WaitDriverUpOnNode(selectedNode, 2*time.Minute)
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
			selectedPool, err = GetPoolWithIOsInGivenNode(stNode)
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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", poolToBeResized.Uuid, selectedNode.Name))

			log.InfoD(fmt.Sprintf("Performing node maintenance cycle on node %s", selectedNode.Name))
			err = Inst().V.RecoverDriver(selectedNode)
			log.FailOnError(err, fmt.Sprintf("error performing maintenance cycle on node %s", selectedNode.Name))

			err = Inst().V.WaitDriverUpOnNode(selectedNode, 2*time.Minute)
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
		StartTorpedoTest("ResizePoolMaintenanceCycle", "Initiate pool expansion using resize-disk and perform node maintenance cycle", nil, 0)

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
			selectedPool, err = GetPoolWithIOsInGivenNode(stNode)
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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", poolToBeResized.Uuid, selectedNode.Name))

			log.InfoD(fmt.Sprintf("Performing pool maintenance cycle on node %s", selectedNode.Name))
			err = Inst().V.RecoverPool(selectedNode)
			log.FailOnError(err, fmt.Sprintf("error performing pool maintenance cycle on node %s", selectedNode.Name))

			err = Inst().V.WaitDriverUpOnNode(selectedNode, 2*time.Minute)
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
			selectedPool, err = GetPoolWithIOsInGivenNode(stNode)
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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
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
		poolIDToResize, err := GetPoolIDWithIOs()
		log.FailOnError(err, "error identifying pool to run test")
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
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			}
		})

		stNode, err := GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "error identifying node to run test")
		log.InfoD(fmt.Sprintf("Entering maintenence mode on node %s", stNode.Name))
		err = Inst().V.EnterMaintenance(*stNode)
		log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenence mode", stNode.Name))
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
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
		})

		log.InfoD(fmt.Sprintf("Exiting maintenence mode on node %s", stNode.Name))
		t := func() (interface{}, bool, error) {
			if err := Inst().V.ExitMaintenance(*stNode); err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 15*time.Minute, 2*time.Minute)
		log.FailOnError(err, fmt.Sprintf("fail to exit maintenence mode in node %s", stNode.Name))
		err = Inst().V.WaitDriverUpOnNode(*stNode, 2*time.Minute)
		log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", stNode.Name))
		dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after exiting maintenance on node %s", stNode.Name))
		status, err = Inst().V.GetNodeStatus(*stNode)
		log.FailOnError(err, fmt.Sprintf("Error getting status on node %s", stNode.Name))
		log.Infof(fmt.Sprintf("Node %s status %s after exit", stNode.Name, status.String()))

		stepLog = fmt.Sprintf("validating pool [%s] expansion", poolToBeResized.Uuid)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			isjournal, err := isJournalEnabled()
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
		poolIDToResize, err := GetPoolIDWithIOs()
		log.FailOnError(err, "error identifying pool to run test")
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
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			}
		})

		stNode, err := GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "error identifying node to run test")
		log.InfoD(fmt.Sprintf("Entering maintenence mode on node %s", stNode.Name))
		err = Inst().V.EnterMaintenance(*stNode)
		log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenence mode", stNode.Name))
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
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

		})
		log.InfoD(fmt.Sprintf("Exiting maintenence mode on node %s", stNode.Name))
		t := func() (interface{}, bool, error) {
			if err := Inst().V.ExitMaintenance(*stNode); err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 15*time.Minute, 2*time.Minute)
		log.FailOnError(err, fmt.Sprintf("fail to exit maintenence mode in node %s", stNode.Name))
		err = Inst().V.WaitDriverUpOnNode(*stNode, 2*time.Minute)
		log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", stNode.Name))
		dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after exiting maintenance on node %s", stNode.Name))
		status, err = Inst().V.GetNodeStatus(*stNode)
		log.FailOnError(err, fmt.Sprintf("Error getting status on node %s", stNode.Name))
		log.Infof(fmt.Sprintf("Node %s status %s after exit", stNode.Name, status.String()))

		stepLog = fmt.Sprintf("validating pool [%s] expansion", poolToBeResized.Uuid)
		Step(stepLog, func() {
			isjournal, err := isJournalEnabled()
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
		poolIDToResize, err := GetPoolIDWithIOs()
		log.FailOnError(err, "error identifying pool to run test")
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
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			}
		})

		stNode, err := GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "error identifying node to run test")
		log.InfoD(fmt.Sprintf("Entering pool maintenence mode on node %s", stNode.Name))
		err = Inst().V.EnterPoolMaintenance(*stNode)
		log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenence mode", stNode.Name))
		status, err := Inst().V.GetNodeStatus(*stNode)
		log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))
		stepLog = fmt.Sprintf("pool expansion to the node %s", stNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", poolToBeResized.Uuid, stNode.Name))

		})
		log.InfoD(fmt.Sprintf("Exiting pool maintenence mode on node %s", stNode.Name))
		t := func() (interface{}, bool, error) {
			if err := Inst().V.ExitPoolMaintenance(*stNode); err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 1*time.Minute)
		err = Inst().V.WaitDriverUpOnNode(*stNode, 2*time.Minute)
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

	JustBeforeEach(func() {
		StartTorpedoTest("PoolMaintenanceModeAddDisk", "pool expansion using add-disk when pool is in maintenance mode", nil, 0)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and put it in maintenance mode"

	It(stepLog, func() {
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
		poolIDToResize, err := GetPoolIDWithIOs()
		log.FailOnError(err, "error identifying pool to run test")
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
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
			}
		})

		stNode, err := GetNodeWithGivenPoolID(poolIDToResize)
		log.FailOnError(err, "error identifying node to run test")
		log.InfoD(fmt.Sprintf("Entering maintenence mode on node %s", stNode.Name))
		err = Inst().V.EnterPoolMaintenance(*stNode)
		log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenence mode", stNode.Name))
		status, err := Inst().V.GetNodeStatus(*stNode)
		log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))
		stepLog = fmt.Sprintf("pool expansion to the node %s", stNode.Name)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", poolToBeResized.Uuid, stNode.Name))

		})
		log.InfoD(fmt.Sprintf("Exiting pool maintenence mode on node %s", stNode.Name))
		t := func() (interface{}, bool, error) {
			if err := Inst().V.ExitPoolMaintenance(*stNode); err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 1*time.Minute)
		err = Inst().V.WaitDriverUpOnNode(*stNode, 2*time.Minute)
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

var _ = Describe("{AddDiskNodeMaintenanceMode}", func() {
	/*
		1. Trigger pool expansion using add-disk
		2. Place node in maintenance mode once expansion starts
		3. Validate pool expansion
		4. Exit maintenance mode
	*/
	JustBeforeEach(func() {
		StartTorpedoTest("AddDiskMaintenanceMode", "pool expansion using add-disk then put node is in maintenance mode", nil, 0)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node,trigger add-disk and put it in maintenance mode"

	It(stepLog, func() {
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
		poolIDToResize, err := GetPoolIDWithIOs()
		log.FailOnError(err, "error identifying pool to run test")
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
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			err = WaitForExpansionToStart(poolToBeResized.Uuid)
			log.FailOnError(err, "pool expansion not started")
			log.InfoD(fmt.Sprintf("Entering maintenence mode on node %s", stNode.Name))
			err = Inst().V.EnterMaintenance(*stNode)
			log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenence mode", stNode.Name))
			status, err := Inst().V.GetNodeStatus(*stNode)
			log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", poolToBeResized.Uuid, stNode.Name))

		})
		log.InfoD(fmt.Sprintf("Exiting maintenence mode on node %s", stNode.Name))
		t := func() (interface{}, bool, error) {
			if err := Inst().V.ExitMaintenance(*stNode); err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 15*time.Minute, 2*time.Minute)
		log.FailOnError(err, fmt.Sprintf("fail to exit maintenence mode in node %s", stNode.Name))
		err = Inst().V.WaitDriverUpOnNode(*stNode, 2*time.Minute)
		log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", stNode.Name))
		dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after exiting maintenance on node %s", stNode.Name))
		status, err := Inst().V.GetNodeStatus(*stNode)
		log.FailOnError(err, "error get node [%s] status", stNode.Name)
		log.Infof(fmt.Sprintf("Node %s status %s after exit", stNode.Name, status.String()))
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
		3. Validate pool expansion
		4. Exit maintenance mode
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("ResizeNodeMaintenanceMode", "pool expansion using resize-disk then put node is in maintenance mode", nil, 0)

	})
	var contexts []*scheduler.Context

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
		poolIDToResize, err := GetPoolIDWithIOs()
		log.FailOnError(err, "error identifying pool to run test")
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
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			err = WaitForExpansionToStart(poolToBeResized.Uuid)
			log.FailOnError(err, "pool expansion not started")
			log.InfoD(fmt.Sprintf("Entering maintenence mode on node %s", stNode.Name))
			err = Inst().V.EnterMaintenance(*stNode)
			log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenence mode", stNode.Name))
			status, err := Inst().V.GetNodeStatus(*stNode)
			log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))
			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", poolToBeResized.Uuid, stNode.Name))

		})
		log.InfoD(fmt.Sprintf("Exiting maintenence mode on node %s", stNode.Name))
		t := func() (interface{}, bool, error) {
			if err := Inst().V.ExitMaintenance(*stNode); err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 15*time.Minute, 2*time.Minute)
		log.FailOnError(err, fmt.Sprintf("fail to exit maintenence mode in node %s", stNode.Name))
		err = Inst().V.WaitDriverUpOnNode(*stNode, 2*time.Minute)
		log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", stNode.Name))
		dash.VerifyFatal(err == nil, true, fmt.Sprintf("PX is up after exiting maintenance on node %s", stNode.Name))
		status, err := Inst().V.GetNodeStatus(*stNode)
		log.FailOnError(err, "error getting node [%s] status", stNode.Name)
		log.Infof(fmt.Sprintf("Node %s status %s after exit", stNode.Name, status.String()))
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
	JustBeforeEach(func() {
		StartTorpedoTest("ResizePoolMaintenanceMode", "pool expansion using resize-disk then put pool in maintenance mode", nil, 0)

	})
	var contexts []*scheduler.Context

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
		poolIDToResize, err := GetPoolIDWithIOs()
		log.FailOnError(err, "error identifying pool to run test")
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
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			err = WaitForExpansionToStart(poolToBeResized.Uuid)
			log.FailOnError(err, "pool expansion not started")
			log.InfoD(fmt.Sprintf("Entering pool maintenence mode on node %s", stNode.Name))
			err = Inst().V.EnterPoolMaintenance(*stNode)
			log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenence mode", stNode.Name))
			status, err := Inst().V.GetNodeStatus(*stNode)
			log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))

			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using resize-disk", poolToBeResized.Uuid, stNode.Name))

		})
		log.InfoD(fmt.Sprintf("Exiting pool maintenence mode on node %s", stNode.Name))
		t := func() (interface{}, bool, error) {
			if err := Inst().V.ExitPoolMaintenance(*stNode); err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 1*time.Minute)
		err = Inst().V.WaitDriverUpOnNode(*stNode, 2*time.Minute)
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

	JustBeforeEach(func() {
		StartTorpedoTest("AddDiskPoolMaintenanceMode", "pool expansion using add-disk then put pool in maintenance mode", nil, 0)

	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and put it in maintenance mode"

	It(stepLog, func() {
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
		poolIDToResize, err := GetPoolIDWithIOs()
		log.FailOnError(err, "error identifying pool to run test")
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
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", poolIDToResize))
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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
			err = WaitForExpansionToStart(poolToBeResized.Uuid)
			log.FailOnError(err, "pool expansion not started")
			log.InfoD(fmt.Sprintf("Entering pool maintenence mode on node %s", stNode.Name))
			err = Inst().V.EnterPoolMaintenance(*stNode)
			log.FailOnError(err, fmt.Sprintf("fail to enter node %s in maintenence mode", stNode.Name))
			status, err := Inst().V.GetNodeStatus(*stNode)
			log.InfoD(fmt.Sprintf("Node %s status %s", stNode.Name, status.String()))

			resizeErr := waitForPoolToBeResized(expectedSize, poolToBeResized.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on node %s expansion using add-disk", poolToBeResized.Uuid, stNode.Name))

		})
		log.InfoD(fmt.Sprintf("Exiting pool maintenence mode on node %s", stNode.Name))
		t := func() (interface{}, bool, error) {
			if err := Inst().V.ExitPoolMaintenance(*stNode); err != nil {
				return nil, true, err
			}
			return nil, false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 1*time.Minute)
		err = Inst().V.WaitDriverUpOnNode(*stNode, 2*time.Minute)
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

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		stNode, err := GetRandomNodeWithPoolIOs(stNodes)
		log.FailOnError(err, "error identifying node to run test")
		selectedPool, err := GetPoolWithIOsInGivenNode(stNode)
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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
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
		StartTorpedoTest("PXRestartAddDisk", " restart PX and Initiate pool expansion using add-disk", nil, 0)

	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool by add-disk"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("rstadddsk-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}
		stNode, err := GetRandomNodeWithPoolIOs(stNodes)
		log.FailOnError(err, "error identifying node to run test")
		selectedPool, err := GetPoolWithIOsInGivenNode(stNode)
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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
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
			err = Inst().V.WaitDriverUpOnNode(*storageNode1, 3*time.Minute)
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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", poolToBeResized.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolToBeResized.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			err = Inst().V.StartDriver(*storageNode2)
			log.FailOnError(err, "error starting vol driver on node [%s]", storageNode2.Name)
			err = Inst().V.WaitDriverUpOnNode(*storageNode2, 3*time.Minute)
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
		step3: After I/O done p1 should be go offline and full, expand the pool p1 when p1 is rebalancing add a new drive with different size so that a new pool would be created
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
		selectedPool := getPoolWithLeastSize()
		selectedNode, err := GetNodeWithGivenPoolID(selectedPool.Uuid)
		log.FailOnError(err, fmt.Sprintf("Failed to get node with pool UUID %s", selectedPool.Uuid))
		log.Infof(fmt.Sprintf("Node %s with pool %s is marked for repl 1", selectedNode.Name, selectedPool.Uuid))
		stNodes := node.GetStorageNodes()
		var secondReplNode node.Node
		for _, stNode := range stNodes {
			if stNode.Name != selectedNode.Name {
				secondReplNode = stNode
			}
		}
		secondNodePools := secondReplNode.Pools
		secondNodePool := secondNodePools[0]
		log.Infof(fmt.Sprintf("Node %s with pool %s is marked for repl 2", secondReplNode.Name, secondNodePool.Uuid))
		drvSize, err := getPoolDiskSize(selectedPool)
		log.FailOnError(err, "error getting drive size for pool [%s]", selectedPool.Uuid)
		expectPoolSize := (selectedPool.TotalSize / units.GiB) + (2 * drvSize)

		isjournal, err := isJournalEnabled()
		log.FailOnError(err, "is journal enabled check failed")
		if secondNodePool.TotalSize/units.GiB < expectPoolSize-3 {
			log.InfoD("Current Size of the pool %s is %d", secondNodePool.Uuid, secondNodePool.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(secondNodePool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectPoolSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			err = waitForPoolToBeResized(expectPoolSize, secondNodePool.Uuid, isjournal)
			log.FailOnError(err, fmt.Sprintf("Error waiting for poor %s resize", secondNodePool.Uuid))
		}

		defer Inst().S.RemoveLabelOnNode(*selectedNode, k8s.NodeType)
		defer Inst().S.RemoveLabelOnNode(secondReplNode, k8s.NodeType)
		err = Inst().S.AddLabelOnNode(*selectedNode, k8s.NodeType, k8s.FastpathNodeType)
		log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", selectedNode.Name))
		err = Inst().S.AddLabelOnNode(secondReplNode, k8s.NodeType, k8s.FastpathNodeType)
		log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", secondReplNode.Name))

		Inst().AppList = append(Inst().AppList, "fio-fastpath")
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

		err = waitForStorageDown(*selectedNode)
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
		selectedPool, err = GetStoragePoolByUUID(offlinePoolUUID)
		log.FailOnError(err, "error getting pool with UUID [%s]", offlinePoolUUID)

		stepLog = fmt.Sprintf("expand pool %s using resize-disk", selectedPool.Uuid)
		var expandedExpectedPoolSize uint64
		Step("", func() {
			expandedExpectedPoolSize = (selectedPool.TotalSize / units.GiB) * 2

			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, selectedPool.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expandedExpectedPoolSize)
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

			log.InfoD("Validate pool rebalance after drive add")
			err = ValidatePoolRebalance()
			log.FailOnError(err, fmt.Sprintf("pool %s rebalance failed", selectedPool.Uuid))
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
			pools, err = Inst().V.ListStoragePools(metav1.LabelSelector{})
			log.FailOnError(err, "error getting storage pools")

			dash.VerifyFatal(len(pools), existingPoolsCount+1, "Validate new pool is created")
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

func adjustReplPools(firstNode, replNode node.Node, isjournal bool) error {

	selectedNodeSize := getTotalPoolSize(firstNode)
	secondReplSize := getTotalPoolSize(replNode)
	if secondReplSize <= selectedNodeSize {
		secondPool := replNode.StoragePools[0]
		maxSize := secondPool.TotalSize / units.GiB
		for _, p := range replNode.StoragePools {
			currSize := p.TotalSize / units.GiB
			if currSize > maxSize {
				maxSize = currSize
				secondPool = p
			}
		}

		expandSize := maxSize * 2
		log.InfoD("Current Size of the pool %s is %d", secondPool.Uuid, secondPool.TotalSize/units.GiB)
		if err := Inst().V.ExpandPool(secondPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expandSize); err != nil {
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
	//step2: After I/O done p1 should be go offline and full, expand the pool p1 using resize-disk
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
		selectedPool := getPoolWithLeastSize()
		selectedNode, err := GetNodeWithGivenPoolID(selectedPool.Uuid)
		log.FailOnError(err, fmt.Sprintf("Failed to get node with pool UUID %s", selectedPool.Uuid))

		stNodes := node.GetStorageDriverNodes()
		var secondReplNode node.Node
		for _, stNode := range stNodes {
			if stNode.Name != selectedNode.Name {
				secondReplNode = stNode
			}
		}

		defer Inst().S.RemoveLabelOnNode(*selectedNode, k8s.NodeType)
		defer Inst().S.RemoveLabelOnNode(secondReplNode, k8s.NodeType)
		err = Inst().S.AddLabelOnNode(*selectedNode, k8s.NodeType, k8s.FastpathNodeType)
		log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", selectedNode.Name))
		err = Inst().S.AddLabelOnNode(secondReplNode, k8s.NodeType, k8s.FastpathNodeType)
		log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", secondReplNode.Name))

		isjournal, err := isJournalEnabled()
		log.FailOnError(err, "is journal enabled check failed")

		err = adjustReplPools(*selectedNode, secondReplNode, isjournal)
		log.FailOnError(err, "Error setting pools for clean volumes")

		Inst().AppList = append(Inst().AppList, "fio-fastpath")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("sfullrz-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		err = waitForStorageDown(*selectedNode)
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
		selectedPool, err = GetStoragePoolByUUID(offlinePoolUUID)
		log.FailOnError(err, "error getting pool with UUID [%s]", offlinePoolUUID)

		var expandedExpectedPoolSize uint64
		Step(stepLog, func() {
			log.InfoD(stepLog)
			expandedExpectedPoolSize = (selectedPool.TotalSize / units.GiB) + 500

			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, selectedPool.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expandedExpectedPoolSize)
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
	//step2: After I/O done p1 should be go offline and full, expand the pool p1 using add-disk
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
		log.InfoD(stepLog)
		selectedPool := getPoolWithLeastSize()
		selectedNode, err := GetNodeWithGivenPoolID(selectedPool.Uuid)
		log.FailOnError(err, fmt.Sprintf("Failed to get node with pool UUID %s", selectedPool.Uuid))
		stNodes := node.GetStorageDriverNodes()
		var secondReplNode node.Node
		for _, stNode := range stNodes {
			if stNode.Name != selectedNode.Name {
				secondReplNode = stNode
			}
		}

		defer Inst().S.RemoveLabelOnNode(*selectedNode, k8s.NodeType)
		defer Inst().S.RemoveLabelOnNode(secondReplNode, k8s.NodeType)
		err = Inst().S.AddLabelOnNode(*selectedNode, k8s.NodeType, k8s.FastpathNodeType)
		log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", selectedNode.Name))
		err = Inst().S.AddLabelOnNode(secondReplNode, k8s.NodeType, k8s.FastpathNodeType)
		log.FailOnError(err, fmt.Sprintf("Failed add label on node %s", secondReplNode.Name))

		isjournal, err := isJournalEnabled()
		log.FailOnError(err, "is journal enabled check failed")

		err = adjustReplPools(*selectedNode, secondReplNode, isjournal)
		log.FailOnError(err, "Error setting pools for clean volumes")

		Inst().AppList = append(Inst().AppList, "fio-fastpath")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("sfullad-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		err = waitForStorageDown(*selectedNode)
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
		selectedPool, err = GetStoragePoolByUUID(offlinePoolUUID)
		log.FailOnError(err, "error getting pool with UUID [%s]", offlinePoolUUID)

		stepLog = fmt.Sprintf("expand pool %s using add-disk", selectedPool.Uuid)
		var expandedExpectedPoolSize uint64
		Step(stepLog, func() {
			log.InfoD(stepLog)
			expandedExpectedPoolSize = (selectedPool.TotalSize / units.GiB) + 500

			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, selectedPool.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expandedExpectedPoolSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")
		})
		stepLog = fmt.Sprintf("Ensure that pool %s expansion is successful", selectedPool.Uuid)
		Step(stepLog, func() {
			log.InfoD(stepLog)

			err = waitForPoolToBeResized(expandedExpectedPoolSize, selectedPool.Uuid, isjournal)
			log.FailOnError(err, "Error waiting for poor resize")
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

		if *status == api.Status_STATUS_STORAGE_DOWN {
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("node %s status is not down yet", n.Name)
	}
	if _, err := task.DoRetryWithTimeout(t, poolResizeTimeout, retryTimeout); err != nil {
		return err
	}

	return nil
}

var _ = Describe("{ResizeClusterNoQuorum}", func() {
	//1) Deploy px with cloud drive.
	//2) Make Cluster out of quorum
	//3) Expand a healthy pools by resize-disk

	var testrailID = 51300
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/51300
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("ResizeClusterNoQuorum", "Initiate pool expansion by resize-disk when cluster is out quorum ", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	stepLog := "should make cluster out of quorum, and expand healthy pool using resize-disk"

	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("resiznoqr-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		stoageDriverNodes := node.GetStorageDriverNodes()

		nonKvdbNodes := make([]node.Node, 0)
		kvdbNodes := make([]node.Node, 0)
		driverDownNodes := make([]node.Node, 0)

		kvdbNodesIDs := make([]string, 0)
		kvdbMembers, err := Inst().V.GetKvdbMembers(stoageDriverNodes[0])
		log.FailOnError(err, "Error getting KVDB members")

		for k := range kvdbMembers {
			kvdbNodesIDs = append(kvdbNodesIDs, k)
		}
		for _, n := range stoageDriverNodes {
			if Contains(kvdbNodesIDs, n.Id) {
				kvdbNodes = append(kvdbNodes, n)
			} else {
				nonKvdbNodes = append(nonKvdbNodes, n)
			}
		}
		numNodesToBeDown := (len(stoageDriverNodes) / 2) + 1
		if len(nonKvdbNodes) < numNodesToBeDown {
			numNodesToBeDown = len(nonKvdbNodes)
		}

		stepLog = "Make cluster out of quorum"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			i := 0
			for _, n := range nonKvdbNodes {
				if i == numNodesToBeDown {
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

		stepLog = fmt.Sprintf("Expanding pool on kvdb node using resize-disk")
		Step(stepLog, func() {

			selPool := kvdbNodes[0].Pools[0]
			poolToBeResized, err := GetStoragePoolByUUID(selPool.Uuid)
			log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selPool.Uuid))
			expectedSize := poolToBeResized.TotalSize * 2 / units.GiB

			log.InfoD("Current Size of the pool %s is %d", selPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			time.Sleep(1 * time.Minute)

			Step("set cluster to running", func() {
				log.InfoD("set cluster to running")
				for _, n := range driverDownNodes {
					err := Inst().V.StartDriver(n)
					log.FailOnError(err, "error starting driver on node %s", n.Name)
					err = Inst().V.WaitDriverUpOnNode(n, 5*time.Minute)
					log.FailOnError(err, "error while waiting for driver up on node %s", n.Name)
				}
			})

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			resizeErr := waitForPoolToBeResized(expectedSize, selPool.Uuid, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool %s on expansion using resize-disk", selPool.Uuid))
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
		selectedPool, err := GetPoolWithIOsInGivenNode(selectedNode)
		log.FailOnError(err, "error while selecting the pool [%s]", selectedPool)

		stepLog := fmt.Sprintf("Expanding pool on node [%s] and pool UUID: [%s] using auto", selectedNode.Name, selectedPool.Uuid)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, "Failed to get pool using UUID [%s]", selectedPool.Uuid)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool %s is %d", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize)
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
		Try pool resize when lot of snapshots are created on the volume
	*/
	var testrailID = 50652
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/50652
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("CreateSnapshotsPoolResize", "Validate storage pool expansion when lots of snapshots present on the system", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context
	var totalSnapshotsPerVol int = 60

	snapshotList := make(map[string][]string)
	var selectedNode node.Node
	var pickNode string

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

		// Get List of Volumes presnet in the Node

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "No Storage Node exists !!")
		}
		log.InfoD("List of Nodes present [%s]", stNodes)

		for _, each := range contexts {
			log.InfoD("Getting context Info [%v]", each)
			Volumes, err := Inst().S.GetVolumes(each)
			log.FailOnError(err, "Listing Volumes Failed")

			log.InfoD("Get all the details of Volumes Present")
			for _, vol := range Volumes {
				log.InfoD("List of Volumes to inspect [%T] , [%s]", vol, vol.ID)
				volInspect, err := Inst().V.InspectVolume(vol.ID)
				log.FailOnError(err, "Failed to Inpect volumes present Err : [%s]", volInspect)
				selectedNode := volInspect.ReplicaSets[0].Nodes
				randomIndex := rand.Intn(len(selectedNode))
				pickNode = selectedNode[randomIndex]

				for _, n := range stNodes {
					if n.Id == pickNode {
						stNode = n
					}
				}
				for snap := 0; snap < totalSnapshotsPerVol; snap++ {
					uuidCreated := uuid.New()
					snapshotName := fmt.Sprintf("snapshot_%s_%s", vol.ID, uuidCreated.String())
					snapshotResponse, err := Inst().V.CreateSnapshot(vol.ID, snapshotName)
					log.FailOnError(err, "error identifying volume [%s]", vol.ID)
					snapshotList[vol.ID] = append(snapshotList[vol.ID], snapshotName)
					log.InfoD("Snapshot [%s] created with ID [%s]", snapshotName, snapshotResponse.GetSnapshotId())
				}
				break

			}
		}

		// Selecting Storage pool based on Pools present on the Node
		selectedPool, err := GetPoolWithIOsInGivenNode(stNode)
		log.FailOnError(err, "error identifying pool running IO [%s]", stNode.Name)

		stepLog = fmt.Sprintf("Expanding pool on node [%s] and pool UUID: [%s] using auto", selectedNode.Name, selectedPool.Uuid)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolToBeResized, err := GetStoragePoolByUUID(selectedPool.Uuid)
			log.FailOnError(err, "Failed to get pool using UUID [%s]", selectedPool.Uuid)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool [%s] is [%d]", selectedPool.Uuid, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(selectedPool.Uuid, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize)
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

func unique(arrayEle []string) []string {
	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range arrayEle {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}

func inResync(vol string) bool {
	volDetails, err := Inst().V.InspectVolume(vol)
	if err != nil {
		log.Error("not in Resync State")
		return false
	}
	for _, v := range volDetails.RuntimeState {
		log.InfoD("RuntimeState is in state %s", v.GetRuntimeState()["RuntimeState"])
		if v.GetRuntimeState()["RuntimeState"] != "resync" {
			return false
		}
	}
	return true
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
		Try pool resize when lot of volumes are in resync state
	*/
	var testrailID = 51301
	// Testrail Corresponds : https://portworx.testrail.net/index.php?/cases/view/51301
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("PoolResizeVolumesResync", "Validate Pool resize when lots of volumes are in resync state", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context
	var vol_ids []string

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
				vol_ids = append(vol_ids, vol.ID)
			}

			// Select Random Volumes for pool Expand
			randomIndex := rand.Intn(len(vol_ids))
			randomVolIDs := vol_ids[randomIndex]

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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			poolToBeResized, err := GetStoragePoolByUUID(rebootPoolID)
			log.InfoD("Pool to be resized %v", poolToBeResized)
			log.FailOnError(err, "Failed to get pool using UUID [%s]", rebootPoolID)
			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			log.InfoD("Restarting the Driver on Node [%s]", restartDriver.Name)
			err = Inst().N.RebootNode(*restartDriver, node.RebootNodeOpts{
				Force: true,
				ConnectionOpts: node.ConnectionOpts{
					Timeout:         1 * time.Minute,
					TimeBeforeRetry: 5 * time.Second,
				},
			})
			log.FailOnError(err, "Rebooting Node failed?")

			log.InfoD("Waiting till Volume is In Resync Mode ")
			if WaitTillVolumeInResync(randomVolIDs) == false {
				log.InfoD("Failed to get Volume in Resync state [%s]", randomVolIDs)
			}

			log.InfoD("Current Size of the pool %s is %d", rebootPoolID, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(rebootPoolID, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize)
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

	JustBeforeEach(func() {
		StartTorpedoTest("PoolIncreaseSize20TB", "Resize a pool of capacity of 100GB to 20TB", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context
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
		poolIDToResize, err := GetPoolIDWithIOs()
		log.FailOnError(err, "error identifying pool to run test")
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize [%s]", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is not in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, "Failed to get pool using UUID [%s]", poolIDToResize)
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		// Marking the expected size to be 2TB
		expectedSize = (2048 * 1024 * 1024 * 1024 * 1024) / units.TiB

		stepLog = "Calculate expected pool size and trigger pool resize"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check is Journal enabled")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}
			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
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
	return true
}

var _ = Describe("{ResizePoolDrivesInDifferentSize}", func() {
	/*
		Resizing the pool should fail when drives in the pool have been resized to different size
	*/
	var testrailID = 51320
	// Testrail Corresponds : https://portworx.testrail.net/index.php?/cases/view/51320
	var runID int

	JustBeforeEach(func() {
		StartTorpedoTest("ResizePoolDrivesInDifferentSize",
			"Resizing the pool should fail when drives in the pool have been resized to different size",
			nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context
	stepLog := "should get the existing storage node and expand the pool by resize-disk"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("resizepooldrivesdiffsize-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Select a Pool with IO Runing poolID returns UUID ( String )
		var poolID int32

		poolUUID, err := GetPoolIDWithIOs()
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)
		log.FailOnError(err, "Failed to get pool using UUID [%v]", poolID)

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

		// Add disk to the Node
		var diskSize uint64
		minDiskSize := 50
		maxDiskSize := 150
		size := rand.Intn(maxDiskSize-minDiskSize) + minDiskSize
		diskSize = (uint64(size) * 1024 * 1024 * 1024) / units.GiB

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
		1) Place pool on maintenence mode
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
		for _, stNode := range stNodes {
			if len(stNode.StoragePools) > 1 {
				nodePools = stNode.StoragePools
				nodeSelected = stNode
				break
			}
		}

		dash.VerifyFatal(len(nodePools) > 1, true, "Node has multiple storage pools?")
		var poolToDelete node.StoragePool
		for _, pl := range nodePools {
			if pl.ID != 0 {
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

		stepLog = fmt.Sprintf("Delete poolID %s on node %s", poolIDToDelete, nodeSelected.Name)

		Step(stepLog, func() {
			log.InfoD(stepLog)
			log.InfoD("Setting pools in maintenance on node %s", nodeSelected.Name)
			err = Inst().V.EnterPoolMaintenance(nodeSelected)
			log.FailOnError(err, "failed to set pool maintenance mode on node %s", nodeSelected.Name)

			time.Sleep(1 * time.Minute)
			expectedStatus := "In Maintenance"
			err = waitForPoolStatusToUpdate(nodeSelected, expectedStatus)
			log.FailOnError(err, fmt.Sprintf("node %s pools are not in status %s", nodeSelected.Name, expectedStatus))

			err = Inst().V.DeletePool(nodeSelected, poolIDToDelete)
			log.FailOnError(err, "failed to delete poolID %s on node %s", poolIDToDelete, nodeSelected.Name)

			err = Inst().V.ExitPoolMaintenance(nodeSelected)
			log.FailOnError(err, "failed to exit pool maintenance mode on node %s", nodeSelected.Name)

			err = Inst().V.WaitDriverUpOnNode(nodeSelected, 5*time.Minute)
			log.FailOnError(err, "volume driver down on node %s", nodeSelected.Name)

			expectedStatus = "Online"
			err = waitForPoolStatusToUpdate(nodeSelected, expectedStatus)
			log.FailOnError(err, fmt.Sprintf("node %s pools are not in status %s", nodeSelected.Name, expectedStatus))

			poolsAfr, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
			log.FailOnError(err, "Failed to list storage pools")

			dash.VerifySafely(len(poolsBfr) > len(poolsAfr), true, "verify pools count is updated after pools deletion")

			poolsMap, err = Inst().V.GetPoolDrives(&nodeSelected)
			log.FailOnError(err, "error getting pool drive from the node [%s]", nodeSelected.Name)
			_, ok := poolsMap[poolIDToDelete]
			dash.VerifyFatal(ok, false, "verify drive is deleted from the node")

		})

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
			log.InfoD("Validate pool rebalance after drive add to the node %s", nodeSelected.Name)
			err = ValidatePoolRebalance()
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
			err = Inst().V.ExpandPool(poolIDSelected, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

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

func waitForPoolStatusToUpdate(nodeSelected node.Node, expectedStatus string) error {
	t := func() (interface{}, bool, error) {
		poolsStatus, err := Inst().V.GetNodePoolsStatus(nodeSelected)
		if err != nil {
			return nil, true, fmt.Errorf("error getting pool status on node %s,err: %v", nodeSelected.Name, err)
		}

		if poolsStatus == nil {
			return nil, false, fmt.Errorf("pools status is nil")
		}

		for k, v := range poolsStatus {
			if v != expectedStatus {
				return nil, true, fmt.Errorf("pool %s is not %s, current status : %s", k, expectedStatus, v)
			}
		}

		return nil, false, nil
	}
	_, err := task.DoRetryWithTimeout(t, 10*time.Minute, 1*time.Minute)
	return err
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

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("voldeletepoolexpand-%d", i))...)
		}

		ValidateApplications(contexts)

		log.Infof("Need to check if volume is close to 200G occupied")
		vol, err := getVolumeWithMinimumSize(contexts, 90)

		// We will change the size, after modifying/deploying a vdbench/fio to write ~200G. Current vdbench is writing 98G
		dash.VerifyFatal(err, nil, "Checking if the desired volume is obtained")
		volID := vol.ID
		volName := vol.Name

		log.Infof("The volume that is having size used around 190 G is %s with name %s", volID, volName)

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
			if poolResizeIsInProgress(poolToBeResized) {
				// Wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, "Failed to get pool using UUID %v", poolToBeResized.Uuid)
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

		// Let the expand complete
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
			isjournal, err := isJournalEnabled()
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
			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
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

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}

		stNode, err := GetRandomNodeWithPoolIOs(stNodes)
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
		stNodes = node.GetStorageNodes()
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
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(selectedNodePool.Uuid)
				log.FailOnError(err, fmt.Sprintf("Failed to get pool using UUID %s", selectedNodePool.Uuid))
			}
		})

		var expectedSize uint64

		stepLog = "trigger pool resize with the same size"
		Step(stepLog, func() {
			expectedSize = (poolToBeResized.TotalSize / units.GiB) + 2

			log.InfoD("Current Size of the pool %s is %d", selectedNodePool.Uuid, poolToBeResized.TotalSize/units.GiB)

			err = Inst().V.ExpandPool(selectedNodePool.Uuid, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, selectedNodePool.Uuid, true)
			dash.VerifyFatal(resizeErr != nil, true, fmt.Sprintf("verify pool expansion using resize-disk with same size failed on pool [%s] in node [%s]", selectedNodePool.Uuid, stNode.Name))
			expandedPool, err := GetStoragePoolByUUID(selectedNodePool.Uuid)
			log.FailOnError(err, "error getting storage pool")
			if expandedPool.LastOperation != nil {
				log.Infof("pool last operation status: %v", expandedPool.LastOperation.Status)
				log.Infof("pool last operation msg: %s", expandedPool.LastOperation.Msg)
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

func addNewPools(n node.Node, numPools int) error {

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

		log.InfoD("Validate pool rebalance after drive add on node %s", n.Name)
		if err = ValidatePoolRebalance(); err != nil {
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
				i := strings.Index(drvSize, "G")
				drvSize = drvSize[:i]
				break outer
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

	JustBeforeEach(func() {
		StartTorpedoTest("ChangedIOPriorityPersistPoolExpand",
			"Changed pool IO_priority should persist post pool expand",
			nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context
	stepLog := "Changed pool IO_priority should persist post pool expand"
	It(stepLog, func() {
		log.InfoD(stepLog)

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("changedioprioritypoolexpand-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get the Pool UUID on which IO is running
		poolUUID, err := GetPoolIDWithIOs()
		log.FailOnError(err, "Failed to get pool using UUID [%v]", poolUUID)
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
			if eachIOPriority != ioPriorityBefore {
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

			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check if Journal enabled")

			log.InfoD("Current Size of the pool [%s] is [%d]", poolUUID, poolToBeResized.TotalSize/units.GiB)
			err = Inst().V.ExpandPool(poolUUID, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, poolUUID, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Verify pool [%s] on node [%s] expansion using auto", poolUUID, nodeDetail.Name))
		})

		// Validate if PoolIO Priority is not changed after pool Expansion
		ioPriorityAfter, err := Inst().V.GetPoolLabelValue(poolUUID, "iopriority")
		log.FailOnError(err, "Failed to get IO Priority for Pool with UUID [%v]", poolUUID)

		log.InfoD(fmt.Sprintf("Priority Before [%s] and Priority after Pool Expansion [%s]", ioPriorityBefore, ioPriorityAfter))
		dash.VerifyFatal(ioPriorityAfter == setIOPriority, true, "IO Priority mismatch after pool expansion")

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

	var contexts []*scheduler.Context
	stepLog := "Verify deletion of invalid pool ids"
	It(stepLog, func() {
		log.InfoD(stepLog)

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("deleteinvalidpoolid-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get the Pool UUID on which IO is running
		poolUUID, err := GetPoolIDWithIOs()
		log.FailOnError(err, "Failed to get pool using UUID [%v]", poolUUID)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)

		PoolDetail, err := GetPoolsDetailsOnNode(*nodeDetail)
		log.FailOnError(err, "Fetching all pool details from the node [%v] failed ", nodeDetail.Name)

		// Delete Pool without entering Maintenance Mode [ PTX-15157 ]
		err = Inst().V.DeletePool(*nodeDetail, "0")
		dash.VerifyFatal(err == nil, false, fmt.Sprintf("Expected Failure as pool not in maintenance mode : Node Detail [%v]", nodeDetail.Name))

		compileText := "service mode delete pool.*unable to delete pool with ID.*[0-9]+.*cause.*operation is not supported"
		re := regexp.MustCompile(compileText)
		if re.MatchString(fmt.Sprintf("%v", err)) == false {
			err = fmt.Errorf("Failed to verify failure string on invalid Pool UUID")
		}

		// invalidPoolID is total Pools present on the node + 1
		invalidPoolID := fmt.Sprintf("%d", len(PoolDetail)+1)

		log.InfoD("Bring Node to Maintenance Mode")
		log.FailOnError(Inst().V.EnterMaintenance(*nodeDetail), fmt.Sprintf("Failed to bring Pool [%s] to Mainteinance Mode on Node [%s]", poolUUID, nodeDetail.Name))

		// Wait for some time before verifying Maintenance state
		time.Sleep(2 * time.Minute)

		log.InfoD("Wait for Node to Enter Maintenance Mode")
		log.FailOnError(WaitTillEnterMaintenanceMode(*nodeDetail), fmt.Sprintf("Failed while waiting for pool to enter maintenance mode on node [%v]", nodeDetail.Name))

		// Delete the Pool with Invalid Pool ID
		err = Inst().V.DeletePool(*nodeDetail, invalidPoolID)
		dash.VerifyFatal(err != nil, true, fmt.Sprintf("Expected Failure? : Node Detail [%v]", nodeDetail.Name))
		log.InfoD("Deleting Pool with InvalidID Errored as expected [%v]", err)

		log.InfoD("Bring Node out of Maintenance Mode")
		log.FailOnError(ExitFromMaintenanceMode(*nodeDetail), fmt.Sprintf("Failed to bring up node [%v] back from maintenance mode", nodeDetail.Name))

		// Verify Alerts generated after Pool Expansion [PWX-28484]
		var severityType = []api.SeverityType{api.SeverityType_SEVERITY_TYPE_ALARM, api.SeverityType_SEVERITY_TYPE_NOTIFY, api.SeverityType_SEVERITY_TYPE_WARNING}
		for _, eachAlert := range severityType {
			alerts, err := Inst().V.GetAlertsUsingResourceTypeBySeverity(api.ResourceType_RESOURCE_TYPE_POOL, eachAlert)
			log.FailOnError(err, "Failed to fetch alerts using severity type [%v] of resource Type [%v]", eachAlert, api.ResourceType_RESOURCE_TYPE_POOL)

			dash.VerifyFatal(len(alerts.Alerts) > 0, true, fmt.Sprintf("did alert generated for resource type [%v] and severity [%v]?", api.ResourceType_RESOURCE_TYPE_POOL, eachAlert))
		}

		JustAfterEach(func() {
			defer EndTorpedoTest()
			log.InfoD("Exit from Maintenance mode if Pool is still in Maintenance")
			log.FailOnError(ExitNodesFromMaintenanceMode(), "exit from maintenance mode failed?")
			AfterEachTest(contexts, testrailID, runID)
		})
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

	var contexts []*scheduler.Context
	stepLog := "Resize with invalid pool ID"
	It(stepLog, func() {
		log.InfoD(stepLog)

		startTime := time.Now()

		// Get alerts from ten hours before current time till current time to get the sufficient alerts
		startMinusTenHours := startTime.Add(time.Duration(-600) * time.Minute)
		endTime := time.Now()
		alertsBefore, err := Inst().V.GetAlertsUsingResourceTypeByTime(api.ResourceType_RESOURCE_TYPE_POOL, startMinusTenHours, endTime)
		log.FailOnError(err, "Failed to fetch alerts between startTime [%v] and endTime [%v]", startMinusTenHours, endTime)

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("invalidpoolid-%d", i))...)
		}
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Get the Pool UUID on which IO is running
		poolUUID, err := GetPoolIDWithIOs()
		log.FailOnError(err, "Failed to get pool using UUID [%v]", poolUUID)
		log.InfoD("Pool UUID on which IO is running [%s]", poolUUID)

		nodeDetail, err := GetNodeWithGivenPoolID(poolUUID)
		log.FailOnError(err, "Failed to get Node Details from PoolUUID [%v]", poolUUID)

		// invalidPoolUUID Generation
		id := uuid.New()
		invalidPoolUUID := id.String()

		// Resize Pool with Invalid Pool ID
		// Do Pool Expand on the Node
		stepLog = fmt.Sprintf("Expanding pool on node [%s] and pool UUID: [%s] using auto", nodeDetail.Name, poolUUID)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			poolToBeResized, err := GetStoragePoolByUUID(poolUUID)
			log.FailOnError(err, "Failed to get pool using UUID [%s]", poolUUID)

			drvSize, err := getPoolDiskSize(poolToBeResized)
			log.FailOnError(err, "error getting drive size for pool [%s]", poolToBeResized.Uuid)
			expectedSize := (poolToBeResized.TotalSize / units.GiB) + drvSize

			log.InfoD("Current Size of the pool [%s] is [%d]", poolUUID, poolToBeResized.TotalSize/units.GiB)

			// Now trying to Expand Pool with Invalid Pool UUID
			err = Inst().V.ExpandPoolUsingPxctlCmd(*nodeDetail, invalidPoolUUID, api.SdkStoragePool_RESIZE_TYPE_AUTO, expectedSize)

			// Verify error on pool expansion failure
			var errMatch error
			errMatch = nil
			re := regexp.MustCompile(fmt.Sprintf(".*failed to find storage pool with UID.*%s.*", invalidPoolUUID))
			if re.MatchString(fmt.Sprintf("%v", err)) == false {
				errMatch = fmt.Errorf("Failed to verify failure using invalid PoolUUID [%v]", invalidPoolUUID)
			}
			dash.VerifyFatal(errMatch, nil, "Pool expand with invalid PoolUUID completed?")

			endTime = time.Now()

			// Get alerts from the cluster between startTime till endTime [ PWX-28484 ]
			log.InfoD("Getting alerts generated by Pool between startTime : [%v] and endTime : [%v]", startMinusTenHours, endTime)
			alerts, err := Inst().V.GetAlertsUsingResourceTypeByTime(api.ResourceType_RESOURCE_TYPE_POOL, startMinusTenHours, endTime)
			log.FailOnError(err, "Failed to fetch alerts between startTime [%v] and endTime [%v]", startMinusTenHours, endTime)
			dash.VerifyFatal(len(alertsBefore.Alerts) < len(alerts.Alerts), true, fmt.Sprintf("did alert generated for resource type [%v] with time specified?", api.ResourceType_RESOURCE_TYPE_POOL))

		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Exit from Maintenance mode if Pool is still in Maintenance")
		log.FailOnError(ExitNodesFromMaintenanceMode(), "exit from maintenance mode failed?")
		AfterEachTest(contexts, testrailID, runID)
	})
})
