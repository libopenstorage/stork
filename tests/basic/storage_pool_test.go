package tests

import (
	"fmt"
	"github.com/portworx/torpedo/drivers/node"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/libopenstorage/openstorage/api"
	. "github.com/onsi/ginkgo"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/units"
	. "github.com/portworx/torpedo/tests"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const poolResizeTimeout = time.Minute * 10
const retryTimeout = time.Minute

var _ = Describe("{StoragePoolExpandDiskResize}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("StoragePoolExpandDiskResize", "Validate storage pool expansion using resize-disk option", nil, 0)
	})

	var contexts []*scheduler.Context
	stepLog := "has to schedule apps, and expand it by resizing a disk"
	It(stepLog, func() {
		dash.Info(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("poolexpand-%d", i))...)
		}

		ValidateApplications(contexts)

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		dash.VerifyFatal(err, nil, "Validate list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, "Validate storage pools exist")

		// pick a random pool from a pools list and resize it
		poolIDToResize = getRandomPoolID(pools)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Validate pool to be resized exist")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		time.Sleep(time.Second * 60)
		stepLog = "Verify that pool resize is non in progress"
		Step(stepLog, func() {
			dash.Info(stepLog)
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				dash.VerifyFatal(err, nil, "Validate get pool using UUID ")
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64
		stepLog = "Calculate expected pool size and trigger pool resize"
		Step(stepLog, func() {
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB

			isjournal, err := isJournalEnabled()
			dash.VerifyFatal(err, nil, "Validate is journal enabled check")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}

			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Validate pool expansion init")

			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("expected new size to be '%d' or '%d', and error %s not not have occured", expectedSize, expectedSizeWithJournal, resizeErr))
		})

		stepLog = "Ensure that new pool has been expanded to the expected size"
		Step(stepLog, func() {
			dash.Info(stepLog)
			ValidateApplications(contexts)

			resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
			dash.VerifyFatal(err, nil, "Validate get pool using UUID ")
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			if newPoolSize == expectedSize || newPoolSize == expectedSizeWithJournal {
				isExpansionSuccess = true
			}
			dash.VerifyFatal(isExpansionSuccess, true, fmt.Sprintf("expected new pool size to be %v or %v, got %v", expectedSize, expectedSizeWithJournal, newPoolSize))

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
		dash.Info(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pooladddisk-%d", i))...)
		}

		ValidateApplications(contexts)

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		dash.VerifyFatal(err, nil, "Validate list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, "Validate storage pools exist")

		// pick a random pool from a pools list and resize it
		poolIDToResize = getRandomPoolID(pools)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Validate pool to be resized exist")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is none in progress"
		Step(stepLog, func() {
			dash.Info(stepLog)
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				dash.VerifyFatal(err, nil, "Validate get pool using UUID ")
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		stepLog = "Calculate expected pool size and trigger pool resize"
		Step(stepLog, func() {
			dash.Info(stepLog)
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB
			expectedSize = roundUpValue(expectedSize)
			isjournal, err := isJournalEnabled()
			dash.VerifyFatal(err, nil, "Validate is journal enabled check")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}

			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Validate pool expansion init")

			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("expected new size to be '%d' or '%d' if pool has journal, and error %s not not have occured", expectedSize, expectedSizeWithJournal, resizeErr))
		})

		Step("Ensure that new pool has been expanded to the expected size", func() {
			ValidateApplications(contexts)

			resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
			dash.VerifyFatal(err, nil, "Validate get pool using UUID ")
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			if newPoolSize == expectedSize || newPoolSize == expectedSizeWithJournal {
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

var _ = Describe("{PoolResizeDiskReboot}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PoolResizeDiskReboot", "Initiate pool expansion using resize-disk and reboot node", nil, 0)
	})

	var contexts []*scheduler.Context

	stepLog := "has to schedule apps, and expand it by resizing a disk"
	It(stepLog, func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("poolresizediskreboot-%d", i))...)
		}

		ValidateApplications(contexts)

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		dash.VerifyFatal(err, nil, "Validate list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, "Validate storage pools exist")

		// pick a random pool from a pools list and resize it
		poolIDToResize = getRandomPoolID(pools)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Validate pool to be resized exist")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		time.Sleep(time.Second * 60)
		stepLog = "Verify that pool resize is none in progress"
		Step(stepLog, func() {
			dash.Info(stepLog)
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				dash.VerifyFatal(err, nil, "Validate get pool using UUID ")
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		stepLog = "Calculate expected pool size and trigger pool resize"
		Step(stepLog, func() {
			dash.Info(stepLog)
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB

			isjournal, err := isJournalEnabled()
			dash.VerifyFatal(err, nil, "Validate is journal enabled check")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}

			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Validate pool expansion init")

			err = WaitForExpansionToStart(poolIDToResize)
			dash.VerifyFatal(err, nil, "Validate expansion is started")

			storageNode, err := GetNodeWithGivenPoolID(poolIDToResize)
			dash.VerifyFatal(err, nil, "Validate get pool using UUID ")
			err = RebootNodeAndWait(*storageNode)
			dash.VerifyFatal(err, nil, "Validate reboot node and wait till it is up")
			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("expected new size to be '%d' or '%d', and error %s not not have occured", expectedSize, expectedSizeWithJournal, resizeErr))
		})

		stepLog = "Ensure that new pool has been expanded to the expected size"
		Step(stepLog, func() {
			dash.Info(stepLog)
			ValidateApplications(contexts)

			resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
			dash.VerifyFatal(err, nil, "Validate get pool using UUID ")
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			if newPoolSize == expectedSize || newPoolSize == expectedSizeWithJournal {
				isExpansionSuccess = true
			}
			dash.VerifyFatal(isExpansionSuccess, true,
				fmt.Sprintf("expected new pool size to be %v or %v, got %v", expectedSize, expectedSizeWithJournal, newPoolSize))
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{PoolAddDiskReboot}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PoolAddDiskReboot", "Initiate pool expansion using add-disk and reboot node", nil, 0)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing pool and expand it by adding a disk"

	It(stepLog, func() {
		dash.Info(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pooladddiskreboot-%d", i))...)
		}

		ValidateApplications(contexts)

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		dash.VerifyFatal(err, nil, "Validate list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, "Validate storage pools exist")

		// pick a random pool from a pools list and resize it
		poolIDToResize = getRandomPoolID(pools)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Validate pool to be resized exist")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is non in progress"
		Step(stepLog, func() {
			dash.Info(stepLog)
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				dash.VerifyFatal(err, nil, "Validate get pool using UUID")
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		stepLog = "Calculate expected pool size and trigger pool resize"
		Step(stepLog, func() {
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB
			expectedSize = roundUpValue(expectedSize)
			isjournal, err := isJournalEnabled()
			dash.VerifyFatal(err, nil, "Validate is journal enabled check")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}

			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Validate pool expansion init")

			err = WaitForExpansionToStart(poolIDToResize)
			dash.VerifyFatal(err, nil, "Validate expansion is started")

			storageNode, err := GetNodeWithGivenPoolID(poolIDToResize)
			dash.VerifyFatal(err, nil, "Validate get pool using UUID ")
			err = RebootNodeAndWait(*storageNode)
			dash.VerifyFatal(err, nil, "Validate reboot node and wait till it is up")
			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("expected new size to be '%d' or '%d' if pool has journal, and error %s not not have occured", expectedSize, expectedSizeWithJournal, resizeErr))
		})

		stepLog = "Ensure that new pool has been expanded to the expected size"
		Step(stepLog, func() {
			ValidateApplications(contexts)

			resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
			dash.VerifyFatal(err, nil, "Validate get pool using UUID ")
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			if newPoolSize == expectedSize || newPoolSize == expectedSizeWithJournal {
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

var _ = Describe("{NodePoolsResizeDisk}", func() {

	nodePoolsExpansion("NodePoolsResizeDisk")

})

var _ = Describe("{NodePoolsAddDisk}", func() {

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

	var contexts []*scheduler.Context
	stepLog := fmt.Sprintf("has to schedule apps, and expand it by %s", option)
	It(stepLog, func() {
		dash.Info(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("nodepools-%s-%d", option, i))...)
		}

		ValidateApplications(contexts)

		var poolsToBeResized []*api.StoragePool

		stNodes := node.GetStorageNodes()
		var nodePoolToExpanded node.Node
		var nodePools []node.StoragePool
		for _, stNode := range stNodes {
			nodePools = stNode.StoragePools
			nodePoolToExpanded = stNode
			if len(nodePools) > 1 {
				break
			}
		}
		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		dash.VerifyFatal(err, nil, "Validate list storage pools")
		dash.VerifyFatal(len(nodePools) > 1, true, "Validate node has multiple storage pools exist")

		for _, p := range nodePools {
			poolsToBeResized = append(poolsToBeResized, pools[p.Uuid])
		}

		dash.VerifyFatal(poolsToBeResized != nil, true, "Validate pools to be resized exist")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wait until the ongoing operation is completed
		stepLog = "Verify that pool resize is none in progress"
		Step(stepLog, func() {
			dash.Info(stepLog)
			for _, poolToBeResized := range poolsToBeResized {
				poolIDToResize := poolToBeResized.Uuid
				if poolResizeIsInProgress(poolToBeResized) {
					// wait until resize is completed and get the updated pool again
					poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get pool using UUID  %s", poolIDToResize))
				}
			}

		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64
		poolsExpectedSizeMap := make(map[string]uint64)
		isjournal, err := isJournalEnabled()
		dash.VerifyFatal(err, nil, "Validate is journal enabled check")
		stepLog = fmt.Sprintf("Calculate expected pool size and trigger pool resize for %s", nodePoolToExpanded.Name)
		Step(stepLog, func() {

			for _, poolToBeResized := range poolsToBeResized {
				expectedSize = poolToBeResized.TotalSize * 2 / units.GiB
				poolsExpectedSizeMap[poolToBeResized.Uuid] = expectedSize

				//To-Do Need to handle the case for multiple pools
				expectedSizeWithJournal = expectedSize
				if isjournal {
					expectedSizeWithJournal = expectedSizeWithJournal - 3
				}
				err = Inst().V.ExpandPool(poolToBeResized.Uuid, operation, expectedSize)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validate pool %s expansion init", poolToBeResized.Uuid))
			}

			for poolUUID, expectedSize := range poolsExpectedSizeMap {
				resizeErr := waitForPoolToBeResized(expectedSize, poolUUID, isjournal)
				expectedSizeWithJournal = expectedSize
				if isjournal {
					expectedSizeWithJournal = expectedSizeWithJournal - 3
				}
				dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("expected new size to be '%d' or '%d', and error %s not not have occured", expectedSize, expectedSizeWithJournal, resizeErr))
			}

		})

		stepLog = "Ensure that pools have been expanded to the expected size"
		Step(stepLog, func() {
			dash.Info(stepLog)
			ValidateApplications(contexts)
			for poolUUID, expectedSize := range poolsExpectedSizeMap {
				resizedPool, err := GetStoragePoolByUUID(poolUUID)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validate get pool using UUID  %s", poolUUID))
				newPoolSize := resizedPool.TotalSize / units.GiB
				isExpansionSuccess := false
				expectedSizeWithJournal = expectedSize
				if isjournal {
					expectedSizeWithJournal = expectedSizeWithJournal - 3
				}
				if newPoolSize == expectedSize || newPoolSize == expectedSizeWithJournal {
					isExpansionSuccess = true
				}
				dash.VerifyFatal(isExpansionSuccess, true, fmt.Sprintf("expected new pool size to be %v or %v, got %v", expectedSize, expectedSizeWithJournal, newPoolSize))
			}

		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
}

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
					dash.VerifyFatal(updatedPoolToBeResized.LastOperation.Status, api.SdkStoragePool_OPERATION_SUCCESSFUL, fmt.Sprintf("PoolResize has failed. Error: %s", updatedPoolToBeResized.LastOperation))
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

	f := func() (interface{}, bool, error) {
		expandedPool, err := GetStoragePoolByUUID(poolIDToResize)
		if err != nil {
			return nil, true, fmt.Errorf("error getting pool by using id %s", poolIDToResize)
		}

		if expandedPool == nil {
			return nil, false, fmt.Errorf("expanded pool value is nil")
		}
		if expandedPool.LastOperation != nil {
			logrus.Infof("Pool Resize Status : %v, Message : %s", expandedPool.LastOperation.Status, expandedPool.LastOperation.Msg)
			if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_FAILED {
				return nil, false, fmt.Errorf("PoolResize has failed. Error: %s", expandedPool.LastOperation)
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
			return nil, true, nil
		}
		return nil, true, fmt.Errorf("pool has not been resized to %d or %d yet. Waiting...Current size is %d", expectedSize, expectedSizeWithJournal, newPoolSize)
	}

	_, err := task.DoRetryWithTimeout(f, poolResizeTimeout, retryTimeout)
	return err
}

func isJournalEnabled() (bool, error) {
	storageSpec, err := Inst().V.GetStorageSpec()
	if err != nil {
		return false, err
	}
	jDev := storageSpec.GetJournalDev()
	if jDev != "" {
		logrus.Infof("JournalDev: %s", jDev)
		return true, nil
	}
	return false, nil
}

func getRandomPoolID(pools map[string]*api.StoragePool) string {
	// pick a random pool from a pools list and resize it
	randomIndex := rand.Intn(len(pools))
	for _, pool := range pools {
		if randomIndex == 0 {
			return pool.Uuid

		}
		randomIndex--
	}
	return ""
}

var _ = Describe("{PoolAddDrive}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PoolAddDrive", "Initiate pool expansion using add-drive", nil, 0)
	})
	var contexts []*scheduler.Context

	stepLog := "should get the existing storage node and expand the pool by adding a drive"

	It(stepLog, func() {
		dash.Info(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pooladddrive-%d", i))...)
		}
		ValidateApplications(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "No storage nodes found")
		}

		driveSpecs, err := GetCloudDriveDeviceSpecs()

		if err != nil {
			dash.VerifyFatal(err, nil, "Error getting cloud drive specs")
		}
		stNode := stNodes[0]
		nodePools := stNode.Pools
		var currentTotalPoolSize uint64
		for _, nodePool := range nodePools {
			currentTotalPoolSize += nodePool.TotalSize / units.GiB
		}
		deviceSpec := driveSpecs[0]
		deviceSpecParams := strings.Split(deviceSpec, ",")
		var specSize uint64
		for _, param := range deviceSpecParams {
			if strings.Contains(param, "size") {
				val := strings.Split(param, "=")[1]
				specSize, err = strconv.ParseUint(val, 10, 64)
				if err != nil {
					dash.VerifyFatal(err, nil, "Error converting size to uint64")
				}
			}
		}
		expectedTotalPoolSize := currentTotalPoolSize + specSize
		stepLog = "Initiate add cloud drive and validate"
		Step(stepLog, func() {
			err = Inst().V.AddCloudDrive(&stNode, deviceSpec)
			if err != nil {
				dash.VerifyFatal(err, nil, fmt.Sprintf("Add cloud drive failed on node %s", stNode.Name))
			} else {
				dash.Info("Validate pool rebalance after drive add")
				err = ValidatePoolRebalance()
				if err != nil {
					dash.VerifySafely(err, nil, "Validate pool rebalance")
				}
			}

			var newTotalPoolSize uint64
			for _, nodePool := range nodePools {
				pool, err := GetStoragePoolByUUID(nodePool.GetUuid())
				if err != nil {
					dash.VerifyFatal(err, nil, fmt.Sprintf("Error getting pool details of %s", nodePool.GetUuid()))
				}
				newTotalPoolSize += pool.GetTotalSize() / units.GiB
			}
			dash.VerifyFatal(newTotalPoolSize, expectedTotalPoolSize, fmt.Sprintf("Validate total pool size after add cloud drive on node %s", stNode.Name))

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})
