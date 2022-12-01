package tests

import (
	"fmt"
	"github.com/portworx/torpedo/drivers/node"

	"github.com/portworx/torpedo/pkg/log"

	"github.com/portworx/torpedo/pkg/testrailuttils"
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const poolResizeTimeout = time.Minute * 180
const retryTimeout = time.Minute * 2

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

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, " Storage pools exist?")

		// pick a random pool from a pools list and resize it
		poolIDToResize = getRandomPoolID(pools)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		time.Sleep(time.Second * 60)
		stepLog = "Verify that pool resize is non in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, "Failed to get pool using UUID ")
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
			log.FailOnError(err, "Failed to get pool using UUID ")
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			if newPoolSize == expectedSize || newPoolSize == expectedSizeWithJournal {
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

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, "Storage pools exist ?")

		// pick a random pool from a pools list and resize it
		poolIDToResize = getRandomPoolID(pools)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is none in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, "Failed to get pool using UUID ")
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

			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Expected new size to be '%d' or '%d' if pool has journal", expectedSize, expectedSizeWithJournal))
		})

		Step("Ensure that new pool has been expanded to the expected size", func() {
			ValidateApplications(contexts)

			resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
			log.FailOnError(err, "Failed to get pool using UUID ")
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
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		time.Sleep(time.Second * 60)
		stepLog = "Verify that pool resize is none in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, "Failed to get pool using UUID ")
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		stepLog = "Calculate expected pool size and trigger pool resize"
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

			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful ?")

			err = WaitForExpansionToStart(poolIDToResize)
			log.FailOnError(err, "Expansion is not started")

			storageNode, err := GetNodeWithGivenPoolID(poolIDToResize)
			log.FailOnError(err, "Failed to get pool using UUID ")
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
			log.FailOnError(err, "Failed to get pool using UUID ")
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			if newPoolSize == expectedSize || newPoolSize == expectedSizeWithJournal {
				isExpansionSuccess = true
			}
			dash.VerifyFatal(isExpansionSuccess, true,
				fmt.Sprintf("Expected new pool size to be %v or %v, got %v", expectedSize, expectedSizeWithJournal, newPoolSize))
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
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pooladddiskreboot-%d", i))...)
		}

		ValidateApplications(contexts)

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "Failed to list storage pools")
		dash.VerifyFatal(len(pools) > 0, true, "Storage pools exist?")

		// pick a random pool from a pools list and resize it
		poolIDToResize = getRandomPoolID(pools)
		dash.VerifyFatal(len(poolIDToResize) > 0, true, fmt.Sprintf("Expected poolIDToResize to not be empty, pool id to resize %s", poolIDToResize))

		poolToBeResized := pools[poolIDToResize]
		dash.VerifyFatal(poolToBeResized != nil, true, "Pool to be resized exist?")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		stepLog = "Verify that pool resize is non in progress"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				log.FailOnError(err, "Failed to get pool using UUID")
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		stepLog = "Calculate expected pool size and trigger pool resize"
		Step(stepLog, func() {
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB
			expectedSize = roundUpValue(expectedSize)
			isjournal, err := isJournalEnabled()
			log.FailOnError(err, "Failed to check is journal enabled")

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}

			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			dash.VerifyFatal(err, nil, "Pool expansion init successful?")

			err = WaitForExpansionToStart(poolIDToResize)
			log.FailOnError(err, "Failed while waiting for expansion to start")

			storageNode, err := GetNodeWithGivenPoolID(poolIDToResize)
			log.FailOnError(err, " Failed to get pool using UUID")
			err = RebootNodeAndWait(*storageNode)
			log.FailOnError(err, "Failed to reboot node and wait till it is up")
			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
			dash.VerifyFatal(resizeErr, nil, fmt.Sprintf("Expected new size to be '%d' or '%d' if pool has journal", expectedSize, expectedSizeWithJournal))
		})

		stepLog = "Ensure that new pool has been expanded to the expected size"
		Step(stepLog, func() {
			ValidateApplications(contexts)

			resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
			log.FailOnError(err, " Failed to get pool using UUID")
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			if newPoolSize == expectedSize || newPoolSize == expectedSizeWithJournal {
				isExpansionSuccess = true
			}
			dash.VerifyFatal(isExpansionSuccess, true,
				fmt.Sprintf("Expected new pool size to be %v or %v if pool has journal, got %v", expectedSize, expectedSizeWithJournal, newPoolSize))
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
		log.InfoD(stepLog)
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
		log.FailOnError(err, "Failed to list storage pools")
		dash.VerifyFatal(len(nodePools) > 1, true, "Node has multiple storage pools?")

		for _, p := range nodePools {
			poolsToBeResized = append(poolsToBeResized, pools[p.Uuid])
		}

		dash.VerifyFatal(poolsToBeResized != nil, true, "Pools pending to be resized")

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wait until the ongoing operation is completed
		stepLog = "Verify that pool resize is none in progress"
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
				expectedSize = poolToBeResized.TotalSize * 2 / units.GiB
				poolsExpectedSizeMap[poolToBeResized.Uuid] = expectedSize

				//To-Do Need to handle the case for multiple pools
				expectedSizeWithJournal = expectedSize
				if isjournal {
					expectedSizeWithJournal = expectedSizeWithJournal - 3
				}
				err = Inst().V.ExpandPool(poolToBeResized.Uuid, operation, expectedSize)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Pool %s expansion init succesful?", poolToBeResized.Uuid))
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
				if newPoolSize == expectedSize || newPoolSize == expectedSizeWithJournal {
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
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("AddNewPoolWhileRebalance", "Validate adding nee storage pool while another pool rebalancing", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	var contexts []*scheduler.Context
	stepLog := "has to schedule apps, and expand it by resizing a disk"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("addnewpoolrebal-%d", i))...)
		}

		ValidateApplications(contexts)

		var poolIDToResize string

		stNodes := node.GetStorageNodes()
		var poolToBeResized *api.StoragePool
		var currentTotalPoolSize uint64
		var err error
		var nodeSelected node.Node
		var pools map[string]*api.StoragePool
		//waiting till one of the volume has enough IO and selecting pool and node  using the volume to run the test
		var volSelected *api.Volume
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
					if usedGiB > 10 {
						volSelected = appVol
						return nil, false, nil
					}
				}
			}
			return nil, true, fmt.Errorf("error getting volume with size atleast 10 GiB used")
		}
		_, err = task.DoRetryWithTimeout(f, 15*time.Minute, retryTimeout)
		log.FailOnError(err, "error identifying volume")

		log.Infof("%+v", volSelected)
		attachedNodeID := volSelected.ReplicaSets[0].Nodes[0]
		volumePools := volSelected.ReplicaSets[0].PoolUuids
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
		expandedExpectedPoolSize := currentTotalPoolSize * 2

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

			err = Inst().V.AddCloudDrive(&nodeSelected, newSpec)
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
				dash.VerifySafely(status, api.Status_STATUS_OK, fmt.Sprintf("validate PX status on node %s", stNode.Name))
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
				return nil, false, fmt.Errorf("PoolResize has failed. Error: %s", expandedPool.LastOperation)
			}
			if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_IN_PROGRESS {
				if strings.Contains(expandedPool.LastOperation.Msg, "Rebalance in progress") {
					if currentLastMsg == expandedPool.LastOperation.Msg {
						return nil, false, fmt.Errorf("pool reblance is not progressing")
					} else {
						currentLastMsg = expandedPool.LastOperation.Msg
						return nil, true, nil
					}
				}
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
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("pooladddrive-%d", i))...)
		}
		ValidateApplications(contexts)

		stNodes := node.GetStorageNodes()
		if len(stNodes) == 0 {
			dash.VerifyFatal(len(stNodes) > 0, true, "Storage nodes found?")
		}

		driveSpecs, err := GetCloudDriveDeviceSpecs()
		log.FailOnError(err, "Error getting cloud drive specs")

		stNode := stNodes[0]
		deviceSpec := driveSpecs[0]
		deviceSpecParams := strings.Split(deviceSpec, ",")
		var specSize uint64
		for _, param := range deviceSpecParams {
			if strings.Contains(param, "size") {
				val := strings.Split(param, "=")[1]
				specSize, err = strconv.ParseUint(val, 10, 64)
				log.FailOnError(err, "Error converting size to uint64")
			}
		}

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		log.FailOnError(err, "error getting pools list")
		dash.VerifyFatal(len(pools) > 0, true, "Verify pools exist")

		var currentTotalPoolSize uint64

		for _, pool := range pools {
			currentTotalPoolSize += pool.GetTotalSize() / units.GiB
		}

		expectedTotalPoolSize := currentTotalPoolSize + specSize

		stepLog = "Initiate add cloud drive and validate"
		Step(stepLog, func() {
			err = Inst().V.AddCloudDrive(&stNode, deviceSpec)
			log.FailOnError(err, fmt.Sprintf("Add cloud drive failed on node %s", stNode.Name))
			log.InfoD("Validate pool rebalance after drive add")
			err = ValidatePoolRebalance()
			log.FailOnError(err, "Pool re-balance failed")
			err = Inst().V.WaitDriverUpOnNode(stNode, 2*time.Minute)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", stNode.Name))
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
		AfterEachTest(contexts)
	})
})
