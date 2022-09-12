package tests

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/libopenstorage/openstorage/api"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/units"
	. "github.com/portworx/torpedo/tests"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const poolResizeTimeout = time.Minute * 90
const retryTimeout = time.Minute

func TestStoragePool(t *testing.T) {
	RegisterFailHandler(Fail)
	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_basic.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecs(t, "StoragePoolExpand Suite")
}

var _ = BeforeSuite(func() {
	InitInstance()
})

var _ = Describe("{StoragePoolExpandDiskResize}", func() {
	var contexts []*scheduler.Context

	It("has to schedule apps, and expand it by resizing a disk", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("poolexpand-%d", i))...)
		}

		ValidateApplications(contexts)

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		Expect(err).NotTo(HaveOccurred())

		Expect(len(pools)).NotTo(Equal(0))

		// pick a random pool from a pools list and resize it
		poolIDToResize = getRandomPoolID(pools)
		Expect(poolIDToResize).ShouldNot(BeEmpty(), "Expected poolIDToResize to not be empty")

		poolToBeResized := pools[poolIDToResize]
		Expect(poolToBeResized).ShouldNot(BeNil())

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		time.Sleep(time.Second * 60)
		Step("Verify that pool resize is non in progress", func() {
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		Step("Calculate expected pool size and trigger pool resize", func() {
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB

			isjournal, err := isJournalEnabled()
			Expect(err).NotTo(HaveOccurred())

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}

			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			Expect(err).NotTo(HaveOccurred())

			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
			Expect(resizeErr).ToNot(HaveOccurred(), fmt.Sprintf("expected new size to be '%d' or '%d', and error %s not not have occured", expectedSize, expectedSizeWithJournal, resizeErr))
		})

		Step("Ensure that new pool has been expanded to the expected size", func() {
			ValidateApplications(contexts)

			resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
			Expect(err).NotTo(HaveOccurred())
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			if newPoolSize == expectedSize || newPoolSize == expectedSizeWithJournal {
				isExpansionSuccess = true
			}
			Expect(isExpansionSuccess).To(BeTrue(), fmt.Sprintf("expected new pool size to be %v or %v, got %v", expectedSize, expectedSizeWithJournal, newPoolSize))

		})
	})
})

var _ = Describe("{StoragePoolExpandDiskAdd}", func() {
	var contexts []*scheduler.Context

	It("should get the existing pool and expand it by adding a disk", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("voldriverdown-%d", i))...)
		}

		ValidateApplications(contexts)

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		Expect(err).NotTo(HaveOccurred())

		Expect(len(pools)).NotTo(Equal(0))

		// pick a random pool from a pools list and resize it
		poolIDToResize = getRandomPoolID(pools)
		Expect(poolIDToResize).ShouldNot(BeEmpty(), "Expected poolIDToResize to not be empty")

		poolToBeResized := pools[poolIDToResize]
		Expect(poolToBeResized).ShouldNot(BeNil())

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		Step("Verify that pool resize is non in progress", func() {
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		Step("Calculate expected pool size and trigger pool resize", func() {
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB
			expectedSize = roundUpValue(expectedSize)
			isjournal, err := isJournalEnabled()
			Expect(err).NotTo(HaveOccurred())

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}

			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			Expect(err).NotTo(HaveOccurred())

			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
			Expect(resizeErr).ToNot(HaveOccurred(), fmt.Sprintf("expected new size to be '%d' or '%d' if pool has journal, and error %s not not have occured", expectedSize, expectedSizeWithJournal, resizeErr))
		})

		Step("Ensure that new pool has been expanded to the expected size", func() {
			ValidateApplications(contexts)

			resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
			Expect(err).NotTo(HaveOccurred())
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			if newPoolSize == expectedSize || newPoolSize == expectedSizeWithJournal {
				isExpansionSuccess = true
			}
			Expect(isExpansionSuccess).To(BeTrue(), fmt.Sprintf("expected new pool size to be %v or %v if pool has journal, got %v", expectedSize, expectedSizeWithJournal, newPoolSize))

		})
	})
})

var _ = Describe("{PoolResizeDiskReboot}", func() {
	var contexts []*scheduler.Context

	It("has to schedule apps, and expand it by resizing a disk", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("poolexpand-%d", i))...)
		}

		ValidateApplications(contexts)

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		Expect(err).NotTo(HaveOccurred())

		Expect(len(pools)).NotTo(Equal(0))

		// pick a random pool from a pools list and resize it
		poolIDToResize = getRandomPoolID(pools)

		Expect(poolIDToResize).ShouldNot(BeEmpty(), "Expected poolIDToResize to not be empty")

		poolToBeResized := pools[poolIDToResize]
		Expect(poolToBeResized).ShouldNot(BeNil())

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		time.Sleep(time.Second * 60)
		Step("Verify that pool resize is non in progress", func() {
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		Step("Calculate expected pool size and trigger pool resize", func() {
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB

			isjournal, err := isJournalEnabled()
			Expect(err).NotTo(HaveOccurred())

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}

			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			Expect(err).NotTo(HaveOccurred())

			err = WaitForExpansionToStart(poolIDToResize)
			Expect(err).NotTo(HaveOccurred())

			storageNode, err := GetNodeWithGivenPoolID(poolIDToResize)
			Expect(err).NotTo(HaveOccurred())
			err = RebootNodeAndWait(*storageNode)
			Expect(err).NotTo(HaveOccurred())
			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
			Expect(resizeErr).ToNot(HaveOccurred(), fmt.Sprintf("expected new size to be '%d' or '%d', and error %s not not have occured", expectedSize, expectedSizeWithJournal, resizeErr))
		})

		Step("Ensure that new pool has been expanded to the expected size", func() {
			ValidateApplications(contexts)

			resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
			Expect(err).NotTo(HaveOccurred())
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			if newPoolSize == expectedSize || newPoolSize == expectedSizeWithJournal {
				isExpansionSuccess = true
			}
			Expect(isExpansionSuccess).To(BeTrue(), fmt.Sprintf("expected new pool size to be %v or %v, got %v", expectedSize, expectedSizeWithJournal, newPoolSize))

		})
	})
})

var _ = Describe("{PoolAddDiskReboot}", func() {
	var contexts []*scheduler.Context

	It("should get the existing pool and expand it by adding a disk", func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("voldriverdown-%d", i))...)
		}

		ValidateApplications(contexts)

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		Expect(err).NotTo(HaveOccurred())

		Expect(len(pools)).NotTo(Equal(0))

		// pick a random pool from a pools list and resize it
		poolIDToResize = getRandomPoolID(pools)
		Expect(poolIDToResize).ShouldNot(BeEmpty(), "Expected poolIDToResize to not be empty")

		poolToBeResized := pools[poolIDToResize]
		Expect(poolToBeResized).ShouldNot(BeNil())

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		Step("Verify that pool resize is non in progress", func() {
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized, err = GetStoragePoolByUUID(poolIDToResize)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		var expectedSize uint64
		var expectedSizeWithJournal uint64

		Step("Calculate expected pool size and trigger pool resize", func() {
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB
			expectedSize = roundUpValue(expectedSize)
			isjournal, err := isJournalEnabled()
			Expect(err).NotTo(HaveOccurred())

			//To-Do Need to handle the case for multiple pools
			expectedSizeWithJournal = expectedSize
			if isjournal {
				expectedSizeWithJournal = expectedSizeWithJournal - 3
			}

			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			Expect(err).NotTo(HaveOccurred())

			err = WaitForExpansionToStart(poolIDToResize)
			Expect(err).NotTo(HaveOccurred())

			storageNode, err := GetNodeWithGivenPoolID(poolIDToResize)
			Expect(err).NotTo(HaveOccurred())
			err = RebootNodeAndWait(*storageNode)
			Expect(err).NotTo(HaveOccurred())
			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize, isjournal)
			Expect(resizeErr).ToNot(HaveOccurred(), fmt.Sprintf("expected new size to be '%d' or '%d' if pool has journal, and error %s not not have occured", expectedSize, expectedSizeWithJournal, resizeErr))
		})

		Step("Ensure that new pool has been expanded to the expected size", func() {
			ValidateApplications(contexts)

			resizedPool, err := GetStoragePoolByUUID(poolIDToResize)
			Expect(err).NotTo(HaveOccurred())
			newPoolSize := resizedPool.TotalSize / units.GiB
			isExpansionSuccess := false
			if newPoolSize == expectedSize || newPoolSize == expectedSizeWithJournal {
				isExpansionSuccess = true
			}
			Expect(isExpansionSuccess).To(BeTrue(), fmt.Sprintf("expected new pool size to be %v or %v if pool has journal, got %v", expectedSize, expectedSizeWithJournal, newPoolSize))

		})
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
	poolSizeHasBeenChanged := false
	if poolToBeResized.LastOperation != nil {
		for {
			pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
			Expect(err).NotTo(HaveOccurred())

			Expect(len(pools)).NotTo(Equal(0))

			updatedPoolToBeResized := pools[poolToBeResized.Uuid]
			Expect(updatedPoolToBeResized).ShouldNot(BeNil())
			if updatedPoolToBeResized.LastOperation.Status != api.SdkStoragePool_OPERATION_SUCCESSFUL {
				Expect(updatedPoolToBeResized.LastOperation.Status).ShouldNot(BeEquivalentTo(api.SdkStoragePool_OPERATION_FAILED), fmt.Sprintf("PoolResize has failed. Error: %s", updatedPoolToBeResized.LastOperation))
				logrus.Infof("Pool Resize is already in progress: %v", updatedPoolToBeResized.LastOperation)
				time.Sleep(time.Second * 90)
				continue
			}
			poolSizeHasBeenChanged = true
			break
		}
	}
	return poolSizeHasBeenChanged
}

func waitForPoolToBeResized(expectedSize uint64, poolIDToResize string, isJournalEnabled bool) error {

	f := func() (interface{}, bool, error) {
		expandedPool, err := GetStoragePoolByUUID(poolIDToResize)
		Expect(err).NotTo(HaveOccurred())

		if expandedPool == nil {
			return nil, false, fmt.Errorf("expanded pool value is nil")
		}
		if expandedPool.LastOperation != nil {
			if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_FAILED {
				return nil, false, fmt.Errorf("PoolResize has failed. Error: %s", expandedPool.LastOperation)
			}
		}
		newPoolSize := expandedPool.TotalSize / units.GiB
		expectedSizeWithJournal := expectedSize
		if isJournalEnabled {
			expectedSizeWithJournal = expectedSizeWithJournal - 3
		}
		if newPoolSize == expectedSize || newPoolSize == expectedSizeWithJournal {
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

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func TestMain(m *testing.M) {
	ParseFlags()
	os.Exit(m.Run())
}
