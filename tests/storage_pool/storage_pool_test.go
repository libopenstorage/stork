package tests

import (
	"fmt"
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
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("voldriverdown-%d", i))...)
		}

		ValidateApplications(contexts)

		var poolIDToResize string

		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		Expect(err).NotTo(HaveOccurred())

		Expect(len(pools)).NotTo(Equal(0))

		// pick a random pool from a pools list and resize it
		for _, pool := range pools {
			poolIDToResize = pool.Uuid
			break
		}
		Expect(poolIDToResize).ShouldNot(BeEmpty(), "Expected poolIDToResize to not be empty")

		poolToBeResized := pools[poolIDToResize]
		Expect(poolToBeResized).ShouldNot(BeNil())

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		Step("Verify that pool resize is non in progress", func() {
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized = getStoragePoolByUUID(poolIDToResize)
			}
		})

		var expectedSize uint64

		Step("Calculate expected pool size and trigger pool resize", func() {
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB

			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK, expectedSize)
			Expect(err).NotTo(HaveOccurred())

			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize)
			Expect(resizeErr).ToNot(HaveOccurred(), fmt.Sprintf("expected new size to be '%d', and error %s not not have occured", expectedSize, resizeErr))
		})

		Step("Ensure that new pool has been expanded to the expected size", func() {
			ValidateApplications(contexts)

			resizedPool := getStoragePoolByUUID(poolIDToResize)
			newPoolSize := resizedPool.TotalSize / units.GiB
			Expect(newPoolSize).To(BeNumerically("==", expectedSize), fmt.Sprintf("expected new pool size to be %v, got %v", expectedSize, newPoolSize))
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
		for _, pool := range pools {
			poolIDToResize = pool.Uuid
			break
		}
		Expect(poolIDToResize).ShouldNot(BeEmpty(), "Expected poolIDToResize to not be empty")

		poolToBeResized := pools[poolIDToResize]
		Expect(poolToBeResized).ShouldNot(BeNil())

		// px will put a new request in a queue, but in this case we can't calculate the expected size,
		// so need to wain until the ongoing operation is completed
		Step("Verify that pool resize is non in progress", func() {
			if poolResizeIsInProgress(poolToBeResized) {
				// wait until resize is completed and get the updated pool again
				poolToBeResized = getStoragePoolByUUID(poolIDToResize)
			}
		})

		var expectedSize uint64

		Step("Calculate expected pool size and trigger pool resize", func() {
			expectedSize = poolToBeResized.TotalSize * 2 / units.GiB

			err = Inst().V.ExpandPool(poolIDToResize, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK, expectedSize)
			Expect(err).NotTo(HaveOccurred())

			resizeErr := waitForPoolToBeResized(expectedSize, poolIDToResize)
			Expect(resizeErr).ToNot(HaveOccurred(), fmt.Sprintf("expected new size to be '%d', and error %s not not have occured", expectedSize, resizeErr))
		})

		Step("Ensure that new pool has been expanded to the expected size", func() {
			ValidateApplications(contexts)

			resizedPool := getStoragePoolByUUID(poolIDToResize)
			newPoolSize := resizedPool.TotalSize / units.GiB
			Expect(newPoolSize).To(BeNumerically("==", expectedSize), fmt.Sprintf("expected new pool size to be %v, got %v", expectedSize, newPoolSize))
		})
	})
})

func poolResizeIsInProgress(poolToBeResized *api.StoragePool) bool {
	poolSizeHasBeenChanged := false
	if poolToBeResized.LastOperation != nil {
		for {
			if poolToBeResized.LastOperation.Status != api.SdkStoragePool_OPERATION_SUCCESSFUL {
				Expect(poolToBeResized.LastOperation.Status).ShouldNot(BeEquivalentTo(api.SdkStoragePool_OPERATION_FAILED), fmt.Sprintf("PoolResize has failed. Error: %s", poolToBeResized.LastOperation))
				logrus.Infof("Pool Resize is already in progress: %v", poolToBeResized.LastOperation)
				time.Sleep(time.Second * 10)
				continue
			}
			poolSizeHasBeenChanged = true
			break
		}
	}
	return poolSizeHasBeenChanged
}

func getStoragePoolByUUID(poolUUID string) *api.StoragePool {
	pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
	Expect(err).NotTo(HaveOccurred())
	Expect(len(pools)).NotTo(Equal(0))

	pool := pools[poolUUID]
	Expect(pool).ShouldNot(BeNil(), "Expected pool to not be nil")
	return pool
}

func waitForPoolToBeResized(expectedSize uint64, poolIDToResize string) error {

	f := func() (interface{}, bool, error) {
		expandedPool := getStoragePoolByUUID(poolIDToResize)
		if expandedPool == nil {
			return nil, false, fmt.Errorf("expanded pool value is nil")
		}
		if expandedPool.LastOperation != nil {
			if expandedPool.LastOperation.Status == api.SdkStoragePool_OPERATION_FAILED {
				return nil, false, fmt.Errorf("PoolResize has failed. Error: %s", expandedPool.LastOperation)
			}
		}
		newPoolSize := expandedPool.TotalSize / units.GiB
		if newPoolSize == expectedSize {
			// storage pool resize has been completed
			return nil, true, nil
		}
		return nil, true, fmt.Errorf("pool has not been resized to %d yet. Waiting...Current size is %d", expectedSize, newPoolSize)
	}

	_, err := task.DoRetryWithTimeout(f, poolResizeTimeout, retryTimeout)
	return err
}

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func TestMain(m *testing.M) {
	ParseFlags()
	os.Exit(m.Run())
}
