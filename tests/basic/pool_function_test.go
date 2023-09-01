package tests

import (
	"github.com/portworx/torpedo/pkg/log"

	"github.com/libopenstorage/openstorage/api"
	. "github.com/onsi/ginkgo"
	"github.com/portworx/torpedo/pkg/units"
	. "github.com/portworx/torpedo/tests"
)

var _ = Describe("{StoragePoolExpandSmoky}", func() {
	BeforeEach(func() {
		StartTorpedoTest(testName, testDescription, nil, 0)
		contexts = scheduleApps()
	})

	JustBeforeEach(func() {
		poolIDToResize = pickPoolToResize()
		poolToBeResized = getStoragePool(poolIDToResize)
	})

	JustAfterEach(func() {
		AfterEachTest(contexts)
	})

	AfterEach(func() {
		appsValidateAndDestroy(contexts)
		EndTorpedoTest()
	})

	testName = "StoragePoolExpandDiskAdd"
	testDescription = "Validate storage pool expansion using add-disk option"
	It("select a pool that has I/O and expand it by 100 GiB with add-disk type. ", func() {
		originalSizeInBytes = poolToBeResized.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB := targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type add-disk",
			poolIDToResize, poolToBeResized.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_ADD_DISK)
		resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
		verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
	})

	testName = "StoragePoolExpandDiskResize"
	testDescription = "Validate storage pool expansion using resize-disk option"
	It("select a pool that has I/O and expand it by 100 GiB with resize-disk type. ", func() {
		originalSizeInBytes = poolToBeResized.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB := targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type resize-disk",
			poolIDToResize, poolToBeResized.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_RESIZE_DISK)
		resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
		verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
	})

	testName = "StoragePoolExpandDiskAuto"
	testDescription = "Validate storage pool expansion using auto option"
	It("select a pool that has I/O and expand it by 100 GiB with auto type. ", func() {
		originalSizeInBytes = poolToBeResized.TotalSize
		targetSizeInBytes = originalSizeInBytes + 100*units.GiB
		targetSizeGiB := targetSizeInBytes / units.GiB

		log.InfoD("Current Size of the pool %s is %d GiB. Trying to expand to %v GiB with type auto",
			poolIDToResize, poolToBeResized.TotalSize/units.GiB, targetSizeGiB)
		triggerPoolExpansion(poolIDToResize, targetSizeGiB, api.SdkStoragePool_RESIZE_TYPE_AUTO)
		resizeErr := waitForOngoingPoolExpansionToComplete(poolIDToResize)
		dash.VerifyFatal(resizeErr, nil, "Pool expansion does not result in error")
		verifyPoolSizeEqualOrLargerThanExpected(poolIDToResize, targetSizeGiB)
	})
})
