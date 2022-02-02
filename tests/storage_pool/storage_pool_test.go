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
	. "github.com/portworx/torpedo/tests"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestStoragePool(t *testing.T) {
	RegisterFailHandler(Fail)
	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_basic.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecs(t, "StoragePoolResize Suite")
}

var _ = BeforeSuite(func() {
	InitInstance()
})

var _ = Describe("{StoragePoolResize}", func() {

	It("should get the existing pool", func() {
		pools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
		if err != nil {
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(len(pools)).ShouldNot(BeEmpty())

		var poolID string
		for _, pool := range pools {
			poolID = pool.Uuid
			break
		}
		// Pick any pool and trigger pool resize command

		poolToResize := pools[poolID]
		oldSize := poolToResize.TotalSize
		newSize := oldSize * 2

		err = Inst().V.ExpandPool(poolID, api.SdkStoragePool_RESIZE_TYPE_AUTO, newSize)
		Expect(err).NotTo(HaveOccurred())

		time.Sleep(time.Second * 40)

		It(fmt.Sprintf("triggers pool resize %s to the size of %v", poolToResize.Uuid, newSize), func() {
			newPools, err := Inst().V.ListStoragePools(metav1.LabelSelector{})
			Expect(err).NotTo(HaveOccurred())

			var newPool *api.StoragePool
			for _, pool := range newPools {
				if poolID == pool.Uuid {
					newPool = pool
					break
				}
			}
			Expect(newPool).ShouldNot(BeNil())
			Expect(newPool.TotalSize).Should(Equal(newSize))
		})
	})

})

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
