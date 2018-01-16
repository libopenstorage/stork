package tests

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

func TestUpgrade(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Torpedo : Upgrade")
}

var _ = BeforeSuite(func() {
	InitInstance()
})

var _ = Describe("Upgrade volume driver", func() {
	It("upgrade volume driver and ensure everything is running fine", func() {
		contexts := ScheduleAndValidate("upgradevolumedriver")

		Step("start the upgrade of volume driver", func() {
			err := Inst().V.UpgradeDriver(Inst().StorageDriverUpgradeVersion)
			Expect(err).NotTo(HaveOccurred())
		})

		Step("validate all apps after upgrade", func() {
			for _, ctx := range contexts {
				ValidateContext(ctx)
			}
		})

		Step("start the downgrade of volume driver", func() {
			err := Inst().V.UpgradeDriver(Inst().StorageDriverBaseVersion)
			Expect(err).NotTo(HaveOccurred())
		})

		Step("validate all apps after downgrade", func() {
			for _, ctx := range contexts {
				ValidateContext(ctx)
			}
		})

		Step("destroy apps", func() {
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
			for _, ctx := range contexts {
				TearDownContext(ctx, opts)
			}
		})
	})
})

var _ = AfterSuite(func() {
	CollectSupport()
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
