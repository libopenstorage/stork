package tests

import (
	"fmt"
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

var _ = Describe("{UpgradeVolumeDriver}", func() {
	It("upgrade volume driver and ensure everything is running fine", func() {
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("upgradevolumedriver-%d", i))...)
		}

		Step("start the upgrade of volume driver", func() {
			err := Inst().V.UpgradeDriver(Inst().StorageDriverUpgradeVersion)
			Expect(err).NotTo(HaveOccurred())
		})

		Step("validate all apps after upgrade", func() {
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

var _ = PDescribe("{UpgradeDowngradeVolumeDriver}", func() {
	It("upgrade and downgrade volume driver and ensure everything is running fine", func() {
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("upgradedowngradevolumedriver-%d", i))...)
		}

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

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
		ValidateAndDestroy(contexts, opts)
	})
})

var _ = AfterSuite(func() {
	CollectSupport()
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
