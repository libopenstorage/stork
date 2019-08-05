package tests

import (
	"fmt"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	. "github.com/portworx/torpedo/tests"
)

func TestUpgrade(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_Upgrade.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : Upgrade", specReporters)
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
			images := getImages(Inst().StorageDriverUpgradeVersion)
			err := Inst().V.UpgradeDriver(images)
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

func getImages(version string) []volume.Image {
	images := make([]volume.Image, 0)
	for _, imagestr := range strings.Split(version, ",") {
		image := strings.Split(imagestr, "=")
		if len(image) > 1 {
			images = append(images, volume.Image{Type: image[0], Version: image[1]})
		} else {
			images = append(images, volume.Image{Type: "", Version: image[0]})
		}

	}
	return images
}

var _ = PDescribe("{UpgradeDowngradeVolumeDriver}", func() {
	It("upgrade and downgrade volume driver and ensure everything is running fine", func() {
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("upgradedowngradevolumedriver-%d", i))...)
		}

		Step("start the upgrade of volume driver", func() {
			images := getImages(Inst().StorageDriverUpgradeVersion)
			err := Inst().V.UpgradeDriver(images)
			Expect(err).NotTo(HaveOccurred())
		})

		Step("validate all apps after upgrade", func() {
			for _, ctx := range contexts {
				ValidateContext(ctx)
			}
		})

		Step("start the downgrade of volume driver", func() {
			images := getImages(Inst().StorageDriverBaseVersion)
			err := Inst().V.UpgradeDriver(images)
			Expect(err).NotTo(HaveOccurred())
		})

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
		ValidateAndDestroy(contexts, opts)
	})
})

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
