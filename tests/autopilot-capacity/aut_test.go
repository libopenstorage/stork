package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

var (
	testName      = "AutopilotVolumeResize"
	timeout       = 30 * time.Minute
	retryInterval = 30 * time.Second
)

func TestAutoPilot(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_autopilot.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : Autopilot", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
})

// This test performs basic test of starting an application, fills up the volume with data
// which is more than the size of volume and waits that volume should be resized.
var _ = Describe(fmt.Sprintf("{%s}", testName), func() {
	It("has to fill up the volume completely, resize the volume, validate and teardown apps", func() {
		var contexts []*scheduler.Context
		var err error

		apParameters := &scheduler.AutopilotParameters{
			Enabled: true,
			Name:    strings.ToLower(testName),
			AutopilotRuleParameters: scheduler.AutopilotRuleParameters{
				ActionsCoolDownPeriod: 60,
				PVCWorkloadSize:       10737418240, //10Gb
				PVCPercentageUsage:    50,
				PVCPercentageScale:    50,
			},
		}
		for i := 0; i < Inst().ScaleFactor; i++ {
			Step("schedule applications", func() {
				taskName := fmt.Sprintf("%s-%v", fmt.Sprintf("%s-%d", strings.ToLower(testName), i), Inst().InstanceID)
				contexts, err = Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
					AppKeys:             Inst().AppList,
					StorageProvisioner:  Inst().Provisioner,
					AutopilotParameters: apParameters,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(contexts).NotTo(BeEmpty())
			})
		}

		for _, ctx := range contexts {
			Step("wait until workload completes on volume", func() {
				err = Inst().S.WaitForRunning(ctx, timeout, retryInterval)
				Expect(err).NotTo(HaveOccurred())
			})
		}

		for _, ctx := range contexts {
			Step("validating volumes and verifying size of volumes", func() {
				err = Inst().S.InspectVolumes(ctx, timeout, retryInterval)
				Expect(err).NotTo(HaveOccurred())
			})
		}

		Step("wait for unscheduled resize of volume (10min)", func() {
			time.Sleep(10 * time.Minute)
		})

		for _, ctx := range contexts {
			Step("validating volumes and verifying size of volumes", func() {
				err = Inst().S.InspectVolumes(ctx, timeout, retryInterval)
				Expect(err).NotTo(HaveOccurred())
			})
		}

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
	PerformSystemCheck()
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
