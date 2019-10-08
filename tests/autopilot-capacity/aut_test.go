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
	testNameSuite   = "AutopilotVolumeResize"
	timeout         = 30 * time.Minute
	workloadTimeout = 2 * time.Hour
	retryInterval   = 30 * time.Second
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
var _ = Describe(fmt.Sprintf("{%sBasic}", testNameSuite), func() {
	It("has to fill up the volume completely, resize the volume, validate and teardown apps", func() {
		var contexts []*scheduler.Context
		var err error
		testName := strings.ToLower(fmt.Sprintf("%sBasic", testNameSuite))

		apParameters := &scheduler.AutopilotParameters{
			Enabled: true,
			Name:    testName,
			AutopilotRuleParameters: scheduler.AutopilotRuleParameters{
				ActionsCoolDownPeriod: 60,
				PVCWorkloadSize:       10737418240, //10Gb
				PVCPercentageUsage:    50,
				PVCPercentageScale:    50,
			},
		}

		Step("schedule applications", func() {
			for i := 0; i < Inst().ScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%v", fmt.Sprintf("%s-%d", testName, i), Inst().InstanceID)
				context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
					AppKeys:             Inst().AppList,
					StorageProvisioner:  Inst().Provisioner,
					AutopilotParameters: apParameters,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(context).NotTo(BeEmpty())
				contexts = append(contexts, context...)
			}
		})

		Step("wait until workload completes on volume", func() {
			for _, ctx := range contexts {
				err = Inst().S.WaitForRunning(ctx, workloadTimeout, retryInterval)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		Step("validating volumes and verifying size of volumes", func() {
			for _, ctx := range contexts {
				err = Inst().S.InspectVolumes(ctx, timeout, retryInterval)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		Step("wait for unscheduled resize of volume (10min)", func() {
			time.Sleep(10 * time.Minute)
		})

		Step("validating volumes and verifying size of volumes", func() {
			for _, ctx := range contexts {
				err = Inst().S.InspectVolumes(ctx, timeout, retryInterval)
				Expect(err).NotTo(HaveOccurred())
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
	PerformSystemCheck()
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
