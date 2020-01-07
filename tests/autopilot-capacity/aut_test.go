package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

var (
	defaultCoolDownPeriod      = int64(60)
	pxVolumeUsagePercent       = "100 * (px_volume_usage_bytes / px_volume_capacity_bytes)"
	pxVolumeCapacityPercent    = "px_volume_capacity_bytes / 1000000000"
	labelSelectorOpGt          = "Gt"
	labelSelectorOpLt          = "Lt"
	specActionName             = "openstorage.io.action.volume/resize"
	ruleActionsScalePercentage = "scalepercentage"
	ruleActionsMaxSize         = "maxsize"
)

var (
	testNameSuite            = "AutopilotVolumeResize"
	timeout                  = 30 * time.Minute
	scaleTimeout             = 2 * time.Hour
	workloadTimeout          = 2 * time.Hour
	retryInterval            = 30 * time.Second
	unscheduledResizeTimeout = 10 * time.Minute
)

var ruleResizeBy50IfPvcUsageMoreThan50 = scheduler.AutopilotRuleParameters{
	// Resize PVC by 50% until volume usage is more than 50Gb
	ActionsCoolDownPeriod: defaultCoolDownPeriod,
	RuleConditionExpressions: []scheduler.AutopilotRuleConditionExpressions{
		{
			Key:      pxVolumeUsagePercent,
			Operator: labelSelectorOpGt,
			Values:   []string{"50"},
		},
	},
	RuleActions: []scheduler.AutopilotRuleActions{
		{
			Name: specActionName,
			Params: map[string]string{
				ruleActionsScalePercentage: "50",
			},
		},
	},
	ExpectedPVCSize: 27179089920,
}

var ruleResizeBy50IfPvcCapacityLessThan10 = scheduler.AutopilotRuleParameters{
	// Resize PVC by 50% until volume is less than 10Gb
	ActionsCoolDownPeriod: defaultCoolDownPeriod,
	RuleConditionExpressions: []scheduler.AutopilotRuleConditionExpressions{
		{
			Key:      pxVolumeCapacityPercent,
			Operator: labelSelectorOpLt,
			Values:   []string{"10"},
		},
	},
	RuleActions: []scheduler.AutopilotRuleActions{
		{
			Name: specActionName,
			Params: map[string]string{
				ruleActionsScalePercentage: "50",
			},
		},
	},
	ExpectedPVCSize: 12079595520,
}

var ruleResizeBy50UntilPvcMaxSize20 = scheduler.AutopilotRuleParameters{
	// Resize PVC by 50% until volume size is 20Gb
	ActionsCoolDownPeriod: defaultCoolDownPeriod,
	RuleConditionExpressions: []scheduler.AutopilotRuleConditionExpressions{
		{
			Key:      pxVolumeUsagePercent,
			Operator: labelSelectorOpGt,
			Values:   []string{"50"},
		},
	},
	RuleActions: []scheduler.AutopilotRuleActions{
		{
			Name: specActionName,
			Params: map[string]string{
				ruleActionsScalePercentage: "50",
				ruleActionsMaxSize:         "20Gi",
			},
		},
	},
	ExpectedPVCSize: 21474836480,
}

var autopilotruleBasicTestCases = []scheduler.AutopilotRuleParameters{
	ruleResizeBy50IfPvcUsageMoreThan50,
	ruleResizeBy50UntilPvcMaxSize20,
}

var autopilotruleScaleTestCases = []scheduler.AutopilotRuleParameters{
	ruleResizeBy50IfPvcCapacityLessThan10,
}

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

// This testsuite is used for performing basic scenarios with Autopilot rules where it
// schedules apps and wait until workload is completed on the volumes and then validates
// PVC sizes of the volumes
var _ = Describe(fmt.Sprintf("{%sWaitForWorkload}", testNameSuite), func() {
	var contexts []*scheduler.Context

	It("has to fill up the volume completely, resize the volume, validate and teardown apps", func() {
		var err error
		testName := strings.ToLower(fmt.Sprintf("%sWaitForWorkload", testNameSuite))

		for _, apRule := range autopilotruleBasicTestCases {
			apParameters := &scheduler.AutopilotParameters{
				Enabled:                 true,
				Name:                    testName,
				AutopilotRuleParameters: apRule,
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

			Step(fmt.Sprintf("wait for unscheduled resize of volume (%s)", unscheduledResizeTimeout), func() {
				time.Sleep(unscheduledResizeTimeout)
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
		}
	})
	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

// This testsuite is used for performing basic scenarios with Autopilot rules where it
// schedules apps and doesn't wait until workload is completed on the volumes and then
// validates PVC sizes of the volumes
var _ = Describe(fmt.Sprintf("{%sDoesNotWaitForWorkload}", testNameSuite), func() {
	var contexts []*scheduler.Context

	It("will resize the volume until the max size of the volume", func() {
		var err error
		testName := strings.ToLower(fmt.Sprintf("%sDoesNotWaitForWorkload", testNameSuite))

		for _, apRule := range autopilotruleScaleTestCases {
			contexts = make([]*scheduler.Context, 0)

			apParameters := &scheduler.AutopilotParameters{
				Enabled:                 true,
				Name:                    testName,
				AutopilotRuleParameters: apRule,
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

			Step("validating volumes and verifying size of volumes", func() {
				for _, ctx := range contexts {
					err = Inst().S.InspectVolumes(ctx, scaleTimeout, retryInterval)
					Expect(err).NotTo(HaveOccurred())
				}
			})

			Step(fmt.Sprintf("wait for unscheduled resize of volume (%s)", unscheduledResizeTimeout), func() {
				time.Sleep(unscheduledResizeTimeout)
			})

			Step("validating volumes and verifying size of volumes", func() {
				for _, ctx := range contexts {
					err = Inst().S.InspectVolumes(ctx, scaleTimeout, retryInterval)
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
		}
	})
	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

// This testsuite is used for performing basic scenarios with Autopilot rules where it
// schedules apps and wait until workload is completed on the volumes. Restarts volume
// driver and validates PVC sizes of the volumes
var _ = Describe(fmt.Sprintf("{%sVolumeDriverDown}", testNameSuite), func() {
	var contexts []*scheduler.Context

	It("has to fill up the volume completely, resize the volume, validate and teardown apps", func() {
		var err error
		testName := strings.ToLower(fmt.Sprintf("%sVolumeDriverDown", testNameSuite))

		for _, apRule := range autopilotruleBasicTestCases {
			contexts = make([]*scheduler.Context, 0)

			apParameters := &scheduler.AutopilotParameters{
				Enabled:                 true,
				Name:                    testName,
				AutopilotRuleParameters: apRule,
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

			Step("get nodes bounce volume driver", func() {
				for _, appNode := range node.GetStorageDriverNodes() {
					Step(
						fmt.Sprintf("stop volume driver %s on node: %s",
							Inst().V.String(), appNode.Name),
						func() {
							StopVolDriverAndWait([]node.Node{appNode})
						})

					Step(
						fmt.Sprintf("starting volume %s driver on node %s",
							Inst().V.String(), appNode.Name),
						func() {
							StartVolDriverAndWait([]node.Node{appNode})
						})

					Step("Giving few seconds for volume driver to stabilize", func() {
						time.Sleep(20 * time.Second)
					})
				}
			})

			Step("validating volumes and verifying size of volumes", func() {
				for _, ctx := range contexts {
					err = Inst().S.InspectVolumes(ctx, timeout, retryInterval)
					Expect(err).NotTo(HaveOccurred())
				}
			})

			Step(fmt.Sprintf("wait for unscheduled resize of volume (%s)", unscheduledResizeTimeout), func() {
				time.Sleep(unscheduledResizeTimeout)
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
		}
	})
	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
