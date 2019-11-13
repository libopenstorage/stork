package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	portworx "github.com/portworx/torpedo/drivers/volume/portworx"
	. "github.com/portworx/torpedo/tests"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	testNameSuite            = "VolResize"
	timeout                  = 30 * time.Minute
	scaleTimeout             = 2 * time.Hour
	workloadTimeout          = 2 * time.Hour
	retryInterval            = 30 * time.Second
	unscheduledResizeTimeout = 10 * time.Minute
)

// Resize PVC by 50% until volume usage is more than 50%
var ruleResizeBy50IfPvcUsageMoreThan50 = apapi.AutopilotRule{
	ObjectMeta: metav1.ObjectMeta{
		Name: "volresize-usage50",
	},
	Spec: apapi.AutopilotRuleSpec{
		Conditions: apapi.RuleConditions{
			Expressions: []*apapi.LabelSelectorRequirement{
				{
					Key:      portworx.PxVolumeUsagePercentMetric,
					Operator: apapi.LabelSelectorOpGt,
					Values:   []string{"50"},
				},
			},
		},
		Actions: []*apapi.RuleAction{
			{
				Name: portworx.VolumeSpecAction,
				Params: map[string]string{
					portworx.RuleActionsScalePercentage: "50",
				},
			},
		},
	},
}

// Resize PVC by 50% until volume usage is more than 90%
var ruleResizeBy50IfPvcUsageMoreThan90 = apapi.AutopilotRule{
	ObjectMeta: metav1.ObjectMeta{
		Name: "volresize-usage90",
	},
	Spec: apapi.AutopilotRuleSpec{
		Conditions: apapi.RuleConditions{
			Expressions: []*apapi.LabelSelectorRequirement{
				{
					Key:      portworx.PxVolumeUsagePercentMetric,
					Operator: apapi.LabelSelectorOpGt,
					Values:   []string{"90"},
				},
			},
		},
		Actions: []*apapi.RuleAction{
			{
				Name: portworx.VolumeSpecAction,
				Params: map[string]string{
					portworx.RuleActionsScalePercentage: "50",
				},
			},
		},
	},
}

// Resize PVC by 50% until volume size is 20Gb
var ruleResizeBy50UntilPvcMaxSize20 = apapi.AutopilotRule{
	ObjectMeta: metav1.ObjectMeta{
		Name: "volresize-maxsize20",
	},
	Spec: apapi.AutopilotRuleSpec{
		Conditions: apapi.RuleConditions{
			Expressions: []*apapi.LabelSelectorRequirement{
				{
					Key:      portworx.PxVolumeUsagePercentMetric,
					Operator: apapi.LabelSelectorOpGt,
					Values:   []string{"50"},
				},
			},
		},
		Actions: []*apapi.RuleAction{
			{
				Name: portworx.VolumeSpecAction,
				Params: map[string]string{
					portworx.RuleActionsScalePercentage: "50",
					portworx.RuleMaxSize:                "21474836480",
				},
			},
		},
	},
}

// Resize PVC by 50% until volume is less than 10Gb
var ruleResizeBy50IfPvcCapacityLessThan10 = apapi.AutopilotRule{
	ObjectMeta: metav1.ObjectMeta{
		Name: "volresize-capacity10",
	},
	Spec: apapi.AutopilotRuleSpec{
		Conditions: apapi.RuleConditions{
			Expressions: []*apapi.LabelSelectorRequirement{
				{
					Key:      portworx.PxVolumeCapacityPercentMetric,
					Operator: apapi.LabelSelectorOpLt,
					Values:   []string{"10"},
				},
			},
		},
		Actions: []*apapi.RuleAction{
			{
				Name: portworx.VolumeSpecAction,
				Params: map[string]string{
					portworx.RuleActionsScalePercentage: "50",
				},
			},
		},
	},
}

var autopilotruleBasicTestCases = []apapi.AutopilotRule{
	ruleResizeBy50IfPvcUsageMoreThan50,
	ruleResizeBy50IfPvcUsageMoreThan90,
	ruleResizeBy50UntilPvcMaxSize20,
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
var _ = Describe(fmt.Sprintf("{%sBasic}", testNameSuite), func() {

	It("has to fill up the volume completely, resize the volume, validate and teardown apps", func() {
		var err error
		var contexts []*scheduler.Context
		testName := strings.ToLower(fmt.Sprintf("%sBasic", testNameSuite))

		Step("schedule applications", func() {
			for i := 0; i < Inst().ScaleFactor; i++ {
				for _, apRule := range autopilotruleBasicTestCases {
					taskName := fmt.Sprintf("%s-%d", fmt.Sprintf("%s-%s", testName, apRule.Name), i)
					apRule.Name = fmt.Sprintf("%s-%d", apRule.Name, i)
					labels := map[string]string{
						"autopilot": apRule.Name,
					}
					apRule.Spec.ActionsCoolDownPeriod = int64(60)
					context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
						AppKeys:            Inst().AppList,
						StorageProvisioner: Inst().Provisioner,
						AutopilotRule:      apRule,
						Labels:             labels,
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(context).NotTo(BeEmpty())
					contexts = append(contexts, context...)
				}
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
	})
})

// This testsuite is used for performing basic scenarios with Autopilot rules where it
// schedules apps and wait until workload is completed on the volumes. Restarts volume
// driver and validates PVC sizes of the volumes
var _ = Describe(fmt.Sprintf("{%sVolumeDriverDown}", testNameSuite), func() {

	It("has to fill up the volume completely, resize the volume, validate and teardown apps", func() {
		var err error
		var contexts []*scheduler.Context
		testName := strings.ToLower(fmt.Sprintf("%sBasic", testNameSuite))

		Step("schedule applications", func() {
			for i := 0; i < Inst().ScaleFactor; i++ {
				for _, apRule := range autopilotruleBasicTestCases {
					taskName := fmt.Sprintf("%s-%d", fmt.Sprintf("%s-%s", testName, apRule.Name), i)
					apRule.Name = fmt.Sprintf("%s-%d", apRule.Name, i)
					labels := map[string]string{
						"autopilot": apRule.Name,
					}
					apRule.Spec.ActionsCoolDownPeriod = int64(60)
					context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
						AppKeys:            Inst().AppList,
						StorageProvisioner: Inst().Provisioner,
						AutopilotRule:      apRule,
						Labels:             labels,
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(context).NotTo(BeEmpty())
					contexts = append(contexts, context...)
				}
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
	})
})

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
