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
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	testNameSuite            = "AutPoolResize"
	workloadTimeout          = 5 * time.Hour
	retryInterval            = 30 * time.Second
	unscheduledResizeTimeout = 10 * time.Minute
)

// Resize storage pool by adding disks by 50% until storage available capacity is less than %70
var ruleResizeBy50IfPoolAvailableUsageMoreThan70AddDisk = apapi.AutopilotRule{
	ObjectMeta: meta_v1.ObjectMeta{
		Name: "storage-pool-resize-add-disk-70",
	},
	Spec: apapi.AutopilotRuleSpec{
		Conditions: apapi.RuleConditions{
			Expressions: []*apapi.LabelSelectorRequirement{
				{
					Key:      portworx.PxPoolAvailableCapacityMetric,
					Operator: apapi.LabelSelectorOpLt,
					Values:   []string{"70"},
				},
			},
		},
		Actions: []*apapi.RuleAction{
			{
				Name: portworx.StorageSpecAction,
				Params: map[string]string{
					portworx.RuleActionsScalePercentage: "50",
					portworx.RuleScaleType:              portworx.RuleScaleTypeAddDisk,
				},
			},
		},
	},
}

// Resize storage pool by 50% until storage available capacity is less than %70
var ruleResizeBy50IfPoolAvailableUsageMoreThan70ResizeDisk = apapi.AutopilotRule{
	ObjectMeta: meta_v1.ObjectMeta{
		Name: "storage-pool-resize-disk-70",
	},
	Spec: apapi.AutopilotRuleSpec{
		Conditions: apapi.RuleConditions{
			Expressions: []*apapi.LabelSelectorRequirement{
				{
					Key:      portworx.PxPoolAvailableCapacityMetric,
					Operator: apapi.LabelSelectorOpLt,
					Values:   []string{"70"},
				},
			},
		},
		Actions: []*apapi.RuleAction{
			{
				Name: portworx.StorageSpecAction,
				Params: map[string]string{
					portworx.RuleActionsScalePercentage: "50",
					portworx.RuleScaleType:              portworx.RuleScaleTypeResizeDisk,
				},
			},
		},
	},
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
// sizes of storage pools by adding new disks to the nodes where volumes reside
var _ = Describe(fmt.Sprintf("{%sBasicResizeAddDisk}", testNameSuite), func() {
	It("has to fill up the volume completely, resize the storage pool(s), validate and teardown apps", func() {
		var err error
		testName := strings.ToLower(fmt.Sprintf("%sBasicAddDisk", testNameSuite))
		apRules := []apapi.AutopilotRule{
			ruleResizeBy50IfPoolAvailableUsageMoreThan70AddDisk,
		}
		var contexts []*scheduler.Context
		labels := map[string]string{
			"autopilot": "storage-pool-expansion-add-disk",
		}
		// adding labels to autopilot rule objects
		for idx := range apRules {
			apRules[idx].Spec.Selector.MatchLabels = labels
		}
		// adding labels to worker nodes
		workerNodes := node.GetWorkerNodes()
		for _, workerNode := range workerNodes {
			err = AddLabelsOnNode(workerNode, labels)
			Expect(err).NotTo(HaveOccurred())
		}

		Step("schedule applications", func() {
			for i := 0; i < Inst().ScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%v", fmt.Sprintf("%s-%d", testName, i), Inst().InstanceID)

				context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
					AppKeys:            Inst().AppList,
					StorageProvisioner: Inst().Provisioner,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(context).NotTo(BeEmpty())
				contexts = append(contexts, context...)
			}
		})

		Step("wait until all apps create volumes", func() {
			for _, ctx := range contexts {
				ValidateVolumes(ctx)
			}
		})

		Step("apply autopilot rules for storage pools", func() {
			err := Inst().V.CreateAutopilotRules(apRules)
			Expect(err).NotTo(HaveOccurred())
		})

		Step("wait until workload completes on volume", func() {
			for _, ctx := range contexts {
				err = Inst().S.WaitForRunning(ctx, workloadTimeout, retryInterval)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		Step("validating and verifying size of storage pools", func() {
			ValidateStoragePools(contexts)
		})

		Step(fmt.Sprintf("wait for unscheduled resize of storage pool (%s)", unscheduledResizeTimeout), func() {
			time.Sleep(unscheduledResizeTimeout)
		})

		Step("validating and verifying size of storage pools", func() {
			ValidateStoragePools(contexts)
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
// schedules apps and wait until workload is completed on the volumes and then validates
// sizes of storage pools on the nodes where volumes reside
var _ = Describe(fmt.Sprintf("{%sBasicResizeDisk}", testNameSuite), func() {
	It("has to fill up the volume completely, resize the storage pool(s), validate and teardown apps", func() {
		var err error
		testName := strings.ToLower(fmt.Sprintf("%sBasicResizeDisk", testNameSuite))
		apRules := []apapi.AutopilotRule{
			ruleResizeBy50IfPoolAvailableUsageMoreThan70ResizeDisk,
		}
		var contexts []*scheduler.Context
		labels := map[string]string{
			"autopilot": "storage-pool-expansion-resize-disk",
		}
		// adding labels to autopilot rule objects
		for idx := range apRules {
			apRules[idx].Spec.Selector.MatchLabels = labels
		}
		// adding labels to worker nodes
		workerNodes := node.GetWorkerNodes()
		for _, workerNode := range workerNodes {
			err = AddLabelsOnNode(workerNode, labels)
			Expect(err).NotTo(HaveOccurred())
		}

		Step("schedule applications", func() {
			for i := 0; i < Inst().ScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%v", fmt.Sprintf("%s-%d", testName, i), Inst().InstanceID)

				context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
					AppKeys:            Inst().AppList,
					StorageProvisioner: Inst().Provisioner,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(context).NotTo(BeEmpty())
				contexts = append(contexts, context...)
			}
		})

		Step("wait until all apps create volumes", func() {
			for _, ctx := range contexts {
				ValidateVolumes(ctx)
			}
		})

		Step("apply autopilot rules for storage pools", func() {
			err := Inst().V.CreateAutopilotRules(apRules)
			Expect(err).NotTo(HaveOccurred())
		})

		Step("wait until workload completes on volume", func() {
			for _, ctx := range contexts {
				err = Inst().S.WaitForRunning(ctx, workloadTimeout, retryInterval)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		Step("validating and verifying size of storage pools", func() {
			ValidateStoragePools(contexts)
		})

		Step(fmt.Sprintf("wait for unscheduled resize of storage pool (%s)", unscheduledResizeTimeout), func() {
			time.Sleep(unscheduledResizeTimeout)
		})

		Step("validating and verifying size of storage pools", func() {
			ValidateStoragePools(contexts)
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
