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
	"github.com/portworx/torpedo/pkg/aututils"
	. "github.com/portworx/torpedo/tests"
)

var (
	testSuiteName            = "PoolExpand"
	workloadTimeout          = 5 * time.Hour
	retryInterval            = 30 * time.Second
	unscheduledResizeTimeout = 10 * time.Minute
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

var _ = Describe(fmt.Sprintf("{%sRestartAutopilot}", testSuiteName), func() {
	It("has to start IO workloads, create rules that resize pools based on capacity, restart autopilot and validate pools have been resized once", func() {
		testName := strings.ToLower(fmt.Sprintf("%sRestartAutopilot", testSuiteName))

		// TODO get pools, create a rule per pool so that all pools get expanded
		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleByTotalSize(31, 10, aututils.RuleScaleTypeAddDisk, nil),
		}
		contexts := scheduleAppsWithAutopilot(testName, apRules)

		// schedule deletion of autopilot once the pool expansion starts
		Step("wait until workload completes on volume", func() {
			for _, ctx := range contexts {
				err := Inst().S.WaitForRunning(ctx, workloadTimeout, retryInterval)
				Expect(err).NotTo(HaveOccurred())
			}
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
// sizes of storage pools by adding new disks to the nodes where volumes reside
var _ = Describe(fmt.Sprintf("{%sAddDisk}", testSuiteName), func() {
	It("has to fill up the volume completely, resize the storage pool(s), validate and teardown apps", func() {
		var err error
		testName := strings.ToLower(fmt.Sprintf("%sAddDisk", testSuiteName))
		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleByAvailableCapacity(70, 50, aututils.RuleScaleTypeAddDisk),
		}
		contexts := scheduleAppsWithAutopilot(testName, apRules)

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
var _ = Describe(fmt.Sprintf("{%sResizeDisk}", testSuiteName), func() {
	It("has to fill up the volume completely, resize the storage pool(s), validate and teardown apps", func() {
		var err error
		testName := strings.ToLower(fmt.Sprintf("%sResizeDisk", testSuiteName))
		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleByAvailableCapacity(70, 50, aututils.RuleScaleTypeResizeDisk),
		}

		contexts := scheduleAppsWithAutopilot(testName, apRules)
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

func scheduleAppsWithAutopilot(testName string, apRules []apapi.AutopilotRule) []*scheduler.Context {
	var contexts []*scheduler.Context
	labels := map[string]string{
		"autopilot": testName,
	}
	// adding labels to autopilot rule objects
	for idx := range apRules {
		apRules[idx].Spec.Selector.MatchLabels = labels
	}

	// adding labels to worker nodes
	workerNodes := node.GetWorkerNodes()
	for _, workerNode := range workerNodes {
		err := AddLabelsOnNode(workerNode, labels)
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

	Step("wait until all volumes are created", func() {
		for _, ctx := range contexts {
			ValidateVolumes(ctx)
		}
	})

	Step("apply autopilot rules for storage pools", func() {
		err := Inst().V.CreateAutopilotRules(apRules)
		Expect(err).NotTo(HaveOccurred())
	})

	return contexts
}

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
