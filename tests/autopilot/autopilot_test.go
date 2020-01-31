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
	testSuiteName            = "AUT"
	workloadTimeout          = 5 * time.Hour
	retryInterval            = 30 * time.Second
	unscheduledResizeTimeout = 10 * time.Minute
)

var autopilotruleBasicTestCases = []apapi.AutopilotRule{
	aututils.PvcRuleByUsageCapacity(50, 50, ""),
	aututils.PvcRuleByUsageCapacity(90, 50, ""),
	aututils.PvcRuleByUsageCapacity(50, 50, "21474836480"),
	aututils.PvcRuleByUsageCapacity(50, 300, ""),
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
var _ = Describe(fmt.Sprintf("{%sPvcBasic}", testSuiteName), func() {

	It("has to fill up the volume completely, resize the volume, validate and teardown apps", func() {
		var err error
		var contexts []*scheduler.Context
		testName := strings.ToLower(fmt.Sprintf("%sPvcBasic", testSuiteName))

		Step("schedule applications", func() {
			for i := 0; i < Inst().ScaleFactor; i++ {
				for id, apRule := range autopilotruleBasicTestCases {
					taskName := fmt.Sprintf("%s-%d-aprule%d", testName, i, id)
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
				ValidateVolumes(ctx)
			}
		})

		Step(fmt.Sprintf("wait for unscheduled resize of volume (%s)", unscheduledResizeTimeout), func() {
			time.Sleep(unscheduledResizeTimeout)
		})

		Step("validating volumes and verifying size of volumes", func() {
			for _, ctx := range contexts {
				ValidateVolumes(ctx)
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
var _ = Describe(fmt.Sprintf("{%sVolumeDriverDown}", testSuiteName), func() {

	It("has to fill up the volume completely, resize the volume, validate and teardown apps", func() {
		var err error
		var contexts []*scheduler.Context
		testName := strings.ToLower(fmt.Sprintf("%sVolumeDriverDown", testSuiteName))

		Step("schedule applications", func() {
			for i := 0; i < Inst().ScaleFactor; i++ {
				for id, apRule := range autopilotruleBasicTestCases {
					taskName := fmt.Sprintf("%s-%d-aprule%d", testName, i, id)
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
				ValidateVolumes(ctx)
			}
		})

		Step(fmt.Sprintf("wait for unscheduled resize of volume (%s)", unscheduledResizeTimeout), func() {
			time.Sleep(unscheduledResizeTimeout)
		})

		Step("validating volumes and verifying size of volumes", func() {
			for _, ctx := range contexts {
				ValidateVolumes(ctx)
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
var _ = Describe(fmt.Sprintf("{%sPoolExpandAddDisk}", testSuiteName), func() {
	It("has to fill up the volume completely, resize the storage pool(s), validate and teardown apps", func() {
		var err error
		testName := strings.ToLower(fmt.Sprintf("%sPoolExpandAddDisk", testSuiteName))
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
var _ = Describe(fmt.Sprintf("{%sPoolExpandResizeDisk}", testSuiteName), func() {
	It("has to fill up the volume completely, resize the storage pool(s), validate and teardown apps", func() {
		var err error
		testName := strings.ToLower(fmt.Sprintf("%sPoolExpandResizeDisk", testSuiteName))
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

// This testsuite is used for performing basic scenarios with Autopilot rules where it
// schedules apps and wait until workload is completed on the volumes and then validates
// PVC sizes of the volumes and sizes of storage pools
var _ = Describe(fmt.Sprintf("{%sPvcAndPoolExpand}", testSuiteName), func() {
	It("has to fill up the volume completely, resize the volumes and storage pool(s), validate and teardown apps", func() {
		var contexts []*scheduler.Context

		testName := strings.ToLower(fmt.Sprintf("%sPvcAndPoolExpand", testSuiteName))
		pvcApRules := []apapi.AutopilotRule{
			aututils.PvcRuleByUsageCapacity(50, 50, ""),
		}
		poolApRules := []apapi.AutopilotRule{
			aututils.PoolRuleByAvailableCapacity(70, 50, aututils.RuleScaleTypeResizeDisk),
		}
		storagePoolsLabels := map[string]string{
			"autopilot": testName,
		}
		// adding labels to autopilot rule objects
		for idx := range poolApRules {
			poolApRules[idx].Spec.Selector.MatchLabels = storagePoolsLabels
		}
		// adding labels to worker nodes
		for _, workerNode := range node.GetWorkerNodes() {
			err := AddLabelsOnNode(workerNode, storagePoolsLabels)
			Expect(err).NotTo(HaveOccurred())
		}

		Step("schedule applications", func() {
			for i := 0; i < Inst().ScaleFactor; i++ {
				for id, apRule := range pvcApRules {
					taskName := fmt.Sprintf("%s-%d-aprule%d", testName, i, id)
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
		Step("apply autopilot rules for storage pools", func() {
			err := Inst().V.CreateAutopilotRules(poolApRules)
			Expect(err).NotTo(HaveOccurred())
		})

		Step("wait until workload completes on volume", func() {
			for _, ctx := range contexts {
				err := Inst().S.WaitForRunning(ctx, workloadTimeout, retryInterval)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		Step("validating volumes and verifying size of volumes", func() {
			for _, ctx := range contexts {
				ValidateVolumes(ctx)
			}
		})

		Step("validate storage pools", func() {
			ValidateStoragePools(contexts)
		})

		Step(fmt.Sprintf("wait for unscheduled resize of volume (%s)", unscheduledResizeTimeout), func() {
			time.Sleep(unscheduledResizeTimeout)
		})

		Step("validating volumes and verifying size of volumes", func() {
			for _, ctx := range contexts {
				ValidateVolumes(ctx)
			}
		})

		Step("validate storage pools", func() {
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
