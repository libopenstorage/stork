package tests

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/libopenstorage/openstorage/pkg/sched"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/pkg/aututils"
	"github.com/portworx/torpedo/pkg/units"
	. "github.com/portworx/torpedo/tests"
	appsapi "k8s.io/api/apps/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	testSuiteName            = "AUT"
	workloadTimeout          = 5 * time.Hour
	retryInterval            = 30 * time.Second
	unscheduledResizeTimeout = 10 * time.Minute
	triggerCheckInterval     = 2 * time.Second
	triggerCheckTimeout      = 30 * time.Minute
	eventCheckInterval       = 2 * time.Second
	eventCheckTimeout        = 30 * time.Minute
	autDeploymentName        = "autopilot"
	autDeploymentNamespace   = "kube-system"
)

var autopilotruleBasicTestCases = []apapi.AutopilotRule{
	aututils.PVCRuleByUsageCapacity(50, 50, ""),
	aututils.PVCRuleByUsageCapacity(90, 50, ""),
	aututils.PVCRuleByUsageCapacity(50, 50, "21474836480"),
	aututils.PVCRuleByUsageCapacity(50, 300, ""),
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

		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleByTotalSize((getTheSmallestPoolSize()/units.GiB)+1, 10, aututils.RuleScaleTypeAddDisk, nil),
		}

		// setup task to delete autopilot pods as soon as it starts doing expansions
		eventCheck := func() (bool, error) {
			for _, apRule := range apRules {
				ruleEvents, err := core.Instance().ListEvents("", meta_v1.ListOptions{
					FieldSelector: fmt.Sprintf("involvedObject.kind=AutopilotRule,involvedObject.name=%s", apRule.Name),
				})
				Expect(err).NotTo(HaveOccurred())

				for _, ruleEvent := range ruleEvents.Items {
					if strings.Contains(ruleEvent.Message, string(apapi.RuleStateActiveActionsInProgress)) {
						return true, nil
					}
				}
			}

			return false, nil
		}

		deleteOpts := &scheduler.DeleteTasksOptions{
			TriggerOptions: scheduler.TriggerOptions{
				TriggerCb:            eventCheck,
				TriggerCheckInterval: triggerCheckInterval,
				TriggerCheckTimeout:  triggerCheckTimeout,
			},
		}

		t := func(interval sched.Interval) {
			err := Inst().S.DeleteTasks(&scheduler.Context{
				App: &spec.AppSpec{
					SpecList: []interface{}{
						&appsapi.Deployment{
							ObjectMeta: meta_v1.ObjectMeta{
								Name:      autDeploymentName,
								Namespace: autDeploymentNamespace,
							},
						},
					},
				},
			}, deleteOpts)
			Expect(err).NotTo(HaveOccurred())
		}

		id, err := sched.Instance().Schedule(t, sched.Periodic(time.Second), time.Now(), true)
		Expect(err).NotTo(HaveOccurred())

		defer sched.Instance().Cancel(id)

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
		testName := strings.ToLower(fmt.Sprintf("%sPoolExpandResizeDisk", testSuiteName))
		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleByAvailableCapacity(70, 50, aututils.RuleScaleTypeResizeDisk),
		}

		contexts := scheduleAppsWithAutopilot(testName, apRules)
		Step("wait until workload completes on volume", func() {
			for _, ctx := range contexts {
				err := Inst().S.WaitForRunning(ctx, workloadTimeout, retryInterval)
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
			aututils.PVCRuleByUsageCapacity(50, 50, ""),
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
			for _, apRule := range poolApRules {
				_, err := Inst().S.CreateAutopilotRule(apRule)
				Expect(err).NotTo(HaveOccurred())
			}

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

var _ = Describe(fmt.Sprintf("{%sPvcExpand}", testSuiteName), func() {
	It("Starts resizing volumes, when the volume capacity is less than 5Gb", func() {
		var contexts []*scheduler.Context
		apRule := aututils.PVCRuleByTotalSize(5, 100, "15Gi")

		testName := strings.ToLower(fmt.Sprintf("%sPvcExpand", testSuiteName))
		apRule.Name = fmt.Sprintf("%s", apRule.Name)
		taskName := fmt.Sprintf("%s", fmt.Sprintf("%s-%s", testName, apRule.Name))
		labels := map[string]string{
			"autopilot": apRule.Name,
		}
		coolDownPeriod := 60
		apRule.Spec.ActionsCoolDownPeriod = int64(coolDownPeriod)
		context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
			AppKeys:            Inst().AppList,
			StorageProvisioner: Inst().Provisioner,
			AutopilotRule:      apRule,
			Labels:             labels,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(context).NotTo(BeEmpty())
		contexts = append(contexts, context...)

		Step("wait until workload completes on volume", func() {
			for _, ctx := range contexts {
				err := Inst().S.WaitForRunning(ctx, workloadTimeout, retryInterval)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		Step("validating volumes and verifying size of volumes", func() {
			for _, ctx := range contexts {
				ValidateVolumes(ctx)
				err = Inst().S.ValidateAutopilotEvents(ctx)
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

var _ = Describe(fmt.Sprintf("{%sPoolResizeFailure}", testSuiteName), func() {
	It("create rules with incorrect scale percentage value, wait for failed event, update incorrect value and validate pools", func() {
		testName := strings.ToLower(fmt.Sprintf("%sPoolResizeFailure", testSuiteName))

		// below rule will lead to the failure of resizing the pool due to large scale
		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleByTotalSize((getTheSmallestPoolSize()/units.GiB)+1, 100*16, aututils.RuleScaleTypeAddDisk, nil),
		}

		contexts := scheduleAppsWithAutopilot(testName, apRules)

		err := waitForAutopilotFailedEvent(apRules, "")
		Expect(err).NotTo(HaveOccurred())

		Step("updating autopilot rules with correct values", func() {
			autopilotRules, err := Inst().S.ListAutopilotRules()
			Expect(err).NotTo(HaveOccurred())
			Expect(autopilotRules.Items).NotTo(BeEmpty())
			for _, apRule := range autopilotRules.Items {
				for _, action := range apRule.Spec.Actions {
					action.Params[aututils.RuleActionsScalePercentage] = "50"
					_, err := Inst().S.UpdateAutopilotRule(apRule)
					Expect(err).NotTo(HaveOccurred())
				}

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
		for _, apRule := range apRules {
			_, err := Inst().S.CreateAutopilotRule(apRule)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	return contexts
}

func getTheSmallestPoolSize() uint64 {
	// find the smallest pool and create a rule with size cap above that
	var smallestPoolSize uint64
	for _, node := range node.GetWorkerNodes() {
		for _, p := range node.StoragePools {
			if smallestPoolSize == 0 {
				smallestPoolSize = p.TotalSize
			} else {
				if p.TotalSize < smallestPoolSize {
					smallestPoolSize = p.TotalSize
				}
			}
		}
	}
	return smallestPoolSize
}

func waitForAutopilotFailedEvent(apRules []apapi.AutopilotRule, objectName string) error {
	t := func() (interface{}, bool, error) {
		for _, apRule := range apRules {
			ruleEvents, err := core.Instance().ListEvents("", meta_v1.ListOptions{
				FieldSelector: fmt.Sprintf("involvedObject.kind=AutopilotRule,involvedObject.name=%s", apRule.Name),
			})

			if err != nil {
				return nil, false, err
			}
			for _, ruleEvent := range ruleEvents.Items {
				if strings.Contains(ruleEvent.Reason, "FailedAction") &&
					strings.Contains(ruleEvent.Message, "failed to execute Action for rule") &&
					strings.Contains(ruleEvent.Message, objectName) {
					return nil, false, nil
				}
			}
		}
		return nil, true, fmt.Errorf("autopilot rules has no failed events")
	}
	if _, err := task.DoRetryWithTimeout(t, eventCheckTimeout, eventCheckInterval); err != nil {
		return err
	}
	return nil
}

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
