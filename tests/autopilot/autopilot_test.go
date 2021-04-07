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
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/pkg/aututils"
	"github.com/portworx/torpedo/pkg/units"
	. "github.com/portworx/torpedo/tests"
	"github.com/sirupsen/logrus"
	appsapi "k8s.io/api/apps/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	testSuiteName            = "AUT"
	workloadTimeout          = 5 * time.Hour
	retryInterval            = 30 * time.Second
	unscheduledResizeTimeout = 10 * time.Minute
	triggerCheckInterval     = 2 * time.Second
	triggerCheckTimeout      = 5 * time.Minute
	autDeploymentName        = "autopilot"
	autDeploymentNamespace   = "kube-system"
)

var autopilotruleBasicTestCases = []apapi.AutopilotRule{
	aututils.PVCRuleByUsageCapacity(50, 50, ""),
	aututils.PVCRuleByUsageCapacity(90, 50, ""),
	aututils.PVCRuleByUsageCapacity(50, 50, "20Gi"),
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
		var contexts []*scheduler.Context
		testName := strings.ToLower(fmt.Sprintf("%sPvcBasic", testSuiteName))

		Step("schedule applications", func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
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
				err := Inst().S.WaitForRunning(ctx, workloadTimeout, retryInterval)
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
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
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
		var contexts []*scheduler.Context

		testName := strings.ToLower(fmt.Sprintf("%sRestartAutopilot", testSuiteName))
		poolLabel := map[string]string{"autopilot": "adddisk"}
		storageNodes := node.GetStorageDriverNodes()
		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleByTotalSize((getTotalPoolSize(storageNodes[0])/units.GiB)+1, 10, aututils.RuleScaleTypeAddDisk, poolLabel),
		}

		// setup task to delete autopilot pods as soon as it starts doing expansions
		eventCheck := func() (bool, error) {
			for _, apRule := range apRules {
				ruleEvents, err := core.Instance().ListEvents("", meta_v1.ListOptions{
					FieldSelector: fmt.Sprintf("involvedObject.kind=AutopilotRule,involvedObject.name=%s", apRule.Name),
				})
				Expect(err).NotTo(HaveOccurred())

				for _, ruleEvent := range ruleEvents.Items {
					if strings.Contains(ruleEvent.Message, aututils.ActiveActionsPendingToActiveActionsInProgress) {
						return true, nil
					}
				}
			}

			return false, nil
		}

		deleteOpts := &scheduler.DeleteTasksOptions{
			TriggerOptions: api.TriggerOptions{
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

		Step("schedule apps with autopilot rules", func() {
			err := AddLabelsOnNode(storageNodes[0], poolLabel)
			Expect(err).NotTo(HaveOccurred())
			contexts = scheduleAppsWithAutopilot(testName, 1, apRules, scheduler.ScheduleOptions{})
		})

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
			for _, apRule := range apRules {
				Inst().S.DeleteAutopilotRule(apRule.Name)
			}
			for _, storageNode := range storageNodes {
				for k := range poolLabel {
					Inst().S.RemoveLabelOnNode(storageNode, k)
				}
			}
		})
	})
})

// This test is used for performing upgrade autopilot when autopilot rules in ActionInProgress state
var _ = Describe(fmt.Sprintf("{%sUpgradeAutopilot}", testSuiteName), func() {
	It("has to start IO workloads, create rules that resize pools based on capacity, upgrade autopilot and validate pools have been resized once", func() {
		var err error
		var contexts []*scheduler.Context

		if Inst().AutopilotUpgradeImage == "" {
			err = fmt.Errorf("no image supplied for upgrading autopilot")
		}
		Expect(err).NotTo(HaveOccurred())

		testName := strings.ToLower(fmt.Sprintf("%sUpgradeAutopilot", testSuiteName))
		poolLabel := map[string]string{"autopilot": "adddisk"}
		storageNodes := node.GetStorageDriverNodes()
		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleByTotalSize((getTotalPoolSize(storageNodes[0])/units.GiB)+1, 10, aututils.RuleScaleTypeAddDisk, poolLabel),
		}

		// setup task to upgrade autopilot pod as soon as it starts doing expansions
		eventCheck := func() (bool, error) {
			for _, apRule := range apRules {
				ruleEvents, err := core.Instance().ListEvents("", meta_v1.ListOptions{
					FieldSelector: fmt.Sprintf("involvedObject.kind=AutopilotRule,involvedObject.name=%s", apRule.Name),
				})
				Expect(err).NotTo(HaveOccurred())

				for _, ruleEvent := range ruleEvents.Items {
					if strings.Contains(ruleEvent.Message, aututils.ActiveActionsPendingToActiveActionsInProgress) {
						return true, nil
					}
				}
			}

			return false, nil
		}

		upgradeOpts := &scheduler.UpgradeAutopilotOptions{
			TriggerOptions: api.TriggerOptions{
				TriggerCb:            eventCheck,
				TriggerCheckInterval: triggerCheckInterval,
				TriggerCheckTimeout:  triggerCheckTimeout,
			},
		}

		t := func(interval sched.Interval) {
			err := upgradeAutopilot(Inst().AutopilotUpgradeImage, upgradeOpts)
			Expect(err).NotTo(HaveOccurred())
		}

		id, err := sched.Instance().Schedule(t, sched.Periodic(time.Second), time.Now(), true)
		Expect(err).NotTo(HaveOccurred())

		defer sched.Instance().Cancel(id)

		Step("schedule apps with autopilot rules", func() {
			err := AddLabelsOnNode(storageNodes[0], poolLabel)
			Expect(err).NotTo(HaveOccurred())
			contexts = scheduleAppsWithAutopilot(testName, 1, apRules, scheduler.ScheduleOptions{})
		})

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
			for _, apRule := range apRules {
				Inst().S.DeleteAutopilotRule(apRule.Name)
			}
			for _, storageNode := range storageNodes {
				for k := range poolLabel {
					Inst().S.RemoveLabelOnNode(storageNode, k)
				}
			}
		})
	})
})

// This testsuite is used for performing basic scenarios with Autopilot rules where it
// schedules apps and wait until workload is completed on the volumes and then validates
// sizes of storage pools by adding new disks to the nodes where volumes reside
var _ = Describe(fmt.Sprintf("{%sPoolExpand}", testSuiteName), func() {
	It("has to fill up the volume completely, resize the storage pool(s), validate and teardown apps", func() {
		var contexts []*scheduler.Context
		testName := strings.ToLower(fmt.Sprintf("%sPoolExpandAddDisk", testSuiteName))

		type testCase struct {
			workerNode    node.Node
			labelSelector map[string]string
			apRule        apapi.AutopilotRule
		}

		poolExpandLabels := map[string]map[string]string{
			"addDiskLabel":          map[string]string{"autopilot": "adddisk"},
			"addDiskFixedSizeLabel": map[string]string{"autopilot": "adddiskfixedsize"},
			"resizeDiskLabel":       map[string]string{"autopilot": "resizedisk"},
			"resizeFixedSizeLabel":  map[string]string{"autopilot": "resizefixedsize"},
		}

		storageNodes := node.GetStorageDriverNodes()

		testCases := []testCase{
			{
				workerNode:    storageNodes[0],
				labelSelector: poolExpandLabels["addDiskLabel"],
				apRule: aututils.PoolRuleByTotalSize((getTotalPoolSize(storageNodes[0])*120/100)/units.GiB, 50,
					aututils.RuleScaleTypeAddDisk, poolExpandLabels["addDiskLabel"]),
			},
			{
				workerNode:    storageNodes[1],
				labelSelector: poolExpandLabels["addDiskFixedSizeLabel"],
				apRule: aututils.PoolRuleFixedScaleSizeByTotalSize((getTotalPoolSize(storageNodes[1])*120/100)/units.GiB, "16Gi",
					aututils.RuleScaleTypeAddDisk, poolExpandLabels["addDiskFixedSizeLabel"]),
			},
			{
				workerNode:    storageNodes[2],
				labelSelector: poolExpandLabels["resizeDiskLabel"],
				apRule: aututils.PoolRuleByTotalSize((getTotalPoolSize(storageNodes[2])*120/100)/units.GiB, 50,
					aututils.RuleScaleTypeResizeDisk, poolExpandLabels["resizeDiskLabel"]),
			},
			{
				workerNode:    storageNodes[3],
				labelSelector: poolExpandLabels["resizeFixedSizeLabel"],
				apRule: aututils.PoolRuleFixedScaleSizeByTotalSize((getTotalPoolSize(storageNodes[3])*120/100)/units.GiB, "32Gi",
					aututils.RuleScaleTypeResizeDisk, poolExpandLabels["resizeFixedSizeLabel"]),
			},
		}

		// check if we have enough storage drive nodes to be able to run all test cases
		Expect(len(testCases)).Should(BeNumerically("<=", len(storageNodes)))

		var apRules []apapi.AutopilotRule
		Step("schedule apps with autopilot rules", func() {
			// adding labels to worker nodes
			for _, tc := range testCases {
				err := AddLabelsOnNode(tc.workerNode, tc.labelSelector)
				Expect(err).NotTo(HaveOccurred())
				apRules = append(apRules, tc.apRule)
			}
		})

		Step("schedule apps with autopilot rules", func() {
			contexts = scheduleAppsWithAutopilot(testName, 2, apRules, scheduler.ScheduleOptions{})
		})

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
			for _, apRule := range apRules {
				Inst().S.DeleteAutopilotRule(apRule.Name)
			}
			for _, storageNode := range storageNodes {
				for key := range poolExpandLabels {
					for k := range poolExpandLabels[key] {
						Inst().S.RemoveLabelOnNode(storageNode, k)
					}
				}
			}
		})

	})
})

// Restart Volume driver during resize pool with add-disk option
var _ = Describe(fmt.Sprintf("{%sPoolExpandRestartVolumeDriver}", testSuiteName), func() {
	It("has to restart portworx during resize pool with add-disk , validate and teardown apps", func() {
		var contexts []*scheduler.Context
		testName := strings.ToLower(fmt.Sprintf("%sPoolExpandRestartVolDriver", testSuiteName))
		poolLabel := map[string]string{"autopilot": "adddisk"}
		storageNode := node.GetStorageDriverNodes()[2]
		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleByTotalSize((getTotalPoolSize(storageNode)/units.GiB)+1, 10, aututils.RuleScaleTypeAddDisk, poolLabel),
		}

		Step("schedule apps with autopilot rules for pool expand", func() {
			err := AddLabelsOnNode(storageNode, poolLabel)
			Expect(err).NotTo(HaveOccurred())
			contexts = scheduleAppsWithAutopilot(testName, 1, apRules, scheduler.ScheduleOptions{PvcSize: 20 * units.GiB})
		})

		Step("restart Volume driver when resize of pool is triggered", func() {
			err := aututils.WaitForAutopilotEvent(apRules[0], "", []string{aututils.AnyToTriggeredEvent})
			Expect(err).NotTo(HaveOccurred())
			err = aututils.WaitForAutopilotEvent(apRules[0], "", []string{aututils.ActiveActionsPendingToActiveActionsInProgress})
			Expect(err).NotTo(HaveOccurred())
			err = Inst().V.RestartDriver(storageNode, nil)
			Expect(err).NotTo(HaveOccurred())
			err = Inst().V.WaitDriverDownOnNode(storageNode)
			Expect(err).NotTo(HaveOccurred())
			err = Inst().V.WaitDriverUpOnNode(storageNode, Inst().DriverStartTimeout)
			Expect(err).NotTo(HaveOccurred())
		})

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
			for _, apRule := range apRules {
				Inst().S.DeleteAutopilotRule(apRule.Name)
			}
			for key := range poolLabel {
				Inst().S.RemoveLabelOnNode(storageNode, key)
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
		poolLabel := map[string]string{"autopilot": "adddisk"}
		pvcLabel := map[string]string{"autopilot": "pvc-expand"}
		storageNodes := node.GetStorageDriverNodes()
		pvcApRules := []apapi.AutopilotRule{
			//aututils.PVCRuleByUsageCapacity(50, 50, ""),
			aututils.PVCRuleByTotalSize(10, 100, ""),
		}
		poolApRules := []apapi.AutopilotRule{
			aututils.PoolRuleByTotalSize((getTotalPoolSize(storageNodes[0])/units.GiB)+1, 10, aututils.RuleScaleTypeAddDisk, poolLabel),
		}

		Step("schedule apps with autopilot rules for pool expand", func() {
			err := AddLabelsOnNode(storageNodes[0], poolLabel)
			Expect(err).NotTo(HaveOccurred())
			contexts = scheduleAppsWithAutopilot(testName, 1, poolApRules, scheduler.ScheduleOptions{PvcSize: 20 * units.GiB})
		})

		Step("schedule applications for PVC expand", func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				for id, apRule := range pvcApRules {
					taskName := fmt.Sprintf("%s-%d-aprule%d", testName, i, id)
					apRule.Name = fmt.Sprintf("%s-%d", apRule.Name, i)
					apRule.Spec.ActionsCoolDownPeriod = int64(60)
					context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
						AppKeys:            Inst().AppList,
						StorageProvisioner: Inst().Provisioner,
						AutopilotRule:      apRule,
						Labels:             pvcLabel,
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(context).NotTo(BeEmpty())
					contexts = append(contexts, context...)
				}
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
			for _, apRule := range poolApRules {
				Inst().S.DeleteAutopilotRule(apRule.Name)
			}
			for _, apRule := range pvcApRules {
				Inst().S.DeleteAutopilotRule(apRule.Name)
			}
			for k := range poolLabel {
				Inst().S.RemoveLabelOnNode(storageNodes[0], k)
			}
		})
	})
})

var _ = Describe(fmt.Sprintf("{%sEvents}", testSuiteName), func() {
	It("has to fill up the volume completely, resize the volumes, validate events and teardown apps", func() {
		var contexts []*scheduler.Context
		testName := strings.ToLower(fmt.Sprintf("%sEvents", testSuiteName))
		apRules := []apapi.AutopilotRule{
			aututils.PVCRuleByTotalSize(11, 100, ""),
		}
		pvcLabel := map[string]string{"autopilot": "pvc-events"}
		volumeSize := int64(5368709120)

		Step("schedule applications for PVC events", func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				for id, apRule := range apRules {
					taskName := fmt.Sprintf("%s-%d-aprule%d", testName, i, id)
					apRule.Name = fmt.Sprintf("%s-%d", apRule.Name, i)
					apRule.Spec.ActionsCoolDownPeriod = int64(60)
					context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
						AppKeys:            Inst().AppList,
						StorageProvisioner: Inst().Provisioner,
						AutopilotRule:      apRule,
						Labels:             pvcLabel,
						PvcSize:            volumeSize,
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(context).NotTo(BeEmpty())
					contexts = append(contexts, context...)
				}
			}
		})

		Step("wait until workload completes on volume", func() {
			for _, ctx := range contexts {
				err := Inst().S.WaitForRunning(ctx, workloadTimeout, retryInterval)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		Step("validating autopilot events", func() {
			for _, ctx := range contexts {
				ValidateVolumes(ctx)
				err := Inst().S.ValidateAutopilotEvents(ctx)
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
		var contexts []*scheduler.Context

		testName := strings.ToLower(fmt.Sprintf("%sPoolResizeFailure", testSuiteName))
		poolLabel := map[string]string{"autopilot": "adddisk"}
		storageNodes := node.GetStorageDriverNodes()
		// below rule will lead to the failure of resizing the pool due to large scale
		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleByTotalSize((getTotalPoolSize(storageNodes[0])/units.GiB)+1, 100*16, aututils.RuleScaleTypeAddDisk, poolLabel),
		}

		Step("schedule apps with autopilot rules for pool expand", func() {
			err := AddLabelsOnNode(storageNodes[0], poolLabel)
			Expect(err).NotTo(HaveOccurred())
			contexts = scheduleAppsWithAutopilot(testName, 1, apRules, scheduler.ScheduleOptions{PvcSize: 20 * units.GiB})
		})

		Step("wait for failed autopilot event", func() {
			err := aututils.WaitForAutopilotEvent(apRules[0], "", []string{aututils.ActiveActionsPendingToActiveActionsInProgress})
			Expect(err).NotTo(HaveOccurred())

			err = aututils.WaitForAutopilotEvent(apRules[0], "FailedAction", []string{aututils.FailedToExecuteActionEvent})
			Expect(err).NotTo(HaveOccurred())
		})

		Step("updating autopilot rules with correct values", func() {
			aRule, err := Inst().S.GetAutopilotRule(apRules[0].Name)
			Expect(err).NotTo(HaveOccurred())
			for i := range aRule.Spec.Actions {
				aRule.Spec.Actions[i].Params[aututils.RuleActionsScalePercentage] = "50"
				_, err := Inst().S.UpdateAutopilotRule(aRule)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		Step("wait for autopilot to trigger an action after update an autopilot rule", func() {
			err := aututils.WaitForAutopilotEvent(apRules[0], "", []string{aututils.ActiveActionsInProgressToActiveActionsPending})
			Expect(err).NotTo(HaveOccurred())

			err = aututils.WaitForAutopilotEvent(apRules[0], "", []string{aututils.ActiveActionsPendingToActiveActionsInProgress})
			Expect(err).NotTo(HaveOccurred())

			err = aututils.WaitForAutopilotEvent(apRules[0], "", []string{aututils.ActiveActionsInProgressToActiveActionsTaken})
			Expect(err).NotTo(HaveOccurred())
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
			for _, apRule := range apRules {
				Inst().S.DeleteAutopilotRule(apRule.Name)
			}
			for k := range poolLabel {
				Inst().S.RemoveLabelOnNode(storageNodes[0], k)
			}
		})
	})
})

var _ = Describe(fmt.Sprintf("{%sRebalanceProvMean}", testSuiteName), func() {
	It("has to create couple volumes on the same pool, run rebalance, validate rebalance and teardown apps", func() {
		var contexts []*scheduler.Context
		testName := strings.ToLower(fmt.Sprintf("%sRebalanceProvMean", testSuiteName))
		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleRebalanceByProvisionedMean([]string{"-20", "20"}, false),
		}
		for i := range apRules {
			apRules[i].Spec.ActionsCoolDownPeriod = int64(60)
		}

		// pick up the first storage node and schedule all volumes to the node's pool
		storageNode := node.GetStorageDriverNodes()[0]
		numberOfVolumes := 3
		// 0.35 value is the 35% of total provisioned size which will trigger rebalance for above autopilot rule
		volumeSize := getVolumeSizeByProvisionedPercentage(storageNode, numberOfVolumes, 0.35)

		Step("schedule apps with autopilot rules", func() {
			contexts = scheduleAppsWithAutopilot(testName, numberOfVolumes, apRules,
				scheduler.ScheduleOptions{
					PvcNodesAnnotation: []string{storageNode.Id},
					PvcSize:            volumeSize,
				},
			)
		})

		Step("validating rebalance jobs", func() {
			for _, apRule := range apRules {
				err := aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.AnyToTriggeredEvent})
				Expect(err).NotTo(HaveOccurred())

				err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActiveActionsPendingToActiveActionsInProgress})
				Expect(err).NotTo(HaveOccurred())

				err = Inst().V.ValidateRebalanceJobs()
				Expect(err).NotTo(HaveOccurred())

				err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActiveActionTakenToNormalEvent})
				Expect(err).NotTo(HaveOccurred())

				err = Inst().S.ValidateAutopilotRuleObjects()
				Expect(err).NotTo(HaveOccurred())

			}
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
			for _, apRule := range apRules {
				Inst().S.DeleteAutopilotRule(apRule.Name)
			}
		})
	})
})

var _ = Describe(fmt.Sprintf("{%sRebalanceUsageMean}", testSuiteName), func() {
	It("has to create couple volumes on the same pool, run rebalance, validate rebalance and teardown apps", func() {
		var contexts []*scheduler.Context
		testName := strings.ToLower(fmt.Sprintf("%sRebalanceUsageMean", testSuiteName))
		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleRebalanceByUsageMean([]string{"-25", "25"}, false),
		}
		for i := range apRules {
			apRules[i].Spec.ActionsCoolDownPeriod = int64(60)
		}

		storageNode := node.GetStorageDriverNodes()[0]
		numberOfVolumes := 3

		Step("schedule apps with autopilot rules", func() {
			contexts = scheduleAppsWithAutopilot(testName, numberOfVolumes, apRules,
				scheduler.ScheduleOptions{PvcNodesAnnotation: []string{storageNode.Id}, PvcSize: 10737418240})
		})

		Step("validating rebalance jobs", func() {
			for _, apRule := range apRules {
				err := aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.NormalToTriggeredEvent})
				Expect(err).NotTo(HaveOccurred())

				err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActiveActionsPendingToActiveActionsInProgress})
				Expect(err).NotTo(HaveOccurred())

				err = Inst().V.ValidateRebalanceJobs()
				Expect(err).NotTo(HaveOccurred())

				err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActiveActionTakenToNormalEvent})
				Expect(err).NotTo(HaveOccurred())

				err = Inst().S.ValidateAutopilotRuleObjects()
				Expect(err).NotTo(HaveOccurred())

			}
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
			for _, apRule := range apRules {
				Inst().S.DeleteAutopilotRule(apRule.Name)
			}
		})
	})
})

var _ = Describe(fmt.Sprintf("{%sRestartAutopilotRebalance}", testSuiteName), func() {
	It("has to start IO workloads, create rules that rebalance pools, restart autopilot and validate pools have been rebalanced", func() {
		var contexts []*scheduler.Context
		testName := strings.ToLower(fmt.Sprintf("%sRestartAutopilotRebalance", testSuiteName))
		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleRebalanceByProvisionedMean([]string{"-20", "20"}, false),
		}
		for i := range apRules {
			apRules[i].Spec.ActionsCoolDownPeriod = int64(60)
		}

		// setup task to delete autopilot pods as soon as an action in progress
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
			TriggerOptions: api.TriggerOptions{
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

		storageNode := node.GetStorageDriverNodes()[0]
		numberOfVolumes := 3
		// 0.35 value is the 35% of total provisioned size which will trigger rebalance for above autopilot rule
		volumeSize := getVolumeSizeByProvisionedPercentage(storageNode, numberOfVolumes, 0.35)

		Step("schedule apps with autopilot rules", func() {
			contexts = scheduleAppsWithAutopilot(testName, numberOfVolumes, apRules,
				scheduler.ScheduleOptions{
					PvcNodesAnnotation: []string{storageNode.Id},
					PvcSize:            volumeSize,
				},
			)
		})

		Step("validating rebalance jobs", func() {
			err = Inst().V.ValidateRebalanceJobs()
			Expect(err).NotTo(HaveOccurred())

			err = aututils.WaitForAutopilotEvent(apRules[0], "", []string{aututils.ActiveActionTakenToNormalEvent})
			Expect(err).NotTo(HaveOccurred())

			err = Inst().S.ValidateAutopilotRuleObjects()
			Expect(err).NotTo(HaveOccurred())

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
			for _, apRule := range apRules {
				Inst().S.DeleteAutopilotRule(apRule.Name)
			}
		})
	})
})

var _ = Describe(fmt.Sprintf("{%sRebalanceProvMeanAndPvc}", testSuiteName), func() {
	It("has to run rebalance and resize PVC at the same time, validate rebalance, PVC sizes and teardown apps", func() {
		var contexts []*scheduler.Context
		testName := strings.ToLower(fmt.Sprintf("%sRebalanceProvsMeanAndPvc", testSuiteName))

		rebalanceApRules := []apapi.AutopilotRule{
			aututils.PoolRuleRebalanceByProvisionedMean([]string{"-50", "20"}, false),
		}

		pvcApRules := []apapi.AutopilotRule{
			aututils.PVCRuleByTotalSize(10, 50, ""),
		}

		for i := range rebalanceApRules {
			rebalanceApRules[i].Spec.ActionsCoolDownPeriod = int64(60)
		}

		workerNode := node.GetWorkerNodes()[0]
		numberOfVolumes := 3
		// 0.35 value is the 35% of total provisioned size which will trigger rebalance for above autopilot rule
		volumeSize := getVolumeSizeByProvisionedPercentage(workerNode, numberOfVolumes, 0.35)

		Step("schedule apps with autopilot rules for ppol expand", func() {
			contexts = scheduleAppsWithAutopilot(testName, numberOfVolumes, rebalanceApRules,
				scheduler.ScheduleOptions{
					PvcNodesAnnotation: []string{workerNode.Id},
					PvcSize:            volumeSize,
				},
			)
		})

		Step("schedule applications for PVC expand", func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
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
						PvcNodesAnnotation: []string{workerNode.Id},
						PvcSize:            7516192768, // 7Gb
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(context).NotTo(BeEmpty())
					contexts = append(contexts, context...)
				}
			}
		})

		Step("validate rebalance jobs", func() {
			for _, apRule := range rebalanceApRules {

				err := aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.NormalToTriggeredEvent})
				Expect(err).NotTo(HaveOccurred())

				err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActiveActionsPendingToActiveActionsInProgress})
				Expect(err).NotTo(HaveOccurred())

				err = Inst().V.ValidateRebalanceJobs()
				Expect(err).NotTo(HaveOccurred())

				err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActiveActionTakenToNormalEvent})
				Expect(err).NotTo(HaveOccurred())

				err = Inst().S.ValidateAutopilotRuleObjects()
				Expect(err).NotTo(HaveOccurred())

			}
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
			for _, apRule := range rebalanceApRules {
				Inst().S.DeleteAutopilotRule(apRule.Name)
			}
			for _, apRule := range pvcApRules {
				Inst().S.DeleteAutopilotRule(apRule.Name)
			}
		})
	})
})

// This testsuite is used for performing rebalance scenarios and pool resize with Autopilot rules where it
// schedules apps on one of the node, waits until workload is completed on the volumes and then validates
// rebalalnce and sizes of storage pools
// NOTE: this test is using volumes with replicaset is 3 and make sure that you have at least 4 nodes to do rebalance
var _ = Describe(fmt.Sprintf("{%sRebalanceProvMeanAndPoolResize}", testSuiteName), func() {
	It("has to run rebalance and resize pools, validate rebalance, validate pools and teardown apps", func() {
		var contexts []*scheduler.Context
		testName := strings.ToLower(fmt.Sprintf("%srebalance", testSuiteName))
		poolLabel := map[string]string{"autopilot": "adddisk"}
		storageNodes := node.GetStorageDriverNodes()
		// check if we have enough storage nodes to run the test
		Expect(len(storageNodes)).Should(BeNumerically(">=", 4))

		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleRebalanceByProvisionedMean([]string{"-50", "20"}, false),
			aututils.PoolRuleByTotalSize((getTotalPoolSize(storageNodes[0])*120/100)/units.GiB, 50, aututils.RuleScaleTypeAddDisk, poolLabel),
			aututils.PoolRuleByTotalSize((getTotalPoolSize(storageNodes[1])*120/100)/units.GiB, 50, aututils.RuleScaleTypeAddDisk, poolLabel),
			aututils.PoolRuleByTotalSize((getTotalPoolSize(storageNodes[2])*120/100)/units.GiB, 50, aututils.RuleScaleTypeAddDisk, poolLabel),
		}

		for i := range apRules {
			apRules[i].Spec.ActionsCoolDownPeriod = int64(60)
		}

		storageNodeIds := []string{}
		// take first 3 (default replicaset for volumes is 3) storage node IDs, label and schedule volumes onto them
		for _, n := range storageNodes[0:3] {
			for k, v := range poolLabel {
				Inst().S.AddLabelOnNode(n, k, v)
			}
			storageNodeIds = append(storageNodeIds, n.Id)
		}

		numberOfVolumes := 3
		// 0.35 value is the 35% of total provisioned size which will trigger rebalance for above autopilot rule
		volumeSize := getVolumeSizeByProvisionedPercentage(storageNodes[0], numberOfVolumes, 0.35)

		Step("schedule apps with autopilot rules", func() {
			contexts = scheduleAppsWithAutopilot(testName, numberOfVolumes, apRules,
				scheduler.ScheduleOptions{PvcNodesAnnotation: storageNodeIds, PvcSize: volumeSize})
		})

		Step("validate rebalance jobs", func() {
			apRule := apRules[0]

			err := aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.NormalToTriggeredEvent})
			Expect(err).NotTo(HaveOccurred())

			err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActiveActionsPendingToActiveActionsInProgress})
			Expect(err).NotTo(HaveOccurred())

			err = Inst().V.ValidateRebalanceJobs()
			Expect(err).NotTo(HaveOccurred())

			err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActiveActionTakenToNormalEvent})
			Expect(err).NotTo(HaveOccurred())

			err = Inst().S.ValidateAutopilotRuleObjects()
			Expect(err).NotTo(HaveOccurred())

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
			for _, apRule := range apRules {
				Inst().S.DeleteAutopilotRule(apRule.Name)
			}
			for _, storageNode := range storageNodes {
				for key := range poolLabel {
					Inst().S.RemoveLabelOnNode(storageNode, key)
				}
			}
		})
	})
})

var _ = Describe(fmt.Sprintf("{%sRebalanceUpdateDelete}", testSuiteName), func() {
	It("has to create couple volumes on the same pool, update and delete autopilot rule, run rebalance, validate rebalance and teardown apps", func() {
		testName := strings.ToLower(fmt.Sprintf("%sRebalanceUpdateDelete", testSuiteName))
		var contexts []*scheduler.Context
		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleRebalanceByProvisionedMean([]string{"-50", "50"}, false),
		}
		for i := range apRules {
			apRules[i].Spec.ActionsCoolDownPeriod = int64(60)
		}

		Step("schedule applications for pool rebalance", func() {
			workerNode := node.GetWorkerNodes()[0]
			numberOfVolumes := 3
			// 0.35 value is the 35% of total provisioned size which will trigger rebalance for above autopilot rule
			volumeSize := getVolumeSizeByProvisionedPercentage(workerNode, numberOfVolumes, 0.35)

			contexts = scheduleAppsWithAutopilot(testName, numberOfVolumes, apRules,
				scheduler.ScheduleOptions{
					PvcNodesAnnotation: []string{workerNode.Id},
					PvcSize:            volumeSize,
				},
			)
		})

		Step("updating rebalance autopilot rule with new values", func() {
			// need to wait unless autopilot rules will be created before update
			time.Sleep(2 * time.Second)
			for _, apRule := range apRules {
				aRule, err := Inst().S.GetAutopilotRule(apRule.Name)
				Expect(err).NotTo(HaveOccurred())
				for i := range aRule.Spec.Conditions.Expressions {
					aRule.Spec.Conditions.Expressions[i].Values = []string{"-20", "20"}
					_, err := Inst().S.UpdateAutopilotRule(aRule)
					Expect(err).NotTo(HaveOccurred())
				}
			}
		})

		Step("deleting rebalance autopilot rule", func() {
			err := aututils.WaitForAutopilotEvent(apRules[0], "", []string{aututils.NormalToTriggeredEvent})
			Expect(err).NotTo(HaveOccurred())

			Inst().S.DeleteAutopilotRule(apRules[0].Name)

			time.Sleep(2 * time.Second)

			apRules, err := Inst().S.ListAutopilotRules()
			Expect(err).NotTo(HaveOccurred())
			Expect(apRules.Items).To(BeEmpty())
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

var _ = Describe(fmt.Sprintf("{%sRebalanceWithApproval}", testSuiteName), func() {
	It("has to run rebalance and wait for approval, approve the rule, run rebalance, validate rebalance and teardown apps", func() {
		testName := strings.ToLower(fmt.Sprintf("%srebalance", testSuiteName))

		apRule := aututils.PoolRuleRebalanceByProvisionedMean([]string{"-50", "20"}, true)
		apRule.Spec.ActionsCoolDownPeriod = int64(10)

		workerNode := node.GetWorkerNodes()[0]
		numberOfVolumes := 3
		// 0.35 value is the 35% of total provisioned size which will trigger rebalance for above autopilot rule
		volumeSize := getVolumeSizeByProvisionedPercentage(workerNode, numberOfVolumes, 0.35)

		contexts := scheduleAppsWithAutopilot(testName, numberOfVolumes, []apapi.AutopilotRule{apRule},
			scheduler.ScheduleOptions{
				PvcNodesAnnotation: []string{workerNode.Id},
				PvcSize:            volumeSize,
			},
		)

		Step("decline and approve the actions for action approval objects", func() {

			// wait for event Normal or Initializing => Triggered (sometimes autopilot triggers from Initializing state)
			err := aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.AnyToTriggeredEvent})
			Expect(err).NotTo(HaveOccurred())

			// wait for event
			err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.TriggeredToActionAwaitingApprovalEvent})
			Expect(err).NotTo(HaveOccurred())

			err = aututils.WaitForActionApprovalsObjects(autDeploymentNamespace, "")
			Expect(err).NotTo(HaveOccurred())

			actionApprovalList, err := Inst().S.ListActionApprovals(autDeploymentNamespace)
			Expect(err).NotTo(HaveOccurred())
			Expect(actionApprovalList.Items).NotTo(BeEmpty())

			for _, actionApproval := range actionApprovalList.Items {
				Step("decline an action", func() {
					// decline an action
					aApproval, err := Inst().S.GetActionApproval(autDeploymentNamespace, actionApproval.Name)
					Expect(err).NotTo(HaveOccurred())
					aApproval.Spec.ApprovalState = apapi.ApprovalStateDeclined
					_, err = Inst().S.UpdateActionApproval(autDeploymentNamespace, aApproval)
					Expect(err).NotTo(HaveOccurred())

					// validate action approval os in declined state
					aApproval, err = Inst().S.GetActionApproval(autDeploymentNamespace, actionApproval.Name)
					Expect(err).NotTo(HaveOccurred())
					Expect(aApproval.Spec.ApprovalState).To(Equal(apapi.ApprovalStateDeclined))
				})
				Step("delete an action approval object", func() {
					// delete an action approval object
					err = Inst().S.DeleteActionApproval(autDeploymentNamespace, actionApproval.Name)
					Expect(err).NotTo(HaveOccurred())

					// wait for event ActionDeclined => Triggered
					err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActionDeclinedToTriggeredEvent})
					Expect(err).NotTo(HaveOccurred())

					// wait for event Triggered => ActionAwaitingApproval
					err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.TriggeredToActionAwaitingApprovalEvent})
					Expect(err).NotTo(HaveOccurred())
				})
			}

			err = aututils.WaitForActionApprovalsObjects(autDeploymentNamespace, "")
			Expect(err).NotTo(HaveOccurred())

			actionApprovalList, err = Inst().S.ListActionApprovals(autDeploymentNamespace)
			Expect(err).NotTo(HaveOccurred())
			for _, actionApproval := range actionApprovalList.Items {
				Step("approve an action", func() {
					// approve an action
					aApproval, err := Inst().S.GetActionApproval(autDeploymentNamespace, actionApproval.Name)
					Expect(err).NotTo(HaveOccurred())
					aApproval.Spec.ApprovalState = apapi.ApprovalStateApproved
					_, err = Inst().S.UpdateActionApproval(autDeploymentNamespace, aApproval)
					Expect(err).NotTo(HaveOccurred())

					// wait for event ActionAwaitingApproval => ActiveActionsPending
					err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActionAwaitingApprovalToActiveActionsPending})
					Expect(err).NotTo(HaveOccurred())

					// wait for event ActiveActionsPending => ActiveActionsInProgress
					err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActiveActionsPendingToActiveActionsInProgress})
					Expect(err).NotTo(HaveOccurred())
				})
				Step("validate rebalance jobs", func() {
					// validate rebalance jobs
					err = Inst().V.ValidateRebalanceJobs()
					Expect(err).NotTo(HaveOccurred())
				})
			}

			// wait for event Triggered => ActionAwaitingApproval
			err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActiveActionTakenToNormalEvent})
			Expect(err).NotTo(HaveOccurred())

			err = Inst().S.ValidateAutopilotRuleObjects()
			Expect(err).NotTo(HaveOccurred())

		})

		Step("destroy apps", func() {
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
			for _, ctx := range contexts {
				TearDownContext(ctx, opts)
			}
			Inst().S.DeleteAutopilotRule(apRule.Name)
		})

	})
})

func scheduleAppsWithAutopilot(testName string, testScaleFactor int, apRules []apapi.AutopilotRule, options scheduler.ScheduleOptions) []*scheduler.Context {
	var contexts []*scheduler.Context

	Step("schedule applications", func() {
		for iGsf := 0; iGsf < Inst().GlobalScaleFactor; iGsf++ {
			for iTsf := 0; iTsf < testScaleFactor; iTsf++ {
				taskName := fmt.Sprintf("%s-%v", fmt.Sprintf("%s-%d-%d", testName, iGsf, iTsf), Inst().InstanceID)

				context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
					AppKeys:            Inst().AppList,
					StorageProvisioner: Inst().Provisioner,
					PvcNodesAnnotation: options.PvcNodesAnnotation,
					PvcSize:            options.PvcSize,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(context).NotTo(BeEmpty())
				contexts = append(contexts, context...)
			}
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

func getTotalPoolSize(node node.Node) uint64 {
	// calculate total storage pools size on the given node
	var totalPoolSize uint64
	for _, p := range node.StoragePools {
		totalPoolSize += p.TotalSize
	}
	return totalPoolSize
}

func upgradeAutopilot(image string, opts *scheduler.UpgradeAutopilotOptions) error {

	upgradeAutopilot := func() error {
		k8sApps := apps.Instance()

		logrus.Infof("Upgrading autopilot with new image %s", image)
		autopilotObj, err := k8sApps.GetDeployment(autDeploymentName, autDeploymentNamespace)

		if err != nil {
			return fmt.Errorf("failed to get autopilot deployment object. Err: %v", err)
		}
		containers := autopilotObj.Spec.Template.Spec.Containers
		for i := range containers {
			containers[i].Image = image
		}
		upgradedAutopilotObj, err := k8sApps.UpdateDeployment(autopilotObj)
		if err != nil {
			return fmt.Errorf("failed to update autopilot version. Err: %v", err)
		}
		if err := k8sApps.ValidateDeployment(upgradedAutopilotObj, k8s.DefaultTimeout, k8s.DefaultRetryInterval); err != nil {
			return fmt.Errorf("failed to validate autopilot deployment %s. Err: %v", autopilotObj.Name, err)
		}

		for _, container := range upgradedAutopilotObj.Spec.Template.Spec.Containers {
			if container.Image != image {
				return fmt.Errorf("failed to upgrade autopilot. New version mismatch. Actual %s, Expected: %s", container.Image, image)
			}
		}
		logrus.Infof("autopilot with new image %s upgraded successfully", image)
		return nil
	}
	if opts == nil {
		return upgradeAutopilot()
	}

	return api.PerformTask(upgradeAutopilot, &opts.TriggerOptions)
}

func getVolumeSizeByProvisionedPercentage(n node.Node, numOfVolumes int, provPercentage float64) int64 {
	workerNodePoolTotalSize := getTotalPoolSize(n)
	return int64(float64(int(workerNodePoolTotalSize)/Inst().GlobalScaleFactor) * provPercentage / float64(numOfVolumes))
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
