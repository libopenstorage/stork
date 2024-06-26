package tests

import (
	"fmt"
	"strings"
	"sync"
	"time"

	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	"github.com/libopenstorage/openstorage/pkg/sched"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/pkg/aututils"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/testrailuttils"
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
	triggerCheckTimeout      = 5 * time.Minute
	autDeploymentName        = "autopilot"
	autDeploymentNamespace   = "kube-system"
)

var autopilotruleBasicTestCases = []apapi.AutopilotRule{
	aututils.PVCRuleByUsageCapacity(50, 50, ""),
	aututils.PVCRuleByUsageCapacity(85, 50, ""),
	aututils.PVCRuleByUsageCapacity(50, 50, "20Gi"),
	aututils.PVCRuleByUsageCapacity(50, 300, ""),
}

var tags = map[string]string{
	"autopilot": "true",
}

// This testsuite is used for performing basic scenarios with Autopilot rules where it
// schedules apps and wait until workload is completed on the volumes and then validates
// PVC sizes of the volumes
var _ = Describe(fmt.Sprintf("{%sPvcBasic}", testSuiteName), func() {
	var testrailID = 85442
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/85442
	var runID int
	JustBeforeEach(func() {
		tags["volumeChange"] = "true"
		StartTorpedoTest(fmt.Sprintf("{%sPvcBasic}", testSuiteName), "Perform basic scenarios with Autopilot", tags, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context
	It("has to fill up the volume completely, resize the volume, validate and teardown apps", func() {
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
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var autopilotPVCRule = []apapi.AutopilotRule{
	aututils.PVCRuleByTotalSize(10, 100, "20Gi"),
}

// This testsuite is used applying autopilot rules on a detached volume and validates the size of PVC
var _ = Describe(fmt.Sprintf("{%sPVCVolDetached}", testSuiteName), func() {
	var testrailID = 93300
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/93300
	var runID int
	tags["volumeChange"] = "true"
	tags["negative"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sPVCVolDetached}", testSuiteName), "Perform basic scenarios with Autopilot with detached volume", tags, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context
	It("has to, resize the volume which is in detached state, validate and teardown apps", func() {
		testName := strings.ToLower(fmt.Sprintf("%sPVCVolDetached", testSuiteName))

		Step("schedule applications", func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				for id, apRule := range autopilotPVCRule {
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
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This testsuite is used applying autopilot rules on a detached volume and validates the size of PVC
var _ = Describe(fmt.Sprintf("{%sToggleAutopilot}", testSuiteName), func() {
	var testrailID = 93323
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/93323
	var runID int
	tags["volumeChange"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sToggleAutopilot}", testSuiteName), "Toggles the autopilot from STC", tags, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context
	itLog := "has to, toggle stc to disable autopilot, then enable it back"
	It(itLog, func() {
		log.InfoD(itLog)
		testName := strings.ToLower(fmt.Sprintf("%sToggleAutopilot", testSuiteName))
		stepLog := "schedule applications with autopilot label"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				for id, apRule := range autopilotPVCRule {
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
					log.FailOnError(err, "Failed to schedule applications")
					contexts = append(contexts, context...)
				}
			}
		})
		stepLog = "validating volumes and verifying size of volumes"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, ctx := range contexts {
				ValidateVolumes(ctx)
			}
		})
		stepLog = "Toggle autopilot in STC"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err := ToggleAutopilotInStc()
			dash.VerifyFatal(err, nil, "Failed to toggle autopilot in STC")
			time.Sleep(30 * time.Second)
		})
		stepLog = fmt.Sprintf("wait for unscheduled resize of volume (%s)", unscheduledResizeTimeout)
		Step(stepLog, func() {
			log.InfoD(stepLog)
			time.Sleep(unscheduledResizeTimeout)
		})
		stepLog = "Toggle autopilot in STC to enable the autopilot to true"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			err := ToggleAutopilotInStc()
			dash.VerifyFatal(err, nil, "Failed to toggle autopilot in STC")
			time.Sleep(30 * time.Second)
		})
		stepLog = "validating volumes and verifying size of volumes after enabling the autopilot to true"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, ctx := range contexts {
				ValidateVolumes(ctx)
			}
		})
		stepLog = "destroy apps"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
			for _, ctx := range contexts {
				TearDownContext(ctx, opts)
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This testsuite is for testing, if disabling prometheus in STC disables autopilot
var _ = Describe(fmt.Sprintf("{%sTogglePrometheus}", testSuiteName), func() {
	var testrailID = 93317
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/93317
	var runID int
	tags["volumeChange"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sTogglePrometheus}", testSuiteName), "Toggles prometheus from STC", tags, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context
	It("has to, toggle stc to disable prometheus, then enable it back", func() {
		testName := strings.ToLower(fmt.Sprintf("%sToggleAutopilot", testSuiteName))
		Step("Toggle prometheus", func() {
			err := TogglePrometheusInStc()
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(30 * time.Second)
		})
		Step("schedule applications", func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				for id, apRule := range autopilotPVCRule {
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

		Step("validating volumes and verifying size of volumes", func() {
			for _, ctx := range contexts {
				ValidateVolumes(ctx)
			}
		})

		Step(fmt.Sprintf("wait for unscheduled resize of volume (%s)", unscheduledResizeTimeout), func() {
			time.Sleep(unscheduledResizeTimeout)
		})

		Step("Toggle autopilot", func() {
			err := TogglePrometheusInStc()
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(30 * time.Second)
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
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var pvcRule = aututils.PVCRuleByTotalSize(10, 100, "20Gi")

// This test checks if removing the label from PVC will not trigger a rule, but when added back it should update the PVC
var _ = Describe(fmt.Sprintf("{%sPVCLabelChange}", testSuiteName), func() {
	var testrailID = 93307
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/93307
	var runID int
	tags["volumeChange"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sPVCLabelChange}", testSuiteName), "Perform basic scenarios with Autopilot", tags, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context
	msg := "has to create a rule, validate the size has not change, update the label, validate the size change"
	log.InfoD(msg)
	It(msg, func() {
		testName := strings.ToLower(fmt.Sprintf("%sPVCLabelChange", testSuiteName))

		Step("schedule applications", func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d-aprule", testName, i)
				pvcRule.Name = fmt.Sprintf("%s-%d", pvcRule.Name, i)
				labels := map[string]string{
					"autopilot": pvcRule.Name,
				}
				pvcRule.Spec.ActionsCoolDownPeriod = int64(60)
				context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
					AppKeys:            Inst().AppList,
					StorageProvisioner: Inst().Provisioner,
					AutopilotRule:      pvcRule,
					Labels:             labels,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(context).NotTo(BeEmpty())
				contexts = append(contexts, context...)
			}
		})
		msg := "Removing the label"
		log.Infof(msg)
		Step(msg, func() {
			time.Sleep(5 * time.Second)
			pvcRule, err := Inst().S.GetAutopilotRule(pvcRule.Name)
			pvcRule.Spec.Selector.MatchLabels["autopilot"] = fmt.Sprintf("%s-No-OP", pvcRule.Name)
			_, err = Inst().S.UpdateAutopilotRule(pvcRule)
			Expect(err).NotTo(HaveOccurred())
		})
		log.Infof("label removed")
		Step("Validate rule not triggered", func() {
			ValidateRuleNotTriggered(contexts, pvcRule.Name)
		})
		msg = "Updating the Label back for rule to take effect"
		log.InfoD(msg)
		Step(msg, func() {
			pvcRule, err := Inst().S.GetAutopilotRule(pvcRule.Name)
			pvcRule.Spec.Selector.MatchLabels["autopilot"] = fmt.Sprintf("%s", pvcRule.Name)
			_, err = Inst().S.UpdateAutopilotRule(pvcRule)
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
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This testsuite validates the autopilot rule can be changed to apply new values to existing rule
var _ = Describe(fmt.Sprintf("{%sPVCUpdateSize}", testSuiteName), func() {
	var testrailID = 93308
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/93308
	var runID int
	tags["volumeChange"] = "true"

	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sPVCUpdateSize}", testSuiteName), "Perform update of the existing rule on volume", tags, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context
	It("has to, apply autopilot rule, update the rule and validate if the new condition is properly applied", func() {
		testName := strings.ToLower(fmt.Sprintf("%sPVCUpdateSize", testSuiteName))
		var taskName string
		apRule := aututils.PVCRuleByTotalSize(10, 100, "20Gi")
		log.InfoD("Scheduling application")
		Step("schedule applications", func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {

				taskName = fmt.Sprintf("%s-%d-aprule0", testName, i)
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
		})

		log.InfoD("Validating volumes")
		Step("validating volumes and verifying size of volumes", func() {
			for _, ctx := range contexts {
				ValidateVolumes(ctx)
			}
		})

		log.InfoD("Updating Autopilot rule")
		Step("updating autopilot rules with correct values", func() {
			aRule, err := Inst().S.GetAutopilotRule(apRule.Name)
			Expect(err).NotTo(HaveOccurred())
			aRule.Spec.Actions[0].Params[aututils.RuleMaxSize] = "30Gi"
			aRule.Spec.Conditions.Expressions[0].Values[0] = "20"
			_, err = Inst().S.UpdateAutopilotRule(aRule)
			Expect(err).NotTo(HaveOccurred())
		})

		log.InfoD("Wait for update to complete")
		Step(fmt.Sprintf("wait for unscheduled resize of volume (%s)", unscheduledResizeTimeout), func() {
			time.Sleep(unscheduledResizeTimeout)
		})

		log.InfoD("Validating volume")
		Step("validating volumes and verifying size of volumes", func() {
			for _, ctx := range contexts {
				ValidateVolumes(ctx)
			}
		})

		log.InfoD("Destroy Apps")
		Step("destroy apps", func() {
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
			for _, ctx := range contexts {
				TearDownContext(ctx, opts)
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This testsuite is used for performing basic scenarios with Autopilot rules where it
// schedules apps and wait until workload is completed on the volumes. Restarts volume
// driver and validates PVC sizes of the volumes
var _ = Describe(fmt.Sprintf("{%sVolumeDriverDown}", testSuiteName), func() {
	var testrailID = 85443
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/85443
	var runID int
	tags["volumeChange"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sVolumeDriverDown}", testSuiteName), "Perform basic scenarios with Autopilot and restart volume driver ", tags, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context
	It("has to fill up the volume completely, resize the volume, validate and teardown apps", func() {
		var err error
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
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe(fmt.Sprintf("{%sRestartAutopilot}", testSuiteName), func() {
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sRestartAutopilot}", testSuiteName), "Restart Autopilot test", nil, 0)
	})
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
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

// This test is used for performing upgrade autopilot when autopilot rules in ActionInProgress state
var _ = Describe(fmt.Sprintf("{%sUpgradeAutopilot}", testSuiteName), func() {
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sUpgradeAutopilot}", testSuiteName), "Upgrade Autopilot test", nil, 0)
	})
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
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

// This testsuite is used for performing basic scenarios with Autopilot rules where it
// schedules apps and wait until workload is completed on the volumes and then validates
// sizes of storage pools by adding new disks to the nodes where volumes reside
var _ = Describe(fmt.Sprintf("{%sPoolExpand}", testSuiteName), func() {
	var testrailID = 85448
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/85448
	var runID int
	tags["poolChange"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sPoolExpand}", testSuiteName), "Pool expansion test on autopilot", tags, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context
	It("has to fill up the volume completely, resize the storage pool(s), validate and teardown apps", func() {
		testName := strings.ToLower(fmt.Sprintf("%sPoolExpandAddDisk", testSuiteName))

		type testCase struct {
			workerNode    node.Node
			labelSelector map[string]string
			apRule        apapi.AutopilotRule
		}

		poolExpandLabels := map[string]map[string]string{
			"addDiskLabel":          {"autopilot": "adddisk"},
			"addDiskFixedSizeLabel": {"autopilot": "adddiskfixedsize"},
			"resizeDiskLabel":       {"autopilot": "resizedisk"},
			"resizeFixedSizeLabel":  {"autopilot": "resizefixedsize"},
		}

		storageNodes := node.GetStorageNodes()

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
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// Restart Volume driver during resize pool with add-disk option
var _ = Describe(fmt.Sprintf("{%sPoolExpandRestartVolumeDriver}", testSuiteName), func() {
	tags["poolChange"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sPoolExpandRestartVolumeDriver}", testSuiteName), "Pool expansion and volume driver restart test on autopilot", tags, 0)
	})
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
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

// This testsuite is used for performing basic scenarios with Autopilot rules where it
// schedules apps and wait until workload is completed on the volumes and then validates
// PVC sizes of the volumes and sizes of storage pools
var _ = Describe(fmt.Sprintf("{%sPvcAndPoolExpand}", testSuiteName), func() {
	tags["poolChange"] = "true"
	tags["volumeChange"] = "true"
	var testrailID = 85449
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/85449
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sPvcAndPoolExpand}", testSuiteName), "PVC and Pool expand test on autopilot", tags, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context
	It("has to fill up the volume completely, resize the volumes and storage pool(s), validate and teardown apps", func() {
		testName := strings.ToLower(fmt.Sprintf("%sPvcAndPoolExpand", testSuiteName))
		poolLabel := map[string]string{"autopilot": "resizedisk"}
		pvcLabel := map[string]string{"autopilot": "pvc-expand"}
		storageNodes := node.GetStorageDriverNodes()
		pvcApRules := []apapi.AutopilotRule{
			//aututils.PVCRuleByUsageCapacity(50, 50, ""),
			aututils.PVCRuleByTotalSize(10, 100, ""),
		}
		poolApRules := []apapi.AutopilotRule{
			aututils.PoolRuleByTotalSize((getTotalPoolSize(storageNodes[0])/units.GiB)+1, 10, aututils.RuleScaleTypeResizeDisk, poolLabel),
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
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This test suite is used to run pool expand on Non cloud drive setups, and the error is expected to happen
var _ = Describe(fmt.Sprintf("{%sPoolExpandInNonCD}", testSuiteName), func() {
	var testrailID = 93319
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/93319
	var runID int
	tags["poolChange"] = "true"
	tags["negative"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sPoolExpandInNonCD}", testSuiteName), "Pool expand on non-cd setup", tags, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context
	It("has to expand pool on non cloud drives based setup", func() {
		testName := strings.ToLower(fmt.Sprintf("%sPoolExpandInNonCD", testSuiteName))
		poolLabel := map[string]string{"autopilot": "adddisk"}
		storageNodes := node.GetStorageDriverNodes()

		poolApRules := []apapi.AutopilotRule{
			aututils.PoolRuleByTotalSize((getTotalPoolSize(storageNodes[0])/units.GiB)+1, 10, aututils.RuleScaleTypeAddDisk, poolLabel),
		}

		Step("schedule apps with autopilot rules for pool expand", func() {
			err := AddLabelsOnNode(storageNodes[0], poolLabel)
			Expect(err).NotTo(HaveOccurred())
			contexts = scheduleAppsWithAutopilot(testName, 1, poolApRules, scheduler.ScheduleOptions{PvcSize: 20 * units.GiB})
		})

		Step("validate storage pools", func() {
			ValidateRuleNotApplied(contexts, poolApRules[0].Name)
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

			for k := range poolLabel {
				Inst().S.RemoveLabelOnNode(storageNodes[0], k)
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe(fmt.Sprintf("{%sEvents}", testSuiteName), func() {
	tags["volumeChange"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sEvents}", testSuiteName), "Events test on autopilot", tags, 0)
	})
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
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe(fmt.Sprintf("{%sPoolResizeFailure}", testSuiteName), func() {
	tags["poolChange"] = "true"
	tags["negative"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sPoolResizeFailure}", testSuiteName), "Pool Resize Failure test on autopilot", tags, 0)
	})
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
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe(fmt.Sprintf("{%sRebalanceProvMean}", testSuiteName), func() {
	tags["rebalance"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sRebalanceProvMean}", testSuiteName), "Create volume and rebalance test on autopilot", tags, 0)
	})
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
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe(fmt.Sprintf("{%sRebalanceUsageMean}", testSuiteName), func() {
	tags["rebalance"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sRebalanceUsageMean}", testSuiteName), "validate rebalance on autopilot", tags, 0)
	})
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
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe(fmt.Sprintf("{%sRestartAutopilotRebalance}", testSuiteName), func() {
	tags["rebalance"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sRestartAutoPilotRebalance}", testSuiteName), "restart autopilot and rebalance test", tags, 0)
	})
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
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe(fmt.Sprintf("{%sRebalanceProvMeanAndPvc}", testSuiteName), func() {
	tags["rebalance"] = "true"
	tags["volumeChange"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sRebalanceProvMeanAndPvc}", testSuiteName), "Rebalance and resize PVC at same time on autopilot", tags, 0)
	})
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
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

// This testsuite is used for performing rebalance scenarios and pool resize with Autopilot rules where it
// schedules apps on one of the node, waits until workload is completed on the volumes and then validates
// rebalalnce and sizes of storage pools
// NOTE: this test is using volumes with replicaset is 3 and make sure that you have at least 4 nodes to do rebalance
var _ = Describe(fmt.Sprintf("{%sRebalanceProvMeanAndPoolResize}", testSuiteName), func() {
	tags["rebalance"] = "true"
	tags["poolChange"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sRebalanceProvMeanAndPoolResize}", testSuiteName), "rebalance and resize pools", tags, 0)
	})
	It("has to run rebalance and resize pools, validate rebalance, validate pools and teardown apps", func() {
		var contexts []*scheduler.Context
		poolLabel := map[string]string{"autopilot": "resizedisk"}
		storageNodes := node.GetStorageNodes()
		// check if we have enough storage nodes to run the test
		Expect(len(storageNodes)).Should(BeNumerically(">=", 4))

		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleRebalanceByProvisionedMean([]string{"-10", "10"}, false),
			aututils.PoolRuleByTotalSize((getTotalPoolSize(storageNodes[0])*120/100)/units.GiB, 50, aututils.RuleScaleTypeResizeDisk, poolLabel),
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
		testName := strings.ToLower(fmt.Sprintf("%srebalance", testSuiteName))
		Step("schedule apps with autopilot rules", func() {
			contexts = scheduleAppsWithAutopilot(testName, numberOfVolumes, apRules,
				scheduler.ScheduleOptions{PvcNodesAnnotation: storageNodeIds, PvcSize: volumeSize})
		})

		Step("validate rebalance jobs", func() {
			err = Inst().S.WaitForRebalanceAROToComplete()
			Expect(err).NotTo(HaveOccurred())
			log.InfoD("=====Rebalance Completed ========")
			err = Inst().V.ValidateRebalanceJobs()
			log.InfoD("====Validate Rebalance Job ========")
			Expect(err).NotTo(HaveOccurred())
		})
		Step("validating and verifying size of storage pools", func() {
			ValidateStoragePools(contexts)
		})
		Step("Validate pool resize ARO", func() {
			var aroAvailable bool
			aroAvailable, err = Inst().S.VerifyPoolResizeARO(apRules[1])
			Expect(err).NotTo(HaveOccurred())
			log.InfoD("aroAvailable value %v", aroAvailable)
			log.InfoD("=====Pool resize ARO verified ========")

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
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe(fmt.Sprintf("{%sRebalanceUpdateDelete}", testSuiteName), func() {
	tags["rebalance"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sRebalanceUpdateDelete}", testSuiteName), "Rebalance Update and delete volume test on autopilot", tags, 0)
	})
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
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe(fmt.Sprintf("{%sRebalanceWithApproval}", testSuiteName), func() {
	tags["rebalance"] = "true"
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sRebalanceWithApproval}", testSuiteName), "Rebalance with approval test on autopilot", tags, 0)
	})
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
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

// This testsuite for cases including: pvc resize in large scale, pvc resize for sharedv4 volume
// executing pool reblance and expansion at the same time
var _ = Describe(fmt.Sprintf("{%sFunctionalTests}", testSuiteName), func() {
	var testrailID = 12345
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/12345
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sFunctionalTests}", testSuiteName), "Perform several autopilot functional tests", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	It("has to run rebalance and resize pools, validate rebalance, validate pools and teardown apps", func() {
		var contexts []*scheduler.Context
		testName := strings.ToLower(fmt.Sprintf("%srebalance", testSuiteName))
		poolLabel := map[string]string{"autopilot": "resizedisk"}
		storageNodes := node.GetStorageNodes()
		// check if we have enough storage nodes to run the test
		Expect(len(storageNodes)).Should(BeNumerically(">=", 4))

		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleRebalanceByProvisionedMean([]string{"-10", "15"}, false),
			aututils.PoolRuleByTotalSize((getTotalPoolSize(storageNodes[0])*120/100)/units.GiB, 50, aututils.RuleScaleTypeResizeDisk, poolLabel),
			aututils.PoolRuleByTotalSize((getTotalPoolSize(storageNodes[1])*120/100)/units.GiB, 50, aututils.RuleScaleTypeResizeDisk, poolLabel),
			aututils.PoolRuleByTotalSize((getTotalPoolSize(storageNodes[2])*120/100)/units.GiB, 50, aututils.RuleScaleTypeResizeDisk, poolLabel),
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

			err := aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.AnyToTriggeredEvent})
			Expect(err).NotTo(HaveOccurred())

			err = Inst().V.ValidateRebalanceJobs()
			Expect(err).NotTo(HaveOccurred())

			err = aututils.WaitForAutopilotEvent(apRule, "", []string{aututils.ActiveActionTakenToAny})
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

	It("has to fill up 100 volumes completely, resize the volumes, validate and teardown apps", func() {
		var contexts []*scheduler.Context
		var scaleFactor = 100
		var appName = "aut-vol-only"
		testName := strings.ToLower(fmt.Sprintf("%sPvcBasicInScale", testSuiteName))

		Step("schedule applications", func() {
			for i := 0; i < scaleFactor; i++ {
				id := 0
				apRule := aututils.PVCRuleByTotalSize(20, 50, "18Gi")
				taskName := fmt.Sprintf("%s-%d-aprule%d", testName, i, id)
				apRule.Name = fmt.Sprintf("%s-%d", apRule.Name, i)
				labels := map[string]string{
					"autopilot": apRule.Name,
				}
				apRule.Spec.ActionsCoolDownPeriod = int64(60)
				context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
					AppKeys:            []string{appName},
					StorageProvisioner: Inst().Provisioner,
					AutopilotRule:      apRule,
					Labels:             labels,
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(context).NotTo(BeEmpty())
				contexts = append(contexts, context...)
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

		Step("destroy apps", func() {
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
			for _, ctx := range contexts {
				TearDownContext(ctx, opts)
			}
		})
	})

	It("has to fill up the sharedv4 volume completely, resize the volume, validate and teardown apps", func() {
		var contexts []*scheduler.Context
		testName := strings.ToLower(fmt.Sprintf("%sPvcBasic", testSuiteName))
		var scaleFactor = 2
		var appName = "aut-postgres-sharedv4"

		Step("schedule applications", func() {
			for i := 0; i < scaleFactor; i++ {
				for id, apRule := range autopilotruleBasicTestCases {
					taskName := fmt.Sprintf("%s-%d-aprule%d", testName, i, id)
					apRule.Name = fmt.Sprintf("%s-%d", apRule.Name, i)
					labels := map[string]string{
						"autopilot": apRule.Name,
					}
					apRule.Spec.ActionsCoolDownPeriod = int64(60)
					context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
						AppKeys:            []string{appName},
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

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
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

//This testsuite is designed to execute fundamental scenarios with Autopilot rules.
//It involves scheduling applications and waiting until the
//Autopilot ActiveActionsPendingToActiveActionsInProgress event is triggered.
//After this event, two storage nodes, one with KVDB, are intentionally crashed,
//and the test checks whether these nodes successfully recover.

var _ = Describe("{AutoPoolExpandCrashTest}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest(fmt.Sprintf("{%sAutoPoolExpandCrashTest}", testSuiteName), "Crash one kvdb node and one storage node when multiple pools are expanded using autopilot", nil, 0)
	})

	It("has to crash one kvdb node and one storage node when multiple pools are expanded with add-disk , validate and teardown apps", func() {
		var contexts []*scheduler.Context
		testName := strings.ToLower(fmt.Sprintf("%sAutoPoolExpandCrashTest", testSuiteName))
		poolLabel := map[string]string{"node-type": "fastpath", "autopilot": "adddisk"}
		crashedNodes := make([]node.Node, 0)
		storageNode := node.GetStorageNodes()[0]
		crashedNodes = append(crashedNodes, storageNode)
		log.InfoD("storage pool %s", storageNode.Name)

		stepLog := "get kvdb node to crash and add label to the node"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			stNodes := node.GetNodesByVoDriverNodeID()
			kvdbNodes, err := GetAllKvdbNodes()
			log.FailOnError(err, "Failed to get list of KVDB nodes from the cluster")
			for _, kvdbNode := range kvdbNodes {
				appNode, ok := stNodes[kvdbNode.ID]
				if !ok {
					log.InfoD("node with id %s not found in the nodes list", kvdbNode.ID)
					continue
				}
				if storageNode.Name == appNode.Name {
					//kvdb shouldn't be same as storagenodes
					continue
				}
				crashedNodes = append(crashedNodes, appNode)
				break
			}
		})

		//map which holds the initial sizes of the global pool
		poolSizes := make(map[int]float64, 0)

		apRules := []apapi.AutopilotRule{
			aututils.PoolRuleByAvailableCapacity(80, 10, aututils.RuleScaleTypeAddDisk),
		}
		provisionStatus, err := GetClusterProvisionStatusOnSpecificNode(crashedNodes[0])
		log.FailOnError(err, "Failed to get cluster provision status")
		var originalTotalSize float64 = 0
		for _, pstatus := range provisionStatus {
			originalTotalSize += pstatus.TotalSize
		}
		poolSizes[0] = originalTotalSize
		provisionStatus, err = GetClusterProvisionStatusOnSpecificNode(crashedNodes[1])
		log.FailOnError(err, "Failed to get cluster provision status")
		originalTotalSize = 0
		for _, pstatus := range provisionStatus {
			originalTotalSize += pstatus.TotalSize
		}
		poolSizes[1] = originalTotalSize
		log.InfoD("Pool size before pool expand:%v", originalTotalSize)

		stepLog = "schedule apps with autopilot rules for pool expand"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, node := range crashedNodes {
				err = AddLabelsOnNode(node, poolLabel)
				log.FailOnError(err, "Failed to add label on node:%v", node.Name)
			}
			contexts = scheduleAppsWithAutopilot(testName, 1, apRules, scheduler.ScheduleOptions{PvcSize: 50 * units.GiB})
		})

		stepLog = "Wait for Autopilot pool expansion, then crash a KVDB and Storage Node"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			//Crash one kvdb node and one storage node where the application is provisioned
			log.InfoD("Started listening for any autopilot event")
			err = aututils.WaitForAutopilotEvent(apRules[0], "", []string{aututils.AnyToTriggeredEvent})
			log.FailOnError(err, "Failed to listen for any autopilot events")
			err = aututils.WaitForAutopilotEvent(apRules[0], "", []string{aututils.ActiveActionsPendingToActiveActionsInProgress})
			log.FailOnError(err, "Failed to listen for active-actions-pending to active-actions-in-progress event")
			var wg sync.WaitGroup
			for _, nodeToCrash := range crashedNodes {
				wg.Add(1)

				go func(nodeToCrash node.Node) {
					defer GinkgoRecover()
					defer wg.Done() // Decrement the WaitGroup counter when the task is done
					stepLog := fmt.Sprintf("crash node: %s", nodeToCrash.Name)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						err := Inst().N.CrashNode(nodeToCrash, node.CrashNodeOpts{
							Force: true,
							ConnectionOpts: node.ConnectionOpts{
								Timeout:         defaultCommandTimeout,
								TimeBeforeRetry: defaultCommandRetry,
							},
						})
						dash.VerifySafely(err, nil, "Validate node is crashed")
					})

					stepLog = fmt.Sprintf("wait for node: %s to be back up", nodeToCrash.Name)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						err := Inst().N.TestConnection(nodeToCrash, node.ConnectionOpts{
							Timeout:         defaultTestConnectionTimeout,
							TimeBeforeRetry: defaultWaitRebootRetry,
						})
						log.FailOnError(err, "Validate node is back up")
					})

					stepLog = fmt.Sprintf("wait for scheduler: %s and volume driver: %s to start",
						Inst().S.String(), Inst().V.String())
					Step(stepLog, func() {
						log.InfoD(stepLog)
						err := Inst().S.IsNodeReady(nodeToCrash)
						log.FailOnError(err, "Validate node is ready")
						err = Inst().V.WaitDriverUpOnNode(nodeToCrash, Inst().DriverStartTimeout)
						log.FailOnError(err, "Validate volume driver is up")
					})
				}(nodeToCrash)
			}
			// Wait for all tasks to finish before proceeding
			wg.Wait()

			//calculate pool size from first node
			provisionStatus, err := GetClusterProvisionStatusOnSpecificNode(crashedNodes[0])
			log.FailOnError(err, "Failed to get cluster info for node:%v", crashedNodes[0])

			var sizeAfterPoolExpand float64 = 0
			for _, pstatus := range provisionStatus {
				sizeAfterPoolExpand += pstatus.TotalSize
			}
			dash.VerifyFatal(sizeAfterPoolExpand > poolSizes[0], true, "Pool expand successfully verified on first node")
			//calculate pool size from second node
			provisionStatus, err = GetClusterProvisionStatusOnSpecificNode(crashedNodes[1])
			log.FailOnError(err, "Failed to get cluster info for node:%v", crashedNodes[1].Name)

			sizeAfterPoolExpand = 0
			for _, pstatus := range provisionStatus {
				sizeAfterPoolExpand += pstatus.TotalSize
			}

			dash.VerifyFatal(sizeAfterPoolExpand > poolSizes[1], true, "Pool expand successfully verified on second node")
			log.InfoD("Pool expand successfully completed, size after pool expand:%v", sizeAfterPoolExpand)

		})
		stepLog = "wait until workload completes on volume"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, ctx := range contexts {
				ValidateContext(ctx)
			}
		})
		stepLog = "validating and verifying size of storage pools"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			ValidateStoragePools(contexts)
		})
		stepLog = "destroy apps"
		Step(stepLog, func() {
			opts := make(map[string]bool)
			opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
			for _, ctx := range contexts {
				TearDownContext(ctx, opts)
			}
			for _, apRule := range apRules {
				err = Inst().S.DeleteAutopilotRule(apRule.Name)
				log.FailOnError(err, "Failed to delete autopilot rule")
				log.Infof("Deleted autopilot rule: %v", apRule.Name)
			}
			for key := range poolLabel {
				for _, node := range crashedNodes {
					err = Inst().S.RemoveLabelOnNode(node, key)
					log.FailOnError(err, "Failed to remove label from node:%v", node.Name)
				}
			}
		})
	})
	JustAfterEach(func() {
		EndTorpedoTest()
	})
})

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

		log.Infof("Upgrading autopilot with new image %s", image)
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
		log.Infof("autopilot with new image %s upgraded successfully", image)
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
