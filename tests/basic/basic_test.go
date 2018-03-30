package tests

import (
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

func TestBasic(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Torpedo : Basic")
}

var _ = BeforeSuite(func() {
	InitInstance()
})

// This test performs basic test of starting an application and destroying it (along with storage)
var _ = Describe("SetupTeardown", func() {
	It("has to setup, validate and teardown apps", func() {
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("setupteardown-%d", i))...)
		}

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true

		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
	})
})

// Volume Driver Plugin is down, unavailable - and the client container should not be impacted.
var _ = Describe("VolumeDriverDown", func() {
	It("has to schedule apps and stop volume driver on app nodes", func() {
		var err error
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("voldriverdown-%d", i))...)
		}

		Step("get nodes for all apps in test and bounce volume driver", func() {
			for _, ctx := range contexts {
				var appNodes []node.Node
				Step(fmt.Sprintf("get nodes for %s app", ctx.App.Key), func() {
					appNodes, err = Inst().S.GetNodesForApp(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(appNodes).NotTo(BeEmpty())
				})

				Step(
					fmt.Sprintf("stop volume driver %s on app %s's nodes: %v",
						Inst().V.String(), ctx.App.Key, appNodes),
					func() {
						StopVolDriverAndWait(appNodes)
					})

				Step("starting volume driver", func() {
					StartVolDriverAndWait(appNodes)
				})

				Step("Giving few seconds for volume driver to stabilize", func() {
					time.Sleep(20 * time.Second)
				})
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

// Volume Driver Plugin has crashed - and the client container should not be impacted.
var _ = Describe("VolumeDriverCrash", func() {
	It("has to schedule apps and crash volume driver on app nodes", func() {
		var err error
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("voldrivercrash-%d", i))...)
		}

		Step("get nodes for all apps in test and crash volume driver", func() {
			for _, ctx := range contexts {
				var appNodes []node.Node
				Step(fmt.Sprintf("get nodes for %s app", ctx.App.Key), func() {
					appNodes, err = Inst().S.GetNodesForApp(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(appNodes).NotTo(BeEmpty())
				})

				Step(
					fmt.Sprintf("crash volume driver %s on app %s's nodes: %v",
						Inst().V.String(), ctx.App.Key, appNodes),
					func() {
						CrashVolDriverAndWait(appNodes)
					})
			}
		})

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
		ValidateAndDestroy(contexts, opts)
	})
})

// Volume driver plugin is down and the client container gets terminated.
// There is a lost unmount call in this case. When the volume driver is
// back up, we should be able to detach and delete the volume.
var _ = Describe("VolumeDriverAppDown", func() {
	It("has to schedule apps, stop volume driver on app nodes and destroy apps", func() {
		var err error
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("voldriverappdown-%d", i))...)
		}

		Step("get nodes for all apps in test and bounce volume driver", func() {
			for _, ctx := range contexts {
				var appNodes []node.Node
				Step(fmt.Sprintf("get nodes for %s app", ctx.App.Key), func() {
					appNodes, err = Inst().S.GetNodesForApp(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(appNodes).NotTo(BeEmpty())
				})

				Step(fmt.Sprintf("stop volume driver %s on app %s's nodes: %v",
					Inst().V.String(), ctx.App.Key, appNodes), func() {
					StopVolDriverAndWait(appNodes)
				})

				Step(fmt.Sprintf("destroy app: %s", ctx.App.Key), func() {
					err = Inst().S.Destroy(ctx, nil)
					Expect(err).NotTo(HaveOccurred())

					Step("wait for few seconds for app destroy to trigger", func() {
						time.Sleep(10 * time.Second)
					})
				})

				Step("restarting volume driver", func() {
					StartVolDriverAndWait(appNodes)
				})

				Step(fmt.Sprintf("wait for destroy of app: %s", ctx.App.Key), func() {
					err = Inst().S.WaitForDestroy(ctx)
					Expect(err).NotTo(HaveOccurred())
				})

				DeleteVolumesAndWait(ctx)
			}
		})
	})
})

// This test deletes all tasks of an application and checks if app converges back to desired state
var _ = Describe("AppTasksDown", func() {
	It("has to schedule app and delete app tasks", func() {
		var err error
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("apptasksdown-%d", i))...)
		}

		Step("delete all application tasks", func() {
			for _, ctx := range contexts {
				Step(fmt.Sprintf("delete tasks for app: %s", ctx.App.Key), func() {
					err = Inst().S.DeleteTasks(ctx)
					Expect(err).NotTo(HaveOccurred())
				})

				ValidateContext(ctx)
			}
		})

		Step("teardown all apps", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, nil)
			}
		})
	})
})

// This test scales up and down an application and checks if app has actually scaled accordingly
var _ = Describe("AppScaleUpAndDown", func() {
	It("has to scale up and scale down the app", func() {
		var contexts []*scheduler.Context
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("applicationscaleupdown-%d", i))...)
		}

		Step("scale up all applications", func() {
			for _, ctx := range contexts {
				Step(fmt.Sprintf("updating scale for app: %s", ctx.App.Key), func() {
					applicationScaleUpMap, err := Inst().S.GetScaleFactorMap(ctx)
					Expect(err).NotTo(HaveOccurred())
					for name, scale := range applicationScaleUpMap {
						applicationScaleUpMap[name] = scale + 1
					}
					err = Inst().S.ScaleApplication(ctx, applicationScaleUpMap)
					Expect(err).NotTo(HaveOccurred())
				})

				ValidateContext(ctx)
			}
		})
		Step("scale down all applications", func() {
			for _, ctx := range contexts {
				Step("scale down all deployments/stateful sets ", func() {
					applicationScaleDownMap, err := Inst().S.GetScaleFactorMap(ctx)
					Expect(err).NotTo(HaveOccurred())
					for name, scale := range applicationScaleDownMap {
						applicationScaleDownMap[name] = scale - 1
					}
					err = Inst().S.ScaleApplication(ctx, applicationScaleDownMap)
					Expect(err).NotTo(HaveOccurred())
				})

				ValidateContext(ctx)
			}
		})
		Step("teardown all apps", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, nil)
			}
		})

	})
})

var _ = AfterSuite(func() {
	CollectSupport()
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
