package tests

import (
	"fmt"
	"math"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	. "github.com/portworx/torpedo/tests"
	"time"
)

func TestVolOps(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Torpedo : VolOps")
}

var _ = BeforeSuite(func() {
	InitInstance()
})

// Volume Replication Decrease
var _ = Describe("{VolumeReplicationDecrease}", func() {
	It("has to schedule apps and decrease replication factor on all volumes of the apps", func() {
		var err error
		var contexts []*scheduler.Context
		expReplMap := make(map[*volume.Volume]int64)
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("volrepldown-%d", i))...)
		}

		Step("get volumes for all apps in test and decrease replication factor", func() {
			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
					appVolumes, err = Inst().S.GetVolumes(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(appVolumes).NotTo(BeEmpty())
				})

				MinRF := Inst().V.GetMinReplicationFactor()
				Step(
					fmt.Sprintf("repl decrease volume driver %s on app %s's volumes: %v",
						Inst().V.String(), ctx.App.Key, appVolumes),
					func() {
						for _, v := range appVolumes {
							errExpected := false
							currRep, err := Inst().V.GetReplicationFactor(v)
							Expect(err).NotTo(HaveOccurred())
							if currRep == MinRF {
								errExpected = true
							}
							expReplMap[v] = int64(math.Max(float64(MinRF), float64(currRep)-1))
							err = Inst().V.SetReplicationFactor(v, currRep-1)
							if !errExpected {
								Expect(err).NotTo(HaveOccurred())
							} else {
								Expect(err).To(HaveOccurred())
							}

						}
					})
				Step(
					fmt.Sprintf("validate successful repl decrease"),
					func() {
						for _, v := range appVolumes {
							newRepl, err := Inst().V.GetReplicationFactor(v)
							Expect(err).NotTo(HaveOccurred())
							Expect(newRepl).To(Equal(expReplMap[v]))
						}
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

// Volume replication increase
var _ = Describe("{VolumeUpdate}", func() {
	It("has to schedule apps and increase replication factor and size on all volumes of the apps", func() {
		var err error
		var contexts []*scheduler.Context
		expReplMap := make(map[*volume.Volume]int64)
		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleAndValidate(fmt.Sprintf("volumeupdate-%d", i))...)
		}

		Step("get volumes for all apps in test and increase replication factor and size", func() {
			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
					appVolumes, err = Inst().S.GetVolumes(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(appVolumes).NotTo(BeEmpty())
				})

				MaxRF := Inst().V.GetMaxReplicationFactor()
				Step(
					fmt.Sprintf("repl increase volume driver %s on app %s's volumes: %v",
						Inst().V.String(), ctx.App.Key, appVolumes),
					func() {
						for _, v := range appVolumes {
							errExpected := false
							currRep, err := Inst().V.GetReplicationFactor(v)
							Expect(err).NotTo(HaveOccurred())
							if currRep == MaxRF {
								errExpected = true
							}
							expReplMap[v] = int64(math.Min(float64(MaxRF), float64(currRep)+1))
							err = Inst().V.SetReplicationFactor(v, currRep+1)
							if !errExpected {
								Expect(err).NotTo(HaveOccurred())
							} else {
								Expect(err).To(HaveOccurred())
							}

						}
					})
				Step(
					fmt.Sprintf("validate successful repl increase"),
					func() {
						for _, v := range appVolumes {
							newRepl, err := Inst().V.GetReplicationFactor(v)
							Expect(err).NotTo(HaveOccurred())
							Expect(newRepl).To(Equal(expReplMap[v]))
						}
					})
				var requestedVols []*volume.Volume
				Step(
					fmt.Sprintf("increase volume size %s on app %s's volumes: %v",
						Inst().V.String(), ctx.App.Key, appVolumes),
					func() {
						requestedVols, err = Inst().S.ResizeVolume(ctx)
						Expect(err).NotTo(HaveOccurred())
						Step("wait for the volumes to be resized", func() {
							time.Sleep(20 * time.Second)
						})

					})
				Step(
					fmt.Sprintf("validate successful volume size increase"),
					func() {
						for _, v := range requestedVols {
							err := Inst().V.ValidateUpdateVolume(v)
							Expect(err).NotTo(HaveOccurred())
						}
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

var _ = AfterSuite(func() {
	PerformSystemCheck()
	CollectSupport()
	ValidateCleanup()
})

func init() {
	ParseFlags()
}
