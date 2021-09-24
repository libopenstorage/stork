package tests

import (
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	. "github.com/portworx/torpedo/tests"
	"github.com/sirupsen/logrus"
)

const (
	defaultCommandRetry   = 5 * time.Second
	defaultCommandTimeout = 1 * time.Minute
)

func TestVolOps(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_VolOps.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : VolOps", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
})

// Volume replication change
var _ = Describe("{VolumeUpdate}", func() {
	var contexts []*scheduler.Context

	It("has to schedule apps and update replication factor and size on all volumes of the apps", func() {
		var err error
		contexts = make([]*scheduler.Context, 0)
		expReplMap := make(map[*volume.Volume]int64)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("volupdate-%d", i))...)
		}

		ValidateApplications(contexts)

		Step("get volumes for all apps in test and update replication factor and size", func() {
			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
					appVolumes, err = Inst().S.GetVolumes(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(appVolumes).NotTo(BeEmpty())
				})
				for _, v := range appVolumes {
					MaxRF := Inst().V.GetMaxReplicationFactor()
					MinRF := Inst().V.GetMinReplicationFactor()

					Step(
						fmt.Sprintf("repl decrease volume driver %s on app %s's volume: %v",
							Inst().V.String(), ctx.App.Key, v),
						func() {
							errExpected := false
							currRep, err := Inst().V.GetReplicationFactor(v)
							Expect(err).NotTo(HaveOccurred())
							if currRep == MinRF {
								errExpected = true
							}
							expReplMap[v] = int64(math.Max(float64(MinRF), float64(currRep)-1))
							err = Inst().V.SetReplicationFactor(v, currRep-1, nil)
							if !errExpected {
								Expect(err).NotTo(HaveOccurred())
							} else {
								Expect(err).To(HaveOccurred())
							}

						})
					Step(
						fmt.Sprintf("validate successful repl decrease on app %s's volume: %v",
							ctx.App.Key, v),
						func() {
							newRepl, err := Inst().V.GetReplicationFactor(v)
							Expect(err).NotTo(HaveOccurred())
							Expect(newRepl).To(Equal(expReplMap[v]))
						})
					Step(
						fmt.Sprintf("repl increase volume driver %s on app %s's volume: %v",
							Inst().V.String(), ctx.App.Key, v),
						func() {
							errExpected := false
							currRep, err := Inst().V.GetReplicationFactor(v)
							Expect(err).NotTo(HaveOccurred())
							// GetMaxReplicationFactory is hardcoded to 3
							// if it increases repl 3 to an aggregated 2 volume, it will fail
							// because it would require 6 worker nodes, since
							// number of nodes required = aggregation level * replication factor
							currAggr, err := Inst().V.GetAggregationLevel(v)
							Expect(err).NotTo(HaveOccurred())
							if currAggr > 1 {
								MaxRF = int64(len(node.GetWorkerNodes())) / currAggr
							}
							if currRep == MaxRF {
								errExpected = true
							}
							expReplMap[v] = int64(math.Min(float64(MaxRF), float64(currRep)+1))
							err = Inst().V.SetReplicationFactor(v, currRep+1, nil)
							if !errExpected {
								Expect(err).NotTo(HaveOccurred())
							} else {
								Expect(err).To(HaveOccurred())
							}
						})
					Step(
						fmt.Sprintf("validate successful repl increase on app %s's volume: %v",
							ctx.App.Key, v),
						func() {
							newRepl, err := Inst().V.GetReplicationFactor(v)
							Expect(err).NotTo(HaveOccurred())
							Expect(newRepl).To(Equal(expReplMap[v]))
						})
				}
				var requestedVols []*volume.Volume
				Step(
					fmt.Sprintf("increase volume size %s on app %s's volumes: %v",
						Inst().V.String(), ctx.App.Key, appVolumes),
					func() {
						requestedVols, err = Inst().S.ResizeVolume(ctx, Inst().ConfigMap)
						Expect(err).NotTo(HaveOccurred())
					})
				Step(
					fmt.Sprintf("validate successful volume size increase on app %s's volumes: %v",
						ctx.App.Key, appVolumes),
					func() {
						for _, v := range requestedVols {
							// Need to pass token before validating volume
							params := make(map[string]string)
							if Inst().ConfigMap != "" {
								params["auth-token"], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
								Expect(err).NotTo(HaveOccurred())
							}
							err := Inst().V.ValidateUpdateVolume(v, params)
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
	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

// Volume replication change
var _ = Describe("{VolumeUpdateForAttachedNode}", func() {
	var contexts []*scheduler.Context

	It("has to schedule apps and update replication factor for attached node", func() {
		var err error
		contexts = make([]*scheduler.Context, 0)
		expReplMap := make(map[*volume.Volume]int64)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("volupdate-%d", i))...)
		}

		ValidateApplications(contexts)

		Step("get volumes for all apps in test and update replication factor and size", func() {
			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
					appVolumes, err = Inst().S.GetVolumes(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(appVolumes).NotTo(BeEmpty())
				})
				for _, v := range appVolumes {
					MaxRF := Inst().V.GetMaxReplicationFactor()
					MinRF := Inst().V.GetMinReplicationFactor()
					currReplicaSet := []string{}
					updateReplicaSet := []string{}
					expectedReplicaSet := []string{}

					Step(
						fmt.Sprintf("repl decrease volume driver %s on app %s's volume: %v",
							Inst().V.String(), ctx.App.Key, v),
						func() {
							errExpected := false
							currRep, err := Inst().V.GetReplicationFactor(v)
							Expect(err).NotTo(HaveOccurred())
							if currRep == MinRF {
								errExpected = true
							}
							attachedNode, err := Inst().V.GetNodeForVolume(v, defaultCommandTimeout, defaultCommandRetry)

							replicaSets, err := Inst().V.GetReplicaSets(v)
							Expect(err).NotTo(HaveOccurred())
							Expect(replicaSets).NotTo(BeEmpty())

							for _, nID := range replicaSets[0].Nodes {
								currReplicaSet = append(currReplicaSet, nID)
							}

							logrus.Infof("ReplicaSet of volume %v is: %v", v.Name, currReplicaSet)
							logrus.Infof("Volume %v is attached to : %v", v.Name, attachedNode.Id)

							for _, n := range currReplicaSet {
								if n == attachedNode.Id {
									updateReplicaSet = append(updateReplicaSet, n)
								} else {
									expectedReplicaSet = append(expectedReplicaSet, n)
								}
							}

							if len(updateReplicaSet) == 0 {
								logrus.Info("Attached node in not part of ReplicatSet, choosing a random node part of set for setting replication factor")
								updateReplicaSet = append(updateReplicaSet, expectedReplicaSet[0])
								expectedReplicaSet = expectedReplicaSet[1:]
							}

							expReplMap[v] = int64(math.Max(float64(MinRF), float64(currRep)-1))
							err = Inst().V.SetReplicationFactor(v, currRep-1, updateReplicaSet)
							if !errExpected {
								Expect(err).NotTo(HaveOccurred())
							} else {
								Expect(err).To(HaveOccurred())
							}

						})
					Step(
						fmt.Sprintf("validate successful repl decrease on app %s's volume: %v",
							ctx.App.Key, v),
						func() {
							newRepl, err := Inst().V.GetReplicationFactor(v)
							logrus.Infof("Got repl factor after update: %v", newRepl)
							Expect(err).NotTo(HaveOccurred())
							Expect(newRepl).To(Equal(expReplMap[v]))
							currReplicaSets, err := Inst().V.GetReplicaSets(v)
							Expect(err).NotTo(HaveOccurred())
							Expect(currReplicaSets).NotTo(BeEmpty())
							reducedReplicaSet := []string{}
							for _, nID := range currReplicaSets[0].Nodes {
								reducedReplicaSet = append(reducedReplicaSet, nID)
							}

							logrus.Infof("ReplicaSet of volume %v is: %v", v.Name, reducedReplicaSet)
							logrus.Infof("Expected ReplicaSet of volume %v is: %v", v.Name, expectedReplicaSet)
							res := reflect.DeepEqual(reducedReplicaSet, expectedReplicaSet)
							Expect(res).To(BeTrue())
						})
					for _, ctx := range contexts {
						ctx.SkipVolumeValidation = true
					}
					ValidateApplications(contexts)
					for _, ctx := range contexts {
						ctx.SkipVolumeValidation = false
					}

					Step(
						fmt.Sprintf("repl increase volume driver %s on app %s's volume: %v",
							Inst().V.String(), ctx.App.Key, v),
						func() {
							errExpected := false
							currRep, err := Inst().V.GetReplicationFactor(v)
							Expect(err).NotTo(HaveOccurred())
							// GetMaxReplicationFactory is hardcoded to 3
							// if it increases repl 3 to an aggregated 2 volume, it will fail
							// because it would require 6 worker nodes, since
							// number of nodes required = aggregation level * replication factor
							currAggr, err := Inst().V.GetAggregationLevel(v)
							Expect(err).NotTo(HaveOccurred())
							if currAggr > 1 {
								MaxRF = int64(len(node.GetWorkerNodes())) / currAggr
							}
							if currRep == MaxRF {
								errExpected = true
							}
							expReplMap[v] = int64(math.Min(float64(MaxRF), float64(currRep)+1))
							err = Inst().V.SetReplicationFactor(v, currRep+1, updateReplicaSet)
							if !errExpected {
								Expect(err).NotTo(HaveOccurred())
							} else {
								Expect(err).To(HaveOccurred())
							}
						})
					Step(
						fmt.Sprintf("validate successful repl increase on app %s's volume: %v",
							ctx.App.Key, v),
						func() {
							newRepl, err := Inst().V.GetReplicationFactor(v)
							Expect(err).NotTo(HaveOccurred())
							Expect(newRepl).To(Equal(expReplMap[v]))
							currReplicaSets, err := Inst().V.GetReplicaSets(v)
							Expect(err).NotTo(HaveOccurred())
							Expect(currReplicaSets).NotTo(BeEmpty())
							increasedReplicaSet := []string{}
							for _, nID := range currReplicaSets[0].Nodes {
								increasedReplicaSet = append(increasedReplicaSet, nID)
							}

							logrus.Infof("ReplicaSet of volume %v is: %v", v.Name, increasedReplicaSet)
							res := reflect.DeepEqual(increasedReplicaSet, currReplicaSet)
							Expect(res).To(BeTrue())
						})
					ValidateApplications(contexts)
				}

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
		AfterEachTest(contexts)
	})
})

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
