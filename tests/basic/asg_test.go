package tests

import (
	"fmt"
	"math/rand"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/testrailuttils"
	. "github.com/portworx/torpedo/tests"

	// https://github.com/kubernetes/client-go/issues/242
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

const (
	scaleTimeout = 10 * time.Minute
)

// This test performs basic test of scaling up and down the asg cluster
var _ = Describe("{ClusterScaleUpDown}", func() {
	var testrailID = 58847
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/58847
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("ClusterScaleUpDown", "Validate storage nodes scale down and scale up", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	It("has to validate that storage nodes are not lost during asg scaledown", func() {
		dash.Infof("Has to validate that storage nodes are not lost during asg scaledown")
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("asgscaleupdown-%d", i))...)
		}

		ValidateApplications(contexts)

		intitialNodeCount, err := Inst().N.GetASGClusterSize()
		dash.VerifyFatal(err, nil, "Verify Get ASG cluster size")

		scaleupCount := intitialNodeCount + intitialNodeCount/2
		stepLog := fmt.Sprintf("scale up cluster from %d to %d nodes and validate",
			intitialNodeCount, (scaleupCount/3)*3)
		Step(stepLog, func() {
			dash.Info(stepLog)
			// After scale up, get fresh list of nodes
			// by re-initializing scheduler and volume driver
			err = Inst().S.RefreshNodeRegistry()
			dash.VerifyFatal(err, nil, "Verify node registry refresh")

			err = Inst().V.RefreshDriverEndpoints()
			Expect(err).NotTo(HaveOccurred())
			dash.VerifyFatal(err, nil, "Verify driver end points refresh")

			Scale(scaleupCount)
			stepLog = "validate number of storage nodes after scale up"
			Step(fmt.Sprintf(stepLog), func() {
				dash.Info(stepLog)
				ValidateClusterSize(scaleupCount)
			})

		})

		stepLog = fmt.Sprintf("scale down cluster back to original size of %d nodes",
			intitialNodeCount)
		Step(stepLog, func() {
			dash.Info(stepLog)
			Scale(intitialNodeCount)

			stepLog = fmt.Sprintf("wait for %s minutes for auto recovery of storeage nodes",
				Inst().AutoStorageNodeRecoveryTimeout.String())

			Step(stepLog, func() {
				dash.Info(stepLog)
				time.Sleep(Inst().AutoStorageNodeRecoveryTimeout)
			})

			// After scale down, get fresh list of nodes
			// by re-initializing scheduler and volume driver
			err = Inst().S.RefreshNodeRegistry()
			dash.VerifyFatal(err, nil, "verify refresh node registry")

			err = Inst().V.RefreshDriverEndpoints()
			dash.VerifyFatal(err, nil, "verify refresh driver end points")

			stepLog = fmt.Sprintf("validate number of storage nodes after scale down")
			Step(stepLog, func() {
				dash.Info(stepLog)
				ValidateClusterSize(intitialNodeCount)
			})
		})

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
		ValidateAndDestroy(contexts, opts)
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

// This test randomly kills one volume driver node and ensures cluster remains
// intact by ASG
var _ = Describe("{ASGKillRandomNodes}", func() {
	var testrailID = 58848
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/58848
	var runID int
	JustBeforeEach(func() {
		StartTorpedoTest("ASGKillRandomNodes", "Validate PX and Apps when ASG enabled nodes are deleted", nil, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	It("keeps killing worker nodes", func() {

		var err error
		contexts = make([]*scheduler.Context, 0)

		// Get list of nodes where storage driver is installed
		storageDriverNodes := node.GetStorageDriverNodes()
		Expect(err).NotTo(HaveOccurred())

		Step("Ensure apps are deployed", func() {
			dash.Info("Deploy Apps")
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("asgchaos-%d", i))...)
			}
		})

		ValidateApplications(contexts)

		stepLog := "Randomly kill one storage node"
		Step(stepLog, func() {
			dash.Info(stepLog)

			// set frequency mins depending on the chaos level
			var frequency int
			switch Inst().ChaosLevel {
			case 5:
				frequency = 15
			case 4:
				frequency = 30
			case 3:
				frequency = 45
			case 2:
				frequency = 60
			case 1:
				frequency = 90
			default:
				frequency = 30

			}
			if Inst().MinRunTimeMins == 0 {
				// Run once
				asgKillANodeAndValidate(storageDriverNodes)

				// Validate applications and tear down
				opts := make(map[string]bool)
				opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
				ValidateAndDestroy(contexts, opts)
			} else {
				// Run once till timer gets triggered
				asgKillANodeAndValidate(storageDriverNodes)

				Step("validate applications", func() {
					for _, ctx := range contexts {
						ValidateContext(ctx)
					}
				})

				// Run repeatedly
				ticker := time.NewTicker(time.Duration(frequency) * time.Minute)
				stopChannel := time.After(time.Duration(Inst().MinRunTimeMins) * time.Minute)
			L:
				for {
					select {
					case <-ticker.C:
						asgKillANodeAndValidate(storageDriverNodes)

						Step("validate applications", func() {
							for _, ctx := range contexts {
								ValidateContext(ctx)
							}
						})
					case <-stopChannel:
						ticker.Stop()
						// ticker may expire/time out in between, apps may not be
						// in correct condition to be validated. Just tear them down.
						opts := make(map[string]bool)
						opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
						Step("destroy apps", func() {
							for _, ctx := range contexts {
								TearDownContext(ctx, opts)
							}
						})
						break L
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

func Scale(count int64) {
	// In multi-zone ASG cluster, node count is per zone
	zones, err := Inst().N.GetZones()
	dash.VerifyFatal(err, nil, "Verify Get zones")

	perZoneCount := count / int64(len(zones))

	// err = Inst().N.SetASGClusterSize(perZoneCount, scaleTimeout)
	// Expect(err).NotTo(HaveOccurred())

	t := func() (interface{}, bool, error) {

		err = Inst().N.SetASGClusterSize(perZoneCount, scaleTimeout)
		if err != nil {
			return "", true, err
		}
		return "", false, nil
	}

	_, err = task.DoRetryWithTimeout(t, 60*time.Minute, 2*time.Minute)
	dash.VerifyFatal(err, nil, "Verify Set ASG Cluster size")

}

func asgKillANodeAndValidate(storageDriverNodes []node.Node) {
	rand.Seed(time.Now().Unix())
	nodeToKill := storageDriverNodes[rand.Intn(len(storageDriverNodes))]

	stepLog := fmt.Sprintf("Deleting node [%v]", nodeToKill.Name)
	Step(stepLog, func() {
		dash.Info(stepLog)
		err := Inst().N.DeleteNode(nodeToKill, nodeDeleteTimeoutMins)
		dash.VerifyFatal(err, nil, fmt.Sprintf("Valdiate node %s deletion", nodeToKill.Name))
	})

	stepLog = "Wait for 10 min. to node get replaced by autoscalling group"
	Step(stepLog, func() {
		log.Info(stepLog)
		time.Sleep(10 * time.Minute)
	})

	err := Inst().S.RefreshNodeRegistry()
	dash.VerifyFatal(err, nil, "Verify node registry refresh")

	err = Inst().V.RefreshDriverEndpoints()
	dash.VerifyFatal(err, nil, "Verfiy driver end points refresh")

	stepLog = fmt.Sprintf("Validate number of storage nodes after killing node [%v]", nodeToKill.Name)
	Step(stepLog, func() {
		dash.Info(stepLog)
		ValidateClusterSize(int64(len(storageDriverNodes)))
	})
}
