package tests

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	. "github.com/portworx/torpedo/tests"
)

const (
	validateReplicationUpdateTimeout = 2 * time.Hour
)

var _ = Describe("{HaIncreaseRebootTarget}", func() {
	testName := "ha-inc-reboot-tgt"
	performHaIncreaseRebootTest(testName)
})

var _ = Describe("{HaIncreaseRebootSource}", func() {
	testName := "ha-inc-reboot-src"
	performHaIncreaseRebootTest(testName)
})

func performHaIncreaseRebootTest(testName string) {
	var contexts []*scheduler.Context

	nodeRebootType := "target"
	testDesc := "HaIncreaseRebootTarget"

	if testName == "ha-inc-reboot-src" {
		nodeRebootType = "source"
		testDesc = "HaIncreaseRebootSource"

	}
	JustBeforeEach(func() {
		StartTorpedoTest(testDesc, fmt.Sprintf("Validate HA increase and reboot %s", nodeRebootType), nil)

	})
	stepLog := fmt.Sprintf("has to perform repl increase and reboot %s node", nodeRebootType)
	It(stepLog, func() {
		dash.Info(stepLog)
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", testName, i))...)
		}

		ValidateApplications(contexts)

		//Reboot target node and source node while repl increase is in progress
		stepLog = fmt.Sprintf("get a volume to  increase replication factor and reboot %s node", nodeRebootType)
		Step(stepLog, func() {
			dash.Info(stepLog)
			storageNodeMap := make(map[string]node.Node)
			storageNodes, err := GetStorageNodes()
			dash.VerifyFatal(err, nil, "Verify Get storage nodes")

			for _, n := range storageNodes {
				storageNodeMap[n.Id] = n
			}

			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				var err error
				stepLog = fmt.Sprintf("get volumes for %s app", ctx.App.Key)
				Step(stepLog, func() {
					dash.Info(stepLog)
					appVolumes, err = Inst().S.GetVolumes(ctx)
					dash.VerifyFatal(err, nil, "Verify Get volumes")
					dash.VerifyFatal(len(appVolumes) > 0, true, fmt.Sprintf("Verify length of app volmues %d", len(appVolumes)))
				})

				for _, v := range appVolumes {
					// Check if volumes are Pure FA/FB DA volumes
					isPureVol, err := Inst().V.IsPureVolume(v)
					dash.VerifyFatal(err, nil, "Validate PURE volume check")
					if isPureVol {
						dash.Warnf("Repl increase on Pure DA Volume [%s] not supported.Skiping this operation", v.Name)
						continue
					}

					currRep, err := Inst().V.GetReplicationFactor(v)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verify Get Repl factor for vil %s", v.Name))

					if currRep != 0 {
						//Reduce replication factor
						if currRep == 3 {
							log.Infof("Current replication is  3, reducing before proceeding")
							opts := volume.Options{
								ValidateReplicationUpdateTimeout: validateReplicationUpdateTimeout,
							}
							err = Inst().V.SetReplicationFactor(v, currRep-1, nil, true, opts)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Validate set repl factor to %d", currRep-1))
						}
					}

					if testName == "ha-inc-reboot-src" {
						HaIncreaseRebootSourceNode(nil, ctx, v, storageNodeMap)
					} else {
						HaIncreaseRebootTargetNode(nil, ctx, v, storageNodeMap)
					}
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
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})

}
