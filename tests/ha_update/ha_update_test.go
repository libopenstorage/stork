package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	. "github.com/portworx/torpedo/tests"
	"github.com/sirupsen/logrus"
	"os"
	"testing"
	"time"
)

const (
	validateReplicationUpdateTimeout = 2 * time.Hour
)

func TestHAUpdate(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_DecommissionNode.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo: DecommissionNode", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
})

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

	if testName == "ha-inc-reboot-src" {
		nodeRebootType = "source"

	}
	It(fmt.Sprintf("has to perform repl increase and reboot %s node", nodeRebootType), func() {
		contexts = make([]*scheduler.Context, 0)

		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("%s-%d", testName, i))...)
		}

		ValidateApplications(contexts)

		//Reboot target node and source node while repl increase is in progress
		Step(fmt.Sprintf("get a volume to  increase replication factor and reboot %s node", nodeRebootType), func() {
			storageNodeMap := make(map[string]node.Node)
			storageNodes, err := GetStorageNodes()
			Expect(err).NotTo(HaveOccurred())

			for _, n := range storageNodes {
				storageNodeMap[n.Id] = n
			}

			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				var err error
				Step(fmt.Sprintf("get volumes for %s app", ctx.App.Key), func() {
					appVolumes, err = Inst().S.GetVolumes(ctx)
					Expect(err).NotTo(HaveOccurred())
					if len(appVolumes) == 0 {
						err = fmt.Errorf("found no volumes for app %s", ctx.App.Key)
						Expect(err).NotTo(HaveOccurred())
					}
				})

				for _, v := range appVolumes {
					// Check if volumes are Pure FA/FB DA volumes
					isPureVol, err := Inst().V.IsPureVolume(v)
					Expect(err).NotTo(HaveOccurred())
					if isPureVol {
						logrus.Warningf("Repl increase on Pure DA Volume [%s] not supported.Skiping this operation", v.Name)
						continue
					}

					currRep, err := Inst().V.GetReplicationFactor(v)
					Expect(err).NotTo(HaveOccurred())

					if currRep != 0 {
						//Reduce replication factor
						if currRep == 3 {
							logrus.Infof("Current replication is  3, reducing before proceeding")
							opts := volume.Options{
								ValidateReplicationUpdateTimeout: validateReplicationUpdateTimeout,
							}
							err = Inst().V.SetReplicationFactor(v, currRep-1, nil, true, opts)
							Expect(err).NotTo(HaveOccurred())
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
		AfterEachTest(contexts)
	})

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
