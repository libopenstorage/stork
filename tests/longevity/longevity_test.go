package tests

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	testTriggersConfigMap = "longevity-triggers"
	configMapNS           = "default"
)

type TestTrigger string

const (
	restartVolDriver TestTrigger = "restartVolDriver"
	rebootNode                   = "rebootNode"
	deleteApp                    = "deleteApp"
)

var (
	// Stores mapping between test trigger and its chaos level.Need to protect with lock due to runtime modification
	chaosMap map[string]int
	// Stores mapping between chaos level and its freq. Values are hardcoded
	triggerInterval map[int]int
)

func TestLongevity(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_basic.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : Longevity", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
})

// This test deletes all tasks of an application and checks if app converges back to desired state
var _ = Describe("{Longevity}", func() {
	var contexts []*scheduler.Context

	It("has to schedule app and introduce test triggers", func() {
		contexts = make([]*scheduler.Context, 0)

		err := watchConfigMap()
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("longevity-%d", i))...)
		}

		ValidateApplications(contexts)

		// TODO: Maintain a map between test triggers and their corrosponding functions
		// and based on enabled trigger types, call them one by one
		var wg sync.WaitGroup
		go deleteApps(&wg, contexts)
		wg.Add(1)

		go rebootNodes(&wg, contexts)
		wg.Add(1)

		wg.Wait()
		Step("teardown all apps", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, nil)
			}
		})
	})
	JustAfterEach(func() {
		AfterEachTest(contexts)
	})
})

func deleteApps(wg *sync.WaitGroup, contexts []*scheduler.Context) {
	defer wg.Done()
	defer GinkgoRecover()

	// Add interval based sleep here to check what time we will exit out of this delete task loop
	minRunTime := Inst().MinRunTimeMins
	timeout := (minRunTime) * 60

	start := time.Now().Local()
	lastInvocationTime := start

	for {
		// if timeout is 0, run indefinitely
		if timeout != 0 && int(time.Since(start).Seconds()) > timeout {
			break
		}

		waitTimeSec, err := getWaitTimeSec(deleteApp)
		Expect(err).NotTo(HaveOccurred())

		if waitTimeSec != 0 {
			if int64(time.Since(lastInvocationTime).Seconds()) > int64(time.Duration(waitTimeSec)) {
				Step("delete all application tasks", func() {
					for _, ctx := range contexts {
						Step(fmt.Sprintf("delete tasks for app: %s", ctx.App.Key), func() {
							err := Inst().S.DeleteTasks(ctx, nil)
							Expect(err).NotTo(HaveOccurred())
						})
						ValidateContext(ctx)
					}
					lastInvocationTime = time.Now().Local()
				})
			}
			time.Sleep(time.Second * 15)
		} else {
			// if sleepTime is 0, then testTrigger is disabled
			logrus.Warnf("Skipping test trigger [%s], since its been disabled via configMap [%s] in namespace [%s]",
				deleteApp, testTriggersConfigMap, configMapNS)
			time.Sleep(time.Second * 15)
		}
	}
}

func rebootNodes(wg *sync.WaitGroup, contexts []*scheduler.Context) {
	defer wg.Done()
	defer GinkgoRecover()
	minRunTime := Inst().MinRunTimeMins
	timeout := (minRunTime) * 60

	start := time.Now().Local()
	lastInvocationTime := start

	for {
		// if timeout is 0, run indefinitely
		if timeout != 0 && int(time.Since(start).Seconds()) > timeout {
			break
		}

		waitTimeSec, err := getWaitTimeSec(rebootNode)
		Expect(err).NotTo(HaveOccurred())

		if waitTimeSec != 0 {
			if int64(time.Since(lastInvocationTime).Seconds()) > int64(time.Duration(waitTimeSec)) {
				Step("get all nodes and reboot one by one", func() {
					nodesToReboot := node.GetWorkerNodes()

					// Reboot node and check driver status
					Step(fmt.Sprintf("reboot node one at a time from the node(s): %v", nodesToReboot), func() {
						// TODO: Below is the same code from existing nodeReboot test
						for _, n := range nodesToReboot {
							if n.IsStorageDriverInstalled {
								Step(fmt.Sprintf("reboot node: %s", n.Name), func() {
									err = Inst().N.RebootNode(n, node.RebootNodeOpts{
										Force: true,
										ConnectionOpts: node.ConnectionOpts{
											Timeout:         1 * time.Minute,
											TimeBeforeRetry: 5 * time.Second,
										},
									})
									Expect(err).NotTo(HaveOccurred())
								})

								Step(fmt.Sprintf("wait for node: %s to be back up", n.Name), func() {
									err = Inst().N.TestConnection(n, node.ConnectionOpts{
										Timeout:         15 * time.Minute,
										TimeBeforeRetry: 10 * time.Second,
									})
									Expect(err).NotTo(HaveOccurred())
								})

								Step(fmt.Sprintf("wait for volume driver to stop on node: %v", n.Name), func() {
									err := Inst().V.WaitDriverDownOnNode(n)
									Expect(err).NotTo(HaveOccurred())
								})

								Step(fmt.Sprintf("wait to scheduler: %s and volume driver: %s to start",
									Inst().S.String(), Inst().V.String()), func() {

									err = Inst().S.IsNodeReady(n)
									Expect(err).NotTo(HaveOccurred())

									err = Inst().V.WaitDriverUpOnNode(n, Inst().DriverStartTimeout)
									Expect(err).NotTo(HaveOccurred())
								})

								Step("validate apps", func() {
									for _, ctx := range contexts {
										ValidateContext(ctx)
									}
								})
							}
						}
					})
				})
			}
			time.Sleep(time.Second * 15)
		} else {
			// if sleepTime is 0, then testTrigger is disabled
			logrus.Warnf("Skipping test trigger [%s], since its been disabled via configMap [%s] in namespace [%s]",
				rebootNode, testTriggersConfigMap, configMapNS)
			time.Sleep(time.Second * 15)
		}
	}
}

func watchConfigMap() error {
	chaosMap = map[string]int{}
	cm, err := core.Instance().GetConfigMap(testTriggersConfigMap, configMapNS)
	if err != nil {
		logrus.Warnf("Error reading config map: %v", err)
		// TODO: Populate triggerInterval with default values
	}
	populateTriggers(cm.Data)

	if err == nil {
		// Apply watch if configMap exists
		fn := func(object runtime.Object) error {
			cm, ok := object.(*v1.ConfigMap)
			if !ok {
				err := fmt.Errorf("invalid object type on configmap watch: %v", object)
				return err
			}
			if len(cm.Data) > 0 {
				populateTriggers(cm.Data)
			}
			return nil
		}

		err = core.Instance().WatchConfigMap(cm, fn)
		if err != nil {
			logrus.Errorf("Failed to watch on config map: %s due to: %v", testTriggersConfigMap, err)
			return err
		}
	}
	return nil
}

func populateTriggers(triggers map[string]string) error {
	for triggerType, chaosLevel := range triggers {
		// TODO: validate triggerType is a valid type
		chaosLevelInt, err := strconv.Atoi(chaosLevel)
		if err != nil {
			return err
		}
		chaosMap[triggerType] = chaosLevelInt
	}
	return nil
}

func populateIntervals() {
	triggerInterval[10] = 900 // Chaos leve 10 = 15 mins
	triggerInterval[9] = 1800 // Chaos level 9 = 30 mins
	triggerInterval[8] = 2700 // Chaos level 8 = 45 mins
	triggerInterval[7] = 3600
	triggerInterval[6] = 4500
	triggerInterval[5] = 5400 // Default global chaos level, 1.5 hrs
	triggerInterval[4] = 6300
	triggerInterval[3] = 7200
	triggerInterval[2] = 8100
	triggerInterval[1] = 9000

	// Chaos Level of 0 means disable test trigger
	triggerInterval[0] = 0
}

func getWaitTimeSec(triggerType string) (int, error) {
	var chaosLevel int
	var ok bool
	if chaosLevel, ok = chaosMap[triggerType]; !ok {
		chaosLevel = Inst().ChaosLevel
		logrus.Warnf("Chaos level for trigger [%s] not found in chaos map. Using global chaos level [%d]",
			triggerType, Inst().ChaosLevel)
	}
	return triggerInterval[chaosLevel], nil
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
