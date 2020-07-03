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
	restartVolDriver = "restartVolDriver"
	crashVolDriver   = "crashVolDriver"
	rebootNode       = "rebootNode"
	deleteApp        = "deleteApp"
)

var (
	// Stores mapping between test trigger and its chaos level.
	chaosMap map[string]int
	// Stores mapping between chaos level and its freq. Values are hardcoded
	triggerIntervalSec map[string]map[int]int
	// Stores which are disruptive triggers. When disruptive triggers are happening in test,
	// other triggers are allowed to happen only after existing triggers are completed.
	disruptiveTriggers map[string]bool
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
	populateIntervals()
	populateDisruptiveTriggers()
})

var _ = Describe("{Longevity}", func() {
	var contexts []*scheduler.Context
	var triggerLock sync.Mutex
	triggerFunctions := map[string]func([]*scheduler.Context){
		rebootNode:       TriggerRebootNodes,
		deleteApp:        TriggerDeleteApps,
		restartVolDriver: TriggerRestartVolDriver,
		crashVolDriver:   TriggerCrashVolDriver,
	}
	It("has to schedule app and introduce test triggers", func() {
		contexts = make([]*scheduler.Context, 0)

		Step(fmt.Sprintf("Start watch on K8S configMap [%s] in namespace [%s]",
			testTriggersConfigMap, configMapNS), func() {
			err := watchConfigMap()
			Expect(err).NotTo(HaveOccurred())
		})

		Step("Deploy applications", func() {
			for i := 0; i < Inst().ScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("longevity-%d", i))...)
			}
			ValidateApplications(contexts)
		})

		var wg sync.WaitGroup
		Step("Register test triggers", func() {
			for triggerType, triggerFunc := range triggerFunctions {
				go testTrigger(&wg, contexts, triggerType, triggerFunc, &triggerLock)
				wg.Add(1)
			}
		})

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

func testTrigger(wg *sync.WaitGroup,
	contexts []*scheduler.Context,
	triggerType string,
	triggerFunc func([]*scheduler.Context),
	triggerLoc *sync.Mutex) {
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

		// Get next interval of when trigger should happen
		// This intercal can dynamically change by editing configMap
		waitTimeSec, err := getWaitTimeSec(triggerType)
		Expect(err).NotTo(HaveOccurred())

		logrus.Debugf("WaitTime for trigger [%s] is %d sec\n", triggerType, waitTimeSec)
		if waitTimeSec != 0 {
			// If trigger is not disabled
			if int64(time.Since(lastInvocationTime).Seconds()) > int64(time.Duration(waitTimeSec)) {
				// If its right time to trigger, check no other disruptive trigger is happening at same time
				takeLock(triggerLoc, triggerType)

				triggerFunc(contexts)

				if isDisruptiveTrigger(triggerType) {
					triggerLoc.Unlock()
				}

				lastInvocationTime = time.Now().Local()
			}
		} else {
			// if waitTimeSec is 0, then testTrigger is disabled
			logrus.Warnf("Skipping test trigger [%s], since its been disabled via configMap [%s] in namespace [%s]",
				triggerType, testTriggersConfigMap, configMapNS)
		}
		time.Sleep(time.Second * 15)
	}
}

func watchConfigMap() error {
	chaosMap = map[string]int{}
	cm, err := core.Instance().GetConfigMap(testTriggersConfigMap, configMapNS)
	if err != nil {
		return fmt.Errorf("Error reading config map: %v", err)
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
			return fmt.Errorf("Failed to watch on config map: %s due to: %v", testTriggersConfigMap, err)
		}
	}
	return nil
}

func populateDisruptiveTriggers() {
	disruptiveTriggers = map[string]bool{
		restartVolDriver: false,
		crashVolDriver:   false,
		rebootNode:       true,
		deleteApp:        false,
	}
}

func isDisruptiveTrigger(triggerType string) bool {
	return disruptiveTriggers[triggerType]
}

// takeLock takes lock only if no other disruptive trigger is happening at the same time.
func takeLock(triggerLoc *sync.Mutex, triggerType string) {
	if isDisruptiveTrigger(triggerType) {
		// At a give point in time, only single disruptive trigger is allowed to run.
		// No other disruptive or non-disruptive trigger can run at this time.
		triggerLoc.Lock()
	} else {
		// If trigger is non-disruptive then just check if no other disruptive trigger is running or not
		// and release the lock immidiately so that other non-disruptive triggers can happen.
		triggerLoc.Lock()
		triggerLoc.Unlock()
	}
}

func populateTriggers(triggers map[string]string) error {
	for triggerType, chaosLevel := range triggers {
		chaosLevelInt, err := strconv.Atoi(chaosLevel)
		if err != nil {
			return err
		}
		chaosMap[triggerType] = chaosLevelInt
	}
	return nil
}

func populateIntervals() {
	triggerIntervalSec = map[string]map[int]int{}
	triggerIntervalSec[rebootNode] = map[int]int{}
	triggerIntervalSec[deleteApp] = map[int]int{}
	triggerIntervalSec[crashVolDriver] = map[int]int{}
	triggerIntervalSec[restartVolDriver] = map[int]int{}

	triggerIntervalSec[rebootNode][10] = 1800 // Chaos leve 10 = 30 mins
	triggerIntervalSec[rebootNode][9] = 3600  // Chaos level 9 = 1 hr
	triggerIntervalSec[rebootNode][8] = 5400  // Chaos level 8 = 1.5 hr
	triggerIntervalSec[rebootNode][7] = 7200  // Chaos level 7 = 2 hrs
	triggerIntervalSec[rebootNode][6] = 9000  // Chaos level 6 = 2.5 hrs
	triggerIntervalSec[rebootNode][5] = 10800 // Default global chaos level, 3 hrs

	triggerIntervalSec[deleteApp][10] = 1800 // Chaos leve 10 = 30 mins
	triggerIntervalSec[deleteApp][9] = 3600  // Chaos level 9 = 1 hr
	triggerIntervalSec[deleteApp][8] = 5400  // Chaos level 8 = 1.5 hr
	triggerIntervalSec[deleteApp][7] = 7200  // Chaos level 7 = 2 hrs
	triggerIntervalSec[deleteApp][6] = 9000  // Chaos level 6 = 2.5 hrs
	triggerIntervalSec[deleteApp][5] = 10800 // Default global chaos level, 3 hrs

	triggerIntervalSec[crashVolDriver][10] = 1800 // Chaos leve 10 = 30 mins
	triggerIntervalSec[crashVolDriver][9] = 3600  // Chaos level 9 = 1 hr
	triggerIntervalSec[crashVolDriver][8] = 5400  // Chaos level 8 = 1.5 hr
	triggerIntervalSec[crashVolDriver][7] = 7200  // Chaos level 7 = 2 hrs
	triggerIntervalSec[crashVolDriver][6] = 9000  // Chaos level 6 = 2.5 hrs
	triggerIntervalSec[crashVolDriver][5] = 10800 // Default global chaos level, 3 hrs

	triggerIntervalSec[restartVolDriver][10] = 1800 // Chaos leve 10 = 30 mins
	triggerIntervalSec[restartVolDriver][9] = 3600  // Chaos level 9 = 1 hr
	triggerIntervalSec[restartVolDriver][8] = 5400  // Chaos level 8 = 1.5 hr
	triggerIntervalSec[restartVolDriver][7] = 7200  // Chaos level 7 = 2 hrs
	triggerIntervalSec[restartVolDriver][6] = 9000  // Chaos level 6 = 2.5 hrs
	triggerIntervalSec[restartVolDriver][5] = 10800 // Default global chaos level, 3 hrs

	// Chaos Level of 0 means disable test trigger
	triggerIntervalSec[deleteApp][0] = 0
	triggerIntervalSec[rebootNode][0] = 0
}

func getWaitTimeSec(triggerType string) (int, error) {
	var chaosLevel int
	var ok bool
	chaosLevel, ok = chaosMap[triggerType]
	if !ok {
		chaosLevel = Inst().ChaosLevel
		logrus.Warnf("Chaos level for trigger [%s] not found in chaos map. Using global chaos level [%d]",
			triggerType, Inst().ChaosLevel)
	}
	return (triggerIntervalSec[triggerType][chaosLevel]), nil
}

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func TestMain(m *testing.M) {
	ParseFlags()
	os.Exit(m.Run())
}
