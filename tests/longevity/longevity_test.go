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

var (
	// Stores mapping between test trigger and its chaos level.
	chaosMap map[string]int
	// Stores mapping between chaos level and its freq. Values are hardcoded
	triggerInterval map[string]map[int]time.Duration
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
	triggerEventsChan := make(chan *EventRecord, 100)
	triggerFunctions := map[string]func([]*scheduler.Context, *chan *EventRecord){
		RebootNode:       TriggerRebootNodes,
		DeleteApp:        TriggerDeleteApps,
		RestartVolDriver: TriggerRestartVolDriver,
		CrashVolDriver:   TriggerCrashVolDriver,
		HAUpdate:         TriggerHAUpdate,
		EmailReporter:    TriggerEmailReporter,
	}
	It("has to schedule app and introduce test triggers", func() {
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
				go testTrigger(&wg, contexts, triggerType, triggerFunc, &triggerLock, &triggerEventsChan)
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
	triggerFunc func([]*scheduler.Context, *chan *EventRecord),
	triggerLoc *sync.Mutex,
	triggerEventsChan *chan *EventRecord) {
	defer wg.Done()

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
		waitTime, err := getWaitTime(triggerType)
		Expect(err).NotTo(HaveOccurred())

		if waitTime != 0 {
			// If trigger is not disabled
			if time.Since(lastInvocationTime) > time.Duration(waitTime) {
				// If its right time to trigger, check no other disruptive trigger is happening at same time
				takeLock(triggerLoc, triggerType)

				triggerFunc(contexts, triggerEventsChan)

				if isDisruptiveTrigger(triggerType) {
					triggerLoc.Unlock()
				}

				lastInvocationTime = time.Now().Local()
			}
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
		RestartVolDriver: false,
		CrashVolDriver:   false,
		RebootNode:       true,
		DeleteApp:        false,
		EmailReporter:    false,
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
	triggerInterval = map[string]map[int]time.Duration{}
	triggerInterval[RebootNode] = map[int]time.Duration{}
	triggerInterval[DeleteApp] = map[int]time.Duration{}
	triggerInterval[CrashVolDriver] = map[int]time.Duration{}
	triggerInterval[RestartVolDriver] = map[int]time.Duration{}
	triggerInterval[HAUpdate] = map[int]time.Duration{}
	triggerInterval[EmailReporter] = map[int]time.Duration{}

	baseInterval := 30 * time.Minute
	triggerInterval[RebootNode][10] = 1 * baseInterval
	triggerInterval[RebootNode][9] = 2 * baseInterval
	triggerInterval[RebootNode][8] = 3 * baseInterval
	triggerInterval[RebootNode][7] = 4 * baseInterval
	triggerInterval[RebootNode][6] = 5 * baseInterval
	triggerInterval[RebootNode][5] = 6 * baseInterval // Default global chaos level, 3 hrs

	triggerInterval[CrashVolDriver][10] = 1 * baseInterval
	triggerInterval[CrashVolDriver][9] = 2 * baseInterval
	triggerInterval[CrashVolDriver][8] = 3 * baseInterval
	triggerInterval[CrashVolDriver][7] = 4 * baseInterval
	triggerInterval[CrashVolDriver][6] = 5 * baseInterval
	triggerInterval[CrashVolDriver][5] = 6 * baseInterval // Default global chaos level, 3 hrs

	triggerInterval[RestartVolDriver][10] = 1 * baseInterval
	triggerInterval[RestartVolDriver][9] = 2 * baseInterval
	triggerInterval[RestartVolDriver][8] = 3 * baseInterval
	triggerInterval[RestartVolDriver][7] = 4 * baseInterval
	triggerInterval[RestartVolDriver][6] = 5 * baseInterval
	triggerInterval[RestartVolDriver][5] = 6 * baseInterval // Default global chaos level, 3 hrs

	triggerInterval[EmailReporter][10] = 1 * baseInterval
	triggerInterval[EmailReporter][9] = 2 * baseInterval
	triggerInterval[EmailReporter][8] = 3 * baseInterval
	triggerInterval[EmailReporter][7] = 4 * baseInterval
	triggerInterval[EmailReporter][6] = 5 * baseInterval
	triggerInterval[EmailReporter][5] = 6 * baseInterval // Default global chaos level, 3 hrs

	baseInterval = 15 * time.Minute
	triggerInterval[HAUpdate][10] = 1 * baseInterval
	triggerInterval[HAUpdate][9] = 2 * baseInterval
	triggerInterval[HAUpdate][8] = 3 * baseInterval
	triggerInterval[HAUpdate][7] = 4 * baseInterval
	triggerInterval[HAUpdate][6] = 5 * baseInterval
	triggerInterval[HAUpdate][5] = 6 * baseInterval // Default global chaos level, 1.5 hrs

	baseInterval = 5 * time.Minute
	triggerInterval[DeleteApp][10] = 1 * baseInterval
	triggerInterval[DeleteApp][9] = 2 * baseInterval
	triggerInterval[DeleteApp][8] = 3 * baseInterval
	triggerInterval[DeleteApp][7] = 4 * baseInterval
	triggerInterval[DeleteApp][6] = 5 * baseInterval
	triggerInterval[DeleteApp][5] = 6 * baseInterval // Default global chaos level, 30 mins

	// Chaos Level of 0 means disable test trigger
	triggerInterval[DeleteApp][0] = 0
	triggerInterval[RebootNode][0] = 0
	triggerInterval[CrashVolDriver][0] = 0
	triggerInterval[HAUpdate][0] = 0
	triggerInterval[RestartVolDriver][0] = 0
}

func getWaitTime(triggerType string) (time.Duration, error) {
	var chaosLevel int
	var ok bool
	chaosLevel, ok = chaosMap[triggerType]
	if !ok {
		chaosLevel = Inst().ChaosLevel
		logrus.Warnf("Chaos level for trigger [%s] not found in chaos map. Using global chaos level [%d]",
			triggerType, Inst().ChaosLevel)
	}
	return (triggerInterval[triggerType][chaosLevel]), nil
}

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func TestMain(m *testing.M) {
	ParseFlags()
	os.Exit(m.Run())
}
