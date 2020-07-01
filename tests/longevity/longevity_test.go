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
	triggerIntervalMins map[string]map[int]int
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

var _ = Describe("{Longevity}", func() {
	var contexts []*scheduler.Context
	triggerFunctions := map[string]func([]*scheduler.Context){
		rebootNode:       TriggerRebootNodes,
		deleteApp:        TriggerDeleteApps,
		restartVolDriver: TriggerRestartVolDriver,
		crashVolDriver:   TriggerCrashVolDriver,
	}
	It("has to schedule app and introduce test triggers", func() {
		contexts = make([]*scheduler.Context, 0)

		err := watchConfigMap()
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < Inst().ScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("longevity-%d", i))...)
		}

		ValidateApplications(contexts)

		var wg sync.WaitGroup
		for triggerType, triggerFunc := range triggerFunctions {
			go testTrigger(&wg, contexts, triggerType, triggerFunc)
			wg.Add(1)
		}

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
	triggerFunc func([]*scheduler.Context)) {
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

		waitTimeSec, err := getWaitTimeSec(triggerType)
		Expect(err).NotTo(HaveOccurred())

		logrus.Debugf("WaitTime for trigger [%s] is %d sec\n", triggerType, waitTimeSec)
		if waitTimeSec != 0 {
			if int64(time.Since(lastInvocationTime).Seconds()) > int64(time.Duration(waitTimeSec)) {
				triggerFunc(contexts)
				lastInvocationTime = time.Now().Local()
			}
			time.Sleep(time.Second * 15)
		} else {
			// if waitTimeSec is 0, then testTrigger is disabled
			logrus.Warnf("Skipping test trigger [%s], since its been disabled via configMap [%s] in namespace [%s]",
				triggerType, testTriggersConfigMap, configMapNS)
			time.Sleep(time.Second * 15)
		}
	}
}

func watchConfigMap() error {
	populateIntervals()
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
	triggerIntervalMins = map[string]map[int]int{}
	triggerIntervalMins[rebootNode] = map[int]int{}
	triggerIntervalMins[deleteApp] = map[int]int{}
	triggerIntervalMins[crashVolDriver] = map[int]int{}
	triggerIntervalMins[restartVolDriver] = map[int]int{}

	triggerIntervalMins[rebootNode][10] = 1800 // Chaos leve 10 = 30 mins
	triggerIntervalMins[rebootNode][9] = 3600  // Chaos level 9 = 1 hr
	triggerIntervalMins[rebootNode][8] = 5400  // Chaos level 8 = 1.5 hr
	triggerIntervalMins[rebootNode][7] = 7200  // Chaos level 7 = 2 hrs
	triggerIntervalMins[rebootNode][6] = 9000  // Chaos level 6 = 2.5 hrs
	triggerIntervalMins[rebootNode][5] = 10800 // Default global chaos level, 3 hrs

	triggerIntervalMins[deleteApp][10] = 1800 // Chaos leve 10 = 30 mins
	triggerIntervalMins[deleteApp][9] = 3600  // Chaos level 9 = 1 hr
	triggerIntervalMins[deleteApp][8] = 5400  // Chaos level 8 = 1.5 hr
	triggerIntervalMins[deleteApp][7] = 7200  // Chaos level 7 = 2 hrs
	triggerIntervalMins[deleteApp][6] = 9000  // Chaos level 6 = 2.5 hrs
	triggerIntervalMins[deleteApp][5] = 10800 // Default global chaos level, 3 hrs

	triggerIntervalMins[crashVolDriver][10] = 1800 // Chaos leve 10 = 30 mins
	triggerIntervalMins[crashVolDriver][9] = 3600  // Chaos level 9 = 1 hr
	triggerIntervalMins[crashVolDriver][8] = 5400  // Chaos level 8 = 1.5 hr
	triggerIntervalMins[crashVolDriver][7] = 7200  // Chaos level 7 = 2 hrs
	triggerIntervalMins[crashVolDriver][6] = 9000  // Chaos level 6 = 2.5 hrs
	triggerIntervalMins[crashVolDriver][5] = 10800 // Default global chaos level, 3 hrs

	triggerIntervalMins[restartVolDriver][10] = 1800 // Chaos leve 10 = 30 mins
	triggerIntervalMins[restartVolDriver][9] = 3600  // Chaos level 9 = 1 hr
	triggerIntervalMins[restartVolDriver][8] = 5400  // Chaos level 8 = 1.5 hr
	triggerIntervalMins[restartVolDriver][7] = 7200  // Chaos level 7 = 2 hrs
	triggerIntervalMins[restartVolDriver][6] = 9000  // Chaos level 6 = 2.5 hrs
	triggerIntervalMins[restartVolDriver][5] = 10800 // Default global chaos level, 3 hrs

	// Chaos Level of 0 means disable test trigger
	triggerIntervalMins[deleteApp][0] = 0
	triggerIntervalMins[rebootNode][0] = 0
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
	return (triggerIntervalMins[triggerType][chaosLevel]), nil
}

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func TestMain(m *testing.M) {
	ParseFlags()
	os.Exit(m.Run())
}
