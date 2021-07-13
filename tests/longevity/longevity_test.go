package tests

import (
	"fmt"
	"os"
	"strconv"
	"strings"
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
	controllLoopSleepTime = time.Second * 15
)

var (
	// Stores mapping between test trigger and its chaos level.
	chaosMap map[string]int
	// Stores mapping between chaos level and its freq. Values are hardcoded
	triggerInterval map[string]map[int]time.Duration
	// Stores which are disruptive triggers. When disruptive triggers are happening in test,
	// other triggers are allowed to happen only after existing triggers are complete.
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
		DeployApps:       TriggerDeployNewApps,
		RebootNode:       TriggerRebootNodes,
		RestartVolDriver: TriggerRestartVolDriver,
		CrashVolDriver:   TriggerCrashVolDriver,
		HAIncrease:       TriggerHAIncrease,
		HADecrease:       TriggerHADecrease,
		EmailReporter:    TriggerEmailReporter,
		AppTaskDown:      TriggerAppTaskDown,
		CoreChecker:      TriggerCoreChecker,
	}
	It("has to schedule app and introduce test triggers", func() {
		Step(fmt.Sprintf("Start watch on K8S configMap [%s/%s]",
			configMapNS, testTriggersConfigMap), func() {
			err := watchConfigMap()
			Expect(err).NotTo(HaveOccurred())
		})

		TriggerDeployNewApps([]*scheduler.Context{}, &triggerEventsChan)

		var wg sync.WaitGroup
		Step("Register test triggers", func() {
			for triggerType, triggerFunc := range triggerFunctions {
				go testTrigger(&wg, contexts, triggerType, triggerFunc, &triggerLock, &triggerEventsChan)
				wg.Add(1)
			}
		})
		CollectEventRecords(&triggerEventsChan)
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
		// This interval can dynamically change by editing configMap
		waitTime, isTriggerEnabled := isTriggerEnabled(triggerType)

		if isTriggerEnabled && time.Since(lastInvocationTime) > time.Duration(waitTime) {
			// If trigger is not disabled and its right time to trigger,

			logrus.Infof("Waiting for lock for trigger [%s]\n", triggerType)
			triggerLoc.Lock()
			logrus.Infof("Successfully taken lock for trigger [%s]\n", triggerType)
			/* PTX-2667: check no other disruptive trigger is happening at same time
			if isDisruptiveTrigger(triggerType) {
			   // At a give point in time, only single disruptive trigger is allowed to run.
			   // No other disruptive or non-disruptive trigger can run at this time.
			   triggerLoc.Lock()
			} else {
			   // If trigger is non-disruptive then just check if no other disruptive trigger is running or not
			   // and release the lock immidiately so that other non-disruptive triggers can happen.
				triggerLoc.Lock()
				logrus.Infof("===No other disruptive event happening. Able to take lock for [%s]\n", triggerType)
				triggerLoc.Unlock()
				logrus.Infof("===Releasing lock for non-disruptive event [%s]\n", triggerType)
			}*/

			triggerFunc(contexts, triggerEventsChan)

			//if isDisruptiveTrigger(triggerType) {
			triggerLoc.Unlock()
			logrus.Infof("Successfully released lock for trigger [%s]\n", triggerType)
			//}

			lastInvocationTime = time.Now().Local()

		}
		time.Sleep(controllLoopSleepTime)
	}
}

func watchConfigMap() error {
	chaosMap = map[string]int{}
	cm, err := core.Instance().GetConfigMap(testTriggersConfigMap, configMapNS)
	if err != nil {
		return fmt.Errorf("Error reading config map: %v", err)
	}
	err = populateDataFromConfigMap(&cm.Data)
	if err != nil {
		return err
	}

	// Apply watch if configMap exists
	fn := func(object runtime.Object) error {
		cm, ok := object.(*v1.ConfigMap)
		if !ok {
			err := fmt.Errorf("invalid object type on configmap watch: %v", object)
			return err
		}
		if len(cm.Data) > 0 {
			err = populateDataFromConfigMap(&cm.Data)
			if err != nil {
				return err
			}
		}
		return nil
	}

	err = core.Instance().WatchConfigMap(cm, fn)
	if err != nil {
		return fmt.Errorf("Failed to watch on config map: %s due to: %v", testTriggersConfigMap, err)
	}
	return nil
}

func populateDisruptiveTriggers() {
	disruptiveTriggers = map[string]bool{
		HAIncrease:       true,
		HADecrease:       false,
		RestartVolDriver: false,
		CrashVolDriver:   false,
		RebootNode:       true,
		EmailReporter:    false,
		AppTaskDown:      false,
		DeployApps:       false,
	}
}

func isDisruptiveTrigger(triggerType string) bool {
	return disruptiveTriggers[triggerType]
}

func populateDataFromConfigMap(configData *map[string]string) error {
	setEmailRecipients(configData)
	err := setSendGridEmailAPIKey(configData)
	if err != nil {
		return err
	}

	err = populateTriggers(configData)
	if err != nil {
		return err
	}
	return nil
}

func setEmailRecipients(configData *map[string]string) {
	// Get email recipients from configMap
	if emailRecipients, ok := (*configData)[EmailRecipientsConfigMapField]; !ok {
		logrus.Warnf("No [%s] field found in [%s] config-map in [%s] namespace."+
			"Defaulting email recipients to [%s].\n",
			EmailRecipientsConfigMapField, testTriggersConfigMap, configMapNS, DefaultEmailRecipient)
		EmailRecipients = []string{DefaultEmailRecipient}
	} else {
		EmailRecipients = strings.Split(emailRecipients, ",")
		delete(*configData, EmailRecipientsConfigMapField)
	}
}

func setSendGridEmailAPIKey(configData *map[string]string) error {
	if apiKey, ok := (*configData)[SendGridEmailAPIKeyField]; ok {
		SendGridEmailAPIKey = apiKey
		delete(*configData, SendGridEmailAPIKeyField)
		return nil
	}
	return fmt.Errorf("Failed to find [%s] field in config-map [%s] in namespace [%s]",
		SendGridEmailAPIKeyField, testTriggersConfigMap, configMapNS)
}

func populateTriggers(triggers *map[string]string) error {
	for triggerType, chaosLevel := range *triggers {
		chaosLevelInt, err := strconv.Atoi(chaosLevel)
		if err != nil {
			return fmt.Errorf("Failed to get chaos levels from configMap [%s] in [%s] namespace. Error:[%v]",
				testTriggersConfigMap, configMapNS, err)
		}
		chaosMap[triggerType] = chaosLevelInt
	}
	return nil
}

func populateIntervals() {
	triggerInterval = map[string]map[int]time.Duration{}
	triggerInterval[RebootNode] = map[int]time.Duration{}
	triggerInterval[CrashVolDriver] = map[int]time.Duration{}
	triggerInterval[RestartVolDriver] = map[int]time.Duration{}
	triggerInterval[HAIncrease] = map[int]time.Duration{}
	triggerInterval[HADecrease] = map[int]time.Duration{}
	triggerInterval[EmailReporter] = map[int]time.Duration{}
	triggerInterval[AppTaskDown] = map[int]time.Duration{}
	triggerInterval[DeployApps] = map[int]time.Duration{}
	triggerInterval[CoreChecker] = map[int]time.Duration{}

	baseInterval := 60 * time.Minute
	triggerInterval[RebootNode][10] = 1 * baseInterval
	triggerInterval[RebootNode][9] = 3 * baseInterval
	triggerInterval[RebootNode][8] = 6 * baseInterval
	triggerInterval[RebootNode][7] = 9 * baseInterval
	triggerInterval[RebootNode][6] = 12 * baseInterval
	triggerInterval[RebootNode][5] = 15 * baseInterval
	triggerInterval[RebootNode][4] = 18 * baseInterval
	triggerInterval[RebootNode][3] = 21 * baseInterval
	triggerInterval[RebootNode][2] = 24 * baseInterval
	triggerInterval[RebootNode][1] = 27 * baseInterval

	triggerInterval[CrashVolDriver][10] = 1 * baseInterval
	triggerInterval[CrashVolDriver][9] = 3 * baseInterval
	triggerInterval[CrashVolDriver][8] = 6 * baseInterval
	triggerInterval[CrashVolDriver][7] = 9 * baseInterval
	triggerInterval[CrashVolDriver][6] = 12 * baseInterval
	triggerInterval[CrashVolDriver][5] = 15 * baseInterval
	triggerInterval[CrashVolDriver][4] = 18 * baseInterval
	triggerInterval[CrashVolDriver][3] = 21 * baseInterval
	triggerInterval[CrashVolDriver][2] = 24 * baseInterval
	triggerInterval[CrashVolDriver][1] = 27 * baseInterval

	triggerInterval[RestartVolDriver][10] = 1 * baseInterval
	triggerInterval[RestartVolDriver][9] = 3 * baseInterval
	triggerInterval[RestartVolDriver][8] = 6 * baseInterval
	triggerInterval[RestartVolDriver][7] = 9 * baseInterval
	triggerInterval[RestartVolDriver][6] = 12 * baseInterval
	triggerInterval[RestartVolDriver][5] = 15 * baseInterval // Default global chaos level, 3 hrs
	triggerInterval[RestartVolDriver][4] = 18 * baseInterval
	triggerInterval[RestartVolDriver][3] = 21 * baseInterval
	triggerInterval[RestartVolDriver][2] = 24 * baseInterval
	triggerInterval[RestartVolDriver][1] = 27 * baseInterval

	triggerInterval[AppTaskDown][10] = 1 * baseInterval
	triggerInterval[AppTaskDown][9] = 3 * baseInterval
	triggerInterval[AppTaskDown][8] = 6 * baseInterval
	triggerInterval[AppTaskDown][7] = 9 * baseInterval
	triggerInterval[AppTaskDown][6] = 12 * baseInterval
	triggerInterval[AppTaskDown][5] = 15 * baseInterval // Default global chaos level, 1 hr
	triggerInterval[AppTaskDown][4] = 18 * baseInterval
	triggerInterval[AppTaskDown][3] = 21 * baseInterval
	triggerInterval[AppTaskDown][2] = 24 * baseInterval
	triggerInterval[AppTaskDown][1] = 27 * baseInterval

	triggerInterval[HAIncrease][10] = 1 * baseInterval
	triggerInterval[HAIncrease][9] = 3 * baseInterval
	triggerInterval[HAIncrease][8] = 6 * baseInterval
	triggerInterval[HAIncrease][7] = 9 * baseInterval
	triggerInterval[HAIncrease][6] = 12 * baseInterval
	triggerInterval[HAIncrease][5] = 15 * baseInterval // Default global chaos level, 1.5 hrs
	triggerInterval[HAIncrease][4] = 18 * baseInterval
	triggerInterval[HAIncrease][3] = 21 * baseInterval
	triggerInterval[HAIncrease][2] = 24 * baseInterval
	triggerInterval[HAIncrease][1] = 27 * baseInterval

	triggerInterval[HADecrease][10] = 1 * baseInterval
	triggerInterval[HADecrease][9] = 3 * baseInterval
	triggerInterval[HADecrease][8] = 6 * baseInterval
	triggerInterval[HADecrease][7] = 9 * baseInterval
	triggerInterval[HADecrease][6] = 12 * baseInterval
	triggerInterval[HADecrease][5] = 15 * baseInterval // Default global chaos level, 3 hrs
	triggerInterval[HADecrease][4] = 18 * baseInterval
	triggerInterval[HADecrease][3] = 21 * baseInterval
	triggerInterval[HADecrease][2] = 24 * baseInterval
	triggerInterval[HADecrease][1] = 27 * baseInterval

	baseInterval = 6 * time.Hour
	triggerInterval[EmailReporter][10] = 1 * baseInterval
	triggerInterval[EmailReporter][9] = 2 * baseInterval
	triggerInterval[EmailReporter][8] = 3 * baseInterval
	triggerInterval[EmailReporter][7] = 4 * baseInterval
	triggerInterval[EmailReporter][6] = 5 * baseInterval
	triggerInterval[EmailReporter][5] = 6 * baseInterval // Default global chaos level, 3 hrs
	triggerInterval[EmailReporter][4] = 7 * baseInterval
	triggerInterval[EmailReporter][3] = 8 * baseInterval
	triggerInterval[EmailReporter][2] = 9 * baseInterval
	triggerInterval[EmailReporter][1] = 10 * baseInterval

	triggerInterval[CoreChecker][10] = 1 * baseInterval
	triggerInterval[CoreChecker][9] = 2 * baseInterval
	triggerInterval[CoreChecker][8] = 3 * baseInterval
	triggerInterval[CoreChecker][7] = 4 * baseInterval
	triggerInterval[CoreChecker][6] = 5 * baseInterval
	triggerInterval[CoreChecker][5] = 6 * baseInterval // Default global chaos level, 3 hrs
	triggerInterval[CoreChecker][4] = 7 * baseInterval
	triggerInterval[CoreChecker][3] = 8 * baseInterval
	triggerInterval[CoreChecker][2] = 9 * baseInterval
	triggerInterval[CoreChecker][1] = 10 * baseInterval

	triggerInterval[DeployApps][10] = 1 * baseInterval
	triggerInterval[DeployApps][9] = 2 * baseInterval
	triggerInterval[DeployApps][8] = 3 * baseInterval
	triggerInterval[DeployApps][7] = 4 * baseInterval
	triggerInterval[DeployApps][6] = 5 * baseInterval
	triggerInterval[DeployApps][5] = 6 * baseInterval // Default global chaos level, 3 hrs
	triggerInterval[DeployApps][4] = 7 * baseInterval
	triggerInterval[DeployApps][3] = 8 * baseInterval
	triggerInterval[DeployApps][2] = 9 * baseInterval
	triggerInterval[DeployApps][1] = 10 * baseInterval

	// Chaos Level of 0 means disable test trigger
	triggerInterval[DeployApps][0] = 0
	triggerInterval[RebootNode][0] = 0
	triggerInterval[CrashVolDriver][0] = 0
	triggerInterval[HAIncrease][0] = 0
	triggerInterval[HADecrease][0] = 0
	triggerInterval[RestartVolDriver][0] = 0
	triggerInterval[AppTaskDown][0] = 0
}

func isTriggerEnabled(triggerType string) (time.Duration, bool) {
	var chaosLevel int
	var ok bool
	chaosLevel, ok = chaosMap[triggerType]
	if !ok {
		chaosLevel = Inst().ChaosLevel
		logrus.Warnf("Chaos level for trigger [%s] not found in chaos map. Using global chaos level [%d]",
			triggerType, Inst().ChaosLevel)
	}
	if triggerInterval[triggerType][chaosLevel] != 0 {
		return triggerInterval[triggerType][chaosLevel], true
	}
	return triggerInterval[triggerType][chaosLevel], false
}

var _ = AfterSuite(func() {
	PerformSystemCheck()
	ValidateCleanup()
})

func TestMain(m *testing.M) {
	ParseFlags()
	os.Exit(m.Run())
}
