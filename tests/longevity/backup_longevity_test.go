package tests

import (
	"fmt"
	"sync"

	"github.com/portworx/torpedo/pkg/log"

	. "github.com/onsi/ginkgo/v2"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

var _ = Describe("{BackupLongevity}", func() {

	IsBackupLongevityRun = true

	contexts := make([]*scheduler.Context, 0)
	var triggerLock sync.Mutex
	var emailTriggerLock sync.Mutex
	var populateDone bool
	triggerEventsChan := make(chan *EventRecord, 100)
	triggerBackupFunctions = map[string]func(*[]*scheduler.Context, *chan *EventRecord){
		CreatePxBackup:           TriggerCreateBackup,
		CreatePxBackupAndRestore: TriggerCreateBackupAndRestore,
		CreateRandomRestore:      TriggerCreateRandomRestore,
		DeployBackupApps:         TriggerDeployBackupApps,
		CreatePxLockedBackup:     TriggerCreateLockedBackup,
	}
	//Creating a distinct trigger to make sure email triggers at regular intervals
	emailTriggerFunction = map[string]func(){
		EmailReporter: TriggerEmailReporter,
	}

	BeforeEach(func() {
		if !populateDone {
			tags := map[string]string{
				"longevity": "true",
			}
			StartPxBackupTorpedoTest("BackupLongevityTest",
				"Longevity Run For Backup", tags, 0, ATrivedi, Q1FY24)
			populateBackupIntervals()
			// populateBackupDisruptiveTriggers()
			populateDone = true
		}
	})

	It("has to schedule app and introduce test triggers", func() {
		log.InfoD("schedule apps and start test triggers")
		watchLog := fmt.Sprintf("Start watch on K8S configMap [%s/%s]",
			configMapNS, testTriggersConfigMap)

		Step(watchLog, func() {
			log.InfoD(watchLog)
			watchConfigMap()
		})

		TriggerDeployBackupApps(&contexts, &triggerEventsChan)
		TriggerAddBackupCluster(&contexts, &triggerEventsChan)
		TriggerAddBackupCredAndBucket(&contexts, &triggerEventsChan)
		TriggerAddLockedBackupCredAndBucket(&contexts, &triggerEventsChan)

		var wg sync.WaitGroup
		Step("Register test triggers", func() {
			for triggerType, triggerFunc := range triggerBackupFunctions {
				log.InfoD("Registering trigger: [%v]", triggerType)
				go backupEventTrigger(&wg, &contexts, triggerType, triggerFunc, &triggerLock, &triggerEventsChan)
				wg.Add(1)
			}
		})
		log.InfoD("Finished registering test triggers")
		if Inst().MinRunTimeMins != 0 {
			log.InfoD("Longevity Tests  timeout set to %d  minutes", Inst().MinRunTimeMins)
		}

		Step("Register email trigger", func() {
			for triggerType, triggerFunc := range emailTriggerFunction {
				log.InfoD("Registering email trigger: [%v]", triggerType)
				go emailEventTrigger(&wg, triggerType, triggerFunc, &emailTriggerLock)
				wg.Add(1)
			}
		})
		log.InfoD("Finished registering email trigger")

		CollectEventRecords(&triggerEventsChan)
		wg.Wait()
		close(triggerEventsChan)
		Step("teardown all apps", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, nil)
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})
