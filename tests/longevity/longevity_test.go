package tests

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/portworx/torpedo/pkg/log"

	. "github.com/onsi/ginkgo/v2"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

const (
	podDestroyTimeout = 5 * time.Minute
)

var _ = Describe("{Longevity}", func() {
	contexts := make([]*scheduler.Context, 0)
	var triggerLock sync.Mutex
	var emailTriggerLock sync.Mutex
	var populateDone bool
	triggerEventsChan := make(chan *EventRecord, 100)
	triggerFunctions = map[string]func(*[]*scheduler.Context, *chan *EventRecord){
		DeployApps:       TriggerDeployNewApps,
		RebootNode:       TriggerRebootNodes,
		ValidatePdsApps:  TriggerValidatePdsApps,
		CrashNode:        TriggerCrashNodes,
		CrashPXDaemon:    TriggerCrashPXDaemon,
		RestartVolDriver: TriggerRestartVolDriver,
		CrashVolDriver:   TriggerCrashVolDriver,
		HAIncrease:       TriggerHAIncrease,
		HADecrease:       TriggerHADecrease,
		VolumeClone:      TriggerVolumeClone,
		VolumeResize:     TriggerVolumeResize,
		//EmailReporter:        TriggerEmailReporter,
		AppTaskDown:                       TriggerAppTaskDown,
		AppTasksDown:                      TriggerAppTasksDown,
		AddDrive:                          TriggerAddDrive,
		CoreChecker:                       TriggerCoreChecker,
		CloudSnapShot:                     TriggerCloudSnapShot,
		LocalSnapShot:                     TriggerLocalSnapShot,
		DeleteLocalSnapShot:               TriggerDeleteLocalSnapShot,
		MetadataPoolResizeDisk:            TriggerMetadataPoolResizeDisk,
		PoolAddDisk:                       TriggerPoolAddDisk,
		UpgradeStork:                      TriggerUpgradeStork,
		VolumesDelete:                     TriggerVolumeDelete,
		UpgradeVolumeDriver:               TriggerUpgradeVolumeDriver,
		AutoFsTrim:                        TriggerAutoFsTrim,
		UpdateVolume:                      TriggerVolumeUpdate,
		UpdateIOProfile:                   TriggerVolumeIOProfileUpdate,
		RestartManyVolDriver:              TriggerRestartManyVolDriver,
		RebootManyNodes:                   TriggerRebootManyNodes,
		NodeDecommission:                  TriggerNodeDecommission,
		NodeRejoin:                        TriggerNodeRejoin,
		CsiSnapShot:                       TriggerCsiSnapShot,
		CsiSnapRestore:                    TriggerCsiSnapRestore,
		RelaxedReclaim:                    TriggerRelaxedReclaim,
		Trashcan:                          TriggerTrashcan,
		KVDBFailover:                      TriggerKVDBFailover,
		ValidateDeviceMapper:              TriggerValidateDeviceMapperCleanup,
		MetroDR:                           TriggerMetroDR,
		AsyncDR:                           TriggerAsyncDR,
		AsyncDRMigrationSchedule:          TriggerAsyncDRMigrationSchedule,
		ConfluentAsyncDR:                  TriggerConfluentAsyncDR,
		KafkaAsyncDR:                      TriggerKafkaAsyncDR,
		MongoAsyncDR:                      TriggerMongoAsyncDR,
		AsyncDRVolumeOnly:                 TriggerAsyncDRVolumeOnly,
		AutoFsTrimAsyncDR:                 TriggerAutoFsTrimAsyncDR,
		IopsBwAsyncDR:                     TriggerIopsBwAsyncDR,
		StorkApplicationBackup:            TriggerStorkApplicationBackup,
		StorkAppBkpVolResize:              TriggerStorkAppBkpVolResize,
		StorkAppBkpHaUpdate:               TriggerStorkAppBkpHaUpdate,
		StorkAppBkpPxRestart:              TriggerStorkAppBkpPxRestart,
		StorkAppBkpPoolResize:             TriggerStorkAppBkpPoolResize,
		RestartKvdbVolDriver:              TriggerRestartKvdbVolDriver,
		HAIncreaseAndReboot:               TriggerHAIncreaseAndReboot,
		AddDiskAndReboot:                  TriggerPoolAddDiskAndReboot,
		ResizeDiskAndReboot:               TriggerPoolResizeDiskAndReboot,
		AutopilotRebalance:                TriggerAutopilotPoolRebalance,
		VolumeCreatePxRestart:             TriggerVolumeCreatePXRestart,
		DeleteOldNamespaces:               TriggerDeleteOldNamespaces,
		MetroDRMigrationSchedule:          TriggerMetroDRMigrationSchedule,
		CloudSnapShotRestore:              TriggerCloudSnapshotRestore,
		LocalSnapShotRestore:              TriggerLocalSnapshotRestore,
		AggrVolDepReplResizeOps:           TriggerAggrVolDepReplResizeOps,
		AddStorageNode:                    TriggerAddOCPStorageNode,
		AddStoragelessNode:                TriggerAddOCPStoragelessNode,
		OCPStorageNodeRecycle:             TriggerOCPStorageNodeRecycle,
		HAIncreaseAndCrashPX:              TriggerHAIncreaseAndCrashPX,
		HAIncreaseAndRestartPX:            TriggerHAIncreaseAndPXRestart,
		NodeMaintenanceCycle:              TriggerNodeMaintenanceCycle,
		PoolMaintenanceCycle:              TriggerPoolMaintenanceCycle,
		StorageFullPoolExpansion:          TriggerStorageFullPoolExpansion,
		HAIncreaseWithPVCResize:           TriggerHAIncreasWithPVCResize,
		ReallocateSharedMount:             TriggerReallocSharedMount,
		CreateAndRunFioOnVcluster:         TriggerCreateAndRunFioOnVcluster,
		CreateAndRunMultipleFioOnVcluster: TriggerCreateAndRunMultipleFioOnVcluster,
		VolumeDriverDownVCluster:          TriggerVolumeDriverDownVCluster,
		SetDiscardMounts:                  TriggerSetDiscardMounts,
		ResetDiscardMounts:                TriggerResetDiscardMounts,
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
			StartTorpedoTest("PX-Longevity", "Validate PX longevity workflow", tags, 0)

			populateIntervals()
			populateDisruptiveTriggers()
			populateDone = true
		}
	})

	It("has to schedule app and introduce test triggers", func() {
		log.InfoD("schedule apps and start test triggers")
		watchLog := fmt.Sprintf("Start watch on K8S configMap [%s/%s]",
			configMapNS, testTriggersConfigMap)

		Step(watchLog, func() {
			log.InfoD(watchLog)
			err := watchConfigMap()
			if err != nil {
				log.Fatalf(fmt.Sprintf("%v", err))
			}
		})

		if pureTopologyEnabled {
			var err error
			labels, err = SetTopologyLabelsOnNodes()
			if err != nil {
				log.Fatalf(fmt.Sprintf("%v", err))
			}
			Inst().TopologyLabels = labels
		}

		Inst().IsHyperConverged = hyperConvergedTypeEnabled

		enableNFSProxyValidation()
		TriggerDeployNewApps(&contexts, &triggerEventsChan)

		var wg sync.WaitGroup
		Step("Register test triggers", func() {
			for triggerType, triggerFunc := range triggerFunctions {
				log.InfoD("Registering trigger: [%v]", triggerType)
				go testTrigger(&wg, &contexts, triggerType, triggerFunc, &triggerLock, &triggerEventsChan)
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

func enableNFSProxyValidation() {
	masterNodes := node.GetMasterNodes()
	if len(masterNodes) == 0 {
		log.Errorf("no master nodes found")
		return
	}

	masterNode := masterNodes[0]
	err := SetupProxyServer(masterNode)
	if err != nil {
		log.Errorf("error setting up proxy server on master node %s, Err:%s", masterNode.Name, err.Error())
		return
	}

	addresses := masterNode.Addresses
	if len(addresses) == 0 {
		log.Errorf("no addresses found for node [%s]", masterNode.Name)
		return
	}
	err = CreateNFSProxyStorageClass("portworx-proxy-volume-volume", addresses[0], "/exports/testnfsexportdir")
	if err != nil {
		log.Errorf("error creating storage class for proxy volume, Err: %s", err.Error())
		return
	}
	Inst().AppList = append(Inst().AppList, "nginx-proxy-deployment")

}

var _ = Describe("{UpgradeLongevity}", func() {
	var (
		triggerLock                sync.Mutex
		disruptiveTriggerLock      sync.Mutex
		emailTriggerLock           sync.Mutex
		populateDone               bool
		triggerEventsChan          = make(chan *EventRecord, 100)
		disruptiveTriggerFunctions = make(map[string]TriggerFunction)
		upgradeTriggerFunction     = make(map[string]TriggerFunction)
		wg                         sync.WaitGroup
		// upgradeExecutionThreshold determines the number of times each function needs to execute before upgrading
		upgradeExecutionThreshold int
		// disruptiveTriggerWrapper wraps a TriggerFunction with triggerLock to prevent concurrent execution of test triggers
		disruptiveTriggerWrapper func(fn TriggerFunction) TriggerFunction
		contexts                 []*scheduler.Context
	)

	JustBeforeEach(func() {
		contexts = make([]*scheduler.Context, 0)
		triggerFunctions = map[string]func(*[]*scheduler.Context, *chan *EventRecord){
			CloudSnapShot:        TriggerCloudSnapShot,
			HAIncrease:           TriggerHAIncrease,
			PoolAddDisk:          TriggerPoolAddDisk,
			LocalSnapShot:        TriggerLocalSnapShot,
			HADecrease:           TriggerHADecrease,
			VolumeResize:         TriggerVolumeResize,
			CloudSnapShotRestore: TriggerCloudSnapshotRestore,
			LocalSnapShotRestore: TriggerLocalSnapshotRestore,
			AddStorageNode:       TriggerAddOCPStorageNode,
		}
		// disruptiveTriggerFunctions are mapped to their respective handlers and are invoked by a separate testTrigger
		disruptiveTriggerFunctions = map[string]TriggerFunction{
			RebootNode:           TriggerRebootNodes,
			RestartVolDriver:     TriggerRestartVolDriver,
			CrashNode:            TriggerCrashNodes,
			RestartKvdbVolDriver: TriggerRestartKvdbVolDriver,
			NodeDecommission:     TriggerNodeDecommission,
			AppTasksDown:         TriggerAppTasksDown,
		}
		// Creating a distinct trigger to make sure email triggers at regular intervals
		emailTriggerFunction = map[string]func(){
			EmailReporter: TriggerEmailReporter,
		}
		// Creating a distinct trigger to ensure upgrade is triggered after a specified number of events have occurred
		upgradeTriggerFunction = map[string]TriggerFunction{
			UpgradeVolumeDriver: TriggerUpgradeVolumeDriver,
		}
		if !populateDone {
			tags := map[string]string{
				"longevity": "true",
			}
			StartTorpedoTest("UpgradeLongevity", "Validate upgrade longevity workflow", tags, 0)
			populateIntervals()
			populateDisruptiveTriggers()
			populateDone = true
		}
		if Inst().MinRunTimeMins != 0 {
			log.InfoD("Upgrade longevity tests timeout set to %d minutes", Inst().MinRunTimeMins)
		}
		upgradeExecutionThreshold = 1 // default value
		if val, err := strconv.Atoi(os.Getenv("LONGEVITY_UPGRADE_EXECUTION_THRESHOLD")); err == nil && val > 0 {
			upgradeExecutionThreshold = val
		}
		disruptiveTriggerWrapper = func(fn TriggerFunction) TriggerFunction {
			return func(contexts *[]*scheduler.Context, recordChan *chan *EventRecord) {
				triggerLock.Lock()
				defer triggerLock.Unlock()
				fn(contexts, recordChan)
			}
		}
	})

	It("has to schedule app and register test triggers", func() {
		Step(fmt.Sprintf("Start watch on K8S configMap [%s/%s]", configMapNS, testTriggersConfigMap), func() {
			log.InfoD("Starting watch on K8S configMap [%s/%s]", configMapNS, testTriggersConfigMap)
			err := watchConfigMap()
			log.FailOnError(err, "failed to watch on K8S configMap [%s/%s]. Err: %v", configMapNS, testTriggersConfigMap, err)
		})

		Step("Set topology labels on nodes", func() {
			log.InfoD("Setting topology labels on nodes")
			if pureTopologyEnabled {
				var err error
				labels, err = SetTopologyLabelsOnNodes()
				if err != nil {
					log.Fatalf("failed to set topology labels on nodes. Err: %v", err)
				}
				Inst().TopologyLabels = labels
			}
			Inst().IsHyperConverged = hyperConvergedTypeEnabled
		})

		Step("Deploy new apps", func() {
			log.InfoD("Deploying new apps")
			TriggerDeployNewApps(&contexts, &triggerEventsChan)
			dash.VerifySafely(len(contexts) > 0, true, "Verifying if the new apps are deployed")
		})

		Step("Register test triggers", func() {
			log.InfoD("Registering test triggers")
			for triggerType, triggerFunc := range triggerFunctions {
				log.InfoD("Registering trigger: [%v]", triggerType)
				wg.Add(1)
				go testTrigger(&wg, &contexts, triggerType, triggerFunc, &triggerLock, &triggerEventsChan)
			}
			log.InfoD("Finished registering test triggers")
		})

		Step("Register disruptive test triggers", func() {
			log.InfoD("Registering disruptive test triggers")
			for triggerType, triggerFunc := range disruptiveTriggerFunctions {
				log.InfoD("Registering disruptive trigger: [%v]", triggerType)
				wg.Add(1)
				go testTrigger(&wg, &contexts, triggerType, disruptiveTriggerWrapper(triggerFunc), &disruptiveTriggerLock, &triggerEventsChan)
			}
			log.InfoD("Finished registering disruptive test triggers")
		})

		Step("Register email trigger", func() {
			for triggerType, triggerFunc := range emailTriggerFunction {
				log.InfoD("Registering email trigger: [%v]", triggerType)
				wg.Add(1)
				go emailEventTrigger(&wg, triggerType, triggerFunc, &emailTriggerLock)
			}
			log.InfoD("Finished registering email trigger")
		})

		Step("Register upgrade trigger", func() {
			log.InfoD("Registering upgrade trigger")
			wg.Add(1)
			for triggerType, triggerFunc := range upgradeTriggerFunction {
				go func(triggerType string, triggerFunc TriggerFunction) {
					defer wg.Done()
					start := time.Now().Local()
					timeout := Inst().MinRunTimeMins * 60
					currentEndpointIndex := 0
					for {
						upgradeEndpoints := strings.Split(Inst().UpgradeStorageDriverEndpointList, ",")
						if timeout != 0 && int(time.Since(start).Seconds()) > timeout {
							log.InfoD("Longevity Tests timed out with timeout %d minutes", Inst().MinRunTimeMins)
							break
						}
						if currentEndpointIndex >= len(upgradeEndpoints) {
							continue
						}
						minTestExecCount := math.MaxInt32
						// Iterating over triggerFunctions to calculate testExecSum and minTestExecCount
						for trigger := range triggerFunctions {
							count := TestExecutionCounter.GetCount(trigger)
							if count < minTestExecCount {
								minTestExecCount = count
							}
						}
						// Iterating over disruptiveTriggerFunctions to update testExecSum and minTestExecCount
						for trigger := range disruptiveTriggerFunctions {
							if ChaosMap[trigger] != 0 {
								count := TestExecutionCounter.GetCount(trigger)
								if count < minTestExecCount {
									minTestExecCount = count
								}
							}
						}
						if minTestExecCount >= (currentEndpointIndex+1)*upgradeExecutionThreshold {
							Inst().UpgradeStorageDriverEndpointList = upgradeEndpoints[currentEndpointIndex]
							currentEndpointIndex++
							log.Infof("Waiting for lock for trigger [%s]\n", triggerType)
							// Using disruptiveTriggerLock to avoid concurrent execution with any running disruptive test
							disruptiveTriggerLock.Lock()
							log.Infof("Successfully taken lock for trigger [%s]\n", triggerType)
							log.Warnf("Triggering function %s based on TextExecutionCountMap: %+v", triggerType, TestExecutionCounter)
							triggerFunc(&contexts, &triggerEventsChan)
							log.Infof("Trigger Function completed for [%s]\n", triggerType)
							disruptiveTriggerLock.Unlock()
							log.Infof("Successfully released lock for trigger [%s]\n", triggerType)
							Inst().UpgradeStorageDriverEndpointList = strings.Join(upgradeEndpoints, ",")
						}
						time.Sleep(controlLoopSleepTime)
					}
				}(triggerType, triggerFunc)
			}
			log.InfoD("Finished registering upgrade trigger")
		})

		Step("Collect events while waiting for the triggers to be completed", func() {
			log.InfoD("Collecting events while waiting for the triggers to be completed")
			go CollectEventRecords(&triggerEventsChan)
			wg.Wait()
			close(triggerEventsChan)
		})

		Step("teardown all apps", func() {
			log.InfoD("tearing down all apps")
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

func testTrigger(wg *sync.WaitGroup,
	contexts *[]*scheduler.Context,
	triggerType string,
	triggerFunc func(*[]*scheduler.Context, *chan *EventRecord),
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
			log.InfoD("Longevity Tests timed out with timeout %d  minutes", minRunTime)
			break
		}

		// Get next interval of when trigger should happen
		// This interval can dynamically change by editing configMap
		waitTime, isTriggerEnabled := isTriggerEnabled(triggerType)

		if isTriggerEnabled && time.Since(lastInvocationTime) > time.Duration(waitTime) {
			// If trigger is not disabled and its right time to trigger,

			log.Infof("Waiting for lock for trigger [%s]\n", triggerType)
			triggerLoc.Lock()
			log.Infof("Successfully taken lock for trigger [%s]\n", triggerType)
			/* PTX-2667: check no other disruptive trigger is happening at same time
			if isDisruptiveTrigger(triggerType) {
			   // At a give point in time, only single disruptive trigger is allowed to run.
			   // No other disruptive or non-disruptive trigger can run at this time.
			   triggerLoc.Lock()
			} else {
			   // If trigger is non-disruptive then just check if no other disruptive trigger is running or not
			   // and release the lock immediately so that other non-disruptive triggers can happen.
				triggerLoc.Lock()
				log.Infof("===No other disruptive event happening. Able to take lock for [%s]\n", triggerType)
				triggerLoc.Unlock()
				log.Infof("===Releasing lock for non-disruptive event [%s]\n", triggerType)
			}*/

			triggerFunc(contexts, triggerEventsChan)
			log.Infof("Trigger Function completed for [%s]\n", triggerType)

			//if isDisruptiveTrigger(triggerType) {
			triggerLoc.Unlock()
			log.Infof("Successfully released lock for trigger [%s]\n", triggerType)
			//}

			lastInvocationTime = time.Now().Local()

		}
		time.Sleep(controlLoopSleepTime)
	}
	os.Exit(0)
}
