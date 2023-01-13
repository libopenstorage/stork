package tests

import (
	"fmt"
	"strings"
	"time"

	"github.com/portworx/torpedo/pkg/log"

	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"

	. "github.com/onsi/ginkgo"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

var storkLabel = map[string]string{"name": "stork"}

const (
	pxctlCDListCmd = "pxctl cd list"
)

// UpgradeStork test performs upgrade hops of Stork based on a given list of upgradeEndpoints
var _ = Describe("{UpgradeStork}", func() {
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35269
	JustBeforeEach(func() {
		upgradeHopsList := make(map[string]string)
		upgradeHopsList["upgradeHops"] = Inst().UpgradeStorageDriverEndpointList
		StartTorpedoTest("UpgradeStork", "Validating Stork upgrade", upgradeHopsList, 0)
		log.InfoD("Stork upgrade hops list [%s]", upgradeHopsList)
	})
	var contexts []*scheduler.Context

	for i := 0; i < Inst().GlobalScaleFactor; i++ {

		It("upgrade Stork and ensure everything is running fine", func() {
			log.InfoD("upgrade Stork and ensure everything is running fine")
			contexts = make([]*scheduler.Context, 0)
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradestork-%d", i))...)

			ValidateApplications(contexts)

			if len(Inst().UpgradeStorageDriverEndpointList) == 0 {
				log.Fatalf("Unable to perform Stork upgrade hops, none were given")
			}

			// Perform upgrade hops of stork based on a given list of upgradeEndpoints
			for _, upgradeHop := range strings.Split(Inst().UpgradeStorageDriverEndpointList, ",") {
				Step("start the upgrade of stork deployment", func() {
					log.InfoD("start the upgrade of Stork deployment")
					err := Inst().V.UpgradeStork(upgradeHop)
					dash.VerifyFatal(err, nil, "Stork upgrade successful?")
				})

				Step("validate all apps after Stork upgrade", func() {
					log.InfoD("validate all apps after Stork upgrade")
					for _, ctx := range contexts {
						ValidateContext(ctx)
					}
				})

				Step("validate Stork pods after upgrade", func() {
					log.InfoD("validate Stork pods after upgrade")
					k8sApps := apps.Instance()

					storkDeploy, err := k8sApps.GetDeployment(storkDeploymentName, storkDeploymentNamespace)
					log.FailOnError(err, "error getting stork deployment spec")

					err = k8sApps.ValidateDeployment(storkDeploy, k8s.DefaultTimeout, k8s.DefaultRetryInterval)
					dash.VerifyFatal(err, nil, "Stork deployment successful?")
				})
			}

			Step("destroy apps", func() {
				log.InfoD("Destroy apps")
				opts := make(map[string]bool)
				opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
				for _, ctx := range contexts {
					TearDownContext(ctx, opts)
				}
			})

		})
	}

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

// UpgradeVolumeDriver test performs upgrade hops of volume driver based on a given list of upgradeEndpoints
var _ = Describe("{UpgradeVolumeDriver}", func() {
	JustBeforeEach(func() {
		upgradeHopsList := make(map[string]string)
		upgradeHopsList["upgradeHops"] = Inst().UpgradeStorageDriverEndpointList
		StartTorpedoTest("UpgradeVolumeDriver", "Validating volume driver upgrade", upgradeHopsList, 0)
		log.InfoD("Volume driver upgrade hops list [%s]", upgradeHopsList)
	})
	var contexts []*scheduler.Context

	It("upgrade volume driver and ensure everything is running fine", func() {
		log.InfoD("upgrade volume driver and ensure everything is running fine")
		contexts = make([]*scheduler.Context, 0)

		storageNodes := node.GetStorageNodes()

		//AddDrive is added to test to Vsphere Cloud drive upgrades when kvdb-device is part of storage in non-kvdb nodes
		isCloudDrive, err := IsCloudDriveInitialised(storageNodes[0])
		log.FailOnError(err, "Cloud drive installation failed")

		if !isCloudDrive {
			for _, storageNode := range storageNodes {
				err := Inst().V.AddBlockDrives(&storageNode, nil)
				if err != nil && strings.Contains(err.Error(), "no block drives available to add") {
					continue
				}
				log.FailOnError(err, "Adding block drive(s) failed.")
			}
		}

		log.InfoD("Scheduling applications and validating")
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradevolumedriver-%d", i))...)
		}

		ValidateApplications(contexts)
		currPXVersion, err := Inst().V.GetDriverVersionOnNode(storageNodes[0])
		if err != nil {
			log.Warnf(fmt.Sprintf("error getting driver version, err %v", err))
		}
		var timeBeforeUpgrade time.Time
		var timeAfterUpgrade time.Time

		Step("start the upgrade of volume driver", func() {
			log.InfoD("start the upgrade of volume driver")

			if len(Inst().UpgradeStorageDriverEndpointList) == 0 {
				log.Fatalf("Unable to perform volume driver upgrade hops, none were given")
			}

			// Perform upgrade hops of volume driver based on a given list of upgradeEndpoints passed
			for _, upgradeHop := range strings.Split(Inst().UpgradeStorageDriverEndpointList, ",") {
				timeBeforeUpgrade = time.Now()
				err := Inst().V.UpgradeDriver(upgradeHop)
				timeAfterUpgrade = time.Now()
				dash.VerifyFatal(err, nil, "Volume driver upgrade successful?")

				durationInMins := int(timeAfterUpgrade.Sub(timeBeforeUpgrade).Minutes())
				expectedUpgradeTime := 9 * len(node.GetStorageDriverNodes())
				dash.VerifySafely(durationInMins <= expectedUpgradeTime, true, "Verify volume drive upgrade within expected time")
				if durationInMins <= expectedUpgradeTime {
					log.InfoD("Upgrade successfully completed in %d minutes which is within %d minutes", durationInMins, expectedUpgradeTime)
				} else {
					log.Errorf("Upgrade took %d minutes to completed which is greater than expected time %d minutes", durationInMins, expectedUpgradeTime)
					dash.VerifySafely(durationInMins <= expectedUpgradeTime, true, "Upgrade took more than expected time to complete")
				}
				upgradeStatus := "PASS"
				if durationInMins <= expectedUpgradeTime {
					log.InfoD("Upgrade successfully completed in %d minutes which is within %d minutes", durationInMins, expectedUpgradeTime)
				} else {
					log.Errorf("Upgrade took %d minutes to completed which is greater than expected time %d minutes", durationInMins, expectedUpgradeTime)
					dash.VerifySafely(durationInMins <= expectedUpgradeTime, true, "Upgrade took more than expected time to complete")
					upgradeStatus = "FAIL"
				}
				updatedPXVersion, err := Inst().V.GetDriverVersionOnNode(storageNodes[0])
				if err != nil {
					log.Warnf(fmt.Sprintf("error getting driver version, err %v", err))
				}
				majorVersion := strings.Split(currPXVersion, "-")[0]
				statsData := make(map[string]string)
				statsData["fromVersion"] = currPXVersion
				statsData["toVersion"] = updatedPXVersion
				statsData["duration"] = fmt.Sprintf("%d mins", durationInMins)
				statsData["status"] = upgradeStatus
				dash.UpdateStats("px-upgrade-stats", "px-enterprise", "upgrade", majorVersion, statsData)

				// Validate Apps after volume driver upgrade
				ValidateApplications(contexts)
			}
		})

		Step("Destroy apps", func() {
			log.InfoD("Destroy apps")
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
})
