package tests

import (
	"fmt"
	"github.com/portworx/torpedo/pkg/log"
	"strings"
	"time"

	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/volume"

	"github.com/portworx/torpedo/pkg/testrailuttils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

var storkLabel = map[string]string{"name": "stork"}

const (
	pxctlCDListCmd = "pxctl cd list"
)

var _ = Describe("{UpgradeVolumeDriver}", func() {
	var testrailID = 35269
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35269
	var runID int
	JustBeforeEach(func() {
		tags := make(map[string]string)
		tags["upgradeTo"] = Inst().StorageDriverUpgradeEndpointVersion
		StartTorpedoTest("UpgradeVolumeDriver", "Validating volume driver upgrade", tags, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	It("upgrade volume driver and ensure everything is running fine", func() {
		log.InfoD("upgrade volume driver and ensure everything is running fine")
		contexts = make([]*scheduler.Context, 0)

		storageNodes := node.GetStorageNodes()

		isCloudDrive, err := IsCloudDriveInitialised(storageNodes[0])
		log.FailOnError(err, "Cloud drive installation failed")

		if !isCloudDrive {
			for _, storageNode := range storageNodes {
				err := Inst().V.AddBlockDrives(&storageNode, nil)
				if err != nil && strings.Contains(err.Error(), "no block drives available to add") {
					continue
				}
				log.Fatalf("Adding block drive(s) failed. ERR: %v", err)
			}
		}
		log.InfoD("Scheduling applications and validating")
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradevolumedriver-%d", i))...)
		}

		ValidateApplications(contexts)
		var timeBeforeUpgrade time.Time
		var timeAfterUpgrade time.Time

		Step("start the upgrade of volume driver", func() {
			log.InfoD("start the upgrade of volume driver")

			IsOperatorBasedInstall, _ := Inst().V.IsOperatorBasedInstall()
			if IsOperatorBasedInstall {
				timeBeforeUpgrade = time.Now()
				status, err := UpgradePxStorageCluster()
				timeAfterUpgrade = time.Now()
				if err != nil {
					log.Fatalf("Failed to Upgrade Px Storage Cluster. ERR: %v", err)
				}
				dash.VerifyFatal(status, true, "Volume driver upgrade successful?")

			} else {
				timeBeforeUpgrade = time.Now()
				err := Inst().V.UpgradeDriver(Inst().StorageDriverUpgradeEndpointURL,
					Inst().StorageDriverUpgradeEndpointVersion,
					false)
				timeAfterUpgrade = time.Now()
				dash.VerifyFatal(err, nil, "Volume drive upgrade for daemon set based set up successful?")
			}

			durationInMins := int(timeAfterUpgrade.Sub(timeBeforeUpgrade).Minutes())
			expectedUpgradeTime := 9 * len(node.GetStorageDriverNodes())
			dash.VerifySafely(durationInMins <= expectedUpgradeTime, true, "Verify volume drive upgrade within expected time")
			if durationInMins <= expectedUpgradeTime {
				log.InfoD("Upgrade successfully completed in %d minutes which is within %d minutes", durationInMins, expectedUpgradeTime)
			} else {
				log.Errorf("Upgrade took %d minutes to completed which is greater than expected time %d minutes", durationInMins, expectedUpgradeTime)
				dash.VerifySafely(durationInMins <= expectedUpgradeTime, true, "Upgrade took more than expected time to complete")
			}
		})

		Step("reinstall and validate all apps after upgrade", func() {
			log.InfoD("reinstall and validate all apps after upgrade")
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradedvolumedriver-%d", i))...)
			}
			ValidateApplications(contexts)
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
		AfterEachTest(contexts, testrailID, runID)
	})
})

var _ = Describe("{UpgradeStork}", func() {
	var testrailID = 11111
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35269
	var runID int
	var contexts []*scheduler.Context
	JustBeforeEach(func() {
		tags := make(map[string]string)
		tags["upgradeTo"] = Inst().StorageDriverUpgradeEndpointVersion
		StartTorpedoTest("UpgradeStork", "Validating stork upgrade", tags, testrailID)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	for i := 0; i < Inst().GlobalScaleFactor; i++ {

		It("upgrade volume driver and ensure everything is running fine", func() {
			log.InfoD("upgrade volume driver and ensure everything is running fine")
			contexts = make([]*scheduler.Context, 0)
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradestorkdeployment-%d", i))...)

			ValidateApplications(contexts)

			Step("start the upgrade of stork deployment", func() {
				log.InfoD("start the upgrade of stork deployment")
				err := Inst().V.UpgradeStork(Inst().StorageDriverUpgradeEndpointURL,
					Inst().StorageDriverUpgradeEndpointVersion)
				dash.VerifyFatal(err, nil, "Stork upgrade successful?")
			})

			Step("validate all apps after upgrade", func() {
				log.InfoD("validate all apps after upgrade")
				for _, ctx := range contexts {
					ValidateContext(ctx)
				}
			})

			Step("destroy apps", func() {
				log.InfoD("Destroy apps")
				opts := make(map[string]bool)
				opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
				for _, ctx := range contexts {
					TearDownContext(ctx, opts)
				}
			})

			Step("validate stork pods after upgrade", func() {
				log.InfoD("validate stork pods after upgrade")
				k8sApps := apps.Instance()

				storkDeploy, err := k8sApps.GetDeployment(storkDeploymentName, storkDeploymentNamespace)
				Expect(err).NotTo(HaveOccurred())

				err = k8sApps.ValidateDeployment(storkDeploy, k8s.DefaultTimeout, k8s.DefaultRetryInterval)
				dash.VerifyFatal(err, nil, "Stork deployment successful?")
			})

		})
	}
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts, testrailID, runID)
	})
})

func getImages(version string) []volume.Image {
	images := make([]volume.Image, 0)
	for _, imagestr := range strings.Split(version, ",") {
		image := strings.Split(imagestr, "=")
		if len(image) > 1 {
			images = append(images, volume.Image{Type: image[0], Version: image[1]})
		} else {
			images = append(images, volume.Image{Type: "", Version: image[0]})
		}

	}
	return images
}

/* We don't support downgrade volume drive, so comment it out
var _ = PDescribe("{UpgradeDowngradeVolumeDriver}", func() {
	It("upgrade and downgrade volume driver and ensure everything is running fine", func() {
		var contexts []*scheduler.Context
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradedowngradevolumedriver-%d", i))...)
		}

		ValidateApplications(contexts)

		Step("start the upgrade of volume driver", func() {
			images := getImages(Inst().StorageDriverUpgradeVersion)
			err := Inst().V.UpgradeDriver(images)
			Expect(err).NotTo(HaveOccurred())
		})

		Step("validate all apps after upgrade", func() {
			for _, ctx := range contexts {
				ValidateContext(ctx)
			}
		})

		Step("start the downgrade of volume driver", func() {
			images := getImages(Inst().StorageDriverBaseVersion)
			err := Inst().V.UpgradeDriver(images)
			Expect(err).NotTo(HaveOccurred())
		})

		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
		ValidateAndDestroy(contexts, opts)
	})
})
*/
