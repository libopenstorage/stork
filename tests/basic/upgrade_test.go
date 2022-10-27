package tests

import (
	"fmt"
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
		StartTorpedoTest("UpgradeVolumeDriver", "Validating volume driver upgrade", nil)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})
	var contexts []*scheduler.Context

	It("upgrade volume driver and ensure everything is running fine", func() {
		dash.Info("upgrade volume driver and ensure everything is running fine")
		contexts = make([]*scheduler.Context, 0)

		storageNodes := node.GetStorageNodes()

		isCloudDrive, err := IsCloudDriveInitialised(storageNodes[0])
		dash.VerifyFatal(err, nil, "Validate cloud drive installation")

		if !isCloudDrive {
			for _, storageNode := range storageNodes {
				err := Inst().V.AddBlockDrives(&storageNode, nil)
				if err != nil && strings.Contains(err.Error(), "no block drives available to add") {
					continue
				}
				dash.VerifyFatal(err, nil, "Verify adding block drive(s)")
			}
		}
		dash.Info("Scheduling applications and validating")
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradevolumedriver-%d", i))...)
		}

		ValidateApplications(contexts)
		var timeBeforeUpgrade time.Time
		var timeAfterUpgrade time.Time

		Step("start the upgrade of volume driver", func() {
			dash.Info("start the upgrade of volume driver")

			IsOperatorBasedInstall, _ := Inst().V.IsOperatorBasedInstall()
			if IsOperatorBasedInstall {
				timeBeforeUpgrade = time.Now()
				status, err := UpgradePxStorageCluster()
				timeAfterUpgrade = time.Now()
				if status {
					dash.Info("Volume Driver upgrade is successful")
				} else {
					dash.Error("Volume Driver upgrade failed")
				}
				dash.VerifyFatal(err, nil, "Verify volume drive upgrade for operator based set up")

			} else {
				timeBeforeUpgrade = time.Now()
				err := Inst().V.UpgradeDriver(Inst().StorageDriverUpgradeEndpointURL,
					Inst().StorageDriverUpgradeEndpointVersion,
					false)
				timeAfterUpgrade = time.Now()
				dash.VerifyFatal(err, nil, "Verify volume drive upgrade for daemon set based set up")
			}

			durationInMins := int(timeAfterUpgrade.Sub(timeBeforeUpgrade).Minutes())
			expectedUpgradeTime := 9 * len(node.GetStorageDriverNodes())
			dash.VerifySafely(durationInMins <= expectedUpgradeTime, true, "Verify volume drive upgrade within expected time")
			if durationInMins <= expectedUpgradeTime {
				dash.Infof("Upgrade successfully completed in %d minutes which is within %d minutes", durationInMins, expectedUpgradeTime)
			} else {
				dash.Errorf("Upgrade took %d minutes to completed which is greater than expected time %d minutes", durationInMins, expectedUpgradeTime)
				dash.VerifySafely(durationInMins <= expectedUpgradeTime, true, "Upgrade took more than expected time to complete")
			}
		})

		Step("reinstall and validate all apps after upgrade", func() {
			dash.Info("reinstall and validate all apps after upgrade")
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradedvolumedriver-%d", i))...)
			}
			ValidateApplications(contexts)
		})

		Step("Destroy apps", func() {
			dash.Info("Destroy apps")
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
		StartTorpedoTest("UpgradeStork", "Validating stork upgrade", nil)
		runID = testrailuttils.AddRunsToMilestone(testrailID)
	})

	for i := 0; i < Inst().GlobalScaleFactor; i++ {

		It("upgrade volume driver and ensure everything is running fine", func() {
			dash.Info("upgrade volume driver and ensure everything is running fine")
			contexts = make([]*scheduler.Context, 0)
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradestorkdeployment-%d", i))...)

			ValidateApplications(contexts)

			Step("start the upgrade of stork deployment", func() {
				dash.Info("start the upgrade of stork deployment")
				err := Inst().V.UpgradeStork(Inst().StorageDriverUpgradeEndpointURL,
					Inst().StorageDriverUpgradeEndpointVersion)
				dash.VerifyFatal(err, nil, "Verify stork upgrade")
			})

			Step("validate all apps after upgrade", func() {
				dash.Info("validate all apps after upgrade")
				for _, ctx := range contexts {
					ValidateContext(ctx)
				}
			})

			Step("destroy apps", func() {
				dash.Info("Destroy apps")
				opts := make(map[string]bool)
				opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
				for _, ctx := range contexts {
					TearDownContext(ctx, opts)
				}
			})

			Step("validate stork pods after upgrade", func() {
				dash.Info("validate stork pods after upgrade")
				k8sApps := apps.Instance()

				storkDeploy, err := k8sApps.GetDeployment(storkDeploymentName, storkDeploymentNamespace)
				Expect(err).NotTo(HaveOccurred())

				err = k8sApps.ValidateDeployment(storkDeploy, k8s.DefaultTimeout, k8s.DefaultRetryInterval)
				dash.VerifyFatal(err, nil, "Verify stork deployment")
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
