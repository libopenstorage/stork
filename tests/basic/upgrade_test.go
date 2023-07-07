package tests

import (
	"fmt"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/pkg/osutils"
	"strings"
	"time"

	"github.com/portworx/torpedo/pkg/log"

	optest "github.com/libopenstorage/operator/pkg/util/test"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"

	. "github.com/onsi/ginkgo"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

var storkLabel = map[string]string{"name": "stork"}

const (
	pxctlCDListCmd  = "pxctl cd list"
	ibmHelmRepoName = "ibm-helm-portworx"
	ibmHelmRepoURL  = "https://raw.githubusercontent.com/portworx/ibm-helm/master/repo/stable"
	helmValuesFile  = "/tmp/values.yaml"
)

const (
	validateStorageClusterTimeout = 40 * time.Minute
)

// UpgradeStork test performs upgrade hops of Stork based on a given list of upgradeEndpoints
var _ = Describe("{UpgradeStork}", func() {
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/35269
	JustBeforeEach(func() {
		upgradeHopsList := make(map[string]string)
		upgradeHopsList["upgradeHops"] = Inst().UpgradeStorageDriverEndpointList
		upgradeHopsList["upgradeStork"] = "true"
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
		upgradeHopsList["upgradeVolumeDriver"] = "true"
		StartTorpedoTest("UpgradeVolumeDriver", "Validating volume driver upgrade", upgradeHopsList, 0)
		log.InfoD("Volume driver upgrade hops list [%s]", upgradeHopsList)
	})
	var contexts []*scheduler.Context

	It("upgrade volume driver and ensure everything is running fine", func() {
		log.InfoD("upgrade volume driver and ensure everything is running fine")
		contexts = make([]*scheduler.Context, 0)

		storageNodes := node.GetStorageNodes()
		numOfNodes := len(node.GetStorageDriverNodes())

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
		var timeBeforeUpgrade time.Time
		var timeAfterUpgrade time.Time

		Step("start the upgrade of volume driver", func() {
			log.InfoD("start the upgrade of volume driver")

			if len(Inst().UpgradeStorageDriverEndpointList) == 0 {
				log.Fatalf("Unable to perform volume driver upgrade hops, none were given")
			}

			// Perform upgrade hops of volume driver based on a given list of upgradeEndpoints passed
			for _, upgradeHop := range strings.Split(Inst().UpgradeStorageDriverEndpointList, ",") {
				currPXVersion, err := Inst().V.GetDriverVersionOnNode(storageNodes[0])
				if err != nil {
					log.Warnf("error getting driver version, Err: %v", err)
				}
				timeBeforeUpgrade = time.Now()
				isDmthinBeforeUpgrade, errDmthinCheck := IsDMthin()
				dash.VerifyFatal(errDmthinCheck, nil, "verified is setup dmthin before upgrade? ")
				err = Inst().V.UpgradeDriver(upgradeHop)
				timeAfterUpgrade = time.Now()
				dash.VerifyFatal(err, nil, "Volume driver upgrade successful?")

				durationInMins := int(timeAfterUpgrade.Sub(timeBeforeUpgrade).Minutes())
				expectedUpgradeTime := 9 * len(node.GetStorageDriverNodes())
				dash.VerifySafely(durationInMins <= expectedUpgradeTime, true, "Verify volume drive upgrade within expected time")
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
					log.Warnf("error getting driver version, Err: %v", err)
				}
				isDmthinAfterUpgrade, errDmthinCheck := IsDMthin()
				dash.VerifyFatal(errDmthinCheck, nil, "verified is setup dmthin after upgrade? ")
				dash.VerifyFatal(isDmthinBeforeUpgrade, isDmthinAfterUpgrade, "setup type remained same pre and post upgrade")
				majorVersion := strings.Split(currPXVersion, "-")[0]
				statsData := make(map[string]string)
				statsData["numOfNodes"] = fmt.Sprintf("%d", numOfNodes)
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

// UpgradeVolumeDriverFromCatalog test performs upgrade hops of volume driver based on a given list of upgradeEndpoints from marketplace
var _ = Describe("{UpgradeVolumeDriverFromCatalog}", func() {
	JustBeforeEach(func() {
		upgradeHopsList := make(map[string]string)
		upgradeHopsList["upgradeHops"] = Inst().UpgradeStorageDriverEndpointList
		upgradeHopsList["upgradeVolumeDriver"] = "true"
		upgradeHopsList["marketplace"] = "true"
		StartTorpedoTest("UpgradeVolumeDriverFromCatalog", "Validating volume driver upgrade from catalog", upgradeHopsList, 0)
		log.InfoD("Volume driver upgrade hops list [%s]", upgradeHopsList)
	})
	var contexts []*scheduler.Context

	stepLog := "upgrade volume driver from catalog and ensure everything is running fine"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)

		storageNodes := node.GetStorageNodes()
		numOfNodes := len(node.GetStorageDriverNodes())

		log.InfoD("Scheduling applications and validating")
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradevolumedriver-%d", i))...)
		}

		ValidateApplications(contexts)
		var timeBeforeUpgrade time.Time
		var timeAfterUpgrade time.Time

		stepLog = "start the upgrade of volume driver from catalog"
		Step(stepLog, func() {
			log.InfoD(stepLog)

			if len(Inst().UpgradeStorageDriverEndpointList) == 0 {
				log.Fatalf("Unable to perform volume driver upgrade hops, none were given")
			}

			if IsIksCluster() {

				log.Infof("Adding ibm helm repo [%s]", ibmHelmRepoName)
				cmd := fmt.Sprintf("helm repo add %s %s", ibmHelmRepoName, ibmHelmRepoURL)
				log.Infof("helm command: %v ", cmd)
				_, _, err := osutils.ExecShell(cmd)
				log.FailOnError(err, fmt.Sprintf("error adding repo [%s]", ibmHelmRepoName))
			}

			// Perform upgrade hops of volume driver based on a given list of upgradeEndpoints passed
			for _, upgradeHop := range strings.Split(Inst().UpgradeStorageDriverEndpointList, ",") {
				dash.VerifyFatal(upgradeHop != "", true, fmt.Sprintf("Verify the Spec Generator URL [%s] is passed", upgradeHop))

				currPXVersion, err := Inst().V.GetDriverVersionOnNode(storageNodes[0])
				if err != nil {
					log.Warnf("error getting driver version, Err: %v", err)
				}
				timeBeforeUpgrade = time.Now()
				upgradeHopSplit := strings.Split(upgradeHop, "/")
				nextPXVersion := upgradeHopSplit[len(upgradeHopSplit)-1]

				if f, err := osutils.FileExists(helmValuesFile); err != nil {
					log.FailOnError(err, "error checking for file [%s]", helmValuesFile)
				} else {
					if f != nil {
						_, err = osutils.DeleteFile(helmValuesFile)
						log.FailOnError(err, "error deleting file [%s]", helmValuesFile)
					}
				}

				pxNamespace, err := Inst().V.GetVolumeDriverNamespace()
				if err != nil {
					log.Errorf("Error in getting portworx namespace. Err: %v", err.Error())
					return
				}
				cmd := fmt.Sprintf("helm get values portworx -n %s > %s", pxNamespace, helmValuesFile)
				log.Infof("Running command: %v ", cmd)
				_, _, err = osutils.ExecShell(cmd)
				log.FailOnError(err, fmt.Sprintf("error getting values for portworx helm chart"))

				f, err := osutils.FileExists(helmValuesFile)
				if err != nil {
					log.FailOnError(err, "error checking for file [%s]", helmValuesFile)
				}
				if f == nil {
					log.FailOnError(err, "file [%s] does not exist", helmValuesFile)
				}

				cmd = fmt.Sprintf("sed -i 's/imageVersion.*/imageVersion: %s/' %s", nextPXVersion, helmValuesFile)
				log.Infof("Running command: %v ", cmd)
				_, _, err = osutils.ExecShell(cmd)
				log.FailOnError(err, fmt.Sprintf("error updating px version in [%s]", helmValuesFile))

				cmd = fmt.Sprintf("helm upgrade portworx -n %s -f %s %s/portworx --debug", pxNamespace, helmValuesFile, ibmHelmRepoName)
				log.Infof("Running command: %v ", cmd)
				_, _, err = osutils.ExecShell(cmd)
				log.FailOnError(err, fmt.Sprintf("error running helm upgrade for portworx"))
				time.Sleep(2 * time.Minute)

				stc, err := Inst().V.GetDriver()
				log.FailOnError(err, "error getting storage cluster spec")

				k8sVersion, err := core.Instance().GetVersion()
				log.FailOnError(err, "error getting k8s version")
				imageList, err := optest.GetImagesFromVersionURL(upgradeHop, k8sVersion.String())
				log.FailOnError(err, "error getting images using URL [%s] and k8s version [%s]", upgradeHop, k8sVersion.String())

				err = optest.ValidateStorageCluster(imageList, stc, validateStorageClusterTimeout, defaultRetryInterval, true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verify PX upgrade from version [%s] to version [%s]", currPXVersion, nextPXVersion))

				timeAfterUpgrade = time.Now()

				durationInMins := int(timeAfterUpgrade.Sub(timeBeforeUpgrade).Minutes())
				expectedUpgradeTime := 9 * len(node.GetStorageDriverNodes())
				dash.VerifySafely(durationInMins <= expectedUpgradeTime, true, "Verify volume drive upgrade within expected time")

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
					log.Warnf("error getting driver version, Err: %v", err)
				}
				majorVersion := strings.Split(currPXVersion, "-")[0]
				statsData := make(map[string]string)
				statsData["numOfNodes"] = fmt.Sprintf("%d", numOfNodes)
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
