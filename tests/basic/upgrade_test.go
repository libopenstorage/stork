package tests

import (
	"fmt"

	"go.uber.org/multierr"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/pkg/kvdbutils"
	"github.com/portworx/torpedo/pkg/osutils"

	"github.com/portworx/torpedo/pkg/log"

	optest "github.com/libopenstorage/operator/pkg/util/test"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"

	. "github.com/onsi/ginkgo/v2"
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
		upgradeHopsList["upgradeStork"] = "true"
		StartTorpedoTest("UpgradeStork", "Validating Stork upgrade", upgradeHopsList, 0)
		log.InfoD("Stork upgrade hops list [%s]", upgradeHopsList)
	})
	var contexts []*scheduler.Context

	It("upgrade Stork and ensure everything is running fine", func() {
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
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
		}

	})

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

			stopSignal := make(chan struct{})

			var mError error
			go doAppsValidation(contexts, stopSignal, &mError)
			defer func() {
				close(stopSignal)
			}()

			// Perform upgrade hops of volume driver based on a given list of upgradeEndpoints passed
			for _, upgradeHop := range strings.Split(Inst().UpgradeStorageDriverEndpointList, ",") {
				var volName string
				var attachedNode *node.Node
				fioJobName := "upg_vol"
				log.Infof(upgradeHop)

				if Inst().N.IsUsingSSH() {
					n := storageNodes[rand.Intn(len(storageNodes))]

					volName = fmt.Sprintf("vol-%s", time.Now().Format("01-02-15h04m05s"))
					volId, err := Inst().V.CreateVolume(volName, 53687091200, 3)
					log.FailOnError(err, "error creating vol-1")
					log.Infof("created vol %s", volId)
					out, err := Inst().V.AttachVolume(volId)
					log.FailOnError(err, "error attaching vol-1")
					log.Infof("attached vol %s", out)
					attachedNode, err = GetNodeForGivenVolumeName(volName)

					log.FailOnError(err, fmt.Sprintf("error getting  attached node for volume %s", volName))

					err = writeFIOData(volName, fioJobName, *attachedNode)
					log.FailOnError(err, fmt.Sprintf("error running fio command on node %s", n.Name))
				}

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

				if attachedNode != nil {
					err = readFIOData(volName, fioJobName, *attachedNode)
					log.FailOnError(err, fmt.Sprintf("error while reading fio data on node %s", attachedNode.Name))
				}
				if mError != nil {
					break
				}
			}
			dash.VerifyFatal(mError, nil, "validate apps during PX upgrade")
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

func doAppsValidation(contexts []*scheduler.Context, stopSignal <-chan struct{}, mError *error) {

	itr := 1
	for {
		log.Infof("Apps validation iteration: #%d", itr)
		select {
		case <-stopSignal:
			log.Infof("Exiting app validations routine")
			return
		default:
			for _, ctx := range contexts {
				errorChan := make(chan error, 50)
				ValidateContext(ctx, &errorChan)
				for err := range errorChan {
					*mError = multierr.Append(*mError, err)
				}
			}
			if *mError != nil {
				return
			}
			itr++
			time.Sleep(30 * time.Second)
		}
	}

}

func writeFIOData(volName, fioJobName string, n node.Node) error {
	mountPath := fmt.Sprintf("/var/lib/osd/mounts/%s", volName)
	creatDir := fmt.Sprintf("mkdir %s", mountPath)

	cmdConnectionOpts := node.ConnectionOpts{
		Timeout:         15 * time.Second,
		TimeBeforeRetry: 5 * time.Second,
		Sudo:            true,
	}

	log.Infof("Running command %s on %s", creatDir, n.Name)
	_, err := Inst().N.RunCommandWithNoRetry(n, creatDir, cmdConnectionOpts)

	if err != nil {
		return err
	}

	mountCmd := fmt.Sprintf("pxctl host mount --path %s %s", mountPath, volName)
	log.Infof("Running command %s on %s", mountCmd, n.Name)
	_, err = Inst().N.RunCommandWithNoRetry(n, mountCmd, cmdConnectionOpts)

	if err != nil {
		return err
	}

	writeCmd := fmt.Sprintf("fio --name=%s --ioengine=libaio --rw=write --bs=4k --numjobs=1 --size=5G --iodepth=256 --directory=%s --output=/tmp/vol_write.log --verify=meta --direct=1 --randrepeat=1 --verify_pattern=0xbeddacef --end_fsync=1", fioJobName, mountPath)

	log.Infof("Running command %s on %s", writeCmd, n.Name)
	_, err = Inst().N.RunCommandWithNoRetry(n, writeCmd, cmdConnectionOpts)

	if err != nil {
		return err
	}

	return nil

}

func readFIOData(volName, fioJobName string, n node.Node) error {
	mountPath := fmt.Sprintf("/var/lib/osd/mounts/%s", volName)

	cmdConnectionOpts := node.ConnectionOpts{
		Timeout:         15 * time.Second,
		TimeBeforeRetry: 5 * time.Second,
		Sudo:            true,
	}

	readCmd := fmt.Sprintf("fio --name=%s --ioengine=libaio --rw=read --bs=4k --numjobs=1 --size=5G --iodepth=256 --directory=%s --output=/tmp/vol_read.log --do_verify=1 --verify=meta --direct=1 --randrepeat=1 --verify_pattern=0xbeddacef --end_fsync=1", fioJobName, mountPath)

	log.Infof("Running command %s on %s", readCmd, n.Name)
	_, err = Inst().N.RunCommandWithNoRetry(n, readCmd, cmdConnectionOpts)

	if err != nil {
		return err
	}

	validationCmd := "cat /tmp/vol_write.log | grep \"error\\|bad magic header\" | wc -l"
	log.Infof("Running command %s on %s", validationCmd, n.Name)
	out, err := Inst().N.RunCommandWithNoRetry(n, validationCmd, cmdConnectionOpts)

	if err != nil {
		return err
	}

	out = strings.TrimSpace(out)
	errCount, err := strconv.Atoi(out)
	if err != nil {
		return err
	}

	if errCount > 0 {
		return fmt.Errorf("error reading fio data after upgrade")
	}

	return nil

}

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

				log.Infof("Adding ibm helm repo [%s]", IBMHelmRepoName)
				cmd := fmt.Sprintf("helm repo add %s %s", IBMHelmRepoName, IBMHelmRepoURL)
				log.Infof("helm command: %v ", cmd)
				_, _, err := osutils.ExecShell(cmd)
				log.FailOnError(err, fmt.Sprintf("error adding repo [%s]", IBMHelmRepoName))
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

				if f, err := osutils.FileExists(IBMHelmValuesFile); err != nil {
					log.FailOnError(err, "error checking for file [%s]", IBMHelmValuesFile)
				} else {
					if f != nil {
						_, err = osutils.DeleteFile(IBMHelmValuesFile)
						log.FailOnError(err, "error deleting file [%s]", IBMHelmValuesFile)
					}
				}

				pxNamespace, err := Inst().V.GetVolumeDriverNamespace()
				if err != nil {
					log.Errorf("Error in getting portworx namespace. Err: %v", err.Error())
					return
				}
				cmd := fmt.Sprintf("helm get values portworx -n %s > %s", pxNamespace, IBMHelmValuesFile)
				log.Infof("Running command: %v ", cmd)
				_, _, err = osutils.ExecShell(cmd)
				log.FailOnError(err, fmt.Sprintf("error getting values for portworx helm chart"))

				f, err := osutils.FileExists(IBMHelmValuesFile)
				if err != nil {
					log.FailOnError(err, "error checking for file [%s]", IBMHelmValuesFile)
				}
				if f == nil {
					log.FailOnError(err, "file [%s] does not exist", IBMHelmValuesFile)
				}

				cmd = fmt.Sprintf("sed -i 's/imageVersion.*/imageVersion: %s/' %s", nextPXVersion, IBMHelmValuesFile)
				log.Infof("Running command: %v ", cmd)
				_, _, err = osutils.ExecShell(cmd)
				log.FailOnError(err, fmt.Sprintf("error updating px version in [%s]", IBMHelmValuesFile))

				cmd = fmt.Sprintf("helm upgrade portworx -n %s -f %s %s/portworx --debug", pxNamespace, IBMHelmValuesFile, IBMHelmRepoName)
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

				storageClusterValidateTimeout := time.Duration(len(node.GetStorageDriverNodes())*9) * time.Minute
				err = optest.ValidateStorageCluster(imageList, stc, storageClusterValidateTimeout, defaultRetryInterval, true)
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

var _ = Describe("{UpgradePxKvdbMemberDown}", func() {
	/*
					https://purestorage.atlassian.net/browse/PTX-21450
				    https://portworx.testrail.net/index.php?/cases/view/94371
					1. Bring down a kvdb member node
			        2. Upgrade PX
			        3. Bring up the kvdb member node
		            4. Verify if the kvdb member joins the cluster and everything is running fine
	*/

	JustBeforeEach(func() {
		upgradeHopsList := make(map[string]string)
		upgradeHopsList["upgradeHops"] = Inst().UpgradeStorageDriverEndpointList
		upgradeHopsList["upgradeVolumeDriver"] = "true"
		StartTorpedoTest("UpgradeVolumeDriver", "Validating volume driver upgrade", upgradeHopsList, 0)
		log.InfoD("Volume driver upgrade hops list [%s]", upgradeHopsList)
	})

	var contexts []*scheduler.Context

	var timeBeforeUpgrade time.Time
	var timeAfterUpgrade time.Time
	var kvdbNode node.Node

	itLog := "Upgrade PX when kvdb member is down and after upgrade the kvdb member should join the kvdb cluster"
	It(itLog, func() {
		log.InfoD(itLog)
		storageNodes, err := GetStorageNodes()
		log.FailOnError(err, "Failed to get storage nodes")
		numOfNodes := len(storageNodes)

		stepLog := "schedule applications"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("upgradevolumedriver-%d", i))...)
			}
		})
		ValidateApplications(contexts)
		defer DestroyApps(contexts, nil)

		dash.VerifyFatal(len(Inst().UpgradeStorageDriverEndpointList) > 0, true, "Verify upgrade storage driver endpoint list is not empty")

		for _, upgradeHop := range strings.Split(Inst().UpgradeStorageDriverEndpointList, ",") {
			stepLog = "Add labels to non kvdb nodes so that they don't try to become kvdb node"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				for _, n := range storageNodes {
					kvdbFlag, err := IsKVDBNode(n)
					log.FailOnError(err, "Failed to check if node is kvdb node")
					if !kvdbFlag {
						err = Inst().S.AddLabelOnNode(n, k8s.MetaDataLabel, "false")
						log.FailOnError(err, "Failed to add labels to node : %s", n.Name)
						log.InfoD("Added label: %v to node: %s", k8s.MetaDataLabel, n.Name)
					}
				}
			})

			stepLog = "Bring down a kvdb member node"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				kvdbNodes, err := GetAllKvdbNodes()
				log.FailOnError(err, "Failed to get kvdb nodes")

				if len(kvdbNodes) < 2 {
					log.FailOnError(fmt.Errorf("KVDB nodes are less than 2"), "KVDB nodes are less than 2")
				}

				kvdbNode, err = node.GetNodeDetailsByNodeID(kvdbNodes[rand.Intn(len(kvdbNodes))-1].ID)
				log.FailOnError(err, "Failed to get kvdb node details")
				log.InfoD("KVDB node to be stopped: %s", kvdbNode.Name)

				err = Inst().V.StopDriver([]node.Node{kvdbNode}, false, nil)
				log.FailOnError(err, "Failed to stop kvdb node")
				log.InfoD("Stopped kvdb node: %s", kvdbNode.Name)
			})

			var wg sync.WaitGroup

			stepLog = "Upgrade PX"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				wg.Add(1)
				go func() {
					defer wg.Done()
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
					expectedUpgradeTime := 15 * len(node.GetStorageDriverNodes())
					dash.VerifySafely(durationInMins <= expectedUpgradeTime, true, "Verify volume drive upgrade within expected time")
					upgradeStatus := "PASS"
					if durationInMins <= expectedUpgradeTime {
						log.InfoD("Upgrade successfully completed in %d minutes which is within %d minutes", durationInMins, expectedUpgradeTime)
					} else {
						log.Errorf("Upgrade took %d minutes to completed which is greater than expected time %d minutes", durationInMins, expectedUpgradeTime)
						dash.VerifySafely(durationInMins <= expectedUpgradeTime, true, "Upgrade took more than expected time to complete")
						upgradeStatus = "FAIL"
					}
					//check if all the versions are updated except one node.
					var count = 0
					for _, n := range storageNodes {
						updatedPXVersion, err := Inst().V.GetDriverVersionOnNode(n)
						if err != nil {
							log.Warnf("error getting driver version, Err: %v", err)
						}
						if updatedPXVersion == currPXVersion {
							count++
						}
					}

					if count == 1 {
						log.Infof("All nodes are updated except the kvdb node")
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
				}()
			})
			stepLog = "Remove metadatalabel from one of the non kvdb node so that it can become kvdb node"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				// Get kvdb members before we remove the labels
				_, err := GetAllKvdbNodes()
				log.FailOnError(err, "Failed to get kvdb nodes before removing labels")

				storageNodes := node.GetStorageNodes()
				for _, n := range storageNodes {
					kvdbFlag, err := IsKVDBNode(n)
					log.FailOnError(err, "Failed to check if node is kvdb node")
					if !kvdbFlag {
						err = Inst().S.RemoveLabelOnNode(n, k8s.MetaDataLabel)
						log.FailOnError(err, "Failed to remove labels to node : %s", n.Name)
						log.InfoD("Removed label: %v to node: %s", k8s.MetaDataLabel, n.Name)
						break
					}
				}

				// Wait for sometime so that the node becomes kvdb node
				time.Sleep(30 * time.Second)
				nodes, err := GetStorageNodes()
				kvdbMembers, err := Inst().V.GetKvdbMembers(nodes[0])
				log.FailOnError(err, "Failed to get kvdb members")
				err = kvdbutils.ValidateKVDBMembers(kvdbMembers)
				log.FailOnError(err, "Failed to validate kvdb members")
			})

			stepLog = "Bring up the kvdb member node which was down"
			Step(stepLog, func() {
				err := Inst().V.StartDriver(kvdbNode)
				log.FailOnError(err, "Failed to start kvdb node")
				log.InfoD("Started kvdb node: %s", kvdbNode.Name)

			})

			wg.Wait()

			stepLog = "Validate PX cluster after upgrade"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				ValidateApplications(contexts)
			})

			stepLog = "Remove metadatalabel from all the nodes"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				storageNodes := node.GetStorageNodes()
				for _, n := range storageNodes {
					err := Inst().S.RemoveLabelOnNode(n, k8s.MetaDataLabel)
					log.FailOnError(err, "Failed to remove labels to node : %s", n.Name)
					log.InfoD("Removed label: %v to node: %s", k8s.MetaDataLabel, n.Name)
				}
			})
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})

})
