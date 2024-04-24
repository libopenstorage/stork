package tests

import (
	context1 "context"
	"fmt"
	"net/url"
	"strings"
	"time"

	apapi "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	oputil "github.com/libopenstorage/operator/pkg/util/test"

	. "github.com/onsi/ginkgo/v2"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/aututils"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/units"
	. "github.com/portworx/torpedo/tests"
)

var _ = Describe("{AddNewDiskToKubevirtVM}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("AddNewDiskToKubevirtVM", "Add a new disk to a kubevirtVM", nil, 0)
	})
	var appCtxs []*scheduler.Context
	var namespace string
	itLog := "Add a new disk to a kubevirtVM"
	It(itLog, func() {
		defer ListEvents("portworx")
		namespace = fmt.Sprintf("kubevirt-%v", time.Now().Unix())
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		numberOfVolumes := 1
		Inst().AppList = []string{"kubevirt-debian-template"}
		stepLog := "Setting up Boot PVC Template"
		Step(stepLog, func() {
			template := ScheduleApplications("template")
			ValidateApplications(template)
		})

		Inst().AppList = []string{"kubevirt-debian-fio-minimal"}
		stepLog = "schedule a kubevirtVM"
		Step(stepLog, func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				appCtxs = append(appCtxs, ScheduleApplicationsOnNamespace(namespace, "test")...)
			}
		})
		ValidateApplications(appCtxs)
		for _, appCtx := range appCtxs {
			bindMount, err := IsVMBindMounted(appCtx, false)
			log.FailOnError(err, "Failed to verify bind mount")
			dash.VerifyFatal(bindMount, true, "Failed to verify bind mount")
		}
		stepLog = "Add one disk to the kubevirt VM"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			_, err := AddDisksToKubevirtVM(appCtxs, numberOfVolumes, "10Gi")
			log.FailOnError(err, "Failed to add disks to kubevirt VM")
			dash.VerifyFatal(true, true, "Failed to add disks to kubevirt VM?")
		})

		stepLog = "Verify the new disk added is also bind mounted"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appCtx := range appCtxs {
				isVmBindMounted, err := IsVMBindMounted(appCtx, true)
				log.FailOnError(err, "Failed to verify disks in kubevirt VM")
				if !isVmBindMounted {
					log.Errorf("The newly added disk to vm %s is not bind mounted", appCtx.App.Key)
				}
			}
		})
		stepLog = "Destroy Applications"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			DestroyApps(appCtxs, nil)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(appCtxs)
	})
})

var _ = Describe("{KubeVirtLiveMigration}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("KubeVirtLiveMigration", "Live migrate a kubevirtVM", nil, 0)
	})
	var appCtxs []*scheduler.Context
	var namespace string
	itLog := "Live migrate a kubevirtVM"
	It(itLog, func() {
		defer ListEvents("portworx")
		namespace = fmt.Sprintf("kubevirt-%v", time.Now().Unix())
		log.InfoD(stepLog)
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		Inst().AppList = []string{"kubevirt-debian-fio-minimal"}
		stepLog := "schedule a kubevirt VM"
		Step(stepLog, func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("test-%v", i)
				appCtxs = append(appCtxs, ScheduleApplicationsOnNamespace(namespace, taskName)...)
			}
		})
		ValidateApplications(appCtxs)
		for _, appCtx := range appCtxs {
			bindMount, err := IsVMBindMounted(appCtx, false)
			log.FailOnError(err, "Failed to verify bind mount")
			dash.VerifyFatal(bindMount, true, "Failed to verify bind mount")
		}
		stepLog = "Live migrate the kubevirt VM"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appCtx := range appCtxs {
				err := StartAndWaitForVMIMigration(appCtx, context1.TODO())
				log.FailOnError(err, "Failed to live migrate kubevirt VM")
			}
		})
		stepLog = "Destroy Applications"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			DestroyApps(appCtxs, nil)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(appCtxs)
	})
})

var _ = Describe("{PxKillBeforeAddDiskToVM}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PxKillBeforeAddDiskToVM", "Kill Px on host node of Kubuevirt VM and then Add a disk", nil, 0)
	})
	var appCtxs []*scheduler.Context
	var nodes []string
	var namespace string
	itLog := "Kill Px then Add disk to Kubevirt VM"
	It(itLog, func() {
		defer ListEvents("portworx")
		namespace = fmt.Sprintf("kubevirt-%v", time.Now().Unix())
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		numberOfVolumes := 1
		Inst().AppList = []string{"kubevirt-debian-fio-minimal"}
		stepLog := "schedule a kubevirtVM"
		Step(stepLog, func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				appCtxs = append(appCtxs, ScheduleApplicationsOnNamespace(namespace, "test")...)
			}
		})
		ValidateApplications(appCtxs)
		for _, appCtx := range appCtxs {
			bindMount, err := IsVMBindMounted(appCtx, false)
			log.FailOnError(err, "Failed to verify bind mount")
			dash.VerifyFatal(bindMount, true, "Failed to verify bind mount")
		}
		stepLog = "Kill Px on node hosting VM"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			// Collect all nodes to restart Px on
			for _, appCtx := range appCtxs {
				vms, err := GetAllVMsFromScheduledContexts([]*scheduler.Context{appCtx})
				log.FailOnError(err, "Failed to get VMs from context")
				for _, vm := range vms {
					nodeName, err := GetNodeOfVM(vm)
					log.FailOnError(err, "Failed to get node of vm %v", vm.Name)
					nodes = append(nodes, nodeName)
				}
			}
			// Restart Px on all relevant nodes one by one
			for _, appNode := range node.GetStorageDriverNodes() {
				for _, vmNode := range nodes {
					if vmNode == appNode.Name {
						stepLog = fmt.Sprintf("stop volume driver %s on node: %s",
							Inst().V.String(), appNode.Name)
						Step(stepLog,
							func() {
								log.InfoD(stepLog)
								StopVolDriverAndWait([]node.Node{appNode})
							})

						stepLog = fmt.Sprintf("starting volume %s driver on node %s",
							Inst().V.String(), appNode.Name)
						Step(stepLog,
							func() {
								log.InfoD(stepLog)
								StartVolDriverAndWait([]node.Node{appNode})
							})

						stepLog = "Giving few seconds for volume driver to stabilize"
						Step(stepLog, func() {
							log.InfoD(stepLog)
							time.Sleep(20 * time.Second)
						})
					}
				}
			}
		})
		stepLog = "Add one disk to the kubevirt VM"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			_, err := AddDisksToKubevirtVM(appCtxs, numberOfVolumes, "10Gi")
			log.FailOnError(err, "Failed to add disks to kubevirt VM")
			dash.VerifyFatal(true, true, "Failed to add disks to kubevirt VM?")
		})
		stepLog = "Verify the new disk added is also bind mounted"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appCtx := range appCtxs {
				isVmBindMounted, err := IsVMBindMounted(appCtx, true)
				log.FailOnError(err, "Failed to verify disks in kubevirt VM")
				if !isVmBindMounted {
					log.Errorf("The newly added disk to vm %s is not bind mounted", appCtx.App.Key)
				}
			}
		})
		stepLog = "Destroy Applications"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			DestroyApps(appCtxs, nil)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(appCtxs)
	})
})

var _ = Describe("{PxKillAfterAddDiskToVM}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("PxKillAfterAddDiskToVM", "Add a disk to Kubevirt VM, kill Px, Add another disk and validate the VM", nil, 0)
	})

	var appCtxs []*scheduler.Context
	var nodes []string
	var namespace string

	itLog := "Add disk to Kubevirt VM, Kill Px and then add another disk"
	It(itLog, func() {
		defer ListEvents("portworx")
		namespace = fmt.Sprintf("kubevirt-%v", time.Now().Unix())
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		numberOfVolumes := 1
		Inst().AppList = []string{"kubevirt-debian-fio-minimal"}
		stepLog := "schedule a kubevirtVM"
		Step(stepLog, func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				appCtxs = append(appCtxs, ScheduleApplicationsOnNamespace(namespace, "test")...)
			}
		})
		ValidateApplications(appCtxs)
		for _, appCtx := range appCtxs {
			bindMount, err := IsVMBindMounted(appCtx, false)
			log.FailOnError(err, "Failed to verify bind mount")
			dash.VerifyFatal(bindMount, true, "Failed to verify bind mount")
		}
		stepLog = "Add one disk to the kubevirt VM"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			success, err := AddDisksToKubevirtVM(appCtxs, numberOfVolumes, "10Gi")
			log.FailOnError(err, "Failed to add disks to kubevirt VM")
			dash.VerifyFatal(success, true, "Failed to add disks to kubevirt VM?")
		})
		stepLog = "Verify the new disk added is also bind mounted"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appCtx := range appCtxs {
				isVmBindMounted, err := IsVMBindMounted(appCtx, true)
				log.FailOnError(err, "Failed to verify disks in kubevirt VM")
				if !isVmBindMounted {
					log.Errorf("The newly added disk to vm %s is not bind mounted", appCtx.App.Key)
				}
			}
		})
		stepLog = "Kill Px on node hosting VM"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			// Collect all nodes to restart Px on
			for _, appCtx := range appCtxs {
				vms, err := GetAllVMsFromScheduledContexts([]*scheduler.Context{appCtx})
				log.FailOnError(err, "Failed to get VMs from context")
				for _, vm := range vms {
					nodeName, err := GetNodeOfVM(vm)
					log.FailOnError(err, "Failed to get node of vm %v", vm.Name)
					nodes = append(nodes, nodeName)
				}
			}
			// Restart Px on all relevant nodes one by one
			for _, appNode := range node.GetStorageDriverNodes() {
				for _, vmNode := range nodes {
					if vmNode == appNode.Name {
						stepLog = fmt.Sprintf("stop volume driver %s on node: %s",
							Inst().V.String(), appNode.Name)
						Step(stepLog,
							func() {
								log.InfoD(stepLog)
								StopVolDriverAndWait([]node.Node{appNode})
							})

						stepLog = fmt.Sprintf("starting volume %s driver on node %s",
							Inst().V.String(), appNode.Name)
						Step(stepLog,
							func() {
								log.InfoD(stepLog)
								StartVolDriverAndWait([]node.Node{appNode})
							})

						stepLog = "Giving few seconds for volume driver to stabilize"
						Step(stepLog, func() {
							log.InfoD(stepLog)
							time.Sleep(20 * time.Second)
						})
					}
				}
			}
		})
		stepLog = "Verify the disks added are still bind mounted"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appCtx := range appCtxs {
				isVmBindMounted, err := IsVMBindMounted(appCtx, true)
				log.FailOnError(err, "Failed to verify disks in kubevirt VM")
				if !isVmBindMounted {
					log.Errorf("The newly added disk to vm %s is not bind mounted", appCtx.App.Key)
				}
			}
		})
		stepLog = "Destroy Applications"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			DestroyApps(appCtxs, nil)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(appCtxs)
	})
})

var _ = Describe("{KubevirtVMVolHaIncrease}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("KubevirtVMVolHaIncrease", "Increase the volume HA of a kubevirt VM", nil, 0)
	})
	var appCtxs []*scheduler.Context
	var namespace string
	itLog := "Increase the volume HA of a kubevirt VM"
	It(itLog, func() {
		defer ListEvents("portworx")
		namespace = fmt.Sprintf("kubevirt-%v", time.Now().Unix())
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		Inst().AppList = []string{"kubevirt-debian-fio-low-ha"}
		stepLog := "schedule a kubevirt VM"
		Step(stepLog, func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				appCtxs = append(appCtxs, ScheduleApplicationsOnNamespace(namespace, "test")...)
			}
		})
		ValidateApplications(appCtxs)
		for _, appCtx := range appCtxs {
			bindMount, err := IsVMBindMounted(appCtx, false)
			log.FailOnError(err, "Failed to verify bind mount")
			dash.VerifyFatal(bindMount, true, "Failed to verify bind mount")
		}
		stepLog = "Increase the volume HA of the kubevirt VM Volumes"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appCtx := range appCtxs {
				vols, err := Inst().S.GetVolumes(appCtx)
				log.FailOnError(err, "Failed to get volumes of kubevirt VM")
				for _, vol := range vols {
					currRep, err := Inst().V.GetReplicationFactor(vol)
					log.FailOnError(err, "Failed to get Repl factor for vil %s", vol.Name)

					if currRep < 3 {
						opts := volume.Options{
							ValidateReplicationUpdateTimeout: validateReplicationUpdateTimeout,
						}
						err = Inst().V.SetReplicationFactor(vol, currRep+1, nil, nil, true, opts)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Validate set repl factor to %d", currRep+1))
					} else {
						log.Warnf("Volume %s has reached maximum replication factor", vol.Name)
					}
				}
			}
		})
		stepLog = "Verify if VM's are still bind mounted even after HA increase"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appCtx := range appCtxs {
				isVmBindMounted, err := IsVMBindMounted(appCtx, true)
				log.FailOnError(err, "Failed to run vm bind mount check")
				if !isVmBindMounted {
					log.Errorf("The newly added replication to vm %s is not bind mounted", appCtx.App.Key)
				}
			}
		})
		stepLog = "Destroy Applications"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			DestroyApps(appCtxs, nil)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(appCtxs)
	})
})

var _ = Describe("{KubevirtVMVolHaDecrease}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("KubevirtVMVolHaDecrease", "Decrease the replication factor of kubevirt Vms", nil, 0)
	})
	var appCtxs []*scheduler.Context
	var namespace string
	itLog := "Decrease the volume HA of a kubevirt VM"
	It(itLog, func() {
		defer ListEvents("portworx")
		namespace = fmt.Sprintf("kubevirt-%v", time.Now().Unix())
		log.InfoD(stepLog)
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()

		Inst().AppList = []string{"kubevirt-debian-fio-minimal"}
		stepLog := "schedule a kubevirt VM"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				appCtxs = append(appCtxs, ScheduleApplicationsOnNamespace(namespace, "test")...)
			}
		})
		ValidateApplications(appCtxs)
		for _, appCtx := range appCtxs {
			bindMount, err := IsVMBindMounted(appCtx, false)
			log.FailOnError(err, "Failed to verify bind mount")
			dash.VerifyFatal(bindMount, true, "Failed to verify bind mount")
		}

		stepLog = "Decrease the volume HA of the kubevirt VM Volumes"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appCtx := range appCtxs {
				vols, err := Inst().S.GetVolumes(appCtx)
				log.FailOnError(err, "Failed to get volumes of kubevirt VM")
				for _, vol := range vols {
					currRep, err := Inst().V.GetReplicationFactor(vol)
					log.FailOnError(err, "Failed to get Repl factor for vil %s", vol.Name)

					if currRep > 1 {
						opts := volume.Options{
							ValidateReplicationUpdateTimeout: validateReplicationUpdateTimeout,
						}
						err = Inst().V.SetReplicationFactor(vol, currRep-1, nil, nil, true, opts)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Validate set repl factor to %d", currRep-1))
					} else {
						log.Warnf("Volume %s has reached maximum replication factor", vol.Name)
					}
				}
			}
		})
		stepLog = "Verify if VM's are still bind mounted even after HA Decrease"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appCtx := range appCtxs {
				isVmBindMounted, err := IsVMBindMounted(appCtx, true)
				log.FailOnError(err, "Failed to run vm bind mount check")
				if !isVmBindMounted {
					log.Errorf("The newly added disk to vm %s is not bind mounted", appCtx.App.Key)
				}
			}
		})
		stepLog = "Destroy Applications"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			DestroyApps(appCtxs, nil)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(appCtxs)
	})
})

var _ = Describe("{LiveMigrationBeforeAddDisk}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("LiveMigrationBeforeAddDisk", "Live Migrate a VM Before Adding a new disk to a kubevirtVM", nil, 0)
	})
	var appCtxs []*scheduler.Context
	var namespace string
	itLog := "Live Migrate a VM and then add a new disk to a kubevirtVM"
	It(itLog, func() {
		defer ListEvents("portworx")
		namespace = fmt.Sprintf("kubevirt-%v", time.Now().Unix())
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		numberOfVolumes := 1
		Inst().AppList = []string{"kubevirt-debian-fio-minimal"}
		stepLog := "schedule a kubevirtVM"
		Step(stepLog, func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				appCtxs = append(appCtxs, ScheduleApplicationsOnNamespace(namespace, "test")...)
			}
		})
		ValidateApplications(appCtxs)
		for _, appCtx := range appCtxs {
			bindMount, err := IsVMBindMounted(appCtx, false)
			log.FailOnError(err, "Failed to verify bind mount")
			dash.VerifyFatal(bindMount, true, "Failed to verify bind mount")
		}
		stepLog = "Live migrate the kubevirt VM"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appCtx := range appCtxs {
				err := StartAndWaitForVMIMigration(appCtx, context1.TODO())
				log.FailOnError(err, "Failed to live migrate kubevirt VM")
			}
		})
		stepLog = "Add one disk to the kubevirt VM"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			success, err := AddDisksToKubevirtVM(appCtxs, numberOfVolumes, "10Gi")
			log.FailOnError(err, "Failed to add disks to kubevirt VM")
			dash.VerifyFatal(success, true, "Failed to add disks to kubevirt VM?")
		})
		stepLog = "Verify the new disk added is also bind mounted"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appCtx := range appCtxs {
				isVmBindMounted, err := IsVMBindMounted(appCtx, true)
				log.FailOnError(err, "Failed to verify disks in kubevirt VM")
				if !isVmBindMounted {
					log.Errorf("The newly added disk to vm %s is not bind mounted", appCtx.App.Key)
				}
			}
		})
		stepLog = "Destroy Applications"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			DestroyApps(appCtxs, nil)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(appCtxs)
	})
})

var _ = Describe("{AddDiskAndLiveMigrate}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("AddDiskAndLiveMigrate", "Live Migrate a VM After Adding a new disk to a kubevirtVM", nil, 0)
	})
	var appCtxs []*scheduler.Context
	var namespace string
	itLog := "Add a new disk to a kubevirtVM and then Live Migrate"
	It(itLog, func() {
		defer ListEvents("portworx")
		namespace = fmt.Sprintf("kubevirt-%v", time.Now().Unix())
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		numberOfVolumes := 1
		Inst().AppList = []string{"kubevirt-debian-fio-minimal"}
		stepLog := "schedule a kubevirtVM"
		Step(stepLog, func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				appCtxs = append(appCtxs, ScheduleApplicationsOnNamespace(namespace, "test")...)
			}
		})
		ValidateApplications(appCtxs)
		for _, appCtx := range appCtxs {
			bindMount, err := IsVMBindMounted(appCtx, false)
			log.FailOnError(err, "Failed to verify bind mount")
			dash.VerifyFatal(bindMount, true, "Failed to verify bind mount")
		}
		stepLog = "Add one disk to the kubevirt VM"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			success, err := AddDisksToKubevirtVM(appCtxs, numberOfVolumes, "10Gi")
			log.FailOnError(err, "Failed to add disks to kubevirt VM")
			dash.VerifyFatal(success, true, "Failed to add disks to kubevirt VM?")
		})
		stepLog = "Verify the new disk added is also bind mounted"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appCtx := range appCtxs {
				isVmBindMounted, err := IsVMBindMounted(appCtx, true)
				log.FailOnError(err, "Failed to verify disks in kubevirt VM")
				if !isVmBindMounted {
					log.Errorf("The newly added disk to vm %s is not bind mounted", appCtx.App.Key)
				}
			}
		})
		stepLog = "Live migrate the kubevirt VM"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, appCtx := range appCtxs {
				err := StartAndWaitForVMIMigration(appCtx, context1.TODO())
				log.FailOnError(err, "Failed to live migrate kubevirt VM")
			}
		})
		stepLog = "Destroy Applications"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			DestroyApps(appCtxs, nil)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(appCtxs)
	})
})

var _ = Describe("{KubeVirtPvcAndPoolExpandWithAutopilot}", func() {
	/*
		PWX:
			https://purestorage.atlassian.net/browse/PWX-36709
		TestRail:
			https://portworx.testrail.net/index.php?/cases/view/93652
			https://portworx.testrail.net/index.php?/cases/view/93653
	*/
	var (
		testName                string
		contexts                []*scheduler.Context
		pvcLabelSelector        = make(map[string]string)
		poolLabelSelector       = make(map[string]string)
		pvcAutoPilotRules       []apapi.AutopilotRule
		poolAutoPilotRules      []apapi.AutopilotRule
		selectedStorageNode     node.Node
		preResizeVolumeMap      = make(map[string]*volume.Volume)
		postResizeVolumeMap     = make(map[string]*volume.Volume)
		stopWaitForRunningChan  = make(chan struct{})
		waitForRunningErrorChan = make(chan error)
	)

	JustBeforeEach(func() {
		testName = "kv-pvc-pool-ap"
		tags := map[string]string{"poolChange": "true", "volumeChange": "true"}
		StartTorpedoTest("KubeVirtPvcAndPoolExpandWithAutopilot", "Kubevirt PVC and Pool expand test with autopilot", tags, 93652)
	})

	It("has to fill up the volume completely, resize the volumes and storage pool(s), validate and teardown apps", func() {
		defer ListEvents("portworx")
		log.InfoD("filling up the volume completely, resizing the volumes and storage pool(s), validating and tearing down apps")

		Step("Create autopilot rules for PVC and pool expand", func() {
			log.InfoD("Creating autopilot rules for PVC and pool expand")
			selectedStorageNode = node.GetStorageDriverNodes()[0]
			log.Infof("Selected storage node: %s", selectedStorageNode.Name)
			pvcLabelSelector = map[string]string{"autopilot": "pvc-expand"}
			pvcAutoPilotRules = []apapi.AutopilotRule{
				aututils.PVCRuleByUsageCapacity(5, 100, "100"),
			}
			poolLabelSelector = map[string]string{"autopilot": "adddisk"}
			poolAutoPilotRules = []apapi.AutopilotRule{
				aututils.PoolRuleByTotalSize((getTotalPoolSize(selectedStorageNode)/units.GiB)+1, 10, aututils.RuleScaleTypeAddDisk, poolLabelSelector),
			}
		})

		Step("schedule applications for PVC expand", func() {
			log.Infof("Scheduling apps with autopilot rules for PVC expand")
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				for id, apRule := range pvcAutoPilotRules {
					taskName := fmt.Sprintf("%s-%d-aprule%d", testName, i, id)
					apRule.Name = fmt.Sprintf("%s-%d", apRule.Name, i)
					apRule.Spec.ActionsCoolDownPeriod = int64(60)
					context, err := Inst().S.Schedule(taskName, scheduler.ScheduleOptions{
						AppKeys:            Inst().AppList,
						StorageProvisioner: Inst().Provisioner,
						AutopilotRule:      apRule,
						Labels:             pvcLabelSelector,
					})
					log.FailOnError(err, "failed to schedule app [%s] with autopilot rule [%s]", taskName, apRule.Name)
					contexts = append(contexts, context...)
				}
			}
		})

		Step("Schedule apps with autopilot rules for pool expand", func() {
			log.InfoD("Scheduling apps with autopilot rules for pool expand")
			log.Infof("Adding labels [%s] on node: %s", poolLabelSelector, selectedStorageNode.Name)
			err := AddLabelsOnNode(selectedStorageNode, poolLabelSelector)
			log.FailOnError(err, "failed to add labels [%s] on node: %s", poolLabelSelector, selectedStorageNode.Name)
			contexts = scheduleAppsWithAutopilot(testName, Inst().GlobalScaleFactor, poolAutoPilotRules, scheduler.ScheduleOptions{PvcSize: 20 * units.GiB})
		})

		Step("Wait until workload completes on volume", func() {
			log.InfoD("Waiting for workload to complete on volume")
			for _, ctx := range contexts {
				err := Inst().S.WaitForRunning(ctx, workloadTimeout, retryInterval)
				log.FailOnError(err, "failed to wait for workload by app [%s] to be running", ctx.App.Key)
			}
			for _, ctx := range contexts {
				isVmBindMounted, err := IsVMBindMounted(ctx, true)
				log.FailOnError(err, fmt.Sprintf("failed to verify bind mount for app [%s]", ctx.App.Key))
				dash.VerifyFatal(isVmBindMounted, true, fmt.Sprintf("failed to verify bind mount for app [%s]", ctx.App.Key))
			}
			for _, ctx := range contexts {
				vols, err := Inst().S.GetVolumes(ctx)
				log.FailOnError(err, "failed to get volumes for app [%s]", ctx.App.Key)
				for _, vol := range vols {
					if vol.ID == "" {
						log.FailOnError(err, "failed to get volume ID for app [%s]", ctx.App.Key)
					}
					preResizeVolumeMap[vol.ID] = vol
				}
			}
		})

		Step("Ensure the app is running while resizing the volumes", func() {
			log.Infof("Ensuring the app is running while resizing the volumes")
			for _, ctx := range contexts {
				go func(ctx *scheduler.Context) {
					defer GinkgoRecover()
					for {
						select {
						case <-stopWaitForRunningChan:
							log.Infof("Stopping wait for running goroutine for app [%s]", ctx.App.Key)
							return
						default:
							err := Inst().S.WaitForRunning(ctx, workloadTimeout, retryInterval)
							if err != nil {
								err = fmt.Errorf("failed to wait for app [%s] to be running at [%v]. Err: [%v]", ctx.App.Key, time.Now(), err)
								waitForRunningErrorChan <- err
							}
						}
						time.Sleep(60 * time.Second)
					}
				}(ctx)
			}
		})

		Step("Validating volumes and verifying size of volumes", func() {
			log.InfoD("Validating volumes and verifying size of volumes")
			for _, ctx := range contexts {
				ValidateVolumes(ctx)
			}
		})

		Step("Validate storage pools", func() {
			log.InfoD("Validating storage pools")
			ValidateStoragePools(contexts)
		})

		Step("Wait for unscheduled resize of volume", func() {
			log.InfoD("Waiting for unscheduled resize of volume for [%v]", unscheduledResizeTimeout)
			time.Sleep(unscheduledResizeTimeout)
		})

		Step("Validating volumes and verifying size of volumes", func() {
			log.Infof("Validating volumes and verifying size of volumes")
			for _, ctx := range contexts {
				ValidateVolumes(ctx)
			}
		})

		Step("Validate storage pools", func() {
			log.InfoD("Validating storage pools")
			ValidateStoragePools(contexts)
			for _, ctx := range contexts {
				vols, err := Inst().S.GetVolumes(ctx)
				log.FailOnError(err, "failed to get volumes for app [%s]", ctx.App.Key)
				for _, vol := range vols {
					if vol.ID == "" {
						log.FailOnError(err, "failed to get volume ID for app [%s]", ctx.App.Key)
					}
					postResizeVolumeMap[vol.ID] = vol
				}
			}
			resizedVolumeCount := 0
			for preVolID, preVol := range preResizeVolumeMap {
				for postVolID, postVol := range postResizeVolumeMap {
					if preVolID == postVolID {
						if postVol.Size > preVol.Size {
							resizedVolumeCount += 1
						}
					}
				}
			}
			dash.VerifyFatal(resizedVolumeCount > 0, true, "No volumes resized")
		})
		Step("Verify bind mount after volume resize", func() {
			log.InfoD("Verify bind mount after volume resize")
			for _, ctx := range contexts {
				isVmBindMounted, err := IsVMBindMounted(ctx, true)
				log.FailOnError(err, fmt.Sprintf("failed to verify bind mount for app [%s]", ctx.App.Key))
				dash.VerifyFatal(isVmBindMounted, true, fmt.Sprintf("failed to verify bind mount for app [%s]", ctx.App.Key))
			}
		})
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
		log.InfoD("Destroying apps")
		log.InfoD("Closing stopWaitForRunningChan and waitForRunningErrorChan")
		close(stopWaitForRunningChan)
		close(waitForRunningErrorChan)
		var waitForRunningErrorList []error
		for err := range waitForRunningErrorChan {
			waitForRunningErrorList = append(waitForRunningErrorList, err)
		}
		dash.VerifyFatal(len(waitForRunningErrorList) == 0, true, fmt.Sprintf("Verifying if the app [%s] is running during resizing failed with errors: %v", testName, waitForRunningErrorList))
		opts := make(map[string]bool)
		opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
		for _, ctx := range contexts {
			TearDownContext(ctx, opts)
		}
		log.InfoD("Removing autopilot rules and node labels")
		for _, apRule := range pvcAutoPilotRules {
			log.Infof("Deleting pvc autopilot rule [%s]", apRule.Name)
			err := Inst().S.DeleteAutopilotRule(apRule.Name)
			log.FailOnError(err, "failed to delete autopilot rule [%s]", apRule.Name)
		}
		for _, apRule := range poolAutoPilotRules {
			log.Infof("Deleting pool autopilot rule [%s]", apRule.Name)
			err := Inst().S.DeleteAutopilotRule(apRule.Name)
			log.FailOnError(err, "failed to delete pool autopilot rule [%s]", apRule.Name)
		}
		for k := range poolLabelSelector {
			log.Infof("Removing label [%s] on node: %s", k, selectedStorageNode.Name)
			err := Inst().S.RemoveLabelOnNode(selectedStorageNode, k)
			log.FailOnError(err, "failed to remove label [%s] on node: %s", k, selectedStorageNode.Name)
		}
	})
})

var _ = Describe("{UpgradeOCPAndValidateKubeVirtApps}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("UpgradeClusterAndValidateKubeVirt", "Upgrade OCP cluster and validate kubevirt apps", nil, 0)
	})

	var appCtxs []*scheduler.Context

	itLog := "Upgrade OCP cluster and validate kubevirt apps"
	It(itLog, func() {
		defer ListEvents("portworx")
		stepLog := "schedule kubevirt VMs"
		Step(stepLog, func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("test-%v", i)
				appCtxs = append(appCtxs, ScheduleApplications(taskName)...)
			}
		})
		stepLog = "validate kubevirt apps before upgrade"
		Step(stepLog, func() {
			ValidateApplications(appCtxs)
			for _, appCtx := range appCtxs {
				isVmBindMounted, err := IsVMBindMounted(appCtx, false)
				log.FailOnError(err, "Failed to verify bind mount")
				dash.VerifyFatal(isVmBindMounted, true, "Failed to verify bind mount?")
			}
		})

		var versions []string
		if len(Inst().SchedUpgradeHops) > 0 {
			versions = strings.Split(Inst().SchedUpgradeHops, ",")
		}
		if len(versions) == 0 {
			log.Fatalf("No versions to upgrade")
			return
		}
		for _, version := range versions {
			Step(fmt.Sprintf("start [%s] scheduler upgrade to version [%s]", Inst().S.String(), version), func() {
				stopSignal := make(chan struct{})

				var mError error
				opver, err := oputil.GetPxOperatorVersion()
				if err == nil && opver.GreaterThanOrEqual(PDBValidationMinOpVersion) {
					go DoPDBValidation(stopSignal, &mError)
					defer func() {
						close(stopSignal)
					}()
				} else {
					log.Warnf("PDB validation skipped. Current Px-Operator version: [%s], minimum required: [%s]. Error: [%v].", opver, PDBValidationMinOpVersion, err)
				}

				err = Inst().S.UpgradeScheduler(version)
				dash.VerifyFatal(mError, nil, "validation of PDB of px-storage during cluster upgrade successful")
				dash.VerifyFatal(err, nil, fmt.Sprintf("verify [%s] upgrade to [%s] is successful", Inst().S.String(), version))

				PrintK8sCluterInfo()
			})

			Step("validate storage components", func() {
				urlToParse := fmt.Sprintf("%s/%s", Inst().StorageDriverUpgradeEndpointURL, Inst().StorageDriverUpgradeEndpointVersion)
				u, err := url.Parse(urlToParse)
				log.FailOnError(err, fmt.Sprintf("error parsing PX version the url [%s]", urlToParse))
				err = Inst().V.ValidateDriver(u.String(), true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("verify volume driver after upgrade to %s", version))

				// Printing cluster node info after the upgrade
				PrintK8sCluterInfo()
			})

			Step("update node drive endpoints", func() {
				// Update NodeRegistry, this is needed as node names and IDs might change after upgrade
				err = Inst().S.RefreshNodeRegistry()
				log.FailOnError(err, "Refresh Node Registry failed")

				// Refresh Driver Endpoints
				err = Inst().V.RefreshDriverEndpoints()
				log.FailOnError(err, "Refresh Driver Endpoints failed")

				// Printing pxctl status after the upgrade
				PrintPxctlStatus()
			})

			stepLog = "validate kubevirt apps after upgrade and destroy"
			Step(stepLog, func() {
				ValidateApplications(appCtxs)
				for _, ctx := range appCtxs {
					isVmBindMounted, err := IsVMBindMounted(ctx, false)
					log.FailOnError(err, "Failed to verify bind mount")
					dash.VerifyFatal(isVmBindMounted, true, "Failed to verify bind mount?")
				}
				DestroyApps(appCtxs, nil)
			})
		}
	})
})

var _ = Describe("{RebootRootDiskAttachedNode}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("RebootRootDiskAttachedNode", "Reboot the node where VMs root disk is attached", nil, 0)
		DeployVMTemplatesAndValidate()
	})
	var appCtxs []*scheduler.Context

	itLog := "Reboot node where Kubevirt VMs root disk is attached"
	It(itLog, func() {
		defer ListEvents("portworx")
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		Inst().AppList = []string{"kubevirt-cirros-live-migration", "kubevirt-windows-vm",
			"kubevirt-fio-pvc-clone", "kubevirt-fio-load-disk-repl-2", "kubevirt-fio-load-multi-disk"}
		stepLog := "schedule a kubevirtVM"
		Step(stepLog, func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				appCtxs = append(appCtxs, ScheduleApplications("reboot")...)
			}
		})
		defer DestroyApps(appCtxs, nil)
		ValidateApplications(appCtxs)
		for _, appCtx := range appCtxs {
			bindMount, err := IsVMBindMounted(appCtx, false)
			log.FailOnError(err, "Failed to verify bind mount after initial deploy")
			dash.VerifyFatal(bindMount, true, "Failed to verify bind mount after intial deploy")
		}

		stepLog = "Get node where VM's root disk is attached and reboot that node"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			for _, virtualMachineCtx := range appCtxs {
				bindMount, err := IsVMBindMounted(virtualMachineCtx, false)
				log.FailOnError(err, "Failed to verify bind mount pre node reboot in namespace: %s", virtualMachineCtx.App.NameSpace)
				dash.VerifyFatal(bindMount, true, "Failed to verify bind mount pre node reboot")

				vms, err := GetAllVMsFromScheduledContexts([]*scheduler.Context{virtualMachineCtx})
				log.FailOnError(err, "Failed to get VMs from scheduled contexts")
				dash.VerifyFatal(len(vms) > 0, true, "Failed to to get VMs from scheduled contexts")

				for _, vm := range vms {
					nodeName, err := GetNodeOfVM(vm)
					log.FailOnError(err, "Failed to get node name for VM: %s", vm.Name)
					log.Infof("Pre-reboot VM [%s] in namespace [%s] is scheduled on node [%s]. Rebooting it.", vm.Name, vm.Namespace, nodeName)
					nodeObj, err := node.GetNodeByName(nodeName)
					log.FailOnError(err, "Failed to get node obj for node name: %s", nodeName)
					err = Inst().N.RebootNodeAndWait(nodeObj)
					log.FailOnError(err, "Failed to reboot  node: %s", nodeObj.Name)
					log.Infof("Succesfully rebooted node: %s", nodeObj.Name)
				}
				ValidateApplications(appCtxs)
				// Get updated VM list and validate bind mount again
				// TODO: PTX-23439 Add validation that VM started on a different node than it's original node
				vms, err = GetAllVMsFromScheduledContexts([]*scheduler.Context{virtualMachineCtx})
				log.FailOnError(err, "Failed to get VMs from scheduled contexts")
				dash.VerifyFatal(len(vms) > 0, true, "Failed to to get VMs from scheduled contexts")
				for _, vm := range vms {
					nodeName, err := GetNodeOfVM(vm)
					log.FailOnError(err, "Failed to get node name for VM: %s", vm.Name)
					log.Infof("Post reboot VM [%s] in namespace [%s] is scheduled on node [%s]", vm.Name, vm.Namespace, nodeName)
				}
				bindMount, err = IsVMBindMounted(virtualMachineCtx, false)
				log.FailOnError(err, "Failed to verify bind mount post node reboot in namespace: %s", virtualMachineCtx.App.NameSpace)
				dash.VerifyFatal(bindMount, true, "Failed to verify bind mount pre node reboot")
			}
			ValidateApplications(appCtxs)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(appCtxs)
	})
})
