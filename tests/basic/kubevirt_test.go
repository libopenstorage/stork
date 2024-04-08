package tests

import (
	context1 "context"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	"time"
)

var _ = Describe("{AddNewDiskToKubevirtVM}", func() {
	JustBeforeEach(func() {
		StartTorpedoTest("AddNewDiskToKubevirtVM", "Add a new disk to a kubevirtVM", nil, 0)
	})
	var appCtxs []*scheduler.Context
	var namespace string
	itLog := "Add a new disk to a kubevirtVM"
	It(itLog, func() {
		namespace = fmt.Sprintf("kubevirt-%v", time.Now().Unix())
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		numberOfVolumes := 1
		Inst().AppList = []string{"kubevirt-fio-low-load-with-ssh"}
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
			// Create config-map with password to VM before calling AddDisksToKubevirtVM
			CreateConfigMap()
			success, err := AddDisksToKubevirtVM(appCtxs, numberOfVolumes, "0.5Gi")
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
		namespace = fmt.Sprintf("kubevirt-%v", time.Now().Unix())
		log.InfoD(stepLog)
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		Inst().AppList = []string{"kubevirt-fio-low-load-with-ssh"}

		stepLog := "schedule a kubevirt VM"
		Step(stepLog, func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("test-%v", i)
				appCtxs = append(appCtxs, ScheduleApplicationsOnNamespace(namespace, taskName)...)
			}
		})
		defer DestroyApps(appCtxs, nil)
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
		namespace = fmt.Sprintf("kubevirt-%v", time.Now().Unix())
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		numberOfVolumes := 1
		Inst().AppList = []string{"kubevirt-fio-low-load-with-ssh"}
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
			success, err := AddDisksToKubevirtVM(appCtxs, numberOfVolumes, "0.5Gi")
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
		namespace = fmt.Sprintf("kubevirt-%v", time.Now().Unix())
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		numberOfVolumes := 1
		Inst().AppList = []string{"kubevirt-fio-low-load-with-ssh"}
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
			success, err := AddDisksToKubevirtVM(appCtxs, numberOfVolumes, "0.5Gi")
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
		stepLog = "Add another disk to the kubevirt VM"
		Step(stepLog, func() {
			log.InfoD(stepLog)
			numberOfVolumes = 2
			success, err := AddDisksToKubevirtVM(appCtxs, numberOfVolumes, "0.5Gi")
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
		namespace = fmt.Sprintf("kubevirt-%v", time.Now().Unix())
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()

		Inst().AppList = []string{"kubevirt-fio-load-disk-repl-2"}
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
		namespace = fmt.Sprintf("kubevirt-%v", time.Now().Unix())
		log.InfoD(stepLog)
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()

		Inst().AppList = []string{"kubevirt-fio-load-multi-disk"}
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

		stepLog = "Verify if VM's are still bind mounted even after HA De"
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
		namespace = fmt.Sprintf("kubevirt-%v", time.Now().Unix())
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		numberOfVolumes := 1
		Inst().AppList = []string{"kubevirt-fio-low-load-with-ssh"}
		stepLog := "schedule a kubevirtVM"
		Step(stepLog, func() {
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				appCtxs = append(appCtxs, ScheduleApplicationsOnNamespace(namespace, "test")...)
			}
		})
		//defer DestroyApps(appCtxs, nil)
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
			success, err := AddDisksToKubevirtVM(appCtxs, numberOfVolumes, "0.5Gi")
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
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(appCtxs)
	})
})
