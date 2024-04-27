package tests

import (
	"fmt"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	corev1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"math/rand"
	"strings"
	"time"
)

func runCommand(cmd string, n node.Node) error {
	_, err := Inst().N.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         20 * time.Second,
		TimeBeforeRetry: 5 * time.Second,
		Sudo:            true,
	})
	if err != nil {
		return err
	}

	return nil
}

// getRandomNumbersFromArrLength returns the list of storage and storageless node which can be split to zones
func getRandomNumbersFromArrLength(arrayLength int, splitLength int) ([]int, []int) {
	myArray := make([]int, arrayLength)
	for i := 0; i < arrayLength; i++ {
		myArray[i] = i
	}
	selected := make(map[int]bool)
	resultSnode := make([]int, 0, splitLength)
	resultSlessnode := make([]int, 0, splitLength)
	for len(selected) < splitLength {
		idx := rand.Intn(len(myArray))
		if selected[myArray[idx]] {
			continue
		}
		selected[myArray[idx]] = true
		resultSnode = append(resultSnode, myArray[idx])
	}
	for _, each := range myArray {
		matched := false
		for _, mElement := range resultSnode {
			if each == mElement {
				matched = true
			}
		}
		if !matched {
			resultSlessnode = append(resultSlessnode, each)
		}
	}
	return resultSnode, resultSlessnode
}

// KillKvdbNode return error in case of command failure
func KillKvdbNode(kvdbNode node.Node) error {
	pid, err := GetKvdbMasterPID(kvdbNode)
	if err != nil {
		return err
	}
	command := fmt.Sprintf("kill -9 %s", pid)
	log.InfoD("killing PID using command [%s]", command)
	err = runCommand(command, kvdbNode)
	if err != nil {
		return err
	}
	return nil
}

// blockIptableRules blocks IPtable rules from the node
func blockIptableRules(zones []node.Node, targetZones []node.Node, revertRules bool) error {

	var targetZoneIPs []string
	for _, each := range targetZones {
		for _, eachAddress := range each.Addresses {
			targetZoneIPs = append(targetZoneIPs, eachAddress)
		}
	}
	for _, eachNode := range zones {
		for _, eachIp := range targetZoneIPs {
			command := fmt.Sprintf("iptables -A INPUT -p tcp -s %s -j DROP", eachIp)
			if revertRules {
				command = fmt.Sprintf("iptables -D INPUT -p tcp -s %s -j DROP", eachIp)
			}
			log.InfoD("Triggering command [%s] from Node [%v]", command, eachNode.Name)
			err := runCommand(command, eachNode)
			if err != nil {
				return err
			}
		}
	}
	for _, eachNode := range zones {
		for _, eachIp := range targetZoneIPs {
			command := fmt.Sprintf("iptables -A OUTPUT -p tcp -s %s -j DROP", eachIp)
			if revertRules {
				command = fmt.Sprintf("iptables -D OUTPUT -p tcp -s %s -j DROP", eachIp)
			}
			log.InfoD("Triggering command [%s] from Node [%v]", command, eachNode.Name)
			err := runCommand(command, eachNode)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// flushIptableRules flushes all IPtable rules on the node specified
func flushIptableRules(n node.Node) error {
	command := "iptables -F"
	if !node.IsMasterNode(n) {
		err := runCommand(command, n)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func getVolumeRuntimeState(vol string) (string, error) {
	volDetails, err := Inst().V.InspectVolume(vol)
	if err != nil {
		return "", err
	}
	var runTimeStat string
	runTimeStat = ""
	for _, v := range volDetails.RuntimeState {
		runTimeStat = v.GetRuntimeState()["RuntimeState"]
	}
	return runTimeStat, nil
}

var _ = Describe("{FordRunFlatResync}", func() {
	/*
		Test Needs 10 VM's running with Internal KVDB
		Cluster should have 6 StorageNodes and 4 Storageless nodes
		App Used Mongodb on multiple Namespace would be running on both storage and storage-less nodes
		Set the IPtables rules in some nodes  of that subnet such that they will not be reachable from all nodes in second subnet
		Make sure cluster is up and IOs are running for MongoDB apps. Most of the volumes will go into degraded state
		Wait for around 30 minutes to hours for more IOs.
		Now set IPtables rules in another subnet
		Remove the Iptables rules from all  nodes in subnet 1 and wait for them to join the cluster back.
		Slowly remove IPtables rules from subnet2 nodes such that some volumes
		 	will be in sync and IO started running. Now immediately block it again on few nodes so
			that volume state will set to ‘NOT in Quorum’
		Once this state is set then kill the etcd process in leader KVDB node and remove all IPtable rules from all nodes.
			and set the IPtable rule in this node (where killed the etcd process)
			so that it is not reachable to other nodes in subnet1
		Wait for three copies to be created for KVDB and cluster to be up.
		Now remove the IPtables rules from a node and wait for it to join.
	*/
	JustBeforeEach(func() {
		StartTorpedoTest("FordRunFlatResync",
			"Ford customer issue for runflat and resync failed PTX-16727",
			nil, 0)
	})
	var contexts []*scheduler.Context
	stepLog := "Ford customer issue for runflat and resync failed"
	It(stepLog, func() {

		var iptablesflushed bool
		iptablesflushed = false

		vInspectBackground := false

		var getKvdbLeaderNode node.Node
		allkvdbNodes, err := GetAllKvdbNodes()
		log.FailOnError(err, "Failed to get list of KVDB nodes from the cluster")

		for _, each := range allkvdbNodes {
			if each.Leader {
				getKvdbLeaderNode, err = node.GetNodeDetailsByNodeID(each.ID)
				log.FailOnError(err, "Unable to get the node details from NodeID [%v]", each.ID)
				break
			}
		}

		// Check if the cluster consists of 10 nodes
		log.InfoD("Get all nodes present in the cluster")
		allNodes := []node.Node{}
		// create an array with storage and storage less nodes added
		for _, each := range node.GetStorageDriverNodes() {
			if each.Id != getKvdbLeaderNode.Id {
				allNodes = append(allNodes, each)
			}
		}

		// Verify total nodes available is minimum of 9
		dash.VerifyFatal(len(allNodes) >= 9, true, "required minimum of 10 nodes for the test to run")

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("fordflatresync-%d", i))...)
		}

		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		var volumesPresent []*volume.Volume

		for _, ctx := range contexts {
			vols, err := Inst().S.GetVolumes(ctx)
			log.FailOnError(err, "failed to get volumes from the contexts")

			// This is done to make sure that volumes should have replica on nodes from both zones
			for _, eachVol := range vols {
				volumesPresent = append(volumesPresent, eachVol)
			}
		}

		done := make(chan bool)

		var nodesSplit1 = []node.Node{}
		var nodesSplit2 = []node.Node{}

		if len(node.GetStorageNodes()) == 6 && len(node.GetStorageLessNodes()) == 6 {
			nodesSplit1 = node.GetStorageNodes()
			nodesSplit2 = node.GetStorageLessNodes()
		} else {
			nodesSplit1 = allNodes[0:4]
			nodesSplit2 = allNodes[5:9]
		}

		log.InfoD("KVDB Leader node is [%v]", getKvdbLeaderNode.Addresses)
		var allStorageExceptKVDB []node.Node
		for _, each := range nodesSplit1 {
			if each.Id != getKvdbLeaderNode.Id {
				allStorageExceptKVDB = append(allStorageExceptKVDB, each)
			}
		}

		// Create 2 zones from the Storage and storageless Node
		zone1 := []node.Node{}
		zone2 := []node.Node{}

		zone1StorageEle, zone2StorageEle := getRandomNumbersFromArrLength(len(allStorageExceptKVDB), len(allStorageExceptKVDB)/2)
		for _, each := range zone1StorageEle {
			log.InfoD(fmt.Sprintf("Adding [%v] to zone1", allStorageExceptKVDB[each].Name))
			zone1 = append(zone1, allStorageExceptKVDB[each])
		}
		for _, each := range zone2StorageEle {
			log.InfoD(fmt.Sprintf("Adding [%v] to zone2", allStorageExceptKVDB[each].Name))
			zone2 = append(zone2, allStorageExceptKVDB[each])
		}

		zone1StorageLessEle, zone2StorageLessEle := getRandomNumbersFromArrLength(len(nodesSplit2), len(nodesSplit2)/2)
		for _, each := range zone1StorageLessEle {
			log.InfoD(fmt.Sprintf("Adding [%v] to zone1", nodesSplit2[each].Name))
			zone1 = append(zone1, nodesSplit2[each])
		}
		for _, each := range zone2StorageLessEle {
			log.InfoD(fmt.Sprintf("Adding [%v] to zone2", nodesSplit2[each].Name))
			zone2 = append(zone2, nodesSplit2[each])
		}

		flushiptables := func() {
			if !iptablesflushed {
				// Flush all iptables from all the nodes available forcefully
				for _, eachNode := range allNodes {
					log.InfoD("Flushing iptables rules on node [%v]", eachNode.Name)
					log.FailOnError(flushIptableRules(eachNode), "Iptables flush all failed on node [%v]", eachNode.Name)
				}
				if vInspectBackground {
					done <- true
				}
			}
		}
		revertZone1 := func() {
			log.FailOnError(blockIptableRules(zone1, zone2, true), "Failed to unblock IPTable rules on target Nodes")
		}
		revertZone2 := func() {
			log.FailOnError(blockIptableRules(zone2, zone1, true), "Failed to unblock IPTable rules on target Nodes")
		}

		// Flush all IPtables on all nodes before running the scripts
		flushiptables()

		// force flush iptables on all the nodes at the end
		defer flushiptables()

		// Run inspect continuously in the background
		go func(volumes []*volume.Volume) {
			for {
				select {
				case <-done:
					return
				default:
					// Get volume inspect on all the available volumes
					for _, each := range volumes {
						vid := each.ID
						_, err := getVolumeRuntimeState(vid)
						if err != nil {
							fmt.Printf("Error while fetching the volume info")
						}
					}
				}
			}
		}(volumesPresent)

		// flag is used to run volume inspect in the background continuously till the time script terminates
		vInspectBackground = true

		// From Zone 1 block all the traffic to systems under zone2
		// From Zone 2 block all the traffic to systems under zone1
		log.InfoD("blocking iptables from all nodes present in zone1 from accessing zone2")
		err = blockIptableRules(zone1, zone2, false)
		log.FailOnError(err, "Failed to revert IPtable Rules on Zone1")

		log.InfoD("Sleeping for 20 minutes for IO to generate on volumes")
		time.Sleep(20 * time.Minute)

		log.InfoD("blocking iptables from all nodes present in zone2 from accessing zone1")
		err = blockIptableRules(zone2, zone1, false)
		log.FailOnError(err, "Failed to set IPtable Rules on zone2")

		log.InfoD("Sleeping for 10 minute before resetting iptables rules")
		time.Sleep(10 * time.Minute)

		// Reverting back Zone1 iptables set
		revertZone1()

		// Reverting back zone2 iptables set
		revertZone2()

		log.InfoD("Sleeping for 10 minute before set iptables rules on zone2")
		time.Sleep(10 * time.Minute)

		// Reset iptables rules on vms under zone2
		err = blockIptableRules(zone2, zone1, false)
		log.FailOnError(err, "Failed to set IPtable Rules on zone2")

		log.InfoD("Killing KVDB PID from KVDB Master Node")
		log.FailOnError(KillKvdbNode(getKvdbLeaderNode), "failed to Kill Kvdb Node")

		// Block IPtable rules on the kvdb node to all the nodes in zone 1
		kvdb := []node.Node{getKvdbLeaderNode}
		log.FailOnError(blockIptableRules(kvdb, zone1, false),
			"Set IPTable rules on kvdb node failed")

		// Wait for some time before checking for file system goes back online
		log.Infof("Waiting for 10 minutes before checking for file system goes back online")
		time.Sleep(10 * time.Minute)

		// Revert back the iptables rules from the kvdb node
		log.FailOnError(blockIptableRules(kvdb, zone1, true),
			"Reverting back IPTable rules on kvdb node failed")

		// Flushing iptables rules on all the nodes present in the cluster before making sure that nodes to come up online
		flushiptables()
		iptablesflushed = true

		// Wait for some more time for nodes to get settled
		log.Infof("Waiting for 15 minutes for nodes to get settled back after reverting iptable rules on kvdb nodes")
		time.Sleep(15 * time.Minute)

		// This is done to make sure that volumes should have replica on nodes from both zones
		for _, eachVol := range volumesPresent {
			volStat, err := getVolumeRuntimeState(eachVol.ID)
			log.FailOnError(err, "Failed to get Run time stat of the volume")
			if volStat != "clean" {
				log.FailOnError(fmt.Errorf("volume [%v] state is not in Clean state. current state is [%s]", eachVol.Name, volStat), "is volume state clean?")
			}
		}

		// Wait for some time for system to be up and all nodes drivers up and running
		for _, each := range node.GetStorageNodes() {
			err = Inst().V.WaitDriverUpOnNode(each, 2*time.Minute)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", each.Name))
		}

		for _, eachVol := range volumesPresent {
			status, err := IsVolumeStatusUP(eachVol)
			log.FailOnError(err, fmt.Sprintf("Volume [%v] is not in expected state", eachVol.Name))
			dash.VerifyFatal(status == true, true, "volume status is not up")
		}

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{ValidateZombieReplicas}", func() {
	/*
			1. Create aggressive localsnap for every minute.
			2. Validate zombie replicas
		https://portworx.atlassian.net/browse/PTX-17088
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("ValidateZombieReplicas", "Create aggressive local snaps and validate zombie replicas", nil, 0)
	})

	var contexts []*scheduler.Context
	stepLog := "has to schedule local snapshots and validate zombie replicas"
	It(stepLog, func() {
		log.InfoD(stepLog)
		contexts = make([]*scheduler.Context, 0)
		policyName := "localintervalpolicy"
		appScale := 5
		var snapStatuses map[storkv1.SchedulePolicyType][]*storkv1.ScheduledVolumeSnapshotStatus
		stepLog = fmt.Sprintf("create schedule policy %s", policyName)
		Step(stepLog, func() {
			log.InfoD(stepLog)

			schedPolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
			if err != nil {
				retain := 3
				interval := 1
				log.InfoD("Creating a interval schedule policy %v with interval %v minutes", policyName, interval)
				schedPolicy = &storkv1.SchedulePolicy{
					ObjectMeta: meta_v1.ObjectMeta{
						Name: policyName,
					},
					Policy: storkv1.SchedulePolicyItem{
						Interval: &storkv1.IntervalPolicy{
							Retain:          storkv1.Retain(retain),
							IntervalMinutes: 1,
						},
					}}

				_, err = storkops.Instance().CreateSchedulePolicy(schedPolicy)
				log.FailOnError(err, fmt.Sprintf("error creating a SchedulePolicy [%s]", policyName))
			}

			appList := Inst().AppList
			defer func() {
				Inst().AppList = appList

			}()
			cmd := "/opt/pwx/bin/runc exec -t portworx ls -l /var/.px/"

			nodeContentsMap := make(map[string][]string)
			for _, n := range node.GetStorageDriverNodes() {
				for _, p := range n.GetPools() {
					rCmd := fmt.Sprintf("%s%d", cmd, p.ID)
					log.Infof("Running command [%s]", rCmd)
					output, err := runCmd(rCmd, n)
					log.FailOnError(err, fmt.Sprintf("error running command [%s] on node [%s]", cmd, n.Name))
					dirContents := strings.Split(output, "\n")
					nodeContentsMap[n.Name] = append(nodeContentsMap[n.Name], dirContents...)
				}
			}

			Inst().AppList = []string{"fio-localsnap"}

			for i := 0; i < appScale; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("localsnap-%d", i))...)
			}

			ValidateApplications(contexts)
			log.Infof("waiting for 15 mins for enough local snaps to be created.")
			time.Sleep(15 * time.Minute)

			stepLog = "Verify that local snap status"
			Step(stepLog, func() {
				log.InfoD(stepLog)

				for _, ctx := range contexts {
					var appVolumes []*volume.Volume
					var err error
					appNamespace := ctx.App.Key + "-" + ctx.UID
					log.Infof("Namespace : %v", appNamespace)
					Step(stepLog, func() {
						log.InfoD(stepLog)
						appVolumes, err = Inst().S.GetVolumes(ctx)
						log.FailOnError(err, "error getting volumes for [%s]", ctx.App.Key)

						if len(appVolumes) == 0 {
							log.FailOnError(fmt.Errorf("no volumes found for [%s]", ctx.App.Key), "error getting volumes for [%s]", ctx.App.Key)
						}
					})
					log.Infof("Got volume count : %v", len(appVolumes))
					scaleFactor := time.Duration(appScale * len(appVolumes))
					err = Inst().S.ValidateVolumes(ctx, scaleFactor*4*time.Minute, defaultRetryInterval, nil)
					log.FailOnError(err, "error validating volumes for [%s]", ctx.App.Key)

					for _, v := range appVolumes {

						snapshotScheduleName := v.Name + "-interval-schedule"
						log.InfoD("snapshotScheduleName : %v for volume: %s", snapshotScheduleName, v.Name)
						snapStatuses, err = storkops.Instance().ValidateSnapshotSchedule(snapshotScheduleName,
							appNamespace,
							snapshotScheduleRetryTimeout,
							snapshotScheduleRetryInterval)
						log.FailOnError(err, fmt.Sprintf("error while getting volume snapshot status for [%s]", snapshotScheduleName))
						for k, v := range snapStatuses {
							log.Infof("Policy Type: %v", k)
							for _, e := range v {
								log.InfoD("ScheduledVolumeSnapShot Name: %v", e.Name)
								log.InfoD("ScheduledVolumeSnapShot status: %v", e.Status)
								snapData, err := Inst().S.GetSnapShotData(ctx, e.Name, appNamespace)
								log.FailOnError(err, fmt.Sprintf("error getting snapshot data for [%s/%s]", appNamespace, e.Name))

								snapType := snapData.Spec.PortworxSnapshot.SnapshotType
								log.InfoD("Snapshot Type: %v", snapType)
								if snapType != "local" {
									err = &scheduler.ErrFailedToGetVolumeParameters{
										App:   ctx.App,
										Cause: fmt.Sprintf("Snapshot Type: %s does not match", snapType),
									}
									log.FailOnError(err, fmt.Sprintf("error validating snapshot data for [%s/%s]", appNamespace, e.Name))

								}

								snapID := snapData.Spec.PortworxSnapshot.SnapshotID
								log.InfoD("Snapshot ID: %v", snapID)

								if snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot == nil ||
									len(snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot.SnapshotID) == 0 {
									err = &scheduler.ErrFailedToGetVolumeParameters{
										App:   ctx.App,
										Cause: fmt.Sprintf("volumesnapshotdata: %s does not have portworx volume source set", snapData.Metadata.Name),
									}
									log.FailOnError(err, fmt.Sprintf("error validating snapshot data for [%s/%s]", appNamespace, e.Name))
								}
							}
						}
					}
				}
			})
			stepLog = "Delete Apps and validate zombie replicas"
			Step(stepLog, func() {
				opts := make(map[string]bool)
				opts[SkipClusterScopedObjects] = true
				DestroyApps(contexts, opts)
				n := node.GetStorageDriverNodes()[0]
				dCmd := "pxctl v l -s | awk '{print $1}' | grep -v ID | xargs -L 1 pxctl v d -f"

				// Execute the command and delete volume local snapshots
				_, err := Inst().N.RunCommandWithNoRetry(n, dCmd, node.ConnectionOpts{
					Timeout:         2 * time.Minute,
					TimeBeforeRetry: 10 * time.Second,
				})
				log.FailOnError(err, fmt.Sprintf("error running command [%s]", dCmd))

				time.Sleep(5 * time.Second)
				postNodeContentsMap := make(map[string][]string)
				for _, n := range node.GetStorageDriverNodes() {
					for _, p := range n.GetPools() {
						rCmd := fmt.Sprintf("%s%d", cmd, p.ID)
						log.Infof("Running command [%s]", rCmd)
						output, err := runCmd(rCmd, n)
						log.FailOnError(err, fmt.Sprintf("error running command [%s] on node [%s]", cmd, n.Name))
						dirContents := strings.Split(output, "\n")
						postNodeContentsMap[n.Name] = append(postNodeContentsMap[n.Name], dirContents...)
					}
					if len(nodeContentsMap[n.Name]) != len(postNodeContentsMap[n.Name]) {
						dash.VerifySafely(len(postNodeContentsMap[n.Name]), len(nodeContentsMap[n.Name]), fmt.Sprintf("replicas not deleted in node [%s], Actual: [%v], Expected: [%v]", n.Name, postNodeContentsMap[n.Name], nodeContentsMap[n.Name]))
					}
				}

			})

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{CreateCloudSnapAndDelete}", func() {
	/*
		1. Create aggressive cloud snaps for every minute
		2. ValidateCloudSnap Deletion
	*/

	JustBeforeEach(func() {
		StartTorpedoTest("CreateCloudSnapAndDelete", "Create aggressive cloud snaps and validate deletion", nil, 0)
	})

	var contexts []*scheduler.Context
	stepLog := "has to schedule cloud snap  and delete cloudsnaps"
	It(stepLog, func() {
		log.InfoD(stepLog)

		err := CreatePXCloudCredential()
		log.FailOnError(err, "failed to create cloud credential")

		contexts = make([]*scheduler.Context, 0)
		policyName := "intervalpolicy"
		appScale := 5

		stepLog = fmt.Sprintf("create schedule policy %s", policyName)
		Step(stepLog, func() {
			log.InfoD(stepLog)

			schedPolicy, err := storkops.Instance().GetSchedulePolicy(policyName)
			if err != nil {
				retain := 3
				interval := 1
				log.InfoD("Creating a interval schedule policy %v with interval %v minutes", policyName, interval)
				schedPolicy = &storkv1.SchedulePolicy{
					ObjectMeta: meta_v1.ObjectMeta{
						Name: policyName,
					},
					Policy: storkv1.SchedulePolicyItem{
						Interval: &storkv1.IntervalPolicy{
							Retain:          storkv1.Retain(retain),
							IntervalMinutes: interval,
						},
					}}

				_, err = storkops.Instance().CreateSchedulePolicy(schedPolicy)
				log.FailOnError(err, fmt.Sprintf("error creating a SchedulePolicy [%s]", policyName))
			}

			defer func() {
				err := storkops.Instance().DeleteSchedulePolicy(policyName)
				log.FailOnError(err, fmt.Sprintf("error deleting a SchedulePolicy [%s]", policyName))
			}()

			for i := 0; i < appScale; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("cloudsnap-%d", i))...)
			}

			ValidateApplications(contexts)
			log.Infof("waiting for 15 mins for enough cloud snaps to be created.")
			time.Sleep(15 * time.Minute)

			stepLog = "Verify that cloud snap status"
			Step(stepLog, func() {
				log.InfoD(stepLog)
				stopPXValidation := false
				defer func() {
					stopPXValidation = true
				}()
				go func() {
					for {
						err := ValidatePXStatus()
						dash.VerifySafely(err, nil, "PX should be up on all the nodes")
						if stopPXValidation {
							break
						}
					}

				}()
				//validating cloudnsnaps for 30 iterations
				for i := 1; i <= 30; i++ {
					log.Infof("validating cloudsnaps iteration : %d", i)
					for _, ctx := range contexts {
						var appVolumes []*volume.Volume
						var err error
						appNamespace := ctx.App.Key + "-" + ctx.UID
						log.Infof("Namespace: %v", appNamespace)
						stepLog = fmt.Sprintf("Getting app volumes for volume %s", ctx.App.Key)
						Step(stepLog, func() {
							log.InfoD(stepLog)
							appVolumes, err = Inst().S.GetVolumes(ctx)
							log.FailOnError(err, "error getting volumes for [%s]", ctx.App.Key)

							if len(appVolumes) == 0 {
								log.FailOnError(fmt.Errorf("no volumes found for [%s]", ctx.App.Key), "error getting volumes for [%s]", ctx.App.Key)
							}
						})
						log.Infof("Got volume count : %v", len(appVolumes))
						scaleFactor := time.Duration(appScale * len(appVolumes))
						err = Inst().S.ValidateVolumes(ctx, scaleFactor*4*time.Minute, defaultRetryInterval, nil)
						log.FailOnError(err, "error validating volumes for [%s]", ctx.App.Key)

						for _, v := range appVolumes {

							isPureVol, err := Inst().V.IsPureVolume(v)
							log.FailOnError(err, "error checking if volume is pure volume")
							if isPureVol {
								log.Warnf("Cloud snapshot is not supported for Pure DA volumes: [%s],Skipping cloud snapshot trigger for pure volume.", v.Name)
								continue
							}

							snapshotScheduleName := v.Name + "-interval-schedule"
							log.InfoD("snapshotScheduleName : %v for volume: %s", snapshotScheduleName, v.Name)
							resp, err := storkops.Instance().GetSnapshotSchedule(snapshotScheduleName, appNamespace)
							log.FailOnError(err, fmt.Sprintf("error getting snapshot schedule for %s, volume:%s in namespace %s", snapshotScheduleName, v.Name, v.Namespace))
							dash.VerifyFatal(len(resp.Status.Items) > 0, true, fmt.Sprintf("verify snapshots exists for %s", snapshotScheduleName))
							for _, snapshotStatuses := range resp.Status.Items {
								if len(snapshotStatuses) > 0 {
									status := snapshotStatuses[len(snapshotStatuses)-1]
									if status == nil {
										log.FailOnError(fmt.Errorf("SnapshotSchedule has an empty migration in it's most recent status"), fmt.Sprintf("error getting latest snapshot status for %s", snapshotScheduleName))
									}
									status, err = WaitForSnapShotToReady(snapshotScheduleName, status.Name, appNamespace)
									log.Infof("Snapshot %s has status %v", status.Name, status.Status)

									if status.Status == snapv1.VolumeSnapshotConditionError {
										resp, _ := storkops.Instance().GetSnapshotSchedule(snapshotScheduleName, appNamespace)
										log.Infof("SnapshotSchedule resp: %v", resp)
										snapData, _ := Inst().S.GetSnapShotData(ctx, status.Name, appNamespace)
										log.Infof("snapData : %v", snapData)
										log.FailOnError(fmt.Errorf("snapshot: %s failed. status: %v", status.Name, status.Status), fmt.Sprintf("cloud snapshot for %s failed", snapshotScheduleName))
									}
									if status.Status == snapv1.VolumeSnapshotConditionPending {
										log.FailOnError(fmt.Errorf("snapshot: %s not completed. status: %v", status.Name, status.Status), fmt.Sprintf("cloud snapshot for %s stuck in pending state", snapshotScheduleName))
									}

									if status.Status == snapv1.VolumeSnapshotConditionReady {
										snapData, err := Inst().S.GetSnapShotData(ctx, status.Name, appNamespace)
										log.FailOnError(err, fmt.Sprintf("error getting snapshot data for [%s/%s]", appNamespace, status.Name))

										snapType := snapData.Spec.PortworxSnapshot.SnapshotType
										log.Infof("Snapshot Type: %v", snapType)
										if snapType != "cloud" {
											err = &scheduler.ErrFailedToGetVolumeParameters{
												App:   ctx.App,
												Cause: fmt.Sprintf("Snapshot Type: %s does not match", snapType),
											}
											log.FailOnError(err, fmt.Sprintf("error validating snapshot data for [%s/%s]", appNamespace, status.Name))
										}

										snapID := snapData.Spec.PortworxSnapshot.SnapshotID
										log.Infof("Snapshot ID: %v", snapID)
										if snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot == nil ||
											len(snapData.Spec.VolumeSnapshotDataSource.PortworxSnapshot.SnapshotID) == 0 {
											err = &scheduler.ErrFailedToGetVolumeParameters{
												App:   ctx.App,
												Cause: fmt.Sprintf("volumesnapshotdata: %s does not have portworx volume source set", snapData.Metadata.Name),
											}
											log.FailOnError(err, fmt.Sprintf("error validating snapshot data for [%s/%s]", appNamespace, status.Name))
										}
									}

								}
							}

						}
					}
					time.Sleep(1 * time.Minute)
				}
			})

			stepLog = "Validate cloud snap deletion"
			Step(stepLog, func() {

				for _, ctx := range contexts {
					vols, err := Inst().S.GetVolumeParameters(ctx)
					log.FailOnError(err, fmt.Sprintf("error getting volume params for %s", ctx.App.Key))
					for vol, params := range vols {
						csBksps, err := Inst().V.GetCloudsnaps(vol, params)
						log.FailOnError(err, fmt.Sprintf("error getting cloud snaps for %s", vol))
						for _, bk := range csBksps {
							log.Infof("Deleting : %s having status : %v, Source volume: %s", bk.Id, bk.Status, bk.SrcVolumeName)
							err = Inst().V.DeleteAllCloudsnaps(vol, bk.SrcVolumeId, params)
							if err != nil && strings.Contains(err.Error(), "Key already exists") {
								continue
							}
							log.FailOnError(err, fmt.Sprintf("error deleting Cloudsnap %s", bk.Id))
						}
					}
				}
				opts := make(map[string]bool)
				opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
				ValidateAndDestroy(contexts, opts)
			})

		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		err = DeleteCloudSnapBucket(contexts)
		log.FailOnError(err, "failed to delete cloud snap bucket")
		AfterEachTest(contexts)
	})
})

func getVolumeAttachedNodeAndKill(volName *volume.Volume) (*node.Node, error) {
	appVol, err := Inst().V.InspectVolume(volName.ID)
	if err != nil {
		return nil, err
	}
	attachedNode := appVol.AttachedOn
	log.InfoD("Volume [%v] is attached to Node [%v]", volName.Name, attachedNode)

	nodeDetails, err := node.GetNodeByIP(attachedNode)
	if err != nil {
		return nil, err
	}

	killPxExecErr := KillPxExecUsingPid(nodeDetails)
	if killPxExecErr != nil {
		return nil, err
	}

	killPxStorageErr := KillPxStorageUsingPid(nodeDetails)
	if killPxStorageErr != nil {
		return nil, err
	}

	return &nodeDetails, nil
}

func isPodStuckNotRunning(nameSpace string) (bool, map[string]string, error) {
	restartDetails := make(map[string]string)
	isPodRestarting := false
	pods, err := GetAllPodsInNameSpace(nameSpace)
	if err != nil {
		log.InfoD("Get all pods from Namespace returned error")
		return false, nil, err
	}
	for _, eachPod := range pods {
		if eachPod.Status.Phase != "Running" && eachPod.Status.Phase != "ContainerCreating" && eachPod.Status.Phase != "Terminating" {
			restartDetails[eachPod.Name] = fmt.Sprintf("%v", eachPod.Status.Phase)
			log.InfoD("Pod Status of Pod [%v] is [%v]", eachPod.Name, eachPod.Status.Phase)
		}
	}
	if len(restartDetails) > 0 {
		isPodRestarting = true
	}
	return isPodRestarting, restartDetails, nil
}

var _ = Describe("{ContainerCreateDeviceRemoval}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("ContainerCreateDeviceRemoval",
			"Test app stuck in Container Creation with Device exists",
			nil, 0)
	})

	var contexts []*scheduler.Context
	appList := make([]string, 0)
	stepLog := "App Stuck in ContainerCreation State with Device Exists in the backend"
	It(stepLog, func() {

		var isPodRestarting bool = false
		var terminateAll bool = false

		for _, appName := range Inst().AppList {
			if strings.Contains(appName, "shared") || strings.Contains(appName, "svc") {
				appList = append(appList, appName)
			}
		}

		if len(appList) == 0 {
			log.FailOnError(fmt.Errorf("sharedv4 or sharedv4 svc apps are mandatory for the test"), "no sharedv4 or sharedv4 svc apps found to deploy")
		}

		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("volwithdeviceexist-%d", i))...)
		}
		ValidateApplications(contexts)

		// Sleep for some time for IO's to start running
		time.Sleep(30 * time.Second)

		allVolumes, err := GetAllVolumesWithIO(contexts)
		log.FailOnError(err, "Failed to get volumes with IO Running")

		log.InfoD("List of volumes with IO Running [%v]", allVolumes)

		// Get Random Volume with IO running on the cluster
		var randomVol *volume.Volume
		for _, eachVol := range allVolumes {
			log.InfoD("Trying volume [%s]", eachVol)
			if strings.Contains(eachVol.Name, "vdbench-pvc-enc-sv4-svc") {
				randomVol = eachVol
				break
			}
		}
		log.Infof("Volume picked for testing device exist issue [%v]", randomVol)

		appVol, err := Inst().V.InspectVolume(randomVol.ID)
		allNodesForVolume := appVol.ReplicaSets[0].Nodes
		log.InfoD("List of nodes for the volume [%s] are [%v]", randomVol.Name, allNodesForVolume)

		go func(nameSpace string) {
			defer GinkgoRecover()
			for {
				podRestarting, restartNode, err := isPodStuckNotRunning(nameSpace)
				if podRestarting && len(restartNode) > 1 {
					isPodRestarting = true
				}

				if err != nil {
					log.InfoD(fmt.Sprintf("Command reported error [%v] on Namespace [%v]", err, nameSpace))
				}

				if terminateAll {
					break
				}

				// Wait for a few min and retry pod status to check if it is settled
				time.Sleep(2 * time.Minute)
				_, restartNodeAfter, err := isPodStuckNotRunning(nameSpace)
				if podRestarting && len(restartNode) > 1 {
					for key := range restartNodeAfter {
						if restartNodeAfter[key] == restartNode[key] {
							isPodRestarting = true
						}
					}
				} else {
					isPodRestarting = false
				}
			}
		}(randomVol.Namespace)

		// Kill Random nodes associated with the volume one co-ordinator node every time
		previousNode := 0
		for loopKill := 0; loopKill < 30; loopKill++ {
			restartedNode := []node.Node{}
			if isPodRestarting == false {
				rand.Seed(time.Now().UnixNano())
				randomNumber := rand.Intn(3) + 1

				/*  Kill random nodes in parallel where 1 node killed will always be Co-ordinator Node.
					doing this to make devices present on all the nodes,
				    Upon restart all device present in the nodes should be removed.
				*/

				if (previousNode == randomNumber) && (randomNumber != 1) {
					previousNode = randomNumber - 1
				} else if (previousNode == randomNumber) && (randomNumber == 1) {
					previousNode = randomNumber + 1
				}

				nodeDetail, err := getVolumeAttachedNodeAndKill(randomVol)
				log.FailOnError(err, "Failed to kill Px Daemons")
				log.InfoD("Volume  is Attached on Node {%s}", nodeDetail.GetMgmtIp())
				restartedNode = append(restartedNode, *nodeDetail)

				if randomNumber == 3 {
					for i := 0; i < 2; i++ {
						nodeDetail, err := getVolumeAttachedNodeAndKill(randomVol)
						log.FailOnError(err, "Failed to kill Px Daemons")
						log.InfoD("Volume  is Attached on Node {%s}", nodeDetail.GetMgmtIp())
						restartedNode = append(restartedNode, *nodeDetail)
					}
				}

				if randomNumber == 2 {
					nodeDetail, err := getVolumeAttachedNodeAndKill(randomVol)
					log.FailOnError(err, "Failed to kill Px Daemons")
					log.InfoD("Volume  is Attached on Node {%s}", nodeDetail.GetMgmtIp())
					restartedNode = append(restartedNode, *nodeDetail)
				}

				if randomNumber == 1 {
					appVol, err = Inst().V.InspectVolume(randomVol.ID)
					log.FailOnError(err, "Failed to get volumes attachment details")
					attachedNode := appVol.AttachedOn
					log.InfoD("Volume [%v] is attached to Node [%v]", randomVol.Name, attachedNode)
				}

				previousNode = randomNumber

				appVol, err = Inst().V.InspectVolume(randomVol.ID)
				log.FailOnError(err, "Failed to get volumes attachment details")
				attachedNode := appVol.AttachedOn
				log.InfoD("Volume [%v] is attached to Node [%v]", randomVol.Name, attachedNode)

				for _, nodes := range restartedNode {
					// Wait for Node to Come back online
					err = Inst().V.WaitDriverUpOnNode(nodes, 20*time.Minute)
					log.FailOnError(err, "Failed Waiting for Node to Come Online")
				}
				time.Sleep(2 * time.Minute)
				restarting, _, err := isPodStuckNotRunning(randomVol.Namespace)
				if restarting == true {
					terminateAll = true
					break
				}

			} else {
				// Waiting for 10 min before checking the details
				time.Sleep(10 * time.Minute)
			}
		}
		if !terminateAll {
			appsValidateAndDestroy(contexts)
		}
		terminateAll = true
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

// blockiSCSIInterfaceOnNode blockRules and unblockRules iscsi interface on Node
func blockiSCSIPortOnNode(n *node.Node, block bool) error {

	// Block all InComing requests from port 3260 on specific Node
	command := fmt.Sprintf("iptables -A INPUT -p tcp -s %v --dport 3260 -j DROP ", n.MgmtIp)
	if !block {
		command = fmt.Sprintf("iptables -D INPUT -p tcp -s %v --dport 3260 -j DROP", n.MgmtIp)
	}
	log.InfoD("Triggering command [%s] from Node [%v]", command, n.Name)
	err := runCommand(command, *n)
	if err != nil {
		return err
	}

	// Block all Outgoing requests from port 3260 from specific Node
	command = fmt.Sprintf("iptables -A OUTPUT -p tcp -s %v --dport 3260 -j DROP ", n.MgmtIp)
	if !block {
		command = fmt.Sprintf("iptables -D OUTPUT -p tcp -s %v --dport 3260 -j DROP", n.MgmtIp)
	}
	log.InfoD("Triggering command [%s] from Node [%v]", command, n.Name)
	err = runCommand(command, *n)
	if err != nil {
		return err
	}

	return nil
}

// getPVCAccessMode returns list of Accessmodes for the PVCs
func getPVCAccessMode(pvcName string, pvcNameSpace string) ([]corev1.PersistentVolumeAccessMode, error) {
	var k8sCore = core.Instance()
	accessModes := []corev1.PersistentVolumeAccessMode{}
	pvc, err := k8sCore.GetPersistentVolumeClaim(pvcName, pvcNameSpace)
	if err != nil {
		return nil, err
	}

	for _, mode := range pvc.Spec.AccessModes {
		log.Infof("Access Mode [%v]", mode)
		accessModes = append(accessModes, mode)
	}

	return accessModes, nil
}

// Flush all IPTable Rules from all the Nodes Created
func flushAllIPtableRulesOnAllNodes() {
	for _, eachNode := range node.GetNodes() {
		command := "iptables -F"
		if !node.IsMasterNode(eachNode) {
			err := runCommand(command, eachNode)
			if err != nil {
				log.FailOnError(err, "Failed to flush iptable rules")
			}
		}
	}
}

var _ = Describe("{FADAPodRecoveryAfterBounce}", func() {

	/*
				PTX : https://purestorage.atlassian.net/browse/PWX-31647
			Test to check if the Pod bounces and Places in the new node when iscsi port fails to Send / Receive Traffic
			Specific to FADA Volumes with RWO type

		Px Implementation :
			A background task will periodically look for any readonly FADA volumes and those pods will be bounced.
			The default period is 15 seconds and this period is configurable through a cluster option called ro-vol-pod-bounce-interval
			If a volume was expected to be read-only during creation (e.g ReadOnlyMany PVCs), it’ll be excluded.

	*/
	JustBeforeEach(func() {
		StartTorpedoTest("FADAPodRecoveryAfterBounce",
			"Verify Pod Recovers from RO mode after Bounce",
			nil, 0)
	})

	itLog := "FADAPodRecoveryAfterBounce"
	It(itLog, func() {
		var contexts []*scheduler.Context
		var k8sCore = core.Instance()

		// Pick all the Volumes with RWO Status, We check if the Volume is with Access Mode RWO and PureBlock Volume
		vols := make([]*volume.Volume, 0)
		stepLog = "Schedule application"
		Step(stepLog, func() {
			contexts = make([]*scheduler.Context, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("fapodrecovery-%d", i))...)
			}
		})

		ValidateApplications(contexts)
		defer DestroyApps(contexts, nil)
		defer flushAllIPtableRulesOnAllNodes()

		stepLog = "Get all Pure Volumes and Validate "
		Step(stepLog, func() {
			for _, ctx := range contexts {
				appVols, err := Inst().S.GetPureVolumes(ctx, "pure_block")
				log.FailOnError(err, fmt.Sprintf("error getting volumes for app [%s]", ctx.App.Key))

				for _, eachVol := range appVols {
					accessModes, err := getPVCAccessMode(eachVol.Name, eachVol.Namespace)
					log.FailOnError(err, "Failed to get AccessModes for the volume [%v]", eachVol.Name)
					for _, eachAMode := range accessModes {
						// Validate if the Volume is Pure Volume
						boolVol, err := Inst().V.IsPureVolume(eachVol)
						log.FailOnError(err, "Failed to get details on the volume [%v]", eachVol.Name)

						// Get Details of the volume , check if the volume is PureBlock Volume
						pureVol, err := IsVolumeTypePureBlock(ctx, eachVol.ID)
						log.FailOnError(err, "Failed to get details on the volume [%v]", eachVol.Name)

						if eachAMode == "ReadWriteOnce" && boolVol && pureVol {
							vols = append(vols, eachVol)
						}
					}
				}
			}
		})

		stepLog = "Validate Pod Bounce on blocking network interface"
		Step(stepLog, func() {
			// Check if the Volume Counts matched criteria is > 0 , if not Fail the test
			log.Infof("List of all volumes present in the cluster [%v]", vols)
			dash.VerifyFatal(len(vols) != 0, true, fmt.Sprintf("failed to get list of Volumes belongs to Pure"))

			// Pick a random Volume to check the pod bounce
			randomIndex := rand.Intn(len(vols))
			volPicked := vols[randomIndex]
			log.Infof("Validating test scenario with Selected volumes [%v]", volPicked)

			inspectVolume, err := Inst().V.InspectVolume(volPicked.ID)
			log.FailOnError(err, "Volumes inspect errored out")
			log.Infof("VOLUME Inspect output [%v]", inspectVolume)

			pods, err := k8sCore.GetPodsUsingPVC(volPicked.Name, volPicked.Namespace)
			log.FailOnError(err, "unable to find the node from the pod")

			for _, eachPod := range pods {
				log.Infof("Validating test on Pod [%v]", eachPod)
				podNode, err := GetNodeFromIPAddress(eachPod.Status.HostIP)
				log.FailOnError(err, "unable to find the node from the pod")
				log.Infof("Pod with Name [%v] placed on Host [%v]", eachPod.Name, eachPod.Status.HostIP)

				// Stop iscsi traffic on the Node
				log.Infof("Blocking IPAddress on Node [%v]", podNode.Name)
				err = blockiSCSIPortOnNode(podNode, true)
				log.FailOnError(err, fmt.Sprintf("Failed to block iSCSI interface on Node [%v]", podNode.Name))

				// Sleep for some time before checking the pod status
				time.Sleep(180 * time.Second)

				// Pod details after blocking IP
				podsOnBlock, err := k8sCore.GetPodsUsingPVC(volPicked.Name, volPicked.Namespace)
				log.FailOnError(err, "unable to find the node from the pod")

				// Verify that Pod Bounces and not in Running state till the time iscsi rules are not reverted
				for _, eachPodAfter := range podsOnBlock {
					if eachPod.Name == eachPodAfter.Name &&
						eachPodAfter.Status.Phase == "Running" {
						log.FailOnError(fmt.Errorf("pod is in Running State  [%v]",
							eachPodAfter.Status.HostIP), "Pod is in Running state")
					}
					log.Infof("Pod with Name [%v] placed on Host [%v] and Phase [%v]",
						eachPod.Name, eachPodAfter.Status.HostIP, eachPodAfter.Status.Phase)
				}

				// Revert iscsi rules that was set on the node
				err = blockiSCSIPortOnNode(podNode, false)

				// Sleep for some time for Px to come up online and working
				time.Sleep(10 * time.Minute)

				// Pod details after blocking IP
				podsAfterblk, err := k8sCore.GetPodsUsingPVC(volPicked.Name, volPicked.Namespace)
				log.FailOnError(err, "unable to find the node from the pod")

				for _, eachPodAfter := range podsAfterblk {
					if eachPod.Name == eachPodAfter.Name &&
						eachPod.Status.StartTime == eachPodAfter.Status.StartTime &&
						eachPodAfter.Status.Phase != "Running" {
						log.FailOnError(fmt.Errorf("Pod didn't bounce on the node [%v]",
							eachPodAfter.Status.HostIP), "Pod didn't bounce on the node")
					}
					log.Infof("Pod with Name [%v] placed on Host [%v] and Phase [%v]",
						eachPod.Name, eachPodAfter.Status.HostIP, eachPodAfter.Status.Phase)
				}
				// Enter and Exit maintenance mode to bring Node up
				log.FailOnError(Inst().V.RecoverDriver(*podNode), "Failed during Node maintenance cycle ")

				// Validate if Volume Driver is up on all the nodes
				log.FailOnError(Inst().V.WaitDriverUpOnNode(*podNode, Inst().DriverStartTimeout),
					"Node did not start within the time specified")
			}
		})
	})

	JustAfterEach(func() {
		log.Infof("In Teardown")
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})

var _ = Describe("{FADAPodRecoveryAllPathDownUsingIptableRule}", func() {

	/*
				PTX : https://purestorage.atlassian.net/browse/PTX-19192
			Test to check if Pod recovers after blocking iptable Rules and later unblocking them

		Px Scenario 1  :
			ALL Path Down with FADA volumes test cases should be tested and automated
			Expectation : PODS should be up and running after connection restored.

	*/
	JustBeforeEach(func() {
		StartTorpedoTest("FADAPodRecoveryAllPathDownUsingIptableRule",
			"Verify Pod Recovers from RO mode after Bounce after blocking iptable Rules",
			nil, 0)
	})

	itLog := "FADAPodRecoveryAllPathDownUsingIptableRule"
	It(itLog, func() {

		var contexts []*scheduler.Context
		var k8sCore = core.Instance()

		// Pick all the Volumes with RWO Status, We check if the Volume is with Access Mode RWO and PureBlock Volume
		vols := make([]*volume.Volume, 0)
		stepLog = "Schedule application"
		Step(stepLog, func() {
			contexts = make([]*scheduler.Context, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				contexts = append(contexts, ScheduleApplications(fmt.Sprintf("fapodrecovery-%d", i))...)
			}
		})

		ValidateApplications(contexts)
		defer flushAllIPtableRulesOnAllNodes()

		stepLog = "Get all Pure Volumes and Validate "
		Step(stepLog, func() {
			for _, ctx := range contexts {
				appVols, err := Inst().S.GetPureVolumes(ctx, "pure_block")
				log.FailOnError(err, fmt.Sprintf("error getting volumes for app [%s]", ctx.App.Key))

				for _, eachVol := range appVols {
					accessModes, err := getPVCAccessMode(eachVol.Name, eachVol.Namespace)
					log.FailOnError(err, "Failed to get AccessModes for the volume [%v]", eachVol.Name)
					for _, eachAMode := range accessModes {
						// Validate if the Volume is Pure Volume
						boolVol, err := Inst().V.IsPureVolume(eachVol)
						log.FailOnError(err, "Failed to get details on the volume [%v]", eachVol.Name)

						// Get Details of the volume , check if the volume is PureBlock Volume
						pureVol, err := IsVolumeTypePureBlock(ctx, eachVol.ID)
						log.FailOnError(err, "Failed to get details on the volume [%v]", eachVol.Name)

						if eachAMode == "ReadWriteOnce" && boolVol && pureVol {
							vols = append(vols, eachVol)
						}
					}
				}
			}
		})

		stepLog = "Get List of all volumes present in the cluster "
		Step(stepLog, func() {
			// Check if the Volume Counts matched criteria is > 0 , if not Fail the test
			log.Infof("List of all volumes present in the cluster [%v]", vols)
			dash.VerifyFatal(len(vols) != 0, true, fmt.Sprintf("failed to get list of Volumes belongs to Pure"))

			log.Infof("List of all volumes present in the cluster [%v]", vols)
		})

		podNodes := []*corev1.Pod{}
		stepLog = "Block iscsi port on all the nodes where volume is placed"
		Step(stepLog, func() {
			for _, eachVol := range vols {
				inspectVolume, err := Inst().V.InspectVolume(eachVol.ID)
				log.FailOnError(err, "Volumes inspect errored out")
				log.Infof("VOLUME Inspect output [%v]", inspectVolume)

				pods, err := k8sCore.GetPodsUsingPVC(eachVol.Name, eachVol.Namespace)
				log.FailOnError(err, "unable to find the node from the pod")

				for _, eachPod := range pods {
					log.Infof("Validating test on Pod [%v]", eachPod)
					podNode, err := GetNodeFromIPAddress(eachPod.Status.HostIP)
					log.FailOnError(err, "unable to find the node from the pod")
					log.Infof("Pod with Name [%v] placed on Host [%v]", eachPod.Name, eachPod.Status.HostIP)

					// Stop iscsi traffic on the Node
					log.Infof("Blocking IPAddress on Node [%v]", podNode.Name)
					err = blockiSCSIPortOnNode(podNode, true)
					log.FailOnError(err, fmt.Sprintf("Failed to block iSCSI interface on Node [%v]", podNode.Name))
					podNodes = append(podNodes, &eachPod)
				}
			}
		})

		// Sleep for sometime for PVC's to go in RO mode while data ingest in progress
		time.Sleep(3 * time.Minute)

		stepLog = "Verify if pods are not in Running state after blocking iptables"
		Step(stepLog, func() {
			for _, eachVol := range vols {
				// Pod details after blocking IP
				podsOnBlock, err := k8sCore.GetPodsUsingPVC(eachVol.Name, eachVol.Namespace)
				log.FailOnError(err, "unable to find the node from the pod")

				// Verify that Pod Bounces and not in Running state till the time iscsi rules are not reverted
				for _, eachPodAfter := range podsOnBlock {
					if eachPodAfter.Status.Phase == "Running" {
						log.FailOnError(fmt.Errorf("pod is in Running State  [%v]",
							eachPodAfter.Status.HostIP), "Pod is in Running state")
					}
					log.Infof("Pod with Name [%v] placed on Host [%v] and Phase [%v]",
						eachPodAfter.Name, eachPodAfter.Status.HostIP, eachPodAfter.Status.Phase)
				}
			}

		})

		stepLog = "Unblock Iptable rules on all the nodes"
		Step(stepLog, func() {
			for _, eachPod := range podNodes {
				log.Infof("Validating test on Pod [%v]", eachPod)
				podNode, err := GetNodeFromIPAddress(eachPod.Status.HostIP)
				log.FailOnError(err, "unable to find the node from the pod")
				// Revert iscsi rules that was set on the node
				err = blockiSCSIPortOnNode(podNode, false)
			}
		})

		// Sleep for some time for Px to come up online and working
		time.Sleep(10 * time.Minute)

		stepLog = "Verify Each pod in Running State after bringing back iptable Rules"
		Step(stepLog, func() {
			for _, eachVol := range vols {
				// Pod details after blocking IP
				podsAfterRevert, err := k8sCore.GetPodsUsingPVC(eachVol.Name, eachVol.Namespace)
				log.FailOnError(err, "unable to find the node from the pod")

				for _, eachPod := range podsAfterRevert {
					if eachPod.Status.Phase != "Running" {
						log.FailOnError(fmt.Errorf("Pod didn't bounce on the node [%v]",
							eachPod.Status.HostIP), "Pod didn't bounce on the node")
					}
				}

			}
		})

		stepLog = "wait for driver Up on all the nodes where iptables were blocked and later recovered"
		Step(stepLog, func() {
			for _, eachPod := range podNodes {
				log.Infof("Validating test on Pod [%v]", eachPod)
				podNode, err := GetNodeFromIPAddress(eachPod.Status.HostIP)
				log.FailOnError(err, "unable to find the node from the pod")

				// Enter and Exit maintenance mode to bring Node up
				log.FailOnError(Inst().V.RecoverDriver(*podNode), "Failed during Node maintenance cycle ")

				// Validate if Volume Driver is up on all the nodes
				log.FailOnError(Inst().V.WaitDriverUpOnNode(*podNode, Inst().DriverStartTimeout),
					"Node did not start within the time specified")
			}
		})

		// Destroy All applications if test Passes
		DestroyApps(contexts, nil)

	})

	JustAfterEach(func() {
		log.Infof("In Teardown")
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})
