package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
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

// getKvdbMasterPID returns the PID of KVDB master node
func getKvdbMasterPID(kvdbNode node.Node) (string, error) {
	var processPid string
	command := "ps -ef | grep -i px-etcd"
	out, err := Inst().N.RunCommand(kvdbNode, command, node.ConnectionOpts{
		Timeout:         20 * time.Second,
		TimeBeforeRetry: 5 * time.Second,
		Sudo:            true,
	})
	if err != nil {
		return "", err
	}

	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		if strings.Contains(line, "px-etcd start") && !strings.Contains(line, "grep") {
			fields := strings.Fields(line)
			processPid = fields[1]
			break
		}
	}
	return processPid, err
}

// killKvdbNode return error in case of command failure
func killKvdbNode(kvdbNode node.Node) error {
	pid, err := getKvdbMasterPID(kvdbNode)
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
	err := runCommand(command, n)
	if err != nil {
		return err
	}
	return nil
}

// getReplicaNodes returns the list of nodes which has replicas
func getReplicaNodes(vol *volume.Volume) ([]string, error) {
	getReplicaSets, err := Inst().V.GetReplicaSets(vol)
	if err != nil {
		return nil, err
	}
	return getReplicaSets[0].Nodes, nil
}

// setReplicaFactorToReplThreeVol Set the replication factor of the volume to 3 , this is needed as a pre-requisites
func setReplicaFactorToReplThreeVol(vol *volume.Volume) error {
	getReplicaSets, err := Inst().V.GetReplicaSets(vol)
	log.FailOnError(err, fmt.Sprintf("Failed to get replication factor on the volume [%v]", vol.Name))
	if len(getReplicaSets[0].Nodes) != 3 {
		log.InfoD(fmt.Sprintf("setting replication factor of volume [%v] to 3", vol.Name))
		err := Inst().V.SetReplicationFactor(vol, 3, nil, nil, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func WaitForReplicationComplete(vol *volume.Volume, replFactor int64) error {
	replicationUpdateTimeout := 60 * time.Minute
	err := Inst().V.WaitForReplicationToComplete(vol, replFactor, replicationUpdateTimeout)
	if err != nil {
		return err
	}
	return nil
}

// setVolumesWithReplicaOnBothZones Configure volume to have replicas spread across zones
func setVolumesWithReplicaOnBothZones(vol *volume.Volume, zone1 []node.Node, zone2 []node.Node) error {

	// setting replica 2 on the volumes
	matchedZone1Nodes := []node.Node{}
	matchedZone2Nodes := []node.Node{}
	replicaNodes, err := getReplicaNodes(vol)
	if err != nil {
		return err
	}
	for _, eachRf := range replicaNodes {
		for _, eachNode := range zone1 {
			if eachRf == eachNode.VolDriverNodeID {
				matchedZone1Nodes = append(matchedZone1Nodes, eachNode)
			}
		}
		for _, eachNode := range zone2 {
			if eachRf == eachNode.VolDriverNodeID {
				matchedZone2Nodes = append(matchedZone2Nodes, eachNode)
			}
		}
	}
	if len(matchedZone1Nodes) >= 1 && len(matchedZone2Nodes) >= 1 {
		log.InfoD(fmt.Sprintf("Volume [%v] is having repl nodes from both zones", vol.Name))
		return nil
	} else if len(matchedZone1Nodes) >= 1 && len(matchedZone2Nodes) == 0 {
		log.InfoD(fmt.Sprintf("Volume [%v] is having repl nodes from Zone1 but not from zone2", vol.Name))
		// Pick the random node from Zone2 and append it to be part of node which can have repl
		randomIndex := rand.Intn(len(zone2))
		randomNode := zone2[randomIndex]
		replNode := []string{randomNode.VolDriverNodeID}

		log.InfoD(fmt.Sprintf("[%s]", replNode))
		replNodes, err := getReplicaNodes(vol)
		if err != nil {
			return err
		}

		if len(replNodes) != 3 {
			err = Inst().V.SetReplicationFactor(vol, 3, replNode, nil, false)
			if err != nil {
				return err
			}
		}

	} else {
		log.InfoD(fmt.Sprintf("Volume [%v] is having repl nodes from Zone2 but not from zone1", vol.Name))
		// Pick the random node from Zone2 and append it to be part of node which can have repl
		randomIndex := rand.Intn(len(zone1))
		randomNode := zone1[randomIndex]
		replNode := []string{randomNode.VolDriverNodeID}
		replNodes, err := getReplicaNodes(vol)
		if err != nil {
			return err
		}
		if len(replNodes) != 3 {
			log.InfoD(fmt.Sprintf("[%s]", replNode))
			err = Inst().V.SetReplicationFactor(vol, 3, replNode, nil, false)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

var _ = Describe("{FordRunFlatResync}", func() {
	/*
		Test Needs 10 VM's running with Internal KVDB
		Cluster should have 6 StorageNodes and 4 Storageless nodes
		App Used Mongodb on multiple Namespace would be running on both storage and storage-less nodes
		Set the IPtables rules in some nodes  of that subnet such that they will not reachable from all nodes in second subnet
		Make sure cluster is up and IOs are running for MongoDB apps. Most of the volumes will go into degraded state
		Wait around a 30 minutes to hours for more IOs.
		Now set IPtables rules in another subnet subnet
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
		contexts = make([]*scheduler.Context, 0)
		Inst().AppList = []string{}
		var ioIntensiveApp = []string{"fio", "fio-writes"}
		for _, eachApp := range ioIntensiveApp {
			Inst().AppList = append(Inst().AppList, eachApp)
		}
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			contexts = append(contexts, ScheduleApplications(fmt.Sprintf("replresyncpoolexpand-%d", i))...)
		}

		time.Sleep(2 * time.Minute)
		ValidateApplications(contexts)
		defer appsValidateAndDestroy(contexts)

		// Check if the cluster consists of 10 nodes
		log.InfoD("Get all nodes present in the cluster")

		allNodes := []node.Node{}
		// create an array with storage and storage less nodes added
		for _, each := range node.GetStorageNodes() {
			allNodes = append(allNodes, each)
		}

		for _, each := range node.GetStorageLessNodes() {
			allNodes = append(allNodes, each)
		}

		// Verify total nodes available is 10
		dash.VerifyFatal(len(allNodes) == 10, true, "required minimum of 10 nodes for the test to run")

		var nodesSplit1 = []node.Node{}
		var nodesSplit2 = []node.Node{}
		if len(node.GetStorageLessNodes()) != 4 && len(node.GetStorageNodes()) > 6 {
			nodesSplit1 = allNodes[0:4]
			nodesSplit2 = allNodes[5:10]
		} else {
			nodesSplit1 = node.GetStorageNodes()
			nodesSplit2 = node.GetStorageLessNodes()
		}

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

		log.InfoD("KVDB Leader node is [%v]", getKvdbLeaderNode.Addresses)
		var allStorageExceptKVDB []node.Node
		for _, each := range nodesSplit1 {
			if each.Id != getKvdbLeaderNode.Id {
				allStorageExceptKVDB = append(allStorageExceptKVDB, each)
			}
		}

		// Create 2 zones from the Storage and storageless Node
		// Each Zone will have 3 storage and 2 Storageless Node
		zone1 := []node.Node{}
		zone2 := []node.Node{}

		zone1StorageEle, zone2StorageEle := getRandomNumbersFromArrLength(len(allStorageExceptKVDB), len(allStorageExceptKVDB)/2)
		for _, each := range zone1StorageEle {
			zone1 = append(zone1, allStorageExceptKVDB[each])
		}
		for _, each := range zone2StorageEle {
			zone2 = append(zone2, allStorageExceptKVDB[each])
		}

		zone1StorageLessEle, zone2StorageLessEle := getRandomNumbersFromArrLength(len(nodesSplit2), len(nodesSplit2)/2)
		for _, each := range zone1StorageLessEle {
			zone1 = append(zone1, nodesSplit2[each])
		}
		for _, each := range zone2StorageLessEle {
			zone2 = append(zone2, nodesSplit2[each])
		}

		// The below lines are intentionally commented out , will be either uncommented / deleted after debugging new scenarios
		/*
			// Set Volume replica on the nodes so that each zone will contribute to replica on the volumes
			for _, ctx := range contexts {
				vols, err := Inst().S.GetVolumes(ctx)
				if err != nil {
					log.FailOnError(err, "failed to get volumes from the contexts")
				}

				// This is done to make sure that volumes should have replica on nodes from both zones
				for _, eachVol := range vols {
					log.FailOnError(setVolumesWithReplicaOnBothZones(eachVol, zone1, zone2), fmt.Sprintf("Failed to set Replica on the volume [%v]", eachVol.Name))
				}
			}*/

		flushiptables := func() {
			if !iptablesflushed {
				// Flush all iptables from all the nodes available forcefully
				for _, eachNode := range allNodes {
					log.InfoD("Flushing iptables rules on node [%v]", eachNode.Name)
					log.FailOnError(flushIptableRules(eachNode), "Iptables flush all failed on node [%v]", eachNode.Name)
				}
			}
		}

		// force flush iptables on all the nodes at the end
		defer flushiptables()

		volumeState, err := GetVolumesInDegradedState(contexts)
		if err != nil {
			fmt.Println("Error not Nil")
		}
		fmt.Println(volumeState)

		revertZone1 := func() {
			log.FailOnError(blockIptableRules(zone1, zone2, true), "Failed to unblock IPTable rules on target Nodes")
		}

		revertZone2 := func() {
			log.FailOnError(blockIptableRules(zone2, zone1, true), "Failed to unblock IPTable rules on target Nodes")
		}

		// From Zone 1 block all the traffic to systems under zone2
		// From Zone 2 block all the traffic to systems under zone1
		log.InfoD("blocking iptables from all nodes present in zone1 from accessing zone2")
		err = blockIptableRules(zone1, zone2, false)
		log.FailOnError(err, "Failed to revert IPtable Rules on Zone1")

		time.Sleep(20 * time.Minute)

		log.InfoD("blocking iptables from all nodes present in zone2 from accessing zone1 ")
		err = blockIptableRules(zone2, zone1, false)
		log.FailOnError(err, "Failed to set IPtable Rules on zone2")

		// Reverting back Zone1 iptables set
		revertZone1()

		// Reverting back zone2 iptables set
		revertZone2()

		// Reset iptables rules on vms under zone2
		err = blockIptableRules(zone2, zone1, false)
		log.FailOnError(err, "Failed to set IPtable Rules on zone2")

		log.InfoD("Killing KVDB PID from KVDB Master Node")
		log.FailOnError(killKvdbNode(getKvdbLeaderNode), "failed to Kill Kvdb Node")
		// Block IPtable rules on the kvdb node to all the nodes in zone 1
		kvdb := []node.Node{getKvdbLeaderNode}

		time.Sleep(10 * time.Minute)
		log.FailOnError(blockIptableRules(kvdb, zone1, false), "Set IPTable rules on kvdb node failed")

		// Wait for some time before checking for file system goes back online
		time.Sleep(30 * time.Minute)

		// Revert back the iptable rules from the kvdb node
		log.FailOnError(blockIptableRules(kvdb, zone1, true), "Reverting back IPTable rules on kvdb node failed")

		// Flushing iptables rules on all the nodes present in the cluster before making sure that nodes to come up online
		flushiptables()
		iptablesflushed = true

		// Wait for some time for system to be up and all nodes drivers up and running
		for _, each := range node.GetStorageNodes() {
			err = Inst().V.WaitDriverUpOnNode(each, 2*time.Minute)
			log.FailOnError(err, fmt.Sprintf("Driver is down on node %s", each.Name))
		}

		for _, ctx := range contexts {
			vols, err := Inst().S.GetVolumes(ctx)
			if err != nil {
				log.FailOnError(err, "failed to get volumes from the contexts")
			}

			// This is done to make sure that volumes should have replica on nodes from both zones
			for _, eachVol := range vols {
				log.FailOnError(VerifyVolumeStatusOnline(eachVol), fmt.Sprintf("Volume [%v] is not in expected state", eachVol.Name))
			}
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		AfterEachTest(contexts)
	})
})
