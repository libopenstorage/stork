package proto

import (
	"github.com/libopenstorage/gossip/types"
	"strconv"
	"testing"
	"time"
)

const DEFAULT_CLUSTER_ID = "test-cluster"

func addKey(g *GossiperImpl) types.StoreKey {
	key := types.StoreKey("new_key")
	value := "new_value"
	g.UpdateSelf(key, value)
	return key
}

func startNode(
	t *testing.T,
	selfIp string,
	nodeId types.NodeId,
	peerIps []string,
	peers map[types.NodeId]types.NodeUpdate,
) (*GossiperImpl, types.StoreKey) {
	g, _ := NewGossiperImpl(selfIp, nodeId, peerIps,
		types.DEFAULT_GOSSIP_VERSION)
	g.UpdateCluster(peers)
	key := addKey(g)
	return g, key
}

func TestQuorumAllNodesUpOneByOne(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9900",
		"127.0.0.2:9901",
	}

	// Start Node0 with cluster size 1
	node0 := types.NodeId("0")
	g0, _ := startNode(t, nodes[0], node0, []string{},
		map[types.NodeId]types.NodeUpdate{
			node0: types.NodeUpdate{nodes[0], true}})

	time.Sleep(g0.GossipInterval())
	status := g0.GetSelfStatus()
	if status != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP,
			status)
	}

	// Start Node1 with cluster size 2
	node1 := types.NodeId("1")
	peers := map[types.NodeId]types.NodeUpdate{
		node0: types.NodeUpdate{nodes[0], true},
		node1: types.NodeUpdate{nodes[1], true}}
	g1, _ := startNode(t, nodes[1], node1, []string{nodes[0]}, peers)
	g0.UpdateCluster(peers)

	time.Sleep(g1.GossipInterval() * time.Duration(len(nodes)+1))

	if g1.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 1 to have status: ", types.NODE_STATUS_UP)
	}

	// Check if Node0 is still Up
	if g0.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP)
	}

	g0.Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
	g1.Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
}

func TestQuorumNodeLoosesQuorumAndGainsBack(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9902",
		"127.0.0.2:9903",
	}

	node0 := types.NodeId("0")
	node1 := types.NodeId("1")
	// Start Node 0
	g0, _ := startNode(t, nodes[0], node0, []string{},
		map[types.NodeId]types.NodeUpdate{
			node0: types.NodeUpdate{nodes[0], true}})

	time.Sleep(g0.GossipInterval())
	selfStatus := g0.GetSelfStatus()
	if selfStatus != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP,
			" Got: ", selfStatus)
	}

	// Simulate new node was added by updating the cluster size, but the new node is not talking to node0
	// Node 0 should loose quorom 1/2
	g0.UpdateCluster(map[types.NodeId]types.NodeUpdate{
		node0: types.NodeUpdate{nodes[0], true},
		node1: types.NodeUpdate{nodes[1], true}})
	time.Sleep(g0.GossipInterval() * time.Duration(len(nodes)+1))
	selfStatus = g0.GetSelfStatus()
	if selfStatus != types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM,
			" Got: ", selfStatus)
	}

	// Sleep for quorum timeout
	time.Sleep(g0.quorumTimeout + 2*time.Second)

	selfStatus = g0.GetSelfStatus()
	if selfStatus != types.NODE_STATUS_NOT_IN_QUORUM {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_NOT_IN_QUORUM,
			" Got: ", selfStatus)
	}

	// Lets start the actual Node 1
	g1, _ := startNode(t, nodes[1], node1, []string{nodes[0]},
		map[types.NodeId]types.NodeUpdate{
			node0: types.NodeUpdate{nodes[0], true},
			node1: types.NodeUpdate{nodes[1], true}})

	// Sleep so that nodes gossip
	time.Sleep(g1.GossipInterval() * time.Duration(len(nodes)+1))

	selfStatus = g0.GetSelfStatus()
	if selfStatus != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP,
			" Got: ", selfStatus)
	}
	selfStatus = g1.GetSelfStatus()
	if selfStatus != types.NODE_STATUS_UP {
		t.Error("Expected Node 1 to have status: ", types.NODE_STATUS_UP,
			" Got: ", selfStatus)
	}
}

func TestQuorumTwoNodesLooseConnectivity(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9904",
		"127.0.0.2:9905",
	}

	node0 := types.NodeId("0")
	node1 := types.NodeId("1")
	g0, _ := startNode(t, nodes[0], node0, []string{},
		map[types.NodeId]types.NodeUpdate{
			node0: types.NodeUpdate{nodes[0], true}})

	time.Sleep(g0.GossipInterval())
	if g0.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_UP)
	}

	// Simulate new node was added by updating the cluster size, but the new node is not talking to node0
	// Node 0 should loose quorom 1/2
	g0.UpdateCluster(map[types.NodeId]types.NodeUpdate{
		node0: types.NodeUpdate{nodes[0], true},
		node1: types.NodeUpdate{nodes[1], true}})
	time.Sleep(g0.GossipInterval() * time.Duration(len(nodes)+1))
	if g0.GetSelfStatus() != types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM)
	}

	// Lets start the actual node 1. We do not supply node 0 Ip address here so that node 1 does not talk to node 0
	// to simulate NO connectivity between node 0 and node 1
	g1, _ := startNode(t, nodes[1], node1, []string{},
		map[types.NodeId]types.NodeUpdate{
			node0: types.NodeUpdate{nodes[0], true},
			node1: types.NodeUpdate{nodes[1], true}})

	// For node 0 the status will change from UP_WAITING_QUORUM to WAITING_QUORUM after
	// the quorum timeout
	time.Sleep(g0.quorumTimeout + 5*time.Second)

	if g0.GetSelfStatus() != types.NODE_STATUS_NOT_IN_QUORUM {
		t.Error("Expected Node 0 to have status: ", types.NODE_STATUS_NOT_IN_QUORUM, " Got: ", g0.GetSelfStatus())
	}
	if g1.GetSelfStatus() != types.NODE_STATUS_NOT_IN_QUORUM {
		t.Error("Expected Node 1 to have status: ", types.NODE_STATUS_NOT_IN_QUORUM, "Got: ", g1.GetSelfStatus())
	}
}

func TestQuorumOneNodeIsolated(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9906",
		"127.0.0.2:9907",
		"127.0.0.3:9908",
	}

	peers := getNodeUpdateMap(nodes)
	var gossipers []*GossiperImpl
	for i, ip := range nodes {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		var g *GossiperImpl
		if i == 0 {
			g, _ = startNode(t, ip, nodeId, []string{}, peers)
		} else {
			g, _ = startNode(t, ip, nodeId, []string{nodes[0]}, peers)
		}

		gossipers = append(gossipers, g)
	}

	// Lets sleep so that the nodes gossip and update their quorum
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))

	for i, g := range gossipers {
		if g.GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", g.GetSelfStatus())
		}
	}

	// Isolate node 1
	// Simulate isolation by stopping gossiper for node 1 and starting it back,
	// but by not providing peer IPs and setting cluster size to 3.
	gossipers[1].Stop(time.Duration(10) * time.Second)
	gossipers[1].InitStore(types.NodeId("1"), "v1", types.NODE_STATUS_NOT_IN_QUORUM, DEFAULT_CLUSTER_ID)
	gossipers[1].Start([]string{})

	// Lets sleep so that the nodes gossip and update their quorum
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))

	for i, g := range gossipers {
		if i == 1 {
			if g.GetSelfStatus() != types.NODE_STATUS_NOT_IN_QUORUM {
				t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_NOT_IN_QUORUM, " Got: ", g.GetSelfStatus())
			}
			continue
		}
		if g.GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", g.GetSelfStatus())
		}
	}
}

func TestQuorumNetworkPartition(t *testing.T) {
	printTestInfo()
	nodes := []string{
		"127.0.0.1:9909",
		"127.0.0.2:9910",
		"127.0.0.3:9911",
		"127.0.0.4:9912",
		"127.0.0.5:9913",
	}

	// Simulate a network parition. Node 0-2 in parition 1. Node 3-4 in partition 2.
	var gossipers []*GossiperImpl
	// Partition 1
	for i := 0; i < 3; i++ {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		var g *GossiperImpl
		g, _ = startNode(t, nodes[i], nodeId,
			[]string{nodes[0], nodes[1], nodes[2]},
			map[types.NodeId]types.NodeUpdate{
				nodeId: types.NodeUpdate{nodes[i], true}})
		gossipers = append(gossipers, g)
	}
	// Parition 2
	for i := 3; i < 5; i++ {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		var g *GossiperImpl
		g, _ = startNode(t, nodes[i], nodeId, []string{nodes[3], nodes[4]},
			map[types.NodeId]types.NodeUpdate{
				nodeId: types.NodeUpdate{nodes[i], true}})
		gossipers = append(gossipers, g)
	}
	// Let the nodes gossip
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))
	for i, g := range gossipers {
		if g.GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", g.GetSelfStatus())
		}
	}

	peers := getNodeUpdateMap(nodes)
	// Setup the partition by updating the cluster size
	for _, g := range gossipers {
		g.UpdateCluster(peers)
	}

	// Let the nodes update their quorum
	time.Sleep(time.Duration(3) * time.Second)
	// Partition 1
	for i := 0; i < 3; i++ {
		if gossipers[i].GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", gossipers[i].GetSelfStatus())
		}

	}
	// Parition 2
	for i := 3; i < 5; i++ {
		if gossipers[i].GetSelfStatus() != types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM, " Got: ", gossipers[i].GetSelfStatus())
		}
	}

	time.Sleep(TestQuorumTimeout)
	// Parition 2
	for i := 3; i < 5; i++ {
		if gossipers[i].GetSelfStatus() != types.NODE_STATUS_NOT_IN_QUORUM {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_NOT_IN_QUORUM, " Got: ", gossipers[i].GetSelfStatus())
		}
	}
}

func TestQuorumEventHandling(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9914",
		"127.0.0.2:9915",
		"127.0.0.3:9916",
		"127.0.0.4:9917",
		"127.0.0.5:9918",
	}

	// Start all nodes
	var gossipers []*GossiperImpl
	for i := 0; i < len(nodes); i++ {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		var g *GossiperImpl
		g, _ = startNode(t, nodes[i], nodeId, []string{nodes[0]},
			map[types.NodeId]types.NodeUpdate{
				nodeId: types.NodeUpdate{nodes[0], true}})
		gossipers = append(gossipers, g)
	}

	// Let the nodes gossip
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	peers := getNodeUpdateMap(nodes)
	// Update the cluster size to 5
	for i := 0; i < len(nodes); i++ {
		gossipers[i].UpdateCluster(peers)
	}

	time.Sleep(2 * time.Second)

	// Bring node 4 down.
	gossipers[4].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))
	//time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	time.Sleep(2 * time.Second)

	for i := 0; i < len(nodes)-1; i++ {
		if gossipers[i].GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", gossipers[i].GetSelfStatus())
		}
	}

	// Bring node 3,node 2, node 1 down
	gossipers[3].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))
	gossipers[2].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))
	gossipers[1].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
	//time.Sleep(types.DEFAULT_GOSSIP_INTERVAL)

	if gossipers[0].GetSelfStatus() != types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
		t.Error("Expected Node 0 status to be ", types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM, " Got: ", gossipers[0].GetSelfStatus())
	}

	// Start Node 2
	gossipers[2].Start([]string{nodes[0]})
	gossipers[2].UpdateCluster(peers)

	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL)

	// Node 0 still not in quorum. But should be up as quorum timeout not occured yet
	if gossipers[0].GetSelfStatus() != types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
		t.Error("Expected Node 0  status to be ",
			types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM, " Got: ",
			gossipers[0].GetSelfStatus())
	}

	// Sleep for quorum timeout to occur
	time.Sleep(gossipers[0].quorumTimeout + 2*time.Second)

	if gossipers[0].GetSelfStatus() != types.NODE_STATUS_NOT_IN_QUORUM {
		t.Error("Expected Node 0 status to be ", types.NODE_STATUS_NOT_IN_QUORUM,
			" Got: ", gossipers[0].GetSelfStatus())
	}

	// Start Node 1
	gossipers[1].Start([]string{nodes[0]})
	gossipers[1].UpdateCluster(peers)

	time.Sleep(time.Duration(2) * types.DEFAULT_GOSSIP_INTERVAL)

	// Node 0 should now be up
	if gossipers[0].GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 status to be ", types.NODE_STATUS_UP, " Got: ", gossipers[0].GetSelfStatus())
	}

}

func TestQuorumRemoveNodes(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9919",
		"127.0.0.2:9920",
		"127.0.0.3:9921",
		"127.0.0.4:9922",
	}

	peers := getNodeUpdateMap(nodes)
	var gossipers []*GossiperImpl
	for i, ip := range nodes {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		var g *GossiperImpl
		if i == 0 {
			g, _ = startNode(t, ip, nodeId, []string{}, peers)
		} else {
			g, _ = startNode(t, ip, nodeId, []string{nodes[0]}, peers)
		}

		gossipers = append(gossipers, g)
	}

	// Lets sleep so that the nodes gossip and update their quorum
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))

	for i, g := range gossipers {
		if g.GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", g.GetSelfStatus())
		}
	}

	// Bring node 3,node 2 down
	gossipers[3].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))
	gossipers[2].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))

	for i := 0; i < 2; i++ {
		if gossipers[i].GetSelfStatus() != types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM, " Got: ", gossipers[i].GetSelfStatus())
		}
	}

	//Remove the two nodes
	delete(peers, types.NodeId("2"))
	delete(peers, types.NodeId("3"))
	gossipers[0].UpdateCluster(peers)
	gossipers[1].UpdateCluster(peers)

	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL)

	for i := 0; i < 2; i++ {
		if gossipers[i].GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ", types.NODE_STATUS_UP, " Got: ", gossipers[i].GetSelfStatus())
		}
	}
}

func TestQuorumAddNodes(t *testing.T) {
	printTestInfo()
	node0Ip := "127.0.0.1:9923"
	node0 := types.NodeId("0")
	peers := make(map[types.NodeId]types.NodeUpdate)
	peers[node0] = types.NodeUpdate{node0Ip, true}
	g0, _ := startNode(t, node0Ip, node0, []string{}, peers)

	// Lets sleep so that the nodes gossip and update their quorum
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(1))

	if g0.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 status to be ", types.NODE_STATUS_UP,
			" Got: ", g0.GetSelfStatus())
	}

	// Add a new node
	node1 := types.NodeId("1")
	node1Ip := "127.0.0.2:9924"
	peers[node1] = types.NodeUpdate{node1Ip, true}
	g0.UpdateCluster(peers)

	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL)
	if g0.GetSelfStatus() != types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
		t.Error("Expected Node 0 status to be ", types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM, " Got: ", g0.GetSelfStatus())
	}

	// Start the new node
	startNode(t, node1Ip, node1, []string{node0Ip}, peers)

	// Lets sleep so that the nodes gossip and update their quorum
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(3))

	if g0.GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 0 status to be ", types.NODE_STATUS_UP, " Got: ", g0.GetSelfStatus())
	}
}

func TestNonQuorumMembersAddRemove(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9925",
		"127.0.0.2:9926",
	}

	peers := getNodeUpdateMap(nodes)
	for id, peer := range peers {
		peer.QuorumMember = false
		peers[id] = peer
	}
	var gossipers []*GossiperImpl
	for i, ip := range nodes {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		var g *GossiperImpl
		if i == 0 {
			g, _ = startNode(t, ip, nodeId, []string{}, peers)
		} else {
			g, _ = startNode(t, ip, nodeId, []string{nodes[0]}, peers)
		}
		gossipers = append(gossipers, g)
	}

	// Lets sleep so that the nodes gossip and update their quorum
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))

	for i, g := range gossipers {
		if g.GetSelfStatus() != types.NODE_STATUS_NOT_IN_QUORUM {
			t.Error("Expected Node ", i, " status to be ",
				types.NODE_STATUS_NOT_IN_QUORUM, " Got: ",
				g.GetSelfStatus())
		}
	}

	// 1. Add another non-quorum member and check cluster still waits for quorum.
	// 2. Add quorum member and check cluster gets into quuorum state.
	newNodes := []string{
		"127.0.0.1:9927",
		"127.0.0.2:9928",
		"127.0.0.3:9929",
	}
	for i, node := range newNodes {
		quorumMember := i != 0
		nodeId := types.NodeId(strconv.Itoa(len(peers)))
		peers[nodeId] = types.NodeUpdate{node, quorumMember}
		for _, g := range gossipers {
			g.UpdateCluster(peers)
		}

		// Start the new node
		newGossiper, _ := startNode(t, node, nodeId, []string{nodes[0]}, peers)
		gossipers = append(gossipers, newGossiper)
		// Sleep so that the nodes gossip and update their quorum
		time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(3))

		expectedStatus := types.NODE_STATUS_NOT_IN_QUORUM
		if quorumMember {
			expectedStatus = types.NODE_STATUS_UP
		}
		for j, g := range gossipers {
			if g.GetSelfStatus() != expectedStatus {
				t.Error("Expected Node ", j, " status to be ", expectedStatus,
					" Got: ", g.GetSelfStatus())
			}
		}
	}

	// nodes are all non-quorum nodes
	nodes = append(nodes, newNodes[0])
	// newNodes are quorum deciding members
	newNodes = newNodes[1:]
	totalNumNodes := len(nodes) + len(newNodes)

	// Shutdown non-quorum members and check cluster is still in quorum
	for i, _ := range nodes {
		gossipers[i].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(
			totalNumNodes))
		time.Sleep(
			types.DEFAULT_GOSSIP_INTERVAL * time.Duration(totalNumNodes+1))
		for j := i + 1; j < totalNumNodes; j++ {
			if gossipers[j].GetSelfStatus() != types.NODE_STATUS_UP {
				t.Error("Expected Node ", i, " status to be ",
					types.NODE_STATUS_UP,
					" Got: ", gossipers[j].GetSelfStatus())
			}
		}

		// Remove then node
		delete(peers, types.NodeId(strconv.Itoa(i)))
		for j := i + 1; j < totalNumNodes; j++ {
			gossipers[j].UpdateCluster(peers)
		}
		time.Sleep(
			types.DEFAULT_GOSSIP_INTERVAL * time.Duration(totalNumNodes+1))

		for j := i + 1; j < totalNumNodes; j++ {
			if gossipers[j].GetSelfStatus() != types.NODE_STATUS_UP {
				t.Error("Expected Node ", j, " status to be ",
					types.NODE_STATUS_UP, " Got: ",
					gossipers[j].GetSelfStatus())
			}
		}
	}
}

func TestMajorityNonQuorumMembersGoDown(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9930",
		"127.0.0.2:9931",
		"127.0.0.3:9932",
	}

	peers := getNodeUpdateMap(nodes)
	for id, peer := range peers {
		if peer.Addr != nodes[2] {
			peer.QuorumMember = false
			peers[id] = peer
		}
	}
	var gossipers []*GossiperImpl
	for i, ip := range nodes {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		var g *GossiperImpl
		if i == 0 {
			g, _ = startNode(t, ip, nodeId, []string{}, peers)
		} else {
			g, _ = startNode(t, ip, nodeId, []string{nodes[0]}, peers)
		}
		gossipers = append(gossipers, g)
	}

	// Sleep so that the nodes gossip and update their quorum
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))

	for i, g := range gossipers {
		if g.GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ",
				types.NODE_STATUS_UP, " Got: ",
				g.GetSelfStatus())
		}
	}

	// Shutdown non-quorum members at once, which are in majority
	// and check cluster remains in quorum
	gossipers[0].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(
		len(nodes)))
	gossipers[1].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(
		len(nodes)))

	time.Sleep(
		types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
	if gossipers[2].GetSelfStatus() != types.NODE_STATUS_UP {
		t.Error("Expected Node 2 status to be ",
			types.NODE_STATUS_UP, " Got: ", gossipers[2].GetSelfStatus())
	}
}

func TestNonMajorityQuorumMembersGoDown(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9934",
		"127.0.0.2:9935",
		"127.0.0.3:9936",
	}

	// nodes[2] is only quorum member
	peers := getNodeUpdateMap(nodes)
	for id, peer := range peers {
		if peer.Addr != nodes[2] {
			peer.QuorumMember = false
			peers[id] = peer
		}
	}
	var gossipers []*GossiperImpl
	for i, ip := range nodes {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		var g *GossiperImpl
		if i == 0 {
			g, _ = startNode(t, ip, nodeId, []string{}, peers)
		} else {
			g, _ = startNode(t, ip, nodeId, []string{nodes[0]}, peers)
		}
		gossipers = append(gossipers, g)
	}

	// Sleep so that the nodes gossip and update their quorum
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))

	for i, g := range gossipers {
		if g.GetSelfStatus() != types.NODE_STATUS_UP {
			t.Error("Expected Node ", i, " status to be ",
				types.NODE_STATUS_UP, " Got: ",
				g.GetSelfStatus())
		}
	}

	// Shutdown quorum members at once, which are in majority
	// and check cluster remains in quorum
	gossipers[2].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(
		len(nodes)))

	time.Sleep(
		types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
	for i := 0; i < 2; i++ {
		if gossipers[i].GetSelfStatus() !=
			types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM {
			t.Error("Expected Node", i, " status to be ",
				types.NODE_STATUS_SUSPECT_NOT_IN_QUORUM, " Got: ",
				gossipers[i].GetSelfStatus())
		}
	}

	// Sleep for quorum timeout
	time.Sleep(gossipers[0].quorumTimeout + 2*time.Second)
	for i := 0; i < 2; i++ {
		if gossipers[i].GetSelfStatus() !=
			types.NODE_STATUS_NOT_IN_QUORUM {
			t.Error("Expected Node", i, " status to be ",
				types.NODE_STATUS_NOT_IN_QUORUM, " Got: ",
				gossipers[i].GetSelfStatus())
		}
	}
}
