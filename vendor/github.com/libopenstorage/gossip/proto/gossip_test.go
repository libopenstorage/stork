package proto

import (
	"github.com/libopenstorage/gossip/types"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	TestQuorumTimeout time.Duration = 30 * time.Second
)

// New returns an initialized Gossip node
// which identifies itself with the given ip
func newGossiperImpl(ip string, selfNodeId types.NodeId, knownIps []string, version, clusterId string) (*GossiperImpl, error) {
	g := new(GossiperImpl)
	gi := types.GossipIntervals{
		GossipInterval:   types.DEFAULT_GOSSIP_INTERVAL,
		PushPullInterval: types.DEFAULT_PUSH_PULL_INTERVAL,
		ProbeInterval:    types.DEFAULT_PROBE_INTERVAL,
		ProbeTimeout:     types.DEFAULT_PROBE_TIMEOUT,
		QuorumTimeout:    TestQuorumTimeout,
	}
	g.Init(ip, selfNodeId, 1, gi, version, clusterId)
	g.selfCorrect = false
	err := g.Start(knownIps)
	return g, err
}

func NewGossiperImpl(
	ip string,
	selfNodeId types.NodeId,
	knownIps []string,
	version string,
) (*GossiperImpl, error) {
	return newGossiperImpl(ip, selfNodeId, knownIps, version, DEFAULT_CLUSTER_ID)
}

func NewGossiperImplWithClusterId(
	ip string,
	selfNodeId types.NodeId,
	knownIps []string,
	version,
	clusterId string,
) (*GossiperImpl, error) {
	return newGossiperImpl(ip, selfNodeId, knownIps, version, clusterId)
}

func getNodeUpdateMap(nodesIp []string) map[types.NodeId]types.NodeUpdate {
	peers := make(map[types.NodeId]types.NodeUpdate)
	for i, ip := range nodesIp {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		peers[nodeId] = types.NodeUpdate{ip, true}
	}
	return peers
}

func TestGossiperStartStopGetNode(t *testing.T) {
	printTestInfo()

	nodesIp := []string{
		"127.0.0.1:8123",
		"127.0.0.2:8124",
		"127.0.0.3:8125",
		"127.0.0.4:8126",
		"127.0.0.5:8127",
	}

	peers := getNodeUpdateMap(nodesIp)

	clusterSize := len(nodesIp)
	gossipers := make([]*GossiperImpl, clusterSize)
	gossipers[0], _ = NewGossiperImpl(nodesIp[0], types.NodeId(strconv.Itoa(0)), []string{}, types.DEFAULT_GOSSIP_VERSION)
	gossipers[0].UpdateCluster(peers)
	// test add nodes
	for i := 1; i < len(nodesIp); i++ {
		gossipers[i], _ = NewGossiperImpl(nodesIp[i], types.NodeId(strconv.Itoa(i)), []string{nodesIp[0]}, types.DEFAULT_GOSSIP_VERSION)
		gossipers[i].UpdateCluster(peers)
	}

	// try adding existing node by starting gossiper on other nodes
	_, err := NewGossiperImpl(nodesIp[1], types.NodeId(strconv.Itoa(1)), []string{nodesIp[0]}, types.DEFAULT_GOSSIP_VERSION)
	if err == nil {
		t.Error("Duplicate node addition did not fail")
	}

	// Assuming the worst case time required to gossip to all other nodes
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodesIp)))
	var peerNodes []string
	// check the nodelist is same
	for i := range nodesIp {
		peerNodes = gossipers[i].GetNodes()
		if len(peerNodes) != len(nodesIp) {
			t.Error("Peer nodes len does not match added nodes, got: ",
				peerNodes, " expected: ", nodesIp)
		}
	}
outer:
	for _, origNode := range nodesIp {
		var origIp string
		for _, peerNode := range peerNodes {
			origIp = strings.Split(origNode, ":")[0]
			if origIp == peerNode {
				continue outer
			}
		}
		t.Error("Peer nodes does not have added node: ", origIp)
	}

	// test stop gossiper
	for i := range nodesIp {
		// It takes time to propagate the leave message
		err := gossipers[i].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodesIp)+1))
		if err != nil {
			t.Error("Error stopping gossiper : ", time.Now(), err)
		}
	}
}

func TestGossiperOnlyOneNodeGossips(t *testing.T) {
	printTestInfo()

	nodesIp := []string{
		"127.0.0.1:9222",
		"127.0.0.2:9223",
		"127.0.0.3:9224",
	}

	peers := getNodeUpdateMap(nodesIp)

	rand.Seed(time.Now().UnixNano())
	id := types.NodeId(strconv.Itoa(0))
	gZero, _ := NewGossiperImpl(nodesIp[0], id, []string{}, types.DEFAULT_GOSSIP_VERSION)
	var otherGossipers []*GossiperImpl
	// First Start the gossipers on all other nodes
	for j, peer := range nodesIp {
		if j == 0 {
			continue
		}
		g, _ := NewGossiperImpl(peer, types.NodeId(strconv.Itoa(j)), []string{nodesIp[0]}, types.DEFAULT_GOSSIP_VERSION)
		g.UpdateCluster(peers)
		otherGossipers = append(otherGossipers, g)
	}

	gZero.UpdateCluster(peers)
	otherGossipers[0].UpdateCluster(peers)
	otherGossipers[1].UpdateCluster(peers)

	// Let the nodes gossip and populate their memberlist
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodesIp)))

	// Now Kill the other nodes
	for _, og := range otherGossipers {
		err := og.Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodesIp)+1))
		if err != nil {
			t.Error("Error in stopping gossiper", err)
		}
	}

	key := types.StoreKey("somekey")
	value := "someValue"
	gZero.UpdateSelf(key, value)

	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodesIp)+1))

	// Sleep for gossip quorum timeout
	time.Sleep(gZero.quorumTimeout + 2*time.Second)

	res := gZero.GetStoreKeyValue(key)
	if len(res) != 3 {
		t.Error("Available nodes not reported ", res)
	}

	for nodeId, n := range res {
		if nodeId != n.Id {
			t.Error("Gossiper Id does not match ",
				nodeId, " n:", n.Id)
		}
		nid, ok := strconv.Atoi(string(nodeId))
		if ok != nil {
			t.Error("Failed to convert node to id ", nodeId, " n.Id", n.Id)
		}
		if nid == 0 {
			if n.Status != types.NODE_STATUS_NOT_IN_QUORUM {
				t.Error("Gossiper ", nid,
					"Expected node status to be: ", types.NODE_STATUS_NOT_IN_QUORUM,
					" but found: ", n.Status)
			}
		}
		if nid != 0 {
			if n.Status != types.NODE_STATUS_DOWN {
				t.Error("Gossiper ", nid,
					"Expected node status to be down: ", nodeId, " n:", n.Status)
			}
		}
	}
	gZero.Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodesIp)+1))
}

func TestGossiperOneNodeNeverGossips(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9622",
		"127.0.0.2:9623",
		"127.0.0.3:9624",
	}
	peers := getNodeUpdateMap(nodes)

	rand.Seed(time.Now().UnixNano())
	gossipers := make(map[int]*GossiperImpl)

	// Start gossipers for all nodes
	for i, nodeId := range nodes {
		id := types.NodeId(strconv.Itoa(i))
		var g *GossiperImpl
		if i == 0 {
			g, _ = NewGossiperImpl(nodeId, id, []string{}, types.DEFAULT_GOSSIP_VERSION)
		} else {
			g, _ = NewGossiperImpl(nodeId, id, []string{nodes[0]}, types.DEFAULT_GOSSIP_VERSION)
		}
		g.UpdateCluster(peers)
		gossipers[i] = g
	}

	key := types.StoreKey("somekey")
	value := "someValue"
	for i, g := range gossipers {
		g.UpdateSelf(key, value+strconv.Itoa(i))
	}

	// Let the nodes gossip and populate their memberlists
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	for i, g := range gossipers {
		res := g.GetStoreKeyValue(key)
		for nodeId, n := range res {
			if nodeId != n.Id {
				t.Error("Gossiper ", i, "Id does not match ",
					nodeId, " n:", n.Id)
			}
			nid, ok := strconv.Atoi(string(nodeId))
			if ok != nil {
				t.Error("Failed to convert node to id ", nodeId, " n.Id", n.Id)
			}
			if nid == 0 && i != 0 {
				if n.Status == types.NODE_STATUS_DOWN {
					t.Error("Gossiper ", i,
						"Expected node status not to be down: ", nodeId, " n:", n)
				}
			}
		}
	}

	// Bring down node 0
	gossipers[0].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))

	// Let the node down propagate in the cluster
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	for i, g := range gossipers {
		res := g.GetStoreKeyValue(key)
		for nodeId, n := range res {
			if nodeId != n.Id {
				t.Error("Gossiper ", i, "Id does not match ",
					nodeId, " n:", n.Id)
			}
			nid, ok := strconv.Atoi(string(nodeId))
			if ok != nil {
				t.Error("Failed to convert node to id ", nodeId, " n.Id", n.Id)
			}
			if nid == 0 {
				if n.Status != types.NODE_STATUS_DOWN {
					t.Error("Gossiper ", i,
						"Expected node status for ", nodeId, " to be down. Got: ", n.Status)
				}
			} else {
				if n.Status != types.NODE_STATUS_UP {
					t.Error("Gossiper ", i, "Expected node", nodeId, " to be up. Got: ", n.Status)
				}
			}
		}
	}

	for i, g := range gossipers {
		if i == 0 {
			continue
		}
		g.Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
	}
}

// For GossipVersion check tests we do not set the clusterSize.
// This bypasses the quorum handling as clusterSize is 0 and every node
// thus satisfies quorum.
func TestGossiperNodeVersionMismatch(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9721",
		"127.0.0.2:9722",
		"127.0.0.3:9723",
		"127.0.0.4:9724",
		"127.0.0.5:9725",
	}
	rand.Seed(time.Now().UnixNano())
	gossipers := make(map[int]*GossiperImpl)

	// Start gossipers for all nodes
	for i, nodeId := range nodes {
		id := types.NodeId(strconv.Itoa(i))
		var g *GossiperImpl
		if i == 0 {
			g, _ = NewGossiperImpl(nodeId, id, []string{}, types.DEFAULT_GOSSIP_VERSION)
		} else {
			if i == 2 || i == 4 {
				// Set a different gossip version
				g, _ = NewGossiperImpl(nodeId, id, []string{nodes[0]}, "v2")
			} else {
				g, _ = NewGossiperImpl(nodeId, id, []string{nodes[0]}, types.DEFAULT_GOSSIP_VERSION)
			}
		}

		gossipers[i] = g
	}

	key := types.StoreKey("somekey")
	value := "someValue"
	for i, g := range gossipers {
		g.UpdateSelf(key, value+strconv.Itoa(i))
	}

	// Let the nodes gossip and populate their memberlists
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	for i, g := range gossipers {
		res := g.GetStoreKeyValue(key)
		if i == 2 || i == 4 {
			// These nodes should not have gossiped
			if len(res) > 1 {
				t.Error("Gossip Version mismatch not entertained. Node ", i, " still has ", len(res), "peers")
			}
			continue
		}
		for nodeId, n := range res {
			if nodeId != n.Id {
				t.Error("Gossiper ", i, "Id does not match ",
					nodeId, " n:", n.Id)
			}
			nid, ok := strconv.Atoi(string(nodeId))
			if ok != nil {
				t.Error("Failed to convert node to id ", nodeId, " n.Id", n.Id)
			}
			if nid == 2 || nid == 4 {
				t.Error("Gossip Version mismatch not entertained. Node ", i, "still has an update for faulty node ", nid)
			}
		}
	}
	for _, g := range gossipers {
		g.Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
	}
}

func TestGossiperGroupingOfNodesWithSameVersion(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9821",
		"127.0.0.2:9822",
		"127.0.0.3:9823",
		"127.0.0.4:9824",
		"127.0.0.5:9825",
	}

	peers1 := make(map[types.NodeId]types.NodeUpdate)
	peers2 := make(map[types.NodeId]types.NodeUpdate)
	for i, ip := range nodes {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		if i != 0 && i%2 == 0 {
			peers2[nodeId] = types.NodeUpdate{ip, true}
		} else {
			peers1[nodeId] = types.NodeUpdate{ip, true}
		}
	}

	rand.Seed(time.Now().UnixNano())
	gossipers := make(map[int]*GossiperImpl)

	// Start gossipers for all nodes
	for i, nodeId := range nodes {
		id := types.NodeId(strconv.Itoa(i))
		var g *GossiperImpl
		if i == 0 {
			g, _ = NewGossiperImpl(nodeId, id, []string{}, types.DEFAULT_GOSSIP_VERSION)
			g.UpdateCluster(peers1)
		} else {
			// Nodes 2 and 4 have same version. They should form a memberlist
			if i == 2 {
				// Set a different gossip version
				g, _ = NewGossiperImpl(nodeId, id, []string{nodes[0]}, "v2")
				g.UpdateCluster(peers2)
			} else if i == 4 {
				// Set a different gossip version
				// This gossiper will try to connect to nodes 0 and 2. 0 should fail and 2 should succeed
				g, _ = NewGossiperImpl(nodeId, id, []string{nodes[0], nodes[2]}, "v2")
				g.UpdateCluster(peers2)
			} else {
				g, _ = NewGossiperImpl(nodeId, id, []string{nodes[0]}, types.DEFAULT_GOSSIP_VERSION)
				g.UpdateCluster(peers1)
			}
		}

		gossipers[i] = g
	}

	key := types.StoreKey("somekey")
	value := "someValue"
	for i, g := range gossipers {
		g.UpdateSelf(key, value+strconv.Itoa(i))
	}

	// Let the nodes gossip and populate their memberlists
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	for i, g := range gossipers {
		res := g.GetStoreKeyValue(key)
		if i == 2 {
			if len(res) < 2 {
				t.Error("Node 2 does not have any update from Node 4")
			}
			_, ok := res[types.NodeId("4")]
			if !ok {
				t.Error("Node 2 does not have update from Node 4 but has updates from other nodes.")
			}
		} else if i == 4 {
			if len(res) < 2 {
				t.Error("Node 2 does not have any update from Node 4")
			}
			_, ok := res[types.NodeId("2")]
			if !ok {
				t.Error("Node 2 does not have update from Node 4 but has updates from other nodes.")
			}
		} else {
			for nodeId, n := range res {
				if nodeId != n.Id {
					t.Error("Gossiper ", i, "Id does not match ",
						nodeId, " n:", n.Id)
				}
				nid, ok := strconv.Atoi(string(nodeId))
				if ok != nil {
					t.Error("Failed to convert node to id ", nodeId, " n.Id", n.Id)
				}
				if nid == 2 || nid == 4 {
					t.Error("Gossip Version mismatch not entertained. Node ", i, "still has an update for faulty node ", nid)
				}
			}

		}
	}
	for _, g := range gossipers {
		g.Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
	}
}

func TestGossiperUpdateNodeIp(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9325",
		"127.0.0.2:9326",
		"127.0.0.3:9327",
	}

	peers := getNodeUpdateMap(nodes)

	rand.Seed(time.Now().UnixNano())
	gossipers := make(map[int]*GossiperImpl)
	var g *GossiperImpl
	for i, nodeId := range nodes {
		id := types.NodeId(strconv.Itoa(i))
		if i == 0 {
			g, _ = NewGossiperImpl(nodeId, id, []string{}, types.DEFAULT_GOSSIP_VERSION)
		} else {
			g, _ = NewGossiperImpl(nodeId, id, []string{nodes[0]}, types.DEFAULT_GOSSIP_VERSION)
		}

		g.UpdateCluster(peers)
		gossipers[i] = g
	}

	key := types.StoreKey("somekey")
	value := "someValue"
	for i, g := range gossipers {
		g.UpdateSelf(key, value+strconv.Itoa(i))
	}

	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))
	// Bring down node 0 and bring it back up with changed IP
	gossipers[0].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))
	changedGossiper, _ := NewGossiperImpl("127.0.0.4:9328", types.NodeId("0"), []string{"127.0.0.3:9327"}, types.DEFAULT_GOSSIP_VERSION)
	changedGossiper.UpdateCluster(peers)
	gossipers[0] = changedGossiper
	// Change the IP in the nodes array
	nodes[0] = "127.0.0.4:9328"

	// Wait for changes to propagate
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
	for i, g := range gossipers {
		res := g.GetStoreKeyValue(key)
		for nodeId, n := range res {
			if nodeId != n.Id {
				t.Error("Gossiper ", i, "Id does not match ",
					nodeId, " n:", n.Id)
			}
			_, ok := strconv.Atoi(string(nodeId))
			if ok != nil {
				t.Error("Failed to convert node to id ", nodeId, " n.Id", n.Id)
			}
			// All nodes must be up
			if n.Status != types.NODE_STATUS_UP {
				t.Error("Gossiper ", i,
					"Expected node status to be up: ", nodeId, "Got :", n.Status)
			}

		}
		// Check the IPs
		peerNodes := g.GetNodes()
		if len(peerNodes) != len(nodes) {
			t.Error("Gossiper for node ", i, "does not have info",
				"about all nodes")
		}
		for _, node := range peerNodes {
			found := false
			for _, origNode := range nodes {
				origIp := strings.Split(origNode, ":")[0]
				if origIp == node {
					found = true
					break
				}
			}
			if found == false {
				t.Error("Gossiper for node ", i, " does not have",
					" updated Ip of other nodes")
			}

		}
	}
	for i := 0; i < len(nodes); i++ {
		gossipers[i].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
	}

}

func verifyGossiperEquality(g1 *GossiperImpl, g2 *GossiperImpl, t *testing.T) {
	// check for the equality
	g1Keys := g1.GetStoreKeys()

	for _, key := range g1Keys {
		g1Values := g1.GetStoreKeyValue(key)
		g2Values := g2.GetStoreKeyValue(key)

		if len(g1Values) != len(g2Values) {
			t.Fatal("Lens mismatch between g1 and g2 values")
		}

		for i := 0; i < len(g1Values); i++ {
			id := types.NodeId(strconv.Itoa(i))
			if g1Values[id].Id != g2Values[id].Id {
				t.Error("Values mismtach between g1 and g2, g1:\n",
					g1Values[id].Id, "\ng2:", g2Values[id].Id)
			}
		}
	}
}

// For this test we disable quorum handling by not setting the cluster size.
// By this we ensure that nodes going up and down is tested and their status
// is being propagated correctly to other nodes.

// TODO: Fix this test. There are timing issues. We have similar tests running
// which do exactly the same
/*func TestGossiperMultipleNodesGoingUpDown(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9152",
		"127.0.0.2:9153",
		"127.0.0.3:9154",
		"127.0.0.4:9155",
		"127.0.0.5:9156",
		"127.0.0.6:9157",
	}

	peers := make(map[types.NodeId]string)
	for i, ip := range nodes {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		peers[nodeId] = ip
	}

	rand.Seed(time.Now().UnixNano())
	gossipers := make(map[string]*GossiperImpl)
	for i, nodeId := range nodes {
		// Select one neighbour and one random peer
		// By selecting a neighbour node we are avoiding a potential
		// network partition
		var neighbourNode, randomNode string
		if i == 0 {
			neighbourNode = ""
		} else {
			neighbourNode = nodes[i-1]
		}

		for count := 0; count < 2; {
			randId := rand.Intn(len(nodes))
			if randId == i || nodes[randId] == neighbourNode {
				continue
			}
			randomNode = nodes[randId]
			count++
		}

		g, _ := NewGossiperImpl(nodeId, types.NodeId(strconv.Itoa(i)), []string{neighbourNode, randomNode}, types.DEFAULT_GOSSIP_VERSION)
		g.UpdateCluster(peers)
		gossipers[nodeId] = g
	}

	updateFunc := func(g *GossiperImpl, max int, t *testing.T) {
		for i := 0; i < max; i++ {
			g.UpdateSelf("sameKey", strconv.Itoa(i))
			g.UpdateSelf(types.StoreKey(g.NodeId()), strconv.Itoa(i*i))
			time.Sleep(g.GossipInterval() + time.Duration(rand.Intn(100)))
		}
	}

	for _, g := range gossipers {
		go updateFunc(g, len(nodes), t)
	}

	// Max duration for update is 1500 + 200 + 100 per update * 10
	// = 1800 mil * 10 = 18000 mil.
	// To add go fork thread, 2000 mil on top.
	// Let gossip go on for another 10 seconds, after which it must settle

	// 2 * 6 in worst case
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	// verify all of them are same
	for i := 1; i < len(nodes); i++ {
		verifyGossiperEquality(gossipers[nodes[0]], gossipers[nodes[i]], t)
	}

	// start another update round, however, we will shut down soem machines
	// in between
	for _, g := range gossipers {
		go updateFunc(g, len(nodes), t)
	}

	shutdownNodes := make(map[int]bool)
	for {
		randId := rand.Intn(len(nodes))
		if randId == 0 {
			continue
		}
		_, ok := shutdownNodes[randId]
		if ok == false {
			shutdownNodes[randId] = true
			gossipers[nodes[randId]].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
			if len(shutdownNodes) == 3 {
				break
			}
		}
	}

	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)*3))
	// verify all of them are same
	for i := 1; i < len(nodes); i++ {
		_, ok := shutdownNodes[i]
		if ok {
			continue
		}
		verifyGossiperEquality(gossipers[nodes[0]], gossipers[nodes[i]], t)

		g := gossipers[nodes[i]]
		keys := g.GetStoreKeys()
		for _, key := range keys {
			values := g.GetStoreKeyValue(key)

			for j, nodeInfo := range values {
				nodeId, _ := strconv.Atoi(string(j))
				_, ok := shutdownNodes[nodeId]
				if ok && nodeInfo.Status == types.NODE_STATUS_UP {
					t.Error("Node not marked down: ", nodeInfo, " for node: ", nodes[i])
				}
			}
		}
	}
}*/

func TestGossiperAddNodeExternally(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9158",
		"127.0.0.2:9159",
	}

	peers := getNodeUpdateMap(nodes)

	rand.Seed(time.Now().UnixNano())
	gossipers := make(map[int]*GossiperImpl)
	var g *GossiperImpl
	for i, nodeId := range nodes {
		id := types.NodeId(strconv.Itoa(i))
		g, _ = NewGossiperImpl(nodeId, id, []string{nodes[0]}, types.DEFAULT_GOSSIP_VERSION)
		g.UpdateCluster(peers)
		gossipers[i] = g
	}

	nodes = append(nodes, "127.0.0.3:9160")
	peers[types.NodeId("2")] = types.NodeUpdate{nodes[2], true}

	for _, g := range gossipers {
		g.UpdateCluster(peers)
	}

	key := types.StoreKey("somekey")
	value := "someValue"
	for i, g := range gossipers {
		g.UpdateSelf(key, value+strconv.Itoa(i))
	}

	// Let the nodes gossip and populate their memberlists
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	// The new node should be added
	for i, g := range gossipers {
		res := g.GetStoreKeyValue(key)
		for nodeId, n := range res {
			nid, ok := strconv.Atoi(string(nodeId))
			if ok != nil {
				t.Error("Failed to convert node to id ", nodeId, " n.Id", n.Id)
			}
			if nid == 2 {
				if n.Status != types.NODE_STATUS_DOWN {
					t.Error("Gossiper ", i,
						"Expected node status to be down: ", nodeId, " n:", n.Status)
				}
			} else {
				if n.Status != types.NODE_STATUS_UP {
					t.Error("Gossiper ", i, "Expected node to be up: ", nodeId,
						" n:", n)
				}
			}
		}
	}

	// Start node 2
	g2, _ := NewGossiperImpl(nodes[2], types.NodeId("2"), []string{nodes[0]}, types.DEFAULT_GOSSIP_VERSION)
	g2.UpdateCluster(peers)

	// Let the nodes gossip and populate their memberlists
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	// The new node should be added
	for i, g := range gossipers {
		res := g.GetStoreKeyValue(key)
		for nodeId, n := range res {
			if n.Status != types.NODE_STATUS_UP {
				t.Error("Gossiper ", i, "Expected node to be up: ", nodeId,
					" n:", n)
			}
		}
	}
}

func TestGossiperRemoveNodeExternally(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9161",
		"127.0.0.2:9162",
		"127.0.0.3:9163",
	}

	peers := getNodeUpdateMap(nodes)

	rand.Seed(time.Now().UnixNano())
	gossipers := make(map[int]*GossiperImpl)
	var g *GossiperImpl
	for i, nodeId := range nodes {
		id := types.NodeId(strconv.Itoa(i))
		g, _ = NewGossiperImpl(nodeId, id, []string{nodes[0]}, types.DEFAULT_GOSSIP_VERSION)
		g.UpdateCluster(peers)
		gossipers[i] = g
	}

	key := types.StoreKey("somekey")
	value := "someValue"
	for i, g := range gossipers {
		g.UpdateSelf(key, value+strconv.Itoa(i))
	}

	// Let the nodes gossip and populate their memberlists
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	// Lets remove one of the nodes
	delete(peers, types.NodeId("0"))
	for i, g := range gossipers {
		if i == 0 {
			continue
		}
		g.UpdateCluster(peers)
	}

	for i, g := range gossipers {
		res := g.GetStoreKeyValue(key)
		if i != 0 {
			if len(res) != 2 {
				t.Error("Gossiper ", i, " still has an update about (%v) nodes instead of 2")
			}
		} else {
			// For node 0 we do not want any checks
			continue
		}

		for nodeId, n := range res {
			if nodeId != n.Id {
				t.Error("Gossiper ", i, "Id does not match ",
					nodeId, " n:", n.Id)
			}
			nid, ok := strconv.Atoi(string(nodeId))
			if ok != nil {
				t.Error("Failed to convert node to id ", nodeId, " n.Id", n.Id)
			}
			if nid == 0 {
				t.Error("Gossiper ", i,
					"Expected no update from node 0")
			}
		}
	}
}

func TestGossiperExternalNodeLeaveSelfKill(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9164",
		"127.0.0.2:9165",
		"127.0.0.3:9166",
	}

	peers := getNodeUpdateMap(nodes)

	rand.Seed(time.Now().UnixNano())
	gossipers := make(map[int]*GossiperImpl)
	var g *GossiperImpl
	for i, nodeId := range nodes {
		id := types.NodeId(strconv.Itoa(i))
		if i == 1 {
			// Isolate node 1
			g, _ = NewGossiperImpl(nodeId, id, []string{}, types.DEFAULT_GOSSIP_VERSION)
		} else {
			g, _ = NewGossiperImpl(nodeId, id, []string{nodes[0]}, types.DEFAULT_GOSSIP_VERSION)
		}
		g.UpdateCluster(peers)
		gossipers[i] = g
	}

	// Let the nodes gossip and populate their memberlists
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	// Call ExternalNodeLeave on node1. It should kill itself.
	killedNode := gossipers[1].ExternalNodeLeave(types.NodeId("0"))
	if killedNode != types.NodeId("1") {
		t.Error("ExternalNodeLeave should have killed Node 1 but killed Node ", killedNode)
	}

	for i := 0; i < len(nodes); i++ {
		gossipers[i].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
	}
}

func TestGossiperExternalNodeLeavePeerKill(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9167",
		"127.0.0.2:9168",
		"127.0.0.3:9169",
	}

	peers := getNodeUpdateMap(nodes)

	rand.Seed(time.Now().UnixNano())
	gossipers := make(map[int]*GossiperImpl)
	var g *GossiperImpl
	for i, nodeId := range nodes {
		id := types.NodeId(strconv.Itoa(i))
		g, _ = NewGossiperImpl(nodeId, id, []string{nodes[0]}, types.DEFAULT_GOSSIP_VERSION)
		g.UpdateCluster(peers)
		gossipers[i] = g
	}

	// Let the nodes gossip and populate their memberlists
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	// Call ExternalNodeLeave on node1. It should kill the peer node.
	killedNode := gossipers[1].ExternalNodeLeave(types.NodeId("0"))
	if killedNode != types.NodeId("0") {
		t.Error("ExternalNodeLeave should have killed Node 1 but killed Node ", killedNode)
	}

	for i := 0; i < len(nodes); i++ {
		gossipers[i].Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
	}
}

func TestGossiperNodesWithDifferentClusterId(t *testing.T) {
	printTestInfo()

	nodes := []string{
		"127.0.0.1:9170",
		"127.0.0.2:9171",
		"127.0.0.3:9172",
		"127.0.0.4:9173",
		"127.0.0.5:9174",
	}

	rand.Seed(time.Now().UnixNano())
	gossipers := make(map[int]*GossiperImpl)

	peers1 := make(map[types.NodeId]types.NodeUpdate)
	peers2 := make(map[types.NodeId]types.NodeUpdate)
	for i, ip := range nodes {
		nodeId := types.NodeId(strconv.FormatInt(int64(i), 10))
		if i == 2 || i == 4 {
			peers2[nodeId] = types.NodeUpdate{ip, true}
		} else {
			peers1[nodeId] = types.NodeUpdate{ip, true}
		}
	}

	// Start gossipers for all nodes
	for i, nodeId := range nodes {
		id := types.NodeId(strconv.Itoa(i))
		var g *GossiperImpl
		if i == 2 || i == 4 {
			// Set a different clusterId
			g, _ = NewGossiperImplWithClusterId(nodeId, id, nodes,
				types.DEFAULT_GOSSIP_VERSION, "test-cluster-1")
			g.UpdateCluster(peers2)
		} else {
			g, _ = NewGossiperImplWithClusterId(nodeId, id, nodes,
				types.DEFAULT_GOSSIP_VERSION, "test-cluster-2")
			g.UpdateCluster(peers1)
		}

		gossipers[i] = g
	}

	key := types.StoreKey("somekey")
	value := "someValue"
	for i, g := range gossipers {
		g.UpdateSelf(key, value+strconv.Itoa(i))
	}

	// Let the nodes gossip and populate their memberlists
	time.Sleep(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)))

	for i, g := range gossipers {
		res := g.GetStoreKeyValue(key)
		if i == 2 || i == 4 {
			// These nodes should not have gossiped
			if len(res) != 2 {
				t.Error("Gossip ClusterId mismatch not entertained. Node ", i, " still has ", len(res), "peers")
			}
			continue
		}
		for nodeId, n := range res {
			if nodeId != n.Id {
				t.Error("Gossiper ", i, "Id does not match ",
					nodeId, " n:", n.Id)
			}
			nid, ok := strconv.Atoi(string(nodeId))
			if ok != nil {
				t.Error("Failed to convert node to id ", nodeId, " n.Id", n.Id)
			}
			if nid == 2 || nid == 4 {
				t.Error("Gossip ClusterId mismatch not entertained. Node ", i, "still has an update for faulty node ", nid)
			}
		}
	}
	time.Sleep(30 * time.Second)
	for _, g := range gossipers {
		g.Stop(types.DEFAULT_GOSSIP_INTERVAL * time.Duration(len(nodes)+1))
	}

}
