package proto

import (
	"fmt"
	"github.com/libopenstorage/gossip/types"
	"math/rand"
	"runtime"
	"strconv"
	"testing"
	"time"
)

const (
	CPU    types.StoreKey = "CPU"
	MEMORY types.StoreKey = "MEMORY"
	ID     types.NodeId   = "4"
)

func printTestInfo() {
	pc := make([]uintptr, 3) // at least 1 entry needed
	runtime.Callers(2, pc)
	f := runtime.FuncForPC(pc[0])
	fmt.Println("RUNNING TEST: ", f.Name())
}

func flipCoin() bool {
	if rand.Intn(100) < 50 {
		return true
	}
	return false
}

func fillUpNodeInfo(node *types.NodeInfo, key types.StoreKey, i int) {
	node.Id = types.NodeId(strconv.Itoa(i))
	node.LastUpdateTs = time.Now()
	node.Status = types.NODE_STATUS_UP

	node.Value = make(types.StoreMap)
	node.Value[types.StoreKey(CPU+key)] = node.Id
	node.Value[types.StoreKey(MEMORY+key)] = node.Id
}

func clearKey(nodes types.NodeInfoMap, key types.StoreKey, id int) {
	nodeId := types.NodeId(strconv.Itoa(id))
	nodeInfo := nodes[nodeId]
	delete(nodeInfo.Value, types.StoreKey(CPU+key))
	delete(nodeInfo.Value, types.StoreKey(MEMORY+key))
}

func fillUpNodeInfoMap(nodes types.NodeInfoMap, key types.StoreKey,
	numOfNodes int) {
	for i := 0; i < numOfNodes; i++ {
		var node types.NodeInfo
		fillUpNodeInfo(&node, key, i)
		nodes[node.Id] = node
	}
}

func TestGossipStoreUpdateSelf(t *testing.T) {
	printTestInfo()
	// emtpy store
	g := NewGossipStore(ID, types.DEFAULT_GOSSIP_VERSION, DEFAULT_CLUSTER_ID)

	id := g.NodeId()
	if id != ID {
		t.Error("Incorrect NodeId(), got: ", id,
			" expected: ", ID)
	}

	value := "string"
	key1 := types.StoreKey("key1")
	// key absent, id absent
	g.UpdateSelf(key1, value)

	nodeInfo, ok := g.nodeMap[ID]
	if !ok || nodeInfo.Value == nil {
		t.Error("UpdateSelf adding new id failed")
	} else {
		nodeValue, ok := nodeInfo.Value[key1]
		if !ok {
			t.Error("UpdateSelf adding new key failed, after update state: ",
				g.nodeMap)
		} else {
			if nodeValue != value || nodeInfo.Id != ID {
				t.Error("UpdateSelf failed, got value: ", nodeInfo.Value,
					" got: ", value)
			}
		}
	}

	// key present id present
	prevTs := time.Now()
	time.Sleep(1 * time.Second)
	value = "newValue"
	g.UpdateSelf(key1, value)
	nodeInfo = g.nodeMap[ID]
	nodeValue := nodeInfo.Value[key1]
	if !prevTs.Before(nodeInfo.LastUpdateTs) {
		t.Error("UpdateSelf failed to update timestamp, prev: ", prevTs,
			" got: ", nodeInfo)
	}
	if nodeValue != value || nodeInfo.Id != ID {
		t.Error("UpdateSelf failed, got value: ", nodeInfo,
			" got: ", value, " expected id: ", ID)
	}
}

func TestGossipStoreGetStoreKeyValue(t *testing.T) {
	printTestInfo()

	// Case: emtpy store
	// Case: key absent
	g := NewGossipStore(ID, types.DEFAULT_GOSSIP_VERSION, DEFAULT_CLUSTER_ID)

	keyList := []types.StoreKey{"key1", "key2"}

	nodeInfoMap := g.GetStoreKeyValue(keyList[0])
	if len(nodeInfoMap) != 1 {
		t.Error("Expected self node info list, got: ", nodeInfoMap)
	}

	// Case: key present with nodes with holes in node ids
	fillUpNodeInfoMap(g.nodeMap, keyList[0], 6)
	if len(g.nodeMap) != 6 {
		t.Error("Failed to fillup node info map properly, got: ",
			g.nodeMap)
	}
	keyCheck := types.StoreKey(CPU + keyList[0])
	delete(g.nodeMap["0"].Value, keyCheck)
	delete(g.nodeMap["2"].Value, keyCheck)
	delete(g.nodeMap["4"].Value, keyCheck)
	nodeInfoMap = g.GetStoreKeyValue(keyCheck)
	if len(nodeInfoMap) != 3 {
		t.Error("Expected list with atleast 6 elements, got: ", nodeInfoMap)
	}

	for i := 0; i < len(nodeInfoMap); i++ {
		id := types.NodeId(strconv.Itoa(i))
		if i%2 == 0 {
			if _, ok := nodeInfoMap[id]; ok {
				t.Error("No node expected, got: ", nodeInfoMap[id])
			}
			continue
		}
		infoMap := nodeInfoMap[id].Value.(types.NodeId)
		if nodeInfoMap[id].Id != id ||
			nodeInfoMap[id].Status != types.NODE_STATUS_UP ||
			infoMap != id {
			t.Error("Invalid node content received, got: ", nodeInfoMap[id])
		}
	}
}

func TestGossipStoreMetaInfo(t *testing.T) {
	printTestInfo()

	g := NewGossipStore(ID, types.DEFAULT_GOSSIP_VERSION, DEFAULT_CLUSTER_ID)

	m := g.MetaInfo()
	if m.Id != ID {
		t.Error("Node meta has invalid ID. Got:", m.Id, " Expected: ", ID)
	}
	if m.GossipVersion != types.DEFAULT_GOSSIP_VERSION {
		t.Error("Gossip Version mismatch. Got: ", m.GossipVersion, " Expected: ", types.DEFAULT_GOSSIP_VERSION)
	}
}

func compareNodeInfo(n1 types.NodeInfo, n2 types.NodeInfo) bool {
	eq := n1.Id == n2.Id && n2.LastUpdateTs == n1.LastUpdateTs // &&
	//n1.Status == n2.Status
	eq = eq && (n1.Value == nil && n2.Value == nil ||
		n1.Value != nil && n2.Value != nil)
	if eq && n1.Value != nil {
		eq = (len(n1.Value) == len(n2.Value))
		if !eq {
			return false
		}
		for key, value := range n1.Value {
			value2, ok := n2.Value[key]
			if !ok {
				eq = false
			}
			if value != value2 {
				eq = false
			}
		}
	}
	return eq
}

func dumpNodeInfo(nodeInfoMap types.NodeInfoMap, s string, t *testing.T) {
	t.Log("\nDUMPING : ", s, " : LEN: ", len(nodeInfoMap))
	for _, nodeInfo := range nodeInfoMap {
		t.Log(nodeInfo)
	}
}

func verifyNodeInfoMapEquality(store types.NodeInfoMap, diff types.NodeInfoMap,
	t *testing.T) {
	if len(store) != len(diff) {
		t.Error("Stores do not match got: ",
			store, " expected: ", diff)
	}

	for id, info := range store {
		if id == ID {
			continue
		}
		dInfo, ok := diff[id]
		if !ok {
			t.Error("Diff does not have id ", id)
			continue
		}

		if !compareNodeInfo(dInfo, info) {
			t.Error("Nodes do not match, o: ", info, " d:", dInfo)
		}
	}
}

func TestGossipStoreUpdateData(t *testing.T) {
	printTestInfo()

	g := NewGossipStore(ID, types.DEFAULT_GOSSIP_VERSION, DEFAULT_CLUSTER_ID)
	time.Sleep(1 * time.Second)
	// empty store and empty diff
	diff := types.NodeInfoMap{}
	g.Update(diff)
	if len(g.nodeMap) != 1 {
		t.Error("Updating empty store with empty diff gave non-empty store: ",
			g.nodeMap)
	}

	// empty store and non-emtpy diff
	diff = make(types.NodeInfoMap)
	nodeLen := 5
	for i := 0; i < nodeLen; i++ {
		g.AddNode(types.NodeId(strconv.Itoa(i)), types.NODE_STATUS_UP, true)
	}
	keyList := []types.StoreKey{"key1", "key2", "key3", "key4", "key5"}
	for _, key := range keyList {
		fillUpNodeInfoMap(types.NodeInfoMap(diff), key, nodeLen)
	}
	g.Update(diff)

	verifyNodeInfoMapEquality(types.NodeInfoMap(g.nodeMap), diff, t)

	for nodeId, nodeInfo := range g.nodeMap {
		// id % 4 == 0 : node id is not existing
		// id % 4 == 1 : store has old timestamp
		// id % 4 == 2 : node id is invalid
		// id % 4 == 3 : store has newer data
		id, _ := strconv.Atoi(string(nodeId))
		switch {
		case id%4 == 0:
			delete(g.nodeMap, nodeId)
		case id%4 == 1:
			olderTime := nodeInfo.LastUpdateTs.UnixNano() - 1000
			nodeInfo.LastUpdateTs = time.Unix(0, olderTime)
		case id%4 == 2:
			if id > 10 {
				nodeInfo.Status = types.NODE_STATUS_INVALID
			} else {
				nodeInfo.Status = types.NODE_STATUS_NEVER_GOSSIPED
			}
		case id%4 == 3:
			n, _ := diff[nodeId]
			olderTime := nodeInfo.LastUpdateTs.UnixNano() - 1000
			n.LastUpdateTs = time.Unix(0, olderTime)
			diff[nodeId] = n
		}
	}

	g.Update(diff)
	for nodeId, nodeInfo := range g.nodeMap {
		// id % 4 == 0 : node id is not existing
		// id % 4 == 1 : store has old timestamp
		// id % 4 == 2 : node id is invalid
		// id % 4 == 3 : store has newer data
		id, _ := strconv.Atoi(string(nodeId))
		switch {
		case id%4 != 3:
			n, _ := diff[nodeId]
			if !compareNodeInfo(n, nodeInfo) {
				t.Error("Update failed, d: ", n, " o:", nodeInfo)
			}
		case id%4 == 3:
			n, _ := diff[nodeId]
			if compareNodeInfo(n, nodeInfo) {
				t.Error("Wrongly Updated latest data d: ", n, " o: ", nodeInfo)
			}
			olderTime := n.LastUpdateTs.UnixNano() + 1000
			ts := time.Unix(0, olderTime).UnixNano()
			if ts != nodeInfo.LastUpdateTs.UnixNano() {
				t.Error("Wrongly Updated latest data d: ", n, " o: ", nodeInfo)
			}
		}
	}
}

func TestGossipStoreGetStoreKeys(t *testing.T) {
	printTestInfo()

	g := NewGossipStore(ID, types.DEFAULT_GOSSIP_VERSION, DEFAULT_CLUSTER_ID)

	keys := g.GetStoreKeys()
	if len(keys) != 0 {
		t.Error("Emtpy store returned keys: ", keys)
	}

	nodeLen := 10
	keyList := []types.StoreKey{"key5"}
	g.nodeMap = make(types.NodeInfoMap)
	for _, key := range keyList {
		fillUpNodeInfoMap(g.nodeMap, key, nodeLen)
	}

	keys = g.GetStoreKeys()
	if len(keys) != 2*len(keyList) {
		t.Error("Storekeys length mismatch, got", len(keys),
			", expected: ", 2*len(keyList))
	}

	for _, key := range keyList {
		found := 0
		for _, retkey := range keys {
			if retkey == (CPU+key) || retkey == (MEMORY+key) {
				found++
			}
		}
		if found != 2 {
			t.Error("Key not found: ", key, " keys:", keyList)
		}
	}
}

func TestGossipStoreBlackBoxTests(t *testing.T) {
	printTestInfo()

	g1 := NewGossipStore(ID, types.DEFAULT_GOSSIP_VERSION, DEFAULT_CLUSTER_ID)
	g2 := NewGossipStore(ID, types.DEFAULT_GOSSIP_VERSION, DEFAULT_CLUSTER_ID)

	nodeLen := 3
	keyList := []types.StoreKey{"key1", "key2", "key3", "key5"}
	g1.nodeMap = make(types.NodeInfoMap)
	g2.nodeMap = make(types.NodeInfoMap)
	for i, key := range keyList {
		if i%2 == 0 {
			fillUpNodeInfoMap(g1.nodeMap, key, nodeLen)
		} else {
			fillUpNodeInfoMap(g2.nodeMap, key, nodeLen)
		}
	}

	g1.Update(g2.GetLocalState())
	g2.Update(g1.GetLocalState())

	if len(g1.nodeMap) != len(g2.nodeMap) &&
		len(g1.nodeMap) != len(keyList) {
		t.Error("States mismatch:g1\n", g1, "\ng2\n", g2)
	}

	store := g1.nodeMap
	diff := g2.nodeMap

	for id, nodeInfo := range store {
		diffNode, ok := diff[id]
		if !ok {
			t.Error("Expected node absent in diff ", id)
			continue
		}

		if nodeInfo.Value == nil || diffNode.Value == nil {
			t.Error("NodeValues are unexpectedly nil !")
			continue
		}

		if len(nodeInfo.Value) != len(diffNode.Value) {
			t.Error("Node values are different s:", nodeInfo.Value, " d:",
				diffNode.Value)
			continue
		}

		for key, value := range nodeInfo.Value {
			diffValue, _ := diffNode.Value[key]
			if diffValue != value {
				t.Error("Values mismatch for key ", key, " s:", value,
					" d:", diffValue)
			}
		}
	}
}
