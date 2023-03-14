package node

import (
	"fmt"
	"sync"

	"github.com/pborman/uuid"
)

var (
	nodeRegistry = make(map[string]Node)
	lock         sync.RWMutex
)

// AddNode adds a node to the node collection
func AddNode(n Node) error {
	if n.uuid != "" {
		return fmt.Errorf("UUID should not be set to add new node")
	}
	lock.Lock()
	defer lock.Unlock()
	n.uuid = uuid.New()
	nodeRegistry[n.uuid] = n
	return nil
}

// UpdateNode updates a given node if it exists in the node collection
func UpdateNode(n Node) error {
	lock.Lock()
	defer lock.Unlock()
	if _, ok := nodeRegistry[n.uuid]; !ok {
		return fmt.Errorf("node to be updated does not exist")
	}
	nodeRegistry[n.uuid] = n
	return nil
}

// DeleteNode method delete a given node if exist in the node collection
func DeleteNode(n Node) error {
	if n.uuid == "" {
		return fmt.Errorf("UUID should be set to delete existing node")
	}
	lock.Lock()
	defer lock.Unlock()
	delete(nodeRegistry, n.uuid)
	return nil
}

// GetNodes returns all the nodes from the node collection
func GetNodes() []Node {
	var nodeList []Node
	for _, n := range nodeRegistry {
		nodeList = append(nodeList, n)
	}
	return nodeList
}

// GetWorkerNodes returns only the worker nodes/agent nodes
func GetWorkerNodes() []Node {
	var nodeList []Node
	for _, n := range nodeRegistry {
		if n.Type == TypeWorker {
			nodeList = append(nodeList, n)
		}
	}
	return nodeList
}

// GetMasterNodes returns only the master nodes/agent nodes
func GetMasterNodes() []Node {
	var nodeList []Node
	for _, n := range nodeRegistry {
		if n.Type == TypeMaster {
			nodeList = append(nodeList, n)
		}
	}
	return nodeList
}

// GetStorageDriverNodes returns only the worker node where storage
// driver is installed
func GetStorageDriverNodes() []Node {
	var nodeList []Node
	for _, n := range nodeRegistry {
		if n.Type == TypeWorker && n.IsStorageDriverInstalled {
			nodeList = append(nodeList, n)
		}
	}
	return nodeList
}

// IsStorageNode returns true if the node is a storage node, false otherwise
func IsStorageNode(n Node) bool {
	return len(n.StoragePools) > 0
}

// GetStorageNodes gets all the nodes with non-empty StoragePools
func GetStorageNodes() []Node {
	var nodeList []Node
	for _, n := range nodeRegistry {
		if IsStorageNode(n) {
			nodeList = append(nodeList, n)
		}
	}
	return nodeList
}

// GetStorageLessNodes gets all the nodes with empty StoragePools
func GetStorageLessNodes() []Node {
	var nodeList []Node
	workerNodes := GetWorkerNodes()
	storageNodes := GetStorageNodes()
	for _, n := range workerNodes {
		isExist := Contains(storageNodes, n)
		if !isExist {
			nodeList = append(nodeList, n)
		}
	}
	return nodeList
}

// GetNodesByTopologyZoneLabel gets all the nodes with Topology Zone Value matching
func GetNodesByTopologyZoneLabel(zone string) []Node {
	var nodeList []Node
	for _, n := range nodeRegistry {
		if n.TopologyZone == zone {
			nodeList = append(nodeList, n)
		}
	}
	return nodeList
}

// GetNodesByTopologyRegionLabel gets all the nodes with Topology Region Value matching
func GetNodesByTopologyRegionLabel(region string) []Node {
	var nodeList []Node
	for _, n := range nodeRegistry {
		if n.TopologyRegion == region {
			nodeList = append(nodeList, n)
		}
	}
	return nodeList
}

// GetMetadataNodes gets all the nodes which serves as internal kvdb metadata node
func GetMetadataNodes() []Node {
	var nodeList []Node
	for _, n := range nodeRegistry {
		if n.IsMetadataNode {
			nodeList = append(nodeList, n)
		}
	}
	return nodeList
}

// GetNodesByName returns map of nodes where the node name is the key
func GetNodesByName() map[string]Node {
	nodeMap := make(map[string]Node)
	for _, n := range nodeRegistry {
		nodeMap[n.Name] = n
	}
	return nodeMap
}

// GetNodesByVoDriverNodeID returns map of nodes where volume driver node id is the key
func GetNodesByVoDriverNodeID() map[string]Node {
	nodeMap := make(map[string]Node)
	for _, n := range nodeRegistry {
		nodeMap[n.VolDriverNodeID] = n
	}
	return nodeMap
}

// Contains checks if the node is present in the given list of nodes
func Contains(nodes []Node, n Node) bool {
	for _, value := range nodes {
		if value.Name == n.Name {
			return true
		}
	}
	return false
}

// GetNodeByName returns a node which matches with given name
func GetNodeByName(nodeName string) (Node, error) {
	for _, n := range nodeRegistry {
		if n.Name == nodeName {
			return n, nil
		}
	}
	return Node{}, fmt.Errorf("failed: Node [%s] not found in node registry", nodeName)
}

// GetNodeByIP return a node which matches with given IP
func GetNodeByIP(nodeIP string) (Node, error) {
	for _, n := range nodeRegistry {
		for _, addr := range n.Addresses {
			if addr == nodeIP {
				return n, nil
			}
		}
	}
	return Node{}, fmt.Errorf("failed: Node with [%s] not found in node registry", nodeIP)
}

// CleanupRegistry removes entry of all nodes from registry
func CleanupRegistry() {
	nodeRegistry = make(map[string]Node)
}

// GetNodeDetailsByNodeName get node details for a given node name
func GetNodeDetailsByNodeName(nodeName string) (Node, error) {
	storageNodes := GetStorageNodes()

	for _, each := range storageNodes {
		if each.Name == nodeName {
			return each, nil
		}
	}
	return Node{}, fmt.Errorf("failed to get Node Details by Node Name [%s] ", nodeName)
}
