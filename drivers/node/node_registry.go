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
		return fmt.Errorf("Node to be updated does not exist")
	}
	nodeRegistry[n.uuid] = n
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

// GetNodesByName returns map of nodes where the node name is the key
func GetNodesByName() map[string]Node {
	nodeMap := make(map[string]Node)
	for _, n := range nodeRegistry {
		nodeMap[n.Name] = n
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
