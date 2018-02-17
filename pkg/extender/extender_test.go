// +build unittest

package extender

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/drivers/volume/mock"
	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api"
)

const (
	mockDriverName   = "MockDriver"
	defaultNamespace = "testNamespace"
)

var driver *mock.Driver
var extender *Extender

func setup(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	storkdriver, err := volume.Get(mockDriverName)
	if err != nil {
		t.Fatalf("Error getting mock volume driver: %v", err)
	}
	var ok bool
	driver, ok = storkdriver.(*mock.Driver)
	if !ok {
		t.Fatalf("Error casting mockdriver")
	}
	if err = storkdriver.Init(nil); err != nil {
		t.Fatalf("Error initializing mock volume driver: %v", err)
	}

	extender = &Extender{
		Driver: storkdriver,
	}

	if err = extender.Start(); err != nil {
		t.Fatalf("Error starting scheduler extender: %v", err)
	}
}

func teardown(t *testing.T) {
	if err := extender.Stop(); err != nil {
		t.Fatalf("Error stopping scheduler extender: %v", err)
	}
}

func newPod(podName string, volumes []string) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName},
	}
	for _, volume := range volumes {
		pvc := driver.NewPVC(volume)
		podVolume := v1.Volume{}
		podVolume.PersistentVolumeClaim = &v1.PersistentVolumeClaimVolumeSource{
			ClaimName: pvc.Name,
		}
		pod.Spec.Volumes = append(pod.Spec.Volumes, podVolume)
	}
	return pod
}

func newNode(hostname string, ip string, rack string, zone string, region string) *v1.Node {
	node := v1.Node{}

	node.Name = hostname
	node.Labels = make(map[string]string)
	node.Labels[mock.RackLabel] = rack
	node.Labels[mock.ZoneLabel] = zone
	node.Labels[mock.RegionLabel] = region

	hostNameAddress := v1.NodeAddress{
		Type:    v1.NodeHostName,
		Address: hostname,
	}
	node.Status.Addresses = append(node.Status.Addresses, hostNameAddress)
	IPAddress := v1.NodeAddress{
		Type:    v1.NodeInternalIP,
		Address: ip,
	}
	node.Status.Addresses = append(node.Status.Addresses, IPAddress)
	return &node
}

func sendRequest(
	path string,
	pod *v1.Pod,
	nodeList *v1.NodeList,
) (*http.Response, error) {
	args := &schedulerapi.ExtenderArgs{
		Pod:   *pod,
		Nodes: nodeList,
	}

	b, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post("http://localhost:8099/"+path,
		"application/json",
		strings.NewReader(string(b)))
	if err != nil {
		return nil, err
	}
	logrus.Infof("Response: %v err: %v", resp, err)

	return resp, nil
}

func sendFilterRequest(
	pod *v1.Pod,
	nodeList *v1.NodeList,
) (*schedulerapi.ExtenderFilterResult, error) {
	resp, err := sendRequest("filter", pod, nodeList)
	if err != nil {
		return nil, err
	}

	decoder := json.NewDecoder(resp.Body)
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logrus.Warnf("Error closing decoder: %v", err)
		}
	}()

	var filterResult schedulerapi.ExtenderFilterResult
	if err := decoder.Decode(&filterResult); err != nil {
		logrus.Errorf("Error decoding filter response: %v", err)
		return nil, err
	}
	return &filterResult, nil
}

func sendPrioritizeRequest(
	pod *v1.Pod,
	nodeList *v1.NodeList,
) (*schedulerapi.HostPriorityList, error) {
	resp, err := sendRequest("prioritize", pod, nodeList)
	if err != nil {
		return nil, err
	}

	decoder := json.NewDecoder(resp.Body)
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logrus.Warnf("Error closing decoder: %v", err)
		}
	}()

	var priorityList schedulerapi.HostPriorityList
	if err := decoder.Decode(&priorityList); err != nil {
		logrus.Errorf("Error decoding filter response: %v", err)
		return nil, err
	}
	return &priorityList, nil
}

func verifyFilterResponse(
	t *testing.T,
	requestNodes *v1.NodeList,
	expectedNodesIndexes []int,
	response *schedulerapi.ExtenderFilterResult,
) {
	match := true
	if len(response.Nodes.Items) != len(expectedNodesIndexes) {
		match = false
		goto done
	}

	for i, j := range expectedNodesIndexes {
		responseNode := response.Nodes.Items[i]
		requestNode := requestNodes.Items[j]
		for add := range responseNode.Status.Addresses {
			if responseNode.Status.Addresses[add].Type != requestNode.Status.Addresses[add].Type ||
				responseNode.Status.Addresses[add].Address != requestNode.Status.Addresses[add].Address {
				match = false
				goto done
			}
		}
	}

done:
	if !match {
		t.Fatalf("Response does not match expected nodes.\n"+
			"RequestNodes: %v\n Response: %v\n ExpectedNodesIndexes: %v\n",
			requestNodes, response, expectedNodesIndexes)
	}
}

func verifyPrioritizeResponse(
	t *testing.T,
	requestNodes *v1.NodeList,
	expectedScores []int,
	response *schedulerapi.HostPriorityList,
) {
	match := true
	if len(requestNodes.Items) != len(*response) {
		match = false
		goto done
	}

	for i, node := range requestNodes.Items {
		for _, address := range node.Status.Addresses {
			if address.Type == v1.NodeHostName {
				for _, respNode := range *response {
					if address.Address == respNode.Host {
						if expectedScores[i] != respNode.Score {
							match = false
							goto done
						}
					}
				}
			}
		}
	}

done:
	if !match {
		t.Fatalf("Response does not match expected Priorities.\n"+
			"RequestNodes: %v\n ExpectedScores: %v\n Response: %v\n",
			requestNodes, expectedScores, response)
	}
}

func TestExtender(t *testing.T) {
	t.Run("setup", setup)
	t.Run("noPVCTest", noPVCTest)
	t.Run("noDriverVolumeTest", noDriverVolumeTest)
	t.Run("noDriverNodeTest", noDriverNodeTest)
	t.Run("singleVolumeTest", singleVolumeTest)
	t.Run("multipleVolumeTest", multipleVolumeTest)
	t.Run("driverErrorTest", driverErrorTest)
	t.Run("driverNodeErrorStateTest", driverNodeErrorStateTest)
	t.Run("zoneTest", zoneTest)
	t.Run("regionTest", regionTest)
	t.Run("teardown", teardown)
}

// Send requests for a pod that doesn't have any PVCs.
// The filter response should return all the input nodes
// The prioritize response should return all nodes with equal priority
func noPVCTest(t *testing.T) {
	pod := newPod("noPVCPod", nil)
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "192.168.0.1", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "192.168.0.2", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "192.168.0.3", "rack1", "a", "us-east-1"))

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		nodes,
		[]int{defaultScore, defaultScore, defaultScore},
		prioritizeResponse)
}

// Create a pod with a PVC which uses a storage class other than
// the mock driver
// The filter response should return all the input nodes
// The prioritize response should return all nodes with equal priority
func noDriverVolumeTest(t *testing.T) {
	pod := newPod("noDriverVolumeTest", nil)
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "192.168.0.1", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "192.168.0.2", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "192.168.0.3", "rack1", "a", "us-east-1"))

	podVolume := v1.Volume{}
	podVolume.PersistentVolumeClaim = &v1.PersistentVolumeClaimVolumeSource{
		ClaimName: "noDriverPVC",
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, podVolume)

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		nodes,
		[]int{defaultScore, defaultScore, defaultScore},
		prioritizeResponse)
}

// Create a pod with a PVC using the mock storage class.
// Place the data on nodes n1, n2. Send requests with node n3, n4, n5
// The filter response should return all the input nodes
// The prioritize response should return all n3 with highest priority because of
// rack locality
func noDriverNodeTest(t *testing.T) {
	nodes := &v1.NodeList{}
	requestNodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node3", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4", "192.168.0.4", "rack3", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "192.168.0.5", "rack4", "", ""))
	requestNodes.Items = nodes.Items
	nodes.Items = append(nodes.Items, *newNode("node1", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "192.168.0.2", "rack2", "", ""))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("noDriverNode", []string{"noDriverNode"})

	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("noDriverNode", provNodes, 1); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	filterResponse, err := sendFilterRequest(pod, requestNodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, requestNodes, []int{0, 1, 2}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, requestNodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		requestNodes,
		[]int{rackPriorityScore, defaultScore, defaultScore},
		prioritizeResponse)
}

// Create a pod with a PVC using the mock storage class.
// Place the data on nodes n1, n2. Send requests with node n1, n2, n3, n4, n5
// The filter response should return all the input nodes
// The prioritize response should assign higher values to n1 and n2, followed by
// n3 and n4 for rack locality and then n5
func singleVolumeTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1.domain", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2.domain", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3.domain", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4.domain", "192.168.0.4", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5.domain", "192.168.0.5", "rack3", "", ""))

	provNodes := []int{0, 1}
	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("singleVolume", []string{"singleVolume"})

	if err := driver.ProvisionVolume("singleVolume", provNodes, 1); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2, 3, 4}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		nodes,
		[]int{nodePriorityScore,
			nodePriorityScore,
			rackPriorityScore,
			rackPriorityScore,
			defaultScore},
		prioritizeResponse)
}

// Create a pod with 2 PVCs using the mock storage class.
// Place the data for volume1 on nodes n1, n2.
// Place the data for volume2 on nodes n2, n3.
// Send requests with node n1, n2, n3, n4, n5
// The filter response should return all the input nodes
// The prioritize response should assign priorities in the following order
// n2 (both volumes local) >> n1 and n3 (one volume local each) >> n5 (both volumes on same rack) >> n4 (one volume on same rack)
func multipleVolumeTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "192.168.0.3", "rack3", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4", "192.168.0.4", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "192.168.0.5", "rack2", "", ""))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("doubleVolumePod", []string{"volume1", "volume2"})

	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("volume1", provNodes, 1); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	provNodes = []int{1, 2}
	if err := driver.ProvisionVolume("volume2", provNodes, 1); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2, 3, 4}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		nodes,
		[]int{nodePriorityScore,
			2 * nodePriorityScore,
			nodePriorityScore,
			rackPriorityScore,
			2 * rackPriorityScore},
		prioritizeResponse)
}

// Create a pod with a PVC using the mock storage class.
// Place the data on nodes n1, n2. Send requests with node n1, n2, n3, n4, n5
// Put the mock driver in error state.
// The filter response should return all the input nodes
// The prioritize response should return all nodes with equal priority
func driverErrorTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "192.168.0.2", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4", "192.168.0.4", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "192.168.0.5", "rack1", "", ""))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("driverErrorPod", []string{"volume1"})
	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("volume1", provNodes, 1); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	driver.SetInterfaceError(fmt.Errorf("Driver error"))
	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2, 3, 4}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		nodes,
		[]int{defaultScore, defaultScore, defaultScore, defaultScore, defaultScore},
		prioritizeResponse)
}

// Create a pod with a PVC using the mock storage class.
// Place the data on nodes n1, n2. Send requests with node n1, n2, n3, n4, n5
// Put n1 in error state.
// The filter response should return n2, n3, n4, n5. Use these nodes for
// prioritize request.
// The prioritize response should assign highest value to n2, followed n4 since
// it in the same rack as n2.
// n4 (data on same rack but node down) and n5 (no locality) should have the
// lowest scores.
func driverNodeErrorStateTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1.domain", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2.domain", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3.domain", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4.domain", "192.168.0.4", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5.domain", "192.168.0.5", "rack3", "", ""))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("driverErrorPod", []string{"volume1"})
	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("volume1", provNodes, 1); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	if err := driver.UpdateNodeStatus(0, volume.NodeOffline); err != nil {
		t.Fatalf("Error setting node status to Offline: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{1, 2, 3, 4}, filterResponse)

	nodes = filterResponse.Nodes
	prioritizeResponse, err := sendPrioritizeRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		nodes,
		[]int{nodePriorityScore,
			defaultScore,
			rackPriorityScore,
			defaultScore},
		prioritizeResponse)
}

// Create a pod with a PVC using the mock storage class.
// Place the data for volume1 on nodes n1, n2.
// Place the data for volume2 on nodes n2, n3.
// Send requests with node n1, n2, n3, n4, n5
// The filter response should return n1, n2, n3, n4, n5. Use these nodes for prioritize request.
// The prioritize response should assign priorities in the following order
// n2 (both volumes local) >> n1 (one volume local and other in same zone) >> n3 (one volume local) >> n4 (one volume in same zone) >> n5 (no locality)
func zoneTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "192.168.0.1", "rack1", "a", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "192.168.0.2", "rack2", "a", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "192.168.0.3", "rack1", "b", ""))
	nodes.Items = append(nodes.Items, *newNode("node4", "192.168.0.4", "rack2", "b", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "192.168.0.5", "rack1", "c", ""))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("zoneTest", []string{"volume1", "volume2"})
	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("volume1", provNodes, 1); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	provNodes = []int{1, 2}
	if err := driver.ProvisionVolume("volume2", provNodes, 1); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2, 3, 4}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		nodes,
		[]int{nodePriorityScore + zonePriorityScore,
			2 * nodePriorityScore,
			nodePriorityScore,
			zonePriorityScore,
			defaultScore},
		prioritizeResponse)
}

// Create a pod with a PVC using the mock storage class.
// Place the data for volume1 on nodes n1, n2.
// Place the data for volume2 on nodes n2, n3.
// Send requests with node n1, n2, n3, n4, n5
// The filter response should return n1, n2, n3, n4, n5. Use these nodes for prioritize request.
// The prioritize response should assign priorities in the following order
// n2 (both volumes local) >> n1 (one volume local and other in same zone) >> n3 (one volume local and other in same region) >> n4 and n5 (no locality)
func regionTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "192.168.0.1", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "192.168.0.2", "rack2", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "192.168.0.3", "rack1", "b", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node4", "192.168.0.4", "rack2", "b", "us-east-2"))
	nodes.Items = append(nodes.Items, *newNode("node5", "192.168.0.5", "rack1", "c", "us-east-2"))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("regionTest", []string{"volume1", "volume2"})
	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("volume1", provNodes, 1); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	provNodes = []int{1, 2}
	if err := driver.ProvisionVolume("volume2", provNodes, 1); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2, 3, 4}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		nodes,
		[]int{nodePriorityScore + zonePriorityScore,
			2 * nodePriorityScore,
			nodePriorityScore + regionPriorityScore,
			defaultScore,
			defaultScore},
		prioritizeResponse)
}
