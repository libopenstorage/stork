//go:build unittest
// +build unittest

package extender

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/drivers/volume/mock"
	fakeclient "github.com/libopenstorage/stork/pkg/client/clientset/versioned/fake"
	restore "github.com/libopenstorage/stork/pkg/snapshot/controllers"
	fakeocpclient "github.com/openshift/client-go/apps/clientset/versioned/fake"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/openshift"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetes "k8s.io/client-go/kubernetes/fake"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest/fake"
	"k8s.io/client-go/tools/record"
	schedulerapi "k8s.io/kube-scheduler/extender/v1"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
)

const (
	mockDriverName   = "MockDriver"
	defaultNamespace = "default"
)

var driver *mock.Driver
var extender *Extender
var fakeStorkClient *fakeclient.Clientset
var fakeOCPClient *fakeocpclient.Clientset
var fakeRestClient *fake.RESTClient

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

	fakeKubeClient := kubernetes.NewSimpleClientset()
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: corev1.New(fakeKubeClient.CoreV1().RESTClient()).Events("")})
	recorder := eventBroadcaster.NewRecorder(legacyscheme.Scheme, v1.EventSource{Component: "storktest"})

	// setup fake k8s instances
	fakeStorkClient = fakeclient.NewSimpleClientset()
	fakeOCPClient = fakeocpclient.NewSimpleClientset()
	fakeRestClient = &fake.RESTClient{}

	core.SetInstance(core.New(fakeKubeClient))
	storkops.SetInstance(storkops.New(fakeKubeClient, fakeStorkClient, nil))
	openshift.SetInstance(openshift.New(fakeKubeClient, fakeOCPClient, nil, nil))

	extender = &Extender{
		Driver:   storkdriver,
		Recorder: recorder,
	}

	if err = extender.Start(); err != nil {
		t.Fatalf("Error starting scheduler extender: %v", err)
	}

	// Wait for extender to be ready before starting test cases
	pod := newPod("setupPod", nil)
	nodes := &v1.NodeList{}
	retries := 5
	for i := 0; i < retries; i++ {
		_, err := sendFilterRequest(pod, nodes)
		if err == nil {
			return
		} else if strings.Contains(err.Error(), "connection refused") {
			logrus.Warnf("Extender not ready, retrying: %v", err)
			time.Sleep(1 * time.Second)
		} else {
			t.Fatalf("Unexpected Error setting up extender: %v", err)
		}
	}
	t.Fatalf("Failed setting up extender")
}

func teardown(t *testing.T) {
	if err := extender.Stop(); err != nil {
		t.Fatalf("Error stopping scheduler extender: %v", err)
	}
}

func newPod(podName string, volumes map[string]bool) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: defaultNamespace},
	}
	for volume, skipLabel := range volumes {
		pvc := driver.NewPVC(volume)
		if skipLabel {
			pvc.ObjectMeta.Annotations = make(map[string]string)
			pvc.ObjectMeta.Annotations[skipScoringLabel] = "true"
		}
		podVolume := v1.Volume{}
		podVolume.PersistentVolumeClaim = &v1.PersistentVolumeClaimVolumeSource{
			ClaimName: pvc.Name,
		}
		pod.Spec.Volumes = append(pod.Spec.Volumes, podVolume)
		_, err := core.Instance().CreatePersistentVolumeClaim(pvc)
		if err != nil {
			logrus.Errorf("Failed to create PVC: %v", err)
			return nil
		}
	}
	pod.Annotations = make(map[string]string)
	return pod
}

func newNode(name, hostname, ip, rack, zone, region string) *v1.Node {
	node := v1.Node{}

	node.Name = name
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
		Pod:   pod,
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

	defer func() {
		if err := resp.Body.Close(); err != nil {
			logrus.Warnf("Error closing decoder: %v", err)
		}
	}()
	if resp.StatusCode != http.StatusOK {
		contents, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(strings.TrimSpace(string(contents)))
	}

	decoder := json.NewDecoder(resp.Body)
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

	defer func() {
		if err := resp.Body.Close(); err != nil {
			logrus.Warnf("Error closing decoder: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		contents, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(strings.TrimSpace(string(contents)))
	}

	decoder := json.NewDecoder(resp.Body)
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
	expectedScores []float64,
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
						if int64(expectedScores[i]) != respNode.Score {
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
	t.Run("pxCSIExtPodNoDriverTest", pxCSIExtPodNoDriverTest)
	t.Run("pxCSIExtPodDriverTest", pxCSIExtPodDriverTest)
	t.Run("pxCSIExtPodStorageDownNodesTest", pxCSIExtPodStorageDownNodesTest)
	t.Run("pxCSIExtPodOfflinePxNodesTest", pxCSIExtPodOfflinePxNodesTest)
	t.Run("noPVCTest", noPVCTest)
	t.Run("noDriverVolumeTest", noDriverVolumeTest)
	t.Run("WFFCVolumeTest", WFFCVolumeTest)
	t.Run("WFFCMultiVolumeTest", WFFCMultiVolumeTest)
	t.Run("noVolumeNodeTest", noVolumeNodeTest)
	t.Run("noDriverNodeTest", noDriverNodeTest)
	t.Run("singleVolumeTest", singleVolumeTest)
	t.Run("multipleVolumeTest", multipleVolumeTest)
	t.Run("multipleVolumeSkipTest", multipleVolumeSkipTest)
	t.Run("multipleVolumeStorageDownTest", multipleVolumeStorageDownTest)
	t.Run("driverErrorTest", driverErrorTest)
	t.Run("driverNodeErrorStateTest", driverNodeErrorStateTest)
	t.Run("zoneTest", zoneTest)
	t.Run("zoneStorageDownNodeTest", zoneStorageDownNodeTest)
	t.Run("regionTest", regionTest)
	t.Run("regionStorageDownNodeTest", regionStorageDownNodeTest)
	t.Run("nodeNameTest", nodeNameTest)
	t.Run("ipTest", ipTest)
	t.Run("invalidRequestsTest", invalidRequestsTest)
	t.Run("noReplicasTest", noReplicasTest)
	t.Run("restorePVCTest", restorePVCTest)
	t.Run("preferLocalNodeTest", preferLocalNodeTest)
	t.Run("extenderMetricsTest", extenderMetricsTest)
	t.Run("preferRemoteNodeOnlyIgnoredForHyperConvergedVolumesTest", preferRemoteNodeOnlyIgnoredForHyperConvergedVolumesTest)
	t.Run("preferRemoteNodeOnlyFailedSchedulingTest", preferRemoteNodeOnlyFailedSchedulingTest)
	t.Run("preferRemoteNodeOnlyAntiHyperConvergenceTest", preferRemoteNodeOnlyAntiHyperConvergenceTest)
	t.Run("antiHyperConvergenceTest", antiHyperConvergenceTest)
	t.Run("offlineNodesAntiHyperConvergenceTest", offlineNodesAntiHyperConvergenceTest)
	t.Run("multiVolumeAntiHyperConvergenceTest", multiVolumeAntiHyperConvergenceTest)
	t.Run("multiVolume2AntiHyperConvergenceTest", multiVolume2AntiHyperConvergenceTest)
	t.Run("multiVolume3PreferRemoteOnlyAntiHyperConvergenceTest", multiVolume3PreferRemoteOnlyAntiHyperConvergenceTest)
	t.Run("multiVolumeSkipAllVolumeScoringTest", multiVolumeSkipAllVolumeScoringTest)
	t.Run("multiVolumeSkipHyperConvergedVolumesScoringTest", multiVolumeSkipHyperConvergedVolumesScoringTest)
	t.Run("multiVolumeWithStorageDownNodesAntiHyperConvergenceTest", multiVolumeWithStorageDownNodesAntiHyperConvergenceTest)
	t.Run("disableHyperConvergenceTest", disableHyperConvergenceTest)
	t.Run("preferLocalNodeWithHyperConvergedVolumesTest", preferLocalNodeWithHyperConvergedVolumesTest)
	t.Run("preferLocalNodeIgnoredWithAntiHyperConvergenceTest", preferLocalNodeIgnoredWithAntiHyperConvergenceTest)
	t.Run("teardown", teardown)
}

// Send scheduler request for px-csi-ext pod with volume driver disabled
// filter response should return all the input nodes
// prioritize response should return all nodes with defaultScore
func pxCSIExtPodNoDriverTest(t *testing.T) {
	pod := newPod("px-csi-ext-foo", nil)
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack1", "a", "us-east-1"))

	if err := driver.UpdateNodeStatus(3, volume.NodeDegraded); err != nil {
		t.Fatalf("Error setting node status to StorageDown: %v", err)
	}

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
		[]float64{defaultScore, defaultScore, defaultScore},
		prioritizeResponse)
}

// Send scheduler request for px-csi-ext pod with PX online on all nodes
// filter response should return all the input nodes
// prioritize response should return all nodes with nodePriorityScore
func pxCSIExtPodDriverTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node6", "node6", "192.168.0.6", "rack1", "", ""))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("px-csi-ext-foo", nil)

	if err := driver.UpdateNodeStatus(5, volume.NodeDegraded); err != nil {
		t.Fatalf("Error setting node status to StorageDown: %v", err)
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
		[]float64{nodePriorityScore, nodePriorityScore, nodePriorityScore, nodePriorityScore, nodePriorityScore},
		prioritizeResponse)
}

// Send scheduler request for px-csi-ext pod with a node in StorageDown state
// filter response should return all the input nodes
// prioritize response should return all nodes giving lower score to StorageDown node
func pxCSIExtPodStorageDownNodesTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack1", "", ""))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("px-csi-ext-foo", nil)

	if err := driver.UpdateNodeStatus(2, volume.NodeStorageDown); err != nil {
		t.Fatalf("Error setting node status to StorageDown: %v", err)
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
		[]float64{nodePriorityScore, nodePriorityScore, nodePriorityScore * (storageDownNodeScorePenaltyPercentage / 100), nodePriorityScore, nodePriorityScore},
		prioritizeResponse)
}

// Send scheduler request for px-csi-ext pod with PX offline on one node
// filter response should return all the input nodes except node with PX offline
// prioritize response should give 0 score to offline PX node
func pxCSIExtPodOfflinePxNodesTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack1", "", ""))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("px-csi-ext-foo", nil)

	if err := driver.UpdateNodeStatus(2, volume.NodeOffline); err != nil {
		t.Fatalf("Error setting node status to StorageDown: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 3, 4}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		nodes,
		[]float64{nodePriorityScore, nodePriorityScore, 0, nodePriorityScore, nodePriorityScore},
		prioritizeResponse)
}

// Send requests for a pod that doesn't have any PVCs.
// The filter response should return all the input nodes
// The prioritize response should return all nodes with equal priority
func noPVCTest(t *testing.T) {
	pod := newPod("noPVCPod", nil)
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "us-east-1"))

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
		[]float64{defaultScore, defaultScore, defaultScore},
		prioritizeResponse)
}

// Create a pod with a PVC which uses a storage class other than
// the mock driver
// The filter response should return all the input nodes
// The prioritize response should return all nodes with equal priority
func noDriverVolumeTest(t *testing.T) {
	pod := newPod("noDriverVolumeTest", nil)
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "us-east-1"))

	if err := driver.CreateCluster(3, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	podVolume := v1.Volume{}
	pvcClaim := &v1.PersistentVolumeClaim{}
	pvcClaim.Name = "noDriverPVC"
	pvcClaim.Spec.VolumeName = "noDriverVol"
	pvcSpec := &v1.PersistentVolumeClaimVolumeSource{
		ClaimName: pvcClaim.Name,
	}
	_, err := core.Instance().CreatePersistentVolumeClaim(pvcClaim)
	require.NoError(t, err)
	podVolume.PersistentVolumeClaim = pvcSpec
	pod.Spec.Volumes = append(pod.Spec.Volumes, podVolume)
	driver.AddPVC(pvcClaim)

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
		[]float64{defaultScore, defaultScore, defaultScore},
		prioritizeResponse)
}

// Create a pod with a PVC which uses the mocked WaitForFirstConusmer storage class
// The filter response should return all the input nodes
// The prioritize response should return all nodes with equal priority
func WFFCVolumeTest(t *testing.T) {
	pod := newPod("WFFCVolumeTest", nil)
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "us-east-1"))

	if err := driver.CreateCluster(3, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	podVolume := v1.Volume{}
	pvcClaim := &v1.PersistentVolumeClaim{}
	pvcClaim.Name = "WFFCPVC"
	pvcClaim.Spec.VolumeName = "WFFCVol"
	mockSC := mock.MockStorageClassNameWFFC
	pvcClaim.Spec.StorageClassName = &mockSC
	pvcSpec := &v1.PersistentVolumeClaimVolumeSource{
		ClaimName: pvcClaim.Name,
	}
	_, err := core.Instance().CreatePersistentVolumeClaim(pvcClaim)
	require.NoError(t, err)
	podVolume.PersistentVolumeClaim = pvcSpec
	pod.Spec.Volumes = append(pod.Spec.Volumes, podVolume)
	driver.AddPVC(pvcClaim)
	provNodes := []int{}
	if err := driver.ProvisionVolume("WFFCVol", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

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
		[]float64{defaultScore, defaultScore, defaultScore},
		prioritizeResponse)
}

// Create a pod with a PVC which uses the mocked WaitForFirstConusmer storage class
// and A normal mocked PVC on 1 node
// The filter response should return all the input nodes
// The prioritize response should prefer the node with the normal PVC on it
func WFFCMultiVolumeTest(t *testing.T) {
	pod := newPod("WFFCMultiVolumeTest", nil)
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "us-east-1"))

	if err := driver.CreateCluster(3, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	// WFFC volume
	podVolume1 := v1.Volume{}
	pvcClaim1 := &v1.PersistentVolumeClaim{}
	pvcClaim1.Name = "WFFCPVC1"
	pvcClaim1.Spec.VolumeName = "WFFCVol1"
	mockSC1 := mock.MockStorageClassNameWFFC
	pvcClaim1.Spec.StorageClassName = &mockSC1
	pvcSpec1 := &v1.PersistentVolumeClaimVolumeSource{
		ClaimName: pvcClaim1.Name,
	}
	_, err := core.Instance().CreatePersistentVolumeClaim(pvcClaim1)
	require.NoError(t, err)
	podVolume1.PersistentVolumeClaim = pvcSpec1
	pod.Spec.Volumes = append(pod.Spec.Volumes, podVolume1)
	driver.AddPVC(pvcClaim1)
	provNodes := []int{}
	if err := driver.ProvisionVolume("WFFCVol1", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	// Normal volume
	podVolume2 := v1.Volume{}
	pvcClaim2 := &v1.PersistentVolumeClaim{}
	pvcClaim2.Name = "normalPVC"
	pvcClaim2.Spec.VolumeName = "normalVol"
	mockSC2 := driver.GetStorageClassName()
	pvcClaim2.Spec.StorageClassName = &mockSC2
	pvcSpec2 := &v1.PersistentVolumeClaimVolumeSource{
		ClaimName: pvcClaim2.Name,
	}
	_, err = core.Instance().CreatePersistentVolumeClaim(pvcClaim2)
	require.NoError(t, err)
	podVolume2.PersistentVolumeClaim = pvcSpec2
	pod.Spec.Volumes = append(pod.Spec.Volumes, podVolume2)
	driver.AddPVC(pvcClaim2)
	provNodes = []int{1}
	if err := driver.ProvisionVolume("normalVol", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

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
		[]float64{rackPriorityScore, nodePriorityScore, rackPriorityScore},
		prioritizeResponse)
}

// Create a pod with a PVC using the mock storage class.
// Place the data on nodes n1, n2. Send requests with node n3, n4, n5
// The filter response should return all the input nodes
// The prioritize response should return n3 with highest priority because of
// rack locality
func noVolumeNodeTest(t *testing.T) {
	nodes := &v1.NodeList{}
	requestNodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack3", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack4", "", ""))
	requestNodes.Items = nodes.Items
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack2", "", ""))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("noVolumeNode", map[string]bool{"noVolumeNode": false})

	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("noVolumeNode", provNodes, 1, nil, false); err != nil {
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
		[]float64{rackPriorityScore, defaultScore, defaultScore},
		prioritizeResponse)
}

// Create a pod with a PVC using the mock storage class.
// Create a storage cluster with 3 nodes n1,n2,n3.
// Send filter request with node n4, n5
// The filter response should return an error
func noDriverNodeTest(t *testing.T) {
	nodes := &v1.NodeList{}
	requestNodes := &v1.NodeList{}
	driverNodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack3", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack4", "", ""))
	requestNodes.Items = nodes.Items
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "", ""))

	driverNodes.Items = nodes.Items[2:4]
	if err := driver.CreateCluster(3, driverNodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("noDriverNode", map[string]bool{"noDriverNode": false})

	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("noDriverNode", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	filterResponse, err := sendFilterRequest(pod, requestNodes)
	if err == nil {
		t.Fatalf("Expected error for filter request, got nil: %v", filterResponse)
	}
}

// Create a pod with a PVC using the mock storage class.
// Place the data on nodes n1, n2. Send requests with node n1, n2, n3, n4, n5
// The filter response should return all the input nodes
// The prioritize response should assign higher values to n1 and n2, followed by
// n3 and n4 for rack locality and then n5
func singleVolumeTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1.domain", "node1.domain", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2.domain", "node2.domain", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3.domain", "node3.domain", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4.domain", "node4.domain", "192.168.0.4", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5.domain", "node5.domain", "192.168.0.5", "rack3", "", ""))

	provNodes := []int{0, 1}
	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("singleVolume", map[string]bool{"singleVolume": false})

	if err := driver.ProvisionVolume("singleVolume", provNodes, 1, nil, false); err != nil {
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
		[]float64{nodePriorityScore,
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
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack3", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack2", "", ""))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("doubleVolumePod", map[string]bool{"volume1": false, "volume2": false})

	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("volume1", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	provNodes = []int{1, 2}
	if err := driver.ProvisionVolume("volume2", provNodes, 1, nil, false); err != nil {
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
		[]float64{nodePriorityScore,
			2 * nodePriorityScore,
			nodePriorityScore,
			rackPriorityScore,
			2 * rackPriorityScore},
		prioritizeResponse)
}

// Create a pod with 2 PVCs using the mock storage class.
// Place the data for volume1 on nodes n1, n2.
// Place the data for volume2 on nodes n2, n3.
// Put the skip scoring label on volume2
// Send requests with node n1, n2, n3, n4, n5
// The filter response should not include node n3.
// The prioritize response should assign priorities in the following order
// n1 & n2 highest priority. n3 should have no priority
func multipleVolumeSkipTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack3", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack2", "", ""))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("doubleVolumeSkipPod", map[string]bool{"included-volume": false, "excluded-volume": true})

	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("included-volume", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	provNodes = []int{1, 2}
	if err := driver.ProvisionVolume("excluded-volume", provNodes, 1, map[string]string{skipScoringLabel: "true"}, false); err != nil {
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
		[]float64{nodePriorityScore,
			nodePriorityScore,
			defaultScore,
			rackPriorityScore,
			rackPriorityScore},
		prioritizeResponse)
}

// Create a pod with 2 PVCs using the mock storage class.
// Place the data for volume1 on nodes n1, n2.
// Place the data for volume2 on nodes n2, n3.
// Set the node status of n3 to be StorageDown
// Send requests with node n1, n2, n3, n4, n5
// The scores returned for n2 should be half the expected value
// The prioritize response should assign priorities in the following order
// n2 (both volumes local) >> n1 (one volume local)  >> n5 (both volumes on same rack) >> n3 (one volume local but node StorageDown) and n4 (one volume on same rack)
// n1 & n3 highest priority. n2 should have half the priority
func multipleVolumeStorageDownTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack3", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack2", "", ""))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("doubleVolumeSkipPod", map[string]bool{"vol1": false, "vol2": true})

	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("vol1", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	provNodes = []int{1, 2}
	if err := driver.ProvisionVolume("vol2", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	if err := driver.UpdateNodeStatus(2, volume.NodeStorageDown); err != nil {
		t.Fatalf("Error setting node status to StorageDown: %v", err)
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
		[]float64{nodePriorityScore,
			2 * nodePriorityScore,
			rackPriorityScore * (storageDownNodeScorePenaltyPercentage / 100),
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
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack1", "", ""))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("driverErrorPod", map[string]bool{"driverErrorTest": false})
	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("volume1", provNodes, 1, nil, false); err != nil {
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
		[]float64{defaultScore, defaultScore, defaultScore, defaultScore, defaultScore},
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
	nodes.Items = append(nodes.Items, *newNode("node1.domain", "node1.domain", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2.domain", "node2.domain", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3.domain", "node3.domain", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4.domain", "node4.domain", "192.168.0.4", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5.domain", "node5.domain", "192.168.0.5", "rack3", "", ""))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("driverErrorPod", map[string]bool{"driverNodeErrorTest": false})
	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("driverNodeErrorTest", provNodes, 1, nil, false); err != nil {
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
		[]float64{nodePriorityScore,
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
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack2", "a", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "b", ""))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack2", "b", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack1", "c", ""))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("zoneTest", map[string]bool{"zoneVolume1": false, "zoneVolume2": false})
	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("zoneVolume1", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	provNodes = []int{1, 2}
	if err := driver.ProvisionVolume("zoneVolume2", provNodes, 1, nil, false); err != nil {
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
		[]float64{nodePriorityScore + zonePriorityScore,
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
// Set n1 in StorageDown state
// The filter response should return n1, n2, n3, n4, n5. Use these nodes for prioritize request.
// The prioritize response should assign priorities in the following order
// n2 (both volumes local) >> n3 (one volume local) >> n1 (one volume local and other
// in same zone but node StorageDown) >>  n4 (one volume in same zone) >> n5 (no locality)
func zoneStorageDownNodeTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack2", "a", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "b", ""))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack2", "b", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack1", "c", ""))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("zoneStorageDownNodeTest", map[string]bool{"zoneVol1": false, "zoneVol2": false})
	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("zoneVol1", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	provNodes = []int{1, 2}
	if err := driver.ProvisionVolume("zoneVol2", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	if err := driver.UpdateNodeStatus(0, volume.NodeStorageDown); err != nil {
		t.Fatalf("Error setting node status to StorageDown: %v", err)
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
	n1ExpectedPriority := rackPriorityScore*(storageDownNodeScorePenaltyPercentage/100) +
		zonePriorityScore*(storageDownNodeScorePenaltyPercentage/100)
	verifyPrioritizeResponse(
		t,
		nodes,
		[]float64{
			n1ExpectedPriority,
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
	nodes.Items = append(nodes.Items, *newNode("node11", "node1", "192.168.0.1", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node21", "node2", "192.168.0.2", "rack2", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node31", "node3", "192.168.0.3", "rack1", "b", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node41", "node4", "192.168.0.4", "rack2", "b", "us-east-2"))
	nodes.Items = append(nodes.Items, *newNode("node51", "node5", "192.168.0.5", "rack1", "c", "us-east-2"))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("regionTest", map[string]bool{"regionVolume1": false, "regionVolume2": false})
	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("regionVolume1", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	provNodes = []int{1, 2}
	if err := driver.ProvisionVolume("regionVolume2", provNodes, 1, nil, false); err != nil {
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
		[]float64{nodePriorityScore + zonePriorityScore,
			2 * nodePriorityScore,
			nodePriorityScore + regionPriorityScore,
			defaultScore,
			defaultScore},
		prioritizeResponse)
}

// Create a pod with a PVC using the mock storage class.
// Place the data for volume1 on nodes n1, n2.
// Place the data for volume2 on nodes n2, n3.
// Send requests with node n1, n2, n3, n4, n5
// Set node n3 to StorageDown state
// The filter response should return n1, n2, n3, n4, n5. Use these nodes for prioritize request.
// The prioritize response should assign priorities in the following order
// n2 (both volumes local) >> n1 (one volume local and other in same region) >> n3 (one volume local and other in same region but in StorageDown state) >> n4 and n5 (no locality)
func regionStorageDownNodeTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node11", "node1", "192.168.0.1", "rack1", "a", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node21", "node2", "192.168.0.2", "rack2", "c", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node31", "node3", "192.168.0.3", "rack1", "b", "us-east-1"))
	nodes.Items = append(nodes.Items, *newNode("node41", "node4", "192.168.0.4", "rack2", "b", "us-east-2"))
	nodes.Items = append(nodes.Items, *newNode("node51", "node5", "192.168.0.5", "rack1", "c", "us-east-2"))

	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("regionStorageDownNodeTest", map[string]bool{"regionVol1": false, "regionVol2": false})
	provNodes := []int{0, 1}
	if err := driver.ProvisionVolume("regionVol1", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	provNodes = []int{1, 2}
	if err := driver.ProvisionVolume("regionVol2", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	if err := driver.UpdateNodeStatus(2, volume.NodeStorageDown); err != nil {
		t.Fatalf("Error setting node status to StorageDown: %v", err)
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
	n3ExpectedScore := (rackPriorityScore * (storageDownNodeScorePenaltyPercentage / 100)) + (regionPriorityScore * (storageDownNodeScorePenaltyPercentage / 100))
	verifyPrioritizeResponse(
		t,
		nodes,
		[]float64{nodePriorityScore + zonePriorityScore,
			2 * nodePriorityScore,
			n3ExpectedScore,
			defaultScore,
			defaultScore},
		prioritizeResponse)
}

// Use IPs as hostname in kubernetes node object
func nodeNameTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "192.168.0.1", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "192.168.0.2", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "192.168.0.3", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4", "192.168.0.4", "192.168.0.4", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "192.168.0.5", "192.168.0.5", "rack3", "", ""))

	provNodes := []int{0, 1}
	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("nodeNameTest", map[string]bool{"nodeNameTest": false})

	if err := driver.ProvisionVolume("nodeNameTest", provNodes, 1, nil, false); err != nil {
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
		[]float64{nodePriorityScore,
			nodePriorityScore,
			rackPriorityScore,
			rackPriorityScore,
			defaultScore},
		prioritizeResponse)
}

// Use different hostnames for scheduler and driver. Only InternalIP should
// match
func ipTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("n1", "n1", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("n2", "n2", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("n3", "n3", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("n4", "n4", "192.168.0.4", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("n5", "n5", "192.168.0.5", "rack3", "", ""))

	provNodes := []int{0, 1}
	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}

	pod := newPod("ipTest", map[string]bool{"ipTest": false})

	if err := driver.ProvisionVolume("ipTest", provNodes, 1, nil, false); err != nil {
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
		[]float64{nodePriorityScore,
			nodePriorityScore,
			rackPriorityScore,
			rackPriorityScore,
			defaultScore},
		prioritizeResponse)
}

func invalidRequestsTest(t *testing.T) {
	resp, err := http.Post("http://localhost:8099/invalidPath",
		"application/json", nil)
	require.NoError(t, err, "Expected no error for invalid path")
	require.Equal(t, http.StatusNotFound, resp.StatusCode, "Excected HTTP NotFound for invalid path")

	resp, err = http.Post("http://localhost:8099/filter",
		"application/json", strings.NewReader("invalidNodes"))
	require.NoError(t, err, "Expected no error for bad request")
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, "Excected HTTP BadRequest for invalid request")

	resp, err = http.Post("http://localhost:8099/prioritize",
		"application/json", strings.NewReader("invalidNodes"))
	require.NoError(t, err, "Expected no error for bad request")
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, "Excected HTTP BadRequest for invalid request")
}

// Create a pod with a PVC using the mock storage class.
// Place the data on nodes n1. Mark n1 as offline Send requests with node n1,
// n2, n3
// The filter response should return an error since no replicas for
// the volume are online
func noReplicasTest(t *testing.T) {
	nodes := &v1.NodeList{}
	requestNodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "", ""))

	if err := driver.CreateCluster(3, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("noReplicasTest", map[string]bool{"noReplicasTest": false})

	provNodes := []int{0}
	if err := driver.ProvisionVolume("noReplicasTest", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	if err := driver.UpdateNodeStatus(0, volume.NodeOffline); err != nil {
		t.Fatalf("Error setting node status to Offline: %v", err)
	}
	_, err := sendFilterRequest(pod, requestNodes)
	require.Error(t, err, "Expected error since no replicas are online")
}

// Verify whether extender is checking restore annotation for pVC
// Create PVC with restore annotation,
// verify pod is not scheduled
func restorePVCTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "", ""))

	restoreAnnotation := make(map[string]string)
	restoreAnnotation[restore.RestoreAnnotation] = "true"
	invalidRestorePod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "invalidRestorePod"},
	}
	podVolume := v1.Volume{}
	// Create PVC Claim with annotation
	pvcClaim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "restorePVC",
			Annotations: restoreAnnotation,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName: "restoreVol",
		},
	}
	pvc, err := core.Instance().CreatePersistentVolumeClaim(pvcClaim)
	require.NoError(t, err)

	// invalid pvc
	invalidPVCSpec := &v1.PersistentVolumeClaimVolumeSource{
		ClaimName: "dummy-pvc-claim",
	}
	podVolume.PersistentVolumeClaim = invalidPVCSpec
	invalidRestorePod.Spec.Volumes = append(invalidRestorePod.Spec.Volumes, podVolume)
	_, err = sendFilterRequest(invalidRestorePod, nodes)
	require.Error(t, err, "Expected error since pvc details invalid")
	require.Contains(t, err.Error(), "Unable to find PVC")

	// restore annotation
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "restorePod", Namespace: defaultNamespace},
	}
	validPVCSpec := &v1.PersistentVolumeClaimVolumeSource{
		ClaimName: pvc.Name,
	}
	podVolume.PersistentVolumeClaim = validPVCSpec
	pod.Spec.Volumes = append(pod.Spec.Volumes, podVolume)
	_, err = sendFilterRequest(pod, nodes)
	require.Error(t, err, "Expected error since pvc has restore annotation")
	require.Contains(t, err.Error(), "Volume restore is in progress for pvc")

	delete(pvc.Annotations, restore.RestoreAnnotation)
	_, err = core.Instance().UpdatePersistentVolumeClaim(pvc)
	require.NoError(t, err)
	_, err = sendFilterRequest(pod, nodes)
	require.NoError(t, err)

	// check empty volume claim
	podVolume.PersistentVolumeClaim = nil
	_, err = sendFilterRequest(pod, nodes)
	require.NoError(t, err)
}

// Create a pod with a PVC using the mock storage class. Add an annotation to
// the pod to prefer a node with the volume replica only.
// Place the data on nodes n1. Send requests with node n2 and n3
// The filter response should return an error since no replicas for
// the volume are online
func preferLocalNodeTest(t *testing.T) {
	nodes := &v1.NodeList{}
	requestNodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "", ""))
	requestNodes.Items = nodes.Items
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "", ""))

	if err := driver.CreateCluster(3, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("preferLocalNodeTest", map[string]bool{"preferLocalNodeTest": false})

	provNodes := []int{0}
	if err := driver.ProvisionVolume("preferLocalNodeTest", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, requestNodes)
	require.NoError(t, err, "Expected no error since scheduling on non-local node was enabled")
	verifyFilterResponse(t, nodes, []int{0, 1}, filterResponse)

	pod.Annotations[preferLocalNodeOnlyAnnotation] = "true"
	_, err = sendFilterRequest(pod, requestNodes)
	require.Error(t, err, "Expected error since local node was not sent in filter request")
}

// stork extender prom-metrics test
func extenderMetricsTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1.domain", "node1.domain", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2.domain", "node2.domain", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3.domain", "node3.domain", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4.domain", "node4.domain", "192.168.0.4", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5.domain", "node5.domain", "192.168.0.5", "rack3", "", ""))

	provNodes := []int{0, 1}
	if err := driver.CreateCluster(5, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	// check if pod is hyper-Converged
	if err := driver.ProvisionVolume("metric-vol-1", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	pod := newPod("HyperPodTest", map[string]bool{"metric-vol-1": false})
	pod.Spec.NodeName = "node1"
	pod.Status.Conditions = make([]v1.PodCondition, 1)
	pod.Status.Conditions[0].Type = v1.PodReady
	pod.Status.Conditions[0].Status = v1.ConditionTrue
	_, err := core.Instance().CreatePod(pod)
	require.NoError(t, err, "failed to create pod")

	time.Sleep(3 * time.Second)
	require.Equal(t, testutil.ToFloat64(HyperConvergedPodsCounter), float64(1), "hyperConverged_pods_total not matched")

	// Semi-Hyper converged pod metrics
	if err := driver.ProvisionVolume("metric-vol-2", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	provNodes2 := []int{1, 2}
	if err := driver.ProvisionVolume("metric-vol-3", provNodes2, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	semiHyperPod := newPod("SemiPodTest", map[string]bool{"metric-vol-2": false, "metric-vol-3": false})
	semiHyperPod.Spec.NodeName = "node1"
	semiHyperPod.Status.Conditions = make([]v1.PodCondition, 1)
	semiHyperPod.Status.Conditions[0].Type = v1.PodReady
	semiHyperPod.Status.Conditions[0].Status = v1.ConditionTrue
	_, err = core.Instance().CreatePod(semiHyperPod)
	require.NoError(t, err, "failed to create pod")

	time.Sleep(3 * time.Second)
	require.Equal(t, testutil.ToFloat64(SemiHyperConvergePodsCounter), float64(1), "semi_hyperConverged_pods_total not matched")

	// non-hyper converged pod metrics
	if err := driver.ProvisionVolume("non-metric-vol", provNodes, 1, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}
	nonHyperPod := newPod("NonHyperPodTest", map[string]bool{"non-metric-vol": false})
	nonHyperPod.Spec.NodeName = "node5"
	nonHyperPod.Status.Conditions = make([]v1.PodCondition, 1)
	nonHyperPod.Status.Conditions[0].Type = v1.PodReady
	nonHyperPod.Status.Conditions[0].Status = v1.ConditionTrue
	_, err = core.Instance().CreatePod(nonHyperPod)
	require.NoError(t, err, "failed to create pod")

	time.Sleep(3 * time.Second)
	require.Equal(t, testutil.ToFloat64(NonHyperConvergePodsCounter), float64(1), "non_hyperConverged_pods_total not matched")
}

// Apply preferRemoteNodeOnly parameter to a hyperconverged service volume
// preferRemoteNodeOnly should get ignored and all nodes should be returned in the filter response
func preferRemoteNodeOnlyIgnoredForHyperConvergedVolumesTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack2", "b", "zone2"))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack2", "b", "zone2"))
	nodes.Items = append(nodes.Items, *newNode("node6", "node6", "192.168.0.6", "rack2", "b", "zone2"))

	if err := driver.CreateCluster(6, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("preferRemoteNodeOnlyIgnoredForHyperConvergedVolumesTest", map[string]bool{"preferRemoteNodeOnlyIgnoredForHyperConvergedVolumesTest": false})

	provNodes := []int{0, 1, 2}
	if err := driver.ProvisionVolume("preferRemoteNodeOnlyIgnoredForHyperConvergedVolumesTest", provNodes, 6, map[string]string{preferRemoteNodeOnlyParameter: "true"}, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2, 3, 4, 5}, filterResponse)
}

// Apply preferRemoteNodeOnly parameter to antiHyperConverged volume and make sure volume is present on all the nodes
// preferRemoteNodeOnly should get honored and filter api should fail since unable to find a valid node
func preferRemoteNodeOnlyFailedSchedulingTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "zone1"))

	if err := driver.CreateCluster(3, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("preferRemoteNodeOnlyFailedSchedulingTest", map[string]bool{"preferRemoteNodeOnlyFailedSchedulingTest": false})

	provNodes := []int{0, 1, 2}
	if err := driver.ProvisionVolume("preferRemoteNodeOnlyFailedSchedulingTest", provNodes, 3, map[string]string{preferRemoteNodeOnlyParameter: "true"}, true); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	_, err := sendFilterRequest(pod, nodes)
	require.Error(t, err, "Expected error since scheduling on local node is not allowed with preferRemoteNodeOnly")
}

// Apply preferRemoteNodeOnly parameter to antiHyperConverged volumes volume
// preferRemoteNodeOnly should get honored and filter api should return non replica nodes
func preferRemoteNodeOnlyAntiHyperConvergenceTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack2", "b", "zone2"))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack2", "b", "zone2"))
	nodes.Items = append(nodes.Items, *newNode("node6", "node6", "192.168.0.6", "rack2", "b", "zone2"))

	if err := driver.CreateCluster(6, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("preferRemoteNodeOnlyAntiHyperConvergenceTest", map[string]bool{"preferRemoteNodeOnlyAntiHyperConvergenceTest": false})

	provNodes := []int{0, 1, 2}
	if err := driver.ProvisionVolume("preferRemoteNodeOnlyAntiHyperConvergenceTest", provNodes, 3, map[string]string{preferRemoteNodeOnlyParameter: "true"}, true); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{3, 4, 5}, filterResponse)

	if err = driver.UpdateNodeStatus(3, volume.NodeOffline); err != nil {
		t.Fatalf("Error setting node status to Offline: %v", err)
	}
	filterResponse, err = sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{4, 5}, filterResponse)

	if err := driver.UpdateNodeStatus(4, volume.NodeStorageDown); err != nil {
		t.Fatalf("Error setting node status to NodeStorageDown: %v", err)
	}
	if err := driver.UpdateNodeStatus(5, volume.NodeStorageDown); err != nil {
		t.Fatalf("Error setting node status to NodeStorageDown: %v", err)
	}
	filterResponse, err = sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{4, 5}, filterResponse)
}

// Use a antiHyperConverged volumes volume such that there are non replica nodes available
// Higher scores should be given to the non antiHyperConverged volumes volume nodes
func antiHyperConvergenceTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack2", "b", "zone2"))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack2", "b", "zone2"))
	nodes.Items = append(nodes.Items, *newNode("node6", "node6", "192.168.0.6", "rack2", "b", "zone2"))

	if err := driver.CreateCluster(6, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("sharedV4ServiceAntiHyperConverged", map[string]bool{"sharedV4ServiceAntiHyperConverged": false})

	provNodes := []int{0, 1, 2}
	if err := driver.ProvisionVolume("sharedV4ServiceAntiHyperConverged", provNodes, 3, nil, true); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2, 3, 4, 5}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		nodes,
		[]float64{defaultScore,
			defaultScore,
			defaultScore,
			nodePriorityScore,
			nodePriorityScore,
			nodePriorityScore},
		prioritizeResponse)
}

// Offline nodes should not be returned in filter response for pods using antiHyperConverged volumes volumes
func offlineNodesAntiHyperConvergenceTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack2", "b", "zone2"))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack2", "b", "zone2"))
	nodes.Items = append(nodes.Items, *newNode("node6", "node6", "192.168.0.6", "rack2", "b", "zone2"))

	if err := driver.CreateCluster(6, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("offlineNodesAntiHyperConvergenceTest", map[string]bool{"offlineNodesAntiHyperConvergenceTest": false})

	provNodes := []int{0, 1, 2}
	if err := driver.ProvisionVolume("offlineNodesAntiHyperConvergenceTest", provNodes, 3, nil, true); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	if err := driver.UpdateNodeStatus(0, volume.NodeOffline); err != nil {
		t.Fatalf("Error setting node status to Offline: %v", err)
	}

	if err := driver.UpdateNodeStatus(3, volume.NodeOffline); err != nil {
		t.Fatalf("Error setting node status to Offline: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{1, 2, 4, 5}, filterResponse)
}

// Deploy both anti hyperconverged regular volumes
// Verify antihyperconvereged nodes take higher score
func multiVolumeAntiHyperConvergenceTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node6", "node6", "192.168.0.6", "rack1", "a", "zone1"))

	if err := driver.CreateCluster(6, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("multiVolumeAntiHyperConvergenceTest", map[string]bool{"HyperConvergedVolumes": false, "sharedV4Svc": false})

	regularVolumeProvNodes := []int{0, 1, 2}
	if err := driver.ProvisionVolume("HyperConvergedVolumes", regularVolumeProvNodes, 3, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	sharedV4SvcProvNodes := []int{3, 4, 5}
	if err := driver.ProvisionVolume("sharedV4Svc", sharedV4SvcProvNodes, 3, nil, true); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2, 3, 4, 5}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		nodes,
		[]float64{2 * nodePriorityScore,
			2 * nodePriorityScore,
			2 * nodePriorityScore,
			defaultScore,
			defaultScore,
			defaultScore},
		prioritizeResponse)
}

// Deploy both antihyperconverged and regular volumes
// Verify antihyperconvereged nodes take higher score
func multiVolume2AntiHyperConvergenceTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node6", "node6", "192.168.0.6", "rack2", "b", "zone2"))

	if err := driver.CreateCluster(6, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("multiVolumeAntiHyperConvergenceTest", map[string]bool{"HyperConvergedVolumes2": false, "sharedV4Svc2": false})

	regularVolumeProvNodes := []int{0, 1, 2}
	if err := driver.ProvisionVolume("HyperConvergedVolumes2", regularVolumeProvNodes, 3, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	sharedV4SvcProvNodes := []int{2, 3, 4}
	if err := driver.ProvisionVolume("sharedV4Svc2", sharedV4SvcProvNodes, 3, nil, true); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2, 3, 4, 5}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		nodes,
		[]float64{2 * nodePriorityScore,
			2 * nodePriorityScore,
			defaultScore,
			defaultScore,
			defaultScore,
			nodePriorityScore},
		prioritizeResponse)
}

// Deploy both anti hyperconveged volume and regular volumes
// Verify antihyperconvereged nodes take higher score
// preferRemoteNodeOnly parameter enabled
func multiVolume3PreferRemoteOnlyAntiHyperConvergenceTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack2", "b", "zone2"))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack2", "b", "zone2"))
	nodes.Items = append(nodes.Items, *newNode("node6", "node6", "192.168.0.6", "rack2", "b", "zone2"))

	filteredNodes := &v1.NodeList{}
	filteredNodes.Items = append(filteredNodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack2", "b", "zone2"))
	filteredNodes.Items = append(filteredNodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack2", "b", "zone2"))
	filteredNodes.Items = append(filteredNodes.Items, *newNode("node6", "node6", "192.168.0.6", "rack2", "b", "zone2"))

	if err := driver.CreateCluster(6, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("multiVolumeAntiHyperConvergenceTest", map[string]bool{"HyperConvergedVolumes3": false, "sharedV4Svc3": false})

	regularVolumeProvNodes := []int{0, 1, 2}
	if err := driver.ProvisionVolume("HyperConvergedVolumes3", regularVolumeProvNodes, 3, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	sharedV4SvcProvNodes := []int{0, 1, 2}
	if err := driver.ProvisionVolume("sharedV4Svc3", sharedV4SvcProvNodes, 3, map[string]string{preferRemoteNodeOnlyParameter: "true"}, true); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{3, 4, 5}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, filteredNodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		filteredNodes,
		[]float64{
			nodePriorityScore,
			nodePriorityScore,
			nodePriorityScore},
		prioritizeResponse)
}

// Verify skipSchedulerScoring is honored for antihyperconvegence volume pods
func multiVolumeSkipHyperConvergedVolumesScoringTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node6", "node6", "192.168.0.6", "rack1", "a", "zone1"))

	if err := driver.CreateCluster(6, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("multiVolumeSkipHyperConvergedVolumesScoringTest", map[string]bool{"HyperConvergedVolumesMultiSkip": true, "sharedV4SvcMultiSkip": true})

	regularVolumeProvNodes := []int{0, 1, 2}
	if err := driver.ProvisionVolume("HyperConvergedVolumesMultiSkip", regularVolumeProvNodes, 3, map[string]string{skipScoringLabel: "true"}, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	sharedV4SvcProvNodes := []int{3, 4, 5}
	if err := driver.ProvisionVolume("sharedV4SvcMultiSkip", sharedV4SvcProvNodes, 3, nil, true); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2, 3, 4, 5}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		nodes,
		[]float64{
			nodePriorityScore,
			nodePriorityScore,
			nodePriorityScore,
			defaultScore,
			defaultScore,
			defaultScore},
		prioritizeResponse)
}

// Verify skipSchedulerScoring is honored for a pod using both regular and antihyperconverged volumes
func multiVolumeSkipAllVolumeScoringTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node6", "node6", "192.168.0.6", "rack1", "a", "zone1"))

	if err := driver.CreateCluster(6, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("multiVolumeSkipAllVolumeScoringTest", map[string]bool{"HyperConvergedVolumesSkip": true, "sharedV4SvcMultiVolumeSkip": true})

	regularVolumeProvNodes := []int{0, 1, 2}
	if err := driver.ProvisionVolume("HyperConvergedVolumesSkip", regularVolumeProvNodes, 3, map[string]string{skipScoringLabel: "true"}, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	sharedV4SvcProvNodes := []int{3, 4, 5}
	if err := driver.ProvisionVolume("sharedV4SvcMultiVolumeSkip", sharedV4SvcProvNodes, 3, map[string]string{skipScoringLabel: "true"}, true); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2, 3, 4, 5}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		nodes,
		[]float64{
			defaultScore,
			defaultScore,
			defaultScore,
			defaultScore,
			defaultScore,
			defaultScore},
		prioritizeResponse)
}

// Verify anti hyperconverged works with multi volumes with StorageDown nodes
func multiVolumeWithStorageDownNodesAntiHyperConvergenceTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node6", "node6", "192.168.0.6", "rack1", "a", "zone1"))

	if err := driver.CreateCluster(6, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("multiVolumeWithStorageDownNodesAntiHyperConvergenceTest", map[string]bool{"StorageDownNodesHyperConvergedVolumes": false, "StorageDownNodeSharedV4Svc": false})

	regularVolumeProvNodes := []int{0, 1, 2}
	if err := driver.ProvisionVolume("StorageDownNodesHyperConvergedVolumes", regularVolumeProvNodes, 3, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	sharedV4SvcProvNodes := []int{3, 4, 5}
	if err := driver.ProvisionVolume("StorageDownNodeSharedV4Svc", sharedV4SvcProvNodes, 3, nil, true); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	if err := driver.UpdateNodeStatus(3, volume.NodeStorageDown); err != nil {
		t.Fatalf("Error setting node status to Offline: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2, 3, 4, 5}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		nodes,
		[]float64{2 * nodePriorityScore,
			2 * nodePriorityScore,
			2 * nodePriorityScore,
			defaultScore,
			defaultScore,
			defaultScore},
		prioritizeResponse)
}

// Verify disableHyperconvergenceAnnotation is honored
func disableHyperConvergenceTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack2", "b", "zone2"))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack2", "b", "zone2"))
	nodes.Items = append(nodes.Items, *newNode("node6", "node6", "192.168.0.6", "rack2", "b", "zone2"))

	if err := driver.CreateCluster(6, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("multiVolumeDisableHyperConvergedTest", map[string]bool{"multiVolumeDisableHyperConvergedTest": false})
	pod.Annotations[preferLocalNodeOnlyAnnotation] = "true"
	pod.Annotations[disableHyperconvergenceAnnotation] = "true"

	provNodes := []int{0, 1, 2}
	if err := driver.ProvisionVolume("multiVolumeDisableHyperConvergedTest", provNodes, 3, nil, true); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2, 3, 4, 5}, filterResponse)

	prioritizeResponse, err := sendPrioritizeRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending prioritize request: %v", err)
	}
	verifyPrioritizeResponse(
		t,
		nodes,
		[]float64{
			defaultScore,
			defaultScore,
			defaultScore,
			defaultScore,
			defaultScore,
			defaultScore},
		prioritizeResponse)
}

// Verify preferLocalNodeOnly is honored
func preferLocalNodeWithHyperConvergedVolumesTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack2", "b", "zone2"))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack2", "b", "zone2"))
	nodes.Items = append(nodes.Items, *newNode("node6", "node6", "192.168.0.6", "rack2", "b", "zone2"))

	if err := driver.CreateCluster(6, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("preferLocalNodeWithHyperConvergedVolumesTest", map[string]bool{"preferLocalNodeWithHyperConvergedVolumesTest": false})
	pod.Annotations[preferLocalNodeOnlyAnnotation] = "true"

	provNodes := []int{0, 1, 2}
	if err := driver.ProvisionVolume("preferLocalNodeWithHyperConvergedVolumesTest", provNodes, 3, nil, false); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2}, filterResponse)
}

// Verify preferLocalNodeOnly is ignored for antihyperconverged volumes
func preferLocalNodeIgnoredWithAntiHyperConvergenceTest(t *testing.T) {
	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1", "192.168.0.1", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2", "192.168.0.2", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3", "192.168.0.3", "rack1", "a", "zone1"))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4", "192.168.0.4", "rack2", "b", "zone2"))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5", "192.168.0.5", "rack2", "b", "zone2"))
	nodes.Items = append(nodes.Items, *newNode("node6", "node6", "192.168.0.6", "rack2", "b", "zone2"))

	if err := driver.CreateCluster(6, nodes); err != nil {
		t.Fatalf("Error creating cluster: %v", err)
	}
	pod := newPod("preferLocalNodeIgnoredWithAntiHyperConvergenceTest", map[string]bool{"preferLocalNodeIgnoredWithAntiHyperConvergenceTest": false})
	pod.Annotations[preferLocalNodeOnlyAnnotation] = "true"

	provNodes := []int{0, 1, 2}
	if err := driver.ProvisionVolume("preferLocalNodeIgnoredWithAntiHyperConvergenceTest", provNodes, 3, nil, true); err != nil {
		t.Fatalf("Error provisioning volume: %v", err)
	}

	filterResponse, err := sendFilterRequest(pod, nodes)
	if err != nil {
		t.Fatalf("Error sending filter request: %v", err)
	}
	verifyFilterResponse(t, nodes, []int{0, 1, 2, 3, 4, 5}, filterResponse)
}
