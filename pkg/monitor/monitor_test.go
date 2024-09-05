//go:build unittest
// +build unittest

package monitor

import (
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/drivers/volume/mock"
	mockVolumeDriver "github.com/libopenstorage/stork/drivers/volume/mock/volume"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/cache"
	fakeclient "github.com/libopenstorage/stork/pkg/client/clientset/versioned/fake"
	mockcache "github.com/libopenstorage/stork/pkg/mock/cache"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kubernetes "k8s.io/client-go/kubernetes/fake"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/util/node"
)

const (
	mockDriverName   = "MockDriver"
	nodeForPod       = "node1.domain"
	defaultNamespace = "default"
)

var (
	driverVolumeName      = "singleVolume"
	attachmentVolumeName  = "attachmentVolume"
	unknownPodsVolumeName = "unknownPodsVolume"

	fakeStorkClient        *fakeclient.Clientset
	driver                 *mock.Driver
	monitor                *Monitor
	nodes                  *v1.NodeList
	testNodeOfflineTimeout time.Duration
	volumeDriver           *mockVolumeDriver.MockDriver
	mockCtrl               *gomock.Controller
	mockCacheInstance      *mockcache.MockSharedInformerCache
)

func TestMonitor(t *testing.T) {
	t.Run("setup", setup)
	t.Run("testUnknownDriverPod", testUnknownDriverPod)
	t.Run("testUnknownOtherDriverPod", testUnknownOtherDriverPod)
	t.Run("testEvictedDriverPod", testEvictedDriverPod)
	t.Run("testEvictedOtherDriverPod", testEvictedOtherDriverPod)
	t.Run("testTempOfflineStorageNode", testTempOfflineStorageNode)
	t.Run("testOfflineStorageNodeDuplicateIP", testOfflineStorageNodeDuplicateIP)
	t.Run("testStorageDownNode", testStorageDownNode)
	t.Run("teardown", teardown)
}

func TestMonitorOfflineNodes(t *testing.T) {
	t.Run("testOfflineStorageNode", testOfflineStorageNode)
	t.Run("testOfflineStorageNodeForCSIExtPod", testOfflineStorageNodeForCSIExtPod)
	t.Run("testOfflineStorageNodesBatchTest", testOfflineStorageNodesBatchTest)
}

func TestMonitorMethods(t *testing.T) {
	t.Run("testGetVolumeDriverNodesToK8sNodeMap", testGetVolumeDriverNodesToK8sNodeMap)
	t.Run("testBatchDeletePodsFromOfflineNodes", testBatchDeletePodsFromOfflineNodes)
}

func setup(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	scheme := runtime.NewScheme()
	err := stork_api.AddToScheme(scheme)
	require.NoError(t, err, "Error adding stork scheme")

	fakeStorkClient = fakeclient.NewSimpleClientset()
	fakeKubeClient := kubernetes.NewSimpleClientset()
	core.SetInstance(core.New(fakeKubeClient))
	storage.SetInstance(storage.New(fakeKubeClient.StorageV1()))
	storkops.SetInstance(storkops.New(fakeKubeClient, fakeStorkClient, nil))

	storkdriver, err := volume.Get(mockDriverName)
	require.NoError(t, err, "Error getting mock volume driver")

	var ok bool
	driver, ok = storkdriver.(*mock.Driver)
	require.True(t, ok, "Error casting mockdriver")

	err = storkdriver.Init(nil)
	require.NoError(t, err, "Error initializing mock volume driver")

	nodes = &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode(nodeForPod, nodeForPod, "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2.domain", "node2.domain", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3.domain", "node3.domain", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4.domain", "node4.domain", "192.168.0.4", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5.domain", "node5.domain", "192.168.0.5", "rack3", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node6.domain", "node6.domain", "192.168.0.1", "rack1", "", ""))

	for _, n := range nodes.Items {
		node, err := core.Instance().CreateNode(&n)
		require.NoError(t, err, "failed to create fake node")
		require.NotNil(t, node, "got nil node from create node api")
	}

	provNodes := []int{0, 1}
	err = driver.CreateCluster(6, nodes)
	require.NoError(t, err, "Error creating cluster")

	err = driver.UpdateNodeIP(5, "192.168.0.1")
	require.NoError(t, err, "Error updating node IP")
	err = driver.UpdateNodeStatus(5, volume.NodeOffline)
	require.NoError(t, err, "Error setting node status to Offline")

	err = driver.ProvisionVolume(driverVolumeName, provNodes, 1, nil, false, false, "")
	require.NoError(t, err, "Error provisioning volume")

	err = driver.ProvisionVolume(attachmentVolumeName, provNodes, 2, nil, false, false, "")
	require.NoError(t, err, "Error provisioning volume")

	err = driver.ProvisionVolume(unknownPodsVolumeName, provNodes, 3, nil, false, false, "")
	require.NoError(t, err, "Error provisioning volume")

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: corev1.New(fakeKubeClient.CoreV1().RESTClient()).Events("")})
	recorder := eventBroadcaster.NewRecorder(legacyscheme.Scheme, v1.EventSource{Component: "storktest"})

	monitor = &Monitor{
		Driver:      storkdriver,
		IntervalSec: 30,
		Recorder:    recorder,
	}

	// overwrite the backoff timers to speed up the tests
	// this accounts to a backoff of 1 min
	nodeWaitCallBackoff = wait.Backoff{
		Duration: initialNodeWaitDelay,
		Factor:   1,
		Steps:    nodeWaitSteps,
	}
	// 30 (interval)  + 60 (backoff) + 5 (buffer)
	testNodeOfflineTimeout = 95 * time.Second

	err = monitor.Start()
	require.NoError(t, err, "failed to start monitor")
}

func teardown(t *testing.T) {
	err := monitor.Stop()
	require.NoError(t, err, "Error stopping monitor")
	prometheus.Unregister(HealthCounter)
}

func setupWithNewMockDriver(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	scheme := runtime.NewScheme()
	err := stork_api.AddToScheme(scheme)
	require.NoError(t, err, "Error adding stork scheme")

	fakeStorkClient = fakeclient.NewSimpleClientset()
	fakeKubeClient := kubernetes.NewSimpleClientset()
	core.SetInstance(core.New(fakeKubeClient))
	storage.SetInstance(storage.New(fakeKubeClient.StorageV1()))
	storkops.SetInstance(storkops.New(fakeKubeClient, fakeStorkClient, nil))

	storkdriver, err := volume.Get(mockDriverName)
	require.NoError(t, err, "Error getting mock volume driver")

	var ok bool
	driver, ok = storkdriver.(*mock.Driver)
	require.True(t, ok, "Error casting mockdriver")

	err = storkdriver.Init(nil)
	require.NoError(t, err, "Error initializing mock volume driver")

	nodes = &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode(nodeForPod, nodeForPod, "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2.domain", "node2.domain", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3.domain", "node3.domain", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4.domain", "node4.domain", "192.168.0.4", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5.domain", "node5.domain", "192.168.0.5", "rack3", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node6.domain", "node6.domain", "192.168.0.1", "rack1", "", ""))

	for _, n := range nodes.Items {
		node, err := core.Instance().CreateNode(&n)
		require.NoError(t, err, "failed to create fake node")
		require.NotNil(t, node, "got nil node from create node api")
	}

	provNodes := []int{0, 1}
	err = driver.CreateCluster(6, nodes)
	require.NoError(t, err, "Error creating cluster")

	err = driver.UpdateNodeIP(5, "192.168.0.1")
	require.NoError(t, err, "Error updating node IP")
	err = driver.UpdateNodeStatus(5, volume.NodeOffline)
	require.NoError(t, err, "Error setting node status to Offline")

	err = driver.ProvisionVolume(driverVolumeName, provNodes, 1, nil, false, false, "")
	require.NoError(t, err, "Error provisioning volume")

	err = driver.ProvisionVolume(attachmentVolumeName, provNodes, 2, nil, false, false, "")
	require.NoError(t, err, "Error provisioning volume")

	err = driver.ProvisionVolume(unknownPodsVolumeName, provNodes, 3, nil, false, false, "")
	require.NoError(t, err, "Error provisioning volume")

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: corev1.New(fakeKubeClient.CoreV1().RESTClient()).Events("")})
	recorder := eventBroadcaster.NewRecorder(legacyscheme.Scheme, v1.EventSource{Component: "storktest"})

	monitor = &Monitor{
		Driver:      storkdriver,
		IntervalSec: 30,
		Recorder:    recorder,
	}

	mockCtrl = gomock.NewController(t)
	volumeDriver = mockVolumeDriver.NewMockDriver(mockCtrl)

	driverNodes := getDriverNodes(len(nodes.Items))
	volumeDriver.EXPECT().GetNodes().Return(driverNodes, nil).Times(1)

	monitor.Driver = volumeDriver

	// overwrite the backoff timers to speed up the tests
	// this accounts to a backoff of 1 min
	nodeWaitCallBackoff = wait.Backoff{
		Duration: initialNodeWaitDelay,
		Factor:   1,
		Steps:    nodeWaitSteps,
	}
	// 30 (interval)  + 60 (backoff) + 5 (buffer)
	testNodeOfflineTimeout = 95 * time.Second

	err = monitor.Start()
	require.NoError(t, err, "failed to start monitor")
}

func setupWithNewMockDriverScale(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	scheme := runtime.NewScheme()
	err := stork_api.AddToScheme(scheme)
	require.NoError(t, err, "Error adding stork scheme")

	numNodes := 50

	fakeStorkClient = fakeclient.NewSimpleClientset()
	fakeKubeClient := kubernetes.NewSimpleClientset()
	core.SetInstance(core.New(fakeKubeClient))
	storage.SetInstance(storage.New(fakeKubeClient.StorageV1()))
	storkops.SetInstance(storkops.New(fakeKubeClient, fakeStorkClient, nil))

	mockCtrl = gomock.NewController(t)
	mockCacheInstance = mockcache.NewMockSharedInformerCache(mockCtrl)
	mockCacheInstance.EXPECT().WatchPods(gomock.Any()).Return(nil).AnyTimes()
	cache.SetTestInstance(mockCacheInstance)

	storkdriver, err := volume.Get(mockDriverName)
	require.NoError(t, err, "Error getting mock volume driver")

	var ok bool
	driver, ok = storkdriver.(*mock.Driver)
	require.True(t, ok, "Error casting mockdriver")

	err = storkdriver.Init(nil)
	require.NoError(t, err, "Error initializing mock volume driver")

	nodes = &v1.NodeList{}
	for i := 0; i < numNodes; i++ {
		nodes.Items = append(nodes.Items, *newNode(fmt.Sprintf("node%d.domain", i), fmt.Sprintf("node%d.domain", i), fmt.Sprintf("192.168.0.%d", i), "rack1", "", ""))
	}

	for _, n := range nodes.Items {
		node, err := core.Instance().CreateNode(&n)
		require.NoError(t, err, "failed to create fake node")
		require.NotNil(t, node, "got nil node from create node api")
	}

	err = driver.CreateCluster(numNodes, nodes)
	require.NoError(t, err, "Error creating cluster")

	for i := 0; i < numNodes; i++ {
		provNodes := []int{i, (i + 1) % numNodes}
		err = driver.ProvisionVolume(fmt.Sprintf("%s%d", driverVolumeName, i), provNodes, 1, nil, false, false, "")
		require.NoError(t, err, "Error provisioning volume")
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: corev1.New(fakeKubeClient.CoreV1().RESTClient()).Events("")})
	recorder := eventBroadcaster.NewRecorder(legacyscheme.Scheme, v1.EventSource{Component: "storktest"})

	monitor = &Monitor{
		Driver:      storkdriver,
		IntervalSec: 30,
		Recorder:    recorder,
	}

	mockCtrl = gomock.NewController(t)
	volumeDriver = mockVolumeDriver.NewMockDriver(mockCtrl)

	driverNodes := getDriverNodes(len(nodes.Items))
	volumeDriver.EXPECT().GetNodes().Return(driverNodes, nil).Times(1)

	monitor.Driver = volumeDriver

	// overwrite the backoff timers to speed up the tests
	// this accounts to a backoff of 1 min
	nodeWaitCallBackoff = wait.Backoff{
		Duration: initialNodeWaitDelay,
		Factor:   1,
		Steps:    nodeWaitSteps,
	}

	err = monitor.Start()
	require.NoError(t, err, "failed to start monitor")
}

func teardownWithNewMockDriver(t *testing.T) {
	mockCtrl.Finish()
	err := monitor.Stop()
	require.NoError(t, err, "Error stopping monitor")
	//monitor = nil
	prometheus.Unregister(HealthCounter)
}

func testUnknownDriverPod(t *testing.T) {
	pod := newPod("driverPod", []string{driverVolumeName}, nodeForPod)
	testLostPod(t, pod, true, true, false)
}

func testUnknownOtherDriverPod(t *testing.T) {
	pod := newPod("otherDriverPod", nil, nodeForPod)
	podVolume := v1.Volume{}
	podVolume.PersistentVolumeClaim = &v1.PersistentVolumeClaimVolumeSource{
		ClaimName: "noDriverPVC",
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, podVolume)

	testLostPod(t, pod, false, true, false)
}

func testEvictedOtherDriverPod(t *testing.T) {
	pod := newPod("otherDriverPod", nil, nodeForPod)
	podVolume := v1.Volume{}
	podVolume.PersistentVolumeClaim = &v1.PersistentVolumeClaimVolumeSource{
		ClaimName: "noDriverPVC",
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, podVolume)

	testLostPod(t, pod, false, false, true)
}

func testEvictedDriverPod(t *testing.T) {
	pod := newPod("driverPod", []string{driverVolumeName}, nodeForPod)
	testLostPod(t, pod, true, false, true)
}

func testLostPod(
	t *testing.T,
	pod *v1.Pod,
	driverPod bool,
	testUnknownPod bool,
	testTaintBasedEviction bool,
) {
	pod, err := core.Instance().CreatePod(pod)
	require.NoError(t, err, "failed to create pod")
	require.NotNil(t, pod, "got nil pod back from create pod")

	pod, err = core.Instance().GetPodByName(pod.Name, "")
	require.NoError(t, err, "failed to get pod from fake API")
	require.NotNil(t, pod, "got nil pod back from get pod")

	info, _, err := driver.GetPodVolumes(&pod.Spec, "", false)
	if driverPod {
		require.NoError(t, err, "failed to get pod from fake API")
		require.NotNil(t, info, "got nil pod volumes from driver")
	} else {
		require.Error(t, err, "expected error when getting pod volumes from mock driver")
		require.Nil(t, info, "expected empty pod volumes from driver")
	}

	if testUnknownPod {
		// make pod unknown
		pod.Status = v1.PodStatus{
			Reason: node.NodeUnreachablePodReason,
		}
	}

	if testTaintBasedEviction {
		pod.ObjectMeta.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		node, err := core.Instance().GetNodeByName(nodeForPod)
		require.NoError(t, err, "failed to get node for pod")
		node.Spec.Taints = []v1.Taint{
			{
				Key:    v1.TaintNodeUnreachable,
				Effect: v1.TaintEffectNoExecute,
			},
		}

		_, err = core.Instance().UpdateNode(node)
		require.NoError(t, err, "failed to taint fake node")
	}

	pod, err = core.Instance().UpdatePod(pod)
	require.NoError(t, err, "failed to update pod")
	require.NotNil(t, pod, "got nil pod back from update pod")

	time.Sleep(2 * time.Second)

	if driverPod {
		// pod should be deleted
		_, err = core.Instance().GetPodByName(pod.Name, "")
		require.Error(t, err, "expected error from get pod as pod should be deleted")
	} else {
		// pod should still be present
		pod, err = core.Instance().GetPodByName(pod.Name, "")
		require.NoError(t, err, "failed to get pod")
		require.NotNil(t, pod, "got nil pod back from get pod")

		// cleanup pod
		err = core.Instance().DeletePod(pod.Name, pod.Namespace, false)
		require.NoError(t, err, "failed to delete pod")
	}
}

func newPod(podName string, volumes []string, nodeName string) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName},
	}
	for _, volume := range volumes {
		pvc := driver.NewPVC(volume, "")
		podVolume := v1.Volume{}
		podVolume.PersistentVolumeClaim = &v1.PersistentVolumeClaimVolumeSource{
			ClaimName: pvc.Name,
		}
		pod.Spec.Volumes = append(pod.Spec.Volumes, podVolume)
	}

	pod.Spec.NodeName = nodeName
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

func testOfflineStorageNode(t *testing.T) {

	setupWithNewMockDriver(t)

	defer teardownWithNewMockDriver(t)

	pod := newPod("driverPod", []string{driverVolumeName}, nodeForPod)
	_, err := core.Instance().CreatePod(pod)
	require.NoError(t, err, "failed to create pod")

	noStoragePod := newPod("noStoragePod", nil, nodeForPod)
	_, err = core.Instance().CreatePod(noStoragePod)
	require.NoError(t, err, "failed to create pod")

	driverNodes := getDriverNodes(len(nodes.Items))
	driverNodes[0].Status = volume.NodeOffline
	volumeDriver.EXPECT().GetNodes().Return(driverNodes, nil).AnyTimes()
	volumes := []*volume.Info{
		{
			VolumeName: "volume1",
			DataNodes:  []string{driverNodes[0].StorageID},
		},
	}

	wffcVolumes := make([]*volume.Info, 0)

	volumeDriver.EXPECT().InspectNode("node1").Return(driverNodes[0], nil).AnyTimes()
	volumeDriver.EXPECT().GetCSIPodPrefix().Return("px-csi-ext-", nil).AnyTimes()
	volumeDriver.EXPECT().GetPodVolumes(&pod.Spec, pod.Namespace, false).Return(volumes, wffcVolumes, nil).AnyTimes()
	volumeDriver.EXPECT().GetPodVolumes(&noStoragePod.Spec, noStoragePod.Namespace, false).Return(wffcVolumes, wffcVolumes, nil).AnyTimes()

	testNodeOfflineTimeout = 95 * time.Second
	time.Sleep(testNodeOfflineTimeout)
	_, err = core.Instance().GetPodByName(pod.Name, "")
	require.Error(t, err, "expected error from get pod as pod should be deleted")
	_, err = core.Instance().GetPodByName(noStoragePod.Name, "")
	require.NoError(t, err, "expected no error from get pod as pod should not be deleted")
}

func testOfflineStorageNodesBatchTest(t *testing.T) {

	setupWithNewMockDriverScale(t)

	defer teardownWithNewMockDriver(t)
	numNodes := 50
	offlineNodes := 12
	// The test creates the following pods with 1 pod in each node
	pods := make([]*v1.Pod, 0)
	for i := 0; i < numNodes; i++ {
		pod := newPod(fmt.Sprintf("driverPod%d", i), []string{fmt.Sprintf("%s%d", driverVolumeName, i)}, fmt.Sprintf("node%d.domain", i))
		_, err := core.Instance().CreatePod(pod)
		require.NoError(t, err, "failed to create pod %s for node %d", pod.Name, i)
		pods = append(pods, pod)
	}

	for i := 0; i < numNodes; i++ {
		podList := v1.PodList{}
		podList.Items = append(podList.Items, *pods[i])
		mockCacheInstance.EXPECT().ListTransformedPods(fmt.Sprintf("node%d.domain", i)).Return(&podList, nil).AnyTimes()
	}

	driverNodes := getDriverNodes(len(nodes.Items))
	// Mark offlineNodes number of nodes as offline
	for i := 0; i <= offlineNodes; i++ {
		driverNodes[i].Status = volume.NodeOffline
	}
	volumeDriver.EXPECT().GetNodes().Return(driverNodes, nil).AnyTimes()
	volumeDriver.EXPECT().GetCSIPodPrefix().Return("px-csi-ext-", nil).AnyTimes()

	for i := 0; i < numNodes; i++ {
		volumes := []*volume.Info{
			{
				VolumeName: fmt.Sprintf("volume%d", i),
				DataNodes:  []string{driverNodes[0].StorageID},
			},
		}
		wffcVolumes := make([]*volume.Info, 0)
		volumeDriver.EXPECT().InspectNode(fmt.Sprintf("node%d", i)).Return(driverNodes[i], nil).AnyTimes()
		volumeDriver.EXPECT().GetPodVolumes(&pods[i].Spec, pods[i].Namespace, false).Return(volumes, wffcVolumes, nil).AnyTimes()
	}

	// 30 (interval)  + 60 (backoff) + 5 (buffer)
	time.Sleep(95 * time.Second)
	// Check if only 5 pods have been deleted
	// Not relying on from which nodes pods have been deleted as the nodes may get picked up in random order
	podList, err := core.Instance().ListPods(nil)
	require.NoError(t, err, "failed to get pods")
	require.Equal(t, numNodes-nodeBatchSizeForPodDeletion, len(podList.Items), "only batch number of pods should have been deleted")

	// 60 (backoff) + 5 (buffer)
	time.Sleep(65 * time.Second)
	podList, err = core.Instance().ListPods(nil)
	require.NoError(t, err, "failed to get pods")
	require.Equal(t, numNodes-(2*nodeBatchSizeForPodDeletion), len(podList.Items), " Another batch number of pods should have been deleted")

	// Last iterations
	// 60 (backoff) + 5 (buffer)
	time.Sleep(65 * time.Second)
	podList, err = core.Instance().ListPods(nil)
	require.NoError(t, err, "failed to get pods")
	require.Equal(t, numNodes-offlineNodes, len(podList.Items), "All pods in offlines nodes should have been deleted")
}

func testStorageDownNode(t *testing.T) {
	storageDownPod := newPod("storageDownPod", []string{driverVolumeName}, nodeForPod)
	storageDownPod.Spec.NodeName = "node2.domain"
	node2Index := 1
	_, err := core.Instance().CreatePod(storageDownPod)
	require.NoError(t, err, "failed to create pod")

	err = driver.UpdateNodeStatus(node2Index, volume.NodeStorageDown)
	require.NoError(t, err, "Error setting node status to StorageDown")
	defer func() {
		err = driver.UpdateNodeStatus(node2Index, volume.NodeOnline)
		require.NoError(t, err, "Error setting node status to Online")
	}()

	time.Sleep(testNodeOfflineTimeout)
	_, err = core.Instance().GetPodByName(storageDownPod.Name, "")
	require.NoError(t, err, "expected no error from get pod as pod should not be deleted")
}

func testTempOfflineStorageNode(t *testing.T) {
	pod := newPod("driverPodTemp", []string{driverVolumeName}, nodeForPod)
	_, err := core.Instance().CreatePod(pod)
	require.NoError(t, err, "failed to create pod")

	noStoragePod := newPod("noStoragePodTemp", nil, nodeForPod)
	_, err = core.Instance().CreatePod(noStoragePod)
	require.NoError(t, err, "failed to create pod")

	err = driver.UpdateNodeStatus(0, volume.NodeOffline)
	require.NoError(t, err, "Error setting node status to Offline")

	go func() {
		time.Sleep(30 * time.Second)
		err = driver.UpdateNodeStatus(0, volume.NodeOnline)
		require.NoError(t, err, "Error setting node status to Online")
	}()

	time.Sleep(60 * time.Second)
	_, err = core.Instance().GetPodByName(pod.Name, "")
	require.NoError(t, err, "expected no error from get pod as pod should not be deleted")
	_, err = core.Instance().GetPodByName(noStoragePod.Name, "")
	require.NoError(t, err, "expected no error from get pod as pod should not be deleted")
}

func testOfflineStorageNodeDuplicateIP(t *testing.T) {
	pod := newPod("driverPodDuplicateIPTest", []string{driverVolumeName}, nodeForPod)
	_, err := core.Instance().CreatePod(pod)
	require.NoError(t, err, "failed to create pod")

	time.Sleep(35 * time.Second)
	_, err = core.Instance().GetPodByName(pod.Name, "")
	require.NoError(t, err, "expected no error from get pod as pod should not be deleted")
}

func testOfflineStorageNodeForCSIExtPod(t *testing.T) {
	setupWithNewMockDriver(t)

	defer teardownWithNewMockDriver(t)

	pod := newPod("px-csi-ext-foo", nil, nodeForPod)
	_, err := core.Instance().CreatePod(pod)
	require.NoError(t, err, "failed to create pod")

	driverNodes := getDriverNodes(len(nodes.Items))
	driverNodes[0].Status = volume.NodeOffline
	volumeDriver.EXPECT().GetNodes().Return(driverNodes, nil).AnyTimes()
	volumeDriver.EXPECT().InspectNode("node1").Return(driverNodes[0], nil).AnyTimes()
	volumeDriver.EXPECT().GetCSIPodPrefix().Return("px-csi-ext-", nil).AnyTimes()

	testNodeOfflineTimeout = 95 * time.Second
	time.Sleep(testNodeOfflineTimeout)
	_, err = core.Instance().GetPodByName(pod.Name, "")
	require.Error(t, err, "expected error from get pod as pod should be deleted")
}

func testGetVolumeDriverNodesToK8sNodeMap(t *testing.T) {

	monitor := &Monitor{}

	core.SetInstance(core.New(kubernetes.NewSimpleClientset()))

	// Test when k8s nodes are more than driver nodes
	nodes = &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1.domain", "node1.domain", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2.domain", "node2.domain", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3.domain", "node3.domain", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4.domain", "node4.domain", "192.168.0.4", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5.domain", "node5.domain", "192.168.0.5", "rack3", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node6.domain", "node6.domain", "192.168.0.1", "rack1", "", ""))

	for _, n := range nodes.Items {
		node, err := core.Instance().CreateNode(&n)
		require.NoError(t, err, "failed to create fake node")
		require.NotNil(t, node, "got nil node from create node api")
	}

	driverNodes := []*volume.NodeInfo{
		{
			StorageID:   "node1",
			SchedulerID: "node1.domain",
			Hostname:    "node1.domain",
		},
		{
			StorageID:   "node3",
			SchedulerID: "node3.domain",
			Hostname:    "node3.domain",
		},
		{
			StorageID:   "node2",
			SchedulerID: "node2.domain",
			Hostname:    "node2.domain",
		},
	}

	nodeMap := monitor.getVolumeDriverNodesToK8sNodeMap(driverNodes)
	require.Equal(t, 3, len(nodeMap), "Expected nodeMap length to be 3, got %d", len(nodeMap))

	for _, driverNode := range driverNodes {
		require.Contains(t, nodeMap, driverNode.StorageID, "Expected driver node %s to be present in nodeMap", driverNode.StorageID)
		require.Equal(t, nodeMap[driverNode.StorageID].Name, driverNode.SchedulerID, "Expected driver node %s to be equal to nodeMap entry", driverNode.StorageID)
	}

	// Test when driver nodes are more than k8s nodes
	for _, node := range nodes.Items[3:] {
		err := core.Instance().DeleteNode(node.Name)
		require.NoError(t, err, "failed to delete fake node")
	}
	nodes.Items = nodes.Items[0:3]

	driverNodes = append(driverNodes, &volume.NodeInfo{StorageID: "node7", SchedulerID: "node7.domain", Hostname: "node7.domain"})
	driverNodes = append(driverNodes, &volume.NodeInfo{StorageID: "node8", SchedulerID: "node8.domain", Hostname: "node8.domain"})
	driverNodes = append(driverNodes, &volume.NodeInfo{StorageID: "node9", SchedulerID: "node9.domain", Hostname: "node9.domain"})
	nodeMap = monitor.getVolumeDriverNodesToK8sNodeMap(driverNodes)

	require.Equal(t, 3, len(nodeMap), "Expected nodeMap length to be 3, got %d", len(nodeMap))
	for _, driverNode := range driverNodes[0:3] {
		require.Contains(t, nodeMap, driverNode.StorageID, "Expected driver node %s to be present in nodeMap", driverNode.StorageID)
		require.Equal(t, nodeMap[driverNode.StorageID].Name, driverNode.SchedulerID, "Expected driver node %s to be equal to nodeMap entry", driverNode.StorageID)
	}
	for _, drdriverNode := range driverNodes[3:] {
		require.NotContains(t, nodeMap, drdriverNode.StorageID, "Expected driver node %s to not be present in nodeMap", drdriverNode.StorageID)
	}
}

func testBatchDeletePodsFromOfflineNodes(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	scheme := runtime.NewScheme()
	err := stork_api.AddToScheme(scheme)
	require.NoError(t, err, "Error adding stork scheme")

	fakeKubeClient := kubernetes.NewSimpleClientset()
	core.SetInstance(core.New(fakeKubeClient))

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockVolumeDriver := mockVolumeDriver.NewMockDriver(mockCtrl)
	monitor := &Monitor{
		Driver: mockVolumeDriver,
	}

	storkdriver, err := volume.Get(mockDriverName)
	require.NoError(t, err, "Error getting mock volume driver")

	var ok bool
	driver, ok = storkdriver.(*mock.Driver)
	require.True(t, ok, "Error casting mockdriver")

	err = storkdriver.Init(nil)
	require.NoError(t, err, "Error initializing mock volume driver")

	nodes = &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode("node1", "node1.domain", "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2", "node2.domain", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3", "node3.domain", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4", "node4.domain", "192.168.0.4", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5", "node5.domain", "192.168.0.5", "rack3", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node6", "node6.domain", "192.168.0.1", "rack1", "", ""))

	for _, n := range nodes.Items {
		node, err := core.Instance().CreateNode(&n)
		require.NoError(t, err, "failed to create fake node")
		require.NotNil(t, node, "got nil node from create node api")
	}

	// Test the batchDeletePodsFromOfflineNodes function for different number of pods per node
	node := &volume.NodeInfo{
		Hostname:    "node1.domain",
		SchedulerID: "node1",
		StorageID:   "storage1",
		Status:      volume.NodeOffline,
		RawStatus:   "offline",
	}
	for _, podsPerNode := range []int{podDeleteBatchSize - 1, podDeleteBatchSize, podDeleteBatchSize + 1, 2 * podDeleteBatchSize} {
		for i := 1; i <= podsPerNode; i++ {
			namespace := v1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("ns-%d", i),
				},
			}
			_, err := core.Instance().CreateNamespace(&namespace)
			require.NoError(t, err, "failed to create namespace %s", namespace.Name)
		}

		nodeToPodsMap := createPodsOnNodes(t, []string{"node1", "node2", "node3", "node4", "node5", "node6"}, podsPerNode)

		// Create a fake recorder
		eventBroadcaster := record.NewBroadcaster()
		eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: corev1.New(fakeKubeClient.CoreV1().RESTClient()).Events("")})
		recorder := eventBroadcaster.NewRecorder(legacyscheme.Scheme, v1.EventSource{Component: "storktest"})
		monitor.Recorder = recorder

		mockVolumeDriver.EXPECT().InspectNode("storage1").Return(node, nil).Times(int(math.Ceil(float64(podsPerNode) / float64(podDeleteBatchSize))))

		// Call the batchDeletePodsFromOfflineNodes function
		monitor.batchDeletePodsFromOfflineNodes(nodeToPodsMap["node1"], node, false)

		// Check that all the pods running in node1 which is the offline node have been deleted
		// Since FakeClient does not support FieldSelector (it's a limitation of fake client), better to get all pods
		// and check no pod is running on the offline node
		curPods, err := core.Instance().GetPods("", nil)
		require.NoError(t, err, "failed to get pods from fake API")
		require.Equal(t, podsPerNode*(len(nodes.Items)-1), len(curPods.Items), "expected no pods in offline node")
		for _, pod := range curPods.Items {
			require.NotEqual(t, "node1", pod.Spec.NodeName, "expected no pod to be running on offline node")
		}

		err = core.Instance().DeletePods(curPods.Items, true)
		require.NoError(t, err, "failed to delete pods")
		for i := 1; i <= podsPerNode; i++ {
			ns := fmt.Sprintf("ns-%d", i)
			err := core.Instance().DeleteNamespace(ns)
			require.NoError(t, err, "failed to delete namespace %s", ns)
		}

	}

	// Test the batchDeletePodsFromOfflineNodes function when node status is not offline anymore
	node = &volume.NodeInfo{
		Hostname:    "node1.domain",
		SchedulerID: "node1",
		StorageID:   "storage1",
		Status:      volume.NodeOnline,
		RawStatus:   "online",
	}
	podsPerNode := 7
	for i := 1; i <= podsPerNode; i++ {
		namespace := v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("ns-%d", i),
			},
		}
		_, err := core.Instance().CreateNamespace(&namespace)
		require.NoError(t, err, "failed to create namespace %s", namespace.Name)
	}

	nodeToPodsMap := createPodsOnNodes(t, []string{"node1", "node2", "node3", "node4", "node5", "node6"}, podsPerNode)
	// Create a fake recorder
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: corev1.New(fakeKubeClient.CoreV1().RESTClient()).Events("")})
	recorder := eventBroadcaster.NewRecorder(legacyscheme.Scheme, v1.EventSource{Component: "storktest"})
	monitor.Recorder = recorder

	//mockVolumeDriver.EXPECT().InspectNode("storage1").Return(node, nil).Times(int(math.Ceil(float64(podsPerNode)/float64(podDeleteBatchSize))))
	mockVolumeDriver.EXPECT().InspectNode("storage1").Return(node, nil).Times(1)

	// Call the batchDeletePodsFromOfflineNodes function
	monitor.batchDeletePodsFromOfflineNodes(nodeToPodsMap["node1"], node, false)

	// Check that all the pods running in node1 which is the offline node have been deleted
	// Since FakeClient does not support FieldSelector (it's a limitation of fake client), better to get all pods
	// and check no pod is running on the offline node
	curPods, err := core.Instance().GetPods("", nil)
	require.NoError(t, err, "failed to get pods from fake API")
	require.Equal(t, podsPerNode*(len(nodes.Items)), len(curPods.Items), "expected no pods to have been deleted")
	podsInConcerenedNode := 0
	for _, pod := range curPods.Items {
		if pod.Spec.NodeName == "node1" {
			podsInConcerenedNode++
		}
	}
	require.Equal(t, podsPerNode, podsInConcerenedNode, "expected no pod to have been deleted from node1")

	err = core.Instance().DeletePods(curPods.Items, true)
	require.NoError(t, err, "failed to delete pods")
	for i := 1; i <= podsPerNode; i++ {
		ns := fmt.Sprintf("ns-%d", i)
		err := core.Instance().DeleteNamespace(ns)
		require.NoError(t, err, "failed to delete namespace %s", ns)
	}
}

func createPodsOnNodes(t *testing.T, nodes []string, podCountPerNode int) map[string][]v1.Pod {
	pods := make(map[string][]v1.Pod)
	for _, nodeName := range nodes {
		for i := 1; i <= podCountPerNode; i++ {
			pod := v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("pod-%s-%d", nodeName, i),
					Namespace: fmt.Sprintf("ns-%d", i),
				},
				Spec: v1.PodSpec{
					NodeName: nodeName,
				},
			}
			_, err := core.Instance().CreatePod(&pod)
			if !k8serror.IsAlreadyExists(err) {
				require.NoError(t, err, "failed to create pod %s", pod.Name)
			}
			pods[nodeName] = append(pods[nodeName], pod)
		}
	}
	return pods
}

func getDriverNodes(numNodes int) []*volume.NodeInfo {
	driverNodes := make([]*volume.NodeInfo, 0)
	for i := 0; i < numNodes; i++ {
		driverNodes = append(driverNodes, &volume.NodeInfo{
			Hostname:    "node" + strconv.Itoa(i+1),
			SchedulerID: "node" + strconv.Itoa(i+1),
			StorageID:   "node" + strconv.Itoa(i+1),
			Status:      volume.NodeOnline,
			RawStatus:   "Ready",
		})
	}
	return driverNodes
}
