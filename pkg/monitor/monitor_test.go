// +build unittest

package monitor

import (
	"testing"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/drivers/volume/mock"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	fakeclient "github.com/libopenstorage/stork/pkg/client/clientset/versioned/fake"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetes "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest/fake"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/pkg/util/node"
)

const (
	mockDriverName   = "MockDriver"
	driverVolumeName = "singleVolume"
	nodeForPod       = "node1.domain"
)

var (
	fakeStorkClient *fakeclient.Clientset
	fakeRestClient  *fake.RESTClient
	driver          *mock.Driver
	monitor         *Monitor
)

func init() {
	resetTest()
}

func resetTest() {
	scheme := runtime.NewScheme()
	stork_api.AddToScheme(scheme)
	fakeStorkClient = fakeclient.NewSimpleClientset()
	fakeKubeClient := kubernetes.NewSimpleClientset()

	k8s.Instance().SetClient(fakeKubeClient, nil, fakeStorkClient, nil, nil, nil)
}

func TestMonitor(t *testing.T) {
	t.Run("setup", setup)
	t.Run("testUnknownDriverPod", testUnknownDriverPod)
	t.Run("testUnknownOtherDriverPod", testUnknownOtherDriverPod)
	t.Run("testEvictedDriverPod", testEvictedDriverPod)
	t.Run("testEvictedOtherDriverPod", testEvictedOtherDriverPod)
}

func setup(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)

	storkdriver, err := volume.Get(mockDriverName)
	require.NoError(t, err, "Error getting mock volume driver")

	var ok bool
	driver, ok = storkdriver.(*mock.Driver)
	require.True(t, ok, "Error casting mockdriver")

	err = storkdriver.Init(nil)
	require.NoError(t, err, "Error initializing mock volume driver")

	monitor = &Monitor{
		Driver: storkdriver,
	}

	err = monitor.Start()
	require.NoError(t, err, "failed to start monitor")

	nodes := &v1.NodeList{}
	nodes.Items = append(nodes.Items, *newNode(nodeForPod, nodeForPod, "192.168.0.1", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node2.domain", "node2.domain", "192.168.0.2", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node3.domain", "node3.domain", "192.168.0.3", "rack1", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node4.domain", "node4.domain", "192.168.0.4", "rack2", "", ""))
	nodes.Items = append(nodes.Items, *newNode("node5.domain", "node5.domain", "192.168.0.5", "rack3", "", ""))

	for _, n := range nodes.Items {
		node, err := k8s.Instance().CreateNode(&n)
		require.NoError(t, err, "failed to create fake node")
		require.NotNil(t, node, "got nil node from create node api")
	}

	provNodes := []int{0, 1}
	err = driver.CreateCluster(5, nodes)
	require.NoError(t, err, "Error creating cluster")

	err = driver.ProvisionVolume(driverVolumeName, provNodes, 1)
	require.NoError(t, err, "Error provisioning volume")
}

func testUnknownDriverPod(t *testing.T) {
	pod := newPod("driverPod", []string{driverVolumeName})
	testLostPod(t, pod, true, true, false)
}

func testUnknownOtherDriverPod(t *testing.T) {
	pod := newPod("otherDriverPod", nil)
	podVolume := v1.Volume{}
	podVolume.PersistentVolumeClaim = &v1.PersistentVolumeClaimVolumeSource{
		ClaimName: "noDriverPVC",
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, podVolume)

	testLostPod(t, pod, false, true, false)
}

func testEvictedOtherDriverPod(t *testing.T) {
	pod := newPod("otherDriverPod", nil)
	podVolume := v1.Volume{}
	podVolume.PersistentVolumeClaim = &v1.PersistentVolumeClaimVolumeSource{
		ClaimName: "noDriverPVC",
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, podVolume)

	testLostPod(t, pod, false, false, true)
}

func testEvictedDriverPod(t *testing.T) {
	pod := newPod("driverPod", []string{driverVolumeName})
	testLostPod(t, pod, true, false, true)
}

func testLostPod(
	t *testing.T,
	pod *v1.Pod,
	driverPod bool,
	testUnknownPod bool,
	testTaintBasedEviction bool,
) {
	pod, err := k8s.Instance().CreatePod(pod)
	require.NoError(t, err, "failed to create pod")
	require.NotNil(t, pod, "got nil pod back from create pod")

	pod, err = k8s.Instance().GetPodByName(pod.Name, "")
	require.NoError(t, err, "failed to get pod from fake API")
	require.NotNil(t, pod, "got nil pod back from get pod")

	info, err := driver.GetPodVolumes(&pod.Spec, "")
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
		node, err := k8s.Instance().GetNodeByName(nodeForPod)
		require.NoError(t, err, "failed to get node for pod")
		node.Spec.Taints = []v1.Taint{
			{
				Key:    algorithm.TaintNodeUnreachable,
				Effect: v1.TaintEffectNoExecute,
			},
		}

		_, err = k8s.Instance().UpdateNode(node)
		require.NoError(t, err, "failed to taint fake node")
	}

	pod, err = k8s.Instance().UpdatePod(pod)
	require.NoError(t, err, "failed to update pod")
	require.NotNil(t, pod, "got nil pod back from update pod")

	time.Sleep(2 * time.Second)

	if driverPod {
		// pod should be deleted
		pod, err = k8s.Instance().GetPodByName(pod.Name, "")
		require.Error(t, err, "expected error from get pod as pod should be deleted")
	} else {
		// pod should still be present
		pod, err = k8s.Instance().GetPodByName(pod.Name, "")
		require.NoError(t, err, "failed to get pod")
		require.NotNil(t, pod, "got nil pod back from get pod")

		// cleanup pod
		err = k8s.Instance().DeletePod(pod.Name, pod.Namespace, false)
		require.NoError(t, err, "failed to delete pod")
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

	pod.Spec.NodeName = nodeForPod
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
