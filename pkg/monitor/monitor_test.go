//go:build unittest
// +build unittest

package monitor

import (
	"testing"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/drivers/volume/mock"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	fakeclient "github.com/libopenstorage/stork/pkg/client/clientset/versioned/fake"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
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
)

func TestMonitor(t *testing.T) {
	t.Run("setup", setup)
	t.Run("testUnknownDriverPod", testUnknownDriverPod)
	t.Run("testUnknownOtherDriverPod", testUnknownOtherDriverPod)
	t.Run("testEvictedDriverPod", testEvictedDriverPod)
	t.Run("testEvictedOtherDriverPod", testEvictedOtherDriverPod)
	t.Run("testTempOfflineStorageNode", testTempOfflineStorageNode)
	t.Run("testOfflineStorageNode", testOfflineStorageNode)
	t.Run("testOfflineStorageNodeDuplicateIP", testOfflineStorageNodeDuplicateIP)
	t.Run("testVolumeAttachmentCleanup", testVolumeAttachmentCleanup)
	t.Run("testOfflineStorageNodeForCSIExtPod", testOfflineStorageNodeForCSIExtPod)
	t.Run("testStorageDownNode", testStorageDownNode)
	t.Run("teardown", teardown)
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

	err = driver.ProvisionVolume(driverVolumeName, provNodes, 1, nil, false)
	require.NoError(t, err, "Error provisioning volume")

	err = driver.ProvisionVolume(attachmentVolumeName, provNodes, 2, nil, false)
	require.NoError(t, err, "Error provisioning volume")

	err = driver.ProvisionVolume(unknownPodsVolumeName, provNodes, 3, nil, false)
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

func testOfflineStorageNode(t *testing.T) {
	pod := newPod("driverPod", []string{driverVolumeName})
	_, err := core.Instance().CreatePod(pod)
	require.NoError(t, err, "failed to create pod")

	noStoragePod := newPod("noStoragePod", nil)
	_, err = core.Instance().CreatePod(noStoragePod)
	require.NoError(t, err, "failed to create pod")

	err = driver.UpdateNodeStatus(0, volume.NodeOffline)
	require.NoError(t, err, "Error setting node status to Offline")
	defer func() {
		err = driver.UpdateNodeStatus(0, volume.NodeOnline)
		require.NoError(t, err, "Error setting node status to Online")
	}()

	time.Sleep(testNodeOfflineTimeout)
	_, err = core.Instance().GetPodByName(pod.Name, "")
	require.Error(t, err, "expected error from get pod as pod should be deleted")
	_, err = core.Instance().GetPodByName(noStoragePod.Name, "")
	require.NoError(t, err, "expected no error from get pod as pod should not be deleted")
}

func testStorageDownNode(t *testing.T) {
	storageDownPod := newPod("storageDownPod", []string{driverVolumeName})
	storageDownPod.Spec.NodeName = "node2.domain"
	node2Index := 1
	_, err := core.Instance().CreatePod(storageDownPod)
	require.NoError(t, err, "failed to create pod")

	err = driver.UpdateNodeStatus(node2Index, volume.NodeStorageDown)
	require.NoError(t, err, "Error setting node status to Degraded")
	defer func() {
		err = driver.UpdateNodeStatus(node2Index, volume.NodeOnline)
		require.NoError(t, err, "Error setting node status to Online")
	}()

	time.Sleep(testNodeOfflineTimeout)
	_, err = core.Instance().GetPodByName(storageDownPod.Name, "")
	require.NoError(t, err, "expected no error from get pod as pod should not be deleted")
}

func testTempOfflineStorageNode(t *testing.T) {
	pod := newPod("driverPodTemp", []string{driverVolumeName})
	_, err := core.Instance().CreatePod(pod)
	require.NoError(t, err, "failed to create pod")

	noStoragePod := newPod("noStoragePodTemp", nil)
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
	pod := newPod("driverPodDuplicateIPTest", []string{driverVolumeName})
	_, err := core.Instance().CreatePod(pod)
	require.NoError(t, err, "failed to create pod")

	time.Sleep(35 * time.Second)
	_, err = core.Instance().GetPodByName(pod.Name, "")
	require.NoError(t, err, "expected no error from get pod as pod should not be deleted")
}

func testVolumeAttachmentCleanup(t *testing.T) {
	onlineNodeID := 1
	offlineNodeID := 2
	podsUnknownNodeID := 3
	nodeToKeepOnline := nodes.Items[onlineNodeID].Name
	nodeToTakeOffline := nodes.Items[offlineNodeID].Name
	nodeToPutUnknownPodsOn := nodes.Items[podsUnknownNodeID].Name

	// Create PVC and PV for all test volumes.
	for _, volumeName := range []string{driverVolumeName, attachmentVolumeName, unknownPodsVolumeName} {
		_, err := core.Instance().CreatePersistentVolumeClaim(&v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      volumeName,
				Namespace: defaultNamespace,
			},
		})
		require.NoError(t, err, "failed to create pv for %s", volumeName)
		_, err = core.Instance().CreatePersistentVolume(&v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:      volumeName,
				Namespace: "",
			},
			Spec: v1.PersistentVolumeSpec{
				ClaimRef: &v1.ObjectReference{
					Name:      volumeName,
					Namespace: defaultNamespace,
				},
			},
		})
		require.NoError(t, err, "failed to create pvc for %s", volumeName)
	}

	// Create multiple pods on different nodes, some with volumeattachments, some without.
	// Stop the driver on the node with the attachment and make sure only that pod and volumeattachment are deleted.

	// Create two pods on node N1 that will remain healthy. One attached, one not.
	healthyPodAttached := newPod("testVolumeAttachmentCleanupHealtyAttached", []string{driverVolumeName})
	healthyPodAttached.Spec.NodeName = nodeToKeepOnline
	_, err := core.Instance().CreatePod(healthyPodAttached)
	require.NoError(t, err, "failed to create healthy attached pod")
	_, err = storage.Instance().CreateVolumeAttachment(&storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-healthy",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			NodeName: nodeToKeepOnline,
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &driverVolumeName,
			},
		},
	})
	require.NoError(t, err, "failed to create healthy pod volume attachment")

	healthyPodDetached := newPod("testVolumeAttachmentCleanupHealthyDetached", []string{driverVolumeName})
	healthyPodDetached.Spec.NodeName = nodeToKeepOnline
	_, err = core.Instance().CreatePod(healthyPodDetached)
	require.NoError(t, err, "failed to create healthy detached pod")

	// Create two pods on node N2 that will be taken offline temporarily. One attached, one not.
	unhealthyPodAttached := newPod("testVolumeAttachmentCleanupUnheathyAttached", []string{attachmentVolumeName})
	unhealthyPodAttached.Spec.NodeName = nodeToTakeOffline
	_, err = core.Instance().CreatePod(unhealthyPodAttached)
	require.NoError(t, err, "failed to create pod")
	_, err = storage.Instance().CreateVolumeAttachment(&storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-unhealthy",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			NodeName: nodeToTakeOffline,
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &attachmentVolumeName,
			},
		},
	})
	require.NoError(t, err, "failed to create unhealthy pod volume attachment")

	unhealthyPodDetached := newPod("testVolumeAttachmentCleanupUnheathyDetached", []string{attachmentVolumeName})
	unhealthyPodDetached.Spec.NodeName = nodeToTakeOffline
	_, err = core.Instance().CreatePod(unhealthyPodDetached)
	require.NoError(t, err, "failed to create pod")

	// Create two pods on node N3 that will have unknown state. One attached, one not.
	unknownPodAttached := newPod("testVolumeAttachmentCleanupUnknownPodAttached", []string{unknownPodsVolumeName})
	unknownPodAttached.Spec.NodeName = nodeToPutUnknownPodsOn
	unknownPodAttached.Status = v1.PodStatus{
		Reason: node.NodeUnreachablePodReason,
	}
	_, err = core.Instance().CreatePod(unknownPodAttached)
	require.NoError(t, err, "failed to create pod")
	_, err = storage.Instance().CreateVolumeAttachment(&storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-unknown",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			NodeName: nodeToPutUnknownPodsOn,
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &unknownPodsVolumeName,
			},
		},
	})
	require.NoError(t, err, "failed to create unknown pod volume attachment")

	unknownPodDetached := newPod("testVolumeAttachmentCleanupUnknownPodDetached", []string{unknownPodsVolumeName})
	unknownPodDetached.Spec.NodeName = nodeToPutUnknownPodsOn
	unknownPodDetached.Status = v1.PodStatus{
		Reason: node.NodeUnreachablePodReason,
	}
	_, err = core.Instance().CreatePod(unknownPodDetached)
	require.NoError(t, err, "failed to create unknown detached pod")

	// Kill N2
	err = driver.UpdateNodeStatus(offlineNodeID, volume.NodeOffline)
	require.NoError(t, err, "Error setting node status to Offline")
	defer func() {
		err = driver.UpdateNodeStatus(offlineNodeID, volume.NodeOnline)
		require.NoError(t, err, "Error setting node status to Online")
	}()

	// VolumeAttachments (VA) for N2 and N3 should be deleted, but VA for N1 should remain.
	time.Sleep(testNodeOfflineTimeout)

	vaList, err := storage.Instance().ListVolumeAttachments()
	require.NoError(t, err, "expected no error from list vol attachments")

	// There should be exactly one attachment left - the healthy one.
	require.Equal(t, 1, len(vaList.Items))
	require.Equal(t, "va-healthy", vaList.Items[0].Name)

	// Healthy pods should remain
	_, err = core.Instance().GetPodByName(healthyPodAttached.Name, "")
	require.NoError(t, err, "expected no error from get pod as pod should not be deleted")

	_, err = core.Instance().GetPodByName(healthyPodDetached.Name, "")
	require.NoError(t, err, "expected no error from get pod as pod should not be deleted")

	// Unhealthy pods should be deleted.
	_, err = core.Instance().GetPodByName(unhealthyPodAttached.Name, "")
	require.Error(t, err, "expected error from get pod as pod should be deleted")

	_, err = core.Instance().GetPodByName(unhealthyPodDetached.Name, "")
	require.Error(t, err, "expected error from get pod as pod should be deleted")

	// Unknown pods should be deleted.
	_, err = core.Instance().GetPodByName(unknownPodAttached.Name, "")
	require.Error(t, err, "expected error from get pod as pod should be deleted")

	_, err = core.Instance().GetPodByName(unknownPodDetached.Name, "")
	require.Error(t, err, "expected error from get pod as pod should be deleted")

	// total pods rescheduled during UT's
	require.Equal(t, testutil.ToFloat64(HealthCounter), float64(8), "pods_reschduled_total not matched")
}

func testOfflineStorageNodeForCSIExtPod(t *testing.T) {
	pod := newPod("px-csi-ext-foo", nil)
	_, err := core.Instance().CreatePod(pod)
	require.NoError(t, err, "failed to create pod")

	err = driver.UpdateNodeStatus(0, volume.NodeOffline)
	require.NoError(t, err, "Error setting node status to Offline")
	defer func() {
		err = driver.UpdateNodeStatus(0, volume.NodeOnline)
		require.NoError(t, err, "Error setting node status to Online")
	}()

	time.Sleep(testNodeOfflineTimeout)
	_, err = core.Instance().GetPodByName(pod.Name, "")
	require.Error(t, err, "expected error from get pod as pod should be deleted")
}
