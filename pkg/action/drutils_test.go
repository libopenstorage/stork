package action

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/libopenstorage/openstorage/api"
	mockVolumeDriver "github.com/libopenstorage/stork/drivers/volume/mock/volume"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	fakeclient "github.com/libopenstorage/stork/pkg/client/clientset/versioned/fake"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetes "k8s.io/client-go/kubernetes/fake"
)

var fakeStorkClient *fakeclient.Clientset

func TestGetRemoteNodeName(t *testing.T) {
	// Setup
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockVolumeDriver := mockVolumeDriver.NewMockDriver(mockCtrl)

	ac := &ActionController{
		volDriver: mockVolumeDriver,
	}

	action := &storkv1.Action{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-action",
			Namespace: "default",
		},
	}

	// Test getRemoteNodeName with source domain before witness domain
	clusterDomains := &storkv1.ClusterDomains{
		LocalDomain: "dest",
		ClusterDomainInfos: []storkv1.ClusterDomainInfo{
			{
				Name:       "dest",
				State:      storkv1.ClusterDomainActive,
				SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
			},
			{
				Name:       "source",
				State:      storkv1.ClusterDomainActive,
				SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
			},
			{
				Name:       "witness",
				State:      storkv1.ClusterDomainActive,
				SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
			},
		},
	}

	domainToNodesMap := map[string][]*api.Node{
		"source": {
			&api.Node{
				Id:       "node1",
				DomainID: "source",
			},
			&api.Node{
				Id:       "node2",
				DomainID: "source",
			},
			&api.Node{
				Id:       "node3",
				DomainID: "source",
			},
		},
		"dest": {
			&api.Node{
				Id:       "node4",
				DomainID: "dest",
			},
			&api.Node{
				Id:       "node5",
				DomainID: "dest",
			},
			&api.Node{
				Id:       "node6",
				DomainID: "dest",
			},
		},
		"witness": {
			&api.Node{
				Id:       "node7",
				DomainID: "witness",
			},
		},
	}

	remoteNode := ac.getRemoteNodeName(action, clusterDomains, domainToNodesMap)
	expectedRemoteNode := "source"
	if remoteNode != expectedRemoteNode {
		t.Errorf("Expected remote node to be %s, but got %s", expectedRemoteNode, remoteNode)
	}

	// Test getRemoteNodeName with witness domain before source domain
	clusterDomains = &storkv1.ClusterDomains{
		LocalDomain: "dest",
		ClusterDomainInfos: []storkv1.ClusterDomainInfo{
			{
				Name:       "witness",
				State:      storkv1.ClusterDomainActive,
				SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
			},
			{
				Name:       "source",
				State:      storkv1.ClusterDomainActive,
				SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
			},
			{
				Name:       "dest",
				State:      storkv1.ClusterDomainActive,
				SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
			},
		},
	}

	// Test getRemoteNodeName with no witness domain
	clusterDomains = &storkv1.ClusterDomains{
		LocalDomain: "dest",
		ClusterDomainInfos: []storkv1.ClusterDomainInfo{
			{
				Name:       "dest",
				State:      storkv1.ClusterDomainActive,
				SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
			},
			{
				Name:       "source",
				State:      storkv1.ClusterDomainActive,
				SyncStatus: storkv1.ClusterDomainSyncStatusInSync,
			},
		},
	}

	delete(domainToNodesMap, "witness")
	remoteNode = ac.getRemoteNodeName(action, clusterDomains, domainToNodesMap)
	expectedRemoteNode = "source"
	if remoteNode != expectedRemoteNode {
		t.Errorf("Expected remote node to be %s, but got %s", expectedRemoteNode, remoteNode)
	}

	// Test getRemoteNodeName with no domain
	clusterDomains = &storkv1.ClusterDomains{}
	domainToNodesMap = map[string][]*api.Node{}

	remoteNode = ac.getRemoteNodeName(action, clusterDomains, domainToNodesMap)
	expectedRemoteNode = ""
	if remoteNode != expectedRemoteNode {
		t.Errorf("Expected remote node to be %s, but got %s", expectedRemoteNode, remoteNode)
	}
}

/*
TestCheckForPodStatusUsingPVC tests the checkForPodStatusUsingPVC function that
returns the following three values:
- terminated: true if all pods using PVC are terminated
- terminationInProgress: true if any pod using PVC is not terminated
- error: error if any error occurs

Cases:
1. All pods terminated.
2. Some pods terminated, some not.
3. All pods terminated, one in completed state.
*/
func TestCheckForPodStatusUsingPVC(t *testing.T) {
	scheme := runtime.NewScheme()
	err := storkv1.AddToScheme(scheme)
	require.NoError(t, err, "Error adding stork scheme")
	fakeStorkClient = fakeclient.NewSimpleClientset()
	fakeKubeClient := kubernetes.NewSimpleClientset()
	fakeClient := core.New(fakeKubeClient)
	core.SetInstance(fakeClient)

	// Create some pods that are using a PVC with different status.
	completedPod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "dummy-container",
					VolumeMounts: []corev1.VolumeMount{
						{
							Name: "data-volume",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "data-volume",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "test-pvc",
							ReadOnly:  false,
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
		},
	}

	// Create the pod.
	_, err = core.Instance().CreatePod(&completedPod)
	log.FailOnError(t, err, "failed to create test pod")
	pvcMap := make(map[string]map[string]int)
	pvcMap["default"] = make(map[string]int)
	pvcMap["default"]["test-pvc"] = 1

	isTerminated, isTerminationInProgress, err := checkForPodStatusUsingPVC(pvcMap, fakeClient)
	assert.NoError(t, err, "failed to check status of pods using PVC")
	assert.Equal(t, true, isTerminated, "pods should show as terminated")
	assert.Equal(t, true, isTerminationInProgress, "pods should show as termination in progress")

	// Create a pod in Running state which should not be ignored while checking
	// the pods not terminated.
	runningPod := completedPod.DeepCopy()
	runningPod.Name = "test-pod1"
	runningPod.Status.Phase = corev1.PodRunning
	_, err = core.Instance().CreatePod(runningPod)
	log.FailOnError(t, err, "failed to create test pod")
	pvcMap["default"]["test-pvc"] = 2

	isTerminated, isTerminationInProgress, err = checkForPodStatusUsingPVC(pvcMap, fakeClient)
	assert.NoError(t, err, "failed to check status of pods using PVC")
	assert.Equal(t, false, isTerminated, "all pods should NOT show as terminated")
	assert.Equal(t, false, isTerminationInProgress, "no pods should show as termination in progress")

	// Change the status of running pod to completed. Now it should again return true for terminated.
	runningPod.Status.Phase = corev1.PodSucceeded
	_, err = core.Instance().UpdatePod(runningPod)
	log.FailOnError(t, err, "failed to create test pod")

	isTerminated, isTerminationInProgress, err = checkForPodStatusUsingPVC(pvcMap, fakeClient)
	assert.NoError(t, err, "failed to check status of pods using PVC")
	assert.Equal(t, true, isTerminated, "pods should show as terminated")
	assert.Equal(t, true, isTerminationInProgress, "pods should show as termination in progress")
}
