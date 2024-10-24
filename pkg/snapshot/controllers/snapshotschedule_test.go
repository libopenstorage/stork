package controllers

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	fakeclient "github.com/libopenstorage/stork/pkg/client/clientset/versioned/fake"
	k8sextops "github.com/libopenstorage/stork/pkg/crud/externalstorage"
	storkops "github.com/libopenstorage/stork/pkg/crud/stork"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/resource"
	kubernetes "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest/fake"
)

// TestCleanupErroredSnapshots tests the `cleanupErroredSnapshots` function that
// cleans up snapshots in error state and older than a certain cutoff time period.
func TestCleanupErroredSnapshots(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockSnap := NewMockOps(ctrl)
	fakeStorkClient := fakeclient.NewSimpleClientset()
	var unstructuredSerializer = resource.UnstructuredPlusDefaultContentConfig().NegotiatedSerializer
	fakeRestClient := &fake.RESTClient{
		NegotiatedSerializer: unstructuredSerializer,
	}
	fakeKubeClient := kubernetes.NewSimpleClientset()
	core.SetInstance(core.New(fakeKubeClient))
	storkops.SetInstance(storkops.New(fakeKubeClient, fakeStorkClient, fakeRestClient))

	// Creating a dummy snapshotschedule object.
	creationTime1 := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	creationTime2 := metav1.NewTime(time.Now().Add(-7 * time.Minute))
	creationTime3 := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	creationTime4 := metav1.NewTime(time.Now().Add(-90 * time.Second))
	creationTime5 := metav1.NewTime(time.Now().Add(-45 * time.Second))
	creationTime6 := metav1.NewTime(time.Now().Add(-30 * time.Second))
	creationTime7 := metav1.NewTime(time.Now().Add(-3 * time.Minute))
	snapshotSchedule := &stork_api.VolumeSnapshotSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-schedule",
			Namespace: "test-ns",
		},
		Status: stork_api.VolumeSnapshotScheduleStatus{
			Items: map[stork_api.SchedulePolicyType][]*stork_api.ScheduledVolumeSnapshotStatus{
				"policy1": {
					{
						Name:              "snapshot1",
						Status:            snapv1.VolumeSnapshotConditionError,
						CreationTimestamp: creationTime1,
					},
					{
						Name:              "snapshot2",
						Status:            snapv1.VolumeSnapshotConditionReady,
						CreationTimestamp: creationTime2,
					},
					{
						Name:              "snapshot3",
						Status:            snapv1.VolumeSnapshotConditionError,
						CreationTimestamp: creationTime3,
					},
					{
						Name:              "snapshot4",
						Status:            snapv1.VolumeSnapshotConditionError,
						CreationTimestamp: creationTime4,
					},
					{
						Name:              "snapshot5",
						Status:            snapv1.VolumeSnapshotConditionPending,
						CreationTimestamp: creationTime5,
					},
					{
						Name:              "snapshot6",
						Status:            snapv1.VolumeSnapshotConditionError,
						CreationTimestamp: creationTime6,
					},
					{
						Name:              "snapshot7",
						Status:            snapv1.VolumeSnapshotConditionError,
						CreationTimestamp: creationTime7,
						Deleted:           true,
						Message:           "Node on which volume is attached is not online",
					},
				},
			},
		},
	}

	// Apply the snapshotschedule.
	snapshotSchedule, err := storkops.Instance().CreateSnapshotSchedule(snapshotSchedule)
	assert.NoError(t, err)

	// Mock and assert the snapshot and snapshotschedule calls.
	mockSnap.EXPECT().GetSnapshot(gomock.Eq("snapshot1"), gomock.Eq("test-ns")).Return(
		getSnapshotObject("snapshot1", "test-ns", creationTime1, snapv1.VolumeSnapshotConditionError, "Node on which volume is attached is not online"), nil).AnyTimes()
	mockSnap.EXPECT().GetSnapshot(gomock.Eq("snapshot3"), gomock.Eq("test-ns")).Return(
		getSnapshotObject("snapshot3", "test-ns", creationTime3, snapv1.VolumeSnapshotConditionError, "Couldn't find the pvc to snapshot"), nil).AnyTimes()
	mockSnap.EXPECT().GetSnapshot(gomock.Eq("snapshot4"), gomock.Eq("test-ns")).Return(
		getSnapshotObject("snapshot4", "test-ns", creationTime4, snapv1.VolumeSnapshotConditionError, "Network failure"), nil).AnyTimes()
	mockSnap.EXPECT().DeleteSnapshot(gomock.Eq("snapshot1"), gomock.Eq("test-ns")).Return(nil).AnyTimes()
	mockSnap.EXPECT().DeleteSnapshot(gomock.Eq("snapshot3"), gomock.Eq("test-ns")).Return(nil).AnyTimes()
	mockSnap.EXPECT().DeleteSnapshot(gomock.Eq("snapshot4"), gomock.Eq("test-ns")).Return(nil).AnyTimes()

	k8sextops.SetInstance(mockSnap)
	s := SnapshotScheduleController{}
	err = s.cleanupErroredSnapshots(snapshotSchedule)
	assert.NoError(t, err)
	schedule, err := storkops.Instance().GetSnapshotSchedule(snapshotSchedule.Name, snapshotSchedule.Namespace)
	assert.NoError(t, err)
	assert.Equal(t, schedule.Status.Items["policy1"][0].Deleted, true)
	assert.Equal(t, schedule.Status.Items["policy1"][0].Message, "Node on which volume is attached is not online")
	assert.Equal(t, schedule.Status.Items["policy1"][2].Deleted, true)
	assert.Equal(t, schedule.Status.Items["policy1"][2].Message, "Couldn't find the pvc to snapshot")
	assert.Equal(t, schedule.Status.Items["policy1"][3].Deleted, true)
	assert.Equal(t, schedule.Status.Items["policy1"][3].Message, "Network failure")

	// Fields for the undeleted volumesnapshots should not be updated in the snapshotschedule object.
	assert.Empty(t, schedule.Status.Items["policy1"][1].Deleted)
	assert.Empty(t, schedule.Status.Items["policy1"][1].Message)
	assert.Empty(t, schedule.Status.Items["policy1"][4].Deleted)
	assert.Empty(t, schedule.Status.Items["policy1"][4].Message)
	assert.Empty(t, schedule.Status.Items["policy1"][5].Deleted)
	assert.Empty(t, schedule.Status.Items["policy1"][5].Message)
}

// getSnapshotObject is a helper function to frame the snapshot object and return it based on the parameters provided.
func getSnapshotObject(name, namespace string, creationTime metav1.Time, status snapv1.VolumeSnapshotConditionType, errorMessage string) *snapv1.VolumeSnapshot {
	return &snapv1.VolumeSnapshot{
		Metadata: metav1.ObjectMeta{
			Name:              name,
			Namespace:         namespace,
			CreationTimestamp: creationTime,
		},
		Status: snapv1.VolumeSnapshotStatus{
			CreationTimestamp: creationTime,
			Conditions: []snapv1.VolumeSnapshotCondition{
				{
					Type:    status,
					Status:  v1.ConditionTrue,
					Message: errorMessage,
				},
			},
		},
	}
}
