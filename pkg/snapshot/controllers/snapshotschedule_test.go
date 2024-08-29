package controllers

import (
	"testing"
	"time"

	snapv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	v1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	k8sextops "github.com/portworx/sched-ops/k8s/externalstorage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestCleanupErroredSnapshots tests the `cleanupErroredSnapshots` function that
// cleans up snapshots in error state and older than a certain cutoff time period.
func TestCleanupErroredSnapshots(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockSnap := NewMockOps(ctrl)
	mockSched := NewMockSnapshotScheduleOps(ctrl)

	// Creating a dummy snapshotschedule object.
	creationTime1 := metav1.NewTime(time.Now().Add(-30 * time.Minute))
	creationTime2 := metav1.NewTime(time.Now().Add(-25 * time.Minute))
	creationTime3 := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	creationTime4 := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	creationTime5 := metav1.NewTime(time.Now().Add(-1 * time.Minute))
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
				},
			},
		},
	}

	// Expected updated schedule at the end.
	updatedSchedule := &stork_api.VolumeSnapshotSchedule{
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
						Message:           "snapshot deleted due to being in error state for more than cutoff period",
						Deleted:           true,
					},
					{
						Name:              "snapshot4",
						Status:            snapv1.VolumeSnapshotConditionError,
						CreationTimestamp: creationTime4,
						Message:           "snapshot deleted due to being in error state for more than cutoff period",
						Deleted:           true,
					},
					{
						Name:              "snapshot5",
						Status:            snapv1.VolumeSnapshotConditionPending,
						CreationTimestamp: creationTime5,
					},
				},
			},
		},
	}

	// Mock and assert the snapshot and snapshotschedule calls.
	mockSnap.EXPECT().GetSnapshot(gomock.Eq("snapshot1"), gomock.Eq("test-ns")).Return(
		getSnapshotObject("snapshot1", "test-ns", creationTime1, snapv1.VolumeSnapshotConditionError), nil).AnyTimes()
	mockSnap.EXPECT().GetSnapshot(gomock.Eq("snapshot3"), gomock.Eq("test-ns")).Return(
		getSnapshotObject("snapshot3", "test-ns", creationTime3, snapv1.VolumeSnapshotConditionError), nil).AnyTimes()
	mockSnap.EXPECT().GetSnapshot(gomock.Eq("snapshot4"), gomock.Eq("test-ns")).Return(
		getSnapshotObject("snapshot4", "test-ns", creationTime4, snapv1.VolumeSnapshotConditionError), nil).AnyTimes()
	mockSnap.EXPECT().DeleteSnapshot(gomock.Eq("snapshot1"), gomock.Eq("test-ns")).Return(nil).AnyTimes()
	mockSnap.EXPECT().DeleteSnapshot(gomock.Eq("snapshot3"), gomock.Eq("test-ns")).Return(nil).AnyTimes()
	mockSnap.EXPECT().DeleteSnapshot(gomock.Eq("snapshot4"), gomock.Eq("test-ns")).Return(nil).AnyTimes()
	mockSched.EXPECT().UpdateSnapshotSchedule(gomock.Cond(func(snapshotschedule any) bool {
		// Fields for the deleted volumesnapshots should be updated in the snapshotschedule object.
		schedule := snapshotschedule.(*v1alpha1.VolumeSnapshotSchedule)
		assert.Equal(t, schedule.Status.Items["policy1"][0].Deleted, true)
		assert.Equal(t, schedule.Status.Items["policy1"][0].Message, errorSnapshotDeletionMessage)
		assert.Equal(t, schedule.Status.Items["policy1"][2].Deleted, true)
		assert.Equal(t, schedule.Status.Items["policy1"][2].Message, errorSnapshotDeletionMessage)
		assert.Equal(t, schedule.Status.Items["policy1"][3].Deleted, true)
		assert.Equal(t, schedule.Status.Items["policy1"][3].Message, errorSnapshotDeletionMessage)

		// Fields for the undeleted volumesnapshots should not be updated in the snapshotschedule object.
		assert.Empty(t, schedule.Status.Items["policy1"][1].Deleted)
		assert.Empty(t, schedule.Status.Items["policy1"][1].Message)
		assert.Empty(t, schedule.Status.Items["policy1"][4].Deleted)
		assert.Empty(t, schedule.Status.Items["policy1"][4].Message)
		return true
	})).Return(updatedSchedule, nil).AnyTimes()

	k8sextops.SetInstance(mockSnap)
	storkops.SetInstance(mockSched)
	s := SnapshotScheduleController{}
	err := s.cleanupErroredSnapshots(snapshotSchedule)
	assert.NoError(t, err)
}

// getSnapshotObject is a helper function to frame the snapshot object and return it based on the parameters provided.
func getSnapshotObject(name, namespace string, creationTime metav1.Time, status snapv1.VolumeSnapshotConditionType) *snapv1.VolumeSnapshot {
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
					Type:   status,
					Status: v1.ConditionTrue,
				},
			},
		},
	}
}
