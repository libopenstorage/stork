package snapshotutils

import (
	"fmt"
	"github.com/portworx/torpedo/pkg/log"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/stork"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	storkops = stork.Instance()
)

const (
	snapshotScheduleRetryInterval = 10 * time.Second
	snapshotScheduleRetryTimeout  = 5 * time.Minute
)

// ValidateSnapshotSchedule validates the scheduled snapshots
func ValidateSnapshotSchedule(snapshotScheduleName string, appNamespace string) error {
	log.Infof("snapshotScheduleName : %s", snapshotScheduleName)
	log.Infof("Namespace : %v", appNamespace)
	snapStatuses, err := storkops.ValidateSnapshotSchedule(snapshotScheduleName,
		appNamespace,
		snapshotScheduleRetryTimeout,
		snapshotScheduleRetryInterval)
	if err != nil {
		log.Errorf("Got error while getting volume snapshot status :%v", err.Error())
		return err
	}
	if len(snapStatuses) == 0 {
		err = fmt.Errorf("No cloud snaps available in %s ", appNamespace)
		return err
	}
	for k, v := range snapStatuses {
		log.Infof("Policy Type: %v", k)
		for _, e := range v {
			log.Infof("ScheduledVolumeSnapShot Name: %v", e.Name)
			log.Infof("ScheduledVolumeSnapShot status: %v", e.Status)
		}
	}
	return nil
}

// SchedulePolicyInDefaultNamespace creates schedulePolicy
func SchedulePolicyInDefaultNamespace(policyName string, interval int, retain int) error {
	//Create snapshot schedule interval.
	log.Infof("Creating a interval schedule policy %v with interval %v minutes", policyName, interval)
	schedPolicy := &storkv1.SchedulePolicy{
		ObjectMeta: meta_v1.ObjectMeta{
			Name: policyName,
		},
		Policy: storkv1.SchedulePolicyItem{
			Interval: &storkv1.IntervalPolicy{
				Retain:          storkv1.Retain(retain),
				IntervalMinutes: interval,
			},
		}}
	_, err := storkops.CreateSchedulePolicy(schedPolicy)
	if err != nil {
		return err
	}
	return nil
}
