package groupsnapshot

import (
	"fmt"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/groupsnapshot/controllers"
	"github.com/libopenstorage/stork/pkg/rule"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
)

// GroupSnapshot instance
type GroupSnapshot struct {
	Driver                  volume.Driver
	Recorder                record.EventRecorder
	groupSnapshotController *controllers.GroupSnapshotController
}

// Init init
func (m *GroupSnapshot) Init() error {
	m.groupSnapshotController = &controllers.GroupSnapshotController{
		Driver:   m.Driver,
		Recorder: m.Recorder,
	}

	if err := m.groupSnapshotController.Init(); err != nil {
		return fmt.Errorf("error initializing groupSnapshot controller: %v", err)
	}

	if err := m.performRuleRecovery(); err != nil {
		return fmt.Errorf("error doing recovery on pending group snapshot rules: %v", err)
	}

	return nil
}

func (m *GroupSnapshot) performRuleRecovery() error {
	allGroupSnaps, err := k8s.Instance().ListGroupSnapshots(v1.NamespaceAll)
	if err != nil {
		logrus.Errorf("Failed to list all group snapshots due to: %v. Will retry.", err)
		return err
	}

	if allGroupSnaps == nil {
		return nil
	}

	var lastError error
	for _, groupSnap := range allGroupSnaps.Items {
		controllers.SetKind(&groupSnap)
		err := rule.PerformRuleRecovery(&groupSnap)
		if err != nil {
			lastError = err
		}
	}
	return lastError
}
