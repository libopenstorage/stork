package groupsnapshot

import (
	"fmt"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/groupsnapshot/controllers"
	"github.com/libopenstorage/stork/pkg/rule"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// GroupSnapshot instance
type GroupSnapshot struct {
	Driver   volume.Driver
	Recorder record.EventRecorder
}

// Init init
func (m *GroupSnapshot) Init(mgr manager.Manager) error {
	r := controllers.NewGroupSnapshot(mgr, m.Driver, m.Recorder)

	if err := r.Init(mgr); err != nil {
		return fmt.Errorf("initializing groupSnapshot controller: %v", err)
	}

	if err := m.performRuleRecovery(); err != nil {
		return fmt.Errorf("error doing recovery on pending group snapshot rules: %v", err)
	}

	return nil
}

func (m *GroupSnapshot) performRuleRecovery() error {
	allGroupSnaps, err := storkops.Instance().ListGroupSnapshots(corev1.NamespaceAll)
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
