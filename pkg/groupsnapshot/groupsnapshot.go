package groupsnapshot

import (
	"fmt"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/groupsnapshot/controllers"
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

	err := m.groupSnapshotController.Init()
	if err != nil {
		return fmt.Errorf("error initializing groupSnapshot controller: %v", err)
	}
	return nil
}
