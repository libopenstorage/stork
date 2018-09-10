package migration

import (
	"fmt"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/migration/controllers"
	"k8s.io/client-go/tools/record"
)

// Migration migration
type Migration struct {
	Driver                volume.Driver
	Recorder              record.EventRecorder
	clusterPairController *controllers.ClusterPairController
	migrationController   *controllers.MigrationController
}

// Init init
func (m *Migration) Init() error {
	m.clusterPairController = &controllers.ClusterPairController{
		Driver:   m.Driver,
		Recorder: m.Recorder,
	}
	err := m.clusterPairController.Init()
	if err != nil {
		return fmt.Errorf("error initializing clusterpair controller: %v", err)
	}

	m.migrationController = &controllers.MigrationController{
		Driver:   m.Driver,
		Recorder: m.Recorder,
	}
	err = m.migrationController.Init()
	if err != nil {
		return fmt.Errorf("error initializing migration controller: %v", err)
	}
	return nil
}
