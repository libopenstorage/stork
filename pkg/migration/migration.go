package migration

import (
	"fmt"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/migration/controllers"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"k8s.io/client-go/tools/record"
)

// Migration migration
type Migration struct {
	Driver                      volume.Driver
	Recorder                    record.EventRecorder
	ResourceCollector           resourcecollector.ResourceCollector
	clusterPairController       *controllers.ClusterPairController
	migrationController         *controllers.MigrationController
	migrationScheduleController *controllers.MigrationScheduleController
}

// Init init
func (m *Migration) Init(migrationAdminNamespace string) error {
	m.clusterPairController = &controllers.ClusterPairController{
		Driver:   m.Driver,
		Recorder: m.Recorder,
	}
	err := m.clusterPairController.Init()
	if err != nil {
		return fmt.Errorf("error initializing clusterpair controller: %v", err)
	}

	m.migrationController = &controllers.MigrationController{
		Driver:            m.Driver,
		Recorder:          m.Recorder,
		ResourceCollector: m.ResourceCollector,
	}
	err = m.migrationController.Init(migrationAdminNamespace)
	if err != nil {
		return fmt.Errorf("error initializing migration controller: %v", err)
	}
	m.migrationScheduleController = &controllers.MigrationScheduleController{
		Recorder: m.Recorder,
	}
	err = m.migrationScheduleController.Init()
	if err != nil {
		return fmt.Errorf("error initializing migration schedule controller: %v", err)
	}
	return nil
}
