package migration

import (
	"fmt"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/migration/controllers"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/manager"
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
func (m *Migration) Init(mgr manager.Manager, migrationAdminNamespace string) error {
	m.clusterPairController = controllers.NewClusterPair(mgr, m.Driver, m.Recorder)
	err := m.clusterPairController.Init(mgr)
	if err != nil {
		return fmt.Errorf("error initializing clusterpair controller: %v", err)
	}

	m.migrationController = controllers.NewMigration(mgr, m.Driver, m.Recorder, m.ResourceCollector)
	err = m.migrationController.Init(mgr, migrationAdminNamespace)
	if err != nil {
		return fmt.Errorf("error initializing migration controller: %v", err)
	}

	m.migrationScheduleController = controllers.NewMigrationSchedule(mgr, m.Driver, m.Recorder)
	err = m.migrationScheduleController.Init(mgr)
	if err != nil {
		return fmt.Errorf("error initializing migration schedule controller: %v", err)
	}
	return nil
}
