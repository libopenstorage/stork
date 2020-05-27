package clusterdomains

import (
	"fmt"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/clusterdomains/controllers"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// ClusterDomains is a wrapper over the cluster domains CRD controllers
type ClusterDomains struct {
	Driver                         volume.Driver
	Recorder                       record.EventRecorder
	clusterDomainsStatusController *controllers.ClusterDomainsStatusController
	clusterDomainUpdateController  *controllers.ClusterDomainUpdateController
}

// Init initializes all the cluster domain controllers
func (c *ClusterDomains) Init(mgr manager.Manager) error {
	c.clusterDomainsStatusController = controllers.NewClusterDomainsStatus(mgr, c.Driver, c.Recorder)
	if err := c.clusterDomainsStatusController.Init(mgr); err != nil {
		return fmt.Errorf("error initializing clusterdomainsstatus controller: %v", err)
	}
	c.clusterDomainUpdateController = controllers.NewClusterDomainUpdate(mgr, c.Driver, c.Recorder)
	if err := c.clusterDomainUpdateController.Init(mgr); err != nil {
		return fmt.Errorf("error initializing clusterdomainupdate controller: %v", err)
	}
	return nil
}
