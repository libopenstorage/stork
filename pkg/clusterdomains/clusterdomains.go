package clusterdomains

import (
	"fmt"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/clusterdomains/controllers"
	"k8s.io/client-go/tools/record"
)

// ClusterDomains is a wrapper over the cluster domains CRD controllers
type ClusterDomains struct {
	Driver                         volume.Driver
	Recorder                       record.EventRecorder
	clusterDomainsStatusController *controllers.ClusterDomainsStatusController
	clusterDomainUpdateController  *controllers.ClusterDomainUpdateController
}

// Init initializes all the cluster domain controllers
func (c *ClusterDomains) Init() error {
	c.clusterDomainsStatusController = &controllers.ClusterDomainsStatusController{
		Driver:   c.Driver,
		Recorder: c.Recorder,
	}
	if err := c.clusterDomainsStatusController.Init(); err != nil {
		return fmt.Errorf("error initializing clusterdomainsstatus controller: %v", err)
	}
	c.clusterDomainUpdateController = &controllers.ClusterDomainUpdateController{
		Driver:   c.Driver,
		Recorder: c.Recorder,
	}
	if err := c.clusterDomainUpdateController.Init(); err != nil {
		return fmt.Errorf("error initializing clusterdomainupdate controller: %v", err)
	}
	return nil
}
