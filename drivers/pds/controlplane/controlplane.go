package controlplane

import (
	"github.com/portworx/torpedo/drivers/pds/api"

	log "github.com/sirupsen/logrus"
)

// ControlPlane PDS
type ControlPlane struct {
	ControlPlaneURL string
	components      *api.Components
}

// GetRegistrationToken return token to register a target cluster.
func (cp *ControlPlane) GetRegistrationToken(tenantID string) (string, error) {
	log.Info("Fetch the registration token.")

	saClient := cp.components.ServiceAccount
	serviceAccounts, _ := saClient.ListServiceAccounts(tenantID)
	var agentWriterID string
	for _, sa := range serviceAccounts {
		if sa.GetName() == "Default-AgentWriter" {
			agentWriterID = sa.GetId()
		}
	}
	token, err := saClient.GetServiceAccountToken(agentWriterID)
	if err != nil {
		return "", err
	}
	return token.GetToken(), nil
}

// GetDNSZone fetches DNS zone for deployment.
func (cp *ControlPlane) GetDNSZone(tenantID string) (string, error) {
	tenantComp := cp.components.Tenant
	tenant, err := tenantComp.GetTenant(tenantID)
	if err != nil {
		log.Panicf("Unable to fetch the tenant info.\n Error - %v", err)
	}
	log.Infof("Get DNS Zone for the tenant. Name -  %s, Id - %s", tenant.GetName(), tenant.GetId())
	dnsModel, err := tenantComp.GetDNS(tenantID)
	if err != nil {
		log.Infof("Unable to fetch the DNSZone info. \n Error - %v", err)
	}
	return dnsModel.GetDnsZone(), err
}

// NewControlPlane to create control plane instance.
func NewControlPlane(url string, components *api.Components) *ControlPlane {
	return &ControlPlane{
		ControlPlaneURL: url,
		components:      components,
	}
}
