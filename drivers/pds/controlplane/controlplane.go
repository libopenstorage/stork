package controlplane

import (
	"fmt"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/pds/api"
	pdsapi "github.com/portworx/torpedo/drivers/pds/api"
	"github.com/portworx/torpedo/pkg/log"
	"net"
	"net/url"
	"strings"
)

// ControlPlane PDS
type ControlPlane struct {
	ControlPlaneURL string
	components      *api.Components
}

var (
	isavailable                bool
	isTemplateavailable        bool
	isStorageTemplateAvailable bool

	resourceTemplateID   string
	appConfigTemplateID  string
	storageTemplateID    string
	resourceTemplateName = "Small"
)

const (
	appConfigTemplateName = "QaDefault"
	storageTemplateName   = "QaDefault"
)

var (
	accountID          string
	tenantID           string
	projectID          string
	isAccountAvailable bool
	serviceType        = "LoadBalancer"
	components         *api.Components
)

// K8s Instances
var (
	k8sCore = core.Instance()
	k8sApps = apps.Instance()
)

func GetApiComponents(ControlPlaneURL string) (*api.Components, error) {
	apiConf := pds.NewConfiguration()
	endpointURL, err := url.Parse(ControlPlaneURL)
	if err != nil {
		return nil, err
	}
	apiConf.Host = endpointURL.Host
	apiConf.Scheme = endpointURL.Scheme
	apiClient := pds.NewAPIClient(apiConf)
	components := pdsapi.NewComponents(apiClient)
	return components, nil
}

// SetupPDSTest returns few params required to run the test
func (cp *ControlPlane) SetupPDSTest(ControlPlaneURL, ClusterType, AccountName, TenantName, ProjectName string) (string, string, string, string, string, string, error) {
	var err error

	if strings.EqualFold(ClusterType, "onprem") || strings.EqualFold(ClusterType, "ocp") {
		serviceType = "ClusterIP"
	}
	log.InfoD("Deployment service type %s", serviceType)

	components, err = GetApiComponents(ControlPlaneURL)
	if err != nil {
		return "", "", "", "", "", "", err
	}
	acc := components.Account
	accounts, err := acc.GetAccountsList()
	if err != nil {
		return "", "", "", "", "", "", err
	}

	isAccountAvailable = false
	for i := 0; i < len(accounts); i++ {
		log.InfoD("Account Name: %v", accounts[i].GetName())
		if accounts[i].GetName() == AccountName {
			isAccountAvailable = true
			accountID = accounts[i].GetId()
			break
		}
	}
	if !isAccountAvailable {
		return "", "", "", "", "", "", fmt.Errorf("account %v is not available", AccountName)
	}
	log.InfoD("Account Detail- Name: %s, UUID: %s ", AccountName, accountID)
	tnts := components.Tenant
	tenants, _ := tnts.GetTenantsList(accountID)
	for _, tenant := range tenants {
		if tenant.GetName() == TenantName {
			tenantID = tenant.GetId()
			break
		}

	}
	log.InfoD("Tenant Details- Name: %s, UUID: %s ", TenantName, tenantID)
	dnsZone, err := cp.GetDNSZone(tenantID)
	if err != nil {
		return "", "", "", "", "", "", err
	}
	log.InfoD("DNSZone: %s, tenantName: %s, accountName: %s", dnsZone, TenantName, AccountName)
	projcts := components.Project
	projects, _ := projcts.GetprojectsList(tenantID)
	for _, project := range projects {
		if project.GetName() == ProjectName {
			projectID = project.GetId()
			break
		}
	}
	log.InfoD("Project Details- Name: %s, UUID: %s ", ProjectName, projectID)

	ns, err := k8sCore.GetNamespace("kube-system")
	log.Infof("kube-system ns-> %v", ns)
	if err != nil {
		return "", "", "", "", "", "", fmt.Errorf("error Get kube-system ns-> %v", err)
	}
	clusterID := string(ns.GetObjectMeta().GetUID())
	if len(clusterID) > 0 {
		log.InfoD("clusterID %v", clusterID)
	} else {
		return "", "", "", "", "", "", fmt.Errorf("unable to get the clusterID")
	}

	return accountID, tenantID, dnsZone, projectID, serviceType, clusterID, err
}

// GetStorageTemplate return the storage template id
func (cp *ControlPlane) GetStorageTemplate(tenantID string) (string, error) {
	log.InfoD("Get the storage template")
	storageTemplates, err := components.StorageSettingsTemplate.ListTemplates(tenantID)
	if err != nil {
		return "", err
	}
	isStorageTemplateAvailable = false
	for i := 0; i < len(storageTemplates); i++ {
		if storageTemplates[i].GetName() == storageTemplateName {
			isStorageTemplateAvailable = true
			log.InfoD("Storage template details -----> Name %v,Repl %v , Fg %v , Fs %v",
				storageTemplates[i].GetName(),
				storageTemplates[i].GetRepl(),
				storageTemplates[i].GetFg(),
				storageTemplates[i].GetFs())
			storageTemplateID = storageTemplates[i].GetId()
		}
	}
	if !isStorageTemplateAvailable {
		log.Fatalf("storage template %v is not available ", storageTemplateName)
	}
	return storageTemplateID, nil
}

// DeleteAppConfigTemplates deletes the templates of given prefix
func (cp *ControlPlane) DeleteAppConfigTemplates(tenantID string, templatePrefixes []string) error {
	appTemplates, err := components.AppConfigTemplate.ListTemplates(tenantID)
	if err != nil {
		return fmt.Errorf("error occued while listing templates %v", err)
	}
	count := 0

	for _, templatePrefix := range templatePrefixes {
		for _, appTemplate := range appTemplates {
			if strings.Contains(*appTemplate.Name, templatePrefix) {
				log.Debugf("TemplateName %s", *appTemplate.Name)
				_, err := components.AppConfigTemplate.DeleteTemplate(appTemplate.GetId())
				if err != nil {
					return err
				}
				log.InfoD("Template %s Deleted", *appTemplate.Name)
				count++
			}
		}
	}

	log.Debugf("Number of templates deleted: %d", count)
	return nil
}

// DeleteStorageConfigTemplates deletes the templates of given prefix
func (cp *ControlPlane) DeleteStorageConfigTemplates(tenantID string, templatePrefixes []string) error {
	appTemplates, err := components.StorageSettingsTemplate.ListTemplates(tenantID)
	if err != nil {
		return fmt.Errorf("error occued while listing templates %v", err)
	}
	count := 0
	for _, templatePrefix := range templatePrefixes {
		for _, appTemplate := range appTemplates {
			if strings.Contains(*appTemplate.Name, templatePrefix) {
				log.Debugf("TemplateName %s", *appTemplate.Name)
				_, err := components.StorageSettingsTemplate.DeleteTemplate(appTemplate.GetId())
				if err != nil {
					return err
				}
				log.InfoD("Template %s Deleted", *appTemplate.Name)
				count++
			}
		}
	}
	log.Debugf("Number of templates deleted: %d", count)
	return nil
}

// DeleteResourceSettingTemplates deletes the templates of given prefix
func (cp *ControlPlane) DeleteResourceSettingTemplates(tenantID string, templatePrefixes []string) error {
	resourceTemplates, err := components.ResourceSettingsTemplate.ListTemplates(tenantID)
	if err != nil {
		return fmt.Errorf("error occued while listing templates %v", err)
	}
	count := 0
	for _, templatePrefix := range templatePrefixes {
		for _, resourceTemp := range resourceTemplates {
			if strings.Contains(*resourceTemp.Name, templatePrefix) {
				log.Debugf("TemplateName %s", *resourceTemp.Name)
				_, err := components.ResourceSettingsTemplate.DeleteTemplate(resourceTemp.GetId())
				if err != nil {
					return err
				}
				log.InfoD("Template %s Deleted", *resourceTemp.Name)
				count++
			}
		}
	}
	log.Debugf("Number of templates deleted: %d", count)
	return nil
}

// GetAppConfTemplate returns the app config template id
func (cp *ControlPlane) GetAppConfTemplate(tenantID string, ds string) (string, error) {
	appConfigs, err := components.AppConfigTemplate.ListTemplates(tenantID)
	if err != nil {
		return "", err
	}
	isavailable = false
	isTemplateavailable = false
	var dataServiceId string

	dsModel, err := components.DataService.ListDataServices()
	if err != nil {
		return "", fmt.Errorf("An Error Occured while listing dataservices %v", err)

	}
	for _, v := range dsModel {
		if *v.Name == ds {
			dataServiceId = *v.Id
		}
	}

	for i := 0; i < len(appConfigs); i++ {
		if appConfigs[i].GetName() == appConfigTemplateName {
			isTemplateavailable = true
			if dataServiceId == appConfigs[i].GetDataServiceId() {
				appConfigTemplateID = appConfigs[i].GetId()
				isavailable = true
			}
		}
	}
	if !(isavailable && isTemplateavailable) {
		log.Errorf("App Config Template with name %v does not exist", appConfigTemplateName)
	}
	return appConfigTemplateID, nil
}

// update template name with custom name
func (cp *ControlPlane) UpdateResourceTemplateName(TemplateName string) string {
	log.Infof("Updating the resource template name with : %v", TemplateName)
	resourceTemplateName = TemplateName
	return resourceTemplateName
}

// GetResourceTemplate get the resource template id
func (cp *ControlPlane) GetResourceTemplate(tenantID string, supportedDataService string) (string, error) {
	log.Infof("Get the resource template for each data services")
	resourceTemplates, err := components.ResourceSettingsTemplate.ListTemplates(tenantID)
	if err != nil {
		return "", err
	}
	isavailable = false
	isTemplateavailable = false
	for i := 0; i < len(resourceTemplates); i++ {
		if resourceTemplates[i].GetName() == resourceTemplateName {
			isTemplateavailable = true
			dataService, err := components.DataService.GetDataService(resourceTemplates[i].GetDataServiceId())
			if err != nil {
				return "", err
			}
			if dataService.GetName() == supportedDataService {
				log.Infof("Data service name: %v", dataService.GetName())
				log.Infof("Resource template details ---> Name %v, Id : %v ,DataServiceId %v , StorageReq %v , Memoryrequest %v",
					resourceTemplates[i].GetName(),
					resourceTemplates[i].GetId(),
					resourceTemplates[i].GetDataServiceId(),
					resourceTemplates[i].GetStorageRequest(),
					resourceTemplates[i].GetMemoryRequest())

				isavailable = true
				resourceTemplateID = resourceTemplates[i].GetId()
			}
		}
	}
	if !(isavailable && isTemplateavailable) {
		log.Errorf("Template with Name %v does not exis", resourceTemplateName)
	}
	return resourceTemplateID, nil
}

// GetRegistrationToken return token to register a target cluster.
func (cp *ControlPlane) GetRegistrationToken(tenantID string) (string, error) {
	log.Info("Fetch the registration token.")

	saClient := components.ServiceAccount
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

// ValidateDNSEndpoint
func (cp *ControlPlane) ValidateDNSEndpoint(dnsEndPoint string) error {
	log.Infof("Dataservice endpoint is: [%s]", dnsEndPoint)
	_, err := net.Dial("tcp", dnsEndPoint)
	if err != nil {
		log.Errorf("Failed to connect to the dns endpoint with err: %v", err)
		return err
	} else {
		log.Infof("DNS endpoint is reachable and ready to accept connections")
	}
	return nil
}

// GetDNSZone fetches DNS zone for deployment.
func (cp *ControlPlane) GetDNSZone(tenantID string) (string, error) {
	tenantComp := components.Tenant
	log.Debugf("tenantComp is initialized...")
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
