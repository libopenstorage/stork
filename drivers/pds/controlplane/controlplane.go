package controlplane

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	pds "github.com/portworx/pds-api-go-client/pds/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/pds/api"
	pdsapi "github.com/portworx/torpedo/drivers/pds/api"
	"github.com/portworx/torpedo/pkg/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"net"
	"net/url"
	"strings"
	"time"
)

// ControlPlane PDS
type ControlPlane struct {
	ControlPlaneURL string
	components      *api.Components
}

type Templates struct {
	CpuLimit       string
	CpuRequest     string
	DataServiceID  string
	MemoryLimit    string
	MemoryRequest  string
	Name           string
	StorageRequest string
	FsType         string
	ReplFactor     int32
	Provisioner    string
	Secure         bool
	VolGroups      bool
}

var (
	isavailable                bool
	isTemplateavailable        bool
	isStorageTemplateAvailable bool
	resourceTemplateID         string
	appConfigTemplateID        string
	storageTemplateID          string
	resourceTemplateName       = "Small"
)

const (
	storageTemplateName   = "QaDefault"
	appConfigTemplateName = "QaDefault"
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

func (cp *ControlPlane) CreateCustomResourceTemplate(tenantID string, templates Templates) (*pds.ModelsStorageOptionsTemplate, *pds.ModelsResourceSettingsTemplate, error) {
	stConfigModel, err := components.StorageSettingsTemplate.CreateTemplate(tenantID, templates.VolGroups, templates.FsType, templates.Name, templates.Provisioner, templates.ReplFactor, templates.Secure)
	if err != nil {
		return nil, nil, err
	}
	log.InfoD("Created Storage Configuration is- %v with Storage-templateID: %v", stConfigModel.GetName(), stConfigModel.GetId())
	resConfigModel, err := components.ResourceSettingsTemplate.CreateTemplate(tenantID, templates.CpuLimit, templates.CpuRequest, templates.DataServiceID, templates.MemoryLimit, templates.MemoryRequest, templates.Name, templates.StorageRequest)
	if err != nil {
		return nil, nil, err
	}
	log.InfoD("Created Resource Configuration is- %v with Resource-templateID: %v", stConfigModel.GetName(), stConfigModel.GetId())
	return stConfigModel, resConfigModel, nil
}

func (cp *ControlPlane) UpdateCustomResourceTemplates(cusResourceTempId string, templates Templates) (*pds.ModelsResourceSettingsTemplate, error) {
	resConfigModelUpdated, err := components.ResourceSettingsTemplate.UpdateTemplate(cusResourceTempId, templates.CpuLimit, templates.CpuRequest, templates.MemoryLimit, templates.MemoryRequest, templates.Name, templates.StorageRequest)
	if err != nil {
		return nil, err
	}
	log.InfoD("Updated Resource Configuration is- %v with Resource-templateID: %v", resConfigModelUpdated.GetName(), resConfigModelUpdated.GetId())
	return resConfigModelUpdated, nil
}

func (cp *ControlPlane) UpdateCustomStorageTemplates(cusStorageTempId string, templates Templates) (*pds.ModelsStorageOptionsTemplate, error) {
	stConfigModelUpdated, err := components.StorageSettingsTemplate.UpdateTemplate(cusStorageTempId, templates.VolGroups, templates.FsType, templates.Name, templates.ReplFactor, templates.Secure)
	if err != nil {
		return nil, err
	}
	log.InfoD("Updated Storage Configuration is- %v with Storage-templateID: %v", stConfigModelUpdated.GetName(), stConfigModelUpdated.GetId())
	return stConfigModelUpdated, nil
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

func (cp *ControlPlane) CleanupCustomTemplates(storageTemplateIDs []string, resourceTemplateIDs []string) error {
	for _, stId := range storageTemplateIDs {
		_, err := components.StorageSettingsTemplate.DeleteTemplate(stId)
		if err != nil {
			return fmt.Errorf("failed to delete storage template with ID- %v", stId)
		}
	}
	for _, resId := range resourceTemplateIDs {
		_, err := components.ResourceSettingsTemplate.DeleteTemplate(resId)
		if err != nil {
			return fmt.Errorf("failed to delete storage template with ID- %v", resId)
		}
	}
	return nil

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

// CreatePostgreSQLClientAndConnect takes connectionString as parameter and does a ping then returns client
func (cp *ControlPlane) CreatePostgreSQLClientAndConnect(connectionString string) (*pgx.Conn, error) {
	log.Debugf("Connection string %s", connectionString)
	conn, err := pgx.Connect(context.Background(), connectionString)
	if err != nil {
		return nil, fmt.Errorf("error while connecting to the database")
	}
	err = conn.Ping(context.Background())
	if err != nil {
		return nil, fmt.Errorf("error while pinging the database")
	}
	return conn, nil
}

// CreateMongoDBClientAndConnect takes connectionString as parameter and does a ping then returns client
func (cp *ControlPlane) CreateMongoDBClientAndConnect(connectionString string) (*mongo.Client, error) {
	log.Debugf("Connection string %s", connectionString)
	clientOptions := options.Client().ApplyURI(connectionString)

	// Create a MongoDB client
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, fmt.Errorf("error while connecting to mongoDB: [%v]", err)
	}
	// Ping the MongoDB server to ensure connectivity
	err = client.Ping(context.Background(), nil)
	if err != nil {
		return nil, fmt.Errorf("error occured during ping: [%v]", err)
	}
	log.Infof("Connected to MongoDB!")

	// Close the client when done
	err = client.Disconnect(context.Background())
	if err != nil {
		return nil, err
	}

	return client, nil
}

// ValidateIfTLSEnabled takes db related parameters to establish connection and returns error if connection is unsuccessful
func (cp *ControlPlane) ValidateIfTLSEnabled(username, password, dnsEndPoint, port string) error {
	log.Infof("Data service endpoint is: [%s]", dnsEndPoint)
	connectionString := fmt.Sprintf("mongodb://%s:%s@%s:%s", username, password, dnsEndPoint, port)

	_, err := cp.CreateMongoDBClientAndConnect(connectionString)
	if err != nil {
		return err
	}

	return nil
}

// ValidateDNSEndpoint
func (cp *ControlPlane) ValidateDNSEndpoint(dnsEndPoint string) error {
	log.Infof("Dataservice endpoint is: [%s]", dnsEndPoint)
	log.Debugf("sleeping for 5 min, before validating dns endpoint")
	time.Sleep(5 * time.Minute)
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
