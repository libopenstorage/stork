package rke

import (
	"flag"
	"fmt"
	"github.com/portworx/sched-ops/k8s/core"
	portworx2 "github.com/portworx/torpedo/drivers/backup/portworx"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	kube "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/pkg/errors"
	"github.com/portworx/torpedo/pkg/log"
	_ "github.com/rancher/norman/clientbase"
	rancherClientBase "github.com/rancher/norman/clientbase"
	"github.com/rancher/norman/types"
	rancherClient "github.com/rancher/rancher/pkg/client/generated/management/v3"
	"os"
	"strings"
	"time"
)

const (
	// scheduleName is the name of the kubernetes scheduler driver implementation
	schedulerName = "rke"
	// SystemdScheduleServiceName is the name of the system service responsible for scheduling
	SystemdScheduleServiceName = "kubelet"
)

var RancherMap = make(map[string]*RancherClusterParameters)

type Rancher struct {
	kube.K8s
	client *rancherClient.Client
}

type RancherClusterParameters struct {
	Token     string
	Endpoint  string
	AccessKey string
	SecretKey string
}

// String returns the string name of this driver.
func (r *Rancher) String() string {
	return schedulerName
}

// Init Initializes the driver
func (r *Rancher) Init(scheduleOpts scheduler.InitOptions) error {
	var err error
	err = r.K8s.Init(scheduleOpts)
	if err != nil {
		return err
	}
	// This is needed only in case of px-backup as other platforms do not create new RKE client
	if flag.Lookup("backup-driver").Value.(flag.Getter).Get().(string) == portworx2.DriverName {
		rkeParametersValue, err := r.GetRancherClusterParametersValue()
		if err != nil {
			return err
		}
		rancherClientOpts := rancherClientBase.ClientOpts{
			URL:       rkeParametersValue.Endpoint,
			TokenKey:  rkeParametersValue.Token,
			AccessKey: rkeParametersValue.AccessKey,
			SecretKey: rkeParametersValue.SecretKey,
			Insecure:  true,
		}
		r.client, err = rancherClient.NewClient(&rancherClientOpts)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetRancherClusterParametersValue returns the rancher token, endpoint, secret key, access key
func (r *Rancher) GetRancherClusterParametersValue() (*RancherClusterParameters, error) {
	var rkeParameters RancherClusterParameters
	var rkeToken string
	// TODO Rancher URL for cloud cluster will not be fetched from master node IP
	masterNodeName := node.GetMasterNodes()[0].Name
	log.Infof("The master node here is %v", masterNodeName)
	endpoint := "https://" + masterNodeName + "/v3"
	rkeParameters.Endpoint = endpoint
	rkeToken = os.Getenv("SOURCE_RKE_TOKEN")
	if rkeToken == "" {
		return nil, fmt.Errorf("env variable SOURCE_RKE_TOKEN should not be empty")
	}
	rkeParameters.Token = rkeToken
	rkeParameters.AccessKey = strings.Split(rkeToken, ":")[0]
	rkeParameters.SecretKey = strings.Split(rkeToken, ":")[1]
	return &rkeParameters, nil
}

// UpdateRancherClient updates the rancher client based on the current cluster context
func (r *Rancher) UpdateRancherClient(clusterName string) error {
	var rkeParametersValue RancherClusterParameters
	var err error
	var rkeToken string
	masterNodeName := node.GetMasterNodes()[0].Name
	endpoint := "https://" + masterNodeName + "/v3"
	if clusterName == "destination-config" {
		rkeToken = os.Getenv("DESTINATION_RKE_TOKEN")
		if rkeToken == "" {
			return fmt.Errorf("env variable DESTINATION_RKE_TOKEN should not be empty")
		}
	} else if clusterName == "source-config" {
		rkeToken = os.Getenv("SOURCE_RKE_TOKEN")
		if rkeToken == "" {
			return fmt.Errorf("env variable SOURCE_RKE_TOKEN should not be empty")
		}
	} else {
		return fmt.Errorf("cluster name is not correct")
	}
	accessKey := strings.Split(rkeToken, ":")[0]
	secretKey := strings.Split(rkeToken, ":")[1]
	rkeParametersValue.Token = rkeToken
	rkeParametersValue.SecretKey = secretKey
	rkeParametersValue.AccessKey = accessKey
	rkeParametersValue.Endpoint = endpoint
	rancherClientOpts := rancherClientBase.ClientOpts{
		URL:       endpoint,
		TokenKey:  rkeToken,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Insecure:  true,
	}
	r.client, err = rancherClient.NewClient(&rancherClientOpts)
	if err != nil {
		return err
	}
	RancherMap[clusterName] = &rkeParametersValue
	return nil
}

// GetActiveRancherClusterID returns the ID of active rancher cluster
func (r *Rancher) GetActiveRancherClusterID(clusterName string) (string, error) {
	var clusterId string
	clusterCollection, err := r.client.Cluster.List(nil)
	if err != nil {
		return "", err
	}
	for _, cluster := range clusterCollection.Data {
		if cluster.Name == clusterName {
			clusterId = cluster.ID
			return clusterId, nil
		}
	}
	return "", fmt.Errorf("cluster with cluster name %s is not present", clusterName)
}

// CreateRancherProject creates new project in rancher cluster
func (r *Rancher) CreateRancherProject(projectName string, projectDescription string, clusterName string, label map[string]string, annotation map[string]string) (*rancherClient.Project, error) {
	var clusterId string
	var newProject *rancherClient.Project
	clusterId, err := r.GetActiveRancherClusterID(clusterName)
	if err != nil {
		return nil, err
	}
	projectRequest := &rancherClient.Project{
		Name:        projectName,
		Description: projectDescription,
		ClusterID:   clusterId,
		Labels:      label,
		Annotations: annotation,
	}
	newProject, err = r.client.Project.Create(projectRequest)
	if err != nil {
		return nil, err
	}
	return newProject, nil
}

// GetProjectID return the project ID
func (r *Rancher) GetProjectID(projectName string) (string, error) {
	var projectId string
	projectList, err := r.client.Project.List(nil)
	if err != nil {
		return "", err
	}
	for _, project := range projectList.Data {
		if project.Name == projectName {
			projectId = project.ID
			return projectId, nil
		}
	}
	return projectId, fmt.Errorf("no project matching the given projectName %s was found", projectName)
}

// CreateUserForRancherProject Creates a rancher user and adds the user to the project
func (r *Rancher) CreateUserForRancherProject(projectName string, username string, password string) (string, error) {
	userAnnotation := make(map[string]string)
	userLabel := make(map[string]string)

	projectId, err := r.GetProjectID(projectName)
	if err != nil {
		return "", err
	}
	userAnnotation["field.cattle.io/projectId"] = projectId
	userLabel["field.cattle.io/projectId"] = strings.Split(projectId, ":")[1]

	userRequest := &rancherClient.User{
		Username:    username,
		Password:    password,
		Name:        fmt.Sprintf("Test " + username),
		Annotations: userAnnotation,
		Labels:      userLabel,
	}
	newUser, err := r.client.User.Create(userRequest)
	if err != nil {
		return "", fmt.Errorf("failed to create the user %s: %w", username, err)
	}
	// ProjectRoleTemplateBinding is an RBAC for Rancher, which can be assigned to users to give them necessary permissions in a project
	// the role of the user can either be "project-member" or "project-owner", we are restricting the role to a member
	roleRequest := &rancherClient.ProjectRoleTemplateBinding{
		ProjectID:      projectId,
		UserID:         newUser.ID,
		RoleTemplateID: "project-member",
	}
	_, err = r.client.ProjectRoleTemplateBinding.Create(roleRequest)
	if err != nil {
		return "", err
	}
	log.InfoD("User [%s] is successfully created with user id [%s] and was added to the project [%s]", username, newUser.ID, projectName)
	return newUser.ID, nil
}

// CreateMultipleUsersForRancherProject Creates multiple rancher users based on the supplied user map
func (r *Rancher) CreateMultipleUsersForRancherProject(projectName string, userMap map[string]string) ([]string, error) {
	log.InfoD("Creating multiple users for rancher project")
	var userIDList []string
	for username, password := range userMap {
		userID, err := r.CreateUserForRancherProject(projectName, username, password)
		if err != nil {
			return nil, err
		}
		userIDList = append(userIDList, userID)
	}
	return userIDList, nil
}

// ValidateUsersInProject Validates the rancher users for a project by comparing the project members and the supplied list of users
func (r *Rancher) ValidateUsersInProject(projectName string, userList []string) error {
	var actualUserListForProject []string
	projectId, err := r.GetProjectID(projectName)
	if err != nil {
		return err
	}
	//the returned roleMapping collection would include details about the permissions and roles assigned for the entire rancher cluster
	roleMapping, err := r.client.ProjectRoleTemplateBinding.List(nil)
	if err != nil {
		return err
	}
	//the if condition filters the data based on the supplied project and the role.Name, the value for role.Name will either be "creator-project-owner" or a user id of a user associated with that project.
	for _, role := range roleMapping.Data {
		if role.ProjectID == projectId && role.Name != "creator-project-owner" {
			actualUserListForProject = append(actualUserListForProject, role.UserID)
		}
	}
	//here we are trying to compare if all the users supplied to this function are present in the project
	for _, user := range userList {
		userFound := false
		for _, actualUser := range actualUserListForProject {
			if user == actualUser {
				userFound = true
				break
			}
		}
		//even if a single user from the supplied list isn't found in actualUserListForProject, we return an error
		if !userFound {
			return fmt.Errorf("user %s is not part of the actual user list for the project", user)
		}
	}
	return nil
}

// DeleteRancherUsers Deletes all the users who are a part of the supplied list
func (r *Rancher) DeleteRancherUsers(userIDList []string) error {
	for _, userID := range userIDList {
		userObj, err := r.client.User.ByID(userID)
		if err != nil {
			return err
		}
		err = r.client.User.Delete(userObj)
		if err != nil {
			return err
		}
		log.InfoD("The user [%s] is deleted", userID)
	}
	return nil
}

//Reason for updating the namespace with label and annotation for moving it to any project instead of using the inbuilt function:
//For project related operation we need to import https://github.com/rancher/rancher/tree/release/v2.7/pkg/client/generated/management
//For namespace related operation we need to import https://github.com/rancher/rancher/tree/release/v2.7/pkg/client/generated/cluster
//Since these are in two different packages, two different client needs to be created.Creating or updating namespace using the cluster package does not work

// AddNamespacesToProject adds namespace to the given project
func (r *Rancher) AddNamespacesToProject(projectName string, nsList []string) error {
	var projectId string
	var err error
	namespaceAnnotation := make(map[string]string)
	namespaceLabel := make(map[string]string)
	projectId, err = r.GetProjectID(projectName)
	if err != nil {
		return err
	}
	namespaceAnnotation["field.cattle.io/projectId"] = projectId
	namespaceLabel["field.cattle.io/projectId"] = strings.Split(projectId, ":")[1]
	for _, ns := range nsList {
		ns, err := core.Instance().GetNamespace(ns)
		if err != nil {
			return err
		}
		newLabels := kube.MergeMaps(ns.Labels, namespaceLabel)
		newAnnotation := kube.MergeMaps(ns.Annotations, namespaceAnnotation)
		ns.SetLabels(newLabels)
		ns.SetAnnotations(newAnnotation)
		_, err = core.Instance().UpdateNamespace(ns)
		if err != nil {
			return err
		}
	}
	return nil
}

// ValidateProjectOfNamespaces validates if the namespaces belong to the given project
func (r *Rancher) ValidateProjectOfNamespaces(projectName string, nsList []string) error {
	var projectId string
	var err error
	projectId, err = r.GetProjectID(projectName)
	if err != nil {
		return err
	}
	for _, ns := range nsList {
		ns, err := core.Instance().GetNamespace(ns)
		if err != nil {
			return err
		}
		nsLabels := ns.GetLabels()
		nsAnnotation := ns.GetAnnotations()
		for labelKey, labelValue := range nsLabels {
			if labelKey == "field.cattle.io/projectId" {
				if labelValue != strings.Split(projectId, ":")[1] {
					return fmt.Errorf("the namespace does not belong to the expected project %s with project ID %s as label does not matches the projectID", projectName, projectId)
				}
				for annotationKey, annotationValue := range nsAnnotation {
					if annotationKey == "field.cattle.io/projectId" {
						if annotationValue != projectId {
							return fmt.Errorf("the namespace does not belong to the expected project %s with project ID %s as annotation does not matches the projectID", projectName, projectId)
						} else {
							return nil
						}
					}
				}
				return fmt.Errorf("could not get the required key:field.cattle.io/projectId in annotation for ns %s", ns.Name)
			}
		}
		return fmt.Errorf("could not get the required key:field.cattle.io/projectId in annotation for ns %s", ns.Name)
	}
	return nil
}

// DeleteRancherProject deletes the given project in rancher cluster
func (r *Rancher) DeleteRancherProject(uid string) error {
	var err error
	projectObj, err := r.client.Project.ByID(uid)
	if err != nil {
		return err
	}
	err = r.client.Project.Delete(projectObj)
	if err != nil {
		return err
	}
	return nil
}

// SaveSchedulerLogsToFile gathers all scheduler logs into a file
func (r *Rancher) SaveSchedulerLogsToFile(n node.Node, location string) error {
	driver, _ := node.Get(r.K8s.NodeDriverName)
	// requires 2>&1 since docker logs command send the logs to stderr instead of stdout
	cmd := fmt.Sprintf("docker logs %s > %s/kubelet.log 2>&1", SystemdScheduleServiceName, location)
	_, err := driver.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         kube.DefaultTimeout,
		TimeBeforeRetry: kube.DefaultRetryInterval,
		Sudo:            true,
	})
	return err
}

// RemoveNamespaceFromProject moves namespace to no project
func (r *Rancher) RemoveNamespaceFromProject(nsList []string) error {
	var namespaceParameterList []map[string]string
	for _, ns := range nsList {
		ns, err := core.Instance().GetNamespace(ns)
		if err != nil {
			return err
		}
		namespaceParameterList = append(namespaceParameterList, ns.Labels)
		namespaceParameterList = append(namespaceParameterList, ns.Annotations)
		for _, val := range namespaceParameterList {
			delete(val, "field.cattle.io/projectId")
		}
		_, err = core.Instance().UpdateNamespace(ns)
		if err != nil {
			return err
		}
	}
	return nil
}

// ChangeProjectForNamespace moves namespace from one project to other project
func (r *Rancher) ChangeProjectForNamespace(projectName string, nsList []string) error {
	var projectId string
	var namespaceParameterList []map[string]string
	namespaceAnnotation := make(map[string]string)
	namespaceLabel := make(map[string]string)
	for _, ns := range nsList {
		ns, err := core.Instance().GetNamespace(ns)
		if err != nil {
			return err
		}
		projectId, err = r.GetProjectID(projectName)
		if err != nil {
			return err
		}
		namespaceParameterList = append(namespaceParameterList, ns.Labels)
		namespaceParameterList = append(namespaceParameterList, ns.Annotations)
		for _, val := range namespaceParameterList {
			delete(val, "field.cattle.io/projectId")
		}
		namespaceAnnotation["field.cattle.io/projectId"] = projectId
		namespaceLabel["field.cattle.io/projectId"] = strings.Split(projectId, ":")[1]
		newLabels := kube.MergeMaps(ns.Labels, namespaceLabel)
		newAnnotation := kube.MergeMaps(ns.Annotations, namespaceAnnotation)
		ns.SetLabels(newLabels)
		ns.SetAnnotations(newAnnotation)
		_, err = core.Instance().UpdateNamespace(ns)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteNode deletes the given node
func (r *Rancher) DeleteNode(node node.Node) error {
	// TODO implement this method
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "DeleteActionApproval()",
	}
}

func (r *Rancher) SetASGClusterSize(perZoneCount int64, timeout time.Duration) error {
	// ScaleCluster is not supported
	return &errors.ErrNotSupported{
		Type:      "Function",
		Operation: "SetASGClusterSize()",
	}
}

// CreateCustomPodSecurityAdmissionConfigurationTemplate creates a custom PSA template
func (r *Rancher) CreateCustomPodSecurityAdmissionConfigurationTemplate(psaTemplateName string, description string, psaTemplateDefaults *rancherClient.PodSecurityAdmissionConfigurationTemplateDefaults, psaTemplateExemptions *rancherClient.PodSecurityAdmissionConfigurationTemplateExemptions) error {
	PSAInterface := &rancherClient.PodSecurityAdmissionConfigurationTemplate{
		Name:        psaTemplateName,
		Description: description,
		Configuration: &rancherClient.PodSecurityAdmissionConfigurationTemplateSpec{
			Defaults:   psaTemplateDefaults,
			Exemptions: psaTemplateExemptions,
		},
	}
	log.InfoD("Create custom PSA template: %v with template defaults %v and template exemptions %v", psaTemplateName, psaTemplateDefaults, psaTemplateExemptions)
	_, err := r.client.PodSecurityAdmissionConfigurationTemplate.Create(PSAInterface)
	return err
}

// GetRKEClusterList returns the list of RKE clusters added to Rancher
func (r *Rancher) GetRKEClusterList() ([]string, error) {
	var clusterList []string
	log.InfoD("Getting list of RKE clusters added to Rancher")
	clusterCollection, err := r.client.Cluster.List(nil)
	if err != nil {
		return clusterList, err
	}
	for _, cluster := range clusterCollection.Data {
		clusterList = append(clusterList, cluster.Name)
	}
	return clusterList, nil
}

// GetCurrentClusterWidePSA returns the current cluster wide PSA configured
func (r *Rancher) GetCurrentClusterWidePSA(clusterName string) (string, error) {
	clusterCollection, err := r.client.Cluster.List(nil)
	if err != nil {
		return "", err
	}
	for _, cluster := range clusterCollection.Data {
		if cluster.Name == clusterName {
			log.InfoD("The cluster wide PSA for cluster %v is %v", clusterName, cluster.DefaultPodSecurityAdmissionConfigurationTemplateName)
			return cluster.DefaultPodSecurityAdmissionConfigurationTemplateName, nil
		}
	}
	return "", fmt.Errorf("cluster with cluster name %s is not present", clusterName)
}

// GetPodSecurityAdmissionConfigurationTemplateList returns the list of PSA Templates present in the Rancher
func (r *Rancher) GetPodSecurityAdmissionConfigurationTemplateList() (*rancherClient.PodSecurityAdmissionConfigurationTemplateCollection, error) {
	var listOptions *types.ListOpts
	log.InfoD("Fetching the list PSA Templates present in the Rancher")
	psaList, err := r.client.PodSecurityAdmissionConfigurationTemplate.ListAll(listOptions)
	return psaList, err
}

// UpdateClusterWidePSA add the cluster wide PSA to the given cluster
func (r *Rancher) UpdateClusterWidePSA(clusterName string, psaName string) error {
	clusterCollection, err := r.client.Cluster.List(nil)
	if err != nil {
		return err
	}
	log.InfoD("Updating cluster wide PSA %v for cluster %v", psaName, clusterName)
	for _, cluster := range clusterCollection.Data {
		if cluster.Name == clusterName {
			clusterInterface := &rancherClient.Cluster{
				Name: cluster.Name,
				DefaultPodSecurityAdmissionConfigurationTemplateName: psaName,
			}
			_, err = r.client.Cluster.Update(&cluster, clusterInterface)
			return err
		}
	}
	return fmt.Errorf("cluster with cluster name %s is not present", clusterName)
}

func init() {
	r := &Rancher{}
	scheduler.Register(schedulerName, r)
}
