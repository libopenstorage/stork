package rke

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	kube "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/pkg/log"
	_ "github.com/rancher/norman/clientbase"
	rancherClient "github.com/rancher/rancher/pkg/client/generated/management/v3"
	"io/ioutil"
	"net/http"
	"strings"
)

const (
	// scheduleName is the name of the kubernetes scheduler driver implementation
	schedulerName = "rke"
	// SystemdScheduleServiceName is the name of the system service responsible for scheduling
	SystemdScheduleServiceName = "kubelet"
	// PxLabelNameKey is key for map
	PxLabelNameKey = "name"
	// PxLabelValue portworx pod label
	PxLabelValue = "portworx"
)

var RancherClusterParametersValue *RancherClusterParameters

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

// Will uncomment the Init after PR https://portworx.atlassian.net/browse/PA-1335 is merged.
/*
// Init Initialize the driver
func (r *Rancher) Init(scheduleOpts scheduler.InitOptions) error {
	var err error
	err = r.K8s.Init(scheduleOpts)
	RancherClusterParametersValue, err = r.GetRancherClusterParametersValue()
	if err != nil {
		return err
	}
	rancherClientOpts := rancherClientBase.ClientOpts{
		URL:       RancherClusterParametersValue.Endpoint,
		TokenKey:  RancherClusterParametersValue.Token,
		AccessKey: RancherClusterParametersValue.AccessKey,
		SecretKey: RancherClusterParametersValue.SecretKey,
		Insecure:  true,
	}
	r.client, err = rancherClient.NewClient(&rancherClientOpts)
	if err != nil {
		return err
	}
	return nil
}
*/

// GetRancherClusterParametersValue returns the rancher token, endpoint, secret key, access key
func (r *Rancher) GetRancherClusterParametersValue() (*RancherClusterParameters, error) {
	var rkeData map[string]interface{}
	var rkeParameters RancherClusterParameters
	// TODO Rancher URL for cloud cluster will not be fetched from master node IP
	masterNodeName := node.GetMasterNodes()[0].Name
	endpoint := "https://" + masterNodeName + "/v3"
	rancherURL := "https://" + masterNodeName + "/v3-public/localProviders/local?action=login"
	//TODO: Get the values from config map after https://github.com/portworx/torpedo/pull/1517 is merged
	username := ""
	password := ""
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	body := strings.NewReader(fmt.Sprintf(`{"username":"%s","password":"%s"}`, username, password))
	req, err := http.NewRequest("POST", rancherURL, body)
	if err != nil {
		log.Errorf("Failed to create rancher POST request:", err)
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("Failed to send request:", err)
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("Failed to read response:", err)
		return nil, err
	}
	response := string(respBody)
	err = json.Unmarshal([]byte(response), &rkeData)
	if err != nil {
		log.Errorf("Error:", err)
		return nil, err
	}
	rkeParameters.Endpoint = endpoint
	rkeToken, res := rkeData["token"].(string)
	if res != true {
		return nil, fmt.Errorf("token in rkeData is not of string type")
	}
	if len(strings.Split(rkeToken, ":")) != 2 {
		return nil, fmt.Errorf("invalid RKE token value")
	}
	rkeParameters.Token = rkeToken
	rkeParameters.AccessKey = strings.Split(rkeToken, ":")[0]
	rkeParameters.SecretKey = strings.Split(rkeToken, ":")[1]
	return &rkeParameters, nil
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
func (r *Rancher) CreateRancherProject(projectName string, projectDescription string, clusterName string) (*rancherClient.Project, error) {
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
	// requires 2>&1 since docker logs command send the logs to stdrr instead of sdout
	cmd := fmt.Sprintf("docker logs %s > %s/kubelet.log 2>&1", SystemdScheduleServiceName, location)
	_, err := driver.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         kube.DefaultTimeout,
		TimeBeforeRetry: kube.DefaultRetryInterval,
		Sudo:            true,
	})
	return err
}

func init() {
	r := &Rancher{}
	scheduler.Register(schedulerName, r)
}
