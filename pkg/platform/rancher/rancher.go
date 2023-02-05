package rancher

import (
	"fmt"
	api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	rancherclientbase "github.com/rancher/norman/clientbase"
	"github.com/rancher/norman/types"
	rancherclient "github.com/rancher/rancher/pkg/client/generated/management/v3"
)

type Rancher struct {
	client *rancherclient.Client
}

func (r *Rancher) Init(rancherConfig api.RancherConfig) error {
	var err error
	rancherClientOpts := rancherclientbase.ClientOpts{
		URL:      rancherConfig.Endpoint,
		TokenKey: rancherConfig.Token,
		Insecure: true,
	}
	r.client, err = rancherclient.NewClient(&rancherClientOpts)
	if err != nil {
		return fmt.Errorf("error getting rancher client, %v", err)
	}
	return nil
}

func (r *Rancher) ListProjectNames() (map[string]string, error) {
	projectNameList := make(map[string]string)
	projects, err := r.client.Project.ListAll(&types.ListOpts{})
	if err != nil {
		return nil, fmt.Errorf("error listing rancher projects, %v", err)
	}
	for _, project := range projects.Data {
		projectNameList[project.ID] = project.Name
	}
	return projectNameList, err
}
