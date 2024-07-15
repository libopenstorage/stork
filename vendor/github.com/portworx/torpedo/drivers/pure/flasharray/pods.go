package flasharray

import "github.com/portworx/torpedo/pkg/log"

type PodServices struct {
	client *Client
}

func (p *PodServices) CreatePod(params map[string]string, data interface{}) (*[]PodResponse, error) {
	req, _ := p.client.NewRequest("POST", "pods", params, data)
	m := &[]PodResponse{}
	_, err := p.client.Do(req, m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (p *PodServices) ListAllAvailablePods(params map[string]string) ([]PodResponse, error) {
	req, err := p.client.NewRequest("GET", "pods", params, nil)
	if err != nil {
		return nil, err
	}
	m := []PodResponse{}
	_, err = p.client.Do(req, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}
func (p *PodServices) PatchPod(patchParams map[string]string, data interface{}) ([]PodResponse, error) {
	req, err := p.client.NewRequest("PATCH", "pods", patchParams, data)
	if err != nil {
		return nil, err
	}
	m := []PodResponse{}
	_, err = p.client.Do(req, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}
func (p *PodServices) DeletePod(patchParams, deleteParams map[string]string, data interface{}) error {
	podResp, err := p.PatchPod(patchParams, data)
	if err != nil {
		return err
	}
	log.InfoD("Pod [%v] Patched Successfully", podResp[0].Items[0].Name)
	req, _ := p.client.NewRequest("DELETE", "pods", deleteParams, nil)
	m := &[]PodResponse{}
	_, err = p.client.Do(req, m)
	if err != nil {
		return err
	}

	return nil
}
