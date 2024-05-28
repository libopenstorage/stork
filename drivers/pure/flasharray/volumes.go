package flasharray

type VolumeServices struct {
	client *Client
}

func (fs *VolumeServices) ListAllAvailableVolumes(params map[string]string, data interface{}) ([]Volumes, error) {
	params["destroyed"] = "false"
	req, err := fs.client.NewRequest("GET", "volumes", params, data)
	if err != nil {
		return nil, err
	}
	m := []Volumes{}
	_, err = fs.client.Do(req, &m, true)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (fs *VolumeServices) ListAllDestroyedVolumes(params map[string]string, data interface{}) ([]Volumes, error) {
	params["destroyed"] = "true"
	req, err := fs.client.NewRequest("GET", "volumes", params, data)
	if err != nil {
		return nil, err
	}
	m := []Volumes{}
	_, err = fs.client.Do(req, &m, true)
	if err != nil {
		return nil, err
	}
	return m, nil
}
