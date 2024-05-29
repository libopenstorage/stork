package flasharray

type VolumeServices struct {
	client *Client
}

func (vols *VolumeServices) ListAllAvailableVolumes(params map[string]string, data interface{}) ([]VolResponse, error) {
	req, err := vols.client.NewRequest("GET", "volumes", params, data)
	if err != nil {
		return nil, err
	}
	m := []VolResponse{}
	_, err = vols.client.Do(req, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}
