package flasharray

type RealmsServices struct {
	client *Client
}

func (realms *RealmsServices) ListAllAvailableRealms(params map[string]string) ([]RealmResponse, error) {
	req, err := realms.client.NewRequest("GET", "realms", params, nil)
	if err != nil {
		return nil, err
	}
	m := []RealmResponse{}
	_, err = realms.client.Do(req, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}
