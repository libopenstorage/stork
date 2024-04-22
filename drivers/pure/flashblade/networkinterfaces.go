package flashblade

type NetworkInterfaceService struct {
	client *Client
}

func (net *NetworkInterfaceService) ListNetworkInterfaces(params map[string]string, data interface{}) ([]NetResponse, error) {
	req, _ := net.client.NewRequest("GET", "network-interfaces", params, data)
	m := []NetResponse{}
	_, err := net.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (net *NetworkInterfaceService) ListAllArraySubnets(params map[string]string, data interface{}) ([]SubNetResponse, error) {
	req, _ := net.client.NewRequest("GET", "subnets", params, data)
	m := []SubNetResponse{}
	_, err := net.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}
	return m, nil
}
