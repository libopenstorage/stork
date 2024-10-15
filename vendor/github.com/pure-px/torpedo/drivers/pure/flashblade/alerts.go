package flashblade

import "fmt"

type AlertService struct {
	client *Client
}

func (a *AlertService) GetAlerts(params map[string]string, data interface{}) ([]AlertResponse, error) {
	req, _ := a.client.NewGetRequests("alerts", params, data)
	fmt.Printf("%v", req)
	m := []AlertResponse{}
	if _, err := a.client.Do(req, &m, true); err != nil {
		return nil, err
	}
	return m, nil
}
