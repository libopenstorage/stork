/*
   Copyright 2018 David Evans

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package flasharray

import (
	"fmt"
)

// AlertService is a struct for the alert endpoints
type AlertService struct {
	client *Client
}

// ListAlerts Lists the email recipients that are designated to receive Purity alert messages
func (a *AlertService) ListAlerts(params map[string]string) ([]Alert, error) {

	req, _ := a.client.NewRequest("GET", "alert", params, nil)
	m := []Alert{}
	if _, err := a.client.Do(req, &m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// GetAlert Lists the information about the specified email recipient
func (a *AlertService) GetAlert(name string) (*Alert, error) {

	path := fmt.Sprintf("alert/%s", name)
	req, _ := a.client.NewRequest("GET", path, nil, nil)
	m := &Alert{}
	if _, err := a.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// CreateAlert Designates and valid email address to receive Purity alert messages
// Up to 20 addresses can be designated in an array.
func (a *AlertService) CreateAlert(alert string, data interface{}) (*Alert, error) {

	path := fmt.Sprintf("alert/%s", alert)
	req, _ := a.client.NewRequest("POST", path, nil, data)
	m := &Alert{}
	if _, err := a.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// TestAlert Tests the ability of the array to send alert messages to all of the designated email addresses.
func (a *AlertService) TestAlert(address string) (*Alert, error) {

	path := fmt.Sprintf("alert/%s", address)
	req, _ := a.client.NewRequest("PUT", path, nil, nil)
	m := &Alert{}
	if _, err := a.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// TestAlerts Tests the ability of the array to send alert messages to all of the designated email addresses.
func (a *AlertService) TestAlerts() (*Alert, error) {

	req, _ := a.client.NewRequest("PUT", "alert", nil, nil)
	m := &Alert{}
	if _, err := a.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// SetAlert Modifies a alert
func (a *AlertService) SetAlert(alert string, data interface{}) (*Alert, error) {

	path := fmt.Sprintf("alert/%s", alert)
	req, _ := a.client.NewRequest("PUT", path, nil, data)
	m := &Alert{}
	if _, err := a.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// EnableAlert enable the transmission of alert messages to the specified email address
func (a *AlertService) EnableAlert(address string) (*Alert, error) {

	data := map[string]bool{"enabled": true}
	m, err := a.SetAlert(address, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// DisableAlert disable the transmission of alert messages to the specified email address
func (a *AlertService) DisableAlert(address string) (*Alert, error) {

	data := map[string]bool{"enabled": false}
	m, err := a.SetAlert(address, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// DeleteAlert deletes a alert
func (a *AlertService) DeleteAlert(address string) (*Alert, error) {

	path := fmt.Sprintf("alert/%s", address)
	req, _ := a.client.NewRequest("DELETE", path, nil, nil)
	m := &Alert{}
	if _, err := a.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}
