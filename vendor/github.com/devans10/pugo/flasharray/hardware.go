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

// HardwareService struct for hardware API endpoints
type HardwareService struct {
	client *Client
}

// GetDrive lists drive attributes for specified drive
func (n *HardwareService) GetDrive(name string) (*Drive, error) {

	path := fmt.Sprintf("drive/%s", name)
	req, _ := n.client.NewRequest("GET", path, nil, nil)
	m := &Drive{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListDrives lists all drive attributes
func (n *HardwareService) ListDrives() ([]Drive, error) {

	req, _ := n.client.NewRequest("GET", "drive", nil, nil)
	m := []Drive{}
	_, err := n.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// GetHardware lists attributes for specified hardware device
func (n *HardwareService) GetHardware(name string) (*Component, error) {

	path := fmt.Sprintf("hardware/%s", name)
	req, _ := n.client.NewRequest("GET", path, nil, nil)
	m := &Component{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListHardware lists hardware device attributes
func (n *HardwareService) ListHardware() ([]Component, error) {

	req, _ := n.client.NewRequest("GET", "hardware", nil, nil)
	m := []Component{}
	_, err := n.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// SetHardware modifies an attribute for a specified hardware device
func (n *HardwareService) SetHardware(name string, data interface{}) (*Component, error) {

	path := fmt.Sprintf("hardware/%s", name)
	req, _ := n.client.NewRequest("PUT", path, nil, data)
	m := &Component{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}
