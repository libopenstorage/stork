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

// HostService struct for host API endpoints
type HostService struct {
	client *Client
}

// ConnectHost connects a volume to a host
func (h *HostService) ConnectHost(host string, volume string, data interface{}) (*ConnectedVolume, error) {

	path := fmt.Sprintf("host/%s/volume/%s", host, volume)
	req, _ := h.client.NewRequest("POST", path, nil, data)
	m := &ConnectedVolume{}
	_, err := h.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// CreateHost creates a new host
func (h *HostService) CreateHost(name string, data interface{}) (*Host, error) {

	path := fmt.Sprintf("host/%s", name)
	req, _ := h.client.NewRequest("POST", path, nil, data)
	m := &Host{}
	_, err := h.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// DeleteHost deletes a host
func (h *HostService) DeleteHost(name string) (*Host, error) {

	path := fmt.Sprintf("host/%s", name)
	req, _ := h.client.NewRequest("DELETE", path, nil, nil)
	m := &Host{}
	_, err := h.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// DisconnectHost disconnects a volume from a host
func (h *HostService) DisconnectHost(host string, volume string) (*ConnectedVolume, error) {

	path := fmt.Sprintf("host/%s/volume/%s", host, volume)
	req, _ := h.client.NewRequest("DELETE", path, nil, nil)
	m := &ConnectedVolume{}
	_, err := h.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// GetHost returns the attributes of the given host
func (h *HostService) GetHost(name string, params map[string]string) (*Host, error) {

	path := fmt.Sprintf("host/%s", name)
	req, _ := h.client.NewRequest("GET", path, params, nil)
	m := &Host{}
	_, err := h.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// AddHost adds a host to a protection group
func (h *HostService) AddHost(host string, pgroup string) (*HostPgroup, error) {

	path := fmt.Sprintf("host/%s/pgroup/%s", host, pgroup)
	req, _ := h.client.NewRequest("POST", path, nil, nil)
	m := &HostPgroup{}
	_, err := h.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// RemoveHost removes a host from a protection group
func (h *HostService) RemoveHost(host string, pgroup string) (*HostPgroup, error) {

	path := fmt.Sprintf("host/%s/pgroup/%s", host, pgroup)
	req, _ := h.client.NewRequest("DELETE", path, nil, nil)
	m := &HostPgroup{}
	_, err := h.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListHostConnections lists the host's volume  connections
func (h *HostService) ListHostConnections(host string, params map[string]string) ([]ConnectedVolume, error) {

	path := fmt.Sprintf("host/%s/volume", host)
	req, _ := h.client.NewRequest("GET", path, params, nil)
	m := []ConnectedVolume{}
	_, err := h.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListHosts lists the attributes of the hosts
func (h *HostService) ListHosts(params map[string]string) ([]Host, error) {

	req, _ := h.client.NewRequest("GET", "host", params, nil)
	m := []Host{}
	_, err := h.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// RenameHost renames a host
func (h *HostService) RenameHost(host string, name string) (*Host, error) {

	data := map[string]string{"name": name}
	m, err := h.SetHost(host, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// SetHost modifies the attributes of the specified host
func (h *HostService) SetHost(name string, data interface{}) (*Host, error) {

	path := fmt.Sprintf("host/%s", name)
	req, _ := h.client.NewRequest("PUT", path, nil, data)
	m := &Host{}
	_, err := h.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}
