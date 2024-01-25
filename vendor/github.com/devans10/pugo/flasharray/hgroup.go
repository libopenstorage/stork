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

// HostgroupService struct for hgroup API endpoints
type HostgroupService struct {
	client *Client
}

// ConnectHostgroup connects a Volume to a hostgroup
func (h *HostgroupService) ConnectHostgroup(hgroup string, volume string, data interface{}) (*ConnectedVolume, error) {

	path := fmt.Sprintf("hgroup/%s/volume/%s", hgroup, volume)
	req, _ := h.client.NewRequest("POST", path, nil, data)
	m := &ConnectedVolume{}
	_, err := h.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// CreateHostgroup creates a new hostgroup
func (h *HostgroupService) CreateHostgroup(name string, data interface{}) (*Hostgroup, error) {

	path := fmt.Sprintf("hgroup/%s", name)
	req, _ := h.client.NewRequest("POST", path, nil, data)
	m := &Hostgroup{}
	_, err := h.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// DeleteHostgroup deletes a hostgroup
func (h *HostgroupService) DeleteHostgroup(name string) (*Hostgroup, error) {

	path := fmt.Sprintf("hgroup/%s", name)
	req, _ := h.client.NewRequest("DELETE", path, nil, nil)
	m := &Hostgroup{}
	_, err := h.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// DisconnectHostgroup disconnects a volume from a hostgroup
func (h *HostgroupService) DisconnectHostgroup(hgroup string, volume string) (*ConnectedVolume, error) {

	path := fmt.Sprintf("hgroup/%s/volume/%s", hgroup, volume)
	req, _ := h.client.NewRequest("DELETE", path, nil, nil)
	m := &ConnectedVolume{}
	_, err := h.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// GetHostgroup returns a map of the hostgroup attributes
// see API reference on array for list of valid parameters
func (h *HostgroupService) GetHostgroup(name string, params map[string]string) (*Hostgroup, error) {

	path := fmt.Sprintf("hgroup/%s", name)
	req, _ := h.client.NewRequest("GET", path, params, nil)
	m := &Hostgroup{}
	_, err := h.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// AddHostgroup adds a hostgroup to a Protection Group
func (h *HostgroupService) AddHostgroup(hgroup string, pgroup string) (*HostgroupPgroup, error) {

	path := fmt.Sprintf("hgroup/%s/pgroup/%s", hgroup, pgroup)
	req, _ := h.client.NewRequest("POST", path, nil, nil)
	m := &HostgroupPgroup{}
	_, err := h.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// RemoveHostgroup removes a hostgroup from a protection group.
func (h *HostgroupService) RemoveHostgroup(hgroup string, pgroup string) (*HostgroupPgroup, error) {

	path := fmt.Sprintf("hgroup/%s/pgroup/%s", hgroup, pgroup)
	req, _ := h.client.NewRequest("DELETE", path, nil, nil)
	m := &HostgroupPgroup{}
	_, err := h.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListHostgroupConnections lists the hostgroup volume connections
func (h *HostgroupService) ListHostgroupConnections(hgroup string) ([]HostgroupConnection, error) {

	path := fmt.Sprintf("hgroup/%s/volume", hgroup)
	req, _ := h.client.NewRequest("GET", path, nil, nil)
	m := []HostgroupConnection{}
	_, err := h.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListHostgroups lists hostgroups
func (h *HostgroupService) ListHostgroups(params map[string]string) ([]Hostgroup, error) {

	req, _ := h.client.NewRequest("GET", "hgroup", params, nil)
	m := []Hostgroup{}
	_, err := h.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// RenameHostgroup renames a hostgroup
func (h *HostgroupService) RenameHostgroup(hgroup string, name string) (*Hostgroup, error) {

	data := map[string]string{"name": name}
	m, err := h.SetHostgroup(hgroup, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// SetHostgroup modifies the specified hostgroup's attributes
func (h *HostgroupService) SetHostgroup(name string, data interface{}) (*Hostgroup, error) {

	path := fmt.Sprintf("hgroup/%s", name)
	req, _ := h.client.NewRequest("PUT", path, nil, data)
	m := &Hostgroup{}
	_, err := h.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}
