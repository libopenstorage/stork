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

// VgroupService struct for vgroup API endpoints
type VgroupService struct {
	client *Client
}

// CreateVgroup creates a Vgroup
func (v *VgroupService) CreateVgroup(name string) (*Vgroup, error) {

	path := fmt.Sprintf("vgroup/%s", name)
	req, _ := v.client.NewRequest("POST", path, nil, nil)
	m := &Vgroup{}
	_, err := v.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// DestroyVgroup destroys a Vgroup
func (v *VgroupService) DestroyVgroup(name string) (*Vgroup, error) {

	path := fmt.Sprintf("vgroup/%s", name)
	req, _ := v.client.NewRequest("DELETE", path, nil, nil)
	m := &Vgroup{}
	_, err := v.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// EradicateVgroup eradicates a deleted Vgroup
func (v *VgroupService) EradicateVgroup(vgroup string) (*Vgroup, error) {

	data := map[string]bool{"eradicate": true}
	path := fmt.Sprintf("vgroup/%s", vgroup)
	req, _ := v.client.NewRequest("DELETE", path, nil, data)
	m := &Vgroup{}
	_, err := v.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// GetVgroup lists Vgroup attributes
func (v *VgroupService) GetVgroup(name string) (*Vgroup, error) {

	path := fmt.Sprintf("vgroup/%s", name)
	req, _ := v.client.NewRequest("GET", path, nil, nil)
	m := &Vgroup{}
	_, err := v.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListVgroups lists attributes for Vgroups
func (v *VgroupService) ListVgroups() ([]Vgroup, error) {

	req, _ := v.client.NewRequest("GET", "vgroup", nil, nil)
	m := []Vgroup{}
	_, err := v.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// RecoverVgroup recovers deleted vgroup
func (v *VgroupService) RecoverVgroup(vgroup string) (*Vgroup, error) {

	data := map[string]string{"action": "recover"}
	m, err := v.SetVgroup(vgroup, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// RenameVgroup renames a vgroup
func (v *VgroupService) RenameVgroup(vgroup string, name string) (*Vgroup, error) {

	data := map[string]string{"name": name}
	m, err := v.SetVgroup(vgroup, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// SetVgroup modifies vgroup attribute
func (v *VgroupService) SetVgroup(name string, data interface{}) (*Vgroup, error) {

	path := fmt.Sprintf("vgroup/%s", name)
	req, _ := v.client.NewRequest("PUT", path, nil, data)
	m := &Vgroup{}
	_, err := v.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}
