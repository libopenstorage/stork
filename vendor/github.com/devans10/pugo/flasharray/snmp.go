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

// SnmpService struct for snmp API endpoints
type SnmpService struct {
	client *Client
}

// ListSnmp Lists the designated SNMP managers and their communication and security attributes
func (s *SnmpService) ListSnmp(params map[string]string) ([]SnmpManager, error) {

	req, _ := s.client.NewRequest("GET", "snmp", params, nil)
	m := []SnmpManager{}
	if _, err := s.client.Do(req, &m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// GetSnmp Lists communication and security attributes for the specified SNMP manager
func (s *SnmpService) GetSnmp(name string) (*SnmpManager, error) {

	path := fmt.Sprintf("snmp/%s", name)
	req, _ := s.client.NewRequest("GET", path, nil, nil)
	m := &SnmpManager{}
	if _, err := s.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// CreateSnmp Creates a Purity SNMP manager object that identifies a host (SNMP manager)
// and specifies the protocol attributes for communicating with it.
// Once a manager object is created, the transmission of SNMP traps is immediately enabled.
func (s *SnmpService) CreateSnmp(name string, data interface{}) (*SnmpManager, error) {

	path := fmt.Sprintf("snmp/%s", name)
	req, _ := s.client.NewRequest("POST", path, nil, data)
	m := &SnmpManager{}
	if _, err := s.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// SetSnmp Modifies a SNMP manager
func (s *SnmpService) SetSnmp(name string, data interface{}) (*SnmpManager, error) {

	path := fmt.Sprintf("snmp/%s", name)
	req, _ := s.client.NewRequest("PUT", path, nil, data)
	m := &SnmpManager{}
	if _, err := s.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// DeleteSnmp deletes a SNMP Manager
func (s *SnmpService) DeleteSnmp(name string) (*SnmpManager, error) {

	path := fmt.Sprintf("snmp/%s", name)
	req, _ := s.client.NewRequest("DELETE", path, nil, nil)
	m := &SnmpManager{}
	if _, err := s.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}
