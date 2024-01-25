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

// NetworkService struct for network API endpoints
type NetworkService struct {
	client *Client
}

// DisableNetworkInterface disables a network interface.
// param: iface: Name of network interface to be disabled.
// Returns an object describing the interface.
func (n *NetworkService) DisableNetworkInterface(iface string) (*NetworkInterface, error) {

	data := map[string]bool{"enabled": false}
	m, err := n.SetNetworkInterface(iface, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// EnableNetworkInterface enables a network interface.
// param: iface: Name of network interface to be enabled.
// Returns an object describing the interface.
func (n *NetworkService) EnableNetworkInterface(iface string) (*NetworkInterface, error) {

	data := map[string]bool{"enabled": true}
	m, err := n.SetNetworkInterface(iface, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// GetNetworkInterface lists network interface attributes
func (n *NetworkService) GetNetworkInterface(iface string) (*NetworkInterface, error) {

	path := fmt.Sprintf("network/%s", iface)
	req, _ := n.client.NewRequest("GET", path, nil, nil)
	m := &NetworkInterface{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListNetworkInterfaces list the attributes of the network interfaces
func (n *NetworkService) ListNetworkInterfaces() ([]NetworkInterface, error) {

	req, _ := n.client.NewRequest("GET", "network", nil, nil)
	m := []NetworkInterface{}
	_, err := n.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// SetNetworkInterface modifies network interface attributes
func (n *NetworkService) SetNetworkInterface(iface string, data interface{}) (*NetworkInterface, error) {

	path := fmt.Sprintf("network/%s", iface)
	req, _ := n.client.NewRequest("PUT", path, nil, data)
	m := &NetworkInterface{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// CreateSubnet creates a subnet
// param: subnet - Name of subnet to be created
// param: prefix - Routing prefix of subnet to be created
// note:
// prefix should be specified as an IPv4 CIDR address.
// ("xxx.xxx.xxx.xxx/nn", representing prefix and prefix length)
func (n *NetworkService) CreateSubnet(subnet string, prefix string) (*Subnet, error) {

	data := map[string]string{"prefix": prefix}
	path := fmt.Sprintf("subnet/%s", subnet)
	req, _ := n.client.NewRequest("POST", path, nil, data)
	m := &Subnet{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// DeleteSubnet deletes a subnet
// param: subnet - Name of subnet to be deleted
func (n *NetworkService) DeleteSubnet(subnet string) (*Subnet, error) {

	path := fmt.Sprintf("subnet/%s", subnet)
	req, _ := n.client.NewRequest("DELETE", path, nil, nil)
	m := &Subnet{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// DisableSubnet disables a subnet
// param: subnet: Name of subnet to be disabled.
// Returns an object describing the subnet
func (n *NetworkService) DisableSubnet(subnet string) (*Subnet, error) {

	data := map[string]bool{"enabled": false}
	m, err := n.SetSubnet(subnet, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// EnableSubnet enables a subnet
// param: subnet: Name of subnet to be enabled.
// Returns an object describing the subnet
func (n *NetworkService) EnableSubnet(subnet string) (*Subnet, error) {

	data := map[string]bool{"enabled": true}
	m, err := n.SetSubnet(subnet, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// GetSubnet lists subnet attributes
func (n *NetworkService) GetSubnet(subnet string) (*Subnet, error) {

	path := fmt.Sprintf("subnet/%s", subnet)
	req, _ := n.client.NewRequest("GET", path, nil, nil)
	m := &Subnet{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListSubnets lists attributes of subnets
func (n *NetworkService) ListSubnets() ([]Subnet, error) {

	req, _ := n.client.NewRequest("GET", "subnet", nil, nil)
	m := []Subnet{}
	_, err := n.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// RenameSubnet renames a subnet
// param: subnet: Name of subnet to be renamed.
// param: name: Name to change the subnet to
// Returns an object describing the subnet
func (n *NetworkService) RenameSubnet(subnet string, name string) (*Subnet, error) {

	data := map[string]string{"name": name}
	m, err := n.SetSubnet(subnet, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// SetSubnet modifies subnet attributes
func (n *NetworkService) SetSubnet(subnet string, data interface{}) (*Subnet, error) {

	path := fmt.Sprintf("subnet/%s", subnet)
	req, _ := n.client.NewRequest("PUT", path, nil, data)
	m := &Subnet{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// CreateVlanInterface creates a VLAN Interface
// param: iface - Name of interface to be created
// param: subnet - Subnet to be associated with the new interface
func (n *NetworkService) CreateVlanInterface(iface string, subnet string) (*NetworkInterface, error) {

	data := map[string]string{"subnet": subnet}
	path := fmt.Sprintf("network/vif/%s", iface)
	req, _ := n.client.NewRequest("POST", path, nil, data)
	m := &NetworkInterface{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// DeleteVlanInterface deletes a VLAN Interface
// param: iface - Name of iface to be deleted
func (n *NetworkService) DeleteVlanInterface(iface string) (*NetworkInterface, error) {

	path := fmt.Sprintf("network/vif/%s", iface)
	req, _ := n.client.NewRequest("DELETE", path, nil, nil)
	m := &NetworkInterface{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// GetDNS gets DNS settings
func (n *NetworkService) GetDNS() (*DNS, error) {

	req, _ := n.client.NewRequest("GET", "dns", nil, nil)
	m := &DNS{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// SetDNS modifies DNS settings
func (n *NetworkService) SetDNS(data interface{}) (*DNS, error) {

	req, _ := n.client.NewRequest("PUT", "dns", nil, data)
	m := &DNS{}
	_, err := n.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, err
}

// ListPorts lists attributes of the ports
func (n *NetworkService) ListPorts(data interface{}) ([]Port, error) {

	req, _ := n.client.NewRequest("GET", "port", nil, data)
	m := []Port{}
	_, err := n.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}
