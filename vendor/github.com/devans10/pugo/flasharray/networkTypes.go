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

// NetworkInterface struct for object returned by array
type NetworkInterface struct {
	Name     string   `json:"name,omitempty"`
	Address  string   `json:"address,omitempty"`
	Gateway  string   `json:"gateway,omitempty"`
	Netmask  string   `json:"netmask,omitempty"`
	Enabled  bool     `json:"enabled,omitempty"`
	Subnet   string   `json:"subnet,omitempty"`
	Mtu      int      `json:"mtu,omitempty"`
	Services []string `json:"services,omitempty"`
	Slaves   []string `json:"slaves,omitempty"`
	Hwaddr   string   `json:"hwaddr,omitempty"`
	Speed    int      `json:"speed,omitempty"`
}

// Subnet struct for object returned by array
type Subnet struct {
	Name     string   `json:"name,omitempty"`
	Prefix   string   `json:"prefix,omitempty"`
	Enabled  bool     `json:"enabled,omitempty"`
	Vlan     int      `json:"vlan,omitempty"`
	Gateway  string   `json:"gateway,omitempty"`
	Services []string `json:"services,omitempty"`
	Mtu      int      `json:"mtu,omitempty"`
}

// DNS struct for object returned by array
type DNS struct {
	Nameservers []string `json:"nameservers,omitempty"`
	Domain      string   `json:"domain,omitempty"`
}

// Port struct for object returned by array
type Port struct {
	Name     string `json:"name,omitempty"`
	Portal   string `json:"portal,omitempty"`
	Failover string `json:"failover,omitempty"`
	Iqn      string `json:"iqn,omitempty"`
	Wwn      string `json:"wwn,omitempty"`
}
