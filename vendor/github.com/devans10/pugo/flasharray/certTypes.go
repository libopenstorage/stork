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

// Certificate is a struct for the cert endpoint data
// returned by the array
type Certificate struct {
	Status      string `json:"status,omitempty"`
	IssuedTo    string `json:"issued_to,omitempty"`
	ValidFrom   string `json:"valid_from,omitempty"`
	Name        string `json:"name,omitempty"`
	Locality    string `json:"locality,omitempty"`
	Country     string `json:"country,omitempty"`
	IssuedBy    string `json:"issued_by,omitempty"`
	ValidTo     string `json:"valid_to,omitempty"`
	State       string `json:"state,omitempty"`
	KeySize     int    `json:"key_size,omitempty"`
	OrgUnit     string `json:"organizational_unit,omitempty"`
	Org         string `json:"organization,omitempty"`
	Email       string `json:"email,omitempty"`
	Certificate string `json:"certificate,omitempty"`
	CSR         string `json:"certificate_signing_request,omitempty"`
	CommonName  string `json:"common_name,omitempty"`
	SelfSigned  bool   `json:"self_signed,omitempty"`
}
