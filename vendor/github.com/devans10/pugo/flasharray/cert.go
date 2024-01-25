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

// CertService struct for the cert endpoints
type CertService struct {
	client *Client
}

// ListCert Lists all available certificates
func (c *CertService) ListCert() ([]Certificate, error) {

	req, _ := c.client.NewRequest("GET", "cert", nil, nil)
	m := []Certificate{}
	if _, err := c.client.Do(req, &m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// GetCert Lists attributes or exports the specified certificate
func (c *CertService) GetCert(name string, params map[string]string) (*Certificate, error) {

	path := fmt.Sprintf("cert/%s", name)
	req, _ := c.client.NewRequest("GET", path, params, nil)
	m := &Certificate{}
	if _, err := c.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// GetCSR Constructs a certificate signing request(CSR) for signing by a certificate authority(CA)
func (c *CertService) GetCSR(name string, params map[string]string) (*Certificate, error) {

	path := fmt.Sprintf("cert/certificate_signing_request/%s", name)
	req, _ := c.client.NewRequest("GET", path, params, nil)
	m := &Certificate{}
	if _, err := c.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// CreateCert Creates a self-signed certificate or imports a certificate signed by a certificate authority(CA)
func (c *CertService) CreateCert(name string, data interface{}) (*Certificate, error) {

	path := fmt.Sprintf("cert/%s", name)
	req, _ := c.client.NewRequest("POST", path, nil, data)
	m := &Certificate{}
	if _, err := c.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// SetCert Creates (and optionally initializes) a new certificate
func (c *CertService) SetCert(name string, data interface{}) (*Certificate, error) {

	path := fmt.Sprintf("cert/%s", name)
	req, _ := c.client.NewRequest("PUT", path, nil, data)
	m := &Certificate{}
	if _, err := c.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// DeleteCert deletes a certificate
func (c *CertService) DeleteCert(name string) (*Certificate, error) {

	path := fmt.Sprintf("cert/%s", name)
	req, _ := c.client.NewRequest("DELETE", path, nil, nil)
	m := &Certificate{}
	if _, err := c.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}
