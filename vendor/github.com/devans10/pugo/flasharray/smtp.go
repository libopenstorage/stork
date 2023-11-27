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

// SMTPService struct for smtp API endpoints
type SMTPService struct {
	client *Client
}

// GetSMTP Get the attributes of the current smtp server configuration
func (s *SMTPService) GetSMTP() (*SMTP, error) {

	req, _ := s.client.NewRequest("GET", "smtp", nil, nil)
	m := &SMTP{}
	if _, err := s.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// SetSMTP Set the attributes of the current smtp server configuration
func (s *SMTPService) SetSMTP(data interface{}) (*SMTP, error) {

	req, _ := s.client.NewRequest("POST", "smtp", nil, data)
	m := &SMTP{}
	if _, err := s.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}
