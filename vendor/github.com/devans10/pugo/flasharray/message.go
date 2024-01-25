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

// MessageService struct for the message API endpoints
type MessageService struct {
	client *Client
}

// ListMessages Lists alert events, audit records, and user login sessions
func (a *MessageService) ListMessages(params map[string]string) ([]Message, error) {

	req, _ := a.client.NewRequest("GET", "message", params, nil)
	m := []Message{}
	if _, err := a.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// SetMessage Modifies a message
func (a *MessageService) SetMessage(id int, data interface{}) (*Message, error) {

	path := fmt.Sprintf("message/%d", id)
	req, _ := a.client.NewRequest("PUT", path, nil, data)
	m := &Message{}
	if _, err := a.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// FlagMessage Flags a given message
func (a *MessageService) FlagMessage(id int) (*Message, error) {

	data := map[string]bool{"flagged": true}
	m, err := a.SetMessage(id, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// UnflagMessage Unflags a given message
func (a *MessageService) UnflagMessage(id int) (*Message, error) {

	data := map[string]bool{"flagged": false}
	m, err := a.SetMessage(id, data)
	if err != nil {
		return nil, err
	}

	return m, err
}
