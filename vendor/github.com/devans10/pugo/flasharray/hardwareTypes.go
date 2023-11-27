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

// Drive struct for data returned by array
type Drive struct {
	Name              string `json:"name"`
	Capacity          int    `json:"capacity,omitempty"`
	Details           string `json:"details,omitempty"`
	LastEvacCompleted string `json:"last_evac_completed,omitempty"`
	LastFailure       string `json:"last_failure,omitempty"`
	Protocol          string `json:"protocol,omitempty"`
	Status            string `json:"status,omitempty"`
	Type              string `json:"type,omitempty"`
}

// Component struct for data returned by array
type Component struct {
	Name        string `json:"name"`
	Details     string `json:"details,omitempty"`
	Identify    string `json:"identify,omitempty"`
	Index       int    `json:"index,omitempty"`
	Model       string `json:"model,omitempty"`
	Serial      string `json:"serial,omitempty"`
	Slot        int    `json:"slot,omitempty"`
	Speed       int    `json:"speed,omitempty"`
	Status      string `json:"status,omitempty"`
	Temperature int    `json:"temperature,omitempty"`
	Voltage     int    `json:"voltage,omitempty"`
}
