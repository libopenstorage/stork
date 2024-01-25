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

// Message struct for the object returned by the array
type Message struct {
	ComponentName string `json:"component_name,omitempty"`
	ComponentType string `json:"component_type,omitempty"`
	Details       string `json:"details,omitempty"`
	Event         string `json:"event,omitempty"`
	ID            int    `json:"id,omitempty"`
	Opened        string `json:"opened,omitempty"`
	User          string `json:"user,omitempty"`
}
