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

// Protectiongroup struct for object returned by array
type Protectiongroup struct {
	Name               string                   `json:"name,omitempty"`
	Hgroups            []string                 `json:"hgroups,omitempty"`
	Source             string                   `json:"source,omitempty"`
	Hosts              []string                 `json:"hosts,omitempty"`
	Volumes            []string                 `json:"volumes,omitempty"`
	Targets            []map[string]interface{} `json:"targets,omitempty"`
	Allfor             int                      `json:"all_for,omitempty"`
	Allowed            bool                     `json:"allowed,omitempty"`
	Days               int                      `json:"days,omitempty"`
	Perday             int                      `json:"per_day,omitempty"`
	ReplicateAt        int                      `json:"replicate_at,omitempty"`
	ReplicateBlackout  map[string]int           `json:"replicate_blackout,omitempty"`
	ReplicateEnabled   bool                     `json:"replicate_enabled,omitempty"`
	ReplicateFrequency int                      `json:"replicate_frequency,omitempty"`
	SnapAt             int                      `json:"snap_at,omitempty"`
	SnapEnabled        bool                     `json:"snap_enabled,omitempty"`
	SnapFrequency      int                      `json:"snap_frequency,omitempty"`
	TargetAllfor       int                      `json:"target_all_for,omitempty"`
	TargetDays         int                      `json:"target_days,omitempty"`
	TargetPerDay       int                      `json:"target_per_day,omitempty"`
}

// ProtectiongroupSnapshot struct for object returned by array
type ProtectiongroupSnapshot struct {
	Source  string `json:"source"`
	Name    string `json:"name"`
	Created string `json:"created"`
}
