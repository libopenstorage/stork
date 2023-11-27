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

// Host struct for the host object returned from the array
type Host struct {
	Name           string   `json:"name,omitempty"`
	Wwn            []string `json:"wwn,omitempty"`
	Iqn            []string `json:"iqn,omitempty"`
	Nqn            []string `json:"nqn,omitempty"`
	HostPassword   string   `json:"host_password,omitempty"`
	HostUser       string   `json:"host_user,omitempty"`
	Personality    string   `json:"personality,omitempty"`
	PreferredArray []string `json:"preferred_array,omitempty"`
	TargetPassword string   `json:"target_password,omitempty"`
	TargetUser     string   `json:"target_user,omitempty"`
	Hgroup         string   `json:"hgroup,omitempty"`

	// Metrics returned with the action=monitor flag
	WritesPerSec      *int   `json:"writes_per_sec,omitempty"`
	ReadsPerSec       *int   `json:"reads_per_sec,omitempty"`
	UsecPerWriteOp    *int   `json:"usec_per_write_op,omitempty"`
	UsecPerReadOp     *int   `json:"usec_per_read_op,omitempty"`
	SanUsecPerReadOp  *int   `json:"san_usec_per_read_op,omitempty"`
	SanUsecPerWriteOp *int   `json:"san_usec_per_write_op,omitempty"`
	QueueDepth        *int   `json:"queue_depth,omitempty"`
	OutputPerSec      *int   `json:"output_per_sec,omitempty"`
	InputPerSec       *int   `json:"input_per_sec,omitempty"`
	Time              string `json:"time,omitempty"`

	// Metrics returned with the space=True flag
	Snapshots        *int     `json:"snapshots,omitempty"`
	Volumes          *int     `json:"volumes,omitempty"`
	DataReduction    *float64 `json:"data_reduction,omitempty"`
	Total            *int     `json:"total,omitempty"`
	ThinProvisioning *float64 `json:"thin_provisioning,omitempty"`
	TotalReduction   *float64 `json:"total_reduction,omitempty"`

	// Metrics returned if action=monitor,size=true
	BytesPerRead  *int `json:"bytes_per_read,omitempty"`
	BytesPerWrite *int `json:"bytes_per_write,omitempty"`
	BytesPerOp    *int `json:"bytes_per_op,omitempty"`
}

// ConnectedVolume struct for object returned from the array
type ConnectedVolume struct {
	Vol    string `json:"vol,omitempty"`
	Name   string `json:"name,omitempty"`
	Lun    int    `json:"lun,omitempty"`
	Hgroup string `json:"hgroup,omitempty"`
}

// HostPgroup struct for object returned from the array
type HostPgroup struct {
	Name   string `json:"name,omitempty"`
	Pgroup string `json:"protection_group,omitempty"`
}
