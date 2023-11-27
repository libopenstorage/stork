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

// ConsoleLock type console_lock describes the console_lock status of the array.
type ConsoleLock struct {
	ConsoleLock string `json:"console_lock"`
}

// Array gives information about the array
type Array struct {
	ID        string `json:"id,omitempty"`
	ArrayName string `json:"array_name,omitempty"`
	Version   string `json:"version,omitempty"`
	Revision  string `json:"revision,omitempty"`
	Time      string `json:"time,omitempty"`

	// Controllers
	Mode   string `json:"mode,omitempty"`
	Model  string `json:"model,omitempty"`
	Name   string `json:"name,omitempty"`
	Status string `json:"status,omitempty"`

	// Space
	Capacity         int     `json:"capacity,omitempty"`
	DataReduction    float64 `json:"data_reduction,omitempty"`
	Hostname         string  `json:"hostname,omitempty"`
	Parity           float64 `json:"parity,omitempty"`
	SharedSpace      int     `json:"shared_space,omitempty"`
	Snapshots        int     `json:"snapshots,omitempty"`
	System           int     `json:"system,omitempty"`
	ThinProvisioning float64 `json:"thin_provisioning,omitempy"`
	Total            int     `json:"total,omitempty"`
	TotalReduction   float64 `json:"total_reduction,omitempty"`
	Volumes          int     `json:"volumes,omitempty"`

	// Monitor
	SanUsecPerReadOp  int `json:"san_usec_per_read_op,omitempty"`
	SanUsecPerWriteOp int `json:"san_usec_per_write_op,omitempty"`
	UsecPerReadOp     int `json:"usec_per_read_op,omitempty"`
	UsecPerWriteOp    int `json:"usec_per_write_op,omitempty"`
	QueueDepth        int `json:"queue_depth,omitempty"`
	ReadsPerSec       int `json:"reads_per_sec,omitempty"`
	WritesPerSec      int `json:"writes_per_sec,omitempty"`
	InputPerSec       int `json:"input_per_sec,omitempty"`
	OutputPerSec      int `json:"output_per_sec,omitempty"`

	// Metrics returned if action=monitor,size=true
	BytesPerRead  int `json:"bytes_per_read,omitempty"`
	BytesPerWrite int `json:"bytes_per_write,omitempty"`
	BytesPerOp    int `json:"bytes_per_op,omitempty"`
}

// Phonehome struct is the information returned by array
type Phonehome struct {
	Phonehome string `json:"phonehome,omitempty"`
	Status    string `json:"status,omitempty"`
	Action    string `json:"action,omitempty"`
}

// RemoteAssist struct for information returned by array
type RemoteAssist struct {
	Status string `json:"status,omitempty"`
	Name   string `json:"name,omitempty"`
	Port   string `json:"port,omitempty"`
}

// ArrayConnection struct for information returned by array about
// about the connection to a remote array
type ArrayConnection struct {
	Throttled          bool     `json:"throttled"`
	ArrayName          string   `json:"array_name"`
	Version            string   `json:"version"`
	Connected          bool     `json:"connected"`
	ManagementAddress  string   `json:"management_address"`
	ReplicationAddress string   `json:"replication_address"`
	Type               []string `json:"type"`
	ID                 string   `json:"id"`
}
