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

// Dirsrv struct for data returned by array
type Dirsrv struct {
	BindUser     string   `json:"bind_user"`
	BindPassword string   `json:"bind_password"`
	BaseDn       string   `json:"base_dn"`
	CheckPeer    bool     `json:"check_peer"`
	Enabled      bool     `json:"enabled"`
	URI          []string `json:"uri"`
}

// DirsrvTest struct for data returned by array
type DirsrvTest struct {
	Output string `json:"output"`
}

// DirsrvRole struct for data returned by array
type DirsrvRole struct {
	Name      string `json:"name,omitempty"`
	Group     string `json:"group,omitempty"`
	GroupBase string `json:"group_base,omitempty"`
}
