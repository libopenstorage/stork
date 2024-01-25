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

// User struct for object returned by array
type User struct {
	Name string `json:"name"`
	Role string `json:"role"`
	Type string `json:"type"`
}

// Token struct for object returned by array
type Token struct {
	APIToken string `json:"api_token,omitempty"`
	Created  string `json:"created,omitempty"`
	Expires  string `json:"expires,omitempty"`
	Type     string `json:"type,omitempty"`
	Name     string `json:"name,omitempty"`
}

// PublicKey struct for object returned by array
type PublicKey struct {
	Publickey string `json:"publickey,omitempty"`
	Type      string `json:"type,omitempty"`
	Name      string `json:"name,omitempty"`
}

// GlobalAdmin struct for object returned by array
type GlobalAdmin struct {
}

// LockoutInfo struct for object returned by array
type LockoutInfo struct {
}
