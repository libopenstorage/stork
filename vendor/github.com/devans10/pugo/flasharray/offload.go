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

// OffloadService struct for offload API endpoints
type OffloadService struct {
	client *Client
}

// ConnectNFSOffload connects array to NFS Offload server
func (o *OffloadService) ConnectNFSOffload(name string, address string, mountPoint string) (*NFSOffload, error) {

	data := map[string]string{"name": name, "address": address, "mount_point": mountPoint}
	path := fmt.Sprintf("nfs_offload/%s", name)
	req, _ := o.client.NewRequest("POST", path, nil, data)
	m := &NFSOffload{}
	_, err := o.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// DisconnectNFSOffload disconnects array from an NFS Offload server
func (o *OffloadService) DisconnectNFSOffload(name string) (*NFSOffload, error) {

	path := fmt.Sprintf("nfs_offload/%s", name)
	req, _ := o.client.NewRequest("DELETE", path, nil, nil)
	m := &NFSOffload{}
	_, err := o.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// GetNFSOffload lists NFS offload attributes
func (o *OffloadService) GetNFSOffload(name string) (*NFSOffload, error) {

	path := fmt.Sprintf("nfs_offload/%s", name)
	req, _ := o.client.NewRequest("GET", path, nil, nil)
	m := &NFSOffload{}
	_, err := o.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}
