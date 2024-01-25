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

// VolumeService struct for volume API endpoints
type VolumeService struct {
	client *Client
}

// SetVolume is a helper function that sets the parameter passed in the data interface
// of the volume in the name argument.
// A Volume object is returned with the new values.
func (v *VolumeService) SetVolume(name string, data interface{}) (*Volume, error) {

	path := fmt.Sprintf("volume/%s", name)
	req, _ := v.client.NewRequest("PUT", path, nil, data)
	m := &Volume{}
	_, err := v.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// CreateSnapshot function creates a volume snapshot of the volume passed in the argument.
func (v *VolumeService) CreateSnapshot(volume string, suffix string) (*Volume, error) {
	volumes := []string{volume}
	m, err := v.CreateSnapshots(volumes, suffix)
	if err != nil {
		return nil, err
	}

	return &m[0], err
}

// CreateSnapshots function will create a snapshot of all the volumes passed in the volumes slice.
// an array of volume objects is returned.
func (v *VolumeService) CreateSnapshots(volumes []string, suffix string) ([]Volume, error) {

	data := make(map[string]interface{})
	data["snap"] = true
	data["source"] = volumes
	data["suffix"] = suffix
	req, _ := v.client.NewRequest("POST", "volume", nil, data)
	m := []Volume{}
	_, err := v.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// CreateVolume function will create a volume of the given size.  The size is an integer in bytes
func (v *VolumeService) CreateVolume(name string, size int) (*Volume, error) {

	path := fmt.Sprintf("volume/%s", name)
	data := map[string]int{"size": size}
	req, _ := v.client.NewRequest("POST", path, nil, data)
	m := &Volume{}
	_, err := v.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// CreateConglomerateVolume creates a conglomerate volume.
// This is not a typical volume thus there is no size.  It's main purpose to connect to a
// host/hgroup to create a PE LUN.  Once the conglomerate volume is connected to a
// host/hgroup, it is used as a protocol-endpoint to connect a vvol to a host/hgroup to
// allow traffic.
func (v *VolumeService) CreateConglomerateVolume(name string) (*Volume, error) {

	path := fmt.Sprintf("volume/%s", name)
	data := map[string]bool{"protocol_endpoint": true}
	req, _ := v.client.NewRequest("POST", path, nil, data)
	m := &Volume{}
	_, err := v.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// CopyVolume clones a volume and return a dictionary describing the new volume.
func (v *VolumeService) CopyVolume(dest string, source string, overwrite bool) (*Volume, error) {

	path := fmt.Sprintf("volume/%s", dest)
	data := map[string]interface{}{"source": source, "overwrite": overwrite}
	req, _ := v.client.NewRequest("POST", path, nil, data)
	m := &Volume{}
	_, err := v.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// DeleteVolume deletes an existing volume or snapshot
func (v *VolumeService) DeleteVolume(name string) (*Volume, error) {

	path := fmt.Sprintf("volume/%s", name)
	req, _ := v.client.NewRequest("DELETE", path, nil, nil)
	m := &Volume{}
	_, err := v.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// EradicateVolume eradicates a deleted volume or snapshot
func (v *VolumeService) EradicateVolume(name string) (*Volume, error) {

	path := fmt.Sprintf("volume/%s", name)
	data := map[string]bool{"eradicate": true}
	req, _ := v.client.NewRequest("DELETE", path, nil, data)
	m := &Volume{}
	_, err := v.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ExtendVolume increases the size of the volume
func (v *VolumeService) ExtendVolume(name string, size int) (*Volume, error) {

	data := make(map[string]interface{})
	data["size"] = size
	data["truncate"] = false
	m, err := v.SetVolume(name, data)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// GetVolume lists attributes of the specified volume
func (v *VolumeService) GetVolume(name string, params map[string]string) (*Volume, error) {

	path := fmt.Sprintf("volume/%s", name)
	if params["action"] == "monitor" {
		m, err := v.MonitorVolume(name, params)
		if err != nil {
			return nil, err
		}
		vol := m[0]
		return &vol, nil
	}
	req, _ := v.client.NewRequest("GET", path, params, nil)
	m := &Volume{}
	_, err := v.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// MonitorVolume returns metrics of the specified volume.  This is similar to GetVolume when
// params = {"action": "monitor"}, but that endpoint returns an array instead of a single
// Volume struct.
func (v *VolumeService) MonitorVolume(name string, params map[string]string) ([]Volume, error) {

	path := fmt.Sprintf("volume/%s", name)
	p := map[string]string{"action": "monitor"}
	for k, v := range params {
		p[k] = v
	}
	req, _ := v.client.NewRequest("GET", path, p, nil)
	m := []Volume{}
	_, err := v.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// AddVolume adds a volume to a protection group
func (v *VolumeService) AddVolume(volume string, pgroup string) (*VolumePgroup, error) {

	path := fmt.Sprintf("volume/%s/pgroup/%s", volume, pgroup)
	req, _ := v.client.NewRequest("POST", path, nil, nil)
	m := &VolumePgroup{}
	_, err := v.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// RemoveVolume removes a volume from a protection group
func (v *VolumeService) RemoveVolume(volume string, pgroup string) (*VolumePgroup, error) {

	path := fmt.Sprintf("volume/%s/pgroup/%s", volume, pgroup)
	req, _ := v.client.NewRequest("DELETE", path, nil, nil)
	m := &VolumePgroup{}
	_, err := v.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListVolumeBlockDiff lists Volume Block Differences
func (v *VolumeService) ListVolumeBlockDiff(name string, params map[string]string) ([]Block, error) {
	path := fmt.Sprintf("volume/%s/diff", name)
	req, _ := v.client.NewRequest("GET", path, params, nil)
	m := []Block{}
	_, err := v.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListVolumePrivateConnections lists Volume Private Connections
func (v *VolumeService) ListVolumePrivateConnections(name string) ([]Connection, error) {
	path := fmt.Sprintf("volume/%s/host", name)
	req, _ := v.client.NewRequest("GET", path, nil, nil)
	m := []Connection{}
	_, err := v.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListVolumeSharedConnections lists Volume Shared Connections
func (v *VolumeService) ListVolumeSharedConnections(name string) ([]Connection, error) {
	path := fmt.Sprintf("volume/%s/hgroup", name)
	req, _ := v.client.NewRequest("GET", path, nil, nil)
	m := []Connection{}
	_, err := v.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// ListVolumes lists attributes for volumes
func (v *VolumeService) ListVolumes(params map[string]string) ([]Volume, error) {

	req, _ := v.client.NewRequest("GET", "volume", params, nil)
	m := []Volume{}
	_, err := v.client.Do(req, &m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// RenameVolume renames a volume
func (v *VolumeService) RenameVolume(volume string, name string) (*Volume, error) {

	data := map[string]string{"name": name}
	m, err := v.SetVolume(volume, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// RecoverVolume recovers a deleted volume
func (v *VolumeService) RecoverVolume(volume string) (*Volume, error) {

	params := map[string]string{"action": "recover"}
	path := fmt.Sprintf("volume/%s", volume)
	req, _ := v.client.NewRequest("PUT", path, params, nil)
	m := &Volume{}
	_, err := v.client.Do(req, m, false)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// TruncateVolume decreses the size of a volume
// WARNING!!
// Potential data loss
func (v *VolumeService) TruncateVolume(name string, size int) (*Volume, error) {

	data := make(map[string]interface{})
	data["size"] = size
	data["truncate"] = true
	m, err := v.SetVolume(name, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// MoveVolume moves a volume to a different container
func (v *VolumeService) MoveVolume(name string, container string) (*Volume, error) {

	data := map[string]string{"container": container}
	m, err := v.SetVolume(name, data)
	if err != nil {
		return nil, err
	}

	return m, err
}
