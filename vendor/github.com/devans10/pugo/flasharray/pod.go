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

// PodService struct for pod API endpoints
type PodService struct {
	client *Client
}

// ListPods Lists the attributes or displays the performance monitoring details for pods
func (p *PodService) ListPods(params map[string]string) ([]Pod, error) {

	req, _ := p.client.NewRequest("GET", "pod", params, nil)
	m := []Pod{}
	if _, err := p.client.Do(req, &m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// GetPod Lists the attributes or displays the performance monitoring details for the specified pod
func (p *PodService) GetPod(name string, params map[string]string) (*Pod, error) {

	path := fmt.Sprintf("pod/%s", name)
	req, _ := p.client.NewRequest("GET", path, params, nil)
	m := &Pod{}
	if _, err := p.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// CreatePod Creates a new pod
func (p *PodService) CreatePod(pod string, data interface{}) (*Pod, error) {

	path := fmt.Sprintf("pod/%s", pod)
	req, _ := p.client.NewRequest("POST", path, nil, data)
	m := &Pod{}
	if _, err := p.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// ConnectPod Stretches a pod to a peer array
func (p *PodService) ConnectPod(pod string, array string) (*Pod, error) {

	path := fmt.Sprintf("pod/%s/array/%s", pod, array)
	req, _ := p.client.NewRequest("POST", path, nil, nil)
	m := &Pod{}
	if _, err := p.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// SetPod Modifies a pod
func (p *PodService) SetPod(pod string, data interface{}) (*Pod, error) {

	path := fmt.Sprintf("pod/%s", pod)
	req, _ := p.client.NewRequest("PUT", path, nil, data)
	m := &Pod{}
	if _, err := p.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// RenamePod renames a pod
func (p *PodService) RenamePod(pod string, name string) (*Pod, error) {

	data := map[string]string{"name": name}
	m, err := p.SetPod(pod, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// RecoverPod recovers a deleted pod
func (p *PodService) RecoverPod(pod string) (*Pod, error) {

	data := map[string]string{"action": "recover"}
	m, err := p.SetPod(pod, data)
	if err != nil {
		return nil, err
	}

	return m, err
}

// DeletePod deletes a pod
func (p *PodService) DeletePod(pod string) (*Pod, error) {

	path := fmt.Sprintf("pod/%s", pod)
	req, _ := p.client.NewRequest("DELETE", path, nil, nil)
	m := &Pod{}
	if _, err := p.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// EradicatePod eradicates a deleted pod
func (p *PodService) EradicatePod(pod string) (*Pod, error) {

	path := fmt.Sprintf("pod/%s", pod)
	data := map[string]bool{"eradicate": true}
	req, _ := p.client.NewRequest("DELETE", path, nil, data)
	m := &Pod{}
	if _, err := p.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}

// DisconnectPod Disconnects a pod frp, a peer array
func (p *PodService) DisconnectPod(pod string, array string) (*Pod, error) {

	path := fmt.Sprintf("pod/%s/array/%s", pod, array)
	req, _ := p.client.NewRequest("DELETE", path, nil, nil)
	m := &Pod{}
	if _, err := p.client.Do(req, m, false); err != nil {
		return nil, err
	}

	return m, nil
}
