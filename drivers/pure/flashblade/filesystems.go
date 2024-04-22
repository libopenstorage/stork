package flashblade

type FileSystemService struct {
	client *Client
}

func (fs *FileSystemService) GetAllFileSystems(params map[string]string, data interface{}) ([]FSResponse, error) {
	req, _ := fs.client.NewRequest("GET", "file-systems", params, data)
	m := []FSResponse{}
	_, err := fs.client.Do(req, &m, true)
	if err != nil {
		return nil, err
	}
	return m, nil
}

/* Below set of Functions applicable for Snapshot Scheduling Policies for the filesystem */
// GetSnapshotSchedulingPolicies Get list of Snapshot Scheduling policies of the filesystem
func (fs *FileSystemService) GetSnapshotSchedulingPolicies(params map[string]string, data interface{}) ([]PolicyResponse, error) {
	req, _ := fs.client.NewRequest("GET", "file-systems/policies", params, data)
	m := []PolicyResponse{}
	_, err := fs.client.Do(req, &m, true)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// ApplySnapshotSchedulingPolicies Apply a snapshot scheduling policy to a file system. Only one file system can be mapped to a policy at a time.
func (fs *FileSystemService) ApplySnapshotSchedulingPolicies(policies *Policies) (*PolicyResponse, error) {
	req, _ := fs.client.NewRequest("POST", "file-systems/policies", nil, policies)
	m := &PolicyResponse{}
	_, err := fs.client.Do(req, &m, true)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (fs *FileSystemService) DeleteSnapshotSchedulingPolicies(params map[string]string, data interface{}) (*PolicyResponse, error) {
	req, _ := fs.client.NewRequest("DELETE", "file-systems/policies", params, data)
	m := &PolicyResponse{}
	_, err := fs.client.Do(req, &m, true)
	if err != nil {
		return nil, err
	}
	return m, nil
}
