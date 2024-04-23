package flashblade

type FileSystemService struct {
	client *Client
}

func (fs *FileSystemService) GetAllFileSystems(params map[string]string, data interface{}) ([]FSResponse, error) {
	req, err := fs.client.NewRequest("GET", "file-systems", params, data)
	if err != nil {
		return nil, err
	}
	m := []FSResponse{}
	_, err = fs.client.Do(req, &m, true)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// CreateNewFileSystem Creates New Filesystem on the cluster
// fsName : Name of the filesystem that needs to be created
// Data should be Interface map[string]interface{}
/* e.x :
    {
	"nfs": {
		"v3_enabled": true,
		"v4_1_enabled": true,
		"rules": "*(rw,no_root_squash)"
		}
}
*/
func (fs *FileSystemService) CreateNewFileSystem(params map[string]string, data interface{}) ([]FsItem, error) {
	req, err := fs.client.NewRequest("POST", "file-systems", params, data)
	if err != nil {
		return nil, err
	}
	m := []FsItem{}
	_, err = fs.client.Do(req, &m, true)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// DeleteFilesystem Deletes the filesystem present in the cluster
func (fs *FileSystemService) DeleteFilesystem(params map[string]string) error {
	_, err := fs.client.NewRequest("DELETE", "file-systems", params, nil)
	if err != nil {
		return err
	}
	return nil
}

// ModifyFilesystemParms Deletes the filesystem present in the cluster
func (fs *FileSystemService) ModifyFilesystemParameters(params map[string]string, data interface{}) ([]FsItem, error) {
	req, err := fs.client.NewRequest("PATCH", "file-systems", params, data)
	if err != nil {
		return nil, err
	}
	m := []FsItem{}
	_, err = fs.client.Do(req, &m, true)
	if err != nil {
		return nil, err
	}
	return m, nil

}

/* Below set of Functions applicable for Snapshot Scheduling Policies for the filesystem */
// GetSnapshotSchedulingPolicies Get list of Snapshot Scheduling policies of the filesystem
func (fs *FileSystemService) GetSnapshotSchedulingPolicies(params map[string]string, data interface{}) ([]PolicyResponse, error) {
	req, err := fs.client.NewRequest("GET", "file-systems/policies", params, data)
	if err != nil {
		return nil, err
	}
	m := []PolicyResponse{}
	_, err = fs.client.Do(req, &m, true)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// ApplySnapshotSchedulingPolicies Apply a snapshot scheduling policy to a file system. Only one file system can be mapped to a policy at a time.
func (fs *FileSystemService) ApplySnapshotSchedulingPolicies(policies *Policies) (*PolicyResponse, error) {
	req, err := fs.client.NewRequest("POST", "file-systems/policies", nil, policies)
	if err != nil {
		return nil, err
	}
	m := &PolicyResponse{}
	_, err = fs.client.Do(req, &m, true)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (fs *FileSystemService) DeleteSnapshotSchedulingPolicies(params map[string]string, data interface{}) (*PolicyResponse, error) {
	req, err := fs.client.NewRequest("DELETE", "file-systems/policies", params, data)
	if err != nil {
		return nil, err
	}
	m := &PolicyResponse{}
	_, err = fs.client.Do(req, &m, true)
	if err != nil {
		return nil, err
	}
	return m, nil
}
