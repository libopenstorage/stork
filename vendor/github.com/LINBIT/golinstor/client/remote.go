package client

import (
	"context"
	"fmt"

	"github.com/google/go-querystring/query"
)

type LinstorRemote struct {
	RemoteName string `json:"remote_name,omitempty"`
	Url        string `json:"url,omitempty"`
	Passphrase string `json:"passphrase,omitempty"`
	ClusterId  string `json:"cluster_id,omitempty"`
}

type RemoteList struct {
	S3Remotes      []S3Remote      `json:"s3_remotes,omitempty"`
	LinstorRemotes []LinstorRemote `json:"linstor_remotes,omitempty"`
}

type S3Remote struct {
	RemoteName   string `json:"remote_name,omitempty"`
	Endpoint     string `json:"endpoint,omitempty"`
	Bucket       string `json:"bucket,omitempty"`
	Region       string `json:"region,omitempty"`
	AccessKey    string `json:"access_key,omitempty"`
	SecretKey    string `json:"secret_key,omitempty"`
	UsePathStyle bool   `json:"use_path_style,omitempty"`
}

type RemoteProvider interface {
	// GetAll returns the list of all registered remotes.
	GetAll(ctx context.Context, opts ...*ListOpts) (RemoteList, error)
	// GetAllLinstor returns the list of LINSTOR remotes.
	GetAllLinstor(ctx context.Context, opts ...*ListOpts) ([]LinstorRemote, error)
	// GetAllS3 returns the list of S3 remotes.
	GetAllS3(ctx context.Context, opts ...*ListOpts) ([]S3Remote, error)
	// CreateLinstor creates a new LINSTOR remote.
	CreateLinstor(ctx context.Context, create LinstorRemote) error
	// CreateS3 creates a new S3 remote.
	CreateS3(ctx context.Context, create S3Remote) error
	// Delete a named remote.
	Delete(ctx context.Context, remoteName string) error
	// ModifyLinstor modifies the given LINSTOR remote.
	ModifyLinstor(ctx context.Context, remoteName string, modify LinstorRemote) error
	// ModifyS3 modifies the given S3 remote.
	ModifyS3(ctx context.Context, remoteName string, modify S3Remote) error
}

var _ RemoteProvider = &RemoteService{}

type RemoteService struct {
	client *Client
}

func (r *RemoteService) GetAll(ctx context.Context, opts ...*ListOpts) (RemoteList, error) {
	var list RemoteList
	_, err := r.client.doGET(ctx, "/v1/remotes", &list, opts...)
	return list, err
}

func (r *RemoteService) GetAllLinstor(ctx context.Context, opts ...*ListOpts) ([]LinstorRemote, error) {
	var list []LinstorRemote
	_, err := r.client.doGET(ctx, "/v1/remotes/linstor", &list, opts...)
	return list, err
}

func (r *RemoteService) GetAllS3(ctx context.Context, opts ...*ListOpts) ([]S3Remote, error) {
	var list []S3Remote
	_, err := r.client.doGET(ctx, "/v1/remotes/s3", &list, opts...)
	return list, err
}

func (r *RemoteService) CreateLinstor(ctx context.Context, create LinstorRemote) error {
	_, err := r.client.doPOST(ctx, "/v1/remotes/linstor", create)
	return err
}

func (r *RemoteService) CreateS3(ctx context.Context, create S3Remote) error {
	_, err := r.client.doPOST(ctx, "/v1/remotes/s3", create)
	return err
}

func (r *RemoteService) Delete(ctx context.Context, remoteName string) error {
	vals, err := query.Values(&struct {
		RemoteName string `url:"remote_name"`
	}{RemoteName: remoteName})
	if err != nil {
		return fmt.Errorf("failed to encode remote name: %w", err)
	}

	_, err = r.client.doDELETE(ctx, "/v1/remotes?"+vals.Encode(), nil)
	return err
}

func (r *RemoteService) ModifyLinstor(ctx context.Context, remoteName string, modify LinstorRemote) error {
	_, err := r.client.doPUT(ctx, "/v1/remotes/linstor/"+ remoteName, modify)
	return err
}

func (r *RemoteService) ModifyS3(ctx context.Context, remoteName string, modify S3Remote) error {
	_, err := r.client.doPUT(ctx, "/v1/remotes/s3/"+ remoteName, modify)
	return err
}
