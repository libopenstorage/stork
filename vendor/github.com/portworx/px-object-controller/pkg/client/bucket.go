package client

import (
	"context"

	"github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/openstorage/pkg/correlation"
	"github.com/libopenstorage/openstorage/pkg/grpcutil"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	logrus *log.Logger
)

func init() {
	logrus = correlation.NewPackageLogger("pkg/client")
}

func (c *Client) CreateBucket(ctx context.Context, req *api.BucketCreateRequest) (*api.BucketCreateResponse, error) {
	logrus.WithContext(ctx).Infof("CreateBucket request received. BucketID: %s", req.GetName())

	conn, err := c.getConn()
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Unable to connect to SDK server: %v", err)
	}

	ctx, cancel := grpcutil.WithDefaultTimeout(ctx)
	defer cancel()

	// Check ID is valid with the specified volume capabilities
	return api.NewOpenStorageBucketClient(conn).Create(ctx, req)
}

func (c *Client) DeleteBucket(ctx context.Context, req *api.BucketDeleteRequest) (*api.BucketDeleteResponse, error) {
	logrus.WithContext(ctx).Infof("DeleteBucket request received. BucketID: %s", req.GetBucketId())

	conn, err := c.getConn()
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Unable to connect to SDK server: %v", err)
	}

	ctx, cancel := grpcutil.WithDefaultTimeout(ctx)
	defer cancel()

	// Check ID is valid with the specified volume capabilities
	return api.NewOpenStorageBucketClient(conn).Delete(ctx, req)
}

func (c *Client) AccessBucket(ctx context.Context, req *api.BucketGrantAccessRequest) (*api.BucketGrantAccessResponse, error) {
	logrus.WithContext(ctx).Infof("AccessBucket request received. BucketID: %s", req.GetBucketId())

	conn, err := c.getConn()
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Unable to connect to SDK server: %v", err)
	}

	ctx, cancel := grpcutil.WithDefaultTimeout(ctx)
	defer cancel()

	// Check ID is valid with the specified volume capabilities
	return api.NewOpenStorageBucketClient(conn).GrantAccess(ctx, req)
}

func (c *Client) RevokeBucket(ctx context.Context, req *api.BucketRevokeAccessRequest) (*api.BucketRevokeAccessResponse, error) {
	logrus.WithContext(ctx).Infof("RevokeAccess request received. BucketID: %s", req.GetBucketId())

	conn, err := c.getConn()
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Unable to connect to SDK server: %v", err)
	}

	ctx, cancel := grpcutil.WithDefaultTimeout(ctx)
	defer cancel()

	// Check ID is valid with the specified volume capabilities
	return api.NewOpenStorageBucketClient(conn).RevokeAccess(ctx, req)
}
