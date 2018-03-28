package etcdv3

import (
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/etcd/common"
	"github.com/portworx/kvdb/test"
	"github.com/stretchr/testify/assert"

	"golang.org/x/net/context"
)

func TestAll(t *testing.T) {
	test.Run(New, t, common.TestStart, common.TestStop)
	// Run the basic tests with an authenticated etcd
	// Uncomment if you have an auth enabled etcd setup. Checkout the test/kv.go for options
	//test.RunAuth(New, t)
	test.RunControllerTests(New, t)
}

func TestIsRetryNeeded(t *testing.T) {
	fn := "TestIsRetryNeeded"
	key := "test"
	retryCount := 1

	// context.DeadlineExceeded
	retry, err := isRetryNeeded(context.DeadlineExceeded, fn, key, retryCount)
	assert.EqualError(t, context.DeadlineExceeded, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")

	// etcdserver.ErrTimeout
	retry, err = isRetryNeeded(etcdserver.ErrTimeout, fn, key, retryCount)
	assert.EqualError(t, etcdserver.ErrTimeout, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")

	// etcdserver.ErrUnhealthy
	retry, err = isRetryNeeded(etcdserver.ErrUnhealthy, fn, key, retryCount)
	assert.EqualError(t, etcdserver.ErrUnhealthy, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")

	// rpctypes.ErrGRPCTimeout
	retry, err = isRetryNeeded(rpctypes.ErrGRPCTimeout, fn, key, retryCount)
	assert.EqualError(t, rpctypes.ErrGRPCTimeout, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")

	// rpctypes.ErrGRPCEmptyKey
	retry, err = isRetryNeeded(rpctypes.ErrGRPCEmptyKey, fn, key, retryCount)
	assert.EqualError(t, kvdb.ErrNotFound, err.Error(), "Unexpcted error")
	assert.False(t, retry, "Expected a retry")

	// etcd v3.2.x uses following grpc error format
	grpcErr := grpc.Errorf(codes.Unavailable, "desc = some grpc error")
	retry, err = isRetryNeeded(grpcErr, fn, key, retryCount)
	assert.EqualError(t, grpcErr, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")

	// etcd v3.3.x uses the following grpc error format
	grpcErr = status.New(codes.Unavailable, "desc = some grpc error").Err()
	retry, err = isRetryNeeded(grpcErr, fn, key, retryCount)
	assert.EqualError(t, grpcErr, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")
}
