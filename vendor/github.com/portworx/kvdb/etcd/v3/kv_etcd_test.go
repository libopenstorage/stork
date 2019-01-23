package etcdv3

import (
	"fmt"
	"testing"
	"time"

	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/etcd/common"
	"github.com/portworx/kvdb/test"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	// etcd is sending the error ErrTimeout over grpc instead of the actual ErrGRPCTimeout
	// hence isRetryNeeded cannot do an actual error comparison
	retry, err = isRetryNeeded(fmt.Errorf("etcdserver: request timed out"), fn, key, retryCount)
	assert.True(t, retry, "Expected a retry")

	// rpctypes.ErrGRPCTimeout
	retry, err = isRetryNeeded(rpctypes.ErrGRPCTimeout, fn, key, retryCount)
	assert.EqualError(t, rpctypes.ErrGRPCTimeout, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")

	// rpctypes.ErrGRPCNoLeader
	retry, err = isRetryNeeded(rpctypes.ErrGRPCNoLeader, fn, key, retryCount)
	assert.EqualError(t, rpctypes.ErrGRPCNoLeader, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")

	// rpctypes.ErrGRPCEmptyKey
	retry, err = isRetryNeeded(rpctypes.ErrGRPCEmptyKey, fn, key, retryCount)
	assert.EqualError(t, kvdb.ErrNotFound, err.Error(), "Unexpcted error")
	assert.False(t, retry, "Expected no retry")

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

	// grpc error of ContextDeadlineExceeded
	gErr := status.New(codes.DeadlineExceeded, "context deadline exceeded").Err()
	retry, err = isRetryNeeded(gErr, fn, key, retryCount)
	assert.EqualError(t, gErr, err.Error(), "Unexpcted error")
	assert.True(t, retry, "Expected a retry")

	retry, err = isRetryNeeded(kvdb.ErrNotFound, fn, key, retryCount)
	assert.EqualError(t, kvdb.ErrNotFound, err.Error(), "Unexpcted error")
	assert.False(t, retry, "Expected no retry")

	retry, err = isRetryNeeded(kvdb.ErrValueMismatch, fn, key, retryCount)
	assert.EqualError(t, kvdb.ErrValueMismatch, err.Error(), "Unexpcted error")
	assert.False(t, retry, "Expected no retry")

}

func TestCasWithRestarts(t *testing.T) {
	fmt.Println("casWithRestarts")
	testFn := func(lockChan chan int, kv kvdb.Kvdb, kvPair *kvdb.KVPair, val string) {
		_, err := kv.CompareAndSet(kvPair, kvdb.KVFlags(0), []byte(val))
		assert.NoError(t, err, "CompareAndSet should succeed on an correct value")
		lockChan <- 1
	}
	testWithRestarts(t, testFn)

}

func TestCadWithRestarts(t *testing.T) {
	fmt.Println("cadWithRestarts")
	testFn := func(lockChan chan int, kv kvdb.Kvdb, kvPair *kvdb.KVPair, val string) {
		_, err := kv.CompareAndDelete(kvPair, kvdb.KVFlags(0))
		assert.NoError(t, err, "CompareAndDelete should succeed on an correct value")
		lockChan <- 1
	}
	testWithRestarts(t, testFn)
}

func testWithRestarts(t *testing.T, testFn func(chan int, kvdb.Kvdb, *kvdb.KVPair, string)) {
	key := "foo/cadCasWithRestarts"
	val := "great"
	err := common.TestStart(true)
	assert.NoError(t, err, "Unable to start kvdb")
	kv := newKv(t)

	defer func() {
		kv.DeleteTree(key)
	}()

	kvPair, err := kv.Put(key, []byte(val), 0)
	assert.NoError(t, err, "Unxpected error in Put")

	kvPair, err = kv.Get(key)
	assert.NoError(t, err, "Failed in Get")
	fmt.Println("stopping kvdb")
	err = common.TestStop()
	assert.NoError(t, err, "Unable to stop kvdb")

	lockChan := make(chan int)
	go func() {
		testFn(lockChan, kv, kvPair, val)
	}()

	fmt.Println("starting kvdb")
	err = common.TestStart(false)
	assert.NoError(t, err, "Unable to start kvdb")
	select {
	case <-time.After(10 * time.Second):
		assert.Fail(t, "Unable to take a lock whose session is expired")
	case <-lockChan:
	}

}

func newKv(t *testing.T) kvdb.Kvdb {
	kv, err := New("pwx/test", nil, nil, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	return kv
}
