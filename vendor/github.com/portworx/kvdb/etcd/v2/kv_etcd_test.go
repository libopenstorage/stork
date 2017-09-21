package etcdv2

import (
	"testing"

	"github.com/portworx/kvdb/test"
)

func TestAll(t *testing.T) {
	test.Run(New, t)
	// Run the basic tests with an authenticated etcd
	// Uncomment if you have an auth enabled etcd setup. Checkout the test/kv.go for options
	//test.RunAuth(New, t)
}
