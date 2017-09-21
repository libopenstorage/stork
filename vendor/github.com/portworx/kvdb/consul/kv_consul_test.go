package consul

import (
	"testing"

	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/test"
	"github.com/stretchr/testify/assert"
)

func TestAll(t *testing.T) {
	kv := test.Run(New, t)
	createUsingCAS(kv, t)
}

func createUsingCAS(kv kvdb.Kvdb, t *testing.T) {
	defer func() {
		_ = kv.DeleteTree("foo")
	}()
	kvPair := &kvdb.KVPair{Key: "foo/createKey", ModifiedIndex: 0}
	_, err := kv.CompareAndSet(kvPair, kvdb.KVModifiedIndex, []byte("some"))
	assert.NoError(t, err, "CompareAndSet failed on create")

	kvPair, err = kv.Get("foo/createKey")
	assert.NoError(t, err, "Failed in Get")

	kvPair.ModifiedIndex = 0
	_, err = kv.CompareAndSet(kvPair, kvdb.KVModifiedIndex, []byte("some"))
	assert.Error(t, err, "CompareAndSet did not fail on create")
}
