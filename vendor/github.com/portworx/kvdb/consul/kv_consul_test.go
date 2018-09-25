package consul

import (
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/test"
	"github.com/stretchr/testify/assert"
)

var (
	cmd *exec.Cmd
)

func TestAll(t *testing.T) {
	// Run the common test suite
	test.Run(New, t, Start, Stop)

	// Run consul specific tests
	err := Start(true)
	assert.NoError(t, err, "Unable to start kvdb")
	// Wait for kvdb to start
	time.Sleep(5 * time.Second)

	kv, err := New("pwx/test", nil, nil, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	createUsingCAS(kv, t)
	err = Stop()
	assert.NoError(t, err, "Unable to stop kvdb")
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
func Start(removeDir bool) error {
	if removeDir {
		if err := os.RemoveAll("/tmp/consul"); err != nil {
			return err
		}
		if err := os.MkdirAll("/tmp/consul", os.ModeDir); err != nil {
			return err
		}
	}

	//consul agent -server -client=0.0.0.0  -data-dir /opt/consul/data -bind 0.0.0.0 -syslog -bootstrap-expect 1 -advertise 127.0.0.1
	cmd = exec.Command("consul", "agent", "-server", "-advertise", "127.0.0.1", "-bind", "0.0.0.0", "-data-dir", "/tmp/consul", "-bootstrap-expect", "1")
	err := cmd.Start()
	time.Sleep(5 * time.Second)
	return err
}

func Stop() error {
	return cmd.Process.Kill()
}
