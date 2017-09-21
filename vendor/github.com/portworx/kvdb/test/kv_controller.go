package test

import (
	"fmt"
	"strings"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/portworx/kvdb"
	"github.com/stretchr/testify/require"
)

const (
	urlPrefix = "http://"
	localhost = "localhost"
)

var (
	names = []string{"infra0", "infra1", "infra2"}
	clientUrls = []string{"http://127.0.0.1:20379", "http://127.0.0.1:21379", "http://127.0.0.1:22379"}
	peerPorts = []string{"20380", "21380", "22380"}
	dataDirs = []string{"/tmp/node0", "/tmp/node1", "/tmp/node2"}
	cmds map[int]*exec.Cmd
)

// RunControllerTests is a test suite for kvdb controller APIs
func RunControllerTests(datastoreInit kvdb.DatastoreInit, t *testing.T) {
	cleanup()
	// Initialize node 0
	cmds = make(map[int]*exec.Cmd)
	index := 0
	initCluster := make(map[string][]string)
	peerURL := urlPrefix + localhost + ":" + peerPorts[index]
	initCluster[names[index]] = []string{peerURL}
	cmd, err := startEtcd(index, initCluster, "new")
	if err != nil {
		t.Fatalf(err.Error())
	}
	cmds[index] = cmd
	kv, err := datastoreInit("pwx/test", []string{clientUrls[index]}, nil, fatalErrorCb())
	if err != nil {
		cmd.Process.Kill()
		t.Fatalf(err.Error())
	}

	testAddMember(kv, t)
	testRemoveMember(kv, t)
	testReAdd(kv, t)
	controllerLog("Stopping all etcd processes")
	for _, cmd := range cmds {
		cmd.Process.Kill()
	}
}

func testAddMember(kv kvdb.Kvdb, t *testing.T) {
	controllerLog("testAddMember")
	// Add node 1
	index := 1
	controllerLog("Adding node 1")
	initCluster, err := kv.AddMember(localhost, peerPorts[index], names[index])
	require.NoError(t, err, "Error on AddMember")
	require.Equal(t, 2, len(initCluster), "Init Cluster length does not match")
	cmd, err := startEtcd(index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[index] = cmd

	// Check the list returned
	list, err := kv.ListMembers()
	require.NoError(t, err, "Error on ListMembers")
	require.Equal(t, 2, len(list), "List returned different length of cluster")
}

func testRemoveMember(kv kvdb.Kvdb, t *testing.T) {
	controllerLog("testRemoveMember")
	// Add node 2
	index := 2
	controllerLog("Adding node 2")
	initCluster, err := kv.AddMember(localhost, peerPorts[index], names[index])
	require.NoError(t, err, "Error on AddMember")
	require.Equal(t, 3, len(initCluster), "Init Cluster length does not match")
	cmd, err := startEtcd(index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[index] = cmd
	// Check the list returned
	list, err := kv.ListMembers()
	require.NoError(t, err, "Error on ListMembers")
	require.Equal(t, 3, len(list), "List returned different length of cluster")

	
	// Remove node 1
	index = 1
	controllerLog("Removing node 1")
	err = kv.RemoveMember(names[index])
	require.NoError(t, err, "Error on RemoveMember")

	cmd, _ = cmds[index]
	cmd.Process.Kill()
	delete(cmds, index)
	// Check the list returned
	list, err = kv.ListMembers()
	require.NoError(t, err, "Error on ListMembers")
	require.Equal(t, 2, len(list), "List returned different length of cluster")
}

func testReAdd(kv kvdb.Kvdb, t *testing.T) {
	controllerLog("testReAdd")
	// Add node 1 back
	index := 1
	controllerLog("Re-adding node 1")
	// For re-adding we need to delete the data-dir of this member
	os.RemoveAll(dataDirs[index])
	initCluster, err := kv.AddMember(localhost, peerPorts[index], names[index])
	require.NoError(t, err, "Error on AddMember")
	require.Equal(t, 3, len(initCluster), "Init Cluster length does not match")
	cmd, err := startEtcd(index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[index] = cmd

	// Check the list returned
	list, err := kv.ListMembers()
	require.NoError(t, err, "Error on ListMembers")
	require.Equal(t, 3, len(list), "List returned different length of cluster")

}

func startEtcd(index int, initCluster map[string][]string, initState string) (*exec.Cmd, error){
	peerURL := urlPrefix + localhost + ":" + peerPorts[index]
	clientURL := clientUrls[index]
	initialCluster := ""
	for name, ip := range initCluster {
		initialCluster = initialCluster + name + "=" + ip[0] + ","
	}
	fmt.Println("Starting etcd for node ", index, "with initial cluster: ", initialCluster)
	initialCluster = strings.TrimSuffix(initialCluster, ",")
	etcdArgs := []string{
		"--name="+
		names[index],
		"--initial-advertise-peer-urls="+
		peerURL,
		"--listen-peer-urls="+
		peerURL,
		"--listen-client-urls="+
		clientURL,
		"--advertise-client-urls="+
		clientURL,
		"--initial-cluster="+
		initialCluster,
		"--data-dir="+
		dataDirs[index],
		"--initial-cluster-state="+
		initState,
	}

	cmd := exec.Command("/tmp/test-etcd/etcd", etcdArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("Failed to run %v(%v) : %v",
			names[index], etcdArgs, err.Error())
	}
	// XXX: Replace with check for etcd is up
	time.Sleep(10 * time.Second)
	return cmd, nil
}

func cleanup() {
	for _, dir := range dataDirs {
		os.RemoveAll(dir)
		os.MkdirAll(dir, 0777)
	}
}

func controllerLog(log string) {
	fmt.Println("--------------------")
	fmt.Println(log)
	fmt.Println("--------------------")
}
