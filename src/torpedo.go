package main

import (
	"log"
	"os"
	"scheduler"
)

type testDriverFunc func(scheduler.Driver) error

// Create dynamic volumes.
func testDynamicVolume(d scheduler.Driver) error {
}

// Verify that the volume driver can deal with an uneven number of mounts
// and unmounts and allow the volume to get mounted on another node.
func testRemoteForceMount(d scheduler.Driver) error {
}

// Volume Driver Plugin is down, unavailable - and the client container should
// not be impacted.
func testDriverDown(d scheduler.Driver) error {
}

// Volume driver plugin is down and the client container gets terminated.
// There is a lost unmount call in this case, but the container should i
// be able to come up on another system and use the volume.
func testDriverDownContainerDown(d scheduler.Driver) error {
}

// A container is using a volume on node X.  Node X is now powered off.
func testNodePowerOff(d scheduler.Driver) error {
}

// Storage plugin is down.  Scheduler tries to create a container using the
// provider’s volume.
func testPluginDown(d scheduler.Driver) error {
}

// A container is running on node X.  Node X loses network access and is
// partitioned away.  Node Y that is in the cluster can use the volume for
// another container.
func testNetworkDown(d scheduler.Driver) error {
}

// A container is running on node X.  Node X can only see a subset of the
// storage cluster.  That is, it can see the entire DC/OS cluster, but just the
// storage cluster gets a network partition. Node Y that is in the cluster
// can use the volume for another container.
func testNetworkPartition(d scheduler.Driver) error {
}

// Docker daemon crashes and live restore is disabled.
func testDockerDown(d scheduler.Driver) error {
}

// Docker daemon crashes and live restore is enabled.
func testDockerDownLiveRestore(d scheduler.Driver) error {
}

func run(d scheduler.Driver) error {
	// Add new test functions here.
	testFuncs := []testDriverFunc{
		testDynamicVolume,
		testRemoteForceMount,
	}

	for f := range testFuncs {
		if err := f(d); err != nil {
			return err
		}
	}
}

func main() {
	if d, err := scheduler.Get(os.Args[1]); err != nil {
		log.Errorf("Cannot find driver %v", os.Args[1])
		os.Exit(-1)
	} else {
		d.Init()
	}

	if run(d) != nil {
		return os.Exit(-1)
	}
}
