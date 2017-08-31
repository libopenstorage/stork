package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"

	"github.com/giantswarm/yochu/systemd"
)

// testDriverFunc runs a specific external storage test case.  It takes
// in a scheduler driver and an external volume provider as arguments.
type testDriverFunc func(scheduler.Driver, volume.Driver) error

const (
	dockerServiceName = "docker.service"

	// Name of the external volume for the Torpedo tests.
	volName = "torpedo_vol"

	// Use the inline volume specification so that we can test
	// volume options being dynamically parsed and used inline.
	dynName = "size=10G,repl=2,name=" + volName
)

var (
	// Docker image to use as the test workload.
	testImage = "torpedo/fio"

	// Test image command line arguments.  This is passed into the testImage.
	testArgs = []string{
		"fio",
		"--blocksize=64k",
		"--directory=/mnt/",
		"--ioengine=libaio",
		"--readwrite=write",
		"--size=1G",
		"--name=test",
		"--verify=meta",
		"--do_verify=1",
		"--verify_pattern=0xDeadBeef",
		"--direct=1",
		"--gtod_reduce=1",
		"--iodepth=1",
		"--randrepeat=1",
	}
)

// Create dynamic volumes.  Make sure that a task can use the dynamic volume
// in the inline format as size=x,repl=x,compress=x,name=foo.
// This test will fail if the storage driver is not able to parse the size correctly.
func testDynamicVolume(
	s scheduler.Driver,
	v volume.Driver,
) error {
	taskName := "testDynamicVolume"

	// Pick the first node to start the task
	nodes, err := s.GetNodes()
	if err != nil {
		return err
	}

	host := nodes[0]

	// Remove any container and volume for this test - previous run may have failed.
	s.DestroyByName(host, taskName)
	v.CleanupVolume(volName)

	t := scheduler.Task{
		Name: taskName,
		IP:   host,
		Img:  testImage,
		Tag:  "latest",
		Cmd:  testArgs,
		Vol: scheduler.Volume{
			Driver: v.String(),
			Name:   dynName,
			Path:   "/mnt/",
			Size:   10240,
		},
	}

	ctx, err := s.Create(t)
	if err != nil {
		return err
	}

	defer func() {
		if ctx != nil {
			s.Destroy(ctx)
		}
		v.CleanupVolume(volName)
	}()

	// Run the task and wait for completion.  This task will exit and
	// must not be re-started by the scheduler.
	if err = s.Run(ctx); err != nil {
		return err
	}

	if ctx.Status != 0 {
		return fmt.Errorf("exit status %v\nStdout: %v\nStderr: %v",
			ctx.Status,
			ctx.Stdout,
			ctx.Stderr,
		)
	}

	// Verify that the volume properties are honored.
	vol, err := s.InspectVolume(host, dynName)
	if err != nil {
		return err
	}

	if vol.Driver != v.String() {
		return fmt.Errorf(
			"dynamic volume creation failed, incorrect volume driver (driver = %v)",
			vol.Driver,
		)
	}
	return nil
}

// Volume Driver Plugin is down, unavailable - and the client container should
// not be impacted.
func testDriverDown(
	s scheduler.Driver,
	v volume.Driver,
) error {
	taskName := "testDriverDown"

	// Pick the first node to start the task
	nodes, err := s.GetNodes()
	if err != nil {
		return err
	}

	host := nodes[0]

	// Remove any container and volume for this test - previous run may have failed.
	s.DestroyByName(host, taskName)
	v.CleanupVolume(volName)

	t := scheduler.Task{
		Name: taskName,
		IP:   host,
		Img:  testImage,
		Tag:  "latest",
		Cmd:  testArgs,
		Vol: scheduler.Volume{
			Driver: v.String(),
			Name:   dynName,
			Path:   "/mnt/",
			Size:   10240,
		},
	}

	ctx, err := s.Create(t)

	if err != nil {
		return err
	}

	defer func() {
		if ctx != nil {
			s.Destroy(ctx)
		}
		v.CleanupVolume(volName)
	}()

	if err = s.Schedule(ctx); err != nil {
		return err
	}

	// Sleep for fio to get going...
	time.Sleep(20 * time.Second)

	// Stop the volume driver.
	log.Printf("Stopping the %v volume driver\n", v.String())
	if err = v.StopDriver(ctx.Task.IP); err != nil {
		return err
	}

	// Sleep for fio to keep going...
	time.Sleep(20 * time.Second)

	// Restart the volume driver.
	log.Printf("Starting the %v volume driver\n", v.String())
	if err = v.StartDriver(ctx.Task.IP); err != nil {
		return err
	}

	log.Printf("Waiting for the test task to exit\n")
	if err = s.WaitDone(ctx); err != nil {
		return err
	}

	if ctx.Status != 0 {
		return fmt.Errorf("exit status %v\nStdout: %v\nStderr: %v",
			ctx.Status,
			ctx.Stdout,
			ctx.Stderr,
		)
	}
	return nil
}

// Volume driver plugin is down and the client container gets terminated.
// There is a lost unmount call in this case. When the volume driver is
// back up, we should be able to detach and delete the volume.
func testDriverDownContainerDown(
	s scheduler.Driver,
	v volume.Driver,
) error {
	taskName := "testDriverDownContainerDown"

	// Pick the first node to start the task
	nodes, err := s.GetNodes()
	if err != nil {
		return err
	}

	host := nodes[0]

	// Remove any container and volume for this test - previous run may have failed.
	s.DestroyByName(host, taskName)
	v.CleanupVolume(volName)

	t := scheduler.Task{
		Name: taskName,
		IP:   host,
		Img:  testImage,
		Tag:  "latest",
		Cmd:  testArgs,
		Vol: scheduler.Volume{
			Driver: v.String(),
			Name:   dynName,
			Path:   "/mnt/",
			Size:   10240,
		},
	}

	ctx, err := s.Create(t)
	if err != nil {
		return err
	}

	defer func() {
		if ctx != nil {
			s.Destroy(ctx)
		}
		v.CleanupVolume(volName)
	}()

	if err = s.Schedule(ctx); err != nil {
		return err
	}

	// Sleep for fio to get going...
	time.Sleep(20 * time.Second)

	// Stop the volume driver.
	log.Printf("Stopping the %v volume driver\n", v.String())
	if err = v.StopDriver(ctx.Task.IP); err != nil {
		return err
	}

	// Wait for the task to exit. This will lead to a lost Unmount/Detach call.
	log.Printf("Waiting for the test task to exit\n")
	if err = s.WaitDone(ctx); err != nil {
		return err
	}

	if ctx.Status == 0 {
		return fmt.Errorf("unexpected success exit status %v\nStdout: %v\nStderr: %v",
			ctx.Status,
			ctx.Stdout,
			ctx.Stderr,
		)
	}

	// Restart the volume driver.
	log.Printf("Starting the %v volume driver\n", v.String())
	if err = v.StartDriver(ctx.Task.IP); err != nil {
		return err
	}

	// Check to see if you can delete the volume from another node
	log.Printf("Deleting the attached volume: %v from %v\n", volName, nodes[1])
	if err = s.DeleteVolume(nodes[1], volName); err != nil {
		return err
	}

	return nil
}

// Verify that the volume driver can deal with an event where Docker and the
// client container crash on this system.  The volume should be able
// to get moounted on another node.
func testRemoteForceMount(
	s scheduler.Driver,
	v volume.Driver,
) error {
	taskName := "testRemoteForceMount"

	// Pick the first node to start the task
	nodes, err := s.GetNodes()
	if err != nil {
		return err
	}

	host := nodes[0]

	// Remove any container and volume for this test - previous run may have failed.
	s.DestroyByName(host, taskName)
	v.CleanupVolume(volName)

	t := scheduler.Task{
		Name: taskName,
		Img:  testImage,
		IP:   host,
		Tag:  "latest",
		Cmd:  testArgs,
		Vol: scheduler.Volume{
			Driver: v.String(),
			Name:   dynName,
			Path:   "/mnt/",
			Size:   10240,
		},
	}

	ctx, err := s.Create(t)
	if err != nil {
		return err
	}

	sc, err := systemd.NewSystemdClient()
	if err != nil {
		return err
	}
	defer func() {
		if err = sc.Start(dockerServiceName); err != nil {
			log.Printf("Error while restarting Docker: %v\n", err)
		}
		if ctx != nil {
			s.Destroy(ctx)
		}
		v.CleanupVolume(volName)
	}()

	log.Printf("Starting test task on local node.\n")
	if err = s.Schedule(ctx); err != nil {
		return err
	}

	// Sleep for fio to get going...
	time.Sleep(20 * time.Second)

	// Kill Docker.
	log.Printf("Stopping Docker.\n")
	if err = sc.Stop(dockerServiceName); err != nil {
		return err
	}

	// 40 second grace period before we try to use the volume elsewhere.
	time.Sleep(40 * time.Second)

	// Start a task on a new system with this same volume.
	log.Printf("Creating the test task on a new host.\n")
	t.IP = scheduler.ExternalHost
	if ctx, err = s.Create(t); err != nil {
		log.Printf("Error while creating remote task: %v\n", err)
		return err
	}

	if err = s.Schedule(ctx); err != nil {
		return err
	}

	// Sleep for fio to get going...
	time.Sleep(20 * time.Second)

	// Wait for the task to exit. This will lead to a lost Unmount/Detach call.
	log.Printf("Waiting for the test task to exit\n")
	if err = s.WaitDone(ctx); err != nil {
		return err
	}

	if ctx.Status != 0 {
		return fmt.Errorf("exit status %v\nStdout: %v\nStderr: %v",
			ctx.Status,
			ctx.Stdout,
			ctx.Stderr,
		)
	}

	// Restart Docker.
	log.Printf("Restarting Docker.\n")
	for i, err := 0, sc.Start(dockerServiceName); err != nil; i, err = i+1, sc.Start(dockerServiceName) {
		if err.Error() == systemd.JobExecutionTookTooLongError.Error() {
			if i < 20 {
				log.Printf("Docker taking too long to start... retry attempt %v\n", i)
			} else {
				return fmt.Errorf("could not restart Docker")
			}
		} else {
			return err
		}
	}

	// Wait for the volume driver to start.
	log.Printf("Waiting for the %v volume driver to start back up\n", v.String())
	if err = v.WaitStart(ctx.Task.IP); err != nil {
		return err
	}

	// Check to see if you can delete the volume.
	log.Printf("Deleting the attached volume: %v from this host\n", volName)
	if err = s.DeleteVolume("localhost", volName); err != nil {
		return err
	}
	return nil
}

// A container is using a volume on node X.  Node X is now powered off.
func testNodePowerOff(
	s scheduler.Driver,
	v volume.Driver,
) error {
	return nil
}

// Storage plugin is down.  Scheduler tries to create a container using the
// provider’s volume.
func testPluginDown(
	d scheduler.Driver,
	v volume.Driver,
) error {
	return nil
}

// A container is running on node X.  Node X loses network access and is
// partitioned away.  Node Y that is in the cluster can use the volume for
// another container.
func testNetworkDown(
	d scheduler.Driver,
	v volume.Driver,
) error {
	return nil
}

// A container is running on node X.  Node X can only see a subset of the
// storage cluster.  That is, it can see the entire DC/OS cluster, but just the
// storage cluster gets a network partition. Node Y that is in the cluster
// can use the volume for another container.
func testNetworkPartition(
	s scheduler.Driver,
	v volume.Driver,
) error {
	return nil
}

// Docker daemon crashes and live restore is enabled.
func testDockerDownLiveRestore(
	s scheduler.Driver,
	v volume.Driver,
) error {
	return nil
}

func run(
	s scheduler.Driver,
	v volume.Driver,
	testName string,
) error {
	if err := s.Init(); err != nil {
		log.Fatalf("Error initializing schedule driver")
		return err
	}

	if err := v.Init(); err != nil {
		log.Fatalf("Error initializing volume driver")
		return err
	}

	// Add new test functions here.
	testFuncs := map[string]testDriverFunc{
		"testDynamicVolume":           testDynamicVolume,
		"testRemoteForceMount":        testRemoteForceMount,
		"testDriverDown":              testDriverDown,
		"testDriverDownContainerDown": testDriverDownContainerDown,
		"testNodePowerOff":            testNodePowerOff,
		"testPluginDown":              testPluginDown,
		"testNetworkDown":             testNetworkDown,
		"testNetworkPartition":        testNetworkPartition,
		"testDockerDownLiveRestore":   testDockerDownLiveRestore,
	}

	if testName != "" {
		log.Printf("Executing single test %v\n", testName)
		f, ok := testFuncs[testName]

		if !ok {
			return fmt.Errorf("unknown test function %v", testName)
		}

		if err := f(s, v); err != nil {
			log.Printf("\tTest %v Failed with Error: %v.\n", testName, err)
			return err
		}
		log.Printf("\tTest %v Passed.\n", testName)
		return nil
	}

	for n, f := range testFuncs {
		log.Printf("Executing test %v\n", n)
		if err := f(s, v); err != nil {
			log.Printf("\tTest %v Failed with Error: %v.\n", n, err)
		} else {
			log.Printf("\tTest %v Passed.\n", n)
		}
	}

	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %v <scheduler> <volume driver> [testName]\n", os.Args[0])
		os.Exit(-1)
	}

	nodes := strings.Split(os.Getenv("CLUSTER_NODES"), ",")
	if len(nodes) < 3 {
		log.Printf("There are not enough nodes in this cluster.  Most tests will fail.\n")
		log.Printf("Use 'export CLUSTER_NODES=\"192.168.1.100,192.168.1.101,192.168.1.102\"'")
	}

	testName := ""
	if len(os.Args) > 3 {
		testName = os.Args[3]
	}

	if s, err := scheduler.Get(os.Args[1]); err != nil {
		log.Fatalf("Cannot find scheduler driver %v\n", os.Args[1])
		os.Exit(-1)
	} else if v, err := volume.Get(os.Args[2]); err != nil {
		log.Fatalf("Cannot find scheduler driver %v\n", os.Args[1])
		os.Exit(-1)
	} else {
		if run(s, v, testName) != nil {
			os.Exit(-1)
		}
	}

	log.Printf("Test suite complete with this driver: %v, and this scheduler: %v\n",
		os.Args[2],
		os.Args[1],
	)
}
