package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/portworx/torpedo/scheduler"
	"github.com/portworx/torpedo/volume"
)

// testDriverFunc runs a specific external storage test case.  It takes
// in a scheduler driver and an external volume provider as arguments.
type testDriverFunc func(scheduler.Driver, volume.Driver) error

const (
	volName = "torpedo_fiovol"

	// Use the inline volume specification so that we can test
	// volume options being dynamically parsed and used inline.
	dynName = "size=10G,name=" + volName
)

// Create dynamic volumes.  Make sure that a task can use the dynamic volume
// in the inline format as size=x,repl=x,compress=x,name=foo.
// This test will fail if the storage driver is not able to parse the size correctly.
func testDynamicVolume(
	d scheduler.Driver,
	v volume.Driver,
) error {
	taskName := "testDynamicVolume"

	// Remove any container and volume for this test - previous run may have failed.
	d.DestroyByName("localhost", taskName)
	v.RemoveVolume(volName)

	t := scheduler.Task{
		Name: taskName,
		Ip:   "localhost",
		Img:  "gourao/fio",
		Tag:  "latest",
		Cmd: []string{
			"fio",
			"--blocksize=64k",
			"--directory=/mnt/",
			"--ioengine=libaio",
			"--readwrite=write",
			"--size=5G",
			"--name=test",
			"--verify=meta",
			"--do_verify=1",
			"--verify_pattern=0xDeadBeef",
			"--direct=1",
			"--gtod_reduce=1",
			"--iodepth=1",
			"--randrepeat=1",
		},
		Vol: scheduler.Volume{
			Driver: v.String(),
			Name:   dynName,
			Path:   "/mnt/",
			Size:   10240,
		},
	}

	if ctx, err := d.Create(t); err != nil {
		return err
	} else {
		defer func() {
			d.Destroy(ctx)
			v.RemoveVolume(volName)
		}()

		// Run the task and wait for completion.  This task will exit and
		// must not be re-started by the scheduler.
		if err = d.Run(ctx); err != nil {
			return err
		}

		if ctx.Status != 0 {
			return fmt.Errorf("Exit status %v\nStdout: %v\nStderr: %v\n",
				ctx.Status,
				ctx.Stdout,
				ctx.Stderr,
			)
		}
	}

	// Verify that the volume properties are honored.
	if vol, err := d.InspectVolume("", dynName); err != nil {
		return err
	} else {
		if vol.Driver != v.String() {
			return fmt.Errorf(
				"Dynamic volume creation failed, incorrect volume driver (driver = %v).",
				vol.Driver,
			)
		}
	}
	return nil
}

// Volume Driver Plugin is down, unavailable - and the client container should
// not be impacted.
func testDriverDown(
	d scheduler.Driver,
	v volume.Driver,
) error {
	taskName := "testDriverDown"

	// Remove any container and volume for this test - previous run may have failed.
	d.DestroyByName("localhost", taskName)
	v.RemoveVolume(volName)

	t := scheduler.Task{
		Name: taskName,
		Ip:   "localhost",
		Img:  "gourao/fio",
		Tag:  "latest",
		Cmd: []string{
			"fio",
			"--blocksize=64k",
			"--directory=/mnt/",
			"--ioengine=libaio",
			"--readwrite=write",
			"--size=5G",
			"--name=test",
			"--verify=meta",
			"--do_verify=1",
			"--verify_pattern=0xDeadBeef",
			"--direct=1",
			"--gtod_reduce=1",
			"--iodepth=1",
			"--randrepeat=1",
		},
		Vol: scheduler.Volume{
			Driver: v.String(),
			Name:   dynName,
			Path:   "/mnt/",
			Size:   10240,
		},
	}

	if ctx, err := d.Create(t); err != nil {
		return err
	} else {
		defer func() {
			d.Destroy(ctx)
			v.RemoveVolume(volName)
		}()

		if err = d.Start(ctx); err != nil {
			return err
		}

		// Sleep for fio to get going...
		time.Sleep(20 * time.Second)

		// Stop the volume driver.
		log.Printf("Stopping the %v volume driver\n", v.String())
		if err = v.Stop(ctx.Task.Ip); err != nil {
			return err
		}

		// Sleep for fio to keep going...
		time.Sleep(20 * time.Second)

		// Restart the volume driver.
		log.Printf("Starting the %v volume driver\n", v.String())
		if err = v.Start(ctx.Task.Ip); err != nil {
			return err
		}

		log.Printf("Waiting for the test task to exit\n")
		if err = d.WaitDone(ctx); err != nil {
			return err
		}

		if ctx.Status != 0 {
			return fmt.Errorf("Exit status %v\nStdout: %v\nStderr: %v\n",
				ctx.Status,
				ctx.Stdout,
				ctx.Stderr,
			)
		}
	}
	return nil
}

// Volume driver plugin is down and the client container gets terminated.
// There is a lost unmount call in this case. When the volume driver is
// back up, we should be able to detach and delete the volume.
func testDriverDownContainerDown(
	d scheduler.Driver,
	v volume.Driver,
) error {
	taskName := "testDriverDownContainerDown"

	// Remove any container and volume for this test - previous run may have failed.
	d.DestroyByName("localhost", taskName)
	v.RemoveVolume(volName)

	t := scheduler.Task{
		Name: taskName,
		Ip:   "localhost",
		Img:  "gourao/fio",
		Tag:  "latest",
		Cmd: []string{
			"fio",
			"--blocksize=64k",
			"--directory=/mnt/",
			"--ioengine=libaio",
			"--readwrite=write",
			"--size=5G",
			"--name=test",
			"--verify=meta",
			"--do_verify=1",
			"--verify_pattern=0xDeadBeef",
			"--direct=1",
			"--gtod_reduce=1",
			"--iodepth=1",
			"--randrepeat=1",
		},
		Vol: scheduler.Volume{
			Driver: v.String(),
			Name:   dynName,
			Path:   "/mnt/",
			Size:   10240,
		},
	}

	if ctx, err := d.Create(t); err != nil {
		return err
	} else {
		defer func() {
			d.Destroy(ctx)
			v.RemoveVolume(volName)
		}()

		if err = d.Start(ctx); err != nil {
			return err
		}

		// Sleep for fio to get going...
		time.Sleep(20 * time.Second)

		// Stop the volume driver.
		log.Printf("Stopping the %v volume driver\n", v.String())
		if err = v.Stop(ctx.Task.Ip); err != nil {
			return err
		}

		// Wait for the task to exit. This will lead to a lost Unmount/Detach call.
		log.Printf("Waiting for the test task to exit\n")
		if err = d.WaitDone(ctx); err != nil {
			return err
		}

		if ctx.Status == 0 {
			return fmt.Errorf("Unexpected success exit status %v\nStdout: %v\nStderr: %v\n",
				ctx.Status,
				ctx.Stdout,
				ctx.Stderr,
			)
		}

		// Restart the volume driver.
		log.Printf("Starting the %v volume driver\n", v.String())
		if err = v.Start(ctx.Task.Ip); err != nil {
			return err
		}

		// Check to see if you can delete the volume.
		log.Printf("Deleting the attached volume: %v from this host\n", volName)
		if err = d.DeleteVolume("localhost", volName); err != nil {
			return err
		}
	}
	return nil
}

// Verify that the volume driver can deal with an uneven number of mounts
// and unmounts and allow the volume to get mounted on another node.
func testRemoteForceMount(
	d scheduler.Driver,
	v volume.Driver,
) error {
	taskName := "testRemoteForceMount"

	// Remove any container and volume for this test - previous run may have failed.
	d.DestroyByName("localhost", taskName)
	v.RemoveVolume(volName)

	t := scheduler.Task{
		Name: taskName,
		Img:  "gourao/fio",
		Ip:   "localhost",
		Tag:  "latest",
		Cmd: []string{
			"fio",
			"--blocksize=64k",
			"--directory=/mnt/",
			"--ioengine=libaio",
			"--readwrite=write",
			"--size=5G",
			"--name=test",
			"--verify=meta",
			"--do_verify=1",
			"--verify_pattern=0xDeadBeef",
			"--direct=1",
			"--gtod_reduce=1",
			"--iodepth=1",
			"--randrepeat=1",
		},
		Vol: scheduler.Volume{
			Driver: v.String(),
			Name:   dynName,
			Path:   "/mnt/",
			Size:   10240,
		},
	}

	if ctx, err := d.Create(t); err != nil {
		return err
	} else {
		defer func() {
			d.Destroy(ctx)
			v.RemoveVolume(volName)
		}()

		if err = d.Start(ctx); err != nil {
			return err
		}

		// Sleep for fio to get going...
		time.Sleep(20 * time.Second)

		// Stop the volume driver.
		log.Printf("Stopping the %v volume driver\n", v.String())
		if err = v.Stop(ctx.Task.Ip); err != nil {
			return err
		}

		// Wait for the task to exit. This will lead to a lost Unmount/Detach call.
		log.Printf("Waiting for the test task to exit\n")
		if err = d.WaitDone(ctx); err != nil {
			return err
		}

		if ctx.Status == 0 {
			return fmt.Errorf("Unexpected success exit status %v\nStdout: %v\nStderr: %v\n",
				ctx.Status,
				ctx.Stdout,
				ctx.Stderr,
			)
		}

		// Restart the volume driver.
		log.Printf("Starting the %v volume driver\n", v.String())
		if err = v.Start(ctx.Task.Ip); err != nil {
			return err
		}

		// Check to see if you can delete the volume.
		log.Printf("Deleting the attached volume: %v from this host\n", volName)
		if err = d.DeleteVolume("localhost", volName); err != nil {
			return err
		}
	}
	return nil
}

// A container is using a volume on node X.  Node X is now powered off.
func testNodePowerOff(
	d scheduler.Driver,
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
	d scheduler.Driver,
	v volume.Driver,
) error {
	return nil
}

// Docker daemon crashes and live restore is disabled.
func testDockerDown(
	d scheduler.Driver,
	v volume.Driver,
) error {
	return nil
}

// Docker daemon crashes and live restore is enabled.
func testDockerDownLiveRestore(
	d scheduler.Driver,
	v volume.Driver,
) error {
	return nil
}

func run(
	d scheduler.Driver,
	v volume.Driver,
	testName string,
) error {
	if err := d.Init(); err != nil {
		return err
	}

	if err := v.Init(); err != nil {
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
		"testDockerDown":              testDockerDown,
		"testDockerDownLiveRestore":   testDockerDownLiveRestore,
	}

	if testName != "" {
		if f, ok := testFuncs[testName]; !ok {
			return fmt.Errorf("Unknown test function %v", testName)
		} else {
			if err := f(d, v); err != nil {
				log.Printf("\tTest %v Failed with Error: %v.\n", testName, err)
				return err
			}
			log.Printf("\tTest %v Passed.\n", testName)
			return nil
		}
	} else {
		for n, f := range testFuncs {
			log.Printf("Executing test %v\n", n)
			if err := f(d, v); err != nil {
				log.Printf("\tTest %v Failed with Error: %v.\n", n, err)
				return err
			}
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
	}

	testName := ""
	if len(os.Args) >= 3 {
		testName = os.Args[3]
	}

	if d, err := scheduler.Get(os.Args[1]); err != nil {
		log.Fatalf("Cannot find scheduler driver %v\n", os.Args[1])
		os.Exit(-1)
	} else if v, err := volume.Get(os.Args[2]); err != nil {
		log.Fatalf("Cannot find scheduler driver %v\n", os.Args[1])
		os.Exit(-1)
	} else {
		if run(d, v, testName) != nil {
			os.Exit(-1)
		}
	}

	log.Printf("All tests have passed with this driver: %v, and this scheduler: %v\n",
		os.Args[2],
		os.Args[1],
	)
}
