package main

import (
	"fmt"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/portworx/torpedo/drivers/scheduler"
	_ "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/volume"
	_ "github.com/portworx/torpedo/drivers/volume/portworx"
	"github.com/portworx/torpedo/pkg/errors"
)

type torpedo struct {
	instanceID string
	s          scheduler.Driver
	v          volume.Driver
}

// testDriverFunc runs a specific external storage test case.  It takes
// in a scheduler driver and an external volume provider as arguments.
type testDriverFunc func() error

// Create dynamic volumes.  Make sure that a task can use the dynamic volume
// in the inline format as size=x,repl=x,compress=x,name=foo.
// This test will fail if the storage driver is not able to parse the size correctly.
func (t *torpedo) testDynamicVolume() error {
	taskName := fmt.Sprintf("testdynamicvolume-%v", t.instanceID)

	contexts, err := t.s.Schedule(taskName, scheduler.ScheduleOptions{})
	if err != nil {
		return err
	}

	for _, ctx := range contexts {
		if ctx.Status != 0 {
			return fmt.Errorf("exit status %v\nStdout: %v\nStderr: %v",
				ctx.Status,
				ctx.Stdout,
				ctx.Stderr,
			)
		}

		if err := t.validateVolumes(ctx); err != nil {
			return err
		}

		if err := t.s.WaitForRunning(ctx); err != nil {
			return err
		}

		if err := t.tearDownContext(ctx); err != nil {
			return err
		}
	}

	return err
}

// validateVolumes validates the volume with the scheduler and volume driver
func (t *torpedo) validateVolumes(ctx *scheduler.Context) error {
	if err := t.s.InspectVolumes(ctx); err != nil {
		return &errors.ErrValidateVol{
			ID:    ctx.UID,
			Cause: err.Error(),
		}
	}

	// Get all volumes with their params and ask volume driver to inspect them
	volumes, err := t.s.GetVolumeParameters(ctx)
	if err != nil {
		return &errors.ErrValidateVol{
			ID:    ctx.UID,
			Cause: err.Error(),
		}
	}

	for vol, params := range volumes {
		if err := t.v.InspectVolume(vol, params); err != nil {
			return &errors.ErrValidateVol{
				ID:    ctx.UID,
				Cause: err.Error(),
			}
		}
	}

	return nil
}

func (t *torpedo) tearDownContext(ctx *scheduler.Context) error {
	if err := t.s.Destroy(ctx); err != nil {
		return err
	}

	if err := t.s.WaitForDestroy(ctx); err != nil {
		return err
	}

	if err := t.s.DeleteVolumes(ctx); err != nil {
		return err
	}
	return nil
}

// Volume Driver Plugin is down, unavailable - and the client container should
// not be impacted.
func (t *torpedo) testDriverDown() error {
	/*	taskName := "testDriverDown"

		// Pick the first node to start the task
		nodes, err := s.GetNodes()
		if err != nil {
			return err
		}

		host := nodes[0]

		// Remove any container and volume for this test - previous run may have failed.
		// TODO: cleanup task and volume

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

		// Sleep for postgres to get going...
		time.Sleep(20 * time.Second)

		// Stop the volume driver.
		logrus.Printf("Stopping the %v volume driver\n", v.String())
		if err = v.StopDriver(ctx.Task.IP); err != nil {
			return err
		}

		// Sleep for postgres to keep going...
		time.Sleep(20 * time.Second)

		// Restart the volume driver.
		logrus.Printf("Starting the %v volume driver\n", v.String())
		if err = v.StartDriver(ctx.Task.IP); err != nil {
			return err
		}

		logrus.Printf("Waiting for the test task to exit\n")
		if err = s.WaitDone(ctx); err != nil {
			return err
		}

		if ctx.Status != 0 {
			return fmt.Errorf("exit status %v\nStdout: %v\nStderr: %v",
				ctx.Status,
				ctx.Stdout,
				ctx.Stderr,
			)
		}*/
	return nil
}

// Volume driver plugin is down and the client container gets terminated.
// There is a lost unmount call in this case. When the volume driver is
// back up, we should be able to detach and delete the volume.
func (t *torpedo) testDriverDownContainerDown() error {
	/*
		taskName := "testDriverDownContainerDown"

		// Pick the first node to start the task
		nodes, err := s.GetNodes()
		if err != nil {
			return err
		}

		host := nodes[0]

		// Remove any container and volume for this test - previous run may have failed.
		// TODO: cleanup task and volume

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

		// Sleep for postgres to get going...
		time.Sleep(20 * time.Second)

		// Stop the volume driver.
		logrus.Printf("Stopping the %v volume driver\n", v.String())
		if err = v.StopDriver(ctx.Task.IP); err != nil {
			return err
		}

		// Wait for the task to exit. This will lead to a lost Unmount/Detach call.
		logrus.Printf("Waiting for the test task to exit\n")
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
		logrus.Printf("Starting the %v volume driver\n", v.String())
		if err = v.StartDriver(ctx.Task.IP); err != nil {
			return err
		}

		// Check to see if you can delete the volume from another node
		logrus.Printf("Deleting the attached volume: %v from %v\n", volName, nodes[1])
		if err = s.DeleteVolumes(volName); err != nil {
			return err
		}
	*/

	return nil
}

// Verify that the volume driver can deal with an event where Docker and the
// client container crash on this system.  The volume should be able
// to get moounted on another node.
func (t *torpedo) testRemoteForceMount() error {
	/*	taskName := "testRemoteForceMount"

		// Pick the first node to start the task
		nodes, err := s.GetNodes()
		if err != nil {
			return err
		}

		host := nodes[0]

		// Remove any container and volume for this test - previous run may have failed.
		// TODO: cleanup task and volume

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
				logrus.Printf("Error while restarting Docker: %v\n", err)
			}
			if ctx != nil {
				s.Destroy(ctx)
			}
			v.CleanupVolume(volName)
		}()

		logrus.Printf("Starting test task on local node.\n")
		if err = s.Schedule(ctx); err != nil {
			return err
		}

		// Sleep for postgres to get going...
		time.Sleep(20 * time.Second)

		// Kill Docker.
		logrus.Printf("Stopping Docker.\n")
		if err = sc.Stop(dockerServiceName); err != nil {
			return err
		}

		// 40 second grace period before we try to use the volume elsewhere.
		time.Sleep(40 * time.Second)

		// Start a task on a new system with this same volume.
		logrus.Printf("Creating the test task on a new host.\n")
		t.IP = scheduler.ExternalHost
		if ctx, err = s.Create(t); err != nil {
			logrus.Printf("Error while creating remote task: %v\n", err)
			return err
		}

		if err = s.Schedule(ctx); err != nil {
			return err
		}

		// Sleep for postgres to get going...
		time.Sleep(20 * time.Second)

		// Wait for the task to exit. This will lead to a lost Unmount/Detach call.
		logrus.Printf("Waiting for the test task to exit\n")
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
		logrus.Printf("Restarting Docker.\n")
		for i, err := 0, sc.Start(dockerServiceName); err != nil; i, err = i+1, sc.Start(dockerServiceName) {
			if err.Error() == systemd.JobExecutionTookTooLongError.Error() {
				if i < 20 {
					logrus.Printf("Docker taking too long to start... retry attempt %v\n", i)
				} else {
					return fmt.Errorf("could not restart Docker")
				}
			} else {
				return err
			}
		}

		// Wait for the volume driver to start.
		logrus.Printf("Waiting for the %v volume driver to start back up\n", v.String())
		if err = v.WaitStart(ctx.Task.IP); err != nil {
			return err
		}

		// Check to see if you can delete the volume.
		logrus.Printf("Deleting the attached volume: %v from this host\n", volName)
		if err = s.DeleteVolumes(volName); err != nil {
			return err
		}*/
	return nil
}

// A container is using a volume on node X.  Node X is now powered off.
func (t *torpedo) testNodePowerOff() error {
	return nil
}

// Storage plugin is down.  Scheduler tries to create a container using the
// provider’s volume.
func (t *torpedo) testPluginDown() error {
	return nil
}

// A container is running on node X.  Node X loses network access and is
// partitioned away.  Node Y that is in the cluster can use the volume for
// another container.
func (t *torpedo) testNetworkDown() error {
	return nil
}

// A container is running on node X.  Node X can only see a subset of the
// storage cluster.  That is, it can see the entire DC/OS cluster, but just the
// storage cluster gets a network partition. Node Y that is in the cluster
// can use the volume for another container.
func (t *torpedo) testNetworkPartition() error {
	return nil
}

// Docker daemon crashes and live restore is enabled.
func (t *torpedo) testDockerDownLiveRestore() error {
	return nil
}

func (t *torpedo) run(testName string) error {
	logrus.Printf("Running torpedo test: %v", t.instanceID)

	if err := t.s.Init(); err != nil {
		logrus.Fatalf("Error initializing schedule driver. Err: %v", err)
		return err
	}

	if err := t.v.Init(t.s.String()); err != nil {
		logrus.Fatalf("Error initializing volume driver. Err: %v", err)
		return err
	}

	// Add new test functions here.
	testFuncs := map[string]testDriverFunc{
		"testDynamicVolume": t.testDynamicVolume,
		/*		"testRemoteForceMount":        t.testRemoteForceMount,
				"testDriverDown":              t.testDriverDown,
				"testDriverDownContainerDown": t.testDriverDownContainerDown,
				"testNodePowerOff":            t.testNodePowerOff,
				"testPluginDown":              t.testPluginDown,
				"testNetworkDown":             t.testNetworkDown,
				"testNetworkPartition":        t.testNetworkPartition,
				"testDockerDownLiveRestore":   t.testDockerDownLiveRestore,*/
	}

	if testName != "" {
		logrus.Infof("Executing single test %v", testName)
		f, ok := testFuncs[testName]
		if !ok {
			return &errors.ErrNotFound{
				ID:   testName,
				Type: "Test",
			}
		}

		if err := f(); err != nil {
			logrus.Infof("Test %v Failed with Error: %v.", testName, err)
			return err
		}
		logrus.Infof("Test %v Passed.", testName)
		return nil
	}

	for n, f := range testFuncs {
		logrus.Infof("Executing test %v", n)
		if err := f(); err != nil {
			logrus.Infof("Test %v Failed with Error: %v.", n, err)
		} else {
			logrus.Infof("Test %v Passed.", n)
		}
	}

	return nil
}

func main() {
	// TODO: switch to a proper argument parser
	if len(os.Args) < 2 {
		logrus.Infof("Usage: %v <scheduler> <volume driver> [testName]", os.Args[0])
		os.Exit(-1)
	}

	testName := ""
	if len(os.Args) > 3 {
		testName = os.Args[3]
	}

	// TODO: implement Node driver

	if s, err := scheduler.Get(os.Args[1]); err != nil {
		logrus.Fatalf("Cannot find scheduler driver for %v. Err: %v\n", os.Args[1], err)
		os.Exit(-1)
	} else if v, err := volume.Get(os.Args[2]); err != nil {
		logrus.Fatalf("Cannot find volume driver for %v. Err: %v\n", os.Args[2], err)
		os.Exit(-1)
	} else {
		t := torpedo{
			instanceID: time.Now().Format("01-02-15h04m05s"),
			s:          s,
			v:          v,
		}

		if t.run(testName) != nil {
			os.Exit(-1)
		}

		logrus.Printf("Test suite complete with volume driver: %v, and scheduler: %v\n",
			t.v.String(),
			t.s.String(),
		)
	}
}
