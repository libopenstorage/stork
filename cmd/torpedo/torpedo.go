package main

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/portworx/torpedo/drivers/node"
	_ "github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/portworx/torpedo/drivers/scheduler"
	_ "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/volume"
	_ "github.com/portworx/torpedo/drivers/volume/portworx"
	"github.com/portworx/torpedo/pkg/errors"
)

// DefaultSpecsRoot specifies the default location of the base specs directory in the torpedo container
const DefaultSpecsRoot = "/specs"

type torpedo struct {
	instanceID string
	s          scheduler.Driver
	v          volume.Driver
	n          node.Driver
}

// testDriverFunc runs a specific external storage test case.  It takes
// in a scheduler driver and an external volume provider as arguments.
type testDriverFunc func() error

// testSetupTearDown performs basic test of starting an application and destroying it (along with storage)
func (t *torpedo) testSetupTearDown() error {
	taskName := fmt.Sprintf("setupteardown-%v", t.instanceID)

	logrus.Infof("[%v] Scheduling new applications", taskName)
	contexts, err := t.s.Schedule(taskName, scheduler.ScheduleOptions{})
	if err != nil {
		return err
	}

	for _, ctx := range contexts {
		logrus.Infof("[%v] Validating %v", taskName, ctx.App.Key)
		if err := t.validateContext(ctx); err != nil {
			return err
		}

		logrus.Infof("[%v] Tearing down %v", taskName, ctx.App.Key)
		if err := t.tearDownContext(ctx); err != nil {
			return err
		}
	}

	return err
}

// Volume Driver Plugin is down, unavailable - and the client container should
// not be impacted.
func (t *torpedo) testDriverDown() error {
	taskName := fmt.Sprintf("driverdown-%v", t.instanceID)
	logrus.Infof("[%v] Scheduling new applications", taskName)

	contexts, err := t.s.Schedule(taskName, scheduler.ScheduleOptions{})
	if err != nil {
		return err
	}

	for _, ctx := range contexts {
		logrus.Infof("[%v] Validating %v", taskName, ctx.App.Key)
		if err := t.validateContext(ctx); err != nil {
			return err
		}

		appNodes, err := t.s.GetNodesForApp(ctx)
		if err != nil {
			return err
		}

		if len(appNodes) == 0 {
			return fmt.Errorf("error: found 0 nodes for app: %v (uid: %v)", ctx.App.Key, ctx.UID)
		}

		logrus.Infof("[%v] Stopping volume driver: %v on app nodes", taskName, t.v.String())
		for _, n := range appNodes {
			if err := t.v.StopDriver(n); err != nil {
				return err
			}
		}

		// TODO add a PerformIO interface to applications

		// Sleep for apps to get going...
		time.Sleep(20 * time.Second)

		logrus.Infof("[%v] Re-starting volume driver on app nodes", taskName)
		for _, n := range appNodes {
			if err := t.v.StartDriver(n); err != nil {
				return err
			}
		}

		logrus.Infof("[%v] Waiting for volume driver:%v to start", taskName, t.v.String())
		for _, n := range appNodes {
			if err := t.v.WaitStart(n); err != nil {
				return err
			}
		}

		logrus.Infof("[%v] Re-validating %v", taskName, ctx.App.Key)
		if err := t.validateContext(ctx); err != nil {
			return err
		}

		logrus.Infof("[%v] Tearing down %v", taskName, ctx.App.Key)
		if err := t.tearDownContext(ctx); err != nil {
			return err
		}

	}

	return nil
}

// Volume driver plugin is down and the client container gets terminated.
// There is a lost unmount call in this case. When the volume driver is
// back up, we should be able to detach and delete the volume.
func (t *torpedo) testDriverDownAppDown() error {
	taskName := fmt.Sprintf("driverappdown-%v", t.instanceID)
	logrus.Infof("[%v] Scheduling new applications", taskName)
	contexts, err := t.s.Schedule(taskName, scheduler.ScheduleOptions{})
	if err != nil {
		return err
	}

	for _, ctx := range contexts {
		logrus.Infof("[%v] Validating %v", taskName, ctx.App.Key)
		if err := t.validateContext(ctx); err != nil {
			return err
		}

		appNodes, err := t.s.GetNodesForApp(ctx)
		if err != nil {
			return err
		}

		if len(appNodes) == 0 {
			return fmt.Errorf("error: found 0 nodes for app: %v (uid: %v)", ctx.App.Key, ctx.UID)
		}

		logrus.Infof("[%v] Stopping volume driver on app nodes", taskName)
		for _, n := range appNodes {
			if err := t.v.StopDriver(n); err != nil {
				return err
			}
		}

		// Sleep for apps to get going...
		time.Sleep(20 * time.Second)

		logrus.Infof("[%v] Destroying application: %v", taskName, ctx.App.Key)
		if err := t.s.Destroy(ctx); err != nil {
			return err
		}

		logrus.Infof("[%v] Re-starting volume driver on app nodes", taskName)
		for _, n := range appNodes {
			if err := t.v.StartDriver(n); err != nil {
				return err
			}
		}

		logrus.Infof("[%v] Waiting for volume driver: %v to start", taskName, t.v.String())
		for _, n := range appNodes {
			if err := t.v.WaitStart(n); err != nil {
				return err
			}
		}

		// Wait for applications to be destroyed
		if err := t.s.WaitForDestroy(ctx); err != nil {
			return err
		}

		logrus.Infof("[%v] Tearing down storage for: %v", taskName, ctx.App.Key)
		if err := t.s.DeleteVolumes(ctx); err != nil {
			return err
		}

		// TODO add WaitForDestroyVolumes
	}

	return nil
}

// testAppTasksDown deletes all tasks of an application and checks if app converges back to desired state
func (t *torpedo) testAppTasksDown() error {
	taskName := fmt.Sprintf("apptasksdown-%v", t.instanceID)
	logrus.Infof("[%v] Scheduling new applications", taskName)
	contexts, err := t.s.Schedule(taskName, scheduler.ScheduleOptions{})
	if err != nil {
		return err
	}

	for _, ctx := range contexts {
		logrus.Infof("[%v] Validating %v", taskName, ctx.App.Key)
		if err := t.validateContext(ctx); err != nil {
			return err
		}

		logrus.Infof("[%v] Destroying tasks for application: %v", taskName, ctx.App.Key)
		if err := t.s.DeleteTasks(ctx); err != nil {
			return err
		}

		logrus.Infof("[%v] Re-validating %v", taskName, ctx.App.Key)
		if err := t.validateContext(ctx); err != nil {
			return err
		}

		logrus.Infof("[%v] Tearing down %v", taskName, ctx.App.Key)
		if err := t.tearDownContext(ctx); err != nil {
			return err
		}
	}

	return nil
}

// testNodeReboot reboots one of the nodes on which an app is running
func (t *torpedo) testNodeReboot(allNodes bool) error {
	taskName := fmt.Sprintf("nodereboot-%v", t.instanceID)
	if allNodes {
		taskName = fmt.Sprintf("all%v", taskName)
	}

	logrus.Infof("[%v] Scheduling new applications", taskName)
	contexts, err := t.s.Schedule(taskName, scheduler.ScheduleOptions{})
	if err != nil {
		return err
	}

	for _, ctx := range contexts {
		logrus.Infof("[%v] Validating %v", taskName, ctx.App.Key)
		if err := t.validateContext(ctx); err != nil {
			return err
		}

		appNodes, err := t.s.GetNodesForApp(ctx)
		if err != nil {
			return err
		}

		if len(appNodes) == 0 {
			return fmt.Errorf("error: found 0 nodes for app: %v (uid: %v)", ctx.App.Key, ctx.UID)
		}

		var nodesToReboot []node.Node
		if allNodes {
			nodesToReboot = appNodes
		} else {
			nodesToReboot = append(nodesToReboot, appNodes[0])
		}

		for _, n := range nodesToReboot {
			logrus.Infof("[%v] Rebooting: %v", taskName, n.Name)
			if err := t.n.RebootNode(n, node.RebootNodeOpts{
				Force: false,
			}); err != nil {
				return err
			}
		}

		time.Sleep(20 * time.Second)

		// Wait for node to be back
		for _, n := range nodesToReboot {
			logrus.Infof("[%v] Testing connectivity with: %v", taskName, n.Name)
			if err := t.n.TestConnection(n, node.TestConectionOpts{
				Timeout:         15 * time.Minute,
				TimeBeforeRetry: 10 * time.Second,
			}); err != nil {
				return err
			}

			if err := t.s.IsNodeReady(n); err != nil {
				return err
			}

			if err := t.v.WaitStart(n); err != nil {
				return err
			}
		}

		logrus.Infof("[%v] Re-validating %v", taskName, ctx.App.Key)
		if err := t.validateContext(ctx); err != nil {
			return err
		}

		logrus.Infof("[%v] Tearing down %v", taskName, ctx.App.Key)
		if err := t.tearDownContext(ctx); err != nil {
			return err
		}
	}

	return err
}

func (t *torpedo) validateContext(ctx *scheduler.Context) error {
	var err error
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
	if err = t.s.WaitForRunning(ctx); err != nil {
		return err
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
	var err error
	if err = t.s.Destroy(ctx); err != nil {
		return err
	}
	if err = t.s.WaitForDestroy(ctx); err != nil {
		return err
	}
	if err = t.s.DeleteVolumes(ctx); err != nil {
		return err
	}

	return err
}

/*
// Storage plugin is down.  Scheduler tries to create a container using the
// provider’s volume.
func (t *torpedo) testPluginDown() error {
	return &errors.ErrNotSupported{
		Operation: "testPluginDown",
	}
}

// A container is running on node X.  Node X loses network access and is
// partitioned away.  Node Y that is in the cluster can use the volume for
// another container.
func (t *torpedo) testNetworkDown() error {
	return &errors.ErrNotSupported{
		Operation: "testNetworkDown",
	}
}

// A container is running on node X.  Node X can only see a subset of the
// storage cluster.  That is, it can see the entire DC/OS cluster, but just the
// storage cluster gets a network partition. Node Y that is in the cluster
// can use the volume for another container.
func (t *torpedo) testNetworkPartition() error {
	return &errors.ErrNotSupported{
		Operation: "testNetworkPartition",
	}
}

// Docker daemon crashes and live restore is enabled.
func (t *torpedo) testDockerDownLiveRestore() error {
	return &errors.ErrNotSupported{
		Operation: "testDockerDownLiveRestore",
	}
}

// Verify that the volume driver can deal with an event where Docker and the
// client container crash on this system.  The volume should be able
// to get moounted on another node.
func (t *torpedo) testRemoteForceMount() error {
*/ /*	taskName := "testRemoteForceMount"

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
	}*/ /*
	return &errors.ErrNotSupported{
		Operation: "testRemoteForceMount",
	}

}*/

// TODO replace run() with go tests
func (t *torpedo) run(testName string) error {
	logrus.Printf("Running torpedo instance: %v", t.instanceID)

	if err := t.s.Init(path.Join(DefaultSpecsRoot, t.s.String())); err != nil {
		logrus.Fatalf("Error initializing schedule driver. Err: %v", err)
		return err
	}

	if err := t.v.Init(t.s.String()); err != nil {
		logrus.Fatalf("Error initializing volume driver. Err: %v", err)
		return err
	}

	if err := t.n.Init(t.s.String()); err != nil {
		logrus.Fatalf("Error initializing node driver. Err: %v", err)
		return err
	}

	// Add new test functions here.
	testFuncs := map[string]testDriverFunc{
		"testSetupTearDown":     func() error { return t.testSetupTearDown() },
		"testOneNodeReboot":     func() error { return t.testNodeReboot(false) },
		"testAllNodeReboot":     func() error { return t.testNodeReboot(true) },
		"testDriverDown":        func() error { return t.testDriverDown() },
		"testDriverDownAppDown": func() error { return t.testDriverDownAppDown() },
		"testAppTasksDown":      func() error { return t.testAppTasksDown() },
	}

	if testName != "" {
		logrus.Infof("***** Test %v starting *****", testName)
		f, ok := testFuncs[testName]
		if !ok {
			return &errors.ErrNotFound{
				ID:   testName,
				Type: "Test",
			}
		}

		if err := f(); err != nil {
			logrus.Infof("***** Test %v Failed with Error: %v *****", testName, err)
			return err
		}
		logrus.Infof("***** Test %v Passed *****", testName)
		return nil
	}

	for n, f := range testFuncs {
		logrus.Infof("***** Test %v starting *****", n)
		if err := f(); err != nil {
			logrus.Infof("***** Test %v Failed with Error: %v *****", n, err)
		} else {
			logrus.Infof("***** Test %v Passed *****", n)
		}
	}

	return nil
}

func main() {
	// TODO: switch to a proper argument parser
	if len(os.Args) < 3 {
		logrus.Infof("Usage: %v <scheduler> <volume driver> <node driver> [testName]", os.Args[0])
		os.Exit(-1)
	}

	testName := "" // TODO csv or yaml based args
	if len(os.Args) > 4 {
		testName = os.Args[4]
	}

	if s, err := scheduler.Get(os.Args[1]); err != nil {
		logrus.Fatalf("Cannot find scheduler driver for %v. Err: %v\n", os.Args[1], err)
		os.Exit(-1)
	} else if v, err := volume.Get(os.Args[2]); err != nil {
		logrus.Fatalf("Cannot find volume driver for %v. Err: %v\n", os.Args[2], err)
		os.Exit(-1)
	} else if n, err := node.Get(os.Args[3]); err != nil {
		logrus.Fatalf("Cannot find node driver for %v. Err: %v\n", os.Args[3], err)
		os.Exit(-1)
	} else {
		t := torpedo{
			instanceID: time.Now().Format("01-02-15h04m05s"),
			s:          s,
			v:          v,
			n:          n,
		}

		if t.run(testName) != nil {
			os.Exit(-1)
		}

		logrus.Printf("Test suite complete with volume driver: %v, and scheduler: %v, node: %v\n",
			t.v.String(),
			t.s.String(),
			t.n.String(),
		)
	}
}
