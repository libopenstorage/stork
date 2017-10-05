package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/portworx/torpedo/drivers/node"
	_ "github.com/portworx/torpedo/drivers/node/aws"
	_ "github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/portworx/torpedo/drivers/scheduler"
	_ "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/volume"
	_ "github.com/portworx/torpedo/drivers/volume/portworx"
	"github.com/portworx/torpedo/pkg/errors"
)

const (
	// DefaultSpecsRoot specifies the default location of the base specs directory in the torpedo container
	DefaultSpecsRoot     = "/specs"
	schedulerCliFlag     = "scheduler,s"
	nodeDriverCliFlag    = "node-driver,n"
	storageDriverCliFlag = "storage,v"
	testsCliFlag         = "tests,t"
	logLocationCliFlag   = "log-location"
)

type torpedo struct {
	instanceID string
	s          scheduler.Driver
	v          volume.Driver
	n          node.Driver
	logLoc     string
}

// testDriverFunc runs a specific external storage test case.  It takes
// in a scheduler driver and an external volume provider as arguments.
type testDriverFunc func() error

// testSetupTearDown performs basic test of starting an application and destroying it (along with storage)
func (t *torpedo) testSetupTearDown() error {
	taskName := fmt.Sprintf("setupteardown-%v", t.instanceID)

	logrus.Infof("[Test: %v] Scheduling new applications", taskName)
	contexts, err := t.s.Schedule(taskName, scheduler.ScheduleOptions{})
	if err != nil {
		return err
	}

	for _, ctx := range contexts {
		logrus.Infof("[Test: %v] Validating %v", taskName, ctx.App.Key)
		if err := t.validateContext(ctx); err != nil {
			return err
		}
	}

	opts := make(map[string]bool)
	opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
	for _, ctx := range contexts {
		logrus.Infof("[Test: %v] Tearing down %v", taskName, ctx.App.Key)
		if err := t.tearDownContext(ctx, opts); err != nil {
			return err
		}
	}

	return err
}

// Volume Driver Plugin is down, unavailable - and the client container should
// not be impacted.
func (t *torpedo) testDriverDown() error {
	taskName := fmt.Sprintf("driverdown-%v", t.instanceID)
	logrus.Infof("[Test: %v] Scheduling new applications", taskName)

	contexts, err := t.s.Schedule(taskName, scheduler.ScheduleOptions{})
	if err != nil {
		return err
	}

	opts := make(map[string]bool)
	opts[scheduler.OptionsWaitForResourceLeakCleanup] = true
	for _, ctx := range contexts {
		logrus.Infof("[Test: %v] Validating %v", taskName, ctx.App.Key)
		if err := t.validateContext(ctx); err != nil {
			return err
		}
	}

	for _, ctx := range contexts {
		appNodes, err := t.s.GetNodesForApp(ctx)
		if err != nil {
			return err
		}

		if len(appNodes) == 0 {
			return fmt.Errorf("error: found 0 nodes for app: %v (uid: %v)", ctx.App.Key, ctx.UID)
		}

		logrus.Infof("[Test: %v] Stopping volume driver: %v on app: %v nodes",
			taskName, t.v.String(), ctx.App.Key)
		for _, n := range appNodes {
			if err := t.v.StopDriver(n); err != nil {
				return err
			}
		}

		// Sleep for apps to get going...
		time.Sleep(40 * time.Second)

		logrus.Infof("[Test: %v] Re-starting volume driver on app nodes", taskName)
		for _, n := range appNodes {
			if err := t.v.StartDriver(n); err != nil {
				return err
			}
		}

		logrus.Infof("[Test: %v] Waiting for volume driver:%v to start", taskName, t.v.String())
		for _, n := range appNodes {
			if err := t.v.WaitStart(n); err != nil {
				return err
			}
		}

		logrus.Infof("[Test: %v] Re-validating %v", taskName, ctx.App.Key)
		if err := t.validateContext(ctx); err != nil {
			return err
		}

		logrus.Infof("[Test: %v] Tearing down %v", taskName, ctx.App.Key)
		if err := t.tearDownContext(ctx, opts); err != nil {
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
	logrus.Infof("[Test: %v] Scheduling new applications", taskName)
	contexts, err := t.s.Schedule(taskName, scheduler.ScheduleOptions{})
	if err != nil {
		return err
	}

	for _, ctx := range contexts {
		logrus.Infof("[Test: %v] Validating %v", taskName, ctx.App.Key)
		if err := t.validateContext(ctx); err != nil {
			return err
		}
	}

	for _, ctx := range contexts {
		appNodes, err := t.s.GetNodesForApp(ctx)
		if err != nil {
			return err
		}

		if len(appNodes) == 0 {
			return fmt.Errorf("error: found 0 nodes for app: %v (uid: %v)", ctx.App.Key, ctx.UID)
		}

		logrus.Infof("[Test: %v] Stopping volume driver: %v on app: %v nodes",
			taskName, t.v.String(), ctx.App.Key)
		for _, n := range appNodes {
			if err := t.v.StopDriver(n); err != nil {
				return err
			}
		}

		// Sleep for apps to get going...
		time.Sleep(40 * time.Second)

		logrus.Infof("[Test: %v] Destroying application: %v", taskName, ctx.App.Key)
		if err := t.s.Destroy(ctx, nil); err != nil {
			return err
		}

		logrus.Infof("[Test: %v] Re-starting volume driver on app: %v nodes", taskName, ctx.App.Key)
		for _, n := range appNodes {
			if err := t.v.StartDriver(n); err != nil {
				return err
			}
		}

		logrus.Infof("[Test: %v] Waiting for volume driver: %v to start", taskName, t.v.String())
		for _, n := range appNodes {
			if err := t.v.WaitStart(n); err != nil {
				return err
			}
		}

		// Wait for applications to be destroyed
		if err := t.s.WaitForDestroy(ctx); err != nil {
			return err
		}

		logrus.Infof("[Test: %v] Tearing down storage for: %v", taskName, ctx.App.Key)
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
	logrus.Infof("[Test: %v] Scheduling new applications", taskName)
	contexts, err := t.s.Schedule(taskName, scheduler.ScheduleOptions{})
	if err != nil {
		return err
	}

	for _, ctx := range contexts {
		logrus.Infof("[Test: %v] Validating %v", taskName, ctx.App.Key)
		if err := t.validateContext(ctx); err != nil {
			return err
		}
	}

	for _, ctx := range contexts {
		logrus.Infof("[Test: %v] Destroying tasks for application: %v", taskName, ctx.App.Key)
		if err := t.s.DeleteTasks(ctx); err != nil {
			return err
		}

		logrus.Infof("[Test: %v] Re-validating %v", taskName, ctx.App.Key)
		if err := t.validateContext(ctx); err != nil {
			return err
		}

		logrus.Infof("[Test: %v] Tearing down %v", taskName, ctx.App.Key)
		if err := t.tearDownContext(ctx, nil); err != nil {
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

	logrus.Infof("[Test: %v] Scheduling new applications", taskName)
	contexts, err := t.s.Schedule(taskName, scheduler.ScheduleOptions{})
	if err != nil {
		return err
	}

	for _, ctx := range contexts {
		logrus.Infof("[Test: %v] Validating %v", taskName, ctx.App.Key)
		if err := t.validateContext(ctx); err != nil {
			return err
		}
	}

	for _, ctx := range contexts {
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
			logrus.Infof("[Test: %v] Rebooting: %v as app: %v is running on it", taskName, n.Name, ctx.App.Key)
			if err := t.n.RebootNode(n, node.RebootNodeOpts{
				Force: false,
			}); err != nil {
				return err
			}
		}

		time.Sleep(20 * time.Second)

		// Wait for node to be back
		for _, n := range nodesToReboot {
			logrus.Infof("[Test: %v] Testing connectivity with: %v", taskName, n.Name)
			if err := t.n.TestConnection(n, node.ConnectionOpts{
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

		logrus.Infof("[Test: %v] Re-validating %v", taskName, ctx.App.Key)
		if err := t.validateContext(ctx); err != nil {
			return err
		}

		logrus.Infof("[Test: %v] Tearing down %v", taskName, ctx.App.Key)
		if err := t.tearDownContext(ctx, nil); err != nil {
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

	if err = t.validateVolumes(ctx); err != nil {
		gsbErr := t.generateSupportBundle(ctx)
		if gsbErr != nil {
			logrus.Warnf("In attempting to generate support bundle, Err: %v", gsbErr)
		}
		return err
	}
	if err = t.s.WaitForRunning(ctx); err != nil {
		gsbErr := t.generateSupportBundle(ctx)
		if gsbErr != nil {
			logrus.Warnf("In attempting to generate support bundle, Err: %v", gsbErr)
		}
		return err
	}
	return err
}

// generateSupportBundle gathers logs and any artifacts pertinent to the scheduler and dumps them in the defined location
func (t *torpedo) generateSupportBundle(ctx *scheduler.Context) error {
	if ctx == nil || t.s == nil {
		return fmt.Errorf("Invalid context or scheduler. Cannot generate support bundle")
	}
	out, err := t.s.Describe(ctx)
	if err != nil {
		return fmt.Errorf("Couldn't generate support bundle for torpedo instance: %s. Err: %v", t.instanceID, err)
	}
	err = ioutil.WriteFile(fmt.Sprintf("%s/supportbundle_%s_%v.log", t.logLoc, t.instanceID, time.Now().Format(time.RFC3339)), []byte(out), 0644)
	if err != nil {
		return fmt.Errorf("Couldn't write the support bundle file because of err: %v", err)
	}
	return nil
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

func (t *torpedo) tearDownContext(ctx *scheduler.Context, opts map[string]bool) error {
	var err error
	if err = t.s.Destroy(ctx, opts); err != nil {
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
func (t *torpedo) run(tests string) error {
	logrus.Printf("Running torpedo instance: %v", t.instanceID)

	if err := t.s.Init(path.Join(DefaultSpecsRoot, t.s.String()), t.n.String()); err != nil {
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

	if tests != "" {
		testList := strings.Split(tests, ",")
		for _, testName := range testList {
			logrus.Printf("***** Test %v starting *****\n", testName)
			f, ok := testFuncs[testName]
			if !ok {
				return &errors.ErrNotFound{
					ID:   testName,
					Type: "Test",
				}
			}

			if err := f(); err != nil {
				logrus.Printf("***** Test %v Failed with Error: %v *****\n", testName, err)
			} else {
				logrus.Printf("***** Test %v Passed *****\n", testName)
			}
		}
	} else {
		for n, f := range testFuncs {
			logrus.Infof("***** Test %v starting *****\n", n)
			if err := f(); err != nil {
				logrus.Printf("***** Test %v Failed with Error: %v *****\n", n, err)
			} else {
				logrus.Printf("***** Test %v Passed *****\n", n)
			}
		}
	}

	return nil
}

func main() {

	app := cli.NewApp()
	app.Name = "torpedo"
	app.Usage = "Run the torpedo scheduler test suite."

	app.Commands = []cli.Command{
		{
			Name:    "fire",
			Aliases: []string{"f"},
			Usage:   "Starts the basic test suite",
			Action:  fireTorpedo,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  schedulerCliFlag,
					Usage: "Name of the scheduler to use",
					Value: "k8s",
				},
				cli.StringFlag{
					Name:  storageDriverCliFlag,
					Usage: "Name of the storage driver to use",
					Value: "pxd",
				},
				cli.StringFlag{
					Name:  nodeDriverCliFlag,
					Usage: "Name of the node driver to use",
					Value: "ssh",
				},
				cli.StringFlag{
					Name:  testsCliFlag,
					Usage: "Comma-separated list of tests. [Default:  runs all the tests]",
					Value: "",
				},
				cli.StringFlag{
					Name:  logLocationCliFlag,
					Usage: "Path to save logs/artifacts upon failure. Default: /mnt/torpedo_support_dir",
					Value: "/mnt/torpedo_support_dir",
				},
			},
		},
	}
	app.Run(os.Args)

}

func fireTorpedo(c *cli.Context) {
	s := c.String("scheduler")
	v := c.String("storage")
	n := c.String("node-driver")
	tests := c.String("tests")
	logLoc := c.String("log-location")

	if schedulerDriver, err := scheduler.Get(s); err != nil {
		logrus.Fatalf("Cannot find scheduler driver for %v. Err: %v\n", s, err)
		os.Exit(-1)
	} else if volumeDriver, err := volume.Get(v); err != nil {
		logrus.Fatalf("Cannot find volume driver for %v. Err: %v\n", v, err)
		os.Exit(-1)
	} else if nodeDriver, err := node.Get(n); err != nil {
		logrus.Fatalf("Cannot find node driver for %v. Err: %v\n", n, err)
		os.Exit(-1)
	} else if err := os.MkdirAll(logLoc, os.ModeDir); err != nil {
		logrus.Fatalf("Cannot create path %s for saving support bundle. Error: %v", logLoc, err)
	} else {
		t := torpedo{
			instanceID: time.Now().Format("01-02-15h04m05s"),
			s:          schedulerDriver,
			v:          volumeDriver,
			n:          nodeDriver,
			logLoc:     logLoc,
		}

		if err := t.run(tests); err != nil {
			logrus.Infof("Torpedo failed with the following error : %v", err)
			os.Exit(-1)
		}

		logrus.Printf("Torpedo completed with volume driver: %v, and scheduler: %v, node: %v\n",
			t.v.String(),
			t.s.String(),
			t.n.String(),
		)
	}
}

func init() {
	logrus.SetLevel(logrus.InfoLevel)
	logrus.StandardLogger().Hooks.Add(NewHook())
}
