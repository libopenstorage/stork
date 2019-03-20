package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/libopenstorage/stork/drivers/volume"
	_ "github.com/libopenstorage/stork/drivers/volume/portworx"
	"github.com/libopenstorage/stork/pkg/cluster"
	"github.com/libopenstorage/stork/pkg/controller"
	"github.com/libopenstorage/stork/pkg/extender"
	"github.com/libopenstorage/stork/pkg/groupsnapshot"
	"github.com/libopenstorage/stork/pkg/initializer"
	"github.com/libopenstorage/stork/pkg/migration"
	"github.com/libopenstorage/stork/pkg/monitor"
	"github.com/libopenstorage/stork/pkg/pvcwatcher"
	"github.com/libopenstorage/stork/pkg/rule"
	"github.com/libopenstorage/stork/pkg/schedule"
	"github.com/libopenstorage/stork/pkg/snapshot"
	"github.com/libopenstorage/stork/pkg/version"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	api_v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	core_v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	componentconfig "k8s.io/kubernetes/pkg/apis/componentconfig/v1alpha1"
)

const (
	defaultLockObjectName      = "stork"
	defaultLockObjectNamespace = "kube-system"
	eventComponentName         = "stork"
)

var ext *extender.Extender

func main() {
	// Parse empty flags to suppress warnings from the snapshotter which uses
	// glog
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		log.Warnf("Error parsing flag: %v", err)
	}
	err = flag.Set("logtostderr", "true")
	if err != nil {
		log.Fatalf("Error setting glog flag: %v", err)
	}

	app := cli.NewApp()
	app.Name = "stork"
	app.Usage = "STorage Orchestartor Runtime for Kubernetes (STORK)"
	app.Version = version.Version
	app.Action = run

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "verbose",
			Usage: "Enable verbose logging",
		},
		cli.StringFlag{
			Name:  "driver,d",
			Usage: "Storage driver name",
		},
		cli.BoolTFlag{
			Name:  "leader-elect",
			Usage: "Enable leader election (default: true)",
		},
		cli.StringFlag{
			Name:  "lock-object-name",
			Usage: "Name for the lock object (default: stork)",
			Value: defaultLockObjectName,
		},
		cli.StringFlag{
			Name:  "lock-object-namespace",
			Usage: "Namespace for the lock object (default: kube-system)",
			Value: defaultLockObjectNamespace,
		},
		cli.BoolTFlag{
			Name:  "snapshotter",
			Usage: "Enable snapshotter (default: true)",
		},
		cli.BoolTFlag{
			Name:  "extender",
			Usage: "Enable scheduler extender for hyperconvergence (default: true)",
		},
		cli.BoolTFlag{
			Name:  "health-monitor",
			Usage: "Enable health monitoring of the storage driver (default: true)",
		},
		cli.Int64Flag{
			Name:  "health-monitor-interval",
			Usage: "The interval in seconds to monitor the health of the storage driver (default: 120, min: 30)",
		},
		cli.BoolTFlag{
			Name:  "migration-controller",
			Usage: "Start the migration controller (default: true)",
		},
		cli.BoolFlag{
			Name:  "app-initializer",
			Usage: "EXPERIMENTAL: Enable application initializer to update scheduler name automatically (default: false)",
		},
		cli.StringFlag{
			Name:  "migration-admin-namespace",
			Usage: "Namespace to be used by a cluster admin which can migrate all other namespaces (default: none)",
		},
		cli.BoolFlag{
			Name:  "storage-cluster-controller",
			Usage: "Start the storage cluster controller (default: false)",
		},
		cli.BoolTFlag{
			Name:  "pvc-watcher",
			Usage: "Start the controller to monitor PVC creation and deletions (default: true)",
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatalf("Error starting stork: %v", err)
	}
}

func run(c *cli.Context) {

	log.Infof("Starting stork version %v", version.Version)
	driverName := c.String("driver")
	if len(driverName) == 0 {
		log.Fatalf("driver option is required")
	}

	verbose := c.Bool("verbose")
	if verbose {
		log.SetLevel(log.DebugLevel)
	}

	d, err := volume.Get(driverName)
	if err != nil {
		log.Fatalf("Error getting Stork Driver %v: %v", driverName, err)
	}

	if err = d.Init(nil); err != nil {
		log.Fatalf("Error initializing Stork Driver %v: %v", driverName, err)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Error getting cluster config: %v", err)
	}

	k8sClient, err := clientset.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error getting client, %v", err)
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&core_v1.EventSinkImpl{Interface: core_v1.New(k8sClient.Core().RESTClient()).Events("")})
	recorder := eventBroadcaster.NewRecorder(legacyscheme.Scheme, api_v1.EventSource{Component: eventComponentName})

	if c.Bool("extender") {
		ext = &extender.Extender{
			Driver: d,
		}

		if err = ext.Start(); err != nil {
			log.Fatalf("Error starting scheduler extender: %v", err)
		}
	}

	runFunc := func(_ <-chan struct{}) {
		runStork(d, recorder, c)
	}

	if c.BoolT("leader-elect") {

		lockObjectName := c.String("lock-object-name")
		lockObjectNamespace := c.String("lock-object-namespace")

		id, err := os.Hostname()
		if err != nil {
			log.Fatalf("Error getting hostname: %v", err)
		}

		lockConfig := resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: recorder,
		}

		resourceLock, err := resourcelock.New(
			resourcelock.ConfigMapsResourceLock,
			lockObjectNamespace,
			lockObjectName,
			k8sClient.CoreV1(),
			lockConfig)
		if err != nil {
			log.Fatalf("Error creating resource lock: %v", err)
		}

		defaultConfig := &componentconfig.LeaderElectionConfiguration{}
		componentconfig.SetDefaults_LeaderElectionConfiguration(defaultConfig)

		leaderElectionConfig := leaderelection.LeaderElectionConfig{
			Lock:          resourceLock,
			LeaseDuration: defaultConfig.LeaseDuration.Duration,
			RenewDeadline: defaultConfig.RenewDeadline.Duration,
			RetryPeriod:   defaultConfig.RetryPeriod.Duration,

			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: runFunc,
				OnStoppedLeading: func() {
					log.Fatalf("Stork lost master")
				},
			},
		}
		leaderElector, err := leaderelection.NewLeaderElector(leaderElectionConfig)
		if err != nil {
			log.Fatalf("Error creating leader elector: %v", err)
		}

		leaderElector.Run()
	} else {
		runFunc(nil)
	}
}

func runStork(d volume.Driver, recorder record.EventRecorder, c *cli.Context) {
	if err := controller.Init(); err != nil {
		log.Fatalf("Error initializing controller: %v", err)
	}

	if err := rule.Init(); err != nil {
		log.Fatalf("Error initializing rule: %v", err)
	}

	initializer := &initializer.Initializer{
		Driver: d,
	}
	if c.Bool("app-initializer") {
		if err := initializer.Start(); err != nil {
			log.Fatalf("Error starting initializer: %v", err)
		}
	}

	monitor := &monitor.Monitor{
		Driver:      d,
		IntervalSec: c.Int64("health-monitor-interval"),
	}

	if c.Bool("health-monitor") {
		if err := monitor.Start(); err != nil {
			log.Fatalf("Error starting storage monitor: %v", err)
		}
	}

	if err := schedule.Init(); err != nil {
		log.Fatalf("Error initializing schedule: %v", err)
	}

	snapshot := &snapshot.Snapshot{
		Driver:   d,
		Recorder: recorder,
	}
	if c.Bool("snapshotter") {
		if err := snapshot.Start(); err != nil {
			log.Fatalf("Error starting snapshot controller: %v", err)
		}

		groupsnapshotInst := groupsnapshot.GroupSnapshot{
			Driver:   d,
			Recorder: recorder,
		}
		if err := groupsnapshotInst.Init(); err != nil {
			log.Fatalf("Error initializing groupsnapshot controller: %v", err)
		}
	}
	pvcWatcher := pvcwatcher.PVCWatcher{
		Driver:   d,
		Recorder: recorder,
	}
	if c.Bool("pvc-watcher") {
		if err := pvcWatcher.Start(); err != nil {
			log.Fatalf("Error starting pvc watcher: %v", err)
		}
	}

	if c.Bool("migration-controller") {
		migrationAdminNamespace := c.String("migration-admin-namespace")
		migration := migration.Migration{
			Driver:   d,
			Recorder: recorder,
		}
		if err := migration.Init(migrationAdminNamespace); err != nil {
			log.Fatalf("Error initializing migration: %v", err)
		}
	}

	if c.Bool("storage-cluster-controller") {
		clusterController := cluster.Controller{
			Recorder: recorder,
		}
		if err := clusterController.Init(); err != nil {
			log.Fatalf("Error initializing cluster controller: %v", err)
		}
	}

	// The controller should be started at the end
	err := controller.Run()
	if err != nil {
		log.Fatalf("Error starting controller: %v", err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		<-signalChan
		log.Printf("Shutdown signal received, exiting...")
		if c.Bool("extender") {
			if err := ext.Stop(); err != nil {
				log.Warnf("Error stopping extender: %v", err)
			}
		}
		if c.Bool("health-monitor") {
			if err := monitor.Stop(); err != nil {
				log.Warnf("Error stopping monitor: %v", err)
			}
		}
		if c.Bool("snapshotter") {
			if err := snapshot.Stop(); err != nil {
				log.Warnf("Error stopping snapshot controllers: %v", err)
			}
		}
		if c.Bool("app-initializer") {
			if err := initializer.Stop(); err != nil {
				log.Warnf("Error stopping app-initializer: %v", err)
			}
		}
		if err := d.Stop(); err != nil {
			log.Warnf("Error stopping driver: %v", err)
		}
		os.Exit(0)
	}
}
