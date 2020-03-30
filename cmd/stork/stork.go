package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	_ "github.com/libopenstorage/stork/drivers/volume/aws"
	_ "github.com/libopenstorage/stork/drivers/volume/azure"
	_ "github.com/libopenstorage/stork/drivers/volume/gcp"
	_ "github.com/libopenstorage/stork/drivers/volume/portworx"
	"github.com/libopenstorage/stork/pkg/applicationmanager"
	"github.com/libopenstorage/stork/pkg/clusterdomains"
	"github.com/libopenstorage/stork/pkg/controller"
	"github.com/libopenstorage/stork/pkg/dataexport"
	"github.com/libopenstorage/stork/pkg/dbg"
	"github.com/libopenstorage/stork/pkg/extender"
	"github.com/libopenstorage/stork/pkg/groupsnapshot"
	"github.com/libopenstorage/stork/pkg/migration"
	"github.com/libopenstorage/stork/pkg/monitor"
	"github.com/libopenstorage/stork/pkg/pvcwatcher"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/libopenstorage/stork/pkg/rule"
	"github.com/libopenstorage/stork/pkg/schedule"
	"github.com/libopenstorage/stork/pkg/snapshot"
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/libopenstorage/stork/pkg/webhookadmission"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	api_v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	core_v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
)

const (
	defaultLockObjectName      = "stork"
	defaultLockObjectNamespace = "kube-system"
	defaultAdminNamespace      = "kube-system"
	eventComponentName         = "stork"
	debugFilePath              = "/var/cores"
)

var ext *extender.Extender
var webhook *webhookadmission.Controller

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
			Usage: "Name for the lock object",
			Value: defaultLockObjectName,
		},
		cli.StringFlag{
			Name:  "lock-object-namespace",
			Usage: "Namespace for the lock object",
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
			Value: 120,
			Usage: "The interval in seconds to monitor the health of the storage driver (min: 30)",
		},
		cli.BoolTFlag{
			Name:  "migration-controller",
			Usage: "Start the migration controller (default: true)",
		},
		cli.BoolTFlag{
			Name:  "application-controller",
			Usage: "Start the controllers for managing applications (default: true)",
		},
		cli.StringFlag{
			Name:  "admin-namespace",
			Value: defaultAdminNamespace,
			Usage: "Namespace to be used by a cluster admin which can migrate and backup all other namespaces",
		},
		cli.StringFlag{
			Name:  "migration-admin-namespace",
			Value: defaultAdminNamespace,
			Usage: "Namespace to be used by a cluster admin which can migrate all other namespaces (Deprecated, please use admin-namespace)",
		},
		cli.BoolTFlag{
			Name:  "cluster-domain-controllers",
			Usage: "Start the cluster domain controllers (default: true)",
		},
		cli.BoolTFlag{
			Name:  "pvc-watcher",
			Usage: "Start the controller to monitor PVC creation and deletions (default: true)",
		},
		cli.BoolTFlag{
			Name:  "data-export-controller",
			Usage: "Start the controller to monitor DataExport resources (default: true)",
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatalf("Error starting stork: %v", err)
	}
}

func run(c *cli.Context) {
	dbg.Init(c.App.Name, debugFilePath)

	log.Infof("Starting stork version %v", version.Version)
	driverName := c.String("driver")

	verbose := c.Bool("verbose")
	if verbose {
		log.SetLevel(log.DebugLevel)
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
	eventBroadcaster.StartRecordingToSink(&core_v1.EventSinkImpl{Interface: k8sClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, api_v1.EventSource{Component: eventComponentName})

	var d volume.Driver
	if driverName != "" {
		d, err = volume.Get(driverName)
		if err != nil {
			log.Fatalf("Error getting Stork Driver %v: %v", driverName, err)
		}

		if err = d.Init(nil); err != nil {
			log.Fatalf("Error initializing Stork Driver %v: %v", driverName, err)
		}

		if c.Bool("extender") {
			ext = &extender.Extender{
				Driver:   d,
				Recorder: recorder,
			}

			if err = ext.Start(); err != nil {
				log.Fatalf("Error starting scheduler extender: %v", err)
			}
		}
	}
	webhook = &webhookadmission.Controller{
		Driver:   d,
		Recorder: recorder,
	}
	if err := webhook.Start(); err != nil {
		log.Fatalf("error starting webhook controller: %v", err)
	}

	runFunc := func(context.Context) {
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
			k8sClient.CoordinationV1(),
			lockConfig)
		if err != nil {
			log.Fatalf("Error creating resource lock: %v", err)
		}

		leaderElectionConfig := leaderelection.LeaderElectionConfig{
			Lock:          resourceLock,
			LeaseDuration: 15 * time.Second,
			RenewDeadline: 10 * time.Second,
			RetryPeriod:   2 * time.Second,

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

		leaderElector.Run(context.Background())
	} else {
		runFunc(nil)
	}
}

func runStork(d volume.Driver, recorder record.EventRecorder, c *cli.Context) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	if err := controller.Init(); err != nil {
		log.Fatalf("Error initializing controller: %v", err)
	}

	if err := rule.Init(); err != nil {
		log.Fatalf("Error initializing rule: %v", err)
	}

	resourceCollector := resourcecollector.ResourceCollector{
		Driver: d,
	}
	if err := resourceCollector.Init(nil); err != nil {
		log.Fatalf("Error initializing ResourceCollector: %v", err)
	}
	adminNamespace := c.String("admin-namespace")
	if adminNamespace == "" {
		adminNamespace = c.String("migration-admin-namespace")
	}

	monitor := &monitor.Monitor{
		Driver:      d,
		IntervalSec: c.Int64("health-monitor-interval"),
	}
	snapshot := &snapshot.Snapshot{
		Driver:   d,
		Recorder: recorder,
	}
	if err := schedule.Init(); err != nil {
		log.Fatalf("Error initializing schedule: %v", err)
	}
	if d != nil {
		if c.Bool("health-monitor") {
			if err := monitor.Start(); err != nil {
				log.Fatalf("Error starting storage monitor: %v", err)
			}
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
			migration := migration.Migration{
				Driver:            d,
				Recorder:          recorder,
				ResourceCollector: resourceCollector,
			}
			if err := migration.Init(adminNamespace); err != nil {
				log.Fatalf("Error initializing migration: %v", err)
			}
		}

		if c.Bool("cluster-domain-controllers") {
			clusterDomains := clusterdomains.ClusterDomains{
				Driver:   d,
				Recorder: recorder,
			}
			if err := clusterDomains.Init(); err != nil {
				log.Fatalf("Error initializing cluster domain controllers: %v", err)
			}
		}
	}

	if c.Bool("application-controller") {
		appManager := applicationmanager.ApplicationManager{
			Driver:            d,
			Recorder:          recorder,
			ResourceCollector: resourceCollector,
		}
		if err := appManager.Init(adminNamespace, signalChan); err != nil {
			log.Fatalf("Error initializing application manager: %v", err)
		}
	}

	if c.Bool("data-export-controller") {
		de := dataexport.New()
		if err := de.Init(); err != nil {
			log.Fatalf("Error initializing data-export controller: %v", err)
		}
	}

	// The controller should be started at the end
	err := controller.Run()
	if err != nil {
		log.Fatalf("Error starting controller: %v", err)
	}

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
		if err := d.Stop(); err != nil {
			log.Warnf("Error stopping driver: %v", err)
		}
		if err := webhook.Stop(); err != nil {
			log.Warnf("error stopping webhook controller %v", err)
		}
		os.Exit(0)
	}
}
