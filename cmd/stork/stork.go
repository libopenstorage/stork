package main

import (
	"flag"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/libopenstorage/stork/drivers/volume"
	_ "github.com/libopenstorage/stork/drivers/volume/portworx"
	stork "github.com/libopenstorage/stork/pkg/apis/stork.com"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork.com/v1alpha1"
	"github.com/libopenstorage/stork/pkg/extender"
	"github.com/libopenstorage/stork/pkg/initializer"
	"github.com/libopenstorage/stork/pkg/monitor"
	"github.com/libopenstorage/stork/pkg/snapshot"
	"github.com/portworx/talisman/pkg/crd"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	api_v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	clientset "k8s.io/client-go/kubernetes"
	core_v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/client/leaderelectionconfig"
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
	app.Version = "1.1.1"
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
			Value: "stork",
		},
		cli.StringFlag{
			Name:  "lock-object-namespace",
			Usage: "Namespace for the lock object (default: kube-system)",
			Value: "kube-system",
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
		cli.BoolFlag{
			Name:  "app-initializer",
			Usage: "EXPERIMENTAL: Enable application initializer to update scheduler name automatically (default: false)",
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatalf("Error starting stork: %v", err)
	}
}
func run(c *cli.Context) {

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

	if c.Bool("extender") {
		ext = &extender.Extender{
			Driver: d,
		}

		if err = ext.Start(); err != nil {
			log.Fatalf("Error starting scheduler extender: %v", err)
		}
	}

	runFunc := func(_ <-chan struct{}) {
		runStork(d, c)
	}

	leaderConfig := leaderelectionconfig.DefaultLeaderElectionConfiguration()
	leaderConfig.LeaderElect = c.BoolT("leader-elect")

	if leaderConfig.LeaderElect {
		config, err := rest.InClusterConfig()
		if err != nil {
			log.Fatalf("Error getting cluster config: %v", err)
		}

		k8sClient, err := clientset.NewForConfig(config)
		if err != nil {
			log.Fatalf("Error getting client, %v", err)
		}

		leaderConfig.ResourceLock = resourcelock.ConfigMapsResourceLock
		lockObjectName := c.String("lock-object-name")
		lockObjectNamespace := c.String("lock-object-namespace")

		eventBroadcaster := record.NewBroadcaster()
		eventBroadcaster.StartLogging(glog.Infof)
		eventBroadcaster.StartRecordingToSink(&core_v1.EventSinkImpl{Interface: core_v1.New(k8sClient.Core().RESTClient()).Events("")})
		recorder := eventBroadcaster.NewRecorder(legacyscheme.Scheme, api_v1.EventSource{Component: "stork"})

		id, err := os.Hostname()
		if err != nil {
			log.Fatalf("Error getting hostname: %v", err)
		}

		lockConfig := resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: recorder,
		}

		resourceLock, err := resourcelock.New(
			leaderConfig.ResourceLock,
			lockObjectNamespace,
			lockObjectName,
			k8sClient.CoreV1(),
			lockConfig)
		if err != nil {
			log.Fatalf("Error creating resource lock: %v", err)
		}

		leaderElector, err := leaderelection.NewLeaderElector(
			leaderelection.LeaderElectionConfig{
				Lock:          resourceLock,
				LeaseDuration: leaderConfig.LeaseDuration.Duration,
				RenewDeadline: leaderConfig.RenewDeadline.Duration,
				RetryPeriod:   leaderConfig.RetryPeriod.Duration,

				Callbacks: leaderelection.LeaderCallbacks{
					OnStartedLeading: runFunc,
					OnStoppedLeading: func() {
						log.Fatalf("Stork lost master")
					},
				},
			})
		if err != nil {
			log.Fatalf("Error creating leader elector: %v", err)
		}

		leaderElector.Run()
	} else {
		runFunc(nil)
	}
}

func runStork(d volume.Driver, c *cli.Context) {
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

	// Register the CRDs
	log.Infof("Registering stork CRDs")
	cfg, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Error building kubeconfig: %s", err.Error())
	}
	kubeClient := clientset.NewForConfigOrDie(cfg)
	apiExtClientset := apiextensionsclient.NewForConfigOrDie(cfg)
	ctx := crd.Context{
		Clientset:             kubeClient,
		APIExtensionClientset: apiExtClientset,
		Interval:              500 * time.Millisecond,
		Timeout:               60 * time.Second,
	}
	resources := []crd.CustomResource{
		{
			Name:    "storkrule",
			Plural:  "storkrules",
			Group:   stork.GroupName,
			Version: stork.Version,
			Scope:   apiextensionsv1beta1.NamespaceScoped,
			Kind:    reflect.TypeOf(storkapi.StorkRule{}).Name(),
		},
	}
	err = crd.CreateCRD(ctx, resources)
	if err != nil {
		log.Fatalf("failed to create CRD. Err: %v", err)
	}

	snapshotController := &snapshotcontroller.SnapshotController{
		Driver: d,
	}
	if c.Bool("snapshotter") {
		if err := d.PerformRecovery(); err != nil {
			log.Warnf("failed to perform recovery for driver: %s due to: %v", d.String(), err)
		}

		if err := snapshotController.Start(); err != nil {
			log.Fatalf("Error starting snapshot controller: %v", err)
		}
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
			if err := snapshotController.Stop(); err != nil {
				log.Warnf("Error stopping snapshot controller: %v", err)
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
