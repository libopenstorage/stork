package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/golang/glog"
	"github.com/libopenstorage/stork/drivers/volume"
	_ "github.com/libopenstorage/stork/drivers/volume/portworx"
	"github.com/libopenstorage/stork/pkg/extender"
	"github.com/libopenstorage/stork/pkg/monitor"
	"github.com/libopenstorage/stork/pkg/snapshot"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	api_v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	core_v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/api"
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
	app.Version = "0.3"
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
			Usage: "Enable leader election",
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
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatalf("Error starting stork: %v", err)
	}
}
func run(c *cli.Context) {

	driverName := c.String("driver")
	if len(driverName) == 0 {
		log.Fatalf("driver option is required")
		os.Exit(-1)
	}

	verbose := c.Bool("verbose")
	if verbose {
		log.SetLevel(log.DebugLevel)
	}

	d, err := volume.Get(driverName)
	if err != nil {
		log.Fatalf("Error getting Stork Driver %v: %v", driverName, err)
		os.Exit(-1)
	}

	if err = d.Init(nil); err != nil {
		log.Fatalf("Error initializing Stork Driver %v: %v", driverName, err)
		os.Exit(-1)
	}

	ext = &extender.Extender{
		Driver: d,
	}

	if err = ext.Start(); err != nil {
		log.Fatalf("Error starting scheduler extender: %v", err)
		os.Exit(-1)
	}

	runFunc := func(_ <-chan struct{}) {
		runStork(d)
	}

	leaderConfig := leaderelectionconfig.DefaultLeaderElectionConfiguration()
	leaderConfig.LeaderElect = c.BoolT("leader-elect")

	if leaderConfig.LeaderElect {
		config, err := rest.InClusterConfig()
		if err != nil {
			log.Fatalf("Error getting cluster config: %v", err)
			os.Exit(-1)
		}

		k8sClient, err := clientset.NewForConfig(config)
		if err != nil {
			log.Fatalf("Error getting client, %v", err)
			os.Exit(-1)
		}

		leaderConfig.ResourceLock = resourcelock.ConfigMapsResourceLock
		lockObjectName := c.String("lock-object-name")
		lockObjectNamespace := c.String("lock-object-namespace")

		eventBroadcaster := record.NewBroadcaster()
		eventBroadcaster.StartLogging(glog.Infof)
		eventBroadcaster.StartRecordingToSink(&core_v1.EventSinkImpl{Interface: core_v1.New(k8sClient.Core().RESTClient()).Events("")})
		recorder := eventBroadcaster.NewRecorder(api.Scheme, api_v1.EventSource{Component: "stork"})

		id, err := os.Hostname()
		if err != nil {
			log.Fatalf("Error getting hostname: %v", err)
			os.Exit(-1)
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
			os.Exit(-1)
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
			os.Exit(-1)
		}

		leaderElector.Run()
	} else {
		runFunc(nil)
	}
}

func runStork(d volume.Driver) {
	monitor := &monitor.Monitor{
		Driver: d,
	}

	if err := monitor.Start(); err != nil {
		log.Fatalf("Error starting storage monitor: %v", err)
		os.Exit(-1)
	}

	snapshotController := &snapshotcontroller.SnapshotController{
		Driver: d,
	}

	if err := snapshotController.Start(); err != nil {
		log.Fatalf("Error starting snapshot controller: %v", err)
		os.Exit(-1)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-signalChan:
			log.Printf("Shutdown signal received, exiting...")
			if err := ext.Stop(); err != nil {
				log.Warnf("Error stopping extender: %v", err)
			}
			if err := monitor.Stop(); err != nil {
				log.Warnf("Error stopping monitor: %v", err)
			}
			if err := snapshotController.Stop(); err != nil {
				log.Warnf("Error stopping snapshot controller: %v", err)
			}
			os.Exit(0)
		}
	}
}
