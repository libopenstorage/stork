package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/libopenstorage/stork/drivers/volume"
	_ "github.com/libopenstorage/stork/drivers/volume/portworx"
	"github.com/libopenstorage/stork/pkg/extender"
	"github.com/libopenstorage/stork/pkg/monitor"
	"github.com/libopenstorage/stork/pkg/snapshot"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

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
	app.Version = "0.1"
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

	extender := &extender.Extender{
		Driver: d,
	}

	if err = extender.Start(); err != nil {
		log.Fatalf("Error starting scheduler extender: %v", err)
		os.Exit(-1)
	}

	monitor := &monitor.Monitor{
		Driver: d,
	}

	if err = monitor.Start(); err != nil {
		log.Fatalf("Error starting storage monitor: %v", err)
		os.Exit(-1)
	}

	snapshotController := &snapshotcontroller.SnapshotController{
		Driver: d,
	}

	if err = snapshotController.Start(); err != nil {
		log.Fatalf("Error starting snapshot controller: %v", err)
		os.Exit(-1)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-signalChan:
			log.Printf("Shutdown signal received, exiting...")
			if err := extender.Stop(); err != nil {
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
