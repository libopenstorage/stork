package main

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/libopenstorage/stork/drivers/volume"
	_ "github.com/libopenstorage/stork/drivers/volume/portworx"
	"github.com/libopenstorage/stork/pkg/extender"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "stork"
	app.Usage = "STorage ORchestartor for Kubernetes"
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

	if err = d.Init(); err != nil {
		log.Fatalf("Error initializing Stork Driver %v: %v", driverName, err)
		os.Exit(-1)
	}

	extender := &extender.Extender{
		Driver: d,
	}

	if err = extender.Init(); err != nil {
		log.Fatalf("Error initializing scheduler extender: %v", err)
		os.Exit(-1)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-signalChan:
			log.Printf("Shutdown signal received, exiting...")
			os.Exit(0)
		}
	}
}
