package main

import (
	"os"

	log "github.com/Sirupsen/logrus"

	"github.com/libopenstorage/stork/drivers"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage %v <driver name>", os.Args[0])
		os.Exit(-1)
	}

	drv := os.Args[1]

	d, err := drivers.Get(drv)
	if err != nil {
		log.Fatalf("Error getting Stork Driver %v: %v", drv, err)
		os.Exit(-1)
	}

	if err = d.Init(); err != nil {
		log.Fatalf("Error initializing Stork Driver %v: %v", drv, err)
		os.Exit(-1)
	}

	os.Exit(0)
}
