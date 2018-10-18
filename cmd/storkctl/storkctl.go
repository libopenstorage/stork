package main

import (
	"os"

	_ "github.com/libopenstorage/stork/drivers/volume/portworx"
	"github.com/libopenstorage/stork/pkg/storkctl"
)

func main() {
	if err := storkctl.NewCommand(os.Stdin, os.Stdout, os.Stdout).Execute(); err != nil {
		os.Exit(1)
	}
}
