package main

import (
	"os"

	_ "github.com/libopenstorage/stork/drivers/volume/portworx"
	"github.com/libopenstorage/stork/pkg/storkctl"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

func main() {
	factory := storkctl.NewFactory()
	if err := storkctl.NewCommand(factory, os.Stdin, os.Stdout, os.Stdout).Execute(); err != nil {
		os.Exit(1)
	}
}
