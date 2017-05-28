package volume

import (
	dockerclient "github.com/fsouza/go-dockerclient"
)

type driver struct {
}

func (d *driver) String() string {
	return "pxd"
}

// Portworx runs as a container - so all we need to do is ask docker to
// stop the running portworx container.
func (d *driver) Stop(Ip string) error {
	// TODO: use IP
	endpoint := "unix:///var/run/docker.sock"
	docker, err := dockerclient.NewClient(endpoint)
	if err != nil {
		return err
	} else {
		if err = docker.Ping(); err != nil {
			return err
		}
	}

	// Find the Portworx container

	// Stop the Portworx container

	return nil
}

func (d *driver) Start(Ip string) error {
	return nil
}

func init() {
	register("pxd", &driver{})
}
