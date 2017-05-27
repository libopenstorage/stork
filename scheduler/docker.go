package scheduler

import (
// _dockerclient "github.com/fsouza/go-dockerclient"
)

type driver struct {
}

func (d *driver) Init() error {
	return nil
}

func (d *driver) GetNodes() ([]string, error) {
	nodes := make([]string, 0)

	return nodes, nil
}

func (d *driver) Create(t Task) (*Context, error) {
	return &Context{}, nil
}

func (d *driver) Run(ctx *Context) error {
	return nil
}

func (d *driver) InspectVolume(name string) (*Volume, error) {
	v := Volume{}
	return &v, nil
}

func init() {
	register("docker", &driver{})
}
