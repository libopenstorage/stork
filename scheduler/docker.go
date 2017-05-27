package scheduler

import (
	dockerclient "github.com/fsouza/go-dockerclient"
)

var (
	endpoint = "unix:///var/run/docker.sock"
)

type driver struct {
	docker *dockerclient.Client
}

func (d *driver) Init() error {
	if docker, err := dockerclient.NewClient(endpoint); err != nil {
		return err
	} else {
		if err = docker.Ping(); err != nil {
			return err
		}
		d.docker = docker
	}
	return nil
}

func (d *driver) GetNodes() ([]string, error) {
	nodes := make([]string, 0)

	return nodes, nil
}

func (d *driver) Create(t Task) (*Context, error) {
	context := Context{}

	po := dockerclient.PullImageOptions{
		Repository: t.Img,
		Tag:        t.Tag,
	}

	if err := d.docker.PullImage(
		po,
		dockerclient.AuthConfiguration{},
	); err != nil {
		return nil, err
	}

	hostConfig := dockerclient.HostConfig{
		RestartPolicy: dockerclient.RestartPolicy{
			Name:              "unless-stopped",
			MaximumRetryCount: 0,
		},
	}

	config := dockerclient.Config{
		Image: t.Img + ":" + t.Tag,
		Cmd:   []string{t.Cmd},
	}

	co := dockerclient.CreateContainerOptions{
		Name:       t.Name,
		Config:     &config,
		HostConfig: &hostConfig}
	if con, err := d.docker.CreateContainer(co); err != nil {
		return nil, err
	} else {
		context.Id = con.ID
	}

	return &context, nil
}

func (d *driver) Run(ctx *Context) error {
	hostConfig := dockerclient.HostConfig{
		RestartPolicy: dockerclient.RestartPolicy{
			Name:              "unless-stopped",
			MaximumRetryCount: 0,
		},
	}

	if err := d.docker.StartContainer(ctx.Id, &hostConfig); err != nil {
		return err
	}

	// Wait for the container to exit and collect it's stdout and stderr.

	return nil
}

func (d *driver) InspectVolume(name string) (*Volume, error) {
	v := Volume{}
	return &v, nil
}

func init() {
	register("docker", &driver{})
}
