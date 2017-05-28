package scheduler

import (
	"bytes"
	"log"
	"os"
	"strings"

	dockerclient "github.com/fsouza/go-dockerclient"
)

var (
	endpoint string
	nodes    []string
)

type driver struct {
	docker *dockerclient.Client
}

func (d *driver) Init() error {
	log.Printf("Using the Docker scheduler driver.\n")
	log.Printf("Docker daemon is available at: %v.\n", endpoint)
	log.Printf("The following hosts are in the cluster: [%v].\n", nodes)

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
			Name:              "no",
			MaximumRetryCount: 0,
		},
	}

	config := dockerclient.Config{
		Image:        t.Img + ":" + t.Tag,
		AttachStdout: true,
		AttachStderr: true,
		Cmd:          []string{t.Cmd},
	}

	co := dockerclient.CreateContainerOptions{
		Name:       t.Name,
		Config:     &config,
		HostConfig: &hostConfig}
	if con, err := d.docker.CreateContainer(co); err != nil {
		return nil, err
	} else {
		context.Name = t.Name
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
	if status, err := d.docker.WaitContainer(ctx.Id); err != nil {
		return err
	} else {
		buf := bytes.NewBuffer([]byte(""))
		lo := dockerclient.LogsOptions{
			Container:    ctx.Id,
			Stdout:       true,
			Stderr:       false,
			RawTerminal:  false,
			Timestamps:   false,
			OutputStream: buf,
		}
		if err := d.docker.Logs(lo); err != nil {
			return err
		}
		ctx.Stdout = buf.String()

		buf = bytes.NewBuffer([]byte(""))
		lo = dockerclient.LogsOptions{
			Container:    ctx.Id,
			Stdout:       false,
			Stderr:       true,
			RawTerminal:  false,
			Timestamps:   false,
			OutputStream: buf,
		}
		if err := d.docker.Logs(lo); err != nil {
			return err
		}
		ctx.Stderr = buf.String()

		ctx.Status = status
	}

	return nil
}

func (d *driver) Destroy(ctx *Context) error {
	opts := dockerclient.RemoveContainerOptions{ID: ctx.Id}
	if err := d.docker.RemoveContainer(opts); err != nil {
		log.Panicf("Unable to delete task %v: %v\n", ctx.Name, err)
		return err
	}
	return nil
}

func (d *driver) InspectVolume(name string) (*Volume, error) {
	v := Volume{}
	return &v, nil
}

func init() {
	if endpoint = os.Getenv("DOCKER_HOST"); endpoint == "" {
		endpoint = "unix:///var/run/docker.sock"
	}

	nodes = strings.Split(os.Getenv("CLUSTER_NODES"), ",")

	register("docker", &driver{})
}
