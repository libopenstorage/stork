package scheduler

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	dockerclient "github.com/fsouza/go-dockerclient"
)

var (
	nodes []string
)

type driver struct {
}

func ifaceToIP(iface *net.Interface) (string, error) {
	addrs, err := iface.Addrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip == nil || ip.IsLoopback() {
			continue
		}
		ip = ip.To4()
		if ip == nil {
			continue // not an ipv4 address
		}
		if ip.String() == "" {
			continue // address is empty string
		}
		return ip.String(), nil
	}

	return "", fmt.Errorf("node not connected to the network")
}

func connect(ip string) (*dockerclient.Client, string, error) {
	if ip == ExternalHost {
		// Find any other host except this one.
		ifaces, err := net.Interfaces()
		if err != nil {
			return nil, "", err
		}

		for _, n := range nodes {
			localIP := false
			for _, iface := range ifaces {
				if iface.Flags&net.FlagUp == 0 {
					continue // interface down
				}
				if iface.Flags&net.FlagLoopback != 0 {
					continue // loopback interface
				}

				ifaceIP, err := ifaceToIP(&iface)
				if err != nil {
					continue
				}

				if ifaceIP == n {
					localIP = true
					break
				}
			}

			if !localIP {
				log.Printf("Selecting Docker host %v\n", n)
				ip = n
				break
			}
		}
	}

	if ip == ExternalHost {
		return nil, "", fmt.Errorf("cannot find any other Docker host in the cluster")
	}

	endpoint := ""
	if ip == "" {
		if endpoint = os.Getenv("DOCKER_HOST"); endpoint == "" {
			endpoint = "unix:///var/run/docker.sock"
		}
	} else {
		endpoint = "http://" + ip + ":2375"
	}

	docker, err := dockerclient.NewClient(endpoint)
	if err != nil {
		return nil, "", err
	}

	if err = docker.Ping(); err != nil {
		return nil, "", err
	}

	return docker, ip, nil
}

func (d *driver) Init() error {
	log.Printf("Using the Docker scheduler driver.\n")
	log.Printf("The following hosts are in the cluster: %v.\n", nodes)
	return nil
}

func (d *driver) GetNodes() ([]string, error) {
	nodes := make([]string, 0)
	return nodes, nil
}

func (d *driver) Create(t Task) (*Context, error) {
	context := Context{}

	docker, ip, err := connect(t.IP)
	if err != nil {
		return nil, err
	}

	t.IP = ip

	po := dockerclient.PullImageOptions{
		Repository: t.Img,
		Tag:        t.Tag,
	}

	if err := docker.PullImage(
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
		Binds: []string{
			t.Vol.Name + ":" + t.Vol.Path,
		},
		VolumeDriver: t.Vol.Driver,
	}

	config := dockerclient.Config{
		Image:        t.Img + ":" + t.Tag,
		AttachStdout: true,
		AttachStderr: true,
		Cmd:          t.Cmd,
	}

	co := dockerclient.CreateContainerOptions{
		Name:       t.Name,
		Config:     &config,
		HostConfig: &hostConfig,
	}

	con, err := docker.CreateContainer(co)
	if err != nil {
		return nil, err
	}

	context.Task = t
	context.ID = con.ID

	return &context, nil
}

// Run to completion.
func (d *driver) Run(ctx *Context) error {
	docker, _, err := connect(ctx.Task.IP)
	if err != nil {
		return err
	}

	hostConfig := dockerclient.HostConfig{
		RestartPolicy: dockerclient.RestartPolicy{
			Name:              "no",
			MaximumRetryCount: 0,
		},
		Binds: []string{
			ctx.Task.Vol.Name + ":" + ctx.Task.Vol.Path,
		},
		VolumeDriver: ctx.Task.Vol.Driver,
	}

	if err := docker.StartContainer(ctx.ID, &hostConfig); err != nil {
		return err
	}

	// Wait for the container to exit and collect it's stdout and stderr.
	status, err := docker.WaitContainer(ctx.ID)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer([]byte(""))
	lo := dockerclient.LogsOptions{
		Container:    ctx.ID,
		Stdout:       true,
		Stderr:       false,
		RawTerminal:  false,
		Timestamps:   false,
		OutputStream: buf,
	}
	if err := docker.Logs(lo); err != nil {
		return err
	}
	ctx.Stdout = buf.String()

	buf = bytes.NewBuffer([]byte(""))
	lo = dockerclient.LogsOptions{
		Container:    ctx.ID,
		Stdout:       false,
		Stderr:       true,
		RawTerminal:  false,
		Timestamps:   false,
		OutputStream: buf,
	}
	if err := docker.Logs(lo); err != nil {
		return err
	}
	ctx.Stderr = buf.String()
	ctx.Status = status

	return nil
}

func (d *driver) Start(ctx *Context) error {
	docker, _, err := connect(ctx.Task.IP)
	if err != nil {
		return err
	}

	hostConfig := dockerclient.HostConfig{
		RestartPolicy: dockerclient.RestartPolicy{
			Name:              "no",
			MaximumRetryCount: 0,
		},
		Binds: []string{
			ctx.Task.Vol.Name + ":" + ctx.Task.Vol.Path,
		},
		VolumeDriver: ctx.Task.Vol.Driver,
	}

	if err := docker.StartContainer(ctx.ID, &hostConfig); err != nil {
		return err
	}

	return nil
}

func (d *driver) WaitDone(ctx *Context) error {
	docker, _, err := connect(ctx.Task.IP)
	if err != nil {
		return err
	}

	// Wait for the container to exit and collect it's stdout and stderr.
	status, err := docker.WaitContainer(ctx.ID)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer([]byte(""))
	lo := dockerclient.LogsOptions{
		Container:    ctx.ID,
		Stdout:       true,
		Stderr:       false,
		RawTerminal:  false,
		Timestamps:   false,
		OutputStream: buf,
	}
	if err := docker.Logs(lo); err != nil {
		return err
	}
	ctx.Stdout = buf.String()

	buf = bytes.NewBuffer([]byte(""))
	lo = dockerclient.LogsOptions{
		Container:    ctx.ID,
		Stdout:       false,
		Stderr:       true,
		RawTerminal:  false,
		Timestamps:   false,
		OutputStream: buf,
	}
	if err := docker.Logs(lo); err != nil {
		return err
	}

	ctx.Stderr = buf.String()
	ctx.Status = status

	return nil
}

func (d *driver) Destroy(ctx *Context) error {
	docker, _, err := connect(ctx.Task.IP)
	if err != nil {
		return err
	}

	opts := dockerclient.RemoveContainerOptions{
		ID:            ctx.ID,
		Force:         true,
		RemoveVolumes: true,
	}
	if err := docker.RemoveContainer(opts); err != nil {
		return err
	}

	log.Printf("Deleted task: %v\n", ctx.Task.Name)
	return nil
}

func (d *driver) DestroyByName(ip, name string) error {
	docker, _, err := connect(ip)
	if err != nil {
		return err
	}

	lo := dockerclient.ListContainersOptions{
		All:  true,
		Size: false,
	}

	allContainers, err := docker.ListContainers(lo)
	if err != nil {
		return err
	}
	for _, c := range allContainers {
		info, err := docker.InspectContainer(c.ID)
		if err != nil {
			return err
		}

		if info.Name == "/"+name {
			if err = docker.StopContainer(c.ID, 0); err != nil {
				if _, ok := err.(*dockerclient.ContainerNotRunning); !ok {
					log.Printf("Error while stopping task %v: %v",
						info.Name,
						err,
					)
					return err
				}
			}
			ro := dockerclient.RemoveContainerOptions{
				ID:            c.ID,
				Force:         true,
				RemoveVolumes: true,
			}

			if err = docker.RemoveContainer(ro); err != nil {
				log.Printf(
					"Error while removing task %v: %v",
					info.Name,
					err,
				)
				return err
			}
			log.Printf("Deleted task: %v\n", name)
			return nil
		}
	}

	return nil
}

func (d *driver) InspectVolume(ip, name string) (*Volume, error) {
	docker, _, err := connect(ip)
	if err != nil {
		return nil, err
	}

	vol, err := docker.InspectVolume(name)
	if err != nil {
		return nil, err
	}
	// TODO: Get volume size in a generic way.
	v := Volume{
		// Size:   sz,
		Driver: vol.Driver,
	}
	return &v, nil
}

func (d *driver) DeleteVolume(ip, name string) error {
	docker, _, err := connect(ip)
	if err != nil {
		return err
	}

	if err := docker.RemoveVolume(name); err != nil {
		return err
	}

	// There is a bug with the dockerclient.  Even if the volume could not
	// be removed, it returns nil.  So make sure the volume was infact deleted.
	if _, err := docker.InspectVolume(name); err == nil {
		return fmt.Errorf("volume %v could not be deleted", name)
	}

	return nil
}

func init() {
	nodes = strings.Split(os.Getenv("CLUSTER_NODES"), ",")

	register("docker", &driver{})
}
