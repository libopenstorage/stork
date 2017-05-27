package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	dockerclient "github.com/fsouza/go-dockerclient"
	"os/exec"
	s "os/signal"
	"regexp"
	"syscall"
)

const (
	pxImage         = "portworx/px-enterprise"
	pxImageTag      = "latest"
	pxContainerName = "portworx"
)

func SignalHandlers(handlers map[os.Signal]func()) {
	c := make(chan os.Signal, 1)

	for sig := range handlers {
		s.Notify(c, sig)
	}

	go func() {
		for sig := range c {
			go func(sig os.Signal) {
				if hdler, ok := handlers[sig]; ok {
					fmt.Printf("calling signal handler '%v'\n", handlers[sig])
					hdler()
				} else {
					fmt.Printf("signal: %v - no signal handler function.\n", sig)
				}
			}(sig)
		}
	}()
}

func handlerSigTerm() {
	docker, err := getDockerClient()
	if err != nil || docker == nil {
		fmt.Printf("Failed to get docker client. Err: %v\n", err)
		return
	}

	cList, err := getPxContainers(docker)
	if err != nil {
		fmt.Errorf("Failed to list containers. Err: %v\n", err)
		return
	}

	for _, c := range cList {
		fmt.Printf("Stopping px container: %v (%v)\n", c.Names, c.ID)
		err = docker.StopContainer(c.ID, 30)
		if err != nil {
			fmt.Printf("Failed to stop px container. Err: %v\n", err)
			continue
		}

		fmt.Printf("Stopped px container: %v (%v) succesfully\n", c.Names, c.ID)

		fmt.Printf("Removing px container: %v (%v)\n", c.Names, c.ID)
		opts := dockerclient.RemoveContainerOptions{ID: c.ID}
		err = docker.RemoveContainer(opts)
		if err != nil {
			fmt.Printf("Failed to remove px container. Err: %v\n", err)
			continue
		}

		fmt.Printf("Removed px container: %v (%v) succesfully\n", c.Names, c.ID)
	}
}

func handlerSigKill() {
	docker, err := getDockerClient()
	if err != nil || docker == nil {
		fmt.Printf("Failed to get docker client. Err: %v\n", err)
		return
	}

	cList, err := getPxContainers(docker)
	if err != nil {
		fmt.Errorf("Failed to list containers. Err: %v\n", err)
		return
	}

	for _, c := range cList {
		fmt.Printf("Killing px container: %v (%v)\n", c.Names, c.ID)
		opts := dockerclient.KillContainerOptions{ID: c.ID}
		err = docker.KillContainer(opts)
		if err != nil {
			fmt.Printf("Failed to kill px container. Err: %v\n", err)
			continue
		}
		fmt.Printf("Killing px container: %v (%v) succesfully\n", c.Names, c.ID)

		fmt.Printf("Removing px container: %v (%v)\n", c.Names, c.ID)
		rmOpts := dockerclient.RemoveContainerOptions{ID: c.ID, Force: true}
		err = docker.RemoveContainer(rmOpts)
		if err != nil {
			fmt.Printf("Failed to remove px container. Err: %v\n", err)
			continue
		}

		fmt.Printf("Removed px container: %v (%v) succesfully\n", c.Names, c.ID)
	}

	os.Exit(0)
}

func getPxContainers(docker *dockerclient.Client) ([]dockerclient.APIContainers, error) {
	filters := make(map[string][]string)
	filters["ancestor"] = append(filters["ancestor"], pxImage+":"+pxImageTag)

	lo := dockerclient.ListContainersOptions{
		All:     true,
		Size:    false,
		Filters: filters,
	}

	cList, err := docker.ListContainers(lo)
	if err != nil {
		fmt.Errorf("Failed to list containers. Err: %v\n", err)
		return nil, err
	}

	return cList, nil
}

func enableSharedMounts() error {
	cmd := exec.Command("nsenter", "--mount=/media/host/proc/1/ns/mnt", "--", "mount", "--make-shared", "/")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		fmt.Printf("Failed to enable shared mounts. Err: %v Stderr: %v\n", err, stderr)
		return err
	}

	fmt.Println("Enabled shared mounts succesfully")
	return nil
}

func getDockerClient() (*dockerclient.Client, error) {
	docker, err := dockerclient.NewClient("unix:///var/run/docker.sock")
	if err != nil {
		fmt.Println("Could not connect to Docker... is Docker running on this host? ",
			err.Error())
		return nil, err
	}

	err = docker.Ping()
	if err != nil {
		fmt.Println("Could not connect to Docker... is Docker running on this host? ",
			err.Error())
		return nil, err
	}

	return docker, nil
}

func install(args []string) error {
	docker, err := getDockerClient()
	if err != nil || docker == nil {
		fmt.Printf("Failed to get docker client. Err: %v\n", err)
		return err
	}

	cList, err := getPxContainers(docker)
	if err != nil {
		fmt.Errorf("Failed to list containers. Err: %v\n", err)
		return err
	}

	for _, c := range cList {
		fmt.Printf("Found existing px container: %v (%v)\n", c.Names, c.ID)
		if c.State == "running" {
			fmt.Printf("px container: %v (%v) is already running.\n", c.Names, c.ID)
			return nil
		}

		opts := dockerclient.RemoveContainerOptions{ID: c.ID}
		err = docker.RemoveContainer(opts)
		if err != nil {
			fmt.Println("Could not remove existing Portworx container: ", err.Error())
			return err
		}
	}

	fmt.Println("Downloading Portworx...")

	origStdout := os.Stdout
	rPipe, wPipe, err := os.Pipe()
	if err != nil {
		fmt.Printf("Failed to get os Pipe. Err: %v", err)
		return err
	}

	os.Stdout = wPipe

	dpullOut := make(chan string)
	go func() {
		var bufout bytes.Buffer
		io.Copy(&bufout, rPipe)
		dpullOut <- bufout.String()
	}()

	po := dockerclient.PullImageOptions{
		Repository:        pxImage,
		Tag:               pxImageTag,
		OutputStream:      os.Stdout,
		InactivityTimeout: 840 * time.Second,
	}

	regUser := os.Getenv("REGISTRY_USER")
	regPass := os.Getenv("REGISTRY_PASS")
	if err = docker.PullImage(po, dockerclient.AuthConfiguration{
		Username: regUser,
		Password: regPass,
	}); err != nil {
		fmt.Println("Could not connect to Docker... is Docker running on this host?")
		return err
	}

	// Back to normal state.
	wPipe.Close()
	os.Stdout = origStdout
	dOutput := <-dpullOut
	pline := ""
	for _, line := range strings.Split(dOutput, "\n") {
		if len(line) > 0 && len(pline) > 0 {
			fmt.Printf("\r%s", strings.Repeat(" ", len(pline)))
		}
		if len(line) > 0 {
			fmt.Printf("\r%s", line)
			time.Sleep(800 * time.Millisecond)
		}
		pline = line
	}
	fmt.Print("\n")

	fmt.Println("Starting Portworx...")

	dInfo, err := docker.Info()
	if err != nil {
		fmt.Printf("Failed to get docker info. Err: %v\n", err)
		return err
	}

	hdrs := "/usr/src"
	matched, _ := regexp.MatchString(".*coreos.*", dInfo.KernelVersion)
	if matched {
		hdrs = "/lib/modules"
	}

	hostConfig := dockerclient.HostConfig{
		RestartPolicy: dockerclient.RestartPolicy{
			Name:              "unless-stopped",
			MaximumRetryCount: 0,
		},
		NetworkMode: "host",
		Privileged:  true,
		IpcMode:     "host",
		Binds: []string{
			"/run/docker/plugins:/run/docker/plugins",
			"/var/lib/osd:/var/lib/osd:shared",
			"/dev:/dev",
			"/etc/pwx:/etc/pwx",
			"/opt/pwx/bin:/export_bin:shared",
			"/var/run/docker.sock:/var/run/docker.sock",
			"/var/cores:/var/cores",
			hdrs + ":" + hdrs,
		},
	}

	config := dockerclient.Config{
		Image: pxImage + ":" + pxImageTag,
		Cmd:   args,
	}

	co := dockerclient.CreateContainerOptions{
		Name:       pxContainerName,
		Config:     &config,
		HostConfig: &hostConfig}
	con, err := docker.CreateContainer(co)
	if err != nil {
		fmt.Println("Warning, could not create the Portworx container: ",
			err.Error())
		return err
	}

	err = docker.StartContainer(con.ID, &hostConfig)
	if err != nil {
		fmt.Println("Warning, could not start the Portworx container: ",
			err.Error())
		return err
	}

	fmt.Println("Install Done.  Portworx monitor running.")
	return nil
}

func main() {
	handlersMap := make(map[os.Signal]func())
	handlersMap[syscall.SIGTERM] = handlerSigTerm // docker stop
	handlersMap[syscall.SIGKILL] = handlerSigKill // docker rm -f
	SignalHandlers(handlersMap)

	enableSharedMounts()

	err := install(os.Args[1:])
	if err != nil {
		fmt.Println("Failed to start px container. Err: ", err)
		return
	}

	select {}
}
