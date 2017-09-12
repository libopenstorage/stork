package ssh

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/task"
	ssh_pkg "golang.org/x/crypto/ssh"
)

const (
	// DriverName is the name of the ssh driver
	DriverName = "ssh"
	// DefaultUsername is the default username used for ssh operations
	DefaultUsername = "torpedo"
	// DefaultPassword is the default username used for ssh operations
	DefaultPassword = "t0rped0"
	// DefaultSSHPort is the default port used for ssh operations
	DefaultSSHPort = 22
)

type ssh struct {
	node.Driver
	username    string
	password    string
	schedDriver scheduler.Driver
	sshConfig   *ssh_pkg.ClientConfig
	// TODO key-based ssh
}

func (s *ssh) String() string {
	return DriverName
}

func (s *ssh) Init(sched string) error {
	var err error

	s.sshConfig = &ssh_pkg.ClientConfig{
		User: s.username,
		Auth: []ssh_pkg.AuthMethod{
			ssh_pkg.Password(s.password),
		},
	}

	s.schedDriver, err = scheduler.Get(sched)
	if err != nil {
		return err
	}

	nodes := s.schedDriver.GetNodes()
	for _, n := range nodes {
		if err := s.TestConnection(n, node.TestConectionOpts{
			Timeout:         1 * time.Minute,
			TimeBeforeRetry: 10 * time.Second,
		}); err != nil {
			return &ErrFailedToTestConnection{
				Node:  n,
				Cause: fmt.Sprintf("failed to test connection due to: %v", err),
			}
		}
	}

	return nil
}

func (s *ssh) TestConnection(n node.Node, options node.TestConectionOpts) error {
	addr, err := s.getAddrToConnect(n)
	if err != nil {
		return &ErrFailedToTestConnection{
			Node:  n,
			Cause: fmt.Sprintf("failed to get node address due to: %v", err),
		}
	}

	t := func() error {
		return s.doCmd(addr, "hostname", false)
	}

	if err := task.DoRetryWithTimeout(t, options.Timeout, options.TimeBeforeRetry); err != nil {
		return &ErrFailedToTestConnection{
			Node:  n,
			Cause: err.Error(),
		}
	}

	return nil
}

func (s *ssh) RebootNode(n node.Node, options node.RebootNodeOpts) error {
	addr, err := s.getAddrToConnect(n)
	if err != nil {
		return &ErrFailedToRebootNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to get node address due to: %v", err),
		}
	}

	rebootCmd := "sudo reboot"
	if options.Force {
		rebootCmd = rebootCmd + " -f"
	}

	t := func() error {
		return s.doCmd(addr, rebootCmd, true)
	}

	if err := task.DoRetryWithTimeout(t, 1*time.Minute, 10*time.Second); err != nil {
		return &ErrFailedToRebootNode{
			Node:  n,
			Cause: err.Error(),
		}
	}

	return nil
}

func (s *ssh) ShutdownNode(n node.Node, options node.ShutdownNodeOpts) error {
	addr, err := s.getAddrToConnect(n)
	if err != nil {
		return &ErrFailedToShutdownNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to get node address due to: %v", err),
		}
	}

	shutdownCmd := "sudo shutdown"
	if options.Force {
		shutdownCmd = "halt"
	}

	t := func() error {
		return s.doCmd(addr, shutdownCmd, true)
	}

	if err := task.DoRetryWithTimeout(t, 1*time.Minute, 10*time.Second); err != nil {
		return &ErrFailedToShutdownNode{
			Node:  n,
			Cause: err.Error(),
		}
	}

	return nil
}

func (s *ssh) doCmd(addr string, cmd string, ignoreErr bool) error {
	connection, err := ssh_pkg.Dial("tcp", fmt.Sprintf("%v:%d", addr, DefaultSSHPort), s.sshConfig)
	if err != nil {
		return &ErrFailedToRunCommand{
			Addr:  addr,
			Cause: fmt.Sprintf("failed to dial: %v", err),
		}
	}

	session, err := connection.NewSession()
	if err != nil {
		return &ErrFailedToRunCommand{
			Addr:  addr,
			Cause: fmt.Sprintf("failed to create session: %s", err),
		}
	}

	defer session.Close()

	modes := ssh_pkg.TerminalModes{
		ssh_pkg.ECHO:          0,     // disable echoing
		ssh_pkg.TTY_OP_ISPEED: 14400, // input speed = 14.4kbaud
		ssh_pkg.TTY_OP_OSPEED: 14400, // output speed = 14.4kbaud
	}

	if err := session.RequestPty("xterm", 80, 40, modes); err != nil {
		return &ErrFailedToRunCommand{
			Addr:  addr,
			Cause: fmt.Sprintf("request for pseudo terminal failed: %s", err),
		}
	}

	stdout, err := session.StdoutPipe()
	if err != nil {
		return &ErrFailedToRunCommand{
			Addr:  addr,
			Cause: fmt.Sprintf("Unable to setup stdout for session: %v", err),
		}
	}

	chOut := make(chan string)
	go func() {
		var bufout bytes.Buffer
		io.Copy(&bufout, stdout)
		chOut <- bufout.String()
	}()

	stderr, err := session.StderrPipe()
	if err != nil {
		return &ErrFailedToRunCommand{
			Addr:  addr,
			Cause: fmt.Sprintf("Unable to setup stderr for session: %v", err),
		}
	}

	chErr := make(chan string)
	go func() {
		var buferr bytes.Buffer
		io.Copy(&buferr, stderr)
		chErr <- buferr.String()
	}()

	if err = session.Run(cmd); !ignoreErr && err != nil {
		return &ErrFailedToRunCommand{
			Addr:  addr,
			Cause: fmt.Sprintf("failed to run command due to: %v", err),
		}
	}

	return nil
}

func (s *ssh) getAddrToConnect(n node.Node) (string, error) {
	if n.Addresses == nil || len(n.Addresses) == 0 {
		return "", fmt.Errorf("no address available to connect")
	}

	addr := n.Addresses[0] // TODO don't stick to first address
	return addr, nil
}

func init() {
	s := &ssh{
		Driver:   node.NotSupportedDriver,
		username: DefaultUsername,
		password: DefaultPassword,
	}

	node.Register(DriverName, s)
}
