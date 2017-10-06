package ssh

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
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
	// DefaultSSHKey is the default public keyPath path used for ssh operations
	DefaultSSHKey = "/home/torpedo/key4torpedo.pem"
)

type ssh struct {
	node.Driver
	username    string
	password    string
	keyPath     string
	schedDriver scheduler.Driver
	sshConfig   *ssh_pkg.ClientConfig
	// TODO keyPath-based ssh
}

func (s *ssh) String() string {
	return DriverName
}

// returns ssh.Signer from user you running app home path + cutted keyPath path.
// (ex. pubkey,err := getKeyFile("/.ssh/id_rsa") )
func getKeyFile(keypath string) (ssh_pkg.Signer, error) {
	file := keypath
	buf, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	pubkey, err := ssh_pkg.ParsePrivateKey(buf)
	if err != nil {
		return nil, err
	}

	return pubkey, nil
}

func (s *ssh) Init(sched string) error {
	var err error

	keyPath := os.Getenv("TORPEDO_SSH_KEY")
	if len(keyPath) == 0 {
		s.keyPath = DefaultSSHKey
	} else {
		s.keyPath = keyPath
	}

	username := os.Getenv("TORPEDO_SSH_USER")
	if len(username) == 0 {
		s.username = DefaultUsername
	} else {
		s.username = username
	}

	if s.password != "" {
		s.sshConfig = &ssh_pkg.ClientConfig{
			User: s.username,
			Auth: []ssh_pkg.AuthMethod{
				ssh_pkg.Password(s.password),
			},
			HostKeyCallback: ssh_pkg.InsecureIgnoreHostKey(),
		}
	} else if s.keyPath != "" {
		pubkey, err := getKeyFile(s.keyPath)
		if err != nil {
			return fmt.Errorf("Error getting public keyPath from keyfile")
		}
		s.sshConfig = &ssh_pkg.ClientConfig{
			User: s.username,
			Auth: []ssh_pkg.AuthMethod{
				ssh_pkg.PublicKeys(pubkey),
			},
			HostKeyCallback: ssh_pkg.InsecureIgnoreHostKey(),
		}

	} else {
		return fmt.Errorf("Unknown auth type")
	}

	s.schedDriver, err = scheduler.Get(sched)
	if err != nil {
		return err
	}

	nodes := s.schedDriver.GetNodes()
	for _, n := range nodes {
		if n.Type == node.TypeWorker {
			if err := s.TestConnection(n, node.ConnectionOpts{
				Timeout:         1 * time.Minute,
				TimeBeforeRetry: 10 * time.Second,
			}); err != nil {
				return &node.ErrFailedToTestConnection{
					Node:  n,
					Cause: err.Error(),
				}
			}
		}
	}

	return nil
}

func (s *ssh) TestConnection(n node.Node, options node.ConnectionOpts) error {
	_, err := s.getAddrToConnect(n, options)
	if err != nil {
		return &node.ErrFailedToTestConnection{
			Node:  n,
			Cause: fmt.Sprintf("failed to get node address due to: %v", err),
		}
	}

	return nil
}

func (s *ssh) RebootNode(n node.Node, options node.RebootNodeOpts) error {
	addr, err := s.getAddrToConnect(n, options.ConnectionOpts)
	if err != nil {
		return &node.ErrFailedToRebootNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to get node address due to: %v", err),
		}
	}

	rebootCmd := "sudo reboot"
	if options.Force {
		rebootCmd = rebootCmd + " -f"
	}

	t := func() (interface{}, error) {
		return s.doCmd(addr, rebootCmd, true)
	}

	if _, err := task.DoRetryWithTimeout(t, 1*time.Minute, 10*time.Second); err != nil {
		return &node.ErrFailedToRebootNode{
			Node:  n,
			Cause: err.Error(),
		}
	}

	return nil
}

func (s *ssh) ShutdownNode(n node.Node, options node.ShutdownNodeOpts) error {
	addr, err := s.getAddrToConnect(n, options.ConnectionOpts)
	if err != nil {
		return &node.ErrFailedToShutdownNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to get node address due to: %v", err),
		}
	}

	shutdownCmd := "sudo shutdown"
	if options.Force {
		shutdownCmd = "halt"
	}

	t := func() (interface{}, error) {
		return s.doCmd(addr, shutdownCmd, true)
	}

	if _, err := task.DoRetryWithTimeout(t, 1*time.Minute, 10*time.Second); err != nil {
		return &node.ErrFailedToShutdownNode{
			Node:  n,
			Cause: err.Error(),
		}
	}

	return nil
}

func (s *ssh) CheckIfPathExists(path string, n node.Node, options node.ConnectionOpts) (bool, error) {
	addr, err := s.getAddrToConnect(n, options)
	if err != nil {
		return false, &node.ErrFailedToCheckPathOnNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to get node address due to: %v", err),
		}
	}

	readDirCmd := "sudo ls " + path

	// An error is returned if path is not present on the remote node
	if _, err := s.doCmd(addr, readDirCmd, false); err != nil {
		return false, nil
	}
	return true, nil
}

func (s *ssh) doCmd(addr string, cmd string, ignoreErr bool) (string, error) {
	var out string
	connection, err := ssh_pkg.Dial("tcp", fmt.Sprintf("%s:%d", addr, DefaultSSHPort), s.sshConfig)
	if err != nil {
		return "", &node.ErrFailedToRunCommand{
			Addr:  addr,
			Cause: fmt.Sprintf("failed to dial: %v", err),
		}
	}

	session, err := connection.NewSession()
	if err != nil {
		return "", &node.ErrFailedToRunCommand{
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
		return "", &node.ErrFailedToRunCommand{
			Addr:  addr,
			Cause: fmt.Sprintf("request for pseudo terminal failed: %s", err),
		}
	}

	stdout, err := session.StdoutPipe()
	if err != nil {
		return "", &node.ErrFailedToRunCommand{
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
		return "", &node.ErrFailedToRunCommand{
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
	byteout, err := session.CombinedOutput(cmd)
	out = fmt.Sprintf("%v", byteout)
	if ignoreErr == false && err != nil {
		return out, &node.ErrFailedToRunCommand{
			Addr:  addr,
			Cause: fmt.Sprintf("failed to run command due to: %v", err),
		}
	}
	return out, nil
}

func (s *ssh) getAddrToConnect(n node.Node, options node.ConnectionOpts) (string, error) {
	if n.Addresses == nil || len(n.Addresses) == 0 {
		return "", fmt.Errorf("no address available to connect")
	}

	addr, err := s.getOneUsableAddr(n, options)
	return addr, err
}

func (s *ssh) getOneUsableAddr(n node.Node, options node.ConnectionOpts) (string, error) {
	for _, addr := range n.Addresses {
		t := func() (interface{}, error) {
			return s.doCmd(addr, "hostname", false)
		}
		if _, err := task.DoRetryWithTimeout(t, options.Timeout, options.TimeBeforeRetry); err == nil {
			n.UsableAddr = addr
			return addr, nil
		}
	}
	return "", fmt.Errorf("no usable address found. Tried: %v. "+
		"Ensure you have setup the nodes for ssh access as per the README", n.Addresses)
}

func init() {
	s := &ssh{
		Driver:   node.NotSupportedDriver,
		username: DefaultUsername,
		password: DefaultPassword,
		keyPath:  DefaultSSHKey,
	}

	node.Register(DriverName, s)
}
