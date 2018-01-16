package ssh

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	ssh_pkg "golang.org/x/crypto/ssh"
)

const (
	// DriverName is the name of the ssh driver
	DriverName = "ssh"
	// DefaultUsername is the default username used for ssh operations
	DefaultUsername = "root"
	// DefaultSSHPort is the default port used for ssh operations
	DefaultSSHPort = 22
	// DefaultSSHKey is the default public keyPath path used for ssh operations
	DefaultSSHKey = "/home/torpedo/key4torpedo.pem"
)

type ssh struct {
	node.Driver
	username  string
	password  string
	keyPath   string
	sshConfig *ssh_pkg.ClientConfig
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

func (s *ssh) Init() error {
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

	password := os.Getenv("TORPEDO_SSH_PASSWORD")
	if len(password) != 0 {
		s.password = password
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

	nodes := node.GetWorkerNodes()
	for _, n := range nodes {
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

	t := func() (interface{}, bool, error) {
		out, err := s.doCmd(addr, rebootCmd, true)
		return out, true, err
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

	t := func() (interface{}, bool, error) {
		out, err := s.doCmd(addr, shutdownCmd, true)
		return out, true, err
	}

	if _, err := task.DoRetryWithTimeout(t, 1*time.Minute, 10*time.Second); err != nil {
		return &node.ErrFailedToShutdownNode{
			Node:  n,
			Cause: err.Error(),
		}
	}

	return nil
}

func (s *ssh) YankDrive(n node.Node, driveNameToFail string, options node.ConnectionOpts) (string, error) {
	// Currently only works for iSCSI drives
	// TODO: Make it generic (Add support dev mapper devices)
	addr, err := s.getAddrToConnect(n, options)
	if err != nil {
		return "", &node.ErrFailedToYankDrive{
			Node:  n,
			Cause: fmt.Sprintf("failed to get node address due to: %v", err),
		}
	}

	// Get the HBA number for the drive which would be then used to recover the drive
	hbaCmd := "lsscsi | grep -n " + driveNameToFail + "| awk -F\":\" '{print $2}'" + "| awk -F\"[\" '{print $2}'"
	driveID, err := s.doCmd(addr, hbaCmd, false)
	if err != nil {
		return "", &node.ErrFailedToYankDrive{
			Node:  n,
			Cause: fmt.Sprintf("unable to find HBA attribute of the drive %v due to: %v", driveNameToFail, err),
		}
	}

	driveID = strings.TrimRight(driveID, "\n")
	driveNameToFail = strings.Trim(driveNameToFail, "/")
	devices := strings.Split(driveNameToFail, "/")

	// Disable the block device, so that it returns IO errors
	yankCommand := "echo 1 > /sys/block/" + devices[len(devices)-1] + "/device/delete"

	_, err = s.doCmd(addr, yankCommand, false)
	if err != nil {
		return "", &node.ErrFailedToYankDrive{
			Node:  n,
			Cause: fmt.Sprintf("failed to yank drive %v due to: %v", driveNameToFail, err),
		}
	}
	return driveID, nil
}

func (s *ssh) RecoverDrive(n node.Node, driveNameToRecover string, driveUUIDToRecover string, options node.ConnectionOpts) error {
	addr, err := s.getAddrToConnect(n, options)
	if err != nil {
		return &node.ErrFailedToRecoverDrive{
			Node:  n,
			Cause: fmt.Sprintf("failed to get node address due to: %v", err),
		}
	}

	// Enable the drive by rescaning
	recoverCmd := "echo \" - - -\" > /sys/class/scsi_host/host" + driveUUIDToRecover + "/scan"
	_, err = s.doCmd(addr, recoverCmd, false)
	if err != nil {
		return &node.ErrFailedToRecoverDrive{
			Node:  n,
			Cause: fmt.Sprintf("Unable to rescan the drive (%v): %v", driveNameToRecover, err),
		}
	}
	return nil
}

func (s *ssh) RunCommand(n node.Node, command string, options node.ConnectionOpts) (string, error) {
	addr, err := s.getAddrToConnect(n, options)
	if err != nil {
		return "", &node.ErrFailedToRunCommand{
			Addr:  n.Name,
			Cause: fmt.Sprintf("failed to get node address due to: %v", err),
		}
	}

	output, err := s.doCmd(addr, command, false)
	if err != nil {
		return "", &node.ErrFailedToRunCommand{
			Addr:  n.Name,
			Cause: fmt.Sprintf("unable to run cmd (%v): %v", command, err),
		}
	}

	return output, nil
}

func (s *ssh) FindFiles(path string, n node.Node, options node.FindOpts) (string, error) {
	addr, err := s.getAddrToConnect(n, options.ConnectionOpts)
	if err != nil {
		return "", &node.ErrFailedToFindFileOnNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to get node address due to: %v", err),
		}
	}

	findCmd := "sudo find " + path
	if options.Name != "" {
		findCmd += " -name " + options.Name
	}
	if options.MinDepth > 0 {
		findCmd += " -mindepth " + strconv.Itoa(options.MinDepth)
	}
	if options.MaxDepth > 0 {
		findCmd += " -maxdepth " + strconv.Itoa(options.MaxDepth)
	}

	t := func() (interface{}, bool, error) {
		out, err := s.doCmd(addr, findCmd, true)
		return out, true, err
	}

	out, err := task.DoRetryWithTimeout(t,
		options.ConnectionOpts.Timeout,
		options.ConnectionOpts.TimeBeforeRetry)

	if err != nil {
		return "", &node.ErrFailedToFindFileOnNode{
			Node:  n,
			Cause: err.Error(),
		}
	}
	return out.(string), nil
}

func (s *ssh) Systemctl(n node.Node, service string, options node.SystemctlOpts) error {
	addr, err := s.getAddrToConnect(n, options.ConnectionOpts)
	if err != nil {
		return &node.ErrFailedToRunSystemctlOnNode{
			Node:  n,
			Cause: fmt.Sprintf("failed to get node address due to: %v", err),
		}
	}

	systemctlCmd := fmt.Sprintf("sudo systemctl %v %v", options.Action, service)
	t := func() (interface{}, bool, error) {
		out, err := s.doCmd(addr, systemctlCmd, false)
		return out, true, err
	}

	if _, err := task.DoRetryWithTimeout(t,
		options.ConnectionOpts.Timeout,
		options.ConnectionOpts.TimeBeforeRetry); err != nil {
		return &node.ErrFailedToRunSystemctlOnNode{
			Node:  n,
			Cause: err.Error(),
		}
	}
	return nil
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

	byteout, err := session.Output(cmd)
	out = string(byteout)
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
		t := func() (interface{}, bool, error) {
			out, err := s.doCmd(addr, "hostname", false)
			return out, true, err
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
		keyPath:  DefaultSSHKey,
	}

	node.Register(DriverName, s)
}
