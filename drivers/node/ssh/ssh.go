package ssh

import (
	"fmt"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	k8s_driver "github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	volumedriver "github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"github.com/portworx/torpedo/pkg/log"
	ssh_pkg "golang.org/x/crypto/ssh"
	"io/ioutil"
	appsv1_api "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
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

const (
	defaultTimeout        = 5 * time.Minute
	execPodDaemonSetLabel = "debug"
)

// SSH ssh node driver
type SSH struct {
	node.Driver
	username         string
	password         string
	keyPath          string
	sshConfig        *ssh_pkg.ClientConfig
	specDir          string
	execPodNamespace string
	// TODO keyPath-based ssh
}

var (
	k8sApps = apps.Instance()
	k8sCore = core.Instance()
)

func (s *SSH) String() string {
	return DriverName
}

// returns ssh.Signer from user you running app home path + cutted keyPath path.
// (ex. pubkey,err := getKeyFile("/.ssh/id_rsa") )
func getKeyFile(keypath string) (ssh_pkg.Signer, error) {
	file := keypath
	buf, err := ioutil.ReadFile(file)
	if err != nil {
		log.Errorf("failed to read ssh key file. Cause: %s", err.Error())
		return nil, err
	}

	pubkey, err := ssh_pkg.ParsePrivateKey(buf)
	if err != nil {
		log.Errorf("failed to parse private key. Cause: %s", err.Error())
		return nil, err
	}

	return pubkey, nil
}

// IsUsingSSH returns true if the command will be run using ssh
func (s *SSH) IsUsingSSH() bool {
	return len(os.Getenv("TORPEDO_SSH_KEY")) > 0 || len(os.Getenv("TORPEDO_SSH_PASSWORD")) > 0
}

// IsNodeRebootedInGivenTimeRange return true if node rebooted in given time range
func (s *SSH) IsNodeRebootedInGivenTimeRange(n node.Node, timerange time.Duration) (bool, error) {
	log.Infof("Checking the uptime for a node %s", n.SchedulerNodeName)
	uptimeCmd := "sudo uptime -s"

	t := func() (interface{}, bool, error) {
		out, err := s.doCmd(n, node.ConnectionOpts{
			Timeout:         1 * time.Minute,
			TimeBeforeRetry: 10 * time.Second,
		}, uptimeCmd, true)
		return out, true, err
	}

	out, err := task.DoRetryWithTimeout(t, 1*time.Minute, 10*time.Second)
	if err != nil {
		return false, &node.ErrFailedToRunCommand{
			Node:  n,
			Cause: fmt.Sprintf("Failed to run uptime command in node %v", n),
		}
	}

	upTime := strings.Fields(strings.TrimSpace(out.(string)))

	// Converting the unix date to timestamp
	thetime, err := time.Parse(time.RFC3339, upTime[0]+"T"+upTime[1]+"+00:00")
	if err != nil {
		return false, fmt.Errorf("Unable to parse uptime command output. Err: %s", err)
	}

	uptimeEpoch := thetime.Unix()
	curEpoch := time.Now().Unix()
	diff := curEpoch - uptimeEpoch
	tRangeInSeconds := int64(timerange / time.Second)
	if diff > tRangeInSeconds {
		return false, nil
	}
	return true, nil
}

// GetDeviceMapperCount return device mapper count in a node
func (s *SSH) GetDeviceMapperCount(n node.Node, timerange time.Duration) (int, error) {
	log.Infof("Getting the current devicemapper devices counts in a node %s", n.SchedulerNodeName)
	devMappCmd := "sudo multipath -ll 2>&1|grep dm-|wc -l"

	t := func() (interface{}, bool, error) {
		out, err := s.doCmd(n, node.ConnectionOpts{
			Timeout:         1 * time.Minute,
			TimeBeforeRetry: 10 * time.Second,
		}, devMappCmd, true)
		return out, true, err
	}

	out, err := task.DoRetryWithTimeout(t, 1*time.Minute, 10*time.Second)
	if err != nil {
		return -1, &node.ErrFailedToRunCommand{
			Node:  n,
			Cause: fmt.Sprintf("Failed to run multipath command in node %v", n),
		}
	}

	count, err := strconv.Atoi(strings.Fields(strings.TrimSpace(out.(string)))[0])
	if err != nil {
		return -1, err
	}
	log.Infof("Currently [%v] device mapped to a node: [%v]", count, n.Name)
	return count, nil
}

// Init initializes SSH node driver
func (s *SSH) Init(nodeOpts node.InitOptions) error {
	var err error
	s.specDir = nodeOpts.SpecDir

	execPodNamespace, err := getExecPodNamespace()
	if err != nil {
		return err
	}
	s.execPodNamespace = execPodNamespace

	nodes := node.GetWorkerNodes()
	if s.IsUsingSSH() {
		err = s.initSSH()
	} else {
		err = s.initExecPod()
	}

	if err != nil {
		return err
	}

	for _, n := range nodes {
		if !n.IsStorageDriverInstalled {
			continue
		}
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

func (s *SSH) initExecPod() error {
	var ds *appsv1_api.DaemonSet
	var err error

	if ds, err = k8sApps.GetDaemonSet(execPodDaemonSetLabel, s.execPodNamespace); ds == nil {
		d, err := scheduler.Get(k8s_driver.SchedName)
		specFactory, err := spec.NewFactory(fmt.Sprintf("%s/%s", s.specDir, execPodDaemonSetLabel), volumedriver.GetStorageProvisioner(), d)
		if err != nil {
			return fmt.Errorf("Error while loading debug daemonset spec file. Err: %s", err)
		}
		dsSpec, err := specFactory.Get(execPodDaemonSetLabel)
		if err != nil {
			return fmt.Errorf("Error while getting debug daemonset spec. Err: %s", err)
		}
		dsSpec.SpecList[0].(*appsv1_api.DaemonSet).Namespace = s.execPodNamespace
		ds, err = k8sApps.CreateDaemonSet(dsSpec.SpecList[0].(*appsv1_api.DaemonSet), metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("Error while creating debug daemonset. Err: %s", err)
		}
	}
	err = k8sApps.ValidateDaemonSet(ds.Name, ds.Namespace, defaultTimeout)
	if err != nil {
		return fmt.Errorf("Error while validating debug daemonset. Err: %s", err)
	}
	return nil
}

func (s *SSH) initSSH() error {
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
			Timeout:         time.Second * 5,
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
			Timeout:         time.Second * 5,
		}

	} else {
		return fmt.Errorf("Unknown auth type")
	}

	return nil
}

// TestConnection tests the connection to the given node
func (s *SSH) TestConnection(n node.Node, options node.ConnectionOpts) error {
	var err error
	var cmd string

	if s.IsUsingSSH() {
		cmd = "hostname"
	} else {
		cmd = "date"
	}

	if _, err = s.doCmd(n, options, cmd, false); err != nil {
		return &node.ErrFailedToTestConnection{
			Node:  n,
			Cause: err.Error(),
		}
	}

	return nil
}

// RebootNode reboots given node
func (s *SSH) RebootNode(n node.Node, options node.RebootNodeOpts) error {
	log.Infof("Rebooting node %s", n.SchedulerNodeName)
	rebootCmd := "sudo reboot"
	if options.Force {
		rebootCmd = rebootCmd + " -f"
	}

	t := func() (interface{}, bool, error) {
		out, err := s.doCmd(n, options.ConnectionOpts, rebootCmd, true)
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

// InjectNetworkError by dropping packets or introdiucing delay in packet tramission
// nodes=> list of nodes where network injection should be done.
// errorInjectionType => pass "delay" or "drop"
// operationType => add/change/delete
// dropPercentage => intger value from 1 to 100
// delayInMilliseconds => 1 to 1000
func (s *SSH) InjectNetworkError(nodes []node.Node, errorInjectionType string, operationType string, dropPercentage int, delayInMilliseconds int) error {
	//tc qdisc add dev eth0 root netem loss 20%
	//tc qdisc change dev eth0 root netem delay 5000ms 5000ms
	var cmd string
	dropInPercentage := strconv.Itoa(dropPercentage) + "%"
	delayInMillisescond := strconv.Itoa(delayInMilliseconds) + "ms"
	if errorInjectionType == "delay" {
		cmd = fmt.Sprintf("%s %s  %s %s %s", "sudo tc qdisc", operationType, "dev eth0 root netem delay ",
			delayInMillisescond, delayInMillisescond)
		log.Infof("Delay %v ", delayInMillisescond)
	} else if errorInjectionType == "drop" {
		cmd = fmt.Sprintf("%s %s  %s %s", "sudo tc qdisc", operationType, "dev eth0 root netem loss",
			dropInPercentage)
		log.Infof("DropPercentage %v ", dropInPercentage)
	} else {
		return fmt.Errorf("Invalid network error injection type %v", errorInjectionType)
	}
	log.Infof("Error injection type %v ", errorInjectionType)
	log.Infof("Operation type %v ", operationType)
	connectionOps := node.ConnectionOpts{
		Timeout:         10 * time.Second,
		TimeBeforeRetry: 10 * time.Second,
	}
	for _, n := range nodes {
		log.Infof("Error injection on Node name : %s of type : %s ", n.Name, errorInjectionType)
		t := func() (interface{}, bool, error) {
			out, err := s.doCmd(n, connectionOps, cmd, true)
			return out, true, err
		}

		if _, err := task.DoRetryWithTimeout(t, 10*time.Second, 10*time.Second); err != nil {
			return &node.ErrFailedToSetNetworkErrorOnNode{
				Node:  n,
				Cause: err.Error(),
			}
		}
	}
	return nil
}

// CrashNode crashes given node
func (s *SSH) CrashNode(n node.Node, options node.CrashNodeOpts) error {
	log.Infof("Crashing node %s", n.SchedulerNodeName)
	crashCmd := "echo c > /proc/sysrq-trigger"

	t := func() (interface{}, bool, error) {
		out, err := s.doCmd(n, options.ConnectionOpts, crashCmd, true)
		return out, true, err
	}

	if _, err := task.DoRetryWithTimeout(t, 1*time.Minute, 10*time.Second); err != nil {
		return &node.ErrFailedToCrashNode{
			Node:  n,
			Cause: err.Error(),
		}
	}

	return nil
}

// ShutdownNode shuts down given node
func (s *SSH) ShutdownNode(n node.Node, options node.ShutdownNodeOpts) error {
	shutdownCmd := "sudo shutdown"
	if options.Force {
		shutdownCmd = "halt"
	}

	t := func() (interface{}, bool, error) {
		out, err := s.doCmd(n, options.ConnectionOpts, shutdownCmd, true)
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

// YankDrive yanks given drive on given node
func (s *SSH) YankDrive(n node.Node, driveNameToFail string, options node.ConnectionOpts) (string, error) {
	// Currently only works for iSCSI drives
	// TODO: Make it generic (Add support dev mapper devices)

	//Get the scsi bus ID
	busIDCmd := "lsscsi | grep " + driveNameToFail + " | awk -F\":\" '{print $1}'" + "| awk -F\"[\" '{print $2}'"
	busID, err := s.doCmd(n, options, busIDCmd, false)
	if err != nil {
		return "", &node.ErrFailedToYankDrive{
			Node:  n,
			Cause: fmt.Sprintf("unable to find host bus attribute of the drive %v due to: %v", driveNameToFail, err),
		}
	}

	driveNameToFail = strings.Trim(driveNameToFail, "/")
	devices := strings.Split(driveNameToFail, "/")
	bus := strings.TrimRight(busID, "\n")

	// Disable the block device, so that it returns IO errors
	yankCommand := "echo 1 > /sys/block/" + devices[len(devices)-1] + "/device/delete"
	if _, err = s.doCmd(n, options, yankCommand, false); err != nil {
		return "", &node.ErrFailedToYankDrive{
			Node:  n,
			Cause: fmt.Sprintf("failed to yank drive %v due to: %v", driveNameToFail, err),
		}
	}
	return bus, nil
}

// RecoverDrive recovers given drive on given node
func (s *SSH) RecoverDrive(n node.Node, driveNameToRecover string, driveUUIDToRecover string, options node.ConnectionOpts) error {
	// Enable the drive by rescaning
	recoverCmd := "echo \" - - -\" > /sys/class/scsi_host/host" + driveUUIDToRecover + "\"/\"scan"
	if _, err := s.doCmd(n, options, recoverCmd, false); err != nil {
		return &node.ErrFailedToRecoverDrive{
			Node:  n,
			Cause: fmt.Sprintf("Unable to rescan the drive (%v): %v", driveNameToRecover, err),
		}
	}
	return nil
}

// RunCommand runs given command on given node
func (s *SSH) RunCommand(n node.Node, command string, options node.ConnectionOpts) (string, error) {
	t := func() (interface{}, bool, error) {
		output, err := s.doCmd(n, options, command, options.IgnoreError)
		if err != nil {
			return "", true, &node.ErrFailedToRunCommand{
				Addr:  n.Name,
				Cause: fmt.Sprintf("unable to run cmd (%v): %v", command, err),
			}
		}
		return output, false, nil
	}

	output, err := task.DoRetryWithTimeout(t, options.Timeout, options.TimeBeforeRetry)
	if err != nil {
		return "", err
	}

	return output.(string), nil
}

// RunCommandWithNoRetry runs given command on given node but with no retries
func (s *SSH) RunCommandWithNoRetry(n node.Node, command string, options node.ConnectionOpts) (string, error) {
	if s.IsUsingSSH() {
		return s.doCmdSSH(n, options, command, options.IgnoreError)
	}
	return s.doCmdUsingPodWithoutRetry(n, command)
}

// FindFiles finds files from give path on given node
func (s *SSH) FindFiles(path string, n node.Node, options node.FindOpts) (string, error) {
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
	if options.Type != "" {
		findCmd += " -type " + string(options.Type)
	}
	if options.Empty {
		findCmd += " -empty"
	}

	t := func() (interface{}, bool, error) {
		out, err := s.doCmd(n, options.ConnectionOpts, findCmd, true)
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

// Systemctl allows to run systemctl commands on a give node
func (s *SSH) Systemctl(n node.Node, service string, options node.SystemctlOpts) error {
	systemctlCmd := fmt.Sprintf("sudo systemctl %v %v", options.Action, service)
	t := func() (interface{}, bool, error) {
		out, err := s.doCmd(n, options.ConnectionOpts, systemctlCmd, false)
		if err != nil {
			return out, true, err
		}
		return out, false, nil
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

// SystemctlUnitExist checks if a given service exists on the node
func (s *SSH) SystemctlUnitExist(n node.Node, service string, options node.SystemctlOpts) (bool, error) {
	systemctlCmd := fmt.Sprintf("sudo systemctl list-units --full --all | grep \"%s.service\" || true", service)
	t := func() (interface{}, bool, error) {
		out, err := s.doCmd(n, options.ConnectionOpts, systemctlCmd, false)
		if err != nil {
			return out, true, err
		}
		return out, false, nil
	}
	out, err := task.DoRetryWithTimeout(t, options.ConnectionOpts.Timeout, options.ConnectionOpts.TimeBeforeRetry)
	if err != nil {
		return false, &node.ErrFailedToRunSystemctlOnNode{
			Node:  n,
			Cause: err.Error(),
		}
	}

	return len(out.(string)) > 0, nil
}

func (s *SSH) doCmd(n node.Node, options node.ConnectionOpts, cmd string, ignoreErr bool) (string, error) {

	if s.IsUsingSSH() {
		return s.doCmdSSH(n, options, cmd, ignoreErr)
	}
	return s.doCmdUsingPod(n, options, cmd, ignoreErr)
}

func (s *SSH) doCmdUsingPodWithoutRetry(n node.Node, cmd string) (string, error) {
	cmds := []string{"nsenter", "--mount=/hostproc/1/ns/mnt", "/bin/bash", "-c", cmd}
	allPodsForNode, err := k8sCore.GetPodsByNode(n.Name, s.execPodNamespace)
	if err != nil {
		log.Errorf("failed to get pods in node: %s err: %v", n.Name, err)
		return "", err
	}
	var debugPod *v1.Pod
	for _, pod := range allPodsForNode.Items {
		if pod.Labels["name"] == execPodDaemonSetLabel && k8sCore.IsPodReady(pod) {
			debugPod = &pod
			break
		}
	}

	if debugPod == nil {
		return "", &node.ErrFailedToRunCommand{
			Node:  n,
			Cause: fmt.Sprintf("debug pod not found in node %v", n),
		}
	}
	return k8sCore.RunCommandInPod(cmds, debugPod.Name, "", debugPod.Namespace)
}

func (s *SSH) doCmdUsingPod(n node.Node, options node.ConnectionOpts, cmd string, ignoreErr bool) (string, error) {
	var debugPod *v1.Pod
	t := func() (interface{}, bool, error) {
		if debugPod == nil {
			log.Debugf("Finding the debug pod to run command on node %s", n.Name)
			allPodsForNode, err := k8sCore.GetPodsByNode(n.Name, s.execPodNamespace)
			if err != nil {
				log.Errorf("failed to get pods in node: %s err: %v", n.Name, err)
				return nil, true, err
			}
			for _, pod := range allPodsForNode.Items {
				if pod.Labels["name"] == execPodDaemonSetLabel && k8sCore.IsPodReady(pod) {
					debugPod = &pod
					break
				}
			}
		}
		if debugPod == nil {
			return nil, true, &node.ErrFailedToRunCommand{
				Node:  n,
				Cause: fmt.Sprintf("debug pod not found in node %v", n),
			}
		}
		cmds := []string{"nsenter", "--mount=/hostproc/1/ns/mnt", "/bin/bash", "-c", cmd}
		log.Debugf("Running command on pod %s [%s]", debugPod.Name, cmds)
		output, err := k8sCore.RunCommandInPod(cmds, debugPod.Name, "", debugPod.Namespace)
		if !ignoreErr && err != nil {
			return nil, true, &node.ErrFailedToRunCommand{
				Node: n,
				Cause: fmt.Sprintf("failed to run command in pod. command: %v , err: %v, pod: %v",
					cmds, err, debugPod),
			}
		}
		return output, false, nil
	}

	output, err := task.DoRetryWithTimeout(t, options.Timeout, options.TimeBeforeRetry)
	if err != nil {
		return "", err
	}
	return output.(string), nil
}

func (s *SSH) doCmdSSH(n node.Node, options node.ConnectionOpts, cmd string, ignoreErr bool) (string, error) {
	var out string
	var sterr string
	connection, err := s.getConnection(n, options)
	if err != nil {
		return "", &node.ErrFailedToRunCommand{
			Addr:  n.UsableAddr,
			Cause: fmt.Sprintf("failed to dial: %v", err),
		}
	}

	session, err := connection.NewSession()
	if err != nil {
		return "", &node.ErrFailedToRunCommand{
			Addr:  n.UsableAddr,
			Cause: fmt.Sprintf("failed to create session: %s", err),
		}
	}
	defer session.Close()

	stderr, err := session.StderrPipe()
	if err != nil {
		return "", fmt.Errorf("fail to setup stderr")
	}

	stdout, err := session.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("fail to setup stdout")
	}
	if options.Sudo {
		cmd = fmt.Sprintf("sudo su -c '%s' -", cmd) // Hyphen necessary to preserve PATH for commands like "which pxctl"
	}
	session.Start(cmd)
	err = session.Wait()
	if resp, err1 := ioutil.ReadAll(stdout); err1 == nil {
		out = string(resp)
	} else {
		return "", fmt.Errorf("fail to read stdout")
	}
	if resp, err1 := ioutil.ReadAll(stderr); err1 == nil {
		sterr = string(resp)
	} else {
		return "", fmt.Errorf("fail to read stderr")
	}

	if ignoreErr == false && err != nil {
		return out, &node.ErrFailedToRunCommand{
			Addr:  n.UsableAddr,
			Cause: fmt.Sprintf("failed to run command due to: %v", sterr),
		}
	}
	return out, nil
}

func (s *SSH) getConnection(n node.Node, options node.ConnectionOpts) (*ssh_pkg.Client, error) {
	if n.Addresses == nil || len(n.Addresses) == 0 {
		return nil, fmt.Errorf("no address available to connect")
	}

	addr, err := s.getConnectionOnUsableAddr(n, options)
	return addr, err
}

func (s *SSH) getConnectionOnUsableAddr(n node.Node, options node.ConnectionOpts) (*ssh_pkg.Client, error) {
	var sshErr error
	var cli interface{}
	for _, addr := range n.Addresses {
		t := func() (interface{}, bool, error) {
			// check if address is responding on port 22
			endpoint := net.JoinHostPort(addr, strconv.Itoa(int(DefaultSSHPort)))
			conn, err := ssh_pkg.Dial("tcp", endpoint, s.sshConfig)
			return conn, true, err
		}
		if cli, sshErr = task.DoRetryWithTimeout(t, options.Timeout, options.TimeBeforeRetry); sshErr == nil {
			n.UsableAddr = addr
			return cli.(*ssh_pkg.Client), nil
		}
	}
	return nil, fmt.Errorf("no usable address found. Tried: %v. Error: %v"+
		"Ensure you have setup the nodes for ssh access as per the README", n.Addresses, sshErr)
}

// SystemCheck check if any cores are generated on given node
func (s *SSH) SystemCheck(n node.Node, options node.ConnectionOpts) (string, error) {
	findOpts := node.FindOpts{
		ConnectionOpts: options,
		Name:           "core-px*",
		Type:           node.File,
	}
	file, err := s.FindFiles("/var/cores/", n, findOpts)
	if err != nil {
		return "", &node.ErrFailedToSystemCheck{
			Node:  n,
			Cause: fmt.Sprintf("failed to check for core files due to: %v", err),
		}
	}
	return file, nil
}

// GetBlockDrives returns the block drives on the node
func (s *SSH) GetBlockDrives(n node.Node, options node.SystemctlOpts) (map[string]*node.BlockDrive, error) {
	drives := make(map[string]*node.BlockDrive)
	driveCmd := fmt.Sprintf("sudo /bin/lsblk -f -P -s -d -p -o NAME,LABEL,SIZE,MOUNTPOINT,FSTYPE,TYPE")
	t := func() (interface{}, bool, error) {
		out, err := s.doCmd(n, options.ConnectionOpts, driveCmd, false)
		if err != nil {
			return out, true, err
		}
		return out, false, nil
	}
	out, err := task.DoRetryWithTimeout(t, options.ConnectionOpts.Timeout, options.ConnectionOpts.TimeBeforeRetry)
	if err != nil {
		return drives, &node.ErrFailedToRunCommand{
			Node:  n,
			Cause: err.Error(),
		}
	}
	driveOutput := fmt.Sprintf("%s", out)

	for _, line := range strings.Split(strings.TrimSpace(driveOutput), "\n") {
		drive := &node.BlockDrive{}
		columns := strings.Split(line, " ")
		for _, col := range columns {
			if ok, _ := regexp.MatchString("^NAME", col); ok {
				lastBin := strings.LastIndex(col, "=")
				drive.Path = col[lastBin+2 : len(col)-1]
			}
			if ok, _ := regexp.MatchString("^LABEL", col); ok {
				lastBin := strings.Index(col, "=")
				labels := col[lastBin+2 : len(col)-1]
				labelsList := strings.Split(labels, ",")
				driveLabels := make(map[string]string)
				for _, label := range labelsList {
					labelBin := strings.LastIndex(label, "=")
					if labelBin != -1 {
						kv := strings.Split(label, "=")
						driveLabels[kv[0]] = kv[1]
					} else {
						driveLabels[label] = ""
					}
				}
				drive.Labels = driveLabels
			}
			if ok, _ := regexp.MatchString("^MOUNTPOINT", col); ok {
				lastBin := strings.LastIndex(col, "=")
				drive.MountPoint = col[lastBin+2 : len(col)-1]
			}
			if ok, _ := regexp.MatchString("^SIZE", col); ok {
				lastBin := strings.LastIndex(col, "=")
				drive.Size = col[lastBin+2 : len(col)-1]
			}
			if ok, _ := regexp.MatchString("^FSTYPE", col); ok {
				lastBin := strings.LastIndex(col, "=")
				drive.FSType = col[lastBin+2 : len(col)-1]
			}
			if ok, _ := regexp.MatchString("^TYPE", col); ok {
				lastBin := strings.LastIndex(col, "=")
				drive.Type = col[lastBin+2 : len(col)-1]
			}
		}
		drives[drive.Path] = drive
	}
	return drives, nil
}

func getExecPodNamespace() (string, error) {
	var allServices *v1.ServiceList
	var err error
	if allServices, err = k8sCore.ListServices("", metav1.ListOptions{}); err != nil {
		return "", err
	}
	for _, svc := range allServices.Items {
		if svc.Name == schedops.PXServiceName {
			return svc.Namespace, nil
		}
	}
	return "", fmt.Errorf("can't find %s Portworx service from list of services.", schedops.PXServiceName)
}

// New returns a new SSH object
func New() *SSH {
	return &SSH{
		Driver: node.NotSupportedDriver,
	}
}

func init() {
	s := New()
	s.username = DefaultUsername
	s.keyPath = DefaultSSHKey

	node.Register(DriverName, s)
}
