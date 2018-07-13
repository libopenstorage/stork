package cmdexecutor

import (
	"fmt"
	"strings"
	"time"

	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	// StatusFileFormat is the format specifier used to generate the status file path
	StatusFileFormat = "/tmp/stork-cmd-done-%s"
	// KillFileFormat is the format specifier used to generate the kill file path
	KillFileFormat     = "/tmp/killme-%s"
	cmdWaitFormat      = "touch %s && tail -f /dev/null;"
	cmdStatusFormat    = "stat %s"
	waitScriptLocation = "/tmp/wait.sh"
	waitCmdPlaceholder = "${WAIT_CMD}"
	// StatusConfigMapName is name of the config map the command executor uses to persist failed statuses
	StatusConfigMapName = "cmdexecutor-status"
)

const (
	cmdStatusCheckInitialDelay = 2 * time.Second
	cmdStatusCheckFactor       = 1
)

// Executor is an interace to start and wait for async commands in pods
type Executor interface {
	// Start starts the command in the pod asynchronously
	Start(chan error) error
	// Wait checks if the command started in pod completed successfully
	//	timeoutInSecs is number of seconds after which the check should timeout.
	Wait(timeoutInSecs time.Duration) error
	// GetPod returns the pod namespace and name for the executor instance
	GetPod() (string, string)
	// GetContainer returns the container inside the pod for the executor instance
	GetContainer() string
	// GetCommand returns the pod command for the executor instance
	GetCommand() string
}

type cmdExecutor struct {
	podNamespace string
	podName      string
	container    string
	command      string
	statusFile   string
	taskID       string
}

// Init creates an instance of a command executor to run the given command
func Init(podNamespace, podName, container, command, taskID string) Executor {
	return &cmdExecutor{
		podNamespace: podNamespace,
		podName:      podName,
		container:    container,
		command:      command,
		taskID:       taskID,
	}
}

func (c *cmdExecutor) Start(errChan chan error) error {
	if !strings.Contains(c.command, waitCmdPlaceholder) {
		return fmt.Errorf("given command: %s needs to have ${WAIT_CMD} placeholder", c.command)
	}

	// create status script in target pod
	c.statusFile = fmt.Sprintf(StatusFileFormat, c.taskID)
	killFile := fmt.Sprintf(KillFileFormat, c.taskID)
	waitScriptCreateCmd := fmt.Sprintf("rm -rf %s %s && echo 'touch %s && while [ ! -f %s ]; do sleep 2; done' > %s && chmod +x %s",
		c.statusFile, killFile, c.statusFile, killFile, waitScriptLocation, waitScriptLocation)
	cmdSplit := []string{"/bin/sh", "-c", waitScriptCreateCmd}
	_, err := k8s.Instance().RunCommandInPod(cmdSplit, c.podName, c.container, c.podNamespace)
	if err != nil {
		err = fmt.Errorf("failed to create wait script in pod: [%s] %s using command: %s due to err: %v",
			c.podNamespace, c.podName, waitScriptCreateCmd, err)
		logrus.Errorf(err.Error())
		return err
	}

	command := strings.Replace(c.command, waitCmdPlaceholder, waitScriptLocation, -1)
	go func() {
		logrus.Infof("Running command: %s on pod: [%s] %s", command, c.podNamespace, c.podName)
		cmdSplit = []string{"/bin/sh", "-c", command}
		_, err = k8s.Instance().RunCommandInPod(cmdSplit, c.podName, c.container, c.podNamespace)
		if err != nil {
			err = fmt.Errorf("failed to run command: %s in pod: [%s] %s due to err: %v",
				command, c.podNamespace, c.podName, err)
			logrus.Errorf(err.Error())
		}

		errChan <- err
	}()

	return nil
}

func (c *cmdExecutor) Wait(timeoutInSecs time.Duration) error {
	if len(c.statusFile) == 0 {
		return fmt.Errorf("status file for command: %s in pod: [%s] %s is not set",
			c.command, c.podNamespace, c.podName)
	}

	cmdStatuCheckSteps := int(timeoutInSecs * time.Second / cmdStatusCheckInitialDelay)
	if cmdStatuCheckSteps == 0 {
		cmdStatuCheckSteps = 1
	}

	cmdCheckBackoff := wait.Backoff{
		Duration: cmdStatusCheckInitialDelay,
		Factor:   cmdStatusCheckFactor,
		Jitter:   0.1,
		Steps:    cmdStatuCheckSteps,
	}

	logrus.Infof("check status on pod: [%s] %s with backoff: %v and status file: %s",
		c.podNamespace, c.podName, cmdCheckBackoff, c.statusFile)

	statusCmd := fmt.Sprintf(cmdStatusFormat, c.statusFile)
	if err := wait.ExponentialBackoff(cmdCheckBackoff, func() (bool, error) {
		_, err := k8s.Instance().RunCommandInPod([]string{"/bin/sh", "-c", statusCmd},
			c.podName, c.container, c.podNamespace)
		if err != nil {
			return false, nil
		}

		return true, nil
	}); err != nil {
		err = fmt.Errorf("status command: %s failed to run in pod: [%s] %s due to %v",
			statusCmd, c.podNamespace, c.podName, err)
		return err
	}

	// Remove status file
	if err := wait.ExponentialBackoff(cmdCheckBackoff, func() (bool, error) {
		_, err := k8s.Instance().RunCommandInPod([]string{
			"/bin/sh",
			"-c",
			fmt.Sprintf("rm -rf %s", c.statusFile)}, c.podName, c.container, c.podNamespace)
		if err != nil {
			return false, nil
		}

		return true, nil
	}); err != nil {
		logrus.Warnf("failed to remove status file: %s due to: %v", c.statusFile, err)
	}

	return nil
}

func (c *cmdExecutor) GetPod() (string, string) {
	return c.podNamespace, c.podName
}

func (c *cmdExecutor) GetCommand() string {
	return c.command
}

func (c *cmdExecutor) GetContainer() string {
	return c.container
}
