package cmdexecutor

import (
	"fmt"
	"strings"
	"time"

	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	"github.com/skyrings/skyring-common/tools/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	statusFileFormat   = "/tmp/stork-cmd-done-%s"
	cmdWaitFormat      = "touch %s && tail -f /dev/null;"
	cmdStatusFormat    = "stat %s"
	killFile           = "/tmp/killme"
	waitScriptLocation = "/tmp/wait.sh"
	waitCmdPlaceholder = "${WAIT_CMD}"
)

const (
	cmdStatusCheckInitialDelay = 2 * time.Second
	cmdStatusCheckFactor       = 1
)

// StartAsyncPodCommand starts the given command in the given pod async and returns a status file
// inside the pod that will be present if the command succeeded.
func StartAsyncPodCommand(podNamespace, podName, container, command string) (string /*status file */, error) {
	if !strings.Contains(command, waitCmdPlaceholder) {
		return "", fmt.Errorf("given command: %s needs to have ${WAIT_CMD} placeholder", command)
	}

	uid, err := uuid.New()
	if err != nil {
		return "", fmt.Errorf("failed to generate uuid due to: %v", err)
	}

	// create status script in target pod
	statusFile := fmt.Sprintf(statusFileFormat, uid.String())
	waitScriptCreateCmd := fmt.Sprintf("rm -rf %s %s && echo 'touch %s && while [ ! -f %s ]; do sleep 2; done' > %s && chmod +x %s",
		statusFile, killFile, statusFile, killFile, waitScriptLocation, waitScriptLocation)
	cmdSplit := []string{"/bin/sh", "-c", waitScriptCreateCmd}
	_, err = k8s.Instance().RunCommandInPod(cmdSplit, podName, container, podNamespace)
	if err != nil {
		logrus.Errorf("failed to run command: %s command due to err: %v", command, err)
		return "", err
	}

	command = strings.Replace(command, waitCmdPlaceholder, waitScriptLocation, -1)
	go func() {
		logrus.Infof("Running command: %s on pod: [%s] %s", command, podNamespace, podName)
		cmdSplit = []string{"/bin/sh", "-c", command}
		_, err = k8s.Instance().RunCommandInPod(cmdSplit, podName, container, podNamespace)
		if err != nil {
			logrus.Errorf("failed to run command: %s command due to err: %v", command, err)
		}
	}()

	return statusFile, nil
}

// CheckFileExistsInPod checks if the given status file exists in the given pod and if found deletes it
//	timeoutInSecs is number of seconds after which the check should timeout.
func CheckFileExistsInPod(podNamespace, podName, container, statusFile string, timeoutInSecs time.Duration) error {
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

	logrus.Infof("check status on pod: [%s] %s with backooff: %v", podNamespace, podName, cmdCheckBackoff)

	statusCmd := fmt.Sprintf(cmdStatusFormat, statusFile)
	if err := wait.ExponentialBackoff(cmdCheckBackoff, func() (bool, error) {
		_, err := k8s.Instance().RunCommandInPod([]string{"/bin/sh", "-c", statusCmd}, podName, container, podNamespace)
		if err != nil {
			return false, nil
		}

		return true, nil
	}); err != nil {
		logrus.Errorf("status command: %s failed to run. %v", statusCmd, err)
		return err
	}

	// Remove status file
	if err := wait.ExponentialBackoff(cmdCheckBackoff, func() (bool, error) {
		_, err := k8s.Instance().RunCommandInPod([]string{
			"/bin/sh",
			"-c",
			fmt.Sprintf("rm -rf %s", statusFile)}, podName, container, podNamespace)
		if err != nil {
			return false, nil
		}

		return true, nil
	}); err != nil {
		logrus.Warnf("failed to remove status file: %s due to: %v", statusFile, err)
	}

	return nil
}
