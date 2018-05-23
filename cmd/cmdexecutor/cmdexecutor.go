package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/libopenstorage/stork/pkg/version"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	"github.com/skyrings/skyring-common/tools/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
)

const defaultStatusCheckTimeout = 15 * time.Minute
const (
	statusFileFormat   = "/tmp/stork-cmd-done-%s"
	cmdWaitFormat      = "touch %s && tail -f /dev/null;"
	cmdStatusFormat    = "stat %s"
	waitCmdPlaceholder = "${WAIT_CMD}"
)

const (
	cmdStatusCheckInitialDelay = 2 * time.Second
	cmdStatusCheckFactor       = 1
)

type arrayFlags []string

func (i *arrayFlags) String() string {
	return strings.Join(*i, ",")
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func parsePodNameAndNamespace(podString string) (string, string) {
	if strings.Contains(podString, "/") {
		parts := strings.Split(podString, "/")
		return parts[0], parts[1]
	}

	return "default", podString
}

func createPodStringFromNameAndNamespace(namespace, name string) string {
	return namespace + "/" + name
}

// startAsyncPodCommand starts the given command in the given pod async and returns a status file
// inside the pod that will be present if the command succeeded.
func startAsyncPodCommand(podNamespace, podName, container, command string) (string /*status file */, error) {
	if !strings.Contains(command, waitCmdPlaceholder) {
		return "", fmt.Errorf("given command: %s needs to have ${WAIT_CMD} placeholder", command)
	}

	uid, _ := uuid.New()
	statusFile := fmt.Sprintf(statusFileFormat, uid.String())
	waitCommand := fmt.Sprintf(cmdWaitFormat, statusFile)
	command = strings.Replace(command, waitCmdPlaceholder, waitCommand, -1)

	go func() {
		logrus.Infof("Running command: %s on pod: [%s] %s", command, podNamespace, podName)
		cmdSplit := []string{"/bin/sh", "-c", command}
		cmdSplit = append(cmdSplit, strings.Split(command, " ")...)
		_, err := k8s.Instance().RunCommandInPod(cmdSplit, podName, container, podNamespace)
		if err != nil {
			logrus.Errorf("failed to run command: %s command due to err: %v", command, err)
		}
	}()

	return statusFile, nil
}

// checkFileExistsInPod checks if the given status file exists in the given pod.
//	timeoutInSecs is number of seconds after which the check should timeout.
func checkFileExistsInPod(podNamespace, podName, container, statusFile string, timeoutInSecs time.Duration) error {
	cmdStatuCheckSteps := int(timeoutInSecs * time.Second / cmdStatusCheckInitialDelay)
	if cmdStatuCheckSteps == 0 {
		cmdStatuCheckSteps = 1
	}

	cmdCheckBackoff := wait.Backoff{
		Duration: cmdStatusCheckInitialDelay,
		Factor:   cmdStatusCheckFactor,
		Steps:    cmdStatuCheckSteps,
	}

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

	return nil
}

func main() {
	logrus.Infof("Running pod command executor: %v", version.Version)
	flag.Parse()

	if len(podList) == 0 {
		logrus.Fatalf("no pods specified to the command executor")
	}

	if len(command) == 0 {
		logrus.Fatalf("no command specified to the command executor")
	}

	logrus.Infof("Using timeout: %v seconds", statusCheckTimeout)

	statusFileMap := make(map[string]string, 0)
	// Start the commands
	for _, pod := range podList {
		namespace, name := parsePodNameAndNamespace(pod)
		statusFile, err := startAsyncPodCommand(namespace, name, podContainer, command)
		if err != nil {
			logrus.Fatalf("failed to run command in pod: [%s] %s due to: %v", namespace, name, err)
		}

		statusFileMap[createPodStringFromNameAndNamespace(namespace, name)] = statusFile
	}

	// Check command status
	logrus.Infof("Checking status on command: %s on pods", command)
	failedPods := make(map[string]error, 0)
	for podString, statusFile := range statusFileMap {
		namespace, name := parsePodNameAndNamespace(podString)
		err := checkFileExistsInPod(namespace, name, podContainer, statusFile, time.Duration(statusCheckTimeout))
		if err != nil {
			failedPods[podString] = err
		}
	}

	if len(failedPods) > 0 {
		logrus.Fatalf("pod executor failed as following commands failed. %v", failedPods)
	}

	logrus.Infof("successfully executed command: %s on all pods: %v. Exiting...", command, podList)
}

// command line arguments
var (
	podList            arrayFlags
	podContainer       string
	command            string
	statusCheckTimeout int64
)

func init() {
	flag.Var(&podList, "pod", "Pod on which to run the command. Format: <pod-namespace>/<pod-name> e.g dev/pod-12345")
	flag.StringVar(&podContainer, "container", "", "(Optional) name of the container withing the pod on which to run the command. If not specified, executor will pick the first container.")
	flag.StringVar(&command, "cmd", "", "The command to run inside the pod")
	flag.Int64Var(&statusCheckTimeout, "timeout", int64(defaultStatusCheckTimeout), "Time in seconds to wait for the command to succeeded on a single pod")
}
