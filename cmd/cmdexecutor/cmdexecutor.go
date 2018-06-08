package main

import (
	"flag"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/libopenstorage/stork/pkg/cmdexecutor"
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/sirupsen/logrus"
)

const (
	defaultStatusCheckTimeout = 15 * time.Minute
	statusFile                = "/tmp/cmdexecutor-status"
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
	_, err := os.Stat(statusFile)
	if err == nil {
		err = os.Remove(statusFile)
		if err != nil {
			logrus.Fatalf("failed to remove statusfile: %s due to: %v", statusFile, err)
		}
	}

	statusFileMap := make(map[string]string, 0)
	// Start the commands
	for _, pod := range podList {
		namespace, name := parsePodNameAndNamespace(pod)
		statusFile, err := cmdexecutor.StartAsyncPodCommand(namespace, name, podContainer, command)
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
		err := cmdexecutor.CheckFileExistsInPod(namespace, name, podContainer, statusFile, time.Duration(statusCheckTimeout))
		if err != nil {
			failedPods[podString] = err
		}
	}

	if len(failedPods) > 0 {
		logrus.Fatalf("pod executor failed as following commands failed. %v", failedPods)
	}

	logrus.Infof("successfully executed command: %s on all pods: %v...", command, podList)
	_, err = os.OpenFile(statusFile, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		logrus.Fatalf("failed to create statusfile: %s due to: %v", statusFile, err)
	}

	runtime.Goexit()
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
