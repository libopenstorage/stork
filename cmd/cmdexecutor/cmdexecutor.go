package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/libopenstorage/stork/pkg/cmdexecutor"
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/sirupsen/logrus"
)

const (
	defaultStatusCheckTimeout = 900
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

func parsePodNameAndNamespace(podString string) (string, string, error) {
	if strings.Contains(podString, "/") {
		parts := strings.Split(podString, "/")
		if len(parts) != 2 {
			return "", "", fmt.Errorf("invalid pod string: %s", podString)
		}

		return parts[0], parts[1], nil
	}

	return "default", podString, nil
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

	if len(taskID) == 0 {
		logrus.Fatalf("no taskid specified to the command executor")
	}

	logrus.Infof("Using timeout: %v seconds", statusCheckTimeout)
	_, err := os.Stat(statusFile)
	if err == nil {
		err = os.Remove(statusFile)
		if err != nil {
			logrus.Fatalf("failed to remove statusfile: %s due to: %v", statusFile, err)
		}
	}

	executors := make([]cmdexecutor.Executor, 0)
	// Start the commands
	for _, pod := range podList {
		namespace, name, err := parsePodNameAndNamespace(pod)
		if err != nil {
			logrus.Fatalf("failed to parse pod due to: %v", err)
		}

		logrus.Infof("pod: [%s] %s", namespace, name)
		executor := cmdexecutor.Init(namespace, name, podContainer, command, taskID)
		err = executor.Start()
		if err != nil {
			logrus.Fatalf("failed to run command in pod: [%s] %s due to: %v", namespace, name, err)
		}

		executors = append(executors, executor)
	}

	// Check command status
	logrus.Infof("Checking status on command: %s on pods", command)
	failedPods := make(map[string]error)
	for _, executor := range executors {
		err := executor.Wait(time.Duration(statusCheckTimeout))
		if err != nil {
			ns, name := executor.GetPod()
			failedPods[fmt.Sprintf("%s/%s", ns, name)] = err
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
}

// command line arguments
var (
	podList            arrayFlags
	podContainer       string
	command            string
	statusCheckTimeout int64
	taskID             string
)

func init() {
	flag.Var(&podList, "pod", "Pod on which to run the command. Format: <pod-namespace>/<pod-name> e.g dev/pod-12345")
	flag.StringVar(&podContainer, "container", "", "(Optional) name of the container withing the pod on which to run the command. If not specified, executor will pick the first container.")
	flag.StringVar(&command, "cmd", "", "The command to run inside the pod")
	flag.StringVar(&taskID, "taskid", "", "A unique ID the caller can provide which can be later used to clean the status files created by the command executor.")
	flag.Int64Var(&statusCheckTimeout, "timeout", int64(defaultStatusCheckTimeout), "Time in seconds to wait for the command to succeeded on a single pod")
}
