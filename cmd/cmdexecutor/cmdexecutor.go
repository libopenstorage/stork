package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/libopenstorage/stork/pkg/cmdexecutor"
	"github.com/libopenstorage/stork/pkg/cmdexecutor/status"
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

	// Get hostname which will be used as a key to track status of this command executor's commands
	hostname, err := getHostname()
	if err != nil {
		logrus.Fatalf(err.Error())
	}

	executors := make([]cmdexecutor.Executor, 0)
	errChans := make(map[string]chan error)
	// Start the commands
	for _, pod := range podList {
		namespace, name, err := parsePodNameAndNamespace(pod)
		if err != nil {
			logrus.Fatalf("failed to parse pod due to: %v", err)
		}

		executor := cmdexecutor.Init(namespace, name, podContainer, command, taskID)
		errChan := make(chan error)
		errChans[createPodStringFromNameAndNamespace(namespace, name)] = errChan

		err = executor.Start(errChan)
		if err != nil {
			msg := fmt.Sprintf("failed to run command in pod: [%s] %s due to: %v", namespace, name, err)
			persistStatusErr := status.Persist(hostname, msg)
			if persistStatusErr != nil {
				logrus.Warnf("failed to persist cmd executor status due to: %v", persistStatusErr)
			}
			logrus.Fatalf(msg)
		}

		executors = append(executors, executor)
	}

	// Create an aggregrate channel for all error channels for above executors
	aggErrorChan := make(chan error)
	for _, ch := range errChans {
		go func(c chan error) {
			for err := range c {
				aggErrorChan <- err
			}
		}(ch)
	}

	// Check command status
	done := make(chan bool)
	logrus.Infof("Checking status on command: %s", command)
	for _, executor := range executors {
		ns, name := executor.GetPod()
		podKey := createPodStringFromNameAndNamespace(ns, name)
		go func(errChan chan error, doneChan chan bool, execInst cmdexecutor.Executor) {
			err := execInst.Wait(time.Duration(statusCheckTimeout))
			if err != nil {
				errChan <- err
				return
			}

			doneChan <- true
		}(errChans[podKey], done, executor)
	}

	// Now go into a wait loop which will exit if either of the 2 things happen
	//   1) If any of the executors return error    (FAIL)
	//   2) All the executors complete successfully (PASS)
	doneCount := 0
Loop:
	for {
		select {
		case err := <-aggErrorChan:
			// If we hit any error, persist the error using hostname as key and then exit
			persistStatusErr := status.Persist(hostname, err.Error())
			if persistStatusErr != nil {
				logrus.Warnf("failed to persist cmd executor status due to: %v", persistStatusErr)
			}

			logrus.Fatalf(err.Error())
		case isDone := <-done:
			if isDone {
				// as each executor is done, track how many are done
				doneCount++
				if doneCount == len(executors) {
					logrus.Infof("successfully executed command: %s on all pods: %v", command, podList)
					_, err = os.OpenFile(statusFile, os.O_RDONLY|os.O_CREATE, 0666)
					if err != nil {
						logrus.Fatalf("failed to create statusfile: %s due to: %v", statusFile, err)
					}
					// All executors are done, we can exit successfully now
					break Loop
				}
			}
		}
	}
}

func getHostname() (string, error) {
	var err error
	hostname := os.Getenv("HOSTNAME")
	if len(hostname) == 0 {
		hostname, err = os.Hostname()
		if err != nil {
			return "", fmt.Errorf("failed to get hostname of command executor due to: %v", err)
		}
	}

	return hostname, nil
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
	flag.StringVar(&podContainer, "container", "", "(Optional) name of the container within the pod on which to run the command. If not specified, executor will pick the first container.")
	flag.StringVar(&command, "cmd", "", "The command to run inside the pod")
	flag.StringVar(&taskID, "taskid", "", "A unique ID the caller can provide which can be later used to clean the status files created by the command executor.")
	flag.Int64Var(&statusCheckTimeout, "timeout", int64(defaultStatusCheckTimeout), "Time in seconds to wait for the command to succeeded on a single pod")
}
