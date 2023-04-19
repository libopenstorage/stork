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
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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

// Parses label selector (e.g. "label1=value1,label2=value2") into a map of strings.
func parseLabelSelector(labelSelectorString string) (map[string]string, error) {
	labelSelector, err := metav1.ParseToLabelSelector(labelSelectorString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse label selector due to: %v", err)
	}
	selectorsMap, err := metav1.LabelSelectorAsMap(labelSelector)
	if err != nil {
		return nil, fmt.Errorf("failed to convert label selector to map of strings due to: %v", err)
	}
	return selectorsMap, nil
}

func createPodStringFromNameAndNamespace(namespace, name string) string {
	return namespace + "/" + name
}

func main() {
	logrus.Infof("Running pod command executor: %v", version.Version)
	flag.Parse()

	podListSpecified := len(podList) > 0
	selectorAndNamespaceSpecified := labelSelector != "" && namespace != ""

	if labelSelector != "" && namespace == "" {
		logrus.Fatalf("-namespace must be specified when using -selector")
	}

	if podListSpecified && selectorAndNamespaceSpecified {
		logrus.Fatalf("-pod args cannot be used in combination with -selector arg")
	}

	if !podListSpecified && !selectorAndNamespaceSpecified {
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

	var podNames []types.NamespacedName
	// Get list of specified by the -pod args.
	if podListSpecified {
		podNames, err = getPodNamesFromArgs(podList)
		if err != nil {
			logrus.Fatalf(err.Error())
		}
	}

	// Get list of pods specified by the -selector and -namespace args.
	if selectorAndNamespaceSpecified {
		podNames, err = getPodNamesUsingLabelSelector(labelSelector, namespace)
		if err != nil {
			logrus.Fatalf(err.Error())
		}
	}

	executors := make([]cmdexecutor.Executor, 0)
	stdoutChans := make(map[string]chan string)
	errChans := make(map[string]chan error)
	// Start the commands
	for _, pod := range podNames {
		executor := cmdexecutor.Init(pod.Namespace, pod.Name, podContainer, command, taskID)
		stdoutChan := make(chan string)
		errChan := make(chan error)
		errChans[createPodStringFromNameAndNamespace(pod.Namespace, pod.Name)] = errChan
		stdoutChans[createPodStringFromNameAndNamespace(pod.Namespace, pod.Name)] = stdoutChan

		err = executor.Start(stdoutChan, errChan)
		if err != nil {
			msg := fmt.Sprintf("failed to run command in pod: [%s] %s due to: %v", pod.Namespace, pod.Name, err)
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
		go func(errChan chan error, stdoutChan chan string, doneChan chan bool, execInst cmdexecutor.Executor) {
			err := execInst.Wait(time.Duration(statusCheckTimeout) * time.Second)
			// log the output of command execution before returning/marking it done
			for stdout := range stdoutChan {
				logrus.Infof("[%s] :: %s", podKey, stdout)
			}
			if err != nil {
				errChan <- err
				return
			}

			doneChan <- true
		}(errChans[podKey], stdoutChans[podKey], done, executor)
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
			if err != nil {
				persistStatusErr := status.Persist(hostname, err.Error())
				if persistStatusErr != nil {
					logrus.Warnf("failed to persist cmd executor status due to: %v", persistStatusErr)
				}

				logrus.Fatalf(err.Error())
			}
		case isDone := <-done:
			if isDone {
				// as each executor is done, track how many are done
				doneCount++
				if doneCount == len(executors) {
					logrus.Infof("successfully executed command: %s on all pods: %v", command, podNames)
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

func getPodNamesFromArgs(podList []string) ([]types.NamespacedName, error) {
	var podNames []types.NamespacedName
	for _, pod := range podList {
		namespace, name, err := parsePodNameAndNamespace(pod)
		if err != nil {
			return nil, fmt.Errorf("failed to parse pod due to: %v", err)
		}
		podNames = append(podNames, types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		})
	}
	return podNames, nil
}

func getPodNamesUsingLabelSelector(labelSelector, namespace string) ([]types.NamespacedName, error) {
	selectorsMap, err := parseLabelSelector(labelSelector)
	if err != nil {
		return nil, err
	}

	pods, err := core.Instance().GetPods(namespace, selectorsMap)
	if err != nil {
		return nil, err
	}

	var podNames []types.NamespacedName
	for _, pod := range pods.Items {
		podNames = append(podNames, types.NamespacedName{
			Namespace: pod.Namespace,
			Name:      pod.Name,
		})
	}
	return podNames, nil
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
	labelSelector      string
	namespace          string
	podContainer       string
	command            string
	statusCheckTimeout int64
	taskID             string
)

func init() {
	flag.Var(&podList, "pod", "Pod on which to run the command. Format: <pod-namespace>/<pod-name> e.g dev/pod-12345")
	flag.StringVar(&labelSelector, "selector", "", "Label selector to specify pods on which to run the command. Must be used with the -namespace flag.")
	flag.StringVar(&namespace, "namespace", "", "Name of the namespace of the pods on which to run the command.")
	flag.StringVar(&podContainer, "container", "", "(Optional) name of the container within the pod on which to run the command. If not specified, executor will pick the first container.")
	flag.StringVar(&command, "cmd", "", "The command to run inside the pod")
	flag.StringVar(&taskID, "taskid", "", "A unique ID the caller can provide which can be later used to clean the status files created by the command executor.")
	flag.Int64Var(&statusCheckTimeout, "timeout", int64(defaultStatusCheckTimeout), "Time in seconds to wait for the command to succeeded on a single pod")
}
