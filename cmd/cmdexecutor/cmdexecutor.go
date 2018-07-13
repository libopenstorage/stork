package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/libopenstorage/stork/pkg/cmdexecutor"
	"github.com/libopenstorage/stork/pkg/version"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
			persistStatusErr := persistStatus(msg)
			if persistStatusErr != nil {
				logrus.Warnf("failed to persist cmd executor status due to: %v", persistStatusErr)
			}
			logrus.Fatalf(msg)
		}

		executors = append(executors, executor)
	}

	// Create an aggregrate channel for errors
	aggChan := make(chan error)
	for _, ch := range errChans {
		go func(c chan error) {
			for err := range c {
				aggChan <- err
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

	// Wait on the aggregrated channel for errors and completion
	doneCount := 0
Loop:
	for {
		select {
		case err := <-aggChan:
			persistStatusErr := persistStatus(err.Error())
			if persistStatusErr != nil {
				logrus.Warnf("failed to persist cmd executor status due to: %v", persistStatusErr)
			}

			logrus.Fatalf(err.Error())
		case isDone := <-done:
			if isDone {
				doneCount++
				if doneCount == len(executors) {
					logrus.Infof("successfully executed command: %s on all pods: %v", command, podList)
					_, err = os.OpenFile(statusFile, os.O_RDONLY|os.O_CREATE, 0666)
					if err != nil {
						logrus.Fatalf("failed to create statusfile: %s due to: %v", statusFile, err)
					}
					break Loop
				}
			}
		}
	}
}

func persistStatus(status string) error {
	var err error
	hostname := os.Getenv("HOSTNAME")
	if len(hostname) == 0 {
		hostname, err = os.Hostname()
		if err != nil {
			return fmt.Errorf("failed to get hostname of command executor due to: %v", err)
		}
	}

	if len(hostname) == 0 {
		return fmt.Errorf("failed to get hostname of command executor")
	}

	cm, err := k8s.Instance().GetConfigMap(cmdexecutor.StatusConfigMapName, meta_v1.NamespaceSystem)
	if err != nil {
		if errors.IsNotFound(err) {
			// create one
			defaultData := map[string]string{
				hostname: "",
			}
			cm = &v1.ConfigMap{
				ObjectMeta: meta_v1.ObjectMeta{
					Namespace: meta_v1.NamespaceSystem,
					Name:      cmdexecutor.StatusConfigMapName,
				},
				Data: defaultData,
			}
			cm, err = k8s.Instance().CreateConfigMap(cm)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	cmCopy := cm.DeepCopy()
	if cmCopy.Data == nil {
		cmCopy.Data = make(map[string]string)
	}
	cmCopy.Data[hostname] = status
	cm, err = k8s.Instance().UpdateConfigMap(cmCopy)
	if err != nil {
		return err
	}

	return nil
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
