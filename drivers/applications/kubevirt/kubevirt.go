package kubevirt

import (
	"fmt"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	. "github.com/portworx/torpedo/drivers/applications/apptypes"
	"github.com/portworx/torpedo/drivers/node"
	. "github.com/portworx/torpedo/drivers/utilities"
	"github.com/portworx/torpedo/pkg/log"
	"golang.org/x/net/context"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"strings"
	"time"
)

const (
	sshPodName      = "ssh-pod"
	sshPodNamespace = "default"
	sshContainer    = "ssh-container"
)

type KubevirtConfig struct {
	Hostname     string
	User         string
	Password     string
	Namespace    string
	DataCommands map[string]map[string][]string
	IPAddress    string
	NodeDriver   node.Driver
}

// RunCmdInVM runs a command in the VM by SSHing into it
func (app *KubevirtConfig) ExecuteCommand(commands []string, ctx context.Context) ([]string, error) {
	// Kubevirt client
	var k8sCore = core.Instance()
	var result []string

	log.Infof("VM Name - %s", app.Hostname)
	log.Infof("IP Address - %s", app.IPAddress)

	log.Infof("Username - %s", app.User)

	if os.Getenv("CLUSTER_PROVIDER") == "openshift" {
		// Executing the actual command
		for _, eachCommand := range commands {
			cmdArgs := getSSHCommandArgs(app.User, app.Password, app.IPAddress, eachCommand)
			output, err := k8sCore.RunCommandInPod(cmdArgs, sshPodName, sshContainer, sshPodNamespace)
			if err != nil {
				return result, err
			}
			result = append(result, output)
			time.Sleep(2 * time.Second)
		}

	} else {
		workerNode := node.GetWorkerNodes()[0]
		t := func() (interface{}, bool, error) {
			for _, eachCommand := range commands {
				cmd := strings.Join(getSSHCommandArgs(app.User, app.Password, app.IPAddress, eachCommand), " ")
				log.Infof("Executing = [%s]", cmd)
				output, err := RunCmdGetOutputOnNode(cmd, workerNode, app.NodeDriver)
				if err != nil {
					log.Infof("Error encountered")
					if isConnectionError(err.Error()) {
						log.Infof("Output of cmd %s - \n%s", cmd, output)
						return "", true, err
					} else {
						return "", false, err
					}
				}
				result = append(result, output)
				time.Sleep(2 * time.Second)

			}
			return result, false, nil
		}
		_, err := task.DoRetryWithTimeout(t, 2*time.Minute, 30*time.Second)
		if err != nil {
			return result, err
		}
	}
	log.Infof("Final Output - [%v]", result)
	return result, nil
}

func (app *KubevirtConfig) WaitForVMToBoot() error {
	var k8sCore = core.Instance()
	testCmdArgs := getSSHCommandArgs(app.User, app.Password, app.IPAddress, "hostname")

	// If the cluster provider is openshift then we are creating ssh pod and running the command in it
	if os.Getenv("CLUSTER_PROVIDER") == "openshift" {
		log.Infof("Cluster is openshift hence creating the SSH Pod")
		err := initSSHPod(sshPodNamespace)
		if err != nil {
			return err
		}

		// To check if the ssh server is up and running
		t := func() (interface{}, bool, error) {
			output, err := k8sCore.RunCommandInPod(testCmdArgs, sshPodName, sshContainer, sshPodNamespace)
			if err != nil {
				log.Infof("Error encountered - [%s]", err.Error())
				if isConnectionError(err.Error()) {
					log.Infof("Test connection output - \n%s", output)
					return "", true, err
				} else {
					return "", false, err
				}
			}
			log.Infof("Test connection success output - \n%s", output)
			return "", false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 10*time.Minute, 30*time.Second)
		return err
	} else {
		workerNode := node.GetWorkerNodes()[0]
		t := func() (interface{}, bool, error) {
			cmd := strings.Join(getSSHCommandArgs(app.User, app.Password, app.IPAddress, "hostname"), " ")
			log.Infof("Executing = [%s]", cmd)
			output, err := RunCmdGetOutputOnNode(cmd, workerNode, app.NodeDriver)
			if err != nil {
				log.Infof("Error encountered")
				if isConnectionError(err.Error()) {
					log.Infof("Output of cmd %s - \n%s", cmd, output)
					return "", true, err
				} else {
					return "", false, err
				}
			}
			return "", false, nil
		}
		_, err := task.DoRetryWithTimeout(t, 10*time.Minute, 30*time.Second)
		if err != nil {
			return err
		}
	}

	return nil

}

// DefaultPort returns default port for kubevirt
func (app *KubevirtConfig) DefaultPort() int {
	return 22
}

// StartData starts injecting continous data to the application
func (app *KubevirtConfig) StartData(command <-chan string, ctx context.Context) error {
	log.Warnf("Not implemented for kubevirt")
	return nil
}

// InsertBackupData inserts data before and after backup
func (app *KubevirtConfig) InsertBackupData(ctx context.Context, identifier string, commands []string) error {
	var err error
	log.InfoD("Inserting data")
	if len(commands) == 0 {
		log.InfoD("Inserting below data : %s", strings.Join(app.DataCommands[identifier]["insert"], "\n"))
		_, err = app.ExecuteCommand(app.DataCommands[identifier]["insert"], ctx)
	} else {
		log.InfoD("Inserting below data : %s", strings.Join(commands, "\n"))
		_, err = app.ExecuteCommand(commands, ctx)
	}

	return err
}

// GetBackupData gets the sql queries inserted before or after backup
func (app *KubevirtConfig) GetBackupData(identifier string) []string {
	if _, ok := app.DataCommands[identifier]; ok {
		return app.DataCommands[identifier]["select"]
	} else {
		return nil
	}
}

// GetRandomDataCommands creates random CRUD queries for an application
func (app *KubevirtConfig) GetRandomDataCommands(count int) map[string][]string {
	return GenerateRandomCommandToCreateFiles(count)
}

// Update the existing file io commands
func (app *KubevirtConfig) UpdateDataCommands(count int, identifier string) {
	app.DataCommands[identifier] = GenerateRandomCommandToCreateFiles(count)
	log.InfoD("Data Commands updated")
}

// Add file io commands
func (app *KubevirtConfig) AddDataCommands(identifier string, commands map[string][]string) {
	app.DataCommands[identifier] = commands
	log.InfoD("Data commands added")
}

// GetApplicationType retruns the application type
func (app *KubevirtConfig) GetApplicationType() string {
	return Kubevirt
}

// GetNamespace returns the application namespace
func (app *KubevirtConfig) GetNamespace() string {
	return app.Namespace
}

// CheckDataPresent checks if the mentioned entry is present or not in the database
func (app *KubevirtConfig) CheckDataPresent(lsCommands []string, ctx context.Context) error {
	_, err := app.ExecuteCommand(lsCommands, ctx)
	return err
}

// isConnectionError checks if the error message is a connection error
func isConnectionError(errorMessage string) bool {
	return strings.Contains(errorMessage, "Connection refused") || strings.Contains(errorMessage, "Host is unreachable") ||
		strings.Contains(errorMessage, "No route to host")
}

// getSSHCommandArgs command retuns the SSH command Args
func getSSHCommandArgs(username, password, ipAddress, cmd string) []string {
	return []string{"sshpass", "-p", password, "ssh", "-o", "StrictHostKeyChecking=no", fmt.Sprintf("%s@%s", username, ipAddress), cmd}
}

// initSSHPod creates a pod with ssh server installed and running along with sshpass utility
func initSSHPod(namespace string) error {
	var p *corev1.Pod
	var err error
	var k8sCore = core.Instance()
	// Check if namespace exists
	_, err = k8sCore.GetNamespace(namespace)
	if err != nil {
		// Namespace doesn't exist, create it
		log.Infof("Namespace %s does not exist. Creating...", namespace)
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespace,
			},
		}
		if _, err = k8sCore.CreateNamespace(ns); err != nil {
			log.Errorf("Failed to create namespace %s: %v", namespace, err)
			return err
		}
		log.Infof("Namespace %s created successfully", namespace)
	}
	if p, err = k8sCore.GetPodByName(sshPodName, namespace); p == nil {
		sshPodSpec := &corev1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      sshPodName,
				Namespace: namespace,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "ssh-container",
						Image: "ubuntu:latest",
						Command: []string{
							"/bin/bash",
							"-c",
						},
						Args: []string{
							"apt-get update && apt-get install -y openssh-server sshpass && service ssh start && echo 'root:toor' | chpasswd && sleep infinity",
						},
					},
				},
			},
		}
		log.Infof("Creating ssh pod")
		_, err = k8sCore.CreatePod(sshPodSpec)
		if err != nil {
			log.Errorf("An Error Occured while creating %v", err)
			return err
		}
		t := func() (interface{}, bool, error) {
			pod, err := k8sCore.GetPodByName(sshPodName, namespace)
			if err != nil {
				return "", false, err
			}
			if !k8sCore.IsPodRunning(*pod) {
				return "", true, fmt.Errorf("waiting for pod %s to be in running state", sshPodName)
			}
			// Adding static sleep to let the ssh server start
			time.Sleep(30 * time.Second)
			log.Infof("ssh pod creation complete")
			return "", false, nil
		}
		_, err = task.DoRetryWithTimeout(t, 5*time.Minute, 30*time.Second)
		return err
	}
	return err
}
