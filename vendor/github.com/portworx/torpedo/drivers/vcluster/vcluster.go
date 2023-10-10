package vcluster

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/pkg/log"
)

var (
	UpdatedClusterContext string
	CurrentClusterContext string
	ContextChange         = false
	NginxApp              = "nginx"
)

const (
	vClusterCreationTimeout   = 5 * time.Minute
	VClusterRetryInterval     = 2 * time.Second
	VclusterConnectionTimeout = 60 * time.Second
)

// SwitchKubeContext This method switches kube context between host and any vcluster
func SwitchKubeContext(target string) error {
	cmd := exec.Command("kubectl", "config", "get-contexts", "-o", "name")
	out, err := cmd.Output()
	if err != nil {
		return err
	}
	contexts := strings.Split(string(out), "\n")
	var desiredContext string
	if target == "host" {
		for _, ctx := range contexts {
			if ctx == "kubernetes-admin@cluster.local" {
				desiredContext = ctx
				break
			}
		}
	} else {
		prefix := fmt.Sprintf("vcluster_%s_", target)
		for _, ctx := range contexts {
			if strings.HasPrefix(ctx, prefix) {
				desiredContext = ctx
				break
			}
		}
	}
	if desiredContext == "" {
		return fmt.Errorf("Context for %s not found", target)
	}
	log.Infof("Desired Context is : %v", desiredContext)
	cmd = exec.Command("kubectl", "config", "use-context", desiredContext)
	if _, err = cmd.CombinedOutput(); err != nil {
		return err
	}
	cmd = exec.Command("kubectl", "config", "current-context")
	out, err = cmd.Output()
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(out)) != desiredContext {
		return fmt.Errorf("Failed to switch to the desired context: %s", desiredContext)
	}
	return nil
}

// DeleteVCluster This method deletes a vcluster
func DeleteVCluster(vclusterName string) error {
	cmd := exec.Command("vcluster", "delete", vclusterName)
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

// CreateVCluster This method creates a vcluster. This requires vcluster.yaml saved in a specific location.
func CreateVCluster(vclusterName string, absPath string) error {
	cmd := exec.Command("vcluster", "create", vclusterName, "-f", absPath, "--connect=false")
	err := cmd.Run()
	if err != nil {
		return err
	}
	log.Infof("vCluster with the name %v created successfully", vclusterName)
	return nil
}

// WaitForVClusterRunning This method waits for vcluster to come up in Running state and waits for a specific timeout to throw an error
func WaitForVClusterRunning(vclusterName string, timeout time.Duration) error {
	f := func() (interface{}, bool, error) {
		cmd := exec.Command("vcluster", "list")
		output, err := cmd.Output()
		if err != nil {
			return nil, true, err
		}
		if strings.Contains(string(output), vclusterName) && strings.Contains(string(output), "Running") {
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("Vcluster is not yet in running state")
	}
	_, err := task.DoRetryWithTimeout(f, vClusterCreationTimeout, VClusterRetryInterval)
	return err
}
