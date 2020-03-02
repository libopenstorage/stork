package rke

import (
	"fmt"

	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	kube "github.com/portworx/torpedo/drivers/scheduler/k8s"
)

const (
	// SchedName is the name of the kubernetes scheduler driver implementation
	SchedName = "rke"
	// SystemdSchedServiceName is the name of the system service resposible for scheduling
	SystemdSchedServiceName = "kubelet"
)

type rke struct {
	kube.K8s
}

func (k *rke) SaveSchedulerLogsToFile(n node.Node, location string) error {
	driver, _ := node.Get(k.K8s.NodeDriverName)
	// requires 2>&1 since docker logs command send the logs to stdrr instead of sdout
	cmd := fmt.Sprintf("docker logs %s > %s/kubelet.log 2>&1", SystemdSchedServiceName, location)
	_, err := driver.RunCommand(n, cmd, node.ConnectionOpts{
		Timeout:         kube.DefaultTimeout,
		TimeBeforeRetry: kube.DefaultRetryInterval,
		Sudo:            true,
	})
	return err
}

func init() {
	k := &rke{}
	scheduler.Register(SchedName, k)
}
