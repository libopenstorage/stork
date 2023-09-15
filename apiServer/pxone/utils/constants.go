package utils

import (
	"github.com/portworx/sched-ops/k8s/core"
	"time"
)

type HelmPayload struct {
	Command string `json:"command"`
}

const (
	mysql                        = "mysql"
	cassandra                    = "cassandra"
	fio                          = "fio"
	mongodb                      = "mongodb"
	pgbench                      = "pgbench"
	testName                     = "pxone-automation"
	pxNameSpace                  = "kube-system"
	defaultWaitRebootRetry       = 10 * time.Second
	defaultCommandRetry          = 5 * time.Second
	defaultCommandTimeout        = 1 * time.Minute
	defaultTestConnectionTimeout = 15 * time.Minute
)

var (
	errors  []error
	errChan = make(chan error, 100)
	k8sCore = core.Instance()
)
