package utils

import "time"

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
	defaultWaitRebootRetry       = 10 * time.Second
	defaultCommandRetry          = 5 * time.Second
	defaultCommandTimeout        = 1 * time.Minute
	defaultTestConnectionTimeout = 15 * time.Minute
)

var (
	errors  []error
	errChan = make(chan error, 100)
)
