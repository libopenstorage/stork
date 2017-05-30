// Package systemd provides systemd related actions.
package systemd

import (
	"time"

	systemdPkg "github.com/coreos/go-systemd/dbus"
)

var vLogger = func(f string, v ...interface{}) {}

const waitJobExecution = 60 * time.Second

// Configure sets the logger for this package.
func Configure(vl func(f string, v ...interface{})) {
	vLogger = vl
}

// SystemdClient is a client for managing systemd.
type SystemdClient struct{}

// NewSystemdClient returns a new SystemdClient.
func NewSystemdClient() (*SystemdClient, error) {
	return &SystemdClient{}, nil
}

// Reload instructs systemd to scan for and reload unit files.
// Equivalent to a 'systemctl daemon-reload'.
// See https://godoc.org/github.com/coreos/go-systemd/dbus#Conn.Reload.
func (sdc *SystemdClient) Reload() error {
	vLogger("  call SystemdClient.Reload()")

	conn, err := systemdPkg.New()
	if err != nil {
		return maskAny(err)
	}

	if err := conn.Reload(); err != nil {
		return maskAny(err)
	}

	return nil
}

// Start instructs systemd to start a job.
// See http://godoc.org/github.com/coreos/go-systemd/dbus#Conn.StartUnit.
func (sdc *SystemdClient) Start(unit string) error {
	vLogger("  call SystemdClient.Start(unit): %s", unit)

	conn, err := systemdPkg.New()
	if err != nil {
		return maskAny(err)
	}

	strChan := make(chan string, 1)
	if _, err := conn.StartUnit(unit, "replace", strChan); err != nil {
		return maskAny(err)
	}

	select {
	case res := <-strChan:
		switch res {
		case "done":
			return nil
		case "canceled":
			return maskAny(JobCanceledError)
		case "timeout":
			return maskAny(JobTimeoutError)
		case "failed":
			// We need a start considered to be failed, when the unit is already running.
			return nil
		case "dependency":
			return maskAny(JobDependencyError)
		case "skipped":
			return maskAny(JobSkippedError)
		default:
			// that should never happen
			vLogger("  unexpected systemd response: '%s'", res)
			return maskAny(UnknownSystemdResponseError)
		}
	case <-timeoutJobExecution():
		return maskAny(JobExecutionTookTooLongError)
	}

	return nil
}

// Stop instructs systemd to stop a job.
// See https://godoc.org/github.com/coreos/go-systemd/dbus#Conn.StopUnit.
func (sdc *SystemdClient) Stop(unit string) error {
	vLogger("  call SystemdClient.Stop(unit): %s", unit)

	conn, err := systemdPkg.New()
	if err != nil {
		return maskAny(err)
	}

	strChan := make(chan string, 1)
	if _, err := conn.StopUnit(unit, "replace", strChan); err != nil {
		return maskAny(err)
	}

	select {
	case res := <-strChan:
		switch res {
		case "done":
			return nil
		case "canceled":
			// In case the job that is stopped is canceled (because it was running),
			// it is stopped, so all good.
			return nil
		case "timeout":
			return maskAny(JobTimeoutError)
		case "failed":
			return maskAny(JobFailedError)
		case "dependency":
			return maskAny(JobDependencyError)
		case "skipped":
			return maskAny(JobSkippedError)
		default:
			// that should never happen
			vLogger("  unexpected systemd response: '%s'", res)
			return maskAny(UnknownSystemdResponseError)
		}
	case <-timeoutJobExecution():
		return maskAny(JobExecutionTookTooLongError)
	}

	return nil
}

// Exists checks if a unit exists in systemd.
func (sdc *SystemdClient) Exists(unit string) (bool, error) {
	vLogger("  call SystemdClient.Exists(unit): %s", unit)

	conn, err := systemdPkg.New()
	if err != nil {
		return false, maskAny(err)
	}

	ustates, err := conn.ListUnits()
	if err != nil {
		return false, maskAny(err)
	}

	for _, ustate := range ustates {
		if ustate.Name == unit {
			return true, nil
		}
	}

	return false, nil
}

func timeoutJobExecution() chan bool {
	timeout := make(chan bool, 1)

	go func() {
		time.Sleep(waitJobExecution)
		timeout <- true
	}()

	return timeout
}
