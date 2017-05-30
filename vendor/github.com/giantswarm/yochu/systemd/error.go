package systemd

import (
	"github.com/juju/errgo"
)

// Errors related to systemd.
var (
	JobCanceledError             = errgo.New("job has been canceled before it finished execution")
	JobTimeoutError              = errgo.New("job timeout was reached")
	JobFailedError               = errgo.New("job failed")
	JobSkippedError              = errgo.New("job was skipped because it didn't apply to the units current state")
	JobDependencyError           = errgo.New("job failed because of failed dependency")
	JobExecutionTookTooLongError = errgo.New("job execution took too long")
	UnknownSystemdResponseError  = errgo.New("received unknown systemd response")

	maskAny = errgo.MaskFunc(errgo.Any)
)

// IsJobDependency returns true if the given err is a JobDependencyError.
func IsJobDependency(err error) bool {
	return errgo.Cause(err) == JobDependencyError
}
