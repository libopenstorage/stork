package task

import (
	"time"

	"github.com/onsi/ginkgo"
	"github.com/portworx/sched-ops/task"
)

func DoRetryWithTimeoutWithGinkgoRecover(t func() (interface{}, bool, error), timeout, timeBeforeRetry time.Duration) (interface{}, error) {
	cb := func() (interface{}, bool, error) {
		defer ginkgo.GinkgoRecover()
		return t()
	}
	return task.DoRetryWithTimeout(cb, timeout, timeBeforeRetry)
}
