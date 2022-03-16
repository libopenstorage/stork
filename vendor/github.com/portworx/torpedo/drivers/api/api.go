package api

import (
	"fmt"
	"time"

	"github.com/libopenstorage/openstorage/pkg/sched"
	"github.com/portworx/sched-ops/task"
	tp_errors "github.com/portworx/torpedo/pkg/errors"
	"github.com/sirupsen/logrus"
)

// TriggerOptions are common options used to check if any action is okay to be triggered/performed
type TriggerOptions struct {
	// TriggerCb is the callback function to invoke to check trigger condition
	TriggerCb TriggerCallbackFunc
	// TriggerCheckInterval is the interval at which to check the trigger conditions
	TriggerCheckInterval time.Duration
	// TriggerCheckTimeout is the duration at which the trigger checks should timeout. If the trigger
	TriggerCheckTimeout time.Duration
	// BlockForTrigger if set true will block the call until the TriggerCb either passes or times out
	BlockForTrigger bool
}

// TriggerCallbackFunc is a callback function that are used by scheduler APIs to decide when to trigger/perform actions.
// the function should return true, when it is the right time to perform the respective action
type TriggerCallbackFunc func() (bool, error)

const (
	// defaultTriggerCheckInterval is default for the interval in trigger options
	defaultTriggerCheckInterval = 5 * time.Second
	// defaultTriggerCheckTimeout is the default timeout for checking the trigger options
	defaultTriggerCheckTimeout = 5 * time.Minute
)

// PerformTask invokes the function using the given trigger options to decide when to invoke the task
func PerformTask(userTask func() error, opts *TriggerOptions) error {
	fn := "PerformTask"
	if opts == nil {
		return userTask()
	}

	f := func() error {
		if opts.TriggerCheckTimeout == time.Duration(0) {
			opts.TriggerCheckTimeout = defaultTriggerCheckTimeout
		}

		if opts.TriggerCheckInterval == time.Duration(0) {
			opts.TriggerCheckInterval = defaultTriggerCheckInterval
		}

		// perform trigger checks and then perform the actual task
		userTaskWithTriggerChecks := func() (interface{}, bool, error) {
			triggered, err := opts.TriggerCb()
			if err != nil {
				logrus.Warnf("%s: failed to invoke trigger callback function due to: %v", fn, err)
				return false, false, err
			}

			if triggered {
				// run user task and return
				return triggered, false, userTask()
			}

			return false, true, fmt.Errorf("%s: trigger check has not been met yet", fn)
		}

		triggered, err := task.DoRetryWithTimeout(userTaskWithTriggerChecks, opts.TriggerCheckTimeout, opts.TriggerCheckInterval)
		if err != nil {
			if triggered == nil {
				// timeout error is expected if the trigger conditions don't meet within above timeouts. For any other error,
				// return the error
				_, timedOut := err.(*task.ErrTimedOut)
				if timedOut {
					return &tp_errors.ErrOperationNotPerformed{
						Operation: "Task",
						Reason:    fmt.Sprintf("%s: Trigger checks did not pass", fn),
					}
				}
				return &tp_errors.ErrOperationNotPerformed{
					Operation: "Task",
					Reason:    fmt.Sprintf("%s:Trigger checks could not be performed: %v", fn, err),
				}
			}
			return err
		}

		return nil
	}

	// sync
	if opts.BlockForTrigger {
		return f()
	}

	// async
	_, err := sched.Instance().Schedule(
		func(_ sched.Interval) {
			if err := f(); err != nil {
				logrus.Errorf("%s: failed to execute user task due to: %v", fn, err)
			}
		},
		sched.Periodic(time.Second),
		time.Now(), true)
	return err
}
