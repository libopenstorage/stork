package task

import (
	"context"
	"errors"
	"fmt"
	"github.com/pure-px/torpedo/pkg/log"

	"strings"
	"time"
)

// ErrTimedOut is returned when an operation times out
// Is this type used anywhere? If not we can get rid off it in favor context.DeadlineExceeded
type ErrTimedOut struct {
	// Reason is the reason for the timeout
	Reason string
}

func (e *ErrTimedOut) Error() string {
	errString := "timed out performing task."
	if len(e.Reason) > 0 {
		errString = fmt.Sprintf("%s, Error was: %s", errString, e.Reason)
	}

	return errString
}

type contextKey string

const (
	TimeoutKey         contextKey = "timeout"
	TimeBeforeRetryKey contextKey = "timeBeforeRetry"
	TestNameKey        contextKey = "testName"
)

// DoRetryWithTimeout performs given task with given timeout and timeBeforeRetry

func DoRetryWithTimeoutWithCtx(t func() (interface{}, bool, error), parentContext context.Context) (interface{}, error) {
	// Use context.Context as a standard go way of timeout and cancellation propagation amount goroutines.
	timeout, ok := parentContext.Value(TimeoutKey).(time.Duration)
	if !ok {
		return nil, errors.New("context does not have timeout")
	}
	timeBeforeRetry, ok := parentContext.Value(TimeBeforeRetryKey).(time.Duration)
	if !ok {
		return nil, errors.New("context does not have retry time")
	}

	ctx, cancel := context.WithTimeout(parentContext, timeout)
	defer cancel()

	resultChan := make(chan interface{})
	errChan := make(chan error)
	errInRetires := make([]string, 0)

	go func() {
		testName, ok := ctx.Value(TestNameKey).(string)
		if ok && len(testName) > 0 {
			log.SetTestName(testName)
		}
		for {
			select {
			case <-ctx.Done():

				if ctx.Err() != nil {
					errChan <- ctx.Err()
				}

				return
			default:
				out, retry, err := t()
				if err != nil {
					if retry {
						errInRetires = append(errInRetires, err.Error())
						log.Infof("DoRetryWithTimeoutWithCtx - Error: {%v}, Next try in [%v], timeout [%v]", err, timeBeforeRetry, timeout)
						time.Sleep(timeBeforeRetry)
					} else {
						errChan <- err
						return
					}
				} else {
					resultChan <- out
					return
				}
			}
		}
	}()

	select {
	case result := <-resultChan:
		return result, nil
	case err := <-errChan:
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, &ErrTimedOut{
				Reason: fmt.Sprintf("DoRetryWithTimeout timed out. Errors generated in retries: {%s}", strings.Join(errInRetires, "}\n{")),
			}
		}

		return nil, err
	}
}
