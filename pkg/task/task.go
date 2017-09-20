package task

import (
	"errors"
	"time"
)

//TODO: export the type: type Task func() (string, error)

// ErrTimedOut is returned when an operation times out
var ErrTimedOut = errors.New("timed out performing task")

// DoRetryWithTimeout performs given task with given timeout and timeBeforeRetry
func DoRetryWithTimeout(t func() (interface{}, error), timeout, timeBeforeRetry time.Duration) (interface{}, error) {
	done := make(chan bool, 1)
	quit := make(chan bool, 1)
	var out interface{}
	var err error

	go func() {
		for {
			select {
			case q := <-quit:
				if q {
					return
				}

			default:
				out, err = t()
				if err == nil {
					done <- true
					return
				}

				time.Sleep(timeBeforeRetry)
			}
		}
	}()

	select {
	case <-done:
		return out, nil
	case <-time.After(timeout):
		quit <- true
		return out, ErrTimedOut
	}
}
