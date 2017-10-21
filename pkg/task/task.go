package task

import (
	"errors"
	"time"

	"github.com/Sirupsen/logrus"
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
		count := 0
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

				logrus.Infof("%v. Retry count: %v Next retry in: %v", err, count, timeBeforeRetry)
				time.Sleep(timeBeforeRetry)
			}

			count++
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
