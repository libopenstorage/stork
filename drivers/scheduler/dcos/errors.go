package dcos

import (
	"fmt"

	"github.com/portworx/torpedo/drivers/scheduler/spec"
)

// ErrFailedToScheduleApp error type for failing to scheduler an app
type ErrFailedToScheduleApp struct {
	// App is the app that failed to schedule
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToScheduleApp) Error() string {
	return fmt.Sprintf("Failed to schedule app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToValidateApp error type for failing to validate an app
type ErrFailedToValidateApp struct {
	// App is the app whose validation failed
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToValidateApp) Error() string {
	return fmt.Sprintf("Failed to validate app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToDestroyApp error type for failing to destroy an app
type ErrFailedToDestroyApp struct {
	// App is the app that failed to destroy
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToDestroyApp) Error() string {
	return fmt.Sprintf("Failed to destory app: %v due to err: %v", e.App.Key, e.Cause)
}

// ErrFailedToValidateAppDestroy error type for failing to validate destory of an app
type ErrFailedToValidateAppDestroy struct {
	// App is the app that failed to destroy
	App *spec.AppSpec
	// Cause is the underlying cause of the error
	Cause string
}

func (e *ErrFailedToValidateAppDestroy) Error() string {
	return fmt.Sprintf("Failed to validate destroy of app: %v due to err: %v", e.App.Key, e.Cause)
}
