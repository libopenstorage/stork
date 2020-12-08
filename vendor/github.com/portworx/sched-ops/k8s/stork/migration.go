package stork

import (
	"fmt"
	"time"

	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MigrationOps is an interface to perform k8s Migration operations
type MigrationOps interface {
	// CreateMigration creates the Migration
	CreateMigration(*storkv1alpha1.Migration) (*storkv1alpha1.Migration, error)
	// GetMigration gets the Migration
	GetMigration(string, string) (*storkv1alpha1.Migration, error)
	// ListMigrations lists all the Migrations
	ListMigrations(string) (*storkv1alpha1.MigrationList, error)
	// UpdateMigration updates the Migration
	UpdateMigration(*storkv1alpha1.Migration) (*storkv1alpha1.Migration, error)
	// DeleteMigration deletes the Migration
	DeleteMigration(string, string) error
	// ValidateMigration validate the Migration status
	ValidateMigration(string, string, time.Duration, time.Duration) error
	// GetMigrationSchedule gets the MigrationSchedule
	GetMigrationSchedule(string, string) (*storkv1alpha1.MigrationSchedule, error)
	// CreateMigrationSchedule creates a MigrationSchedule
	CreateMigrationSchedule(*storkv1alpha1.MigrationSchedule) (*storkv1alpha1.MigrationSchedule, error)
	// UpdateMigrationSchedule updates the MigrationSchedule
	UpdateMigrationSchedule(*storkv1alpha1.MigrationSchedule) (*storkv1alpha1.MigrationSchedule, error)
	// ListMigrationSchedules lists all the MigrationSchedules
	ListMigrationSchedules(string) (*storkv1alpha1.MigrationScheduleList, error)
	// DeleteMigrationSchedule deletes the MigrationSchedule
	DeleteMigrationSchedule(string, string) error
	// ValidateMigrationSchedule validates the given MigrationSchedule. It checks the status of each of
	// the migrations triggered for this schedule and returns a map of successfull migrations. The key of the
	// map will be the schedule type and value will be list of migrations for that schedule type.
	// The caller is expected to validate if the returned map has all migrations expected at that point of time
	ValidateMigrationSchedule(string, string, time.Duration, time.Duration) (
		map[storkv1alpha1.SchedulePolicyType][]*storkv1alpha1.ScheduledMigrationStatus, error)
	// WatchMigration watch the Migration object
	WatchMigration(namespace string, fn WatchFunc, listOptions metav1.ListOptions) error
}

// GetMigration gets the Migration
func (c *Client) GetMigration(name string, namespace string) (*storkv1alpha1.Migration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().Migrations(namespace).Get(name, metav1.GetOptions{})
}

// ListMigrations lists all the Migrations
func (c *Client) ListMigrations(namespace string) (*storkv1alpha1.MigrationList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().Migrations(namespace).List(metav1.ListOptions{})
}

// CreateMigration creates the Migration
func (c *Client) CreateMigration(migration *storkv1alpha1.Migration) (*storkv1alpha1.Migration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().Migrations(migration.Namespace).Create(migration)
}

// DeleteMigration deletes the Migration
func (c *Client) DeleteMigration(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().Migrations(namespace).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// UpdateMigration updates the Migration
func (c *Client) UpdateMigration(migration *storkv1alpha1.Migration) (*storkv1alpha1.Migration, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().Migrations(migration.Namespace).Update(migration)
}

// ValidateMigration validate the Migration status
func (c *Client) ValidateMigration(name string, namespace string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		resp, err := c.GetMigration(name, namespace)
		if err != nil {
			return "", true, err
		}

		if resp.Status.Status == storkv1alpha1.MigrationStatusSuccessful {
			return "", false, nil
		} else if resp.Status.Status == storkv1alpha1.MigrationStatusFailed {
			return "", false, &errors.ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("Migration Status %v", resp.Status.Status),
				Type:  resp,
			}
		}

		return "", true, &errors.ErrFailedToValidateCustomSpec{
			Name:  name,
			Cause: fmt.Sprintf("Migration Status %v", resp.Status.Status),
			Type:  resp,
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}

	return nil
}

// GetMigrationSchedule gets the MigrationSchedule
func (c *Client) GetMigrationSchedule(name string, namespace string) (*storkv1alpha1.MigrationSchedule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().MigrationSchedules(namespace).Get(name, metav1.GetOptions{})
}

// ListMigrationSchedules lists all the MigrationSchedules
func (c *Client) ListMigrationSchedules(namespace string) (*storkv1alpha1.MigrationScheduleList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().MigrationSchedules(namespace).List(metav1.ListOptions{})
}

// CreateMigrationSchedule creates a MigrationSchedule
func (c *Client) CreateMigrationSchedule(migrationSchedule *storkv1alpha1.MigrationSchedule) (*storkv1alpha1.MigrationSchedule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().MigrationSchedules(migrationSchedule.Namespace).Create(migrationSchedule)
}

// UpdateMigrationSchedule updates the MigrationSchedule
func (c *Client) UpdateMigrationSchedule(migrationSchedule *storkv1alpha1.MigrationSchedule) (*storkv1alpha1.MigrationSchedule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().MigrationSchedules(migrationSchedule.Namespace).Update(migrationSchedule)
}

// DeleteMigrationSchedule deletes the MigrationSchedule
func (c *Client) DeleteMigrationSchedule(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().MigrationSchedules(namespace).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ValidateMigrationSchedule validates the given MigrationSchedule. It checks the status of each of
// the migrations triggered for this schedule and returns a map of successfull migrations. The key of the
// map will be the schedule type and value will be list of migrations for that schedule type.
// The caller is expected to validate if the returned map has all migrations expected at that point of time
func (c *Client) ValidateMigrationSchedule(name string, namespace string, timeout, retryInterval time.Duration) (
	map[storkv1alpha1.SchedulePolicyType][]*storkv1alpha1.ScheduledMigrationStatus, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	t := func() (interface{}, bool, error) {
		resp, err := c.GetMigrationSchedule(name, namespace)
		if err != nil {
			return nil, true, err
		}

		if len(resp.Status.Items) == 0 {
			return nil, true, &errors.ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("0 migrations have yet run for the migration schedule"),
				Type:  resp,
			}
		}

		failedMigrations := make([]string, 0)
		pendingMigrations := make([]string, 0)
		for _, migrationStatuses := range resp.Status.Items {
			// The check below assumes that the status will not have a failed migration if the last one succeeded
			// so just get the last status
			if len(migrationStatuses) > 0 {
				status := migrationStatuses[len(migrationStatuses)-1]
				if status == nil {
					return nil, true, &errors.ErrFailedToValidateCustomSpec{
						Name:  name,
						Cause: "MigrationSchedule has an empty migration in it's most recent status",
						Type:  resp,
					}
				}

				if status.Status == storkv1alpha1.MigrationStatusSuccessful {
					continue
				}

				if status.Status == storkv1alpha1.MigrationStatusFailed {
					failedMigrations = append(failedMigrations,
						fmt.Sprintf("migration: %s failed. status: %v", status.Name, status.Status))
				} else {
					pendingMigrations = append(pendingMigrations,
						fmt.Sprintf("migration: %s is not done. status: %v", status.Name, status.Status))
				}
			}
		}

		if len(failedMigrations) > 0 {
			return nil, false, &errors.ErrFailedToValidateCustomSpec{
				Name: name,
				Cause: fmt.Sprintf("MigrationSchedule failed as one or more migrations have failed. %s",
					failedMigrations),
				Type: resp,
			}
		}

		if len(pendingMigrations) > 0 {
			return nil, true, &errors.ErrFailedToValidateCustomSpec{
				Name: name,
				Cause: fmt.Sprintf("MigrationSchedule has certain migrations pending: %s",
					pendingMigrations),
				Type: resp,
			}
		}

		return resp.Status.Items, false, nil
	}

	ret, err := task.DoRetryWithTimeout(t, timeout, retryInterval)
	if err != nil {
		return nil, err
	}

	migrations, ok := ret.(map[storkv1alpha1.SchedulePolicyType][]*storkv1alpha1.ScheduledMigrationStatus)
	if !ok {
		return nil, fmt.Errorf("invalid type when checking migration schedules: %v", migrations)
	}

	return migrations, nil
}

// WatchMigration sets up a watcher that listens for changes on migration objects
func (c *Client) WatchMigration(namespace string, fn WatchFunc, listOptions metav1.ListOptions) error {
	if err := c.initClient(); err != nil {
		return err
	}

	listOptions.Watch = true
	watchInterface, err := c.stork.StorkV1alpha1().Migrations(namespace).Watch(listOptions)
	if err != nil {
		logrus.WithError(err).Error("error invoking the watch api for migration")
		return err
	}

	// fire off watch function
	go c.handleWatch(watchInterface, &storkv1alpha1.Migration{}, "", fn, listOptions)
	return nil
}
