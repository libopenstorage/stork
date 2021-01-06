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

// ApplicationBackupRestoreOps is an interface to perfrom k8s Application Backup
// and Restore operations
type ApplicationBackupRestoreOps interface {
	// CreateApplicationBackup creates the ApplicationBackup
	CreateApplicationBackup(*storkv1alpha1.ApplicationBackup) (*storkv1alpha1.ApplicationBackup, error)
	// GetApplicationBackup gets the ApplicationBackup
	GetApplicationBackup(string, string) (*storkv1alpha1.ApplicationBackup, error)
	// ListApplicationBackups lists all the ApplicationBackups
	ListApplicationBackups(string) (*storkv1alpha1.ApplicationBackupList, error)
	// UpdateApplicationBackup updates the ApplicationBackup
	UpdateApplicationBackup(*storkv1alpha1.ApplicationBackup) (*storkv1alpha1.ApplicationBackup, error)
	// DeleteApplicationBackup deletes the ApplicationBackup
	DeleteApplicationBackup(string, string) error
	// ValidateApplicationBackup validates the ApplicationBackup
	ValidateApplicationBackup(string, string, time.Duration, time.Duration) error
	// WatchApplicationBackup watch the ApplicationBackup
	WatchApplicationBackup(namespace string, fn WatchFunc, listOptions metav1.ListOptions) error
	// CreateApplicationRestore creates the ApplicationRestore
	CreateApplicationRestore(*storkv1alpha1.ApplicationRestore) (*storkv1alpha1.ApplicationRestore, error)
	// GetApplicationRestore gets the ApplicationRestore
	GetApplicationRestore(string, string) (*storkv1alpha1.ApplicationRestore, error)
	// ListApplicationRestores lists all the ApplicationRestores
	ListApplicationRestores(string) (*storkv1alpha1.ApplicationRestoreList, error)
	// UpdateApplicationRestore updates the ApplicationRestore
	UpdateApplicationRestore(*storkv1alpha1.ApplicationRestore) (*storkv1alpha1.ApplicationRestore, error)
	// DeleteApplicationRestore deletes the ApplicationRestore
	DeleteApplicationRestore(string, string) error
	// ValidateApplicationRestore validates the ApplicationRestore
	ValidateApplicationRestore(string, string, time.Duration, time.Duration) error
	// WatchApplicationRestore watch the ApplicationRestore
	WatchApplicationRestore(namespace string, fn WatchFunc, listOptions metav1.ListOptions) error
	// GetApplicationBackupSchedule gets the ApplicationBackupSchedule
	GetApplicationBackupSchedule(string, string) (*storkv1alpha1.ApplicationBackupSchedule, error)
	// CreateApplicationBackupSchedule creates an ApplicationBackupSchedule
	CreateApplicationBackupSchedule(*storkv1alpha1.ApplicationBackupSchedule) (*storkv1alpha1.ApplicationBackupSchedule, error)
	// UpdateApplicationBackupSchedule updates the ApplicationBackupSchedule
	UpdateApplicationBackupSchedule(*storkv1alpha1.ApplicationBackupSchedule) (*storkv1alpha1.ApplicationBackupSchedule, error)
	// ListApplicationBackupSchedules lists all the ApplicationBackupSchedules
	ListApplicationBackupSchedules(string) (*storkv1alpha1.ApplicationBackupScheduleList, error)
	// DeleteApplicationBackupSchedule deletes the ApplicationBackupSchedule
	DeleteApplicationBackupSchedule(string, string) error
	// ValidateApplicationBackupSchedule validates the given ApplicationBackupSchedule. It checks the status of each of
	// the backups triggered for this schedule and returns a map of successfull backups. The key of the
	// map will be the schedule type and value will be list of backups for that schedule type.
	// The caller is expected to validate if the returned map has all backups expected at that point of time
	ValidateApplicationBackupSchedule(string, string, int, time.Duration, time.Duration) (
		map[storkv1alpha1.SchedulePolicyType][]*storkv1alpha1.ScheduledApplicationBackupStatus, error)
}

// CreateApplicationBackup creates the ApplicationBackup
func (c *Client) CreateApplicationBackup(backup *storkv1alpha1.ApplicationBackup) (*storkv1alpha1.ApplicationBackup, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationBackups(backup.Namespace).Create(backup)
}

// GetApplicationBackup gets the ApplicationBackup
func (c *Client) GetApplicationBackup(name string, namespace string) (*storkv1alpha1.ApplicationBackup, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationBackups(namespace).Get(name, metav1.GetOptions{})
}

// ListApplicationBackups lists all the ApplicationBackups
func (c *Client) ListApplicationBackups(namespace string) (*storkv1alpha1.ApplicationBackupList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationBackups(namespace).List(metav1.ListOptions{})
}

// DeleteApplicationBackup deletes the ApplicationBackup
func (c *Client) DeleteApplicationBackup(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().ApplicationBackups(namespace).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// UpdateApplicationBackup updates the ApplicationBackup
func (c *Client) UpdateApplicationBackup(backup *storkv1alpha1.ApplicationBackup) (*storkv1alpha1.ApplicationBackup, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationBackups(backup.Namespace).Update(backup)
}

// ValidateApplicationBackup validates the ApplicationBackup
func (c *Client) ValidateApplicationBackup(name, namespace string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		applicationbackup, err := c.GetApplicationBackup(name, namespace)
		if err != nil {
			return "", true, err
		}

		if applicationbackup.Status.Status == storkv1alpha1.ApplicationBackupStatusSuccessful {
			return "", false, nil
		}

		return "", true, &errors.ErrFailedToValidateCustomSpec{
			Name:  applicationbackup.Name,
			Cause: fmt.Sprintf("Application backup failed . Error: %v .Expected status: %v Actual status: %v", err, storkv1alpha1.ApplicationBackupStatusSuccessful, applicationbackup.Status.Status),
			Type:  applicationbackup,
		}

	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}

// GetApplicationRestore gets the ApplicationRestore
func (c *Client) GetApplicationRestore(name string, namespace string) (*storkv1alpha1.ApplicationRestore, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationRestores(namespace).Get(name, metav1.GetOptions{})
}

// ListApplicationRestores lists all the ApplicationRestores
func (c *Client) ListApplicationRestores(namespace string) (*storkv1alpha1.ApplicationRestoreList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationRestores(namespace).List(metav1.ListOptions{})
}

// CreateApplicationRestore creates the ApplicationRestore
func (c *Client) CreateApplicationRestore(restore *storkv1alpha1.ApplicationRestore) (*storkv1alpha1.ApplicationRestore, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationRestores(restore.Namespace).Create(restore)
}

// DeleteApplicationRestore deletes the ApplicationRestore
func (c *Client) DeleteApplicationRestore(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().ApplicationRestores(namespace).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ValidateApplicationRestore validates the ApplicationRestore
func (c *Client) ValidateApplicationRestore(name, namespace string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		applicationrestore, err := c.stork.StorkV1alpha1().ApplicationRestores(namespace).Get(name, metav1.GetOptions{})
		if err != nil {
			return "", true, err
		}

		if applicationrestore.Status.Status == storkv1alpha1.ApplicationRestoreStatusSuccessful ||
			applicationrestore.Status.Status == storkv1alpha1.ApplicationRestoreStatusPartialSuccess {
			return "", false, nil
		}
		return "", true, &errors.ErrFailedToValidateCustomSpec{
			Name: applicationrestore.Name,
			Cause: fmt.Sprintf("Application restore failed . Error: %v .Expected status: %v/%v Actual status: %v",
				err,
				storkv1alpha1.ApplicationRestoreStatusSuccessful,
				storkv1alpha1.ApplicationRestoreStatusPartialSuccess,
				applicationrestore.Status.Status),
			Type: applicationrestore,
		}
	}
	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}

// UpdateApplicationRestore updates the ApplicationRestore
func (c *Client) UpdateApplicationRestore(restore *storkv1alpha1.ApplicationRestore) (*storkv1alpha1.ApplicationRestore, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationRestores(restore.Namespace).Update(restore)
}

// GetApplicationBackupSchedule gets the ApplicationBackupSchedule
func (c *Client) GetApplicationBackupSchedule(name string, namespace string) (*storkv1alpha1.ApplicationBackupSchedule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationBackupSchedules(namespace).Get(name, metav1.GetOptions{})
}

// ListApplicationBackupSchedules lists all the ApplicationBackupSchedules
func (c *Client) ListApplicationBackupSchedules(namespace string) (*storkv1alpha1.ApplicationBackupScheduleList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationBackupSchedules(namespace).List(metav1.ListOptions{})
}

// CreateApplicationBackupSchedule creates an ApplicationBackupSchedule
func (c *Client) CreateApplicationBackupSchedule(applicationBackupSchedule *storkv1alpha1.ApplicationBackupSchedule) (*storkv1alpha1.ApplicationBackupSchedule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationBackupSchedules(applicationBackupSchedule.Namespace).Create(applicationBackupSchedule)
}

// UpdateApplicationBackupSchedule updates the ApplicationBackupSchedule
func (c *Client) UpdateApplicationBackupSchedule(applicationBackupSchedule *storkv1alpha1.ApplicationBackupSchedule) (*storkv1alpha1.ApplicationBackupSchedule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ApplicationBackupSchedules(applicationBackupSchedule.Namespace).Update(applicationBackupSchedule)
}

// DeleteApplicationBackupSchedule deletes the ApplicationBackupSchedule
func (c *Client) DeleteApplicationBackupSchedule(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().ApplicationBackupSchedules(namespace).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ValidateApplicationBackupSchedule validates the given ApplicationBackupSchedule. It checks the status of each of
// the backups triggered for this schedule and returns a map of successfull backups. The key of the
// map will be the schedule type and value will be list of backups for that schedule type.
// The caller is expected to validate if the returned map has all backups expected at that point of time
func (c *Client) ValidateApplicationBackupSchedule(name string, namespace string, expectedSuccess int, timeout, retryInterval time.Duration) (
	map[storkv1alpha1.SchedulePolicyType][]*storkv1alpha1.ScheduledApplicationBackupStatus, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	t := func() (interface{}, bool, error) {
		resp, err := c.GetApplicationBackupSchedule(name, namespace)
		if err != nil {
			return nil, true, err
		}

		if len(resp.Status.Items) == 0 {
			return nil, true, &errors.ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("0 backups have yet run for the backup schedule"),
				Type:  resp,
			}
		}

		failedBackups := make([]string, 0)
		pendingBackups := make([]string, 0)
		success := 0
		for _, backupStatuses := range resp.Status.Items {
			// The check below assumes that the status will not have a failed
			// backup if the last one succeeded so just get the last status
			if len(backupStatuses) > 0 {
				status := backupStatuses[len(backupStatuses)-1]
				if status == nil {
					return nil, true, &errors.ErrFailedToValidateCustomSpec{
						Name:  name,
						Cause: "ApplicationBackupSchedule has an empty backup in it's most recent status",
						Type:  resp,
					}
				}

				if status.Status == storkv1alpha1.ApplicationBackupStatusSuccessful {
					success++
					continue
				}

				if status.Status == storkv1alpha1.ApplicationBackupStatusFailed {
					failedBackups = append(failedBackups,
						fmt.Sprintf("backup: %s failed. status: %v", status.Name, status.Status))
				} else {
					pendingBackups = append(pendingBackups,
						fmt.Sprintf("backup: %s is not done. status: %v", status.Name, status.Status))
				}
			}
		}

		if len(failedBackups) > 0 {
			return nil, false, &errors.ErrFailedToValidateCustomSpec{
				Name: name,
				Cause: fmt.Sprintf("ApplicationBackupSchedule failed as one or more backups have failed. %s",
					failedBackups),
				Type: resp,
			}
		}

		if success == expectedSuccess {
			return resp.Status.Items, false, nil
		}

		if len(pendingBackups) > 0 {
			return nil, true, &errors.ErrFailedToValidateCustomSpec{
				Name: name,
				Cause: fmt.Sprintf("ApplicationBackupSchedule has certain backups pending: %s",
					pendingBackups),
				Type: resp,
			}
		}

		return resp.Status.Items, false, nil
	}

	ret, err := task.DoRetryWithTimeout(t, timeout, retryInterval)
	if err != nil {
		return nil, err
	}

	backups, ok := ret.(map[storkv1alpha1.SchedulePolicyType][]*storkv1alpha1.ScheduledApplicationBackupStatus)
	if !ok {
		return nil, fmt.Errorf("invalid type when checking backup schedules: %v", backups)
	}

	return backups, nil
}

// WatchApplicationBackup sets up a watcher that listens for changes on application backups
func (c *Client) WatchApplicationBackup(namespace string, fn WatchFunc, listOptions metav1.ListOptions) error {
	if err := c.initClient(); err != nil {
		return err
	}

	listOptions.Watch = true
	watchInterface, err := c.stork.StorkV1alpha1().ApplicationBackups(namespace).Watch(listOptions)
	if err != nil {
		logrus.WithError(err).Error("error invoking the watch api for application backups")
		return err
	}

	// fire off watch function
	go c.handleWatch(watchInterface, &storkv1alpha1.ApplicationBackup{}, "", fn, listOptions)
	return nil
}

// WatchApplicationRestore sets up a watcher that listens for changes on application restores
func (c *Client) WatchApplicationRestore(namespace string, fn WatchFunc, listOptions metav1.ListOptions) error {
	if err := c.initClient(); err != nil {
		return err
	}

	listOptions.Watch = true
	watchInterface, err := c.stork.StorkV1alpha1().ApplicationRestores(namespace).Watch(listOptions)
	if err != nil {
		logrus.WithError(err).Error("error invoking the watch api for application restores")
		return err
	}

	// fire off watch function
	go c.handleWatch(watchInterface, &storkv1alpha1.ApplicationRestore{}, "", fn, listOptions)
	return nil
}
