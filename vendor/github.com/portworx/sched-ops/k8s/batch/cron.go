package batch

import (
	"context"
	"fmt"
	"time"

	schederrors "github.com/portworx/sched-ops/k8s/errors"
	v1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CronOps is an interface to perform kubernetes related operations on the crd resources.
type CronOps interface {
	// CreateCronJob creates the given cronJob
	CreateCronJob(cronJob *v1.CronJob) (*v1.CronJob, error)
	// UpdateCronJob updates the given cronJob
	UpdateCronJob(cronJob *v1.CronJob) (*v1.CronJob, error)
	// GetCronJob returns the cronJob given name and namespace
	GetCronJob(name, namespace string) (*v1.CronJob, error)
	// DeleteCronJob deletes the given cronJob
	DeleteCronJob(name, namespace string) error
	// ValidateCronJob validates the given cronJob
	ValidateCronJob(cronJob *v1.CronJob, timeout, retryInterval time.Duration) error
	// ListCronJobs list cronjobs in given namespace
	ListCronJobs(namespace string, filterOptions metav1.ListOptions) (*v1.CronJobList, error)
}

var NamespaceDefault = "default"

// CreateCronJob creates the given cronJob
func (c *Client) CreateCronJob(cronJob *v1.CronJob) (*v1.CronJob, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	ns := cronJob.Namespace
	if len(ns) == 0 {
		ns = NamespaceDefault
	}

	return c.batch.CronJobs(ns).Create(context.TODO(), cronJob, metav1.CreateOptions{})
}

// UpdateCronJob updates the given cronJob
func (c *Client) UpdateCronJob(cronJob *v1.CronJob) (*v1.CronJob, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.batch.CronJobs(cronJob.Namespace).Update(context.TODO(), cronJob, metav1.UpdateOptions{})
}

// GetCronJob returns the cronJob given name and namespace
func (c *Client) GetCronJob(name, namespace string) (*v1.CronJob, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.batch.CronJobs(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// DeleteCronJob deletes the given cronJob
func (c *Client) DeleteCronJob(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.batch.CronJobs(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}

// ValidateCronJob validates the given cronJob
func (c *Client) ValidateCronJob(cronJob *v1.CronJob, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}

	result, err := c.batch.CronJobs(cronJob.Namespace).Get(context.TODO(), cronJob.Name, metav1.GetOptions{})
	if result == nil {
		return err
	}
	if result.Status.LastScheduleTime.IsZero() {
		return &schederrors.ErrFailedToExecCronJob{
			Name:  result.Name,
			Cause: fmt.Sprintf("Cron job %s was not executed after %v", result.Name, timeout),
		}
	}
	return nil
}

// ListCronJobs returns the cronJobs in given namespace
func (c *Client) ListCronJobs(namespace string, filterOptions metav1.ListOptions) (*v1.CronJobList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.batch.CronJobs(namespace).List(context.TODO(), filterOptions)
}
