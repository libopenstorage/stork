package batch

import (
	"context"
	"fmt"
	"time"

	schederrors "github.com/portworx/sched-ops/k8s/errors"
	v1beta1 "k8s.io/api/batch/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CronV1beta1Ops is an interface to perform kubernetes related operations on the cronjob resources.
type CronV1beta1Ops interface {
	// CreateCronJobV1beta1 creates the given cronJob
	CreateCronJobV1beta1(cronJob *v1beta1.CronJob) (*v1beta1.CronJob, error)
	// UpdateCronJobV1beta1 updates the given cronJob
	UpdateCronJobV1beta1(cronJob *v1beta1.CronJob) (*v1beta1.CronJob, error)
	// GetCronJobV1beta1 returns the cronJob given name and namespace
	GetCronJobV1beta1(name, namespace string) (*v1beta1.CronJob, error)
	// DeleteCronJobV1beta1 deletes the given cronJob
	DeleteCronJobV1beta1(name, namespace string) error
	// ValidateCronJobV1beta1 validates the given cronJob
	ValidateCronJobV1beta1(cronJob *v1beta1.CronJob, timeout, retryInterval time.Duration) error
	// ListCronJobsV1beta1 list cronjobs in given namespace
	ListCronJobsV1beta1(namespace string, filterOptions metav1.ListOptions) (*v1beta1.CronJobList, error)
}

// CreateCronJobV1beta1 creates the given cronJob
func (c *Client) CreateCronJobV1beta1(cronJob *v1beta1.CronJob) (*v1beta1.CronJob, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	ns := cronJob.Namespace
	if len(ns) == 0 {
		ns = NamespaceDefault
	}

	return c.batchv1beta1.CronJobs(ns).Create(context.TODO(), cronJob, metav1.CreateOptions{})
}

// UpdateCronJobV1beta1 updates the given cronJob
func (c *Client) UpdateCronJobV1beta1(cronJob *v1beta1.CronJob) (*v1beta1.CronJob, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.batchv1beta1.CronJobs(cronJob.Namespace).Update(context.TODO(), cronJob, metav1.UpdateOptions{})
}

// GetCronJobV1beta1 returns the cronJob given name and namespace
func (c *Client) GetCronJobV1beta1(name, namespace string) (*v1beta1.CronJob, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.batchv1beta1.CronJobs(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// DeleteCronJobV1beta1 deletes the given cronJob
func (c *Client) DeleteCronJobV1beta1(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.batchv1beta1.CronJobs(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}

// ValidateCronJobV1beta1 validates the given cronJob
func (c *Client) ValidateCronJobV1beta1(cronJob *v1beta1.CronJob, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}

	result, err := c.batchv1beta1.CronJobs(cronJob.Namespace).Get(context.TODO(), cronJob.Name, metav1.GetOptions{})
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

// ListCronJobsV1beta1 returns the cronJobs in given namespace
func (c *Client) ListCronJobsV1beta1(namespace string, filterOptions metav1.ListOptions) (*v1beta1.CronJobList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.batchv1beta1.CronJobs(namespace).List(context.TODO(), filterOptions)
}
