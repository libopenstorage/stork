package batch

import (
	"fmt"
	"time"

	schederrors "github.com/portworx/sched-ops/k8s/errors"
	v1beta1 "k8s.io/api/batch/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Ops is an interface to perform kubernetes related operations on the crd resources.
type CronOps interface {
	// CreateCronJob creates the given cronJob
	CreateCronJob(cronJob *v1beta1.CronJob) (*v1beta1.CronJob, error)
	// UpdateCronJob creates the given cronJob
	UpdateCronJob(cronJob *v1beta1.CronJob) (*v1beta1.CronJob, error)
	// GetCronJob returns the cronJob given name and namespace
	GetCronJob(name, namespace string) (*v1beta1.CronJob, error)
	// DeleteCronJob deletes the given cronJob
	DeleteCronJob(name, namespace string) error
	// ValidateCronJob validates the given cronJob
	ValidateCronJob(cronJob *v1beta1.CronJob, timeout, retryInterval time.Duration) error
}

var NamespaceDefault = "default"

// CreateCron creates the given cronJob
func (c *Client) CreateCronJob(cronJob *v1beta1.CronJob) (*v1beta1.CronJob, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	ns := cronJob.Namespace
	if len(ns) == 0 {
		ns = NamespaceDefault
	}

	return c.batchv1beta1.CronJobs(ns).Create(cronJob)
}

// UpdateCronJob creates the given cronJob
func (c *Client) UpdateCronJob(cronJob *v1beta1.CronJob) (*v1beta1.CronJob, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.batchv1beta1.CronJobs(cronJob.Namespace).Update(cronJob)
}

// GetCronJob returns the cronJob given name and namespace
func (c *Client) GetCronJob(name, namespace string) (*v1beta1.CronJob, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.batchv1beta1.CronJobs(namespace).Get(name, metav1.GetOptions{})
}

// DeleteCronJob deletes the given cronJob
func (c *Client) DeleteCronJob(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.batchv1beta1.CronJobs(namespace).Delete(name, &metav1.DeleteOptions{})
}

// ValidateCronJob validates the given cronJob
func (c *Client) ValidateCronJob(cronJob *v1beta1.CronJob, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}

	result, err := c.batchv1beta1.CronJobs(cronJob.Namespace).Get(cronJob.Name, metav1.GetOptions{})
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
