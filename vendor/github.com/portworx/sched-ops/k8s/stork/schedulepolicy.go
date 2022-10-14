package stork

import (
	"context"

	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SchedulePolicyOps is an interface to manage SchedulePolicy Object
type SchedulePolicyOps interface {
	// CreateSchedulePolicy creates a SchedulePolicy
	CreateSchedulePolicy(*storkv1alpha1.SchedulePolicy) (*storkv1alpha1.SchedulePolicy, error)
	// GetSchedulePolicy gets the SchedulePolicy
	GetSchedulePolicy(string) (*storkv1alpha1.SchedulePolicy, error)
	// ListSchedulePolicies lists all the SchedulePolicies
	ListSchedulePolicies() (*storkv1alpha1.SchedulePolicyList, error)
	// UpdateSchedulePolicy updates the SchedulePolicy
	UpdateSchedulePolicy(*storkv1alpha1.SchedulePolicy) (*storkv1alpha1.SchedulePolicy, error)
	// DeleteSchedulePolicy deletes the SchedulePolicy
	DeleteSchedulePolicy(string) error
}

// CreateSchedulePolicy creates a SchedulePolicy
func (c *Client) CreateSchedulePolicy(schedulePolicy *storkv1alpha1.SchedulePolicy) (*storkv1alpha1.SchedulePolicy, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().SchedulePolicies().Create(context.TODO(), schedulePolicy, metav1.CreateOptions{})
}

// GetSchedulePolicy gets the SchedulePolicy
func (c *Client) GetSchedulePolicy(name string) (*storkv1alpha1.SchedulePolicy, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().SchedulePolicies().Get(context.TODO(), name, metav1.GetOptions{})
}

// ListSchedulePolicies lists all the SchedulePolicies
func (c *Client) ListSchedulePolicies() (*storkv1alpha1.SchedulePolicyList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().SchedulePolicies().List(context.TODO(), metav1.ListOptions{})
}

// UpdateSchedulePolicy updates the SchedulePolicy
func (c *Client) UpdateSchedulePolicy(schedulePolicy *storkv1alpha1.SchedulePolicy) (*storkv1alpha1.SchedulePolicy, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().SchedulePolicies().Update(context.TODO(), schedulePolicy, metav1.UpdateOptions{})
}

// DeleteSchedulePolicy deletes the SchedulePolicy
func (c *Client) DeleteSchedulePolicy(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().SchedulePolicies().Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}
