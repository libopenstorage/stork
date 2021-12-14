package stork

import (
	"context"

	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NamespacedSchedulePolicyOps is an interface to manage NamespacedSchedulePolicy Object
type NamespacedSchedulePolicyOps interface {
	// CreateNamespacedSchedulePolicy creates a NamespacedSchedulePolicy
	CreateNamespacedSchedulePolicy(*storkv1alpha1.NamespacedSchedulePolicy) (*storkv1alpha1.NamespacedSchedulePolicy, error)
	// GetNamespacedSchedulePolicy gets the NamespacedSchedulePolicy
	GetNamespacedSchedulePolicy(string, string) (*storkv1alpha1.NamespacedSchedulePolicy, error)
	// ListNamespacedSchedulePolicies lists all the NamespacedSchedulePolicies
	ListNamespacedSchedulePolicies(namespace string, filterOptions metav1.ListOptions) (*storkv1alpha1.NamespacedSchedulePolicyList, error)
	// UpdateNamespacedSchedulePolicy updates the NamespacedSchedulePolicy
	UpdateNamespacedSchedulePolicy(*storkv1alpha1.NamespacedSchedulePolicy) (*storkv1alpha1.NamespacedSchedulePolicy, error)
	// DeleteNamespacedSchedulePolicy deletes the NamespacedSchedulePolicy
	DeleteNamespacedSchedulePolicy(string, string) error
}

// CreateNamespacedSchedulePolicy creates a NamespacedSchedulePolicy
func (c *Client) CreateNamespacedSchedulePolicy(schedulePolicy *storkv1alpha1.NamespacedSchedulePolicy) (*storkv1alpha1.NamespacedSchedulePolicy, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().NamespacedSchedulePolicies(schedulePolicy.Namespace).Create(context.TODO(), schedulePolicy, metav1.CreateOptions{})
}

// GetNamespacedSchedulePolicy gets the NamespacedSchedulePolicy
func (c *Client) GetNamespacedSchedulePolicy(name string, namespace string) (*storkv1alpha1.NamespacedSchedulePolicy, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().NamespacedSchedulePolicies(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// ListNamespacedSchedulePolicies lists all the NamespacedSchedulePolicies
func (c *Client) ListNamespacedSchedulePolicies(namespace string, filterOptions metav1.ListOptions) (*storkv1alpha1.NamespacedSchedulePolicyList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().NamespacedSchedulePolicies(namespace).List(context.TODO(), filterOptions)
}

// UpdateNamespacedSchedulePolicy updates the NamespacedSchedulePolicy
func (c *Client) UpdateNamespacedSchedulePolicy(schedulePolicy *storkv1alpha1.NamespacedSchedulePolicy) (*storkv1alpha1.NamespacedSchedulePolicy, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().NamespacedSchedulePolicies(schedulePolicy.Namespace).Update(context.TODO(), schedulePolicy, metav1.UpdateOptions{})
}

// DeleteNamespacedSchedulePolicy deletes the NamespacedSchedulePolicy
func (c *Client) DeleteNamespacedSchedulePolicy(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().NamespacedSchedulePolicies(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}
