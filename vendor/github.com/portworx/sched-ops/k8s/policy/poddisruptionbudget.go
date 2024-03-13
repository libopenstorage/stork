package policy

import (
	"context"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodDisruptionBudgetOps is an interface to perform k8s Pod Disruption Budget operations
type PodDisruptionBudgetOps interface {
	// CreatePodDisruptionBudget creates the given pod disruption budget
	CreatePodDisruptionBudget(policy *policyv1.PodDisruptionBudget) (*policyv1.PodDisruptionBudget, error)
	// GetPodDisruptionBudget gets the given pod disruption budget
	GetPodDisruptionBudget(name, namespace string) (*policyv1.PodDisruptionBudget, error)
	// ListPodDisruptionBudget lists the pod disruption budgets
	ListPodDisruptionBudget(namespace string) (*policyv1.PodDisruptionBudgetList, error)
	// UpdatePodDisruptionBudget updates the given pod disruption budget
	UpdatePodDisruptionBudget(policy *policyv1.PodDisruptionBudget) (*policyv1.PodDisruptionBudget, error)
	// DeletePodDisruptionBudget deletes the given pod disruption budget
	DeletePodDisruptionBudget(name, namespace string) error
}

// CreatePodDisruptionBudget creates the given pod disruption budget
func (c *Client) CreatePodDisruptionBudget(podDisruptionBudget *policyv1.PodDisruptionBudget) (*policyv1.PodDisruptionBudget, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.client.PolicyV1().PodDisruptionBudgets(podDisruptionBudget.Namespace).Create(context.TODO(), podDisruptionBudget, metav1.CreateOptions{})
}

// GetPodDisruptionBudget gets the given pod disruption budget
func (c *Client) GetPodDisruptionBudget(name, namespace string) (*policyv1.PodDisruptionBudget, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.client.PolicyV1().PodDisruptionBudgets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// ListPodDisruptionBudget gets the given pod disruption budget
func (c *Client) ListPodDisruptionBudget(namespace string) (*policyv1.PodDisruptionBudgetList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.client.PolicyV1().PodDisruptionBudgets(namespace).List(context.TODO(), metav1.ListOptions{})
}

// UpdatePodDisruptionBudget updates the given pod disruption budget
func (c *Client) UpdatePodDisruptionBudget(podDisruptionBudget *policyv1.PodDisruptionBudget) (*policyv1.PodDisruptionBudget, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.client.PolicyV1().PodDisruptionBudgets(podDisruptionBudget.Namespace).Update(context.TODO(), podDisruptionBudget, metav1.UpdateOptions{})
}

// DeletePodDisruptionBudget deletes the given pod disruption budget
func (c *Client) DeletePodDisruptionBudget(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.client.PolicyV1().PodDisruptionBudgets(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}
