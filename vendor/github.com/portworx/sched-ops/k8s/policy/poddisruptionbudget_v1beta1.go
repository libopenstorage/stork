package policy

import (
	"context"

	policyv1beta1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodDisruptionBudgetV1Beta1Ops is an interface to perform k8s Pod Disruption Budget operations
type PodDisruptionBudgetV1Beta1Ops interface {
	// CreatePodDisruptionBudgetV1beta1 creates the given pod disruption budget
	CreatePodDisruptionBudgetV1beta1(policy *policyv1beta1.PodDisruptionBudget) (*policyv1beta1.PodDisruptionBudget, error)
	// GetPodDisruptionBudgetV1beta1 gets the given pod disruption budget
	GetPodDisruptionBudgetV1beta1(name, namespace string) (*policyv1beta1.PodDisruptionBudget, error)
	// ListPodDisruptionBudgetV1beta1 lists the pod disruption budgets
	ListPodDisruptionBudgetV1beta1(namespace string) (*policyv1beta1.PodDisruptionBudgetList, error)
	// UpdatePodDisruptionBudgetV1beta1 updates the given pod disruption budget
	UpdatePodDisruptionBudgetV1beta1(policy *policyv1beta1.PodDisruptionBudget) (*policyv1beta1.PodDisruptionBudget, error)
	// DeletePodDisruptionBudgetV1beta1 deletes the given pod disruption budget
	DeletePodDisruptionBudgetV1beta1(name, namespace string) error
}

// CreatePodDisruptionBudgetV1beta1 creates the given pod disruption budget
func (c *Client) CreatePodDisruptionBudgetV1beta1(podDisruptionBudget *policyv1beta1.PodDisruptionBudget) (*policyv1beta1.PodDisruptionBudget, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.client.PolicyV1beta1().PodDisruptionBudgets(podDisruptionBudget.Namespace).Create(context.TODO(), podDisruptionBudget, metav1.CreateOptions{})
}

// GetPodDisruptionBudgetV1beta1 gets the given pod disruption budget
func (c *Client) GetPodDisruptionBudgetV1beta1(name, namespace string) (*policyv1beta1.PodDisruptionBudget, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.client.PolicyV1beta1().PodDisruptionBudgets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// ListPodDisruptionBudgetV1beta1 gets the given pod disruption budget
func (c *Client) ListPodDisruptionBudgetV1beta1(namespace string) (*policyv1beta1.PodDisruptionBudgetList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.client.PolicyV1beta1().PodDisruptionBudgets(namespace).List(context.TODO(), metav1.ListOptions{})
}

// UpdatePodDisruptionBudgetV1beta1 updates the given pod disruption budget
func (c *Client) UpdatePodDisruptionBudgetV1beta1(podDisruptionBudget *policyv1beta1.PodDisruptionBudget) (*policyv1beta1.PodDisruptionBudget, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.client.PolicyV1beta1().PodDisruptionBudgets(podDisruptionBudget.Namespace).Update(context.TODO(), podDisruptionBudget, metav1.UpdateOptions{})
}

// DeletePodDisruptionBudgetV1beta1 deletes the given pod disruption budget
func (c *Client) DeletePodDisruptionBudgetV1beta1(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.client.PolicyV1beta1().PodDisruptionBudgets(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}
