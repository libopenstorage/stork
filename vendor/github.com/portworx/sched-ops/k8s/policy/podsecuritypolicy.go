package policy

import (
	"context"

	policyv1beta1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodSecurityPolicyOps is an interface to perform k8s Pod Security Policy operations
type PodSecurityPolicyOps interface {
	// CreatePodSecurityPolicy creates the given pod security policy
	CreatePodSecurityPolicy(policy *policyv1beta1.PodSecurityPolicy) (*policyv1beta1.PodSecurityPolicy, error)
	// GetPodSecurityPolicy gets the given pod security policy
	GetPodSecurityPolicy(name string) (*policyv1beta1.PodSecurityPolicy, error)
	// ListPodSecurityPolicies list pods security policies
	ListPodSecurityPolicies() (*policyv1beta1.PodSecurityPolicyList, error)
	// UpdatePodSecurityPolicy updates the give pod security policy
	UpdatePodSecurityPolicy(policy *policyv1beta1.PodSecurityPolicy) (*policyv1beta1.PodSecurityPolicy, error)
	// DeletePodSecurityPolicy deletes the given pod security policy
	DeletePodSecurityPolicy(name string) error
}

// CreatePodSecurityPolicy creates the given pod security policy
func (c *Client) CreatePodSecurityPolicy(policy *policyv1beta1.PodSecurityPolicy) (*policyv1beta1.PodSecurityPolicy, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.policy.PodSecurityPolicies().Create(context.TODO(), policy, metav1.CreateOptions{})
}

// GetPodSecurityPolicy gets the given pod security policy
func (c *Client) GetPodSecurityPolicy(name string) (*policyv1beta1.PodSecurityPolicy, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.policy.PodSecurityPolicies().Get(context.TODO(), name, metav1.GetOptions{})
}

// ListPodSecurityPolicies gets the given pod security policy
func (c *Client) ListPodSecurityPolicies() (*policyv1beta1.PodSecurityPolicyList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.policy.PodSecurityPolicies().List(context.TODO(), metav1.ListOptions{})
}

// UpdatePodSecurityPolicy updates the give pod security policy
func (c *Client) UpdatePodSecurityPolicy(policy *policyv1beta1.PodSecurityPolicy) (*policyv1beta1.PodSecurityPolicy, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.policy.PodSecurityPolicies().Update(context.TODO(), policy, metav1.UpdateOptions{})
}

// DeletePodSecurityPolicy deletes the given pod security policy
func (c *Client) DeletePodSecurityPolicy(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.policy.PodSecurityPolicies().Delete(context.TODO(), name, metav1.DeleteOptions{})
}
