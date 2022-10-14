package autopilot

import (
	autv1alpha1 "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ActionApprovalInterface has methods to work with ActionApproval resources.
type ActionApprovalInterface interface {
	// CreateActionApproval creates the ActionApproval object
	CreateActionApproval(actionApproval *autv1alpha1.ActionApproval) (*autv1alpha1.ActionApproval, error)
	// GetActionApproval gets the ActionApproval for the provided name
	GetActionApproval(namespace, name string) (*autv1alpha1.ActionApproval, error)
	// UpdateActionApproval updates the ActionApproval
	UpdateActionApproval(namespace string, actionApproval *autv1alpha1.ActionApproval) (*autv1alpha1.ActionApproval, error)
	// DeleteActionApproval deletes the ActionApproval of the given name
	DeleteActionApproval(namespace, name string) error
	// ListActionApprovals lists ActionApproval
	ListActionApprovals(namespace string) (*autv1alpha1.ActionApprovalList, error)
}

// CreateActionApproval creates the ActionApproval object
func (c *Client) CreateActionApproval(actionApproval *autv1alpha1.ActionApproval) (*autv1alpha1.ActionApproval, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.autopilot.AutopilotV1alpha1().ActionApprovals(actionApproval.Namespace).Create(actionApproval)
}

// GetActionApproval gets the ActionApproval for the provided name
func (c *Client) GetActionApproval(namespace, name string) (*autv1alpha1.ActionApproval, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.autopilot.AutopilotV1alpha1().ActionApprovals(namespace).Get(name, metav1.GetOptions{})
}

// UpdateActionApproval updates the ActionApproval
func (c *Client) UpdateActionApproval(namespace string, actionApproval *autv1alpha1.ActionApproval) (*autv1alpha1.ActionApproval, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.autopilot.AutopilotV1alpha1().ActionApprovals(namespace).Update(actionApproval)
}

// DeleteActionApproval deletes the ActionApproval of the given name
func (c *Client) DeleteActionApproval(namespace, name string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.autopilot.AutopilotV1alpha1().ActionApprovals(namespace).Delete(name, &metav1.DeleteOptions{})
}

// ListActionApprovals lists ActionApproval
func (c *Client) ListActionApprovals(namespace string) (*autv1alpha1.ActionApprovalList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.autopilot.AutopilotV1alpha1().ActionApprovals(namespace).List(metav1.ListOptions{})
}
