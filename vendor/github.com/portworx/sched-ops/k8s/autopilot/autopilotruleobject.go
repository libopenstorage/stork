package autopilot

import (
	autv1alpha1 "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RuleObjectOps is an interface to perform k8s AutopilotRuleObjects operations
type RuleObjectOps interface {
	// CreateAutopilotRuleObject creates the AutopilotRuleObject object
	CreateAutopilotRuleObject(*autv1alpha1.AutopilotRuleObject) (*autv1alpha1.AutopilotRuleObject, error)
	// GetAutopilotRuleObject gets the AutopilotRuleObject for the provided name
	GetAutopilotRuleObject(namespace, name string) (*autv1alpha1.AutopilotRuleObject, error)
	// UpdateAutopilotRuleObject updates the AutopilotRuleObject
	UpdateAutopilotRuleObject(namespace string, object *autv1alpha1.AutopilotRuleObject) (*autv1alpha1.AutopilotRuleObject, error)
	// DeleteAutopilotRuleObject deletes the AutopilotRuleObject of the given name
	DeleteAutopilotRuleObject(namespace, name string) error
	// ListAutopilotRules lists AutopilotRulesObjects
	ListAutopilotRuleObjects(namespace string) (*autv1alpha1.AutopilotRuleObjectList, error)
}

// CreateAutopilotRuleObject creates the AutopilotRuleObject object
func (c *Client) CreateAutopilotRuleObject(ruleObject *autv1alpha1.AutopilotRuleObject) (*autv1alpha1.AutopilotRuleObject, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.autopilot.AutopilotV1alpha1().AutopilotRuleObjects(ruleObject.Namespace).Create(ruleObject)
}

// GetAutopilotRuleObject gets the AutopilotRuleObject for the provided name
func (c *Client) GetAutopilotRuleObject(namespace, name string) (*autv1alpha1.AutopilotRuleObject, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.autopilot.AutopilotV1alpha1().AutopilotRuleObjects(namespace).Get(name, metav1.GetOptions{})
}

// UpdateAutopilotRuleObject updates the AutopilotRuleObject
func (c *Client) UpdateAutopilotRuleObject(namespace string, ruleObject *autv1alpha1.AutopilotRuleObject) (*autv1alpha1.AutopilotRuleObject, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.autopilot.AutopilotV1alpha1().AutopilotRuleObjects(namespace).Update(ruleObject)
}

// DeleteAutopilotRuleObject deletes the AutopilotRuleObject of the given name
func (c *Client) DeleteAutopilotRuleObject(namespace, name string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.autopilot.AutopilotV1alpha1().AutopilotRuleObjects(namespace).Delete(name, &metav1.DeleteOptions{})
}

// ListAutopilotRuleObjects lists AutopilotRuleObjects
func (c *Client) ListAutopilotRuleObjects(namespace string) (*autv1alpha1.AutopilotRuleObjectList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.autopilot.AutopilotV1alpha1().AutopilotRuleObjects(namespace).List(metav1.ListOptions{})
}
