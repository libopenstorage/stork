package autopilot

import (
	autv1alpha1 "github.com/libopenstorage/autopilot-api/pkg/apis/autopilot/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RuleOps is an interface to perform k8s AutopilotRule operations
type RuleOps interface {
	// CreateAutopilotRule creates the AutopilotRule object
	CreateAutopilotRule(*autv1alpha1.AutopilotRule) (*autv1alpha1.AutopilotRule, error)
	// GetAutopilotRule gets the AutopilotRule for the provided name
	GetAutopilotRule(string) (*autv1alpha1.AutopilotRule, error)
	// UpdateAutopilotRule updates the AutopilotRule
	UpdateAutopilotRule(*autv1alpha1.AutopilotRule) (*autv1alpha1.AutopilotRule, error)
	// DeleteAutopilotRule deletes the AutopilotRule of the given name
	DeleteAutopilotRule(string) error
	// ListAutopilotRules lists AutopilotRules
	ListAutopilotRules() (*autv1alpha1.AutopilotRuleList, error)
}

// CreateAutopilotRule creates the AutopilotRule object
func (c *Client) CreateAutopilotRule(rule *autv1alpha1.AutopilotRule) (*autv1alpha1.AutopilotRule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.autopilot.AutopilotV1alpha1().AutopilotRules().Create(rule)
}

// GetAutopilotRule gets the AutopilotRule for the provided name
func (c *Client) GetAutopilotRule(name string) (*autv1alpha1.AutopilotRule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.autopilot.AutopilotV1alpha1().AutopilotRules().Get(name, metav1.GetOptions{})
}

// UpdateAutopilotRule updates the AutopilotRule
func (c *Client) UpdateAutopilotRule(rule *autv1alpha1.AutopilotRule) (*autv1alpha1.AutopilotRule, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.autopilot.AutopilotV1alpha1().AutopilotRules().Update(rule)
}

// DeleteAutopilotRule deletes the AutopilotRule of the given name
func (c *Client) DeleteAutopilotRule(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.autopilot.AutopilotV1alpha1().AutopilotRules().Delete(name, &metav1.DeleteOptions{})
}

// ListAutopilotRules lists AutopilotRules
func (c *Client) ListAutopilotRules() (*autv1alpha1.AutopilotRuleList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.autopilot.AutopilotV1alpha1().AutopilotRules().List(metav1.ListOptions{})
}
