package rbac

import (
	"context"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RoleBindingOps is an interface to perform operations on RoleBinding resources.
type RoleBindingOps interface {
	// CreateRoleBinding creates the given role binding
	CreateRoleBinding(role *rbacv1.RoleBinding) (*rbacv1.RoleBinding, error)
	// UpdateRoleBinding updates the given role binding
	UpdateRoleBinding(role *rbacv1.RoleBinding) (*rbacv1.RoleBinding, error)
	// GetRoleBinding gets the given role binding
	GetRoleBinding(name, namespace string) (*rbacv1.RoleBinding, error)
	// DeleteRoleBinding deletes the given role binding
	DeleteRoleBinding(name, namespace string) error
}

// CreateRoleBinding creates the given role binding
func (c *Client) CreateRoleBinding(binding *rbacv1.RoleBinding) (*rbacv1.RoleBinding, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.rbac.RoleBindings(binding.Namespace).Create(context.TODO(), binding, metav1.CreateOptions{})
}

// UpdateRoleBinding updates the given role binding
func (c *Client) UpdateRoleBinding(binding *rbacv1.RoleBinding) (*rbacv1.RoleBinding, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.rbac.RoleBindings(binding.Namespace).Update(context.TODO(), binding, metav1.UpdateOptions{})
}

// GetRoleBinding gets the given role binding
func (c *Client) GetRoleBinding(name, namespace string) (*rbacv1.RoleBinding, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.rbac.RoleBindings(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// DeleteRoleBinding deletes the given role binding
func (c *Client) DeleteRoleBinding(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.rbac.RoleBindings(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}
