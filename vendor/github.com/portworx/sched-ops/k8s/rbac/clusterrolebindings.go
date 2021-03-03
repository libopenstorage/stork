package rbac

import (
	"context"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterRoleBindingOps is an interface to perform operations on ClusterRoleBinding resources.
type ClusterRoleBindingOps interface {
	// GetClusterRoleBinding gets the given cluster role binding
	GetClusterRoleBinding(name string) (*rbacv1.ClusterRoleBinding, error)
	// ListClusterRoleBindings lists the cluster role bindings
	ListClusterRoleBindings() (*rbacv1.ClusterRoleBindingList, error)
	// CreateClusterRoleBinding creates the given cluster role binding
	CreateClusterRoleBinding(role *rbacv1.ClusterRoleBinding) (*rbacv1.ClusterRoleBinding, error)
	// UpdateClusterRoleBinding updates the given cluster role binding
	UpdateClusterRoleBinding(role *rbacv1.ClusterRoleBinding) (*rbacv1.ClusterRoleBinding, error)
	// DeleteClusterRoleBinding deletes the given cluster role binding
	DeleteClusterRoleBinding(roleName string) error
}

// GetClusterRoleBinding gets the given cluster role binding
func (c *Client) GetClusterRoleBinding(name string) (*rbacv1.ClusterRoleBinding, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.rbac.ClusterRoleBindings().Get(context.TODO(), name, metav1.GetOptions{})
}

// ListClusterRoleBindings lists the cluster role bindings
func (c *Client) ListClusterRoleBindings() (*rbacv1.ClusterRoleBindingList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.rbac.ClusterRoleBindings().List(context.TODO(), metav1.ListOptions{})
}

// CreateClusterRoleBinding creates the given cluster role binding
func (c *Client) CreateClusterRoleBinding(binding *rbacv1.ClusterRoleBinding) (*rbacv1.ClusterRoleBinding, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.rbac.ClusterRoleBindings().Create(context.TODO(), binding, metav1.CreateOptions{})
}

// UpdateClusterRoleBinding updates the given cluster role binding
func (c *Client) UpdateClusterRoleBinding(binding *rbacv1.ClusterRoleBinding) (*rbacv1.ClusterRoleBinding, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.rbac.ClusterRoleBindings().Update(context.TODO(), binding, metav1.UpdateOptions{})
}

// DeleteClusterRoleBinding deletes the given cluster role binding
func (c *Client) DeleteClusterRoleBinding(bindingName string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.rbac.ClusterRoleBindings().Delete(context.TODO(), bindingName, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}
