package rbac

import (
	"context"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterRoleOps is an interface to perform operations on ClusterRole resources.
type ClusterRoleOps interface {
	// CreateClusterRole creates the given cluster role
	CreateClusterRole(role *rbacv1.ClusterRole) (*rbacv1.ClusterRole, error)
	// GetClusterRole gets the given cluster role
	GetClusterRole(name string) (*rbacv1.ClusterRole, error)
	// UpdateClusterRole updates the given cluster role
	UpdateClusterRole(role *rbacv1.ClusterRole) (*rbacv1.ClusterRole, error)
	// DeleteClusterRole deletes the given cluster role
	DeleteClusterRole(roleName string) error
}

// CreateClusterRole creates the given cluster role
func (c *Client) CreateClusterRole(role *rbacv1.ClusterRole) (*rbacv1.ClusterRole, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.rbac.ClusterRoles().Create(context.TODO(), role, metav1.CreateOptions{})
}

// GetClusterRole gets the given cluster role
func (c *Client) GetClusterRole(name string) (*rbacv1.ClusterRole, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.rbac.ClusterRoles().Get(context.TODO(), name, metav1.GetOptions{})
}

// UpdateClusterRole updates the given cluster role
func (c *Client) UpdateClusterRole(role *rbacv1.ClusterRole) (*rbacv1.ClusterRole, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.rbac.ClusterRoles().Update(context.TODO(), role, metav1.UpdateOptions{})
}

// DeleteClusterRole deletes the given cluster role
func (c *Client) DeleteClusterRole(roleName string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.rbac.ClusterRoles().Delete(context.TODO(), roleName, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}
