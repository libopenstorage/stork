package rbac

import (
	"context"

	rbac_v1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RoleOps is an interface to perform operations on role resources.
type RoleOps interface {
	// CreateRole creates the given role
	CreateRole(role *rbac_v1.Role) (*rbac_v1.Role, error)
	// UpdateRole updates the given role
	UpdateRole(role *rbac_v1.Role) (*rbac_v1.Role, error)
	// GetRole gets the given role
	GetRole(name, namespace string) (*rbac_v1.Role, error)
	// DeleteRole deletes the given role
	DeleteRole(name, namespace string) error
	// ListRoles returns the list of roles
	ListRoles(namespace string, filterOptions metav1.ListOptions) (*rbac_v1.RoleList, error)
}

// CreateRole creates the given role
func (c *Client) CreateRole(role *rbac_v1.Role) (*rbac_v1.Role, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.rbac.Roles(role.Namespace).Create(context.TODO(), role, metav1.CreateOptions{})
}

// UpdateRole updates the given role
func (c *Client) UpdateRole(role *rbac_v1.Role) (*rbac_v1.Role, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.rbac.Roles(role.Namespace).Update(context.TODO(), role, metav1.UpdateOptions{})
}

// GetRole gets the given role
func (c *Client) GetRole(name, namespace string) (*rbac_v1.Role, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.rbac.Roles(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// DeleteRole deletes the given role
func (c *Client) DeleteRole(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.rbac.Roles(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ListRoles returns the list of roles
func (c *Client) ListRoles(namespace string, filterOptions metav1.ListOptions) (*rbac_v1.RoleList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.rbac.Roles(namespace).List(context.TODO(), filterOptions)
}
