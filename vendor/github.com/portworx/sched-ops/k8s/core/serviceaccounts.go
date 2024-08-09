package core

import (
	"context"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ServiceAccountOps is an interface to perform operations on role resources.
type ServiceAccountOps interface {
	// CreateServiceAccount creates the given service account
	CreateServiceAccount(account *corev1.ServiceAccount) (*corev1.ServiceAccount, error)
	// GetServiceAccount gets the given service account
	GetServiceAccount(name, namespace string) (*corev1.ServiceAccount, error)
	// UpdateServiceAccount updates the given service account
	UpdateServiceAccount(account *corev1.ServiceAccount) (*corev1.ServiceAccount, error)
	// DeleteServiceAccount deletes the given service account
	DeleteServiceAccount(accountName, namespace string) error
	// ListServiceAccount in given namespace
	ListServiceAccount(namespace string, opts metav1.ListOptions) (*corev1.ServiceAccountList, error)
	// CreateToken creates a token associated with a serviceaccount through a tokenRequest
	CreateToken(name, namespace string, tokenRequest *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error)
}

// CreateServiceAccount creates the given service account
func (c *Client) CreateServiceAccount(account *corev1.ServiceAccount) (*corev1.ServiceAccount, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.kubernetes.CoreV1().ServiceAccounts(account.Namespace).Create(context.TODO(), account, metav1.CreateOptions{})
}

// GetServiceAccount gets the given service account
func (c *Client) GetServiceAccount(name, namespace string) (*corev1.ServiceAccount, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.kubernetes.CoreV1().ServiceAccounts(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// ListServiceAccount in given namespace
func (c *Client) ListServiceAccount(namespace string, opts metav1.ListOptions) (*corev1.ServiceAccountList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.kubernetes.CoreV1().ServiceAccounts(namespace).List(context.TODO(), opts)
}

// UpdateServiceAccount updates the given service account
func (c *Client) UpdateServiceAccount(account *corev1.ServiceAccount) (*corev1.ServiceAccount, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.kubernetes.CoreV1().ServiceAccounts(account.Namespace).Update(context.TODO(), account, metav1.UpdateOptions{})
}

// DeleteServiceAccount deletes the given service account
func (c *Client) DeleteServiceAccount(accountName, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.kubernetes.CoreV1().ServiceAccounts(namespace).Delete(context.TODO(), accountName, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// CreateToken creates the server's representation of the tokenRequest associated with a service account
func (c *Client) CreateToken(name, namespace string, tokenRequest *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.kubernetes.CoreV1().ServiceAccounts(namespace).CreateToken(context.TODO(), name, tokenRequest, metav1.CreateOptions{})
}
