package ops

import (
	"context"
	"fmt"
	"time"

	kdmpv1alpha1 "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// VolumeBackupOps is an interface to perform k8s VolumeBackup operations
type VolumeBackupOps interface {
	// CreateVolumeBackup creates the VolumeBackup
	CreateVolumeBackup(ctx context.Context, volumeBackup *kdmpv1alpha1.VolumeBackup) (*kdmpv1alpha1.VolumeBackup, error)
	// GetVolumeBackup gets the VolumeBackup
	GetVolumeBackup(ctx context.Context, name string, namespace string) (*kdmpv1alpha1.VolumeBackup, error)
	// ListVolumeBackups lists all the VolumeBackups
	ListVolumeBackups(ctx context.Context, namespace string) (*kdmpv1alpha1.VolumeBackupList, error)
	// UpdateVolumeBackup updates the VolumeBackup
	UpdateVolumeBackup(ctx context.Context, volumeBackup *kdmpv1alpha1.VolumeBackup) (*kdmpv1alpha1.VolumeBackup, error)
	// DeleteVolumeBackup deletes the VolumeBackup
	DeleteVolumeBackup(ctx context.Context, name string, namespace string) error
	// ValidateVolumeBackup validates the VolumeBackup
	ValidateVolumeBackup(ctx context.Context, name string, namespace string, timeout, retryInterval time.Duration) error
}

// CreateVolumeBackup creates the VolumeBackup
func (c *Client) CreateVolumeBackup(ctx context.Context, volumeBackup *kdmpv1alpha1.VolumeBackup) (*kdmpv1alpha1.VolumeBackup, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().VolumeBackups(volumeBackup.Namespace).Create(ctx, volumeBackup, metav1.CreateOptions{})
}

// GetVolumeBackup gets the VolumeBackup
func (c *Client) GetVolumeBackup(ctx context.Context, name string, namespace string) (*kdmpv1alpha1.VolumeBackup, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().VolumeBackups(namespace).Get(ctx, name, metav1.GetOptions{})
}

// ListVolumeBackups lists all the VolumeBackups
func (c *Client) ListVolumeBackups(ctx context.Context, namespace string) (*kdmpv1alpha1.VolumeBackupList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().VolumeBackups(namespace).List(ctx, metav1.ListOptions{})
}

// UpdateVolumeBackup updates the VolumeBackup
func (c *Client) UpdateVolumeBackup(ctx context.Context, volumeBackup *kdmpv1alpha1.VolumeBackup) (*kdmpv1alpha1.VolumeBackup, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().VolumeBackups(volumeBackup.Namespace).Update(ctx, volumeBackup, metav1.UpdateOptions{})
}

// PatchVolumeBackup applies a patch for a given volumeBackup.
func (c *Client) PatchVolumeBackup(ctx context.Context, name, ns string, pt types.PatchType, jsonPatch []byte) (*kdmpv1alpha1.VolumeBackup, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.kdmp.KdmpV1alpha1().VolumeBackups(ns).Patch(ctx, name, pt, jsonPatch, metav1.PatchOptions{})
}

// DeleteVolumeBackup deletes the VolumeBackup
func (c *Client) DeleteVolumeBackup(ctx context.Context, name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.kdmp.KdmpV1alpha1().VolumeBackups(namespace).Delete(ctx, name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ValidateVolumeBackup validates the VolumeBackup
func (c *Client) ValidateVolumeBackup(ctx context.Context, name, namespace string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		resp, err := c.GetVolumeBackup(ctx, name, namespace)
		if err != nil {
			return "", true, &errors.ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("VolumeBackup failed. Error: %v", err),
				Type:  resp,
			}
		}
		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}
