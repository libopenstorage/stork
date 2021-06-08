package storage

import (
	"context"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VolumeAttachmentOps is an interface to perform k8s VolumeAttachmentOps operations
type VolumeAttachmentOps interface {
	// ListVolumeAttachments lists all volume attachments
	ListVolumeAttachments() (*storagev1.VolumeAttachmentList, error)
	// DeleteVolumeAttachment deletes a given Volume Attachment by name
	DeleteVolumeAttachment(name string) error
	// CreateVolumeAttachment creates a volume attachment
	CreateVolumeAttachment(*storagev1.VolumeAttachment) (*storagev1.VolumeAttachment, error)
	// UpdateVolumeAttachment updates a volume attachment
	UpdateVolumeAttachment(*storagev1.VolumeAttachment) (*storagev1.VolumeAttachment, error)
	// UpdateVolumeAttachmentStatus updates a volume attachment status
	UpdateVolumeAttachmentStatus(*storagev1.VolumeAttachment) (*storagev1.VolumeAttachment, error)
}

// ListVolumeAttachments lists all volume attachments
func (c *Client) ListVolumeAttachments() (*storagev1.VolumeAttachmentList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.storage.VolumeAttachments().List(context.TODO(), metav1.ListOptions{})
}

// DeleteVolumeAttachment deletes a given Volume Attachment by name
func (c *Client) DeleteVolumeAttachment(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.storage.VolumeAttachments().Delete(context.TODO(), name, metav1.DeleteOptions{})
}

// CreateVolumeAttachment creates a volume attachment
func (c *Client) CreateVolumeAttachment(volumeAttachment *storagev1.VolumeAttachment) (*storagev1.VolumeAttachment, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.storage.VolumeAttachments().Create(context.TODO(), volumeAttachment, metav1.CreateOptions{})
}

// UpdateVolumeAttachment updates a volume attachment
func (c *Client) UpdateVolumeAttachment(volumeAttachment *storagev1.VolumeAttachment) (*storagev1.VolumeAttachment, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.storage.VolumeAttachments().Update(context.TODO(), volumeAttachment, metav1.UpdateOptions{})
}

// UpdateVolumeAttachmentStatus updates a volume attachment status
func (c *Client) UpdateVolumeAttachmentStatus(volumeAttachment *storagev1.VolumeAttachment) (*storagev1.VolumeAttachment, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.storage.VolumeAttachments().UpdateStatus(context.TODO(), volumeAttachment, metav1.UpdateOptions{})
}
