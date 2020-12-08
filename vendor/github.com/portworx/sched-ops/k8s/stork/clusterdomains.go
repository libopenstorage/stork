package stork

import (
	"fmt"
	"time"

	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterDomainsOps is an interface to perform k8s ClusterDomains operations
type ClusterDomainsOps interface {
	// CreateClusterDomainsStatus creates the ClusterDomainStatus
	CreateClusterDomainsStatus(*storkv1alpha1.ClusterDomainsStatus) (*storkv1alpha1.ClusterDomainsStatus, error)
	// GetClusterDomainsStatus gets the ClusterDomainsStatus
	GetClusterDomainsStatus(string) (*storkv1alpha1.ClusterDomainsStatus, error)
	// UpdateClusterDomainsStatus updates the ClusterDomainsStatus
	UpdateClusterDomainsStatus(*storkv1alpha1.ClusterDomainsStatus) (*storkv1alpha1.ClusterDomainsStatus, error)
	// DeleteClusterDomainsStatus deletes the ClusterDomainsStatus
	DeleteClusterDomainsStatus(string) error
	// ListClusterDomainStatuses lists ClusterDomainsStatus
	ListClusterDomainStatuses() (*storkv1alpha1.ClusterDomainsStatusList, error)
	// ValidateClusterDomainsStatus validates the ClusterDomainsStatus
	ValidateClusterDomainsStatus(string, map[string]bool, time.Duration, time.Duration) error
	// CreateClusterDomainUpdate creates the ClusterDomainUpdate
	CreateClusterDomainUpdate(*storkv1alpha1.ClusterDomainUpdate) (*storkv1alpha1.ClusterDomainUpdate, error)
	// GetClusterDomainUpdate gets the ClusterDomainUpdate
	GetClusterDomainUpdate(string) (*storkv1alpha1.ClusterDomainUpdate, error)
	// UpdateClusterDomainUpdate updates the ClusterDomainUpdate
	UpdateClusterDomainUpdate(*storkv1alpha1.ClusterDomainUpdate) (*storkv1alpha1.ClusterDomainUpdate, error)
	// DeleteClusterDomainUpdate deletes the ClusterDomainUpdate
	DeleteClusterDomainUpdate(string) error
	// ValidateClusterDomainUpdate validates ClusterDomainUpdate
	ValidateClusterDomainUpdate(string, time.Duration, time.Duration) error
	// ListClusterDomainUpdates lists ClusterDomainUpdates
	ListClusterDomainUpdates() (*storkv1alpha1.ClusterDomainUpdateList, error)
}

// CreateClusterDomainsStatus creates the ClusterDomainStatus
func (c *Client) CreateClusterDomainsStatus(clusterDomainsStatus *storkv1alpha1.ClusterDomainsStatus) (*storkv1alpha1.ClusterDomainsStatus, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ClusterDomainsStatuses().Create(clusterDomainsStatus)
}

// GetClusterDomainsStatus gets the ClusterDomainsStatus
func (c *Client) GetClusterDomainsStatus(name string) (*storkv1alpha1.ClusterDomainsStatus, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ClusterDomainsStatuses().Get(name, metav1.GetOptions{})
}

// UpdateClusterDomainsStatus updates the ClusterDomainsStatus
func (c *Client) UpdateClusterDomainsStatus(clusterDomainsStatus *storkv1alpha1.ClusterDomainsStatus) (*storkv1alpha1.ClusterDomainsStatus, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ClusterDomainsStatuses().Update(clusterDomainsStatus)
}

// DeleteClusterDomainsStatus deletes the ClusterDomainsStatus
func (c *Client) DeleteClusterDomainsStatus(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().ClusterDomainsStatuses().Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ValidateClusterDomainsStatus validates the ClusterDomainsStatus
func (c *Client) ValidateClusterDomainsStatus(name string, domainMap map[string]bool, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		cds, err := c.GetClusterDomainsStatus(name)
		if err != nil {
			return "", true, err
		}

		for _, domainInfo := range cds.Status.ClusterDomainInfos {
			isActive, _ := domainMap[domainInfo.Name]
			if isActive {
				if domainInfo.State != storkv1alpha1.ClusterDomainActive {
					return "", true, &errors.ErrFailedToValidateCustomSpec{
						Name: domainInfo.Name,
						Cause: fmt.Sprintf("ClusterDomainsStatus mismatch. For domain %v "+
							"expected to be active found inactive", domainInfo.Name),
						Type: cds,
					}
				}
			} else {
				if domainInfo.State != storkv1alpha1.ClusterDomainInactive {
					return "", true, &errors.ErrFailedToValidateCustomSpec{
						Name: domainInfo.Name,
						Cause: fmt.Sprintf("ClusterDomainsStatus mismatch. For domain %v "+
							"expected to be inactive found active", domainInfo.Name),
						Type: cds,
					}
				}
			}
		}

		return "", false, nil

	}
	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}

	return nil

}

// ListClusterDomainStatuses lists ClusterDomainsStatus
func (c *Client) ListClusterDomainStatuses() (*storkv1alpha1.ClusterDomainsStatusList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ClusterDomainsStatuses().List(metav1.ListOptions{})
}

// CreateClusterDomainUpdate creates the ClusterDomainUpdate
func (c *Client) CreateClusterDomainUpdate(clusterDomainUpdate *storkv1alpha1.ClusterDomainUpdate) (*storkv1alpha1.ClusterDomainUpdate, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ClusterDomainUpdates().Create(clusterDomainUpdate)
}

// GetClusterDomainUpdate gets the ClusterDomainUpdate
func (c *Client) GetClusterDomainUpdate(name string) (*storkv1alpha1.ClusterDomainUpdate, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ClusterDomainUpdates().Get(name, metav1.GetOptions{})
}

// UpdateClusterDomainUpdate updates the ClusterDomainUpdate
func (c *Client) UpdateClusterDomainUpdate(clusterDomainUpdate *storkv1alpha1.ClusterDomainUpdate) (*storkv1alpha1.ClusterDomainUpdate, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ClusterDomainUpdates().Update(clusterDomainUpdate)
}

// DeleteClusterDomainUpdate deletes the ClusterDomainUpdate
func (c *Client) DeleteClusterDomainUpdate(name string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().ClusterDomainUpdates().Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ValidateClusterDomainUpdate validates ClusterDomainUpdate
func (c *Client) ValidateClusterDomainUpdate(name string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		resp, err := c.GetClusterDomainUpdate(name)
		if err != nil {
			return "", true, err
		}

		if resp.Status.Status == storkv1alpha1.ClusterDomainUpdateStatusSuccessful {
			return "", false, nil
		} else if resp.Status.Status == storkv1alpha1.ClusterDomainUpdateStatusFailed {
			return "", false, &errors.ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("ClusterDomainUpdate Status %v, Reason: %v", resp.Status.Status, resp.Status.Reason),
				Type:  resp,
			}
		}

		return "", true, &errors.ErrFailedToValidateCustomSpec{
			Name:  name,
			Cause: fmt.Sprintf("ClusterDomainUpdate Status %v, Reason: %v", resp.Status.Status, resp.Status.Reason),
			Type:  resp,
		}
	}
	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}

	return nil
}

// ListClusterDomainUpdates lists ClusterDomainUpdates
func (c *Client) ListClusterDomainUpdates() (*storkv1alpha1.ClusterDomainUpdateList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ClusterDomainUpdates().List(metav1.ListOptions{})
}
