package stork

import (
	"fmt"
	"time"

	storkv1alpha1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterPairOps is an interface to perfrom k8s ClusterPair operations
type ClusterPairOps interface {
	// CreateClusterPair creates the ClusterPair
	CreateClusterPair(*storkv1alpha1.ClusterPair) (*storkv1alpha1.ClusterPair, error)
	// GetClusterPair gets the ClusterPair
	GetClusterPair(string, string) (*storkv1alpha1.ClusterPair, error)
	// ListClusterPairs gets all the ClusterPairs
	ListClusterPairs(string) (*storkv1alpha1.ClusterPairList, error)
	// UpdateClusterPair updates the ClusterPair
	UpdateClusterPair(*storkv1alpha1.ClusterPair) (*storkv1alpha1.ClusterPair, error)
	// DeleteClusterPair deletes the ClusterPair
	DeleteClusterPair(string, string) error
	// ValidateClusterPair validates clusterpair status
	ValidateClusterPair(string, string, time.Duration, time.Duration) error
	// WatchClusterPair watch the ClusterPair object
	WatchClusterPair(namespace string, fn WatchFunc, listOptions metav1.ListOptions) error
}

// CreateClusterPair creates the ClusterPair
func (c *Client) CreateClusterPair(pair *storkv1alpha1.ClusterPair) (*storkv1alpha1.ClusterPair, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ClusterPairs(pair.Namespace).Create(pair)
}

// GetClusterPair gets the ClusterPair
func (c *Client) GetClusterPair(name string, namespace string) (*storkv1alpha1.ClusterPair, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ClusterPairs(namespace).Get(name, metav1.GetOptions{})
}

// ListClusterPairs gets all the ClusterPairs
func (c *Client) ListClusterPairs(namespace string) (*storkv1alpha1.ClusterPairList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ClusterPairs(namespace).List(metav1.ListOptions{})
}

// UpdateClusterPair updates the ClusterPair
func (c *Client) UpdateClusterPair(pair *storkv1alpha1.ClusterPair) (*storkv1alpha1.ClusterPair, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.stork.StorkV1alpha1().ClusterPairs(pair.Namespace).Update(pair)
}

// DeleteClusterPair deletes the ClusterPair
func (c *Client) DeleteClusterPair(name string, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.stork.StorkV1alpha1().ClusterPairs(namespace).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// ValidateClusterPair validates clusterpair status
func (c *Client) ValidateClusterPair(name string, namespace string, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		clusterPair, err := c.GetClusterPair(name, namespace)
		if err != nil {
			return "", true, err
		}

		if clusterPair.Status.SchedulerStatus == storkv1alpha1.ClusterPairStatusReady &&
			(clusterPair.Status.StorageStatus == storkv1alpha1.ClusterPairStatusReady ||
				clusterPair.Status.StorageStatus == storkv1alpha1.ClusterPairStatusNotProvided) {
			return "", false, nil
		} else if clusterPair.Status.SchedulerStatus == storkv1alpha1.ClusterPairStatusError ||
			clusterPair.Status.StorageStatus == storkv1alpha1.ClusterPairStatusError {
			return "", true, &errors.ErrFailedToValidateCustomSpec{
				Name:  name,
				Cause: fmt.Sprintf("Storage Status: %v \t Scheduler Status: %v", clusterPair.Status.StorageStatus, clusterPair.Status.SchedulerStatus),
				Type:  clusterPair,
			}
		}

		return "", true, &errors.ErrFailedToValidateCustomSpec{
			Name:  name,
			Cause: fmt.Sprintf("Storage Status: %v \t Scheduler Status: %v", clusterPair.Status.StorageStatus, clusterPair.Status.SchedulerStatus),
			Type:  clusterPair,
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}

	return nil
}

// WatchClusterPair sets up a watcher that listens for changes on cluster pair objects
func (c *Client) WatchClusterPair(namespace string, fn WatchFunc, listOptions metav1.ListOptions) error {
	if err := c.initClient(); err != nil {
		return err
	}

	listOptions.Watch = true
	watchInterface, err := c.stork.StorkV1alpha1().ClusterPairs(namespace).Watch(listOptions)
	if err != nil {
		logrus.WithError(err).Error("error invoking the watch api for cluster pair")
		return err
	}

	// fire off watch function
	go c.handleWatch(watchInterface, &storkv1alpha1.ClusterPair{}, "", fn, listOptions)
	return nil
}
