package apps

import (
	"context"
	"fmt"
	"time"

	"github.com/portworx/sched-ops/k8s/common"
	schederrors "github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DaemonSetOps is an interface to perform k8s daemon set operations
type DaemonSetOps interface {
	// CreateDaemonSet creates the given daemonset
	CreateDaemonSet(ds *appsv1.DaemonSet, opts metav1.CreateOptions) (*appsv1.DaemonSet, error)
	// ListDaemonSets lists all daemonsets in given namespace
	ListDaemonSets(namespace string, listOpts metav1.ListOptions) ([]appsv1.DaemonSet, error)
	// GetDaemonSet gets the the daemon set with given name
	GetDaemonSet(string, string) (*appsv1.DaemonSet, error)
	// ValidateDaemonSet checks if the given daemonset is ready within given timeout
	ValidateDaemonSet(name, namespace string, timeout time.Duration) error
	// GetDaemonSetPods returns list of pods for the daemonset
	GetDaemonSetPods(*appsv1.DaemonSet) ([]corev1.Pod, error)
	// UpdateDaemonSet updates the given daemon set and returns the updated ds
	UpdateDaemonSet(*appsv1.DaemonSet) (*appsv1.DaemonSet, error)
	// DeleteDaemonSet deletes the given daemonset
	DeleteDaemonSet(name, namespace string) error
	// ValidateDaemonSetIsTerminated validates if given daemonset is terminated
	ValidateDaemonSetIsTerminated(name, namespace string, timeout time.Duration) error
}

// CreateDaemonSet creates the given daemonset
func (c *Client) CreateDaemonSet(ds *appsv1.DaemonSet, opts metav1.CreateOptions) (*appsv1.DaemonSet, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.apps.DaemonSets(ds.Namespace).Create(context.TODO(), ds, opts)
}

// ListDaemonSets lists all daemonsets in given namespace
func (c *Client) ListDaemonSets(namespace string, listOpts metav1.ListOptions) ([]appsv1.DaemonSet, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	dsList, err := c.apps.DaemonSets(namespace).List(context.TODO(), listOpts)
	if err != nil {
		return nil, err
	}

	return dsList.Items, nil
}

// GetDaemonSet gets the the daemon set with given name
func (c *Client) GetDaemonSet(name, namespace string) (*appsv1.DaemonSet, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	if len(namespace) == 0 {
		namespace = corev1.NamespaceDefault
	}

	ds, err := c.apps.DaemonSets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return ds, nil
}

// GetDaemonSetPods returns list of pods for the daemonset
func (c *Client) GetDaemonSetPods(ds *appsv1.DaemonSet) ([]corev1.Pod, error) {
	return common.GetPodsByOwner(c.core, ds.UID, ds.Namespace)
}

// ValidateDaemonSet checks if the given daemonset is ready within given timeout
func (c *Client) ValidateDaemonSet(name, namespace string, timeout time.Duration) error {
	t := func() (interface{}, bool, error) {
		ds, err := c.GetDaemonSet(name, namespace)
		if err != nil {
			return "", true, err
		}

		if ds.Status.ObservedGeneration == 0 {
			return "", true, &schederrors.ErrAppNotReady{
				ID:    name,
				Cause: "Observed generation is still 0. Check back status after some time",
			}
		}

		pods, err := c.GetDaemonSetPods(ds)
		if err != nil || pods == nil {
			return "", true, &schederrors.ErrAppNotReady{
				ID:    ds.Name,
				Cause: fmt.Sprintf("Failed to get pods for daemonset. Err: %v", err),
			}
		}

		if len(pods) == 0 {
			return "", true, &schederrors.ErrAppNotReady{
				ID:    ds.Name,
				Cause: "DaemonSet has 0 pods",
			}
		}

		podsOverviewString := common.GeneratePodsOverviewString(pods)

		if ds.Status.DesiredNumberScheduled != ds.Status.UpdatedNumberScheduled {
			return "", true, &schederrors.ErrAppNotReady{
				ID: name,
				Cause: fmt.Sprintf("Not all pods are updated. expected: %v updated: %v. Current pods overview:\n%s",
					ds.Status.DesiredNumberScheduled, ds.Status.UpdatedNumberScheduled, podsOverviewString),
			}
		}

		if ds.Status.NumberUnavailable > 0 {
			return "", true, &schederrors.ErrAppNotReady{
				ID: name,
				Cause: fmt.Sprintf("%d pods are not available. available: %d ready: %d. Current pods overview:\n%s",
					ds.Status.NumberUnavailable, ds.Status.NumberAvailable,
					ds.Status.NumberReady, podsOverviewString),
			}
		}

		if ds.Status.DesiredNumberScheduled != ds.Status.NumberReady {
			return "", true, &schederrors.ErrAppNotReady{
				ID: name,
				Cause: fmt.Sprintf("Expected ready: %v Actual ready:%v Current pods overview:\n%s",
					ds.Status.DesiredNumberScheduled, ds.Status.NumberReady, podsOverviewString),
			}
		}

		var notReadyPods []string
		var readyCount int32
		for _, pod := range pods {
			if !common.IsPodReady(pod) {
				notReadyPods = append(notReadyPods, pod.Name)
			} else {
				readyCount++
			}
		}

		if readyCount == ds.Status.DesiredNumberScheduled {
			return "", false, nil
		}

		return "", true, &schederrors.ErrAppNotReady{
			ID:    ds.Name,
			Cause: fmt.Sprintf("Pod(s): %#v not yet ready", notReadyPods),
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, 15*time.Second); err != nil {
		return err
	}
	return nil
}

// UpdateDaemonSet updates the given daemon set and returns the updated ds
func (c *Client) UpdateDaemonSet(ds *appsv1.DaemonSet) (*appsv1.DaemonSet, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.apps.DaemonSets(ds.Namespace).Update(context.TODO(), ds, metav1.UpdateOptions{})
}

// DeleteDaemonSet deletes the given daemonset
func (c *Client) DeleteDaemonSet(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.apps.DaemonSets(namespace).Delete(context.TODO(),
		name,
		metav1.DeleteOptions{PropagationPolicy: &deleteForegroundPolicy})
}

// ValidateDaemonSetIsTerminated validates if given daemonset is terminated
func (c *Client) ValidateDaemonSetIsTerminated(name, namespace string, timeout time.Duration) error {
	t := func() (interface{}, bool, error) {
		ds, err := c.GetDaemonSet(name, namespace)
		if errors.IsNotFound(err) {
			return nil, false, nil
		} else if err != nil {
			return nil, true, err
		}

		currPods := ds.Status.CurrentNumberScheduled
		if currPods > 0 {
			return nil, true, &schederrors.ErrAppNotTerminated{
				ID:    ds.Name,
				Cause: fmt.Sprintf("%d pods are still present", currPods),
			}
		}

		return nil, true, &schederrors.ErrAppNotTerminated{
			ID:    ds.Name,
			Cause: fmt.Sprintf("daemon is still present"),
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, 15*time.Second); err != nil {
		return err
	}
	return nil
}
