package apps

import (
	"fmt"
	"time"

	"github.com/portworx/sched-ops/k8s/common"
	schederrors "github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ReplicaSetOps is an interface to perform k8s daemon set operations
type ReplicaSetOps interface {
	// CreateReplicaSet creates the given ReplicaSet
	CreateReplicaSet(rs *appsv1.ReplicaSet) (*appsv1.ReplicaSet, error)
	// ListReplicaSets lists all ReplicaSets in given namespace
	ListReplicaSets(namespace string, listOpts metav1.ListOptions) ([]appsv1.ReplicaSet, error)
	// GetReplicaSet gets the the daemon set with given name
	GetReplicaSet(string, string) (*appsv1.ReplicaSet, error)
	// ValidateReplicaSet checks if the given ReplicaSet is ready within given timeout
	ValidateReplicaSet(name, namespace string, timeout time.Duration) error
	// GetReplicaSetPods returns list of pods for the ReplicaSet
	GetReplicaSetPods(*appsv1.ReplicaSet) ([]corev1.Pod, error)
	// UpdateReplicaSet updates the given daemon set and returns the updated rs
	UpdateReplicaSet(*appsv1.ReplicaSet) (*appsv1.ReplicaSet, error)
	// DeleteReplicaSet deletes the given ReplicaSet
	DeleteReplicaSet(name, namespace string) error
	// GetReplicaSetByDeployment deletes the given ReplicaSet
	GetReplicaSetByDeployment(deployment *appsv1.Deployment) (*appsv1.ReplicaSet, error)
}

// CreateReplicaSet creates the given ReplicaSet
func (c *Client) CreateReplicaSet(rs *appsv1.ReplicaSet) (*appsv1.ReplicaSet, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.apps.ReplicaSets(rs.Namespace).Create(rs)
}

// ListReplicaSets lists all ReplicaSets in given namespace
func (c *Client) ListReplicaSets(namespace string, listOpts metav1.ListOptions) ([]appsv1.ReplicaSet, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	rsList, err := c.apps.ReplicaSets(namespace).List(listOpts)
	if err != nil {
		return nil, err
	}

	return rsList.Items, nil
}

// GetReplicaSet gets the the daemon set with given name
func (c *Client) GetReplicaSet(name, namespace string) (*appsv1.ReplicaSet, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	if len(namespace) == 0 {
		namespace = corev1.NamespaceDefault
	}

	rs, err := c.apps.ReplicaSets(namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return rs, nil
}

// GetReplicaSetPods returns list of pods for the ReplicaSet
func (c *Client) GetReplicaSetPods(rs *appsv1.ReplicaSet) ([]corev1.Pod, error) {
	return common.GetPodsByOwner(c.core, rs.UID, rs.Namespace)
}

// ValidateReplicaSet checks if the given ReplicaSet is ready within given timeout
func (c *Client) ValidateReplicaSet(name, namespace string, timeout time.Duration) error {
	t := func() (interface{}, bool, error) {
		rs, err := c.GetReplicaSet(name, namespace)
		if err != nil {
			return "", true, err
		}

		if rs.Status.ObservedGeneration == 0 {
			return "", true, &schederrors.ErrAppNotReady{
				ID:    name,
				Cause: "Observed generation is still 0. Check back status after some time",
			}
		}

		pods, err := c.GetReplicaSetPods(rs)
		if err != nil || pods == nil {
			return "", true, &schederrors.ErrAppNotReady{
				ID:    rs.Name,
				Cause: fmt.Sprintf("Failed to get pods for ReplicaSet. Err: %v", err),
			}
		}

		if len(pods) == 0 {
			return "", true, &schederrors.ErrAppNotReady{
				ID:    rs.Name,
				Cause: "ReplicaSet has 0 pods",
			}
		}

		podsOverviewString := common.GeneratePodsOverviewString(pods)

		if rs.Status.Replicas != rs.Status.AvailableReplicas {
			return "", true, &schederrors.ErrAppNotReady{
				ID: name,
				Cause: fmt.Sprintf("Not all pods are updated. expected: %v updated: %v. Current pods overview:\n%s",
					rs.Status.Replicas, rs.Status.AvailableReplicas, podsOverviewString),
			}
		}

		unavailableReplicas := rs.Status.Replicas - rs.Status.ReadyReplicas
		if unavailableReplicas > 0 {
			return "", true, &schederrors.ErrAppNotReady{
				ID: name,
				Cause: fmt.Sprintf("%d pods are not available. available: %d ready: %d. Current pods overview:\n%s",
					unavailableReplicas, rs.Status.AvailableReplicas,
					rs.Status.ReadyReplicas, podsOverviewString),
			}
		}

		if rs.Status.Replicas != rs.Status.ReadyReplicas {
			return "", true, &schederrors.ErrAppNotReady{
				ID: name,
				Cause: fmt.Sprintf("Expected ready: %v Actual ready:%v Current pods overview:\n%s",
					rs.Status.Replicas, rs.Status.ReadyReplicas, podsOverviewString),
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

		if readyCount == rs.Status.Replicas {
			return "", false, nil
		}

		return "", true, &schederrors.ErrAppNotReady{
			ID:    rs.Name,
			Cause: fmt.Sprintf("Pod(s): %#v not yet ready", notReadyPods),
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, 15*time.Second); err != nil {
		return err
	}
	return nil
}

// UpdateReplicaSet updates the given daemon set and returns the updated rs
func (c *Client) UpdateReplicaSet(rs *appsv1.ReplicaSet) (*appsv1.ReplicaSet, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.apps.ReplicaSets(rs.Namespace).Update(rs)
}

// DeleteReplicaSet deletes the given ReplicaSet
func (c *Client) DeleteReplicaSet(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.apps.ReplicaSets(namespace).Delete(
		name,
		&metav1.DeleteOptions{PropagationPolicy: &deleteForegroundPolicy})
}

// GetReplicaSetByDeployment get ReplicaSet for a Given Deployment
func (c *Client) GetReplicaSetByDeployment(deployment *appsv1.Deployment) (*appsv1.ReplicaSet, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	rsets, err := c.apps.ReplicaSets(deployment.Namespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, rs := range rsets.Items {
		for _, ownerReference := range rs.OwnerReferences {
			if ownerReference.Name == deployment.Name {
				return &rs, nil
			}
		}
	}
	return nil, errors.NewNotFound(schema.GroupResource{
		Group:    "apps",
		Resource: "replicasets",
	}, deployment.Name)
}
