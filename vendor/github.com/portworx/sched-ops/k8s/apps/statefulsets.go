package apps

import (
	"fmt"
	"time"

	"github.com/portworx/sched-ops/k8s/common"
	schederrors "github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// StatefulSetOps is an interface to perform k8s stateful set operations
type StatefulSetOps interface {
	// ListStatefulSets lists all the statefulsets for a given namespace
	ListStatefulSets(namespace string) (*appsv1.StatefulSetList, error)
	// GetStatefulSet returns a statefulset for given name and namespace
	GetStatefulSet(name, namespace string) (*appsv1.StatefulSet, error)
	// CreateStatefulSet creates the given statefulset
	CreateStatefulSet(ss *appsv1.StatefulSet) (*appsv1.StatefulSet, error)
	// UpdateStatefulSet creates the given statefulset
	UpdateStatefulSet(ss *appsv1.StatefulSet) (*appsv1.StatefulSet, error)
	// DeleteStatefulSet deletes the given statefulset
	DeleteStatefulSet(name, namespace string) error
	// ValidateStatefulSet validates the given statefulset if it's running and healthy within the given timeout
	ValidateStatefulSet(ss *appsv1.StatefulSet, timeout time.Duration) error
	// ValidateTerminatedStatefulSet validates if given deployment is terminated
	ValidateTerminatedStatefulSet(ss *appsv1.StatefulSet, timeout, retryInterval time.Duration) error
	// GetStatefulSetPods returns pods for the given statefulset
	GetStatefulSetPods(ss *appsv1.StatefulSet) ([]corev1.Pod, error)
	// DescribeStatefulSet gets status of the statefulset
	DescribeStatefulSet(name, namespace string) (*appsv1.StatefulSetStatus, error)
	// GetStatefulSetsUsingStorageClass returns all statefulsets using given storage class
	GetStatefulSetsUsingStorageClass(scName string) ([]appsv1.StatefulSet, error)
	// GetPVCsForStatefulSet returns all the PVCs for given stateful set
	GetPVCsForStatefulSet(ss *appsv1.StatefulSet) (*corev1.PersistentVolumeClaimList, error)
	// ValidatePVCsForStatefulSet validates the PVCs for the given stateful set
	ValidatePVCsForStatefulSet(ss *appsv1.StatefulSet, timeout, retryInterval time.Duration) error
}

// ListStatefulSets lists all the statefulsets for a given namespace
func (c *Client) ListStatefulSets(namespace string) (*appsv1.StatefulSetList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.apps.StatefulSets(namespace).List(metav1.ListOptions{})
}

// GetStatefulSet returns a statefulset for given name and namespace
func (c *Client) GetStatefulSet(name, namespace string) (*appsv1.StatefulSet, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.apps.StatefulSets(namespace).Get(name, metav1.GetOptions{})
}

// CreateStatefulSet creates the given statefulset
func (c *Client) CreateStatefulSet(statefulset *appsv1.StatefulSet) (*appsv1.StatefulSet, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	ns := statefulset.Namespace
	if len(ns) == 0 {
		ns = corev1.NamespaceDefault
	}

	return c.apps.StatefulSets(ns).Create(statefulset)
}

// DeleteStatefulSet deletes the given statefulset
func (c *Client) DeleteStatefulSet(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.apps.StatefulSets(namespace).Delete(name, &metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// DescribeStatefulSet gets status of the statefulset
func (c *Client) DescribeStatefulSet(ssetName string, ssetNamespace string) (*appsv1.StatefulSetStatus, error) {
	sset, err := c.GetStatefulSet(ssetName, ssetNamespace)
	if err != nil {
		return nil, err
	}

	return &sset.Status, err
}

// UpdateStatefulSet creates the given statefulset
func (c *Client) UpdateStatefulSet(statefulset *appsv1.StatefulSet) (*appsv1.StatefulSet, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.apps.StatefulSets(statefulset.Namespace).Update(statefulset)
}

// ValidateStatefulSet validates the given statefulset if it's running and healthy within the given timeout
func (c *Client) ValidateStatefulSet(statefulset *appsv1.StatefulSet, timeout time.Duration) error {
	t := func() (interface{}, bool, error) {
		sset, err := c.GetStatefulSet(statefulset.Name, statefulset.Namespace)
		if err != nil {
			return "", true, err
		}

		pods, err := c.GetStatefulSetPods(sset)
		if err != nil || pods == nil {
			return "", true, &schederrors.ErrAppNotReady{
				ID:    sset.Name,
				Cause: fmt.Sprintf("Failed to get pods for statefulset. Err: %v", err),
			}
		}

		if len(pods) == 0 {
			return "", true, &schederrors.ErrAppNotReady{
				ID:    sset.Name,
				Cause: "StatefulSet has 0 pods",
			}
		}

		podsOverviewString := common.GeneratePodsOverviewString(pods)

		if *sset.Spec.Replicas != sset.Status.Replicas { // Not sure if this is even needed but for now let's have one check before
			//readiness check
			return "", true, &schederrors.ErrAppNotReady{
				ID: sset.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Observed replicas: %v. Current pods overview:\n%s",
					*sset.Spec.Replicas, sset.Status.Replicas, podsOverviewString),
			}
		}

		if *sset.Spec.Replicas != sset.Status.ReadyReplicas {
			return "", true, &schederrors.ErrAppNotReady{
				ID: sset.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Ready replicas: %v Current pods overview:\n%s",
					*sset.Spec.Replicas, sset.Status.ReadyReplicas, podsOverviewString),
			}
		}

		for _, pod := range pods {
			if !common.IsPodReady(pod) {
				return "", true, &schederrors.ErrAppNotReady{
					ID:    sset.Name,
					Cause: fmt.Sprintf("Pod: %v is not yet ready", pod.Name),
				}
			}
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, 10*time.Second); err != nil {
		return err
	}
	return nil
}

// GetStatefulSetPods returns pods for the given statefulset
func (c *Client) GetStatefulSetPods(statefulset *appsv1.StatefulSet) ([]corev1.Pod, error) {
	return common.GetPodsByOwner(c.core, statefulset.UID, statefulset.Namespace)
}

// ValidateTerminatedStatefulSet validates if given deployment is terminated
func (c *Client) ValidateTerminatedStatefulSet(statefulset *appsv1.StatefulSet, timeout, timeBeforeRetry time.Duration) error {
	t := func() (interface{}, bool, error) {
		sset, err := c.GetStatefulSet(statefulset.Name, statefulset.Namespace)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return "", false, nil
			}

			return "", true, err
		}

		pods, err := c.GetStatefulSetPods(statefulset)
		if err != nil {
			return "", true, &schederrors.ErrAppNotTerminated{
				ID:    sset.Name,
				Cause: fmt.Sprintf("Failed to get pods for statefulset. Err: %v", err),
			}
		}

		if pods != nil && len(pods) > 0 {
			var podNames []string
			for _, pod := range pods {
				if len(pod.Spec.NodeName) > 0 {
					podNames = append(podNames, fmt.Sprintf("%s (node=%s)", pod.Name, pod.Spec.NodeName))
				} else {
					podNames = append(podNames, pod.Name)
				}
			}
			return "", true, &schederrors.ErrAppNotTerminated{
				ID:    sset.Name,
				Cause: fmt.Sprintf("pods: %v are still present", podNames),
			}
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, timeBeforeRetry); err != nil {
		return err
	}
	return nil
}

// GetStatefulSetsUsingStorageClass returns all statefulsets using given storage class
func (c *Client) GetStatefulSetsUsingStorageClass(scName string) ([]appsv1.StatefulSet, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	ss, err := c.apps.StatefulSets("").List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var retList []appsv1.StatefulSet
	for _, s := range ss.Items {
		if s.Spec.VolumeClaimTemplates == nil {
			continue
		}

		for _, template := range s.Spec.VolumeClaimTemplates {
			sc, err := common.GetStorageClassForPVC(c.storage, &template)
			if err == nil && sc.Name == scName {
				retList = append(retList, s)
				break
			}
		}
	}

	return retList, nil
}

// GetPVCsForStatefulSet returns all the PVCs for given stateful set
func (c *Client) GetPVCsForStatefulSet(ss *appsv1.StatefulSet) (*corev1.PersistentVolumeClaimList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	listOptions, err := c.getListOptionsForStatefulSet(ss)
	if err != nil {
		return nil, err
	}

	return c.core.PersistentVolumeClaims(ss.Namespace).List(listOptions)
}

// ValidatePVCsForStatefulSet validates the PVCs for the given stateful set
func (c *Client) ValidatePVCsForStatefulSet(ss *appsv1.StatefulSet, timeout, retryTimeout time.Duration) error {
	listOptions, err := c.getListOptionsForStatefulSet(ss)
	if err != nil {
		return err
	}

	t := func() (interface{}, bool, error) {
		pvcList, err := c.core.PersistentVolumeClaims(ss.Namespace).List(listOptions)
		if err != nil {
			return nil, true, err
		}

		if len(pvcList.Items) < int(*ss.Spec.Replicas) {
			return nil, true, fmt.Errorf("Expected PVCs: %v, Actual: %v", *ss.Spec.Replicas, len(pvcList.Items))
		}

		for _, pvc := range pvcList.Items {
			if err := c.validatePersistentVolumeClaim(&pvc, timeout, retryTimeout); err != nil {
				return nil, true, err
			}
		}

		return nil, false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryTimeout); err != nil {
		return err
	}
	return nil
}

func (c *Client) getListOptionsForStatefulSet(ss *appsv1.StatefulSet) (metav1.ListOptions, error) {
	// TODO: Handle MatchExpressions as well
	lbls := ss.Spec.Selector.MatchLabels

	if len(lbls) == 0 {
		return metav1.ListOptions{}, fmt.Errorf("No labels present to retrieve the PVCs")
	}

	return metav1.ListOptions{
		LabelSelector: labels.FormatLabels(lbls),
	}, nil
}

// validatePersistentVolumeClaim is a copy of core.ValidatePersistentVolumeClaim.
func (c *Client) validatePersistentVolumeClaim(pvc *corev1.PersistentVolumeClaim, timeout, retryInterval time.Duration) error {
	t := func() (interface{}, bool, error) {
		if err := c.initClient(); err != nil {
			return "", true, err
		}

		result, err := c.core.PersistentVolumeClaims(pvc.Namespace).Get(pvc.Name, metav1.GetOptions{})
		if err != nil {
			return "", true, err
		}

		if result.Status.Phase == corev1.ClaimBound {
			return "", false, nil
		}

		return "", true, &schederrors.ErrPVCNotReady{
			ID:    result.Name,
			Cause: fmt.Sprintf("PVC expected status: %v PVC actual status: %v", corev1.ClaimBound, result.Status.Phase),
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}
