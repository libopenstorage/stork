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

// DeploymentOps is an interface to perform k8s deployment operations
type DeploymentOps interface {
	// ListDeployments lists all deployments for the given namespace
	ListDeployments(namespace string, options metav1.ListOptions) (*appsv1.DeploymentList, error)
	// GetDeployment returns a deployment for the give name and namespace
	GetDeployment(name, namespace string) (*appsv1.Deployment, error)
	// CreateDeployment creates the given deployment
	CreateDeployment(*appsv1.Deployment) (*appsv1.Deployment, error)
	// UpdateDeployment updates the given deployment
	UpdateDeployment(*appsv1.Deployment) (*appsv1.Deployment, error)
	// DeleteDeployment deletes the given deployment
	DeleteDeployment(name, namespace string) error
	// ValidateDeployment validates the given deployment if it's running and healthy
	ValidateDeployment(deployment *appsv1.Deployment, timeout, retryInterval time.Duration) error
	// ValidateTerminatedDeployment validates if given deployment is terminated
	ValidateTerminatedDeployment(*appsv1.Deployment, time.Duration, time.Duration) error
	// GetDeploymentPods returns pods for the given deployment
	GetDeploymentPods(*appsv1.Deployment) ([]corev1.Pod, error)
	// DescribeDeployment gets the deployment status
	DescribeDeployment(name, namespace string) (*appsv1.DeploymentStatus, error)
	// GetDeploymentsUsingStorageClass returns all deployments using the given storage class
	GetDeploymentsUsingStorageClass(scName string) ([]appsv1.Deployment, error)
}

// ListDeployments lists all deployments for the given namespace
func (c *Client) ListDeployments(namespace string, options metav1.ListOptions) (*appsv1.DeploymentList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.apps.Deployments(namespace).List(context.TODO(), options)
}

// GetDeployment returns a deployment for the give name and namespace
func (c *Client) GetDeployment(name, namespace string) (*appsv1.Deployment, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	return c.apps.Deployments(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// CreateDeployment creates the given deployment
func (c *Client) CreateDeployment(deployment *appsv1.Deployment) (*appsv1.Deployment, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	ns := deployment.Namespace
	if len(ns) == 0 {
		ns = corev1.NamespaceDefault
	}

	return c.apps.Deployments(ns).Create(context.TODO(), deployment, metav1.CreateOptions{})
}

// DeleteDeployment deletes the given deployment
func (c *Client) DeleteDeployment(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}

	return c.apps.Deployments(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// DescribeDeployment gets the deployment status
func (c *Client) DescribeDeployment(depName, depNamespace string) (*appsv1.DeploymentStatus, error) {
	dep, err := c.GetDeployment(depName, depNamespace)
	if err != nil {
		return nil, err
	}
	return &dep.Status, err
}

// UpdateDeployment updates the given deployment
func (c *Client) UpdateDeployment(deployment *appsv1.Deployment) (*appsv1.Deployment, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.apps.Deployments(deployment.Namespace).Update(context.TODO(), deployment, metav1.UpdateOptions{})
}

// ValidateDeployment validates the given deployment if it's running and healthy
func (c *Client) ValidateDeployment(deployment *appsv1.Deployment, timeout, retryInterval time.Duration) error {
	t := func() (interface{}, bool, error) {
		dep, err := c.GetDeployment(deployment.Name, deployment.Namespace)
		if err != nil {
			return "", true, err
		}

		requiredReplicas := *dep.Spec.Replicas
		shared := false

		if requiredReplicas != 1 {
			foundPVC := false
			for _, vol := range dep.Spec.Template.Spec.Volumes {
				if vol.PersistentVolumeClaim != nil {
					foundPVC = true

					pvcName := vol.PersistentVolumeClaim.ClaimName
					claim, err := c.core.PersistentVolumeClaims(dep.Namespace).Get(context.TODO(), pvcName, metav1.GetOptions{})
					if err != nil {
						return "", true, err
					}

					if common.IsPVCShared(claim) {
						shared = true
						break
					}
				}
			}

			if foundPVC && !shared {
				requiredReplicas = 1
			}
		}

		pods, err := c.GetDeploymentPods(dep)
		if err != nil || pods == nil {
			return "", true, &schederrors.ErrAppNotReady{
				ID:    dep.Name,
				Cause: fmt.Sprintf("Failed to get pods for deployment. Err: %v", err),
			}
		}

		if len(pods) == 0 {
			return "", true, &schederrors.ErrAppNotReady{
				ID:    dep.Name,
				Cause: "Deployment has 0 pods",
			}
		}
		podsOverviewString := common.GeneratePodsOverviewString(pods)
		if requiredReplicas > dep.Status.AvailableReplicas {
			return "", true, &schederrors.ErrAppNotReady{
				ID: dep.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Available replicas: %v Current pods overview:\n%s",
					requiredReplicas, dep.Status.AvailableReplicas, podsOverviewString),
			}
		}

		if requiredReplicas > dep.Status.ReadyReplicas {
			return "", true, &schederrors.ErrAppNotReady{
				ID: dep.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Ready replicas: %v Current pods overview:\n%s",
					requiredReplicas, dep.Status.ReadyReplicas, podsOverviewString),
			}
		}

		if requiredReplicas != dep.Status.UpdatedReplicas && shared {
			return "", true, &schederrors.ErrAppNotReady{
				ID: dep.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Updated replicas: %v Current pods overview:\n%s",
					requiredReplicas, dep.Status.UpdatedReplicas, podsOverviewString),
			}
		}

		// look for "requiredReplicas" number of pods in ready state
		var notReadyPods []string
		var readyCount int32
		for _, pod := range pods {
			if !common.IsPodReady(pod) {
				notReadyPods = append(notReadyPods, pod.Name)
			} else {
				readyCount++
			}
		}

		if readyCount >= requiredReplicas {
			return "", false, nil
		}

		return "", true, &schederrors.ErrAppNotReady{
			ID:    dep.Name,
			Cause: fmt.Sprintf("Pod(s): %#v not yet ready", notReadyPods),
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}

// ValidateTerminatedDeployment validates if given deployment is terminated
func (c *Client) ValidateTerminatedDeployment(deployment *appsv1.Deployment, timeout, timeBeforeRetry time.Duration) error {
	t := func() (interface{}, bool, error) {
		dep, err := c.GetDeployment(deployment.Name, deployment.Namespace)
		if err != nil {
			if errors.IsNotFound(err) {
				return "", false, nil
			}
			return "", true, err
		}

		pods, err := c.GetDeploymentPods(dep)
		if err != nil {
			if errors.IsNotFound(err) {
				return "", false, nil
			}
			return "", true, &schederrors.ErrAppNotTerminated{
				ID:    dep.Name,
				Cause: fmt.Sprintf("Failed to get pods for deployment. Err: %v", err),
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
				ID:    dep.Name,
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

// GetDeploymentPods returns pods for the given deployment
func (c *Client) GetDeploymentPods(deployment *appsv1.Deployment) ([]corev1.Pod, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	rSet, err := c.GetReplicaSetByDeployment(deployment)
	if err != nil {
		return nil, err
	}
	return c.GetReplicaSetPods(rSet)
}

// GetDeploymentsUsingStorageClass returns all deployments using the given storage class
func (c *Client) GetDeploymentsUsingStorageClass(scName string) ([]appsv1.Deployment, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	deps, err := c.apps.Deployments("").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var retList []appsv1.Deployment
	for _, dep := range deps.Items {
		for _, v := range dep.Spec.Template.Spec.Volumes {
			if v.PersistentVolumeClaim == nil {
				continue
			}

			pvcName := v.PersistentVolumeClaim.ClaimName
			pvc, err := c.core.PersistentVolumeClaims(dep.Namespace).Get(context.TODO(), pvcName, metav1.GetOptions{})
			if err != nil {
				continue // don't let one bad pvc stop processing
			}

			sc, err := common.GetStorageClassForPVC(c.storage, pvc)
			if err == nil && sc.Name == scName {
				retList = append(retList, dep)
				break
			}
		}
	}

	return retList, nil
}
