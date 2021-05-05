package openshift

import (
	"context"
	"fmt"
	"time"

	ocpappsv1api "github.com/openshift/api/apps/v1"
	"github.com/portworx/sched-ops/k8s/common"
	"github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/task"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DeploymentConfigOps is an interface to perform ocp deployment config operations
type DeploymentConfigOps interface {
	// ListDeploymentConfigs lists all deployments for the given namespace
	ListDeploymentConfigs(namespace string) (*ocpappsv1api.DeploymentConfigList, error)
	// GetDeploymentConfig returns a deployment for the give name and namespace
	GetDeploymentConfig(name, namespace string) (*ocpappsv1api.DeploymentConfig, error)
	// CreateDeploymentConfig creates the given deployment
	CreateDeploymentConfig(*ocpappsv1api.DeploymentConfig) (*ocpappsv1api.DeploymentConfig, error)
	// UpdateDeploymentConfig updates the given deployment
	UpdateDeploymentConfig(*ocpappsv1api.DeploymentConfig) (*ocpappsv1api.DeploymentConfig, error)
	// DeleteDeploymentConfig deletes the given deployment
	DeleteDeploymentConfig(name, namespace string) error
	// ValidateDeploymentConfig validates the given deployment if it's running and healthy
	ValidateDeploymentConfig(deployment *ocpappsv1api.DeploymentConfig, timeout, retryInterval time.Duration) error
	// ValidateTerminatedDeploymentConfig validates if given deployment is terminated
	ValidateTerminatedDeploymentConfig(*ocpappsv1api.DeploymentConfig) error
	// GetDeploymentConfigPods returns pods for the given deployment
	GetDeploymentConfigPods(*ocpappsv1api.DeploymentConfig) ([]corev1.Pod, error)
	// DescribeDeploymentConfig gets the deployment status
	DescribeDeploymentConfig(name, namespace string) (*ocpappsv1api.DeploymentConfigStatus, error)
	// GetDeploymentConfigsUsingStorageClass returns all deployments using the given storage class
	GetDeploymentConfigsUsingStorageClass(scName string) ([]ocpappsv1api.DeploymentConfig, error)
}

// ListDeploymentConfigs lists all deployments for the given namespace
func (c *Client) ListDeploymentConfigs(namespace string) (*ocpappsv1api.DeploymentConfigList, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.ocpClient.AppsV1().DeploymentConfigs(namespace).List(context.TODO(), metav1.ListOptions{})
}

// GetDeploymentConfig returns a deployment for the give name and namespace
func (c *Client) GetDeploymentConfig(name, namespace string) (*ocpappsv1api.DeploymentConfig, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.ocpClient.AppsV1().DeploymentConfigs(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// CreateDeploymentConfig creates the given deployment
func (c *Client) CreateDeploymentConfig(deployment *ocpappsv1api.DeploymentConfig) (*ocpappsv1api.DeploymentConfig, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}

	ns := deployment.Namespace
	if len(ns) == 0 {
		ns = corev1.NamespaceDefault
	}

	return c.ocpClient.AppsV1().DeploymentConfigs(ns).Create(context.TODO(), deployment, metav1.CreateOptions{})
}

// DeleteDeploymentConfig deletes the given deployment
func (c *Client) DeleteDeploymentConfig(name, namespace string) error {
	if err := c.initClient(); err != nil {
		return err
	}
	return c.ocpClient.AppsV1().DeploymentConfigs(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

// DescribeDeploymentConfig gets the deployment status
func (c *Client) DescribeDeploymentConfig(depName, depNamespace string) (*ocpappsv1api.DeploymentConfigStatus, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	dep, err := c.GetDeploymentConfig(depName, depNamespace)
	if err != nil {
		return nil, err
	}
	return &dep.Status, err
}

// UpdateDeploymentConfig updates the given deployment
func (c *Client) UpdateDeploymentConfig(deployment *ocpappsv1api.DeploymentConfig) (*ocpappsv1api.DeploymentConfig, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	return c.ocpClient.AppsV1().DeploymentConfigs(deployment.Namespace).Update(context.TODO(), deployment, metav1.UpdateOptions{})
}

// ValidateDeploymentConfig validates the given deployment if it's running and healthy
func (c *Client) ValidateDeploymentConfig(deployment *ocpappsv1api.DeploymentConfig, timeout, retryInterval time.Duration) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		dep, err := c.GetDeploymentConfig(deployment.Name, deployment.Namespace)
		if err != nil {
			return "", true, err
		}

		requiredReplicas := dep.Spec.Replicas
		shared := false

		if requiredReplicas != 1 {
			foundPVC := false
			for _, vol := range dep.Spec.Template.Spec.Volumes {
				if vol.PersistentVolumeClaim != nil {
					foundPVC = true

					claim, err := c.kube.CoreV1().
						PersistentVolumeClaims(dep.Namespace).
						Get(context.TODO(), vol.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
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

		pods, err := c.GetDeploymentConfigPods(deployment)
		if err != nil || pods == nil {
			return "", true, &errors.ErrAppNotReady{
				ID:    dep.Name,
				Cause: fmt.Sprintf("Failed to get pods for deployment. Err: %v", err),
			}
		}

		if len(pods) == 0 {
			return "", true, &errors.ErrAppNotReady{
				ID:    dep.Name,
				Cause: "DeploymentConfig has 0 pods",
			}
		}
		podsOverviewString := common.GeneratePodsOverviewString(pods)
		if requiredReplicas > dep.Status.AvailableReplicas {
			return "", true, &errors.ErrAppNotReady{
				ID: dep.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Available replicas: %v Current pods overview:\n%s",
					requiredReplicas, dep.Status.AvailableReplicas, podsOverviewString),
			}
		}

		if requiredReplicas > dep.Status.ReadyReplicas {
			return "", true, &errors.ErrAppNotReady{
				ID: dep.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Ready replicas: %v Current pods overview:\n%s",
					requiredReplicas, dep.Status.ReadyReplicas, podsOverviewString),
			}
		}

		if requiredReplicas != dep.Status.UpdatedReplicas && shared {
			return "", true, &errors.ErrAppNotReady{
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

		return "", true, &errors.ErrAppNotReady{
			ID:    dep.Name,
			Cause: fmt.Sprintf("Pod(s): %#v not yet ready", notReadyPods),
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}

// ValidateTerminatedDeploymentConfig validates if given deployment is terminated
func (c *Client) ValidateTerminatedDeploymentConfig(deployment *ocpappsv1api.DeploymentConfig) error {
	if err := c.initClient(); err != nil {
		return err
	}
	t := func() (interface{}, bool, error) {
		dep, err := c.GetDeploymentConfig(deployment.Name, deployment.Namespace)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return "", false, nil
			}
			return "", true, err
		}

		pods, err := c.GetDeploymentConfigPods(deployment)
		if err != nil {
			return "", true, &errors.ErrAppNotTerminated{
				ID:    dep.Name,
				Cause: fmt.Sprintf("Failed to get pods for deployment. Err: %v", err),
			}
		}

		if pods != nil && len(pods) > 0 {
			var podNames []string
			for _, pod := range pods {
				podNames = append(podNames, pod.Name)
			}
			return "", true, &errors.ErrAppNotTerminated{
				ID:    dep.Name,
				Cause: fmt.Sprintf("pods: %v are still present", podNames),
			}
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, 10*time.Minute, 10*time.Second); err != nil {
		return err
	}
	return nil
}

// GetDeploymentConfigPods returns pods for the given deployment
func (c *Client) GetDeploymentConfigPods(deployment *ocpappsv1api.DeploymentConfig) ([]corev1.Pod, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	rSets, err := c.kube.AppsV1().ReplicaSets(deployment.Namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, rSet := range rSets.Items {
		for _, owner := range rSet.OwnerReferences {
			if owner.Name == deployment.Name {
				return common.GetPodsByOwner(c.kube.CoreV1(), rSet.UID, rSet.Namespace)
			}
		}
	}

	return nil, nil
}

// GetDeploymentConfigsUsingStorageClass returns all deployments using the given storage class
func (c *Client) GetDeploymentConfigsUsingStorageClass(scName string) ([]ocpappsv1api.DeploymentConfig, error) {
	if err := c.initClient(); err != nil {
		return nil, err
	}
	deps, err := c.ocpClient.AppsV1().DeploymentConfigs("").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var retList []ocpappsv1api.DeploymentConfig
	for _, dep := range deps.Items {
		for _, v := range dep.Spec.Template.Spec.Volumes {
			if v.PersistentVolumeClaim == nil {
				continue
			}

			pvcName := v.PersistentVolumeClaim.ClaimName
			pvc, err := c.kube.CoreV1().PersistentVolumeClaims(dep.Namespace).Get(context.TODO(), pvcName, metav1.GetOptions{})
			if err != nil {
				continue // don't let one bad pvc stop processing
			}

			sc, err := common.GetStorageClassForPVC(c.kube.StorageV1(), pvc)
			if err == nil && sc.Name == scName {
				retList = append(retList, dep)
				break
			}
		}
	}

	return retList, nil
}
