package k8s

import (
	"fmt"
	"time"

	ocp_appsv1_api "github.com/openshift/api/apps/v1"
	ocp_securityv1_api "github.com/openshift/api/security/v1"
	ocp_securityv1_client "github.com/openshift/client-go/security/clientset/versioned/typed/security/v1"
	"github.com/portworx/sched-ops/task"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SecurityContextConstraintsOps is an interface to list, get and update security context constraints
type SecurityContextConstraintsOps interface {
	// ListSecurityContextConstraints returns the list of all SecurityContextConstraints, and an error if there is any.
	ListSecurityContextConstraints() (*ocp_securityv1_api.SecurityContextConstraintsList, error)
	// GetSecurityContextConstraints takes name of the securityContextConstraints and returns the corresponding securityContextConstraints object, and an error if there is any.
	GetSecurityContextConstraints(string) (*ocp_securityv1_api.SecurityContextConstraints, error)
	// UpdateSecurityContextConstraints takes the representation of a securityContextConstraints and updates it. Returns the server's representation of the securityContextConstraints, and an error, if there is any.
	UpdateSecurityContextConstraints(*ocp_securityv1_api.SecurityContextConstraints) (*ocp_securityv1_api.SecurityContextConstraints, error)
}

// DeploymentConfigOps is an interface to perform ocp deployment config operations
type DeploymentConfigOps interface {
	// ListDeploymentConfigs lists all deployments for the given namespace
	ListDeploymentConfigs(namespace string) (*ocp_appsv1_api.DeploymentConfigList, error)
	// GetDeploymentConfig returns a deployment for the give name and namespace
	GetDeploymentConfig(name, namespace string) (*ocp_appsv1_api.DeploymentConfig, error)
	// CreateDeploymentConfig creates the given deployment
	CreateDeploymentConfig(*ocp_appsv1_api.DeploymentConfig) (*ocp_appsv1_api.DeploymentConfig, error)
	// UpdateDeploymentConfig updates the given deployment
	UpdateDeploymentConfig(*ocp_appsv1_api.DeploymentConfig) (*ocp_appsv1_api.DeploymentConfig, error)
	// DeleteDeploymentConfig deletes the given deployment
	DeleteDeploymentConfig(name, namespace string) error
	// ValidateDeploymentConfig validates the given deployment if it's running and healthy
	ValidateDeploymentConfig(deployment *ocp_appsv1_api.DeploymentConfig, timeout, retryInterval time.Duration) error
	// ValidateTerminatedDeploymentConfig validates if given deployment is terminated
	ValidateTerminatedDeploymentConfig(*ocp_appsv1_api.DeploymentConfig) error
	// GetDeploymentConfigPods returns pods for the given deployment
	GetDeploymentConfigPods(*ocp_appsv1_api.DeploymentConfig) ([]v1.Pod, error)
	// DescribeDeploymentConfig gets the deployment status
	DescribeDeploymentConfig(name, namespace string) (*ocp_appsv1_api.DeploymentConfigStatus, error)
	// GetDeploymentConfigsUsingStorageClass returns all deployments using the given storage class
	GetDeploymentConfigsUsingStorageClass(scName string) ([]ocp_appsv1_api.DeploymentConfig, error)
}

// Security Context Constraints APIs - BEGIN

func (k *k8sOps) getOcpSecurityClient() ocp_securityv1_client.SecurityV1Interface {
	return k.ocpSecurityClient.SecurityV1()
}

func (k *k8sOps) ListSecurityContextConstraints() (result *ocp_securityv1_api.SecurityContextConstraintsList, err error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.getOcpSecurityClient().SecurityContextConstraints().List(meta_v1.ListOptions{})
}

func (k *k8sOps) GetSecurityContextConstraints(name string) (result *ocp_securityv1_api.SecurityContextConstraints, err error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.getOcpSecurityClient().SecurityContextConstraints().Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) UpdateSecurityContextConstraints(securityContextConstraints *ocp_securityv1_api.SecurityContextConstraints) (result *ocp_securityv1_api.SecurityContextConstraints, err error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.getOcpSecurityClient().SecurityContextConstraints().Update(securityContextConstraints)
}

// Security Context Constraints APIs - END

// DeploymentConfig APIs - BEGIN

func (k *k8sOps) ListDeploymentConfigs(namespace string) (*ocp_appsv1_api.DeploymentConfigList, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.ocpClient.AppsV1().DeploymentConfigs(namespace).List(meta_v1.ListOptions{})
}

func (k *k8sOps) GetDeploymentConfig(name, namespace string) (*ocp_appsv1_api.DeploymentConfig, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	return k.ocpClient.AppsV1().DeploymentConfigs(namespace).Get(name, meta_v1.GetOptions{})
}

func (k *k8sOps) CreateDeploymentConfig(deployment *ocp_appsv1_api.DeploymentConfig) (*ocp_appsv1_api.DeploymentConfig, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	ns := deployment.Namespace
	if len(ns) == 0 {
		ns = v1.NamespaceDefault
	}

	return k.ocpClient.AppsV1().DeploymentConfigs(ns).Create(deployment)
}

func (k *k8sOps) DeleteDeploymentConfig(name, namespace string) error {
	if err := k.initK8sClient(); err != nil {
		return err
	}

	return k.ocpClient.AppsV1().DeploymentConfigs(namespace).Delete(name, &meta_v1.DeleteOptions{
		PropagationPolicy: &deleteForegroundPolicy,
	})
}

func (k *k8sOps) DescribeDeploymentConfig(depName, depNamespace string) (*ocp_appsv1_api.DeploymentConfigStatus, error) {
	dep, err := k.GetDeploymentConfig(depName, depNamespace)
	if err != nil {
		return nil, err
	}
	return &dep.Status, err
}

func (k *k8sOps) UpdateDeploymentConfig(deployment *ocp_appsv1_api.DeploymentConfig) (*ocp_appsv1_api.DeploymentConfig, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}
	return k.ocpClient.AppsV1().DeploymentConfigs(deployment.Namespace).Update(deployment)
}

func (k *k8sOps) ValidateDeploymentConfig(deployment *ocp_appsv1_api.DeploymentConfig, timeout, retryInterval time.Duration) error {
	t := func() (interface{}, bool, error) {
		dep, err := k.GetDeploymentConfig(deployment.Name, deployment.Namespace)
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

					claim, err := k.client.CoreV1().
						PersistentVolumeClaims(dep.Namespace).
						Get(vol.PersistentVolumeClaim.ClaimName, meta_v1.GetOptions{})
					if err != nil {
						return "", true, err
					}

					if k.isPVCShared(claim) {
						shared = true
						break
					}
				}
			}

			if foundPVC && !shared {
				requiredReplicas = 1
			}
		}

		pods, err := k.GetDeploymentConfigPods(deployment)
		if err != nil || pods == nil {
			return "", true, &ErrAppNotReady{
				ID:    dep.Name,
				Cause: fmt.Sprintf("Failed to get pods for deployment. Err: %v", err),
			}
		}

		if len(pods) == 0 {
			return "", true, &ErrAppNotReady{
				ID:    dep.Name,
				Cause: "DeploymentConfig has 0 pods",
			}
		}
		podsOverviewString := k.generatePodsOverviewString(pods)
		if requiredReplicas > dep.Status.AvailableReplicas {
			return "", true, &ErrAppNotReady{
				ID: dep.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Available replicas: %v Current pods overview:\n%s",
					requiredReplicas, dep.Status.AvailableReplicas, podsOverviewString),
			}
		}

		if requiredReplicas > dep.Status.ReadyReplicas {
			return "", true, &ErrAppNotReady{
				ID: dep.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Ready replicas: %v Current pods overview:\n%s",
					requiredReplicas, dep.Status.ReadyReplicas, podsOverviewString),
			}
		}

		if requiredReplicas != dep.Status.UpdatedReplicas && shared {
			return "", true, &ErrAppNotReady{
				ID: dep.Name,
				Cause: fmt.Sprintf("Expected replicas: %v Updated replicas: %v Current pods overview:\n%s",
					requiredReplicas, dep.Status.UpdatedReplicas, podsOverviewString),
			}
		}

		// look for "requiredReplicas" number of pods in ready state
		var notReadyPods []string
		var readyCount int32
		for _, pod := range pods {
			if !k.IsPodReady(pod) {
				notReadyPods = append(notReadyPods, pod.Name)
			} else {
				readyCount++
			}
		}

		if readyCount >= requiredReplicas {
			return "", false, nil
		}

		return "", true, &ErrAppNotReady{
			ID:    dep.Name,
			Cause: fmt.Sprintf("Pod(s): %#v not yet ready", notReadyPods),
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	return nil
}

func (k *k8sOps) ValidateTerminatedDeploymentConfig(deployment *ocp_appsv1_api.DeploymentConfig) error {
	t := func() (interface{}, bool, error) {
		dep, err := k.GetDeploymentConfig(deployment.Name, deployment.Namespace)
		if err != nil {
			if errors.IsNotFound(err) {
				return "", false, nil
			}
			return "", true, err
		}

		pods, err := k.GetDeploymentConfigPods(deployment)
		if err != nil {
			return "", true, &ErrAppNotTerminated{
				ID:    dep.Name,
				Cause: fmt.Sprintf("Failed to get pods for deployment. Err: %v", err),
			}
		}

		if pods != nil && len(pods) > 0 {
			var podNames []string
			for _, pod := range pods {
				podNames = append(podNames, pod.Name)
			}
			return "", true, &ErrAppNotTerminated{
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

func (k *k8sOps) GetDeploymentConfigPods(deployment *ocp_appsv1_api.DeploymentConfig) ([]v1.Pod, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	rSets, err := k.client.AppsV1().ReplicaSets(deployment.Namespace).List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, rSet := range rSets.Items {
		for _, owner := range rSet.OwnerReferences {
			if owner.Name == deployment.Name {
				return k.GetPodsByOwner(rSet.UID, rSet.Namespace)
			}
		}
	}

	return nil, nil
}

func (k *k8sOps) GetDeploymentConfigsUsingStorageClass(scName string) ([]ocp_appsv1_api.DeploymentConfig, error) {
	if err := k.initK8sClient(); err != nil {
		return nil, err
	}

	deps, err := k.ocpClient.AppsV1().DeploymentConfigs("").List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var retList []ocp_appsv1_api.DeploymentConfig
	for _, dep := range deps.Items {
		for _, v := range dep.Spec.Template.Spec.Volumes {
			if v.PersistentVolumeClaim == nil {
				continue
			}

			pvc, err := k.GetPersistentVolumeClaim(v.PersistentVolumeClaim.ClaimName, dep.Namespace)
			if err != nil {
				continue // don't let one bad pvc stop processing
			}

			sc, err := k.getStorageClassForPVC(pvc)
			if err == nil && sc.Name == scName {
				retList = append(retList, dep)
				break
			}
		}
	}

	return retList, nil
}

// DeploymentConfig APIs - END
