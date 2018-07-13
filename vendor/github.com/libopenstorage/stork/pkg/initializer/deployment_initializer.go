package initializer

import (
	"encoding/json"

	"github.com/libopenstorage/stork/drivers/volume"
	storklog "github.com/libopenstorage/stork/pkg/log"
	appv1 "k8s.io/api/apps/v1"
	appv1beta1 "k8s.io/api/apps/v1beta1"
	appv1beta2 "k8s.io/api/apps/v1beta2"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes"
)

func (i *Initializer) initializeDeploymentV1(deployment *appv1.Deployment, clientset *kubernetes.Clientset) error {
	if deployment.ObjectMeta.GetInitializers() == nil {
		return nil
	}

	pendingInitializers := deployment.ObjectMeta.GetInitializers().Pending
	if storkInitializerName != pendingInitializers[0].Name {
		return nil
	}

	oldData, err := json.Marshal(deployment)
	if err != nil {
		return err
	}

	updatedDeployment := deployment.DeepCopy()

	if len(pendingInitializers) == 1 {
		updatedDeployment.ObjectMeta.Initializers.Pending = nil
	} else if len(pendingInitializers) > 1 {
		updatedDeployment.ObjectMeta.Initializers.Pending = append(pendingInitializers[:0], pendingInitializers[1:]...)
	}

	// Only check to update scheduler name if it is set to the default
	if deployment.Spec.Template.Spec.SchedulerName == defaultSchedulerName {
		// Remove the initializer even if we get errors in this step
		driverVolumes, err := i.Driver.GetPodVolumes(&deployment.Spec.Template.Spec, deployment.Namespace)
		if err != nil {
			if _, ok := err.(*volume.ErrPVCPending); ok {
				updatedDeployment.Spec.Template.Spec.SchedulerName = storkSchedulerName
			} else {
				storklog.DeploymentV1Log(deployment).Errorf("Error getting volumes for pod: %v", err)
			}
		} else if len(driverVolumes) != 0 {
			updatedDeployment.Spec.Template.Spec.SchedulerName = storkSchedulerName
		}
	}

	newData, err := json.Marshal(updatedDeployment)
	if err != nil {
		return err
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, appv1.Deployment{})
	if err != nil {
		return err
	}

	_, err = clientset.Extensions().Deployments(deployment.Namespace).Patch(deployment.Name, types.StrategicMergePatchType, patchBytes)
	if err != nil {
		return err
	}
	return nil
}

func (i *Initializer) initializeDeploymentV1Beta1(deployment *appv1beta1.Deployment, clientset *kubernetes.Clientset) error {
	if deployment.ObjectMeta.GetInitializers() == nil {
		return nil
	}

	pendingInitializers := deployment.ObjectMeta.GetInitializers().Pending
	if storkInitializerName != pendingInitializers[0].Name {
		return nil
	}

	oldData, err := json.Marshal(deployment)
	if err != nil {
		return err
	}

	updatedDeployment := deployment.DeepCopy()

	if len(pendingInitializers) == 1 {
		updatedDeployment.ObjectMeta.Initializers.Pending = nil
	} else if len(pendingInitializers) > 1 {
		updatedDeployment.ObjectMeta.Initializers.Pending = append(pendingInitializers[:0], pendingInitializers[1:]...)
	}

	// Only check to update scheduler name if it is set to the default
	if deployment.Spec.Template.Spec.SchedulerName == defaultSchedulerName {
		// Remove the initializer even if we get errors in this step
		driverVolumes, err := i.Driver.GetPodVolumes(&deployment.Spec.Template.Spec, deployment.Namespace)
		if err != nil {
			if _, ok := err.(*volume.ErrPVCPending); ok {
				updatedDeployment.Spec.Template.Spec.SchedulerName = storkSchedulerName
			} else {
				storklog.DeploymentV1Beta1Log(deployment).Errorf("Error getting volumes for pod: %v", err)
			}
		} else if len(driverVolumes) != 0 {
			updatedDeployment.Spec.Template.Spec.SchedulerName = storkSchedulerName
		}
	}

	newData, err := json.Marshal(updatedDeployment)
	if err != nil {
		return err
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, appv1beta1.Deployment{})
	if err != nil {
		return err
	}

	_, err = clientset.Extensions().Deployments(deployment.Namespace).Patch(deployment.Name, types.StrategicMergePatchType, patchBytes)
	if err != nil {
		return err
	}
	return nil
}

func (i *Initializer) initializeDeploymentV1Beta2(deployment *appv1beta2.Deployment, clientset *kubernetes.Clientset) error {
	if deployment.ObjectMeta.GetInitializers() == nil {
		return nil
	}

	pendingInitializers := deployment.ObjectMeta.GetInitializers().Pending
	if storkInitializerName != pendingInitializers[0].Name {
		return nil
	}

	oldData, err := json.Marshal(deployment)
	if err != nil {
		return err
	}

	updatedDeployment := deployment.DeepCopy()

	if len(pendingInitializers) == 1 {
		updatedDeployment.ObjectMeta.Initializers.Pending = nil
	} else if len(pendingInitializers) > 1 {
		updatedDeployment.ObjectMeta.Initializers.Pending = append(pendingInitializers[:0], pendingInitializers[1:]...)
	}

	// Only check to update scheduler name if it is set to the default
	if deployment.Spec.Template.Spec.SchedulerName == defaultSchedulerName {
		// Remove the initializer even if we get errors in this step
		driverVolumes, err := i.Driver.GetPodVolumes(&deployment.Spec.Template.Spec, deployment.Namespace)
		if err != nil {
			if _, ok := err.(*volume.ErrPVCPending); ok {
				updatedDeployment.Spec.Template.Spec.SchedulerName = storkSchedulerName
			} else {
				storklog.DeploymentV1Beta2Log(deployment).Errorf("Error getting volumes for pod: %v", err)
			}
		} else if len(driverVolumes) != 0 {
			updatedDeployment.Spec.Template.Spec.SchedulerName = storkSchedulerName
		}
	}

	newData, err := json.Marshal(updatedDeployment)
	if err != nil {
		return err
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, appv1beta2.Deployment{})
	if err != nil {
		return err
	}

	_, err = clientset.Extensions().Deployments(deployment.Namespace).Patch(deployment.Name, types.StrategicMergePatchType, patchBytes)
	if err != nil {
		return err
	}
	return nil
}
