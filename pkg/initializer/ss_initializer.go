package initializer

import (
	"encoding/json"

	storklog "github.com/libopenstorage/stork/pkg/log"
	appv1 "k8s.io/api/apps/v1"
	appv1beta1 "k8s.io/api/apps/v1beta1"
	appv1beta2 "k8s.io/api/apps/v1beta2"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes"
)

func (i *Initializer) initializeStatefulSetV1(ss *appv1.StatefulSet, clientset *kubernetes.Clientset) error {
	if ss.ObjectMeta.GetInitializers() == nil {
		return nil
	}

	pendingInitializers := ss.ObjectMeta.GetInitializers().Pending
	if storkInitializerName != pendingInitializers[0].Name {
		return nil
	}

	oldData, err := json.Marshal(ss)
	if err != nil {
		return err
	}

	updatedStatefulSet := ss.DeepCopy()

	if len(pendingInitializers) == 1 {
		updatedStatefulSet.ObjectMeta.Initializers.Pending = nil
	} else if len(pendingInitializers) > 1 {
		updatedStatefulSet.ObjectMeta.Initializers.Pending = append(pendingInitializers[:0], pendingInitializers[1:]...)
	}

	// Only check to update scheduler name if it is set to the default
	if ss.Spec.Template.Spec.SchedulerName == defaultSchedulerName {
		// Remove the initializer even if we get errors in this step
		driverVolumeTemplates, err := i.Driver.GetVolumeClaimTemplates(ss.Spec.VolumeClaimTemplates)
		if err != nil {
			storklog.StatefulSetV1Log(ss).Infof("Error getting volume templates for statefulset: %v", err)
		} else if len(driverVolumeTemplates) > 0 {
			updatedStatefulSet.Spec.Template.Spec.SchedulerName = storkSchedulerName
		}
	}

	newData, err := json.Marshal(updatedStatefulSet)
	if err != nil {
		return err
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, appv1.StatefulSet{})
	if err != nil {
		return err
	}

	_, err = clientset.AppsV1().StatefulSets(ss.Namespace).Patch(ss.Name, types.StrategicMergePatchType, patchBytes)
	if err != nil {
		return err
	}
	return nil
}

func (i *Initializer) initializeStatefulSetV1Beta1(ss *appv1beta1.StatefulSet, clientset *kubernetes.Clientset) error {
	if ss.ObjectMeta.GetInitializers() == nil {
		return nil
	}

	pendingInitializers := ss.ObjectMeta.GetInitializers().Pending
	if storkInitializerName != pendingInitializers[0].Name {
		return nil
	}

	oldData, err := json.Marshal(ss)
	if err != nil {
		return err
	}

	updatedStatefulSet := ss.DeepCopy()

	if len(pendingInitializers) == 1 {
		updatedStatefulSet.ObjectMeta.Initializers.Pending = nil
	} else if len(pendingInitializers) > 1 {
		updatedStatefulSet.ObjectMeta.Initializers.Pending = append(pendingInitializers[:0], pendingInitializers[1:]...)
	}

	// Only check to update scheduler name if it is set to the default
	if ss.Spec.Template.Spec.SchedulerName == defaultSchedulerName {
		// Remove the initializer even if we get errors in this step
		driverVolumeTemplates, err := i.Driver.GetVolumeClaimTemplates(ss.Spec.VolumeClaimTemplates)
		if err != nil {
			storklog.StatefulSetV1Beta1Log(ss).Infof("Error getting volume templates for statefulset: %v", err)
		} else if len(driverVolumeTemplates) > 0 {
			updatedStatefulSet.Spec.Template.Spec.SchedulerName = storkSchedulerName
		}
	}

	newData, err := json.Marshal(updatedStatefulSet)
	if err != nil {
		return err
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, appv1beta1.StatefulSet{})
	if err != nil {
		return err
	}

	_, err = clientset.AppsV1beta1().StatefulSets(ss.Namespace).Patch(ss.Name, types.StrategicMergePatchType, patchBytes)
	if err != nil {
		return err
	}
	return nil
}

func (i *Initializer) initializeStatefulSetV1Beta2(ss *appv1beta2.StatefulSet, clientset *kubernetes.Clientset) error {
	if ss.ObjectMeta.GetInitializers() == nil {
		return nil
	}

	pendingInitializers := ss.ObjectMeta.GetInitializers().Pending
	if storkInitializerName != pendingInitializers[0].Name {
		return nil
	}

	oldData, err := json.Marshal(ss)
	if err != nil {
		return err
	}

	updatedStatefulSet := ss.DeepCopy()

	if len(pendingInitializers) == 1 {
		updatedStatefulSet.ObjectMeta.Initializers.Pending = nil
	} else if len(pendingInitializers) > 1 {
		updatedStatefulSet.ObjectMeta.Initializers.Pending = append(pendingInitializers[:0], pendingInitializers[1:]...)
	}

	// Only check to update scheduler name if it is set to the default
	if ss.Spec.Template.Spec.SchedulerName == defaultSchedulerName {
		// Remove the initializer even if we get errors in this step
		driverVolumeTemplates, err := i.Driver.GetVolumeClaimTemplates(ss.Spec.VolumeClaimTemplates)
		if err != nil {
			storklog.StatefulSetV1Beta2Log(ss).Infof("Error getting volume templates for statefulset: %v", err)
		} else if len(driverVolumeTemplates) > 0 {
			updatedStatefulSet.Spec.Template.Spec.SchedulerName = storkSchedulerName
		}
	}

	newData, err := json.Marshal(updatedStatefulSet)
	if err != nil {
		return err
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, appv1beta2.StatefulSet{})
	if err != nil {
		return err
	}

	_, err = clientset.AppsV1beta2().StatefulSets(ss.Namespace).Patch(ss.Name, types.StrategicMergePatchType, patchBytes)
	if err != nil {
		return err
	}
	return nil
}
