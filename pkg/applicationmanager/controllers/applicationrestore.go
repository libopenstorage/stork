package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/apis/stork"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controller"
	"github.com/libopenstorage/stork/pkg/crypto"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/objectstore"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/portworx/sched-ops/k8s"
	"k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
)

// ApplicationRestoreController reconciles applicationrestore objects
type ApplicationRestoreController struct {
	Driver                volume.Driver
	Recorder              record.EventRecorder
	ResourceCollector     resourcecollector.ResourceCollector
	dynamicInterface      dynamic.Interface
	restoreAdminNamespace string
}

// Init Initialize the application restore controller
func (a *ApplicationRestoreController) Init(restoreAdminNamespace string) error {
	err := a.createCRD()
	if err != nil {
		return err
	}

	a.restoreAdminNamespace = restoreAdminNamespace

	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error getting cluster config: %v", err)
	}

	a.dynamicInterface, err = dynamic.NewForConfig(config)
	if err != nil {
		return err
	}

	return controller.Register(
		&schema.GroupVersionKind{
			Group:   stork.GroupName,
			Version: stork_api.SchemeGroupVersion.Version,
			Kind:    reflect.TypeOf(stork_api.ApplicationRestore{}).Name(),
		},
		"",
		resyncPeriod,
		a)
}

func (a *ApplicationRestoreController) setDefaults(restore *stork_api.ApplicationRestore) error {
	if restore.Spec.ReplacePolicy == "" {
		restore.Spec.ReplacePolicy = stork_api.ApplicationRestoreReplacePolicyRetain
	}
	// If no namespaces mappings are provided add mappings for all of them
	if len(restore.Spec.NamespaceMapping) == 0 {
		backup, err := k8s.Instance().GetApplicationBackup(restore.Spec.BackupName, restore.Namespace)
		if err != nil {
			return fmt.Errorf("error getting backup: %v", err)
		}
		if restore.Spec.NamespaceMapping == nil {
			restore.Spec.NamespaceMapping = make(map[string]string)
		}
		for _, ns := range backup.Spec.Namespaces {
			restore.Spec.NamespaceMapping[ns] = ns
		}
	}
	return nil
}

func (a *ApplicationRestoreController) verifyNamespaces(restore *stork_api.ApplicationRestore) error {
	// Check whether namespace is allowed to be restored to before each stage
	// Restrict restores to only the namespace that the object belongs
	// except for the namespace designated by the admin
	if !a.namespaceRestoreAllowed(restore) {
		return fmt.Errorf("Spec.Namespaces should only contain the current namespace")
	}

	for _, ns := range restore.Spec.NamespaceMapping {
		if _, err := k8s.Instance().GetNamespace(ns); err != nil {
			return err
		}
	}
	return nil
}

// Handle updates for ApplicationRestore objects
func (a *ApplicationRestoreController) Handle(ctx context.Context, event sdk.Event) error {
	switch o := event.Object.(type) {
	case *stork_api.ApplicationRestore:
		restore := o
		if event.Deleted {
			return a.Driver.CancelRestore(restore)
		}

		err := a.setDefaults(restore)
		if err != nil {
			log.ApplicationRestoreLog(restore).Errorf(err.Error())
			a.Recorder.Event(restore,
				v1.EventTypeWarning,
				string(stork_api.ApplicationRestoreStatusFailed),
				err.Error())
			return nil
		}

		err = a.verifyNamespaces(restore)
		if err != nil {
			log.ApplicationRestoreLog(restore).Errorf(err.Error())
			a.Recorder.Event(restore,
				v1.EventTypeWarning,
				string(stork_api.ApplicationRestoreStatusFailed),
				err.Error())
			return nil
		}

		var terminationChannels []chan bool

		switch restore.Status.Stage {
		case stork_api.ApplicationRestoreStageInitial:
			// Make sure the namespaces exist
			fallthrough
		case stork_api.ApplicationRestoreStageVolumes:
			err := a.restoreVolumes(restore, terminationChannels)
			if err != nil {
				message := fmt.Sprintf("Error restoring volumes: %v", err)
				log.ApplicationRestoreLog(restore).Errorf(message)
				a.Recorder.Event(restore,
					v1.EventTypeWarning,
					string(stork_api.ApplicationRestoreStatusFailed),
					message)
				return nil
			}
		case stork_api.ApplicationRestoreStageApplications:
			err := a.restoreResources(restore)
			if err != nil {
				message := fmt.Sprintf("Error restoring resources: %v", err)
				log.ApplicationRestoreLog(restore).Errorf(message)
				a.Recorder.Event(restore,
					v1.EventTypeWarning,
					string(stork_api.ApplicationRestoreStatusFailed),
					message)
				return nil
			}

		case stork_api.ApplicationRestoreStageFinal:
			// Do Nothing
			return nil
		default:
			log.ApplicationRestoreLog(restore).Errorf("Invalid stage for restore: %v", restore.Status.Stage)
		}
	}
	return nil
}

func (a *ApplicationRestoreController) namespaceRestoreAllowed(restore *stork_api.ApplicationRestore) bool {
	// Restrict restores to only the namespace that the object belongs
	// except for the namespace designated by the admin
	if restore.Namespace != a.restoreAdminNamespace {
		for _, ns := range restore.Spec.NamespaceMapping {
			if ns != restore.Namespace {
				return false
			}
		}
	}
	return true
}

func (a *ApplicationRestoreController) restoreVolumes(restore *stork_api.ApplicationRestore, terminationChannels []chan bool) error {
	defer func() {
		for _, channel := range terminationChannels {
			channel <- true
		}
	}()

	restore.Status.Stage = stork_api.ApplicationRestoreStageVolumes
	if restore.Status.Volumes == nil || len(restore.Status.Volumes) == 0 {
		volumeInfos, err := a.Driver.StartRestore(restore)
		if err != nil {
			message := fmt.Sprintf("Error starting Application Restore for volumes: %v", err)
			log.ApplicationRestoreLog(restore).Errorf(message)
			a.Recorder.Event(restore,
				v1.EventTypeWarning,
				string(stork_api.ApplicationRestoreStatusFailed),
				message)
			return nil
		}
		restore.Status.Volumes = volumeInfos
		restore.Status.Status = stork_api.ApplicationRestoreStatusInProgress
		err = sdk.Update(restore)
		if err != nil {
			return err
		}

	}

	inProgress := false
	// Skip checking status if no volumes are being restored
	if len(restore.Status.Volumes) != 0 {
		var err error
		volumeInfos, err := a.Driver.GetRestoreStatus(restore)
		if err != nil {
			return err
		}
		if volumeInfos == nil {
			volumeInfos = make([]*stork_api.ApplicationRestoreVolumeInfo, 0)
		}
		restore.Status.Volumes = volumeInfos
		// Store the new status
		err = sdk.Update(restore)
		if err != nil {
			return err
		}

		// Now check if there is any failure or success
		// TODO: On failure of one volume cancel other restores?
		for _, vInfo := range volumeInfos {
			if vInfo.Status == stork_api.ApplicationRestoreStatusInProgress || vInfo.Status == stork_api.ApplicationRestoreStatusInitial ||
				vInfo.Status == stork_api.ApplicationRestoreStatusPending {
				log.ApplicationRestoreLog(restore).Infof("Volume restore still in progress: %v->%v", vInfo.SourceVolume, vInfo.RestoreVolume)
				inProgress = true
			} else if vInfo.Status == stork_api.ApplicationRestoreStatusFailed {
				a.Recorder.Event(restore,
					v1.EventTypeWarning,
					string(vInfo.Status),
					fmt.Sprintf("Error restoring volume %v->%v: %v", vInfo.SourceVolume, vInfo.RestoreVolume, vInfo.Reason))
				restore.Status.Stage = stork_api.ApplicationRestoreStageFinal
				restore.Status.FinishTimestamp = metav1.Now()
				restore.Status.Status = stork_api.ApplicationRestoreStatusFailed
				break
			} else if vInfo.Status == stork_api.ApplicationRestoreStatusSuccessful {
				a.Recorder.Event(restore,
					v1.EventTypeNormal,
					string(vInfo.Status),
					fmt.Sprintf("Volume %v->%v restored successfully", vInfo.SourceVolume, vInfo.RestoreVolume))
			}
		}
	}

	// Return if we have any volume restores still in progress
	if inProgress {
		return nil
	}

	// If the restore hasn't failed move on to the next stage.
	if restore.Status.Status != stork_api.ApplicationRestoreStatusFailed {
		restore.Status.Stage = stork_api.ApplicationRestoreStageApplications
		restore.Status.Status = stork_api.ApplicationRestoreStatusInProgress
		// Update the current state and then move on to restoring resources
		err := sdk.Update(restore)
		if err != nil {
			return err
		}
		err = a.restoreResources(restore)
		if err != nil {
			log.ApplicationRestoreLog(restore).Errorf("Error restoring resources: %v", err)
			return err
		}
	}

	err := sdk.Update(restore)
	if err != nil {
		return err
	}
	return nil
}

func (a *ApplicationRestoreController) downloadObject(
	backup *stork_api.ApplicationBackup,
	backupLocation string,
	namespace string,
	objectName string,
) ([]byte, error) {
	restoreLocation, err := k8s.Instance().GetBackupLocation(backup.Spec.BackupLocation, namespace)
	if err != nil {
		return nil, err
	}
	bucket, err := objectstore.GetBucket(restoreLocation)
	if err != nil {
		return nil, err
	}

	objectPath := backup.Status.BackupPath
	data, err := bucket.ReadAll(context.TODO(), filepath.Join(objectPath, objectName))
	if err != nil {
		return nil, err
	}
	if restoreLocation.Location.EncryptionKey != "" {
		if data, err = crypto.Decrypt(data, restoreLocation.Location.EncryptionKey); err != nil {
			return nil, err
		}
	}

	return data, nil
}

func (a *ApplicationRestoreController) downloadResources(
	backup *stork_api.ApplicationBackup,
	backupLocation string,
	namespace string,
) ([]runtime.Unstructured, error) {
	data, err := a.downloadObject(backup, backupLocation, namespace, resourceObjectName)
	if err != nil {
		return nil, err
	}

	objects := make([]*unstructured.Unstructured, 0)
	if err = json.Unmarshal(data, &objects); err != nil {
		return nil, err
	}
	runtimeObjects := make([]runtime.Unstructured, 0)
	for _, o := range objects {
		runtimeObjects = append(runtimeObjects, o)
	}
	return runtimeObjects, nil
}

func (a *ApplicationRestoreController) updateResourceStatus(
	restore *stork_api.ApplicationRestore,
	object runtime.Unstructured,
	status stork_api.ApplicationRestoreStatusType,
	reason string,
) error {
	var updatedResource *stork_api.ApplicationRestoreResourceInfo
	gkv := object.GetObjectKind().GroupVersionKind()
	metadata, err := meta.Accessor(object)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error getting metadata for object %v %v", object, err)
		return err
	}
	for _, resource := range restore.Status.Resources {
		if resource.Name == metadata.GetName() &&
			resource.Namespace == metadata.GetNamespace() &&
			(resource.Group == gkv.Group || (resource.Group == "core" && gkv.Group == "")) &&
			resource.Version == gkv.Version &&
			resource.Kind == gkv.Kind {
			updatedResource = resource
			break
		}
	}
	if updatedResource == nil {
		updatedResource = &stork_api.ApplicationRestoreResourceInfo{
			Name:      metadata.GetName(),
			Namespace: metadata.GetNamespace(),
			GroupVersionKind: metav1.GroupVersionKind{
				Group:   gkv.Group,
				Version: gkv.Version,
				Kind:    gkv.Kind,
			},
		}
		restore.Status.Resources = append(restore.Status.Resources, updatedResource)
	}

	updatedResource.Status = status
	updatedResource.Reason = reason
	eventType := v1.EventTypeNormal
	if status == stork_api.ApplicationRestoreStatusFailed {
		eventType = v1.EventTypeWarning
	}
	eventMessage := fmt.Sprintf("%v %v/%v: %v",
		gkv,
		updatedResource.Namespace,
		updatedResource.Name,
		reason)
	a.Recorder.Event(restore, eventType, string(status), eventMessage)
	return nil
}

func (a *ApplicationRestoreController) getPVNameMappings(
	restore *stork_api.ApplicationRestore,
) (map[string]string, error) {
	pvNameMappings := make(map[string]string)
	for _, vInfo := range restore.Status.Volumes {
		if vInfo.SourceVolume == "" {
			return nil, fmt.Errorf("SourceVolume missing for restore")
		}
		if vInfo.RestoreVolume == "" {
			return nil, fmt.Errorf("RestoreVolume missing for restore")
		}
		pvNameMappings[vInfo.SourceVolume] = vInfo.RestoreVolume
	}
	return pvNameMappings, nil
}

func (a *ApplicationRestoreController) applyResources(
	restore *stork_api.ApplicationRestore,
	objects []runtime.Unstructured,
) error {
	pvNameMappings, err := a.getPVNameMappings(restore)
	if err != nil {
		return err
	}

	for _, o := range objects {
		err = a.ResourceCollector.PrepareResourceForApply(
			o,
			restore.Spec.NamespaceMapping,
			pvNameMappings)
		if err != nil {
			return err
		}
	}

	// First delete the existing objects if they exist and replace policy is set
	// to Delete
	if restore.Spec.ReplacePolicy == stork_api.ApplicationRestoreReplacePolicyDelete {
		err = a.ResourceCollector.DeleteResources(
			a.dynamicInterface,
			objects)
		if err != nil {
			return err
		}
	}

	for _, o := range objects {
		metadata, err := meta.Accessor(o)
		if err != nil {
			return err
		}
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return err
		}

		log.ApplicationRestoreLog(restore).Infof("Applying %v %v", objectType.GetKind(), metadata.GetName())
		retained := false

		err = a.ResourceCollector.ApplyResource(
			a.dynamicInterface,
			o)
		if err != nil && errors.IsAlreadyExists(err) {
			switch restore.Spec.ReplacePolicy {
			case stork_api.ApplicationRestoreReplacePolicyDelete:
				log.ApplicationRestoreLog(restore).Errorf("Error deleting %v %v during restore: %v", objectType.GetKind(), metadata.GetName(), err)
			case stork_api.ApplicationRestoreReplacePolicyRetain:
				log.ApplicationRestoreLog(restore).Warningf("Error deleting %v %v during restore, ReplacePolicy set to Retain: %v", objectType.GetKind(), metadata.GetName(), err)
				retained = true
				err = nil
			}
		}

		if err != nil {
			if err := a.updateResourceStatus(
				restore,
				o,
				stork_api.ApplicationRestoreStatusFailed,
				fmt.Sprintf("Error applying resource: %v", err)); err != nil {
				return err
			}
		} else if retained {
			if err := a.updateResourceStatus(
				restore,
				o,
				stork_api.ApplicationRestoreStatusRetained,
				"Resource restore skipped as it was already present and ReplacePolicy is set to Retain"); err != nil {
				return err
			}
		} else {
			if err := a.updateResourceStatus(
				restore,
				o,
				stork_api.ApplicationRestoreStatusSuccessful,
				"Resource restored successfully"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *ApplicationRestoreController) restoreResources(
	restore *stork_api.ApplicationRestore,
) error {
	backup, err := k8s.Instance().GetApplicationBackup(restore.Spec.BackupName, restore.Namespace)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error getting backup: %v", err)
		return err
	}

	objects, err := a.downloadResources(backup, restore.Spec.BackupLocation, restore.Namespace)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error downloading resources: %v", err)
		return err
	}

	if err := a.applyResources(restore, objects); err != nil {
		return err
	}

	restore.Status.Stage = stork_api.ApplicationRestoreStageFinal
	restore.Status.FinishTimestamp = metav1.Now()
	restore.Status.Status = stork_api.ApplicationRestoreStatusSuccessful
	for _, resource := range restore.Status.Resources {
		if resource.Status != stork_api.ApplicationRestoreStatusSuccessful {
			restore.Status.Status = stork_api.ApplicationRestoreStatusPartialSuccess
			break
		}
	}

	if err := sdk.Update(restore); err != nil {
		return err
	}

	return nil
}

func (a *ApplicationRestoreController) createCRD() error {
	resource := k8s.CustomResource{
		Name:    stork_api.ApplicationRestoreResourceName,
		Plural:  stork_api.ApplicationRestoreResourcePlural,
		Group:   stork.GroupName,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.ApplicationRestore{}).Name(),
	}
	err := k8s.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return k8s.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}
