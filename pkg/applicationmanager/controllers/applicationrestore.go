package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/apis/stork"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/crypto"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/objectstore"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// NewApplicationRestore creates a new instance of ApplicationRestoreController.
func NewApplicationRestore(mgr manager.Manager, r record.EventRecorder, rc resourcecollector.ResourceCollector) *ApplicationRestoreController {
	return &ApplicationRestoreController{
		client:            mgr.GetClient(),
		recorder:          r,
		resourceCollector: rc,
	}
}

// ApplicationRestoreController reconciles applicationrestore objects
type ApplicationRestoreController struct {
	client runtimeclient.Client

	recorder              record.EventRecorder
	resourceCollector     resourcecollector.ResourceCollector
	dynamicInterface      dynamic.Interface
	restoreAdminNamespace string
}

// Init Initialize the application restore controller
func (a *ApplicationRestoreController) Init(mgr manager.Manager, restoreAdminNamespace string) error {
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

	return controllers.RegisterTo(mgr, "application-restore-controller", a, &storkapi.ApplicationRestore{})
}

func (a *ApplicationRestoreController) setDefaults(restore *storkapi.ApplicationRestore) error {
	if restore.Spec.ReplacePolicy == "" {
		restore.Spec.ReplacePolicy = storkapi.ApplicationRestoreReplacePolicyRetain
	}
	// If no namespaces mappings are provided add mappings for all of them
	if len(restore.Spec.NamespaceMapping) == 0 {
		backup, err := storkops.Instance().GetApplicationBackup(restore.Spec.BackupName, restore.Namespace)
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

func (a *ApplicationRestoreController) verifyNamespaces(restore *storkapi.ApplicationRestore) error {
	// Check whether namespace is allowed to be restored to before each stage
	// Restrict restores to only the namespace that the object belongs
	// except for the namespace designated by the admin
	if !a.namespaceRestoreAllowed(restore) {
		return fmt.Errorf("Spec.Namespaces should only contain the current namespace")
	}
	backup, err := storkops.Instance().GetApplicationBackup(restore.Spec.BackupName, restore.Namespace)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error getting backup: %v", err)
		return err
	}
	return a.createNamespaces(backup, restore.Spec.BackupLocation, restore)
}

func (a *ApplicationRestoreController) createNamespaces(backup *storkapi.ApplicationBackup,
	backupLocation string,
	restore *storkapi.ApplicationRestore) error {
	var namespaces []*v1.Namespace

	nsData, err := a.downloadObject(backup, backupLocation, restore.Namespace, nsObjectName, true)
	if err != nil {
		return err
	}
	if nsData != nil {
		if err = json.Unmarshal(nsData, &namespaces); err != nil {
			return err
		}
		for _, ns := range namespaces {
			if restoreNS, ok := restore.Spec.NamespaceMapping[ns.Name]; ok {
				ns.Name = restoreNS
			} else {
				// Skip namespaces we aren't restoring
				continue
			}
			// create mapped restore namespace with metadata of backed up
			// namespace
			_, err := core.Instance().CreateNamespace(&v1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:        ns.Name,
					Labels:      ns.Labels,
					Annotations: ns.GetAnnotations(),
				},
			})
			log.ApplicationRestoreLog(restore).Infof("Creating dest namespace %v", ns.Name)
			if err != nil {
				if errors.IsAlreadyExists(err) {
					log.ApplicationRestoreLog(restore).Warnf("Namespace already exists, updating dest namespace %v", ns.Name)
					// regardless of replace policy we should always update namespace is
					// its already exist to keep latest annotations/labels
					_, err = core.Instance().UpdateNamespace(&v1.Namespace{
						ObjectMeta: metav1.ObjectMeta{
							Name:        ns.Name,
							Labels:      ns.Labels,
							Annotations: ns.GetAnnotations(),
						},
					})
					if err != nil {
						return err
					}
					continue
				}
				return err
			}
		}
		return nil
	}
	for _, namespace := range restore.Spec.NamespaceMapping {
		if ns, err := core.Instance().GetNamespace(namespace); err != nil {
			if errors.IsNotFound(err) {
				if _, err := core.Instance().CreateNamespace(&v1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name:        ns.Name,
						Labels:      ns.Labels,
						Annotations: ns.GetAnnotations(),
					},
				}); err != nil {
					return err
				}
			}
			return err
		}
	}
	return nil
}

// Reconcile updates for ApplicationRestore objects.
func (a *ApplicationRestoreController) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	logrus.Tracef("Reconciling ApplicationRestore %s/%s", request.Namespace, request.Name)

	// Fetch the ApplicationBackup instance
	restore := &storkapi.ApplicationRestore{}
	err := a.client.Get(context.TODO(), request.NamespacedName, restore)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	if !controllers.ContainsFinalizer(restore, controllers.FinalizerCleanup) {
		controllers.SetFinalizer(restore, controllers.FinalizerCleanup)
		return reconcile.Result{Requeue: true}, a.client.Update(context.TODO(), restore)
	}

	if err = a.handle(context.TODO(), restore); err != nil {
		logrus.Errorf("%s: %s/%s: %s", reflect.TypeOf(a), restore.Namespace, restore.Name, err)
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}

// Handle updates for ApplicationRestore objects
func (a *ApplicationRestoreController) handle(ctx context.Context, restore *storkapi.ApplicationRestore) error {
	if restore.DeletionTimestamp != nil {
		if controllers.ContainsFinalizer(restore, controllers.FinalizerCleanup) {
			if err := a.cleanupRestore(restore); err != nil {
				logrus.Errorf("%s: cleanup: %s", reflect.TypeOf(a), err)
			}
		}

		if restore.GetFinalizers() != nil {
			controllers.RemoveFinalizer(restore, controllers.FinalizerCleanup)
			return a.client.Update(ctx, restore)
		}

		return nil
	}

	err := a.setDefaults(restore)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf(err.Error())
		a.recorder.Event(restore,
			v1.EventTypeWarning,
			string(storkapi.ApplicationRestoreStatusFailed),
			err.Error())
		return nil
	}

	err = a.verifyNamespaces(restore)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf(err.Error())
		a.recorder.Event(restore,
			v1.EventTypeWarning,
			string(storkapi.ApplicationRestoreStatusFailed),
			err.Error())
		return nil
	}

	switch restore.Status.Stage {
	case storkapi.ApplicationRestoreStageInitial:
		// Make sure the namespaces exist
		fallthrough
	case storkapi.ApplicationRestoreStageVolumes:
		err := a.restoreVolumes(restore)
		if err != nil {
			message := fmt.Sprintf("Error restoring volumes: %v", err)
			log.ApplicationRestoreLog(restore).Errorf(message)
			a.recorder.Event(restore,
				v1.EventTypeWarning,
				string(storkapi.ApplicationRestoreStatusFailed),
				message)
			return nil
		}
	case storkapi.ApplicationRestoreStageApplications:
		err := a.restoreResources(restore)
		if err != nil {
			message := fmt.Sprintf("Error restoring resources: %v", err)
			log.ApplicationRestoreLog(restore).Errorf(message)
			a.recorder.Event(restore,
				v1.EventTypeWarning,
				string(storkapi.ApplicationRestoreStatusFailed),
				message)
			return nil
		}

	case storkapi.ApplicationRestoreStageFinal:
		// Do Nothing
		return nil
	default:
		log.ApplicationRestoreLog(restore).Errorf("Invalid stage for restore: %v", restore.Status.Stage)
	}

	return nil
}

func (a *ApplicationRestoreController) namespaceRestoreAllowed(restore *storkapi.ApplicationRestore) bool {
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

func (a *ApplicationRestoreController) getDriversForRestore(restore *storkapi.ApplicationRestore) map[string]bool {
	drivers := make(map[string]bool)
	for _, volumeInfo := range restore.Status.Volumes {
		drivers[volumeInfo.DriverName] = true
	}
	return drivers
}

func (a *ApplicationRestoreController) restoreVolumes(restore *storkapi.ApplicationRestore) error {
	restore.Status.Stage = storkapi.ApplicationRestoreStageVolumes
	if restore.Status.Volumes == nil || len(restore.Status.Volumes) == 0 {
		backup, err := storkops.Instance().GetApplicationBackup(restore.Spec.BackupName, restore.Namespace)
		if err != nil {
			return fmt.Errorf("error getting backup spec for restore: %v", err)
		}
		backupVolumeInfoMappings := make(map[string][]*storkapi.ApplicationBackupVolumeInfo)
		objectMap := storkapi.CreateObjectsMap(restore.Spec.IncludeResources)
		info := storkapi.ObjectInfo{
			GroupVersionKind: metav1.GroupVersionKind{
				Group:   "core",
				Version: "v1",
				Kind:    "PersistentVolumeClaim",
			},
		}

		for _, namespace := range backup.Spec.Namespaces {
			if _, ok := restore.Spec.NamespaceMapping[namespace]; !ok {
				continue
			}
			for _, volumeBackup := range backup.Status.Volumes {
				if volumeBackup.Namespace != namespace {
					continue
				}
				// If a list of resources was specified during restore check if
				// this PVC was included
				info.Name = volumeBackup.PersistentVolumeClaim
				info.Namespace = volumeBackup.Namespace
				if len(objectMap) != 0 {
					if val, present := objectMap[info]; !present || !val {
						continue
					}
				}

				if volumeBackup.DriverName == "" {
					volumeBackup.DriverName = volume.GetDefaultDriverName()
				}
				if backupVolumeInfoMappings[volumeBackup.DriverName] == nil {
					backupVolumeInfoMappings[volumeBackup.DriverName] = make([]*storkapi.ApplicationBackupVolumeInfo, 0)
				}
				backupVolumeInfoMappings[volumeBackup.DriverName] = append(backupVolumeInfoMappings[volumeBackup.DriverName], volumeBackup)
			}
		}

		hasCSIDriverVolume := false
		for driverName, vInfos := range backupVolumeInfoMappings {
			driver, err := volume.Get(driverName)
			if err != nil {
				return err
			}

			// For each driver, check if it needs any additional resources to be
			// restored before starting the volume restore
			objects, err := a.downloadResources(backup, restore.Spec.BackupLocation, restore.Namespace)
			if err != nil {
				log.ApplicationRestoreLog(restore).Errorf("Error downloading resources: %v", err)
				return err
			}

			preRestoreObjects, err := driver.GetPreRestoreResources(backup, objects)
			if err != nil {
				log.ApplicationRestoreLog(restore).Errorf("Error getting PreRestore Resources: %v", err)
				return err
			}
			if err := a.applyResources(restore, preRestoreObjects); err != nil {
				return err
			}

			if driver.String() == "csi" {
				hasCSIDriverVolume = true
			}

			restoreVolumeInfos, err := driver.StartRestore(restore, vInfos)
			if err != nil {
				message := fmt.Sprintf("Error starting Application Restore for volumes: %v", err)
				log.ApplicationRestoreLog(restore).Errorf(message)
				a.recorder.Event(restore,
					v1.EventTypeWarning,
					string(storkapi.ApplicationRestoreStatusFailed),
					message)
				restore.Status.Status = storkapi.ApplicationRestoreStatusFailed
				restore.Status.Stage = storkapi.ApplicationRestoreStageFinal
				restore.Status.Reason = message
				err = a.client.Update(context.TODO(), restore)
				if err != nil {
					return err
				}

				return nil
			}
			restore.Status.Volumes = append(restore.Status.Volumes, restoreVolumeInfos...)
		}

		if hasCSIDriverVolume && restore.Spec.ReplacePolicy == storkapi.ApplicationRestoreReplacePolicyDelete {
			err := a.csiPreDeleteResources(restore)
			if err != nil {
				return fmt.Errorf("error pre-deleting resources for CSI restore: %v", err)
			}
		}

		restore.Status.Status = storkapi.ApplicationRestoreStatusInProgress
		restore.Status.LastUpdateTimestamp = metav1.Now()
		err = a.client.Update(context.TODO(), restore)
		if err != nil {
			return err
		}
	}

	inProgress := false
	// Skip checking status if no volumes are being restored
	if len(restore.Status.Volumes) != 0 {
		drivers := a.getDriversForRestore(restore)
		volumeInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)

		var err error
		for driverName := range drivers {
			driver, err := volume.Get(driverName)
			if err != nil {
				return err
			}

			status, err := driver.GetRestoreStatus(restore)
			if err != nil {
				return fmt.Errorf("error getting restore status for driver %v: %v", driverName, err)
			}
			volumeInfos = append(volumeInfos, status...)
		}

		restore.Status.Volumes = volumeInfos
		restore.Status.LastUpdateTimestamp = metav1.Now()
		// Store the new status
		err = a.client.Update(context.TODO(), restore)
		if err != nil {
			return err
		}

		// Now check if there is any failure or success
		// TODO: On failure of one volume cancel other restores?
		for _, vInfo := range volumeInfos {
			if vInfo.Status == storkapi.ApplicationRestoreStatusInProgress || vInfo.Status == storkapi.ApplicationRestoreStatusInitial ||
				vInfo.Status == storkapi.ApplicationRestoreStatusPending || vInfo.Status == storkapi.ApplicationRestoreStatusDeletingPrevious {
				log.ApplicationRestoreLog(restore).Infof("Volume restore still in progress: %v->%v", vInfo.SourceVolume, vInfo.RestoreVolume)
				inProgress = true
			} else if vInfo.Status == storkapi.ApplicationRestoreStatusFailed {
				a.recorder.Event(restore,
					v1.EventTypeWarning,
					string(vInfo.Status),
					fmt.Sprintf("Error restoring volume %v->%v: %v", vInfo.SourceVolume, vInfo.RestoreVolume, vInfo.Reason))
				restore.Status.Stage = storkapi.ApplicationRestoreStageFinal
				restore.Status.FinishTimestamp = metav1.Now()
				restore.Status.Status = storkapi.ApplicationRestoreStatusFailed
				restore.Status.Reason = vInfo.Reason
				break
			} else if vInfo.Status == storkapi.ApplicationRestoreStatusSuccessful {
				a.recorder.Event(restore,
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
	if restore.Status.Status != storkapi.ApplicationRestoreStatusFailed {
		restore.Status.Stage = storkapi.ApplicationRestoreStageApplications
		restore.Status.Status = storkapi.ApplicationRestoreStatusInProgress
		restore.Status.Reason = "Application resources restore is in progress"
		restore.Status.LastUpdateTimestamp = metav1.Now()
		// Update the current state and then move on to restoring resources
		err := a.client.Update(context.TODO(), restore)
		if err != nil {
			return err
		}
		err = a.restoreResources(restore)
		if err != nil {
			log.ApplicationRestoreLog(restore).Errorf("Error restoring resources: %v", err)
			return err
		}
	}

	restore.Status.LastUpdateTimestamp = metav1.Now()
	// Only on success compute the total restore size
	for _, vInfo := range restore.Status.Volumes {
		restore.Status.TotalSize += vInfo.TotalSize
	}

	err := a.client.Update(context.TODO(), restore)
	if err != nil {
		return err
	}
	return nil
}

func (a *ApplicationRestoreController) downloadObject(
	backup *storkapi.ApplicationBackup,
	backupLocation string,
	namespace string,
	objectName string,
	skipIfNotPresent bool,
) ([]byte, error) {
	restoreLocation, err := storkops.Instance().GetBackupLocation(backup.Spec.BackupLocation, namespace)
	if err != nil {
		return nil, err
	}
	bucket, err := objectstore.GetBucket(restoreLocation)
	if err != nil {
		return nil, err
	}

	objectPath := backup.Status.BackupPath
	if skipIfNotPresent {
		exists, err := bucket.Exists(context.TODO(), filepath.Join(objectPath, objectName))
		if err != nil || !exists {
			return nil, nil
		}
	}

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
	backup *storkapi.ApplicationBackup,
	backupLocation string,
	namespace string,
) ([]runtime.Unstructured, error) {
	// create CRD resource first
	if err := a.downloadCRD(backup, backupLocation, namespace); err != nil {
		return nil, fmt.Errorf("error downloading CRDs: %v", err)
	}
	data, err := a.downloadObject(backup, backupLocation, namespace, resourceObjectName, false)
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

func (a *ApplicationRestoreController) downloadCRD(
	backup *storkapi.ApplicationBackup,
	backupLocation string,
	namespace string,
) error {
	var crds []*apiextensionsv1beta1.CustomResourceDefinition
	crdData, err := a.downloadObject(backup, backupLocation, namespace, crdObjectName, true)
	if err != nil {
		return err
	}
	// No CRDs were uploaded
	if crdData == nil {
		return nil
	}
	if err = json.Unmarshal(crdData, &crds); err != nil {
		return err
	}
	for _, crd := range crds {
		crd.ResourceVersion = ""
		if err := apiextensions.Instance().RegisterCRD(crd); err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
		if err := apiextensions.Instance().ValidateCRD(apiextensions.CustomResource{
			Plural: crd.Spec.Names.Plural,
			Group:  crd.Spec.Group}, validateCRDTimeout, validateCRDInterval); err != nil {
			return err
		}
	}
	return nil
}

func (a *ApplicationRestoreController) updateResourceStatus(
	restore *storkapi.ApplicationRestore,
	object runtime.Unstructured,
	status storkapi.ApplicationRestoreStatusType,
	reason string,
) error {
	var updatedResource *storkapi.ApplicationRestoreResourceInfo
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
		updatedResource = &storkapi.ApplicationRestoreResourceInfo{
			ObjectInfo: storkapi.ObjectInfo{
				Name:      metadata.GetName(),
				Namespace: metadata.GetNamespace(),
				GroupVersionKind: metav1.GroupVersionKind{
					Group:   gkv.Group,
					Version: gkv.Version,
					Kind:    gkv.Kind,
				},
			},
		}
		restore.Status.Resources = append(restore.Status.Resources, updatedResource)
	}

	updatedResource.Status = status
	updatedResource.Reason = reason
	eventType := v1.EventTypeNormal
	if status == storkapi.ApplicationRestoreStatusFailed {
		eventType = v1.EventTypeWarning
	}
	eventMessage := fmt.Sprintf("%v %v/%v: %v",
		gkv,
		updatedResource.Namespace,
		updatedResource.Name,
		reason)
	a.recorder.Event(restore, eventType, string(status), eventMessage)
	return nil
}

func (a *ApplicationRestoreController) getPVNameMappings(
	restore *storkapi.ApplicationRestore,
	objects []runtime.Unstructured,
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

// csiPreDeleteResources must delete resources prior to creating volumes
func (a *ApplicationRestoreController) csiPreDeleteResources(restore *storkapi.ApplicationRestore) error {
	backup, err := storkops.Instance().GetApplicationBackup(restore.Spec.BackupName, restore.Namespace)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error getting backup: %v", err)
		return err
	}

	objects, err := a.downloadResources(backup, restore.Spec.BackupLocation, restore.Namespace)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error downloading resources: %v", err)
		return err
	}

	objectMap := storkapi.CreateObjectsMap(restore.Spec.IncludeResources)
	tempObjects := make([]runtime.Unstructured, 0)
	emptyPVMapping := make(map[string]string)
	for _, o := range objects {
		skip, err := a.resourceCollector.PrepareResourceForApply(
			o,
			objects,
			objectMap,
			restore.Spec.NamespaceMapping,
			emptyPVMapping,
			restore.Spec.IncludeOptionalResourceTypes)
		if err != nil {
			return err
		}
		if !skip {
			tempObjects = append(tempObjects, o)
		}
	}
	objects = tempObjects

	// Delete the existing objects
	err = a.resourceCollector.DeleteResources(
		a.dynamicInterface,
		objects)
	if err != nil {
		return err
	}

	return nil
}

func (a *ApplicationRestoreController) applyResources(
	restore *storkapi.ApplicationRestore,
	objects []runtime.Unstructured,
) error {
	// select resources to apply
	pvNameMappings, err := a.getPVNameMappings(restore, objects)
	if err != nil {
		return err
	}

	objectMap := storkapi.CreateObjectsMap(restore.Spec.IncludeResources)
	tempObjects := make([]runtime.Unstructured, 0)
	for _, o := range objects {
		skip, err := a.resourceCollector.PrepareResourceForApply(
			o,
			objects,
			objectMap,
			restore.Spec.NamespaceMapping,
			pvNameMappings,
			restore.Spec.IncludeOptionalResourceTypes)
		if err != nil {
			return err
		}
		if !skip {
			tempObjects = append(tempObjects, o)
		}
	}
	objects = tempObjects

	// First delete the existing objects if they exist and replace policy is set
	// to Delete
	if restore.Spec.ReplacePolicy == storkapi.ApplicationRestoreReplacePolicyDelete {
		err = a.resourceCollector.DeleteResources(
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

		log.ApplicationRestoreLog(restore).Infof("Applying %v %v/%v", objectType.GetKind(), metadata.GetNamespace(), metadata.GetName())
		retained := false

		err = a.resourceCollector.ApplyResource(
			a.dynamicInterface,
			o)
		if err != nil && errors.IsAlreadyExists(err) {
			switch restore.Spec.ReplacePolicy {
			case storkapi.ApplicationRestoreReplacePolicyDelete:
				log.ApplicationRestoreLog(restore).Errorf("Error deleting %v %v during restore: %v", objectType.GetKind(), metadata.GetName(), err)
			case storkapi.ApplicationRestoreReplacePolicyRetain:
				log.ApplicationRestoreLog(restore).Warningf("Error deleting %v %v during restore, ReplacePolicy set to Retain: %v", objectType.GetKind(), metadata.GetName(), err)
				retained = true
				err = nil
			}
		}

		if err != nil {
			if err := a.updateResourceStatus(
				restore,
				o,
				storkapi.ApplicationRestoreStatusFailed,
				fmt.Sprintf("Error applying resource: %v", err)); err != nil {
				return err
			}
		} else if retained {
			if err := a.updateResourceStatus(
				restore,
				o,
				storkapi.ApplicationRestoreStatusRetained,
				"Resource restore skipped as it was already present and ReplacePolicy is set to Retain"); err != nil {
				return err
			}
		} else {
			if err := a.updateResourceStatus(
				restore,
				o,
				storkapi.ApplicationRestoreStatusSuccessful,
				"Resource restored successfully"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *ApplicationRestoreController) restoreResources(
	restore *storkapi.ApplicationRestore,
) error {
	backup, err := storkops.Instance().GetApplicationBackup(restore.Spec.BackupName, restore.Namespace)
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

	restore.Status.Stage = storkapi.ApplicationRestoreStageFinal
	restore.Status.FinishTimestamp = metav1.Now()
	restore.Status.Status = storkapi.ApplicationRestoreStatusSuccessful
	restore.Status.Reason = "Volumes and resources were restored up successfully"
	for _, resource := range restore.Status.Resources {
		if resource.Status != storkapi.ApplicationRestoreStatusSuccessful {
			restore.Status.Status = storkapi.ApplicationRestoreStatusPartialSuccess
			restore.Status.Reason = "Volumes were restored successfully. Some existing resources were not replaced"
			break
		}
	}

	restore.Status.LastUpdateTimestamp = metav1.Now()
	if err := a.client.Update(context.TODO(), restore); err != nil {
		return err
	}

	return nil
}

func (a *ApplicationRestoreController) cleanupRestore(restore *storkapi.ApplicationRestore) error {
	drivers := a.getDriversForRestore(restore)
	for driverName := range drivers {
		driver, err := volume.Get(driverName)
		if err != nil {
			return fmt.Errorf("get %s driver: %s", driverName, err)
		}
		if err = driver.CancelRestore(restore); err != nil {
			return fmt.Errorf("cancel restore: %s", err)
		}
	}
	return nil
}

func (a *ApplicationRestoreController) createCRD() error {
	resource := apiextensions.CustomResource{
		Name:    storkapi.ApplicationRestoreResourceName,
		Plural:  storkapi.ApplicationRestoreResourcePlural,
		Group:   stork.GroupName,
		Version: storkapi.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(storkapi.ApplicationRestore{}).Name(),
	}
	err := apiextensions.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return apiextensions.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}
