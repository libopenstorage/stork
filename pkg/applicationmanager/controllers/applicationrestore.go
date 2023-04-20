package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/drivers/volume/kdmp"
	"github.com/libopenstorage/stork/pkg/apis/stork"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/crypto"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/objectstore"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/libopenstorage/stork/pkg/utils"
	"github.com/libopenstorage/stork/pkg/version"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	kdmputils "github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/core"
	kdmpShedOps "github.com/portworx/sched-ops/k8s/kdmp"
	"github.com/portworx/sched-ops/k8s/storage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	coreapi "k8s.io/kubernetes/pkg/apis/core"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	defaultStorageClass = "use-default-storage-class"
	maxCrUpdateRetries  = 7
)

// isStorageClassMappingContainsDefault - will check whether any storageclass has use-default-storage-class
// as destination storage class.
func isStorageClassMappingContainsDefault(restore *storkapi.ApplicationRestore) bool {
	for _, value := range restore.Spec.StorageClassMapping {
		if value == defaultStorageClass {
			return true
		}
	}
	return false
}

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

func getRancherProjectMapping(restore *storkapi.ApplicationRestore) map[string]string {
	rancherProjectMapping := map[string]string{}
	if restore.Spec.RancherProjectMapping != nil {
		for key, value := range restore.Spec.RancherProjectMapping {
			rancherProjectMapping[key] = value
			dataKey := strings.Split(key, ":")
			dataVal := strings.Split(value, ":")
			if len(dataKey) == 2 && len(dataVal) == 2 {
				rancherProjectMapping[dataKey[1]] = dataVal[1]
			} else if len(dataKey) == 1 && len(dataVal) == 2 {
				rancherProjectMapping[dataKey[0]] = dataVal[1]
			}
		}
	}
	return rancherProjectMapping
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

	rancherProjectMapping := getRancherProjectMapping(restore)
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
			utils.ParseRancherProjectMapping(ns.Annotations, rancherProjectMapping)
			utils.ParseRancherProjectMapping(ns.Labels, rancherProjectMapping)
			// create mapped restore namespace with metadata of backed up
			// namespace
			_, err := core.Instance().CreateNamespace(&v1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:        ns.Name,
					Labels:      ns.Labels,
					Annotations: ns.GetAnnotations(),
				},
			})
			log.ApplicationRestoreLog(restore).Tracef("Creating dest namespace %v", ns.Name)
			if err != nil {
				if k8s_errors.IsAlreadyExists(err) {
					oldNS, err := core.Instance().GetNamespace(ns.GetName())
					if err != nil {
						return err
					}
					annotations := make(map[string]string)
					labels := make(map[string]string)
					if restore.Spec.ReplacePolicy == storkapi.ApplicationRestoreReplacePolicyDelete {
						// overwrite all annotation in case of replace policy set to delete
						annotations = ns.GetAnnotations()
						labels = ns.GetLabels()
					} else if restore.Spec.ReplacePolicy == storkapi.ApplicationRestoreReplacePolicyRetain {
						// only add new annotation,labels in case of replace policy is set to retain
						annotations = oldNS.GetAnnotations()
						if annotations == nil {
							annotations = make(map[string]string)
						}
						for k, v := range ns.GetAnnotations() {
							if _, ok := annotations[k]; !ok && !strings.Contains(k, utils.CattleProjectPrefix) {
								annotations[k] = v
							}
						}
						labels = oldNS.GetLabels()
						if labels == nil {
							labels = make(map[string]string)
						}
						for k, v := range ns.GetLabels() {
							if _, ok := labels[k]; !ok && !strings.Contains(k, utils.CattleProjectPrefix) {
								labels[k] = v
							}
						}
						utils.ParseRancherProjectMapping(annotations, rancherProjectMapping)
						utils.ParseRancherProjectMapping(labels, rancherProjectMapping)
					}
					log.ApplicationRestoreLog(restore).Tracef("Namespace already exists, updating dest namespace %v", ns.Name)
					_, err = core.Instance().UpdateNamespace(&v1.Namespace{
						ObjectMeta: metav1.ObjectMeta{
							Name:        ns.Name,
							Labels:      labels,
							Annotations: annotations,
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
			if k8s_errors.IsNotFound(err) {
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
func (a *ApplicationRestoreController) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logrus.Infof("Reconciling ApplicationRestore %s/%s", request.Namespace, request.Name)

	// Fetch the ApplicationBackup instance
	restore := &storkapi.ApplicationRestore{}
	err := a.client.Get(context.TODO(), request.NamespacedName, restore)
	if err != nil {
		if k8s_errors.IsNotFound(err) {
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
	// This channel listens on two values if the CR's time stamp to be updated or to quit the go routine
	updateCr := make(chan int, utils.RestoreCrChannelBufferSize)
	// This go routine updates the CR's timestamp every 15 minutes based on the input
	go a.restoreCrTimestampUpdate(restore, updateCr)
	if err = a.handle(context.TODO(), restore, updateCr); err != nil && err != errResourceBusy {
		logrus.Errorf("%s: %s/%s: %s", reflect.TypeOf(a), restore.Namespace, restore.Name, err)
		// The restore CR is done with the go-routine, lets purge it
		updateCr <- utils.QuitRestoreCrTimestampUpdate
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}
	// The restore CR is done with the go-routine, lets purge it
	updateCr <- utils.QuitRestoreCrTimestampUpdate
	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
}

// Handle updates for ApplicationRestore objects
func (a *ApplicationRestoreController) handle(ctx context.Context, restore *storkapi.ApplicationRestore, updateCr chan int) error {
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

	backup, err := storkops.Instance().GetApplicationBackup(restore.Spec.BackupName, restore.Namespace)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error getting backup: %v", err)
		return err
	}

	nfs, err := utils.IsNFSBackuplocationType(backup.Namespace, backup.Spec.BackupLocation)
	if err != nil {
		logrus.Errorf("error in checking backuplocation type")
	}

	if !nfs {
		err = a.verifyNamespaces(restore)
		if err != nil {
			log.ApplicationRestoreLog(restore).Errorf(err.Error())
			a.recorder.Event(restore,
				v1.EventTypeWarning,
				string(storkapi.ApplicationRestoreStatusFailed),
				err.Error())
			return nil
		}
	}

	if len(restore.Spec.StorageClassMapping) >= 1 && isStorageClassMappingContainsDefault(restore) {
		// Update the default storageclass name in storageclassmapping.
		// storageclassMapping will have "use-default-storage-class" as destination storage class,
		// If default storageclass need to be selected.
		scList, err := storage.Instance().GetDefaultStorageClasses()
		if err != nil {
			log.ApplicationRestoreLog(restore).Errorf(err.Error())
			a.recorder.Event(restore,
				v1.EventTypeWarning,
				string(storkapi.ApplicationRestoreStatusFailed),
				err.Error())
			return nil
		}
		// If more than one storageclass is set as default storageclass, update error event
		if len(scList.Items) > 1 {
			errMsg := "more than one storageclass is set as default on destination cluster"
			log.ApplicationRestoreLog(restore).Errorf(errMsg)
			a.recorder.Event(restore,
				v1.EventTypeWarning,
				string(storkapi.ApplicationRestoreStatusFailed),
				errMsg)
			return nil
		}
		// If no storageclass is set as default storageclass, update error event
		if len(scList.Items) == 0 {
			err := fmt.Errorf("no storageclass is set as default on destination cluster")
			log.ApplicationRestoreLog(restore).Errorf(err.Error())
			a.recorder.Event(restore,
				v1.EventTypeWarning,
				string(storkapi.ApplicationRestoreStatusFailed),
				err.Error())
			return nil
		}
		for key, value := range restore.Spec.StorageClassMapping {
			if value == defaultStorageClass {
				restore.Spec.StorageClassMapping[key] = scList.Items[0].Name
			}
		}
		err = a.client.Update(context.TODO(), restore)
		if err != nil {
			return err
		}
	}
	switch restore.Status.Stage {
	case storkapi.ApplicationRestoreStageInitial:
		// Make sure the namespaces exist
		fallthrough
	case storkapi.ApplicationRestoreStageVolumes:
		err := a.restoreVolumes(restore, updateCr)
		if err != nil {
			message := fmt.Sprintf("Error restoring volumes: %v", err)
			log.ApplicationRestoreLog(restore).Errorf(message)
			a.recorder.Event(restore,
				v1.EventTypeWarning,
				string(storkapi.ApplicationRestoreStatusFailed),
				message)
			if _, ok := err.(*volume.ErrStorageProviderBusy); ok {
				return errResourceBusy
			}
			return nil
		}
	case storkapi.ApplicationRestoreStageApplications:
		err := a.restoreResources(restore, updateCr)
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
		// DoNothing
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
		return true
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

func (a *ApplicationRestoreController) getNamespacedObjectsToDelete(restore *storkapi.ApplicationRestore, objects []runtime.Unstructured) ([]runtime.Unstructured, error) {
	tempObjects := make([]runtime.Unstructured, 0)
	for _, o := range objects {
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return nil, err
		}

		// Skip PVs, we will let the PVC handle PV deletion where needed
		if objectType.GetKind() != "PersistentVolume" {
			tempObjects = append(tempObjects, o)
		}
	}

	return tempObjects, nil
}

func (a *ApplicationRestoreController) updateRestoreCRInVolumeStage(
	namespacedName types.NamespacedName,
	status storkapi.ApplicationRestoreStatusType,
	stage storkapi.ApplicationRestoreStageType,
	reason string,
	volumeInfos []*storkapi.ApplicationRestoreVolumeInfo,
) (*storkapi.ApplicationRestore, error) {
	restore := &storkapi.ApplicationRestore{}
	var err error
	for i := 0; i < maxRetry; i++ {
		err := a.client.Get(context.TODO(), namespacedName, restore)
		if err != nil {
			time.Sleep(retrySleep)
			continue
		}
		if restore.Status.Stage == storkapi.ApplicationRestoreStageFinal ||
			restore.Status.Stage == storkapi.ApplicationRestoreStageApplications {
			// updated timestamp for failed restores
			if restore.Status.Status == storkapi.ApplicationRestoreStatusFailed {
				restore.Status.FinishTimestamp = metav1.Now()
				restore.Status.LastUpdateTimestamp = metav1.Now()
				restore.Status.Reason = reason
			}
			return restore, nil
		}
		logrus.Infof("Updating restore  %s/%s in stage/stagus: %s/%s to volume stage", restore.Namespace, restore.Name, restore.Status.Stage, restore.Status.Status)
		restore.Status.Status = status
		restore.Status.Stage = stage
		restore.Status.Reason = reason
		restore.Status.LastUpdateTimestamp = metav1.Now()
		if volumeInfos != nil {
			restore.Status.Volumes = append(restore.Status.Volumes, volumeInfos...)
		}

		err = a.client.Update(context.TODO(), restore)
		if err != nil {
			time.Sleep(retrySleep)
			continue
		} else {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return restore, nil
}

func (a *ApplicationRestoreController) restoreVolumes(restore *storkapi.ApplicationRestore, updateCr chan int) error {
	funct := "restoreVolumes"
	restore.Status.Stage = storkapi.ApplicationRestoreStageVolumes

	var err error
	namespacedName := types.NamespacedName{}
	namespacedName.Namespace = restore.Namespace
	namespacedName.Name = restore.Name
	// In case of kdmp driver we may delete resources first before operating on volumes
	// Hence we need to update the status.
	// This is to prevent px-backup to fail the restore declaring timeout.
	restore, err = a.updateRestoreCRInVolumeStage(
		namespacedName,
		storkapi.ApplicationRestoreStatusInProgress,
		storkapi.ApplicationRestoreStageVolumes,
		"Volume or Resource(kdmp) restores are in progress",
		nil,
	)
	if err != nil {
		return err
	}
	backup, err := storkops.Instance().GetApplicationBackup(restore.Spec.BackupName, restore.Namespace)
	if err != nil {
		return fmt.Errorf("error getting backup spec for restore: %v", err)
	}
	objectMap := storkapi.CreateObjectsMap(restore.Spec.IncludeResources)
	info := storkapi.ObjectInfo{
		GroupVersionKind: metav1.GroupVersionKind{
			Group:   "core",
			Version: "v1",
			Kind:    "PersistentVolumeClaim",
		},
	}

	pvcCount := 0
	restoreDone := 0
	backupVolumeInfoMappings := make(map[string][]*storkapi.ApplicationBackupVolumeInfo)
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

			pvcCount++
			isVolRestoreDone := false
			for _, statusVolume := range restore.Status.Volumes {
				if statusVolume.SourceVolume == volumeBackup.Volume {
					isVolRestoreDone = true
					break
				}
			}
			if isVolRestoreDone {
				restoreDone++
				continue
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
	if restore.Status.Volumes == nil {
		restore.Status.Volumes = make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	}
	restoreCompleteList := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	nfs, err := utils.IsNFSBackuplocationType(backup.Namespace, backup.Spec.BackupLocation)
	if err != nil {
		logrus.Errorf("error in checking backuplocation type")
		return err
	}
	if len(restore.Status.Volumes) != pvcCount {
		// Here backupVolumeInfoMappings is framed based on driver name mapping, hence startRestore()
		// gets called once per driver
		if !nfs {
			for driverName, vInfos := range backupVolumeInfoMappings {
				backupVolInfos := vInfos
				driver, err := volume.Get(driverName)
				//	BL NFS + kdmp = nfs code path
				//	s3 + kdmp = legacy code path
				//	BL NFS + EBS/GKE/Azure = legacy code path
				//	s3 + EBS/GKE/Azure = legacy code path
				existingRestoreVolInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
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
				// Skip pv/pvc if replacepolicy is set to retain to avoid creating
				if restore.Spec.ReplacePolicy == storkapi.ApplicationRestoreReplacePolicyRetain {
					backupVolInfos, existingRestoreVolInfos, err = a.skipVolumesFromRestoreList(restore, objects, driver, vInfos)
					if err != nil {
						log.ApplicationRestoreLog(restore).Errorf("Error while checking pvcs: %v", err)
						return err
					}
				}
				var storageClassesBytes []byte
				if driverName == "csi" {
					storageClassesBytes, err = a.downloadObject(backup, backup.Spec.BackupLocation, backup.Namespace, "storageclasses.json", false)
					if err != nil {
						log.ApplicationRestoreLog(restore).Errorf("Error in a.downloadObject %v", err)
						return err
					}
				}
				preRestoreObjects, err := driver.GetPreRestoreResources(backup, restore, objects, storageClassesBytes)
				if err != nil {
					log.ApplicationRestoreLog(restore).Errorf("Error getting PreRestore Resources: %v", err)
					return err
				}

				// Pre-delete resources for CSI driver
				if (driverName == "csi" || driverName == "kdmp") && restore.Spec.ReplacePolicy == storkapi.ApplicationRestoreReplacePolicyDelete {
					objectMap := storkapi.CreateObjectsMap(restore.Spec.IncludeResources)
					objectBasedOnIncludeResources := make([]runtime.Unstructured, 0)
					var opts resourcecollector.Options
					for _, o := range objects {
						skip, err := a.resourceCollector.PrepareResourceForApply(
							o,
							objects,
							objectMap,
							restore.Spec.NamespaceMapping,
							nil, // no need to set storage class mappings at this stage
							nil,
							restore.Spec.IncludeOptionalResourceTypes,
							nil,
							&opts,
						)
						if err != nil {
							return err
						}
						if !skip {
							objectBasedOnIncludeResources = append(
								objectBasedOnIncludeResources,
								o,
							)
						}
					}
					tempObjects, err := a.getNamespacedObjectsToDelete(
						restore,
						objectBasedOnIncludeResources,
					)
					if err != nil {
						return err
					}
					err = a.resourceCollector.DeleteResources(
						a.dynamicInterface,
						tempObjects, updateCr)
					if err != nil {
						return err
					}
				}
				// pvc creation is not part of kdmp
				if driverName != volume.KDMPDriverName {
					if err := a.applyResources(restore, preRestoreObjects, updateCr); err != nil {
						return err
					}
				}
				restore, err = a.updateRestoreCRInVolumeStage(
					namespacedName,
					storkapi.ApplicationRestoreStatusInProgress,
					storkapi.ApplicationRestoreStageVolumes,
					"Volume restores are in progress",
					existingRestoreVolInfos,
				)
				if err != nil {
					return err
				}
				// Get restore volume batch count
				batchCount := k8sutils.DefaultRestoreVolumeBatchCount
				restoreVolumeBatchCount, err := k8sutils.GetConfigValue(k8sutils.StorkControllerConfigMapName, metav1.NamespaceSystem, k8sutils.RestoreVolumeBatchCountKey)
				if err != nil {
					logrus.Debugf("error in reading %v cm, defaulting restore volume batch count", k8sutils.StorkControllerConfigMapName)
				} else {
					if len(restoreVolumeBatchCount) != 0 {
						batchCount, err = strconv.Atoi(restoreVolumeBatchCount)
						if err != nil {
							logrus.Debugf("error in conversion of restoreVolumeBatchCount: %v", err)
						}
					}
				}
				for i := 0; i < len(backupVolInfos); i += batchCount {
					batchVolInfo := backupVolInfos[i:min(i+batchCount, len(backupVolInfos))]
					restoreVolumeInfos, err := driver.StartRestore(restore, batchVolInfo, preRestoreObjects)
					if err != nil {
						message := fmt.Sprintf("Error starting Application Restore for volumes: %v", err)
						log.ApplicationRestoreLog(restore).Errorf(message)
						if _, ok := err.(*volume.ErrStorageProviderBusy); ok {
							msg := fmt.Sprintf("Volume restores are in progress. Restores are failing for some volumes"+
								" since the storage provider is busy. Restore will be retried. Error: %v", err)
							a.recorder.Event(restore,
								v1.EventTypeWarning,
								string(storkapi.ApplicationRestoreStatusInProgress),
								msg)

							log.ApplicationRestoreLog(restore).Errorf(msg)
							// Update the restore status even for failed restores when storage is busy
							_, updateErr := a.updateRestoreCRInVolumeStage(
								namespacedName,
								storkapi.ApplicationRestoreStatusInProgress,
								storkapi.ApplicationRestoreStageVolumes,
								msg,
								restoreVolumeInfos,
							)
							if updateErr != nil {
								logrus.Warnf("failed to update restore status: %v", updateErr)
							}
							return err
						}
						a.recorder.Event(restore,
							v1.EventTypeWarning,
							string(storkapi.ApplicationRestoreStatusFailed),
							message)
						_, err = a.updateRestoreCRInVolumeStage(namespacedName, storkapi.ApplicationRestoreStatusFailed, storkapi.ApplicationRestoreStageFinal, message, nil)
						return err
					}
					time.Sleep(k8sutils.RestoreVolumeBatchSleepInterval)
					restore, err = a.updateRestoreCRInVolumeStage(
						namespacedName,
						storkapi.ApplicationRestoreStatusInProgress,
						storkapi.ApplicationRestoreStageVolumes,
						"Volume restores are in progress",
						restoreVolumeInfos,
					)
					if err != nil {
						return err
					}
				}

			}
		}
		// Check whether ResourceExport is present or not
		if nfs {
			err = a.client.Update(context.TODO(), restore)
			if err != nil {
				time.Sleep(retrySleep)
				return nil
			}
			crName := getResourceExportCRName(utils.PrefixNFSRestorePVC, string(restore.UID), restore.Namespace)
			resourceExport, err := kdmpShedOps.Instance().GetResourceExport(crName, restore.Namespace)
			if err != nil {
				if k8s_errors.IsNotFound(err) {
					// create resource export CR
					resourceExport = &kdmpapi.ResourceExport{}
					// Adding required label for debugging
					labels := make(map[string]string)
					labels[utils.ApplicationRestoreCRNameKey] = utils.GetValidLabel(restore.Name)
					labels[utils.ApplicationRestoreCRUIDKey] = utils.GetValidLabel(utils.GetShortUID(string(restore.UID)))
					// If restore from px-backup, update the restore object details in the label
					if val, ok := backup.Annotations[utils.PxbackupAnnotationCreateByKey]; ok {
						if val == utils.PxbackupAnnotationCreateByValue {
							labels[utils.RestoreObjectNameKey] = utils.GetValidLabel(backup.Annotations[utils.PxbackupObjectNameKey])
							labels[utils.RestoreObjectUIDKey] = utils.GetValidLabel(backup.Annotations[utils.PxbackupObjectUIDKey])
						}
					}
					resourceExport.Labels = labels
					resourceExport.Annotations = make(map[string]string)
					resourceExport.Annotations[utils.SkipResourceAnnotation] = "true"
					resourceExport.Name = crName
					resourceExport.Namespace = restore.Namespace
					resourceExport.Spec.Type = kdmpapi.ResourceExportBackup
					// TODO: In the restore path we need to change source and destination ref as it is confusing now
					// Usually dest means where it's backed up or restore to
					source := &kdmpapi.ResourceExportObjectReference{
						APIVersion: restore.APIVersion,
						Kind:       restore.Kind,
						Namespace:  restore.Namespace,
						Name:       restore.Name,
					}
					backupLocation, err := storkops.Instance().GetBackupLocation(backup.Spec.BackupLocation, backup.Namespace)
					if err != nil {
						return fmt.Errorf("error getting backup location path: %v", err)
					}
					destination := &kdmpapi.ResourceExportObjectReference{
						// TODO: GetBackupLocation is not returning APIVersion and kind.
						// Hardcoding for now.
						APIVersion: utils.StorkAPIVersion,
						Kind:       utils.BackupLocationKind,
						Namespace:  backupLocation.Namespace,
						Name:       backupLocation.Name,
					}
					resourceExport.Spec.TriggeredFrom = kdmputils.TriggeredFromStork
					storkPodNs, err := k8sutils.GetStorkPodNamespace()
					if err != nil {
						logrus.Errorf("error in getting stork pod namespace: %v", err)
						return err
					}
					resourceExport.Spec.TriggeredFromNs = storkPodNs
					resourceExport.Spec.Source = *source
					resourceExport.Spec.Destination = *destination
					_, err = kdmpShedOps.Instance().CreateResourceExport(resourceExport)
					if err != nil {
						logrus.Errorf("failed to create resourceExport CR %v: %v", crName, err)
						return err
					}
					return nil
				}
				logrus.Errorf("%s error reading resourceExport CR %v: %v", funct, crName, err)
				return nil
			} else {
				var message string
				logrus.Infof("%s re cr %v status %v", funct, crName, resourceExport.Status.Status)
				switch resourceExport.Status.Status {
				case kdmpapi.ResourceExportStatusFailed:
					message = fmt.Sprintf("%s Error creating CR %v for pvc creation: %v", funct, crName, resourceExport.Status.Reason)
					restore.Status.Status = storkapi.ApplicationRestoreStatusFailed
					restore.Status.Stage = storkapi.ApplicationRestoreStageFinal
					restore.Status.Reason = message
					restore.Status.LastUpdateTimestamp = metav1.Now()
					err = a.client.Update(context.TODO(), restore)
					if err != nil {
						return err
					}
					a.recorder.Event(restore,
						v1.EventTypeWarning,
						string(storkapi.ApplicationRestoreStatusFailed),
						message)
					log.ApplicationRestoreLog(restore).Errorf(message)
					return err
				case kdmpapi.ResourceExportStatusInitial:
					return nil
				case kdmpapi.ResourceExportStatusPending:
					return nil
				case kdmpapi.ResourceExportStatusInProgress:
					return nil
				case kdmpapi.ResourceExportStatusSuccessful:
					restoreCompleteList = append(restoreCompleteList, resourceExport.RestoreCompleteList...)
				default:
					logrus.Infof("%s still valid re CR[%v]stage not available", funct, crName)
					return nil
				}
			}
		}
		restore, err = a.updateRestoreCRInVolumeStage(
			namespacedName,
			storkapi.ApplicationRestoreStatusInProgress,
			storkapi.ApplicationRestoreStageVolumes,
			"Volume restores are in progress",
			restoreCompleteList,
		)
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
				vInfo.Status == storkapi.ApplicationRestoreStatusPending {
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
	if inProgress || len(restore.Status.Volumes) != pvcCount {
		return nil
	}

	// If the restore hasn't failed move on to the next stage.
	if restore.Status.Status != storkapi.ApplicationRestoreStatusFailed {
		restore.Status.Stage = storkapi.ApplicationRestoreStageApplications
		restore.Status.Status = storkapi.ApplicationRestoreStatusInProgress
		restore.Status.Reason = "Application resources restore is in progress"
		restore.Status.LastUpdateTimestamp = metav1.Now()
		// Only on success compute the total restore size
		for _, vInfo := range restore.Status.Volumes {
			restore.Status.TotalSize += vInfo.TotalSize
		}
		// Update the current state and then move on to restoring resources
		err := a.client.Update(context.TODO(), restore)
		if err != nil {
			return err
		}
		err = a.restoreResources(restore, updateCr)
		if err != nil {
			log.ApplicationRestoreLog(restore).Errorf("Error restoring resources: %v", err)
			return err
		}
	}
	restore.Status.LastUpdateTimestamp = metav1.Now()

	err = a.client.Update(context.TODO(), restore)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error updating resotre CR in volume restore after invoking restoreResources: %v", err)
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
		return nil, fmt.Errorf("EncryptionKey is deprecated, use EncryptionKeyV2 instead")
	}
	if restoreLocation.Location.EncryptionV2Key != "" {
		var decryptData []byte
		if decryptData, err = crypto.Decrypt(data, restoreLocation.Location.EncryptionV2Key); err != nil {
			logrus.Errorf("ApplicationRestoreController/downloadObject: decrypt failed :%v, returing data direclty", err)
			return data, nil
		}
		return decryptData, nil
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
	var crdsV1 []*apiextensionsv1.CustomResourceDefinition
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
	if err = json.Unmarshal(crdData, &crdsV1); err != nil {
		return err
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error getting cluster config: %v", err)
	}

	client, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return err
	}

	regCrd := make(map[string]bool)
	for _, crd := range crds {
		crd.ResourceVersion = ""
		regCrd[crd.GetName()] = false
		if _, err := client.ApiextensionsV1beta1().CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{}); err != nil && !k8s_errors.IsAlreadyExists(err) {
			regCrd[crd.GetName()] = true
			logrus.Warnf("error registering crds v1beta1 %v,%v", crd.GetName(), err)
			continue
		}
		// wait for crd to be ready
		if err := k8sutils.ValidateCRD(client, crd.GetName()); err != nil {
			logrus.Warnf("Unable to validate crds v1beta1 %v,%v", crd.GetName(), err)
		}
	}

	for _, crd := range crdsV1 {
		if val, ok := regCrd[crd.GetName()]; ok && val {
			crd.ResourceVersion = ""
			var updatedVersions []apiextensionsv1.CustomResourceDefinitionVersion
			// try to apply as v1 crd
			var err error
			if _, err = client.ApiextensionsV1().CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{}); err == nil || k8s_errors.IsAlreadyExists(err) {
				logrus.Infof("registered v1 crds %v,", crd.GetName())
				continue
			}
			// updated fields
			crd.Spec.PreserveUnknownFields = false
			for _, version := range crd.Spec.Versions {
				isTrue := true
				if version.Schema == nil {
					openAPISchema := &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{XPreserveUnknownFields: &isTrue},
					}
					version.Schema = openAPISchema
				} else {
					version.Schema.OpenAPIV3Schema.XPreserveUnknownFields = &isTrue
				}
				updatedVersions = append(updatedVersions, version)
			}
			crd.Spec.Versions = updatedVersions

			if _, err := client.ApiextensionsV1().CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{}); err != nil && !k8s_errors.IsAlreadyExists(err) {
				logrus.Warnf("error registering crdsv1 %v,%v", crd.GetName(), err)
				continue
			}
			// wait for crd to be ready
			if err := k8sutils.ValidateCRDV1(client, crd.GetName()); err != nil {
				logrus.Warnf("Unable to validate crdsv1 %v,%v", crd.GetName(), err)
			}

		}
	}

	return nil
}

func (a *ApplicationRestoreController) updateResourceStatus(
	restore *storkapi.ApplicationRestore,
	object runtime.Unstructured,
	status storkapi.ApplicationRestoreStatusType,
	reason string,
	tempResourceList []*storkapi.ApplicationRestoreResourceInfo,
) ([]*storkapi.ApplicationRestoreResourceInfo, error) {
	var updatedResource *storkapi.ApplicationRestoreResourceInfo
	gkv := object.GetObjectKind().GroupVersionKind()
	metadata, err := meta.Accessor(object)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error getting metadata for object %v %v", object, err)
		return nil, err
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
		// During apply of PV PVC resource, we don't need to have tremporary list for lazy update of resource list
		// and apply timestamp updation to restore CR after every apply of 3000 resource.
		if tempResourceList != nil {
			tempResourceList = append(tempResourceList, updatedResource)
		} else {
			restore.Status.Resources = append(restore.Status.Resources, updatedResource)
		}
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
	return tempResourceList, nil
}

func (a *ApplicationRestoreController) updateResourceStatusFromRestoreCR(
	restore *storkapi.ApplicationRestore,
	resource *kdmpapi.ResourceRestoreResourceInfo,
	status kdmpapi.ResourceRestoreStatus,
	reason string,
) {
	var resourceStatus storkapi.ApplicationRestoreStatusType
	switch status {
	case kdmpapi.ResourceRestoreStatusSuccessful:
		resourceStatus = storkapi.ApplicationRestoreStatusSuccessful
	case kdmpapi.ResourceRestoreStatusRetained:
		resourceStatus = storkapi.ApplicationRestoreStatusRetained
	case kdmpapi.ResourceRestoreStatusFailed:
		resourceStatus = storkapi.ApplicationRestoreStatusFailed
	case kdmpapi.ResourceRestoreStatusInProgress:
		resourceStatus = storkapi.ApplicationRestoreStatusInProgress
	}
	updatedResource := &storkapi.ApplicationRestoreResourceInfo{
		ObjectInfo: storkapi.ObjectInfo{
			Name:      resource.Name,
			Namespace: resource.Namespace,
			GroupVersionKind: metav1.GroupVersionKind{
				Group:   resource.Group,
				Version: resource.Version,
				Kind:    resource.Kind,
			},
		},
		Status: resourceStatus,
		Reason: reason,
	}
	restore.Status.Resources = append(restore.Status.Resources, updatedResource)
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

func getNamespacedPVCLocation(pvc *v1.PersistentVolumeClaim) string {
	return fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name)
}

// getPVCToPVMapping constructs a mapping of PVC name/namespace to PV objects
func getPVCToPVMapping(allObjects []runtime.Unstructured) (map[string]*v1.PersistentVolume, error) {

	// Get mapping of PVC name to PV name
	pvNameToPVCName := make(map[string]string)
	for _, o := range allObjects {
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return nil, err
		}

		// If a PV, assign it to the mapping based on the claimRef UID
		if objectType.GetKind() == "PersistentVolumeClaim" {
			pvc := &v1.PersistentVolumeClaim{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.UnstructuredContent(), pvc); err != nil {
				return nil, fmt.Errorf("error converting to persistent volume: %v", err)
			}

			pvNameToPVCName[pvc.Spec.VolumeName] = getNamespacedPVCLocation(pvc)
		}
	}

	// Get actual mapping of PVC name to PV object
	pvcNameToPV := make(map[string]*v1.PersistentVolume)
	for _, o := range allObjects {
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return nil, err
		}

		// If a PV, assign it to the mapping based on the claimRef UID
		if objectType.GetKind() == "PersistentVolume" {
			pv := &v1.PersistentVolume{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.UnstructuredContent(), pv); err != nil {
				return nil, fmt.Errorf("error converting to persistent volume: %v", err)
			}

			pvcName := pvNameToPVCName[pv.Name]

			// add this PVC name/PV obj mapping
			pvcNameToPV[pvcName] = pv
		}
	}

	return pvcNameToPV, nil
}

func isGenericCSIPersistentVolume(pv *v1.PersistentVolume) (bool, error) {
	driverName, err := volume.GetPVDriver(pv)
	if err != nil {
		return false, err
	}
	if driverName == "csi" {
		return true, nil
	}
	return false, nil
}
func isGenericPersistentVolume(pv *v1.PersistentVolume, volInfos []*storkapi.ApplicationRestoreVolumeInfo) (bool, error) {
	for _, vol := range volInfos {
		if vol.DriverName == kdmp.GetGenericDriverName() && vol.RestoreVolume == pv.Name {
			return true, nil
		}
	}
	return false, nil
}

func isGenericCSIPersistentVolumeClaim(pvc *v1.PersistentVolumeClaim, volInfos []*storkapi.ApplicationRestoreVolumeInfo) (bool, error) {
	for _, vol := range volInfos {
		if vol.DriverName == kdmp.GetGenericDriverName() && vol.PersistentVolumeClaim == pvc.Name {
			return true, nil
		}
	}
	return false, nil
}

func (a *ApplicationRestoreController) skipVolumesFromRestoreList(
	restore *storkapi.ApplicationRestore,
	objects []runtime.Unstructured,
	driver volume.Driver,
	volInfo []*storkapi.ApplicationBackupVolumeInfo,
) ([]*storkapi.ApplicationBackupVolumeInfo, []*storkapi.ApplicationRestoreVolumeInfo, error) {
	existingInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	newVolInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)
	for _, bkupVolInfo := range volInfo {
		restoreVolInfo := &storkapi.ApplicationRestoreVolumeInfo{}
		val, ok := restore.Spec.NamespaceMapping[bkupVolInfo.Namespace]
		if !ok {
			logrus.Infof("skipping namespace %s for restore", bkupVolInfo.Namespace)
			continue
		}

		// get corresponding pvc object from objects list
		pvcObject, err := volume.GetPVCFromObjects(objects, bkupVolInfo)
		if err != nil {
			return newVolInfos, existingInfos, err
		}

		ns := val
		pvc, err := core.Instance().GetPersistentVolumeClaim(pvcObject.Name, ns)
		if err != nil {
			if k8s_errors.IsNotFound(err) {
				newVolInfos = append(newVolInfos, bkupVolInfo)
				continue
			}
			return newVolInfos, existingInfos, fmt.Errorf("erorr getting pvc %s/%s: %v", ns, pvcObject.Name, err)
		}

		restoreVolInfo.PersistentVolumeClaim = bkupVolInfo.PersistentVolumeClaim
		restoreVolInfo.PersistentVolumeClaimUID = bkupVolInfo.PersistentVolumeClaimUID
		restoreVolInfo.SourceNamespace = bkupVolInfo.Namespace
		restoreVolInfo.SourceVolume = bkupVolInfo.Volume
		restoreVolInfo.DriverName = driver.String()
		restoreVolInfo.Status = storkapi.ApplicationRestoreStatusRetained
		restoreVolInfo.RestoreVolume = pvc.Spec.VolumeName
		restoreVolInfo.TotalSize = bkupVolInfo.TotalSize
		restoreVolInfo.Reason = fmt.Sprintf("Skipped from volume restore as policy is set to %s and pvc already exists", storkapi.ApplicationRestoreReplacePolicyRetain)
		existingInfos = append(existingInfos, restoreVolInfo)
	}

	return newVolInfos, existingInfos, nil
}

func (a *ApplicationRestoreController) removeCSIVolumesBeforeApply(
	restore *storkapi.ApplicationRestore,
	objects []runtime.Unstructured,
) ([]runtime.Unstructured, error) {
	tempObjects := make([]runtime.Unstructured, 0)
	// Get PVC to PV mapping first for checking if a PVC is bound to a generic CSI PV
	pvcToPVMapping, err := getPVCToPVMapping(objects)
	if err != nil {
		return nil, fmt.Errorf("failed to get PVC to PV mapping: %v", err)
	}
	for _, o := range objects {
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return nil, err
		}

		switch objectType.GetKind() {
		case "PersistentVolume":
			// check if this PV is a generic CSI one
			var pv v1.PersistentVolume
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.UnstructuredContent(), &pv); err != nil {
				return nil, fmt.Errorf("error converting to persistent volume: %v", err)
			}

			// Check if this PV is a generic CSI one
			isGenericCSIPVC, err := isGenericCSIPersistentVolume(&pv)
			if err != nil {
				return nil, fmt.Errorf("failed to check if PV was provisioned by a CSI driver: %v", err)
			}
			isGenericDriverPV, err := isGenericPersistentVolume(&pv, restore.Status.Volumes)
			if err != nil {
				return nil, err
			}
			// Only add this object if it's not a generic CSI PV
			if !isGenericCSIPVC && !isGenericDriverPV {
				tempObjects = append(tempObjects, o)
			} else {
				log.ApplicationRestoreLog(restore).Debugf("skipping CSI PV in restore: %s", pv.Name)
			}

		case "PersistentVolumeClaim":
			// check if this PVC is a generic CSI one
			var pvc v1.PersistentVolumeClaim
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.UnstructuredContent(), &pvc); err != nil {
				return nil, fmt.Errorf("error converting PVC object: %v: %v", o, err)
			}

			// Find the matching PV for this PVC
			pv, ok := pvcToPVMapping[getNamespacedPVCLocation(&pvc)]
			if !ok {
				log.ApplicationRestoreLog(restore).Debugf("failed to find PV for PVC %s during CSI volume skip. Will not skip volume", pvc.Name)
				tempObjects = append(tempObjects, o)
				continue
			}

			// We have found a PV for this PVC. Check if it is a generic CSI PV
			// that we do not already have native volume driver support for.
			isGenericCSIPVC, err := isGenericCSIPersistentVolume(pv)
			if err != nil {
				return nil, err
			}
			isGenericDriverPVC, err := isGenericCSIPersistentVolumeClaim(&pvc, restore.Status.Volumes)
			if err != nil {
				return nil, err
			}

			// Only add this object if it's not a generic CSI PVC
			if !isGenericCSIPVC && !isGenericDriverPVC {
				tempObjects = append(tempObjects, o)
			} else {
				log.ApplicationRestoreLog(restore).Debugf("skipping PVC in restore: %s", pvc.Name)
			}

		default:
			// add all other objects
			tempObjects = append(tempObjects, o)
		}
	}

	return tempObjects, nil
}

func (a *ApplicationRestoreController) restoreCrTimestampUpdate(
	restore *storkapi.ApplicationRestore,
	updateCr chan int) {
	fn := "restoreCrTimestampUpdate"
	for {
		namespacedName := types.NamespacedName{}
		namespacedName.Namespace = restore.Namespace
		namespacedName.Name = restore.Name
		log.ApplicationRestoreLog(restore).Infof("%v: waiting to receive data from channel...", fn)
		x := <-updateCr
		if x == utils.UpdateRestoreCrTimestampInPrepareResourcePath ||
			x == utils.UpdateRestoreCrTimestampInDeleteResourcePath {
			for i := 0; i < maxCrUpdateRetries; i++ {
				// This get() call is to prevent CR update failure incase it is written via kubectl or any other means.
				// Else in applyResources context all Updates are sequential.
				// And no other datastructure is modified in the in-memory copy of restore CR in parallel to this go-routine
				err := a.client.Get(context.TODO(), namespacedName, restore)
				if err != nil {
					time.Sleep(retrySleep)
					continue
				}
				if x == utils.UpdateRestoreCrTimestampInPrepareResourcePath {
					restore.Status.ResourceRestoreState = storkapi.ApplicationRestoreResourcePreparing
					log.ApplicationRestoreLog(restore).Infof("%v: obtained signal to update restore cr timestamp from prepare resource path", fn)
				} else if x == utils.UpdateRestoreCrTimestampInDeleteResourcePath {
					restore.Status.ResourceRestoreState = storkapi.ApplicationRestoreResourceDeleting
					log.ApplicationRestoreLog(restore).Infof("%v: obtained signal to update restore cr timestamp from delete resource path", fn)
				}
				restore.Status.LastUpdateTimestamp = metav1.Now()
				err = a.client.Update(context.TODO(), restore)
				if err != nil {
					log.ApplicationRestoreLog(restore).Warnf("%v: failed to update restore CR timestamp: %v, the resource len %v", fn, err, len(restore.Status.Resources))
					time.Sleep(retrySleep)
					continue
				}
				break
			}
		} else if x == utils.QuitRestoreCrTimestampUpdate {
			log.ApplicationRestoreLog(restore).Infof("%v: exiting the go-routine that updates the restore CR timestamp", fn)
			return
		}
		time.Sleep(utils.SleepIntervalForCheckingChannel)
	}
}

func (a *ApplicationRestoreController) applyResources(
	restore *storkapi.ApplicationRestore,
	objects []runtime.Unstructured,
	updateCr chan int,
) error {
	fn := "applyResources"
	namespacedName := types.NamespacedName{}
	namespacedName.Namespace = restore.Namespace
	namespacedName.Name = restore.Name

	pvNameMappings, err := a.getPVNameMappings(restore, objects)
	if err != nil {
		return err
	}
	objectMap := storkapi.CreateObjectsMap(restore.Spec.IncludeResources)
	tempObjects := make([]runtime.Unstructured, 0)
	var opts resourcecollector.Options
	if len(restore.Spec.RancherProjectMapping) != 0 {
		rancherProjectMapping := getRancherProjectMapping(restore)
		opts = resourcecollector.Options{
			RancherProjectMappings: rancherProjectMapping,
		}
	}

	for i := 0; i < maxCrUpdateRetries; i++ {
		err := a.client.Get(context.TODO(), namespacedName, restore)
		if err != nil {
			log.ApplicationRestoreLog(restore).Warnf("%v: failed to get restore CR timestamp: %v, the resource len %v", fn, err, len(restore.Status.Resources))
			time.Sleep(retrySleep)
			continue
		}
		restore.Status.LastUpdateTimestamp = metav1.Now()
		restore.Status.RestoredResourceCount = 0
		err = a.client.Update(context.TODO(), restore)
		if err != nil {
			log.ApplicationRestoreLog(restore).Warnf("%v: failed to update restore CR timestamp: %v, the resource len %v", fn, err, len(restore.Status.Resources))
			continue
		}
		break
	}

	// This channel listens on two values if the CR's time stamp to be updated or to quit the go routine
	startTime := time.Now()
	for _, o := range objects {
		elapsedTime := time.Since(startTime)
		if elapsedTime > utils.TimeoutUpdateRestoreCrTimestamp {
			updateCr <- utils.UpdateRestoreCrTimestampInPrepareResourcePath
			startTime = time.Now()
		}
		skip, err := a.resourceCollector.PrepareResourceForApply(
			o,
			objects,
			objectMap,
			restore.Spec.NamespaceMapping,
			restore.Spec.StorageClassMapping,
			pvNameMappings,
			restore.Spec.IncludeOptionalResourceTypes,
			restore.Status.Volumes,
			&opts,
		)
		if err != nil {
			return err
		}
		if !skip {
			tempObjects = append(tempObjects, o)
		}
	}
	objects = tempObjects

	// skip CSI PV/PVCs before applying
	objects, err = a.removeCSIVolumesBeforeApply(restore, objects)
	if err != nil {
		return err
	}
	// First delete the existing objects if they exist and replace policy is set
	// to Delete
	if restore.Spec.ReplacePolicy == storkapi.ApplicationRestoreReplacePolicyDelete {
		err = a.resourceCollector.DeleteResources(
			a.dynamicInterface,
			objects, updateCr)
		if err != nil {
			return err
		}
	}

	restore.Status.ResourceCount = len(objects)
	tempResourceList := make([]*storkapi.ApplicationRestoreResourceInfo, 0)
	for _, o := range objects {
		// every five minutes once, we need to update the restore CR timestamp
		// this is to prevent stale CR timeout to evict the restore CR
		elapsedTime := time.Since(startTime)
		if elapsedTime > utils.TimeoutUpdateRestoreCrProgress {
			restore, err = a.updateRestoreCR(restore, namespacedName,
				len(tempResourceList),
				string(storkapi.ApplicationRestoreResourceApplying),
				restore.Status.ResourceCount)
			if err != nil {
				// no need to return error lets wait for next turn to write timestamp.
				log.ApplicationRestoreLog(restore).Errorf("%v: failed to update timestamp and restored resource count", fn)
			}
			log.ApplicationRestoreLog(restore).Infof("%v: Total resource Count: %v, Applied Resource Count %v, Current Resource State: %v",
				fn, restore.Status.ResourceCount,
				len(tempResourceList),
				restore.Status.ResourceRestoreState)
			startTime = time.Now()
		}
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
			o,
			&opts,
		)
		if err != nil && k8s_errors.IsAlreadyExists(err) {
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
			if tempResourceList, err = a.updateResourceStatus(
				restore,
				o,
				storkapi.ApplicationRestoreStatusFailed,
				fmt.Sprintf("Error applying resource: %v", err),
				tempResourceList,
			); err != nil {
				return err
			}
		} else if retained {
			if tempResourceList, err = a.updateResourceStatus(
				restore,
				o,
				storkapi.ApplicationRestoreStatusRetained,
				"Resource restore skipped as it was already present and ReplacePolicy is set to Retain",
				tempResourceList,
			); err != nil {
				return err
			}
		} else {
			if tempResourceList, err = a.updateResourceStatus(
				restore,
				o,
				storkapi.ApplicationRestoreStatusSuccessful,
				"Resource restored successfully",
				tempResourceList,
			); err != nil {
				return err
			}
		}
	}
	// append the temp slice to the final restore resouce list,
	// By replacing it can lose the previously added resources.
	restore.Status.Resources = append(restore.Status.Resources, tempResourceList...)
	restore.Status.RestoredResourceCount = len(restore.Status.Resources)

	return nil
}

// updateRestoreCR : it updates already applied restore count and total restore count
// and new timestamp to the restore CR. this is needed for progress bar and preventing
// px-backup in deleting the CR.
func (a *ApplicationRestoreController) updateRestoreCR(
	restore *storkapi.ApplicationRestore,
	namespacedName types.NamespacedName,
	restoredCount int,
	resourceRestoreStage string,
	totalResourceCount int,
) (*storkapi.ApplicationRestore, error) {
	fn := "updateRestoreCR"
	//restore := &storkapi.ApplicationRestore{}
	var err error
	for i := 0; i < maxRetry; i++ {
		restore.Status.RestoredResourceCount = restoredCount
		restore.Status.ResourceRestoreState = storkapi.ApplicationRestoreResourceStateType(resourceRestoreStage)
		restore.Status.ResourceCount = totalResourceCount
		restore.Status.LastUpdateTimestamp = metav1.Now()
		err = a.client.Update(context.TODO(), restore)
		if err != nil {
			log.ApplicationRestoreLog(restore).Warnf("%v Failed to update restore cr %v", fn, err)
			err = a.client.Get(context.TODO(), namespacedName, restore)
			if err != nil {
				log.ApplicationRestoreLog(restore).Warnf("%v Failed to get restore cr %v", fn, err)
			}
			time.Sleep(retrySleep)
			continue
		}
		break
	}
	if err != nil {
		return nil, err
	}
	log.ApplicationRestoreLog(restore).Infof("%v updated the restore CR with restoredResourceCount: %v restore resource state: %v ",
		fn, restore.Status.RestoredResourceCount, restore.Status.ResourceRestoreState)
	return restore, nil
}

func (a *ApplicationRestoreController) restoreResources(
	restore *storkapi.ApplicationRestore,
	updateCr chan int,
) error {
	backup, err := storkops.Instance().GetApplicationBackup(restore.Spec.BackupName, restore.Namespace)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error getting backup: %v", err)
		return err
	}
	nfs, err := utils.IsNFSBackuplocationType(backup.Namespace, backup.Spec.BackupLocation)
	if err != nil {
		logrus.Errorf("error in checking backuplocation type: %v", err)
		return err
	}

	doCleanup := true
	if !nfs {
		objects, err := a.downloadResources(backup, restore.Spec.BackupLocation, restore.Namespace)
		if err != nil {
			log.ApplicationRestoreLog(restore).Errorf("Error downloading resources: %v", err)
			return err
		}

		if err := a.applyResources(restore, objects, updateCr); err != nil {
			return err
		}
	} else {
		// Check whether ResourceExport is present or not
		crName := getResourceExportCRName(utils.PrefixRestore, string(restore.UID), restore.Namespace)
		resourceExport, err := kdmpShedOps.Instance().GetResourceExport(crName, restore.Namespace)
		if err != nil {
			if k8s_errors.IsNotFound(err) {
				// create resource export CR
				resourceExport := &kdmpapi.ResourceExport{}
				// Adding required label for debugging
				labels := make(map[string]string)
				labels[utils.ApplicationRestoreCRNameKey] = utils.GetValidLabel(restore.Name)
				labels[utils.ApplicationRestoreCRUIDKey] = utils.GetValidLabel(utils.GetShortUID(string(restore.UID)))
				// If restore from px-backup, update the restore object details in the label
				if val, ok := backup.Annotations[utils.PxbackupAnnotationCreateByKey]; ok {
					if val == utils.PxbackupAnnotationCreateByValue {
						labels[utils.RestoreObjectNameKey] = utils.GetValidLabel(backup.Annotations[utils.PxbackupObjectNameKey])
						labels[utils.RestoreObjectUIDKey] = utils.GetValidLabel(backup.Annotations[utils.PxbackupObjectUIDKey])
					}
				}
				resourceExport.Labels = labels
				resourceExport.Annotations = make(map[string]string)
				resourceExport.Annotations[utils.SkipResourceAnnotation] = "true"
				resourceExport.Name = crName
				resourceExport.Namespace = restore.Namespace
				resourceExport.Spec.Type = kdmpapi.ResourceExportBackup
				resourceExport.Spec.TriggeredFrom = kdmputils.TriggeredFromStork
				storkPodNs, err := k8sutils.GetStorkPodNamespace()
				if err != nil {
					logrus.Errorf("error in getting stork pod namespace: %v", err)
					return err
				}
				resourceExport.Spec.TriggeredFromNs = storkPodNs
				source := &kdmpapi.ResourceExportObjectReference{
					APIVersion: restore.APIVersion,
					Kind:       restore.Kind,
					Namespace:  restore.Namespace,
					Name:       restore.Name,
				}
				backupLocation, err := storkops.Instance().GetBackupLocation(backup.Spec.BackupLocation, backup.Namespace)
				if err != nil {
					return fmt.Errorf("error getting backup location path %v: %v", backup.Spec.BackupLocation, err)
				}
				destination := &kdmpapi.ResourceExportObjectReference{
					// TODO: .GetBackupLocation is not returning APIVersion and kind.
					// Hardcoding for now.
					// APIVersion: backupLocation.APIVersion,
					// Kind:       backupLocation.Kind,
					APIVersion: utils.StorkAPIVersion,
					Kind:       utils.BackupLocationKind,
					Namespace:  backupLocation.Namespace,
					Name:       backupLocation.Name,
				}
				resourceExport.Spec.Source = *source
				resourceExport.Spec.Destination = *destination
				_, err = kdmpShedOps.Instance().CreateResourceExport(resourceExport)
				if err != nil {
					logrus.Errorf("failed to create ResourceExport CR[%v/%v]: %v", resourceExport.Namespace, resourceExport.Name, err)
					return err
				}
				return nil
			}
			logrus.Errorf("failed to get restore resourceExport CR: %v", err)
			// Will retry in the next cycle of reconciler.
			return nil
		} else {
			var message string
			// Check the status of the resourceExport CR and update it to the applicationBackup CR
			logrus.Debugf("resource export: %s, status: %s", resourceExport.Name, resourceExport.Status.Status)
			switch resourceExport.Status.Status {
			case kdmpapi.ResourceExportStatusFailed:
				message = fmt.Sprintf("Error applying resources: %v", err)
				restore.Status.Status = storkapi.ApplicationRestoreStatusFailed
				restore.Status.Stage = storkapi.ApplicationRestoreStageFinal
				restore.Status.Reason = message
				restore.Status.LastUpdateTimestamp = metav1.Now()
				restore.Status.FinishTimestamp = metav1.Now()
				err = a.client.Update(context.TODO(), restore)
				if err != nil {
					return err
				}
				a.recorder.Event(restore,
					v1.EventTypeWarning,
					string(storkapi.ApplicationRestoreStatusFailed),
					message)
				log.ApplicationRestoreLog(restore).Errorf(message)
				return err
			case kdmpapi.ResourceExportStatusSuccessful:
				// Modify to have subresource level updating
				for _, resource := range resourceExport.Status.Resources {
					a.updateResourceStatusFromRestoreCR(
						restore,
						resource,
						resource.Status,
						resource.Reason)
				}
			case kdmpapi.ResourceExportStatusInitial:
				doCleanup = false
			case kdmpapi.ResourceExportStatusPending:
				doCleanup = false
			case kdmpapi.ResourceExportStatusInProgress:
				restore.Status.LastUpdateTimestamp = metav1.Now()
				doCleanup = false
			}
			restore.Status.LastUpdateTimestamp = metav1.Now()
			err = a.client.Update(context.TODO(), restore)
			if err != nil {
				return err
			}
		}
	}

	if !doCleanup {
		return nil
	}
	// Before  updating to final stage, cleanup generic backup CRs, if any.
	err = a.cleanupResources(restore)
	if err != nil {
		return err
	}

	// Add all CSI PVCs and PVs back into resources.
	// CSI PVs are dynamically generated by the CSI controller for restore,
	// so we need to get the new PV name after restore volumes finishes
	if err := a.addCSIVolumeResources(restore); err != nil {
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
	restoreCrSize, err := utils.GetSizeOfObject(restore)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("failed to obtain size of restore CR")
		return err
	}
	var largeResourceSizeLimit int64
	largeResourceSizeLimit = k8sutils.LargeResourceSizeLimitDefault
	configData, err := core.Instance().GetConfigMap(k8sutils.StorkControllerConfigMapName, coreapi.NamespaceSystem)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("failed to read config map %v for large resource size limit", k8sutils.StorkControllerConfigMapName)
	}
	if configData.Data[k8sutils.LargeResourceSizeLimitName] != "" {
		largeResourceSizeLimit, err = strconv.ParseInt(configData.Data[k8sutils.LargeResourceSizeLimitName], 0, 64)
		if err != nil {
			log.ApplicationRestoreLog(restore).Errorf("failed to read config map %v's key %v, setting default value of 1MB", k8sutils.StorkControllerConfigMapName,
				k8sutils.LargeResourceSizeLimitName)
		}
	}

	log.ApplicationRestoreLog(restore).Infof("The size of application restore CR obtained %v bytes", restoreCrSize)
	if restoreCrSize > int(largeResourceSizeLimit) {
		log.ApplicationRestoreLog(restore).Infof("Stripping all the resource info from restore cr as it is a large resource based restore")
		resourceCount := len(restore.Status.Resources)
		// update the flag and resource-count.
		// Strip off the resource info it contributes to bigger size of application restore CR in case of large number of resource
		restore.Status.Resources = make([]*storkapi.ApplicationRestoreResourceInfo, 0)
		restore.Status.ResourceCount = resourceCount
		restore.Status.LargeResourceEnabled = true
	}
	err = a.client.Update(context.TODO(), restore)
	if err != nil {
		log.ApplicationRestoreLog(restore).Infof("completed applying resources but failed to update restore CR: %v", err)
		return err
	}

	return nil
}

func (a *ApplicationRestoreController) addCSIVolumeResources(restore *storkapi.ApplicationRestore) error {
	for _, vrInfo := range restore.Status.Volumes {
		if vrInfo.DriverName != "csi" && vrInfo.DriverName != "kdmp" {
			continue
		}

		// Update PV resource for this volume
		pv, err := core.Instance().GetPersistentVolume(vrInfo.RestoreVolume)
		if err != nil {
			return fmt.Errorf("failed to get PV %s: %v", vrInfo.RestoreVolume, err)
		}
		pvContent, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pv)
		if err != nil {
			return fmt.Errorf("failed to convert PV %s to unstructured: %v", vrInfo.RestoreVolume, err)
		}
		pvObj := &unstructured.Unstructured{}
		pvObj.SetUnstructuredContent(pvContent)
		pvObj.SetGroupVersionKind(schema.GroupVersionKind{
			Kind:    "PersistentVolume",
			Version: "v1",
			Group:   "core",
		})
		if _, err := a.updateResourceStatus(
			restore,
			pvObj,
			vrInfo.Status,
			"Resource restored successfully", nil); err != nil {
			return err
		}

		// Update PVC resource for this volume
		ns, ok := restore.Spec.NamespaceMapping[vrInfo.SourceNamespace]
		if !ok {
			ns = vrInfo.SourceNamespace
		}
		pvc, err := core.Instance().GetPersistentVolumeClaim(vrInfo.PersistentVolumeClaim, ns)
		if err != nil {
			return fmt.Errorf("failed to get PVC %s/%s: %v", ns, vrInfo.PersistentVolumeClaim, err)
		}
		pvcContent, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pvc)
		if err != nil {
			return fmt.Errorf("failed to convert PVC %s to unstructured: %v", vrInfo.RestoreVolume, err)
		}
		pvcObj := &unstructured.Unstructured{}
		pvcObj.SetUnstructuredContent(pvcContent)
		pvcObj.SetGroupVersionKind(schema.GroupVersionKind{
			Kind:    "PersistentVolumeClaim",
			Version: "v1",
			Group:   "core",
		})

		if _, err := a.updateResourceStatus(
			restore,
			pvcObj,
			vrInfo.Status,
			"Resource restored successfully", nil); err != nil {
			return err
		}
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
	var crNames = []string{}
	// Directly calling DeleteResourceExport with out checking backuplocation type.
	// For other backuplocation type, expecting Notfound
	crNames = append(crNames, getResourceExportCRName(utils.PrefixRestore, string(restore.UID), restore.Namespace))
	crNames = append(crNames, getResourceExportCRName(utils.PrefixNFSRestorePVC, string(restore.UID), restore.Namespace))
	for _, crName := range crNames {
		err := kdmpShedOps.Instance().DeleteResourceExport(crName, restore.Namespace)
		if err != nil && !k8s_errors.IsNotFound(err) {
			errMsg := fmt.Sprintf("failed to delete restore resource export CR [%v]: %v", crName, err)
			log.ApplicationRestoreLog(restore).Errorf("%v", errMsg)
			return err
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
	ok, err := version.RequiresV1Registration()
	if err != nil {
		return err
	}
	if ok {
		err := k8sutils.CreateCRD(resource)
		if err != nil && !k8s_errors.IsAlreadyExists(err) {
			return err
		}
		return apiextensions.Instance().ValidateCRD(resource.Plural+"."+resource.Group, validateCRDTimeout, validateCRDInterval)
	}
	err = apiextensions.Instance().CreateCRDV1beta1(resource)
	if err != nil && !k8s_errors.IsAlreadyExists(err) {
		return err
	}
	return apiextensions.Instance().ValidateCRDV1beta1(resource, validateCRDTimeout, validateCRDInterval)
}

func (a *ApplicationRestoreController) cleanupResources(restore *storkapi.ApplicationRestore) error {
	drivers := a.getDriversForRestore(restore)
	for driverName := range drivers {

		driver, err := volume.Get(driverName)
		if err != nil {
			return err
		}
		if err := driver.CleanupRestoreResources(restore); err != nil {
			logrus.Errorf("unable to cleanup post restore resources, err: %v", err)
		}
	}
	var crNames = []string{}
	// Directly calling DeleteResourceExport with out checking backuplocation type.
	// For other backuplocation type, expecting Notfound
	crNames = append(crNames, getResourceExportCRName(utils.PrefixRestore, string(restore.UID), restore.Namespace))
	crNames = append(crNames, getResourceExportCRName(utils.PrefixNFSRestorePVC, string(restore.UID), restore.Namespace))
	for _, crName := range crNames {
		err := kdmpShedOps.Instance().DeleteResourceExport(crName, restore.Namespace)
		if err != nil && !k8s_errors.IsNotFound(err) {
			errMsg := fmt.Sprintf("failed to delete restore resource export CR [%v]: %v", crName, err)
			log.ApplicationRestoreLog(restore).Errorf("%v", errMsg)
			return err
		}
	}
	return nil
}
