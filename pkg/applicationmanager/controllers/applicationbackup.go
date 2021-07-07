package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-openapi/inflect"
	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/apis/stork"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/crypto"
	"github.com/libopenstorage/stork/pkg/errors"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/objectstore"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/libopenstorage/stork/pkg/rule"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"gocloud.dev/gcerrors"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
	runtimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	validateCRDInterval time.Duration = 5 * time.Second
	validateCRDTimeout  time.Duration = 1 * time.Minute

	resourceObjectName = "resources.json"
	crdObjectName      = "crds.json"
	nsObjectName       = "namespaces.json"
	metadataObjectName = "metadata.json"

	backupCancelBackoffInitialDelay = 5 * time.Second
	backupCancelBackoffFactor       = 1
	backupCancelBackoffSteps        = math.MaxInt32

	allNamespacesSpecifier        = "*"
	backupVolumeBatchCountEnvVar  = "BACKUP-VOLUME-BATCH-COUNT"
	defaultBackupVolumeBatchCount = 3
	backupResourcesBatchCount     = 15
	maxRetry                      = 10
	retrySleep                    = 10 * time.Second
)

var (
	backupCancelBackoff = wait.Backoff{
		Duration: backupCancelBackoffInitialDelay,
		Factor:   backupCancelBackoffFactor,
		Steps:    backupCancelBackoffSteps,
	}
	optionalBackupResources = []string{"Job"}
	errResourceBusy         = fmt.Errorf("resource is busy")
)

// NewApplicationBackup creates a new instance of ApplicationBackupController.
func NewApplicationBackup(mgr manager.Manager, r record.EventRecorder, rc resourcecollector.ResourceCollector) *ApplicationBackupController {
	return &ApplicationBackupController{
		client:            mgr.GetClient(),
		recorder:          r,
		resourceCollector: rc,
	}
}

// ApplicationBackupController reconciles applicationbackup objects
type ApplicationBackupController struct {
	client runtimeclient.Client

	recorder             record.EventRecorder
	resourceCollector    resourcecollector.ResourceCollector
	backupAdminNamespace string
	reconcileTime        time.Duration
}

// Init Initialize the application backup controller
func (a *ApplicationBackupController) Init(mgr manager.Manager, backupAdminNamespace string, syncTime int64) error {
	err := a.createCRD()
	if err != nil {
		return err
	}

	a.backupAdminNamespace = backupAdminNamespace
	if err := a.performRuleRecovery(); err != nil {
		logrus.Errorf("Failed to perform recovery for backup rules: %v", err)
		return err
	}
	a.reconcileTime = time.Duration(syncTime) * time.Second
	return controllers.RegisterTo(mgr, "application-backup-controller", a, &stork_api.ApplicationBackup{})
}

// Reconcile updates for ApplicationBackup objects.
func (a *ApplicationBackupController) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logrus.Tracef("Reconciling ApplicationBackup %s/%s", request.Namespace, request.Name)

	// Fetch the ApplicationBackup instance
	backup := &stork_api.ApplicationBackup{}
	err := a.client.Get(context.TODO(), request.NamespacedName, backup)
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

	if !controllers.ContainsFinalizer(backup, controllers.FinalizerCleanup) {
		controllers.SetFinalizer(backup, controllers.FinalizerCleanup)
		return reconcile.Result{Requeue: true}, a.client.Update(context.TODO(), backup)
	}
	if err = a.handle(context.TODO(), backup); err != nil && err != errResourceBusy {
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	return reconcile.Result{RequeueAfter: a.reconcileTime}, nil
}

func setKind(snap *stork_api.ApplicationBackup) {
	snap.Kind = "ApplicationBackup"
	snap.APIVersion = stork_api.SchemeGroupVersion.String()
}

// performRuleRecovery terminates potential background commands running pods for
// all applicationBackup objects
func (a *ApplicationBackupController) performRuleRecovery() error {
	applicationBackups, err := storkops.Instance().ListApplicationBackups(v1.NamespaceAll)
	if err != nil {
		logrus.Errorf("Failed to list all application backups during rule recovery: %v", err)
		return err
	}

	if applicationBackups == nil {
		return nil
	}

	var lastError error
	for _, applicationBackup := range applicationBackups.Items {
		setKind(&applicationBackup)
		err := rule.PerformRuleRecovery(&applicationBackup)
		if err != nil {
			lastError = err
		}
	}
	return lastError
}

func (a *ApplicationBackupController) setDefaults(backup *stork_api.ApplicationBackup) bool {
	updated := false
	if backup.Spec.ReclaimPolicy == "" {
		backup.Spec.ReclaimPolicy = stork_api.ApplicationBackupReclaimPolicyDelete
		updated = true
	}
	if backup.Status.TriggerTimestamp.IsZero() {
		backup.Status.TriggerTimestamp = backup.CreationTimestamp
		updated = true
	}
	return updated
}

func (a *ApplicationBackupController) updateWithAllNamespaces(backup *stork_api.ApplicationBackup) error {
	namespaces, err := core.Instance().ListNamespaces(nil)
	if err != nil {
		return fmt.Errorf("error updating with all namespaces for wildcard: %v", err)
	}
	namespacesToBackup := make([]string, 0)
	for _, ns := range namespaces.Items {
		namespacesToBackup = append(namespacesToBackup, ns.Name)
	}
	backup.Spec.Namespaces = namespacesToBackup
	err = a.client.Update(context.TODO(), backup)
	if err != nil {
		return fmt.Errorf("error updating with all namespaces for wildcard: %v", err)
	}
	return nil
}

// Try to create the backup location path. Ignore errors since this is best
// effort
func (a *ApplicationBackupController) createBackupLocationPath(backup *stork_api.ApplicationBackup) error {
	backupLocation, err := storkops.Instance().GetBackupLocation(backup.Spec.BackupLocation, backup.Namespace)
	if err != nil {
		return fmt.Errorf("error getting backup location path: %v", err)
	}
	if err := objectstore.CreateBucket(backupLocation); err != nil {
		return fmt.Errorf("error creating backup location path: %v", err)
	}
	return nil
}

// handle updates for ApplicationBackup objects
func (a *ApplicationBackupController) handle(ctx context.Context, backup *stork_api.ApplicationBackup) error {
	if backup.DeletionTimestamp != nil {
		if controllers.ContainsFinalizer(backup, controllers.FinalizerCleanup) {
			if err := a.deleteBackup(backup); err != nil {
				logrus.Errorf("%s: cleanup: %s", reflect.TypeOf(a), err)
			}
		}

		if backup.GetFinalizers() != nil {
			controllers.RemoveFinalizer(backup, controllers.FinalizerCleanup)
			return a.client.Update(ctx, backup)
		}

		return nil
	}

	// Check whether namespace is allowed to be backed before each stage
	// Restrict backup to only the namespace that the object belongs
	// except for the namespace designated by the admin
	if !a.namespaceBackupAllowed(backup) {
		err := fmt.Errorf("Spec.Namespaces should only contain the current namespace")
		log.ApplicationBackupLog(backup).Errorf(err.Error())
		a.recorder.Event(backup,
			v1.EventTypeWarning,
			string(stork_api.ApplicationBackupStatusFailed),
			err.Error())
		return nil
	}

	var terminationChannels []chan bool
	var err error

	if a.setDefaults(backup) {
		err = a.client.Update(context.TODO(), backup)
		if err != nil {
			log.ApplicationBackupLog(backup).Errorf("Error updating with defaults: %v", err)
		}
		return nil
	}

	switch backup.Status.Stage {
	case stork_api.ApplicationBackupStageInitial:
		// Make sure the namespaces exist
		for _, ns := range backup.Spec.Namespaces {
			if ns == allNamespacesSpecifier {
				err := a.updateWithAllNamespaces(backup)
				if err != nil {
					log.ApplicationBackupLog(backup).Errorf(err.Error())
					a.recorder.Event(backup,
						v1.EventTypeWarning,
						string(stork_api.ApplicationBackupStatusFailed),
						err.Error())
				}
				return nil
			}
			_, err := core.Instance().GetNamespace(ns)
			if err != nil {
				backup.Status.Status = stork_api.ApplicationBackupStatusFailed
				backup.Status.Reason = fmt.Sprintf("Error checking for namespace %v: %v", ns, err)
				backup.Status.Stage = stork_api.ApplicationBackupStageFinal
				backup.Status.FinishTimestamp = metav1.Now()
				backup.Status.LastUpdateTimestamp = metav1.Now()
				err = fmt.Errorf("error getting namespace %v: %v", ns, err)
				log.ApplicationBackupLog(backup).Errorf(err.Error())
				a.recorder.Event(backup,
					v1.EventTypeWarning,
					string(stork_api.ApplicationBackupStatusFailed),
					err.Error())
				err = a.client.Update(context.TODO(), backup)
				if err != nil {
					log.ApplicationBackupLog(backup).Errorf("Error updating: %v", err)
				}
				return nil
			}
		}

		// Try to create the backupLocation path, just log error if it fails
		err := a.createBackupLocationPath(backup)
		if err != nil {
			log.ApplicationBackupLog(backup).Errorf(err.Error())
			a.recorder.Event(backup,
				v1.EventTypeWarning,
				string(stork_api.ApplicationBackupStatusFailed),
				err.Error())
		}

		// Make sure the rules exist if configured
		if backup.Spec.PreExecRule != "" {
			_, err := storkops.Instance().GetRule(backup.Spec.PreExecRule, backup.Namespace)
			if err != nil {
				message := fmt.Sprintf("Error getting PreExecRule %v: %v", backup.Spec.PreExecRule, err)
				log.ApplicationBackupLog(backup).Errorf(message)
				a.recorder.Event(backup,
					v1.EventTypeWarning,
					string(stork_api.ApplicationBackupStatusFailed),
					message)
				return nil
			}
		}
		if backup.Spec.PostExecRule != "" {
			_, err := storkops.Instance().GetRule(backup.Spec.PostExecRule, backup.Namespace)
			if err != nil {
				message := fmt.Sprintf("Error getting PostExecRule %v: %v", backup.Spec.PreExecRule, err)
				log.ApplicationBackupLog(backup).Errorf(message)
				a.recorder.Event(backup,
					v1.EventTypeWarning,
					string(stork_api.ApplicationBackupStatusFailed),
					message)
				return nil
			}
		}
		fallthrough
	case stork_api.ApplicationBackupStagePreExecRule:
		var inProgress bool
		terminationChannels, inProgress, err = a.runPreExecRule(backup)
		if err != nil {
			message := fmt.Sprintf("Error running PreExecRule: %v", err)
			log.ApplicationBackupLog(backup).Errorf(message)
			a.recorder.Event(backup,
				v1.EventTypeWarning,
				string(stork_api.ApplicationBackupStatusFailed),
				message)
			key := runtimeclient.ObjectKeyFromObject(backup)
			if err != nil {
				return err
			}
			err = a.client.Get(context.TODO(), key, backup)
			if err != nil {
				return err
			}
			backup.Status.Stage = stork_api.ApplicationBackupStageFinal
			backup.Status.Status = stork_api.ApplicationBackupStatusFailed
			backup.Status.Reason = message
			backup.Status.LastUpdateTimestamp = metav1.Now()
			err = a.client.Update(context.TODO(), backup)
			if err != nil {
				return err
			}
			return nil
		}
		if inProgress {
			return nil
		}
		fallthrough
	case stork_api.ApplicationBackupStageVolumes:
		err := a.backupVolumes(backup, terminationChannels)
		if err != nil {
			message := fmt.Sprintf("Error backing up volumes: %v", err)
			log.ApplicationBackupLog(backup).Errorf(message)
			a.recorder.Event(backup,
				v1.EventTypeWarning,
				string(stork_api.ApplicationBackupStatusFailed),
				message)
			if _, ok := err.(*volume.ErrStorageProviderBusy); ok {
				return errResourceBusy
			}
			return nil
		}
	case stork_api.ApplicationBackupStageApplications:
		err := a.backupResources(backup)
		if err != nil {
			message := fmt.Sprintf("Error backing up resources: %v", err)
			log.ApplicationBackupLog(backup).Errorf(message)
			a.recorder.Event(backup,
				v1.EventTypeWarning,
				string(stork_api.ApplicationBackupStatusFailed),
				message)
			return nil
		}

	case stork_api.ApplicationBackupStageFinal:
		// Do Nothing
		return nil
	default:
		log.ApplicationBackupLog(backup).Errorf("Invalid stage for backup: %v", backup.Status.Stage)
	}

	return nil
}

func (a *ApplicationBackupController) namespaceBackupAllowed(backup *stork_api.ApplicationBackup) bool {
	// If the backup is completed it has probably been synced, don't perform
	// check for those
	if backup.Status.Stage == stork_api.ApplicationBackupStageFinal {
		return true
	}
	// Restrict backups to only the namespace that the object belongs to
	// except for the namespace designated by the admin
	if backup.Namespace != a.backupAdminNamespace {
		for _, ns := range backup.Spec.Namespaces {
			if ns != backup.Namespace {
				return false
			}
		}
	}
	return true
}

func (a *ApplicationBackupController) getDriversForBackup(backup *stork_api.ApplicationBackup) map[string]bool {
	drivers := make(map[string]bool)
	for _, volumeInfo := range backup.Status.Volumes {
		drivers[volumeInfo.DriverName] = true
	}
	return drivers
}

func min(x, y int) int {
	if x <= y {
		return x
	}
	return y
}

func (a *ApplicationBackupController) updateBackupCRInVolumeStage(
	namespacedName types.NamespacedName,
	status stork_api.ApplicationBackupStatusType,
	stage stork_api.ApplicationBackupStageType,
	reason string,
	volumeInfos []*stork_api.ApplicationBackupVolumeInfo,
) (*stork_api.ApplicationBackup, error) {
	backup := &stork_api.ApplicationBackup{}
	var err error
	for i := 0; i < maxRetry; i++ {
		err := a.client.Get(context.TODO(), namespacedName, backup)
		if err != nil {
			time.Sleep(retrySleep)
			continue
		}
		// since updateBackupCRInVolumeStage called during volume stage , make sure
		// we are not re-reading CR contents and updating application/final stage to
		// volume stage again
		if backup.Status.Stage == stork_api.ApplicationBackupStageFinal ||
			backup.Status.Stage == stork_api.ApplicationBackupStageApplications {
			return backup, nil
		}
		backup.Status.Status = status
		backup.Status.Stage = stage
		backup.Status.Reason = reason
		backup.Status.LastUpdateTimestamp = metav1.Now()
		if volumeInfos != nil {
			backup.Status.Volumes = append(backup.Status.Volumes, volumeInfos...)
		}
		err = a.client.Update(context.TODO(), backup)
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
	return backup, nil
}

func (a *ApplicationBackupController) backupVolumes(backup *stork_api.ApplicationBackup, terminationChannels []chan bool) error {
	defer func() {
		for _, channel := range terminationChannels {
			channel <- true
		}
	}()
	var err error
	// Start backup of the volumes if we don't have any status stored
	pvcMappings := make(map[string][]v1.PersistentVolumeClaim)

	backupStatusVolMap := make(map[string]string)
	for _, statusVolume := range backup.Status.Volumes {
		backupStatusVolMap[statusVolume.Namespace+"-"+statusVolume.PersistentVolumeClaim] = ""
	}

	backup.Status.Stage = stork_api.ApplicationBackupStageVolumes
	namespacedName := types.NamespacedName{}
	if IsVolsToBeBackedUp(backup) {
		isResourceTypePVC := IsResourceTypePVC(backup)
		objectMap := stork_api.CreateObjectsMap(backup.Spec.IncludeResources)
		info := stork_api.ObjectInfo{
			GroupVersionKind: metav1.GroupVersionKind{
				Group:   "core",
				Version: "v1",
				Kind:    "PersistentVolumeClaim",
			},
		}

		var pvcCount int
		for _, namespace := range backup.Spec.Namespaces {
			pvcList, err := core.Instance().GetPersistentVolumeClaims(namespace, backup.Spec.Selectors)
			if err != nil {
				return fmt.Errorf("error getting list of volumes to migrate: %v", err)
			}

			for _, pvc := range pvcList.Items {
				// If a list of resources was specified during backup check if
				// this PVC was included
				info.Name = pvc.Name
				info.Namespace = pvc.Namespace
				if len(objectMap) != 0 {
					if resourcecollector.IsNsPresentInIncludeResource(objectMap, namespace) {
						if val, present := objectMap[info]; !present || !val {
							continue
						}
					} else {
						// We could have case where includeResource has data, current ns is not part of includeResource
						// and the user has given ResourceType != PVC. In this case we don't
						// want to collect vol data from this ns
						if !isResourceTypePVC {
							break
						}
						return err
					}
				}

				// Don't backup pending or deleting PVCs
				if pvc.Status.Phase != v1.ClaimBound || pvc.DeletionTimestamp != nil {
					continue
				}
				driverName, err := volume.GetPVCDriver(core.Instance(), &pvc)
				if err != nil {
					// Skip unsupported PVCs
					if _, ok := err.(*errors.ErrNotSupported); ok {
						continue
					}
					return err
				}

				if driverName != "" {
					// This PVC needs to be backed up
					pvcCount++
					if pvcMappings[driverName] == nil {
						pvcMappings[driverName] = make([]v1.PersistentVolumeClaim, 0)
					}
					// Don't backup PVCs which are already added to Status and for
					// which backup was triggered
					if _, isVolBackupDone := backupStatusVolMap[pvc.Namespace+"-"+pvc.Name]; isVolBackupDone {
						continue
					}
					pvcMappings[driverName] = append(pvcMappings[driverName], pvc)
					backupStatusVolMap[pvc.Namespace+"-"+pvc.Name] = ""
				}
			}
		}

		if backup.Status.Volumes == nil {
			backup.Status.Volumes = make([]*stork_api.ApplicationBackupVolumeInfo, 0)
		}

		namespacedName.Namespace = backup.Namespace
		namespacedName.Name = backup.Name
		if len(backup.Status.Volumes) != pvcCount {

			for driverName, pvcs := range pvcMappings {
				driver, err := volume.Get(driverName)
				if err != nil {
					return err
				}
				batchCount := defaultBackupVolumeBatchCount
				if len(os.Getenv(backupVolumeBatchCountEnvVar)) != 0 {
					batchCount, err = strconv.Atoi(os.Getenv(backupVolumeBatchCountEnvVar))
					if err != nil {
						batchCount = defaultBackupVolumeBatchCount
					}
				}
				for i := 0; i < len(pvcs); i += batchCount {
					batch := pvcs[i:min(i+batchCount, len(pvcs))]
					volumeInfos, err := driver.StartBackup(backup, batch)
					if err != nil {
						// TODO: If starting backup for a drive fails mark the entire backup
						// as Cancelling, cancel any other started backups and then mark
						// it as failed
						if _, ok := err.(*volume.ErrStorageProviderBusy); ok {
							inProgressMsg := fmt.Sprintf("Volume backups are in progress. Backups are failing for some volumes"+
								" since the storage provider is busy: %v. Backup will be retried", err)
							log.ApplicationBackupLog(backup).Errorf(inProgressMsg)
							a.recorder.Event(backup,
								v1.EventTypeWarning,
								string(stork_api.ApplicationBackupStatusInProgress),
								inProgressMsg)
							backup, updateErr := a.updateBackupCRInVolumeStage(
								namespacedName,
								stork_api.ApplicationBackupStatusInProgress,
								backup.Status.Stage,
								inProgressMsg,
								volumeInfos,
							)
							if updateErr != nil {
								log.ApplicationBackupLog(backup).Errorf("failed to update backup object: %v", updateErr)
							}
							return err
						}
						message := fmt.Sprintf("Error starting ApplicationBackup for volumes: %v", err)
						log.ApplicationBackupLog(backup).Errorf(message)
						a.recorder.Event(backup,
							v1.EventTypeWarning,
							string(stork_api.ApplicationBackupStatusFailed),
							message)
						_, err = a.updateBackupCRInVolumeStage(
							namespacedName,
							stork_api.ApplicationBackupStatusFailed,
							stork_api.ApplicationBackupStageFinal,
							message,
							nil,
						)
						return err
					}
					backup, err = a.updateBackupCRInVolumeStage(
						namespacedName,
						stork_api.ApplicationBackupStatusInProgress,
						backup.Status.Stage,
						"Volume backups are in progress",
						volumeInfos,
					)
					if err != nil {
						return err
					}
				}
			}

			// Terminate any background rules that were started
			for _, channel := range terminationChannels {
				channel <- true
			}
			terminationChannels = nil

			// Run any post exec rules once backup is triggered
			if backup.Spec.PostExecRule != "" {
				err = a.runPostExecRule(backup)
				if err != nil {
					message := fmt.Sprintf("Error running PostExecRule: %v", err)
					log.ApplicationBackupLog(backup).Errorf(message)
					a.recorder.Event(backup,
						v1.EventTypeWarning,
						string(stork_api.ApplicationBackupStatusFailed),
						message)

					backup.Status.Stage = stork_api.ApplicationBackupStageFinal
					backup.Status.FinishTimestamp = metav1.Now()
					backup.Status.LastUpdateTimestamp = metav1.Now()
					backup.Status.Status = stork_api.ApplicationBackupStatusFailed
					backup.Status.Reason = message
					err = a.client.Update(context.TODO(), backup)
					if err != nil {
						return err
					}
					return fmt.Errorf("%v", message)
				}
			}
		}

		inProgress := false
		// Skip checking status if no volumes are being backed up
		if len(backup.Status.Volumes) != 0 {
			drivers := a.getDriversForBackup(backup)

			volumeInfos := make([]*stork_api.ApplicationBackupVolumeInfo, 0)
			for driverName := range drivers {

				driver, err := volume.Get(driverName)
				if err != nil {
					return err
				}

				status, err := driver.GetBackupStatus(backup)
				if err != nil {
					return fmt.Errorf("error getting backup status for driver %v: %v", driverName, err)
				}
				volumeInfos = append(volumeInfos, status...)
			}
			backup.Status.Volumes = volumeInfos

			// Now check if there is any failure or success
			// TODO: On failure of one volume cancel other backups?
			for _, vInfo := range volumeInfos {
				if vInfo.Status == stork_api.ApplicationBackupStatusInProgress || vInfo.Status == stork_api.ApplicationBackupStatusInitial ||
					vInfo.Status == stork_api.ApplicationBackupStatusPending {
					log.ApplicationBackupLog(backup).Infof("Volume backup still in progress: %v", vInfo.Volume)
					inProgress = true
				} else if vInfo.Status == stork_api.ApplicationBackupStatusFailed {
					a.recorder.Event(backup,
						v1.EventTypeWarning,
						string(vInfo.Status),
						fmt.Sprintf("Error backing up volume %v: %v", vInfo.Volume, vInfo.Reason))
					backup.Status.Stage = stork_api.ApplicationBackupStageFinal
					backup.Status.FinishTimestamp = metav1.Now()
					backup.Status.Status = stork_api.ApplicationBackupStatusFailed
					backup.Status.Reason = vInfo.Reason
					break
				} else if vInfo.Status == stork_api.ApplicationBackupStatusSuccessful {
					a.recorder.Event(backup,
						v1.EventTypeNormal,
						string(vInfo.Status),
						fmt.Sprintf("Volume %v backed up successfully", vInfo.Volume))
				}
			}
		}

		// Return if we have any volume backups still in progress
		if inProgress {
			// temporarily store the volume status, So that it will be used during retry.
			volumeInfos := backup.Status.Volumes
			backup.Status.LastUpdateTimestamp = metav1.Now()
			// Store the new status
			err = a.client.Update(context.TODO(), backup)
			if err != nil {
				for i := 0; i < maxRetry; i++ {
					err = a.client.Get(context.TODO(), namespacedName, backup)
					if err != nil {
						time.Sleep(retrySleep)
						continue
					}
					if backup.Status.Stage == stork_api.ApplicationBackupStageFinal {
						return nil
					}
					backup.Status.Volumes = volumeInfos
					backup.Status.LastUpdateTimestamp = metav1.Now()
					err = a.client.Update(context.TODO(), backup)
					if err != nil {
						time.Sleep(retrySleep)
						continue
					} else {
						break
					}
				}
				if err != nil {
					return err
				}
			}
			return nil
		}
	}

	// If the backup hasn't failed move on to the next stage.
	if backup.Status.Status != stork_api.ApplicationBackupStatusFailed {
		backup.Status.Stage = stork_api.ApplicationBackupStageApplications
		backup.Status.Status = stork_api.ApplicationBackupStatusInProgress
		backup.Status.Reason = "Application resources backup is in progress"
		backup.Status.LastUpdateTimestamp = metav1.Now()
		// temporarily store the volume status, So that it will be used during retry.
		volumeInfos := backup.Status.Volumes
		// Update the current state and then move on to backing up resources
		err := a.client.Update(context.TODO(), backup)
		if err != nil {
			for i := 0; i < maxRetry; i++ {
				err = a.client.Get(context.TODO(), namespacedName, backup)
				if err != nil {
					time.Sleep(retrySleep)
					continue
				}
				if backup.Status.Stage == stork_api.ApplicationBackupStageFinal ||
					backup.Status.Stage == stork_api.ApplicationBackupStageApplications {
					return nil
				}
				backup.Status.Stage = stork_api.ApplicationBackupStageApplications
				backup.Status.Status = stork_api.ApplicationBackupStatusInProgress
				backup.Status.Reason = "Application resources backup is in progress"
				backup.Status.LastUpdateTimestamp = metav1.Now()
				backup.Status.Volumes = volumeInfos
				err = a.client.Update(context.TODO(), backup)
				if err != nil {
					time.Sleep(retrySleep)
					continue
				} else {
					break
				}
			}
			if err != nil {
				return err
			}
		}

		err = a.backupResources(backup)
		if err != nil {
			message := fmt.Sprintf("Error backing up resources: %v", err)
			log.ApplicationBackupLog(backup).Errorf(message)
			a.recorder.Event(backup,
				v1.EventTypeWarning,
				string(stork_api.ApplicationBackupStatusFailed),
				message)
			return err
		}
	}

	backup.Status.LastUpdateTimestamp = metav1.Now()
	err = a.client.Update(context.TODO(), backup)
	if err != nil {
		return err
	}
	return nil
}

func (a *ApplicationBackupController) runPreExecRule(backup *stork_api.ApplicationBackup) ([]chan bool, bool, error) {
	if backup.Spec.PreExecRule == "" {
		backup.Status.Stage = stork_api.ApplicationBackupStageVolumes
		backup.Status.Status = stork_api.ApplicationBackupStatusPending
		backup.Status.LastUpdateTimestamp = metav1.Now()
		err := a.client.Update(context.TODO(), backup)
		if err != nil {
			// Ignore error and return true so that it can be reconciled again
			return nil, true, nil
		}
		return nil, false, nil
	}

	backup.Status.Stage = stork_api.ApplicationBackupStagePreExecRule
	backup.Status.Status = stork_api.ApplicationBackupStatusInProgress
	backup.Status.Reason = "Pre-Exec rules are being executed"
	backup.Status.LastUpdateTimestamp = metav1.Now()
	err := a.client.Update(context.TODO(), backup)
	if err != nil {
		// Ignore error and return true so that it can be reconciled again
		return nil, true, nil
	}
	// Get the latest object so that the rules engine can update annotations if
	// required
	key := runtimeclient.ObjectKeyFromObject(backup)
	if err != nil {
		return nil, false, err
	}
	err = a.client.Get(context.TODO(), key, backup)
	if err != nil {
		return nil, false, err
	}

	terminationChannels := make([]chan bool, 0)
	r, err := storkops.Instance().GetRule(backup.Spec.PreExecRule, backup.Namespace)
	if err != nil {
		// TODO: For now keep this as is from the existing code, not sure the use of this for loop
		// as it currently doesn't get executed
		for _, channel := range terminationChannels {
			channel <- true
		}
		return nil, false, err
	}
	for _, ns := range backup.Spec.Namespaces {
		ch, err := rule.ExecuteRule(r, rule.PreExecRule, backup, ns)
		if err != nil {
			for _, channel := range terminationChannels {
				channel <- true
			}
			return nil, false, fmt.Errorf("error executing PreExecRule for namespace %v: %v", ns, err)
		}
		if ch != nil {
			terminationChannels = append(terminationChannels, ch)
		}
	}

	// Get the latest object again since the rules engine could have updated
	// annotations
	key = runtimeclient.ObjectKeyFromObject(backup)
	if err != nil {
		for _, channel := range terminationChannels {
			channel <- true
		}
		return nil, false, err
	}
	err = a.client.Get(context.TODO(), key, backup)
	if err != nil {
		for _, channel := range terminationChannels {
			channel <- true
		}
		return nil, false, err
	}
	return terminationChannels, false, nil
}

func (a *ApplicationBackupController) runPostExecRule(backup *stork_api.ApplicationBackup) error {
	r, err := storkops.Instance().GetRule(backup.Spec.PostExecRule, backup.Namespace)
	if err != nil {
		return err
	}
	for _, ns := range backup.Spec.Namespaces {
		_, err = rule.ExecuteRule(r, rule.PostExecRule, backup, ns)
		if err != nil {
			return fmt.Errorf("error executing PreExecRule for namespace %v: %v", ns, err)
		}
	}
	return nil
}

func (a *ApplicationBackupController) prepareResources(
	backup *stork_api.ApplicationBackup,
	objects []runtime.Unstructured,
) error {
	return nil
}

// GetObjectPath construct the full base path for a given backup
// The format is "namespace/backupName/backupUID" which will be unique for each backup
func GetObjectPath(
	backup *stork_api.ApplicationBackup,
) string {
	return filepath.Join(backup.Namespace, backup.Name, string(backup.UID))
}

// Uploads the given data to the backup location specified in the backup object
func (a *ApplicationBackupController) uploadObject(
	backup *stork_api.ApplicationBackup,
	objectName string,
	data []byte,
) error {
	backupLocation, err := storkops.Instance().GetBackupLocation(backup.Spec.BackupLocation, backup.Namespace)
	if err != nil {
		return err
	}
	bucket, err := objectstore.GetBucket(backupLocation)
	if err != nil {
		return err
	}

	if backupLocation.Location.EncryptionKey != "" {
		if data, err = crypto.Encrypt(data, backupLocation.Location.EncryptionKey); err != nil {
			return err
		}
	}

	objectPath := GetObjectPath(backup)
	writer, err := bucket.NewWriter(context.TODO(), filepath.Join(objectPath, objectName), nil)
	if err != nil {
		return err
	}

	_, err = writer.Write(data)
	if err != nil {
		closeErr := writer.Close()
		if closeErr != nil {
			log.ApplicationBackupLog(backup).Errorf("Error closing writer for objectstore: %v", closeErr)
		}
		return err
	}
	err = writer.Close()
	if err != nil {
		log.ApplicationBackupLog(backup).Errorf("Error closing writer for objectstore: %v", err)
		return err
	}
	return nil
}

// Convert the list of objects to json and upload to the backup location
func (a *ApplicationBackupController) uploadResources(
	backup *stork_api.ApplicationBackup,
	objects []runtime.Unstructured,
) error {
	resKinds := make(map[string]string)
	for _, obj := range objects {
		gvk := obj.GetObjectKind().GroupVersionKind()
		resKinds[gvk.Kind] = gvk.Version
	}
	if err := a.uploadNamespaces(backup); err != nil {
		return err
	}
	// upload CRD to backuplocation
	if err := a.uploadCRDResources(backup, resKinds); err != nil {
		return err
	}
	jsonBytes, err := json.MarshalIndent(objects, "", " ")
	if err != nil {
		return err
	}
	// TODO: Encrypt if requested
	return a.uploadObject(backup, resourceObjectName, jsonBytes)
}
func (a *ApplicationBackupController) uploadNamespaces(backup *stork_api.ApplicationBackup) error {
	var namespaces []*v1.Namespace
	for _, namespace := range backup.Spec.Namespaces {
		ns, err := core.Instance().GetNamespace(namespace)
		if err != nil {
			return err
		}
		ns.ResourceVersion = ""
		namespaces = append(namespaces, ns)
	}
	jsonBytes, err := json.MarshalIndent(namespaces, "", " ")
	if err != nil {
		return err
	}
	if err := a.uploadObject(backup, nsObjectName, jsonBytes); err != nil {
		return err
	}
	return nil
}

func (a *ApplicationBackupController) uploadCRDResources(backup *stork_api.ApplicationBackup, resKinds map[string]string) error {
	crdList, err := storkops.Instance().ListApplicationRegistrations()
	if err != nil {
		return err
	}
	var crds []*apiextensionsv1beta1.CustomResourceDefinition
	for _, crd := range crdList.Items {
		for _, v := range crd.Resources {
			if _, ok := resKinds[v.Kind]; !ok {
				continue
			}
			crdName := inflect.Pluralize(strings.ToLower(v.Kind)) + "." + v.Group
			res, err := apiextensions.Instance().GetCRD(crdName, metav1.GetOptions{})
			if err != nil {
				if k8s_errors.IsNotFound(err) {
					continue
				}
				log.ApplicationBackupLog(backup).Errorf("Unable to get customresourcedefination for %s, err: %v", v.Kind, err)
				return err
			}
			crds = append(crds, res)
		}

	}
	jsonBytes, err := json.MarshalIndent(crds, "", " ")
	if err != nil {
		return err
	}
	if err := a.uploadObject(backup, crdObjectName, jsonBytes); err != nil {
		return err
	}
	return nil
}

// Upload the backup object which should have all the required metadata
func (a *ApplicationBackupController) uploadMetadata(
	backup *stork_api.ApplicationBackup,
) error {
	jsonBytes, err := json.MarshalIndent(backup, "", " ")
	if err != nil {
		return err
	}

	return a.uploadObject(backup, metadataObjectName, jsonBytes)
}

func (a *ApplicationBackupController) backupResources(
	backup *stork_api.ApplicationBackup,
) error {
	var err error
	var resourceTypes []metav1.APIResource
	// Listing all resource types
	if len(backup.Spec.ResourceTypes) != 0 {
		optionalResourceTypes := []string{}
		resourceTypes, err = a.resourceCollector.GetResourceTypes(optionalResourceTypes, true)
		if err != nil {
			log.ApplicationBackupLog(backup).Errorf("Error getting resource types: %v", err)
			return err
		}
	}

	// Don't modify resources if mentioned explicitly in specs
	if backup.Spec.SkipServiceUpdate {
		if a.resourceCollector.Opts == nil {
			a.resourceCollector.Opts = make(map[string]string)
		}
		a.resourceCollector.Opts[resourcecollector.ServiceKind] = "true"
	}

	// Always backup optional resources. When restorting they need to be
	// explicitly added to the spec
	objectMap := stork_api.CreateObjectsMap(backup.Spec.IncludeResources)
	namespacelist := backup.Spec.Namespaces
	// GetResources takes more time, if we have more number of namespaces
	// So, submitting it in batches and in between each batch,
	// updating the LastUpdateTimestamp to show that backup is progressing
	allObjects := make([]runtime.Unstructured, 0)
	for i := 0; i < len(namespacelist); i += backupResourcesBatchCount {
		batch := namespacelist[i:min(i+backupResourcesBatchCount, len(namespacelist))]
		var incResNsBatch []string
		var resourceTypeNsBatch []string
		for _, ns := range batch {
			// As we support both includeResource and ResourceType to be mentioned
			// match out ns for which we want to take includeResource path and
			// for which we want to take ResourceType path
			if len(backup.Spec.ResourceTypes) != 0 {
				if !resourcecollector.IsNsPresentInIncludeResource(objectMap, ns) {
					resourceTypeNsBatch = append(resourceTypeNsBatch, ns)
				} else {
					incResNsBatch = append(incResNsBatch, ns)
				}
			} else {
				incResNsBatch = batch
			}
		}

		if len(incResNsBatch) != 0 {
			objects, err := a.resourceCollector.GetResources(
				incResNsBatch,
				backup.Spec.Selectors,
				objectMap,
				optionalBackupResources,
				true)
			if err != nil {
				log.ApplicationBackupLog(backup).Errorf("Error getting resources: %v", err)
				return err
			}
			allObjects = append(allObjects, objects...)
		}

		if len(resourceTypeNsBatch) != 0 {
			for _, backupResourceType := range backup.Spec.ResourceTypes {
				for _, resource := range resourceTypes {
					if resource.Kind == backupResourceType {
						objects, err := a.resourceCollector.GetResourcesForType(resource, nil, resourceTypeNsBatch, backup.Spec.Selectors, nil, true)
						if err != nil {
							log.ApplicationBackupLog(backup).Errorf("Error getting resources: %v", err)
							return err
						}
						allObjects = append(allObjects, objects.Items...)
					}
				}
			}
		}

		// Do a dummy update to the backup CR to update only the last update timestamp
		namespacedName := types.NamespacedName{}
		namespacedName.Namespace = backup.Namespace
		namespacedName.Name = backup.Name
		for i := 0; i < maxRetry; i++ {
			err = a.client.Get(context.TODO(), namespacedName, backup)
			if err != nil {
				time.Sleep(retrySleep)
				continue
			}
			if backup.Status.Stage == stork_api.ApplicationBackupStageFinal {
				return nil
			}
			backup.Status.LastUpdateTimestamp = metav1.Now()
			err = a.client.Update(context.TODO(), backup)
			if err != nil {
				time.Sleep(retrySleep)
				continue
			} else {
				break
			}
		}
	}
	updatedAllObjects := make([]runtime.Unstructured, 0)
	for _, obj := range allObjects {
		resourceMap := make(map[types.UID]bool)
		metadata, err := meta.Accessor(obj)
		if err != nil {
			return err
		}
		if _, ok := resourceMap[metadata.GetUID()]; ok {
			continue
		}
		resourceMap[metadata.GetUID()] = true
		updatedAllObjects = append(updatedAllObjects, obj)
	}

	if backup.Status.Resources == nil {
		// Save the collected resources infos in the status
		resourceInfos := make([]*stork_api.ApplicationBackupResourceInfo, 0)
		for _, obj := range updatedAllObjects {
			metadata, err := meta.Accessor(obj)
			if err != nil {
				return err
			}

			resourceInfo := &stork_api.ApplicationBackupResourceInfo{
				ObjectInfo: stork_api.ObjectInfo{
					Name:      metadata.GetName(),
					Namespace: metadata.GetNamespace(),
				},
			}
			gvk := obj.GetObjectKind().GroupVersionKind()
			resourceInfo.Kind = gvk.Kind
			resourceInfo.Group = gvk.Group
			// core Group doesn't have a name, so override it
			if resourceInfo.Group == "" {
				resourceInfo.Group = "core"
			}
			resourceInfo.Version = gvk.Version
			resourceInfos = append(resourceInfos, resourceInfo)
		}
		backup.Status.Resources = resourceInfos
		backup.Status.LastUpdateTimestamp = metav1.Now()
		// Store the new status
		err = a.client.Update(context.TODO(), backup)
		if err != nil {
			return err
		}
		return nil
	}

	// Do any additional preparation for the resources if required
	if err = a.prepareResources(backup, updatedAllObjects); err != nil {
		message := fmt.Sprintf("Error preparing resources for backup: %v", err)
		backup.Status.Status = stork_api.ApplicationBackupStatusFailed
		backup.Status.Stage = stork_api.ApplicationBackupStageFinal
		backup.Status.Reason = message
		backup.Status.LastUpdateTimestamp = metav1.Now()
		err = a.client.Update(context.TODO(), backup)
		if err != nil {
			return err
		}
		a.recorder.Event(backup,
			v1.EventTypeWarning,
			string(stork_api.ApplicationBackupStatusFailed),
			message)
		log.ApplicationBackupLog(backup).Errorf(message)
		return err
	}

	// Upload the resources to the backup location
	if err = a.uploadResources(backup, updatedAllObjects); err != nil {
		message := fmt.Sprintf("Error uploading resources: %v", err)
		backup.Status.Status = stork_api.ApplicationBackupStatusFailed
		backup.Status.Stage = stork_api.ApplicationBackupStageFinal
		backup.Status.Reason = message
		backup.Status.LastUpdateTimestamp = metav1.Now()
		err = a.client.Update(context.TODO(), backup)
		if err != nil {
			return err
		}
		a.recorder.Event(backup,
			v1.EventTypeWarning,
			string(stork_api.ApplicationBackupStatusFailed),
			message)
		log.ApplicationBackupLog(backup).Errorf(message)
		return err
	}
	backup.Status.BackupPath = GetObjectPath(backup)
	backup.Status.Stage = stork_api.ApplicationBackupStageFinal
	backup.Status.FinishTimestamp = metav1.Now()
	backup.Status.Status = stork_api.ApplicationBackupStatusSuccessful
	backup.Status.Reason = "Volumes and resources were backed up successfully"

	// Only on success compute the total backup size
	for _, vInfo := range backup.Status.Volumes {
		backup.Status.TotalSize += vInfo.TotalSize
	}
	// Upload the metadata for the backup to the backup location
	if err = a.uploadMetadata(backup); err != nil {
		a.recorder.Event(backup,
			v1.EventTypeWarning,
			string(stork_api.ApplicationBackupStatusFailed),
			fmt.Sprintf("Error uploading metadata: %v", err))
		log.ApplicationBackupLog(backup).Errorf("Error uploading metadata: %v", err)
		return err
	}

	backup.Status.LastUpdateTimestamp = metav1.Now()

	if err = a.client.Update(context.TODO(), backup); err != nil {
		return err
	}

	return nil
}

func (a *ApplicationBackupController) deleteBackup(backup *stork_api.ApplicationBackup) error {
	// Only delete the backup from the backupLocation if the ReclaimPolicy is
	// set to Delete or if it is not successful
	if backup.Spec.ReclaimPolicy != stork_api.ApplicationBackupReclaimPolicyDelete &&
		backup.Status.Status == stork_api.ApplicationBackupStatusSuccessful {
		return nil
	}

	drivers := a.getDriversForBackup(backup)
	for driverName := range drivers {

		driver, err := volume.Get(driverName)
		if err != nil {
			return err
		}

		// Ignore error when cancelling since completed ones could possibly not be
		// cancelled
		if err := driver.CancelBackup(backup); err != nil {
			log.ApplicationBackupLog(backup).Debugf("Error cancelling backup: %v", err)
		}

		if err := driver.DeleteBackup(backup); err != nil {
			return err
		}
	}

	backupLocation, err := storkops.Instance().GetBackupLocation(backup.Spec.BackupLocation, backup.Namespace)
	if err != nil {
		// Can't do anything if the backup location is deleted
		if k8s_errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	bucket, err := objectstore.GetBucket(backupLocation)
	if err != nil {
		return err
	}

	objectPath := backup.Status.BackupPath
	if objectPath != "" {
		if err = bucket.Delete(context.TODO(), filepath.Join(objectPath, resourceObjectName)); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
			return fmt.Errorf("error deleting resources for backup %v/%v: %v", backup.Namespace, backup.Name, err)
		}

		if err = bucket.Delete(context.TODO(), filepath.Join(objectPath, metadataObjectName)); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
			return fmt.Errorf("error deleting metadata for backup %v/%v: %v", backup.Namespace, backup.Name, err)
		}

		if err = bucket.Delete(context.TODO(), filepath.Join(objectPath, crdObjectName)); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
			return fmt.Errorf("error deleting crds for backup %v/%v: %v", backup.Namespace, backup.Name, err)
		}

		if err = bucket.Delete(context.TODO(), filepath.Join(objectPath, nsObjectName)); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
			return fmt.Errorf("error deleting namespaces for backup %v/%v: %v", backup.Namespace, backup.Name, err)
		}
	}

	return nil
}

func (a *ApplicationBackupController) createCRD() error {
	resource := apiextensions.CustomResource{
		Name:    stork_api.ApplicationBackupResourceName,
		Plural:  stork_api.ApplicationBackupResourcePlural,
		Group:   stork.GroupName,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.ApplicationBackup{}).Name(),
	}
	err := apiextensions.Instance().CreateCRD(resource)
	if err != nil && !k8s_errors.IsAlreadyExists(err) {
		return err
	}

	return apiextensions.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}

// IsVolsToBeBackedUp for a given backupspec do we need to have volumes backed up
func IsVolsToBeBackedUp(backup *stork_api.ApplicationBackup) bool {
	// If ResourceType is mentioned and doesn't have PVC in it we would
	// like to skip the vol backups IFF includeResources doesn't have any ref to PVC
	if len(backup.Spec.ResourceTypes) != 0 {
		if IsResourceTypePVC(backup) {
			return true
		}

		// Now we know ResourceType doesn't have PVC, but user could have given a includeResource
		// which could have entry to backup a PVC
		objectInfo := backup.Spec.IncludeResources
		for _, object := range objectInfo {
			if object.Kind == "PersistentVolumeClaim" {
				return true
			}
		}
		return false
	}

	return true
}

// IsResourceTypePVC check if given ResourceType is PVC
func IsResourceTypePVC(backup *stork_api.ApplicationBackup) bool {
	for _, resType := range backup.Spec.ResourceTypes {
		if resType != "PersistentVolumeClaim" {
			continue
		} else {
			return true
		}
	}

	return false
}
