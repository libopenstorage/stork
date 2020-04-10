package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/apis/stork"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	"github.com/libopenstorage/stork/pkg/crypto"
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
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
	metadataObjectName = "metadata.json"

	backupCancelBackoffInitialDelay = 5 * time.Second
	backupCancelBackoffFactor       = 1
	backupCancelBackoffSteps        = math.MaxInt32
)

var backupCancelBackoff = wait.Backoff{
	Duration: backupCancelBackoffInitialDelay,
	Factor:   backupCancelBackoffFactor,
	Steps:    backupCancelBackoffSteps,
}

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
}

// Init Initialize the application backup controller
func (a *ApplicationBackupController) Init(mgr manager.Manager, backupAdminNamespace string) error {
	err := a.createCRD()
	if err != nil {
		return err
	}

	a.backupAdminNamespace = backupAdminNamespace
	if err := a.performRuleRecovery(); err != nil {
		logrus.Errorf("Failed to perform recovery for backup rules: %v", err)
		return err
	}

	return controllers.RegisterTo(mgr, "application-backup-controller", a, &stork_api.ApplicationBackup{})
}

// Reconcile updates for ApplicationBackup objects.
func (a *ApplicationBackupController) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	logrus.Tracef("Reconciling ApplicationBackup %s/%s", request.Namespace, request.Name)

	// Fetch the ApplicationBackup instance
	backup := &stork_api.ApplicationBackup{}
	err := a.client.Get(context.TODO(), request.NamespacedName, backup)
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

	if !controllers.ContainsFinalizer(backup, controllers.FinalizerCleanup) {
		controllers.SetFinalizer(backup, controllers.FinalizerCleanup)
		return reconcile.Result{Requeue: true}, a.client.Update(context.TODO(), backup)
	}

	if err = a.handle(context.TODO(), backup); err != nil {
		logrus.Errorf("%s: %s/%s: %s", reflect.TypeOf(a), backup.Namespace, backup.Name, err)
		return reconcile.Result{RequeueAfter: controllers.DefaultRequeueError}, err
	}

	return reconcile.Result{RequeueAfter: controllers.DefaultRequeue}, nil
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

func (a *ApplicationBackupController) setDefaults(backup *stork_api.ApplicationBackup) {
	if backup.Spec.ReclaimPolicy == "" {
		backup.Spec.ReclaimPolicy = stork_api.ApplicationBackupReclaimPolicyDelete
	}
	if backup.Status.TriggerTimestamp.IsZero() {
		backup.Status.TriggerTimestamp = backup.CreationTimestamp
	}
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

	a.setDefaults(backup)
	switch backup.Status.Stage {
	case stork_api.ApplicationBackupStageInitial:
		// Make sure the namespaces exist
		for _, ns := range backup.Spec.Namespaces {
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
					log.ApplicationBackupLog(backup).Errorf("Error updating")
				}
				return nil
			}
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
		terminationChannels, err = a.runPreExecRule(backup)
		if err != nil {
			message := fmt.Sprintf("Error running PreExecRule: %v", err)
			log.ApplicationBackupLog(backup).Errorf(message)
			a.recorder.Event(backup,
				v1.EventTypeWarning,
				string(stork_api.ApplicationBackupStatusFailed),
				message)
			backup.Status.Stage = stork_api.ApplicationBackupStageInitial
			backup.Status.Status = stork_api.ApplicationBackupStatusInitial
			backup.Status.LastUpdateTimestamp = metav1.Now()
			err := a.client.Update(context.TODO(), backup)
			if err != nil {
				return err
			}
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

func (a *ApplicationBackupController) backupVolumes(backup *stork_api.ApplicationBackup, terminationChannels []chan bool) error {
	defer func() {
		for _, channel := range terminationChannels {
			channel <- true
		}
	}()

	// Start backup of the volumes if we don't have any status stored
	if backup.Status.Volumes == nil {
		pvcMappings := make(map[string][]v1.PersistentVolumeClaim)
		backup.Status.Stage = stork_api.ApplicationBackupStageVolumes
		backup.Status.Volumes = make([]*stork_api.ApplicationBackupVolumeInfo, 0)
		for _, namespace := range backup.Spec.Namespaces {
			pvcList, err := core.Instance().GetPersistentVolumeClaims(namespace, backup.Spec.Selectors)
			if err != nil {
				return fmt.Errorf("error getting list of volumes to migrate: %v", err)
			}

			for _, pvc := range pvcList.Items {
				driverName, err := volume.GetPVCDriver(&pvc)
				if err != nil {
					return err
				}
				if driverName != "" {
					if pvcMappings[driverName] == nil {
						pvcMappings[driverName] = make([]v1.PersistentVolumeClaim, 0)
					}
					pvcMappings[driverName] = append(pvcMappings[driverName], pvc)
				}
			}
		}
		for driverName, pvcs := range pvcMappings {
			driver, err := volume.Get(driverName)
			if err != nil {
				return err
			}

			volumeInfos, err := driver.StartBackup(backup, pvcs)
			if err != nil {
				// TODO: If starting backup for a drive fails mark the entire backup
				// as Cancelling, cancel any other started backups and then mark
				// it as failed
				message := fmt.Sprintf("Error starting ApplicationBackup for volumes: %v", err)
				log.ApplicationBackupLog(backup).Errorf(message)
				a.recorder.Event(backup,
					v1.EventTypeWarning,
					string(stork_api.ApplicationBackupStatusFailed),
					message)
				backup.Status.Status = stork_api.ApplicationBackupStatusFailed
				backup.Status.Stage = stork_api.ApplicationBackupStageFinal
				backup.Status.Reason = message
				backup.Status.LastUpdateTimestamp = metav1.Now()
				err = a.client.Update(context.TODO(), backup)
				if err != nil {
					return err
				}
				return nil
			}

			backup.Status.Volumes = append(backup.Status.Volumes, volumeInfos...)
		}
		backup.Status.Status = stork_api.ApplicationBackupStatusInProgress
		backup.Status.Reason = "Volume backups are in progress"
		backup.Status.LastUpdateTimestamp = metav1.Now()
		err := a.client.Update(context.TODO(), backup)
		if err != nil {
			return err
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
		var err error
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
		backup.Status.LastUpdateTimestamp = metav1.Now()
		// Store the new status
		err = a.client.Update(context.TODO(), backup)
		if err != nil {
			return err
		}

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
		return nil
	}

	// If the backup hasn't failed move on to the next stage.
	if backup.Status.Status != stork_api.ApplicationBackupStatusFailed {
		backup.Status.Stage = stork_api.ApplicationBackupStageApplications
		backup.Status.Status = stork_api.ApplicationBackupStatusInProgress
		backup.Status.Reason = "Application resources backup is in progress"
		backup.Status.LastUpdateTimestamp = metav1.Now()
		// Update the current state and then move on to backing up resources
		err := a.client.Update(context.TODO(), backup)
		if err != nil {
			return err
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
	err := a.client.Update(context.TODO(), backup)
	if err != nil {
		return err
	}
	return nil
}

func (a *ApplicationBackupController) runPreExecRule(backup *stork_api.ApplicationBackup) ([]chan bool, error) {
	if backup.Spec.PreExecRule == "" {
		backup.Status.Stage = stork_api.ApplicationBackupStageVolumes
		backup.Status.Status = stork_api.ApplicationBackupStatusPending
		backup.Status.LastUpdateTimestamp = metav1.Now()
		err := a.client.Update(context.TODO(), backup)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	if backup.Status.Stage == stork_api.ApplicationBackupStageInitial {
		backup.Status.Stage = stork_api.ApplicationBackupStagePreExecRule
		backup.Status.Status = stork_api.ApplicationBackupStatusInProgress
		backup.Status.Reason = "Pre-Exec rules are being executed"
		backup.Status.LastUpdateTimestamp = metav1.Now()
		err := a.client.Update(context.TODO(), backup)
		if err != nil {
			return nil, err
		}
	} else if backup.Status.Status == stork_api.ApplicationBackupStatusInProgress {
		a.recorder.Event(backup,
			v1.EventTypeNormal,
			string(stork_api.ApplicationBackupStatusInProgress),
			fmt.Sprintf("Waiting for PreExecRule %v", backup.Spec.PreExecRule))
		return nil, nil
	}

	terminationChannels := make([]chan bool, 0)
	for _, ns := range backup.Spec.Namespaces {
		r, err := storkops.Instance().GetRule(backup.Spec.PreExecRule, ns)
		if err != nil {
			for _, channel := range terminationChannels {
				channel <- true
			}
			return nil, err
		}

		ch, err := rule.ExecuteRule(r, rule.PreExecRule, backup, ns)
		if err != nil {
			for _, channel := range terminationChannels {
				channel <- true
			}
			return nil, fmt.Errorf("error executing PreExecRule for namespace %v: %v", ns, err)
		}
		if ch != nil {
			terminationChannels = append(terminationChannels, ch)
		}
	}
	return terminationChannels, nil
}

func (a *ApplicationBackupController) runPostExecRule(backup *stork_api.ApplicationBackup) error {
	for _, ns := range backup.Spec.Namespaces {
		r, err := storkops.Instance().GetRule(backup.Spec.PostExecRule, ns)
		if err != nil {
			return err
		}

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

// Construct the full base path for a given backup
// The format is "namespace/backupName/backupUID" which will be unique for each backup
func (a *ApplicationBackupController) getObjectPath(
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

	objectPath := a.getObjectPath(backup)
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
	jsonBytes, err := json.MarshalIndent(objects, "", " ")
	if err != nil {
		return err
	}
	// TODO: Encrypt if requested
	return a.uploadObject(backup, resourceObjectName, jsonBytes)
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
	allObjects, err := a.resourceCollector.GetResources(backup.Spec.Namespaces, backup.Spec.Selectors, true)
	if err != nil {
		log.ApplicationBackupLog(backup).Errorf("Error getting resources: %v", err)
		return err
	}

	// Save the collected resources infos in the status
	resourceInfos := make([]*stork_api.ApplicationBackupResourceInfo, 0)
	for _, obj := range allObjects {
		metadata, err := meta.Accessor(obj)
		if err != nil {
			return err
		}

		resourceInfo := &stork_api.ApplicationBackupResourceInfo{
			Name:      metadata.GetName(),
			Namespace: metadata.GetNamespace(),
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
	if err = a.client.Update(context.TODO(), backup); err != nil {
		return err
	}

	// Do any additional preparation for the resources if required
	if err = a.prepareResources(backup, allObjects); err != nil {
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
	if err = a.uploadResources(backup, allObjects); err != nil {
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
	backup.Status.BackupPath = a.getObjectPath(backup)
	backup.Status.Stage = stork_api.ApplicationBackupStageFinal
	backup.Status.FinishTimestamp = metav1.Now()
	backup.Status.Status = stork_api.ApplicationBackupStatusSuccessful
	backup.Status.Reason = "Volumes and resources were backed up successfuly"

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
		if errors.IsNotFound(err) {
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
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return apiextensions.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}
