package resourceexport

import (
	"context"
	"fmt"
	"reflect"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controllers"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	kdmpcontroller "github.com/portworx/kdmp/pkg/controllers"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/driversinstance"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/kdmp"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// updateResourceExportFields when an update needs to be done to ResourceExport
// user can choose which field to be updated and pass the same to updateStatus()
type updateResourceExportFields struct {
	stage               kdmpapi.ResourceExportStage
	status              kdmpapi.ResourceExportStatus
	reason              string
	id                  string
	resources           []*kdmpapi.ResourceRestoreResourceInfo
	RestoreCompleteList []*storkapi.ApplicationRestoreVolumeInfo
}

func (c *Controller) process(ctx context.Context, in *kdmpapi.ResourceExport) (bool, error) {
	funct := "re.process"
	if in == nil {
		return false, nil
	}
	resourceExport := in.DeepCopy()
	if resourceExport.DeletionTimestamp != nil {
		if controllers.ContainsFinalizer(resourceExport, kdmpcontroller.CleanupFinalizer) {
			err := c.cleanupResources(resourceExport)
			if err != nil {
				return false, nil
			}
		}
		if resourceExport.GetFinalizers() != nil {
			controllers.RemoveFinalizer(resourceExport, kdmpcontroller.CleanupFinalizer)
			err := c.client.Update(context.TODO(), resourceExport)
			if err != nil {
				errMsg := fmt.Sprintf("failed updating resourceExport CR %s: %v", resourceExport.Name, err)
				logrus.Errorf("%v", errMsg)
				return false, fmt.Errorf("%v", errMsg)
			}
		}
		return true, nil
	}
	if resourceExport.Status.Stage == kdmpapi.ResourceExportStageFinal {
		return true, nil
	}

	// Set to initial status to start with
	if resourceExport.Status.Status == "" {
		updateData := updateResourceExportFields{
			stage:  kdmpapi.ResourceExportStageInitial,
			status: kdmpapi.ResourceExportStatusInitial,
			reason: "",
		}
		return true, c.updateStatus(resourceExport, updateData)
	}
	// Get the driver type
	opType, err := getDriverType(resourceExport)
	if err != nil {
		updateData := updateResourceExportFields{
			status: kdmpapi.ResourceExportStatusFailed,
			reason: "fetching driver type failed",
		}
		return false, c.updateStatus(resourceExport, updateData)
	}

	driver, err := driversinstance.Get(opType)
	if err != nil {
		updateData := updateResourceExportFields{
			status: kdmpapi.ResourceExportStatusFailed,
			reason: "fetching driver instance failed",
		}
		return false, c.updateStatus(resourceExport, updateData)
	}
	blName := resourceExport.Spec.Destination.Name
	blNamespace := resourceExport.Spec.Destination.Namespace
	backupLocation, err := kdmpcontroller.ReadBackupLocation(blName, blNamespace, "")

	if err != nil {
		msg := fmt.Sprintf("reading of backuplocation [%v/%v] failed: %v", blNamespace, blName, err)
		logrus.Errorf(msg)
		updateData := updateResourceExportFields{
			status: kdmpapi.ResourceExportStatusFailed,
			reason: fmt.Sprintf("failed reading bl [%v/%v]: %v", blNamespace, blName, err),
		}
		return false, c.updateStatus(resourceExport, updateData)
	}

	switch resourceExport.Status.Stage {
	case kdmpapi.ResourceExportStageInitial:
		// Create ResourceBackup CR
		err = createResourceBackup(resourceExport.Name, resourceExport.Namespace)
		if err != nil {
			updateData := updateResourceExportFields{
				stage:  kdmpapi.ResourceExportStageFinal,
				status: kdmpapi.ResourceExportStatusFailed,
				reason: fmt.Sprintf("failed to create ResourceBackup CR [%v/%v]", resourceExport.Namespace, resourceExport.Name),
			}
			return false, c.updateStatus(resourceExport, updateData)
		}
		// start data transfer
		id, serr := startNfsResourceJob(
			driver,
			utils.KdmpConfigmapName,
			utils.KdmpConfigmapNamespace,
			resourceExport,
			backupLocation,
		)
		logrus.Tracef("%s: startNfsResourceJob id: %v", funct, id)
		if serr != nil {
			logrus.Errorf("%s: serr: %v", funct, serr)
			updateData := updateResourceExportFields{
				stage:  kdmpapi.ResourceExportStageFinal,
				status: kdmpapi.ResourceExportStatusFailed,
				reason: fmt.Sprintf("failed to create startNfsResourceJob job [%v/%v]", resourceExport.Namespace, resourceExport.Name),
			}
			return false, c.updateStatus(resourceExport, updateData)
		}
		updateData := updateResourceExportFields{
			stage:  kdmpapi.ResourceExportStageInProgress,
			status: kdmpapi.ResourceExportStatusInProgress,
			id:     id,
			reason: "",
		}
		return false, c.updateStatus(resourceExport, updateData)
	case kdmpapi.ResourceExportStageInProgress:

		// Read the job status and move the reconciler to next state
		progress, err := driver.JobStatus(resourceExport.Status.TransferID)
		logrus.Tracef("%s job progress: %v", funct, progress)
		if err != nil {
			errMsg := fmt.Sprintf("failed to get %s job status: %s", resourceExport.Status.TransferID, err)
			updateData := updateResourceExportFields{
				status: kdmpapi.ResourceExportStatusFailed,
				reason: errMsg,
			}
			return false, c.updateStatus(resourceExport, updateData)
		}
		if progress.Status == batchv1.JobFailed {
			updateData := updateResourceExportFields{
				stage:  kdmpapi.ResourceExportStageFinal,
				status: kdmpapi.ResourceExportStatusFailed,
				reason: fmt.Sprintf("failed to create ResourceBackup CR [%v/%v]", resourceExport.Namespace, resourceExport.Name),
			}
			if len(progress.Reason) == 0 {
				// As we couldn't get actual reason from executor
				// marking it as internal error
				updateData.reason = "internal error from executor"
				return true, c.updateStatus(resourceExport, updateData)
			}
			return true, c.updateStatus(resourceExport, updateData)
		} else if progress.Status == batchv1.JobConditionType("") {
			updateData := updateResourceExportFields{
				stage:  kdmpapi.ResourceExportStageInProgress,
				status: kdmpapi.ResourceExportStatusInProgress,
				reason: "RestoreExport job in progress",
			}
			return true, c.updateStatus(resourceExport, updateData)
		}

		var rb *kdmpapi.ResourceBackup
		// Get the resourcebackup
		rb, err = kdmp.Instance().GetResourceBackup(resourceExport.Name, resourceExport.Namespace)
		if err != nil {
			errMsg := fmt.Sprintf("failed to get resourcebackup CR [%s/%s]: %s", resourceExport.Namespace, resourceExport.Name, err)
			updateData := updateResourceExportFields{
				status: kdmpapi.ResourceExportStatusFailed,
				reason: errMsg,
			}
			return false, c.updateStatus(resourceExport, updateData)
		}

		switch progress.State {
		case drivers.JobStateFailed:
			errMsg := fmt.Sprintf("%s transfer job failed: %s", resourceExport.Status.TransferID, progress.Reason)
			// If a job has failed it means it has tried all possible retires and given up.
			// In such a scenario we need to fail DE CR and move to clean up stage
			updateData := updateResourceExportFields{
				stage:     kdmpapi.ResourceExportStageFinal,
				status:    kdmpapi.ResourceExportStatusFailed,
				reason:    errMsg,
				resources: rb.Status.Resources,
			}
			return true, c.updateStatus(resourceExport, updateData)
		case drivers.JobStateCompleted:
			// Go for clean up with success state
			updateData := updateResourceExportFields{
				stage:               kdmpapi.ResourceExportStageFinal,
				status:              kdmpapi.ResourceExportStatusSuccessful,
				reason:              "Job successful",
				resources:           rb.Status.Resources,
				RestoreCompleteList: rb.RestoreCompleteList,
			}

			return true, c.updateStatus(resourceExport, updateData)
		}
	case kdmpapi.ResourceExportStageFinal:
		// Do nothing
	}

	return true, nil
}

func (c *Controller) cleanupResources(resourceExport *kdmpapi.ResourceExport) error {
	// clean up resources
	rbNamespace, rbName, err := utils.ParseJobID(resourceExport.Status.TransferID)
	if err != nil {
		errMsg := fmt.Sprintf("failed to parse job ID %v from ResourceeExport CR: %v: %v",
			resourceExport.Status.TransferID, resourceExport.Name, err)
		logrus.Errorf("%v", errMsg)
		return err
	}
	err = kdmp.Instance().DeleteResourceBackup(rbName, rbNamespace)
	if err != nil && !k8sErrors.IsNotFound(err) {
		errMsg := fmt.Sprintf("failed to delete ResourceBackup CR[%v/%v]: %v", rbNamespace, rbName, err)
		logrus.Errorf("%v", errMsg)
		return err
	}
	if err = batch.Instance().DeleteJob(resourceExport.Name, resourceExport.Namespace); err != nil && !k8sErrors.IsNotFound(err) {
		return err
	}
	pvcName := utils.GetPvcNameForJob(rbName)
	if err := core.Instance().DeletePersistentVolumeClaim(pvcName, rbNamespace); err != nil && !k8sErrors.IsNotFound(err) {
		return fmt.Errorf("delete %s/%s pvc: %s", rbNamespace, pvcName, err)
	}
	pvName := utils.GetPvNameForJob(rbName)
	if err := core.Instance().DeletePersistentVolume(pvName); err != nil && !k8sErrors.IsNotFound(err) {
		return fmt.Errorf("delete %s pv: %s", pvName, err)
	}
	if err := utils.CleanServiceAccount(rbName, rbNamespace); err != nil {
		errMsg := fmt.Sprintf("deletion of service account %s/%s failed: %v", rbNamespace, rbName, err)
		logrus.Errorf("%s: %v", "cleanupResources", errMsg)
		return fmt.Errorf(errMsg)
	}
	if err := core.Instance().DeleteSecret(utils.GetCredSecretName(rbName), rbNamespace); err != nil && !k8sErrors.IsNotFound(err) {
		errMsg := fmt.Sprintf("deletion of backup credential secret %s failed: %v", rbName, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	return nil
}

func (c *Controller) updateStatus(re *kdmpapi.ResourceExport, data updateResourceExportFields) error {
	var updErr error
	t := func() (interface{}, bool, error) {
		logrus.Infof("updateStatus data: %+v", data)
		namespacedName := types.NamespacedName{}
		namespacedName.Name = re.Name
		namespacedName.Namespace = re.Namespace
		err := c.client.Get(context.TODO(), namespacedName, re)
		if err != nil {
			errMsg := fmt.Sprintf("failed in getting RE CR %v/%v: %v", re.Namespace, re.Name, err)
			logrus.Infof("%v", errMsg)
			return "", true, fmt.Errorf("%v", errMsg)
		}

		if data.status != "" {
			re.Status.Status = data.status
			re.Status.Reason = data.reason
		}

		if data.id != "" {
			re.Status.TransferID = data.id
		}

		if data.stage != "" {
			re.Status.Stage = data.stage
		}

		if len(data.resources) != 0 {
			re.Status.Resources = data.resources
		}
		if len(data.RestoreCompleteList) != 0 {
			re.RestoreCompleteList = data.RestoreCompleteList
		}

		updErr = c.client.Update(context.TODO(), re)
		if updErr != nil {
			errMsg := fmt.Sprintf("failed updating resourceExport CR %s: %v", re.Name, updErr)
			logrus.Errorf("%v", errMsg)
			return "", true, fmt.Errorf("%v", errMsg)
		}
		return "", false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, kdmpcontroller.TaskDefaultTimeout, kdmpcontroller.TaskProgressCheckInterval); err != nil {
		errMsg := fmt.Sprintf("max retries done, failed updating resourceExport CR %s: %v", re.Name, updErr)
		logrus.Errorf("%v", errMsg)
		// Exhausted all retries, fail the CR
		return fmt.Errorf("%v", errMsg)
	}

	return nil

}

func getDriverType(re *kdmpapi.ResourceExport) (string, error) {
	src := re.Spec.Source
	doBackup := false
	doRestore := false

	if isApplicationBackupRef(src) {
		doBackup = true
	} else if isApplicationRestoreRef(src) {
		doRestore = true
	} else {
		return "", fmt.Errorf("invalid kind for nfs backup destination: expected BackupLocation")
	}

	switch re.Spec.Type {
	case kdmpapi.ResourceExportNFS:
		if doBackup {
			return drivers.NFSBackup, nil
		}
		if doRestore {
			return drivers.NFSRestore, nil
		}
		return "", fmt.Errorf("invalid kind for nfs source: expected nfs type")
	}
	return string(re.Spec.Type), nil
}

func isApplicationBackupRef(ref kdmpapi.ResourceExportObjectReference) bool {
	return ref.Kind == "ApplicationBackup" && ref.APIVersion == "stork.libopenstorage.org/v1alpha1"
}

func isApplicationRestoreRef(ref kdmpapi.ResourceExportObjectReference) bool {
	return ref.Kind == "ApplicationRestore" && ref.APIVersion == "stork.libopenstorage.org/v1alpha1"
}

func startNfsResourceJob(
	drv drivers.Interface,
	jobConfigMap string,
	jobConfigMapNs string,
	re *kdmpapi.ResourceExport,
	bl *storkapi.BackupLocation,
) (string, error) {

	err := utils.CreateNfsSecret(utils.GetCredSecretName(re.Name), bl, re.Namespace, nil)
	if err != nil {
		logrus.Errorf("failed to create NFS cred secret: %v", err)
		return "", fmt.Errorf("failed to create NFS cred secret: %v", err)
	}
	switch drv.Name() {
	case drivers.NFSBackup:
		return drv.StartJob(
			// TODO: below two calls need to be generalized and changed in all the startJob Calls
			// For NFS it need to be populated in ResourceExport CR and passed to Job via its reconciler.
			drivers.WithNfsImageExecutorSource(re.Spec.TriggeredFrom),
			drivers.WithNfsImageExecutorSourceNs(re.Spec.TriggeredFromNs),
			drivers.WithRestoreExport(re.Name),
			drivers.WithJobNamespace(re.Namespace),
			drivers.WithNfsServer(bl.Location.NFSConfig.ServerAddr),
			drivers.WithNfsExportDir(bl.Location.Path),
			drivers.WithAppCRName(re.Spec.Source.Name),
			drivers.WithAppCRNamespace(re.Spec.Source.Namespace),
			drivers.WithNamespace(re.Namespace),
			drivers.WithResoureBackupName(re.Name),
			drivers.WithResoureBackupNamespace(re.Namespace),
			drivers.WithNfsMountOption(bl.Location.NFSConfig.MountOptions),
			drivers.WithNfsSubPath(bl.Location.NFSConfig.SubPath),
		)
	case drivers.NFSRestore:
		return drv.StartJob(
			drivers.WithNfsImageExecutorSource(re.Spec.TriggeredFrom),
			drivers.WithNfsImageExecutorSourceNs(re.Spec.TriggeredFromNs),
			drivers.WithRestoreExport(re.Name),
			drivers.WithJobNamespace(re.Namespace),
			drivers.WithNfsServer(bl.Location.NFSConfig.ServerAddr),
			drivers.WithNfsExportDir(bl.Location.Path),
			drivers.WithAppCRName(re.Spec.Source.Name),
			drivers.WithAppCRNamespace(re.Spec.Source.Namespace),
			drivers.WithNamespace(re.Namespace),
			drivers.WithResoureBackupName(re.Name),
			drivers.WithResoureBackupNamespace(re.Namespace),
			drivers.WithNfsMountOption(bl.Location.NFSConfig.MountOptions),
			drivers.WithNfsSubPath(bl.Location.NFSConfig.SubPath),
		)
	}
	return "", fmt.Errorf("unknown data transfer driver: %s", drv.Name())
}

func createResourceBackup(name, namespace string) error {
	funct := "createResourceBackup"

	rbCR := &kdmpapi.ResourceBackup{
		TypeMeta: metav1.TypeMeta{
			Kind:       reflect.TypeOf(kdmpapi.ResourceBackup{}).Name(),
			APIVersion: "kdmp.portworx.com/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				utils.SkipResourceAnnotation: "true",
			},
		},
		// TODO: As part of restore resources, prefill resources info
		// so that job can update the same
		Spec: kdmpapi.ResourceBackupSpec{},
	}

	_, err := kdmp.Instance().CreateResourceBackup(rbCR)
	if err != nil {
		logrus.Errorf("%s: %v", funct, err)
		return err
	}

	return nil
}
