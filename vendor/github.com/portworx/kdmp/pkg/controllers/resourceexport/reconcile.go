package resourceexport

import (
	"context"
	"fmt"
	"reflect"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	kdmpcontroller "github.com/portworx/kdmp/pkg/controllers"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/driversinstance"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/sched-ops/k8s/kdmp"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// updateResourceExportFields when and update needs to be done to ResourceExport
// user can choose which filed to be updated and pass the same to updateStatus()
type updateResourceExportFields struct {
	stage  kdmpapi.ResourceExportStage
	status kdmpapi.ResourceExportStatus
	reason string
	id     string
	// TODO: Enable for restore
	//resources []*kdmpapi.ResourceInfo
}

func (c *Controller) process(ctx context.Context, in *kdmpapi.ResourceExport) (bool, error) {
	funct := "re reconciler"
	logrus.Infof("entering reconciler process")
	if in == nil {
		return false, nil
	}
	resourceExport := in.DeepCopy()

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

	//var serr error
	switch resourceExport.Status.Stage {
	case kdmpapi.ResourceExportStageInitial:
		// Create ResourceBackup CR
		logrus.Infof("%s line 87", funct)
		err = createResourceBackup(resourceExport.Name, resourceExport.Namespace)
		if err != nil {
			updateData := updateResourceExportFields{
				stage:  kdmpapi.ResourceExportStageFailed,
				status: kdmpapi.ResourceExportStatusFailed,
				reason: fmt.Sprintf("failed to create ResourceBackup CR [%v/%v]", resourceExport.Namespace, resourceExport.Name),
			}
			logrus.Infof("%s line 96", funct)
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
		logrus.Infof("line 106 id: %v", id)
		if serr != nil {
			logrus.Errorf("err: %v", serr)
			updateData := updateResourceExportFields{
				stage:  kdmpapi.ResourceExportStageFailed,
				status: kdmpapi.ResourceExportStatusFailed,
				reason: fmt.Sprintf("failed to create startNfsResourceJob job [%v/%v]", resourceExport.Namespace, resourceExport.Name),
			}
			logrus.Infof("%s line 114", funct)
			return false, c.updateStatus(resourceExport, updateData)
		}
		logrus.Infof("line 103 transfer id: %v", id)
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
		logrus.Infof("%s line 118 progress: %+v", funct, progress)
		if err != nil {
			errMsg := fmt.Sprintf("failed to get %s job status: %s", resourceExport.Status.TransferID, err)
			updateData := updateResourceExportFields{
				status: kdmpapi.ResourceExportStatusFailed,
				reason: errMsg,
			}
			logrus.Infof("%s line 135", funct)
			return false, c.updateStatus(resourceExport, updateData)
		}
		if progress.Status == batchv1.JobFailed {
			updateData := updateResourceExportFields{
				stage:  kdmpapi.ResourceExportStageFailed,
				status: kdmpapi.ResourceExportStatusFailed,
				reason: fmt.Sprintf("failed to create ResourceBackup CR [%v/%v]", resourceExport.Namespace, resourceExport.Name),
			}
			if len(progress.Reason) == 0 {
				logrus.Infof("%s line 145", funct)
				// As we couldn't get actual reason from executor
				// marking it as internal error
				updateData.reason = "internal error from executor"
				return true, c.updateStatus(resourceExport, updateData)
			}
			return true, c.updateStatus(resourceExport, updateData)
		} else if progress.Status == batchv1.JobConditionType("") {
			// TODO: We can avoid these update but this event will eventually catch up after
			// resync time
			updateData := updateResourceExportFields{
				stage:  kdmpapi.ResourceExportStageInProgress,
				status: kdmpapi.ResourceExportStatusInProgress,
				reason: "RestoreExport job in progress",
			}
			logrus.Infof("%s line 160", funct)
			return true, c.updateStatus(resourceExport, updateData)
		}

		switch progress.State {
		case drivers.JobStateFailed:
			logrus.Infof("%s line 155", funct)
			errMsg := fmt.Sprintf("%s transfer job failed: %s", resourceExport.Status.TransferID, progress.Reason)
			// If a job has failed it means it has tried all possible retires and given up.
			// In such a scenario we need to fail DE CR and move to clean up stage
			updateData := updateResourceExportFields{
				stage:  kdmpapi.ResourceExportStageFailed,
				status: kdmpapi.ResourceExportStatusFailed,
				reason: errMsg,
			}
			return true, c.updateStatus(resourceExport, updateData)
		case drivers.JobStateCompleted:
			logrus.Infof("%s line 166", funct)
			// Go for clean up with success state
			updateData := updateResourceExportFields{
				stage:  kdmpapi.ResourceExportStageSuccessful,
				status: kdmpapi.ResourceExportStatusSuccessful,
				reason: "Job successful",
			}
			logrus.Infof("%s line 184", funct)
			return true, c.updateStatus(resourceExport, updateData)
		}
	case kdmpapi.ResourceExportStageSuccessful:
		logrus.Infof("%s line 176", funct)
		// clean up resources
		// Delete ResourceBackup CR
		rbNamespace, rbName, err := utils.ParseJobID(resourceExport.Status.TransferID)
		if err != nil {
			errMsg := fmt.Sprintf("failed to parse job ID %v from ResourceeExport CR: %v: %v",
				resourceExport.Status.TransferID, resourceExport.Name, err)
			updateData := updateResourceExportFields{
				stage:  kdmpapi.ResourceExportStageFailed,
				status: kdmpapi.ResourceExportStatusFailed,
				reason: errMsg,
			}
			logrus.Infof("%s line 200", funct)
			return false, c.updateStatus(resourceExport, updateData)
		}
		err = kdmp.Instance().DeleteResourceBackup(rbName, rbNamespace)
		if err != nil {
			logrus.Infof("%s line 192", funct)
			errMsg := fmt.Sprintf("failed to delete ResourceExport CR[%v/%v]: %v", rbNamespace, rbName, err)
			updateData := updateResourceExportFields{
				stage:  kdmpapi.ResourceExportStageFailed,
				status: kdmpapi.ResourceExportStatusFailed,
				reason: errMsg,
			}
			logrus.Infof("%s line 212", funct)
			return false, c.updateStatus(resourceExport, updateData)
		}
		updateData := updateResourceExportFields{
			stage:  kdmpapi.ResourceExportStageSuccessful,
			status: kdmpapi.ResourceExportStatusSuccessful,
			reason: "Successful",
		}
		// TODO: Deletion of SA to be added
		logrus.Infof("%s line 221", funct)
		return false, c.updateStatus(resourceExport, updateData)
	case kdmpapi.ResourceExportStageFailed:
	case kdmpapi.ResourceExportStageFinal:
		logrus.Infof("%s line 225", funct)
		// TODO: We need to discuss if we need both final and failed state as we don't
		// want to do anything in these states
		// Do nothing, keep all resources for debugging
		//
	}

	return true, nil
}

func (c *Controller) updateStatus(re *kdmpapi.ResourceExport, data updateResourceExportFields) error {
	var updErr error
	t := func() (interface{}, bool, error) {
		namespacedName := types.NamespacedName{}
		namespacedName.Name = re.Name
		namespacedName.Namespace = re.Namespace
		err := c.client.Get(context.TODO(), namespacedName, re)
		if err != nil && !k8sErrors.IsNotFound(err) {
			errMsg := fmt.Sprintf("failed in getting DE CR %v/%v: %v", re.Namespace, re.Name, err)
			logrus.Infof("%v", errMsg)
			return "", true, fmt.Errorf("%v", errMsg)
		}
		// TODO: In the restore path iterate over ResourceInfo{} list and only update the
		// resource whose status as changed
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

	if isApplicationBackupRef(src) {
		doBackup = true
	} else {
		return "", fmt.Errorf("invalid kind for nfs backup destination: expected BackupLocation")
	}

	switch re.Spec.Type {
	case kdmpapi.ResourceExportNFS:
		if doBackup {
			return drivers.NFSBackup, nil
		}
		return "", fmt.Errorf("invalid kind for nfs source: expected nfs type")
	}
	return string(re.Spec.Type), nil
}

func isApplicationBackupRef(ref kdmpapi.ResourceExportObjectReference) bool {
	return ref.Kind == "ApplicationBackup" && ref.APIVersion == "stork.libopenstorage.org/v1alpha1"
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
			// TODO: below two calls need to be generalized and chnaged in all the startJob Calls
			// For NFS it need to be populated in ResourceExport CR and passed to Job via its reconciler.
			drivers.WithKopiaImageExecutorSource("stork"),
			drivers.WithKopiaImageExecutorSourceNs("kube-system"),
			drivers.WithRestoreExport(re.Name),
			drivers.WithJobNamespace(re.Namespace),
			drivers.WithNfsServer(bl.Location.NfsConfig.NfsServerAddr),
			drivers.WithNfsExportDir(bl.Location.Path),
			drivers.WithAppCRName(re.Spec.Source.Name),
			drivers.WithAppCRNamespace(re.Spec.Source.Namespace),
			drivers.WithNamespace(re.Namespace),
			drivers.WithResoureBackupName(re.Name),
			drivers.WithResoureBackupNamespace(re.Namespace),
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
