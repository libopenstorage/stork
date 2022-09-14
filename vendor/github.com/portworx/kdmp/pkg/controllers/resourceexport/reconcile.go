package resourceexport

import (
	"context"
	"fmt"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	kdmpcontroller "github.com/portworx/kdmp/pkg/controllers"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/driversinstance"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// updateResourceExportFields when and update needs to be done to ResourceExport
// user can choose which filed to be updated and pass the same to updateStatus()
type updateResourceExportFields struct {
	status kdmpapi.ResourceExportStatus
	reason string
	// TODO: Enable for restore
	//resources []*kdmpapi.ResourceInfo
}

func (c *Controller) process(ctx context.Context, in *kdmpapi.ResourceExport) (bool, error) {
	logrus.Infof("entering reconciler process")
	if in == nil {
		return false, nil
	}
	resourceExport := in.DeepCopy()

	// Set to initial status to start with
	if resourceExport.Status.Status == "" {
		updateData := updateResourceExportFields{
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
	var serr error
	switch resourceExport.Status.Status {
	case kdmpapi.ResourceExportStatusInitial:
		// start data transfer
		_, serr = startNfsResourceJob(
			driver,
			utils.KdmpConfigmapName,
			utils.KdmpConfigmapNamespace,
			resourceExport,
			backupLocation,
		)
		if serr != nil {
			logrus.Errorf("err: %v", serr)
		}
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

	switch drv.Name() {
	case drivers.NFSBackup:
		return drv.StartJob(
			drivers.WithRestoreExport(re.Name),
			drivers.WithJobNamespace(re.Namespace),
			drivers.WithNfsServer(bl.Location.NfsConfig.NfsServerAddr),
			drivers.WithNfsExportDir(bl.Location.Path),
			drivers.WithAppCRName(re.Spec.Source.Name),
			drivers.WithAppCRNamespace(re.Spec.Source.Namespace),
			drivers.WithNamespace(re.Namespace),
		)
	}
	return "", fmt.Errorf("unknown data transfer driver: %s", drv.Name())
}
