//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/libopenstorage/stork/pkg/log"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/errors"
	"github.com/portworx/sched-ops/k8s/kdmp"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/scheduler"
	apps_api "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	dataExportFailureWaitTimeout = 20 * time.Second
	dataExportSuccessWaitTimeout = 5 * time.Minute
)

func TestDataExportRsync(t *testing.T) {
	var testResult = testResultFail
	instanceID := "dataexport-test"
	appKey := "fio-dataexport"
	currentTestSuite = t.Name()
	defer updateDashStats(t.Name(), &testResult)

	// Check if default storage class is set. This allows us to run this
	// test on all platforms such as AWS, AKS where a default storage class
	// would be set and we can then import data from those PVCs into Portworx.
	scList, err := storage.Instance().GetDefaultStorageClasses()
	log.FailOnError(t, err, "failed to get list of storage classes")
	if len(scList.Items) == 0 {
		// No default storage classes set. Lets set one
		_, err := storage.Instance().CreateStorageClass(&storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "px-csi-default-class-import",
				Annotations: map[string]string{
					"storageclass.kubernetes.io/is-default-class": "true",
				},
			},
			Provisioner: "pxd.portworx.com",
			Parameters: map[string]string{
				"repl": "2",
			},
		})
		log.FailOnError(t, err, "failed to create default storage class")
	}

	ctx, err := schedulerDriver.Schedule(
		instanceID,
		scheduler.ScheduleOptions{
			AppKeys: []string{appKey},
			Labels:  nil,
		},
	)
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctx), 1, "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctx[0], defaultWaitTimeout/2, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for pod to get to running.")

	var (
		namespace, sourcePVC, sourcePV, destPVC, destPV string
		destPVCObj                                      *v1.PersistentVolumeClaim
	)
	namespace = ctx[0].App.NameSpace
	for _, spec := range ctx[0].App.SpecList {
		if obj, ok := spec.(*apps_api.StatefulSet); ok {
			pvcs, err := apps.Instance().GetPVCsForStatefulSet(obj)
			log.FailOnError(t, err, "error getting pvcs for ss")
			Dash.VerifyFatal(t, len(pvcs.Items), 1, "Expected only one pvc")
			sourcePVC = pvcs.Items[0].Name
			sourcePV, err = core.Instance().GetVolumeForPersistentVolumeClaim(&pvcs.Items[0])
			log.FailOnError(t, err, "Failed to get PV from source PVC")
		} else if pvc, ok := spec.(*v1.PersistentVolumeClaim); ok {
			destPVC = pvc.Name
			destPVCObj = pvc
			var err error

			for i := 0; i < 10; i++ {
				destPV, err = core.Instance().GetVolumeForPersistentVolumeClaim(pvc)
				log.FailOnError(t, err, "failed to get dest PV")

				if destPV != "" {
					break
				}
				time.Sleep(6 * time.Second)
			}
			if destPV == "" {
				Dash.Fatal("failed to get destination volume, expected a volume id got blank", destPV)
			}
		}
	}
	Dash.VerifyFatal(t, namespace != "", true, "Find namespace")
	Dash.VerifyFatal(t, sourcePVC != "", true, "Find PVC")
	Dash.VerifyFatal(t, sourcePV != "", true, "Find PV")

	createDataExport := func(deName, sourcePVC, destPVC, namespace string) {
		_, err = kdmp.Instance().CreateDataExport(&kdmpapi.DataExport{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deName,
				Namespace: namespace,
			},
			Spec: kdmpapi.DataExportSpec{
				Type: kdmpapi.DataExportRsync,
				Source: kdmpapi.DataExportObjectReference{
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Namespace:  namespace,
					Name:       sourcePVC,
				},
				Destination: kdmpapi.DataExportObjectReference{
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Namespace:  namespace,
					Name:       destPVC,
				},
			},
		})
		log.FailOnError(t, err, "failed to create a data export CR")
	}

	// Test Case 1: DataExport should fail if the dest PVC is non existent
	deName := "case1-non-existent-pvc"
	createDataExport(deName, sourcePVC, "non-existent-pvc", namespace)

	err = validateAndCleanupDataExport(deName, namespace, kdmpapi.DataExportStageInitial, kdmpapi.DataExportStatusFailed, dataExportFailureWaitTimeout, 10*time.Second)
	log.FailOnError(t, err, "expected validation to succeed")

	// Test Case 2: DataExport should fail if the source PVC is in use
	deName = "case2-source-pvc-in-use"
	createDataExport(deName, sourcePVC, destPVC, namespace)

	err = validateAndCleanupDataExport(deName, namespace, kdmpapi.DataExportStageInitial, kdmpapi.DataExportStatusFailed, dataExportFailureWaitTimeout, 10*time.Second)
	log.FailOnError(t, err, "expected validation to succeed")

	// Test Case 3: DataExport should fail if destination PVC has a smaller size that the source PVC
	deName = "case2-dest-pvc-not-same-size"
	createDataExport(deName, sourcePVC, destPVC, namespace)

	err = validateAndCleanupDataExport(deName, namespace, kdmpapi.DataExportStageInitial, kdmpapi.DataExportStatusFailed, dataExportFailureWaitTimeout, 10*time.Second)
	log.FailOnError(t, err, "expected validation to succeed")

	// Test Case 4: DataExport should succeed
	// Resize the destination PVC to the source PVC size.

	destPVCObj, err = core.Instance().GetPersistentVolumeClaim(destPVC, namespace)
	log.FailOnError(t, err, "failed to get latest destination pvc instance")

	expectedSize := resource.MustParse("50Gi")
	destPVCObj.Spec.Resources.Requests = map[v1.ResourceName]resource.Quantity{
		v1.ResourceStorage: expectedSize,
	}

	_, err = core.Instance().UpdatePersistentVolumeClaim(destPVCObj)
	log.FailOnError(t, err, "failed to issue resize of pvc")

	err = core.Instance().ValidatePersistentVolumeClaimSize(destPVCObj, expectedSize.Value(), dataExportSuccessWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "failed to resize pvc")

	// Scale down the application
	scaleFactor, err := schedulerDriver.GetScaleFactorMap(ctx[0])
	log.FailOnError(t, err, "unexpected error on GetScaleFactorMap")

	for k := range scaleFactor {
		scaleFactor[k] = 0
	}
	err = schedulerDriver.ScaleApplication(ctx[0], scaleFactor)
	log.FailOnError(t, err, "unexpected error on ScaleApplication")

	// Get the size of the source and destination volumes before triggering the DataExport
	// We should see a bump in the destination PVC size
	preDestVol, err := volumeDriver.InspectVolume(destPV)
	log.FailOnError(t, err, "failed to inspect destination volume")
	srcVol, err := volumeDriver.InspectVolume(sourcePV)
	log.FailOnError(t, err, "failed to inspect source volume")

	deName = "case4-successful-export"
	createDataExport(deName, sourcePVC, destPVC, namespace)

	err = validateAndCleanupDataExport(deName, namespace, kdmpapi.DataExportStageFinal, kdmpapi.DataExportStatusSuccessful, dataExportSuccessWaitTimeout, 10*time.Second)
	log.FailOnError(t, err, "expected validation to succeed")

	compareVolSizes := func() (interface{}, bool, error) {
		postDestVol, err := volumeDriver.InspectVolume(destPV)
		log.FailOnError(t, err, "failed to inspect destination volume post DataExport")
		if postDestVol.Usage < preDestVol.Usage {
			return nil, true, fmt.Errorf("destination volume usage is not more than what it was before data export job")
		}
		log.InfoD("size comparision: %v %v", srcVol.Usage, postDestVol.Usage)
		return nil, false, nil
	}

	_, err = task.DoRetryWithTimeout(compareVolSizes, dataExportSuccessWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "size comparison failed after DataExport rsync job completion")
}

func validateAndCleanupDataExport(
	name string,
	namespace string,
	stage kdmpapi.DataExportStage,
	status kdmpapi.DataExportStatus,
	timeout, retryInterval time.Duration,
) error {
	t := func() (interface{}, bool, error) {
		dataExport, err := kdmp.Instance().GetDataExport(name, namespace)
		if err != nil {
			return "", true, err
		}

		if dataExport.Status.Stage == stage &&
			dataExport.Status.Status == status {
			return "", false, nil
		}

		log.InfoD("DataExport Status: %v", dataExport.Status)
		return "", true, &errors.ErrFailedToValidateCustomSpec{
			Name:  name,
			Cause: fmt.Sprintf("Stage: %v \t Status: %v", dataExport.Status.Stage, dataExport.Status.Status),
			Type:  dataExport,
		}
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryInterval); err != nil {
		return err
	}
	if err := kdmp.Instance().DeleteDataExport(name, namespace); err != nil {
		log.Warn("Failed to cleanup data export after successful validation: %v/%v", name, namespace)
	}
	return nil
}
