package snapshotter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"

	kSnapshotv1beta1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1beta1"
	kSnapshotClient "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/crypto"
	"github.com/libopenstorage/stork/pkg/objectstore"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	k8shelper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

const (
	csiProviderName = "csi"
	// snapshotClassNamePrefix is the prefix for snapshot classes per CSI driver
	snapshotClassNamePrefix = "stork-csi-snapshot-class-"
	annPVBindCompleted      = "pv.kubernetes.io/bind-completed"
	annPVBoundByController  = "pv.kubernetes.io/bound-by-controller"
	skipResourceAnnotation  = "stork.libopenstorage.org/skip-resource"

	// snapshotTimeout represents the duration to wait before timing out on snapshot completion
	snapshotTimeout = time.Minute * 5
	// restoreTimeout is the duration to wait before timing out the restore
	restoreTimeout = time.Minute * 5
	// localCSIRetention is the max number of csi snapshots those can be kept locally
	localCSIRetention = 1
	// snapDeleteAnnotation needs to be set if volume snapshot is scheduled for deletion
	snapDeleteAnnotation = "snapshotScheduledForDeletion"
	// snapRestoreAnnotation needs to be set if volume snapshot is scheduled for restore
	snapRestoreAnnotation = "snapshotScheduledForRestore"
	// kdmp config map
	kdmpConfig = "kdmp-config"
	// pvcNameLenLimitForJob is the max length of PVC name that the bound job
	// will incorporate in their names
	pvcNameLenLimitForJob = 237
	// shortRetryTimeout gets used for retry timeout
	shortRetryTimeout = 30 * time.Second
	// shortRetryTimeout gets used for retry timeout interval
	shortRetryTimeoutInterval = 2 * time.Second
)

type csiBackupObject struct {
	VolumeSnapshots        map[string]*kSnapshotv1beta1.VolumeSnapshot        `json:"volumeSnapshots"`
	VolumeSnapshotContents map[string]*kSnapshotv1beta1.VolumeSnapshotContent `json:"volumeSnapshotContents"`
	VolumeSnapshotClasses  map[string]*kSnapshotv1beta1.VolumeSnapshotClass   `json:"volumeSnapshotClasses"`
}

// NewCSIDriver returns the csi implementation of Driver object
func NewCSIDriver() (Driver, error) {
	cs := &csiDriver{}
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	snapClient, err := kSnapshotClient.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	cs.snapshotClient = snapClient
	cs.snapshotClassCreatedForDriver = make(map[string]bool)
	return cs, nil
}

// csiDriver is the csi implementation of the snapshotter.Driver interface
type csiDriver struct {
	snapshotClient                *kSnapshotClient.Clientset
	snapshotClassCreatedForDriver map[string]bool
}

func (c *csiDriver) CreateSnapshot(opts ...Option) (string, string, string, error) {
	o := Options{}
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return "", "", "", err
			}
		}
	}

	// get snapshotclass name based on pv provisioner
	pvc, err := core.Instance().GetPersistentVolumeClaim(o.PVCName, o.PVCNamespace)
	if err != nil {
		return "", "", "", fmt.Errorf("error getting PVC object for (%v/%v): %v", o.PVCName, o.PVCNamespace, err)
	}

	pvName, err := core.Instance().GetVolumeForPersistentVolumeClaim(pvc)
	if err != nil {
		return "", "", "", fmt.Errorf("error getting PV name for PVC (%v/%v): %v", pvc.Namespace, pvc.Name, err)
	}

	pv, err := core.Instance().GetPersistentVolume(pvName)
	if err != nil {
		return "", "", "", fmt.Errorf("error getting pv %v: %v", pvName, err)
	}

	// In case the PV does not contain CSI secion itself, we will error out.
	if pv.Spec.CSI == nil {
		return "", "", "", fmt.Errorf("pv [%v] does not contain CSI section", pv.Name)
	}

	if o.SnapshotClassName == "" {
		return "", "", "", fmt.Errorf("snapshot class cannot be empty, use 'default' to choose the default snapshot class")
	}

	if o.SnapshotClassName == "default" || o.SnapshotClassName == "Default" {
		// Let kubernetes choose the default snapshot class to use
		// for this snapshot. If none is set then the volume snapshot will fail
		o.SnapshotClassName = ""
	} else {
		// For other snapshot class names ensure the volume snapshot class has
		// been created
		if err := c.ensureVolumeSnapshotClassCreated(pv.Spec.CSI.Driver, o.SnapshotClassName); err != nil {
			return "", "", "", err
		}
	}
	snapClassPtr := stringPtr(o.SnapshotClassName)
	if o.SnapshotClassName == "" {
		snapClassPtr = nil
	}

	vs := &kSnapshotv1beta1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:        o.Name,
			Namespace:   o.PVCNamespace,
			Annotations: o.Annotations,
			Labels:      o.Labels,
		},
		Spec: kSnapshotv1beta1.VolumeSnapshotSpec{
			VolumeSnapshotClassName: snapClassPtr,
			Source: kSnapshotv1beta1.VolumeSnapshotSource{
				PersistentVolumeClaimName: stringPtr(o.PVCName),
			},
		},
	}
	if _, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(o.PVCNamespace).Create(
		context.TODO(),
		vs,
		metav1.CreateOptions{},
	); err != nil {
		if k8s_errors.IsAlreadyExists(err) {
			return o.Name, o.PVCNamespace, pv.Spec.CSI.Driver, nil
		}
		return "", "", "", err
	}
	return o.Name, o.PVCNamespace, pv.Spec.CSI.Driver, nil
}

func (c *csiDriver) DeleteSnapshot(name, namespace string, retain bool) error {
	vs, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		if k8s_errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get volumesnapshot object %v/%v: %v", namespace, name, err)
	}

	if vs.Status == nil || vs.Status.BoundVolumeSnapshotContentName == nil {
		return fmt.Errorf("failed to find get status for snapshot: %s/%s", vs.Namespace, vs.Name)
	}

	vsc, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Get(context.TODO(), *vs.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
	if err != nil {
		if k8s_errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	// ensure all vscontent have the desired delete policy
	desiredRetainPolicy := kSnapshotv1beta1.VolumeSnapshotContentRetain
	if !retain {
		desiredRetainPolicy = kSnapshotv1beta1.VolumeSnapshotContentDelete
	}

	if vsc.Spec.DeletionPolicy != desiredRetainPolicy {
		vsc.UID = ""
		vsc.Spec.DeletionPolicy = desiredRetainPolicy
		if _, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Update(context.TODO(), vsc, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}

	err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(vs.Namespace).Delete(context.TODO(), vs.Name, metav1.DeleteOptions{})
	if err != nil && !k8s_errors.IsNotFound(err) {
		return err
	}
	err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Delete(context.TODO(), vsc.Name, metav1.DeleteOptions{})
	if err != nil && !k8s_errors.IsNotFound(err) {
		return err
	}

	logrus.Debugf("started clean up of %v/%v snapshots and %v snapshotcontents in namespace %v", vs.Namespace, vs.Name, vsc.Name, namespace)
	return nil
}

func (c *csiDriver) SnapshotStatus(name, namespace string) (SnapshotInfo, error) {
	var snapshotInfo SnapshotInfo
	snapshot, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		snapshotInfo.Status = StatusFailed
		snapshotInfo.Reason = fmt.Sprintf("snapshot %s lost during backup: %v", name, err)
		return snapshotInfo, err

	}
	snapshotInfo.SnapshotRequest = snapshot
	var snapshotClassName string
	var snapshotClass *kSnapshotv1beta1.VolumeSnapshotClass
	if snapshot.Spec.VolumeSnapshotClassName != nil {
		snapshotClassName = *snapshot.Spec.VolumeSnapshotClassName
	}
	if len(snapshotClassName) > 0 {
		snapshotClass, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Get(context.TODO(), snapshotClassName, metav1.GetOptions{})
		if err != nil {
			snapshotInfo.Status = StatusFailed
			snapshotInfo.Reason = fmt.Sprintf("snapshot class %s lost during backup: %v", snapshotClassName, err)
			return snapshotInfo, err
		}
	}

	volumeSnapshotReady := c.snapshotReady(snapshot)
	var volumeSnapshotContentReady bool
	var snapshotContent *kSnapshotv1beta1.VolumeSnapshotContent
	var contentName string
	if volumeSnapshotReady && snapshot.Status.BoundVolumeSnapshotContentName != nil {
		snapshotContentName := *snapshot.Status.BoundVolumeSnapshotContentName
		snapshotContent, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Get(context.TODO(), snapshotContentName, metav1.GetOptions{})
		if err != nil {
			snapshotInfo.Status = StatusFailed
			snapshotInfo.Reason = fmt.Sprintf("snapshot content %s lost during backup: %v", snapshotClassName, err)
			return snapshotInfo, err
		}
		snapshotInfo.Content = snapshotContent
		snapshotInfo.Class = snapshotClass
		volumeSnapshotContentReady = c.snapshotContentReady(snapshotContent)
		contentName = snapshotContent.Name
	}

	// Evaluate current status of the backup for this PVC. Get all metadata and decide if finished.
	var vsError string
	if snapshot.Status != nil && snapshot.Status.Error != nil && snapshot.Status.Error.Message != nil {
		vsError = *snapshot.Status.Error.Message
	}
	var pvcName string
	if snapshot.Spec.Source.PersistentVolumeClaimName != nil {
		pvcName = *snapshot.Spec.Source.PersistentVolumeClaimName
	}
	size := getSnapshotSize(snapshot)
	if size == 0 && len(pvcName) != 0 {
		// if restoreSize is empty, report PVC size
		pvc, err := core.Instance().GetPersistentVolumeClaim(
			pvcName,
			namespace,
		)
		if err == nil {
			size = getPVCSize(pvc)
		}
	}
	var vscError string
	if contentName != "" {
		vscError = c.getSnapshotContentError(contentName)
	}
	switch {
	case volumeSnapshotReady && volumeSnapshotContentReady:
		snapshotInfo.Status = StatusReady
		snapshotInfo.Reason = "Snapshot successful for volume"
		snapshotInfo.Size = uint64(size)

	case time.Now().After(snapshot.CreationTimestamp.Add(snapshotTimeout)):
		snapshotInfo.Status = StatusFailed
		snapshotInfo.Reason = formatReasonErrorMessage(fmt.Sprintf("snapshot timeout out after %s", snapshotTimeout.String()), vsError, vscError)

	default:
		if len(vsError) > 0 {
			snapshotInfo.Status = StatusFailed
			snapshotInfo.Reason = formatReasonErrorMessage(
				fmt.Sprintf("volume snapshot failed for PVC %s", pvcName), vsError, vscError)
		} else {
			snapshotInfo.Status = StatusInProgress
			snapshotInfo.Reason = formatReasonErrorMessage(
				fmt.Sprintf("volume snapshot in progress for PVC %s", pvcName), vsError, vscError)
		}
		snapshotInfo.Size = uint64(size)
	}
	return snapshotInfo, nil
}

func (c *csiDriver) RestoreVolumeClaim(opts ...Option) (*v1.PersistentVolumeClaim, error) {
	o := Options{}
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return nil, err
			}
		}
	}

	pvc := &o.PVC
	pvc = c.cleanK8sPVCAnnotations(pvc)
	pvc.Namespace = o.RestoreNamespace
	pvc.ResourceVersion = ""
	pvc.Spec.VolumeName = ""

	snapshot, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(o.RestoreNamespace).Get(context.TODO(), o.RestoreSnapshotName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get volumesnapshot %s/%s", o.RestoreNamespace, o.RestoreSnapshotName)
	}

	// Make the pvc size  same as the restore size from the volumesnapshot
	if snapshot.Status.RestoreSize != nil && !snapshot.Status.RestoreSize.IsZero() {
		quantity, err := resource.ParseQuantity(snapshot.Status.RestoreSize.String())
		if err != nil {
			return nil, err
		}
		logrus.Debugf("setting size of pvc %s/%s same as snapshot size %s", pvc.Namespace, pvc.Name, quantity.String())
		pvc.Spec.Resources.Requests[v1.ResourceStorage] = quantity
	}

	pvc.Spec.DataSource = &v1.TypedLocalObjectReference{
		APIGroup: stringPtr("snapshot.storage.k8s.io"),
		Kind:     "VolumeSnapshot",
		Name:     o.RestoreSnapshotName,
	}
	pvc.Status = v1.PersistentVolumeClaimStatus{
		Phase: v1.ClaimPending,
	}
	pvc, err = core.Instance().CreatePersistentVolumeClaim(pvc)
	if err != nil {
		if k8s_errors.IsAlreadyExists(err) {
			return pvc, nil
		}
		return nil, fmt.Errorf("failed to create PVC %s: %s", pvc.Name, err.Error())
	}
	return pvc, nil
}

func (c *csiDriver) RestoreStatus(pvcName, namespace string) (RestoreInfo, error) {
	var restoreInfo RestoreInfo
	// Check on PVC status
	pvc, err := core.Instance().GetPersistentVolumeClaim(pvcName, namespace)
	if err != nil {
		return restoreInfo, err
	}

	// Try to get VS. May not exist yet or may be cleaned up already.
	vsName := pvc.Spec.DataSource.Name
	var vsContentName string
	var restoreSize uint64
	var vsError string
	if vs, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(namespace).Get(context.TODO(), vsName, metav1.GetOptions{}); err == nil && vs != nil {
		// Leave vs as inline to avoid accessing volumesnapshot when it could be nil
		restoreSize = getSnapshotSize(vs)
		if vs.Status != nil && vs.Status.Error != nil && vs.Status.Error.Message != nil {
			vsError = *vs.Status.Error.Message
		}
		if vs.Status != nil && vs.Status.BoundVolumeSnapshotContentName != nil {
			vsContentName = *vs.Status.BoundVolumeSnapshotContentName
		}
	} else {
		logrus.Warnf("did not find volume snapshot %s: %v", vsName, err)
	}

	// Try to get VSContent error message
	var vscError string
	if vsContentName != "" {
		vscError = c.getSnapshotContentError(vsContentName)
	}

	// Use PVC size by default, but replace with restoreSize once it is ready
	size := getPVCSize(pvc)
	if restoreSize != 0 {
		size = restoreSize
	}

	switch {
	case c.pvcBindFinished(pvc):
		err := c.deletejob(pvc, namespace)
		if err != nil {
			restoreInfo.Reason = fmt.Sprintf("Delete job for volume bind for PVC %s failed: %v", pvc.Name, err)
		}
		restoreInfo.VolumeName = pvc.Spec.VolumeName
		restoreInfo.Size = size
		restoreInfo.Status = StatusReady
	case pvc.Status.Phase == v1.ClaimLost:
		restoreInfo.Status = StatusFailed
		restoreInfo.Reason = fmt.Sprintf("Volume restore failed: PVC %s is lost", pvc.Name)
	case pvc.Status.Phase == v1.ClaimPending:
		restoreInfo.Size = size
		restoreInfo.Status = StatusInProgress
		restoreInfo.Reason = formatReasonErrorMessage(fmt.Sprintf("Volume restore in progress: PVC %s is pending", pvc.Name), vsError, vscError)
		if c.pvcWaitingForFirstConsumer(pvc) {
			_, err := c.createJob(pvc, namespace)
			if err != nil {
				restoreInfo.Reason = fmt.Sprintf("Create job for volume bind for PVC %s failed: %v", pvc.Name, err)
			}
		}
	}

	if time.Now().After(pvc.CreationTimestamp.Add(restoreTimeout)) {
		restoreInfo.Status = StatusFailed
		restoreInfo.Reason = formatReasonErrorMessage(fmt.Sprintf("PVC restore timeout out after %s", restoreTimeout.String()), vsError, vscError)
	}
	return restoreInfo, nil
}

func (c *csiDriver) CancelRestore(pvcName, namespace string) error {
	return core.Instance().DeletePersistentVolumeClaim(pvcName, namespace)
}

func (c *csiDriver) cleanK8sPVCAnnotations(pvc *v1.PersistentVolumeClaim) *v1.PersistentVolumeClaim {
	if pvc.Annotations != nil {
		newAnnotations := make(map[string]string)

		// we will remove the following annotations to prevent controller confusion:
		// - pv.kubernetes.io/bind-completed
		// - pv.kubernetes.io/bound-by-controller
		for key, val := range pvc.Annotations {
			if key != annPVBindCompleted && key != annPVBoundByController {
				newAnnotations[key] = val
			}
		}
		pvc.Annotations = newAnnotations
	}

	return pvc
}

func (c *csiDriver) getVolumeSnapshotClass(snapshotClassName string) (*kSnapshotv1beta1.VolumeSnapshotClass, error) {
	return c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Get(context.TODO(), snapshotClassName, metav1.GetOptions{})
}

// GetVolumeSnapshotClass retrieves a backed up volume snapshot class
func (cbo *csiBackupObject) GetVolumeSnapshotClass(snapshotID string) (*kSnapshotv1beta1.VolumeSnapshotClass, error) {
	vs, ok := cbo.VolumeSnapshots[snapshotID]
	if !ok {
		return nil, fmt.Errorf("failed to retrieve volume snapshot for snapshotID %s", snapshotID)
	}

	if vs.Spec.VolumeSnapshotClassName == nil {
		return nil, fmt.Errorf("failed to retrieve volume snapshot class for snapshot %s. Volume snapshot class is undefined", snapshotID)
	}
	vsClassName := *vs.Spec.VolumeSnapshotClassName

	vsClass, ok := cbo.VolumeSnapshotClasses[vsClassName]
	if !ok {
		return nil, fmt.Errorf("failed to retrieve volume snapshot class for snapshotID %s", snapshotID)
	}

	return vsClass, nil
}

func (c *csiDriver) createVolumeSnapshotClass(snapshotClassName, driverName string) (*kSnapshotv1beta1.VolumeSnapshotClass, error) {
	return c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Create(context.TODO(), &kSnapshotv1beta1.VolumeSnapshotClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: snapshotClassName,
		},
		Driver:         driverName,
		DeletionPolicy: kSnapshotv1beta1.VolumeSnapshotContentRetain,
	}, metav1.CreateOptions{})
}

func (c *csiDriver) ensureVolumeSnapshotClassCreated(csiDriverName, snapshotClassName string) error {
	if !c.snapshotClassCreatedForDriver[csiDriverName] {
		vsClass, err := c.getVolumeSnapshotClass(snapshotClassName)
		if k8s_errors.IsNotFound(err) {
			_, err = c.createVolumeSnapshotClass(snapshotClassName, csiDriverName)
			if err != nil {
				return err
			}
			logrus.Debugf("volumesnapshotclass created: %v", snapshotClassName)
		} else if err != nil {
			return err
		}

		// If we've found a vsClass, but it doesn't have a RetainPolicy, update to Retain.
		// This is essential for the storage backend to not delete the snapshot.
		// Some CSI drivers require specific VolumeSnapshotClass parameters, so we will leave those as is.
		if vsClass.DeletionPolicy == kSnapshotv1beta1.VolumeSnapshotContentDelete {
			vsClass.DeletionPolicy = kSnapshotv1beta1.VolumeSnapshotContentRetain
			_, err = c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Update(context.TODO(), vsClass, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
		}

		c.snapshotClassCreatedForDriver[snapshotClassName] = true
	}

	return nil
}

func (c *csiDriver) snapshotReady(vs *kSnapshotv1beta1.VolumeSnapshot) bool {
	return vs.Status != nil && vs.Status.ReadyToUse != nil && *vs.Status.ReadyToUse
}

func (c *csiDriver) snapshotContentReady(vscontent *kSnapshotv1beta1.VolumeSnapshotContent) bool {
	return vscontent.Status != nil && vscontent.Status.ReadyToUse != nil && *vscontent.Status.ReadyToUse
}

func formatReasonErrorMessage(reason, vsError, vscError string) string {
	var snapshotError string

	switch {
	case vsError != "" && vscError != "" && vsError != vscError:
		snapshotError = fmt.Sprintf("snapshot error: %s, snapshotContent error: %s", vsError, vscError)

	case vsError != "":
		snapshotError = fmt.Sprintf("snapshot error: %s", vsError)

	case vscError != "":
		snapshotError = fmt.Sprintf("snapshotContent error: %s", vscError)
	}

	if snapshotError != "" {
		return fmt.Sprintf("%s: %s", reason, snapshotError)
	}

	return reason
}

func getPVCSize(pvc *v1.PersistentVolumeClaim) uint64 {
	size := int64(0)
	reqSize, ok := pvc.Spec.Resources.Requests[v1.ResourceStorage]
	if !ok {
		logrus.Errorf("failed to get PVC size from spec: %s", pvc.Name)
	} else {
		size, ok = reqSize.AsInt64()
		if !ok {
			logrus.Errorf("failed to convert PVC size: %s", pvc.Name)
		}
	}

	return uint64(size)
}

func getSnapshotSize(vs *kSnapshotv1beta1.VolumeSnapshot) uint64 {
	if vs.Status != nil && vs.Status.RestoreSize != nil {
		size, _ := vs.Status.RestoreSize.AsInt64()
		return uint64(size)
	}

	return 0
}

func (c *csiDriver) pvcBindFinished(pvc *v1.PersistentVolumeClaim) bool {
	bindCompleted := pvc.Annotations[annPVBindCompleted]
	boundByController := pvc.Annotations[annPVBoundByController]
	return pvc.Status.Phase == v1.ClaimBound && bindCompleted == "yes" && boundByController == "yes"
}

func (c *csiDriver) pvcWaitingForFirstConsumer(pvc *v1.PersistentVolumeClaim) bool {
	var sc *storagev1.StorageClass
	var err error
	storageClassName := k8shelper.GetPersistentVolumeClaimClass(pvc)
	if storageClassName != "" {
		sc, err = storage.Instance().GetStorageClass(storageClassName)
		if err != nil {
			logrus.Warnf("did not get the storageclass %s for pvc %s/%s, err: %v", storageClassName, pvc.Namespace, pvc.Name, err)
			return false
		}
		return *sc.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer
	}
	return false
}

func (c *csiDriver) getSnapshotContentError(vscName string) string {
	if vsc, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Get(context.TODO(), vscName, metav1.GetOptions{}); err == nil && vsc != nil {
		if vsc.Status != nil && vsc.Status.Error != nil && vsc.Status.Error.Message != nil {
			return *vsc.Status.Error.Message
		}
	} else {
		logrus.Warnf("did not find volume snapshot content %s: %v", vscName, err)
	}

	return ""
}

func stringPtr(s string) *string {
	return &s
}

// uploadObject uploads the given data to the backup location
func (c *csiDriver) UploadSnapshotObjects(
	backupLocation *storkapi.BackupLocation,
	snapshotInfoList []SnapshotInfo,
	objectPath, objectName string,
) error {

	vsMap := make(map[string]*kSnapshotv1beta1.VolumeSnapshot)
	vsContentMap := make(map[string]*kSnapshotv1beta1.VolumeSnapshotContent)
	vsClassMap := make(map[string]*kSnapshotv1beta1.VolumeSnapshotClass)

	for _, snapshotInfo := range snapshotInfoList {
		snapID := snapshotInfo.SnapshotRequest.(*kSnapshotv1beta1.VolumeSnapshot).Name
		vsMap[snapID] = snapshotInfo.SnapshotRequest.(*kSnapshotv1beta1.VolumeSnapshot)
		vsContentMap[snapID] = snapshotInfo.Content.(*kSnapshotv1beta1.VolumeSnapshotContent)
		snapshotClassName := snapshotInfo.Class.(*kSnapshotv1beta1.VolumeSnapshotClass).Name
		vsClassMap[snapshotClassName] = snapshotInfo.Class.(*kSnapshotv1beta1.VolumeSnapshotClass)
	}

	csiBackup := csiBackupObject{
		VolumeSnapshots:        vsMap,
		VolumeSnapshotContents: vsContentMap,
		VolumeSnapshotClasses:  vsClassMap,
	}

	data, err := json.Marshal(csiBackup)
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

	writer, err := bucket.NewWriter(context.TODO(), filepath.Join(objectPath, objectName), nil)
	if err != nil {
		return err
	}

	_, err = writer.Write(data)
	if err != nil {
		closeErr := writer.Close()
		if closeErr != nil {
			logrus.Errorf("error closing writer for objectstore: %v", closeErr)
		}
		return err
	}
	err = writer.Close()
	if err != nil {
		logrus.Errorf("error closing writer for objectstore: %v", err)
		return err
	}
	return nil
}

func (c *csiDriver) DownloadSnapshotObjects(
	backupLocation *storkapi.BackupLocation,
	objectPath string,
) ([]SnapshotInfo, error) {
	var snapshotInfoList []SnapshotInfo
	bucket, err := objectstore.GetBucket(backupLocation)
	if err != nil {
		return snapshotInfoList, err
	}

	exists, err := bucket.Exists(context.TODO(), objectPath)
	if err != nil || !exists {
		return snapshotInfoList, err
	}
	data, err := bucket.ReadAll(context.TODO(), objectPath)
	if err != nil {
		return snapshotInfoList, err
	}
	if backupLocation.Location.EncryptionKey != "" {
		if data, err = crypto.Decrypt(data, backupLocation.Location.EncryptionKey); err != nil {
			return snapshotInfoList, err
		}
	}

	cbo := &csiBackupObject{}
	err = json.Unmarshal(data, cbo)
	if err != nil {
		return snapshotInfoList, err
	}

	for snapID := range cbo.VolumeSnapshots {
		var snapshotInfo SnapshotInfo
		snapshotInfo.SnapshotRequest = cbo.VolumeSnapshots[snapID]
		snapshotInfo.Content = cbo.VolumeSnapshotContents[snapID]
		snapshotInfo.Class, err = cbo.GetVolumeSnapshotClass(snapID)
		if err != nil {
			return snapshotInfoList, err
		}
		snapshotInfoList = append(snapshotInfoList, snapshotInfo)
	}

	return snapshotInfoList, nil
}

func (c *csiDriver) DeleteSnapshotObject(
	backupLocation *storkapi.BackupLocation,
	objectPath string,
) error {
	bucket, err := objectstore.GetBucket(backupLocation)
	if err != nil {
		return err
	}

	if objectPath != "" {
		if err = bucket.Delete(context.TODO(), objectPath); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
			return fmt.Errorf("error deleting object %s: %v", objectPath, err)
		}
	}

	return nil
}

func (c *csiDriver) restoreVolumeSnapshotClass(vsClass *kSnapshotv1beta1.VolumeSnapshotClass) (*kSnapshotv1beta1.VolumeSnapshotClass, error) {
	vsClass.ResourceVersion = ""
	vsClass.UID = ""
	newVSClass, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Create(context.TODO(), vsClass, metav1.CreateOptions{})
	if err != nil {
		if k8s_errors.IsAlreadyExists(err) {
			return vsClass, nil
		}
		return nil, err
	}

	return newVSClass, nil
}

func (c *csiDriver) restoreVolumeSnapshot(
	namespace string,
	vs *kSnapshotv1beta1.VolumeSnapshot,
	vsc *kSnapshotv1beta1.VolumeSnapshotContent,
) (*kSnapshotv1beta1.VolumeSnapshot, error) {
	vs.ResourceVersion = ""
	vs.Spec.Source.PersistentVolumeClaimName = nil
	vs.Spec.Source.VolumeSnapshotContentName = &vsc.Name
	vs.Namespace = namespace
	newVS, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(namespace).Create(context.TODO(), vs, metav1.CreateOptions{})
	if err != nil {
		if k8s_errors.IsAlreadyExists(err) {
			return vs, nil
		}
		return nil, err
	}

	return newVS, nil
}

func (c *csiDriver) restoreVolumeSnapshotContent(
	namespace string,
	vs *kSnapshotv1beta1.VolumeSnapshot,
	vsc *kSnapshotv1beta1.VolumeSnapshotContent,
	retain bool,
) (*kSnapshotv1beta1.VolumeSnapshotContent, error) {
	snapshotHandle := *vsc.Status.SnapshotHandle
	vsc.ResourceVersion = ""
	vsc.Spec.Source.VolumeHandle = nil
	vsc.Spec.Source.SnapshotHandle = &snapshotHandle
	vsc.Spec.VolumeSnapshotRef.Name = vs.Name
	vsc.Spec.VolumeSnapshotRef.Namespace = namespace
	vsc.Spec.VolumeSnapshotRef.UID = vs.UID

	// ensure all vscontent have the desired delete policy
	desiredRetainPolicy := kSnapshotv1beta1.VolumeSnapshotContentRetain
	if !retain {
		desiredRetainPolicy = kSnapshotv1beta1.VolumeSnapshotContentDelete
	}

	vsc.Spec.DeletionPolicy = desiredRetainPolicy
	newVSC, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Create(context.TODO(), vsc, metav1.CreateOptions{})
	if err != nil {
		if k8s_errors.IsAlreadyExists(err) {
			return vsc, nil
		}
		return nil, err
	}

	return newVSC, nil
}

func (c *csiDriver) RecreateSnapshotResources(
	snapshotInfo SnapshotInfo,
	snapshotDriverName string,
	namespace string,
	retain bool,
) (SnapshotInfo, error) {
	var err error
	var newSnapshotInfo SnapshotInfo

	// Get VSC and VS
	vsc := snapshotInfo.Content.(*kSnapshotv1beta1.VolumeSnapshotContent)
	vs := snapshotInfo.SnapshotRequest.(*kSnapshotv1beta1.VolumeSnapshot)
	vsClass := snapshotInfo.Class.(*kSnapshotv1beta1.VolumeSnapshotClass)

	logrus.Infof("recreating snapshot resources for %s/%s", namespace, vs.Name)

	// make sure snapshot class is created for this object.
	// if we have already created it in this batch, do not check if created already.
	err = c.ensureVolumeSnapshotClassCreated(snapshotDriverName, vsClass.Name)
	if err != nil {
		return newSnapshotInfo, err
	}

	// Create vsClass
	newVSClass, err := c.restoreVolumeSnapshotClass(vsClass)
	if err != nil {
		return newSnapshotInfo, fmt.Errorf("failed to restore VolumeSnapshotClass for deletion: %s", err.Error())
	}

	newSnapshotInfo.Class = newVSClass
	logrus.Debugf("created volume snapshot class %s", vsClass.Name)

	// Create VS, bound to VSC
	newVS, err := c.restoreVolumeSnapshot(namespace, vs, vsc)
	if err != nil {
		return newSnapshotInfo, fmt.Errorf("failed to restore VolumeSnapshot for deletion: %s", err.Error())
	}

	newSnapshotInfo.SnapshotRequest = newVS
	logrus.Debugf("created volume snapshot %s/%s", vs.Namespace, vs.Name)

	// Create VSC
	newVSC, err := c.restoreVolumeSnapshotContent(namespace, newVS, vsc, retain)
	if err != nil {
		return newSnapshotInfo, err
	}

	newSnapshotInfo.Content = newVSC
	logrus.Debugf("created volume snapshot content %s for snapshot %s/%s", vsc.Name, vs.Namespace, vs.Name)

	return newSnapshotInfo, nil
}

func getBackupInfoFromObjectKey(objKey string) (string, string) {
	var backupUID, timestamp string

	keySplits := strings.Split(objKey, "/")
	fileName := keySplits[len(keySplits)-1]
	fileSplits := strings.Split(fileName, "-")
	backupUID = strings.Join(fileSplits[0:len(fileSplits)-1], "-")
	timestamp = strings.Split(fileSplits[len(fileSplits)-1], ".")[0]

	return backupUID, timestamp
}

func (c *csiDriver) getCSISnapshotsCRList(backupLocation *storkapi.BackupLocation, pvcUID, objectPath string) ([]string, error) {
	var vsList []string
	var timestamps []string
	timestampBackupMapping := make(map[string]string)

	bucket, err := objectstore.GetBucket(backupLocation)
	if err != nil {
		return vsList, err
	}
	iterator := bucket.List(&blob.ListOptions{
		Prefix: fmt.Sprintf("%s/", objectPath),
	})

	for {
		object, err := iterator.Next(context.TODO())
		if err == io.EOF {
			break
		}
		if err != nil {
			return vsList, err
		}
		if object.IsDir {
			continue
		}

		backupUID, timestamp := getBackupInfoFromObjectKey(object.Key)
		logrus.Debugf("volumes snapshots file: %s, backupUID: %s, timestamp: %s", object.Key, backupUID, timestamp)
		timestamps = append(timestamps, timestamp)
		timestampBackupMapping[timestamp] = object.Key
	}

	sort.Strings(timestamps)
	for _, timestamp := range timestamps {
		vsList = append(vsList, timestampBackupMapping[timestamp])
	}
	return vsList, nil
}

func (c *csiDriver) RetainLocalSnapshots(
	backupLocation *storkapi.BackupLocation,
	snapshotDriverName string,
	snapshotClassName string,
	namespace string,
	pvcUID string,
	objectPath string,
	retain bool,
) error {
	vsCRList, err := c.getCSISnapshotsCRList(backupLocation, pvcUID, objectPath)
	if err != nil {
		return fmt.Errorf("failed in getting list of older volumesnapshot CRs from objectstore : %v", err)
	}

	for index, volumeSnapshotCR := range vsCRList {
		snapshotInfoList, err := c.DownloadSnapshotObjects(backupLocation, volumeSnapshotCR)
		if err != nil {
			return err
		}

		for _, snapshotInfo := range snapshotInfoList {
			vs := snapshotInfo.SnapshotRequest.(*kSnapshotv1beta1.VolumeSnapshot)
			snapName := vs.Name

			vs.Annotations[snapDeleteAnnotation] = "true"
			snapshotInfo.SnapshotRequest = vs
			newSnapInfo, err := c.RecreateSnapshotResources(snapshotInfo, snapshotDriverName, namespace, retain)
			if err != nil {
				return fmt.Errorf("recreating snapshot resources failed for snapshot %s/%s: %v", namespace, snapName, err)
			}
			logrus.Debugf("successfully recreated snapshot resources for snapshot %s/%s", namespace, snapName)

			// Check if the snapshot is scheduled for restore, then don't delete it
			vs = newSnapInfo.SnapshotRequest.(*kSnapshotv1beta1.VolumeSnapshot)
			if restoreAnnotation, ok := vs.Annotations[snapRestoreAnnotation]; ok {
				if restoreAnnotation == "true" {
					logrus.Debugf("volumesnapshot %s is set for restore, hence not deleting it", vs.Name)
					continue
				}
			}

			// Waiting for vs to get bound with vsc as in cleanup path by the time deletesnapshot is running,
			// vsc may not be bound to vs.
			err = c.waitForVolumeSnapshotBound(vs, namespace)
			if err == nil {
				if index < len(vsCRList)-localCSIRetention {
					retain = false
				} else {
					retain = true
				}
				err = c.DeleteSnapshot(
					vs.Name,
					namespace,
					retain, // retain snapshot content
				)
			}
			if err != nil {
				logrus.Errorf("failed to delete the old snapshot %s: %v", vs.Name, err)
			}
			logrus.Debugf("successfully deleted the recreated snapshot resources for snapshot %s/%s", namespace, snapName)
			if !retain {
				// Delete the CRs from objectstore
				err = c.DeleteSnapshotObject(backupLocation, volumeSnapshotCR)
				if err != nil {
					logrus.Errorf("deleting the CRs from objectstore failed for snapshot %s/%s: %v", namespace, snapName, err)
				}
			}
			logrus.Debugf("successfully deleted snapshot resources in objectstore for snapshot %s/%s", namespace, snapName)
		}
	}

	return nil
}

func (c *csiDriver) getLocalSnapshot(backupLocation *storkapi.BackupLocation, pvcUID, backupUID, objectPath string) (SnapshotInfo, error) {
	var snapshotInfo SnapshotInfo
	var found bool
	var vsCRPath string

	vsCRList, err := c.getCSISnapshotsCRList(backupLocation, pvcUID, objectPath)
	if err != nil {
		return snapshotInfo, fmt.Errorf("failed in getting list of older volumesnapshot CRs from objectstore : %v", err)
	}
	for _, volumeSnapshotCR := range vsCRList {
		tempBackupUID, _ := getBackupInfoFromObjectKey(volumeSnapshotCR)
		if tempBackupUID == backupUID {
			logrus.Debugf("resources of local snapshot for backup %s are present in the objectpath %s", backupUID, objectPath)
			vsCRPath = volumeSnapshotCR
			found = true
			break
		}
	}
	if !found {
		msg := fmt.Sprintf("local snapshot resources for backup %s are not found", backupUID)
		logrus.Debugf(msg)
		return snapshotInfo, nil
	}

	snapshotInfoList, err := c.DownloadSnapshotObjects(backupLocation, vsCRPath)
	if err != nil {
		return snapshotInfo, err
	}
	if len(snapshotInfoList) == 0 {
		return snapshotInfo, nil
	}
	return snapshotInfoList[0], nil
}

func (c *csiDriver) RestoreFromLocalSnapshot(backupLocation *storkapi.BackupLocation, pvc *v1.PersistentVolumeClaim, snapshotDriverName, pvcUID, backupUID, objectPath, namespace string) (bool, error) {
	var status bool
	// Is local snapshot present
	snapshotInfo, err := c.getLocalSnapshot(backupLocation, pvcUID, backupUID, objectPath)
	if err != nil {
		return status, err
	}

	if snapshotInfo.SnapshotRequest == nil {
		return status, nil
	}

	vs := snapshotInfo.SnapshotRequest.(*kSnapshotv1beta1.VolumeSnapshot)
	// Set the restore flag before doing restore
	vs.Annotations[snapRestoreAnnotation] = "true"
	snapshotInfo.SnapshotRequest = vs
	newSnapshotInfo, err := c.RecreateSnapshotResources(snapshotInfo, snapshotDriverName, namespace, true)
	if err != nil {
		return status, err
	}
	// Check if the snapshot is scheduled for delete, then don't restore it
	vs = newSnapshotInfo.SnapshotRequest.(*kSnapshotv1beta1.VolumeSnapshot)
	if deleteAnnotation, ok := vs.Annotations[snapDeleteAnnotation]; ok {
		if deleteAnnotation == "true" {
			logrus.Infof("volumesnapshot %s is set for delete, hence not restoring from it", vs.Name)
			return status, nil
		}
	}

	err = c.waitForVolumeSnapshotBound(vs, namespace)
	if err != nil {
		return status, fmt.Errorf("volumesnapshot %s failed to get bound: %v", vs.Name, err)
	}

	// create a new pvc for restore from the snapshot
	pvc, err = c.RestoreVolumeClaim(
		RestoreSnapshotName(vs.Name),
		RestoreNamespace(namespace),
		PVC(*pvc),
	)
	if err != nil {
		return status, fmt.Errorf("failed to restore pvc %s/%s from csi local snapshot: %v", namespace, pvc.Name, err)
	}
	logrus.Debugf("created pvc: %s/%s", pvc.Namespace, pvc.Name)

	status = true
	return status, nil
}

func (c *csiDriver) CleanUpRestoredResources(backupLocation *storkapi.BackupLocation, pvc *v1.PersistentVolumeClaim,
	pvcUID, backupUID, objectPath, namespace string) error {

	// Only delete pvc if pvc object has been passed
	if len(pvc.Name) != 0 {
		err := core.Instance().DeletePersistentVolumeClaim(pvc.Name, namespace)
		if err != nil && !k8s_errors.IsNotFound(err) {
			return err
		}
	}

	// Get local snapshot present
	snapshotInfo, err := c.getLocalSnapshot(backupLocation, pvcUID, backupUID, objectPath)
	if err != nil {
		return err
	}

	// if snapshot is not present, return
	if snapshotInfo.SnapshotRequest == nil {
		return nil
	}

	vs := snapshotInfo.SnapshotRequest.(*kSnapshotv1beta1.VolumeSnapshot)
	err = c.DeleteSnapshot(vs.Name, namespace, true)
	if err != nil {
		return err
	}

	return nil
}

func (c *csiDriver) createJob(pvc *v1.PersistentVolumeClaim, namespace string) (*batchv1.Job, error) {
	jobName := toBoundJobPVCName(pvc.Name, string(pvc.GetUID()))
	// if already the job is running or in completed state , no need to rerun the job
	job, err := batch.Instance().GetJob(jobName, namespace)
	if err == nil {
		return job, nil
	}

	// Setup service account
	if err := utils.SetupServiceAccount(jobName, namespace, roleFor()); err != nil {
		errMsg := fmt.Sprintf("error creating service account %s/%s: %v", namespace, jobName, err)
		logrus.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	jobSpec, err := buildJobSpec(pvc, namespace)
	if err != nil {
		errMsg := fmt.Sprintf("error creating job spec %s/%s: %v", namespace, jobName, err)
		logrus.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	if job, err = batch.Instance().CreateJob(jobSpec); err != nil && !k8s_errors.IsAlreadyExists(err) {
		errMsg := fmt.Sprintf("creation of job %s failed: %v", jobName, err)
		logrus.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	return job, nil
}

func roleFor() *rbacv1.Role {
	return &rbacv1.Role{
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups:     []string{"security.openshift.io"},
				Resources:     []string{"securitycontextconstraints"},
				ResourceNames: []string{"hostaccess"},
				Verbs:         []string{"use"},
			},
		},
	}
}

func (c *csiDriver) deletejob(pvc *v1.PersistentVolumeClaim, namespace string) error {
	jobName := toBoundJobPVCName(pvc.Name, string(pvc.GetUID()))
	t := func() (interface{}, bool, error) {
		if err := batch.Instance().DeleteJobWithForce(jobName, namespace); err != nil && !k8s_errors.IsNotFound(err) {
			return nil, true, fmt.Errorf("deletion of job %s/%s failed: %v", namespace, jobName, err)
		}
		if err := utils.CleanServiceAccount(jobName, namespace); err != nil {
			return nil, true, fmt.Errorf("deletion of service account %s/%s failed: %v", namespace, jobName, err)
		}

		if _, err := batch.Instance().GetJob(jobName, namespace); err != nil && !k8s_errors.IsNotFound(err) {
			if err == nil {
				errMsg := fmt.Sprintf("job %s/%s still exists after deletion: %v", namespace, jobName, err)
				return nil, true, fmt.Errorf(errMsg)
			}
		}

		if _, err := core.Instance().GetServiceAccount(jobName, namespace); err != nil && !k8s_errors.IsNotFound(err) {
			if err == nil {
				errMsg := fmt.Sprintf("service account %s/%s still exists after deletion: %v", namespace, jobName, err)
				return nil, true, fmt.Errorf(errMsg)
			}
		}

		pods, err := core.Instance().GetPodsUsingPVC(pvc.Name, namespace)
		if err == nil && len(pods) > 0 {
			logrus.Debugf("pvc %s/%s is still getting used by job pod %s", namespace, pvc.Name, pods[0].Name)
			return nil, true, fmt.Errorf("pvc %s/%s is still getting used by job pod %s", namespace, pvc.Name, pods[0].Name)
		}

		return nil, false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, shortRetryTimeout, shortRetryTimeoutInterval); err != nil {
		return fmt.Errorf("timed out waiting for cleaning up job %s related resources used for volume bind for pvc: %v", jobName, err)
	}
	return nil
}

func buildJobSpec(
	pvc *v1.PersistentVolumeClaim,
	namespace string,
) (*batchv1.Job, error) {
	jobName := toBoundJobPVCName(pvc.Name, string(pvc.GetUID()))
	cmd := strings.Join([]string{
		"sleep 5",
	}, " ")

	jobPodBackOffLimit := int32(1)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: namespace,
			Annotations: map[string]string{
				utils.SkipResourceAnnotation: "true",
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &jobPodBackOffLimit,
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					RestartPolicy:      v1.RestartPolicyOnFailure,
					ImagePullSecrets:   utils.ToImagePullSecret(utils.KopiaExecutorImageSecret(kdmpConfig, "kube-system")),
					ServiceAccountName: jobName,
					Containers: []v1.Container{
						{
							Name:            "kopiaexecutor",
							Image:           utils.KopiaExecutorImage(kdmpConfig, "kube-system"),
							ImagePullPolicy: v1.PullAlways,
							Command: []string{
								"/bin/sh",
								"-x",
								"-c",
								cmd,
							},
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "vol",
									MountPath: "/data",
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "vol",
							VolumeSource: v1.VolumeSource{
								PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
									ClaimName: pvc.Name,
								},
							},
						},
					},
				},
			},
		},
	}

	return job, nil
}

func (c *csiDriver) waitForVolumeSnapshotBound(vs *kSnapshotv1beta1.VolumeSnapshot, namespace string) error {
	t := func() (interface{}, bool, error) {
		curVS, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(namespace).Get(context.TODO(), vs.Name, metav1.GetOptions{})
		if err != nil {
			return nil, true, fmt.Errorf("failed to get volumesnapshot object %v/%v: %v", namespace, vs.Name, err)
		}
		if curVS.Status == nil || curVS.Status.BoundVolumeSnapshotContentName == nil {
			return nil, true, fmt.Errorf("failed to find get status for snapshot: %s/%s, status: %+v", namespace, vs.Name, vs.Status)
		}
		return nil, false, nil
	}
	if _, err := task.DoRetryWithTimeout(t, shortRetryTimeout, shortRetryTimeoutInterval); err != nil {
		return fmt.Errorf("timed out waiting for vs %s to get bound: %v", vs.Name, err)
	}
	return nil
}

func toBoundJobPVCName(pvcName string, pvcUID string) string {
	truncatedPVCName := pvcName
	if len(pvcName) > pvcNameLenLimitForJob {
		truncatedPVCName = pvcName[:pvcNameLenLimitForJob]
	}
	uidToken := strings.Split(pvcUID, "-")
	return fmt.Sprintf("%s-%s-%s", "bound", truncatedPVCName, uidToken[0])
}
