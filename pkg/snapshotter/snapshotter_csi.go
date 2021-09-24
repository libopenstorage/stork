package snapshotter

import (
	"context"
	"fmt"
	"time"

	kSnapshotv1beta1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1beta1"
	kSnapshotClient "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

const (
	csiProviderName = "csi"
	// snapshotClassNamePrefix is the prefix for snapshot classes per CSI driver
	snapshotClassNamePrefix = "stork-csi-snapshot-class-"
	annPVBindCompleted      = "pv.kubernetes.io/bind-completed"
	annPVBoundByController  = "pv.kubernetes.io/bound-by-controller"

	// snapshotTimeout represents the duration to wait before timing out on snapshot completion
	snapshotTimeout = time.Minute * 5
	// restoreTimeout is the duration to wait before timing out the restore
	restoreTimeout = time.Minute * 5
)

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

	if o.SnapshotClassName == "" {
		o.SnapshotClassName = c.getDefaultSnapshotClassName(pv.Spec.CSI.Driver)
	}

	if err := c.ensureVolumeSnapshotClassCreated(pv.Spec.CSI.Driver, o.SnapshotClassName); err != nil {
		return "", "", "", err
	}
	vs := &kSnapshotv1beta1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:        o.Name,
			Namespace:   o.PVCNamespace,
			Annotations: o.Annotations,
		},
		Spec: kSnapshotv1beta1.VolumeSnapshotSpec{
			VolumeSnapshotClassName: stringPtr(o.SnapshotClassName),
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
		return "", "", "", err
	}
	return o.Name, o.PVCNamespace, pv.Spec.CSI.Driver, nil
}

func (c *csiDriver) DeleteSnapshot(name, namespace string, retain bool) error {
	vs, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshots(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get volumesnapshot object %v/%v: %v", namespace, name, err)
	}

	if vs.Status == nil || vs.Status.BoundVolumeSnapshotContentName == nil {
		return fmt.Errorf("failed to find get status for snapshot: %s/%s", vs.Namespace, vs.Name)
	}

	vsc, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotContents().Get(context.TODO(), *vs.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
	if err != nil {
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
	if snapshot.Spec.VolumeSnapshotClassName != nil {
		snapshotClassName = *snapshot.Spec.VolumeSnapshotClassName
	}
	snapshotClass, err := c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Get(context.TODO(), snapshotClassName, metav1.GetOptions{})
	if err != nil {
		snapshotInfo.Status = StatusFailed
		snapshotInfo.Reason = fmt.Sprintf("snapshot class %s lost during backup: %v", snapshotClassName, err)
		return snapshotInfo, err
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
		snapshotInfo.Status = StatusInProgress
		snapshotInfo.Reason = formatReasonErrorMessage(fmt.Sprintf("volume snapshot in progress for PVC %s", pvcName), vsError, vscError)
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
	pvc.Spec.DataSource = &v1.TypedLocalObjectReference{
		APIGroup: stringPtr("snapshot.storage.k8s.io"),
		Kind:     "VolumeSnapshot",
		Name:     o.RestoreSnapshotName,
	}
	pvc.Status = v1.PersistentVolumeClaimStatus{
		Phase: v1.ClaimPending,
	}
	pvc, err := core.Instance().CreatePersistentVolumeClaim(pvc)
	if err != nil {
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

func (c *csiDriver) getDefaultSnapshotClassName(driverName string) string {
	return snapshotClassNamePrefix + driverName
}

func (c *csiDriver) getVolumeSnapshotClass(snapshotClassName string) (*kSnapshotv1beta1.VolumeSnapshotClass, error) {
	return c.snapshotClient.SnapshotV1beta1().VolumeSnapshotClasses().Get(context.TODO(), snapshotClassName, metav1.GetOptions{})
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
		snapshotError = fmt.Sprintf("Snapshot error: %s. SnapshotContent error: %s", vsError, vscError)

	case vsError != "":
		snapshotError = fmt.Sprintf("Snapshot error: %s", vsError)

	case vscError != "":
		snapshotError = fmt.Sprintf("SnapshotContent error: %s", vscError)
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
