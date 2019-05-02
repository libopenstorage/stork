package controllers

import (
	"context"
	"fmt"
	"reflect"
	"time"

	snap_v1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/apis/stork"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/controller"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"github.com/portworx/sched-ops/k8s"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
)

const (
	annotationPrefix = "stork.libopenstorage.org/"
	// RestoreAnnotation for pvc which has in-place resotre in progress
	RestoreAnnotation = annotationPrefix + "restore-in-progress"
)

// SnapshotRestoreController controller to watch over In-Place snap restore CRD's
type SnapshotRestoreController struct {
	Driver   volume.Driver
	Recorder record.EventRecorder
}

// Init initialize the cluster pair controller
func (c *SnapshotRestoreController) Init() error {
	err := c.createCRD()
	if err != nil {
		return err
	}

	return controller.Register(
		&schema.GroupVersionKind{
			Group:   stork.GroupName,
			Version: stork_api.SchemeGroupVersion.Version,
			Kind:    reflect.TypeOf(stork_api.VolumeSnapshotRestore{}).Name(),
		},
		"",
		1*time.Minute,
		c)
}

// Handle updates for SnapshotRestore objects
func (c *SnapshotRestoreController) Handle(ctx context.Context, event sdk.Event) error {
	var (
		snapRestore *stork_api.VolumeSnapshotRestore
		err         error
	)

	switch o := event.Object.(type) {
	case *stork_api.VolumeSnapshotRestore:
		snapRestore = o
		if snapRestore.Spec.SourceName == "" {
			c.Recorder.Event(snapRestore,
				v1.EventTypeWarning,
				string(snapRestore.Spec.SourceName),
				"Empty Snapshot Name")
			return fmt.Errorf("empty snapshot name")
		}

		if snapRestore.Status.Status == stork_api.VolumeSnapshotRestoreStatusReady {
			return nil
		}

		if event.Deleted {
			return c.handleDelete(snapRestore)
		}

		switch snapRestore.Status.Stage {
		case stork_api.VolumeSnapshotRestoreStageInitial:
			err = c.handleInitial(snapRestore)
		case stork_api.VolumeSnapshotRestoreStageRestore:
			err = c.handleFinal(snapRestore)
			c.Recorder.Event(snapRestore,
				v1.EventTypeNormal,
				string(snapRestore.Status.Status),
				"Snapshot in-Place  Restore completed")
		case stork_api.VolumeSnapshotRestoreStageReady:
			return nil
		default:
			err = fmt.Errorf("invalid stage for volume snapshot restore: %v", snapRestore.Status.Stage)
		}
	}

	if err != nil {
		log.VolumeSnapshotRestoreLog(snapRestore).Errorf("Error handling event: %v err: %v", event, err.Error())
		c.Recorder.Event(snapRestore,
			v1.EventTypeWarning,
			string(stork_api.VolumeSnapshotRestoreStatusError),
			err.Error())
	}

	err = sdk.Update(snapRestore)
	if err != nil {
		return err
	}

	return nil
}

func (c *SnapshotRestoreController) handleInitial(snapRestore *stork_api.VolumeSnapshotRestore) error {
	snapRestore.Status.Stage = stork_api.VolumeSnapshotRestoreStageRestore
	return nil
}

func (c *SnapshotRestoreController) handleFinal(snapRestore *stork_api.VolumeSnapshotRestore) error {
	var err error
	// snapshot is list of snapshots
	snapshotList := []*snap_v1.VolumeSnapshot{}

	snapName := snapRestore.Spec.SourceName
	snapNamespace := snapRestore.Spec.SourceNamespace
	if snapRestore.Spec.GroupSnapshot {
		log.VolumeSnapshotRestoreLog(snapRestore).Infof("GroupVolumeSnapshot In-place restore request for %v", snapName)
		snapshotList, err = k8s.Instance().GetSnapshotsForGroupSnapshot(snapName, snapNamespace)
		if err != nil {
			log.VolumeSnapshotRestoreLog(snapRestore).Errorf("unable to get group snapshot details %v", err)
			return err
		}
	} else {
		// GetSnapshot Details
		snapshot, err := k8s.Instance().GetSnapshot(snapName, snapNamespace)
		if err != nil {
			return fmt.Errorf("unable to get get snapshot  details %s: %v",
				snapName, err)
		}
		snapshotList = append(snapshotList, snapshot)
	}

	// get map of snapID and pvcs
	restoreVolumes, pvcList, err := getRestoreVolumeMap(snapshotList)
	if err != nil {
		return err
	}

	// annotate and delete pods using pvcs
	// TODO: Need to mark deployment/daemonset as stork scheduler
	err = markPVCForRestore(pvcList)
	if err != nil {
		return err
	}

	// Do driver volume snapshot restore here
	err = c.Driver.VolumeSnapshotRestore(snapRestore, restoreVolumes)
	if err != nil {
		snapRestore.Status.Status = stork_api.VolumeSnapshotRestoreStatusError
		err = unmarkPVCForRestore(pvcList)
		if err != nil {
			return err
		}
		return fmt.Errorf("failed to restore pvc details %v", err)
	}

	err = unmarkPVCForRestore(pvcList)
	if err != nil {
		return err
	}

	snapRestore.Status.Status = stork_api.VolumeSnapshotRestoreStatusReady
	snapRestore.Status.Stage = stork_api.VolumeSnapshotRestoreStageReady

	return nil
}

func markPVCForRestore(pvcList []*v1.PersistentVolumeClaim) error {
	for _, pvc := range pvcList {
		if pvc.Annotations == nil {
			pvc.Annotations = make(map[string]string)
		}
		pvc.Annotations[RestoreAnnotation] = "true"
		_, err := k8s.Instance().UpdatePersistentVolumeClaim(pvc)
		if err != nil {
			return err
		}
		pods, err := k8s.Instance().GetPodsUsingPVC(pvc.Name, pvc.Namespace)
		if err != nil {
			return err
		}
		for _, pod := range pods {
			logrus.Infof("Deleting pod %v", pod.Name)
			if err := k8s.Instance().DeletePod(pod.Name, pod.Namespace, true); err != nil {
				logrus.Errorf("Error deleting pod %v: %v", pod.Name, err)
				return err
			}

			if err := k8s.Instance().WaitForPodDeletion(pod.UID, pod.Namespace, 120*time.Second); err != nil {
				logrus.Errorf("Pod is not deleted %v:%v", pod.Name, err)
				return err
			}
		}
	}
	return nil
}

func unmarkPVCForRestore(pvcList []*v1.PersistentVolumeClaim) error {
	// remove annotation from pvc's
	for _, pvc := range pvcList {
		logrus.Infof("Removing annotation for %v", pvc.Name)
		if pvc.Annotations == nil {
			// somehow annotation got deleted but since restore is done,
			// we shouldn't care
			logrus.Warnf("No annotation found for %v", pvc.Name)
			continue
		}
		if _, ok := pvc.Annotations[RestoreAnnotation]; !ok {
			logrus.Warnf("Restore annotation not found for %v", pvc.Name)
			continue
		}
		delete(pvc.Annotations, RestoreAnnotation)
		_, err := k8s.Instance().UpdatePersistentVolumeClaim(pvc)
		if err != nil {
			return err
		}
	}

	return nil
}

func getRestoreVolumeMap(snapshotList []*snap_v1.VolumeSnapshot) (map[string]string, []*v1.PersistentVolumeClaim, error) {
	volumes := make(map[string]string)
	pvcList := []*v1.PersistentVolumeClaim{}

	for _, snap := range snapshotList {
		snapData := string(snap.Spec.SnapshotDataName)
		logrus.Debugf("Getting volume ID for pvc %v", snap.Spec.PersistentVolumeClaimName)
		volID, updatedPvc, err := getVolumeIDFromPVC(
			snap.Spec.PersistentVolumeClaimName,
			snap.Metadata.Namespace)
		if err != nil {
			return volumes, nil, fmt.Errorf("failed to get volume ID for snapshot %v", err)
		}
		volumes[volID] = snapData
		pvcList = append(pvcList, updatedPvc)
	}
	return volumes, pvcList, nil
}

func getVolumeIDFromPVC(pvcName, pvcNamespace string) (string, *v1.PersistentVolumeClaim, error) {
	pvc, err := k8s.Instance().GetPersistentVolumeClaim(pvcName, pvcNamespace)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get pvc details for snapshot %v", err)
	}
	volID, err := k8s.Instance().GetVolumeForPersistentVolumeClaim(pvc)
	if err != nil {
		return "", nil, err
	}
	logrus.Debugf("PVC %v \t VolID %v", pvc.Name, volID)

	return volID, pvc, nil
}

func (c *SnapshotRestoreController) createCRD() error {
	resource := k8s.CustomResource{
		Name:    stork_api.SnapshotRestoreResourceName,
		Plural:  stork_api.SnapshotRestoreResourcePlural,
		Group:   stork.GroupName,
		Version: stork_api.SchemeGroupVersion.Version,
		Scope:   apiextensionsv1beta1.NamespaceScoped,
		Kind:    reflect.TypeOf(stork_api.VolumeSnapshotRestore{}).Name(),
	}
	err := k8s.Instance().CreateCRD(resource)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	return k8s.Instance().ValidateCRD(resource, validateCRDTimeout, validateCRDInterval)
}

func (c *SnapshotRestoreController) handleDelete(snapRestore *stork_api.VolumeSnapshotRestore) error {
	return nil
}
