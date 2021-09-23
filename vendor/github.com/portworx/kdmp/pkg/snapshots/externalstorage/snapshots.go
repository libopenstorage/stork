package externalstorage

import (
	"fmt"

	snapshotv1 "github.com/kubernetes-incubator/external-storage/snapshot/pkg/apis/crd/v1"
	"github.com/kubernetes-incubator/external-storage/snapshot/pkg/client"
	"github.com/portworx/kdmp/pkg/snapshots"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/externalstorage"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	// DefaultStorageClass is a default storage class for external-storage snapshot controller.
	DefaultStorageClass = "snapshot-promoter"
)

// Driver is a external-storage implementation of the snapshot driver.
type Driver struct {
}

// Name returns a name of the driver backend.
func (d Driver) Name() string {
	return snapshots.ExternalStorage
}

// CreateSnapshot creates a volume snapshot for a pvc.
func (d Driver) CreateSnapshot(opts ...snapshots.Option) (string, string, error) {
	o := snapshots.Options{}
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return "", "", err
			}
		}
	}

	if o.RestoreNamespaces == "" {
		o.RestoreNamespaces = "*"
	}

	_, err := externalstorage.Instance().CreateSnapshot(&snapshotv1.VolumeSnapshot{
		Metadata: metav1.ObjectMeta{
			Name:      toSnapName(o.PVCNamespace, o.PVCName),
			Namespace: o.PVCNamespace,
			Annotations: map[string]string{
				snapshots.StorkSnapshotRestoreNamespacesAnnotation: o.RestoreNamespaces,
			},
		},
		Spec: snapshotv1.VolumeSnapshotSpec{
			PersistentVolumeClaimName: o.PVCName,
		},
	})
	if err != nil && !errors.IsAlreadyExists(err) {
		return "", "", err
	}

	return toSnapName(o.PVCNamespace, o.PVCName), o.PVCNamespace, nil
}

// DeleteSnapshot removes a snapshot.
func (d Driver) DeleteSnapshot(name, namespace string) error {
	if err := externalstorage.Instance().DeleteSnapshot(name, namespace); err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

// SnapshotStatus returns a status for a snapshot.
func (d Driver) SnapshotStatus(name, namespace string) (snapshots.Status, error) {
	snap, err := externalstorage.Instance().GetSnapshot(name, namespace)
	if err != nil {
		return "", err
	}
	return getStatus(snap), nil
}

// RestoreVolumeClaim creates a persistent volume claim from a provided snapshot.
func (d Driver) RestoreVolumeClaim(opts ...snapshots.Option) (*corev1.PersistentVolumeClaim, error) {
	o := snapshots.Options{}
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return nil, err
			}
		}
	}

	pvc, err := core.Instance().GetPersistentVolumeClaim(o.PVCName, o.PVCNamespace)
	// return a pvc if it's exist
	if err == nil {
		return pvc, nil
	}
	// create a pvc if it's not found
	if !errors.IsNotFound(err) {
		return nil, err
	}

	in := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      o.PVCName,
			Namespace: o.PVCNamespace,
			Annotations: map[string]string{
				client.SnapshotPVCAnnotation: o.Name,
			},
		},
		Spec: o.PVCSpec,
	}
	in.Spec.StorageClassName = &DefaultStorageClass
	if o.Namespace != "" {
		in.Annotations[snapshots.StorkSnapshotSourceNamespaceAnnotation] = o.Namespace
	}

	pvc, err = core.Instance().CreatePersistentVolumeClaim(in)
	if err != nil {
		return nil, err
	}

	return pvc, nil
}

func toSnapName(pvcNamespace, pvcName string) string {
	return fmt.Sprintf("%s-%s", pvcNamespace, pvcName)
}

func getStatus(snap *snapshotv1.VolumeSnapshot) snapshots.Status {
	if len(snap.Status.Conditions) == 0 {
		return snapshots.StatusUnknown
	}
	latest := snap.Status.Conditions[len(snap.Status.Conditions)-1]

	if latest.Type == snapshotv1.VolumeSnapshotConditionReady && latest.Status == corev1.ConditionTrue {
		return snapshots.StatusReady
	}

	if latest.Type == snapshotv1.VolumeSnapshotConditionPending && latest.Status == corev1.ConditionTrue {
		return snapshots.StatusInProgress
	}

	if latest.Type == snapshotv1.VolumeSnapshotConditionError && latest.Status == corev1.ConditionTrue {
		return snapshots.StatusFailed
	}

	return snapshots.StatusUnknown
}
