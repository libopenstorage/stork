package portworx

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/openstorage/volume"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/pkg/cache"
	mockcache "github.com/libopenstorage/stork/pkg/mock/cache"
	mockosd "github.com/libopenstorage/stork/pkg/mock/osd"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetes "k8s.io/client-go/kubernetes/fake"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

const (
	scName      = "sc1"
	pvcAnnKey   = "ann1"
	pvcAnnValue = "val1"
)

func TestGetPodVolumesSinglePVC(t *testing.T) {
	// Setup
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockDriver := mockosd.NewMockVolumeDriver(mockCtrl)
	mockCache := mockcache.NewMockSharedInformerCache(mockCtrl)
	p := setup(mockDriver, mockCache)

	// Create a sample pod with volumes
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume1",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "claim1",
						},
					},
				},
			},
		},
	}
	createPVAndPVC("claim1", "ns1", "pv1", v1.ClaimBound)

	bindingMode := storagev1.VolumeBindingImmediate
	mockCache.EXPECT().GetStorageClass(scName).Return(&storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: scName,
		},
		VolumeBindingMode: &bindingMode,
	}, nil).Times(1)

	mockDriver.EXPECT().Enumerate(
		&api.VolumeLocator{
			VolumeIds: []string{"pv1"},
		}, nil).Return([]*api.Volume{
		{
			Id: "id1",
			Locator: &api.VolumeLocator{
				Name:         "pv1",
				VolumeLabels: map[string]string{"k": "v"},
			},
			Spec: &api.VolumeSpec{
				Sharedv4:            true,
				Sharedv4ServiceSpec: &api.Sharedv4ServiceSpec{},
				VolumeLabels:        map[string]string{"k1": "v1"},
			},
		},
	}, nil)
	// Call the GetPodVolumes function
	volumes, volumesWFFC, err := p.GetPodVolumes(&pod.Spec, "ns1", true)
	require.NoError(t, err, "failed to get pod volumes")
	require.Len(t, volumes, 1, "incorrect volume count")

	// Make sure constructStorkVolume returns the correct values
	require.Equal(t, "id1", volumes[0].VolumeID, "incorrect volume id")
	require.Equal(t, "pv1", volumes[0].VolumeName, "incorrect volume name")
	require.Equal(t, "v", volumes[0].Labels["k"], "incorrect volume labels from locator")
	require.Equal(t, pvcAnnValue, volumes[0].Labels[pvcAnnKey], "annotations not found")
	require.True(t, volumes[0].NeedsAntiHyperconvergence, "incorrect anti-hyperconvergence flag")
	require.Equal(t, "v1", volumes[0].Labels["k1"], "incorrect volume labels from spec")

	require.Len(t, volumesWFFC, 0, "volumesWFFC should not be found")

	// Change a few return parameters from Enumerate

	mockCache.EXPECT().GetStorageClass(scName).Return(&storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: scName,
		},
		VolumeBindingMode: &bindingMode,
	}, nil).Times(1)
	mockDriver.EXPECT().Enumerate(
		&api.VolumeLocator{
			VolumeIds: []string{"pv1"},
		}, nil).Return([]*api.Volume{
		{
			Id: "id1",
			Locator: &api.VolumeLocator{
				Name: "pv1",
			},
			Spec: &api.VolumeSpec{},
		},
	}, nil)
	// Call the GetPodVolumes function
	volumes, volumesWFFC, err = p.GetPodVolumes(&pod.Spec, "ns1", true)
	require.NoError(t, err, "failed to get pod volumes")
	require.Len(t, volumes, 1, "incorrect volume count")

	// Make sure constructStorkVolume returns the correct values
	require.Equal(t, "id1", volumes[0].VolumeID, "incorrect volume id")
	require.Equal(t, "pv1", volumes[0].VolumeName, "incorrect volume name")
	require.Len(t, volumes[0].Labels, 1, "incorrect volume labels")
	require.Equal(t, pvcAnnValue, volumes[0].Labels[pvcAnnKey], "annotations not found")
	require.False(t, volumes[0].NeedsAntiHyperconvergence, "incorrect anti-hyperconvergence flag")

	require.Len(t, volumesWFFC, 0, "volumesWFFC should not be found")

	// Enumerate fails
	mockCache.EXPECT().GetStorageClass(scName).Return(&storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: scName,
		},
		VolumeBindingMode: &bindingMode,
	}, nil).Times(1)
	mockDriver.EXPECT().Enumerate(
		&api.VolumeLocator{
			VolumeIds: []string{"pv1"},
		}, nil).Return(nil, fmt.Errorf("some error"))
	// Call the GetPodVolumes function
	volumes, volumesWFFC, err = p.GetPodVolumes(&pod.Spec, "ns1", true)
	require.NoError(t, err, "failed to get pod volumes")
	require.Len(t, volumes, 1, "incorrect volume count")

	require.Empty(t, volumes[0].VolumeID, "volume id should be empty")
	require.Equal(t, "pv1", volumes[0].VolumeName, "incorrect volume name")
	require.Len(t, volumes[0].Labels, 1, "incorrect volume labels")
	require.Equal(t, pvcAnnValue, volumes[0].Labels[pvcAnnKey], "annotations should be returned even if enumerate fails")

	require.Len(t, volumesWFFC, 0, "volumesWFFC should not be found")
}

func TestGetPodVolumesMultiplePVC(t *testing.T) {
	// Setup
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockDriver := mockosd.NewMockVolumeDriver(mockCtrl)
	mockCache := mockcache.NewMockSharedInformerCache(mockCtrl)
	p := setup(mockDriver, mockCache)

	// Create a sample pod with volumes
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume1",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "claim1",
						},
					},
				},
				{
					Name: "volume2",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "claim2",
						},
					},
				},
			},
		},
	}
	createPVAndPVC("claim1", "ns1", "pv1", v1.ClaimBound)
	createPVAndPVC("claim2", "ns1", "pv2", v1.ClaimBound)

	bindingMode := storagev1.VolumeBindingImmediate
	mockCache.EXPECT().GetStorageClass(scName).Return(&storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: scName,
		},
		VolumeBindingMode: &bindingMode,
	}, nil).Times(2)

	mockDriver.EXPECT().Enumerate(
		&api.VolumeLocator{
			VolumeIds: []string{"pv1", "pv2"},
		}, nil).Return([]*api.Volume{
		{
			Id: "id1",
			Locator: &api.VolumeLocator{
				Name: "pv1",
			},
			Spec: &api.VolumeSpec{},
		},
		{
			Id: "id2",
			Locator: &api.VolumeLocator{
				Name: "pv2",
			},
			Spec: &api.VolumeSpec{},
		},
	}, nil)
	// Call the GetPodVolumes function
	volumes, volumesWFFC, err := p.GetPodVolumes(&pod.Spec, "ns1", true)
	require.NoError(t, err, "failed to get pod volumes")
	require.Len(t, volumes, 2, "incorrect volume count")
	require.Len(t, volumesWFFC, 0, "volumesWFFC should not be found")
}

func TestGetPodVolumesMultiplePVCWFFC(t *testing.T) {
	// Setup
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockDriver := mockosd.NewMockVolumeDriver(mockCtrl)
	mockCache := mockcache.NewMockSharedInformerCache(mockCtrl)
	p := setup(mockDriver, mockCache)

	// Create a sample pod with volumes
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume1",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "claim1",
						},
					},
				},
				{
					Name: "volume2",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "claim2",
						},
					},
				},
			},
		},
	}
	createPVAndPVC("claim1", "ns1", "pv1", v1.ClaimPending)
	createPVAndPVC("claim2", "ns1", "pv2", v1.ClaimBound)

	// Enumerate should be called only for one PVC which is Bound
	mockDriver.EXPECT().Enumerate(
		&api.VolumeLocator{
			VolumeIds: []string{"pv2"},
		}, nil).Return([]*api.Volume{
		{
			Id: "id2",
			Locator: &api.VolumeLocator{
				Name: "pv2",
			},
			Spec: &api.VolumeSpec{},
		},
	}, nil)

	bindingMode := storagev1.VolumeBindingWaitForFirstConsumer
	mockCache.EXPECT().GetStorageClass(scName).Return(&storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: scName,
		},
		VolumeBindingMode: &bindingMode,
	}, nil).Times(3)
	// Call the GetPodVolumes function
	volumes, volumesWFFC, err := p.GetPodVolumes(&pod.Spec, "ns1", true)
	require.NoError(t, err, "failed to get pod volumes")
	require.Len(t, volumes, 1, "incorrect volume count")
	require.Equal(t, "id2", volumes[0].VolumeID, "incorrect volume id")
	require.Equal(t, "pv2", volumes[0].VolumeName, "incorrect volume name")
	require.Len(t, volumesWFFC, 1, "volumesWFFC should not be found")
	require.Equal(t, "", volumesWFFC[0].VolumeID, "volume id should be empty")
	require.Equal(t, pvcAnnValue, volumes[0].Labels[pvcAnnKey], "annotations should be returned for WFFC volumes")
}

func TestGetPodVolumesPendingPVCs(t *testing.T) {
	// Setup
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockDriver := mockosd.NewMockVolumeDriver(mockCtrl)
	mockCache := mockcache.NewMockSharedInformerCache(mockCtrl)
	p := setup(mockDriver, mockCache)

	// Create a sample pod with volumes
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{
				{
					Name: "volume1",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "claim1",
						},
					},
				},
				{
					Name: "volume2",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "claim2",
						},
					},
				},
			},
		},
	}
	createPVAndPVC("claim1", "ns1", "pv1", v1.ClaimPending)
	createPVAndPVC("claim2", "ns1", "pv2", v1.ClaimBound)

	bindingMode := storagev1.VolumeBindingImmediate
	mockCache.EXPECT().GetStorageClass(scName).Return(&storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: scName,
		},
		VolumeBindingMode: &bindingMode,
	}, nil).Times(2)

	// Enumerate should not be called
	// Call the GetPodVolumes function
	volumes, volumesWFFC, err := p.GetPodVolumes(&pod.Spec, "ns1", true)
	require.Error(t, err, "GetPodVolumes should fail")
	require.Equal(t, err.Error(), "PVC is pending for claim1", "incorrect error message")
	require.Len(t, volumes, 0, "incorrect volume count")
	require.Len(t, volumesWFFC, 0, "volumesWFFC should not be found")
}

func createPVAndPVC(name, namespace, pvName string, status v1.PersistentVolumeClaimPhase) {
	sc := scName
	core.Instance().CreatePersistentVolumeClaim(&v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				pvcAnnKey: pvcAnnValue,
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			VolumeName:       pvName,
			StorageClassName: &sc,
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: status,
		},
	})
	core.Instance().CreatePersistentVolume(&v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
			Annotations: map[string]string{
				pvProvisionedByAnnotation: provisionerName,
			},
		}})
}

func setup(mockDriver volume.VolumeDriver, mockCache cache.SharedInformerCache) storkvolume.Driver {
	p := &portworx{
		initDone: true,
	}

	p.getAdminVolDriver = func() (volume.VolumeDriver, error) {
		return mockDriver, nil
	}

	fakeKubeClient := kubernetes.NewSimpleClientset()

	scheme.AddToScheme(scheme.Scheme)

	// setup fake k8s instances
	core.SetInstance(core.New(fakeKubeClient))
	cache.SetTestInstance(mockCache)
	return p
}
