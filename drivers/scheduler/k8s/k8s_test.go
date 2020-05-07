package k8s

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/portworx/torpedo/drivers"
)

func TestSubstituteVolumeStorageClass(t *testing.T) {
	testCases := []struct {
		ProviderName             string
		pvc                      v1.PersistentVolumeClaim
		expectedStorageClassName string
	}{
		{
			ProviderName: drivers.ProviderAzure,
			pvc: v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						AzureStorageClassKey: "azure-disk",
					},
				},
			},
			expectedStorageClassName: "azure-disk",
		},
	}

	for _, testCase := range testCases {

		k := &K8s{
			ProviderName: testCase.ProviderName,
		}

		k.substitutePvcWithStorageClass(&testCase.pvc)

		if &testCase.pvc.Spec.StorageClassName == nil {
			t.Errorf("Storage class must not be nil")
			continue
		}
		if testCase.expectedStorageClassName != *testCase.pvc.Spec.StorageClassName {
			t.Errorf("Unexpected storage class name expected %v actual %v",
				testCase.expectedStorageClassName, testCase.pvc.Spec.StorageClassName)
		}
	}
}
