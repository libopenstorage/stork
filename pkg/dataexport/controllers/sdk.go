package controllers

import (
	"fmt"
	"strings"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func getJob(name, namespace string) (*batchv1.Job, error) {
	if err := checkMetadata(name, namespace); err != nil {
		return nil, err
	}

	into := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	if err := sdk.Get(into); err != nil {
		return nil, err
	}
	return into, nil
}

func deleteJob(name, namespace string) error {
	if err := checkMetadata(name, namespace); err != nil {
		return err
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	if err := sdk.Delete(job); !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func getPVC(name, namespace string) (*corev1.PersistentVolumeClaim, error) {
	if err := checkMetadata(name, namespace); err != nil {
		return nil, err
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	if err := sdk.Get(pvc); err != nil {
		return nil, err
	}

	return pvc, nil
}

func createPVC(pvc *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, error) {
	if err := checkMetadata(pvc.Name, pvc.Namespace); err != nil {
		return nil, err
	}

	if err := sdk.Create(pvc); err != nil {
		return nil, err
	}

	return pvc, nil
}

func getDataExport(name, namespace string) (*storkapi.DataExport, error) {
	vi := &storkapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	if err := sdk.Get(vi); err != nil {
		return nil, err
	}

	return vi, nil
}

func checkMetadata(name, namespace string) error {
	if strings.TrimSpace(name) == "" || strings.TrimSpace(namespace) == "" {
		return fmt.Errorf("name and namespace should not be empty")
	}
	return nil
}
