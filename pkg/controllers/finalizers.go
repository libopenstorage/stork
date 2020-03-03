package controllers

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ContainsFinalizer checks if a finalizer already exists.
func ContainsFinalizer(meta metav1.Object, finalizer string) bool {
	if meta == nil {
		return false
	}
	for _, f := range meta.GetFinalizers() {
		if f == finalizer {
			return true
		}
	}
	return false
}

// SetFinalizer adds a finalizer if it doesn't exists.
func SetFinalizer(meta metav1.Object, finalizer string) {
	if meta == nil {
		return
	}
	if !ContainsFinalizer(meta, finalizer) {
		meta.SetFinalizers(append(meta.GetFinalizers(), finalizer))
	}
}

// RemoveFinalizer removes a finalizer if it exists.
func RemoveFinalizer(meta metav1.Object, finalizer string) {
	if meta == nil {
		return
	}
	new := make([]string, 0)
	for _, f := range meta.GetFinalizers() {
		if f != finalizer {
			new = append(new, f)
		}
	}
	meta.SetFinalizers(new)
}
