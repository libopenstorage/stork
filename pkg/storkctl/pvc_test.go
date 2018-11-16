// +build unittest

package storkctl

import (
	"testing"

	"k8s.io/api/core/v1"
)

func TestCreatePVCNoName(t *testing.T) {
	cmdArgs := []string{"pvc"}

	var pvcList v1.PersistentVolumeClaimList
	expected := "error: Exactly one argument needs to be provided for pvc name"
	testCommon(t, newCreateCommand, cmdArgs, &pvcList, expected, true)
}

func TestCreatePVCNoSnap(t *testing.T) {
	cmdArgs := []string{"pvc", "pvc1"}

	var pvcList v1.PersistentVolumeClaimList
	expected := "error: Snapshot name needs to be given"
	testCommon(t, newCreateCommand, cmdArgs, &pvcList, expected, true)
}

func TestCreatePVCNoSize(t *testing.T) {
	cmdArgs := []string{"pvc", "-s", "snap1", "pvc1"}

	var pvcList v1.PersistentVolumeClaimList
	expected := "error: Size needs to be provided"
	testCommon(t, newCreateCommand, cmdArgs, &pvcList, expected, true)
}

func TestCreatePVC(t *testing.T) {
	cmdArgs := []string{"pvc", "--snapshot", "snap1", "--size", "1", "pvc1"}

	var pvcList v1.PersistentVolumeClaimList
	expected := "PersistentVolumeClaim pvc1 created successfully\n"
	testCommon(t, newCreateCommand, cmdArgs, &pvcList, expected, false)
}

func TestCreatePVCSourceNamespace(t *testing.T) {
	cmdArgs := []string{"pvc", "--snapshot", "snap1", "--source-ns", "sourcenamespace", "--size", "1", "pvc2"}

	var pvcList v1.PersistentVolumeClaimList
	expected := "PersistentVolumeClaim pvc2 created successfully\n"
	testCommon(t, newCreateCommand, cmdArgs, &pvcList, expected, false)
}
