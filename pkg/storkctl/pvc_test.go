// +build unittest

package storkctl

import (
	"testing"
)

func TestCreatePVCNoName(t *testing.T) {
	cmdArgs := []string{"create", "pvc"}

	expected := "error: exactly one argument needs to be provided for pvc name"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreatePVCNoSnap(t *testing.T) {
	cmdArgs := []string{"create", "pvc", "pvc1"}

	expected := "error: snapshot name needs to be given"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreatePVCNoSize(t *testing.T) {
	cmdArgs := []string{"create", "pvc", "-s", "snap1", "pvc1"}

	expected := "error: size needs to be provided"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreatePVC(t *testing.T) {
	cmdArgs := []string{"create", "pvc", "--snapshot", "snap1", "--size", "1", "pvc1"}

	expected := "PersistentVolumeClaim pvc1 created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestCreatePVCSourceNamespace(t *testing.T) {
	cmdArgs := []string{"create", "pvc", "--snapshot", "snap1", "--source-ns", "sourcenamespace", "--size", "1", "pvc2"}

	expected := "PersistentVolumeClaim pvc2 created successfully\n"
	testCommon(t, cmdArgs, nil, expected, false)
}
