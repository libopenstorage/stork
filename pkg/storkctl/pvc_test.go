// +build unittest

package storkctl

import (
	"testing"
)

func TestCreatePVCNoName(t *testing.T) {
	cmdArgs := []string{"pvc"}

	expected := "error: Exactly one argument needs to be provided for pvc name"
	testCommon(t, newCreateCommand, cmdArgs, nil, expected, true)
}

func TestCreatePVCNoSnap(t *testing.T) {
	cmdArgs := []string{"pvc", "pvc1"}

	expected := "error: Snapshot name needs to be given"
	testCommon(t, newCreateCommand, cmdArgs, nil, expected, true)
}

func TestCreatePVCNoSize(t *testing.T) {
	cmdArgs := []string{"pvc", "-s", "snap1", "pvc1"}

	expected := "error: Size needs to be provided"
	testCommon(t, newCreateCommand, cmdArgs, nil, expected, true)
}

func TestCreatePVC(t *testing.T) {
	cmdArgs := []string{"pvc", "--snapshot", "snap1", "--size", "1", "pvc1"}

	expected := "PersistentVolumeClaim pvc1 created successfully\n"
	testCommon(t, newCreateCommand, cmdArgs, nil, expected, false)
}

func TestCreatePVCSourceNamespace(t *testing.T) {
	cmdArgs := []string{"pvc", "--snapshot", "snap1", "--source-ns", "sourcenamespace", "--size", "1", "pvc2"}

	expected := "PersistentVolumeClaim pvc2 created successfully\n"
	testCommon(t, newCreateCommand, cmdArgs, nil, expected, false)
}
