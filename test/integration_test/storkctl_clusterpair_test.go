//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/storkctl"
	"github.com/pborman/uuid"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/stork"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
)

func TestStorkCtlClusterPair(t *testing.T) {
	// Reset mock time before running any tests.
	err := setMockTime(nil)
	log.FailOnError(t, err, "Error resetting mock time")

	// Switch context to source cluster.
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	destinationKubeConfigPath, err = getDestinationKubeConfigFile()
	log.FailOnError(t, err, "Error getting destination kubeconfig file")
	srcKubeConfigPath, err = getSourceKubeConfigFile()
	log.FailOnError(t, err, "Error getting source kubeconfig file")

	t.Run("testStorkCtlClusterPairNFSBidirectional", testStorkCtlClusterPairNFSBidirectional)
	t.Run("testStorkCtlClusterPairNFSUnidirectional", testStorkCtlClusterPairNFSUnidirectional)
	t.Run("testStorkCtlClusterPairNFSTimeout", testStorkCtlClusterPairNFSTimeout)
}

// testClusterPairNFSBidirectional tests the bidirectional clusterpair creation workflow.
func testStorkCtlClusterPairNFSBidirectional(t *testing.T) {
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting source config")
	}()
	cpName := fmt.Sprintf("testclusterpair-%s", uuid.New())
	cmdFlags := map[string]string{
		"namespace":       defaultAdminNamespace,
		"src-kube-file":   srcKubeConfigPath,
		"dest-kube-file":  destinationKubeConfigPath,
		"provider":        "nfs",
		"nfs-server":      nfsSrvAddr,
		"nfs-export-path": nfsSrvExpPath,
		"nfs-sub-path":    fmt.Sprintf("test-%s", uuid.New()),
	}

	// Execute the storkctl command.
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	cmdArgs := []string{"create", "clusterpair", cpName}

	// Add the custom flags to the command arguments.
	for key, value := range cmdFlags {
		cmdArgs = append(cmdArgs, "--"+key)
		if value != "" {
			cmdArgs = append(cmdArgs, value)
		}
	}
	cmd.SetArgs(cmdArgs)

	// Execute the command.
	log.InfoD("The storkctl command being executed is %v", cmdArgs)
	if err := cmd.Execute(); err != nil {
		log.Error("Storkctl execution failed: %v", err)
		return
	}

	// Get the captured output as a string.
	actualOutput := outputBuffer.String()
	if !strings.Contains(actualOutput, fmt.Sprintf("Cluster pair %s created successfully. Direction: Destination -> Source", cpName)) {
		Dash.Fatal("cluster pair creation failed, expected success got=%s", actualOutput)
	}

	// Cleanup created resource.
	if err = cleanUpClusterPairTestResources(cpName, defaultAdminNamespace); err != nil {
		Dash.Fatal("cluster pair resources cleanup failed, error=", err)
	}

}

// testStorkCtlClusterPairNFSUnidirectional tests the unidirectional clusterpair creation workflow.
func testStorkCtlClusterPairNFSUnidirectional(t *testing.T) {
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting source config")
	}()
	cpName := fmt.Sprintf("testclusterpair-%s", uuid.New())
	cmdFlags := map[string]string{
		"namespace":       defaultAdminNamespace,
		"src-kube-file":   srcKubeConfigPath,
		"dest-kube-file":  destinationKubeConfigPath,
		"provider":        "nfs",
		"nfs-server":      nfsSrvAddr,
		"nfs-export-path": nfsSrvExpPath,
		"nfs-sub-path":    fmt.Sprintf("test-%s", uuid.New()),
	}

	// Execute the storkctl command.
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	cmdArgs := []string{"create", "clusterpair", cpName}

	// Add the custom flags to the command arguments.
	for key, value := range cmdFlags {
		cmdArgs = append(cmdArgs, "--"+key)
		if value != "" {
			cmdArgs = append(cmdArgs, value)
		}
	}
	cmdArgs = append(cmdArgs, "--unidirectional")
	cmd.SetArgs(cmdArgs)

	// Execute the command.
	log.InfoD("The storkctl command being executed is %v", cmdArgs)
	if err := cmd.Execute(); err != nil {
		log.Error("Storkctl execution failed: %v", err)
		return
	}

	// Get the captured output as a string.
	actualOutput := outputBuffer.String()
	if !strings.Contains(actualOutput, fmt.Sprintf("ClusterPair %s created successfully. Direction Source -> Destination", cpName)) {
		Dash.Fatal("cluster pair creation failed, expected success got=%s", actualOutput)
	}

	// Cleanup created resource.
	if err = cleanUpClusterPairTestResources(cpName, defaultAdminNamespace); err != nil {
		Dash.Fatal("cluster pair resources cleanup failed, error=", err)
	}
}

// testStorkCtlClusterPairNFSTimeout tests the --nfs-timeout-seconds flag for the NFS based clusterpair creation workflow.
func testStorkCtlClusterPairNFSTimeout(t *testing.T) {
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting source config")
	}()
	cpName := fmt.Sprintf("testclusterpair-%s", uuid.New())
	cmdFlags := map[string]string{
		"namespace":           defaultAdminNamespace,
		"src-kube-file":       srcKubeConfigPath,
		"dest-kube-file":      destinationKubeConfigPath,
		"provider":            "nfs",
		"nfs-server":          nfsSrvAddr,
		"nfs-export-path":     nfsSrvExpPath,
		"nfs-sub-path":        fmt.Sprintf("test-%s", uuid.New()),
		"nfs-timeout-seconds": "10",
	}

	// Execute the storkctl command.
	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	cmdArgs := []string{"create", "clusterpair", cpName}

	// Add the custom flags to the command arguments.
	for key, value := range cmdFlags {
		cmdArgs = append(cmdArgs, "--"+key)
		if value != "" {
			cmdArgs = append(cmdArgs, value)
		}
	}
	cmd.SetArgs(cmdArgs)

	// Execute the command.
	log.InfoD("The storkctl command being executed is %v", cmdArgs)
	if err := cmd.Execute(); err != nil {
		log.Error("Storkctl execution failed: %v", err)
		return
	}

	// Get the captured output as a string.
	actualOutput := outputBuffer.String()
	if !strings.Contains(actualOutput, fmt.Sprintf("Cluster pair %s created successfully. Direction: Destination -> Source", cpName)) {
		Dash.Fatal("cluster pair creation failed, expected success got=%s", actualOutput)
	}

	// Create a migrationschedule and migration. Wait for migration.
	instanceID := "nfs-timeout-mysql-migration"
	appKey := "mysql-1-pvc"

	triggerMigrationTest(
		t,
		instanceID,
		appKey,
		nil,
		instanceID,
		true,
		true,
		true,
		false,
	)

	// Cleanup created resource.
	if err = cleanUpClusterPairTestResources(cpName, defaultAdminNamespace); err != nil {
		Dash.Fatal("cluster pair resources cleanup failed, error=", err)
	}
}

// cleanUpClusterPairTestResources cleans up the resources that get created during the cluster pair integration
// test execution across both the source and destination cluster.
func cleanUpClusterPairTestResources(name, namespace string) (err error) {
	for i := 0; i < 2; i++ {
		switch i {
		case 0:
			if err = setDestinationKubeConfig(); err != nil {
				return err
			}
		case 1:
			if err = setSourceKubeConfig(); err != nil {
				return err
			}
		}
		if err = stork.Instance().DeleteClusterPair(name, namespace); err != nil && !k8s_errors.IsNotFound(err) {
			return fmt.Errorf("failed to delete cluster pair,error=%s", err)
		}
		if err = stork.Instance().DeleteBackupLocation(name, namespace); err != nil && !k8s_errors.IsNotFound(err) {
			return fmt.Errorf("failed to delete backuplocation,error=%s", err)
		}
		if err = core.Instance().DeleteSecret(name, namespace); err != nil && !k8s_errors.IsNotFound(err) {
			return fmt.Errorf("failed to delete secret,error=%s", err)
		}
	}
	return
}
