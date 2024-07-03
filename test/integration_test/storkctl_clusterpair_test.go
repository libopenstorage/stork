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
	"github.com/portworx/sched-ops/k8s/stork"
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
	if err = stork.Instance().DeleteClusterPair(cpName, defaultAdminNamespace); err != nil {
		Dash.Fatal("failed to delete cluster pair,error=", err)
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
	if err = stork.Instance().DeleteClusterPair(cpName, defaultAdminNamespace); err != nil {
		Dash.Fatal("failed to delete cluster pair,error=", err)
	}
}
