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
	"github.com/portworx/torpedo/drivers/scheduler"
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
	t.Run("testStorkCtlClusterPairNFSTimeout", testStorkCtlClusterPairNFSSecretCreation)
}

// testClusterPairNFSBidirectional tests the bidirectional clusterpair creation workflow.
func testStorkCtlClusterPairNFSBidirectional(t *testing.T) {
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting source config")
	}()

	///////////////////////////
	// Clusterpair creation //
	/////////////////////////
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

	///////////////////////////////////////
	// Deploy app for running migration //
	//////////////////////////////////////
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "Error resetting source config")
	instanceID := "nfs-bidirectional-cp-test"
	appKey := "mysql-1-pvc"

	// Schedule mysql replicas.
	ctxs, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{AppKeys: []string{appKey}, Namespace: instanceID})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for app to get to running state")
	defer blowNamespaceForTest(t, instanceID, false)

	//////////////////////
	// Start migration //
	////////////////////
	migrationName := fmt.Sprintf("%s-%s", instanceID, uuid.New())
	err = startAndValidateMigration(migrationName, cpName, []string{instanceID}, true)
	log.FailOnError(t, err, "failed to validate migration")

	////////////////////////////////
	// Cleanup created resources //
	//////////////////////////////
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

	///////////////////////////
	// Clusterpair creation //
	/////////////////////////
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
	if !strings.Contains(actualOutput, fmt.Sprintf("Cluster pair %s created successfully. Direction: Destination -> Source", cpName)) {
		Dash.Fatal("cluster pair creation failed, expected success got=%s", actualOutput)
	}

	///////////////////////////////////////
	// Deploy app for running migration //
	//////////////////////////////////////
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "Error resetting source config")
	instanceID := "nfs-unidirectional-cp-test"
	appKey := "mysql-1-pvc"

	// Schedule mysql replicas.
	ctxs, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{AppKeys: []string{appKey}, Namespace: instanceID})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for app to get to running state")
	defer blowNamespaceForTest(t, instanceID, false)

	//////////////////////
	// Start migration //
	////////////////////
	migrationName := fmt.Sprintf("%s-%s", instanceID, uuid.New())
	err = startAndValidateMigration(migrationName, cpName, []string{instanceID}, true)
	log.FailOnError(t, err, "failed to validate migration")

	////////////////////////////////
	// Cleanup created resources //
	//////////////////////////////
	if err = cleanUpClusterPairTestResources(cpName, defaultAdminNamespace); err != nil {
		Dash.Fatal("cluster pair resources cleanup failed, error=", err)
	}
}

// testStorkCtlClusterPairNFSSecretCreation tests if the values passed to the storkctl command are reflected
// in the secret created corresponding to the NFS backuplocation.
func testStorkCtlClusterPairNFSSecretCreation(t *testing.T) {
	var err error
	// Reset config in case of error
	defer func() {
		err = setSourceKubeConfig()
		log.FailOnError(t, err, "Error resetting source config")
	}()

	///////////////////////////
	// Clusterpair creation //
	/////////////////////////
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
		"nfs-mount-opts":      "hard,timeo=10",
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
	if !strings.Contains(actualOutput, fmt.Sprintf("Cluster pair %s created successfully. Direction: Destination -> Source", cpName)) {
		Dash.Fatal("cluster pair creation failed, expected success got=%s", actualOutput)
	}

	/////////////////////////////////////////////////////
	// Validate the secret created for backuplocation //
	///////////////////////////////////////////////////
	secretData, err := core.Instance().GetSecret(cpName, defaultAdminNamespace)
	log.FailOnError(t, err, "failed to get backuplocation secret")
	Dash.VerifyFatal(t, string(secretData.Data["nfsIOTimeoutInSecs"]), "10", "invalid timeout seconds value")
	Dash.VerifyFatal(t, string(secretData.Data["mountOptions"]), "hard,timeo=10", "invalid mount option")

	///////////////////////////////////////
	// Deploy app for running migration //
	//////////////////////////////////////
	err = setSourceKubeConfig()
	log.FailOnError(t, err, "Error resetting source config")
	instanceID := "nfs-migration-cp-test"
	appKey := "mysql-1-pvc"

	// Schedule mysql replicas.
	ctxs, err := schedulerDriver.Schedule(instanceID,
		scheduler.ScheduleOptions{AppKeys: []string{appKey}, Namespace: instanceID})
	log.FailOnError(t, err, "Error scheduling task")
	Dash.VerifyFatal(t, len(ctxs), 1, "Only one task should have started")

	err = schedulerDriver.WaitForRunning(ctxs[0], defaultWaitTimeout, defaultWaitInterval)
	log.FailOnError(t, err, "Error waiting for app to get to running state")
	defer blowNamespaceForTest(t, instanceID, false)

	//////////////////////
	// Start migration //
	////////////////////
	migrationName := fmt.Sprintf("%s-%s", instanceID, uuid.New())
	err = startAndValidateMigration(migrationName, cpName, []string{instanceID}, true)
	log.FailOnError(t, err, "failed to validate migration")

	////////////////////////////////
	// Cleanup created resources //
	//////////////////////////////
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

// startAndValidateMigration starts the migration and validates the status if the migration was successful or not.
func startAndValidateMigration(migrationName, cpName string, namespaces []string, expectSuccess bool) (err error) {
	ns := strings.Join(namespaces, ",")
	cmdFlags := map[string]string{
		"namespace":   defaultAdminNamespace,
		"clusterPair": cpName,
		"namespaces":  ns,
	}

	factory := storkctl.NewFactory()
	var outputBuffer bytes.Buffer
	cmd := storkctl.NewCommand(factory, os.Stdin, &outputBuffer, os.Stderr)
	cmdArgs := []string{"create", "migration", migrationName}
	// add the custom args to the command
	for key, value := range cmdFlags {
		cmdArgs = append(cmdArgs, "--"+key)
		if value != "" {
			cmdArgs = append(cmdArgs, value)
		}
	}
	cmd.SetArgs(cmdArgs)
	// execute the command
	log.InfoD("The storkctl command being executed is %v", cmdArgs)
	if err = cmd.Execute(); err != nil {
		err = fmt.Errorf("Storkctl execution failed: %v", err)
		return err
	}
	// Get the captured output as a string
	actualOutput := outputBuffer.String()
	log.InfoD("Actual output is: %s", actualOutput)
	expectedOutput := fmt.Sprintf("Migration %v created successfully\n", migrationName)
	if actualOutput != expectedOutput {
		return fmt.Errorf("Output mismatch,expected=%s,got=%s", expectedOutput, actualOutput)
	}

	// Check the status of the migration.
	if err = stork.Instance().ValidateMigration(migrationName, defaultAdminNamespace, defaultWaitTimeout, defaultWaitInterval); err != nil && expectSuccess {
		return fmt.Errorf("failed to validate migration %s,error=%v", migrationName, err)
	}

	// Cleanup the created migration.
	if err = deleteAndwaitForMigrationDeletion(migrationName, defaultAdminNamespace, migrationRetryTimeout); err != nil {
		return fmt.Errorf("failed to delete migration %s,error=%v", migrationName, err)
	}
	return
}
