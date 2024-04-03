//go:build integrationtest
// +build integrationtest

package integrationtest

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
)

const (
	clusterPairTestDir = "clusterpair-test"
	cpNamespace        = "automation-clusterpair-ns"
	defaultOptionPort  = "9001"
	disableSSL         = "disableSSL"
	ip                 = "ip"
	port               = "port"
	token              = "token"
	CPbackuplocation   = "backuplocation"
)

func TestStorkCtlClusterPair(t *testing.T) {
	err := setSourceKubeConfig()
	require.NoError(t, err, "Failed to set kubeconfig to source cluster: %v", err)
	currentTestSuite = t.Name()
	t.Run("createClusterPairWithEncKeydisableSSLTest", createClusterPairWithEncKeydisableSSLTest)
	t.Run("createClusterPairWithoutEncKeydisableSSLTest", createClusterPairWithoutEncKeydisableSSLTest)
	t.Run("createClusterPairWithEpTest", createClusterPairWithEpTest)
	t.Run("createClusterPairSyncDrTest", createClusterPairSyncDrTest)
}

func createClusterPairWithEncKeydisableSSLTest(t *testing.T) {
	var testrailID, testResult = 297068, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	expectedBlData := map[string]interface{}{
		disableSSL:      true,
		"encryptionKey": "testKey",
	}
	cpName := validateBlData(t, expectedBlData, nil)
	logrus.Infof("CP is %v", cpName)
	expectedCPOptionData := map[string]string{
		"backuplocation": cpName,
		"port":           defaultOptionPort,
		"ip":             "",
		"token":          "",
	}
	validateClusterPairOptions(t, cpName, expectedCPOptionData, false)
	cleanupNamespaces()
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func createClusterPairWithoutEncKeydisableSSLTest(t *testing.T) {
	var testrailID, testResult = 297069, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	expectedBlData := map[string]interface{}{
		disableSSL:      false,
		"encryptionKey": "",
	}
	cpName := validateBlData(t, expectedBlData, nil)
	logrus.Infof("CP is %v", cpName)
	expectedCPOptionData := map[string]string{
		"backuplocation": cpName,
		"port":           defaultOptionPort,
		"ip":             "",
		"token":          "",
	}
	validateClusterPairOptions(t, cpName, expectedCPOptionData, false)
	cleanupNamespaces()
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func createClusterPairWithEpTest(t *testing.T) {
	var testrailID, testResult = 297070, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	srcIps := getNodeIps(t, true)
	err := setDestinationKubeConfig()
	require.NoError(t, err, "failed to set destination kubeconfig: %v", err)
	destIps := getNodeIps(t, false)
	extraAgrs := map[string]string{
		"src-ep":  fmt.Sprintf("%s:%s", srcIps[0], defaultOptionPort),
		"dest-ep": fmt.Sprintf("%s:%s", destIps[0], defaultOptionPort),
	}
	expectedBlData := map[string]interface{}{
		disableSSL:      true,
		"encryptionKey": "testKey",
	}
	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set source kubeconfig: %v", err)
	cpName := validateBlData(t, expectedBlData, extraAgrs)
	logrus.Infof("CP is %v", cpName)
	expectedCPOptionDataSrc := map[string]string{
		"backuplocation": cpName,
		"port":           defaultOptionPort,
		"ip":             destIps[0],
		"token":          "",
	}
	validateClusterPairOptions(t, cpName, expectedCPOptionDataSrc, false)
	err = setDestinationKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig: %v", err)
	expectedCPOptionDataDest := map[string]string{
		"backuplocation": cpName,
		"port":           defaultOptionPort,
		"ip":             srcIps[0],
		"token":          "",
	}
	validateClusterPairOptions(t, cpName, expectedCPOptionDataDest, true)
	cleanupNamespaces()
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func createClusterPairSyncDrTest(t *testing.T) {
	var testrailID, testResult = 297071, testResultFail
	runID := testrailSetupForTest(testrailID, &testResult)
	defer updateTestRail(&testResult, testrailID, runID)
	defer updateDashStats(t.Name(), &testResult)

	extraAgrs := map[string]string{
		"mode": "sync-dr",
	}
	cpName := validateBlData(t, nil, extraAgrs)
	logrus.Infof("CP is %v", cpName)
	validateClusterPairOptions(t, cpName, nil, false)
	cleanupNamespaces()
	testResult = testResultPass
	logrus.Infof("Test status at end of %s test: %s", t.Name(), testResult)
}

func updateSecretData(t *testing.T, secretName string, requiredMap map[string]interface{}) {
	secret, err := core.Instance().GetSecret(secretName, "default")
	require.NoError(t, err, "Error getting secret data")
	for key, newValue := range requiredMap {
		var newValueByte []byte
		switch val := newValue.(type) {
		case string:
			newValueByte = []byte(val)
		case bool:
			newValueByte = []byte(strconv.FormatBool(val))
		default:
			fmt.Printf("Key: %s, Value: %v\n", key, val)
		}
		secret.Data[key] = newValueByte
	}
	_, err = core.Instance().UpdateSecretData(secretName, "default", secret.Data)
}

func validateBlData(t *testing.T, expectedData map[string]interface{}, extraAgrs map[string]string) string {
	cpName := fmt.Sprintf("automation-cluster-pair-%v", time.Now().Format("2006-01-02-150405"))
	if expectedData == nil {
		err := scheduleBidirectionalClusterPair(cpName, cpNamespace, "", "", "", extraAgrs)
		require.NoError(t, err, "failed to create clusterpair: %v", err)
		err = setSourceKubeConfig()
		require.NoError(t, err, "failed to set source kubeconfig: %v", err)
		_, err = storkops.Instance().GetBackupLocation(cpName, cpNamespace)
		require.Error(t, err, "Backuplocation should not be created")
	} else {
		configMap, err := core.Instance().GetConfigMap("secret-config", "default")
		if err != nil {
			require.NoError(t, err, "Error getting configmap secret-config in defaule namespace: %v")
		}
		cmData := configMap.Data
		for location, secret := range cmData {
			logrus.Infof("Creating a cluster-pair using %v as objectstore.", location)
			updateSecretData(t, secret, expectedData)
			err := scheduleBidirectionalClusterPair(cpName, cpNamespace, "", storkv1.BackupLocationType(location), secret, extraAgrs)
			require.NoError(t, err, "failed to create clusterpair: %v", err)
			err = setSourceKubeConfig()
			require.NoError(t, err, "failed to set source kubeconfig: %v", err)
			bl, err := storkops.Instance().GetBackupLocation(cpName, cpNamespace)
			require.NoError(t, err, "Not able to get backup Location for %v clusterpair", cpName)
			for key, value := range expectedData {
				switch key {
				case "encryptionKey":
					logrus.Infof("EncryptionKey is %v", bl.Location.EncryptionV2Key)
					require.Equal(t, value, bl.Location.EncryptionV2Key)
				case disableSSL:
					logrus.Infof("SSL is %v", bl.Location.S3Config.DisableSSL)
					require.Equal(t, value, bl.Location.S3Config.DisableSSL)
				}
			}
		}
	}
	return cpName
}

func validateClusterPairOptions(t *testing.T, name string, expectedData map[string]string, reverse bool) {
	cp, err := storkops.Instance().GetClusterPair(name, cpNamespace)
	cpOptions := cp.Spec.Options
	require.NoError(t, err, "failed to get clusterpair: %v", err)
	if expectedData == nil {
		require.Equal(t, 0, len(cpOptions))
	} else {
		for key, value := range expectedData {
			switch key {
			case CPbackuplocation:
				logrus.Infof("backuplocation is %v", cpOptions[CPbackuplocation])
				require.Equal(t, value, cpOptions[CPbackuplocation])
			case port:
				logrus.Infof("port is %v", cpOptions[port])
				require.Equal(t, value, cpOptions[port])
			case token:
				_, exists := expectedData[key]
				require.Equal(t, exists, true)
			case ip:
				if key == ip {
					if value == "" {
						ips := getNodeIps(t, reverse)
						require.Contains(t, ips, cpOptions[ip])
					} else {
						logrus.Infof("ip is %v", cpOptions[ip])
						require.Equal(t, value, cpOptions[ip])
					}
				}
			}
		}
	}
}

func getNodeIps(t *testing.T, reverse bool) []string {
	err := setDestinationKubeConfig()
	if reverse {
		err = setSourceKubeConfig()
	}
	require.NoError(t, err, "failed to set kubeconfig: %v", err)
	var nodeIps []string
	pxNodes, err := core.Instance().GetNodes()
	require.NoError(t, err, "failed to get PX nodes: %v", err)
	for _, node := range pxNodes.Items {
		if !core.Instance().IsNodeMaster(node) {
			for _, addr := range node.Status.Addresses {
				if addr.Type == corev1.NodeExternalIP || addr.Type == corev1.NodeInternalIP {
					nodeIps = append(nodeIps, addr.Address)
				}
			}
		}
	}
	logrus.Infof("Node IPs are %v", nodeIps)
	err = setSourceKubeConfig()
	require.NoError(t, err, "failed to set kubeconfig to source cluster: %v", err)
	return nodeIps
}

func cleanupNamespaces() {
	err := core.Instance().DeleteNamespace(cpNamespace)
	if err != nil {
		logrus.Infof("Error deleting namespace %s: %v\n", cpNamespace, err)
	}
	err = setDestinationKubeConfig()
	if err != nil {
		logrus.Infof("Error setting kubeconfig on destination cluster")
	}
	err = core.Instance().DeleteNamespace(cpNamespace)
	if err != nil {
		logrus.Infof("Error deleting namespace %s: %v\n", cpNamespace, err)
	}
	err = setSourceKubeConfig()
	if err != nil {
		logrus.Infof("Error setting kubeconfig on source cluster")
	}
}
