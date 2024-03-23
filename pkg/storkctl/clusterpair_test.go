//go:build unittest
// +build unittest

package storkctl

import (
	"fmt"
	"os"
	"testing"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func createClusterPairAndVerify(t *testing.T, name string, namespace string) {
	options := map[string]string{
		"option1": "value1",
		"option2": "value2",
	}
	clusterPair := &storkv1.ClusterPair{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},

		Spec: storkv1.ClusterPairSpec{
			Options: options,
		},
	}

	_, err := storkops.Instance().CreateClusterPair(clusterPair)
	require.NoError(t, err, "Error creating Clusterpair")
	// Make sure it was created correctly
	clusterPair, err = storkops.Instance().GetClusterPair(name, namespace)
	require.NoError(t, err, "Error getting Clusterpair")
	require.Equal(t, name, clusterPair.Name, "Clusterpair name mismatch")
	require.Equal(t, namespace, clusterPair.Namespace, "Clusterpair namespace mismatch")
	require.Equal(t, options, clusterPair.Spec.Options, "Clusterpair options mismatch")
}

func TestGetClusterPairNoPair(t *testing.T) {
	cmdArgs := []string{"get", "clusterpair"}

	expected := "No resources found.\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetClusterPairNotFound(t *testing.T) {
	defer resetTest()
	createClusterPairAndVerify(t, "pair2", "test")
	cmdArgs := []string{"get", "clusterpair", "pair1", "-n", "test"}

	expected := `Error from server (NotFound): clusterpairs.stork.libopenstorage.org "pair1" not found`
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestGetClusterPair(t *testing.T) {
	defer resetTest()

	_, err := core.Instance().CreateNamespace(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test"}})
	require.NoError(t, err, "Error creating test namespace")
	_, err = core.Instance().CreateNamespace(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "test1"}})
	require.NoError(t, err, "Error creating test1 namespace")

	createClusterPairAndVerify(t, "getclusterpairtest", "test")

	cmdArgs := []string{"get", "clusterpair", "getclusterpairtest", "-n", "test"}
	expected := "NAME                 STORAGE-STATUS   SCHEDULER-STATUS   CREATED\n" +
		"getclusterpairtest                                       \n"
	testCommon(t, cmdArgs, nil, expected, false)

	createClusterPairAndVerify(t, "getclusterpairtest2", "test")
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "clusterpair", "getclusterpairtest", "getclusterpairtest2", "-n", "test"}
	expected = "NAME                  STORAGE-STATUS   SCHEDULER-STATUS   CREATED\n" +
		"getclusterpairtest                                        \n" +
		"getclusterpairtest2                                       \n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "clusterpair", "-n", "test"}
	testCommon(t, cmdArgs, nil, expected, false)

	createClusterPairAndVerify(t, "getclusterpairtest", "test1")
	cmdArgs = []string{"get", "clusterpair", "-n", "test1"}
	expected = "NAME                 STORAGE-STATUS   SCHEDULER-STATUS   CREATED\n" +
		"getclusterpairtest                                       \n"
	testCommon(t, cmdArgs, nil, expected, false)

	cmdArgs = []string{"get", "clusterpair", "--all-namespaces"}
	expected = "NAMESPACE   NAME                  STORAGE-STATUS   SCHEDULER-STATUS   CREATED\n" +
		"test        getclusterpairtest                                        \n" +
		"test        getclusterpairtest2                                       \n" +
		"test1       getclusterpairtest                                        \n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGetClusterPairsWithStatus(t *testing.T) {
	defer resetTest()
	createClusterPairAndVerify(t, "clusterpairstatustest", "default")
	clusterPair, err := storkops.Instance().GetClusterPair("clusterpairstatustest", "default")
	require.NoError(t, err, "Error getting Clusterpair")
	clusterPair.CreationTimestamp = metav1.Now()
	clusterPair.Status.StorageStatus = storkv1.ClusterPairStatusReady
	clusterPair.Status.SchedulerStatus = storkv1.ClusterPairStatusReady
	_, err = storkops.Instance().UpdateClusterPair(clusterPair)
	require.NoError(t, err, "Error updating Clusterpair")
	cmdArgs := []string{"get", "clusterpair", "clusterpairstatustest"}
	expected := "NAME                    STORAGE-STATUS   SCHEDULER-STATUS   CREATED\n" +
		"clusterpairstatustest   Ready            Ready              " + toTimeString(clusterPair.CreationTimestamp.Time) + "\n"
	testCommon(t, cmdArgs, nil, expected, false)
}

func TestGenerateClusterPairInvalidName(t *testing.T) {
	cmdArgs := []string{"generate", "clusterpair", "pair_test", "-n", "test"}

	expected := "error: the Name \"pair_test\" is not valid: [a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')]"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestGenerateClusterPairInvalidNamespace(t *testing.T) {
	cmdArgs := []string{"generate", "clusterpair", "pair1", "-n", "test_namespace"}

	expected := "error: the Namespace \"test_namespace\" is not valid: [a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')]"
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestCreateUniDirectionalClusterPairMissingParameters(t *testing.T) {
	cmdArgs := []string{"create", "clusterpair", "uni-pair1", "-n", "test", "--unidirectional"}
	expected := "error: missing parameter \"src-kube-file\" - Kubeconfig file missing for source cluster"
	testCommon(t, cmdArgs, nil, expected, true)

	srcConfig := createTempFile(t, "src.config", "source")
	destConfig := createTempFile(t, "dest.config", "destination")
	defer os.Remove(srcConfig.Name())
	defer os.Remove(destConfig.Name())

	cmdArgs = []string{"create", "clusterpair", "uni-pair1", "-n", "test", "--src-kube-file", srcConfig.Name(), "--unidirectional"}
	expected = "error: missing parameter \"dest-kube-file\" - Kubeconfig file missing for destination cluster"
	testCommon(t, cmdArgs, nil, expected, true)

	cmdArgs = []string{"create", "clusterpair", "uni-pair1", "-n", "test", "--src-kube-file", srcConfig.Name(), "--dest-kube-file", srcConfig.Name(), "-u"}
	expected = "error: source kubeconfig file and destination kubeconfig file should be different"
	testCommon(t, cmdArgs, nil, expected, true)

	srcConfigDuplicate := createTempFile(t, "srcDup.config", "source")
	defer os.Remove(srcConfigDuplicate.Name())

	cmdArgs = []string{"create", "clusterpair", "uni-pair1", "-n", "test", "--src-kube-file", srcConfig.Name(), "--dest-kube-file", srcConfigDuplicate.Name(), "-u"}
	expected = "error: source kubeconfig and destination kubeconfig file should be different"
	testCommon(t, cmdArgs, nil, expected, true)

	cmdArgs = []string{"create", "clusterpair", "uni-pair1", "-n", "test", "--src-kube-file", srcConfig.Name(), "--dest-kube-file", srcConfigDuplicate.Name(), "-u", "--mode", "xyz"}
	expected = fmt.Sprintf("error: invalid mode %s, mode value should either be %s, %s or %s", "xyz", userInputCPModeAsync, userInputCPModeSync, userInputCPModeMigration)
	testCommon(t, cmdArgs, nil, expected, true)
}

func TestGetHostPortFromEndPoint(t *testing.T) {
	ep := "http://px-ep1.com"
	host, port, err := getHostPortFromEndPoint(ep)
	require.NoError(t, err, fmt.Sprintf("hitting error while getting host and port for endpoint %s", ep))
	require.Equal(t, "http://px-ep1.com", host)
	require.Equal(t, "80", port)

	ep = "https://px-ep1.com"
	host, port, err = getHostPortFromEndPoint(ep)
	require.NoError(t, err, fmt.Sprintf("hitting error while getting host and port for endpoint %s", ep))
	require.Equal(t, "https://px-ep1.com", host)
	require.Equal(t, "443", port)

	ep = "http://px-ep1.com:10000"
	host, port, err = getHostPortFromEndPoint(ep)
	require.NoError(t, err, fmt.Sprintf("hitting error while getting host and port for endpoint %s", ep))
	require.Equal(t, "http://px-ep1.com", host)
	require.Equal(t, "10000", port)

	ep = "https://px-ep1.com:10001"
	host, port, err = getHostPortFromEndPoint(ep)
	require.NoError(t, err, fmt.Sprintf("hitting error while getting host and port for endpoint %s", ep))
	require.Equal(t, "https://px-ep1.com", host)
	require.Equal(t, "10001", port)

	ep = "px-ep1.com"
	host, port, err = getHostPortFromEndPoint(ep)
	require.NoError(t, err, fmt.Sprintf("hitting error while getting host and port for endpoint %s", ep))
	require.Equal(t, "px-ep1.com", host)
	require.Equal(t, "80", port)

	ep = "px-ep1.com:500"
	host, port, err = getHostPortFromEndPoint(ep)
	require.NoError(t, err, fmt.Sprintf("hitting error while getting host and port for endpoint %s", ep))
	require.Equal(t, "px-ep1.com", host)
	require.Equal(t, "500", port)

	ep = "10.123.146.176"
	_, _, err = getHostPortFromEndPoint(ep)
	require.Error(t, err, fmt.Sprintf("Received unexpected error:\nport is needed along with ip address %s", ep))

	ep = "10.123.146.176:9001"
	host, port, err = getHostPortFromEndPoint(ep)
	require.NoError(t, err, fmt.Sprintf("hitting error while getting host and port for endpoint %s", ep))
	require.Equal(t, "10.123.146.176", host)
	require.Equal(t, "9001", port)

	ep = "htt://px-ep1.com"
	_, _, err = getHostPortFromEndPoint(ep)
	require.Error(t, err, "Received unexpected error:\ninvalid url scheme px-ep1.com, expected http or https only")

	ep = "htt://px-ep1.com:80"
	_, _, err = getHostPortFromEndPoint(ep)
	require.Error(t, err, "Received unexpected error:\ninvalid url scheme px-ep1.com, expected http or https only")
}

func createTempFile(t *testing.T, fileName string, fileContent string) *os.File {
	f, err := os.CreateTemp("", fileName)
	require.NoError(t, err, "Error creating file %s", fileName)
	if _, err := f.Write([]byte(fileContent)); err != nil {
		require.NoError(t, err, "Error writing to file %s", fileName)
	}
	if err := f.Close(); err != nil {
		require.NoError(t, err, "Error closing the file %s", fileName)
	}
	return f
}

func TestGenerateClusterPairForDifferentModes(t *testing.T) {
	// a fake k8sconfig content
	text := `
    clusters:
    - cluster:
      certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUN5RENDQWJDZ0F3SUJBZ0lCQURBTkJna3Foa2lHOXcwQkFRc0ZBREFWTVJNd0VRWURWUVFERXdwcmRXSmwKY201bGRHVnpNQjRYRFRJd01EUXhNekV4TkRBeU1Wb1hEVE13TURReE56RXhOREF5TVZvd0ZURVRNQkVHQTFVRQpBeE1LYTNWaVpYSnVaWFJsY3pDQ0FTSXdEUVlKS29aSWh2Y05BUUVCQlFBRGdnRVBBRENDQVFvQ2dnRUJBTmt0ClZWWGdXZ1E0eHJyNjJzUzV0N2dRM2Q4aXY4TmF3V2JHck8yTmZPeDRxMjNrd0RiMjVxK2NCVnJoN2txR1FsR2UKV2t2S1hZWDBoZz09LS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=
      server: https://1.2.3.4
    name: fake-cluster
    contexts:
    - context:
      cluster: fake-cluster
      user: fake-user
    name: fake-context
    current-context: fake-context
    kind: Config
    preferences: {}
    users:
    - name: fake-user
    user:
      client-certificate-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUN5RENDQWJDZ0F3SUJBZ0lCQURBTkJna3Foa2lHOXcwQkFRc0ZBREFWTVJNd0VRWURWUVFERXdwcmRXSmwKY201bGRHVnpNQjRYRFRJd01EUXhNekV4TkRBeU1Wb1hEVE13TURReE56RXhOREF5TVZvd0ZURVRNQkVHQTFVRQpBeE1LYTNWaVpYSnVaWFJsY3pDQ0FTSXdEUVlKS29aSWh2Y05BUUVCQlFBRGdnRVBBRENDQVFvQ2dnRUJBTmt0ClZWWGdXZ1E0eHJyNjJzUzV0N2dRM2Q4aXY4TmF3V2JHck8yTmZPeDRxMjNrd0RiMjVxK2NCVnJoN2txR1FsR2UKV2t2S1hZWDBoZz09LS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=
      client-key-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FUR
    `
	// create a temp file with k8s config content
	srcConfig := createTempFile(t, "src.config", text)
	defer os.Remove(srcConfig.Name())

	name := "testClusterPair"
	ns := "testNamespace"
	ip := "127.0.0.1"
	port := "8080"
	token := "abcd1234"
	configFile := srcConfig.Name()
	projectIDMappings := ""
	authSecretNamespace := "authNamespace"
	reverse := true
	ignoreStorageOptions := false

	mode := userInputCPModeAsync
	clusterPair, err := generateClusterPair(name, ns, ip, port, token, configFile, projectIDMappings, authSecretNamespace, reverse, mode, ignoreStorageOptions)
	require.NoError(t, err, "Error generating ClusterPair")

	// Verify the generated ClusterPair object
	require.Equal(t, clusterPairDRModeDisasterRecovery, clusterPair.Spec.Options["mode"], "ClusterPair mode mismatch")

	mode = userInputCPModeSync
	ignoreStorageOptions = true
	clusterPair, err = generateClusterPair(name, ns, ip, port, token, configFile, projectIDMappings, authSecretNamespace, reverse, mode, ignoreStorageOptions)
	require.NoError(t, err, "Error generating ClusterPair")

	// Verify the generated ClusterPair object
	require.Equal(t, 0, len(clusterPair.Spec.Options), "ClusterPair mode mismatch")

	mode = userInputCPModeMigration
	ignoreStorageOptions = false
	clusterPair, err = generateClusterPair(name, ns, ip, port, token, configFile, projectIDMappings, authSecretNamespace, reverse, mode, ignoreStorageOptions)
	require.NoError(t, err, "Error generating ClusterPair")

	// Verify the generated ClusterPair object
	require.Equal(t, clusterPairDRModeOnetimeMigration, clusterPair.Spec.Options["mode"], "ClusterPair mode mismatch")
}
