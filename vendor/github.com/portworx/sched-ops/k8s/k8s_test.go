package k8s

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var testInstance Ops

func TestK8s(t *testing.T) {
	kubeconfig := os.Getenv("KUBECONFIG")
	if len(kubeconfig) == 0 {
		t.Skipf("KUBECONFIG not defined. Skipping tests")
	}

	testInstance = Instance()
	err := testInstance.initK8sClient()
	require.NoError(t, err, "initK8sClient returned error")

	t.Run("basicTests", basicTests)
}

func basicTests(t *testing.T) {
	ver, err := testInstance.GetVersion()
	require.NoError(t, err, "got error while fetching server version")
	require.NotNil(t, ver, "got nil server version")

	fmt.Printf("server version: major=%s minor:%s gitver:%s\n", ver.Major, ver.Minor, ver.GitVersion)
}
