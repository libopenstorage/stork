// +build unittest

package storkctl

import (
	"testing"

	"github.com/libopenstorage/stork/pkg/version"
	"github.com/stretchr/testify/require"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

func TestVersion(t *testing.T) {
	f := NewTestFactory()
	streams, _, buf, _ := genericclioptions.NewTestIOStreams()
	cmd := newVersionCommand(f, streams)
	cmd.SetOutput(buf)
	cmd.SetArgs([]string{"version"})
	err := cmd.Execute()
	require.NoError(t, err, "Error executing command: %v", cmd)
	require.Equal(t, "storkctl Version: "+version.Version+"\n", buf.String())
}
