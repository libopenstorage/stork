package storkctl

import (
	"fmt"

	"github.com/libopenstorage/stork/pkg/version"
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
)

func newVersionCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	versionCommands := &cobra.Command{
		Use:   "version",
		Short: "Print the version of storkctl",
		Run: func(cmd *cobra.Command, args []string) {
			_, err := fmt.Fprintf(ioStreams.Out, "Version: %v\n", version.Version)
			if err != nil {
				panic("Failed to print: " + err.Error())
			}
		},
	}
	return versionCommands
}
