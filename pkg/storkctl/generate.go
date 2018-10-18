package storkctl

import (
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
)

func newGenerateCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	generateCommands := &cobra.Command{
		Use:   "generate",
		Short: "Generate stork specs",
	}

	generateCommands.AddCommand(
		newGenerateClusterPairCommand(cmdFactory, ioStreams),
	)

	return generateCommands
}
