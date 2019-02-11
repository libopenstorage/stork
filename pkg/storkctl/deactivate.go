package storkctl

import (
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
)

func newDeactivateCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	deactivateCommands := &cobra.Command{
		Use:   "deactivate",
		Short: "Deactivate resources",
	}

	deactivateCommands.AddCommand(
		newDeactivateMigrationsCommand(cmdFactory, ioStreams),
	)

	return deactivateCommands
}
