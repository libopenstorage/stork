package storkctl

import (
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

func newDeactivateCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	deactivateCommands := &cobra.Command{
		Use:   "deactivate",
		Short: "Deactivate resources",
	}

	deactivateCommands.AddCommand(
		newDeactivateMigrationsCommand(cmdFactory, ioStreams),
		newDeactivateClusterDomainCommand(cmdFactory, ioStreams),
	)

	return deactivateCommands
}
