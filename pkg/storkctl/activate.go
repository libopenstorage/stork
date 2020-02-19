package storkctl

import (
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

func newActivateCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	activateCommands := &cobra.Command{
		Use:   "activate",
		Short: "Activate resources",
	}

	activateCommands.AddCommand(
		newActivateMigrationsCommand(cmdFactory, ioStreams),
		newActivateClusterDomainCommand(cmdFactory, ioStreams),
	)

	return activateCommands
}
