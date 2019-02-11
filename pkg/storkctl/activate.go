package storkctl

import (
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
)

func newActivateCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	activateCommands := &cobra.Command{
		Use:   "activate",
		Short: "Activate resources",
	}

	activateCommands.AddCommand(
		newActivateMigrationsCommand(cmdFactory, ioStreams),
	)

	return activateCommands
}
