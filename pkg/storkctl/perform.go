package storkctl

import (
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

func newPerformCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	performCommands := &cobra.Command{
		Use:   "perform",
		Short: "perform actions",
	}

	performCommands.AddCommand(
		newPerformFailoverCommand(cmdFactory, ioStreams),
		newPerformFailbackCommand(cmdFactory, ioStreams),
	)
	return performCommands
}
