package storkctl

import (
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

func newUpdateCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	updateCommands := &cobra.Command{
		Use:   "update",
		Short: "Update stork resources",
	}
	updateCommands.AddCommand(
		newUpdateSnapShotScheduleCommand(cmdFactory, ioStreams),
	)
	return updateCommands
}
