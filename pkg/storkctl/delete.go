package storkctl

import (
	"github.com/spf13/cobra"
)

func newDeleteCommand(cmdFactory Factory) *cobra.Command {
	deleteCommands := &cobra.Command{
		Use:   "delete",
		Short: "Delete stork resources",
	}

	deleteCommands.AddCommand(
		newDeleteSnapshotCommand(cmdFactory),
		newDeleteMigrationCommand(cmdFactory),
	)
	return deleteCommands
}
