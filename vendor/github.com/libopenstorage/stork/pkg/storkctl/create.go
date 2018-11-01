package storkctl

import (
	"github.com/spf13/cobra"
)

func newCreateCommand(cmdFactory Factory) *cobra.Command {
	createCommands := &cobra.Command{
		Use:   "create",
		Short: "Create stork resources",
	}

	createCommands.AddCommand(
		newCreateSnapshotCommand(cmdFactory),
		newCreateMigrationCommand(cmdFactory),
		newCreatePVCCommand(cmdFactory),
	)

	return createCommands
}
