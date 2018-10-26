package storkctl

import (
	"github.com/spf13/cobra"
)

func newGenerateCommand(cmdFactory Factory) *cobra.Command {
	generateCommands := &cobra.Command{
		Use:   "generate",
		Short: "Generate stork specs",
	}

	generateCommands.AddCommand(
		newGenerateClusterPairCommand(cmdFactory),
	)

	return generateCommands
}
