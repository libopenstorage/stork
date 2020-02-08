package storkctl

import (
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

func newResumeCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	resumeCommands := &cobra.Command{
		Use:   "resume",
		Short: "Resume schedules",
	}

	resumeCommands.AddCommand(
		newResumeMigrationSchedulesCommand(cmdFactory, ioStreams),
		newResumeSnapshotSchedulesCommand(cmdFactory, ioStreams),
		newResumeApplicationBackupSchedulesCommand(cmdFactory, ioStreams),
	)

	return resumeCommands
}
