package storkctl

import (
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
)

func newResumeCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	resumeCommands := &cobra.Command{
		Use:   "resume",
		Short: "Resume schedules",
	}

	resumeCommands.AddCommand(
		newResumeMigrationSchedulesCommand(cmdFactory, ioStreams),
		newResumeSnapshotSchedulesCommand(cmdFactory, ioStreams),
	)

	return resumeCommands
}
