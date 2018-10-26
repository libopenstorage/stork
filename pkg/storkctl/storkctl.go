package storkctl

import (
	"flag"

	"github.com/portworx/sched-ops/k8s"
	"github.com/spf13/cobra"
)

// NewCommand Create a new storkctl command
func NewCommand() *cobra.Command {

	cmdFactory := NewFactory()
	cmds := &cobra.Command{
		Use:   "storkctl",
		Short: "storkctl manages stork resources",
		PersistentPreRun: func(c *cobra.Command, args []string) {
			if config, err := cmdFactory.GetConfig(); err != nil {
				handleError(err)
			} else {
				k8s.Instance().SetConfig(config)
			}
		},
	}

	cmdFactory.BindFlags(cmds.PersistentFlags())

	cmds.AddCommand(
		newCreateCommand(cmdFactory),
		newDeleteCommand(cmdFactory),
		newGetCommand(cmdFactory),
		newGenerateCommand(cmdFactory),
	)

	cmds.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		handleError(err)
	}

	return cmds
}
