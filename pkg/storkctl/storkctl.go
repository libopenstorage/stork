package storkctl

import (
	"flag"
	"io"

	"github.com/portworx/sched-ops/k8s"
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
)

// NewCommand Create a new storkctl command
func NewCommand(in io.Reader, out io.Writer, errOut io.Writer) *cobra.Command {

	cmdFactory := NewFactory()
	cmds := &cobra.Command{
		Use:   "storkctl",
		Short: "storkctl manages stork resources",
		PersistentPreRun: func(c *cobra.Command, args []string) {
			config, err := cmdFactory.GetConfig()
			if err != nil {
				util.CheckErr(err)
				return
			}
			k8s.Instance().SetConfig(config)
		},
	}

	ioStreams := genericclioptions.IOStreams{In: in, Out: out, ErrOut: errOut}
	cmdFactory.BindFlags(cmds.PersistentFlags())

	cmds.AddCommand(
		newCreateCommand(cmdFactory, ioStreams),
		newDeleteCommand(cmdFactory, ioStreams),
		newGetCommand(cmdFactory, ioStreams),
		newGenerateCommand(cmdFactory, ioStreams),
		newVersionCommand(cmdFactory, ioStreams),
	)

	cmds.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		util.CheckErr(err)
		return nil
	}

	return cmds
}
