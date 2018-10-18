package storkctl

import (
	"io"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	cmdutil "k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
	"k8s.io/kubernetes/pkg/printers"
)

func newGetCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	getCommands := &cobra.Command{
		Use:   "get",
		Short: "Get stork resources",
	}
	getCommands.AddCommand(
		newGetSnapshotCommand(cmdFactory, ioStreams),
		newGetMigrationCommand(cmdFactory, ioStreams),
		newGetClusterPairCommand(cmdFactory, ioStreams),
	)

	return getCommands
}

func printTable(cmd *cobra.Command, object runtime.Object, columns []string, printerFunc interface{}, out io.Writer) error {
	printer := printers.NewHumanReadablePrinter(nil, nil, printers.PrintOptions{})
	if err := printer.Handler(columns, nil, printerFunc); err != nil {
		return err
	}
	return printer.PrintObj(object, out)
}

func printEncoded(cmd *cobra.Command, object runtime.Object, outputFormat string, out io.Writer) error {
	// For encoded outputs, if there is just one object, don't print it as
	// a list. Extract the object and just use that.
	if meta.IsListType(object) {

		objects, err := meta.ExtractList(object)
		if err != nil {
			return err
		}
		if len(objects) == 1 {
			object = objects[0]
		}
	}
	printer, err := cmdutil.PrinterForOptions(&printers.PrintOptions{
		OutputFormatType: outputFormat,
	})
	if err != nil {
		return err
	}
	return printer.PrintObj(object, out)
}

func printObjects(cmd *cobra.Command, object runtime.Object, outputFormat string, columns []string, printerFunc interface{}, out io.Writer) error {
	if outputFormat == outputFormatTable {
		return printTable(cmd, object, columns, printerFunc, out)
	}
	return printEncoded(cmd, object, outputFormat, out)
}
