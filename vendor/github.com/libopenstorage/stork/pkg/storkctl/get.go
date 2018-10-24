package storkctl

import (
	"os"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	cmdutil "k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/printers"
)

func newGetCommand(cmdFactory Factory) *cobra.Command {
	getCommands := &cobra.Command{
		Use:   "get",
		Short: "Get stork resources",
	}
	getCommands.AddCommand(
		newGetSnapshotCommand(cmdFactory),
		newGetMigrationCommand(cmdFactory),
		newGetClusterPairCommand(cmdFactory),
	)

	return getCommands
}

func printTable(cmd *cobra.Command, object runtime.Object, columns []string, printerFunc interface{}) error {
	printer := printers.NewHumanReadablePrinter(nil, nil, printers.PrintOptions{})
	if err := printer.Handler(columns, nil, printerFunc); err != nil {
		return err
	}
	return printer.PrintObj(object, os.Stdout)
}

func printEncoded(cmd *cobra.Command, object runtime.Object, outputFormat string) error {
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
	return printer.PrintObj(object, os.Stdout)
}

func printObjects(cmd *cobra.Command, object runtime.Object, outputFormat string, columns []string, printerFunc interface{}) error {
	if outputFormat == outputFormatTable {
		return printTable(cmd, object, columns, printerFunc)
	}
	return printEncoded(cmd, object, outputFormat)
}
