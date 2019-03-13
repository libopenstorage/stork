package storkctl

import (
	"io"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
		newGetSchedulePolicyCommand(cmdFactory, ioStreams),
		newGetMigrationScheduleCommand(cmdFactory, ioStreams),
		newGetSnapshotScheduleCommand(cmdFactory, ioStreams),
	)

	return getCommands
}

func printTable(
	cmd *cobra.Command,
	object runtime.Object,
	columns []string,
	withNamespace bool,
	printerFunc interface{},
	out io.Writer,
) error {
	printer := printers.NewHumanReadablePrinter(nil, printers.PrintOptions{
		WithNamespace: withNamespace,
	})
	if err := printer.Handler(columns, nil, printerFunc); err != nil {
		return err
	}
	return printer.PrintObj(object, out)
}

func printEncoded(cmd *cobra.Command, object runtime.Object, outputFormat string, out io.Writer) error {
	if meta.IsListType(object) {
		object.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{
			Version: "v1",
			Kind:    "List",
		})
	}
	printer, err := (&genericclioptions.JSONYamlPrintFlags{}).ToPrinter(outputFormat)
	if err != nil {
		return err
	}
	return printer.PrintObj(object, out)
}

func printObjects(cmd *cobra.Command, object runtime.Object, cmdFactory Factory, columns []string, printerFunc interface{}, out io.Writer) error {
	outputFormat, err := cmdFactory.GetOutputFormat()
	if err != nil {
		return err
	}
	if outputFormat == outputFormatTable {
		return printTable(cmd, object, columns, cmdFactory.AllNamespaces(), printerFunc, out)
	}
	return printEncoded(cmd, object, outputFormat, out)
}
