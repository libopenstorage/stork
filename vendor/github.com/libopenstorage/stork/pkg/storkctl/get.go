package storkctl

import (
	"io"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/printers"
	kprinters "k8s.io/kubernetes/pkg/printers"
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
		newGetGroupVolumeSnapshotCommand(cmdFactory, ioStreams),
		newGetClusterDomainsStatusCommand(cmdFactory, ioStreams),
		newGetClusterDomainUpdateCommand(cmdFactory, ioStreams),
		newGetVolumeSnapshotRestoreCommand(cmdFactory, ioStreams),
		newGetApplicationBackupCommand(cmdFactory, ioStreams),
		newGetApplicationBackupScheduleCommand(cmdFactory, ioStreams),
		newGetApplicationRestoreCommand(cmdFactory, ioStreams),
		newGetApplicationCloneCommand(cmdFactory, ioStreams),
		newGetBackupLocationCommand(cmdFactory, ioStreams),
		newGetapplicationRegistrationCommand(cmdFactory, ioStreams),
	)

	return getCommands
}

func getRow(object runtime.Object, cells []interface{}) metav1beta1.TableRow {
	return metav1beta1.TableRow{
		Cells: cells,
		Object: runtime.RawExtension{
			Object: object.DeepCopyObject(),
		},
	}
}

func printTable(
	cmd *cobra.Command,
	object runtime.Object,
	columns []string,
	withNamespace bool,
	printerFunc interface{},
	out io.Writer,
) error {

	columnDefinitions := make([]metav1beta1.TableColumnDefinition, 0)
	for _, c := range columns {
		columnDefinitions = append(columnDefinitions, metav1beta1.TableColumnDefinition{
			Name: c,
		})
	}
	generator := kprinters.NewTableGenerator()
	err := generator.TableHandler(columnDefinitions, printerFunc)
	if err != nil {
		return err
	}
	table, err := generator.GenerateTable(object, kprinters.GenerateOptions{
		NoHeaders: true,
	})
	if err != nil {
		return err
	}

	printer := printers.NewTablePrinter(printers.PrintOptions{
		ColumnLabels:  columns,
		WithNamespace: withNamespace,
	})
	return printer.PrintObj(table, out)
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
