package storkctl

import (
	"fmt"
	"io"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/pkg/printers"
)

var clusterPairColumns = []string{"NAME", "STORAGE-STATUS", "SCHEDULER-STATUS", "CREATED"}

func newGetClusterPairCommand(cmdFactory Factory) *cobra.Command {
	var err error
	getClusterPairCommand := &cobra.Command{
		Use:     "clusterpair",
		Aliases: []string{"cp"},
		Short:   "Get cluster pair resources",
		Run: func(c *cobra.Command, args []string) {
			var clusterPairs *storkv1.ClusterPairList
			if len(args) > 0 {
				clusterPairs = new(storkv1.ClusterPairList)
				for _, pairName := range args {
					pair, err := k8s.Instance().GetClusterPair(pairName)
					if err != nil {
						handleError(err)
					}
					clusterPairs.Items = append(clusterPairs.Items, *pair)
				}
			} else {
				clusterPairs, err = k8s.Instance().ListClusterPairs()
				if err != nil {
					handleError(err)
				}
			}

			if len(clusterPairs.Items) == 0 {
				handleEmptyList()
				return
			}

			outputFormat, err := cmdFactory.GetOutputFormat()
			if err != nil {
				handleError(err)
			}

			if err := printObjects(c, clusterPairs, outputFormat, clusterPairColumns, clusterPairPrinter); err != nil {
				handleError(err)
			}
		},
	}

	return getClusterPairCommand
}

func clusterPairPrinter(clusterPairList *storkv1.ClusterPairList, writer io.Writer, options printers.PrintOptions) error {
	if clusterPairList == nil {
		return nil
	}
	for _, clusterPair := range clusterPairList.Items {
		name := printers.FormatResourceName(options.Kind, clusterPair.Name, options.WithKind)

		creationTime := toTimeString(clusterPair.CreationTimestamp)
		if _, err := fmt.Fprintf(writer, "%v\t%v\t%v\t%v\n",
			name,
			clusterPair.Status.StorageStatus,
			clusterPair.Status.SchedulerStatus,
			creationTime); err != nil {
			return err
		}
	}
	return nil
}
