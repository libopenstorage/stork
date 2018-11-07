package storkctl

import (
	"fmt"
	"io"
	"reflect"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/spf13/cobra"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
	"k8s.io/kubernetes/pkg/printers"
)

const (
	clusterPairSubcommand = "clusterpair"
)

var clusterPairColumns = []string{"NAME", "STORAGE-STATUS", "SCHEDULER-STATUS", "CREATED"}

func newGetClusterPairCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var err error
	getClusterPairCommand := &cobra.Command{
		Use:     clusterPairSubcommand,
		Aliases: []string{"cp"},
		Short:   "Get cluster pair resources",
		Run: func(c *cobra.Command, args []string) {
			var clusterPairs *storkv1.ClusterPairList
			if len(args) > 0 {
				clusterPairs = new(storkv1.ClusterPairList)
				for _, pairName := range args {
					pair, err := k8s.Instance().GetClusterPair(pairName)
					if err != nil {
						handleError(err, ioStreams.ErrOut)
					}
					clusterPairs.Items = append(clusterPairs.Items, *pair)
				}
			} else {
				clusterPairs, err = k8s.Instance().ListClusterPairs()
				if err != nil {
					handleError(err, ioStreams.ErrOut)
				}
			}

			if len(clusterPairs.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}

			outputFormat, err := cmdFactory.GetOutputFormat()
			if err != nil {
				handleError(err, ioStreams.ErrOut)
			}

			if err := printObjects(c, clusterPairs, outputFormat, clusterPairColumns, clusterPairPrinter, ioStreams.Out); err != nil {
				handleError(err, ioStreams.ErrOut)
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

func newGenerateClusterPairCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	generateClusterPairCommand := &cobra.Command{
		Use:   clusterPairSubcommand,
		Short: "Generate a spec to be used for cluster pairing from a remote cluster",
		Run: func(c *cobra.Command, args []string) {
			config, err := cmdFactory.RawConfig()
			if err != nil {
				handleError(err, ioStreams.ErrOut)
			}

			// Prune out all but the current-context and related
			// info
			currentContext := config.CurrentContext
			for context := range config.Contexts {
				if context != currentContext {
					delete(config.Contexts, context)
				}
			}
			currentCluster := config.Contexts[currentContext].Cluster
			for cluster := range config.Clusters {
				if cluster != currentCluster {
					delete(config.Clusters, cluster)
				}
			}
			currentAuthInfo := config.Contexts[currentContext].AuthInfo
			for authInfo := range config.AuthInfos {
				if authInfo != currentAuthInfo {
					delete(config.AuthInfos, authInfo)
				}
			}

			clusterPair := &storkv1.ClusterPair{
				TypeMeta: meta.TypeMeta{
					Kind:       reflect.TypeOf(storkv1.ClusterPair{}).Name(),
					APIVersion: storkv1.SchemeGroupVersion.String(),
				},
				ObjectMeta: meta.ObjectMeta{
					Name: "<insert_name_here>",
				},

				// Replace gloud paths in the config
				Spec: storkv1.ClusterPairSpec{
					Config: config,
					Options: map[string]string{
						"<insert_storage_options_here>": "",
					},
				},
			}
			if err = printEncoded(c, clusterPair, "yaml", ioStreams.Out); err != nil {
				handleError(err, ioStreams.ErrOut)
			}
		},
	}

	return generateClusterPairCommand
}
