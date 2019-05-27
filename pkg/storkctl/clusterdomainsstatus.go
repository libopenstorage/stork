package storkctl

import (
	"fmt"
	"io"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
	"k8s.io/kubernetes/pkg/printers"
)

var clusterDomainsStatusColumns = []string{"NAME", "LOCAL-DOMAIN", "ACTIVE", "INACTIVE", "CREATED"}
var clusterDomainsStatusSubcommand = "clusterdomainsstatus"
var clusterDomainsStatusAliases = []string{"cds"}

func newGetClusterDomainsStatusCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	getClusterDomainsStatusCommand := &cobra.Command{
		Use:     clusterDomainsStatusSubcommand,
		Aliases: clusterDomainsStatusAliases,
		Short:   "Get cluster domain statuses",
		Run: func(c *cobra.Command, args []string) {
			cdStatuses := new(storkv1.ClusterDomainsStatusList)
			var err error
			if len(args) > 0 {
				for _, clusterID := range args {
					cds, err := k8s.Instance().GetClusterDomainsStatus(clusterID)
					if err != nil {
						util.CheckErr(err)
						return
					}
					cdStatuses.Items = append(cdStatuses.Items, *cds)
				}
			} else {
				cdStatuses, err = k8s.Instance().ListClusterDomainStatuses()
				if err != nil {
					util.CheckErr(err)
					return
				}
			}

			if len(cdStatuses.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			if err := printObjects(c, cdStatuses, cmdFactory, clusterDomainsStatusColumns, clusterDomainsStatusPrinter, ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}
	cmdFactory.BindGetFlags(getClusterDomainsStatusCommand.Flags())
	return getClusterDomainsStatusCommand
}

func clusterDomainsStatusPrinter(cdsList *storkv1.ClusterDomainsStatusList, writer io.Writer, options printers.PrintOptions) error {
	if cdsList == nil {
		return nil
	}

	for _, cds := range cdsList.Items {
		name := printers.FormatResourceName(options.Kind, cds.Name, options.WithKind)

		// build a list of active and inactive domains
		var (
			active, inactive       string
			numActive, numInactive int
		)

		for _, cdsInfo := range cds.Status.ClusterDomainInfos {
			if cdsInfo.State == storkv1.ClusterDomainActive {
				if numActive > 0 {
					active = active + ", "
				}
				active = active + cdsInfo.Name + " (" + string(cdsInfo.SyncStatus) + ")"
				numActive++
			} else {
				if numInactive > 0 {
					inactive = inactive + ", "
				}
				inactive = inactive + cdsInfo.Name + " (" + string(cdsInfo.SyncStatus) + ")"
				numInactive++
			}
		}

		creationTime := toTimeString(cds.CreationTimestamp.Time)
		if _, err := fmt.Fprintf(writer, "%v\t%v\t[%v]\t[%v]\t%v\n",
			name,
			cds.Status.LocalDomain,
			active,
			inactive,
			creationTime,
		); err != nil {
			return err
		}
	}
	return nil
}
