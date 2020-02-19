package storkctl

import (
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/spf13/cobra"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/util"
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
					cds, err := storkops.Instance().GetClusterDomainsStatus(clusterID)
					if err != nil {
						util.CheckErr(err)
						return
					}
					cdStatuses.Items = append(cdStatuses.Items, *cds)
				}
			} else {
				cdStatuses, err = storkops.Instance().ListClusterDomainStatuses()
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

func clusterDomainsStatusPrinter(
	cdsList *storkv1.ClusterDomainsStatusList,
	options printers.GenerateOptions,
) ([]metav1beta1.TableRow, error) {
	if cdsList == nil {
		return nil, nil
	}

	rows := make([]metav1beta1.TableRow, 0)
	for _, cds := range cdsList.Items {

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
		row := getRow(&cds,
			[]interface{}{cds.Name,
				cds.Status.LocalDomain,
				active,
				inactive,
				creationTime},
		)
		rows = append(rows, row)

	}
	return rows, nil
}
