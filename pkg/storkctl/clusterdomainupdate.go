package storkctl

import (
	"fmt"
	"io"
	"strconv"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/pborman/uuid"
	"github.com/portworx/sched-ops/k8s"
	"github.com/spf13/cobra"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
	"k8s.io/kubernetes/pkg/printers"
)

var clusterDomainUpdateColumns = []string{"NAME", "CLUSTER-DOMAIN", "ACTION", "STATUS", "CREATED"}
var clusterDomainUpdateSubcommand = "clusterdomainupdate"
var clusterDomainUpdateAliases = []string{"cdu"}
var clusterDomainSubcommand = "clusterdomain"
var clusterDomainAliases = []string{"cd"}

func newActivateClusterDomainCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var allClusterDomains bool
	var nameClusterDomainUpdate string
	activateClusterDomainCommand := &cobra.Command{
		Use:     clusterDomainSubcommand,
		Aliases: clusterDomainAliases,
		Short:   "Activate a cluster domain",
		Run: func(c *cobra.Command, args []string) {
			activationList := []string{}
			if allClusterDomains {
				cdsList, err := k8s.Instance().ListClusterDomainStatuses()
				if err != nil {
					util.CheckErr(err)
					return
				}

				for _, cds := range cdsList.Items {
					activationList = append(activationList, cds.Status.Inactive...)
				}
			} else if len(args) == 1 {
				activationList = []string{args[0]}
			} else {
				util.CheckErr(fmt.Errorf("Exactly one cluster domain name needs to be provided to the activate command"))
				return
			}
			updateUUID := uuid.New()
			for i, clusterDomainName := range activationList {
				var name string
				if len(nameClusterDomainUpdate) > 0 {
					if len(activationList) > 1 {
						name = nameClusterDomainUpdate + "-" + strconv.FormatInt(int64(i), 10)
					} else {
						name = nameClusterDomainUpdate
					}
				} else {
					if len(activationList) > 1 {
						name = updateUUID + "-" + strconv.FormatInt(int64(i), 10)
					} else {
						name = updateUUID
					}
				}
				clusterDomainUpdate := &storkv1.ClusterDomainUpdate{
					ObjectMeta: meta.ObjectMeta{
						Name: name,
					},
					Spec: storkv1.ClusterDomainUpdateSpec{
						ClusterDomain: clusterDomainName,
						Active:        true,
					},
				}
				_, err := k8s.Instance().CreateClusterDomainUpdate(clusterDomainUpdate)
				if err != nil {
					util.CheckErr(fmt.Errorf("Failed to activate cluster domain %v: %v", clusterDomainName, err))
					return
				}
				msg := fmt.Sprintf("Cluster Domain %v activated successfully", clusterDomainName)
				printMsg(msg, ioStreams.Out)
			}
		},
	}
	activateClusterDomainCommand.Flags().BoolVarP(&allClusterDomains, "all", "a", false, "Activate all inactive cluster domains")
	activateClusterDomainCommand.Flags().StringVar(&nameClusterDomainUpdate, "name", "", "Name for the activate cluster domain action")

	return activateClusterDomainCommand
}

func newDeactivateClusterDomainCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var nameClusterDomainUpdate string
	deactivateClusterDomainCommand := &cobra.Command{
		Use:     clusterDomainSubcommand,
		Aliases: clusterDomainAliases,
		Short:   "Deactivate a cluster domain",
		Run: func(c *cobra.Command, args []string) {
			if len(args) == 1 {
				name := uuid.New()
				if len(nameClusterDomainUpdate) > 0 {
					name = nameClusterDomainUpdate
				}
				clusterDomainName := args[0]
				clusterDomainUpdate := &storkv1.ClusterDomainUpdate{
					ObjectMeta: meta.ObjectMeta{
						Name: name,
					},
					Spec: storkv1.ClusterDomainUpdateSpec{
						ClusterDomain: clusterDomainName,
						Active:        false,
					},
				}
				_, err := k8s.Instance().CreateClusterDomainUpdate(clusterDomainUpdate)
				if err != nil {
					util.CheckErr(fmt.Errorf("Failed to deactivate cluster domain %v: %v", clusterDomainName, err))
					return
				}
				msg := fmt.Sprintf("Cluster Domain %v deactivated successfully", clusterDomainName)
				printMsg(msg, ioStreams.Out)
			} else {
				util.CheckErr(fmt.Errorf("Exactly one cluster domain name needs to be provided to the deactivate command"))
				return
			}
		},
	}
	deactivateClusterDomainCommand.Flags().StringVar(&nameClusterDomainUpdate, "name", "", "Name for the deactivate cluster domain action")
	return deactivateClusterDomainCommand
}

func newGetClusterDomainUpdateCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	getClusterDomainUpdateCommand := &cobra.Command{
		Use:     clusterDomainUpdateSubcommand,
		Aliases: clusterDomainUpdateAliases,
		Short:   "Get cluster domain updates",
		Run: func(c *cobra.Command, args []string) {
			cdStatuses := new(storkv1.ClusterDomainUpdateList)
			var err error
			if len(args) > 0 {
				for _, clusterID := range args {
					cds, err := k8s.Instance().GetClusterDomainUpdate(clusterID)
					if err != nil {
						util.CheckErr(err)
						return
					}
					cdStatuses.Items = append(cdStatuses.Items, *cds)
				}
			} else {
				cdStatuses, err = k8s.Instance().ListClusterDomainUpdates()
				if err != nil {
					util.CheckErr(err)
					return
				}
			}

			if len(cdStatuses.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			if err := printObjects(c, cdStatuses, cmdFactory, clusterDomainUpdateColumns, clusterDomainUpdatePrinter, ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}
	cmdFactory.BindGetFlags(getClusterDomainUpdateCommand.Flags())
	return getClusterDomainUpdateCommand
}

func clusterDomainUpdatePrinter(cduList *storkv1.ClusterDomainUpdateList, writer io.Writer, options printers.PrintOptions) error {
	if cduList == nil {
		return nil
	}

	for _, cdu := range cduList.Items {
		name := printers.FormatResourceName(options.Kind, cdu.Name, options.WithKind)

		if options.WithNamespace {
			if _, err := fmt.Fprintf(writer, "%v\t", cdu.Namespace); err != nil {
				return err
			}
		}

		updateAction := "Activate"
		if !cdu.Spec.Active {
			updateAction = "Deactivate"
		}
		creationTime := toTimeString(cdu.CreationTimestamp.Time)
		if _, err := fmt.Fprintf(writer, "%v\t%v\t%v\t%v\t%v\n",
			name,
			cdu.Spec.ClusterDomain,
			updateAction,
			cdu.Status.Status,
			creationTime); err != nil {
			return err
		}
	}
	return nil
}
