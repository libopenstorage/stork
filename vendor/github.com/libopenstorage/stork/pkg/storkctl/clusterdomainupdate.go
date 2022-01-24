package storkctl

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/pborman/uuid"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/spf13/cobra"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubernetes/pkg/printers"
)

var clusterDomainUpdateColumns = []string{"NAME", "CLUSTER-DOMAIN", "ACTION", "STATUS", "CREATED"}
var clusterDomainUpdateSubcommand = "clusterdomainupdate"
var clusterDomainUpdateAliases = []string{"cdu"}
var clusterDomainSubcommand = "clusterdomain"
var clusterDomainAliases = []string{"cd"}

const (
	retryTimeout = 2 * time.Second
	timeout      = 10 * time.Minute
)

func newActivateClusterDomainCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var allClusterDomains bool
	var waitForCompletion bool
	var nameClusterDomainUpdate string
	activateClusterDomainCommand := &cobra.Command{
		Use:     clusterDomainSubcommand,
		Aliases: clusterDomainAliases,
		Short:   "Activate a cluster domain",
		Run: func(c *cobra.Command, args []string) {
			activationList := []string{}
			if allClusterDomains {
				cdsList, err := storkops.Instance().ListClusterDomainStatuses()
				if err != nil {
					util.CheckErr(err)
					return
				}

				for _, cds := range cdsList.Items {
					for _, cdInfo := range cds.Status.ClusterDomainInfos {
						if cdInfo.State == storkv1.ClusterDomainInactive {
							activationList = append(activationList, cdInfo.Name)
						}
					}
				}
			} else if len(args) == 1 {
				clusterDomainName := args[0]
				cdsList, err := storkops.Instance().ListClusterDomainStatuses()
				if err != nil {
					util.CheckErr(fmt.Errorf("failed to list existing cluster domains: %v", err))
					return
				} else if cdsList == nil {
					util.CheckErr(fmt.Errorf("could not find any configured cluster domains"))
					return
				}
				found := false
				for _, cds := range cdsList.Items {
					for _, cdsInfo := range cds.Status.ClusterDomainInfos {
						if cdsInfo.Name == clusterDomainName {
							found = true
							break
						}
					}
				}
				if !found {
					util.CheckErr(fmt.Errorf("provided cluster domain %v not found", clusterDomainName))
					return
				}

				activationList = []string{clusterDomainName}
			} else {
				util.CheckErr(fmt.Errorf("exactly one cluster domain name needs to be provided to the activate command"))
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
				_, err := storkops.Instance().CreateClusterDomainUpdate(clusterDomainUpdate)
				if err != nil {
					util.CheckErr(fmt.Errorf("failed to activate cluster domain %v: %v", clusterDomainName, err))
					return
				}

				msg := fmt.Sprintf("Cluster Domain activate operation started successfully for %v", clusterDomainName)
				printMsg(msg, ioStreams.Out)

				if waitForCompletion {
					if _, err := fmt.Fprintf(ioStreams.Out, "Activating.."); err != nil {
						util.CheckErr(err)
						return
					}
					msg := waitForDomainUpdate(name)
					printMsg(msg, ioStreams.Out)
				}
			}

		},
	}
	activateClusterDomainCommand.Flags().BoolVarP(&allClusterDomains, "all", "a", false, "Activate all inactive cluster domains")
	activateClusterDomainCommand.Flags().BoolVarP(&waitForCompletion, "wait", "", false, "Wait for clusterdomain update to complete")
	activateClusterDomainCommand.Flags().StringVar(&nameClusterDomainUpdate, "name", "", "Name for the activate cluster domain action")

	return activateClusterDomainCommand
}

func newDeactivateClusterDomainCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var nameClusterDomainUpdate string
	var waitForCompletion bool

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
				cdsList, err := storkops.Instance().ListClusterDomainStatuses()
				if err != nil {
					util.CheckErr(fmt.Errorf("failed to list existing cluster domains: %v", err))
					return
				} else if cdsList == nil {
					util.CheckErr(fmt.Errorf("could not find any configured cluster domains"))
					return
				}
				found := false
				for _, cds := range cdsList.Items {
					for _, cdsInfo := range cds.Status.ClusterDomainInfos {
						if cdsInfo.Name == clusterDomainName {
							found = true
							break
						}
					}
				}
				if !found {
					util.CheckErr(fmt.Errorf("provided cluster domain %v not found", clusterDomainName))
					return
				}
				clusterDomainUpdate := &storkv1.ClusterDomainUpdate{
					ObjectMeta: meta.ObjectMeta{
						Name: name,
					},
					Spec: storkv1.ClusterDomainUpdateSpec{
						ClusterDomain: clusterDomainName,
						Active:        false,
					},
				}
				_, err = storkops.Instance().CreateClusterDomainUpdate(clusterDomainUpdate)
				if err != nil {
					util.CheckErr(fmt.Errorf("failed to deactivate cluster domain %v: %v", clusterDomainName, err))
					return
				}

				msg := fmt.Sprintf("Cluster Domain deactivate operation started successfully for %v", clusterDomainName)
				printMsg(msg, ioStreams.Out)

				if waitForCompletion {
					if _, err := fmt.Fprintf(ioStreams.Out, "Deactivating.."); err != nil {
						util.CheckErr(err)
						return
					}
					msg := waitForDomainUpdate(name)
					printMsg(msg, ioStreams.Out)
				}

			} else {
				util.CheckErr(fmt.Errorf("exactly one cluster domain name needs to be provided to the deactivate command"))
				return
			}
		},
	}
	deactivateClusterDomainCommand.Flags().StringVar(&nameClusterDomainUpdate, "name", "", "Name for the deactivate cluster domain action")
	deactivateClusterDomainCommand.Flags().BoolVarP(&waitForCompletion, "wait", "", false, "Wait for clusterdomain update to complete")

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
					cds, err := storkops.Instance().GetClusterDomainUpdate(clusterID)
					if err != nil {
						util.CheckErr(err)
						return
					}
					cdStatuses.Items = append(cdStatuses.Items, *cds)
				}
			} else {
				cdStatuses, err = storkops.Instance().ListClusterDomainUpdates()
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

func clusterDomainUpdatePrinter(
	cduList *storkv1.ClusterDomainUpdateList,
	options printers.GenerateOptions,
) ([]metav1beta1.TableRow, error) {
	if cduList == nil {
		return nil, nil
	}

	rows := make([]metav1beta1.TableRow, 0)
	for _, cdu := range cduList.Items {
		updateAction := "Activate"
		if !cdu.Spec.Active {
			updateAction = "Deactivate"
		}
		creationTime := toTimeString(cdu.CreationTimestamp.Time)
		row := getRow(&cdu,
			[]interface{}{cdu.Name,
				cdu.Spec.ClusterDomain,
				updateAction,
				cdu.Status.Status,
				creationTime},
		)
		rows = append(rows, row)

	}
	return rows, nil
}

func waitForDomainUpdate(name string) string {
	var msg string

	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)
	t := func() (interface{}, bool, error) {
		cds, err := storkops.Instance().GetClusterDomainUpdate(name)
		if err != nil {
			return fmt.Sprintf("Unable to retrive cluster details %v", err), false, err
		}
		fmt.Printf("..%v", cds.Status.Status)
		if cds.Status.Status == storkv1.ClusterDomainUpdateStatusFailed {
			msg = fmt.Sprintf("\nFailed to update ClusterDomain, Reason : %v", cds.Status.Reason)
			return "", false, nil
		}
		if cds.Status.Status == storkv1.ClusterDomainUpdateStatusSuccessful {
			msg = fmt.Sprintf("\nCluster Domain %v updated successfully", cds.Spec.ClusterDomain)
			return "", false, nil
		}
		return nil, true, fmt.Errorf("%v", cds.Status.Status)
	}

	if _, err := task.DoRetryWithTimeout(t, timeout, retryTimeout); err != nil {
		msg = "Timed out performing task"
	}

	return msg
}
