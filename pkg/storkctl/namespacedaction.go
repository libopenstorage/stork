package storkctl

import (
	"strings"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/spf13/cobra"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/util"
)

const (
	failoverCommand             = "failover"
	nameTimeSuffixFormat string = "2006-01-02-150405"
)

func newPerformCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	performCommands := &cobra.Command{
		Use:   "perform",
		Short: "Perform actions",
	}

	performCommands.AddCommand(
		newFailoverCommand(cmdFactory, ioStreams),
	)

	return performCommands
}

func newFailoverCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	getClusterPairCommand := &cobra.Command{
		Use:   failoverCommand,
		Short: "Initiate failover for the specified namespaces",
		Run: func(c *cobra.Command, args []string) {
			namespaces, err := cmdFactory.GetAllNamespaces()
			if err != nil {
				util.CheckErr(err)
				return
			}
			for _, ns := range namespaces {
				namespacedActionList, err := storkops.Instance().ListNamespacedActions(ns)
				if err != nil {
					util.CheckErr(err)
					return
				}
				var namespacedAction storkv1.NamespacedAction
				if len(namespacedActionList.Items) != 0 {
					namespacedAction = namespacedActionList.Items[0]
					if namespacedAction.Spec.Action != storkv1.NamespacedActionNil {
						// Raise error
						return
					}
					namespacedAction.Spec.Action = storkv1.NamespacedActionFailover
					_, err = storkops.Instance().UpdateNamespacedAction(&namespacedAction)
					if err != nil {
						util.CheckErr(err)
						return
					}
				} else {
					namespacedAction = storkv1.NamespacedAction{
						ObjectMeta: v1.ObjectMeta{
							Name:      newActionName(storkv1.NamespacedActionFailover),
							Namespace: ns,
						},
						Spec: storkv1.NamespacedActionSpec{
							Action: storkv1.NamespacedActionFailover,
						},
					}
					_, err = storkops.Instance().CreateNamespacedAction(&namespacedAction)
					if err != nil {
						util.CheckErr(err)
						return
					}
				}
			}
		},
	}
	return getClusterPairCommand
}

func newActionName(action storkv1.NamespacedActionType) string {
	return strings.Join([]string{string(action), time.Now().Format(nameTimeSuffixFormat)}, "-")
}
