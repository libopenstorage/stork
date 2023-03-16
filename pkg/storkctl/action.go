package storkctl

import (
	"fmt"
	"strings"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		Short: "Initiate failover for the given namespaces",
		Run: func(c *cobra.Command, args []string) {
			namespaces, err := cmdFactory.GetAllNamespaces()
			if err != nil {
				util.CheckErr(err)
				return
			}
			for _, namespace := range namespaces {
				if anyActionIncomplete(namespace) {
					printMsg(
						fmt.Sprintf("Cannot perform new action as an action is already scheduled/in-progress for %v", namespace),
						ioStreams.Out)
					continue
				}
				action := storkv1.Action{
					ObjectMeta: metav1.ObjectMeta{
						Name:      newActionName(storkv1.ActionTypeFailover),
						Namespace: namespace,
					},
					Spec: storkv1.ActionSpec{
						ActionType: storkv1.ActionTypeFailover,
					},
					Status: storkv1.ActionStatusScheduled,
				}
				_, err = storkops.Instance().CreateAction(&action)
				if err != nil {
					util.CheckErr(err)
					return
				}
			}
		},
	}
	return getClusterPairCommand
}

// check if there is already an Action scheduled or in-progress
func anyActionIncomplete(namespace string) bool {
	actionList, err := storkops.Instance().ListActions(namespace)
	if err != nil {
		util.CheckErr(err)
		return false
	}
	for _, action := range actionList.Items {
		if action.Status == storkv1.ActionStatusScheduled || action.Status == storkv1.ActionStatusInProgress {
			return true
		}
	}
	return false
}

func newActionName(action storkv1.ActionType) string {
	return strings.Join([]string{string(action), time.Now().Format(nameTimeSuffixFormat)}, "-")
}
