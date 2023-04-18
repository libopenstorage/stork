package storkctl

import (
	"fmt"
	"strings"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/util"
)

const (
	failoverCommand                    = "failover"
	nameTimeSuffixFormat string        = "2006-01-02-150405"
	actionWaitTimeout    time.Duration = 10 * time.Minute
	actionWaitInterval   time.Duration = 10 * time.Second
)

func newPerformCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	performCommands := &cobra.Command{
		Use:   "perform",
		Short: "perform actions",
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
			var actions []storkv1.Action
			for _, namespace := range namespaces {
				if anyActionIncomplete(namespace) {
					printMsg(
						fmt.Sprintf(
							"failed to start failover for namespace %v as an action is already in scheduled/in-progress",
							namespace),
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
					printMsg(
						fmt.Sprintf(
							"failed to start failover for namespace %v due to error %v",
							namespace, err),
						ioStreams.ErrOut)
					continue
				}
				printMsg(fmt.Sprintf("started failover for namespace %v", namespace), ioStreams.Out)
				actions = append(actions, action)
			}
			for _, action := range actions {
				isSuccessful, err := waitForActionToComplete(action.Name, action.Namespace)
				if err != nil {
					var errorMessage string
					if _, ok := err.(*task.ErrTimedOut); ok {
						errorMessage = fmt.Sprintf(
							"timed out waiting for action %v/%v to complete\naction might still be scheduled/in-progress\n",
							action.Namespace, action.Name) +
							getDebugMessage(action.Name, action.Namespace)
					} else {
						errorMessage = fmt.Sprintf(
							"received error when trying to get the action %v/%v: %v",
							action.Namespace, action.Name, err)
					}
					printMsg(errorMessage, ioStreams.ErrOut)
				} else {
					if isSuccessful {
						printMsg(
							fmt.Sprintf("successfully completed action %v/%v", action.Namespace, action.Name),
							ioStreams.Out)
					} else {
						printMsg(
							fmt.Sprintf("failed to complete action %v/%v\n", action.Namespace, action.Name)+
								getDebugMessage(action.Name, action.Namespace),
							ioStreams.Out)
					}
				}
			}

		},
	}
	return getClusterPairCommand
}

func isActionIncomplete(action *storkv1.Action) bool {
	return action.Status == storkv1.ActionStatusScheduled || action.Status == storkv1.ActionStatusInProgress
}

// check if there is already an Action scheduled or in-progress
func anyActionIncomplete(namespace string) bool {
	actionList, err := storkops.Instance().ListActions(namespace)
	if err != nil {
		util.CheckErr(err)
		return false
	}
	for _, action := range actionList.Items {
		if isActionIncomplete(&action) {
			return true
		}
	}
	return false
}

func newActionName(action storkv1.ActionType) string {
	return strings.Join([]string{string(action), time.Now().Format(nameTimeSuffixFormat)}, "-")
}

func waitForActionToComplete(actionName, namespace string) (bool, error) {
	action, err := task.DoRetryWithTimeout(
		func() (interface{}, bool, error) {
			action, err := storkops.Instance().GetAction(actionName, namespace)
			if err != nil {
				return nil, true, err
			}
			return action, isActionIncomplete(action), nil
		},
		actionWaitTimeout,
		actionWaitInterval)
	if err != nil {
		return false, err
	}
	if action.(*storkv1.Action).Status == storkv1.ActionStatusFailed {
		return false, nil
	}
	return true, nil
}

func getDebugMessage(actionName, namespace string) string {
	return "debug info:\n" +
		"- for details on the action, use: " + getCmdDescribeAction(actionName, namespace) +
		"\n- to view stork logs, use: kubectl -n kube-system logs -l name=stork"
}

func getCmdDescribeAction(actionName, namespace string) string {
	return fmt.Sprintf("kubectl describe action %v -n %v", actionName, namespace)
}
