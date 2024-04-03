package storkctl

import (
	"fmt"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/spf13/cobra"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubernetes/pkg/printers"
)

var drActionColumns = []string{"NAME", "CREATED", "STAGE", "STATUS", "MORE INFO"}

func newGetFailoverStatusCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	getFailoverCommand := &cobra.Command{
		Use:   failoverCommand,
		Short: "Get the status of failover actions",
		Run: func(c *cobra.Command, args []string) {
			getDRActionStatus(cmdFactory, ioStreams, storkv1.ActionTypeFailover, c, args)
		},
	}
	return getFailoverCommand
}

func newGetFailbackStatusCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	getFailbackCommand := &cobra.Command{
		Use:   failbackCommand,
		Short: "Get the status of failback actions",
		Run: func(c *cobra.Command, args []string) {
			getDRActionStatus(cmdFactory, ioStreams, storkv1.ActionTypeFailback, c, args)
		},
	}
	return getFailbackCommand
}

func getDRActionStatus(cmdFactory Factory, ioStreams genericclioptions.IOStreams, actionType storkv1.ActionType, c *cobra.Command, args []string) {
	var actions *storkv1.ActionList
	var filteredActionList *storkv1.ActionList
	var err error
	// user has to provide the namespace from which they want to get list of actions using -n flag
	namespace := cmdFactory.GetNamespace()
	if len(args) > 0 {
		// name of action has been specified
		actions = new(storkv1.ActionList)
		for _, actionName := range args {
			action, err := storkops.Instance().GetAction(actionName, namespace)
			if err != nil {
				util.CheckErr(err)
				return
			}
			actions.Items = append(actions.Items, *action)
		}
	} else {
		// fetch all the actions in the given namespace
		actions, err = storkops.Instance().ListActions(namespace)
		if err != nil {
			util.CheckErr(err)
			return
		}
	}

	// filter it down to actions with given actionType
	filteredActionList = new(storkv1.ActionList)
	for _, action := range actions.Items {
		if action.Spec.ActionType == actionType {
			filteredActionList.Items = append(filteredActionList.Items, action)
		}
	}

	if len(filteredActionList.Items) == 0 {
		handleEmptyList(ioStreams.Out)
		return
	}

	if err := printObjects(c, filteredActionList, cmdFactory, drActionColumns, drActionPrinter, ioStreams.Out); err != nil {
		util.CheckErr(err)
		return
	}
}

func drActionPrinter(actionList *storkv1.ActionList, options printers.GenerateOptions) ([]metav1beta1.TableRow, error) {
	if actionList == nil {
		return nil, nil
	}

	rows := make([]metav1beta1.TableRow, 0)
	for _, action := range actionList.Items {
		var row metav1beta1.TableRow
		creationTime := toTimeString(action.CreationTimestamp.Time)
		additionalInfo := action.Status.Reason
		if action.Status.Summary != nil {
			totalNamespaces := len(action.Status.Summary.FailoverSummaryItem) + len(action.Status.Summary.FailbackSummaryItem)
			successfulNamespaces := 0
			rollbackSuccessfulNamespaces := 0
			rollbackFailedNamespaces := 0
			if len(action.Status.Summary.FailoverSummaryItem) > 0 {
				// find out number of successfully scaled up namespaces
				for _, item := range action.Status.Summary.FailoverSummaryItem {
					if item.Status == storkv1.ActionStatusSuccessful {
						successfulNamespaces++
					} else if item.Status == storkv1.ActionStatusRollbackSuccessful {
						rollbackSuccessfulNamespaces++
					} else if item.Status == storkv1.ActionStatusRollbackFailed {
						rollbackFailedNamespaces++
					}
				}
			} else if len(action.Status.Summary.FailbackSummaryItem) > 0 {
				// find out number of successfully scaled up namespaces
				for _, item := range action.Status.Summary.FailbackSummaryItem {
					if item.Status == storkv1.ActionStatusSuccessful {
						successfulNamespaces++
					} else if item.Status == storkv1.ActionStatusRollbackSuccessful {
						rollbackSuccessfulNamespaces++
					} else if item.Status == storkv1.ActionStatusRollbackFailed {
						rollbackFailedNamespaces++
					}
				}
			} else {
				continue
			}
			if rollbackSuccessfulNamespaces > 0 || rollbackFailedNamespaces > 0 {
				additionalInfo = fmt.Sprintf("Rolled back Apps in : %d/%d namespaces", rollbackSuccessfulNamespaces, totalNamespaces)
				// In case of rollback we have a failure reason as well
				if action.Status.Reason != "" {
					additionalInfo += " ; " + action.Status.Reason
				}
			} else {
				additionalInfo = fmt.Sprintf("Scaled up Apps in : %d/%d namespaces", successfulNamespaces, totalNamespaces)
			}
		}
		row = getRow(&action,
			[]interface{}{action.Name,
				creationTime,
				prettyDRStageNames(action.Status.Stage, action.Spec.ActionType),
				action.Status.Status,
				additionalInfo,
			},
		)
		rows = append(rows, row)
	}
	return rows, nil
}

func prettyDRStageNames(stage storkv1.ActionStageType, actionType storkv1.ActionType) string {
	switch stage {
	case storkv1.ActionStageInitial:
		return "Validations"
	case storkv1.ActionStageScaleDownSource:
		return "Scale Down (on source)"
	case storkv1.ActionStageScaleDownDestination:
		return "Scale Down (on destination)"
	case storkv1.ActionStageWaitAfterScaleDown:
		if actionType == storkv1.ActionTypeFailover {
			return "Waiting for Apps to Scale Down (on source)"
		} else if actionType == storkv1.ActionTypeFailback {
			return "Waiting for Apps to Scale Down (on destination)"
		}
	case storkv1.ActionStageLastMileMigration:
		if actionType == storkv1.ActionTypeFailover {
			return "Last Mile Migration (from source -> destination)"
		} else if actionType == storkv1.ActionTypeFailback {
			return "Last Mile Migration (from destination -> source)"
		}
	case storkv1.ActionStageScaleUpSource:
		if actionType == storkv1.ActionTypeFailover {
			return "Rolling Back Scale Down (on source)"
		} else if actionType == storkv1.ActionTypeFailback {
			return "Scale Up (on source)"
		}
	case storkv1.ActionStageScaleUpDestination:
		if actionType == storkv1.ActionTypeFailover {
			return "Scale Up (on destination)"
		} else if actionType == storkv1.ActionTypeFailback {
			return "Rolling Back Scale Down (on destination)"
		}
	case storkv1.ActionStageFinal:
		return "Completed"
	default:
		return string(stage)
	}
	return string(stage)
}
