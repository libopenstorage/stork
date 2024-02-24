package storkctl

import (
	"fmt"
	"k8s.io/apimachinery/pkg/util/validation"
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

var mockTime *time.Time

// setMockTime is used in tests to update the time
func setMockTime(mt *time.Time) {
	mockTime = mt
}

// GetCurrentTime returns the current time as per the scheduler
func GetCurrentTime() time.Time {
	if mockTime != nil {
		return *mockTime
	}
	return time.Now()
}

func newFailoverCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var referenceMigrationSchedule string
	var skipDeactivateSource bool
	var includeNamespaceList []string
	var excludeNamespaceList []string
	var namespaceList []string
	performFailoverCommand := &cobra.Command{
		Use:   failoverCommand,
		Short: "Initiate failover of the given migration schedule",
		Run: func(c *cobra.Command, args []string) {
			if len(referenceMigrationSchedule) == 0 {
				util.CheckErr(fmt.Errorf("referenceMigrationSchedule name needs to be provided for failover"))
				return
			}
			// namespace of the migrationSchedule is provided by user using the -n / --namespace global flag
			migrationScheduleNs := cmdFactory.GetNamespace()
			migrSchedObj, err := storkops.Instance().GetMigrationSchedule(referenceMigrationSchedule, migrationScheduleNs)
			if err != nil {
				util.CheckErr(fmt.Errorf("unable to find the referenceMigrationSchedule %v in the %v namespace", referenceMigrationSchedule, migrationScheduleNs))
				return
			}
			if !skipDeactivateSource {
				clusterPair := migrSchedObj.Spec.Template.Spec.ClusterPair
				_, err = storkops.Instance().GetClusterPair(clusterPair, migrationScheduleNs)
				if err != nil {
					util.CheckErr(fmt.Errorf("unable to find the cluster pair %v in the %v namespace", clusterPair, migrationScheduleNs))
					return
				}
			}
			migrationNamespaceList := migrSchedObj.Spec.Template.Spec.Namespaces
			migrationNamespaceSelectors := migrSchedObj.Spec.Template.Spec.NamespaceSelectors
			// update the migrationNamespaces list by fetching namespaces based on provided label selectors
			migrationNamespaces, err := getMigrationNamespaces(migrationNamespaceList, migrationNamespaceSelectors)
			if err != nil {
				util.CheckErr(fmt.Errorf("unable to get the namespaces based on the --namespace-selectors in the provided migrationSchedule: %v", err))
				return
			}
			// at most one of exclude-namespaces or include-namespaces can be provided
			if len(includeNamespaceList) != 0 && len(excludeNamespaceList) != 0 {
				util.CheckErr(fmt.Errorf("can provide only one of --include-namespaces or --exclude-namespaces values at once"))
				return
			} else if len(includeNamespaceList) != 0 {
				if isSubset, nonSubsetStrings := isSubset(includeNamespaceList, migrationNamespaces); isSubset {
					// Branch 1: Only failover some of the namespaces being migrated by the given migrationSchedule
					namespaceList = includeNamespaceList
				} else {
					util.CheckErr(fmt.Errorf("provided namespaces %v are not a subset of the namespaces being migrated by the given migrationSchedule", nonSubsetStrings))
					return
				}
			} else if len(excludeNamespaceList) != 0 {
				if isSubset, nonSubsetStrings := isSubset(excludeNamespaceList, migrationNamespaces); isSubset {
					// Branch 2: Exclude some of the namespaces being migrated by the given migrationSchedule from failover
					namespaceList = excludeListAFromListB(excludeNamespaceList, migrationNamespaces)
				} else {
					util.CheckErr(fmt.Errorf("provided namespaces %v are not a subset of the namespaces being migrated by the given migrationSchedule", nonSubsetStrings))
					return
				}
			} else {
				// Branch 3: Failover all the namespaces being migrated by the given migrationSchedule
				namespaceList = migrationNamespaces
			}
			actionType := storkv1.ActionTypeFailover
			actionParameters := storkv1.ActionParameter{
				FailoverParameter: &storkv1.FailoverParameter{
					FailoverNamespaces:         namespaceList,
					MigrationScheduleReference: referenceMigrationSchedule,
					DeactivateSource:           !skipDeactivateSource,
				},
			}
			actionName := getActionName(actionType, referenceMigrationSchedule)
			action, err := createActionCR(actionName, migrationScheduleNs, actionType, actionParameters)
			migrationScheduleName := migrationScheduleNs + "/" + referenceMigrationSchedule
			if err != nil {
				util.CheckErr(fmt.Errorf("failed to start failover for migrationSchedule %v : %v", migrationScheduleName, err))
				return
			}
			printMsg(fmt.Sprintf("Started failover for migrationSchedule %v", migrationScheduleName), ioStreams.Out)
			printMsg(getActionStatusMessage(action), ioStreams.Out)
		},
	}
	performFailoverCommand.Flags().BoolVar(&skipDeactivateSource, "skip-deactivate-source", false, "If present, applications in the source cluster will not be scaled down as part of the failover.")
	performFailoverCommand.Flags().StringVarP(&referenceMigrationSchedule, "migration-reference", "m", "", "Specify the migration schedule to failover. Also specify the namespace of this migrationSchedule using the -n flag")
	performFailoverCommand.Flags().StringSliceVar(&includeNamespaceList, "include-namespaces", nil, "Specify the comma-separated list of subset namespaces to be failed over. By default, all namespaces part of the migrationSchedule are failed over")
	performFailoverCommand.Flags().StringSliceVar(&excludeNamespaceList, "exclude-namespaces", nil, "Specify the comma-separated list of subset namespaces to be skipped during the failover. By default, all namespaces part of the migrationSchedule are failed over")
	return performFailoverCommand
}

// given the name, namespace, actionType and actionParameters create an Action CR
func createActionCR(actionName string, namespace string, actionType storkv1.ActionType, actionParameters storkv1.ActionParameter) (*storkv1.Action, error) {
	action := storkv1.Action{
		ObjectMeta: metav1.ObjectMeta{
			Name:      actionName,
			Namespace: namespace,
		},
		Spec: storkv1.ActionSpec{
			ActionType:      actionType,
			ActionParameter: &actionParameters,
		},
		Status: storkv1.ActionStatus{Status: storkv1.ActionStatusInitial},
	}
	actionObj, err := storkops.Instance().CreateAction(&action)
	if err != nil {
		return nil, err
	}
	return actionObj, nil
}

func getActionStatusMessage(action *storkv1.Action) string {
	return fmt.Sprintf("To check %v status use the command : `storkctl get %v %v -n %v`", action.Spec.ActionType, action.Spec.ActionType, action.Name, action.Namespace)
}

func getActionName(actionType storkv1.ActionType, referenceResourceName string) string {
	actionPrefix := string(actionType)
	actionSuffix := GetCurrentTime().Format(nameTimeSuffixFormat)
	lenAffixes := len(actionPrefix) + len(actionSuffix) + 2 // +2 for the 2 '-'s
	if len(referenceResourceName) >= validation.DNS1123SubdomainMaxLength-lenAffixes {
		referenceResourceName = referenceResourceName[:validation.DNS1123SubdomainMaxLength-lenAffixes]
	}
	return strings.Join([]string{actionPrefix, referenceResourceName, actionSuffix}, "-")
}
