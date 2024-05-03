package storkctl

import (
	"fmt"
	"github.com/libopenstorage/stork/pkg/utils"
	"strings"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	migration "github.com/libopenstorage/stork/pkg/migration/controllers"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/util"
)

const (
	failoverCommand                    = "failover"
	failbackCommand                    = "failback"
	nameTimeSuffixFormat string        = "2006-01-02-150405"
	actionWaitTimeout    time.Duration = 10 * time.Minute
	actionWaitInterval   time.Duration = 10 * time.Second
)

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

func newPerformFailoverCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var referenceMigrationSchedule string
	var skipSourceOperations bool
	var includeNamespaceList []string
	var excludeNamespaceList []string
	performFailoverCommand := &cobra.Command{
		Use:   failoverCommand,
		Short: "Initiate failover of the given migration schedule",
		Run: func(c *cobra.Command, args []string) {
			// namespace of the MigrationSchedule is provided by the user using the -n / --namespace global flag
			migrationScheduleNs := cmdFactory.GetNamespace()
			namespaceList, err := validationsForPerformDRCommands(storkv1.ActionTypeFailover, migrationScheduleNs, referenceMigrationSchedule, includeNamespaceList, excludeNamespaceList, skipSourceOperations)
			if err != nil {
				util.CheckErr(err)
				return
			}
			actionParameters := storkv1.ActionParameter{
				FailoverParameter: storkv1.FailoverParameter{
					FailoverNamespaces:         namespaceList,
					MigrationScheduleReference: referenceMigrationSchedule,
					SkipSourceOperations:       &skipSourceOperations,
				},
			}
			err = createActionCR(storkv1.ActionTypeFailover, migrationScheduleNs, referenceMigrationSchedule, actionParameters, ioStreams)
			if err != nil {
				util.CheckErr(err)
				return
			}
		},
	}
	performFailoverCommand.Flags().BoolVar(&skipSourceOperations, "skip-source-operations", false, "If present, operations performed on the source cluster will be skipped, and applications on the current cluster will be scaled up")
	performFailoverCommand.Flags().StringVarP(&referenceMigrationSchedule, "migration-reference", "m", "", "Specify the MigrationSchedule to failover. Also specify the namespace of this MigrationSchedule using the -n flag")
	performFailoverCommand.Flags().StringSliceVar(&includeNamespaceList, "include-namespaces", nil, "Specify the comma-separated list of subset namespaces to be failed over. By default, all namespaces part of the MigrationSchedule are failed over")
	performFailoverCommand.Flags().StringSliceVar(&excludeNamespaceList, "exclude-namespaces", nil, "Specify the comma-separated list of subset namespaces to be skipped during the failover. By default, all namespaces part of the MigrationSchedule are failed over")
	return performFailoverCommand
}

func newPerformFailbackCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var referenceMigrationSchedule string
	var includeNamespaceList []string
	var excludeNamespaceList []string
	performFailbackCommand := &cobra.Command{
		Use:   failbackCommand,
		Short: "Initiate failback of the given migration schedule",
		Run: func(c *cobra.Command, args []string) {
			// namespace of the MigrationSchedule is provided by the user using the -n / --namespace global flag
			migrationScheduleNs := cmdFactory.GetNamespace()
			namespaceList, err := validationsForPerformDRCommands(storkv1.ActionTypeFailback, migrationScheduleNs, referenceMigrationSchedule, includeNamespaceList, excludeNamespaceList, false)
			if err != nil {
				util.CheckErr(err)
				return
			}
			actionParameters := storkv1.ActionParameter{
				FailbackParameter: storkv1.FailbackParameter{
					FailbackNamespaces:         namespaceList,
					MigrationScheduleReference: referenceMigrationSchedule,
				},
			}
			err = createActionCR(storkv1.ActionTypeFailback, migrationScheduleNs, referenceMigrationSchedule, actionParameters, ioStreams)
			if err != nil {
				util.CheckErr(err)
				return
			}
		},
	}
	performFailbackCommand.Flags().StringVarP(&referenceMigrationSchedule, "migration-reference", "m", "", "Specify the MigrationSchedule to failback. Also specify the namespace of this MigrationSchedule using the -n flag")
	performFailbackCommand.Flags().StringSliceVar(&includeNamespaceList, "include-namespaces", nil, "Specify the comma-separated list of subset namespaces to be failed back. By default, all namespaces part of the MigrationSchedule are failed back")
	performFailbackCommand.Flags().StringSliceVar(&excludeNamespaceList, "exclude-namespaces", nil, "Specify the comma-separated list of subset namespaces to be skipped during the failback. By default, all namespaces part of the MigrationSchedule are failed back")
	return performFailbackCommand
}

// validationsForPerformDRCommands performs the validations for failover/failback and returns the resultant namespaceList
func validationsForPerformDRCommands(actionType storkv1.ActionType, migrationScheduleNs string, referenceMigrationSchedule string, includeNamespaceList []string, excludeNamespaceList []string, skipSourceOperations bool) ([]string, error) {
	var namespaceList []string
	if len(referenceMigrationSchedule) == 0 {
		return nil, fmt.Errorf("reference MigrationSchedule name needs to be provided for %s", actionType)
	}

	migrSchedObj, err := storkops.Instance().GetMigrationSchedule(referenceMigrationSchedule, migrationScheduleNs)
	if err != nil {
		return nil, fmt.Errorf("unable to find the reference MigrationSchedule %v in the %v namespace", referenceMigrationSchedule, migrationScheduleNs)
	}

	if actionType == storkv1.ActionTypeFailover {
		// MigrationSchedule provided to failover must be a static copy
		if migrSchedObj.GetAnnotations() == nil || migrSchedObj.GetAnnotations()[migration.StorkMigrationScheduleCopied] != "true" {
			return nil, fmt.Errorf("ensure that `storkctl perform failover` is run in the cluster you want to failover to")
		}
	}

	// clusterPair specified in the reference MigrationSchedule should always exist in the destination cluster in failback
	// for failover only if skipSourceOperations flag is provided, cluster-pair presence is optional
	if !(actionType == storkv1.ActionTypeFailover && skipSourceOperations) {
		clusterPair := migrSchedObj.Spec.Template.Spec.ClusterPair
		_, err = storkops.Instance().GetClusterPair(clusterPair, migrationScheduleNs)
		if err != nil {
			return nil, fmt.Errorf("unable to find the ClusterPair %v in the %v namespace", clusterPair, migrationScheduleNs)
		}
	}

	migrationNamespaceList := migrSchedObj.Spec.Template.Spec.Namespaces
	migrationNamespaceSelectors := migrSchedObj.Spec.Template.Spec.NamespaceSelectors
	// update the migrationNamespaces list by fetching namespaces based on provided label selectors
	migrationNamespaces, err := utils.GetMergedNamespacesWithLabelSelector(migrationNamespaceList, migrationNamespaceSelectors)
	if err != nil {
		return nil, fmt.Errorf("unable to get the namespaces based on the --namespace-selectors in the provided MigrationSchedule: %v", err)
	}
	// at most one of exclude-namespaces or include-namespaces can be provided
	if len(includeNamespaceList) != 0 && len(excludeNamespaceList) != 0 {
		return nil, fmt.Errorf("can provide only one of --include-namespaces or --exclude-namespaces values at once")
	} else if len(includeNamespaceList) != 0 {
		if isSubList, _, nonSubsetStrings := utils.IsSubList(includeNamespaceList, migrationNamespaces); isSubList {
			// Branch 1: Only failover/failback some of the namespaces being migrated by the given migrationSchedule
			namespaceList = includeNamespaceList
		} else {
			return nil, fmt.Errorf("provided namespaces %v are not a subset of the namespaces being migrated by the given MigrationSchedule", nonSubsetStrings)
		}
	} else if len(excludeNamespaceList) != 0 {
		if isSubList, _, nonSubsetStrings := utils.IsSubList(excludeNamespaceList, migrationNamespaces); isSubList {
			// Branch 2: Exclude some of the namespaces being migrated by the given migrationSchedule from failover/failback
			namespaceList = utils.ExcludeListAFromListB(excludeNamespaceList, migrationNamespaces)
		} else {
			return nil, fmt.Errorf("provided namespaces %v are not a subset of the namespaces being migrated by the given MigrationSchedule", nonSubsetStrings)
		}
	} else {
		// Branch 3: Failover/Failback all the namespaces being migrated by the given migrationSchedule
		namespaceList = migrationNamespaces
	}
	return namespaceList, nil
}

func createActionCR(actionType storkv1.ActionType, migrationScheduleNs string, referenceMigrationSchedule string, actionParameters storkv1.ActionParameter, ioStreams genericclioptions.IOStreams) error {
	actionName := getActionName(actionType, referenceMigrationSchedule)
	actionObj := storkv1.Action{
		ObjectMeta: metav1.ObjectMeta{
			Name:      actionName,
			Namespace: migrationScheduleNs,
		},
		Spec: storkv1.ActionSpec{
			ActionType:      actionType,
			ActionParameter: actionParameters,
		},
		Status: storkv1.ActionStatus{Status: storkv1.ActionStatusInitial},
	}
	action, err := storkops.Instance().CreateAction(&actionObj)
	migrationScheduleName := migrationScheduleNs + "/" + referenceMigrationSchedule
	if err != nil {
		return fmt.Errorf("failed to start %s for MigrationSchedule %v : %v", actionType, migrationScheduleName, err)
	}
	printMsg(fmt.Sprintf("Started %s for MigrationSchedule %v", actionType, migrationScheduleName), ioStreams.Out)
	printMsg(getActionStatusMessage(action), ioStreams.Out)
	return nil
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
