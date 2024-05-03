package storkctl

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/libopenstorage/stork/pkg/utils"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/resourceutils"
	"github.com/portworx/sched-ops/k8s/core"
	dynamicops "github.com/portworx/sched-ops/k8s/dynamic"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubernetes/pkg/printers"
)

const (
	migrTimeout = 6 * time.Hour
	stage       = "STAGE"
	status      = "STATUS"
	statusOk    = "Ok\n"

	// StorkMigrationScheduleCopied indicating migrated migrationscheduleobject
	StorkMigrationScheduleCopied = "stork.libopenstorage.org/static-copy"
	// StorkMigrationNamespace indicating the namespace of migration cr
	StorkMigrationNamespace = "stork.libopenstorage.org/migrationNamespace"
)

var (
	migrRetryTimeout = 30 * time.Second
)

var migrationColumns = []string{"NAME", "CLUSTERPAIR", "STAGE", "STATUS", "VOLUMES", "RESOURCES", "CREATED", "ELAPSED", "TOTAL BYTES TRANSFERRED"}
var migrationSubcommand = "migrations"
var migrationAliases = []string{"migration"}

func newCreateMigrationCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var migrationName string
	var clusterPair string
	var namespaceList []string
	var includeResources bool
	var startApplications bool
	var preExecRule string
	var postExecRule string
	var includeVolumes bool
	var waitForCompletion, validate bool
	var fileName string

	createMigrationCommand := &cobra.Command{
		Use:     migrationSubcommand,
		Aliases: migrationAliases,
		Short:   "Start a migration",
		Run: func(c *cobra.Command, args []string) {
			if fileName != "" {
				if err := validateMigrationFromFile(fileName, cmdFactory.GetNamespace()); err != nil {
					util.CheckErr(err)
					return
				}
				return
			}
			if len(args) != 1 {
				util.CheckErr(fmt.Errorf("exactly one name needs to be provided for migration name"))
				return
			}
			migrationName = args[0]
			if len(clusterPair) == 0 {
				util.CheckErr(fmt.Errorf("ClusterPair name needs to be provided for migration"))
				return
			}
			if len(namespaceList) == 0 {
				util.CheckErr(fmt.Errorf("need to provide atleast one namespace to migrate"))
				return
			}
			migration := &storkv1.Migration{
				Spec: storkv1.MigrationSpec{
					ClusterPair:       clusterPair,
					Namespaces:        namespaceList,
					IncludeResources:  &includeResources,
					IncludeVolumes:    &includeVolumes,
					StartApplications: &startApplications,
					PreExecRule:       preExecRule,
					PostExecRule:      postExecRule,
				},
			}
			migration.Name = migrationName
			migration.Namespace = cmdFactory.GetNamespace()
			if validate {
				if err := ValidateMigration(migration); err != nil {
					util.CheckErr(err)
					return
				}
				return
			}
			_, err := storkops.Instance().CreateMigration(migration)
			if err != nil {
				util.CheckErr(err)
				return
			}

			if waitForCompletion {
				msg, err := waitForMigration(migration.Name, migration.Namespace, ioStreams)
				if err != nil {
					util.CheckErr(err)
					return
				}
				printMsg(msg, ioStreams.Out)
			} else {
				msg := "Migration " + migrationName + " created successfully"
				printMsg(msg, ioStreams.Out)
			}
		},
	}
	createMigrationCommand.Flags().StringSliceVarP(&namespaceList, "namespaces", "", nil, "Comma separated list of namespaces to migrate")
	createMigrationCommand.Flags().StringVarP(&clusterPair, "clusterPair", "c", "", "ClusterPair name for migration")
	createMigrationCommand.Flags().BoolVarP(&includeResources, "includeResources", "r", true, "Include resources in the migration")
	createMigrationCommand.Flags().BoolVarP(&includeVolumes, "includeVolumes", "", true, "Include volumees in the migration")
	createMigrationCommand.Flags().BoolVarP(&waitForCompletion, "wait", "", false, "Wait for migration to complete")
	createMigrationCommand.Flags().BoolVarP(&validate, "dry-run", "", false, "Validate migration params before starting migration")
	createMigrationCommand.Flags().BoolVarP(&startApplications, "startApplications", "a", true, "Start applications on the destination cluster after migration")
	createMigrationCommand.Flags().StringVarP(&preExecRule, "preExecRule", "", "", "Rule to run before executing migration")
	createMigrationCommand.Flags().StringVarP(&postExecRule, "postExecRule", "", "", "Rule to run after executing migration")
	createMigrationCommand.Flags().StringVarP(&fileName, "file", "f", "", "file to run migration")

	return createMigrationCommand
}

func newActivateMigrationsCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var allNamespaces bool
	activateMigrationCommand := &cobra.Command{
		Use:     migrationSubcommand,
		Aliases: migrationAliases,
		Short:   "Activate apps that were created from a migration",
		Run: func(c *cobra.Command, args []string) {
			activationNamespaces := make([]string, 0)
			migrationScheduleNamespaces := make([]string, 0)
			config, err := cmdFactory.GetConfig()
			if err != nil {
				util.CheckErr(err)
				return
			}
			if qps := cmdFactory.GetQPS(); qps > 1 {
				config.QPS = float32(qps)
			}
			if burst := cmdFactory.GetBurst(); burst > 1 {
				config.Burst = burst
			}
			if allNamespaces {
				namespaces, err := core.Instance().ListNamespaces(nil)
				if err != nil {
					util.CheckErr(err)
					return
				}
				for _, ns := range namespaces.Items {
					activationNamespaces = append(activationNamespaces, ns.Name)
				}
				migrationScheduleNamespaces = append(migrationScheduleNamespaces, activationNamespaces...)
			} else {
				activationNamespaces = append(activationNamespaces, cmdFactory.GetNamespace())
				migrationScheduleNamespace := GetMigrationScheduleCRNamespace(cmdFactory.GetNamespace())
				if len(migrationScheduleNamespace) > 0 {
					migrationScheduleNamespaces = append(migrationScheduleNamespaces, migrationScheduleNamespace)
				} else {
					migrationScheduleNamespaces = append(migrationScheduleNamespaces, cmdFactory.GetNamespace())
				}
			}

			for _, ns := range activationNamespaces {
				resourceutils.ScaleReplicas(ns, true, printFunc(ioStreams), config)
			}

			for _, ns := range migrationScheduleNamespaces {
				migrationSchedules, err := storkops.Instance().ListMigrationSchedules(ns)
				if err != nil {
					util.CheckErr(err)
					return
				}
				// MigrationSchedule will only be relevant if it migrates at least one of the activation namespaces
				// Only for such migrationSchedules we will need to update applicationActivated:true
				for _, migrSched := range migrationSchedules.Items {
					isMigrSchedRelevant, err := utils.DoesMigrationScheduleMigrateNamespaces(migrSched, activationNamespaces)
					if err != nil {
						util.CheckErr(err)
						return
					}
					if isMigrSchedRelevant {
						migrSched.Status.ApplicationActivated = true
						_, err := storkops.Instance().UpdateMigrationSchedule(&migrSched)
						if err != nil {
							util.CheckErr(err)
							return
						}
						printMsg(fmt.Sprintf("Setting the ApplicationActivated status in the MigrationSchedule %v/%v to true", migrSched.Namespace, migrSched.Name), ioStreams.Out)
					}
				}
			}
		},
	}
	activateMigrationCommand.Flags().BoolVarP(&allNamespaces, "all-namespaces", "a", false, "Activate applications in all namespaces")

	return activateMigrationCommand
}

func newDeactivateMigrationsCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var allNamespaces bool

	deactivateMigrationCommand := &cobra.Command{
		Use:     migrationSubcommand,
		Aliases: migrationAliases,
		Short:   "Deactivate apps that were created from a migration",
		Run: func(c *cobra.Command, args []string) {
			deactivationNamespaces := make([]string, 0)
			migrationScheduleNamespaces := make([]string, 0)
			config, err := cmdFactory.GetConfig()
			if err != nil {
				util.CheckErr(err)
				return
			}
			if allNamespaces {
				namespaces, err := core.Instance().ListNamespaces(nil)
				if err != nil {
					util.CheckErr(err)
					return
				}
				for _, ns := range namespaces.Items {
					deactivationNamespaces = append(deactivationNamespaces, ns.Name)
				}
				migrationScheduleNamespaces = append(migrationScheduleNamespaces, deactivationNamespaces...)

			} else {
				deactivationNamespaces = append(deactivationNamespaces, cmdFactory.GetNamespace())
				migrationScheduleNamespace := GetMigrationScheduleCRNamespace(cmdFactory.GetNamespace())
				if len(migrationScheduleNamespace) > 0 {
					migrationScheduleNamespaces = append(migrationScheduleNamespaces, migrationScheduleNamespace)
				} else {
					migrationScheduleNamespaces = append(migrationScheduleNamespaces, cmdFactory.GetNamespace())
				}
			}

			for _, ns := range deactivationNamespaces {
				resourceutils.ScaleReplicas(ns, false, printFunc(ioStreams), config)
			}

			for _, ns := range migrationScheduleNamespaces {
				migrationSchedules, err := storkops.Instance().ListMigrationSchedules(ns)
				if err != nil {
					util.CheckErr(err)
					return
				}
				for _, migrSched := range migrationSchedules.Items {
					isMigrSchedRelevant, err := utils.DoesMigrationScheduleMigrateNamespaces(migrSched, deactivationNamespaces)
					if err != nil {
						util.CheckErr(err)
						return
					}
					if isMigrSchedRelevant && migrSched.GetAnnotations() != nil {
						if _, ok := migrSched.GetAnnotations()[StorkMigrationScheduleCopied]; ok {
							// check status of all migrated app in cluster
							migrSched.Status.ApplicationActivated = false
							_, err := storkops.Instance().UpdateMigrationSchedule(&migrSched)
							if err != nil {
								util.CheckErr(err)
								return
							}
							printMsg(fmt.Sprintf("Setting the ApplicationActivated status in the MigrationSchedule %v/%v to false", migrSched.Namespace, migrSched.Name), ioStreams.Out)
						}
					}
				}
			}

		},
	}
	deactivateMigrationCommand.Flags().BoolVarP(&allNamespaces, "all-namespaces", "a", false, "Deactivate applications in all namespaces")

	return deactivateMigrationCommand
}

func printFunc(ioStreams genericclioptions.IOStreams) func(string, string) {
	return func(msg, stream string) {
		switch stream {
		case "out":
			printMsg(msg, ioStreams.Out)
		case "err":
			printMsg(msg, ioStreams.ErrOut)
		default:
			printMsg("printFunc received invalid stream", ioStreams.ErrOut)
			printMsg(msg, ioStreams.ErrOut)
		}
	}
}

func newGetMigrationCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var clusterPair string
	getMigrationCommand := &cobra.Command{
		Use:     migrationSubcommand,
		Aliases: migrationAliases,
		Short:   "Get migration resources",
		Run: func(c *cobra.Command, args []string) {
			var migrations *storkv1.MigrationList
			var err error
			namespaces, err := cmdFactory.GetAllNamespaces()
			if err != nil {
				util.CheckErr(err)
				return
			}
			if len(args) > 0 {
				migrations = new(storkv1.MigrationList)
				for _, migrationName := range args {
					for _, ns := range namespaces {
						migration, err := storkops.Instance().GetMigration(migrationName, ns)
						if err != nil {
							util.CheckErr(err)
							return
						}
						migrations.Items = append(migrations.Items, *migration)
					}
				}
			} else {
				var tempMigrations storkv1.MigrationList
				for _, ns := range namespaces {
					migrations, err = storkops.Instance().ListMigrations(ns)
					if err != nil {
						util.CheckErr(err)
						return
					}
					tempMigrations.Items = append(tempMigrations.Items, migrations.Items...)
				}
				migrations = &tempMigrations
			}

			if len(clusterPair) != 0 {
				var tempMigrations storkv1.MigrationList

				for _, migration := range migrations.Items {
					if migration.Spec.ClusterPair == clusterPair {
						tempMigrations.Items = append(tempMigrations.Items, migration)
						continue
					}
				}
				migrations = &tempMigrations
			}

			if len(migrations.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			if cmdFactory.IsWatchSet() {
				if err := printObjectsWithWatch(c, migrations, cmdFactory, migrationColumns, migrationPrinter, ioStreams.Out); err != nil {
					util.CheckErr(err)
					return
				}
				return
			}
			if err := printObjects(c, migrations, cmdFactory, migrationColumns, migrationPrinter, ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}
	getMigrationCommand.Flags().StringVarP(&clusterPair, "clusterpair", "c", "", "Name of the cluster pair for which to list migrations")
	cmdFactory.BindGetFlags(getMigrationCommand.Flags())

	return getMigrationCommand
}

func newDeleteMigrationCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var clusterPair string
	deleteMigrationCommand := &cobra.Command{
		Use:     migrationSubcommand,
		Aliases: migrationAliases,
		Short:   "Delete migration resources",
		Run: func(c *cobra.Command, args []string) {
			var migrations []string

			if len(clusterPair) == 0 {
				if len(args) == 0 {
					util.CheckErr(fmt.Errorf("at least one argument needs to be provided for migration name"))
					return
				}
				migrations = args
			} else {
				migrationList, err := storkops.Instance().ListMigrations(cmdFactory.GetNamespace())
				if err != nil {
					util.CheckErr(err)
					return
				}
				for _, migration := range migrationList.Items {
					if migration.Spec.ClusterPair == clusterPair {
						migrations = append(migrations, migration.Name)
					}
				}
			}

			if len(migrations) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}

			deleteMigrations(migrations, cmdFactory.GetNamespace(), ioStreams)
		},
	}
	deleteMigrationCommand.Flags().StringVarP(&clusterPair, "clusterPair", "c", "", "Name of the ClusterPair for which to delete ALL migrations")

	return deleteMigrationCommand
}

func deleteMigrations(migrations []string, namespace string, ioStreams genericclioptions.IOStreams) {
	for _, migration := range migrations {
		err := storkops.Instance().DeleteMigration(migration, namespace)
		if err != nil {
			util.CheckErr(err)
			return
		}
		msg := fmt.Sprintf("Migration %v deleted successfully", migration)
		printMsg(msg, ioStreams.Out)
	}
}

func migrationPrinter(
	migrationList *storkv1.MigrationList,
	options printers.GenerateOptions,
) ([]metav1beta1.TableRow, error) {
	if migrationList == nil {
		return nil, nil
	}

	rows := make([]metav1beta1.TableRow, 0)
	for _, migration := range migrationList.Items {
		var row metav1beta1.TableRow
		creationTime := toTimeString(migration.CreationTimestamp.Time)
		if migration.Status.Summary != nil {
			resourceStatus := strconv.FormatUint(migration.Status.Summary.NumberOfMigratedResources, 10) +
				"/" + strconv.FormatUint(migration.Status.Summary.TotalNumberOfResources, 10)
			volumeStatus := strconv.FormatUint(migration.Status.Summary.NumberOfMigratedVolumes, 10) +
				"/" + strconv.FormatUint(migration.Status.Summary.TotalNumberOfVolumes, 10)

			elapsed := "Volumes (" + migration.Status.Summary.ElapsedTimeForVolumeMigration + ") " +
				"Resources (" + migration.Status.Summary.ElapsedTimeForResourceMigration + ")"
			row = getRow(&migration,
				[]interface{}{migration.Name,
					migration.Spec.ClusterPair,
					migration.Status.Stage,
					migration.Status.Status,
					volumeStatus,
					resourceStatus,
					creationTime,
					elapsed,
					strconv.FormatUint(migration.Status.Summary.TotalBytesMigrated, 10),
				},
			)
		} else {
			// Keeping this else for backward compatibility where there
			// is a migration object which does not have MigrationSummary
			volumeStatus := "N/A"
			if migration.Spec.IncludeVolumes == nil || *migration.Spec.IncludeVolumes {
				totalVolumes := len(migration.Status.Volumes)
				doneVolumes := 0
				for _, volume := range migration.Status.Volumes {
					if volume.Status == storkv1.MigrationStatusSuccessful {
						doneVolumes++
					}
				}
				volumeStatus = fmt.Sprintf("%v/%v", doneVolumes, totalVolumes)
			}

			resourceStatus := "N/A"
			if migration.Spec.IncludeResources == nil || *migration.Spec.IncludeResources {
				totalResources := len(migration.Status.Resources)
				doneResources := 0
				for _, resource := range migration.Status.Resources {
					if resource.Status == storkv1.MigrationStatusSuccessful {
						doneResources++
					}
				}
				resourceStatus = fmt.Sprintf("%v/%v", doneResources, totalResources)
			}

			elapsed := ""
			if !migration.CreationTimestamp.IsZero() {
				if migration.Status.Stage == storkv1.MigrationStageFinal {
					if !migration.Status.FinishTimestamp.IsZero() {
						elapsed = migration.Status.FinishTimestamp.Sub(migration.CreationTimestamp.Time).String()
					}
				} else {
					elapsed = time.Since(migration.CreationTimestamp.Time).String()
				}
			}
			row = getRow(&migration,
				[]interface{}{migration.Name,
					migration.Spec.ClusterPair,
					migration.Status.Stage,
					migration.Status.Status,
					volumeStatus,
					resourceStatus,
					creationTime,
					elapsed,
					"Available with stork v2.9.0+",
				},
			)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func waitForMigration(name, namespace string, ioStreams genericclioptions.IOStreams) (string, error) {
	var msg string
	var err error

	log.SetFlags(0)
	log.SetOutput(io.Discard)
	heading := fmt.Sprintf("%s\t\t%-20s", stage, status)
	printMsg(heading, ioStreams.Out)
	t := func() (interface{}, bool, error) {
		migrResp, err := storkops.Instance().GetMigration(name, namespace)
		if err != nil {
			util.CheckErr(err)
			return "", false, err
		}
		stat := fmt.Sprintf("%s\t\t%-20s", migrResp.Status.Stage, migrResp.Status.Status)
		printMsg(stat, ioStreams.Out)
		if migrResp.Status.Status == storkv1.MigrationStatusSuccessful ||
			migrResp.Status.Status == storkv1.MigrationStatusPartialSuccess {
			msg = fmt.Sprintf("Migration %v completed successfully", name)
			return "", false, nil
		}
		if migrResp.Status.Status == storkv1.MigrationStatusFailed {
			msg = fmt.Sprintf("Migration %v failed", name)
			return "", false, nil
		}
		return "", true, fmt.Errorf("%v", migrResp.Status.Status)
	}
	// sleep just so that instead of blank initial stage/status,
	// we have something at start
	time.Sleep(5 * time.Second)
	if _, err = task.DoRetryWithTimeout(t, migrTimeout, migrRetryTimeout); err != nil {
		msg = "Timed out performing task"
	}

	return msg, err
}

func validateMigrationFromFile(migrSpec, namespace string) error {
	if migrSpec == "" {
		return fmt.Errorf("empty file path")

	}
	file, err := os.Open(migrSpec)
	if err != nil {
		return fmt.Errorf("error opening file %v: %v", migrSpec, err)
	}
	data, err := io.ReadAll(bufio.NewReader(file))
	if err != nil {
		return fmt.Errorf("error reading file %v: %v", migrSpec, err)
	}
	migration := &storkv1.Migration{}
	dec := yaml.NewYAMLOrJSONDecoder(bytes.NewReader([]byte(data)), len(data))
	if err := dec.Decode(&migration); err != nil {
		return err
	}
	if namespace == "" {
		return fmt.Errorf("empty namespace given")

	}
	if migration.Namespace == "" {
		migration.Namespace = namespace
	}

	if err := ValidateMigration(migration); err != nil {
		return err
	}
	return nil

}

// ValidateMigration of given name and namespace
func ValidateMigration(migr *storkv1.Migration) error {
	// check namespaces exists
	status := statusOk
	fmt.Printf("[Optional] Checking Migration Namespaces....")
	for _, namespace := range migr.Spec.Namespaces {
		_, err := core.Instance().GetNamespace(namespace)
		if err != nil {
			status = fmt.Sprintf("Missing namespace %v\n", namespace)
		}
	}
	fmt.Print(status)
	// check clusterpair
	fmt.Printf("[Required] Checking ClusterPair Status....")
	if migr.Spec.ClusterPair == "" {
		status = "ClusterPair is empty\n"
	}
	cp, err := storkops.Instance().GetClusterPair(migr.Spec.ClusterPair, migr.Namespace)
	if err != nil {
		return err
	}
	if cp.Status.StorageStatus != storkv1.ClusterPairStatusReady ||
		cp.Status.SchedulerStatus != storkv1.ClusterPairStatusReady {
		status = "NotReady\n"
	}
	fmt.Print(status)

	fmt.Printf("[Optional] Checking PreExec Rule....")
	status = statusOk
	if migr.Spec.PreExecRule == "" {
		status = "NotConfigured\n"
	} else {
		if _, err := storkops.Instance().GetRule(migr.Spec.PreExecRule, migr.Namespace); err != nil && errors.IsNotFound(err) {
			status = "NotFound\n"
		} else if err != nil {
			return err
		}
	}
	fmt.Print(status)

	fmt.Printf("[Optional] Checking PostExec Rule....")
	status = statusOk
	if migr.Spec.PostExecRule == "" {
		status = "NotConfigured\n"
	} else {
		if _, err := storkops.Instance().GetRule(migr.Spec.PostExecRule, migr.Namespace); err != nil && errors.IsNotFound(err) {
			status = "NotFound\n"
		} else if err != nil {
			return err
		}
	}
	fmt.Print(status)

	fmt.Printf("All Ready!!\n")
	return nil
}

func GetMigrationScheduleCRNamespace(namespace string) string {
	var migrationScheduleNamespace string
	dynamicClient := dynamicops.Instance()
	// Checking in following resources for migrationschedule namespace annotation
	resources := []struct {
		kind       string
		apiVersion string
	}{
		{"PersistentVolumeClaim", "v1"},
		{"PersistentVolume", "v1"},
		{"StatefulSet", "apps/v1"},
		{"Deployment", "apps/v1"},
		{"DeploymentConfig", "apps.openshift.io/v1"},
		{"Service", "v1"},
		{"ConfigMap", "v1"},
		{"DaemonSet", "apps/v1"},
		{"ReplicaSet", "apps/v1"},
		{"Secret", "v1"},
		{"ServiceAccount", "v1"},
		{"ResourceQuota", "v1"},
		{"Role", "rbac.authorization.k8s.io/v1"},
		{"RoleBinding", "rbac.authorization.k8s.io/v1"},
		{"ClusterRole", "rbac.authorization.k8s.io/v1"},
		{"ClusterRoleBinding", "rbac.authorization.k8s.io/v1"},
		{"Job", "batch/v1"},
		{"CronJob", "batch/v1"},
		{"LimitRange", "v1"},
		{"PodDisruptionBudget", "policy/v1"},
		{"ValidatingWebhookConfiguration", "admissionregistration.k8s.io/v1"},
		{"MutatingWebhookConfiguration", "admissionregistration.k8s.io/v1"},
		{"ImageStream", "image.openshift.io/v1"},
		{"Template", "template.openshift.io/v1"},
		{"Route", "route.openshift.io/v1"},
	}
	for _, resource := range resources {
		objects, err := dynamicClient.ListObjects(&metav1.ListOptions{
			TypeMeta: metav1.TypeMeta{
				Kind:       resource.kind,
				APIVersion: resource.apiVersion,
			},
		}, namespace)
		if err != nil {
			continue
		}
		for _, o := range objects.Items {
			if _, ok := o.GetAnnotations()[StorkMigrationNamespace]; ok {
				migrationScheduleNamespace = o.GetAnnotations()[StorkMigrationNamespace]
				if len(migrationScheduleNamespace) > 0 {
					break
				}
			}
		}
		if len(migrationScheduleNamespace) > 0 {
			break
		}
	}
	return migrationScheduleNamespace
}
