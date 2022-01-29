package storkctl

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-openapi/inflect"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	migration "github.com/libopenstorage/stork/pkg/migration/controllers"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/dynamic"
	"github.com/portworx/sched-ops/k8s/openshift"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	k8sdynamic "k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubernetes/pkg/printers"
)

const (
	migrTimeout = 6 * time.Hour
	stage       = "STAGE"
	status      = "STATUS"
	statusOk    = "Ok\n"
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
					activationNamespaces = append(activationNamespaces, ns.Name)
				}

			} else {
				activationNamespaces = append(activationNamespaces, cmdFactory.GetNamespace())
			}

			for _, ns := range activationNamespaces {
				updateStatefulSets(ns, true, ioStreams)
				updateDeployments(ns, true, ioStreams)
				updateDeploymentConfigs(ns, true, ioStreams)
				updateIBPObjects("IBPPeer", ns, true, ioStreams)
				updateIBPObjects("IBPCA", ns, true, ioStreams)
				updateIBPObjects("IBPOrderer", ns, true, ioStreams)
				updateIBPObjects("IBPConsole", ns, true, ioStreams)
				updateCRDObjects(ns, true, ioStreams, config)
				updateCronJobObjects(ns, true, ioStreams)
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

			} else {
				deactivationNamespaces = append(deactivationNamespaces, cmdFactory.GetNamespace())
			}

			for _, ns := range deactivationNamespaces {
				updateStatefulSets(ns, false, ioStreams)
				updateDeployments(ns, false, ioStreams)
				updateDeploymentConfigs(ns, false, ioStreams)
				updateIBPObjects("IBPPeer", ns, false, ioStreams)
				updateIBPObjects("IBPCA", ns, false, ioStreams)
				updateIBPObjects("IBPOrderer", ns, false, ioStreams)
				updateIBPObjects("IBPConsole", ns, false, ioStreams)
				updateCRDObjects(ns, false, ioStreams, config)
				updateCronJobObjects(ns, false, ioStreams)
			}

		},
	}
	deactivateMigrationCommand.Flags().BoolVarP(&allNamespaces, "all-namespaces", "a", false, "Deactivate applications in all namespaces")

	return deactivateMigrationCommand
}

func updateStatefulSets(namespace string, activate bool, ioStreams genericclioptions.IOStreams) {
	statefulSets, err := apps.Instance().ListStatefulSets(namespace)
	if err != nil {
		util.CheckErr(err)
		return
	}
	for _, statefulSet := range statefulSets.Items {
		if replicas, update := getUpdatedReplicaCount(statefulSet.Annotations, activate, ioStreams); update {
			statefulSet.Spec.Replicas = &replicas
			_, err := apps.Instance().UpdateStatefulSet(&statefulSet)
			if err != nil {
				printMsg(fmt.Sprintf("Error updating replicas for statefulset %v/%v : %v", statefulSet.Namespace, statefulSet.Name, err), ioStreams.ErrOut)
				continue
			}
			printMsg(fmt.Sprintf("Updated replicas for statefulset %v/%v to %v", statefulSet.Namespace, statefulSet.Name, replicas), ioStreams.Out)
		}

	}
}

func updateDeployments(namespace string, activate bool, ioStreams genericclioptions.IOStreams) {
	deployments, err := apps.Instance().ListDeployments(namespace, metav1.ListOptions{})
	if err != nil {
		util.CheckErr(err)
		return
	}
	for _, deployment := range deployments.Items {
		if replicas, update := getUpdatedReplicaCount(deployment.Annotations, activate, ioStreams); update {
			deployment.Spec.Replicas = &replicas
			_, err := apps.Instance().UpdateDeployment(&deployment)
			if err != nil {
				printMsg(fmt.Sprintf("Error updating replicas for deployment %v/%v : %v", deployment.Namespace, deployment.Name, err), ioStreams.ErrOut)
				continue
			}
			printMsg(fmt.Sprintf("Updated replicas for deployment %v/%v to %v", deployment.Namespace, deployment.Name, replicas), ioStreams.Out)
		}
	}
}

func updateDeploymentConfigs(namespace string, activate bool, ioStreams genericclioptions.IOStreams) {
	deployments, err := openshift.Instance().ListDeploymentConfigs(namespace)
	if err != nil {
		if !errors.IsNotFound(err) {
			util.CheckErr(err)
		}
		return
	}
	for _, deployment := range deployments.Items {
		if replicas, update := getUpdatedReplicaCount(deployment.Annotations, activate, ioStreams); update {
			deployment.Spec.Replicas = replicas
			_, err := openshift.Instance().UpdateDeploymentConfig(&deployment)
			if err != nil {
				printMsg(fmt.Sprintf("Error updating replicas for deploymentconfig %v/%v : %v", deployment.Namespace, deployment.Name, err), ioStreams.ErrOut)
				continue
			}
			printMsg(fmt.Sprintf("Updated replicas for deploymentconfig %v/%v to %v", deployment.Namespace, deployment.Name, replicas), ioStreams.Out)
		}
	}
}

func updateCRDObjects(ns string, activate bool, ioStreams genericclioptions.IOStreams, config *rest.Config) {
	crdList, err := storkops.Instance().ListApplicationRegistrations()
	if err != nil {
		util.CheckErr(err)
		return
	}
	configClient, err := k8sdynamic.NewForConfig(config)
	if err != nil {
		util.CheckErr(err)
		return
	}
	ruleset := inflect.NewDefaultRuleset()
	ruleset.AddPlural("quota", "quotas")
	ruleset.AddPlural("prometheus", "prometheuses")
	ruleset.AddPlural("mongodbcommunity", "mongodbcommunity")
	for _, res := range crdList.Items {
		for _, crd := range res.Resources {
			var client k8sdynamic.ResourceInterface
			opts := &metav1.ListOptions{
				TypeMeta: metav1.TypeMeta{
					Kind:       crd.Kind,
					APIVersion: crd.Group + "/" + crd.Version},
			}
			gvk := schema.FromAPIVersionAndKind(opts.APIVersion, opts.Kind)
			resourceInterface := configClient.Resource(gvk.GroupVersion().WithResource(ruleset.Pluralize(strings.ToLower(gvk.Kind))))
			client = resourceInterface.Namespace(ns)
			objects, err := client.List(context.TODO(), *opts)
			if err != nil {
				if errors.IsNotFound(err) {
					continue
				}
				util.CheckErr(err)
				return
			}
			for _, o := range objects.Items {
				specPath := strings.Split(crd.SuspendOptions.Path, ".")
				if len(specPath) > 1 {
					var disableVersion interface{}
					if crd.SuspendOptions.Type == "bool" {
						disableVersion = !activate
					} else if crd.SuspendOptions.Type == "int" {
						replicas, _ := getSuspendIntOpts(o.GetAnnotations(), activate, ioStreams)
						disableVersion = replicas
					} else if crd.SuspendOptions.Type == "string" {
						suspend, err := getSuspendStringOpts(o.GetAnnotations(), activate, ioStreams)
						if err != nil {
							util.CheckErr(err)
							return
						}
						disableVersion = suspend
					} else {
						util.CheckErr(fmt.Errorf("invalid type %v to suspend cr", crd.SuspendOptions.Type))
						return
					}
					err := unstructured.SetNestedField(o.Object, disableVersion, specPath...)
					if err != nil {
						printMsg(fmt.Sprintf("Error updating \"%v\" for %v %v/%v to %v : %v", crd.SuspendOptions.Path, strings.ToLower(crd.Kind), o.GetNamespace(), o.GetName(), disableVersion, err), ioStreams.ErrOut)
						continue
					}

					_, err = client.Update(context.TODO(), &o, metav1.UpdateOptions{}, "")
					if err != nil {
						printMsg(fmt.Sprintf("Error updating \"%v\" for %v %v/%v to %v : %v", crd.SuspendOptions.Path, strings.ToLower(crd.Kind), o.GetNamespace(), o.GetName(), disableVersion, err), ioStreams.ErrOut)
						continue
					}
					printMsg(fmt.Sprintf("Updated \"%v\" for %v %v/%v to %v", crd.SuspendOptions.Path, strings.ToLower(crd.Kind), o.GetNamespace(), o.GetName(), disableVersion), ioStreams.Out)
					if !activate {
						if crd.PodsPath == "" {
							continue
						}
						podpath := strings.Split(crd.PodsPath, ".")
						pods, found, err := unstructured.NestedStringSlice(o.Object, podpath...)
						if err != nil {
							printMsg(fmt.Sprintf("Error getting pods for %v %v/%v : %v", strings.ToLower(crd.Kind), o.GetNamespace(), o.GetName(), err), ioStreams.ErrOut)
							continue
						}
						if !found {
							continue
						}
						for _, pod := range pods {
							err = core.Instance().DeletePod(o.GetNamespace(), pod, true)
							printMsg(fmt.Sprintf("Error deleting pod %v for %v %v/%v : %v", pod, strings.ToLower(crd.Kind), o.GetNamespace(), o.GetName(), err), ioStreams.ErrOut)
							continue
						}
					}
				}

			}

		}
	}
}
func updateIBPObjects(kind string, namespace string, activate bool, ioStreams genericclioptions.IOStreams) {
	objects, err := dynamic.Instance().ListObjects(
		&metav1.ListOptions{
			TypeMeta: metav1.TypeMeta{
				Kind:       kind,
				APIVersion: "ibp.com/v1alpha1"},
		},
		namespace)
	if err != nil {
		if !errors.IsNotFound(err) {
			util.CheckErr(err)
		}
		return
	}
	for _, o := range objects.Items {
		if replicas, update := getUpdatedReplicaCount(o.GetAnnotations(), activate, ioStreams); update {
			err := unstructured.SetNestedField(o.Object, int64(replicas), "spec", "replicas")
			if err != nil {
				printMsg(fmt.Sprintf("Error updating replicas for %v %v/%v : %v", strings.ToLower(kind), o.GetNamespace(), o.GetName(), err), ioStreams.ErrOut)
				continue
			}
			_, err = dynamic.Instance().UpdateObject(&o)
			if err != nil {
				printMsg(fmt.Sprintf("Error updating replicas for %v %v/%v : %v", strings.ToLower(kind), o.GetNamespace(), o.GetName(), err), ioStreams.ErrOut)
				continue
			}
			printMsg(fmt.Sprintf("Updated replicas for %v %v/%v to %v", strings.ToLower(kind), o.GetNamespace(), o.GetName(), replicas), ioStreams.Out)
		}
	}
}

func updateCronJobObjects(namespace string, activate bool, ioStreams genericclioptions.IOStreams) {
	cronJobs, err := batch.Instance().ListCronJobs(namespace)
	if err != nil {
		util.CheckErr(err)
		return
	}

	for _, cronJob := range cronJobs.Items {
		*cronJob.Spec.Suspend = !activate
		_, err = batch.Instance().UpdateCronJob(&cronJob)
		if err != nil {
			printMsg(fmt.Sprintf("Error updating suspend option for cronJob %v/%v : %v", cronJob.Namespace, cronJob.Name, err), ioStreams.ErrOut)
			continue
		}
		printMsg(fmt.Sprintf("Updated suspend option for cronjob %v/%v to %v", cronJob.Namespace, cronJob.Name, !activate), ioStreams.Out)
	}

}
func getSuspendStringOpts(annotations map[string]string, activate bool, ioStreams genericclioptions.IOStreams) (string, error) {
	crdOpts := migration.StorkMigrationCRDActivateAnnotation
	if !activate {
		crdOpts = migration.StorkMigrationCRDDeactivateAnnotation
	}
	suspend, present := annotations[crdOpts]
	if !present {
		return "", fmt.Errorf("required migration annotation not found %s", crdOpts)
	}
	return suspend, nil
}

func getSuspendIntOpts(annotations map[string]string, activate bool, ioStreams genericclioptions.IOStreams) (int64, bool) {
	if intOpts, present := annotations[migration.StorkMigrationCRDActivateAnnotation]; present {
		var replicas int64
		if activate {
			parsedReplicas, err := strconv.Atoi(intOpts)
			if err != nil {
				printMsg(fmt.Sprintf("Error parsing replicas for app : %v", err), ioStreams.ErrOut)
				return 0, false
			}
			replicas = int64(parsedReplicas)
		} else {
			replicas = 0
		}
		return replicas, true
	}

	return 0, false
}

func getUpdatedReplicaCount(annotations map[string]string, activate bool, ioStreams genericclioptions.IOStreams) (int32, bool) {
	if replicas, present := annotations[migration.StorkMigrationReplicasAnnotation]; present {
		var updatedReplicas int32
		if activate {
			parsedReplicas, err := strconv.Atoi(replicas)
			if err != nil {
				printMsg(fmt.Sprintf("Error parsing replicas for app : %v", err), ioStreams.ErrOut)
				return 0, false
			}
			updatedReplicas = int32(parsedReplicas)
		} else {
			updatedReplicas = 0
		}
		return updatedReplicas, true
	}

	return 0, false
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

			row = getRow(&migration,
				[]interface{}{migration.Name,
					migration.Spec.ClusterPair,
					migration.Status.Stage,
					migration.Status.Status,
					volumeStatus,
					resourceStatus,
					creationTime,
					migration.Status.Summary.ElapsedTime,
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
	log.SetOutput(ioutil.Discard)
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
	data, err := ioutil.ReadAll(bufio.NewReader(file))
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
