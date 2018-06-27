package storkctl

import (
	"fmt"
	"io"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/pkg/printers"
)

var migrationColumns = []string{"NAME", "CLUSTERPAIR", "STAGE", "STATUS", "VOLUMES", "RESOURCES", "CREATED"}
var migrationSubcommand = "migrations"
var migrationAliases = []string{"migration"}

func newCreateMigrationCommand(cmdFactory Factory) *cobra.Command {
	var migrationName string
	var clusterPair string
	var namespaceList []string
	var includeResources bool
	var startApplications bool

	createMigrationCommand := &cobra.Command{
		Use:     migrationSubcommand,
		Aliases: migrationAliases,
		Short:   "Start a migration",
		PreRunE: cobra.ExactArgs(1),
		Run: func(c *cobra.Command, args []string) {
			if len(args) != 1 {
				handleError(fmt.Errorf("Exactly one name needs to be provided for migration name"))
			} else {
				migrationName = args[0]
			}
			if len(clusterPair) == 0 {
				handleError(fmt.Errorf("ClusterPair name needs to be provided for migration"))
			}
			if len(namespaceList) == 0 {
				handleError(fmt.Errorf("Need to provide atleast one namespace to migrate"))
			}

			migration := &storkv1.Migration{
				Spec: storkv1.MigrationSpec{
					ClusterPair:       clusterPair,
					Namespaces:        namespaceList,
					IncludeResources:  includeResources,
					StartApplications: startApplications,
				},
			}
			migration.Name = migrationName
			err := k8s.Instance().CreateMigration(migration)
			if err != nil {
				handleError(err)
			}
			fmt.Printf("Migration %v created successfully\n", migration.Name)
		},
	}
	createMigrationCommand.Flags().StringSliceVarP(&namespaceList, "namespaces", "", nil, "Comma separated list of namespaces to migrate")
	createMigrationCommand.Flags().StringVarP(&clusterPair, "clusterPair", "c", "", "ClusterPair name for migration")
	createMigrationCommand.Flags().BoolVarP(&includeResources, "includeResources", "r", true, "Include resources in the migration")
	createMigrationCommand.Flags().BoolVarP(&startApplications, "startApplications", "a", true, "Start applications on the destination cluster after migration")

	return createMigrationCommand
}

func newGetMigrationCommand(cmdFactory Factory) *cobra.Command {
	var clusterPair string
	getMigrationCommand := &cobra.Command{
		Use:     migrationSubcommand,
		Aliases: migrationAliases,
		Short:   "Get migration resources",
		Run: func(c *cobra.Command, args []string) {
			var migrations *storkv1.MigrationList
			var err error

			if len(args) > 0 {
				migrations = new(storkv1.MigrationList)
				for _, migrationName := range args {
					migration, err := k8s.Instance().GetMigration(migrationName)
					if err != nil {
						handleError(err)
					}
					migrations.Items = append(migrations.Items, *migration)
				}
			} else {
				migrations, err = k8s.Instance().ListMigrations()
				if err != nil {
					handleError(err)
				}
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
				handleEmptyList()
				return
			}

			outputFormat, err := cmdFactory.GetOutputFormat()
			if err != nil {
				handleError(err)
			}

			if err := printObjects(c, migrations, outputFormat, migrationColumns, migrationPrinter); err != nil {
				handleError(err)
			}
		},
	}
	getMigrationCommand.Flags().StringVarP(&clusterPair, "clusterpair", "c", "", "Name of the cluster pair for which to list migrations")

	return getMigrationCommand
}

func newDeleteMigrationCommand(cmdFactory Factory) *cobra.Command {
	var clusterPair string
	deleteMigrationCommand := &cobra.Command{
		Use:     migrationSubcommand,
		Aliases: migrationAliases,
		Short:   "Delete migration resources",
		Run: func(c *cobra.Command, args []string) {
			var migrations []string

			if len(clusterPair) == 0 {
				if len(args) == 0 {
					handleError(fmt.Errorf("Atleast one argument needs to be provided for migration name"))
				}
				migrations = args
			} else {
				migrationList, err := k8s.Instance().ListMigrations()
				if err != nil {
					handleError(err)
				}
				for _, migration := range migrationList.Items {
					if migration.Spec.ClusterPair == clusterPair {
						migrations = append(migrations, migration.Name)
					}
				}
			}

			if len(migrations) == 0 {
				handleEmptyList()
			}

			deleteMigrations(migrations)
		},
	}
	deleteMigrationCommand.Flags().StringVarP(&clusterPair, "clusterPair", "c", "", "Name of the ClusterPair for which to delete ALL migrations")

	return deleteMigrationCommand
}

func deleteMigrations(migrations []string) {
	for _, migration := range migrations {
		err := k8s.Instance().DeleteMigration(migration)
		if err != nil {
			handleError(err)
		} else {
			fmt.Printf("Migration %v deleted successfully\n", migration)
		}
	}
}

func migrationPrinter(migrationList *storkv1.MigrationList, writer io.Writer, options printers.PrintOptions) error {
	if migrationList == nil {
		return nil
	}
	for _, migration := range migrationList.Items {
		name := printers.FormatResourceName(options.Kind, migration.Name, options.WithKind)

		if options.WithNamespace {
			if _, err := fmt.Fprintf(writer, "%v\t", migration.Namespace); err != nil {
				return err
			}
		}
		totalVolumes := len(migration.Status.Volumes)
		doneVolumes := 0
		for _, volume := range migration.Status.Volumes {
			if volume.Status == storkv1.MigrationStatusSuccessful {
				doneVolumes++
			}
		}
		totalResources := len(migration.Status.Resources)
		doneResources := 0
		for _, resource := range migration.Status.Resources {
			if resource.Status == storkv1.MigrationStatusSuccessful {
				doneResources++
			}
		}

		creationTime := toTimeString(migration.CreationTimestamp)
		if _, err := fmt.Fprintf(writer, "%v\t%v\t%v\t%v\t%v/%v\t%v/%v\t%v\n",
			name,
			migration.Spec.ClusterPair,
			migration.Status.Stage,
			migration.Status.Status,
			doneVolumes,
			totalVolumes,
			doneResources,
			totalResources,
			creationTime); err != nil {
			return err
		}
	}
	return nil
}
