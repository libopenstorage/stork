package storkctl

import (
	"fmt"
	"io"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/portworx/sched-ops/k8s"
	"github.com/spf13/cobra"
	"k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/genericclioptions"
	"k8s.io/kubernetes/pkg/printers"
)

const (
	backupLocationSubcommand = "backuplocation"
	hiddenString             = "<HIDDEN>"
)

var s3BackupLocationColumns = []string{"NAME", "PATH", "ACCESS-KEY-ID", "SECRET-ACCESS-KEY", "REGION", "ENDPOINT", "SSL-DISABLED"}
var azureBackupLocationColumns = []string{"NAME", "PATH", "STORAGE-ACCOUNT-NAME", "STORAGE-ACCOUNT-KEY"}
var googleBackupLocationColumns = []string{"NAME", "PATH", "PROJECT-ID"}

func newGetBackupLocationCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var showSecrets bool
	getBackupLocationCommand := &cobra.Command{
		Use:     backupLocationSubcommand,
		Aliases: []string{"bl"},
		Short:   "Get BackupLocations",
		Run: func(c *cobra.Command, args []string) {
			var backupLocations *storkv1.BackupLocationList
			if len(args) > 0 {
				if cmdFactory.AllNamespaces() {
					util.CheckErr(fmt.Errorf("a resource cannot be retrieved by name across all namespaces"))
					return
				}

				backupLocations = new(storkv1.BackupLocationList)
				for _, name := range args {
					bl, err := k8s.Instance().GetBackupLocation(name, cmdFactory.GetNamespace())
					if err == nil {
						backupLocations.Items = append(backupLocations.Items, *bl)
					} else {
						util.CheckErr(err)
						return
					}
				}
			} else {
				namespaces, err := cmdFactory.GetAllNamespaces()
				if err != nil {
					util.CheckErr(err)
					return
				}

				var tempBackupLocations storkv1.BackupLocationList
				for _, ns := range namespaces {
					backupLocations, err := k8s.Instance().ListBackupLocations(ns)
					if err != nil {
						util.CheckErr(err)
						return
					}
					tempBackupLocations.Items = append(tempBackupLocations.Items, backupLocations.Items...)

				}
				backupLocations = &tempBackupLocations
			}

			if len(backupLocations.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}

			s3BackupLocations := &storkv1.BackupLocationList{}
			azureBackupLocations := &storkv1.BackupLocationList{}
			googleBackupLocations := &storkv1.BackupLocationList{}
			unknownBackupLocations := &storkv1.BackupLocationList{}
			for _, bl := range backupLocations.Items {
				switch bl.Location.Type {
				case storkv1.BackupLocationS3:
					if !showSecrets && bl.Location.S3Config != nil {
						bl.Location.S3Config.SecretAccessKey = hiddenString
					}
					s3BackupLocations.Items = append(s3BackupLocations.Items, bl)
				case storkv1.BackupLocationAzure:
					if !showSecrets && bl.Location.AzureConfig != nil {
						bl.Location.AzureConfig.StorageAccountKey = hiddenString
					}
					azureBackupLocations.Items = append(azureBackupLocations.Items, bl)
				case storkv1.BackupLocationGoogle:
					if !showSecrets && bl.Location.GoogleConfig != nil {
						bl.Location.GoogleConfig.AccountKey = hiddenString
					}
					googleBackupLocations.Items = append(googleBackupLocations.Items, bl)
				default:
					unknownBackupLocations.Items = append(unknownBackupLocations.Items, bl)
				}
			}
			outputFormat, err := cmdFactory.GetOutputFormat()
			if err != nil {
				util.CheckErr(err)
				return
			}

			if outputFormat == outputFormatTable {
				if len(s3BackupLocations.Items) != 0 {
					if _, err := fmt.Fprintf(ioStreams.Out, "\nS3:\n---\n"); err != nil {
						util.CheckErr(err)
						return
					}
					if err := printObjects(c, s3BackupLocations, cmdFactory, s3BackupLocationColumns, s3BackupLocationPrinter, ioStreams.Out); err != nil {
						util.CheckErr(err)
						return
					}
				}
				if len(googleBackupLocations.Items) != 0 {
					if _, err := fmt.Fprintf(ioStreams.Out, "\nGoogleCloudStorage:\n-------------------\n"); err != nil {
						util.CheckErr(err)
						return
					}
					if err := printObjects(c, googleBackupLocations, cmdFactory, googleBackupLocationColumns, googleBackupLocationPrinter, ioStreams.Out); err != nil {
						util.CheckErr(err)
						return
					}
				}
				if len(azureBackupLocations.Items) != 0 {
					if _, err := fmt.Fprintf(ioStreams.Out, "\nAzureBlob:\n----------\n"); err != nil {
						util.CheckErr(err)
						return
					}
					if err := printObjects(c, azureBackupLocations, cmdFactory, azureBackupLocationColumns, azureBackupLocationPrinter, ioStreams.Out); err != nil {
						util.CheckErr(err)
						return
					}
				}
			} else {
				if err := printObjects(c, backupLocations, cmdFactory, nil, nil, ioStreams.Out); err != nil {
					util.CheckErr(err)
					return
				}
			}
		},
	}

	getBackupLocationCommand.Flags().BoolVarP(&showSecrets, "showSecrets", "s", false, "Display the secret information from the backupLocations")
	cmdFactory.BindGetFlags(getBackupLocationCommand.Flags())

	return getBackupLocationCommand
}

func s3BackupLocationPrinter(backupLocationList *storkv1.BackupLocationList, writer io.Writer, options printers.PrintOptions) error {
	if backupLocationList == nil {
		return nil
	}
	for _, backupLocation := range backupLocationList.Items {
		name := printers.FormatResourceName(options.Kind, backupLocation.Name, options.WithKind)
		if options.WithNamespace {
			if _, err := fmt.Fprintf(writer, "%v\t", backupLocation.Namespace); err != nil {
				return err
			}
		}

		if _, err := fmt.Fprintf(writer, "%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
			name,
			backupLocation.Location.Path,
			backupLocation.Location.S3Config.AccessKeyID,
			backupLocation.Location.S3Config.SecretAccessKey,
			backupLocation.Location.S3Config.Region,
			backupLocation.Location.S3Config.Endpoint,
			backupLocation.Location.S3Config.DisableSSL,
		); err != nil {
			return err
		}
	}
	return nil
}
func azureBackupLocationPrinter(backupLocationList *storkv1.BackupLocationList, writer io.Writer, options printers.PrintOptions) error {
	if backupLocationList == nil {
		return nil
	}
	for _, backupLocation := range backupLocationList.Items {
		name := printers.FormatResourceName(options.Kind, backupLocation.Name, options.WithKind)
		if options.WithNamespace {
			if _, err := fmt.Fprintf(writer, "%v\t", backupLocation.Namespace); err != nil {
				return err
			}
		}

		if _, err := fmt.Fprintf(writer, "%v\t%v\t%v\t%v\n",
			name,
			backupLocation.Location.Path,
			backupLocation.Location.AzureConfig.StorageAccountName,
			backupLocation.Location.AzureConfig.StorageAccountKey,
		); err != nil {
			return err
		}
	}
	return nil
}

func googleBackupLocationPrinter(backupLocationList *storkv1.BackupLocationList, writer io.Writer, options printers.PrintOptions) error {
	if backupLocationList == nil {
		return nil
	}
	for _, backupLocation := range backupLocationList.Items {
		name := printers.FormatResourceName(options.Kind, backupLocation.Name, options.WithKind)
		if options.WithNamespace {
			if _, err := fmt.Fprintf(writer, "%v\t", backupLocation.Namespace); err != nil {
				return err
			}
		}

		if _, err := fmt.Fprintf(writer, "%v\t%v\t%v\n",
			name,
			backupLocation.Location.Path,
			backupLocation.Location.GoogleConfig.ProjectID,
		); err != nil {
			return err
		}
	}
	return nil
}
