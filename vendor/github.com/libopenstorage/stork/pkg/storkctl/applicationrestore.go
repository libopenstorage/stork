package storkctl

import (
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubernetes/pkg/printers"
)

var (
	restoreStatusRetryInterval = 30 * time.Second
	restoreStatusRetryTimeout  = 6 * time.Hour
)

var applicationRestoreColumns = []string{"NAME", "STAGE", "STATUS", "VOLUMES", "RESOURCES", "CREATED", "ELAPSED"}
var applicationRestoreSubcommand = "applicationrestores"
var applicationRestoreAliases = []string{"applicationrestore", "apprestore", "apprestores"}

func newCreateApplicationRestoreCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var applicationRestoreName string
	var backupLocation string
	var waitForCompletion bool
	var backupName string
	var replacePolicy string
	var resources string
	var nsMapping string

	createApplicationRestoreCommand := &cobra.Command{
		Use:     applicationRestoreSubcommand,
		Aliases: applicationRestoreAliases,
		Short:   "Start an applicationRestore",
		Run: func(c *cobra.Command, args []string) {
			if len(args) != 1 {
				util.CheckErr(fmt.Errorf("exactly one name needs to be provided for applicationrestore name"))
				return
			}
			if backupLocation == "" {
				util.CheckErr(fmt.Errorf("need to provide BackupLocation to use for restore"))
				return
			}
			_, err := storkops.Instance().GetBackupLocation(backupLocation, cmdFactory.GetNamespace())
			if err != nil {
				util.CheckErr(fmt.Errorf("backuplocation %s does not exist in namespace %s", backupLocation, cmdFactory.GetNamespace()))
				return
			}

			if backupName == "" {
				util.CheckErr(fmt.Errorf("need to provide BackupName to restore"))
				return
			}

			backup, err := storkops.Instance().GetApplicationBackup(backupName, cmdFactory.GetNamespace())
			if err != nil {
				util.CheckErr(fmt.Errorf("applicationbackup %s does not exist in namespace %s", backupName, cmdFactory.GetNamespace()))
				return
			}
			backupObjectsMap := getBackupObjectsMap(backup)

			applicationRestoreName = args[0]
			applicationRestore := &storkv1.ApplicationRestore{
				Spec: storkv1.ApplicationRestoreSpec{
					BackupLocation: backupLocation,
					BackupName:     backupName,
					ReplacePolicy:  storkv1.ApplicationRestoreReplacePolicyType(replacePolicy),
				},
			}
			if len(resources) > 0 {
				objects, err := getObjectInfos(resources, backupObjectsMap, ioStreams)
				if err != nil {
					util.CheckErr(fmt.Errorf("error in creating applicationrestore: %v", err))
					return
				}
				applicationRestore.Spec.IncludeResources = objects
			}

			applicationRestore.Spec.NamespaceMapping, err = getNamespaceMapping(backup, nsMapping)
			if err != nil {
				util.CheckErr(err)
				return
			}
			applicationRestore.Name = applicationRestoreName
			applicationRestore.Namespace = cmdFactory.GetNamespace()
			_, err = storkops.Instance().CreateApplicationRestore(applicationRestore)
			if err != nil {
				util.CheckErr(err)
				return
			}

			msg := "ApplicationRestore " + applicationRestoreName + " started successfully"
			printMsg(msg, ioStreams.Out)

			if waitForCompletion {
				msg, err := waitForApplicationRestore(applicationRestore.Name, applicationRestore.Namespace, ioStreams)
				if err != nil {
					util.CheckErr(err)
					return
				}
				printMsg(msg, ioStreams.Out)
			}
		},
	}
	createApplicationRestoreCommand.Flags().BoolVarP(&waitForCompletion, "wait", "", false, "Wait for applicationrestore to complete")
	createApplicationRestoreCommand.Flags().StringVarP(&backupLocation, "backupLocation", "l", "", "BackupLocation to use for the restore")
	createApplicationRestoreCommand.Flags().StringVarP(&backupName, "backupName", "b", "", "Backup to restore from")
	createApplicationRestoreCommand.Flags().StringVarP(&replacePolicy, "replacePolicy", "r", "Retain", "Policy to use if resources being restored already exist (Retain or Delete).")
	createApplicationRestoreCommand.Flags().StringVarP(&nsMapping, "namespaceMapping", "", "", "Namespace mapping for each of the backed up namespaces, ex: <\"srcns1:destns1,srcns2:destns2\">")
	createApplicationRestoreCommand.Flags().StringVarP(&resources, "resources", "", "",
		"Specific resources for restoring, should be given in format \"<kind>/<namespace>/<name>,<kind>/<namespace>/<name>,<kind>/<name>\", ex: \"<Deployment>/<ns1>/<dep1>,<PersistentVolumeClaim>/<ns1>/<pvc1>,<ClusterRole>/<clusterrole1>\"")

	return createApplicationRestoreCommand
}

func newGetApplicationRestoreCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	getApplicationRestoreCommand := &cobra.Command{
		Use:     applicationRestoreSubcommand,
		Aliases: applicationRestoreAliases,
		Short:   "Get applicationrestore resources",
		Run: func(c *cobra.Command, args []string) {
			var applicationRestores *storkv1.ApplicationRestoreList
			var err error

			namespaces, err := cmdFactory.GetAllNamespaces()
			if err != nil {
				util.CheckErr(err)
				return
			}
			if len(args) > 0 {
				applicationRestores = new(storkv1.ApplicationRestoreList)
				for _, applicationRestoreName := range args {
					for _, ns := range namespaces {
						applicationRestore, err := storkops.Instance().GetApplicationRestore(applicationRestoreName, ns)
						if err != nil {
							util.CheckErr(err)
							return
						}
						applicationRestores.Items = append(applicationRestores.Items, *applicationRestore)
					}
				}
			} else {
				var tempApplicationRestores storkv1.ApplicationRestoreList
				for _, ns := range namespaces {
					applicationRestores, err = storkops.Instance().ListApplicationRestores(ns, metav1.ListOptions{})
					if err != nil {
						util.CheckErr(err)
						return
					}
					tempApplicationRestores.Items = append(tempApplicationRestores.Items, applicationRestores.Items...)
				}
				applicationRestores = &tempApplicationRestores
			}

			if len(applicationRestores.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			if cmdFactory.IsWatchSet() {
				if err := printObjectsWithWatch(c, applicationRestores, cmdFactory, applicationRestoreColumns, applicationRestorePrinter, ioStreams.Out); err != nil {
					util.CheckErr(err)
					return
				}
				return
			}
			if err := printObjects(c, applicationRestores, cmdFactory, applicationRestoreColumns, applicationRestorePrinter, ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}
	cmdFactory.BindGetFlags(getApplicationRestoreCommand.Flags())

	return getApplicationRestoreCommand
}

func newDeleteApplicationRestoreCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	deleteApplicationRestoreCommand := &cobra.Command{
		Use:     applicationRestoreSubcommand,
		Aliases: applicationRestoreAliases,
		Short:   "Delete applicationrestore resources",
		Run: func(c *cobra.Command, args []string) {
			var applicationRestores []string

			if len(args) == 0 {
				util.CheckErr(fmt.Errorf("at least one argument needs to be provided for applicationrestore name"))
				return
			}
			applicationRestores = args

			deleteApplicationRestores(applicationRestores, cmdFactory.GetNamespace(), ioStreams)
		},
	}

	return deleteApplicationRestoreCommand
}

func deleteApplicationRestores(applicationRestores []string, namespace string, ioStreams genericclioptions.IOStreams) {
	for _, applicationRestore := range applicationRestores {
		err := storkops.Instance().DeleteApplicationRestore(applicationRestore, namespace)
		if err != nil {
			util.CheckErr(err)
			return
		}
		msg := fmt.Sprintf("ApplicationRestore %v deleted successfully", applicationRestore)
		printMsg(msg, ioStreams.Out)
	}
}

func applicationRestorePrinter(
	applicationRestoreList *storkv1.ApplicationRestoreList,
	options printers.GenerateOptions,
) ([]metav1beta1.TableRow, error) {
	if applicationRestoreList == nil {
		return nil, nil
	}
	rows := make([]metav1beta1.TableRow, 0)
	for _, applicationRestore := range applicationRestoreList.Items {
		name := applicationRestore.Name

		totalVolumes := len(applicationRestore.Status.Volumes)
		doneVolumes := 0
		for _, volume := range applicationRestore.Status.Volumes {
			if volume.Status == storkv1.ApplicationRestoreStatusSuccessful {
				doneVolumes++
			}
		}
		volumeStatus := fmt.Sprintf("%v/%v", doneVolumes, totalVolumes)

		elapsed := ""
		if !applicationRestore.CreationTimestamp.IsZero() {
			if applicationRestore.Status.Stage == storkv1.ApplicationRestoreStageFinal {
				if !applicationRestore.Status.FinishTimestamp.IsZero() {
					elapsed = applicationRestore.Status.FinishTimestamp.Sub(applicationRestore.CreationTimestamp.Time).String()
				}
			} else {
				elapsed = time.Since(applicationRestore.CreationTimestamp.Time).String()
			}
		}

		creationTime := toTimeString(applicationRestore.CreationTimestamp.Time)
		row := getRow(&applicationRestore,
			[]interface{}{name,
				applicationRestore.Status.Stage,
				applicationRestore.Status.Status,
				volumeStatus,
				len(applicationRestore.Status.Resources),
				creationTime,
				elapsed},
		)
		rows = append(rows, row)
	}
	return rows, nil
}

func waitForApplicationRestore(name, namespace string, ioStreams genericclioptions.IOStreams) (string, error) {
	var msg string
	var err error

	log.SetFlags(0)
	log.SetOutput(io.Discard)
	heading := fmt.Sprintf("%s\t\t%-20s", stage, status)
	printMsg(heading, ioStreams.Out)
	t := func() (interface{}, bool, error) {
		restore, err := storkops.Instance().GetApplicationRestore(name, namespace)
		if err != nil {
			util.CheckErr(err)
			return "", false, err
		}
		stat := fmt.Sprintf("%s\t\t%-20s", restore.Status.Stage, restore.Status.Status)
		printMsg(stat, ioStreams.Out)
		if restore.Status.Status == storkv1.ApplicationRestoreStatusSuccessful ||
			restore.Status.Status == storkv1.ApplicationRestoreStatusPartialSuccess {
			msg = fmt.Sprintf("ApplicationRestore %v completed successfully", name)
			return "", false, nil
		}
		if restore.Status.Status == storkv1.ApplicationRestoreStatusFailed {
			msg = fmt.Sprintf("ApplicationRestore %v failed", name)
			return "", false, nil
		}
		return "", true, fmt.Errorf("%v", restore.Status.Status)
	}
	// sleep just so that instead of blank initial stage/status,
	// we have something at start
	time.Sleep(5 * time.Second)
	if _, err = task.DoRetryWithTimeout(t, restoreStatusRetryTimeout, restoreStatusRetryInterval); err != nil {
		msg = "Timed out performing task"
	}

	return msg, err
}

func getNamespaceMapping(backup *storkv1.ApplicationBackup, inputMappings string) (map[string]string, error) {
	inputMappingMap := make(map[string]string)
	outputNSMapping := make(map[string]string)
	if len(inputMappings) > 0 {
		for _, inputMapping := range strings.Split(inputMappings, ",") {
			nsMapping := strings.Split(inputMapping, ":")
			if len(nsMapping) == 2 {
				inputMappingMap[nsMapping[0]] = nsMapping[1]
			} else {
				return outputNSMapping, fmt.Errorf("invalid input namespace mapping %s", inputMapping)
			}
		}
	}
	for _, ns := range backup.Spec.Namespaces {
		outputNSMapping[ns] = ns
		if newNS, ok := inputMappingMap[ns]; ok {
			outputNSMapping[ns] = newNS
		}
	}
	return outputNSMapping, nil
}

func getBackupObjectsMap(backup *storkv1.ApplicationBackup) map[string]*storkv1.ApplicationBackupResourceInfo {
	objectsMap := make(map[string]*storkv1.ApplicationBackupResourceInfo)
	for _, resource := range backup.Status.Resources {
		if len(resource.Namespace) > 0 {
			objKey := getObjectKeyNameInBackupObjectsMap(strings.ToLower(resource.Kind), resource.Namespace, resource.Name)
			objectsMap[objKey] = resource
		} else {
			objKey := getObjectKeyNameInBackupObjectsMap(strings.ToLower(resource.Kind), "", resource.Name)
			objectsMap[objKey] = resource
		}
	}
	return objectsMap
}

func getObjectInfos(resourceString string, backupObjectsMap map[string]*storkv1.ApplicationBackupResourceInfo, ioStreams genericclioptions.IOStreams) ([]storkv1.ObjectInfo, error) {
	objects := make([]storkv1.ObjectInfo, 0)
	var err error

	resources := strings.Split(resourceString, ",")
	for _, resource := range resources {
		// resources will be provided like "<type>/<ns>/<name>,<type>/<name>"
		resourceDetails := strings.Split(resource, "/")
		object := storkv1.ObjectInfo{}
		var objectKey string
		if len(resourceDetails) == 3 {
			object.Name = resourceDetails[2]
			object.Namespace = resourceDetails[1]
			// strict check on name and namespace for matching
			objectKey = getObjectKeyNameInBackupObjectsMap(strings.ToLower(resourceDetails[0]), resourceDetails[1], resourceDetails[2])
		} else if len(resourceDetails) == 2 {
			object.Name = resourceDetails[1]
			objectKey = getObjectKeyNameInBackupObjectsMap(strings.ToLower(resourceDetails[0]), "", resourceDetails[1])
		} else {
			err = fmt.Errorf("unsupported resource input format %s, should follow format \"<kind>/<namespace>/<name>,<kind>/<name>\"", resource)
			return objects, err
		}

		if len(objectKey) > 0 {
			if _, ok := backupObjectsMap[objectKey]; !ok {
				err = fmt.Errorf("error getting resource %s with name %s in applicationbackup", resourceDetails[0], object.Name)
				return objects, err
			}
			backupObject := backupObjectsMap[objectKey]
			object.Kind = backupObject.Kind
			object.Version = backupObject.Version
			object.Group = backupObject.Group
			objects = append(objects, object)
		}
	}
	return objects, nil
}

func getObjectKeyNameInBackupObjectsMap(kind string, name string, namespace string) string {
	if len(namespace) > 0 {
		return fmt.Sprintf("%s_%s_%s", kind, namespace, name)
	} else {
		return fmt.Sprintf("%s_%s", kind, name)
	}
}
