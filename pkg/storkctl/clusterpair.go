package storkctl

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"

	clusterclient "github.com/libopenstorage/openstorage/api/client/cluster"
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/utils"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/validation"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubernetes/pkg/printers"
)

const (
	clusterPairSubcommand  = "clusterpair"
	cmdPathKey             = "cmd-path"
	gcloudPath             = "./google-cloud-sdk/bin/gcloud"
	gcloudBinaryName       = "gcloud"
	skipResourceAnnotation = "stork.libopenstorage.org/skip-resource"
	storkCreatedAnnotation = "stork.libopenstorage.org/created-by-stork"
	pxAdminTokenSecret     = "px-admin-token"
	secretNamespace        = "openstorage.io/auth-secret-namespace"
	secretName             = "openstorage.io/auth-secret-name"

	userInputCPModeAsync     = "async-dr"
	userInputCPModeSync      = "sync-dr"
	userInputCPModeMigration = "migration"

	clusterPairDRModeDisasterRecovery = "DisasterRecovery"
	clusterPairDRModeOnetimeMigration = "OneTimeMigration"
)

var (
	clusterPairColumns = []string{"NAME", "STORAGE-STATUS", "SCHEDULER-STATUS", "CREATED"}

	projectMappingHelpString = "Project mappings between source and destination clusters (currently supported only for Rancher).\n" +
		"Use comma-separated <source-project-id>=<dest-project-id> pairs.\n" +
		"For the project-id, you can also have a cluster-id field added as a prefix to the project-id.\n" +
		"It is recommended to include both one-to-one mappings of the project-id and Rancher cluster-id prefixed project-id as follows:\n" +
		"<source-project-id>=<dest-project-id>,<source-cluster-id>:<source-project-id>=<dest-cluster-id>:<dest-project-id>"
)

func newGetClusterPairCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	getClusterPairCommand := &cobra.Command{
		Use:     clusterPairSubcommand,
		Aliases: []string{"cp"},
		Short:   "Get cluster pair resources",
		Run: func(c *cobra.Command, args []string) {
			var clusterPairs *storkv1.ClusterPairList

			namespaces, err := cmdFactory.GetAllNamespaces()
			if err != nil {
				util.CheckErr(err)
				return
			}

			if len(args) > 0 {
				clusterPairs = new(storkv1.ClusterPairList)
				for _, pairName := range args {
					for _, ns := range namespaces {
						pair, err := storkops.Instance().GetClusterPair(pairName, ns)
						if err != nil {
							util.CheckErr(err)
							return
						}
						clusterPairs.Items = append(clusterPairs.Items, *pair)
					}
				}
			} else {
				var tempClusterPairs storkv1.ClusterPairList
				for _, ns := range namespaces {
					clusterPairs, err = storkops.Instance().ListClusterPairs(ns)
					if err != nil {
						util.CheckErr(err)
						return
					}
					tempClusterPairs.Items = append(tempClusterPairs.Items, clusterPairs.Items...)
				}
				clusterPairs = &tempClusterPairs
			}

			if len(clusterPairs.Items) == 0 {
				handleEmptyList(ioStreams.Out)
				return
			}
			if cmdFactory.IsWatchSet() {
				if err := printObjectsWithWatch(c, clusterPairs, cmdFactory, clusterPairColumns, clusterPairPrinter, ioStreams.Out); err != nil {
					util.CheckErr(err)
					return
				}
				return
			}
			if err := printObjects(c, clusterPairs, cmdFactory, clusterPairColumns, clusterPairPrinter, ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}

	cmdFactory.BindGetFlags(getClusterPairCommand.Flags())

	return getClusterPairCommand
}

func clusterPairPrinter(
	clusterPairList *storkv1.ClusterPairList,
	options printers.GenerateOptions,
) ([]metav1beta1.TableRow, error) {
	if clusterPairList == nil {
		return nil, nil
	}
	rows := make([]metav1beta1.TableRow, 0)
	for _, clusterPair := range clusterPairList.Items {
		creationTime := toTimeString(clusterPair.CreationTimestamp.Time)
		row := getRow(&clusterPair,
			[]interface{}{clusterPair.Name,
				clusterPair.Status.StorageStatus,
				clusterPair.Status.SchedulerStatus,
				creationTime},
		)
		rows = append(rows, row)
	}
	return rows, nil
}

func getStringData(fileName string) (string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return "", fmt.Errorf("error opening file %v: %v", fileName, err)
	}
	data, err := io.ReadAll(bufio.NewReader(file))
	if err != nil {
		return "", fmt.Errorf("error reading file %v: %v", fileName, err)
	}

	return string(data), nil
}

func getByteData(fileName string) ([]byte, error) {
	data, err := getStringData(fileName)
	if err != nil {
		return nil, err
	}
	return []byte(data), nil
}

func newGenerateClusterPairCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var storageOptions, projectMappingsStr string
	generateClusterPairCommand := &cobra.Command{
		Use:   clusterPairSubcommand,
		Short: "Generate a spec to be used for cluster pairing from a remote cluster",
		Run: func(c *cobra.Command, args []string) {
			if len(args) != 1 {
				util.CheckErr(fmt.Errorf("exactly one name needs to be provided for clusterpair name"))
				return
			}
			clusterPairName := args[0]
			config, err := cmdFactory.RawConfig()
			if err != nil {
				util.CheckErr(err)
				return
			}
			if errors := validation.NameIsDNSSubdomain(clusterPairName, false); len(errors) != 0 {
				err := fmt.Errorf("the Name \"%v\" is not valid: %v", clusterPairName, errors)
				util.CheckErr(err)
				return
			}
			if errors := validation.ValidateNamespaceName(cmdFactory.GetNamespace(), false); len(errors) != 0 {
				err := fmt.Errorf("the Namespace \"%v\" is not valid: %v", cmdFactory.GetNamespace(), errors)
				util.CheckErr(err)
				return
			}
			opts := make(map[string]string)
			if storageOptions == "" {
				opts["insert_storage_options_here"] = ""
			} else {
				stOpts := strings.Split(storageOptions, ",")
				if len(stOpts) == 0 {
					util.CheckErr(fmt.Errorf("invalid storage options"))
					return
				}
				for _, v := range stOpts {
					s := strings.Split(v, "=")
					if len(s) != 2 {
						util.CheckErr(fmt.Errorf("invalid storage options"))
						return
					}
					opts[s[0]] = s[1]
				}
			}
			// Prune out all but the current-context and related
			// info
			currConfig, err := pruneConfigContexts(config)
			if err != nil {
				util.CheckErr(err)
				return
			}
			clusterPair := &storkv1.ClusterPair{
				TypeMeta: meta.TypeMeta{
					Kind:       reflect.TypeOf(storkv1.ClusterPair{}).Name(),
					APIVersion: storkv1.SchemeGroupVersion.String(),
				},
				ObjectMeta: meta.ObjectMeta{
					Name:      clusterPairName,
					Namespace: cmdFactory.GetNamespace(),
				},

				Spec: storkv1.ClusterPairSpec{
					Config:  currConfig,
					Options: opts,
				},
			}
			if len(projectMappingsStr) > 0 {
				projectIDMap, err := utils.ParseKeyValueList(strings.Split(projectMappingsStr, ","))
				if err != nil {
					util.CheckErr(fmt.Errorf("invalid %v", projectMappingHelpString))
				}
				clusterPair.Spec.PlatformOptions = storkv1.PlatformSpec{
					Rancher: &storkv1.RancherSpec{ProjectMappings: projectIDMap},
				}
			}

			if err = printEncoded(c, clusterPair, "yaml", ioStreams.Out); err != nil {
				util.CheckErr(err)
				return
			}
		},
	}

	generateClusterPairCommand.Flags().StringVarP(&storageOptions, "storageoptions", "s", "", "comma seperated key-value pair storage options")
	generateClusterPairCommand.Flags().StringVarP(&projectMappingsStr, "project-mappings", "", "", projectMappingHelpString)
	return generateClusterPairCommand
}

func newCreateClusterPairCommand(cmdFactory Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
	var sIP, dIP, sEP, sPort, dPort, dEP, srcToken, destToken, projectMappingsStr string
	var mode string
	var sFile, dFile string
	var provider, bucket, encryptionKey string
	var s3AccessKey, s3SecretKey, s3Region, s3EndPoint, s3StorageClass string
	var disableSSL bool
	var azureAccountName, azureAccountKey string
	var googleProjectID, googleJSONKey string
	var syncDR bool
	var pxAuthTokenSrc, pxAuthSecretNamespaceSrc string
	var pxAuthTokenDest, pxAuthSecretNamespaceDest string
	var backupLocationName string
	var unidirectional bool

	createClusterPairCommand := &cobra.Command{
		Use:   clusterPairSubcommand,
		Short: "Create ClusterPair on source and destination cluster",
		Run: func(c *cobra.Command, args []string) {
			if len(args) != 1 {
				util.CheckErr(fmt.Errorf("exactly one name needs to be provided for clusterpair name"))
				return
			}
			clusterPairName := args[0]
			// validate given name and namespace
			if errors := validation.NameIsDNSSubdomain(clusterPairName, false); len(errors) != 0 {
				err := fmt.Errorf("the Name \"%v\" is not valid: %v", clusterPairName, errors)
				util.CheckErr(err)
				return
			}
			if errors := validation.ValidateNamespaceName(cmdFactory.GetNamespace(), false); len(errors) != 0 {
				err := fmt.Errorf("the Namespace \"%v\" is not valid: %v", cmdFactory.GetNamespace(), errors)
				util.CheckErr(err)
				return
			}

			if len(sIP) > 0 || len(sPort) > 0 || len(dIP) > 0 || len(dPort) > 0 {
				return
			}

			if mode != userInputCPModeAsync && mode != userInputCPModeSync && mode != userInputCPModeMigration {
				util.CheckErr(fmt.Errorf("invalid mode %s, mode value should either be %s, %s or %s", mode, userInputCPModeAsync, userInputCPModeSync, userInputCPModeMigration))
				return
			}

			if mode == userInputCPModeSync {
				syncDR = true
			}

			if len(sFile) == 0 {
				util.CheckErr(getMissingParameterError("src-kube-file", "Kubeconfig file missing for source cluster"))
				return
			}

			if len(dFile) == 0 {
				util.CheckErr(getMissingParameterError("dest-kube-file", "Kubeconfig file missing for destination cluster"))
				return
			}

			if sFile == dFile {
				util.CheckErr(fmt.Errorf("source kubeconfig file and destination kubeconfig file should be different"))
				return
			}

			sameFiles, err := utils.CompareFiles(sFile, dFile)
			if err != nil {
				util.CheckErr(err)
				return
			}
			if sameFiles {
				util.CheckErr(fmt.Errorf("source kubeconfig and destination kubeconfig file should be different"))
				return
			}
			// Handling the syncDR cases here
			if syncDR {
				srcClusterPair, err := generateClusterPair(clusterPairName, cmdFactory.GetNamespace(), dIP, dPort, destToken, dFile, projectMappingsStr, pxAuthSecretNamespaceDest, false, mode, true)
				if err != nil {
					util.CheckErr(err)
					return
				}

				conf, err := getConfig(sFile).ClientConfig()
				if err != nil {
					util.CheckErr(err)
					return
				}

				storkops.Instance().SetConfig(conf)
				core.Instance().SetConfig(conf)
				if len(cmdFactory.GetNamespace()) > 0 {
					err = createNamespace(sFile, cmdFactory.GetNamespace())
					if err != nil {
						util.CheckErr(err)
						return
					}
				}
				_, err = storkops.Instance().CreateClusterPair(srcClusterPair)
				if err != nil {
					util.CheckErr(err)
					return
				}
				printMsg(fmt.Sprintf("ClusterPair %s created successfully. Direction Source -> Destination\n", clusterPairName), ioStreams.Out)

				if unidirectional {
					return
				}
				destClusterPair, err := generateClusterPair(clusterPairName, cmdFactory.GetNamespace(), sIP, sPort, srcToken, sFile, projectMappingsStr, pxAuthSecretNamespaceSrc, true, mode, true)
				if err != nil {
					util.CheckErr(err)
					return
				}

				conf, err = getConfig(dFile).ClientConfig()
				if err != nil {
					util.CheckErr(err)
					return
				}
				storkops.Instance().SetConfig(conf)
				core.Instance().SetConfig(conf)

				if len(cmdFactory.GetNamespace()) > 0 {
					err = createNamespace(dFile, cmdFactory.GetNamespace())
					if err != nil {
						util.CheckErr(err)
						return
					}
				}
				_, err = storkops.Instance().CreateClusterPair(destClusterPair)
				if err != nil {
					util.CheckErr(err)
					return
				}
				printMsg(fmt.Sprintf("Cluster pair %s created successfully. Direction: Destination -> Source", clusterPairName), ioStreams.Out)
				return
			}

			//Handling the asyncDR and migration cases here onwards
			// Bail out if portworx-api service type is not loadbalancer type and the endpoints are not provided.
			if !unidirectional && len(sEP) == 0 {
				srcPXEndpoint, err := getPXEndPointDetails(sFile)
				if err != nil {
					err = fmt.Errorf("unable to get portworx endpoint in source cluster. Err: %v", err)
					util.CheckErr(err)
					return
				}
				printMsg(fmt.Sprintf("Source portworx endpoint is %s", srcPXEndpoint), ioStreams.Out)
				sEP = srcPXEndpoint
			}

			if len(dEP) == 0 {
				destPXEndpoint, err := getPXEndPointDetails(dFile)
				if err != nil {
					err = fmt.Errorf("unable to get portworx endpoint in destination cluster. Err: %v", err)
					util.CheckErr(err)
					return
				}
				printMsg(fmt.Sprintf("Destination portworx endpoint is %s", destPXEndpoint), ioStreams.Out)
				dEP = destPXEndpoint
			}

			dIP, dPort, err := getHostPortFromEndPoint(dEP)
			if err != nil {
				printMsg(fmt.Sprintf("Error parsing destination px endpoint %s: %v", dEP, err), ioStreams.Out)
				return
			}

			if len(destToken) == 0 {
				pxAuthTokenDest, pxAuthSecretNamespaceDest, err = getPXAuthToken(dFile)
				if err != nil {
					printMsg(fmt.Sprintf("Got error while fetching px auth token in destination cluster: %v", err), ioStreams.Out)
				}
				if len(pxAuthTokenDest) > 0 {
					printMsg("Fetching px token with auth token in destination cluster", ioStreams.Out)
				}
				token, err := getPXToken(fmt.Sprintf("%s:%s", dIP, dPort), pxAuthTokenDest)
				if err != nil {
					err = fmt.Errorf("got error while fetching px token in destination cluster %s. Err: %v", dEP, err)
					util.CheckErr(err)
					return
				}
				destToken = token
			}

			sConf, err := getConfig(sFile).ClientConfig()
			if err != nil {
				util.CheckErr(err)
				return
			}
			dConf, err := getConfig(dFile).ClientConfig()
			if err != nil {
				util.CheckErr(err)
				return
			}

			if len(cmdFactory.GetNamespace()) > 0 {
				err = createNamespace(sFile, cmdFactory.GetNamespace())
				if err != nil {
					util.CheckErr(fmt.Errorf("error in creating namespace %s in source cluster: %v", cmdFactory.GetNamespace(), err))
					return
				}
				err = createNamespace(dFile, cmdFactory.GetNamespace())
				if err != nil {
					util.CheckErr(fmt.Errorf("error in creating namespace %s in destination cluster: %v", cmdFactory.GetNamespace(), err))
					return
				}
			}

			// backuplocation object init as it is used in different if blocks
			backupLocation := &storkv1.BackupLocation{
				ObjectMeta: meta.ObjectMeta{
					Name:      clusterPairName,
					Namespace: cmdFactory.GetNamespace(),
					Annotations: map[string]string{
						skipResourceAnnotation: "true",
						storkCreatedAnnotation: "true",
					},
				},
				Location: storkv1.BackupLocationItem{},
			}

			if len(backupLocationName) > 0 {
				err = verifyBackupLocationExistence(sFile, backupLocationName, cmdFactory.GetNamespace())
				if err != nil {
					util.CheckErr(fmt.Errorf("input objectstoreLocation %s existence check failed with error in source cluster: %v", backupLocationName, err))
					return
				}
				err = verifyBackupLocationExistence(dFile, backupLocationName, cmdFactory.GetNamespace())
				if err != nil {
					util.CheckErr(fmt.Errorf("input objectstoreLocation %s existence check failed with error in destination cluster: %v", backupLocationName, err))
					return
				}
			} else {
				// create backuplocation secrets in both source and destination
				credentialData := make(map[string][]byte)
				switch provider {
				case "s3":
					if s3AccessKey == "" {
						util.CheckErr(getMissingParameterError("s3-access-key", "Access Key missing for S3"))
						return
					}
					if s3SecretKey == "" {
						util.CheckErr(getMissingParameterError("s3-secret-key", "Secret Key missing for S3"))
						return
					}
					if len(s3EndPoint) == 0 {
						util.CheckErr(getMissingParameterError("s3-endpoint", "Endpoint missing for S3"))
						return
					}
					if len(s3Region) == 0 {
						util.CheckErr(getMissingParameterError("s3-region", "Region missing for S3"))
						return
					}
					credentialData["endpoint"] = []byte(s3EndPoint)
					credentialData["accessKeyID"] = []byte(s3AccessKey)
					credentialData["secretAccessKey"] = []byte(s3SecretKey)
					credentialData["region"] = []byte(s3Region)
					credentialData["disableSSL"] = []byte(strconv.FormatBool(disableSSL))
					backupLocation.Location.Type = storkv1.BackupLocationS3
				case "azure":
					if azureAccountName == "" {
						util.CheckErr(getMissingParameterError("azure-account-name", "Account Name missing for Azure"))
						return
					}
					if azureAccountKey == "" {
						util.CheckErr(getMissingParameterError("azure-account-key", "Account Key missing for Azure"))
						return
					}
					credentialData["storageAccountName"] = []byte(azureAccountName)
					credentialData["storageAccountKey"] = []byte(azureAccountKey)
					backupLocation.Location.Type = storkv1.BackupLocationAzure
				case "google":
					if len(googleProjectID) == 0 {
						util.CheckErr(getMissingParameterError("google-project-id", "Project ID missing for Google"))
						return
					}
					if len(googleJSONKey) == 0 {
						util.CheckErr(getMissingParameterError("google-key-file-path", "Json key file path missing for Google"))
						return
					}
					// Read the jsonkey file
					keyData, err := os.ReadFile(googleJSONKey)
					if err != nil {
						util.CheckErr(fmt.Errorf("failed to read json key file: %v", err))
						return
					}
					credentialData["accountKey"] = keyData
					credentialData["projectID"] = []byte(googleProjectID)
					backupLocation.Location.Type = storkv1.BackupLocationGoogle
				default:
					util.CheckErr(getMissingParameterError("provider", "External objectstore provider needs to be either of azure, google, s3"))
					return
				}

				credentialData["path"] = []byte(bucket)
				credentialData["type"] = []byte(provider)
				credentialData["encryptionKey"] = []byte(encryptionKey)
				backupLocationName = backupLocation.Name

				storkops.Instance().SetConfig(sConf)
				core.Instance().SetConfig(sConf)

				// check for already existing backuplocation
				_, err = storkops.Instance().GetBackupLocation(backupLocationName, cmdFactory.GetNamespace())
				if err == nil {
					util.CheckErr(fmt.Errorf("objectstoreLocation %s already exists in source cluster namespace %s, instead use \"--use-existing-objectstorelocation %s\" to use existing location", backupLocationName, cmdFactory.GetNamespace(), backupLocationName))
					return
				}

				printMsg(fmt.Sprintf("\nCreating Secret and ObjectstoreLocation in source cluster in namespace %v...", cmdFactory.GetNamespace()), ioStreams.Out)
				secret := &v1.Secret{
					ObjectMeta: meta.ObjectMeta{
						Name:      clusterPairName,
						Namespace: cmdFactory.GetNamespace(),
						Annotations: map[string]string{
							skipResourceAnnotation: "true",
							storkCreatedAnnotation: "true",
						},
					},
					Data: credentialData,
					Type: v1.SecretTypeOpaque,
				}
				_, err = core.Instance().CreateSecret(secret)
				if err != nil {
					util.CheckErr(err)
					return
				}

				backupLocation.Location.SecretConfig = clusterPairName
				var annotations = make(map[string]string)
				annotations[skipResourceAnnotation] = "true"
				_, err = storkops.Instance().CreateBackupLocation(backupLocation)
				if err != nil {
					err := fmt.Errorf("unable to create ObjectstoreLocation in source. Err: %v", err)
					util.CheckErr(err)
					return
				}
				printMsg(fmt.Sprintf("ObjectstoreLocation %v created on source cluster in namespace %v\n", clusterPairName, cmdFactory.GetNamespace()), ioStreams.Out)

				storkops.Instance().SetConfig(dConf)
				core.Instance().SetConfig(dConf)
				// check for already existing backuplocation
				_, err = storkops.Instance().GetBackupLocation(backupLocationName, cmdFactory.GetNamespace())
				if err == nil {
					util.CheckErr(fmt.Errorf("objectstoreLocation %s already exists in destination cluster namespace %s, instead use \"--use-existing-objectstorelocation %s\" to use existing location", backupLocationName, cmdFactory.GetNamespace(), backupLocationName))
					return
				}

				printMsg(fmt.Sprintf("Creating Secret and ObjectstoreLocation in destination cluster in namespace %v...", cmdFactory.GetNamespace()), ioStreams.Out)
				_, err = core.Instance().CreateSecret(secret)
				if err != nil {
					util.CheckErr(err)
					return
				}
				_, err = storkops.Instance().CreateBackupLocation(backupLocation)
				if err != nil {
					err := fmt.Errorf("unable to create ObjectstoreLocation in destination. Err: %v", err)
					util.CheckErr(err)
					return
				}
				printMsg(fmt.Sprintf("ObjectstoreLocation %v created on destination cluster in namespace %v\n", clusterPairName, cmdFactory.GetNamespace()), ioStreams.Out)
			}

			printMsg("Creating a cluster pair. Direction: Source -> Destination", ioStreams.Out)
			srcClusterPair, err := generateClusterPair(clusterPairName, cmdFactory.GetNamespace(), dIP, dPort, destToken, dFile, projectMappingsStr, pxAuthSecretNamespaceDest, false, mode, false)
			if err != nil {
				util.CheckErr(err)
				return
			}
			storkops.Instance().SetConfig(sConf)
			core.Instance().SetConfig(sConf)
			srcClusterPair.Spec.Options[storkv1.BackupLocationResourceName] = backupLocationName
			_, err = storkops.Instance().CreateClusterPair(srcClusterPair)
			if err != nil {
				util.CheckErr(err)
				return
			}
			printMsg(fmt.Sprintf("ClusterPair %s created successfully. Direction Source -> Destination\n", clusterPairName), ioStreams.Out)

			// Create clusterpair in destination only if unidirectional flag is not set
			if !unidirectional {
				sIP, sPort, err := getHostPortFromEndPoint(sEP)
				if err != nil {
					printMsg(fmt.Sprintf("Error parsing source px endpoint %s: %v", sEP, err), ioStreams.Out)
					return
				}

				if len(srcToken) == 0 {
					pxAuthTokenSrc, pxAuthSecretNamespaceSrc, err = getPXAuthToken(sFile)
					if err != nil {
						printMsg(fmt.Sprintf("Got error while fetching px auth token in source cluster: %v", err), ioStreams.Out)
					}
					if len(pxAuthTokenSrc) > 0 {
						printMsg("Fetching px token with auth token in source cluster", ioStreams.Out)
					}
					token, err := getPXToken(fmt.Sprintf("%s:%s", sIP, sPort), pxAuthTokenSrc)
					if err != nil {
						err := fmt.Errorf("got error while fetching px token in source cluster %s. Err: %v", sEP, err)
						util.CheckErr(err)
						return
					}
					srcToken = token
				}
				destClusterPair, err := generateClusterPair(clusterPairName, cmdFactory.GetNamespace(), sIP, sPort, srcToken, sFile, projectMappingsStr, pxAuthSecretNamespaceSrc, true, mode, false)
				if err != nil {
					util.CheckErr(err)
					return
				}

				printMsg("Creating a cluster pair. Direction: Destination -> Source", ioStreams.Out)
				storkops.Instance().SetConfig(dConf)
				core.Instance().SetConfig(dConf)
				destClusterPair.Spec.Options[storkv1.BackupLocationResourceName] = backupLocation.Name
				_, err = storkops.Instance().CreateClusterPair(destClusterPair)
				if err != nil {
					util.CheckErr(err)
					return
				}
				printMsg(fmt.Sprintf("Cluster pair %s created successfully. Direction: Destination -> Source", clusterPairName), ioStreams.Out)
			}
		},
	}

	createClusterPairCommand.Flags().StringVarP(&sIP, "src-ip", "", "", "IP of storage node from source cluster")
	createClusterPairCommand.Flags().MarkDeprecated("src-ip", "instead provide --src-ep")
	createClusterPairCommand.Flags().StringVarP(&sPort, "src-port", "", "", "Port of storage node from source cluster")
	createClusterPairCommand.Flags().MarkDeprecated("src-port", "instead provide --src-ep")
	createClusterPairCommand.Flags().StringVarP(&sEP, "src-ep", "", "", "(Optional)Endpoint of portworx-api service in source cluster")
	createClusterPairCommand.Flags().StringVarP(&sFile, "src-kube-file", "", "", "Path to the kubeconfig of source cluster")
	createClusterPairCommand.Flags().StringVarP(&dIP, "dest-ip", "", "", "IP of storage node from destination cluster")
	createClusterPairCommand.Flags().MarkDeprecated("dest-ip", "instead provide --dest-ep")
	createClusterPairCommand.Flags().StringVarP(&dPort, "dest-port", "", "", "Port of storage node from destination cluster")
	createClusterPairCommand.Flags().MarkDeprecated("dest-port", "instead provide --dest-ep")
	createClusterPairCommand.Flags().StringVarP(&dEP, "dest-ep", "", "", "(Optional)Endpoint of portworx-api service in destination cluster")
	createClusterPairCommand.Flags().StringVarP(&dFile, "dest-kube-file", "", "", "Path to the kubeconfig of destination cluster")
	createClusterPairCommand.Flags().StringVarP(&srcToken, "src-token", "", "", "(Optional)Source cluster token for cluster pairing")
	createClusterPairCommand.Flags().StringVarP(&destToken, "dest-token", "", "", "(Optional)Destination cluster token for cluster pairing")
	createClusterPairCommand.Flags().StringVarP(&projectMappingsStr, "project-mappings", "", "", projectMappingHelpString)
	createClusterPairCommand.Flags().StringVarP(&mode, "mode", "", userInputCPModeAsync, fmt.Sprintf("Mode of DR. [%s, %s, %s]", userInputCPModeAsync, userInputCPModeSync, userInputCPModeMigration))
	createClusterPairCommand.Flags().BoolVarP(&unidirectional, "unidirectional", "u", false, "(Optional) to create Clusterpair from source -> dest only")
	createClusterPairCommand.Flags().StringVarP(&backupLocationName, "use-existing-objectstorelocation", "", "", "(Optional) Objectstorelocation with the provided name should be present in both source and destination cluster")
	// New parameters for creating backuplocation secret
	createClusterPairCommand.Flags().StringVarP(&provider, "provider", "p", "", "External objectstore provider name. [s3, azure, google]")
	createClusterPairCommand.Flags().StringVar(&bucket, "bucket", "", "Bucket name")
	createClusterPairCommand.Flags().StringVar(&encryptionKey, "encryption-key", "", "Encryption key for encrypting the data stored in the objectstore.")
	// AWS
	createClusterPairCommand.Flags().StringVar(&s3AccessKey, "s3-access-key", "", "Access Key for S3")
	createClusterPairCommand.Flags().StringVar(&s3SecretKey, "s3-secret-key", "", "Secret Key for S3")
	createClusterPairCommand.Flags().StringVar(&s3Region, "s3-region", "", "Region for S3")
	createClusterPairCommand.Flags().StringVar(&s3EndPoint, "s3-endpoint", "", "EndPoint for S3")
	createClusterPairCommand.Flags().StringVar(&s3StorageClass, "s3-storage-class", "", "Storage Class for S3")
	createClusterPairCommand.Flags().BoolVar(&disableSSL, "disable-ssl", false, "Set to true to disable ssl when using S3")
	// Azure
	createClusterPairCommand.Flags().StringVar(&azureAccountName, "azure-account-name", "", "Account name for Azure")
	createClusterPairCommand.Flags().StringVar(&azureAccountKey, "azure-account-key", "", "Account key for Azure")
	// Google
	createClusterPairCommand.Flags().StringVar(&googleProjectID, "google-project-id", "", "Project ID for Google")
	createClusterPairCommand.Flags().StringVar(&googleJSONKey, "google-key-file-path", "", "Json key file path for Google")

	return createClusterPairCommand
}

func generateClusterPair(
	name string,
	ns string,
	ip string,
	port string,
	token string,
	configFile string,
	projectIDMappings string,
	authSecretNamespace string,
	reverse bool,
	mode string,
	ignoreStorageOptions bool) (*storkv1.ClusterPair, error) {
	opts := make(map[string]string)
	if !ignoreStorageOptions {
		opts["ip"] = ip
		opts["port"] = port
		// extract token from px-endpoint command
		opts["token"] = token
	}
	if mode == userInputCPModeAsync {
		opts["mode"] = clusterPairDRModeDisasterRecovery
	} else if mode == userInputCPModeMigration {
		opts["mode"] = clusterPairDRModeOnetimeMigration
	}
	config, err := getConfig(configFile).RawConfig()
	if err != nil {
		return nil, err
	}
	currConfig, err := pruneConfigContexts(config)
	if err != nil {
		return nil, err
	}
	var projectIDMap map[string]string
	if len(projectIDMappings) != 0 {
		projectIDMap, err = utils.ParseKeyValueList(strings.Split(projectIDMappings, ","))
		if err != nil {
			return nil, fmt.Errorf("invalid project-mappings provided: %v", err)
		}
	}

	clusterPair := &storkv1.ClusterPair{
		TypeMeta: meta.TypeMeta{
			Kind:       reflect.TypeOf(storkv1.ClusterPair{}).Name(),
			APIVersion: storkv1.SchemeGroupVersion.String(),
		},
		ObjectMeta: meta.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},

		Spec: storkv1.ClusterPairSpec{
			Config:  currConfig,
			Options: opts,
		},
	}
	if len(projectIDMap) != 0 {
		clusterPair.Spec.PlatformOptions.Rancher = &storkv1.RancherSpec{}
		clusterPair.Spec.PlatformOptions.Rancher.ProjectMappings = make(map[string]string)
		for k, v := range projectIDMap {
			if reverse {
				clusterPair.Spec.PlatformOptions.Rancher.ProjectMappings[v] = k
			} else {
				clusterPair.Spec.PlatformOptions.Rancher.ProjectMappings[k] = v
			}

		}
	}

	// Add the annotations for auth enabled clusters
	if len(authSecretNamespace) > 0 {
		annotations := make(map[string]string)
		annotations[secretNamespace] = authSecretNamespace
		annotations[secretName] = pxAdminTokenSecret
		clusterPair.ObjectMeta.Annotations = annotations
	}

	return clusterPair, nil
}

// Prune out all but the current-context and related
// info
func pruneConfigContexts(config clientcmdapi.Config) (clientcmdapi.Config, error) {
	var err error
	currentContext := config.CurrentContext
	for context := range config.Contexts {
		if context != currentContext {
			delete(config.Contexts, context)
		}
	}
	if config.Contexts[currentContext] != nil {
		currentCluster := config.Contexts[currentContext].Cluster
		for cluster := range config.Clusters {
			if cluster != currentCluster {
				delete(config.Clusters, cluster)
			}
		}
		currentAuthInfo := config.Contexts[currentContext].AuthInfo
		for authInfo := range config.AuthInfos {
			if authInfo != currentAuthInfo {
				delete(config.AuthInfos, authInfo)
			}
		}

		if config.AuthInfos[currentAuthInfo] != nil {
			// Replace gcloud paths in the config
			if config.AuthInfos[currentAuthInfo].AuthProvider != nil &&
				config.AuthInfos[currentAuthInfo].AuthProvider.Config != nil {
				if cmdPath, present := config.AuthInfos[currentAuthInfo].AuthProvider.Config[cmdPathKey]; present {
					if strings.HasSuffix(cmdPath, gcloudBinaryName) {
						config.AuthInfos[currentAuthInfo].AuthProvider.Config[cmdPathKey] = gcloudPath
					}
				}
			}

			// Replace file paths with inline data
			if config.AuthInfos[currentAuthInfo].ClientCertificate != "" && len(config.AuthInfos[currentAuthInfo].ClientCertificateData) == 0 {
				config.AuthInfos[currentAuthInfo].ClientCertificateData, err = getByteData(config.AuthInfos[currentAuthInfo].ClientCertificate)
				if err != nil {
					return config, err
				}
				config.AuthInfos[currentAuthInfo].ClientCertificate = ""
			}
			if config.AuthInfos[currentAuthInfo].ClientKey != "" && len(config.AuthInfos[currentAuthInfo].ClientKeyData) == 0 {
				config.AuthInfos[currentAuthInfo].ClientKeyData, err = getByteData(config.AuthInfos[currentAuthInfo].ClientKey)
				if err != nil {
					return config, err
				}
				config.AuthInfos[currentAuthInfo].ClientKey = ""
			}
			if config.AuthInfos[currentAuthInfo].TokenFile != "" && len(config.AuthInfos[currentAuthInfo].Token) == 0 {
				config.AuthInfos[currentAuthInfo].Token, err = getStringData(config.AuthInfos[currentAuthInfo].TokenFile)
				if err != nil {
					return config, err
				}
				config.AuthInfos[currentAuthInfo].TokenFile = ""
			}
		}
		if config.Clusters[currentCluster] != nil &&
			config.Clusters[currentCluster].CertificateAuthority != "" &&
			len(config.Clusters[currentCluster].CertificateAuthorityData) == 0 {

			config.Clusters[currentCluster].CertificateAuthorityData, err = getByteData(config.Clusters[currentCluster].CertificateAuthority)
			if err != nil {
				return config, err
			}
			config.Clusters[currentCluster].CertificateAuthority = ""
		}
	}
	return config, nil
}

func getConfig(configFile string) clientcmd.ClientConfig {
	configLoadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configLoadingRules.ExplicitPath = configFile
	configOverrides := &clientcmd.ConfigOverrides{}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(configLoadingRules, configOverrides)
}

func getPXEndPointDetails(config string) (string, error) {
	var ep string
	client, err := core.NewInstanceFromConfigFile(config)
	if err != nil {
		return ep, err
	}

	services, err := client.ListServices("", meta.ListOptions{LabelSelector: "name=portworx-api"})
	if err != nil || len(services.Items) == 0 {
		err := fmt.Errorf("unable to retrieve portworx-api service. Err: %v", err)
		return ep, err
	}

	// Currently we are handling only the cases where portworx-api service is either of type LoadBalancer or clusterIP.
	// For custom upstream network objects like route and ingress, User has to provide the endpoints.
	svc := services.Items[0]
	// Only if px svc is load balancer type get the IP or Host details
	if svc.Spec.Type == v1.ServiceTypeLoadBalancer {
		if len(svc.Status.LoadBalancer.Ingress) > 0 {
			if len(svc.Status.LoadBalancer.Ingress[0].Hostname) > 0 {
				ep = svc.Status.LoadBalancer.Ingress[0].Hostname
			} else if len(svc.Status.LoadBalancer.Ingress[0].IP) > 0 {
				ep = svc.Status.LoadBalancer.Ingress[0].IP
			}
		}

		for _, svcPort := range svc.Spec.Ports {
			if svcPort.Name == "px-api" {
				port := strconv.Itoa(int(svcPort.Port))
				ep = net.JoinHostPort(ep, port)
				break
			}
		}
	} else if svc.Spec.Type == v1.ServiceTypeClusterIP {
		endPoints, err := client.GetEndpoints("portworx-api", svc.Namespace)
		if err != nil {
			return ep, fmt.Errorf("unable to retrieve portworx-api endpoint. Err: %v", err)
		}
		if len(endPoints.Subsets) > 0 && len(endPoints.Subsets[0].Addresses) > 0 {
			ip := endPoints.Subsets[0].Addresses[0].IP
			for _, epPort := range endPoints.Subsets[0].Ports {
				if epPort.Name == "px-api" {
					port := strconv.Itoa(int(epPort.Port))
					ep = net.JoinHostPort(ip, port)
				}
			}
		}
	} else {
		return ep, fmt.Errorf("an explicit portworx endpoint is required when portworx-api service type is %s", svc.Spec.Type)
	}
	return ep, nil
}

func getPXToken(pxEndpoint, pxToken string) (token string, err error) {
	clnt, err := clusterclient.NewInsecureTLSAuthClusterClient(pxEndpoint, "v1", pxToken, "")
	if err != nil {
		return token, err
	}
	mgr := clusterclient.ClusterManager(clnt)
	resp, err := mgr.GetPairToken(false)
	if err != nil {
		return token, err
	}
	token = resp.GetToken()

	return token, nil
}

func getPXAuthToken(configFile string) (authToken string, authSecretNamespace string, err error) {
	// Create cluster-pair on source cluster
	conf, err := getConfig(configFile).ClientConfig()
	if err != nil {
		util.CheckErr(err)
		return authToken, authSecretNamespace, err
	}

	core.Instance().SetConfig(conf)
	// Get the token from the secret px-admin-token
	secrets, err := core.Instance().ListSecret("", meta.ListOptions{})
	if err != nil {
		return authToken, authSecretNamespace, fmt.Errorf("failed to list secrets, err %v", err)
	}

	var secretData []byte
	var ok bool
	for _, secret := range secrets.Items {
		if secret.Name == pxAdminTokenSecret {
			authTokenSecret, err := core.Instance().GetSecret(secret.Name, secret.Namespace)
			if err != nil {
				return authToken, authSecretNamespace, fmt.Errorf("unable to retrieve %v secret: %v", secret.Name, err)
			}

			if secretData, ok = authTokenSecret.Data["auth-token"]; !ok {
				return authToken, authSecretNamespace, fmt.Errorf("invalid secret key data")
			}
			return string(secretData), secret.Namespace, nil
		}
	}

	// If token secret is not found, return empty token as it is not secured cluster.
	return authToken, authSecretNamespace, nil
}

// check if ep is like a url (basically with scheme http or https)
// if it has scheme, get the port details or assign default port as 80 or 443 based on the scheme.
// if it does not have any scheme, use splitHostPort to get host and port details.
func getHostPortFromEndPoint(ep string) (string, string, error) {
	var host, port string
	u, err := url.Parse(ep)
	// error gets hit for if ep is ip:port
	if err != nil {
		host, port, err = net.SplitHostPort(ep)
		if err != nil {
			return host, port, fmt.Errorf("unable to parse endpoint %s, err: %v", ep, err)
		}
	} else {
		// for cases only hostname or ip (ex: abc.com or 170.160.150.140)
		if len(u.Scheme) == 0 && len(u.Host) == 0 {
			if net.ParseIP(ep) == nil { //  not supporting only ip address
				host = ep
				port = "80"
			} else {
				return host, port, fmt.Errorf("port is needed along with ip address %s", ep)
			}
		} else if len(u.Host) == 0 { // for cases abc.com:9090
			host, port, err = net.SplitHostPort(ep)
			if err != nil {
				return host, port, fmt.Errorf("unable to parse endpoint %s, err: %v", ep, err)
			}
		} else if u.Scheme == "http" || u.Scheme == "https" {
			port = u.Port()
			if len(port) == 0 { // for cases http://abc.com or https://abc.com
				host = fmt.Sprintf("%s://%s", u.Scheme, u.Host)
				if u.Scheme == "http" {
					port = "80"
				} else if u.Scheme == "https" {
					port = "443"
				}
			} else {
				host, port, err = net.SplitHostPort(u.Host) // for cases http://abc.com:9090 or https://abc.com:9090
				if err != nil {
					return host, port, fmt.Errorf("unable to parse endpoint %s, err: %v", ep, err)
				}
				host = fmt.Sprintf("%s://%s", u.Scheme, host)
			}
		} else { // ht://abc.com
			return host, port, fmt.Errorf("invalid endpoint %s", ep)
		}
	}
	return host, port, nil
}

func getMissingParameterError(param string, desc string) error {
	return fmt.Errorf("missing parameter %q - %s", param, desc)
}

func createNamespace(config string, namespace string) error {
	conf, err := getConfig(config).ClientConfig()
	if err != nil {
		return err
	}
	coreOps, err := core.NewForConfig(conf)
	if err != nil {
		return err
	}
	_, err = coreOps.CreateNamespace(&v1.Namespace{
		ObjectMeta: meta.ObjectMeta{
			Name: namespace,
		},
	})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func verifyBackupLocationExistence(config string, blName string, namespace string) error {
	conf, err := getConfig(config).ClientConfig()
	if err != nil {
		return err
	}
	storkOps, err := storkops.NewForConfig(conf)
	if err != nil {
		return err
	}
	bl, err := storkOps.GetBackupLocation(blName, namespace)
	if err != nil {
		return err
	}
	if len(bl.Location.SecretConfig) > 0 {
		coreOps, err := core.NewForConfig(conf)
		if err != nil {
			return err
		}
		_, err = coreOps.GetSecret(bl.Location.SecretConfig, namespace)
		if err != nil {
			return err
		}
	}
	return nil
}
