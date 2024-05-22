package tests

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	_ "github.com/portworx/px-backup-api/pkg/kubeauth/gcp"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/aetosutil"
	"github.com/portworx/torpedo/pkg/log"
	"github.com/portworx/torpedo/pkg/s3utils"
	. "github.com/portworx/torpedo/tests"
)

func getBucketNameSuffix() string {
	bucketNameSuffix, present := os.LookupEnv("BUCKET_NAME")
	if present && bucketNameSuffix != "" {
		return bucketNameSuffix
	} else {
		return "default-suffix"
	}
}

func getGlobalBucketName(provider string) string {
	switch provider {
	case drivers.ProviderAws:
		return GlobalAWSBucketName
	case drivers.ProviderAzure:
		return GlobalAzureBucketName
	case drivers.ProviderGke:
		return GlobalGCPBucketName
	case drivers.ProviderNfs:
		return GlobalNFSBucketName
	default:
		return GlobalAWSBucketName
	}
}

func getGlobalLockedBucketName(provider string) string {
	if provider == drivers.ProviderAws {
		return GlobalAWSLockedBucketName
	} else {
		log.Errorf("environment variable [%s] not provided with valid values", "PROVIDERS")
		return ""
	}
}

func TestBasic(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Torpedo : Backup")
}

// triggerCleanup returns if cleanup should happen or not based on ENVs set in torpedo
func triggerCleanup() bool {
	lockedBucketName := os.Getenv("LOCKED_BUCKET_NAME")
	if lockedBucketName == "" {
		return true
	}

	// TODO: Add code not cleanup if you wish to debug anything

	return false
}

// BackupInitInstance initialises instances required for backup
func BackupInitInstance() {
	var err error
	var token string
	var commitID string
	log.Infof("Inside BackupInitInstance")
	err = Inst().S.Init(scheduler.InitOptions{
		SpecDir:             Inst().SpecDir,
		VolDriverName:       Inst().V.String(),
		StorageProvisioner:  Inst().Provisioner,
		NodeDriverName:      Inst().N.String(),
		CustomAppConfig:     Inst().CustomAppConfig,
		SecretConfigMapName: Inst().ConfigMap,
		SecureApps:          Inst().SecureAppList,
	})
	log.FailOnError(err, "Error occurred while Scheduler Driver Initialization")
	if Inst().ConfigMap != "" {
		log.Infof("Using Config Map: %s ", Inst().ConfigMap)
		token, err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
		log.FailOnError(err, "Error occured while getting token from config map")
		log.Infof("Token used for initializing: %s ", token)
	} else {
		token = ""
	}
	err = Inst().N.Init(node.InitOptions{
		SpecDir: Inst().SpecDir,
	})
	log.FailOnError(err, "Error occurred while Node Driver Initialization")
	err = Inst().V.Init(Inst().S.String(), Inst().N.String(), token, Inst().Provisioner, Inst().CsiGenericDriverConfigMap)
	log.FailOnError(err, "Error occurred while Volume Driver Initialization")
	if Inst().Backup != nil {
		err = Inst().Backup.Init(Inst().S.String(), Inst().N.String(), Inst().V.String(), token)
		log.FailOnError(err, "Error occurred while Backup Driver Initialization")
	}
	SetupTestRail()

	// Getting Px version info
	pxVersion, err := Inst().V.GetDriverVersion()
	log.FailOnError(err, "Error occurred while getting PX version")
	if len(strings.Split(pxVersion, "-")) > 1 {
		commitID = strings.Split(pxVersion, "-")[1]
	} else {
		commitID = "NA"
	}
	t := Inst().Dash.TestSet
	t.CommitID = commitID
	if pxVersion != "" {
		t.Tags["px-version"] = pxVersion
	}

	// Getting Px-Backup server version info and setting Aetos Dashboard tags
	PxBackupVersion, err = GetPxBackupVersionString()
	log.FailOnError(err, "Error getting Px Backup version")
	log.Infof("Running with px-backup version <<< %s >>>", PxBackupVersion)
	PxBackupBuildDate, err := GetPxBackupBuildDate()
	log.FailOnError(err, "Error getting Px Backup build date")
	t.Tags["px-backup-version"] = PxBackupVersion
	t.Tags["px-backup-build-date"] = PxBackupBuildDate
	t.Tags["storageProvisioner"] = Inst().Provisioner
	t.Tags["pureVolume"] = fmt.Sprintf("%t", Inst().PureVolumes)
	t.Tags["pureSANType"] = Inst().PureSANType

	Inst().Dash.TestSetUpdate(t)
	// Setting the common password
	CommonPassword = backup.PxCentralAdminPwd + RandomString(4)
	// Dumping source and destination kubeconfig to file system path
	log.Infof("Dumping source and destination kubeconfig to file system path")
	kubeconfigs := os.Getenv("KUBECONFIGS")
	dash.VerifyFatal(kubeconfigs != "", true, "Getting KUBECONFIGS Environment variable")
	kubeconfigList := strings.Split(kubeconfigs, ",")
	dash.VerifyFatal(len(kubeconfigList) < 2, false, "minimum 2 kubeconfigs are required for source and destination cluster")
	DumpKubeconfigs(kubeconfigList)
	GlobalGkeSecretString, err = GetGkeSecret()
	if os.Getenv("CLUSTER_PROVIDER") == drivers.ProviderRke {
		// Switch context to destination cluster to update RancherMap with destination cluster details
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "Switching context to destination cluster failed")
		// Switch context to destination cluster to update RancherMap with source cluster details
		err = SetSourceKubeConfig()
		log.FailOnError(err, "Switching context to source cluster failed")
	}
}

var dash *aetosutil.Dashboard
var _ = BeforeSuite(func() {
	var err error
	dash = Inst().Dash
	dash.TestSetBegin(dash.TestSet)
	log.Infof("Backup Init instance")
	BackupInitInstance()
	StartTorpedoTest("Setup buckets", "Creating one generic bucket to be used in all cases", nil, 0)
	defer EndTorpedoTest()
	// Get all the values from the cloud_config.json persist into struct which can be globally accessed
	GlobalCredentialConfig, err = GetConfigObj()
	dash.VerifyFatal(err, nil, "Fetching the cloud config details and persisting into globalConfig struct")
	// Create the first bucket from the list to be used as generic bucket
	providers := GetBackupProviders()
	bucketNameSuffix := getBucketNameSuffix()
	for _, provider := range providers {
		switch provider {
		case drivers.ProviderAws:
			GlobalAWSBucketName = fmt.Sprintf("%s-%s", GlobalAWSBucketPrefix, bucketNameSuffix)
			CreateBucket(provider, GlobalAWSBucketName)
			log.Infof("Bucket created with name - %s", GlobalAWSBucketName)
			s3EncryptionPolicy := os.Getenv("S3_ENCRYPTION_POLICY")
			if s3EncryptionPolicy != "" {
				sseDetails, err := s3utils.GetS3SSEDetailsFromEnv()
				log.FailOnError(err, "Failed to get sse details form environment")
				policy, err := GenerateS3BucketPolicy(string(sseDetails.SseType), string(sseDetails.SseEncryptionPolicy), GlobalAWSBucketName)
				log.FailOnError(err, "Failed to generate s3 bucket policy check for the correctness of policy parameters")
				err = UpdateS3BucketPolicy(GlobalAWSBucketName, policy)
				log.FailOnError(err, "Failed to apply bucket policy")
				log.Infof("Updated S3 bucket policy - %s", GlobalAWSBucketName)
			}
		case drivers.ProviderAzure:
			GlobalAzureBucketName = fmt.Sprintf("%s-%s", GlobalAzureBucketPrefix, bucketNameSuffix)
			CreateBucket(provider, GlobalAzureBucketName)
			log.Infof("Bucket created with name - %s", GlobalAzureBucketName)
		case drivers.ProviderGke:
			GlobalGCPBucketName = fmt.Sprintf("%s-%s", GlobalGCPBucketPrefix, bucketNameSuffix)
			CreateBucket(provider, GlobalGCPBucketName)
			log.Infof("Bucket created with name - %s", GlobalGCPBucketName)
		case drivers.ProviderNfs:
			GlobalNFSBucketName = fmt.Sprintf("%s-%s", GlobalNFSBucketPrefix, RandomString(6))
		}
	}
	lockedBucketNameSuffix, present := os.LookupEnv("LOCKED_BUCKET_NAME")
	if present {
		for _, provider := range providers {
			switch provider {
			case drivers.ProviderAws:
				GlobalAWSLockedBucketName = fmt.Sprintf("%s-%s", GlobalAWSLockedBucketPrefix, lockedBucketNameSuffix)
			case drivers.ProviderAzure:
				GlobalAzureLockedBucketName = fmt.Sprintf("%s-%s", GlobalAzureLockedBucketPrefix, lockedBucketNameSuffix)
			case drivers.ProviderGke:
				GlobalGCPLockedBucketName = fmt.Sprintf("%s-%s", GlobalGCPLockedBucketPrefix, lockedBucketNameSuffix)
			}
		}
	} else {
		log.Infof("Locked bucket name not provided")
	}

	// Create Global pre-rule and post-rule for the application used
	flagFromEnv := os.Getenv("USE_GLOBAL_RULES")
	if flagFromEnv == "" {
		GlobalRuleFlag = false
	} else {
		GlobalRuleFlag, err = strconv.ParseBool(flagFromEnv)
		dash.VerifyFatal(err, nil, "Parsing USE_GLOBAL_RULES environment variable")
	}

	if GlobalRuleFlag {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		GlobalPreRuleName, GlobalPostRuleName, err = CreateRuleForBackupWithMultipleApplications(BackupOrgID, Inst().AppList, ctx)
		dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of pre and post exec rules for applications from px-admin"))

		if GlobalPreRuleName != "" {
			GlobalPreRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, GlobalPreRuleName)
			log.FailOnError(err, "Fetching pre backup rule [%s] uid", GlobalPreRuleName)
			log.Infof("Pre backup rule [%s] uid: [%s]", GlobalPreRuleName, GlobalPreRuleUid)
			err = AddRuleOwnership(GlobalPreRuleName, GlobalPreRuleUid, nil, nil, Invalid, Admin, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for Global pre-rule of application"))
		}
		if GlobalPostRuleName != "" {
			GlobalPostRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, GlobalPostRuleName)
			log.FailOnError(err, "Fetching post backup rule [%s] uid", GlobalPostRuleName)
			log.Infof("Post backup rule [%s] uid: [%s]", GlobalPostRuleName, GlobalPostRuleUid)
			err = AddRuleOwnership(GlobalPostRuleName, GlobalPostRuleUid, nil, nil, Invalid, Admin, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for Global Post-rule of application"))
		}
	}
	pxBackupNamespace, err := backup.GetPxBackupNamespace()
	log.FailOnError(err, "failed to get Px-Backup namespace")
	PvcListBeforeRun, err = GetPVCListForNamespace(pxBackupNamespace)
	log.FailOnError(err, "failed to list PVCs before run")
	log.Infof("PVC list before the run is [%s]", PvcListBeforeRun)
})

var _ = AfterSuite(func() {
	StartTorpedoTest("Environment cleanup", "Removing Px-Backup entities created during the test execution", nil, 0)
	defer dash.TestSetEnd()
	defer EndTorpedoTest()

	cleanup := triggerCleanup()
	log.InfoD(fmt.Sprintf("Cleanup state is set to %t", cleanup))
	if cleanup {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")

		//Cleanup policy
		s3EncryptionPolicy := os.Getenv("S3_ENCRYPTION_POLICY")
		if s3EncryptionPolicy != "" {
			err = RemoveS3BucketPolicy(GlobalAWSBucketName)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verify removal of S3 bucket policy"))
		}

		// Cleanup all backups
		allBackups, err := GetAllBackupsAdmin()
		dash.VerifySafely(err, nil, "Verifying fetching of all backups")
		for _, backupName := range allBackups {
			backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, BackupOrgID)
			dash.VerifySafely(err, nil, fmt.Sprintf("Getting backuip UID for backup %s", backupName))
			_, err = DeleteBackup(backupName, backupUID, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying backup deletion - %s", backupName))
		}

		// Cleanup all restores
		allRestores, err := GetAllRestoresAdmin()
		for _, restoreName := range allRestores {
			err = DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying restore deletion - %s", restoreName))
		}

		// Deleting clusters and the corresponding cloud cred
		var clusterCredName string
		var clusterCredUID string
		kubeconfigs := os.Getenv("KUBECONFIGS")
		kubeconfigList := strings.Split(kubeconfigs, ",")
		for _, kubeconfig := range kubeconfigList {
			clusterName := strings.Split(kubeconfig, "-")[0] + "-cluster"
			isPresent, err := IsClusterPresent(clusterName, ctx, BackupOrgID)
			if err != nil {
				Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Verifying if cluster [%s] is present", clusterName))
			}
			if isPresent {
				clusterReq := &api.ClusterInspectRequest{OrgId: BackupOrgID, Name: clusterName}
				clusterResp, err := Inst().Backup.InspectCluster(ctx, clusterReq)
				if err != nil {
					if strings.Contains(err.Error(), "object not found") {
						log.InfoD("Cluster %s is not present", clusterName)
					} else {
						Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Inspecting cluster [%s]", clusterName))
					}
				} else {
					clusterObj := clusterResp.GetCluster()
					clusterProvider := GetClusterProviders()
					for _, provider := range clusterProvider {
						switch provider {
						case drivers.ProviderRke:
							if clusterObj.PlatformCredentialRef != nil {
								clusterCredName = clusterObj.PlatformCredentialRef.Name
								clusterCredUID = clusterObj.PlatformCredentialRef.Uid
							} else {
								log.Warnf("the platform credential ref of the cluster [%s] is nil", clusterName)
							}
						default:
							if clusterObj.CloudCredentialRef != nil {
								clusterCredName = clusterObj.CloudCredentialRef.Name
								clusterCredUID = clusterObj.CloudCredentialRef.Uid
							} else {
								log.Warnf("the cloud credential ref of the cluster [%s] is nil", clusterName)
							}
						}

						err = DeleteCluster(clusterName, BackupOrgID, ctx, false)
						Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", clusterName))
						clusterDeleteStatus := func() (interface{}, bool, error) {
							status, err := IsClusterPresent(clusterName, ctx, BackupOrgID)
							if err != nil {
								return "", true, fmt.Errorf("cluster %s still present with error %v", clusterName, err)
							}
							if status {
								return "", true, fmt.Errorf("cluster %s is not deleted yet", clusterName)
							}
							return "", false, nil
						}
						_, err = task.DoRetryWithTimeout(clusterDeleteStatus, ClusterDeleteTimeout, ClusterDeleteRetryTime)
						Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", clusterName))
						if clusterCredName != "" {
							err = DeleteCloudCredential(clusterCredName, BackupOrgID, clusterCredUID)
							Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of cluster cloud cred [%s]", clusterCredName))
						}
					}
				}
			}
		}

		// Cleanup all backup locations
		allBackupLocations, err := GetAllBackupLocations(ctx)
		dash.VerifySafely(err, nil, "Verifying fetching of all backup locations")
		for backupLocationUid, backupLocationName := range allBackupLocations {
			err = DeleteBackupLocation(backupLocationName, backupLocationUid, BackupOrgID, true)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying backup location deletion - %s", backupLocationName))
		}

		backupLocationDeletionSuccess := func() (interface{}, bool, error) {
			allBackupLocations, err := GetAllBackupLocations(ctx)
			dash.VerifySafely(err, nil, "Verifying fetching of all backup locations")
			if len(allBackupLocations) > 0 {
				return "", true, fmt.Errorf("found %d backup locations", len(allBackupLocations))
			} else {
				return "", false, nil
			}
		}
		_, err = DoRetryWithTimeoutWithGinkgoRecover(backupLocationDeletionSuccess, 5*time.Minute, 30*time.Second)
		dash.VerifySafely(err, nil, "Verifying backup location deletion success")

		// Cleanup all cloud credentials
		allCloudCredentials, err := GetAllCloudCredentials(ctx)
		dash.VerifySafely(err, nil, "Verifying fetching of all cloud credentials")
		for cloudCredentialUid, cloudCredentialName := range allCloudCredentials {
			err = DeleteCloudCredential(cloudCredentialName, BackupOrgID, cloudCredentialUid)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cloud cred %s", cloudCredentialName))
		}
		cloudCredentialDeletionSuccess := func() (interface{}, bool, error) {
			allCloudCredentials, err := GetAllCloudCredentials(ctx)
			dash.VerifySafely(err, nil, "Verifying fetching of all cloud credentials")
			if len(allCloudCredentials) > 0 {
				return "", true, fmt.Errorf("found %d cloud credentials", len(allBackupLocations))
			} else {
				return "", false, nil
			}
		}
		_, err = DoRetryWithTimeoutWithGinkgoRecover(cloudCredentialDeletionSuccess, 5*time.Minute, 30*time.Second)
		dash.VerifySafely(err, nil, "Verifying cloud credential deletion success")

		// Cleanup all buckets after suite
		providers := GetBackupProviders()
		for _, provider := range providers {
			switch provider {
			case drivers.ProviderAws:
				DeleteBucket(provider, GlobalAWSBucketName)
				log.Infof("Bucket deleted - %s", GlobalAWSBucketName)
			case drivers.ProviderAzure:
				DeleteBucket(provider, GlobalAzureBucketName)
				log.Infof("Bucket deleted - %s", GlobalAzureBucketName)
			case drivers.ProviderGke:
				DeleteBucket(provider, GlobalGCPBucketName)
				log.Infof("Bucket deleted - %s", GlobalGCPBucketName)
			case drivers.ProviderNfs:
				DeleteBucket(provider, GlobalNFSBucketName)
				log.Infof("NFS subpath deleted - %s", GlobalNFSBucketName)
			}
		}

		// Cleanup all non admin users
		allUsers, err := backup.GetAllUsers()
		dash.VerifySafely(err, nil, "Verifying cleaning up of all users from keycloak")
		for _, user := range allUsers {
			if !strings.Contains(user.Name, "admin") {
				err = backup.DeleteUser(user.Name)
				dash.VerifySafely(err, nil, fmt.Sprintf("Verifying user [%s] deletion", user.Name))
			} else {
				log.Infof("User %s was not deleted", user.Name)
			}
		}

		// Cleanup all non admin groups
		allGroups, err := backup.GetAllUsers()
		dash.VerifySafely(err, nil, "Verifying cleaning up of all groups from keycloak")
		for _, group := range allGroups {
			if !strings.Contains(group.Name, "admin") && !strings.Contains(group.Name, "app") {
				err = backup.DeleteGroup(group.Name)
				dash.VerifySafely(err, nil, fmt.Sprintf("Verifying group [%s] deletion", group.Name))
			} else {
				log.Infof("Group %s was not deleted", group.Name)
			}
		}

		// Fetch PVC list for Px-Backup namespace
		pxBackupNamespace, err := backup.GetPxBackupNamespace()
		log.FailOnError(err, "failed to get Px-Backup namespace")
		PvcListAfterRun, err = GetPVCListForNamespace(pxBackupNamespace)
		log.FailOnError(err, "failed to list PVCs after run")
		log.Infof("PVC list after the run is [%s]", PvcListAfterRun)

		//TO DO: Uncomment the below part after fixing ValidatePVCCleanup
		// Verify PVC Cleanup on PX-Backup namespace
		//if err := ValidatePVCCleanup(PvcListBeforeRun, PvcListAfterRun); err != nil {
		//	log.FailOnError(err, "PVC cleanup validation failed")
		//}
		//fmt.Println("PVC cleanup validation passed.")
	}
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
