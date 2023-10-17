package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
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
	"os"
	"strings"
	"testing"
	"time"
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
		return globalAWSBucketName
	case drivers.ProviderAzure:
		return globalAzureBucketName
	case drivers.ProviderGke:
		return globalGCPBucketName
	case drivers.ProviderNfs:
		return globalNFSBucketName
	default:
		return globalAWSBucketName
	}
}

func getGlobalLockedBucketName(provider string) string {
	switch provider {
	case drivers.ProviderAws:
		return globalAWSLockedBucketName
	default:
		log.Errorf("environment variable [%s] not provided with valid values", "PROVIDERS")
		return ""
	}
}

func TestBasic(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_basic.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : Backup", specReporters)
}

// BackupInitInstance initialises instances required for backup
func BackupInitInstance() {
	var err error
	var token string
	var commitID string
	log.Infof("Inside BackupInitInstance")
	err = Inst().S.Init(scheduler.InitOptions{
		SpecDir:            Inst().SpecDir,
		VolDriverName:      Inst().V.String(),
		StorageProvisioner: Inst().Provisioner,
		NodeDriverName:     Inst().N.String(),
	})
	log.FailOnError(err, "Error occurred while Scheduler Driver Initialization")
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
	PxBackupBuildDate, err := GetPxBackupBuildDate()
	log.FailOnError(err, "Error getting Px Backup build date")
	t.Tags["px-backup-version"] = PxBackupVersion
	t.Tags["px-backup-build-date"] = PxBackupBuildDate
	t.Tags["storageProvisioner"] = Inst().Provisioner
	t.Tags["pureVolume"] = fmt.Sprintf("%t", Inst().PureVolumes)
	t.Tags["pureSANType"] = Inst().PureSANType

	Inst().Dash.TestSetUpdate(t)
	// Setting the common password
	commonPassword = backup.PxCentralAdminPwd + RandomString(4)
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
	providers := getProviders()
	bucketNameSuffix := getBucketNameSuffix()
	for _, provider := range providers {
		switch provider {
		case drivers.ProviderAws:
			globalAWSBucketName = fmt.Sprintf("%s-%s", globalAWSBucketPrefix, bucketNameSuffix)
			CreateBucket(provider, globalAWSBucketName)
			log.Infof("Bucket created with name - %s", globalAWSBucketName)
			s3EncryptionPolicy := os.Getenv("S3_ENCRYPTION_POLICY")
			if s3EncryptionPolicy != "" {
				sseDetails, err := s3utils.GetS3SSEDetailsFromEnv()
				log.FailOnError(err, "Failed to get sse details form environment")
				policy, err := GenerateS3BucketPolicy(string(sseDetails.SseType), string(sseDetails.SseEncryptionPolicy), globalAWSBucketName)
				log.FailOnError(err, "Failed to generate s3 bucket policy check for the correctness of policy parameters")
				err = UpdateS3BucketPolicy(globalAWSBucketName, policy)
				log.FailOnError(err, "Failed to apply bucket policy")
				log.Infof("Updated S3 bucket policy - %s", globalAWSBucketName)
			}
		case drivers.ProviderAzure:
			globalAzureBucketName = fmt.Sprintf("%s-%s", globalAzureBucketPrefix, bucketNameSuffix)
			CreateBucket(provider, globalAzureBucketName)
			log.Infof("Bucket created with name - %s", globalAzureBucketName)
		case drivers.ProviderGke:
			globalGCPBucketName = fmt.Sprintf("%s-%s", globalGCPBucketPrefix, bucketNameSuffix)
			CreateBucket(provider, globalGCPBucketName)
			log.Infof("Bucket created with name - %s", globalGCPBucketName)
		case drivers.ProviderNfs:
			globalNFSBucketName = fmt.Sprintf("%s-%s", globalNFSBucketPrefix, RandomString(6))
		}
	}
	lockedBucketNameSuffix, present := os.LookupEnv("LOCKED_BUCKET_NAME")
	if present {
		for _, provider := range providers {
			switch provider {
			case drivers.ProviderAws:
				globalAWSLockedBucketName = fmt.Sprintf("%s-%s", globalAWSLockedBucketPrefix, lockedBucketNameSuffix)
			case drivers.ProviderAzure:
				globalAzureLockedBucketName = fmt.Sprintf("%s-%s", globalAzureLockedBucketPrefix, lockedBucketNameSuffix)
			case drivers.ProviderGke:
				globalGCPLockedBucketName = fmt.Sprintf("%s-%s", globalGCPLockedBucketPrefix, lockedBucketNameSuffix)
			}
		}
	} else {
		log.Infof("Locked bucket name not provided")
	}
})

var _ = AfterSuite(func() {
	StartTorpedoTest("Environment cleanup", "Removing Px-Backup entities created during the test execution", nil, 0)
	defer dash.TestSetEnd()
	defer EndTorpedoTest()

	ctx, err := backup.GetAdminCtxFromSecret()
	log.FailOnError(err, "Fetching px-central-admin ctx")

	//Cleanup policy
	s3EncryptionPolicy := os.Getenv("S3_ENCRYPTION_POLICY")
	if s3EncryptionPolicy != "" {
		err = RemoveS3BucketPolicy(globalAWSBucketName)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verify removal of S3 bucket policy"))
	}

	// Cleanup all backups
	allBackups, err := GetAllBackupsAdmin()
	for _, backupName := range allBackups {
		backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, orgID)
		dash.VerifySafely(err, nil, fmt.Sprintf("Getting backuip UID for backup %s", backupName))
		_, err = DeleteBackup(backupName, backupUID, orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verifying backup deletion - %s", backupName))
	}

	// Cleanup all restores
	allRestores, err := GetAllRestoresAdmin()
	for _, restoreName := range allRestores {
		err = DeleteRestore(restoreName, orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verifying restore deletion - %s", restoreName))
	}

	// Deleting clusters and the corresponding cloud cred
	var clusterCredName string
	var clusterCredUID string
	kubeconfigs := os.Getenv("KUBECONFIGS")
	kubeconfigList := strings.Split(kubeconfigs, ",")
	for _, kubeconfig := range kubeconfigList {
		clusterName := strings.Split(kubeconfig, "-")[0] + "-cluster"
		isPresent, err := IsClusterPresent(clusterName, ctx, orgID)
		if err != nil {
			Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Verifying if cluster [%s] is present", clusterName))
		}
		if isPresent {
			clusterReq := &api.ClusterInspectRequest{OrgId: orgID, Name: clusterName}
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
					err = DeleteCluster(clusterName, orgID, ctx, true)
					Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", clusterName))
					clusterDeleteStatus := func() (interface{}, bool, error) {
						status, err := IsClusterPresent(clusterName, ctx, orgID)
						if err != nil {
							return "", true, fmt.Errorf("cluster %s still present with error %v", clusterName, err)
						}
						if status {
							return "", true, fmt.Errorf("cluster %s is not deleted yet", clusterName)
						}
						return "", false, nil
					}
					_, err = task.DoRetryWithTimeout(clusterDeleteStatus, clusterDeleteTimeout, clusterDeleteRetryTime)
					Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", clusterName))
					if clusterCredName != "" {
						err = DeleteCloudCredential(clusterCredName, orgID, clusterCredUID)
						Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of cluster cloud cred [%s]", clusterCredName))
					}
				}
			}
		}
	}
	// Cleanup all backup locations
	allBackupLocations, err := getAllBackupLocations(ctx)
	dash.VerifySafely(err, nil, "Verifying fetching of all backup locations")
	for backupLocationUid, backupLocationName := range allBackupLocations {
		err = DeleteBackupLocation(backupLocationName, backupLocationUid, orgID, true)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verifying backup location deletion - %s", backupLocationName))
	}

	backupLocationDeletionSuccess := func() (interface{}, bool, error) {
		allBackupLocations, err := getAllBackupLocations(ctx)
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
	allCloudCredentials, err := getAllCloudCredentials(ctx)
	dash.VerifySafely(err, nil, "Verifying fetching of all cloud credentials")
	for cloudCredentialUid, cloudCredentialName := range allCloudCredentials {
		err = DeleteCloudCredential(cloudCredentialName, orgID, cloudCredentialUid)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cloud cred %s", cloudCredentialName))
	}

	cloudCredentialDeletionSuccess := func() (interface{}, bool, error) {
		allCloudCredentials, err := getAllCloudCredentials(ctx)
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
	providers := getProviders()
	for _, provider := range providers {
		switch provider {
		case drivers.ProviderAws:
			DeleteBucket(provider, globalAWSBucketName)
			log.Infof("Bucket deleted - %s", globalAWSBucketName)
		case drivers.ProviderAzure:
			DeleteBucket(provider, globalAzureBucketName)
			log.Infof("Bucket deleted - %s", globalAzureBucketName)
		case drivers.ProviderGke:
			DeleteBucket(provider, globalGCPBucketName)
			log.Infof("Bucket deleted - %s", globalGCPBucketName)
		case drivers.ProviderNfs:
			DeleteBucket(provider, globalNFSBucketName)
			log.Infof("NFS subpath deleted - %s", globalNFSBucketName)
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
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
