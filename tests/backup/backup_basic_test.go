package tests

import (
	"fmt"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers"
	"github.com/portworx/torpedo/drivers/backup"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/portworx/torpedo/pkg/aetosutil"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

const (
	storkDeploymentName                       = "stork"
	storkDeploymentNamespace                  = "kube-system"
	clusterName                               = "tp-cluster"
	restoreNamePrefix                         = "tp-restore"
	configMapName                             = "kubeconfigs"
	destinationClusterName                    = "destination-cluster"
	backupLocationName                        = "tp-blocation"
	appReadinessTimeout                       = 10 * time.Minute
	defaultRetryInterval                      = 10 * time.Second
	enumerateBatchSize                        = 100
	taskNamePrefix                            = "backupcreaterestore"
	orgID                                     = "default"
	defaultTimeout                            = 5 * time.Minute
	usersToBeCreated                          = "USERS_TO_CREATE"
	groupsToBeCreated                         = "GROUPS_TO_CREATE"
	maxUsersInGroup                           = "MAX_USERS_IN_GROUP"
	maxBackupsToBeCreated                     = "MAX_BACKUPS"
	maxWaitPeriodForBackupCompletionInMinutes = 20
	globalAWSBucketPrefix                     = "global-aws"
	globalAzureBucketPrefix                   = "global-azure"
	globalGCPBucketPrefix                     = "global-gcp"
	globalAWSLockedBucketPrefix               = "global-aws-locked"
	globalAzureLockedBucketPrefix             = "global-azure-locked"
	globalGCPLockedBucketPrefix               = "global-gcp-locked"
	userName                                  = "testuser"
	firstName                                 = "firstName"
	lastName                                  = "lastName"
	password                                  = "Password1"
)

var (
	// enable all the after suite actions
	wantAllAfterSuiteActions bool = true
	// selectively enable after suite actions by setting wantAllAfterSuiteActions to false and setting these to true
	wantAfterSuiteSystemCheck     bool = false
	wantAfterSuiteValidateCleanup bool = false
	// User should keep updating preRuleApp, postRuleApp
	preRuleApp                  = []string{"cassandra", "postgres"}
	postRuleApp                 = []string{"cassandra"}
	globalAWSBucketName         string
	globalAzureBucketName       string
	globalGCPBucketName         string
	globalAWSLockedBucketName   string
	globalAzureLockedBucketName string
	globalGCPLockedBucketName   string
)

func getBucketNameSuffix() string {
	bucketNameSuffix, present := os.LookupEnv("BUCKET_NAME")
	if present {
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

var dash *aetosutil.Dashboard
var _ = BeforeSuite(func() {
	dash = Inst().Dash
	log.Infof("Init instance")
	InitInstance()
	dash.TestSetBegin(dash.TestSet)
	StartTorpedoTest("Setup buckets", "Creating one generic bucket to be used in all cases", nil, 0)
	defer EndTorpedoTest()
	// Create the first bucket from the list to be used as generic bucket
	providers := getProviders()
	bucketNameSuffix := getBucketNameSuffix()
	for _, provider := range providers {
		switch provider {
		case drivers.ProviderAws:
			globalAWSBucketName = fmt.Sprintf("%s-%s", globalAWSBucketPrefix, bucketNameSuffix)
			CreateBucket(provider, globalAWSBucketName)
			log.Infof("Bucket created with name - %s", globalAWSBucketName)
		case drivers.ProviderAzure:
			globalAzureBucketName = fmt.Sprintf("%s-%s", globalAzureBucketPrefix, bucketNameSuffix)
			CreateBucket(provider, globalAzureBucketName)
			log.Infof("Bucket created with name - %s", globalAzureBucketName)
		case drivers.ProviderGke:
			globalGCPBucketName = fmt.Sprintf("%s-%s", globalGCPBucketPrefix, bucketNameSuffix)
			CreateBucket(provider, globalGCPBucketName)
			log.Infof("Bucket created with name - %s", globalGCPBucketName)
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

	// Cleanup all non admin users
	ctx, err := backup.GetAdminCtxFromSecret()
	log.FailOnError(err, "Fetching px-central-admin ctx")
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

	// Cleanup all backups
	allBackups, err := GetAllBackupsAdmin()
	for _, backupName := range allBackups {
		_, err = DeleteBackup(backupName, getBackupUID(backupName, orgID), orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verifying backup deletion - %s", backupName))
	}

	// Cleanup all restores
	allRestores, err := GetAllRestoresAdmin()
	for _, restoreName := range allRestores {
		err = DeleteRestore(restoreName, orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verifying restore deletion - %s", restoreName))
	}

	// Cleanup all backup locations
	allBackupLocations, err := getAllBackupLocations(ctx)
	dash.VerifySafely(err, nil, "Verifying fetching of all backup locations")
	for backupLocationUid, backupLocationName := range allBackupLocations {
		err = DeleteBackupLocation(backupLocationName, backupLocationUid, orgID)
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
	_, err = task.DoRetryWithTimeout(backupLocationDeletionSuccess, 5*time.Minute, 30*time.Second)
	dash.VerifySafely(err, nil, "Verifying backup location deletion success")

	// Cleanup all cloud credentials
	allCloudCredentials, err := getAllCloudCredentials(ctx)
	dash.VerifySafely(err, nil, "Verifying fetching of all cloud credentials")
	for cloudCredentialUid, cloudCredentialName := range allCloudCredentials {
		DeleteCloudCredential(cloudCredentialName, orgID, cloudCredentialUid)
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
	_, err = task.DoRetryWithTimeout(cloudCredentialDeletionSuccess, 5*time.Minute, 30*time.Second)
	dash.VerifySafely(err, nil, "Verifying backup location deletion success")

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
		}
	}
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
