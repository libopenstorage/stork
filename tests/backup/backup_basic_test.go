package tests

import (
	"fmt"
	"github.com/portworx/torpedo/drivers"
	"os"
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
	StartTorpedoTest("CleanupBuckets", "Removing buckets", nil, 0)
	defer dash.TestSetEnd()
	defer EndTorpedoTest()
	// Cleanup all bucket after suite
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
