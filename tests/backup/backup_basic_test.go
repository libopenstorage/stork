package tests

import (
	"fmt"
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
)

var (
	// enable all the after suite actions
	wantAllAfterSuiteActions bool = true
	// selectively enable after suite actions by setting wantAllAfterSuiteActions to false and setting these to true
	wantAfterSuiteSystemCheck     bool = false
	wantAfterSuiteValidateCleanup bool = false
	// User should keep updating preRuleApp, postRuleApp
	preRuleApp  = []string{"cassandra", "postgres"}
	postRuleApp = []string{"cassandra"}
)

func getBucketName() []string {
	bucketName := os.Getenv("BUCKET_NAME")
	return strings.Split(bucketName, ",")
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
	bucketNames := getBucketName()
	for _, provider := range providers {
		bucketName := fmt.Sprintf("%s-%s", provider, bucketNames[0])
		CreateBucket(provider, bucketName)
	}
})

var _ = AfterSuite(func() {
	StartTorpedoTest("CleanupBuckets", "Removing buckets", nil, 0)
	defer dash.TestSetEnd()
	defer EndTorpedoTest()
	// Cleanup all bucket after suite
	providers := getProviders()
	bucketNames := getBucketName()
	for _, provider := range providers {
		for _, BucketName := range bucketNames {
			bucketName := fmt.Sprintf("%s-%s", provider, BucketName)
			DeleteBucket(provider, bucketName)
		}
	}

})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
