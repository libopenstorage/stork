package tests

import (
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/torpedo/drivers/scheduler"
	. "github.com/portworx/torpedo/tests"
)

const (
	orgID                             = "tp-org"
	BLocationName                     = "tp-blocation"
	ClusterName                       = "tp-cluster"
	CredName                          = "tp-backup-cred"
	BackupName                        = "tp-backup"
	RestoreName                       = "tp-restore"
	BackupRestoreCompletionTimeoutMin = 3
	RetrySeconds                      = 30
	ConfigMapName                     = "kubeconfigs"
	KubeconfigDirectory               = "/tmp"
	SourceClusterName                 = "source-cluster"
	DestinationClusterName            = "destination-cluster"
)

func TestBackup(t *testing.T) {
	RegisterFailHandler(Fail)

	var specReporters []Reporter
	junitReporter := reporters.NewJUnitReporter("/testresults/junit_basic.xml")
	specReporters = append(specReporters, junitReporter)
	RunSpecsWithDefaultAndCustomReporters(t, "Torpedo : Backup", specReporters)
}

var _ = BeforeSuite(func() {
	InitInstance()
	CreateOrganization(orgID)
	CreateCloudCredential(CredName, orgID)
	CreateBackupLocation(BLocationName, CredName, api.BackupLocationInfo_S3, orgID)
	CreateSourceAndDestClusters(CredName, orgID)
})

// This test performs basic test of starting an application and destroying it (along with storage)
var _ = Describe("{BackupCreateRestore}", func() {
	var contexts []*scheduler.Context
	var bkpNamespaces []string
	var namespaceMapping map[string]string
	labelSelectores := make(map[string]string)
	It("has to connect and check the backup setup", func() {

		Step("Deploy applications", func() {
			contexts = make([]*scheduler.Context, 0)
			bkpNamespaces = make([]string, 0)
			for i := 0; i < Inst().ScaleFactor; i++ {
				taskName := fmt.Sprintf("backupcreaterestore-%d", i)
				appContexts := ScheduleApplications(taskName)
				contexts = append(contexts, appContexts...)
				for _, ctx := range appContexts {
					bkpNamespaces = append(bkpNamespaces, GetAppNamespace(ctx, taskName))
				}
			}
			//namespaceMapping = GetRestoreAppNamespaceMapping(bkpNamespaces)
			ValidateApplications(contexts)
		})

		Step(fmt.Sprintf("Create Backup [%s]", BackupName), func() {
			CreateBackup(BackupName, SourceClusterName, BLocationName,
				bkpNamespaces, labelSelectores, orgID)
		})

		Step(fmt.Sprintf("Wait for Backup [%s] to complete", BackupName), func() {
			err := Inst().Backup.WaitForBackupCompletion(BackupName, orgID,
				BackupRestoreCompletionTimeoutMin*time.Minute,
				RetrySeconds*time.Second)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to wait for backup [%s] to complete. Error: [%v]",
					BackupName, err))
		})

		Step(fmt.Sprintf("Create Restore [%s]", RestoreName), func() {
			CreateRestore(RestoreName, BackupName,
				namespaceMapping, DestinationClusterName, orgID)
		})

		Step(fmt.Sprintf("Wait for Restore [%s] to complete", RestoreName), func() {
			err := Inst().Backup.WaitForRestoreCompletion(RestoreName, orgID,
				BackupRestoreCompletionTimeoutMin*time.Minute,
				RetrySeconds*time.Second)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to wait for restore [%s] to complete. Error: [%v]",
					BackupName, err))
		})

		Step("teardown all backed up apps", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, nil)
			}
		})

		// Change namespaces to restored apps only after backed up apps are cleaned up
		// to avoid switching back namespaces to backup namespaces
		Step(fmt.Sprintf("Validate Restore [%s]", RestoreName), func() {
			err := ChangeNamespaces(contexts, namespaceMapping)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to change namespace to restored namespace. Error: [%v]", err))
			ValidateApplications(contexts)
		})

		Step("teardown all restored apps", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, nil)
			}
		})

	})
})

func BackupCleanup() {
	DeleteBackup(BackupName, ClusterName, orgID)
	DeleteCluster(ClusterName, orgID)
	DeleteBackupLocation(BLocationName, orgID)
	DeleteCloudCredential(CredName, orgID)
}

var _ = AfterSuite(func() {
	//PerformSystemCheck()
	//ValidateCleanup()
	//	BackupCleanup()
})

func TestMain(m *testing.M) {
	// call flag.Parse() here if TestMain uses flags
	ParseFlags()
	os.Exit(m.Run())
}
