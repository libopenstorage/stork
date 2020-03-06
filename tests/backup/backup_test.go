package tests

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
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
	CreateCluster(ClusterName, CredName, orgID)
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
			namespaceMapping = GetRestoreAppNamespaceMapping(bkpNamespaces)
			ValidateApplications(contexts)
		})

		Step(fmt.Sprintf("Create Backup [%s]", BackupName), func() {
			CreateBackup(BackupName, ClusterName, BLocationName,
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
			_, err := Inst().Backup.CreateNewRestore(RestoreName, BackupName, namespaceMapping, ClusterName, orgID)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to create restore [%s] on cluster [%s]. Error: [%v]",
					RestoreName, ClusterName, err))
		})

		Step(fmt.Sprintf("Wait for Restore [%s] to complete", BackupName), func() {
			err := Inst().Backup.WaitForRestoreCompletion(RestoreName, orgID,
				BackupRestoreCompletionTimeoutMin*time.Minute,
				RetrySeconds*time.Second)
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to wait for restore [%s] to complete. Error: [%v]",
					BackupName, err))

		})
	})
})

func CreateOrganization(orgID string) {
	Step(fmt.Sprintf("Create organization [%s]", orgID), func() {
		backupDriver := Inst().Backup
		req := &api.OrganizationCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name: orgID,
			},
		}
		_, err := backupDriver.CreateOrganization(req)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to create organization [%s]", orgID))
	})
}

// TODO: There is no delete org API
/*func DeleteOrganization(orgID string) {
	Step(fmt.Sprintf("Delete organization [%s]", orgID), func() {
		backupDriver := Inst().Backup
		req := &api.Delete{
			CreateMetadata: &api.CreateMetadata{
				Name: orgID,
			},
		}
		_, err := backupDriver.CreateOrganization(req)
		Expect(err).NotTo(HaveOccurred())
	})
}
*/

func DeleteCloudCredential(name string, orgID string) {
	Step(fmt.Sprintf("Delete cloud credential [%s] in org [%s]", name, orgID), func() {
		backupDriver := Inst().Backup

		credDeleteRequest := &api.CloudCredentialDeleteRequest{
			Name:  name,
			OrgId: orgID,
		}
		backupDriver.DeleteCloudCredential(credDeleteRequest)
		// Best effort cleanup, dont fail test, if deletion fails
		// Expect(err).NotTo(HaveOccurred(),
		//	fmt.Sprintf("Failed to delete cloud credential [%s] in org [%s]", name, orgID))
		// TODO: validate CreateCloudCredentialResponse also
	})

}

func CreateCloudCredential(name string, orgID string) {

	Step(fmt.Sprintf("Create cloud credential [%s] in org [%s]", name, orgID), func() {
		backupDriver := Inst().Backup

		// TODO: add separate function to return cred object based on type
		id := os.Getenv("AWS_ACCESS_KEY_ID")
		Expect(id).NotTo(Equal(""),
			"AWS_ACCESS_KEY_ID Environment variable should not be empty")

		secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
		Expect(secret).NotTo(Equal(""),
			"AWS_SECRET_ACCESS_KEY Environment variable should not be empty")

		awsConfig := &api.AWSConfig{
			AccessKey: id,
			SecretKey: secret,
		}
		credConfig := &api.CloudCredentialInfo_AwsConfig{
			AwsConfig: awsConfig,
		}
		metadata := &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
		}
		credInfo := &api.CloudCredentialInfo{
			Type:   api.CloudCredentialInfo_AWS,
			Config: credConfig,
		}
		credCreateRequest := &api.CloudCredentialCreateRequest{
			CreateMetadata:  metadata,
			CloudCredential: credInfo,
		}
		_, err := backupDriver.CreateCloudCredential(credCreateRequest)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to create cloud credential [%s] in org [%s]", name, orgID))
		// TODO: validate CreateCloudCredentialResponse also
	})

}

func CreateBackupLocation(name string, cloudCred string,
	bLocationType api.BackupLocationInfo_Type, orgID string) {

	Step(fmt.Sprintf("Create backup location [%s] in org [%s]", name, orgID), func() {

		switch bLocationType {
		case api.BackupLocationInfo_S3:
			CreateS3BackupLocation(name, cloudCred, orgID)
		case api.BackupLocationInfo_Azure, api.BackupLocationInfo_Google:
			// TODO add support for other platforms
			return
		default:
			return
		}
	})
}

func CreateS3BackupLocation(name string, cloudCred string, orgID string) {
	backupDriver := Inst().Backup
	path := os.Getenv("BUCKET_NAME")
	Expect(path).NotTo(Equal(""),
		"BUCKET_NAME Environment variable should not be empty")

	endpoint := os.Getenv("S3_ENDPOINT")
	Expect(endpoint).NotTo(Equal(""),
		"S3_ENDPOINT Environment variable should not be empty")

	region := os.Getenv("S3_REGION")
	Expect(endpoint).NotTo(Equal(""),
		"S3_REGION Environment variable should not be empty")

	disableSSL := os.Getenv("S3_DISABLE_SSL")
	Expect(endpoint).NotTo(Equal(""),
		"S3_DISABLE_SSL Environment variable should not be empty")
	disableSSLBool, err := strconv.ParseBool(disableSSL)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("S3_DISABLE_SSL=%s is not a valid boolean value", disableSSL))

	encryptionKey := "torpedo"
	metadata := &api.CreateMetadata{
		Name:  name,
		OrgId: orgID,
	}
	S3Config := &api.S3Config{
		Endpoint:   endpoint,
		Region:     region,
		DisableSsl: disableSSLBool,
	}
	bkpS3Config := &api.BackupLocationInfo_S3Config{
		S3Config: S3Config,
	}
	bLocationInfo := &api.BackupLocationInfo{
		Path:            path,
		EncryptionKey:   encryptionKey,
		CloudCredential: cloudCred,
		Type:            api.BackupLocationInfo_S3,
		Config:          bkpS3Config,
	}
	bLocationCreateReq := &api.BackupLocationCreateRequest{
		CreateMetadata: metadata,
		BackupLocation: bLocationInfo,
	}
	_, err = backupDriver.CreateBackupLocation(bLocationCreateReq)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to create backuplocation [%s] in org [%s]", name, orgID))
}

func DeleteBackupLocation(name string, orgID string) {
	Step(fmt.Sprintf("Delete backup location [%s] in org [%s]", name, orgID), func() {
		backupDriver := Inst().Backup
		bLocationDeleteReq := &api.BackupLocationDeleteRequest{
			Name:  name,
			OrgId: orgID,
		}
		backupDriver.DeleteBackupLocation(bLocationDeleteReq)
		// Best effort cleanup, dont fail test, if deletion fails
		//Expect(err).NotTo(HaveOccurred(),
		//	fmt.Sprintf("Failed to delete backup location [%s] in org [%s]", name, orgID))
		// TODO: validate createBackupLocationResponse also
	})
}

func CreateCluster(name string, cloudCred string, orgID string) {

	Step(fmt.Sprintf("Create cluster [%s] in org [%s]", name, orgID), func() {
		backupDriver := Inst().Backup

		// TODO: Add support for adding multiple clusters from
		// comma separated list of kubeconfig files
		kubeconfigPath := os.Getenv("PXBACKUP_KUBECONFIG")
		Expect(kubeconfigPath).NotTo(Equal(""),
			"PXBACKUP_KUBECONFIG Environment variable should not be empty")

		kubeconfigRaw, err := ioutil.ReadFile(kubeconfigPath)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Error while reading kubeconfig file from location [%s]", kubeconfigPath))

		metadata := &api.CreateMetadata{
			Name:  name,
			OrgId: orgID,
		}
		clusterInfo := &api.ClusterInfo{
			Kubeconfig:      base64.StdEncoding.EncodeToString(kubeconfigRaw),
			CloudCredential: cloudCred,
		}
		clusterCreateReq := &api.ClusterCreateRequest{
			CreateMetadata: metadata,
			Cluster:        clusterInfo,
		}
		_, err = backupDriver.CreateCluster(clusterCreateReq)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to create cluster [%s] in org [%s]", name, orgID))
		// TODO: validate createClusterResponse also
	})
}

func DeleteCluster(name string, orgID string) {

	Step(fmt.Sprintf("Delete cluster [%s] in org [%s]", name, orgID), func() {
		backupDriver := Inst().Backup
		clusterDeleteReq := &api.ClusterDeleteRequest{
			OrgId: orgID,
			Name:  name,
		}
		backupDriver.DeleteCluster(clusterDeleteReq)
		// Best effort cleanup, dont fail test, if deletion fails
		//Expect(err).NotTo(HaveOccurred(),
		//	fmt.Sprintf("Failed to delete cluster [%s] in org [%s]", name, orgID))
	})
}

func CreateBackup(backupName string, clusterName string, bLocation string,
	namespaces []string, labelSelectores map[string]string, orgID string) {

	Step(fmt.Sprintf("Create backup [%s] in org [%s] from cluster [%s]",
		backupName, orgID, clusterName), func() {

		backupDriver := Inst().Backup
		metadata := &api.CreateMetadata{
			Name:  backupName,
			OrgId: orgID,
		}
		bkpCreateRequest := &api.BackupCreateRequest{
			CreateMetadata: metadata,
			BackupLocation: bLocation,
			Cluster:        clusterName,
			Namespaces:     namespaces,
			LabelSelectors: labelSelectores,
		}
		_, err := backupDriver.CreateBackup(bkpCreateRequest)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to create backup [%s] in org [%s]", backupName, orgID))
		// TODO: validate createClusterResponse also

	})
}

func CreateRestore(restoreName string, backupName string,
	clusterName string, orgID string) {

	Step(fmt.Sprintf("Create restore [%s] in org [%s] on cluster [%s]",
		restoreName, orgID, clusterName), func() {

		backupDriver := Inst().Backup
		metadata := &api.CreateMetadata{
			Name:  restoreName,
			OrgId: orgID,
		}

		createRestoreReq := &api.RestoreCreateRequest{
			CreateMetadata: metadata,
			Backup:         backupName,
			Cluster:        clusterName,
		}

		_, err := backupDriver.CreateRestore(createRestoreReq)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to create restore [%s] in org [%s] on cluster [%s]",
				restoreName, orgID, clusterName))
		// TODO: validate createClusterResponse also
	})
}

func DeleteBackup(backupName string, clusterName string, orgID string) {

	Step(fmt.Sprintf("Delete backup [%s] in org [%s] from cluster [%s]",
		backupName, orgID, clusterName), func() {

		backupDriver := Inst().Backup
		bkpDeleteRequest := &api.BackupDeleteRequest{
			Name:  backupName,
			OrgId: orgID,
		}
		backupDriver.DeleteBackup(bkpDeleteRequest)
		// Best effort cleanup, dont fail test, if deletion fails
		//Expect(err).NotTo(HaveOccurred(),
		//	fmt.Sprintf("Failed to delete backup [%s] in org [%s]", backupName, orgID))
		// TODO: validate createClusterResponse also
	})
}

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
