package tests

import (
	"fmt"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

// This testcase verifies Px Backup upgrade
var _ = Describe("{UpgradePxBackup}", func() {

	JustBeforeEach(func() {
		StartTorpedoTest("UpgradePxBackup", "Upgrading backup", nil, 0)
	})
	It("Upgrade Px Backup", func() {
		Step("Upgrade Px Backup", func() {
			log.InfoD("Upgrade Px Backup to version %s", latestPxBackupVersion)
			err := UpgradePxBackup(latestPxBackupVersion)
			dash.VerifyFatal(err, nil, "Verifying Px Backup upgrade completion")
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(make([]*scheduler.Context, 0))
		log.Infof("No cleanup required for this testcase")

	})
})

// StorkUpgradeWithBackup validates backups with stork upgrade
var _ = Describe("{StorkUpgradeWithBackup}", func() {
	var (
		contexts             []*scheduler.Context
		appContexts          []*scheduler.Context
		backupLocationName   string
		backupLocationUID    string
		cloudCredUID         string
		bkpNamespaces        []string
		scheduleNames        []string
		cloudAccountName     string
		backupName           string
		scheduleName         string
		schBackupName        string
		schPolicyUid         string
		upgradeStorkImageStr string
		clusterUid           string
		clusterStatus        api.ClusterInfo_StatusInfo_Status
	)
	var testrailID = 58023 // testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/58023
	labelSelectors := make(map[string]string)
	cloudCredUIDMap := make(map[string]string)
	backupLocationMap := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	timeStamp := strconv.Itoa(int(time.Now().Unix()))
	periodicPolicyName := fmt.Sprintf("%s-%s", "periodic", timeStamp)

	JustBeforeEach(func() {
		StartTorpedoTest("StorkUpgradeWithBackup", "Validates the scheduled backups and creation of new backup after stork upgrade", nil, testrailID)
		log.Infof("Application installation")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})

	It("StorkUpgradeWithBackup", func() {
		Step("Validate deployed applications", func() {
			ValidateApplications(contexts)
		})
		providers := getProviders()
		Step("Adding Cloud Account", func() {
			log.InfoD("Adding cloud account")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudAccountName = fmt.Sprintf("%s-%v", provider, timeStamp)
				cloudCredUID = uuid.New()
				cloudCredUIDMap[cloudCredUID] = cloudAccountName
				err := CreateCloudCredential(provider, cloudAccountName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudAccountName, orgID, provider))
			}
		})

		Step("Adding Backup Location", func() {
			log.InfoD("Adding Backup Location")
			for _, provider := range providers {
				cloudAccountName = fmt.Sprintf("%s-%v", provider, timeStamp)
				backupLocationName = fmt.Sprintf("auto-bl-%v", time.Now().Unix())
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				err := CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudAccountName, cloudCredUID,
					getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of adding backup location - %s", backupLocationName))
			}
		})

		Step("Creating Schedule Policiy", func() {
			log.InfoD("Creating Schedule Policy")
			periodicSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, 15, 5)
			periodicPolicyStatus := Inst().Backup.BackupSchedulePolicy(periodicPolicyName, uuid.New(), orgID, periodicSchedulePolicyInfo)
			dash.VerifyFatal(periodicPolicyStatus, nil, fmt.Sprintf("Verification of creating periodic schedule policy - %s", periodicPolicyName))
		})

		Step("Adding Clusters for backup", func() {
			log.InfoD("Adding application clusters")
			ctx, err := backup.GetAdminCtxFromSecret()
			dash.VerifySafely(err, nil, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of creating source - %s and destination - %s clusters", SourceClusterName, destinationClusterName))
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Creating schedule backups", func() {
			log.InfoD("Creating schedule backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			schPolicyUid, err = Inst().Backup.GetSchedulePolicyUid(orgID, ctx, periodicPolicyName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching uid of periodic schedule policy named [%s]", periodicPolicyName))
			for _, namespace := range bkpNamespaces {
				scheduleName = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				err = CreateScheduleBackup(scheduleName, SourceClusterName, backupLocationName, backupLocationUID, []string{namespace},
					labelSelectors, orgID, "", "", "", "", periodicPolicyName, schPolicyUid, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of creating schedule backup with schedule name - %s", scheduleName))
				scheduleNames = append(scheduleNames, scheduleName)
			}
		})

		Step("Upgrade the stork version", func() {
			log.InfoD("Upgrade the stork version")
			upgradeStorkImageStr = getEnv(upgradeStorkImage, latestStorkImage)
			err := upgradeStorkVersion(upgradeStorkImageStr)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of stork version upgrade to - %s", upgradeStorkImageStr))
		})

		Step("Verifying scheduled backup after stork version upgrade", func() {
			log.InfoD("Verifying scheduled backup after stork version upgrade")
			log.Infof("waiting 15 minutes for another backup schedule to trigger")
			time.Sleep(15 * time.Minute)
			for _, scheduleName := range scheduleNames {
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				allScheduleBackupNames, err := Inst().Backup.GetAllScheduleBackupNames(ctx, scheduleName, orgID)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of all schedule backups with schedule name - %s", scheduleName))
				dash.VerifyFatal(len(allScheduleBackupNames) > 1, true, fmt.Sprintf("Verfiying the backup count is increased for backups with schedule name - %s", scheduleName))
				//Get the status of latest backup
				schBackupName, err = GetLatestScheduleBackupName(ctx, scheduleName, orgID)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of latest schedule backup with schedule name - %s", scheduleName))
				err = backupSuccessCheck(schBackupName, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
				dash.VerifyFatal(err, nil, "Inspecting the backup success for - "+schBackupName)
			}
		})

		Step("Verify creating new backups after upgrade", func() {
			log.InfoD("Verify creating new backups after upgrade")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				err = CreateBackup(backupName, SourceClusterName, backupLocationName, backupLocationUID, []string{namespace},
					labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup [%s] creation", backupName))
			}
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Clean up objects after test execution")
		log.Infof("Deleting backup schedule")
		for _, scheduleName := range scheduleNames {
			err = DeleteSchedule(scheduleName, SourceClusterName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		}
		log.Infof("Deleting backup schedule policy")
		policyList := []string{periodicPolicyName}
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, policyList)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", policyList))
		log.Infof("Deleting the deployed apps after test execution")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudAccountName, cloudCredUID, ctx)
	})
})
