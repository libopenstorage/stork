package tests

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/apps"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/backup/portworx"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	appsV1 "k8s.io/api/apps/v1"
)

// This test restarts volume driver (PX) while backup is in progress
var _ = Describe("{BackupRestartPX}", func() {
	var (
		appList = Inst().AppList
	)
	var preRuleNameList []string
	var postRuleNameList []string
	var scheduledAppContexts []*scheduler.Context
	labelSelectors := make(map[string]string)
	CloudCredUIDMap := make(map[string]string)
	var backupLocation string
	var backupLocationUID string
	var cloudCredUID string
	backupLocationMap := make(map[string]string)
	var bkpNamespaces []string
	var clusterUid string
	var cloudCredName string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	bkpNamespaces = make([]string, 0)
	backupNamespaceMap := make(map[string]string)
	appContextsToBackupMap := make(map[string][]*scheduler.Context)

	JustBeforeEach(func() {
		StartTorpedoTest("BackupRestartPX", "Restart PX when backup in progress", nil, 55818)
		log.InfoD("Verifying if the pre/post rules for the required apps are present in the list or not")
		for i := 0; i < len(appList); i++ {
			if Contains(postRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["post_action_list"]; ok {
					dash.VerifyFatal(ok, true, "Post Rule details mentioned for the apps")
				}
			}
			if Contains(preRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["pre_action_list"]; ok {
					dash.VerifyFatal(ok, true, "Pre Rule details mentioned for the apps")
				}
			}
		}
		log.InfoD("Deploy applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})
	It("Restart PX when backup in progress", func() {
		Step("Validate applications", func() {
			ValidateApplications(scheduledAppContexts)
		})

		Step("Creating rules for backup", func() {
			log.InfoD("Creating pre rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				preRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "pre")
				log.FailOnError(err, "Creating pre rule for deployed apps failed")
				dash.VerifyFatal(preRuleStatus, true, "Verifying pre rule for backup")
				preRuleNameList = append(preRuleNameList, ruleName)
			}
			log.InfoD("Creating post rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				postRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "post")
				log.FailOnError(err, "Creating post rule for deployed apps failed")
				dash.VerifyFatal(postRuleStatus, true, "Verifying Post rule for backup")
				postRuleNameList = append(postRuleNameList, ruleName)
			}
		})

		Step("Creating cloud credentials", func() {
			log.InfoD("Creating cloud credentials")
			providers := getProviders()
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				cloudCredUID = uuid.New()
				CloudCredUIDMap[cloudCredUID] = cloudCredName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
			}
		})

		Step("Register cluster for backup", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Creating backup location", func() {
			log.InfoD("Creating backup location")
			providers := getProviders()
			for _, provider := range providers {
				backupLocation = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocation
				err := CreateBackupLocation(provider, backupLocation, backupLocationUID, cloudCredName, cloudCredUID,
					getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocation))
			}
		})

		Step("Start backup of application to bucket", func() {
			for _, namespace := range bkpNamespaces {
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				preRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, preRuleNameList[0])
				postRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, postRuleNameList[0])
				backupName := fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				backupNamespaceMap[namespace] = backupName
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				appContextsToBackupMap[backupName] = appContextsToBackup
				_, err = CreateBackupWithoutCheck(ctx, backupName, SourceClusterName, backupLocation, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, preRuleNameList[0], preRuleUid, postRuleNameList[0], postRuleUid)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of backup [%s]", backupName))
			}
		})

		Step(fmt.Sprintf("Restart volume driver nodes starts"), func() {
			log.InfoD("Restart PX on nodes")
			storageNodes := node.GetWorkerNodes()
			for index := range storageNodes {
				// Just restart storage driver on one of the node where volume backup is in progress
				err := Inst().V.RestartDriver(storageNodes[index], nil)
				log.FailOnError(err, "Failed to Restart driver")
				err = Inst().V.WaitDriverUpOnNode(storageNodes[index], time.Minute*5)
				dash.VerifyFatal(err, nil, "Validate volume is up")
			}
		})

		Step("Check if backup is successful when the PX restart happened", func() {
			log.InfoD("Check if backup is successful post px restarts")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName := backupNamespaceMap[namespace]
				err := backupSuccessCheckWithValidation(ctx, backupName, appContextsToBackupMap[backupName], orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success and Validation of the backup [%s]", backupName))

			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		log.InfoD("Deleting backup location, cloud creds and clusters")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})

})

// This test performs basic test of starting an application, backing it up and killing stork while
// performing backup and restores.
var _ = Describe("{KillStorkWithBackupsAndRestoresInProgress}", func() {
	var (
		appList = Inst().AppList
	)
	var preRuleNameList []string
	var postRuleNameList []string
	var scheduledAppContexts []*scheduler.Context
	labelSelectors := make(map[string]string)
	CloudCredUIDMap := make(map[string]string)
	var backupLocation string
	var backupLocationUID string
	var cloudCredUID string
	backupLocationMap := make(map[string]string)
	var clusterUid string
	var cloudCredName string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	bkpNamespaces := make([]string, 0)
	appContextsToBackupMap := make(map[string][]*scheduler.Context)
	var backupNames []string

	JustBeforeEach(func() {
		StartTorpedoTest("KillStorkWithBackupsAndRestoresInProgress", "Kill Stork when backups and restores in progress", nil, 55819)
		log.InfoD("Verifying if the pre/post rules for the required apps are present in the list or not")
		for i := 0; i < len(appList); i++ {
			if Contains(postRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["post_action_list"]; ok {
					dash.VerifyFatal(ok, true, "Post Rule details mentioned for the apps")
				}
			}
			if Contains(preRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["pre_action_list"]; ok {
					dash.VerifyFatal(ok, true, "Pre Rule details mentioned for the apps")
				}
			}
		}
		log.InfoD("Deploy applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})
	It("Kill Stork when backup and restore in-progress", func() {
		Step("Validate applications", func() {
			ValidateApplications(scheduledAppContexts)
		})

		Step("Creating rules for backup", func() {
			log.InfoD("Creating pre rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				preRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "pre")
				log.FailOnError(err, "Creating pre rule for deployed apps failed")
				dash.VerifyFatal(preRuleStatus, true, "Verifying pre rule for backup")
				preRuleNameList = append(preRuleNameList, ruleName)
			}
			log.InfoD("Creating post rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				postRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "post")
				log.FailOnError(err, "Creating post rule for deployed apps failed")
				dash.VerifyFatal(postRuleStatus, true, "Verifying Post rule for backup")
				postRuleNameList = append(postRuleNameList, ruleName)
			}
		})

		Step("Creating cloud credentials", func() {
			log.InfoD("Creating cloud credentials")
			providers := getProviders()
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				cloudCredUID = uuid.New()
				CloudCredUIDMap[cloudCredUID] = cloudCredName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
			}
		})

		Step("Register cluster for backup", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Creating backup location", func() {
			log.InfoD("Creating backup location")
			providers := getProviders()
			for _, provider := range providers {
				backupLocation = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocation
				err := CreateBackupLocation(provider, backupLocation, backupLocationUID, cloudCredName, cloudCredUID,
					getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocation))
			}
		})

		Step("Start backup of application to bucket", func() {
			for _, namespace := range bkpNamespaces {
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				preRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, preRuleNameList[0])
				postRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, postRuleNameList[0])
				backupName := fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				appContextsToBackupMap[backupName] = appContextsToBackup
				_, err = CreateBackupWithoutCheck(ctx, backupName, SourceClusterName, backupLocation, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, preRuleNameList[0], preRuleUid, postRuleNameList[0], postRuleUid)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup %s", backupName))
				backupNames = append(backupNames, backupName)
			}
		})

		Step("Kill stork when backup in progress", func() {
			log.InfoD("Kill stork when backup in progress")
			err := DeletePodWithLabelInNamespace(getPXNamespace(), storkLabel)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Killing stork while backups %s is in progress", backupNames))
		})

		Step("Check if backup is successful when the stork restart happened", func() {
			log.InfoD("Check if backup is successful post stork restarts")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				err := backupSuccessCheckWithValidation(ctx, backupName, appContextsToBackupMap[backupName], orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success and Validation of the backup [%s]", backupName))
			}
		})
		Step("Validate applications", func() {
			ValidateApplications(scheduledAppContexts)
		})
		Step("Restoring the backups application", func() {
			for _, backupName := range backupNames {
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				_, err = CreateRestoreWithoutCheck(fmt.Sprintf("%s-restore", backupName), backupName, nil, SourceClusterName, orgID, ctx)
				log.FailOnError(err, "Failed while trying to restore [%s] the backup [%s]", fmt.Sprintf("%s-restore", backupName), backupName)
			}
		})
		Step("Kill stork when restore in-progress", func() {
			log.InfoD("Kill stork when restore in-progress")
			err := DeletePodWithLabelInNamespace(getPXNamespace(), storkLabel)
			dash.VerifyFatal(err, nil, "Killing stork while all the restores are in progress")
		})
		Step("Check if restore is successful when the stork restart happened", func() {
			log.InfoD("Check if restore is successful post stork restarts")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				restoreName := fmt.Sprintf("%s-restore", backupName)
				err = restoreSuccessCheck(restoreName, orgID, maxWaitPeriodForRestoreCompletionInMinute*time.Minute, 30*time.Second, ctx)
				dash.VerifyFatal(err, nil, "Inspecting the restore success for - "+restoreName)
			}
		})
		Step("Validate applications", func() {
			ValidateApplications(scheduledAppContexts)
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		backupDriver := Inst().Backup
		for _, backupName := range backupNames {
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
			log.Infof("About to delete backup - %s", backupName)
			_, err = DeleteBackup(backupName, backupUID, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting backup - [%s]", backupName))
		}

		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// This test does restart the px-backup pod, Mongo pods during backup sharing
var _ = Describe("{RestartBackupPodDuringBackupSharing}", func() {
	numberOfUsers := 10
	var scheduledAppContexts []*scheduler.Context
	userContexts := make([]context.Context, 0)
	CloudCredUIDMap := make(map[string]string)
	backupMap := make(map[string]string, 0)
	var backupLocation string
	var backupLocationUID string
	var cloudCredUID string
	backupLocationMap := make(map[string]string)
	var bkpNamespaces []string
	var backupNames []string
	var users []string
	var backupName string
	var clusterUid string
	var cloudCredName string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	timeStamp := time.Now().Unix()
	bkpNamespaces = make([]string, 0)

	JustBeforeEach(func() {
		StartTorpedoTest("RestartBackupPodDuringBackupSharing", "Restart backup pod during backup sharing", nil, 82948)
		log.InfoD("Deploy applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})
	It("Restart backup pod during backup sharing", func() {
		Step("Validate applications", func() {
			ValidateApplications(scheduledAppContexts)
		})

		Step("Creating cloud credentials", func() {
			log.InfoD("Creating cloud credentials")
			providers := getProviders()
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, timeStamp)
				cloudCredUID = uuid.New()
				CloudCredUIDMap[cloudCredUID] = cloudCredName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
			}
		})

		Step("Register cluster for backup", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating source %s and destination %s cluster", SourceClusterName, destinationClusterName))
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Creating backup location", func() {
			log.InfoD("Creating backup location")
			providers := getProviders()
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, timeStamp)
				backupLocation = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocation
				err := CreateBackupLocation(provider, backupLocation, backupLocationUID, cloudCredName, cloudCredUID,
					getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocation))
			}
		})

		Step("Start backup of application to bucket", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			backupName = fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespaces[0]})
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocation, backupLocationUID, appContextsToBackup, nil, orgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
			backupNames = append(backupNames, backupName)
		})

		Step("Create users", func() {
			log.InfoD("Creating users")
			users = createUsers(numberOfUsers)
			log.Infof("Created %v users and users list is %v", numberOfUsers, users)
		})

		Step("Share Backup with multiple users", func() {
			log.InfoD("Sharing Backup with multiple users")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = ShareBackup(backupName, nil, users, ViewOnlyAccess, ctx)
			log.FailOnError(err, "Failed to share backup %s with users", backupName)
		})

		Step("Restart backup pod when backup sharing is in-progress", func() {
			log.InfoD("Restart backup pod when backup sharing is in-progress")
			backupPodLabel := make(map[string]string)
			backupPodLabel["app"] = "px-backup"
			pxbNamespace, err := backup.GetPxBackupNamespace()
			dash.VerifyFatal(err, nil, "Getting px-backup namespace")
			err = DeletePodWithLabelInNamespace(pxbNamespace, backupPodLabel)
			dash.VerifyFatal(err, nil, "Restart backup pod when backup sharing is in-progress")
			err = ValidatePodByLabel(backupPodLabel, pxbNamespace, 5*time.Minute, 30*time.Second)
			log.FailOnError(err, "Checking if px-backup pod is in running state")
		})

		Step("Validate the shared backup with users", func() {
			// Get Admin Context - needed to share backup and get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, user := range users {
				// Get user context
				ctxNonAdmin, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "Fetching non admin ctx")
				userContexts = append(userContexts, ctxNonAdmin)

				// Register Source and Destination cluster
				log.InfoD("Registering Source and Destination clusters from user context for user -%s", user)
				err = CreateSourceAndDestClusters(orgID, "", "", ctxNonAdmin)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating source and destination cluster for user %s", user))

				for _, backup := range backupNames {
					// Get Backup UID
					backupDriver := Inst().Backup
					backupUID, err := backupDriver.GetBackupUID(ctx, backup, orgID)
					log.FailOnError(err, "Failed while trying to get backup UID for - %s", backup)
					backupMap[backup] = backupUID

					// Start Restore
					restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
					err = CreateRestore(restoreName, backup, nil, destinationClusterName, orgID, ctxNonAdmin, nil)

					// Restore validation to make sure that the user with cannot restore
					dash.VerifyFatal(strings.Contains(err.Error(), "failed to retrieve backup location"), true,
						fmt.Sprintf("Verifying backup restore [%s] is not possible for backup [%s] with user [%s]", restoreName, backup, user))

				}
			}
		})

		Step("Share Backup with multiple users", func() {
			log.InfoD("Sharing Backup with multiple users")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = ShareBackup(backupName, nil, users, RestoreAccess, ctx)
			log.FailOnError(err, "Failed to share backup %s with users", backupName)
		})

		Step("Restart mongo pods when backup sharing is in-progress", func() {
			log.InfoD("Restart mongo pod when backup sharing is in-progress")
			mongoDBPodLabel := make(map[string]string)
			mongoDBPodLabel["app.kubernetes.io/component"] = mongodbStatefulset
			pxbNamespace, err := backup.GetPxBackupNamespace()
			dash.VerifyFatal(err, nil, "Getting px-backup namespace")
			err = DeletePodWithLabelInNamespace(pxbNamespace, mongoDBPodLabel)
			dash.VerifyFatal(err, nil, "Restart mongo pod when backup sharing is in-progress")
			err = IsMongoDBReady()
			log.FailOnError(err, "Checking if mongo db pod is in running state")
		})
		Step("Validate the shared backup with users", func() {
			for _, user := range users {
				// Get user context
				ctxNonAdmin, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "Fetching non admin ctx")

				for _, backup := range backupNames {
					// Start Restore
					restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
					err = CreateRestore(restoreName, backup, nil, destinationClusterName, orgID, ctxNonAdmin, nil)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Restore the backup %s for user %s", backup, user))

					// Delete restore
					err = DeleteRestore(restoreName, orgID, ctxNonAdmin)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting restore %s", restoreName))

				}
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		log.InfoD("Deleting the backups")
		for _, backup := range backupNames {
			_, err := DeleteBackup(backup, backupMap[backup], orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting the backup %s", backup))
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
		var wg sync.WaitGroup
		log.Infof("Cleaning up users")
		for _, userName := range users {
			wg.Add(1)
			go func(userName string) {
				defer GinkgoRecover()
				defer wg.Done()
				err := backup.DeleteUser(userName)
				log.FailOnError(err, "Error deleting user %v", userName)
			}(userName)
		}
		wg.Wait()
	})
})

// CancelAllRunningBackupJobs cancels all the running backup jobs while backups are in progress
var _ = Describe("{CancelAllRunningBackupJobs}", func() {
	var (
		cloudCredName     string
		cloudCredUID      string
		bkpLocationName   string
		backupLocationUID string
		srcClusterUid     string
		appNamespaces     []string
		backupNames       []string
		srcClusterStatus  api.ClusterInfo_StatusInfo_Status
		destClusterStatus api.ClusterInfo_StatusInfo_Status
		contexts          []*scheduler.Context
		appContexts       []*scheduler.Context
	)
	backupLocationMap := make(map[string]string)
	labelSelectors := make(map[string]string)
	numberOfBackups := 4

	JustBeforeEach(func() {
		StartTorpedoTest("CancelAllRunningBackupJobs",
			"Cancel all the running backup jobs while backups are in progress", nil, 58045)

		log.InfoD("Deploying applications required for the testcase")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
			}
		}
	})

	It("Cancel All Running Backup Jobs and validate", func() {
		var sem = make(chan struct{}, numberOfBackups)
		var wg sync.WaitGroup
		Step("Validating the deployed applications", func() {
			log.InfoD("Validating the deployed applications")
			ValidateApplications(contexts)
		})
		Step("Adding cloud credential and backup location", func() {
			log.InfoD("Adding cloud credential and backup location")
			providers := getProviders()
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cloudcred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("%s-%s-%v-bl", provider, getGlobalBucketName(provider), time.Now().Unix())
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID,
					getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", bkpLocationName))
			}
		})
		Step("Registering source and destination clusters for backup", func() {
			log.InfoD("Registering source and destination clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating source cluster %s and destination cluster %s", SourceClusterName, destinationClusterName))
			srcClusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(srcClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			srcClusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			destClusterStatus, err = Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(destClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
		})
		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range appNamespaces {
				for i := 0; i < numberOfBackups; i++ {
					sem <- struct{}{}
					backupName := fmt.Sprintf("%s-%s-%d-%v", BackupNamePrefix, namespace, i, time.Now().Unix())
					backupNames = append(backupNames, backupName)
					wg.Add(1)
					go func(backupName string, namespace string) {
						defer GinkgoRecover()
						defer wg.Done()
						defer func() { <-sem }()
						_, err = CreateBackupByNamespacesWithoutCheck(backupName, SourceClusterName, bkpLocationName, backupLocationUID,
							[]string{namespace}, labelSelectors, orgID, srcClusterUid, "", "", "", "", ctx)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Taking backup %s of application- %s", backupName, namespace))
					}(backupName, namespace)
				}
			}
			wg.Wait()
			log.Infof("The list of backups taken are: %v", backupNames)
		})
		Step("Cancelling the ongoing backups", func() {
			log.InfoD("Cancelling the ongoing backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			backupInProgressStatus := api.BackupInfo_StatusInfo_InProgress
			backupPendingStatus := api.BackupInfo_StatusInfo_Pending
			for _, backupName := range backupNames {
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, orgID)
					log.FailOnError(err, fmt.Sprintf("Getting UID for backup %v", backupName))
					backupInspectRequest := &api.BackupInspectRequest{
						Name:  backupName,
						Uid:   backupUID,
						OrgId: orgID,
					}
					backupProgressCheckFunc := func() (interface{}, bool, error) {
						resp, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
						if err != nil {
							return "", false, err
						}
						actual := resp.GetBackup().GetStatus().Status
						if actual == backupInProgressStatus {
							return "", false, nil
						}
						if actual == backupPendingStatus {
							return "", true, fmt.Errorf("backup status for [%s] expected was [%v] but got [%s]", backupName, backupInProgressStatus, actual)
						} else {
							return "", false, fmt.Errorf("backup status for [%s] expected was [%v] but got [%s]", backupName, backupInProgressStatus, actual)
						}
					}
					_, err = DoRetryWithTimeoutWithGinkgoRecover(backupProgressCheckFunc, maxWaitPeriodForBackupJobCancellation*time.Minute, backupJobCancellationRetryTime*time.Second)
					dash.VerifySafely(err, nil, fmt.Sprintf("Verfiying backup %s is in progress", backupName))

					_, err = DeleteBackup(backupName, backupUID, orgID, ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting backup %s while backup is in progress", backupName))
				}(backupName)
			}
			wg.Wait()
			log.InfoD("Sleeping for 60 seconds for the backup cancellation to take place")
			time.Sleep(60 * time.Second)
		})
		Step("Verifying if all the backup creation is cancelled", func() {
			log.InfoD("Verifying if all the backup creation is cancelled")
			adminBackups, err := GetAllBackupsAdmin()
			log.FailOnError(err, "Getting the list of backups after backup cancellation")
			log.Infof("The list of backups after backup cancellation is %v", adminBackups)
			if len(adminBackups) != 0 {
				backupJobCancelStatus := func() (interface{}, bool, error) {
					adminBackups, err := GetAllBackupsAdmin()
					if err != nil {
						return "", true, err
					}
					for _, backupName := range backupNames {
						if IsPresent(adminBackups, backupName) {
							return "", true, fmt.Errorf("%v backup is still present", backupName)
						}
					}
					return "", false, nil
				}
				_, err = DoRetryWithTimeoutWithGinkgoRecover(backupJobCancelStatus, backupDeleteTimeout, backupDeleteRetryTime)
				if err != nil {
					adminBackups, error1 := GetAllBackupsAdmin()
					log.FailOnError(error1, "Getting the list of backups after backup cancellation")
					log.Infof("The list of backups still present after backup cancellation is %v,Error:%v", adminBackups, err)
				}
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup jobs cancellation while backups are in progress"))
			}
			log.Infof("All the backups created by this testcase is deleted after backup cancellation")
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.Infof("Deleting the deployed applications")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// ScaleMongoDBWhileBackupAndRestore scales down MongoDB to repl=0 and backup to original replica when backups and restores are in progress
var _ = Describe("{ScaleMongoDBWhileBackupAndRestore}", func() {
	var (
		scheduledAppContexts []*scheduler.Context
		appNamespaces        []string
		cloudCredName        string
		cloudCredUID         string
		bkpLocationName      string
		backupLocationUID    string
		srcClusterUid        string
		srcClusterStatus     api.ClusterInfo_StatusInfo_Status
		destClusterStatus    api.ClusterInfo_StatusInfo_Status
		backupNames          []string
		restoreNames         []string
		pxBackupNS           string
		err                  error
		ctx                  context.Context
		statefulSet          *appsV1.StatefulSet
		originalReplicaCount int32
	)
	backupLocationMap := make(map[string]string)
	labelSelectors := make(map[string]string)
	appContextsToBackupMap := make(map[string][]*scheduler.Context)
	numberOfBackups := 5
	scaledDownReplica := int32(0)

	JustBeforeEach(func() {
		StartTorpedoTest("ScaleMongoDBWhileBackupAndRestore",
			"Scale down MongoDB to repl=0 when backups and restores are in progress", nil, 58075)
		log.InfoD("Deploying applications required for the testcase")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})

	It("Scale down MongoDB when backups/restores are in progress and validate", func() {
		var sem = make(chan struct{}, numberOfBackups)
		var wg sync.WaitGroup
		Step("Validating the deployed applications", func() {
			log.InfoD("Validating the deployed applications")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Adding cloud credential and backup location", func() {
			log.InfoD("Adding cloud credential and backup location")
			providers := getProviders()
			ctx, err = backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cloudcred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("%s-%s-%v-bl", provider, getGlobalBucketName(provider), time.Now().Unix())
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err = CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID,
					getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", bkpLocationName))
			}
		})
		Step("Registering source and destination clusters for backup", func() {
			log.InfoD("Registering source and destination clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating source cluster %s and destination cluster %s", SourceClusterName, destinationClusterName))
			srcClusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(srcClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			srcClusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			destClusterStatus, err = Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(destClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
		})
		Step("Getting the replica factor of mongodb statefulset in backup namespace before taking backup", func() {
			pxBackupNS, err = backup.GetPxBackupNamespace()
			dash.VerifyFatal(err, nil, "Getting backup namespace")
			log.InfoD("Getting the replica factor of mongodb statefulset in backup namespace [%s] before taking backup", pxBackupNS)
			statefulSet, err = apps.Instance().GetStatefulSet(mongodbStatefulset, pxBackupNS)
			dash.VerifyFatal(err, nil, "Getting mongodb statefulset details")
			originalReplicaCount = *statefulSet.Spec.Replicas
			log.Infof("Number of replica for mongodb pod before backup is %v", originalReplicaCount)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Getting mongodb statefulset replica in backup namespace %s", pxBackupNS))
			dash.VerifyFatal(originalReplicaCount > scaledDownReplica, true, "Verifying mongodb statefulset replica before taking backup")
		})
		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err = backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range appNamespaces {
				for i := 0; i < numberOfBackups; i++ {
					sem <- struct{}{}
					backupName := fmt.Sprintf("%s-%s-%d-%v", BackupNamePrefix, namespace, i, time.Now().Unix())
					backupNames = append(backupNames, backupName)
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					appContextsToBackupMap[backupName] = appContextsToBackup
					wg.Add(1)
					go func(backupName string, namespace string, appContextsToBackup []*scheduler.Context) {
						defer GinkgoRecover()
						defer wg.Done()
						defer func() { <-sem }()
						_, err := CreateBackupWithoutCheck(ctx, backupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, srcClusterUid, "", "", "", "")
						dash.VerifyFatal(err, nil, fmt.Sprintf("Taking backup [%s] of application [%s]", backupName, namespace))
					}(backupName, namespace, appContextsToBackup)
				}
			}
			wg.Wait()
			log.Infof("The list of backups taken are: %v", backupNames)
		})
		Step("Scaling MongoDB statefulset replica to 0 and back to original replica while backup is in progress", func() {
			log.InfoD("Scaling MongoDB statefulset replica to 0 while backup is in progress")
			*statefulSet.Spec.Replicas = scaledDownReplica
			statefulSet, err = apps.Instance().UpdateStatefulSet(statefulSet)
			dash.VerifyFatal(err, nil, "Scaling down MongoDB statefulset replica to 0")
			log.Infof("mongodb replica after scaling to 0 is %v", *statefulSet.Spec.Replicas)
			dash.VerifyFatal(*statefulSet.Spec.Replicas == scaledDownReplica, true, "Verify mongodb statefulset replica after scaling down")
			log.InfoD("Sleeping for 1 minute so that at least one request is hit to mongodb for the created backups")
			time.Sleep(1 * time.Minute)
			log.InfoD("Scaling MongoDB statefulset to original replica while backup is in progress")
			statefulSet, err = apps.Instance().GetStatefulSet(mongodbStatefulset, pxBackupNS)
			dash.VerifyFatal(err, nil, "Getting mongodb statefulset details")
			*statefulSet.Spec.Replicas = originalReplicaCount
			statefulSet, err = apps.Instance().UpdateStatefulSet(statefulSet)
			dash.VerifyFatal(err, nil, "Scaling backup MongoDB statefulset replica to original count")
			log.Infof("mongodb replica after scaling back to original replica is %v", *statefulSet.Spec.Replicas)
			dash.VerifyFatal(*statefulSet.Spec.Replicas == originalReplicaCount, true, "Verify mongodb statefulset replica after scaling back to original")
			log.Infof("Verify that at least one mongodb pod is in Ready state")
			mongoDBPodStatus := func() (interface{}, bool, error) {
				statefulSet, err = apps.Instance().GetStatefulSet(mongodbStatefulset, pxBackupNS)
				if err != nil {
					return "", true, err
				}
				if statefulSet.Status.ReadyReplicas < 2 {
					return "", true, fmt.Errorf("no mongodb pods are ready yet")
				}
				return "", false, nil
			}
			_, err = DoRetryWithTimeoutWithGinkgoRecover(mongoDBPodStatus, podStatusTimeOut, podStatusRetryTime)
			log.FailOnError(err, "Verify status of mongodb pod")
			log.Infof("Number of mongodb pods in Ready state are %v", statefulSet.Status.ReadyReplicas)
			dash.VerifyFatal(statefulSet.Status.ReadyReplicas > 1, true, "Verifying that at least one mongodb pod is in Ready state")
		})
		Step("Check if backup is successful after MongoDB statefulset is scaled back to original replica", func() {
			log.InfoD("Check if backup is successful after MongoDB statefulset is scaled back to original replica")
			backupPodLabel := map[string]string{
				"app": "px-backup",
			}
			err = ValidatePodByLabel(backupPodLabel, pxBackupNS, 5*time.Minute, 30*time.Second)
			log.FailOnError(err, "Checking if px-backup pod is in running state")
			ctx, err = backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				err := backupSuccessCheckWithValidation(ctx, backupName, appContextsToBackupMap[backupName], orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success and Validation of the backup [%s]", backupName))
			}
		})
		Step("Restoring the backups taken", func() {
			log.InfoD("Restoring the backups taken")
			ctx, err = backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				sem <- struct{}{}
				restoreName := fmt.Sprintf("%s-restore", backupName)
				restoreNames = append(restoreNames, restoreName)
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					defer func() { <-sem }()
					_, err = CreateRestoreWithoutCheck(restoreName, backupName, nil, destinationClusterName, orgID, ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Restoring the backup %s with name %s", backupName, restoreName))
				}(backupName)
			}
			wg.Wait()
			log.Infof("The list of restores are: %v", restoreNames)
		})
		Step("Scaling MongoDB statefulset replica to 0 and back to original replica while restore is in progress", func() {
			log.InfoD("Scaling MongoDB statefulset replica to 0 while restore is in progress")
			statefulSet, err = apps.Instance().GetStatefulSet(mongodbStatefulset, pxBackupNS)
			dash.VerifyFatal(err, nil, "Getting mongodb statefulset details")
			*statefulSet.Spec.Replicas = scaledDownReplica
			statefulSet, err = apps.Instance().UpdateStatefulSet(statefulSet)
			dash.VerifyFatal(err, nil, "Scaling down MongoDB statefulset replica to 0")
			log.Infof("mongodb replica after scaling to 0 is %v", *statefulSet.Spec.Replicas)
			dash.VerifyFatal(*statefulSet.Spec.Replicas == scaledDownReplica, true, "Getting mongodb statefulset replica after scaling down")
			log.InfoD("Sleeping for 1 minute so that at least one request is hit to mongodb for the restores taken")
			time.Sleep(1 * time.Minute)
			log.InfoD("Scaling MongoDB statefulset to original replica while restore is in progress")
			statefulSet, err = apps.Instance().GetStatefulSet(mongodbStatefulset, pxBackupNS)
			dash.VerifyFatal(err, nil, "Getting mongodb statefulset details")
			*statefulSet.Spec.Replicas = originalReplicaCount
			statefulSet, err = apps.Instance().UpdateStatefulSet(statefulSet)
			dash.VerifyFatal(err, nil, "Scaling back MongoDB statefulset replica to original count")
			log.Infof("mongodb replica after scaling back to original replica is %v", *statefulSet.Spec.Replicas)
			dash.VerifyFatal(*statefulSet.Spec.Replicas == originalReplicaCount, true, "Verify mongodb statefulset replica after scaling back to original")
			log.Infof("Verify that at least one mongodb pod is in Ready state")
			mongoDBPodStatus := func() (interface{}, bool, error) {
				statefulSet, err = apps.Instance().GetStatefulSet(mongodbStatefulset, pxBackupNS)
				if err != nil {
					return "", true, err
				}
				if statefulSet.Status.ReadyReplicas < 1 {
					return "", true, fmt.Errorf("no mongodb pods are ready yet")
				}
				return "", false, nil
			}
			_, err = DoRetryWithTimeoutWithGinkgoRecover(mongoDBPodStatus, podStatusTimeOut, podStatusRetryTime)
			log.FailOnError(err, "Verify status of mongodb pod")
			log.Infof("Number of mongodb pods in Ready state are %v", statefulSet.Status.ReadyReplicas)
			dash.VerifyFatal(statefulSet.Status.ReadyReplicas > 0, true, "Verifying that at least one mongodb pod is in Ready state")
		})
		Step("Check if restore is successful after MongoDB statefulset is scaled back to original replica", func() {
			log.InfoD("Check if restore is successful after MongoDB statefulset is scaled back to original replica")
			backupPodLabel := map[string]string{
				"app": "px-backup",
			}
			err = ValidatePodByLabel(backupPodLabel, pxBackupNS, 5*time.Minute, 30*time.Second)
			log.FailOnError(err, "Checking if px-backup pod is in running state")
			ctx, err = backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, restoreName := range restoreNames {
				err = restoreSuccessCheck(restoreName, orgID, maxWaitPeriodForRestoreCompletionInMinute*time.Minute, 30*time.Second, ctx)
				dash.VerifyFatal(err, nil, "Verifying the restore status for restore-"+restoreName)
			}
		})
	})

	JustAfterEach(func() {
		var wg sync.WaitGroup
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Updating the mongodb statefulset replica count as it was at the start of this testcase")
		statefulSet, err = apps.Instance().GetStatefulSet(mongodbStatefulset, pxBackupNS)
		dash.VerifySafely(err, nil, "Getting mongodb statefulset details")
		if *statefulSet.Spec.Replicas != originalReplicaCount {
			*statefulSet.Spec.Replicas = originalReplicaCount
			statefulSet, err = apps.Instance().UpdateStatefulSet(statefulSet)
			dash.VerifySafely(err, nil, "Scaling back MongoDB statefulset replica to original count")
		}
		err := IsMongoDBReady()
		dash.VerifySafely(err, nil, "Validating Mongo DB pods")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.Infof("Deleting the deployed applications")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		log.InfoD("Deleting the restores taken")
		for _, restoreName := range restoreNames {
			wg.Add(1)
			go func(restoreName string) {
				defer GinkgoRecover()
				defer wg.Done()
				err = DeleteRestore(restoreName, orgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))
			}(restoreName)
		}
		wg.Wait()
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// RebootNodesWhenBackupsAreInProgress reboots worker nodes from application cluster when backup is in progress
// source cluster is backup cluster and destination cluster is application cluster for this testcase
var _ = Describe("{RebootNodesWhenBackupsAreInProgress}", func() {
	var (
		scheduledAppContexts     []*scheduler.Context
		appNamespaces            []string
		cloudCredName            string
		cloudCredUID             string
		bkpLocationName          string
		backupLocationUID        string
		destClusterUid           string
		srcClusterStatus         api.ClusterInfo_StatusInfo_Status
		destClusterStatus        api.ClusterInfo_StatusInfo_Status
		backupNames              []string
		newBackupNames           []string
		listOfStorageDriverNodes []node.Node
		ctx                      context.Context
	)
	labelSelectors := make(map[string]string)
	numberOfBackups, _ := strconv.Atoi(getEnv(maxBackupsToBeCreated, "2"))
	backupLocationMap := make(map[string]string)
	appContextsToBackupMap := make(map[string][]*scheduler.Context)
	newAppContextsToBackupMap := make(map[string][]*scheduler.Context)

	JustBeforeEach(func() {
		StartTorpedoTest("RebootNodesWhenBackupsAreInProgress",
			"Reboots node when backup is in progress", nil, 55817)
		var err error
		ctx, err = backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Switching cluster context to application[destination] cluster which does not have px-backup deployed")
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "Switching context to destination cluster failed")
		log.InfoD("Deploying applications required for the testcase on application cluster")

		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < numberOfBackups; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
		log.Infof("The list of namespaces are %v", appNamespaces)
	})
	It("Reboot node when backup is in progress", func() {
		var sem = make(chan struct{}, numberOfBackups)
		var wg sync.WaitGroup

		Step("Validating the deployed applications", func() {
			log.InfoD("Validating the deployed applications on destination cluster")
			ValidateApplications(scheduledAppContexts)
			log.InfoD("Switching cluster context back to source[backup] cluster")
			err := SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster required for creating backup location")
		})

		Step("Adding cloud credential and backup location", func() {
			log.InfoD("Adding cloud credential and backup location")
			providers := getProviders()
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cloudcred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("%s-%s-%v-bl", provider, getGlobalBucketName(provider), time.Now().Unix())
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID,
					getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", bkpLocationName))
			}
		})
		Step("Registering source and destination clusters for backup", func() {
			log.InfoD("Registering source and destination clusters for backup")
			err := CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating source cluster %s and destination cluster %s", SourceClusterName, destinationClusterName))
			srcClusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(srcClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			destClusterStatus, err = Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(destClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
			destClusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, destinationClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", destinationClusterName))
		})
		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			for _, namespace := range appNamespaces {
				sem <- struct{}{}
				backupName := fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				backupNames = append(backupNames, backupName)
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				appContextsToBackupMap[backupName] = appContextsToBackup
				wg.Add(1)
				go func(backupName string, namespace string, appContextsToBackup []*scheduler.Context) {
					defer GinkgoRecover()
					defer wg.Done()
					defer func() { <-sem }()
					// Here we are using destination cluster as application cluster
					_, err := CreateBackupWithoutCheck(ctx, backupName, destinationClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, destClusterUid, "", "", "", "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Taking backup [%s] of application [%s]", backupName, namespace))
				}(backupName, namespace, appContextsToBackup)
			}
			wg.Wait()
			log.InfoD("The list of backups taken are: %v", backupNames)
		})
		Step("Reboot one worker node on application cluster when backup is in progress", func() {
			log.InfoD("Switching cluster context to application[destination] cluster")
			err := SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			listOfStorageDriverNodes = node.GetStorageDriverNodes()
			err = Inst().N.RebootNode(listOfStorageDriverNodes[0], node.RebootNodeOpts{
				Force: true,
				ConnectionOpts: node.ConnectionOpts{
					Timeout:         rebootNodeTimeout,
					TimeBeforeRetry: rebootNodeTimeBeforeRetry,
				},
			})
			dash.VerifyFatal(err, nil, fmt.Sprintf("Rebooting worker node %v", listOfStorageDriverNodes[0].Name))
		})
		Step("Check if backup is successful after one worker node on application cluster is rebooted", func() {
			log.InfoD("Check if backup is successful after one worker node on application cluster is rebooted")
			for _, backupName := range backupNames {
				err := backupSuccessCheckWithValidation(ctx, backupName, appContextsToBackupMap[backupName], orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success and Validation of the backup [%s]", backupName))
			}
		})
		Step("Check if the rebooted node on application cluster is up now", func() {
			log.InfoD("Check if the rebooted node on application cluster is up now")
			listOfStorageDriverNodes = node.GetStorageDriverNodes()
			nodeReadyStatus := func() (interface{}, bool, error) {
				err := Inst().S.IsNodeReady(listOfStorageDriverNodes[0])
				if err != nil {
					return "", true, err
				}
				return "", false, nil
			}
			_, err := DoRetryWithTimeoutWithGinkgoRecover(nodeReadyStatus, K8sNodeReadyTimeout*time.Minute, K8sNodeRetryInterval*time.Second)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the status of rebooted node %s", listOfStorageDriverNodes[0].Name))
			err = Inst().V.WaitDriverUpOnNode(listOfStorageDriverNodes[0], Inst().DriverStartTimeout)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the node driver status of rebooted node %s", listOfStorageDriverNodes[0].Name))
		})
		Step("Validating the deployed applications after node reboot", func() {
			log.InfoD("Validating the deployed applications on destination cluster after node reboot")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Taking new backup of applications", func() {
			log.InfoD("Taking new backup of applications")
			for _, namespace := range appNamespaces {
				sem <- struct{}{}
				backupName := fmt.Sprintf("new-%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				newBackupNames = append(newBackupNames, backupName)
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				newAppContextsToBackupMap[backupName] = appContextsToBackup
				wg.Add(1)
				go func(backupName string, namespace string, appContextsToBackup []*scheduler.Context) {
					defer GinkgoRecover()
					defer wg.Done()
					defer func() { <-sem }()
					// Here we are using destination cluster as application cluster
					_, err := CreateBackupWithoutCheck(ctx, backupName, destinationClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, destClusterUid, "", "", "", "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Taking backup [%s] of application [%s]", backupName, namespace))
				}(backupName, namespace, appContextsToBackup)
			}
			wg.Wait()
			log.InfoD("The list of new backups taken are: %v", newBackupNames)
		})
		Step("Reboot 2 worker nodes on application cluster when backup is in progress", func() {
			log.InfoD("Reboot 2 worker node on application cluster when backup is in progress")
			listOfStorageDriverNodes = node.GetStorageDriverNodes()
			for i := 0; i < 2; i++ {
				err := Inst().N.RebootNode(listOfStorageDriverNodes[i], node.RebootNodeOpts{
					Force: true,
					ConnectionOpts: node.ConnectionOpts{
						Timeout:         rebootNodeTimeout,
						TimeBeforeRetry: rebootNodeTimeBeforeRetry,
					},
				})
				dash.VerifyFatal(err, nil, fmt.Sprintf("Rebooting worker node %v", listOfStorageDriverNodes[i].Name))
			}
		})
		Step("Check if backup is successful after two worker nodes are rebooted", func() {
			log.InfoD("Check if backup is successful after two worker nodes are rebooted")
			for _, backupName := range newBackupNames {
				err := backupSuccessCheckWithValidation(ctx, backupName, newAppContextsToBackupMap[backupName], orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success and Validation of the backup [%s]", backupName))
			}
		})
		Step("Check if the rebooted nodes on application cluster are up now", func() {
			log.InfoD("Check if the rebooted nodes on application cluster are up now")
			listOfStorageDriverNodes = node.GetStorageDriverNodes()
			for i := 0; i < 2; i++ {
				nodeReadyStatus := func() (interface{}, bool, error) {
					err := Inst().S.IsNodeReady(listOfStorageDriverNodes[i])
					if err != nil {
						return "", true, err
					}
					return "", false, nil
				}
				_, err := DoRetryWithTimeoutWithGinkgoRecover(nodeReadyStatus, K8sNodeReadyTimeout*time.Minute, K8sNodeRetryInterval*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the status of rebooted node %s", listOfStorageDriverNodes[i].Name))
				err = Inst().V.WaitDriverUpOnNode(listOfStorageDriverNodes[i], Inst().DriverStartTimeout)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the node driver status of rebooted node %s", listOfStorageDriverNodes[i].Name))
			}
		})
		Step("Validating the deployed applications after node reboot", func() {
			log.InfoD("Validating the deployed applications on destination cluster after node reboot")
			ValidateApplications(scheduledAppContexts)
		})
	})

	JustAfterEach(func() {
		defer func() {
			err := SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster")
		}()
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Check if the rebooted nodes on application cluster are up now")
		log.Infof("Switching cluster context to destination cluster")
		err := SetDestinationKubeConfig()
		log.FailOnError(err, "Switching context to destination cluster failed")
		listOfStorageDriverNodes = node.GetStorageDriverNodes()
		for _, node := range listOfStorageDriverNodes {
			nodeReadyStatus := func() (interface{}, bool, error) {
				err := Inst().S.IsNodeReady(node)
				if err != nil {
					return "", true, err
				}
				return "", false, nil
			}
			_, err := DoRetryWithTimeoutWithGinkgoRecover(nodeReadyStatus, K8sNodeReadyTimeout*time.Minute, K8sNodeRetryInterval*time.Second)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the status of rebooted node %s", node.Name))
			err = Inst().V.WaitDriverUpOnNode(node, Inst().DriverStartTimeout)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the node driver status of rebooted node %s", node.Name))
		}
		log.Infof("Deleting the deployed applications on application cluster")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		log.Infof("Switching cluster context back to source cluster")
		err = SetSourceKubeConfig()
		log.FailOnError(err, "Switching context to source cluster")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// ScaleDownPxBackupPodWhileBackupAndRestoreIsInProgress scales down px-backup deployment to 0 and backup to original replica when backups and restores are in progress
var _ = Describe("{ScaleDownPxBackupPodWhileBackupAndRestoreIsInProgress}", func() {
	var (
		scheduledAppContexts []*scheduler.Context
		appNamespaces        []string
		cloudCredName        string
		cloudCredUID         string
		bkpLocationName      string
		backupLocationUID    string
		srcClusterUid        string
		srcClusterStatus     api.ClusterInfo_StatusInfo_Status
		destClusterStatus    api.ClusterInfo_StatusInfo_Status
		backupNames          []string
		restoreNames         []string
		pxBackupNS           string
		err                  error
		ctx                  context.Context
		backupDeployment     *appsV1.Deployment
		originalReplicaCount int32
	)
	backupLocationMap := make(map[string]string)
	labelSelectors := make(map[string]string)
	appContextsToBackupMap := make(map[string][]*scheduler.Context)
	numberOfBackups := 4
	scaledDownReplica := int32(0)

	JustBeforeEach(func() {
		StartTorpedoTest("ScaleDownPxBackupPodWhileBackupAndRestoreIsInProgress",
			"Scale down px-backup deployment to 0 when backups and restores are in progress", nil, 58074)
		log.InfoD("Deploying applications required for the testcase")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})

	It("Scale down px-backup deployment when backups/restores are in progress and validate", func() {
		var sem = make(chan struct{}, numberOfBackups)
		var wg sync.WaitGroup
		Step("Validating the deployed applications", func() {
			log.InfoD("Validating the deployed applications")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Adding cloud credential and backup location", func() {
			log.InfoD("Adding cloud credential and backup location")
			providers := getProviders()
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cloudcred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("%s-%s-%v-bl", provider, getGlobalBucketName(provider), time.Now().Unix())
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				ctx, err = backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", bkpLocationName))
			}
		})
		Step("Registering source and destination clusters for backup", func() {
			log.InfoD("Registering source and destination clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating source cluster %s and destination cluster %s", SourceClusterName, destinationClusterName))
			srcClusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(srcClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			srcClusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			destClusterStatus, err = Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(destClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
		})
		Step("Getting the replica factor of px-backup deployment in backup namespace before taking backup", func() {
			pxBackupNS, err = backup.GetPxBackupNamespace()
			log.FailOnError(err, "Getting backup namespace")
			log.Infof("Getting the replica factor of px-backup deployment in backup namespace [%s] before taking backup", pxBackupNS)
			backupDeployment, err = apps.Instance().GetDeployment(pxBackupDeployment, pxBackupNS)
			log.FailOnError(err, fmt.Sprintf("Getting px-backup deployment replica in backup namespace %s", pxBackupNS))
			originalReplicaCount = *backupDeployment.Spec.Replicas
			log.Infof("Replica count for px-backup pod before taking backup is %v", originalReplicaCount)
			dash.VerifyFatal(originalReplicaCount > scaledDownReplica, true, "Verifying px_backup deployment replica before taking backup")
		})
		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err = backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range appNamespaces {
				for i := 0; i < numberOfBackups; i++ {
					sem <- struct{}{}
					time.Sleep(5 * time.Second)
					backupName := fmt.Sprintf("%s-%s-%d-%v", BackupNamePrefix, namespace, i, time.Now().Unix())
					backupNames = append(backupNames, backupName)
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					appContextsToBackupMap[backupName] = appContextsToBackup
					wg.Add(1)
					go func(backupName string, namespace string, appContextsToBackup []*scheduler.Context) {
						defer GinkgoRecover()
						defer wg.Done()
						defer func() { <-sem }()
						_, err := CreateBackupWithoutCheck(ctx, backupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, srcClusterUid, "", "", "", "")
						dash.VerifyFatal(err, nil, fmt.Sprintf("Taking backup [%s] of application [%s]", backupName, namespace))
					}(backupName, namespace, appContextsToBackup)
				}
			}
			wg.Wait()
			log.InfoD("The list of backups taken are: %v", backupNames)
		})
		Step("Scaling px-backup deployment replica count to 0 and back to original replica while backup is in progress", func() {
			log.InfoD("Scaling px-backup deployment replica count to 0 while backup is in progress")
			*backupDeployment.Spec.Replicas = scaledDownReplica
			updatedBackupDeployment, err := apps.Instance().UpdateDeployment(backupDeployment)
			log.FailOnError(err, fmt.Sprintf("Scaling down px-backup deployment replica to 0"))
			log.Infof("px-backup replica count after scaling to 0 is %v", *updatedBackupDeployment.Spec.Replicas)
			dash.VerifyFatal(*updatedBackupDeployment.Spec.Replicas == scaledDownReplica, true, "Verify px-backup deployment replica after scaling down")
			log.InfoD("Sleeping for 10 minute while backup is in progress and px-backup deployment's replica count is 0")
			time.Sleep(10 * time.Minute)
			log.InfoD("Scaling px-backup deployment to original replica while backup is in progress")
			backupDeployment, err = apps.Instance().GetDeployment(pxBackupDeployment, pxBackupNS)
			log.FailOnError(err, fmt.Sprintf("Getting px_backup deployment before scaling"))
			*backupDeployment.Spec.Replicas = originalReplicaCount
			updatedBackupDeployment, err = apps.Instance().UpdateDeployment(backupDeployment)
			log.FailOnError(err, fmt.Sprintf("Scaling px_backup deployment back to original replica"))
			dash.VerifyFatal(*updatedBackupDeployment.Spec.Replicas, originalReplicaCount, "Verify px-backup deployment replica after scaling back to original")
			log.Infof("Verify px-backup pod status after scaling the replica count to original")
			pxBackupPodStatus := func() (interface{}, bool, error) {
				backupDeployment, err = apps.Instance().GetDeployment(pxBackupDeployment, pxBackupNS)
				if err != nil {
					return "", true, err
				}
				if backupDeployment.Status.ReadyReplicas != originalReplicaCount {
					return "", true, fmt.Errorf("px-backup pod is not ready yet")
				}
				return "", false, nil
			}
			_, err = DoRetryWithTimeoutWithGinkgoRecover(pxBackupPodStatus, podStatusTimeOut, podStatusRetryTime)
			log.FailOnError(err, "Validating if the px_backup pod is ready")
			log.Infof("Number of px-backup pods in Ready state are %v", backupDeployment.Status.ReadyReplicas)
			dash.VerifyFatal(backupDeployment.Status.ReadyReplicas == originalReplicaCount, true, "Verifying if the px-backup pod is in Ready state")
		})
		Step("Check if backup is successful after px-backup deployment is scaled back to original replica", func() {
			log.InfoD("Check if backup is successful after px-backup deployment is scaled back to original replica")
			ctx, err = backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				err := backupSuccessCheckWithValidation(ctx, backupName, appContextsToBackupMap[backupName], orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success and Validation of the backup [%s]", backupName))
			}
		})
		Step("Restoring the backups taken", func() {
			log.InfoD("Restoring the backups taken")
			ctx, err = backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				sem <- struct{}{}
				restoreName := fmt.Sprintf("%s-restore-%v", backupName, time.Now().Unix())
				restoreNames = append(restoreNames, restoreName)
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					defer func() { <-sem }()
					_, err = CreateRestoreWithoutCheck(restoreName, backupName, nil, destinationClusterName, orgID, ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Restoring the backup %s with name %s", backupName, restoreName))
				}(backupName)
			}
			wg.Wait()
			log.InfoD("The list of restores are: %v", restoreNames)
		})
		Step("Scaling px-backup deployment replica to 0 and back to original replica while restore is in progress", func() {
			log.InfoD("Scaling px-backup deployment replica to 0 while restore is in progress")
			*backupDeployment.Spec.Replicas = scaledDownReplica
			updatedBackupDeployment, err := apps.Instance().UpdateDeployment(backupDeployment)
			log.FailOnError(err, fmt.Sprintf("Scaling down px-backup deployment replica to 0"))
			log.Infof("px-backup replica after scaling to 0 is %v", *updatedBackupDeployment.Spec.Replicas)
			dash.VerifyFatal(*updatedBackupDeployment.Spec.Replicas == scaledDownReplica, true, "Verifying px-backup deployment replica after scaling down")
			log.InfoD("Sleeping for 10 minute while restore is in progress and px-backup deployment's replica count is 0")
			time.Sleep(10 * time.Minute)
			log.InfoD("Scaling px-backup deployment to original replica while restore is in progress")
			backupDeployment, err = apps.Instance().GetDeployment(pxBackupDeployment, pxBackupNS)
			log.FailOnError(err, fmt.Sprintf("Getting the deployment before scaling backup to original replica"))
			*backupDeployment.Spec.Replicas = originalReplicaCount
			updatedBackupDeployment, err = apps.Instance().UpdateDeployment(backupDeployment)
			log.FailOnError(err, fmt.Sprintf("Scaling down px-backup deployment replica to original replica count"))
			dash.VerifyFatal(*updatedBackupDeployment.Spec.Replicas, originalReplicaCount, "Verify px-backup deployment replica after scaling back to original replica count")
			log.Infof("Verify that px-backup pod is in Ready state")
			pxBackupPodStatus := func() (interface{}, bool, error) {
				backupDeployment, err = apps.Instance().GetDeployment(pxBackupDeployment, pxBackupNS)
				if err != nil {
					return "", true, err
				}
				if backupDeployment.Status.ReadyReplicas < originalReplicaCount {
					return "", true, fmt.Errorf("px-backup pod is not ready yet")
				}
				return "", false, nil
			}
			_, err = DoRetryWithTimeoutWithGinkgoRecover(pxBackupPodStatus, podStatusTimeOut, podStatusRetryTime)
			log.FailOnError(err, "Validating if the px_backup pod is ready")
			log.Infof("Number of px_backup pod in Ready state are %v", backupDeployment.Status.ReadyReplicas)
			dash.VerifyFatal(backupDeployment.Status.ReadyReplicas == originalReplicaCount, true, "Verifying that px_backup pod is in Ready state")
		})
		Step("Check if restore is successful after px_backup deployment is scaled back to original replica", func() {
			log.InfoD("Check if restore is successful after px_backup deployment is scaled back to original replica")
			ctx, err = backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, restoreName := range restoreNames {
				err = restoreSuccessCheck(restoreName, orgID, maxWaitPeriodForRestoreCompletionInMinute*time.Minute, 30*time.Second, ctx)
				log.FailOnError(err, "Getting the status for restore- "+restoreName)
			}
		})
	})

	JustAfterEach(func() {
		var wg sync.WaitGroup
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Updating the px_backup deployment replica count as it was at the start of this testcase")
		backupDeployment, err = apps.Instance().GetDeployment(pxBackupDeployment, pxBackupNS)
		log.FailOnError(err, "Getting px-backup deployment")
		if *backupDeployment.Spec.Replicas != originalReplicaCount {
			*backupDeployment.Spec.Replicas = originalReplicaCount
			_, err := apps.Instance().UpdateDeployment(backupDeployment)
			log.FailOnError(err, "Updating the px_backup deployment replica count to originalReplicaCount")
		}
		log.Infof("Verify that all the px_backup deployment pod are in Ready state at the end of the testcase")
		pxBackupPodStatus := func() (interface{}, bool, error) {
			backupDeployment, err = apps.Instance().GetDeployment(pxBackupDeployment, pxBackupNS)
			if err != nil {
				return "", true, err
			}
			if backupDeployment.Status.ReadyReplicas != originalReplicaCount {
				return "", true, fmt.Errorf("px_backup pod is not ready yet")
			}
			return "", false, nil
		}
		_, err = DoRetryWithTimeoutWithGinkgoRecover(pxBackupPodStatus, 10*time.Minute, 30*time.Second)
		dash.VerifySafely(err, nil, "Validating if the px_backup pod is ready")
		log.Infof("Number of px_backup pods in Ready state are %v", backupDeployment.Status.ReadyReplicas)
		dash.VerifySafely(backupDeployment.Status.ReadyReplicas == originalReplicaCount, true, "Verifying that px_backup pod is in Ready state at the end of the testcase")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.Infof("Deleting the deployed applications")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		log.InfoD("Deleting the restores taken")
		for _, restoreName := range restoreNames {
			wg.Add(1)
			go func(restoreName string) {
				defer GinkgoRecover()
				defer wg.Done()
				err = DeleteRestore(restoreName, orgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))
			}(restoreName)
		}
		wg.Wait()
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// CancelAllRunningRestoreJobs cancels all the running restore jobs while restores are in progress
var _ = Describe("{CancelAllRunningRestoreJobs}", func() {
	var (
		scheduledAppContexts []*scheduler.Context
		appNamespaces        []string
		cloudAccountName     string
		cloudAccountUID      string
		bkpLocationName      string
		backupLocationUID    string
		srcClusterUid        string
		backupNames          []string
		srcClusterStatus     api.ClusterInfo_StatusInfo_Status
		destClusterStatus    api.ClusterInfo_StatusInfo_Status
		restoreNames         []string
	)
	backupLocationMap := make(map[string]string)
	labelSelectors := make(map[string]string)
	backupNamesMap := make(map[string][]string)
	numberOfBackups := 4

	JustBeforeEach(func() {
		StartTorpedoTest("CancelAllRunningRestoreJobs", "Cancel all the running restore jobs while restores are in progress", nil, 58058)
		log.InfoD("Deploying applications required for the testcase")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})

	It("Cancel All Running Restore Jobs and validate", func() {
		var wg sync.WaitGroup
		Step("Validating the deployed applications", func() {
			log.InfoD("Validating the deployed applications")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Adding cloud account and backup location", func() {
			log.InfoD("Adding cloud account and backup location")
			providers := getProviders()
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudAccountName = fmt.Sprintf("%s-%s-%v", "cloudcred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("%s-%s-%v-bl", provider, getGlobalBucketName(provider), time.Now().Unix())
				cloudAccountUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err := CreateCloudCredential(provider, cloudAccountName, cloudAccountUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud account named [%s] for org [%s] with [%s] as provider", cloudAccountName, orgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudAccountName, cloudAccountUID,
					getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", bkpLocationName))
			}
		})

		Step("Registering source and destination clusters for backup", func() {
			log.InfoD("Registering source and destination clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating source cluster %s and destination cluster %s", SourceClusterName, destinationClusterName))
			srcClusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(srcClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			srcClusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			destClusterStatus, err = Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(destClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
		})

		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range appNamespaces {
				backupNames := make([]string, 0)
				for i := 0; i < numberOfBackups; i++ {
					time.Sleep(10 * time.Second)
					backupName := fmt.Sprintf("%s-%s-%d-%v", BackupNamePrefix, namespace, i, time.Now().Unix())
					backupNames = append(backupNames, backupName)
					wg.Add(1)
					go func(backupName string, namespace string) {
						defer GinkgoRecover()
						defer wg.Done()
						appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
						err := CreateBackupWithValidation(ctx, backupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, srcClusterUid, "", "", "", "")
						dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s] of namespace (scheduled Context) [%s]", backupName, namespace))
					}(backupName, namespace)
				}
				backupNamesMap[namespace] = backupNames
			}
			wg.Wait()
			log.Infof("The list of backups taken are: %v", backupNames)
		})

		Step("Restoring the backed up applications", func() {
			log.InfoD("Restoring the backed up applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range appNamespaces {
				namespaceMapping := make(map[string]string)
				for _, backupName := range backupNamesMap[namespace] {
					restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, backupName)
					restoreNames = append(restoreNames, restoreName)
					customNamespace := fmt.Sprintf("new-namespace-%s", RandomString(10))
					namespaceMapping[namespace] = customNamespace
					wg.Add(1)
					go func(restoreName string, backupName string, namespaceMapping map[string]string) {
						defer GinkgoRecover()
						defer wg.Done()
						_, err = CreateRestoreWithoutCheck(restoreName, backupName, namespaceMapping, destinationClusterName, orgID, ctx)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Restoring the backup %s with name %s", backupName, restoreName))
					}(restoreName, backupName, namespaceMapping)
				}
			}
			wg.Wait()
			log.Infof("The list of restores taken are: %v", restoreNames)
		})

		Step("Cancelling the ongoing restores", func() {
			log.InfoD("Cancelling the ongoing restores")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, restoreName := range restoreNames {
				wg.Add(1)
				go func(restoreName string) {
					defer GinkgoRecover()
					defer wg.Done()
					restoreInspectRequest := &api.RestoreInspectRequest{
						Name:  restoreName,
						OrgId: orgID,
					}
					restoreInProgressStatus := api.RestoreInfo_StatusInfo_InProgress
					restorePendingStatus := api.RestoreInfo_StatusInfo_Pending
					restoreProgressCheckFunc := func() (interface{}, bool, error) {
						resp, err := Inst().Backup.InspectRestore(ctx, restoreInspectRequest)
						if err != nil {
							return "", false, err
						}
						actual := resp.GetRestore().GetStatus().Status
						if actual == restoreInProgressStatus {
							return "", false, nil
						}
						if actual == restorePendingStatus {
							return "", true, fmt.Errorf("restore status for [%s] expected was [%v] but got [%s]", restoreName, restoreInProgressStatus, actual)
						} else {
							return "", false, fmt.Errorf("restore status for [%s] expected was [%v] but got [%s]", restoreName, restoreInProgressStatus, actual)
						}
					}
					_, err := DoRetryWithTimeoutWithGinkgoRecover(restoreProgressCheckFunc, maxWaitPeriodForRestoreCompletionInMinute*time.Minute, restoreJobProgressRetryTime*time.Second)
					dash.VerifySafely(err, nil, fmt.Sprintf("Verfiying restore %s is in progress", restoreName))
					err = DeleteRestore(restoreName, orgID, ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting restore %s while restore is in progress", restoreName))
				}(restoreName)
			}
			wg.Wait()
		})

		Step("Verifying if all the restore creation is cancelled", func() {
			log.InfoD("Verifying if all the restore creation is cancelled")
			adminRestores, err := GetAllRestoresAdmin()
			log.FailOnError(err, "Getting the list of restores after restore cancellation")
			log.Infof("The list of restore after restore cancellation is %v", adminRestores)
			if len(adminRestores) != 0 {
				restoreJobCancelStatus := func() (interface{}, bool, error) {
					adminRestores, err := GetAllRestoresAdmin()
					if err != nil {
						return "", true, err
					}
					for _, restoreName := range restoreNames {
						if IsPresent(adminRestores, restoreName) {
							return "", true, fmt.Errorf("%v restore is still present", restoreName)
						}
					}
					return "", false, nil
				}
				_, err = DoRetryWithTimeoutWithGinkgoRecover(restoreJobCancelStatus, maxWaitPeriodForRestoreJobCancellation*time.Minute, restoreJobCancellationRetryTime*time.Second)
				if err != nil {
					adminRestores, err := GetAllRestoresAdmin()
					log.FailOnError(err, "Getting the list of restores after restore cancellation")
					log.Infof("The list of restores after restore cancellation  %v", adminRestores)
				}
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restore jobs cancellation while restores is in progress"))
			}
			log.Infof("All the restores created by this testcase are deleted after restore cancellation")
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.Infof("Deleting the deployed applications")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		log.InfoD("Deleting the remaining restores in case of failure")
		adminRestores, err := GetAllRestoresAdmin()
		for _, restoreName := range adminRestores {
			err = DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting user restore %s", restoreName))
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudAccountName, cloudAccountUID, ctx)
	})
})
