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
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
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
	var contexts []*scheduler.Context
	labelSelectors := make(map[string]string)
	CloudCredUIDMap := make(map[string]string)
	var appContexts []*scheduler.Context
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
	It("Restart PX when backup in progress", func() {
		Step("Validate applications", func() {
			ValidateApplications(contexts)
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
				_, err = CreateBackupWithoutCheck(backupName, SourceClusterName, backupLocation, backupLocationUID, []string{namespace},
					labelSelectors, orgID, clusterUid, preRuleNameList[0], preRuleUid, postRuleNameList[0], postRuleUid, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup %s", backupName))
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

				err := backupSuccessCheck(backupName, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
				dash.VerifyFatal(err, nil, "Inspecting the backup success for - "+backupName)

			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)

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
	var contexts []*scheduler.Context
	labelSelectors := make(map[string]string)
	CloudCredUIDMap := make(map[string]string)
	var appContexts []*scheduler.Context
	var backupLocation string
	var backupLocationUID string
	var cloudCredUID string
	backupLocationMap := make(map[string]string)
	var clusterUid string
	var cloudCredName string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	bkpNamespaces := make([]string, 0)
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
	It("Kill Stork when backup and restore in-progress", func() {
		Step("Validate applications", func() {
			ValidateApplications(contexts)
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
				_, err = CreateBackupWithoutCheck(backupName, SourceClusterName, backupLocation, backupLocationUID, []string{namespace},
					labelSelectors, orgID, clusterUid, preRuleNameList[0], preRuleUid, postRuleNameList[0], postRuleUid, ctx)
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
				err = backupSuccessCheck(backupName, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
				dash.VerifyFatal(err, nil, "Inspecting the backup success for - "+backupName)
			}
		})
		Step("Validate applications", func() {
			ValidateApplications(contexts)
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
			ValidateApplications(contexts)
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)

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
	var contexts []*scheduler.Context
	userContexts := make([]context.Context, 0)
	CloudCredUIDMap := make(map[string]string)
	backupMap := make(map[string]string, 0)
	var appContexts []*scheduler.Context
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
	It("Restart backup pod during backup sharing", func() {
		Step("Validate applications", func() {
			ValidateApplications(contexts)
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
			err = CreateBackup(backupName, SourceClusterName, backupLocation, backupLocationUID, []string{bkpNamespaces[0]},
				nil, orgID, clusterUid, "", "", "", "", ctx)
			log.FailOnError(err, "Backup creation failed for backup %s", backupName)
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
			pods, err := core.Instance().GetPods("px-backup", backupPodLabel)
			dash.VerifyFatal(err, nil, "Getting px-backup pod")
			for _, pod := range pods.Items {
				err = core.Instance().ValidatePod(&pod, 5*time.Minute, 30*time.Second)
				log.FailOnError(err, fmt.Sprintf("Failed to validate pod [%s]", pod.GetName()))
			}
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
			backupPodLabel := make(map[string]string)
			backupPodLabel["app.kubernetes.io/component"] = mongodbStatefulset
			pxbNamespace, err := backup.GetPxBackupNamespace()
			dash.VerifyFatal(err, nil, "Getting px-backup namespace")
			err = DeletePodWithLabelInNamespace(pxbNamespace, backupPodLabel)
			dash.VerifyFatal(err, nil, "Restart mongo pod when backup sharing is in-progress")
			pods, err := core.Instance().GetPods("px-backup", backupPodLabel)
			dash.VerifyFatal(err, nil, "Getting mongo pods")
			for _, pod := range pods.Items {
				err = core.Instance().ValidatePod(&pod, 20*time.Minute, 30*time.Second)
				log.FailOnError(err, fmt.Sprintf("Failed to validate pod [%s]", pod.GetName()))
			}
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
		defer EndPxBackupTorpedoTest(contexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)

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
		contexts          []*scheduler.Context
		appContexts       []*scheduler.Context
		appNamespaces     []string
		cloudCredName     string
		cloudCredUID      string
		bkpLocationName   string
		backupLocationUID string
		srcClusterUid     string
		srcClusterStatus  api.ClusterInfo_StatusInfo_Status
		destClusterStatus api.ClusterInfo_StatusInfo_Status
		backupNames       []string
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
					backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, i)
					backupNames = append(backupNames, backupName)
					wg.Add(1)
					go func(backupName string, namespace string) {
						defer GinkgoRecover()
						defer wg.Done()
						defer func() { <-sem }()
						_, err = CreateBackupWithoutCheck(backupName, SourceClusterName, bkpLocationName, backupLocationUID,
							[]string{namespace}, labelSelectors, orgID, srcClusterUid, "", "", "", "", ctx)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Taking backup %s of application- %s", backupName, namespace))
					}(backupName, namespace)
				}
			}
			wg.Wait()
			log.Infof("The list of backups taken are: %v", backupNames)
			log.InfoD("Sleeping for 20 seconds so that the request reaches stork and the backup creation process is started")
			time.Sleep(20 * time.Second)
		})
		Step("Cancelling the ongoing backups", func() {
			log.InfoD("Cancelling the ongoing backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				sem <- struct{}{}
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					defer func() { <-sem }()
					backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, orgID)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Getting UID for backup %v", backupName))
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
			log.Infof("The list of backups after backup cancellation is %v", adminBackups)
			log.FailOnError(err, "Getting the list of backups after backup cancellation")
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
				_, err = task.DoRetryWithTimeout(backupJobCancelStatus, maxWaitPeriodForBackupJobCancellation*time.Minute, backupJobCancellationRetryTime*time.Second)
				if err != nil {
					adminBackups, error := GetAllBackupsAdmin()
					log.Infof("The list of backups after backup cancellation and wait of 10 minutes is %v", adminBackups)
					log.FailOnError(error, "Getting the list of backups after backup cancellation and wait of 10 minutes")
				}
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup jobs cancellation while backup is in progress"))
			}
			log.Infof("All the backups created by this testcase is deleted after backup cancellation")
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		var wg sync.WaitGroup
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.Infof("Deleting the deployed applications")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)

		log.InfoD("Deleting the remaining backups in case of failure")
		adminBackups, err := GetAllBackupsAdmin()
		for _, backupName := range backupNames {
			if IsPresent(adminBackups, backupName) {
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, orgID)
					dash.VerifySafely(err, nil, fmt.Sprintf("Getting backup UID for backup %v", backupName))
					_, err = DeleteBackup(backupName, backupUID, orgID, ctx)
					dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup %s", backupName))
				}(backupName)
			}
		}
		wg.Wait()
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// ScaleMongoDBWhileBackupAndRestore scales down MongoDB to repl=0 and backup to original replica when backups and restores are in progress
var _ = Describe("{ScaleMongoDBWhileBackupAndRestore}", func() {
	var (
		contexts             []*scheduler.Context
		appContexts          []*scheduler.Context
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
	numberOfBackups := 5
	scaledDownReplica := int32(0)

	JustBeforeEach(func() {
		StartTorpedoTest("ScaleMongoDBWhileBackupAndRestore",
			"Scale down MongoDB to repl=0 when backups and restores are in progress", nil, 58075)
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

	It("Scale down MongoDB when backups/restores are in progress and validate", func() {
		var sem = make(chan struct{}, numberOfBackups)
		var wg sync.WaitGroup
		Step("Validating the deployed applications", func() {
			log.InfoD("Validating the deployed applications")
			ValidateApplications(contexts)
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
			ctx, err = backup.GetAdminCtxFromSecret()
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
					backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, i)
					backupNames = append(backupNames, backupName)
					wg.Add(1)
					go func(backupName string, namespace string) {
						defer GinkgoRecover()
						defer wg.Done()
						defer func() { <-sem }()
						_, err = CreateBackupWithoutCheck(backupName, SourceClusterName, bkpLocationName, backupLocationUID,
							[]string{namespace}, labelSelectors, orgID, srcClusterUid, "", "", "", "", ctx)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Taking backup %s of application- %s", backupName, namespace))
					}(backupName, namespace)
				}
			}
			wg.Wait()
			log.Infof("The list of backups taken are: %v", backupNames)
		})
		Step("Scaling MongoDB statefulset replica to 0 and back to original replica while backup is in progress", func() {
			log.InfoD("Scaling MongoDB statefulset replica to 0 while backup is in progress")
			*statefulSet.Spec.Replicas = scaledDownReplica
			statefulSet, err = apps.Instance().UpdateStatefulSet(statefulSet)
			log.Infof("mongodb replica after scaling to 0 is %v", *statefulSet.Spec.Replicas)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Scaling down MongoDB statefulset replica to 0"))
			dash.VerifyFatal(*statefulSet.Spec.Replicas == scaledDownReplica, true, "Verify mongodb statefulset replica after scaling down")
			log.InfoD("Sleeping for 1 minute so that at least one request is hit to mongodb for the created backups")
			time.Sleep(1 * time.Minute)
			log.InfoD("Scaling MongoDB statefulset to original replica while backup is in progress")
			statefulSet, err = apps.Instance().GetStatefulSet(mongodbStatefulset, pxBackupNS)
			*statefulSet.Spec.Replicas = originalReplicaCount
			statefulSet, err = apps.Instance().UpdateStatefulSet(statefulSet)
			log.Infof("mongodb replica after scaling back to original replica is %v", *statefulSet.Spec.Replicas)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Scaling MongoDB statefulset back to original replica"))
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
			_, err = task.DoRetryWithTimeout(mongoDBPodStatus, mongodbPodStatusTimeout, mongodbPodStatusRetryTime)
			log.Infof("Number of mongodb pods in Ready state are %v", statefulSet.Status.ReadyReplicas)
			dash.VerifyFatal(statefulSet.Status.ReadyReplicas > 0, true, "Verifying that at least one mongodb pod is in Ready state")
		})
		Step("Check if backup is successful after MongoDB statefulset is scaled back to original replica", func() {
			log.InfoD("Check if backup is successful after MongoDB statefulset is scaled back to original replica")
			ctx, err = backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				err = backupSuccessCheck(backupName, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
				dash.VerifyFatal(err, nil, "Verifying the backup status for backup - "+backupName)
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
			*statefulSet.Spec.Replicas = scaledDownReplica
			statefulSet, err = apps.Instance().UpdateStatefulSet(statefulSet)
			log.Infof("mongodb replica after scaling to 0 is %v", *statefulSet.Spec.Replicas)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Scaling down MongoDB statefulset replica to 0"))
			dash.VerifyFatal(*statefulSet.Spec.Replicas == scaledDownReplica, true, "Getting mongodb statefulset replica after scaling down")
			log.InfoD("Sleeping for 1 minute so that at least one request is hit to mongodb for the restores taken")
			time.Sleep(1 * time.Minute)
			log.InfoD("Scaling MongoDB statefulset to original replica while restore is in progress")
			statefulSet, err = apps.Instance().GetStatefulSet(mongodbStatefulset, pxBackupNS)
			*statefulSet.Spec.Replicas = originalReplicaCount
			statefulSet, err = apps.Instance().UpdateStatefulSet(statefulSet)
			log.Infof("mongodb replica after scaling back to original replica is %v", *statefulSet.Spec.Replicas)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Scaling MongoDB statefulset back to original replica"))
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
			_, err = task.DoRetryWithTimeout(mongoDBPodStatus, mongodbPodStatusTimeout, mongodbPodStatusRetryTime)
			log.Infof("Number of mongodb pods in Ready state are %v", statefulSet.Status.ReadyReplicas)
			dash.VerifyFatal(statefulSet.Status.ReadyReplicas > 0, true, "Verifying that at least one mongodb pod is in Ready state")
		})
		Step("Check if restore is successful after MongoDB statefulset is scaled back to original replica", func() {
			log.InfoD("Check if restore is successful after MongoDB statefulset is scaled back to original replica")
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
		EndPxBackupTorpedoTest(contexts)
		log.InfoD("Updating the mongodb statefulset replica count as it was at the start of this testcase")
		statefulSet, err = apps.Instance().GetStatefulSet(mongodbStatefulset, pxBackupNS)
		if *statefulSet.Spec.Replicas != originalReplicaCount {
			*statefulSet.Spec.Replicas = originalReplicaCount
			statefulSet, err = apps.Instance().UpdateStatefulSet(statefulSet)
		}
		log.Infof("Verify that all the mongodb pod are in Ready state at the end of the testcase")
		mongoDBPodStatus := func() (interface{}, bool, error) {
			statefulSet, err = apps.Instance().GetStatefulSet(mongodbStatefulset, pxBackupNS)
			if err != nil {
				return "", true, err
			}
			if statefulSet.Status.ReadyReplicas != originalReplicaCount {
				return "", true, fmt.Errorf("mongodb pods are not ready yet")
			}
			return "", false, nil
		}
		_, err = task.DoRetryWithTimeout(mongoDBPodStatus, 10*time.Minute, 30*time.Second)
		log.Infof("Number of mongodb pods in Ready state are %v", statefulSet.Status.ReadyReplicas)
		dash.VerifySafely(statefulSet.Status.ReadyReplicas == originalReplicaCount, true, "Verifying that all the mongodb pods are in Ready state at the end of the testcase")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")

		log.Infof("Deleting the deployed applications")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)
		
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
		contexts          []*scheduler.Context
		appContexts       []*scheduler.Context
		appNamespaces     []string
		cloudCredName     string
		cloudCredUID      string
		bkpLocationName   string
		backupLocationUID string
		destClusterUid    string
		srcClusterStatus  api.ClusterInfo_StatusInfo_Status
		destClusterStatus api.ClusterInfo_StatusInfo_Status
		backupNames       []string
		newBackupNames    []string
		listOfWorkerNodes []node.Node
	)
	labelSelectors := make(map[string]string)
	numberOfBackups, _ := strconv.Atoi(getEnv(maxBackupsToBeCreated, "2"))
	backupLocationMap := make(map[string]string)
	JustBeforeEach(func() {
		StartTorpedoTest("RebootNodesWhenBackupsAreInProgress",
			"Reboots node when backup is in progress", nil, 55817)
		log.InfoD("Switching cluster context to application[destination] cluster which does not have px-backup deployed")
		SetDestinationKubeConfig()
		log.InfoD("Deploying applications required for the testcase on application cluster")
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < numberOfBackups; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
			}
		}
		log.Infof("The list of namespaces are %v", appNamespaces)
	})
	It("Reboot node when backup is in progress", func() {
		var sem = make(chan struct{}, numberOfBackups)
		var wg sync.WaitGroup
		Step("Validating the deployed applications", func() {
			log.InfoD("Validating the deployed applications on destination cluster")
			ValidateApplications(contexts)
		})
		Step("Switching cluster context back to source[backup] cluster", func() {
			log.InfoD("Switching cluster context back to source[backup] cluster")
			err := SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster")
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
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
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
			destClusterStatus, err = Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(destClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
			destClusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, destinationClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", destinationClusterName))
		})
		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range appNamespaces {
				sem <- struct{}{}
				backupName := fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				backupNames = append(backupNames, backupName)
				wg.Add(1)
				go func(backupName string, namespace string) {
					defer GinkgoRecover()
					defer wg.Done()
					defer func() { <-sem }()
					// Here we are using destination cluster as application cluster
					_, err = CreateBackupWithoutCheck(backupName, destinationClusterName, bkpLocationName, backupLocationUID,
						[]string{namespace}, labelSelectors, orgID, destClusterUid, "", "", "", "", ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Taking backup %s of application- %s", backupName, namespace))
				}(backupName, namespace)
			}
			wg.Wait()
			log.InfoD("The list of backups taken are: %v", backupNames)
		})
		Step("Reboot one worker node on application cluster when backup is in progress", func() {
			log.InfoD("Switching cluster context to application[destination] cluster")
			SetDestinationKubeConfig()
			listOfWorkerNodes = node.GetWorkerNodes()
			err := Inst().N.RebootNode(listOfWorkerNodes[0], node.RebootNodeOpts{
				Force: true,
				ConnectionOpts: node.ConnectionOpts{
					Timeout:         rebootNodeTimeout,
					TimeBeforeRetry: rebootNodeTimeBeforeRetry,
				},
			})
			dash.VerifyFatal(err, nil, fmt.Sprintf("Rebooting worker node %v", listOfWorkerNodes[0].Name))
			log.InfoD("Switching cluster context back to source cluster")
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster")

		})
		Step("Check if backup is successful after one worker node on application cluster is rebooted", func() {
			log.InfoD("Check if backup is successful after one worker node on application cluster is rebooted")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				err := backupSuccessCheck(backupName, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
				dash.VerifyFatal(err, nil, "Inspecting backup success for - "+backupName)
			}
		})
		Step("Check if the rebooted node on application cluster is up now", func() {
			log.InfoD("Check if the rebooted node on application cluster is up now")
			log.InfoD("Switching cluster context to destination cluster")
			SetDestinationKubeConfig()
			listOfWorkerNodes = node.GetWorkerNodes()
			nodeReadyStatus := func() (interface{}, bool, error) {
				err := Inst().S.IsNodeReady(listOfWorkerNodes[0])
				if err != nil {
					return "", true, err
				}
				return "", false, nil
			}
			_, err := task.DoRetryWithTimeout(nodeReadyStatus, K8sNodeReadyTimeout*time.Minute, K8sNodeRetryInterval*time.Second)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the status of rebooted node %s", listOfWorkerNodes[0].Name))
			err = Inst().V.WaitDriverUpOnNode(listOfWorkerNodes[0], Inst().DriverStartTimeout)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the node driver status of rebooted node %s", listOfWorkerNodes[0].Name))
			log.InfoD("Switching cluster context back to source[backup] cluster")
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster")
		})
		Step("Taking new backup of applications", func() {
			log.InfoD("Taking new backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range appNamespaces {
				sem <- struct{}{}
				backupName := fmt.Sprintf("new-%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				newBackupNames = append(newBackupNames, backupName)
				wg.Add(1)
				go func(backupName string, namespace string) {
					defer GinkgoRecover()
					defer wg.Done()
					defer func() { <-sem }()
					// Here we are using destination cluster as application cluster
					_, err = CreateBackupWithoutCheck(backupName, destinationClusterName, bkpLocationName, backupLocationUID,
						[]string{namespace}, labelSelectors, orgID, destClusterUid, "", "", "", "", ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Taking backup %s of application- %s", backupName, namespace))
				}(backupName, namespace)
			}
			wg.Wait()
			log.InfoD("The list of new backups taken are: %v", newBackupNames)
		})
		Step("Reboot 2 worker nodes on application cluster when backup is in progress", func() {
			log.InfoD("Reboot 2 worker node on application cluster when backup is in progress")
			log.InfoD("Switching cluster context to application[destination] cluster")
			SetDestinationKubeConfig()
			listOfWorkerNodes = node.GetWorkerNodes()
			for i := 0; i < 2; i++ {
				err := Inst().N.RebootNode(listOfWorkerNodes[i], node.RebootNodeOpts{
					Force: true,
					ConnectionOpts: node.ConnectionOpts{
						Timeout:         rebootNodeTimeout,
						TimeBeforeRetry: rebootNodeTimeBeforeRetry,
					},
				})
				dash.VerifyFatal(err, nil, fmt.Sprintf("Rebooting worker node %v", listOfWorkerNodes[i].Name))
			}
			log.InfoD("Switching cluster context back to source cluster")
			err := SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster")
		})
		Step("Check if backup is successful after two worker nodes are rebooted", func() {
			log.InfoD("Check if backup is successful after two worker nodes are rebooted")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range newBackupNames {
				err := backupSuccessCheck(backupName, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
				dash.VerifyFatal(err, nil, "Inspecting backup success for - "+backupName)
			}
		})
		Step("Check if the rebooted nodes on application cluster are up now", func() {
			log.InfoD("Check if the rebooted nodes on application cluster are up now")
			log.Infof("Switching cluster context to destination cluster")
			SetDestinationKubeConfig()
			listOfWorkerNodes = node.GetWorkerNodes()
			for i := 0; i < 2; i++ {
				nodeReadyStatus := func() (interface{}, bool, error) {
					err := Inst().S.IsNodeReady(listOfWorkerNodes[i])
					if err != nil {
						return "", true, err
					}
					return "", false, nil
				}
				_, err := task.DoRetryWithTimeout(nodeReadyStatus, K8sNodeReadyTimeout*time.Minute, K8sNodeRetryInterval*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the status of rebooted node %s", listOfWorkerNodes[i].Name))
				err = Inst().V.WaitDriverUpOnNode(listOfWorkerNodes[i], Inst().DriverStartTimeout)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the node driver status of rebooted node %s", listOfWorkerNodes[i].Name))
			}
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
		log.InfoD("Check if the rebooted nodes on application cluster are up now")
		log.Infof("Switching cluster context to destination cluster")
		SetDestinationKubeConfig()
		listOfWorkerNodes = node.GetWorkerNodes()
		for _, node := range listOfWorkerNodes {
			nodeReadyStatus := func() (interface{}, bool, error) {
				err := Inst().S.IsNodeReady(node)
				if err != nil {
					return "", true, err
				}
				return "", false, nil
			}
			_, err := task.DoRetryWithTimeout(nodeReadyStatus, K8sNodeReadyTimeout*time.Minute, K8sNodeRetryInterval*time.Second)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the status of rebooted node %s", node.Name))
			err = Inst().V.WaitDriverUpOnNode(node, Inst().DriverStartTimeout)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the node driver status of rebooted node %s", node.Name))
		}
		log.Infof("Deleting the deployed applications on application cluster")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)
		log.Infof("Switching cluster context back to source cluster")
		err := SetSourceKubeConfig()
		log.FailOnError(err, "Switching context to source cluster")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})