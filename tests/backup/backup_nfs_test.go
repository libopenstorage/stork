package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	"time"
)

// DeleteNfsExecutorPodWhileBackupAndRestoreInProgress deletes the nfs executor pod while backup and restore are in progress and validates their status
var _ = Describe("{DeleteNfsExecutorPodWhileBackupAndRestoreInProgress}", func() {
	var (
		bkpLocationName          string
		backupLocationUID        string
		schedulePolicyName       string
		schedulePolicyUID        string
		scheduleName             string
		firstSchBackupName       string
		customResourceBackupName string
		clusterUid               string
		singleNamespaceBackup    string
		multiNamespaceRestore    string
		singleNamespaceRestore   string
		currentBackupName        string
		scheduleBackup           string
		customResourceBackup     string
		singleNamespaceBkp       string
		appNamespaces            []string
		restoreNames             []string
		schedulePolicyInterval   = int64(15)
		currentContext           []*scheduler.Context
		contexts                 []*scheduler.Context
		appContexts              []*scheduler.Context
		scheduledAppContexts     []*scheduler.Context
		appContextsToBackup      []*scheduler.Context
		schedulePolicyInfo       *api.SchedulePolicyInfo
	)
	backupLocationMap := make(map[string]string)
	scheduleBackup = "scheduleBackup"
	customResourceBackup = "customResourceBackup"
	singleNamespaceBkp = "singleNamespaceBkp"

	JustBeforeEach(func() {
		StartTorpedoTest("DeleteNfsExecutorPodWhileBackupAndRestoreInProgress", "Delete nfs executor pod while backup and restore are in progress and validate the status", nil, 86105)
		log.InfoD("Scheduling Applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < 5; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
		log.Infof("The list of namespaces deployed are", appNamespaces)

	})
	It("To validate backup and restore status after deleting the respective nfs executor pod while in progress", func() {
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Creating NFS backup location", func() {
			log.InfoD("Creating NFS backup location")
			backupLocationProviders := getProviders()
			for _, provider := range backupLocationProviders {
				bkpLocationName = fmt.Sprintf("%s-%s-%s", provider, getGlobalBucketName(provider), RandomString(10))
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err := CreateBackupLocation(provider, bkpLocationName, backupLocationUID, "", "", getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating NFS backup location %s", bkpLocationName))
			}
		})

		Step("Registering application clusters for backup", func() {
			log.InfoD("Registering application clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			log.FailOnError(err, "Fetching [%s] uid", SourceClusterName)
		})

		Step("Create schedule policy", func() {
			log.InfoD("Creating schedule policy")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			schedulePolicyName = fmt.Sprintf("%s-%v", "periodic-schedule-policy", RandomString(5))
			schedulePolicyInfo = Inst().Backup.CreateIntervalSchedulePolicy(5, schedulePolicyInterval, 5)
			userSchedulePolicyCreateRequest := &api.SchedulePolicyCreateRequest{
				CreateMetadata: &api.CreateMetadata{
					Name:  schedulePolicyName,
					OrgId: orgID,
				},
				SchedulePolicy: schedulePolicyInfo,
			}
			_, err = Inst().Backup.CreateSchedulePolicy(ctx, userSchedulePolicyCreateRequest)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation schedule policy %s", schedulePolicyName))
			schedulePolicyUID, err = Inst().Backup.GetSchedulePolicyUid(orgID, ctx, schedulePolicyName)
			log.FailOnError(err, "failed to fetch uid for schedule policy- %s", schedulePolicyName)
		})

		Step("Validating backup status after deleting the respective nfs executor pod while backup is in progress", func() {
			log.InfoD("Validating backup status after deleting the respective nfs executor pod while backup is in progress")
			backupToNfsExecutorPodNamespaceMapping := map[string]string{scheduleBackup: multiAppNfsPodDeploymentNamespace, customResourceBackup: multiAppNfsPodDeploymentNamespace, singleNamespaceBkp: appNamespaces[0]}
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for backupType, namespace := range backupToNfsExecutorPodNamespaceMapping {
				resourceTypeFilter := make([]string, 0)
				switch backupType {
				case "scheduleBackup":
					log.InfoD("Taking schedule backup of multiple namespaces")
					scheduleName = fmt.Sprintf("schedule-bkp-%v", RandomString(5))
					_, err = CreateScheduleBackupWithoutCheck(scheduleName, SourceClusterName, bkpLocationName, backupLocationUID, appNamespaces, make(map[string]string), orgID, "", "", "", "", schedulePolicyName, schedulePolicyUID, ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of scheduled backup with schedule name [%s]", schedulePolicyName))
					firstSchBackupName, err = GetFirstScheduleBackupName(ctx, scheduleName, orgID)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the name of the first schedule backup"))
					err = suspendBackupSchedule(scheduleName, schedulePolicyName, orgID, ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending backup schedule [%s] so that no new nfs executor pod is deployed for this schedule", scheduleName))
					currentBackupName = firstSchBackupName
					currentContext = scheduledAppContexts
				case "customResourceBackup":
					log.InfoD("Taking custom resource backup of all namespaces")
					customResourceBackupName = fmt.Sprintf("%s-custom-resource-%v", BackupNamePrefix, RandomString(5))
					err = CreateBackupWithCustomResourceTypeWithoutValidation(customResourceBackupName, SourceClusterName, bkpLocationName, backupLocationUID, appNamespaces, make(map[string]string), orgID, clusterUid, "", "", "", "", []string{"PersistentVolumeClaim"}, ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of custom resource backup - %s", customResourceBackupName))
					resourceTypeFilter = append(resourceTypeFilter, "PersistentVolumeClaim")
					currentBackupName = customResourceBackupName
					currentContext = scheduledAppContexts
				case "singleNamespaceBkp":
					log.InfoD("Taking backup of single namespace")
					singleNamespaceBackup = fmt.Sprintf("single-ns-%s-%v-backup", appNamespaces[0], RandomString(5))
					appContextsToBackup = FilterAppContextsByNamespace(scheduledAppContexts, []string{appNamespaces[0]})
					_, err = CreateBackupWithoutCheck(ctx, singleNamespaceBackup, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, make(map[string]string), orgID, clusterUid, "", "", "", "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup %s with single namespace", singleNamespaceBackup))
					currentBackupName = singleNamespaceBackup
					currentContext = appContextsToBackup
				default:
					log.InfoD("Backup type provided is not supported for this testcase")
				}
				log.InfoD("Deleting nfs executor pod while backup %s of type %s is in progress", currentBackupName, backupType)
				err = DeletePodWhileBackupInProgress(ctx, orgID, currentBackupName, namespace, nfsBackupExecutorPodLabel)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Delete nfs executor pod while backup %s of type %s is in progress", currentBackupName, backupType))
				log.InfoD("Verifying backup %s status of type %s after deleting nfs executor pod", currentBackupName, backupType)
				err = backupSuccessCheckWithValidation(ctx, currentBackupName, currentContext, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, resourceTypeFilter...)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of backup:[%s] status of type %s after deleting nfs executor pod", currentBackupName, backupType))
			}
		})

		Step("Validating restore status after deleting the respective nfs executor pod while restore is in progress", func() {
			log.InfoD("Validating restore status after deleting the respective nfs executor pod while restore is in progress")
			multiNamespaceRestore = fmt.Sprintf("multi-ns-%s-%s-%v", restoreNamePrefix, firstSchBackupName, RandomString(5))
			singleNamespaceRestore = fmt.Sprintf("single-ns-%s-%s-%v", restoreNamePrefix, singleNamespaceBackup, RandomString(5))
			restoreToNfsExecutorPodNamespaceAndBackupMapping := map[string][]string{multiNamespaceRestore: {multiAppNfsPodDeploymentNamespace, firstSchBackupName}, singleNamespaceRestore: {appNamespaces[0], singleNamespaceBackup}}
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.Infof("Switching the context to destination cluster as restore nfs executor pod are created in destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			for restoreName, namespaceBackup := range restoreToNfsExecutorPodNamespaceAndBackupMapping {
				log.InfoD("Restoring the backup %s without check", namespaceBackup[1])
				_, err = CreateRestoreWithoutCheck(restoreName, namespaceBackup[1], make(map[string]string), destinationClusterName, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating in-progress restore [%s] from backup %s", restoreName, namespaceBackup[1]))
				restoreNames = append(restoreNames, restoreName)
				log.InfoD("Deleting nfs executor pod while restore %s is in progress", restoreName)
				err = DeletePodWhileRestoreInProgress(ctx, orgID, restoreName, namespaceBackup[0], nfsRestoreExecutorPodLabel)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting nfs executor pod while restore %s is in progress", restoreName))
				log.InfoD("Verifying restore %s status after deleting nfs executor pod", restoreName)
				err = restoreSuccessCheck(restoreName, orgID, maxWaitPeriodForRestoreCompletionInMinute*time.Minute, restoreJobProgressRetryTime*time.Minute, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restore %s taken from backup %v after deleting nfs executor pod", restoreName, namespaceBackup[1]))
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		err := SetSourceKubeConfig()
		log.FailOnError(err, "Switching context to source cluster failed")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.Infof("Deleting the deployed applications")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		log.InfoD("Deleting the restores taken")
		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore %s", restoreName))
		}
		err = DeleteSchedule(scheduleName, SourceClusterName, orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, []string{schedulePolicyName})
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", []string{schedulePolicyName}))
		CleanupCloudSettingsAndClusters(backupLocationMap, "", "", ctx)
	})
})
