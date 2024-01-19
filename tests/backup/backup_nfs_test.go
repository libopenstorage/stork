package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/torpedo/drivers"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	"strings"
	"sync"
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
		StartPxBackupTorpedoTest("DeleteNfsExecutorPodWhileBackupAndRestoreInProgress", "Delete nfs executor pod while backup and restore are in progress and validate the status", nil, 86105, Sagrawal, Q3FY24)
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
				err := CreateBackupLocation(provider, bkpLocationName, backupLocationUID, "", "", getGlobalBucketName(provider), orgID, "", true)
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

// RemoveJSONFilesFromNFSBackupLocation deletes the .json files from the NFS backup location,check the status of the backups and then perform the restore
var _ = Describe("{RemoveJSONFilesFromNFSBackupLocation}", func() {
	var (
		bkpLocationName          string
		backupLocationUID        string
		customResourceBackupName string
		clusterUid               string
		singleNamespaceBackup    string
		multipleNamespaceBackup  string
		newBackupName            string
		appNamespaces            []string
		appContexts              []*scheduler.Context
		scheduledAppContexts     []*scheduler.Context
		labelSelectors           map[string]string
		globalBucket             string
		backupNames              []string
		restoreNames             []string
		backupLocationMap        = make(map[string]string)
	)

	JustBeforeEach(func() {
		labelSelectors = make(map[string]string)
		StartPxBackupTorpedoTest("RemoveJSONFilesFromNFSBackupLocation", "This TC deletes the .json files from the NFS backup location,check the status of the backups and then perform the restore", nil, 86098, Sabrarhussaini, Q4FY23)
		log.InfoD("Scheduling Applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
		log.Infof("The list of namespaces deployed are", appNamespaces)
	})

	It("To validate the backup and restore when the json files are deleted", func() {
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Creating NFS backup location", func() {
			log.InfoD("Creating NFS backup location")
			globalBucket = getGlobalBucketName(drivers.ProviderNfs)
			bkpLocationName = fmt.Sprintf("%s-%s-%s", drivers.ProviderNfs, globalBucket, RandomString(10))
			backupLocationUID = uuid.New()
			backupLocationMap[backupLocationUID] = bkpLocationName
			err := CreateBackupLocation(drivers.ProviderNfs, bkpLocationName, backupLocationUID, "", "", globalBucket, orgID, "", true)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating NFS backup location %s", bkpLocationName))
		})

		Step("Registering application clusters for backup", func() {
			log.InfoD("Registering application clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.Infof("Creating source [%s] and destination [%s] clusters", SourceClusterName, destinationClusterName)
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with App-User ctx", SourceClusterName, destinationClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			log.FailOnError(err, "Fetching [%s] uid", SourceClusterName)
		})

		Step("Taking backup of single namespaces", func() {
			log.InfoD(fmt.Sprintf("Taking backup of single namespaces [%v]", appNamespaces[0]))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			singleNamespaceBackup = fmt.Sprintf("single-ns-%s-%v-backup", appNamespaces[0], RandomString(5))
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{appNamespaces[0]})
			err = CreateBackupWithValidation(ctx, singleNamespaceBackup, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", singleNamespaceBackup))
			backupNames = append(backupNames, singleNamespaceBackup)
		})

		Step("Taking backup of multiple namespaces", func() {
			log.InfoD(fmt.Sprintf("Taking backup of multiple namespaces [%v]", appNamespaces))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			multipleNamespaceBackup = fmt.Sprintf("multiple-ns-%v-backup", RandomString(5))
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, appNamespaces)
			err = CreateBackupWithValidation(ctx, multipleNamespaceBackup, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", multipleNamespaceBackup))
			backupNames = append(backupNames, multipleNamespaceBackup)
		})

		Step("Taking custom resource backup of all namespaces", func() {
			log.InfoD("Taking custom resource backup of all namespaces")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			customResourceBackupName = fmt.Sprintf("%s-custom-resource-%v", BackupNamePrefix, RandomString(5))
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, appNamespaces)
			err = CreateBackupWithCustomResourceTypeWithValidation(ctx, customResourceBackupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, []string{"PersistentVolumeClaim"}, nil, orgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of custom resource backup - %s", customResourceBackupName))
			backupNames = append(backupNames, customResourceBackupName)
		})

		Step("Remove the JSON files from the NFS backup location for all the backups", func() {
			log.InfoD("Remove the JSON files from the NFS backup location for all the backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, orgID)
				log.FailOnError(err, fmt.Sprintf("Getting UID for backup %v", backupName))
				backupInspectRequest := &api.BackupInspectRequest{
					Name:  backupName,
					Uid:   backupUID,
					OrgId: orgID,
				}
				resp, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
				log.FailOnError(err, fmt.Sprintf("error inspecting backup %v", backupName))
				currentBackupPath := globalBucket + "/" + resp.Backup.BackupPath
				log.Infof("Deleting the JSON files from the NFS backup location for backup %v", backupName)
				err = DeleteFilesFromNFSLocation(currentBackupPath, "*.json")
				log.FailOnError(err, fmt.Sprintf("Faced error while deleting the JSON files from path [%s]", currentBackupPath))
			}
		})

		Step("Verify if the backups are in CloudBackupMissing state after the JSON file deletion", func() {
			log.InfoD("Verify if the backups are in CloudBackupMissing state after the JSON file deletion")
			var wg sync.WaitGroup
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					log.Infof("Verifying the backup status for the backup %v", backupName)
					bkpUid, err := Inst().Backup.GetBackupUID(ctx, backupName, orgID)
					log.FailOnError(err, "Fetching backup uid")
					backupInspectRequest := &api.BackupInspectRequest{
						Name:  backupName,
						Uid:   bkpUid,
						OrgId: orgID,
					}
					requiredStatus := api.BackupInfo_StatusInfo_CloudBackupMissing
					backupCloudBackupMissingCheckFunc := func() (interface{}, bool, error) {
						resp, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
						if err != nil {
							return "", false, err
						}
						actual := resp.GetBackup().GetStatus().Status
						if actual == requiredStatus {
							return "", false, nil
						}
						return "", true, fmt.Errorf("backup status for [%s] expected was [%v] but got [%s]", backupName, requiredStatus, actual)
					}
					_, err = DoRetryWithTimeoutWithGinkgoRecover(backupCloudBackupMissingCheckFunc, 20*time.Minute, 30*time.Second)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verfiying backup %s is in CloudBackup missing state", backupName))
				}(backupName)
			}
			wg.Wait()
		})

		Step("Verify if the restores for the above backups get failed", func() {
			log.InfoD("Verify if the restores for the above backups get failed")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, backupName)
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, appNamespaces)
				err = CreateRestoreWithValidation(ctx, restoreName, backupName, make(map[string]string), make(map[string]string), destinationClusterName, orgID, appContextsToBackup)
				dash.VerifyFatal(strings.Contains(err.Error(), "CloudBackup objects are missing"), true, fmt.Sprintf("Verifying if the restore [%s] is getting Failed after JSON file deletion.", restoreName))
				restoreNames = append(restoreNames, restoreName)
			}
		})

		Step("Taking a new backup of namespaces", func() {
			log.InfoD(fmt.Sprintf("Taking a new backup of namespaces [%v]", appNamespaces))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			newBackupName = fmt.Sprintf("new-backup-%v", RandomString(8))
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, appNamespaces)
			err = CreateBackupWithValidation(ctx, newBackupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", newBackupName))
			backupNames = append(backupNames, newBackupName)
		})

		Step("Verify if the restores for the new backup is successful", func() {
			log.InfoD("Verify if the restores for the new backup is successful")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			newRestoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, newBackupName)
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, appNamespaces)
			err = CreateRestoreWithValidation(ctx, newRestoreName, newBackupName, make(map[string]string), make(map[string]string), destinationClusterName, orgID, appContextsToBackup)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the restore [%s] is successful.", newRestoreName))
			restoreNames = append(restoreNames, newRestoreName)
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
		CleanupCloudSettingsAndClusters(backupLocationMap, "", "", ctx)
		log.InfoD("Switching context to destination cluster for clean up")
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "Unable to switch context to destination cluster [%s]", destinationClusterName)
		DestroyApps(scheduledAppContexts, opts)
		log.InfoD("Switching back context to Source cluster")
		err = SetSourceKubeConfig()
		log.FailOnError(err, "Unable to switch context to source cluster [%s]", SourceClusterName)
	})
})

// CloudSnapshotMissingValidationForNFSLocation this TC validates the Cloud Snapshot missing status for backups after deleting the Cloud Snapshots on the NFS location
var _ = Describe("{CloudSnapshotMissingValidationForNFSLocation}", func() {
	var (
		bkpLocationName            string
		backupLocationUID          string
		customResourceBackupName   string
		clusterUid                 string
		appNamespaces              []string
		appContexts                []*scheduler.Context
		scheduledAppContexts       []*scheduler.Context
		globalBucket               string
		backupNames                []string
		backupLocationMap          = make(map[string]string)
		periodicSchedulePolicyName string
		singleNSScheduleName       string
		singleNSScheduledBackup    string
		multipleNSScheduleName     string
		multipleNSScheduledBackup  string
		newScheduleName            string
		newScheduledBackup         string
		restoreNames               []string
		scheduleNames              []string
		periodicSchedulePolicyUid  string
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("CloudSnapshotMissingValidationForNFSLocation", "This TC validates the Cloud Snapshot missing status for backups after deleting the Cloud Snapshots on the NFS location", nil, 86094, Sabrarhussaini, Q4FY23)
		log.InfoD("Scheduling Applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
		log.Infof("The list of namespaces deployed are", appNamespaces)
	})

	It("To validate the backup status when the cloud snapshots are deleted", func() {
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Creating NFS backup location", func() {
			log.InfoD("Creating NFS backup location")
			globalBucket = getGlobalBucketName(drivers.ProviderNfs)
			bkpLocationName = fmt.Sprintf("%s-%s-%s", drivers.ProviderNfs, globalBucket, RandomString(10))
			backupLocationUID = uuid.New()
			backupLocationMap[backupLocationUID] = bkpLocationName
			err := CreateBackupLocation(drivers.ProviderNfs, bkpLocationName, backupLocationUID, "", "", globalBucket, orgID, "", true)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating NFS backup location %s", bkpLocationName))
		})

		Step("Creating Schedule Policy", func() {
			log.InfoD("Creating Schedule Policy")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			periodicSchedulePolicyName = fmt.Sprintf("%s-%v", "periodic", RandomString(4))
			periodicSchedulePolicyUid = uuid.New()
			periodicSchedulePolicyInterval := int64(15)
			err = CreateBackupScheduleIntervalPolicy(5, periodicSchedulePolicyInterval, 5, periodicSchedulePolicyName, periodicSchedulePolicyUid, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval [%v] minutes named [%s]", periodicSchedulePolicyInterval, periodicSchedulePolicyName))
		})

		Step("Registering application clusters for backup", func() {
			log.InfoD("Registering application clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.Infof("Creating source [%s] and destination [%s] clusters", SourceClusterName, destinationClusterName)
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with App-User ctx", SourceClusterName, destinationClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			log.FailOnError(err, "Fetching [%s] uid", SourceClusterName)
		})

		Step(fmt.Sprintf("Taking a scheduled backup of single namespaces"), func() {
			log.InfoD(fmt.Sprintf("Taking a scheduled backup of single namespaces [%v]", appNamespaces[0]))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			singleNSScheduleName = fmt.Sprintf("single-ns-bkp-schedule-%v", RandomString(4))
			singleNSScheduledBackup, err = CreateScheduleBackupWithValidation(ctx, singleNSScheduleName, SourceClusterName, bkpLocationName, backupLocationUID, scheduledAppContexts, make(map[string]string), orgID, "", "", "", "", periodicSchedulePolicyName, periodicSchedulePolicyUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of schedule backup with schedule name [%s]", singleNSScheduleName))
			err = suspendBackupSchedule(singleNSScheduleName, periodicSchedulePolicyName, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule [%s]", singleNSScheduleName))
			backupNames = append(backupNames, singleNSScheduledBackup)
			scheduleNames = append(scheduleNames, singleNSScheduleName)
		})

		Step(fmt.Sprintf("Taking a scheduled backup of multiple namespaces"), func() {
			log.InfoD(fmt.Sprintf("Taking a scheduled backup of multiple namespaces [%v]", appNamespaces))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			multipleNSScheduleName = fmt.Sprintf("multiple-ns-bkp-schedule-%v", RandomString(4))
			multipleNSScheduledBackup, err = CreateScheduleBackupWithValidation(ctx, multipleNSScheduleName, SourceClusterName, bkpLocationName, backupLocationUID, scheduledAppContexts, make(map[string]string), orgID, "", "", "", "", periodicSchedulePolicyName, periodicSchedulePolicyUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of schedule backup with schedule name [%s]", multipleNSScheduleName))
			err = suspendBackupSchedule(multipleNSScheduleName, periodicSchedulePolicyName, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule [%s]", multipleNSScheduleName))
			backupNames = append(backupNames, multipleNSScheduledBackup)
			scheduleNames = append(scheduleNames, multipleNSScheduleName)
		})

		Step("Taking a custom resource backup of all namespaces", func() {
			log.InfoD("Taking a custom resource backup of all namespaces")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			customResourceBackupName = fmt.Sprintf("%s-custom-resource-%v", BackupNamePrefix, RandomString(5))
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, appNamespaces)
			err = CreateBackupWithCustomResourceTypeWithValidation(ctx, customResourceBackupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, []string{"PersistentVolumeClaim"}, nil, orgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of custom resource backup - %s", customResourceBackupName))
			backupNames = append(backupNames, customResourceBackupName)
		})

		Step("Remove the Cloud Snapshots from the NFS backup location for all the backups", func() {
			log.InfoD("Remove the Cloud Snapshots from the NFS backup location for all the backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, orgID)
				log.FailOnError(err, fmt.Sprintf("Getting UID for backup %v", backupName))
				backupInspectRequest := &api.BackupInspectRequest{
					Name:  backupName,
					Uid:   backupUID,
					OrgId: orgID,
				}
				resp, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
				log.FailOnError(err, fmt.Sprintf("error inspecting backup %v", backupName))
				volumePath := resp.Backup.Volumes
				for _, ele := range volumePath {
					bkpID := ele.BackupId
					parts := strings.Split(bkpID, "/")
					subparts := strings.Split(parts[1], "-")
					currentSnapShotPath := parts[0] + "/" + subparts[0] + "/" + subparts[0] + "-" + subparts[1]
					log.InfoD("Deleting the Cloud Snapshots from the NFS backup location for backup [%s] from path [%s]", backupName, currentSnapShotPath)
					DeleteNfsSubPath(currentSnapShotPath)
				}
				//the below code is a work around and can be removed once PB-3788 is fixed.
				metadataPath := globalBucket + "/" + resp.Backup.BackupPath + "/" + "metadata.json"
				log.Infof("Deleting the cloud snapshots from the NFS backup location for backup [%v] with backup path [%s]", backupName, metadataPath)
				DeleteNfsSubPath(metadataPath)
			}
		})

		Step("Verify if the backups are in CloudBackupMissing state after the cloud snapshot deletion", func() {
			log.InfoD("Verify if the backups are in CloudBackupMissing state after the cloud snapshot deletion")
			var wg sync.WaitGroup
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					bkpUid, err := Inst().Backup.GetBackupUID(ctx, backupName, orgID)
					log.FailOnError(err, "Fetching backup uid for backup %s", backupName)
					backupInspectRequest := &api.BackupInspectRequest{
						Name:  backupName,
						Uid:   bkpUid,
						OrgId: orgID,
					}
					requiredStatus := api.BackupInfo_StatusInfo_CloudBackupMissing
					backupCloudBackupMissingCheckFunc := func() (interface{}, bool, error) {
						resp, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
						if err != nil {
							return "", false, err
						}
						actual := resp.GetBackup().GetStatus().Status
						if actual == requiredStatus {
							return "", false, nil
						}
						return "", true, fmt.Errorf("backup status for [%s] expected was [%v] but got [%s]", backupName, requiredStatus, actual)
					}
					_, err = DoRetryWithTimeoutWithGinkgoRecover(backupCloudBackupMissingCheckFunc, 20*time.Minute, 30*time.Second)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verfiying backup %s is in CloudBackup missing state", backupName))
				}(backupName)
			}
			wg.Wait()
		})

		Step("Verify if the restores for the above backups get failed", func() {
			log.InfoD("Verify if the restores for the above backups get failed")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, backupName)
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, appNamespaces)
				err = CreateRestoreWithValidation(ctx, restoreName, backupName, make(map[string]string), make(map[string]string), destinationClusterName, orgID, appContextsToBackup)
				dash.VerifyFatal(strings.Contains(err.Error(), "CloudBackup objects are missing"), true, fmt.Sprintf("Verifying if the restore [%s] is getting Failed after JSON file deletion.", restoreName))
				restoreNames = append(restoreNames, restoreName)
			}
		})

		Step(fmt.Sprintf("Taking a new scheduled backup of multiple namespaces"), func() {
			log.InfoD(fmt.Sprintf("Taking a new scheduled backup of multiple namespaces [%v]", appNamespaces))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			newScheduleName = fmt.Sprintf("new-bkp-schedule-%v", RandomString(4))
			newScheduledBackup, err = CreateScheduleBackupWithValidation(ctx, newScheduleName, SourceClusterName, bkpLocationName, backupLocationUID, scheduledAppContexts, make(map[string]string), orgID, "", "", "", "", periodicSchedulePolicyName, periodicSchedulePolicyUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of schedule backup with schedule name [%s]", multipleNSScheduleName))
			err = suspendBackupSchedule(newScheduleName, periodicSchedulePolicyName, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule [%s]", newScheduleName))
			backupNames = append(backupNames, newScheduledBackup)
			scheduleNames = append(scheduleNames, newScheduleName)
		})

		Step("Verify if the restores for the new scheduled backup is successful", func() {
			log.InfoD("Verify if the restores for the new scheduled backup is successful")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			newRestoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, newScheduledBackup)
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, appNamespaces)
			err = CreateRestoreWithValidation(ctx, newRestoreName, newScheduledBackup, make(map[string]string), make(map[string]string), destinationClusterName, orgID, appContextsToBackup)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the restore [%s] is successful.", newRestoreName))
			restoreNames = append(restoreNames, newRestoreName)
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		err := SetSourceKubeConfig()
		log.FailOnError(err, "Switching context to source cluster failed")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.Infof("Cleaning up schedules")
		var wg sync.WaitGroup
		for _, schedule := range scheduleNames {
			wg.Add(1)
			go func(schedule string) {
				defer GinkgoRecover()
				defer wg.Done()
				err = DeleteSchedule(schedule, SourceClusterName, orgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting Backup Schedule [%s]", schedule))
			}(schedule)
		}
		wg.Wait()
		log.Infof("Cleaning up restores")
		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore %s", restoreName))
		}
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.Infof("Deleting the deployed applications")
		DestroyApps(scheduledAppContexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, "", "", ctx)
	})
})
