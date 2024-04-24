package tests

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// This testcase verifies backup and restore with non-existing and deleted custom stork admin namespaces
var _ = Describe("{BackupAndRestoreWithNonExistingAdminNamespaceAndUpdatedResumeSuspendBackupPolicies}", Label(TestCaseLabelsMap[BackupAndRestoreWithNonExistingAdminNamespaceAndUpdatedResumeSuspendBackupPolicies]...), func() {

	var (
		newAdminNamespace                     string // New admin namespace to be set as custom admin namespace
		backupName                            string
		scheduledAppContexts                  []*scheduler.Context
		bkpNamespaces                         []string
		clusterUid                            string
		clusterStatus                         api.ClusterInfo_StatusInfo_Status
		restoreName                           string
		cloudCredName                         string
		cloudCredUID                          string
		backupLocationUID                     string
		bkpLocationName                       string
		numDeployments                        int
		providers                             []string
		backupLocationMap                     map[string]string
		labelSelectors                        map[string]string
		selectedBkpNamespaceMapping           map[string]string
		multipleRestoreMapping                map[string]string
		restoreNames                          []string
		periodicSchedulePolicyName            string
		schPolicyUid                          string
		scheduleName                          string
		scheduleNames                         []string
		periodicSchedulePolicyNameAfterUpdate string
		scheduleBackupNameBeforeUpdate        string
		scheduleBackupNameAfterUpdate         string
		scheduleAndBackup                     map[string]string
		controlChannel                        chan string
		errorGroup                            *errgroup.Group
	)
	JustBeforeEach(func() {
		newAdminNamespace = StorkNamePrefix
		backupName = fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
		bkpNamespaces = make([]string, 0)
		restoreName = fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
		backupLocationMap = make(map[string]string)
		labelSelectors = make(map[string]string)
		scheduleAndBackup = make(map[string]string)

		numDeployments = 5 // 5 apps deployed in 5 namespaces
		providers = GetBackupProviders()

		StartPxBackupTorpedoTest("BackupAndRestoreWithNonExistingAdminNamespaceAndUpdatedResumeSuspendBackupPolicies", "Test to verify stork-namespace, backup CRs, restore CRs in case of non existing admin namespace and suspended, resumed backup schedules", nil, 93700, ATrivedi, Q4FY24)
		scheduledAppContexts = make([]*scheduler.Context, 0)
		log.InfoD("Starting to deploy applications")
		for i := 0; i < numDeployments; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = AppReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})
	It("Test to verify stork-namespace, backup CRs, restore CRs in case of non existing admin namespace and suspended, resumed backup schedules", func() {

		Step("Validating deployed applications", func() {
			log.InfoD("Validating deployed applications")
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})
		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("%s-%s-bl", provider, getGlobalBucketName(provider))
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, BackupOrgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", bkpLocationName))
			}
		})
		Step("Registering cluster for backup", func() {
			log.InfoD("Registering cluster for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})
		Step("Create schedule policy before admin namespace update", func() {
			log.InfoD("Creating a schedule policy")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			periodicSchedulePolicyName = fmt.Sprintf("%s-%v", "periodic", time.Now().Unix())
			periodicSchedulePolicyUid := uuid.New()
			periodicSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, 15, 5)
			err = Inst().Backup.BackupSchedulePolicy(periodicSchedulePolicyName, periodicSchedulePolicyUid, BackupOrgID, periodicSchedulePolicyInfo)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval 15 minutes named [%s]", periodicSchedulePolicyName))
			periodicSchedulePolicyUid, err = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicSchedulePolicyName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching uid of periodic schedule policy named [%s]", periodicSchedulePolicyName))
		})
		Step("Creating schedule backups for applications before admin namespace update", func() {
			log.InfoD("Creating schedule backups before admin namespace update")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			schPolicyUid, _ = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicSchedulePolicyName)
			scheduleNameBeforeUpdate := fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, time.Now().Unix())
			scheduleBackupNameBeforeUpdate, err = CreateScheduleBackupWithCRValidation(ctx, scheduleNameBeforeUpdate, SourceClusterName, bkpLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, BackupOrgID, "", "", "", "", periodicSchedulePolicyName, schPolicyUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of schedule backup with schedule name [%s]", scheduleNameBeforeUpdate))
			scheduleNames = append(scheduleNames, scheduleNameBeforeUpdate)
			scheduleAndBackup[scheduleNameBeforeUpdate] = periodicSchedulePolicyName
		})
		Step("Creating new admin namespaces", func() {
			log.InfoD("Creating new admin namespace - %v", newAdminNamespace)
			nsSpec := &v1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: newAdminNamespace,
				},
			}
			ns, err := core.Instance().CreateNamespace(nsSpec)
			log.FailOnError(err, fmt.Sprintf("Unable to create namespace [%s]", newAdminNamespace))
			log.InfoD("Created Namespace - %v", ns.Name)
		})
		Step("Modifying Admin Namespace for Stork", func() {
			log.InfoD("Modifying Admin Namespace for Stork to %v", newAdminNamespace)
			_, err := ChangeStorkAdminNamespace(newAdminNamespace)
			log.FailOnError(err, "Unable to update admin namespace")
			log.Infof("Admin namespace updated successfully")
		})
		Step("Taking backup of multiple namespaces", func() {
			log.InfoD(fmt.Sprintf("Taking backup of multiple namespaces [%v]", bkpNamespaces))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			err = CreateBackupWithCRValidation(backupName, SourceClusterName, bkpLocationName, backupLocationUID, bkpNamespaces, labelSelectors, BackupOrgID, clusterUid, "", "", "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
		})
		Step("Create schedule policy after stork admin namespace update", func() {
			log.InfoD("Creating a schedule policy after admin namespace update")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			periodicSchedulePolicyNameAfterUpdate = fmt.Sprintf("%s-%v", "periodic", time.Now().Unix())
			periodicSchedulePolicyUidAfterUpdate := uuid.New()
			periodicSchedulePolicyInfoAfterUpdate := Inst().Backup.CreateIntervalSchedulePolicy(5, 15, 5)
			err = Inst().Backup.BackupSchedulePolicy(periodicSchedulePolicyNameAfterUpdate, periodicSchedulePolicyUidAfterUpdate, BackupOrgID, periodicSchedulePolicyInfoAfterUpdate)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval 15 minutes named [%s]", periodicSchedulePolicyNameAfterUpdate))
			periodicSchedulePolicyUidAfterUpdate, err = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicSchedulePolicyNameAfterUpdate)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching uid of periodic schedule policy named [%s]", periodicSchedulePolicyNameAfterUpdate))
		})
		Step("Creating schedule backups for applications after admin namespace update", func() {
			log.InfoD("Creating schedule backups after admin namespace update")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			schPolicyUid, _ = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicSchedulePolicyNameAfterUpdate)
			scheduleNameAfterUpdate := fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, time.Now().Unix())
			scheduleBackupNameAfterUpdate, err = CreateScheduleBackupWithCRValidation(ctx, scheduleNameAfterUpdate, SourceClusterName, bkpLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, BackupOrgID, "", "", "", "", periodicSchedulePolicyNameAfterUpdate, schPolicyUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of schedule backup with schedule name [%s]", scheduleNameAfterUpdate))
			scheduleNames = append(scheduleNames, scheduleNameAfterUpdate)
			scheduleAndBackup[scheduleNameAfterUpdate] = periodicSchedulePolicyNameAfterUpdate
		})
		Step("Restoring backup from before admin namespace update", func() {
			log.InfoD("Restoring backup of multiple namespaces")
			restoreName = fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			selectedBkpNamespaceMapping = make(map[string]string)
			multipleRestoreMapping = make(map[string]string)
			for _, namespace := range bkpNamespaces {
				selectedBkpNamespaceMapping[namespace] = namespace
			}
			log.InfoD("Selected application namespaces to restore: [%v]", bkpNamespaces)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateRestoreWithCRValidation(restoreName, scheduleBackupNameBeforeUpdate, selectedBkpNamespaceMapping, SourceClusterName, BackupOrgID, ctx, make(map[string]string))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))

			// Restore to custom namespace
			for _, namespace := range bkpNamespaces {
				restoredNameSpace := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
				multipleRestoreMapping[namespace] = restoredNameSpace
			}
			log.Infof("Custom restore map %v", multipleRestoreMapping)
			customRestoreName := fmt.Sprintf("%s-%v", "before-update", RandomString(7))
			err = CreateRestoreWithCRValidation(customRestoreName, scheduleBackupNameBeforeUpdate, multipleRestoreMapping, SourceClusterName, BackupOrgID, ctx, make(map[string]string))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying multiple backup restore [%s]", customRestoreName))
			restoreNames = append(restoreNames, restoreName, customRestoreName)
		})
		Step("Restoring backup of from after admin namespace update", func() {
			log.InfoD("Restoring backup of multiple namespaces")
			selectedBkpNamespaceMapping = make(map[string]string)
			multipleRestoreMapping = make(map[string]string)
			restoreName = fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			for _, namespace := range bkpNamespaces {
				selectedBkpNamespaceMapping[namespace] = namespace
			}
			log.InfoD("Selected application namespaces to restore: [%v]", bkpNamespaces)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateRestoreWithCRValidation(restoreName, scheduleBackupNameAfterUpdate, selectedBkpNamespaceMapping, SourceClusterName, BackupOrgID, ctx, make(map[string]string))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))

			// Restore to custom namespace
			for _, namespace := range bkpNamespaces {
				restoredNameSpace := fmt.Sprintf("%s-%s", RestoreNamePrefix, RandomString(7))
				multipleRestoreMapping[namespace] = restoredNameSpace
			}
			log.Infof("Custom restore map %v", multipleRestoreMapping)
			customRestoreName := fmt.Sprintf("%s-%v", "after-update", time.Now().Unix())
			err = CreateRestoreWithCRValidation(customRestoreName, scheduleBackupNameAfterUpdate, multipleRestoreMapping, SourceClusterName, BackupOrgID, ctx, make(map[string]string))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying multiple backup restore [%s]", customRestoreName))
			restoreNames = append(restoreNames, restoreName, customRestoreName)
		})
		Step("Suspending the existing backup schedules", func() {
			log.InfoD("Suspending the existing backup schedules")
			log.InfoD("All backup schedules and policies - [%v]", scheduleAndBackup)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for backup, policy := range scheduleAndBackup {
				err = SuspendBackupSchedule(backup, policy, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule [%s] for user [%s]", backup, policy))
			}
		})
		Step("Resume the existing backup schedules", func() {
			log.InfoD("Resume the existing backup schedules")
			ctx, err := backup.GetAdminCtxFromSecret()
			for backupSchedule, policy := range scheduleAndBackup {
				err = ResumeBackupSchedule(backupSchedule, policy, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of resuming backup schedule - [%s]", backupSchedule))
			}
			log.Infof("Waiting 5 minute for another schedule backup to trigger")
			time.Sleep(5 * time.Minute)
		})
		Step("Trying restore and custom restore with resumed backup scheduled", func() {
			log.InfoD("Get the latest schedule backup and verify the backup status")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for backupSchedule := range scheduleAndBackup {
				latestScheduleBkpName, err := GetLatestScheduleBackupName(ctx, backupSchedule, BackupOrgID)
				log.FailOnError(err, "Error while getting latest schedule backup name")
				err = BackupSuccessCheckWithValidation(ctx, latestScheduleBkpName, scheduledAppContexts, BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success and Validation of latest schedule backup [%s]", latestScheduleBkpName))

				log.InfoD("Restoring backup of multiple namespaces")
				restoreName = fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
				log.InfoD("Selected application namespaces to restore: [%v]", bkpNamespaces)
				err = CreateRestoreWithCRValidation(restoreName, latestScheduleBkpName, selectedBkpNamespaceMapping, SourceClusterName, BackupOrgID, ctx, make(map[string]string))
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))

				// Restore to custom namespace
				for _, namespace := range bkpNamespaces {
					restoredNameSpace := fmt.Sprintf("%s-%s", RestoreNamePrefix, RandomString(7))
					multipleRestoreMapping[namespace] = restoredNameSpace
				}
				log.Infof("Custom restore map %v", multipleRestoreMapping)
				customRestoreName := fmt.Sprintf("%s-%v", "after-update", time.Now().Unix())
				err = CreateRestoreWithCRValidation(customRestoreName, latestScheduleBkpName, multipleRestoreMapping, SourceClusterName, BackupOrgID, ctx, make(map[string]string))
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying multiple backup restore [%s]", customRestoreName))
				restoreNames = append(restoreNames, restoreName, customRestoreName)
			}
		})
		Step("Deleting new admin namespace", func() {
			log.Info("Deleting namespace - %v", newAdminNamespace)
			err := DeleteAppNamespace(newAdminNamespace)
			log.FailOnError(err, "Unable to delete admin namespace")
			log.InfoD("Namespace - %v - deleted successfully", newAdminNamespace)
		})
		Step("Restoring backup of multiple namespaces after admin namespace removal", func() {
			log.InfoD("Restoring backup of multiple namespaces")
			selectedBkpNamespaceMapping = make(map[string]string)
			multipleRestoreMapping = make(map[string]string)
			restoreName = fmt.Sprintf("%s-%s", RestoreNamePrefix, RandomString(7))
			for _, namespace := range bkpNamespaces {
				selectedBkpNamespaceMapping[namespace] = namespace
			}
			log.InfoD("Selected application namespaces to restore: [%v]", bkpNamespaces)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateRestoreWithCRValidation(restoreName, backupName, selectedBkpNamespaceMapping, SourceClusterName, BackupOrgID, ctx, make(map[string]string))
			dash.VerifyFatal(strings.Contains(err.Error(), fmt.Sprintf("Restore create: failed to get target namespace: namespaces \"%s\" not found", newAdminNamespace)), true, fmt.Sprintf("Verifying restore creation after deleting new admin namespace [%s]. Error : %s", newAdminNamespace, err.Error()))

			// Restore to custom namespace
			for _, namespace := range bkpNamespaces {
				restoredNameSpace := fmt.Sprintf("%s-%s", backupName, RandomString(7))
				multipleRestoreMapping[namespace] = restoredNameSpace
			}
			log.Infof("Custom restore map %v", multipleRestoreMapping)
			customRestoreName := fmt.Sprintf("%s-%v", "multiple-application", time.Now().Unix())
			err = CreateRestoreWithCRValidation(customRestoreName, backupName, multipleRestoreMapping, SourceClusterName, BackupOrgID, ctx, make(map[string]string))
			dash.VerifyFatal(strings.Contains(err.Error(), fmt.Sprintf("Restore create: failed to get target namespace: namespaces \"%s\" not found", newAdminNamespace)), true, fmt.Sprintf("Verifying restore creation after deleting new admin namespace [%s]. Error : %s", newAdminNamespace, err.Error()))
		})
		Step("Taking backup of multiple namespaces after admin namespace removal", func() { // This step should fail
			backupName = fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
			restoreName = fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			log.InfoD(fmt.Sprintf("Taking backup of multiple namespaces [%v]", bkpNamespaces))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateBackupWithCRValidation(backupName, SourceClusterName, bkpLocationName, backupLocationUID, bkpNamespaces, labelSelectors, BackupOrgID, clusterUid, "", "", "", "", ctx)
			dash.VerifyFatal(strings.Contains(err.Error(), fmt.Sprintf("failed to get target namespace: namespaces \"%s\" not found", newAdminNamespace)), true, fmt.Sprintf("Verifying backup creation after deleting new admin namespace [%s]. Error : %s", newAdminNamespace, err.Error()))
		})
		Step("Creating schedule backups for applications after admin namespace removal", func() {
			log.InfoD("Creating schedule backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			schPolicyUid, _ = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicSchedulePolicyNameAfterUpdate)
			scheduleName = fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, time.Now().Unix())
			_, err = CreateScheduleBackupWithCRValidation(ctx, scheduleName, SourceClusterName, bkpLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, BackupOrgID, "", "", "", "", periodicSchedulePolicyNameAfterUpdate, schPolicyUid)
			dash.VerifyFatal(strings.Contains(err.Error(), fmt.Sprintf("failed to get target namespace: namespaces \"%s\" not found", newAdminNamespace)), true, fmt.Sprintf("Verifying backup creation after deleting new admin namespace [%s]. Error : %s", newAdminNamespace, err.Error()))
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Creating new admin namespace again for other tests - %v", newAdminNamespace)
		nsSpec := &v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: newAdminNamespace,
			},
		}
		_, err = core.Instance().GetNamespace(newAdminNamespace)
		if err != nil {
			ns, err := core.Instance().CreateNamespace(nsSpec)
			log.FailOnError(err, fmt.Sprintf("Unable to create namespace [%s]", newAdminNamespace))
			log.InfoD("Created Namespace - %v", ns.Name)
		}
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.Infof("Deleting backup schedule policy")
		for _, scheduleName := range scheduleNames {
			err = DeleteSchedule(scheduleName, SourceClusterName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		}
		log.InfoD("Deleting deployed applications")
		err = DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")
		log.InfoD("Deleting backups")
		err = DeleteAllBackups(ctx, BackupOrgID)
		log.FailOnError(err, "Unable to delete all backups")
		log.InfoD("Deleting restore")
		log.InfoD(fmt.Sprintf("Restore names %v", restoreNames))
		for _, restores := range restoreNames {
			err := DeleteRestore(restores, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the deletion of the restore named [%s]", restores))
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})

})
