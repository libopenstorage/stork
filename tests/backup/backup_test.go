package tests

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"

	. "github.com/portworx/torpedo/tests"
)

// This testcase verifies if the backup pods are in Ready state or not
var _ = Describe("{BackupClusterVerification}", func() {
	JustBeforeEach(func() {
		log.Infof("No pre-setup required for this testcase")
		StartTorpedoTest("Backup: BackupClusterVerification", "Validating backup cluster pods", nil, 0)
	})
	It("Backup Cluster Verification", func() {
		Step("Check the status of backup pods", func() {
			log.InfoD("Check the status of backup pods")
			err := Inst().Backup.ValidateBackupCluster()
			dash.VerifyFatal(err, nil, "Backup Cluster Verification successful")
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(make([]*scheduler.Context, 0))
		log.Infof("No cleanup required for this testcase")
	})
})

// This is a sample test case to verify User/Group Management and role mapping
var _ = Describe("{UserGroupManagement}", func() {
	JustBeforeEach(func() {
		log.Infof("No pre-setup required for this testcase")
		StartTorpedoTest("Backup: UserGroupManagement", "Creating users and adding them to groups", nil, 0)
	})
	It("User and group role mappings", func() {
		Step("Create Users", func() {
			err := backup.AddUser("testuser1", "test", "user1", "testuser1@localhost.com", commonPassword)
			log.FailOnError(err, "Failed to create user")
		})
		Step("Create Groups", func() {
			err := backup.AddGroup("testgroup1")
			log.FailOnError(err, "Failed to create group")
		})
		Step("Add users to group", func() {
			err := backup.AddGroupToUser("testuser1", "testgroup1")
			log.FailOnError(err, "Failed to assign group to user")
		})
		Step("Assign role to groups", func() {
			err := backup.AddRoleToGroup("testgroup1", backup.ApplicationOwner, "testing from torpedo")
			log.FailOnError(err, "Failed to assign group to user")
		})
		Step("Verify Application Owner role permissions for user", func() {
			isUserRoleMapped, err := ValidateUserRole("testuser1", backup.ApplicationOwner)
			log.FailOnError(err, "User does not contain the expected role")
			dash.VerifyFatal(isUserRoleMapped, true, "Verifying the user role mapping")
		})
		Step("Update role to groups", func() {
			err := backup.DeleteRoleFromGroup("testgroup1", backup.ApplicationOwner, "removing role from testgroup1")
			log.FailOnError(err, "Failed to delete role from group")
			err = backup.AddRoleToGroup("testgroup1", backup.ApplicationUser, "testing from torpedo")
			log.FailOnError(err, "Failed to add role to group")
		})
		Step("Verify Application User role permissions for user", func() {
			isUserRoleMapped, err := ValidateUserRole("testuser1", backup.ApplicationUser)
			log.FailOnError(err, "User does not contain the expected role")
			dash.VerifyFatal(isUserRoleMapped, true, "Verifying the user role mapping")
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(make([]*scheduler.Context, 0))
		log.Infof("Cleanup started")
		err := backup.DeleteUser("testuser1")
		dash.VerifySafely(err, nil, "Delete user testuser1")
		err = backup.DeleteGroup("testgroup1")
		dash.VerifySafely(err, nil, "Delete group testgroup1")
		log.Infof("Cleanup done")
	})
})

// This testcase verifies basic backup rule,backup location, cloud setting
var _ = Describe("{BasicBackupCreation}", func() {
	var (
		clusterStatus                  api.ClusterInfo_StatusInfo_Status
		backupNames                    []string                // backups in px-backup
		restoreNames                   []string                // restores in px-backup
		sourceClusterAppsContexts      []*scheduler.Context    // Each Context is for one Namespace which corresponds to one App
		destinationClusterAppsContexts []*scheduler.Context    // Each Context is for one Namespace which corresponds to one App
		backupContexts                 []*BackupRestoreContext // Each Context is for one backup in px-backup
		restoreContexts                []*BackupRestoreContext // Each Context is for one restore in px-backup
		preRuleNameList                []string
		postRuleNameList               []string
		clusterUid                     string
		cloudCredName                  string
		cloudCredUID                   string
		backupLocationUID              string
		backupLocationName             string
	)

	var (
		appList           = Inst().AppList
		sourceNamespaces  = make([]string, 0)
		backupLocationMap = make(map[string]string)
		labelSelectors    = make(map[string]string)
		providers         = getProviders()
		intervalName      = fmt.Sprintf("%s-%v", "interval", time.Now().Unix())
		dailyName         = fmt.Sprintf("%s-%v", "daily", time.Now().Unix())
		weeklyName        = fmt.Sprintf("%s-%v", "weekly", time.Now().Unix())
		monthlyName       = fmt.Sprintf("%s-%v", "monthly", time.Now().Unix())
	)

	JustBeforeEach(func() {
		StartTorpedoTest("Backup: BasicBackupCreation", "Deploying backup", nil, 0)

		log.InfoD("Deploy applications")

		log.InfoD("switching to source context")
		err := SetSourceKubeConfig()
		log.FailOnError(err, "failed to switch to context to source cluster")

		log.InfoD("scheduling applications")
		sourceClusterAppsContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, appCtx := range appContexts {
				appCtx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(appCtx, taskName)
				// (sourceNamespaces, sourceClusterAppsContexts) will always correspoond
				// TODO: combine them somehow
				sourceNamespaces = append(sourceNamespaces, namespace)
				sourceClusterAppsContexts = append(sourceClusterAppsContexts, appCtx)
			}
		}
	})

	It("Basic Backup Creation", func() {
		Step("Validating applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(sourceClusterAppsContexts)

			log.InfoD("switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "failed to SetClusterContext to default cluster")
		})
		Step("Creating rules for backup", func() {
			log.InfoD("Creating rules for backup")
			log.InfoD("Creating pre rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				preRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "pre")
				log.FailOnError(err, "Creating pre rule for deployed app [%s] failed", appList[i])
				dash.VerifyFatal(preRuleStatus, true, "Verifying pre rule for backup")
				if ruleName != "" {
					preRuleNameList = append(preRuleNameList, ruleName)
				}
			}
			log.InfoD("Creating post rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				postRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "post")
				log.FailOnError(err, "Creating post rule for deployed app [%s] failed", appList[i])
				dash.VerifyFatal(postRuleStatus, true, "Verifying Post rule for backup")
				if ruleName != "" {
					postRuleNameList = append(postRuleNameList, ruleName)
				}
			}
		})
		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				backupLocationName = fmt.Sprintf("%s-%s-bl", provider, getGlobalBucketName(provider))
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, "Creating backup location")
			}
		})
		Step("Creating backup schedule policies", func() {
			log.InfoD("Creating backup schedule policies")
			log.InfoD("Creating backup interval schedule policy")
			intervalSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, 15, 2)
			intervalPolicyStatus := Inst().Backup.BackupSchedulePolicy(intervalName, uuid.New(), orgID, intervalSchedulePolicyInfo)
			dash.VerifyFatal(intervalPolicyStatus, nil, "Creating interval schedule policy")

			log.InfoD("Creating backup daily schedule policy")
			dailySchedulePolicyInfo := Inst().Backup.CreateDailySchedulePolicy(1, "9:00AM", 2)
			dailyPolicyStatus := Inst().Backup.BackupSchedulePolicy(dailyName, uuid.New(), orgID, dailySchedulePolicyInfo)
			dash.VerifyFatal(dailyPolicyStatus, nil, "Creating daily schedule policy")

			log.InfoD("Creating backup weekly schedule policy")
			weeklySchedulePolicyInfo := Inst().Backup.CreateWeeklySchedulePolicy(1, backup.Friday, "9:10AM", 2)
			weeklyPolicyStatus := Inst().Backup.BackupSchedulePolicy(weeklyName, uuid.New(), orgID, weeklySchedulePolicyInfo)
			dash.VerifyFatal(weeklyPolicyStatus, nil, "Creating weekly schedule policy")

			log.InfoD("Creating backup monthly schedule policy")
			monthlySchedulePolicyInfo := Inst().Backup.CreateMonthlySchedulePolicy(1, 29, "9:20AM", 2)
			monthlyPolicyStatus := Inst().Backup.BackupSchedulePolicy(monthlyName, uuid.New(), orgID, monthlySchedulePolicyInfo)
			dash.VerifyFatal(monthlyPolicyStatus, nil, "Creating monthly schedule policy")
		})
		Step("Registering cluster for backup", func() {
			log.InfoD("Registering cluster for backup")
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

		Step("Taking backup of application from source cluster", func() {
			log.InfoD("taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			backupNames = make([]string, 0)
			backupContexts = make([]*BackupRestoreContext, 0)
			for i, namespace := range sourceNamespaces {
				backupName := fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				log.InfoD("creating backup [%s] in source cluster [%s] (%s), organization [%s], of namespace [%s], in backup location [%s]", backupName, SourceClusterName, clusterUid, orgID, namespace, backupLocationName)
				err := CreateBackup(backupName, SourceClusterName, backupLocationName, backupLocationUID, []string{namespace}, labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
				dash.VerifyFatal(err, nil, "Verifying backup creation")
				backupNames = append(backupNames, backupName)

				log.InfoD("Validating Backup Creation")
				backupCtx, err := ValidateBackup(backupName, orgID, ctx, []*scheduler.Context{sourceClusterAppsContexts[i]})
				dash.VerifyFatal(err, nil, "Validating backup creation")
				backupContexts = append(backupContexts, backupCtx)
			}
		})

		Step("Restoring the backed up namespaces", func() {
			log.InfoD("Restoring the backed up namespaces")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for i, sourceNamespace := range sourceNamespaces {
				restoreName := fmt.Sprintf("%s-%s-%s", "test-restore", sourceNamespace, RandomString(4))
				for strings.Contains(strings.Join(restoreNames, ","), restoreName) {
					restoreName = fmt.Sprintf("%s-%s-%s", "test-restore", sourceNamespace, RandomString(4))
				}
				log.InfoD("Restoring [%s] namespace from the [%s] backup", sourceNamespace, backupNames[i])
				err = CreateRestore(restoreName, backupNames[i], make(map[string]string), destinationClusterName, orgID, ctx, make(map[string]string))
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
				restoreNames = append(restoreNames, restoreName)

				destinationClusterConfigPath, err := GetDestinationClusterConfigPath()
				log.FailOnError(err, "failed to get kubeconfig path for destination cluster. Error: [%v]", err)

				restoreCtx, err := ValidateRestore(restoreName, destinationClusterConfigPath, orgID, ctx, backupContexts[i], make(map[string]string), make(map[string]string))
				dash.VerifyFatal(err, nil, fmt.Sprintf("validation of restore [%s] is success", restoreName))
				restoreContexts = append(restoreContexts, restoreCtx)
			}
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(sourceClusterAppsContexts)

		log.InfoD("switching to default context")
		err := SetClusterContext("")
		log.FailOnError(err, "failed to SetClusterContext to default cluster")

		policyList := []string{intervalName, dailyName, weeklyName, monthlyName}
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		if len(preRuleNameList) > 0 {
			for _, ruleName := range preRuleNameList {
				err := Inst().Backup.DeleteRuleForBackup(orgID, ruleName)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup pre rules [%s]", ruleName))
			}
		}
		if len(postRuleNameList) > 0 {
			for _, ruleName := range postRuleNameList {
				err := Inst().Backup.DeleteRuleForBackup(orgID, ruleName)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup post rules [%s]", ruleName))
			}
		}
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, policyList)
		dash.VerifySafely(err, nil, "Deleting backup schedule policies")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true

		log.InfoD("switching to source context")
		err = SetSourceKubeConfig()
		log.FailOnError(err, "failed to switch to context to source cluster")

		log.Info("Deleting deployed namespaces")
		ValidateAndDestroy(sourceClusterAppsContexts, opts)

		log.InfoD("switching to destination context")
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "failed to switch to context to destination cluster")

		destinationClusterAppsContexts = make([]*scheduler.Context, 0)
		// only adding restoreContexts, not restoreContexts
		for _, restoreCtx := range restoreContexts {
			destinationClusterAppsContexts = append(destinationClusterAppsContexts, restoreCtx.schedulerCtxs...)
		}
		log.InfoD("deleting deployed applications (initial restore) on destination clusters")
		ValidateAndDestroy(destinationClusterAppsContexts, opts)

		log.InfoD("switching to default context")
		err = SetClusterContext("")
		log.FailOnError(err, "failed to SetClusterContext to default cluster")

		backupDriver := Inst().Backup
		log.Info("Deleting backed up namespaces")
		for _, backupName := range backupNames {
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
			backupDeleteResponse, err := DeleteBackup(backupName, backupUID, orgID, ctx)
			log.FailOnError(err, "Backup [%s] could not be deleted", backupName)
			dash.VerifyFatal(backupDeleteResponse.String(), "", fmt.Sprintf("Verifying [%s] backup deletion is successful", backupName))
		}
		log.Info("Deleting restored namespaces")
		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore [%s]", restoreName))
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})
