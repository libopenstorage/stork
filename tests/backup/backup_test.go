package tests

import (
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/backup/portworx"
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
			err := backup.AddUser("testuser1", "test", "user1", "testuser1@localhost.com", "Password1")
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
		appList           = Inst().AppList
		backupName        string
		contexts          []*scheduler.Context
		preRuleNameList   []string
		postRuleNameList  []string
		appContexts       []*scheduler.Context
		bkpNamespaces     []string
		clusterUid        string
		clusterStatus     api.ClusterInfo_StatusInfo_Status
		restoreName       string
		cloudCredName     string
		cloudCredUID      string
		backupLocationUID string
		bkpLocationName   string
		backupLocationMap map[string]string
		labelSelectors    map[string]string
		namespaceMapping  map[string]string
		providers         []string
		intervalName      string
		dailyName         string
		weeklyName        string
		monthlyName       string
		backupNames       []string
		restoreNames      []string
	)

	JustBeforeEach(func() {
		backupLocationMap = make(map[string]string)
		labelSelectors = make(map[string]string)
		bkpNamespaces = make([]string, 0)
		namespaceMapping = make(map[string]string)
		providers = getProviders()
		intervalName = fmt.Sprintf("%s-%v", "interval", time.Now().Unix())
		dailyName = fmt.Sprintf("%s-%v", "daily", time.Now().Unix())
		weeklyName = fmt.Sprintf("%s-%v", "weekly", time.Now().Unix())
		monthlyName = fmt.Sprintf("%s-%v", "monthly", time.Now().Unix())
		StartTorpedoTest("Backup: BasicBackupCreation", "Deploying backup", nil, 0)
		log.InfoD("Verifying if the pre/post rules for the required apps are present in the AppParameters or not")
		for i := 0; i < len(appList); i++ {
			if Contains(preRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["pre"]; ok {
					dash.VerifyFatal(ok, true, fmt.Sprintf("Pre Rule details mentioned for the app [%s]", appList[i]))
				}
			}
			if Contains(postRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["post"]; ok {
					dash.VerifyFatal(ok, true, fmt.Sprintf("Post Rule details mentioned for the app [%s]", appList[i]))
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
	It("Basic Backup Creation", func() {
		Step("Validating applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(contexts)
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
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("%s-%s-bl", provider, getGlobalBucketName(provider))
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID)
				err := CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
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
			clusterStatus, clusterUid = Inst().Backup.RegisterBackupCluster(orgID, SourceClusterName, "")
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying backup cluster with uid [%s]", clusterUid))
		})
		Step("Taking backup of all namespaces", func() {
			log.InfoD("Taking backup of all namespaces")
			ctx, err := backup.GetAdminCtxFromSecret()
			dash.VerifyFatal(err, nil, "Getting context")
			for _, namespace := range bkpNamespaces {
				backupName = fmt.Sprintf("%s-%s-%s", BackupNamePrefix, namespace, RandomString(4))
				for strings.Contains(strings.Join(backupNames, ","), backupName) {
					backupName = fmt.Sprintf("%s-%s-%s", BackupNamePrefix, namespace, RandomString(4))
				}
				backupNames = append(backupNames, backupName)
				err = CreateBackup(backupName, SourceClusterName, bkpLocationName, backupLocationUID, []string{namespace},
					labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying [%s] backup creation", backupName))
			}
		})
		Step("Restoring the backed up namespaces", func() {
			log.InfoD("Restoring the backed up namespaces")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for index, namespace := range bkpNamespaces {
				restoreName = fmt.Sprintf("%s-%s-%s", "test-restore", namespace, RandomString(4))
				for strings.Contains(strings.Join(restoreNames, ","), restoreName) {
					restoreName = fmt.Sprintf("%s-%s-%s", "test-restore", namespace, RandomString(4))
				}
				restoreNames = append(restoreNames, restoreName)
				log.InfoD("Restoring [%s] namespace from the [%s] backup", namespace, backupNames[index])
				err = CreateRestore(restoreName, backupNames[index], namespaceMapping, destinationClusterName, orgID, ctx, make(map[string]string))
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
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
		log.Info("Deleting deployed namespaces")
		ValidateAndDestroy(contexts, opts)
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
