package tests

import (
	"context"
	"fmt"
	"math/rand"

	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	driver_api "github.com/portworx/torpedo/drivers/api"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/backup/portworx"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/spec"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/log"

	. "github.com/portworx/torpedo/tests"

	appsapi "k8s.io/api/apps/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	bucketName     string
	backupLocation string
)

func TearDownBackupRestore(bkpNamespaces []string, restoreNamespaces []string) {
	for _, bkpNamespace := range bkpNamespaces {
		BackupName := fmt.Sprintf("%s-%s", BackupNamePrefix, bkpNamespace)
		backupUID := getBackupUID(BackupName, OrgID)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		DeleteBackup(BackupName, backupUID, OrgID, ctx)
	}
	for _, restoreNamespace := range restoreNamespaces {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		RestoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, restoreNamespace)
		DeleteRestore(RestoreName, OrgID, ctx)
	}

	ctx, err := backup.GetAdminCtxFromSecret()
	log.FailOnError(err, "Fetching px-central-admin ctx")
	DeleteCluster(destinationClusterName, OrgID, ctx)
	DeleteCluster(SourceClusterName, orgID, ctx)
	// Need to add backup location UID for Delete Backup Location call
	DeleteBackupLocation(backupLocationName, "", OrgID)
	DeleteCloudCredential(CredName, OrgID, CloudCredUID)
}

func DeleteCloudAccounts(backupLocationMap map[string]string, credName string, cloudCredUID string) {
	for backupLocationUID, bkpLocationName := range backupLocationMap {
		DeleteBackupLocation(bkpLocationName, backupLocationUID, orgID)
	}
	time.Sleep(time.Minute * 3)
	ctx, err := backup.GetAdminCtxFromSecret()
	log.FailOnError(err, "Fetching px-central-admin ctx")
	DeleteCloudCredential(credName, orgID, cloudCredUID)
	DeleteCluster(SourceClusterName, orgID, ctx)
	DeleteCluster(destinationClusterName, orgID, ctx)
}

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
		defer EndTorpedoTest()
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
		defer EndTorpedoTest()
		log.Infof("Cleanup started")
		err := backup.DeleteUser("testuser1")
		log.FailOnError(err, "Failed to delete user(s)")
		err = backup.DeleteGroup("testgroup1")
		log.FailOnError(err, "Failed to delete group(s)")
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
	)
	backupLocationMap := make(map[string]string)
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	var namespaceMapping map[string]string
	namespaceMapping = make(map[string]string)
	providers := getProviders()
	timestamp := strconv.Itoa(int(time.Now().Unix()))
	intervalName := fmt.Sprintf("%s-%s", "interval", timestamp)
	dailyName := fmt.Sprintf("%s-%s", "daily", timestamp)
	weeklyName := fmt.Sprintf("%s-%s", "weekly", timestamp)
	monthlyName := fmt.Sprintf("%s-%s", "monthly", timestamp)
	JustBeforeEach(func() {
		StartTorpedoTest("Backup: BasicBackupCreation", "Deploying backup", nil, 0)
		log.InfoD("Verifying if the pre/post rules for the required apps are present in the AppParameters or not ")
		for i := 0; i < len(appList); i++ {
			if Contains(postRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["post"]; ok {
					dash.VerifyFatal(ok, true, "Post Rule details mentioned for the apps")
				}
			}
			if Contains(preRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["pre"]; ok {
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
	It("Basic Backup Creation", func() {

		Step("Validate applications", func() {
			ValidateApplications(contexts)
		})
		Step("Creating rules for backup", func() {
			log.InfoD("Creating pre rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				preRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "pre")
				log.FailOnError(err, "Creating pre rule for deployed apps failed")
				dash.VerifyFatal(preRuleStatus, true, "Verifying pre rule for backup")

				if ruleName != "" {
					preRuleNameList = append(preRuleNameList, ruleName)
				}
			}
			log.InfoD("Creating post rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				postRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "post")
				log.FailOnError(err, "Creating post rule for deployed apps failed")
				dash.VerifyFatal(postRuleStatus, true, "Verifying Post rule for backup")
				if ruleName != "" {
					postRuleNameList = append(postRuleNameList, ruleName)
				}
			}
		})
		Step("Creating bucket,backup location and cloud setting", func() {
			log.InfoD("Creating bucket,backup location and cloud setting")
			bucketNames := getBucketName()
			for _, provider := range providers {
				bucketName := fmt.Sprintf("%s-%s", provider, bucketNames[0])
				cloudCredName = fmt.Sprintf("%s-%s", "cred", provider)
				bkpLocationName = fmt.Sprintf("%s-%s-bl", provider, bucketNames[0])
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID)
				time.Sleep(time.Minute * 3)
				CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, bucketName, orgID, "")
			}
		})
		Step("Creating backup schedule policies", func() {
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
		Step("Register cluster for backup", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			CreateSourceAndDestClusters(orgID, "", "", ctx)
			clusterStatus, clusterUid = Inst().Backup.RegisterBackupCluster(orgID, SourceClusterName, "")
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, "Verifying backup cluster")
		})
		Step("Taking backup of applications", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			dash.VerifyFatal(err, nil, "Getting context")
			for _, namespace := range bkpNamespaces {
				backupName = fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				err = CreateBackup(backupName, SourceClusterName, bkpLocationName, backupLocationUID, []string{namespace},
					labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
				dash.VerifyFatal(err, nil, "Verifying backup creation")
			}
		})
		Step("Restoring the backed up application", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName = fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				restoreName = fmt.Sprintf("%s-%s", "test-restore", namespace)
				CreateRestore(restoreName, backupName, namespaceMapping, destinationClusterName, orgID, ctx)
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		policyList := []string{intervalName, dailyName, weeklyName, monthlyName}
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		if len(preRuleNameList) > 0 {
			for _, ruleName := range preRuleNameList {
				err := Inst().Backup.DeleteRuleForBackup(orgID, ruleName)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting  backup pre rules %s", ruleName))
			}
		}
		if len(postRuleNameList) > 0 {
			for _, ruleName := range postRuleNameList {
				err := Inst().Backup.DeleteRuleForBackup(orgID, ruleName)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting  backup post rules %s", ruleName))
			}
		}
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, policyList)
		dash.VerifySafely(err, nil, "Deleting backup schedule policies")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.Info(" Deleting deployed applications")
		ValidateAndDestroy(contexts, opts)
		backupDriver := Inst().Backup
		for _, namespace := range bkpNamespaces {
			backupName = fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
			DeleteBackup(backupName, backupUID, orgID, ctx)
		}
		for _, namespace := range bkpNamespaces {
			restoreName = fmt.Sprintf("%s-%s", "test-restore", namespace)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			DeleteRestore(restoreName, orgID, ctx)
		}
		DeleteCloudAccounts(backupLocationMap, cloudCredName, cloudCredUID)
	})
})

var _ = Describe("{DifferentAccessSameUser}", func() {
	var (
		contexts          []*scheduler.Context
		appContexts       []*scheduler.Context
		bkpNamespaces     []string
		clusterUid        string
		clusterStatus     api.ClusterInfo_StatusInfo_Status
		groupName         string
		userName          string
		backupName        string
		backupLocationUID string
		cloudCredName     string
		cloudCredUID      string
		bkpLocationName   string
	)
	userContexts := make([]context.Context, 0)
	backupLocationMap := make(map[string]string)
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	JustBeforeEach(func() {
		StartTorpedoTest("DifferentAccessSameUser",
			"Take a backup and add user with readonly access and the group  with full access", nil, 82938)
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
	It("Different Access Same User", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		dash.VerifyFatal(err, nil, "Getting context")
		Step("Validate applications", func() {
			log.InfoD("Validate applications ")
			ValidateApplications(contexts)
		})
		Step("Create Users", func() {
			log.InfoD("Creating users testuser")
			userName = "testuser"
			firstName := "FirstName"
			lastName := "LastName"
			email := "testuser@cnbu.com"
			err := backup.AddUser(userName, firstName, lastName, email, "Password1")
			log.FailOnError(err, "Failed to create user - %s", userName)

		})
		Step("Create Groups", func() {
			log.InfoD("Creating group testGroup")
			groupName = fmt.Sprintf("testGroup")
			err := backup.AddGroup(groupName)
			log.FailOnError(err, "Failed to create group - %v", groupName)

		})
		Step("Add users to group", func() {
			log.InfoD("Adding user to groups")
			err := backup.AddGroupToUser(userName, groupName)
			dash.VerifyFatal(err, nil, "Adding user to group")
			usersOfGroup, err := backup.GetMembersOfGroup(groupName)
			log.FailOnError(err, "Error fetching members of the group - %v", groupName)
			log.Infof("Group [%v] contains the following users: \n%v", groupName, usersOfGroup)

		})
		Step("Creating bucket,backup location and cloud setting", func() {
			log.InfoD("Creating bucket,backup location and cloud setting")
			bucketNames := getBucketName()
			providers := getProviders()
			for _, provider := range providers {
				bucketName := fmt.Sprintf("%s-%s", provider, bucketNames[0])
				cloudCredName = fmt.Sprintf("%s-%s", "cloudcred", provider)
				bkpLocationName = fmt.Sprintf("%s-%s-bl", provider, bucketNames[0])
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID)
				time.Sleep(time.Minute * 3)
				CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, bucketName, orgID, "")
			}
		})
		Step("Register cluster for backup", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			CreateSourceAndDestClusters(orgID, "", "", ctx)
			clusterStatus, clusterUid = Inst().Backup.RegisterBackupCluster(orgID, SourceClusterName, "")
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, "Verifying backup cluster")
		})
		Step("Taking backup of applications", func() {
			backupName = fmt.Sprintf("%s-%s", BackupNamePrefix, bkpNamespaces[0])
			err = CreateBackup(backupName, SourceClusterName, bkpLocationName, backupLocationUID, []string{bkpNamespaces[0]},
				labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
			dash.VerifyFatal(err, nil, "Verifying backup creation")
		})
		Step("Share backup with  user having readonly access", func() {
			log.InfoD("Adding user with readonly access")
			ShareBackup(backupName, nil, []string{userName}, ViewOnlyAccess, ctx)
		})
		Step("Share backup with group having full access", func() {
			log.InfoD("Adding user with readonly access")
			ShareBackup(backupName, []string{groupName}, nil, FullAccess, ctx)
		})
		Step("Share Backup with View Only access to a user of Full access group and Validate", func() {
			log.InfoD("Backup is shared with Group having FullAccess after it is shared with user having ViewOnlyAccess, therefore user should have FullAccess")
			ctxNonAdmin, err := backup.GetNonAdminCtx(userName, "Password1")
			log.FailOnError(err, "Fetching user ctx")
			userContexts = append(userContexts, ctxNonAdmin)
			log.InfoD("Registering Source and Destination clusters from user context")
			CreateSourceAndDestClusters(orgID, "", "", ctxNonAdmin)
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			// Try restore with user having RestoreAccess and it should pass
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin)
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s to validate user can delete restore  ", restoreName)
			DeleteRestore(restoreName, orgID, ctxNonAdmin)
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
			backupDeleteResponse, err := DeleteBackup(backupName, backupUID, orgID, ctxNonAdmin)
			log.FailOnError(err, "Backup [%s] could not be deleted by user [%s] with delete response %s", backupName, userName, backupDeleteResponse)
			dash.VerifyFatal(backupDeleteResponse.String(), "", "Verifying backup deletion is successful")
		})
	})

	JustAfterEach(func() {
		// For all the delete methods we need to add return and handle the error here
		defer EndTorpedoTest()
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)
		backupDriver := Inst().Backup
		for _, namespace := range bkpNamespaces {
			backupName = fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
			DeleteBackup(backupName, backupUID, orgID, ctx)
		}
		time.Sleep(time.Minute * 3)
		err = backup.DeleteUser(userName)
		dash.VerifySafely(err, nil, "Deleting  user")
		err = backup.DeleteGroup(groupName)
		dash.VerifySafely(err, nil, "Deleting  group")
		DeleteCloudAccounts(backupLocationMap, cloudCredName, cloudCredUID)
	})
})

var _ = Describe("{ShareBackupWithUsersAndGroups}", func() {
	numberOfUsers := 30
	numberOfGroups := 3
	groupSize := 10
	numberOfBackups := 9
	users := make([]string, 0)
	groups := make([]string, 0)
	backupNames := make([]string, 0)
	userContexts := make([]context.Context, 0)
	var contexts []*scheduler.Context
	labelSelectors := make(map[string]string)
	var backupLocationUID string
	var customBucketName string
	var cloudCredUID string
	var CloudCredUidList []string
	var appContexts []*scheduler.Context
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var customBackupLocationName string
	var credName string
	bkpNamespaces = make([]string, 0)
	providers := getProviders()

	JustBeforeEach(func() {
		StartTorpedoTest("ShareLargeNumberOfBackups",
			"Share large number of backups with multiple users and groups with View only, Restore and Full Access", nil, 82934)
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
	It("Share large number of backups", func() {
		Step("Validate applications and get their labels", func() {
			log.InfoD("Validate applications and get their labels")
			ValidateApplications(contexts)
			log.Infof("Create list of pod selector for the apps deployed")
		})

		Step("Create Users", func() {
			log.InfoD("Creating %d users", numberOfUsers)
			var wg sync.WaitGroup
			for i := 1; i <= numberOfUsers; i++ {
				userName := fmt.Sprintf("testuser%v", i)
				firstName := fmt.Sprintf("FirstName%v", i)
				lastName := fmt.Sprintf("LastName%v", i)
				email := fmt.Sprintf("testuser%v@cnbu.com", i)
				wg.Add(1)
				go func(userName, firstName, lastName, email string) {
					err := backup.AddUser(userName, firstName, lastName, email, "Password1")
					log.FailOnError(err, "Failed to create user - %s", userName)
					users = append(users, userName)
					wg.Done()
				}(userName, firstName, lastName, email)
			}
			wg.Wait()
		})

		Step("Create Groups", func() {
			log.InfoD("Creating %d groups", numberOfGroups)
			var wg sync.WaitGroup
			for i := 1; i <= numberOfGroups; i++ {
				groupName := fmt.Sprintf("testGroup%v", i)
				wg.Add(1)
				go func(groupName string) {
					err := backup.AddGroup(groupName)
					log.FailOnError(err, "Failed to create group - %v", groupName)
					groups = append(groups, groupName)
					wg.Done()
				}(groupName)
			}
			wg.Wait()
		})

		Step("Add users to group", func() {
			log.InfoD("Adding users to groups")
			var wg sync.WaitGroup
			for i := 0; i < len(users); i++ {
				groupIndex := i / groupSize
				wg.Add(1)
				go func(i, groupIndex int) {
					err := backup.AddGroupToUser(users[i], groups[groupIndex])
					log.FailOnError(err, "Failed to assign group to user")
					wg.Done()
				}(i, groupIndex)
			}
			wg.Wait()

			// Print the groups
			for _, group := range groups {
				usersOfGroup, err := backup.GetMembersOfGroup(group)
				log.FailOnError(err, "Error fetching members of the group - %v", group)
				log.Infof("Group [%v] contains the following users: \n%v", group, usersOfGroup)
			}
		})

		Step("Adding Credentials and Registering Backup Location", func() {
			log.InfoD("Creating bucket, cloud credentials and backup location")
			for _, provider := range providers {
				customBucketName = fmt.Sprintf("%s-%v", getBucketName()[0], time.Now().Unix())
				CreateBucket(provider, customBucketName)
				cloudCredUID = uuid.New()
				CloudCredUidList = append(CloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				CreateCloudCredential(provider, credName, cloudCredUID, orgID)
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				//TODO: Eliminate time.Sleep
				time.Sleep(time.Minute * 1)
				customBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, customBucketName, orgID, "")
				log.InfoD("Created Backup Location with name - %s", customBackupLocationName)
			}
		})

		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			CreateSourceAndDestClusters(orgID, "", "", ctx)
			clusterStatus, clusterUid = Inst().Backup.RegisterBackupCluster(orgID, SourceClusterName, "")
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, "Verifying backup cluster status")
		})

		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			var sem = make(chan struct{}, 10)
			var wg sync.WaitGroup
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				for i := 0; i < numberOfBackups; i++ {
					sem <- struct{}{}
					time.Sleep(3 * time.Second)
					backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
					backupNames = append(backupNames, backupName)
					wg.Add(1)
					go func(backupName string) {
						defer GinkgoRecover()
						defer wg.Done()
						defer func() { <-sem }()
						err = CreateBackup(backupName, SourceClusterName, customBackupLocationName, backupLocationUID, []string{namespace},
							labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
						dash.VerifyFatal(err, nil, "Verifying backup creation")
					}(backupName)
				}
				wg.Wait()
			}
			log.Infof("List of backups - %v", backupNames)
		})

		Step("Sharing backup with groups", func() {
			log.InfoD("Sharing backups with groups")
			backupsToBeSharedWithEachGroup := 3
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for i, backupName := range backupNames {
				groupIndex := i / backupsToBeSharedWithEachGroup
				switch groupIndex {
				case 0:
					err = ShareBackup(backupName, []string{groups[groupIndex]}, nil, ViewOnlyAccess, ctx)
					log.FailOnError(err, "Failed to share backup %s", backupName)
				case 1:
					err = ShareBackup(backupName, []string{groups[groupIndex]}, nil, RestoreAccess, ctx)
					log.FailOnError(err, "Failed to share backup %s", backupName)
				case 2:
					err = ShareBackup(backupName, []string{groups[groupIndex]}, nil, FullAccess, ctx)
					log.FailOnError(err, "Failed to share backup %s", backupName)
				default:
					err = ShareBackup(backupName, []string{groups[0]}, nil, ViewOnlyAccess, ctx)
					log.FailOnError(err, "Failed to share backup %s", backupName)
				}
			}
		})

		Step("Share Backup with Full access to a user of View Only access group and Validate", func() {
			log.InfoD("Share Backup with Full access to a user of View Only access group and Validate")
			// Get user from the view access group
			username, err := backup.GetRandomUserFromGroup(groups[0])
			log.FailOnError(err, "Failed to get a random user from group [%s]", groups[0])
			log.Infof("Sharing backup with user - %s", username)

			// Get Admin Context - needed to share backup and get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			// Share backup with the user
			backupName := backupNames[0]
			err = ShareBackup(backupName, nil, []string{username}, FullAccess, ctx)
			log.FailOnError(err, "Failed to share backup %s", backupName)

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(username, "Password1")
			log.FailOnError(err, "Fetching %s ctx", username)
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			CreateSourceAndDestClusters(orgID, "", "", ctxNonAdmin)

			// Start Restore
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin)
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)

			// Restore validation to make sure that the user with Full Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			DeleteRestore(restoreName, orgID, ctxNonAdmin)

			// Get Backup UID
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)

			// Delete backup to confirm that the user has Full Access
			backupDeleteResponse, err := DeleteBackup(backupName, backupUID, orgID, ctxNonAdmin)
			log.FailOnError(err, "Backup [%s] could not be deleted by user [%s]", backupName, username)
			dash.VerifyFatal(backupDeleteResponse.String(), "", "Verifying backup deletion is successful")
		})

		Step("Share Backup with View Only access to a user of Full access group and Validate", func() {
			log.InfoD("Share Backup with View Only access to a user of Full access group and Validate")
			// Get user from the view access group
			username, err := backup.GetRandomUserFromGroup(groups[2])
			log.FailOnError(err, "Failed to get a random user from group [%s]", groups[2])
			log.Infof("Sharing backup with user - %s", username)

			// Get Admin Context - needed to share backup and get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			// Share backup with the user
			backupName := backupNames[6]
			err = ShareBackup(backupName, nil, []string{username}, ViewOnlyAccess, ctx)
			log.FailOnError(err, "Failed to share backup %s", backupName)

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(username, "Password1")
			log.FailOnError(err, "Fetching %s ctx", username)
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			CreateSourceAndDestClusters(orgID, "", "", ctxNonAdmin)

			// Get Backup UID
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)

			// Delete backup to confirm that the user cannot delete the backup
			_, err = DeleteBackup(backupName, backupUID, orgID, ctxNonAdmin)
			log.Infof("Error message - %s", err.Error())
			dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to delete backup"), true, "Verifying backup deletion is not possible")
		})

		Step("Share Backup with Restore access to a user of View Only access group and Validate", func() {
			log.InfoD("Share Backup with Restore access to a user of View Only access group and Validate")
			// Get user from the view only access group
			username, err := backup.GetRandomUserFromGroup(groups[0])
			log.FailOnError(err, "Failed to get a random user from group [%s]", groups[0])
			log.Infof("Sharing backup with user - %s", username)

			// Get Admin Context - needed to share backup and get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			// Share backup with the user
			backupName := backupNames[1]
			err = ShareBackup(backupName, nil, []string{username}, RestoreAccess, ctx)
			log.FailOnError(err, "Failed to share backup %s", backupName)

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(username, "Password1")
			log.FailOnError(err, "Fetching %s ctx", username)
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			CreateSourceAndDestClusters(orgID, "", "", ctxNonAdmin)

			// Start Restore
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin)
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)

			// Restore validation to make sure that the user with can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			DeleteRestore(restoreName, orgID, ctxNonAdmin)

			// Get Backup UID
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)

			// Delete backup to confirm that the user cannot delete the backup
			_, err = DeleteBackup(backupName, backupUID, orgID, ctxNonAdmin)
			dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to delete backup"), true, "Verifying backup deletion is not possible")
		})

		Step("Validate Restore access for a user of Restore group", func() {
			log.InfoD("Validate Restore access for a user of Restore group")
			// Get user from the restore access group
			username, err := backup.GetRandomUserFromGroup(groups[1])
			log.FailOnError(err, "Failed to get a random user from group [%s]", groups[1])

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(username, "Password1")
			log.FailOnError(err, "Fetching %s ctx", username)
			userContexts = append(userContexts, ctxNonAdmin)

			// Get Admin Context - needed to share backup and get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			CreateSourceAndDestClusters(orgID, "", "", ctxNonAdmin)

			// Start Restore
			backupName := backupNames[3]
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin)
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)

			// Restore validation to make sure that the user with can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			DeleteRestore(restoreName, orgID, ctxNonAdmin)
			log.InfoD("Deleting Restore [%s] was successful", restoreName)

			// Get Backup UID
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)

			// Delete backup to confirm that the user cannot delete the backup
			_, err = DeleteBackup(backupName, backupUID, orgID, ctxNonAdmin)
			dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to delete backup"), true, "Verifying backup deletion is not possible")
		})

		Step("Validate that user with View Only access cannot restore or delete the backup", func() {
			log.InfoD("Validate that user with View Only access cannot restore or delete the backup")
			// Get user from the view only access group
			username, err := backup.GetRandomUserFromGroup(groups[0])
			log.FailOnError(err, "Failed to get a random user from group [%s]", groups[0])

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(username, "Password1")
			log.FailOnError(err, "Fetching %s ctx", username)
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			CreateSourceAndDestClusters(orgID, "", "", ctxNonAdmin)

			// Start Restore
			backupName := backupNames[2]
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin)
			// Restore validation to make sure that the user with View Access cannot restore
			dash.VerifyFatal(strings.Contains(err.Error(), "failed to retrieve backup location"), true, "Verifying backup restore is not possible")

			// Get Admin Context - needed to get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			// Get Backup UID
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)

			// Delete backup to confirm that the user cannot delete the backup
			_, err = DeleteBackup(backupName, backupUID, orgID, ctxNonAdmin)
			dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to delete backup"), true, "Verifying backup deletion is not possible")
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Deleting the deployed apps after the testcase")
		for i := 0; i < len(contexts); i++ {
			opts := make(map[string]bool)
			opts[SkipClusterScopedObjects] = true
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			err := Inst().S.Destroy(contexts[i], opts)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verify destroying app %s, Err: %v", taskName, err))
		}
		var wg sync.WaitGroup
		log.Infof("Cleaning up users")
		for _, userName := range users {
			wg.Add(1)
			go func(userName string) {
				defer wg.Done()
				err := backup.DeleteUser(userName)
				log.FailOnError(err, "Error deleting user %v", userName)
			}(userName)
		}
		wg.Wait()

		log.Infof("Cleaning up groups")
		for _, groupName := range groups {
			wg.Add(1)
			go func(groupName string) {
				defer wg.Done()
				err := backup.DeleteGroup(groupName)
				log.FailOnError(err, "Error deleting user %v", groupName)
			}(groupName)
		}
		wg.Wait()

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")

		log.Infof("Deleting registered clusters for admin context")
		DeleteCluster(SourceClusterName, orgID, ctx)
		DeleteCluster(destinationClusterName, orgID, ctx)

		log.Infof("Deleting registered clusters for non-admin context")
		for _, ctxNonAdmin := range userContexts {
			DeleteCluster(SourceClusterName, orgID, ctxNonAdmin)
			DeleteCluster(destinationClusterName, orgID, ctxNonAdmin)
		}

		backupDriver := Inst().Backup
		for _, backupName := range backupNames {
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
			log.Infof("About to delete backup - %s", backupName)
			DeleteBackup(backupName, backupUID, orgID, ctx)
		}

		log.Infof("Cleaning up backup location - %s", customBackupLocationName)
		DeleteBackupLocation(customBackupLocationName, backupLocationUID, orgID)

		log.Infof("Cleaning up buckets")
		for _, provider := range providers {
			DeleteBucket(provider, customBucketName)
		}

		log.Infof("Cleaning cloud credential")
		//TODO: Eliminate time.Sleep
		time.Sleep(time.Minute * 3)
		DeleteCloudCredential(credName, orgID, cloudCredUID)
	})
})

var _ = Describe("{CancelClusterBackupShare}", func() {
	numberOfUsers := 10
	numberOfGroups := 1
	groupSize := 10
	numberOfBackups := 6
	users := make([]string, 0)
	groups := make([]string, 0)
	backupNames := make([]string, 0)
	userContexts := make([]context.Context, 0)
	var contexts []*scheduler.Context
	labelSelectors := make(map[string]string)
	var backupLocationUID string
	var customBucketName string
	var cloudCredUID string
	var CloudCredUidList []string
	var appContexts []*scheduler.Context
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var customBackupLocationName string
	var credName string
	var chosenUser string
	individualUser := "autogenerated-user"
	bkpNamespaces = make([]string, 0)
	providers := getProviders()

	JustBeforeEach(func() {
		StartTorpedoTest("CancelClusterBackupShare",
			"Share all backups at cluster level with a user group and revoke it and validate", nil, 82935)
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
	It("Share all backups at cluster level with a user group and revoke it and validate", func() {
		Step("Validate applications and get their labels", func() {
			log.InfoD("Validate applications")
			ValidateApplications(contexts)
		})

		Step("Create Users", func() {
			log.InfoD("Creating %d users to be added to the group", numberOfUsers)
			var wg sync.WaitGroup
			for i := 1; i <= numberOfUsers; i++ {
				userName := fmt.Sprintf("testuser%v", i)
				firstName := fmt.Sprintf("FirstName%v", i)
				lastName := fmt.Sprintf("LastName%v", i)
				email := fmt.Sprintf("testuser%v@cnbu.com", i)
				wg.Add(1)
				go func(userName, firstName, lastName, email string) {
					err := backup.AddUser(userName, firstName, lastName, email, "Password1")
					log.FailOnError(err, "Failed to create user - %s", userName)
					users = append(users, userName)
					wg.Done()
				}(userName, firstName, lastName, email)
			}
			wg.Wait()

			log.InfoD("Creating a user with username - [%s] who is not part of any group", individualUser)
			firstName := "autogenerated-firstname"
			lastName := "autogenerated-last name"
			email := "autogenerated-email@cnbu.com"
			err := backup.AddUser(individualUser, firstName, lastName, email, "Password1")
			log.FailOnError(err, "Failed to create user - %s", individualUser)
		})

		Step("Create Groups", func() {
			log.InfoD("Creating %d groups", numberOfGroups)
			var wg sync.WaitGroup
			for i := 1; i <= numberOfGroups; i++ {
				groupName := fmt.Sprintf("testGroup%v", i)
				wg.Add(1)
				go func(groupName string) {
					err := backup.AddGroup(groupName)
					log.FailOnError(err, "Failed to create group - %v", groupName)
					groups = append(groups, groupName)
					wg.Done()
				}(groupName)
			}
			wg.Wait()
		})

		Step("Add users to group", func() {
			log.InfoD("Adding users to groups")
			var wg sync.WaitGroup
			for i := 0; i < len(users); i++ {
				groupIndex := i / groupSize
				wg.Add(1)
				go func(i, groupIndex int) {
					err := backup.AddGroupToUser(users[i], groups[groupIndex])
					log.FailOnError(err, "Failed to assign group to user")
					wg.Done()
				}(i, groupIndex)
			}
			wg.Wait()

			// Print the groups
			for _, group := range groups {
				usersOfGroup, err := backup.GetMembersOfGroup(group)
				log.FailOnError(err, "Error fetching members of the group - %v", group)
				log.Infof("Group [%v] contains the following users: \n%v", group, usersOfGroup)
			}
		})

		Step("Adding Credentials and Registering Backup Location", func() {
			log.InfoD("Creating bucket, cloud credentials and backup location")
			for _, provider := range providers {
				customBucketName = fmt.Sprintf("%s-%v", getBucketName()[0], time.Now().Unix())
				CreateBucket(provider, customBucketName)
				cloudCredUID = uuid.New()
				CloudCredUidList = append(CloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				CreateCloudCredential(provider, credName, cloudCredUID, orgID)
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				//TODO: Eliminate time.Sleep
				time.Sleep(time.Minute * 1)
				customBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, customBucketName, orgID, "")
				log.InfoD("Created Backup Location with name - %s", customBackupLocationName)
			}
		})

		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			CreateSourceAndDestClusters(orgID, "", "", ctx)
			clusterStatus, clusterUid = Inst().Backup.RegisterBackupCluster(orgID, SourceClusterName, "")
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, "Verifying backup cluster status")
		})

		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			var sem = make(chan struct{}, 10)
			var wg sync.WaitGroup
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				for i := 0; i < numberOfBackups; i++ {
					sem <- struct{}{}
					time.Sleep(3 * time.Second)
					backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
					backupNames = append(backupNames, backupName)
					wg.Add(1)
					go func(backupName string) {
						defer GinkgoRecover()
						defer wg.Done()
						defer func() { <-sem }()
						CreateBackup(backupName, SourceClusterName, customBackupLocationName, backupLocationUID, []string{namespace},
							labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
					}(backupName)
				}
				wg.Wait()
			}
			log.Infof("List of backups - %v", backupNames)
		})

		Step("Share all backups with Full Access in source cluster with a group and a user who is not part of the group", func() {
			log.InfoD("Share all backups with Full Access in source cluster with a group and a user who is not part of the group")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = ClusterUpdateBackupShare(SourceClusterName, []string{groups[0]}, []string{individualUser}, FullAccess, true, ctx)
			log.FailOnError(err, "Failed sharing all backups for cluster [%s]", SourceClusterName)
		})

		Step("Validate Full Access of backups shared at cluster level", func() {
			log.InfoD("Validate Full Access of backups shared at cluster level for a user of a group")
			// Get user from group
			var err error
			chosenUser, err = backup.GetRandomUserFromGroup(groups[0])
			log.FailOnError(err, "Failed to get a random user from group [%s]", groups[0])
			log.Infof("User chosen to validate full access - %s", chosenUser)

			// Get Admin Context - needed to share backup and get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(chosenUser, "Password1")
			log.FailOnError(err, "Fetching %s ctx", chosenUser)
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			CreateSourceAndDestClusters(orgID, "", "", ctxNonAdmin)

			// Start Restore
			backupName := backupNames[5]
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin)
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)

			// Restore validation to make sure that the user with Full Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			DeleteRestore(restoreName, orgID, ctxNonAdmin)

			// Get Backup UID
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)

			// Delete backup to confirm that the user has Full Access
			backupDeleteResponse, err := DeleteBackup(backupName, backupUID, orgID, ctxNonAdmin)
			log.FailOnError(err, "Backup [%s] could not be deleted by user [%s]", backupName, chosenUser)
			dash.VerifyFatal(backupDeleteResponse.String(), "",
				fmt.Sprintf("Verifying backup [%s] deletion is successful by user [%s]", backupName, chosenUser))

			// Now validating with individual user who is not part of any group
			// Get user context
			log.InfoD("Validate Full Access of backups shared at cluster level for an individual user - %s", individualUser)
			ctxNonAdmin, err = backup.GetNonAdminCtx(individualUser, "Password1")
			log.FailOnError(err, "Fetching %s ctx", individualUser)
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			CreateSourceAndDestClusters(orgID, "", "", ctxNonAdmin)

			// Start Restore
			backupName = backupNames[4]
			restoreName = fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin)
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)

			// Restore validation to make sure that the user with Full Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			DeleteRestore(restoreName, orgID, ctxNonAdmin)

			// Get Backup UID
			backupUID, err = backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)

			// Delete backup to confirm that the user has Full Access
			backupDeleteResponse, err = DeleteBackup(backupName, backupUID, orgID, ctxNonAdmin)
			log.FailOnError(err, "Backup [%s] could not be deleted by user [%s]", backupName, individualUser)
			dash.VerifyFatal(backupDeleteResponse.String(), "",
				fmt.Sprintf("Verifying backup [%s] deletion is successful by user [%s]", backupName, individualUser))
		})

		Step("Share all backups with Restore Access in source cluster with a group and a user who is not part of the group", func() {
			log.InfoD("Share all backups with Full Access in source cluster with a group and a user who is not part of the group")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = ClusterUpdateBackupShare(SourceClusterName, []string{groups[0]}, []string{"autogenerated-user"}, RestoreAccess, true, ctx)
			log.FailOnError(err, "Failed sharing all backups for cluster [%s]", SourceClusterName)
		})

		Step("Validate Restore Access of backups shared at cluster level", func() {
			log.InfoD("Validate Restore Access of backups shared at cluster level")
			log.Infof("User chosen to validate restore access - %s", chosenUser)

			// Get Admin Context - needed to share backup and get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(chosenUser, "Password1")
			log.FailOnError(err, "Fetching %s ctx", chosenUser)

			// Start Restore
			backupName := backupNames[3]
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin)
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)

			// Restore validation to make sure that the user with Restore Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			DeleteRestore(restoreName, orgID, ctxNonAdmin)

			// Get Backup UID
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)

			// Delete backup to confirm that the user cannot delete the backup
			_, err = DeleteBackup(backupName, backupUID, orgID, ctxNonAdmin)
			dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to delete backup"), true, "Verifying backup deletion is not possible")

			// Now validating with individual user who is not part of any group
			// Get user context
			log.InfoD("Validate Restore Access of backups shared at cluster level for an individual user - %s", individualUser)
			ctxNonAdmin, err = backup.GetNonAdminCtx(individualUser, "Password1")
			log.FailOnError(err, "Fetching %s ctx", individualUser)

			// Start Restore
			backupName = backupNames[2]
			restoreName = fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin)
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)

			// Restore validation to make sure that the user with Restore Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			DeleteRestore(restoreName, orgID, ctxNonAdmin)

			// Get Backup UID
			backupUID, err = backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)

			// Delete backup to confirm that the user cannot delete the backup
			_, err = DeleteBackup(backupName, backupUID, orgID, ctxNonAdmin)
			dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to delete backup"), true, "Verifying backup deletion is not possible")
		})

		Step("Share all backups with View Only Access in source cluster with a group and a user who is not part of the group", func() {
			log.InfoD("Share all backups with View Only Access in source cluster with a group and a user who is not part of the group")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = ClusterUpdateBackupShare(SourceClusterName, []string{groups[0]}, []string{individualUser}, ViewOnlyAccess, true, ctx)
			log.FailOnError(err, "Failed sharing all backups for cluster [%s]", SourceClusterName)
		})

		Step("Validate View Only Access of backups shared at cluster level", func() {
			log.InfoD("Validate View Only Access of backups shared at cluster level")
			log.Infof("User chosen to validate view only access - %s", chosenUser)

			// Get Admin Context - needed to share backup and get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(chosenUser, "Password1")
			log.FailOnError(err, "Fetching %s ctx", chosenUser)

			// Start Restore
			backupName := backupNames[1]
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin)

			// Restore validation to make sure that the user with View Access cannot restore
			dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to restore backup"), true, "Verifying backup restore is not possible")

			// Get Backup UID
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)

			// Delete backup to confirm that the user cannot delete the backup
			_, err = DeleteBackup(backupName, backupUID, orgID, ctxNonAdmin)
			dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to delete backup"), true, "Verifying backup deletion is not possible")

			// Now validating with individual user who is not part of any group
			// Get user context
			log.InfoD("Validate View Only Access of backups shared at cluster level for an individual user - %s", individualUser)
			ctxNonAdmin, err = backup.GetNonAdminCtx(individualUser, "Password1")
			log.FailOnError(err, "Fetching %s ctx", individualUser)

			// Start Restore
			restoreName = fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin)

			// Restore validation to make sure that the user with View Access cannot restore
			dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to restore backup"), true, "Verifying backup restore is not possible")

			// Get Backup UID
			backupUID, err = backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)

			// Delete backup to confirm that the user cannot delete the backup
			_, err = DeleteBackup(backupName, backupUID, orgID, ctxNonAdmin)
			dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to delete backup"), true, "Verifying backup deletion is not possible")

		})

		Step("Revoke all the shared backups in source cluster", func() {
			log.InfoD("Share all backups with View Only Access in source cluster with a group and a user who is not part of the group")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = ClusterUpdateBackupShare(SourceClusterName, []string{groups[0]}, []string{individualUser}, ViewOnlyAccess, false, ctx)
			log.FailOnError(err, "Failed sharing all backups for cluster [%s]", SourceClusterName)
		})

		Step("Validate that no groups or users have access to backups shared at cluster level", func() {
			log.InfoD("Validate no groups or users have access to backups shared at cluster level")
			log.Infof("User chosen to validate no access - %s", chosenUser)

			// Enumerate all the backups available to the user
			userBackups, err := GetAllBackupsForUser(chosenUser, "Password1")
			log.FailOnError(err, "Failed to get all backups for user - [%s]", chosenUser)
			log.Infof("Backups user [%s] has access to - %v", chosenUser, userBackups)
			log.InfoD("Checking backups user [%s] has after revoking", chosenUser)
			noAccessCheck := func() (interface{}, bool, error) {
				if len(userBackups) > 0 {
					return "", true, fmt.Errorf("Waiting for all backup access - [%v] to be revoked for user = [%s]",
						userBackups, chosenUser)
				}
				return "", false, nil
			}
			task.DoRetryWithTimeout(noAccessCheck, 5*time.Minute, 30*time.Second)
			dash.VerifyFatal(len(userBackups), 0, fmt.Sprintf("Validating that user [%s] has access to no backups", chosenUser))

			// Now validating with individual user who is not part of any group
			// Get user context
			log.InfoD("Validate no access of backups shared at cluster level for an individual user - %s", individualUser)
			userBackups1, err := GetAllBackupsForUser(individualUser, "Password1")
			log.FailOnError(err, "Failed to get all backups for user - [%s]", individualUser)
			log.Infof("Backups user [%s] has access to - %v", individualUser, userBackups1)
			log.InfoD("Checking backups user [%s] has after revoking", individualUser)
			noAccessCheck = func() (interface{}, bool, error) {
				if len(userBackups1) > 0 {
					return "", true, fmt.Errorf("Waiting for all backup access - [%v] to be revoked for user = [%s]",
						userBackups1, individualUser)
				}
				return "", false, nil
			}
			task.DoRetryWithTimeout(noAccessCheck, 5*time.Minute, 30*time.Second)
			dash.VerifyFatal(len(userBackups1), 0, fmt.Sprintf("Validating that user [%s] has access to no backups", individualUser))
		})

	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Deleting the deployed apps after the testcase")
		for i := 0; i < len(contexts); i++ {
			opts := make(map[string]bool)
			opts[SkipClusterScopedObjects] = true
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			err := Inst().S.Destroy(contexts[i], opts)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verify destroying app %s, Err: %v", taskName, err))
		}
		var wg sync.WaitGroup
		log.Infof("Cleaning up users")
		for _, userName := range users {
			wg.Add(1)
			go func(userName string) {
				defer wg.Done()
				err := backup.DeleteUser(userName)
				log.FailOnError(err, "Error deleting user %v", userName)
			}(userName)
		}
		wg.Wait()
		err := backup.DeleteUser(individualUser)
		log.FailOnError(err, "Error deleting user %v", individualUser)

		log.Infof("Cleaning up groups")
		for _, groupName := range groups {
			wg.Add(1)
			go func(groupName string) {
				defer wg.Done()
				err := backup.DeleteGroup(groupName)
				log.FailOnError(err, "Error deleting user %v", groupName)
			}(groupName)
		}
		wg.Wait()

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")

		log.Infof("Deleting registered clusters for admin context")
		DeleteCluster(SourceClusterName, orgID, ctx)
		DeleteCluster(destinationClusterName, orgID, ctx)

		log.Infof("Deleting registered clusters for non-admin context")
		for _, ctxNonAdmin := range userContexts {
			DeleteCluster(SourceClusterName, orgID, ctxNonAdmin)
			DeleteCluster(destinationClusterName, orgID, ctxNonAdmin)
		}

		backupDriver := Inst().Backup
		for _, backupName := range backupNames {
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
			log.Infof("About to delete backup - %s", backupName)
			DeleteBackup(backupName, backupUID, orgID, ctx)
		}

		log.Infof("Cleaning up backup location - %s", customBackupLocationName)
		DeleteBackupLocation(customBackupLocationName, backupLocationUID, orgID)

		log.Infof("Cleaning up buckets")
		for _, provider := range providers {
			DeleteBucket(provider, customBucketName)
		}

		log.Infof("Cleaning cloud credential")
		//TODO: Eliminate time.Sleep
		time.Sleep(time.Minute * 3)
		DeleteCloudCredential(credName, orgID, cloudCredUID)
	})
})

var _ = Describe("{ShareBackupAndEdit}", func() {
	numberOfUsers := 2
	users := make([]string, 0)
	backupNames := make([]string, 0)
	userContexts := make([]context.Context, 0)
	var contexts []*scheduler.Context
	var BucketName string
	var BackupLocationName string
	var backupLocationUID string
	var cloudCredUID string
	var cloudCred1UID string
	var CloudCredUidList []string
	var appContexts []*scheduler.Context
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var credName string
	var cred1Name string
	bkpNamespaces = make([]string, 0)
	providers := getProviders()
	buckets := getBucketName()
	JustBeforeEach(func() {
		StartTorpedoTest("ShareBackupAndEdit",
			"Share backup with restore and fullaccess mode and edit the shared backup", nil, 0)
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
	It("Share the backup and edit", func() {
		Step("Validate applications and get their labels", func() {
			log.InfoD("Validate applications and get their labels")
			ValidateApplications(contexts)
			log.Infof("Create list of pod selector for the apps deployed")
		})
		Step("Create Users", func() {
			log.InfoD("Creating %d users", numberOfUsers)
			var wg sync.WaitGroup
			for i := 1; i <= numberOfUsers; i++ {
				userName := fmt.Sprintf("testuser%v", i)
				firstName := fmt.Sprintf("FirstName%v", i)
				lastName := fmt.Sprintf("LastName%v", i)
				email := fmt.Sprintf("testuser%v@cnbu.com", i)
				wg.Add(1)
				go func(userName, firstName, lastName, email string) {
					err := backup.AddUser(userName, firstName, lastName, email, "Password1")
					log.FailOnError(err, "Failed to create user - %s", userName)
					users = append(users, userName)
					wg.Done()
				}(userName, firstName, lastName, email)
			}
			wg.Wait()
		})
		Step("Adding Credentials and Registering Backup Location", func() {
			log.InfoD("Creating bucket, cloud credentials and backup location")
			for _, provider := range providers {
				BucketName = fmt.Sprintf("%s-%v", provider, buckets[0])
				cloudCredUID = uuid.New()
				CloudCredUidList = append(CloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				CreateCloudCredential(provider, credName, cloudCredUID, orgID)
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				cloudCred1UID = uuid.New()
				CloudCredUidList = append(CloudCredUidList, cloudCred1UID)
				cred1Name = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				CreateCloudCredential(provider, cred1Name, cloudCred1UID, orgID)
				//TODO: Eliminate time.Sleep
				time.Sleep(time.Minute * 1)
				BackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				CreateBackupLocation(provider, BackupLocationName, backupLocationUID, credName, cloudCredUID, BucketName, orgID, "")
				log.InfoD("Created Backup Location with name - %s", BackupLocationName)
			}
		})
		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			CreateSourceAndDestClusters(orgID, "", "", ctx)
			clusterStatus, clusterUid = Inst().Backup.RegisterBackupCluster(orgID, SourceClusterName, "")
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, "Verifying backup cluster status")
		})
		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
			backupNames = append(backupNames, backupName)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateBackup(backupName, SourceClusterName, BackupLocationName, backupLocationUID, []string{bkpNamespaces[0]},
				nil, orgID, clusterUid, "", "", "", "", ctx)
			dash.VerifyFatal(err, nil, "Verifying backup creation")
		})
		Step("Share backup with user restore mode and validate", func() {
			log.InfoD("Share backup with user restore mode and validate")
			log.Infof("Sharing backup with user - %s", users[0])

			// Get Admin Context - needed to share backup and get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			// Share backup with the user
			err = ShareBackup(backupNames[0], nil, []string{users[0]}, RestoreAccess, ctx)
			log.FailOnError(err, "Failed to share backup %s", backupNames[0])

			//updtae the backup with another cred
			log.InfoD("updtae the backup with another cred")
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, backupNames[0], orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupNames[0])
			UpdateBackup(backupNames[0], backupUID, orgID, cred1Name, cloudCred1UID, ctx)

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(users[0], "Password1")
			log.FailOnError(err, "Fetching px-central-admin ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			CreateSourceAndDestClusters(orgID, "", "", ctxNonAdmin)

			// Start Restore
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupNames[0], make(map[string]string), destinationClusterName, orgID, ctxNonAdmin)
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupNames[0], restoreName)

			// Restore validation to make sure that the user with Full Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupNames[0], restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			DeleteRestore(restoreName, orgID, ctxNonAdmin)

		})
		Step("Share backup with user restore mode and validate", func() {
			log.InfoD("Share backup with user restore mode and validate")
			log.Infof("Sharing backup with user - %s", users[1])

			// Get Admin Context - needed to share backup and get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			// Share backup with the user
			err = ShareBackup(backupNames[0], nil, []string{users[1]}, FullAccess, ctx)
			log.FailOnError(err, "Failed to share backup %s", backupNames[0])

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(users[1], "Password1")
			log.FailOnError(err, "Fetching px-central-admin ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			CreateSourceAndDestClusters(orgID, "", "", ctxNonAdmin)

			// Get Backup UID
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, backupNames[0], orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupNames[0])

			//update the backup with another cred
			log.InfoD("updtae the backup with another cred")
			UpdateBackup(backupNames[0], backupUID, orgID, credName, cloudCredUID, ctxNonAdmin)

			// Start Restore
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupNames[0], make(map[string]string), destinationClusterName, orgID, ctxNonAdmin)
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupNames[0], restoreName)

			// Restore validation to make sure that the user with Full Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupNames[0], restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			DeleteRestore(restoreName, orgID, ctxNonAdmin)

			// Delete backup to confirm that the user has Full Access
			backupDeleteResponse, err := DeleteBackup(backupNames[0], backupUID, orgID, ctxNonAdmin)
			log.FailOnError(err, "Backup [%s] could not be deleted by user [%s]", backupNames[0], users[1])
			dash.VerifyFatal(backupDeleteResponse.String(), "", "Verifying backup deletion is successful")
		})
	})
	JustAfterEach(func() {
		log.InfoD("Deleting the deployed apps after the testcase")
		for i := 0; i < len(contexts); i++ {
			opts := make(map[string]bool)
			opts[SkipClusterScopedObjects] = true
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			err := Inst().S.Destroy(contexts[i], opts)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verify destroying app %s, Err: %v", taskName, err))
		}
		var wg sync.WaitGroup
		defer EndTorpedoTest()
		log.Infof("Cleaning up users")
		for _, userName := range users {
			wg.Add(1)
			go func(userName string) {
				defer wg.Done()
				err := backup.DeleteUser(userName)
				log.FailOnError(err, "Error deleting user %v", userName)
			}(userName)
		}
		wg.Wait()

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")

		log.Infof("Deleting registered clusters for admin context")
		DeleteCluster(SourceClusterName, orgID, ctx)
		DeleteCluster(destinationClusterName, orgID, ctx)

		log.Infof("Deleting registered clusters for non-admin context")
		for _, ctxNonAdmin := range userContexts {
			DeleteCluster(SourceClusterName, orgID, ctxNonAdmin)
			DeleteCluster(destinationClusterName, orgID, ctxNonAdmin)
		}

		log.Infof("Cleaning up backup location - %s", BackupLocationName)
		DeleteBackupLocation(BackupLocationName, backupLocationUID, orgID)

		log.Infof("Cleaning cloud credential")
		//TODO: Eliminate time.Sleep
		time.Sleep(time.Minute * 3)
		DeleteCloudCredential(credName, orgID, cloudCredUID)
		DeleteCloudCredential(cred1Name, orgID, cloudCred1UID)
	})
})

// This testcase verifies alternating backups between locked and unlocked bucket
var _ = Describe("{BackupAlternatingBetweenLockedAndUnlockedBuckets}", func() {
	var (
		appList = Inst().AppList
	)
	var preRuleNameList []string
	var postRuleNameList []string
	var contexts []*scheduler.Context
	labelSelectors := make(map[string]string)
	CloudCredUIDMap := make(map[string]string)
	BackupLocationMap := make(map[string]string)
	var backupList []string
	var appContexts []*scheduler.Context
	var backupLocation string
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	bkpNamespaces = make([]string, 0)
	providers := getProviders()
	JustBeforeEach(func() {
		StartTorpedoTest("BackupAlternatingBetweenLockedAndUnlockedBucket", "Deploying backup", nil, 0)
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
	It("Backup alternating between locked and unlocked buckets", func() {
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
			for _, provider := range providers {
				CredName := fmt.Sprintf("%s-%s", "cred", provider)
				CloudCredUID = uuid.New()
				CloudCredUIDMap[CloudCredUID] = CredName
				CreateCloudCredential(provider, CredName, CloudCredUID, orgID)
			}
		})

		Step("Creating a locked bucket and backup location", func() {
			log.InfoD("Creating locked buckets and backup location")
			bucketNames := getBucketName()
			modes := [2]string{"GOVERNANCE", "COMPLIANCE"}
			for _, provider := range providers {
				for _, mode := range modes {
					CredName := fmt.Sprintf("%s-%s", "cred", provider)
					bucketName := fmt.Sprintf("%s-%s-%s-locked", provider, bucketNames[1], strings.ToLower(mode))
					backupLocation = fmt.Sprintf("%s-%s-%s-lock", provider, bucketNames[1], strings.ToLower(mode))
					err := CreateS3Bucket(bucketName, true, 3, mode)
					log.FailOnError(err, "Unable to create locked s3 bucket %s", bucketName)
					BackupLocationUID = uuid.New()
					BackupLocationMap[BackupLocationUID] = backupLocation
					CreateBackupLocation(provider, backupLocation, BackupLocationUID, CredName, CloudCredUID,
						bucketName, orgID, "")
				}
			}
			log.InfoD("Successfully created locked buckets and backup location")
		})

		Step("Creating backup location for unlocked bucket", func() {
			log.InfoD("Creating backup location for unlocked bucket")
			bucketNames := getBucketName()
			for _, provider := range providers {
				CredName := fmt.Sprintf("%s-%s", "cred", provider)
				bucketName := fmt.Sprintf("%s-%s", provider, bucketNames[0])
				backupLocation = fmt.Sprintf("%s-%s-unlockedbucket", provider, bucketNames[0])
				BackupLocationUID = uuid.New()
				BackupLocationMap[BackupLocationUID] = backupLocation
				CreateBackupLocation(provider, backupLocation, BackupLocationUID, CredName, CloudCredUID,
					bucketName, orgID, "")
			}
		})

		Step("Register cluster for backup", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			CreateSourceAndDestClusters(orgID, "", "", ctx)
			clusterStatus, clusterUid = Inst().Backup.RegisterBackupCluster(orgID, SourceClusterName, "")
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, "Verifying backup cluster")
		})

		Step("Taking backup of application to locked and unlocked bucket", func() {
			for _, namespace := range bkpNamespaces {
				for backupLocationUID, backupLocationName := range BackupLocationMap {
					ctx, err := backup.GetAdminCtxFromSecret()
					dash.VerifyFatal(err, nil, "Getting context")
					preRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, preRuleNameList[0])
					postRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, postRuleNameList[0])
					backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, namespace, backupLocationName)
					backupList = append(backupList, backupName)
					err = CreateBackup(backupName, SourceClusterName, backupLocationName, backupLocationUID, []string{namespace},
						labelSelectors, orgID, clusterUid, preRuleNameList[0], preRuleUid, postRuleNameList[0], postRuleUid, ctx)
					dash.VerifyFatal(err, nil, "Verifying backup creation")
				}
			}
		})
		Step("Restoring the backups application", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for range bkpNamespaces {
				for _, backupName := range backupList {
					err = CreateRestore(fmt.Sprintf("%s-restore", backupName), backupName, nil, SourceClusterName, orgID, ctx)
					dash.VerifyFatal(err, nil, "Restore failed")
				}
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Deleting the deployed apps after the testcase")
		for i := 0; i < len(contexts); i++ {
			opts := make(map[string]bool)
			opts[SkipClusterScopedObjects] = true
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			err := Inst().S.Destroy(contexts[i], opts)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verify destroying app %s, Err: %v", taskName, err))
		}

		log.InfoD("Deleting backup location and cloud setting")
		for backupLocationUID, backupLocationName := range BackupLocationMap {
			DeleteBackupLocation(backupLocationName, backupLocationUID, orgID)
		}
		// Need sleep as it takes some time for
		time.Sleep(time.Minute * 1)
		for CloudCredUID, CredName := range CloudCredUIDMap {
			DeleteCloudCredential(CredName, orgID, CloudCredUID)
		}
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")

		log.Infof("Deleting registered clusters for admin context")
		DeleteCluster(SourceClusterName, orgID, ctx)
		DeleteCluster(destinationClusterName, orgID, ctx)
	})
})

// https://portworx.atlassian.net/browse/PB-3486
// UI testing is need to validate that user with FullAccess cannot duplicate the backup shared
var _ = Describe("{ShareBackupsAndClusterWithUser}", func() {
	var (
		contexts          []*scheduler.Context
		appContexts       []*scheduler.Context
		bkpNamespaces     []string
		clusterUid        string
		clusterStatus     api.ClusterInfo_StatusInfo_Status
		userName          string
		backupName        string
		backupLocationUID string
		cloudCredName     string
		cloudCredUID      string
		bkpLocationName   string
		userBackupName    string
		ctxNonAdmin       context.Context
	)
	backupLocationMap := make(map[string]string)
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	JustBeforeEach(func() {
		StartTorpedoTest("ShareBackupsAndClusterWithUser",
			"Share backup to user with full access and try to duplicate the backup from the shared user", nil, 82943)
		log.InfoD("Deploy applications need fot taking backup")
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
	It("Share Backup With Full Access Users and try to duplicate the backup", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		dash.VerifyFatal(err, nil, "Getting context")
		Step("Validate applications", func() {
			log.InfoD("Validate applications ")
			ValidateApplications(contexts)
		})
		Step("Create Users", func() {
			log.InfoD("Creating users testuser with FullAccess")
			userName = "testuser"
			firstName := "FirstName"
			lastName := "LastName"
			email := "testuser@cnbu.com"
			err := backup.AddUser(userName, firstName, lastName, email, "Password1")
			log.FailOnError(err, "Failed to create user - %s", userName)
		})
		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating bucket,backup location and cloud setting")
			bucketNames := getBucketName()
			providers := getProviders()
			for _, provider := range providers {
				bucketName := fmt.Sprintf("%s-%s", provider, bucketNames[0])
				cloudCredName = fmt.Sprintf("%s-%s", "cloudcred", provider)
				bkpLocationName = fmt.Sprintf("%s-%s-bl", provider, bucketNames[0])
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID)
				// TODO: Remove time.Sleep: PA-509
				time.Sleep(time.Minute * 4)
				CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, bucketName, orgID, "")
			}
		})
		Step("Register cluster for backup", func() {
			CreateSourceAndDestClusters(orgID, "", "", ctx)
			clusterStatus, clusterUid = Inst().Backup.RegisterBackupCluster(orgID, SourceClusterName, "")
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, "Verifying backup cluster")
		})
		Step("Taking backup of applications", func() {
			backupName = fmt.Sprintf("%s-%s", BackupNamePrefix, bkpNamespaces[0])
			err = CreateBackup(backupName, SourceClusterName, bkpLocationName, backupLocationUID, []string{bkpNamespaces[0]},
				labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
			dash.VerifyFatal(err, nil, "Verifying backup creation")
		})
		Step("Share backup with  user having full access", func() {
			log.InfoD("Share backup with  user having full access")
			ShareBackup(backupName, nil, []string{userName}, FullAccess, ctx)
		})
		Step("Create  backup from the shared user with FullAccess", func() {
			log.InfoD("Validating if user with  FullAccess cannot duplicate backup shared but can create new backup")
			// User with FullAccess cannot duplicate will be validated through UI only
			ctxNonAdmin, err = backup.GetNonAdminCtx(userName, "Password1")
			log.FailOnError(err, "Fetching user ctx")
			log.InfoD("Registering Source and Destination clusters from user context")
			CreateSourceAndDestClusters(orgID, "", "", ctxNonAdmin)
			userBackupName = fmt.Sprintf("%s-%s-%s", "user", BackupNamePrefix, bkpNamespaces[0])
			err = CreateBackup(userBackupName, SourceClusterName, bkpLocationName, backupLocationUID, []string{bkpNamespaces[0]},
				labelSelectors, orgID, clusterUid, "", "", "", "", ctxNonAdmin)
			dash.VerifyFatal(err, nil, "Verifying that create backup should pass ")
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)
		log.Infof("Deleting backup created by px-central-admin")
		backupDriver := Inst().Backup
		backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
		dash.VerifySafely(err, nil, "Getting backup UID")
		DeleteBackup(backupName, backupUID, orgID, ctx)
		// TODO: Remove time.Sleep: PA-509
		time.Sleep(time.Minute * 3)
		log.Infof("Deleting backup created by user")
		userBackupUID, err := backupDriver.GetBackupUID(ctxNonAdmin, userBackupName, orgID)
		dash.VerifySafely(err, nil, "Getting backup UID of user")
		DeleteBackup(userBackupName, userBackupUID, orgID, ctxNonAdmin)
		// TODO: Remove time.Sleep
		time.Sleep(time.Minute * 3)
		log.Infof("Cleaning up users")
		err = backup.DeleteUser(userName)
		log.FailOnError(err, "Error in deleting user")
		log.Infof("Deleting registered clusters for non-admin context")
		DeleteCluster(SourceClusterName, orgID, ctxNonAdmin)
		DeleteCluster(destinationClusterName, orgID, ctxNonAdmin)
		DeleteCloudAccounts(backupLocationMap, cloudCredName, cloudCredUID)
	})
})

// This test performs basic test of starting an application, backing it up and killing stork while
// performing backup.
var _ = Describe("{BackupCreateKillStorkRestore}", func() {
	var (
		contexts         []*scheduler.Context
		bkpNamespaces    []string
		namespaceMapping map[string]string
		taskNamePrefix   = "backupcreaterestore"
	)
	labelSelectores := make(map[string]string)
	namespaceMapping = make(map[string]string)
	volumeParams := make(map[string]map[string]string)

	BeforeEach(func() {
		StartTorpedoTest("BackupCreateKillStorkRestore", "Validate Backup Create and Kill Stork and Restore", nil, 0)
		wantAllAfterSuiteActions = false
	})

	It("has to connect and check the backup setup", func() {
		Step("Setup backup", func() {
			// Set cluster context to cluster where torpedo is running
			SetClusterContext("")
			SetupBackup(taskNamePrefix)
		})

		sourceClusterConfigPath, err := GetSourceClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))

		SetClusterContext(sourceClusterConfigPath)

		Step("Deploy applications", func() {
			contexts = make([]*scheduler.Context, 0)
			bkpNamespaces = make([]string, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
				log.Infof("Task name %s\n", taskName)
				appContexts := ScheduleApplications(taskName)
				contexts = append(contexts, appContexts...)
				for _, ctx := range appContexts {
					// Override default App readiness time out of 5 mins with 10 mins
					ctx.ReadinessTimeout = appReadinessTimeout
					namespace := GetAppNamespace(ctx, taskName)
					bkpNamespaces = append(bkpNamespaces, namespace)
				}
			}

			// Skip volume validation until other volume providers are implemented.
			for _, ctx := range contexts {
				ctx.SkipVolumeValidation = true
			}

			ValidateApplications(contexts)
			for _, ctx := range contexts {
				for vol, params := range GetVolumeParameters(ctx) {
					volumeParams[vol] = params
				}
			}
		})

		log.Info("Wait for IO to proceed\n")
		time.Sleep(time.Minute * 5)

		// TODO(stgleb): Add multi-namespace backup when ready in px-backup
		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
			Step(fmt.Sprintf("Create backup full name %s:%s:%s",
				SourceClusterName, namespace, backupName), func() {
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				err = CreateBackup(backupName,
					SourceClusterName, backupLocationName, BackupLocationUID,
					[]string{namespace}, labelSelectores, orgID, "", "", "", "", "", ctx)
				dash.VerifyFatal(err, nil, "Verifying backup creation")
			})
		}

		Step("Kill stork during backup", func() {
			// setup task to delete stork pods as soon as it starts doing backup
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				backupUID := getBackupUID(backupName, orgID)
				req := &api.BackupInspectRequest{
					Name:  backupName,
					OrgId: orgID,
					Uid:   backupUID,
				}

				log.Infof("backup %s wait for running", backupName)
				err := Inst().Backup.WaitForBackupRunning(context.Background(),
					req, BackupRestoreCompletionTimeoutMin*time.Minute,
					RetrySeconds*time.Second)

				if err != nil {
					log.Warnf("backup %s wait for running err %v",
						backupName, err)
					continue
				} else {
					break
				}
			}

			ctx := &scheduler.Context{
				App: &spec.AppSpec{
					SpecList: []interface{}{
						&appsapi.Deployment{
							ObjectMeta: meta_v1.ObjectMeta{
								Name:      storkDeploymentName,
								Namespace: storkDeploymentNamespace,
							},
						},
					},
				},
			}
			log.Infof("Execute task for killing stork")
			err := Inst().S.DeleteTasks(ctx, nil)
			Expect(err).NotTo(HaveOccurred())
		})

		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
			Step(fmt.Sprintf("Wait for backup %s to complete", backupName), func() {
				err := Inst().Backup.WaitForBackupCompletion(
					context.Background(),
					backupName, orgID,
					BackupRestoreCompletionTimeoutMin*time.Minute,
					RetrySeconds*time.Second)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to wait for backup [%s] to complete. Error: [%v]",
						backupName, err))
			})
		}

		Step("teardown all applications on source cluster before switching context to destination cluster", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, map[string]bool{
					SkipClusterScopedObjects:                    true,
					scheduler.OptionsWaitForResourceLeakCleanup: true,
					scheduler.OptionsWaitForDestroy:             true,
				})
			}
		})

		destClusterConfigPath, err := GetDestinationClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

		SetClusterContext(destClusterConfigPath)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
			restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, namespace)
			Step(fmt.Sprintf("Create restore %s:%s:%s from backup %s:%s:%s",
				destinationClusterName, namespace, restoreName,
				SourceClusterName, namespace, backupName), func() {
				CreateRestore(restoreName, backupName, namespaceMapping,
					destinationClusterName, orgID, ctx)
			})
		}

		for _, namespace := range bkpNamespaces {
			restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, namespace)
			Step(fmt.Sprintf("Wait for restore %s:%s to complete",
				namespace, restoreName), func() {

				err := Inst().Backup.WaitForRestoreCompletion(context.Background(), restoreName, orgID,
					BackupRestoreCompletionTimeoutMin*time.Minute,
					RetrySeconds*time.Second)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to wait for restore [%s] to complete. Error: [%v]",
						restoreName, err))
			})
		}

		// Change namespaces to restored apps only after backed up apps are cleaned up
		// to avoid switching back namespaces to backup namespaces
		Step("Validate Restored applications", func() {
			destClusterConfigPath, err := GetDestinationClusterConfigPath()
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

			SetClusterContext(destClusterConfigPath)

			// Populate contexts
			for _, ctx := range contexts {
				ctx.SkipClusterScopedObject = true
				ctx.SkipVolumeValidation = true
			}
			ValidateRestoredApplications(contexts, volumeParams)
		})

		Step("teardown all restored apps", func() {
			for _, ctx := range contexts {
				TearDownContext(ctx, nil)
			}
		})

		Step("teardown backup objects", func() {
			TearDownBackupRestore(bkpNamespaces, bkpNamespaces)
		})
	})
	AfterEach(func() {
		EndTorpedoTest()
	})
})

// This performs scale test of px-backup and kills stork in the middle of
// backup process.
var _ = Describe("{MultiProviderBackupKillStork}", func() {
	var (
		kubeconfigs    string
		kubeconfigList []string
	)

	contexts := make(map[string][]*scheduler.Context)
	bkpNamespaces := make(map[string][]string)
	labelSelectores := make(map[string]string)
	namespaceMapping := make(map[string]string)
	taskNamePrefix := "backup-multi-provider"
	providerUID := make(map[string]string)

	BeforeEach(func() {
		wantAllAfterSuiteActions = false
		StartTorpedoTest("MultiProviderBackupKillStork", "Performs scale test of px-backup and kills stork in the middle", nil, 0)
	})

	It("has to connect and check the backup setup", func() {
		providers := getProviders()

		Step("Setup backup", func() {
			kubeconfigs = os.Getenv("KUBECONFIGS")

			if len(kubeconfigs) == 0 {
				Expect(kubeconfigs).NotTo(BeEmpty(),
					fmt.Sprintf("KUBECONFIGS %s must not be empty", kubeconfigs))
			}

			kubeconfigList = strings.Split(kubeconfigs, ",")
			// Validate user has provided at least 1 kubeconfig for cluster
			if len(kubeconfigList) == 0 {
				Expect(kubeconfigList).NotTo(BeEmpty(),
					fmt.Sprintf("kubeconfigList %v must have at least one", kubeconfigList))
			}

			// Set cluster context to cluster where torpedo is running
			SetClusterContext("")
			DumpKubeconfigs(kubeconfigList)

			for _, provider := range providers {
				log.Infof("Run Setup backup with object store provider: %s", provider)
				orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
					provider, Inst().InstanceID)
				bucketName = fmt.Sprintf("%s-%s-%s", BucketNamePrefix, provider, Inst().InstanceID)
				CredName := fmt.Sprintf("%s-%s", CredName, provider)
				CloudCredUID = uuid.New()
				backupLocation = fmt.Sprintf("%s-%s", backupLocationName, provider)
				providerUID[provider] = uuid.New()

				CreateBucket(provider, bucketName)
				CreateOrganization(orgID)
				CreateCloudCredential(provider, CredName, CloudCredUID, orgID)
				CreateBackupLocation(provider, backupLocation, providerUID[provider], CredName, CloudCredUID, BucketName, orgID, "")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				CreateProviderClusterObject(provider, kubeconfigList, CredName, orgID, ctx)
			}
		})

		// Moment in time when tests should finish
		end := time.Now().Add(time.Duration(Inst().MinRunTimeMins) * time.Minute)

		for time.Now().Before(end) {
			Step("Deploy applications", func() {
				for _, provider := range providers {
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					log.Infof("Set context to %s", providerClusterConfigPath)
					SetClusterContext(providerClusterConfigPath)

					providerContexts := make([]*scheduler.Context, 0)
					providerNamespaces := make([]string, 0)

					// Rescan specs for each provider to reload provider specific specs
					log.Infof("Rescan specs for provider %s", provider)
					err = Inst().S.RescanSpecs(Inst().SpecDir, provider)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to rescan specs from %s for storage provider %s. Error: [%v]",
							Inst().SpecDir, provider, err))

					log.Infof("Start deploy applications for provider %s", provider)
					for i := 0; i < Inst().GlobalScaleFactor; i++ {
						taskName := fmt.Sprintf("%s-%s-%d", taskNamePrefix, provider, i)
						log.Infof("Task name %s\n", taskName)
						appContexts := ScheduleApplications(taskName)
						providerContexts = append(providerContexts, appContexts...)

						for _, ctx := range appContexts {
							namespace := GetAppNamespace(ctx, taskName)
							providerNamespaces = append(providerNamespaces, namespace)
						}
					}

					contexts[provider] = providerContexts
					bkpNamespaces[provider] = providerNamespaces
				}
			})

			Step("Validate applications", func() {
				for _, provider := range providers {
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					SetClusterContext(providerClusterConfigPath)

					// In case of non-portworx volume provider skip volume validation until
					// other volume providers are implemented.
					for _, ctx := range contexts[provider] {
						ctx.SkipVolumeValidation = true
						ctx.ReadinessTimeout = BackupRestoreCompletionTimeoutMin * time.Minute
					}

					log.Infof("validate applications for provider %s", provider)
					ValidateApplications(contexts[provider])
				}
			})

			log.Info("Wait for IO to proceed\n")
			time.Sleep(time.Minute * 5)

			// Perform all backup operations concurrently
			// TODO(stgleb): Add multi-namespace backup when ready in px-backup
			for _, provider := range providers {
				providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
				SetClusterContext(providerClusterConfigPath)

				ctx, _ := context.WithTimeout(context.Background(),
					BackupRestoreCompletionTimeoutMin*time.Minute)
				errChan := make(chan error)
				for _, namespace := range bkpNamespaces[provider] {
					go func(provider, namespace string) {
						clusterName := fmt.Sprintf("%s-%s", clusterName, provider)
						backupLocation := fmt.Sprintf("%s-%s", backupLocationName, provider)
						backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, provider,
							namespace)
						orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
							provider, Inst().InstanceID)
						// NOTE: We don't use CreateBackup/Restore method here since it has ginkgo assertion
						// which must be called inside of goroutine with GinkgoRecover https://onsi.github.io/ginkgo/#marking-specs-as-failed
						Step(fmt.Sprintf("Create backup full name %s:%s:%s in organization %s",
							clusterName, namespace, backupName, orgID), func() {
							backupDriver := Inst().Backup
							bkpCreateRequest := &api.BackupCreateRequest{
								CreateMetadata: &api.CreateMetadata{
									Name:  backupName,
									OrgId: orgID,
								},
								BackupLocation: backupLocation,
								Cluster:        clusterName,
								Namespaces:     []string{namespace},
								LabelSelectors: labelSelectores,
							}
							//ctx, err := backup.GetPxCentralAdminCtx()
							ctx, err := backup.GetAdminCtxFromSecret()
							Expect(err).NotTo(HaveOccurred(),
								fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
									err))
							_, err = backupDriver.CreateBackup(ctx, bkpCreateRequest)
							errChan <- err
						})
					}(provider, namespace)
				}

				for i := 0; i < len(bkpNamespaces[provider]); i++ {
					select {
					case <-ctx.Done():
						Expect(ctx.Err()).NotTo(HaveOccurred(),
							fmt.Sprintf("Failed to complete backup for provider %s cluster. Error: [%v]", provider, ctx.Err()))
					case err := <-errChan:
						Expect(err).NotTo(HaveOccurred(),
							fmt.Sprintf("Failed to complete backup for provider %s cluster. Error: [%v]", provider, err))
					}
				}
			}

			Step("Kill stork during backup", func() {
				for provider, providerNamespaces := range bkpNamespaces {
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					SetClusterContext(providerClusterConfigPath)

					log.Infof("Kill stork during backup for provider %s", provider)
					// setup task to delete stork pods as soon as it starts doing backup
					for _, namespace := range providerNamespaces {
						backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, provider, namespace)
						orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
							provider, Inst().InstanceID)

						// Wait until all backups/restores start running
						backupUID := getBackupUID(backupName, orgID)
						req := &api.BackupInspectRequest{
							Name:  backupName,
							OrgId: orgID,
							Uid:   backupUID,
						}

						log.Infof("backup %s wait for running", backupName)
						err := Inst().Backup.WaitForBackupRunning(context.Background(),
							req, BackupRestoreCompletionTimeoutMin*time.Minute,
							RetrySeconds*time.Second)

						Expect(err).NotTo(HaveOccurred())
					}
					killStork()
				}
			})

			// wait until all backups are completed, there is no need to parallel here
			for provider, namespaces := range bkpNamespaces {
				providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
				SetClusterContext(providerClusterConfigPath)

				for _, namespace := range namespaces {
					backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, provider, namespace)
					orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
						provider, Inst().InstanceID)
					Step(fmt.Sprintf("Wait for backup %s to complete in organization %s",
						backupName, orgID), func() {
						err := Inst().Backup.WaitForBackupCompletion(
							context.Background(),
							backupName, orgID,
							BackupRestoreCompletionTimeoutMin*time.Minute,
							RetrySeconds*time.Second)
						Expect(err).NotTo(HaveOccurred(),
							fmt.Sprintf("Failed to wait for backup [%s] to complete. Error: [%v]",
								backupName, err))
					})
				}
			}

			Step("teardown all applications on source cluster before switching context to destination cluster", func() {
				for _, provider := range providers {
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					log.Infof("Set config to %s", providerClusterConfigPath)
					SetClusterContext(providerClusterConfigPath)

					for _, ctx := range contexts[provider] {
						TearDownContext(ctx, map[string]bool{
							SkipClusterScopedObjects:                    true,
							scheduler.OptionsWaitForResourceLeakCleanup: true,
							scheduler.OptionsWaitForDestroy:             true,
						})
					}
				}
			})

			for provider := range bkpNamespaces {
				providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
				SetClusterContext(providerClusterConfigPath)

				ctx, _ := context.WithTimeout(context.Background(),
					BackupRestoreCompletionTimeoutMin*time.Minute)
				errChan := make(chan error)
				for _, namespace := range bkpNamespaces[provider] {
					go func(provider, namespace string) {
						clusterName := fmt.Sprintf("%s-%s", clusterName, provider)
						backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, provider, namespace)
						restoreName := fmt.Sprintf("%s-%s-%s", restoreNamePrefix, provider, namespace)
						orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
							provider, Inst().InstanceID)
						Step(fmt.Sprintf("Create restore full name %s:%s:%s in organization %s",
							clusterName, namespace, backupName, orgID), func() {
							// NOTE: We don't use CreateBackup/Restore method here since it has ginkgo assertion
							// which must be called inside of gorutuine with GinkgoRecover https://onsi.github.io/ginkgo/#marking-specs-as-failed
							backupDriver := Inst().Backup
							createRestoreReq := &api.RestoreCreateRequest{
								CreateMetadata: &api.CreateMetadata{
									Name:  restoreName,
									OrgId: orgID,
								},
								Backup:           backupName,
								Cluster:          clusterName,
								NamespaceMapping: namespaceMapping,
							}
							//ctx, err := backup.GetPxCentralAdminCtx()
							ctx, err := backup.GetAdminCtxFromSecret()
							Expect(err).NotTo(HaveOccurred(),
								fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
									err))
							_, err = backupDriver.CreateRestore(ctx, createRestoreReq)

							errChan <- err
						})
					}(provider, namespace)
				}

				for i := 0; i < len(bkpNamespaces[provider]); i++ {
					select {
					case <-ctx.Done():
						Expect(err).NotTo(HaveOccurred(),
							fmt.Sprintf("Failed to complete backup for provider %s cluster. Error: [%v]", provider, ctx.Err()))
					case err := <-errChan:
						Expect(err).NotTo(HaveOccurred(),
							fmt.Sprintf("Failed to complete backup for provider %s cluster. Error: [%v]", provider, err))
					}
				}
			}

			Step("Kill stork during restore", func() {
				for provider, providerNamespaces := range bkpNamespaces {
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					SetClusterContext(providerClusterConfigPath)

					log.Infof("Kill stork during restore for provider %s", provider)
					// setup task to delete stork pods as soon as it starts doing backup
					for _, namespace := range providerNamespaces {
						restoreName := fmt.Sprintf("%s-%s-%s", restoreNamePrefix, provider, namespace)
						orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
							provider, Inst().InstanceID)

						// Wait until all backups/restores start running
						req := &api.RestoreInspectRequest{
							Name:  restoreName,
							OrgId: orgID,
						}

						log.Infof("restore %s wait for running", restoreName)
						err := Inst().Backup.WaitForRestoreRunning(context.Background(),
							req, BackupRestoreCompletionTimeoutMin*time.Minute,
							RetrySeconds*time.Second)

						Expect(err).NotTo(HaveOccurred())
					}
					log.Infof("Kill stork task")
					killStork()
				}
			})

			for provider, providerNamespaces := range bkpNamespaces {
				providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
				SetClusterContext(providerClusterConfigPath)

				for _, namespace := range providerNamespaces {
					restoreName := fmt.Sprintf("%s-%s-%s", restoreNamePrefix, provider, namespace)
					orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
						provider, Inst().InstanceID)
					Step(fmt.Sprintf("Wait for restore %s:%s to complete",
						namespace, restoreName), func() {
						err := Inst().Backup.WaitForRestoreCompletion(context.Background(),
							restoreName, orgID,
							BackupRestoreCompletionTimeoutMin*time.Minute,
							RetrySeconds*time.Second)
						Expect(err).NotTo(HaveOccurred(),
							fmt.Sprintf("Failed to wait for restore [%s] to complete. Error: [%v]",
								restoreName, err))
					})
				}
			}

			// Change namespaces to restored apps only after backed up apps are cleaned up
			// to avoid switching back namespaces to backup namespaces
			Step("Validate Restored applications", func() {
				// Populate contexts
				for _, provider := range providers {
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					SetClusterContext(providerClusterConfigPath)

					for _, ctx := range contexts[provider] {
						ctx.SkipClusterScopedObject = true
						ctx.SkipVolumeValidation = true
						ctx.ReadinessTimeout = BackupRestoreCompletionTimeoutMin * time.Minute

						err := Inst().S.WaitForRunning(ctx, defaultTimeout, defaultRetryInterval)
						Expect(err).NotTo(HaveOccurred())
					}

					ValidateApplications(contexts[provider])
				}
			})

			Step("teardown all restored apps", func() {
				for _, provider := range providers {
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					SetClusterContext(providerClusterConfigPath)

					for _, ctx := range contexts[provider] {
						TearDownContext(ctx, map[string]bool{
							scheduler.OptionsWaitForResourceLeakCleanup: true,
							scheduler.OptionsWaitForDestroy:             true,
						})
					}
				}
			})

			Step("teardown backup and restore objects", func() {
				for provider, providerNamespaces := range bkpNamespaces {
					log.Infof("teardown backup and restore objects for provider %s", provider)
					providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
					Expect(err).NotTo(HaveOccurred(),
						fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
					SetClusterContext(providerClusterConfigPath)

					ctx, _ := context.WithTimeout(context.Background(),
						BackupRestoreCompletionTimeoutMin*time.Minute)
					errChan := make(chan error)

					for _, namespace := range providerNamespaces {
						go func(provider, namespace string) {
							clusterName := fmt.Sprintf("%s-%s", clusterName, provider)
							backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, provider, namespace)
							orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
								provider, Inst().InstanceID)
							Step(fmt.Sprintf("Delete backup full name %s:%s:%s",
								clusterName, namespace, backupName), func() {
								backupDriver := Inst().Backup
								backupUID := getBackupUID(backupName, orgID)
								bkpDeleteRequest := &api.BackupDeleteRequest{
									Name:  backupName,
									OrgId: orgID,
									Uid:   backupUID,
								}
								//	ctx, err = backup.GetPxCentralAdminCtx()
								ctx, err = backup.GetAdminCtxFromSecret()
								Expect(err).NotTo(HaveOccurred(),
									fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
										err))
								_, err = backupDriver.DeleteBackup(ctx, bkpDeleteRequest)

								ctx, _ := context.WithTimeout(context.Background(),
									BackupRestoreCompletionTimeoutMin*time.Minute)

								if err = backupDriver.WaitForBackupDeletion(ctx, backupName, orgID,
									BackupRestoreCompletionTimeoutMin*time.Minute,
									RetrySeconds*time.Second); err != nil {
									errChan <- err
									return
								}

								errChan <- err
							})
						}(provider, namespace)

						go func(provider, namespace string) {
							clusterName := fmt.Sprintf("%s-%s", clusterName, provider)
							restoreName := fmt.Sprintf("%s-%s-%s", restoreNamePrefix, provider, namespace)
							orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix),
								provider, Inst().InstanceID)
							Step(fmt.Sprintf("Delete restore full name %s:%s:%s",
								clusterName, namespace, restoreName), func() {
								backupDriver := Inst().Backup
								deleteRestoreReq := &api.RestoreDeleteRequest{
									OrgId: orgID,
									Name:  restoreName,
								}
								//ctx, err = backup.GetPxCentralAdminCtx()
								ctx, err = backup.GetAdminCtxFromSecret()
								Expect(err).NotTo(HaveOccurred(),
									fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
										err))
								_, err = backupDriver.DeleteRestore(ctx, deleteRestoreReq)

								ctx, _ := context.WithTimeout(context.Background(),
									BackupRestoreCompletionTimeoutMin*time.Minute)

								log.Infof("Wait for restore %s is deleted", restoreName)
								if err = backupDriver.WaitForRestoreDeletion(ctx, restoreName, orgID,
									BackupRestoreCompletionTimeoutMin*time.Minute,
									RetrySeconds*time.Second); err != nil {
									errChan <- err
									return
								}

								errChan <- err
							})
						}(provider, namespace)
					}

					for i := 0; i < len(providerNamespaces)*2; i++ {
						select {
						case <-ctx.Done():
							Expect(err).NotTo(HaveOccurred(),
								fmt.Sprintf("Failed to complete backup for provider %s cluster. Error: [%v]", provider, ctx.Err()))
						case err := <-errChan:
							Expect(err).NotTo(HaveOccurred(),
								fmt.Sprintf("Failed to complete backup for provider %s cluster. Error: [%v]", provider, err))
						}
					}
				}
			})
		}

		Step("teardown backup objects for test", func() {
			for _, provider := range providers {
				providerClusterConfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to get kubeconfig path for provider %s cluster. Error: [%v]", provider, err))
				SetClusterContext(providerClusterConfigPath)

				log.Infof("Run Setup backup with object store provider: %s", provider)
				orgID := fmt.Sprintf("%s-%s-%s", strings.ToLower(taskNamePrefix), provider, Inst().InstanceID)
				bucketName := fmt.Sprintf("%s-%s-%s", BucketNamePrefix, provider, Inst().InstanceID)
				CredName := fmt.Sprintf("%s-%s", CredName, provider)
				backupLocation := fmt.Sprintf("%s-%s", backupLocationName, provider)
				clusterName := fmt.Sprintf("%s-%s", clusterName, provider)

				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				DeleteCluster(clusterName, orgID, ctx)
				// Need to add backup location UID for Delete Backup Location call
				DeleteBackupLocation(backupLocation, "", orgID)
				DeleteCloudCredential(CredName, orgID, CloudCredUID)
				DeleteBucket(provider, bucketName)
			}
		})
	})
	AfterEach(func() {
		EndTorpedoTest()
	})
})

func killStork() {
	ctx := &scheduler.Context{
		App: &spec.AppSpec{
			SpecList: []interface{}{
				&appsapi.Deployment{
					ObjectMeta: meta_v1.ObjectMeta{
						Name:      storkDeploymentName,
						Namespace: storkDeploymentNamespace,
					},
				},
			},
		},
	}
	log.Infof("Execute task for killing stork")
	err := Inst().S.DeleteTasks(ctx, nil)
	Expect(err).NotTo(HaveOccurred())
}

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

	JustBeforeEach(func() {
		StartTorpedoTest("BackupRestartPX", "Restart PX when backup in progress", nil, 0)
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
			for _, provider := range providers {
				cloudCredName := fmt.Sprintf("%s-%s", "cred", provider)
				cloudCredUID = uuid.New()
				CloudCredUIDMap[cloudCredUID] = CredName
				CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID)
			}
		})

		Step("Register cluster for backup", func() {
			ctx, _ := backup.GetAdminCtxFromSecret()
			CreateSourceAndDestClusters(orgID, "", "", ctx)
			clusterStatus, clusterUid = Inst().Backup.RegisterBackupCluster(orgID, SourceClusterName, "")
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, "Verifying backup cluster")
		})

		Step("Creating backup location", func() {
			log.InfoD("Creating backup location")
			bucketNames := getBucketName()
			providers := getProviders()
			for _, provider := range providers {
				cloudCredName := fmt.Sprintf("%s-%s", "cred", provider)
				bucketName := fmt.Sprintf("%s-%s", provider, bucketNames[0])
				backupLocation = fmt.Sprintf("%s-%s", provider, bucketNames[0])
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocation
				CreateBackupLocation(provider, backupLocation, backupLocationUID, cloudCredName, cloudCredUID,
					bucketName, orgID, "")
			}
		})

		Step("Start backup of application to bucket", func() {
			for _, namespace := range bkpNamespaces {
				ctx, err := backup.GetAdminCtxFromSecret()
				dash.VerifyFatal(err, nil, "Getting context")
				preRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, preRuleNameList[0])
				postRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, postRuleNameList[0])
				backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, namespace, backupLocationName)
				CreateBackupWithoutCheck(backupName, SourceClusterName, backupLocation, backupLocationUID, []string{namespace},
					labelSelectors, orgID, clusterUid, preRuleNameList[0], preRuleUid, postRuleNameList[0], postRuleUid, ctx)
			}
		})

		storageNodes := node.GetWorkerNodes()
		Step(fmt.Sprintf("Restart volume driver nodes starts"), func() {
			log.InfoD("Restart PX on nodes")
			for index := range storageNodes {
				// Just restart storage driver on one of the node where volume backup is in progress
				Inst().V.RestartDriver(storageNodes[index], nil)
			}
		})

		Step("Check if backup is successful when the PX restart happened", func() {
			log.InfoD("Check if backup is successful post px restarts")
			var bkpUid string
			backupDriver := Inst().Backup
			ctx, err := backup.GetAdminCtxFromSecret()
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, namespace, backupLocationName)
				backupSuccessCheck := func() (interface{}, bool, error) {
					bkpUid, err = backupDriver.GetBackupUID(ctx, backupName, orgID)
					log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
					backupInspectRequest := &api.BackupInspectRequest{
						Name:  backupName,
						Uid:   bkpUid,
						OrgId: orgID,
					}
					resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
					log.FailOnError(err, "Inspecting the backup taken with request:\n%v", backupInspectRequest)
					actual := resp.GetBackup().GetStatus().Status
					expected := api.BackupInfo_StatusInfo_Success
					if actual != expected {
						return "", true, fmt.Errorf("backup status expected was [%s] but got [%s]", expected, actual)
					}
					return "", false, nil
				}
				task.DoRetryWithTimeout(backupSuccessCheck, 10*time.Minute, 30*time.Second)
				bkpUid, err = backupDriver.GetBackupUID(ctx, backupName, orgID)
				log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
				backupInspectRequest := &api.BackupInspectRequest{
					Name:  backupName,
					Uid:   bkpUid,
					OrgId: orgID,
				}
				resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
				log.FailOnError(err, "Inspecting the backup taken with request:\n%v", backupInspectRequest)
				dash.VerifyFatal(resp.GetBackup().GetStatus().Status, api.BackupInfo_StatusInfo_Success, "Inspecting the backup success for - "+resp.GetBackup().GetName())
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Deleting the deployed apps after the testcase")
		for i := 0; i < len(contexts); i++ {
			opts := make(map[string]bool)
			opts[SkipClusterScopedObjects] = true
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			err := Inst().S.Destroy(contexts[i], opts)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verify destroying app %s, Err: %v", taskName, err))
		}

		log.InfoD("Deleting backup location, cloud creds and clusters")
		DeleteCloudAccounts(backupLocationMap, cloudCredName, cloudCredUID)
	})

})

var _ = Describe("{BackupRestoreSimultaneous}", func() {
	var (
		contexts           []*scheduler.Context
		bkpNamespaces      []string
		namespaceMapping   map[string]string
		taskNamePrefix     = "backuprestoresimultaneous"
		successfulBackups  int
		successfulRestores int
	)

	labelSelectors := make(map[string]string)
	namespaceMapping = make(map[string]string)
	bkpNamespaceErrors := make(map[string]error)
	volumeParams := make(map[string]map[string]string)
	restoreNamespaces := make([]string, 0)

	BeforeEach(func() {
		wantAllAfterSuiteActions = false
		StartTorpedoTest("BackupRestoreSimultaneous", "Backup Restore Simultaneously", nil, 0)
	})

	It("has to perform simultaneous backups and restores", func() {
		//ctx, err := backup.GetPxCentralAdminCtx()
		ctx, err := backup.GetAdminCtxFromSecret()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
				err))

		Step("Setup backup", func() {
			// Set cluster context to cluster where torpedo is running
			SetClusterContext("")
			SetupBackup(taskNamePrefix)
		})

		sourceClusterConfigPath, err := GetSourceClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))

		SetClusterContext(sourceClusterConfigPath)

		Step("Deploy applications", func() {
			contexts = make([]*scheduler.Context, 0)
			bkpNamespaces = make([]string, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
				log.Infof("Task name %s\n", taskName)
				appContexts := ScheduleApplications(taskName)
				contexts = append(contexts, appContexts...)
				for _, ctx := range appContexts {
					// Override default App readiness time out of 5 mins with 10 mins
					ctx.ReadinessTimeout = appReadinessTimeout
					namespace := GetAppNamespace(ctx, taskName)
					bkpNamespaces = append(bkpNamespaces, namespace)
				}
			}

			// Skip volume validation until other volume providers are implemented.
			for _, ctx := range contexts {
				ctx.SkipVolumeValidation = true
			}

			ValidateApplications(contexts)
			for _, ctx := range contexts {
				for vol, params := range GetVolumeParameters(ctx) {
					volumeParams[vol] = params
				}
			}
		})

		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
			Step(fmt.Sprintf("Create backup full name %s:%s:%s",
				SourceClusterName, namespace, backupName), func() {
				err = CreateBackupGetErr(backupName,
					SourceClusterName, backupLocationName, BackupLocationUID,
					[]string{namespace}, labelSelectors, OrgID)
				if err != nil {
					bkpNamespaceErrors[namespace] = err
				}
			})
		}

		var wg sync.WaitGroup
		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
			error, ok := bkpNamespaceErrors[namespace]
			if ok {
				log.Warnf("Skipping waiting for backup %s because %s", backupName, error)
			} else {
				wg.Add(1)
				go func(wg *sync.WaitGroup, namespace, backupName string) {
					defer wg.Done()
					Step(fmt.Sprintf("Wait for backup %s to complete", backupName), func() {
						err = Inst().Backup.WaitForBackupCompletion(
							ctx,
							backupName, OrgID,
							BackupRestoreCompletionTimeoutMin*time.Minute,
							RetrySeconds*time.Second)
						if err != nil {
							bkpNamespaceErrors[namespace] = err
							log.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
								backupName, err)
						}
					})
				}(&wg, namespace, backupName)
			}
		}
		wg.Wait()

		successfulBackups = len(bkpNamespaces) - len(bkpNamespaceErrors)

		if successfulBackups == len(bkpNamespaces) {
			Step("teardown all applications on source cluster before switching context to destination cluster", func() {
				for _, ctx := range contexts {
					TearDownContext(ctx, map[string]bool{
						SkipClusterScopedObjects:                    true,
						scheduler.OptionsWaitForResourceLeakCleanup: true,
						scheduler.OptionsWaitForDestroy:             true,
					})
				}
			})
		}

		destClusterConfigPath, err := GetDestinationClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

		SetClusterContext(destClusterConfigPath)
		for _, namespace := range bkpNamespaces {
			restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, namespace)
			error, ok := bkpNamespaceErrors[namespace]
			if ok {
				log.Infof("Skipping create restore %s because %s", restoreName, error)
			} else {
				restoreNamespaces = append(restoreNamespaces, namespace)
				backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				Step(fmt.Sprintf("Create restore %s:%s:%s from backup %s:%s:%s",
					destinationClusterName, namespace, restoreName,
					SourceClusterName, namespace, backupName), func() {
					err = CreateRestoreGetErr(restoreName, backupName, namespaceMapping,
						destinationClusterName, OrgID)
					if err != nil {
						bkpNamespaceErrors[namespace] = err
					}
				})
			}
		}

		for _, namespace := range bkpNamespaces {
			restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, namespace)
			error, ok := bkpNamespaceErrors[namespace]
			if ok {
				log.Infof("Skipping waiting for restore %s because %s", restoreName, error)
			} else {
				wg.Add(1)
				go func(wg *sync.WaitGroup, namespace, restoreName string) {
					defer wg.Done()
					Step(fmt.Sprintf("Wait for restore %s:%s to complete",
						namespace, restoreName), func() {
						err = Inst().Backup.WaitForRestoreCompletion(ctx, restoreName, OrgID,
							BackupRestoreCompletionTimeoutMin*time.Minute,
							RetrySeconds*time.Second)
						if err != nil {
							bkpNamespaceErrors[namespace] = err
							log.Errorf("Failed to wait for restore [%s] to complete. Error: [%v]",
								restoreName, err)
						}
					})
				}(&wg, namespace, restoreName)
			}
		}
		wg.Wait()

		// Change namespaces to restored apps only after backed up apps are cleaned up
		// to avoid switching back namespaces to backup namespaces
		Step("Validate Restored applications", func() {
			destClusterConfigPath, err := GetDestinationClusterConfigPath()
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

			SetClusterContext(destClusterConfigPath)

			// Populate contexts
			for _, ctx := range contexts {
				ctx.SkipClusterScopedObject = true
				ctx.SkipVolumeValidation = true
			}

			ValidateRestoredApplicationsGetErr(contexts, volumeParams, bkpNamespaceErrors)
		})

		successfulRestores = len(bkpNamespaces) - len(bkpNamespaceErrors)

		if len(bkpNamespaceErrors) == 0 {
			Step("teardown all restored apps", func() {
				for _, ctx := range contexts {
					TearDownContext(ctx, nil)
				}
			})

			Step("teardown backup objects", func() {
				TearDownBackupRestore(bkpNamespaces, restoreNamespaces)
			})
		}

		Step("report statistics", func() {
			log.Infof("%d/%d backups succeeded.", successfulBackups, len(bkpNamespaces))
			log.Infof("%d/%d restores succeeded.", successfulRestores, successfulBackups)
		})

		Step("view errors", func() {
			log.Infof("There were %d errors during this test", len(bkpNamespaceErrors))

			var combinedErrors []string
			for namespace, err := range bkpNamespaceErrors {
				errString := fmt.Sprintf("%s: %s", namespace, err.Error())
				combinedErrors = append(combinedErrors, errString)
			}

			if len(combinedErrors) > 0 {
				err = fmt.Errorf(strings.Join(combinedErrors, "\n"))
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})
	AfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe("{BackupRestoreOverPeriod}", func() {
	var (
		numBackups             = 0
		successfulBackups      = 0
		successfulBackupNames  []string
		numRestores            = 0
		successfulRestores     = 0
		successfulRestoreNames []string
	)
	var (
		contexts         []*scheduler.Context //for restored apps
		bkpNamespaces    []string
		namespaceMapping map[string]string
		taskNamePrefix   = "backuprestoreperiod"
	)
	labelSelectores := make(map[string]string)
	namespaceMapping = make(map[string]string)
	volumeParams := make(map[string]map[string]string)
	namespaceContextMap := make(map[string][]*scheduler.Context)

	BeforeEach(func() {
		wantAllAfterSuiteActions = false
		StartTorpedoTest("BackupRestoreOverPeriod", "Backup and Restore Over a period of time", nil, 0)
	})

	It("has to connect and check the backup setup", func() {
		//ctx, err := backup.GetPxCentralAdminCtx()
		ctx, err := backup.GetAdminCtxFromSecret()
		log.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)

		Step("Setup backup", func() {
			// Set cluster context to cluster where torpedo is running
			SetClusterContext("")
			SetupBackup(taskNamePrefix)
		})
		sourceClusterConfigPath, err := GetSourceClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))

		SetClusterContext(sourceClusterConfigPath)
		Step("Deploy applications", func() {
			successfulBackupNames = make([]string, 0)
			successfulRestoreNames = make([]string, 0)
			contexts = make([]*scheduler.Context, 0)
			bkpNamespaces = make([]string, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
				log.Infof("Task name %s\n", taskName)
				appContexts := ScheduleApplications(taskName)
				contexts = append(contexts, appContexts...)
				for _, ctx := range appContexts {
					// Override default App readiness time out of 5 mins with 10 mins
					ctx.ReadinessTimeout = appReadinessTimeout
					namespace := GetAppNamespace(ctx, taskName)
					namespaceContextMap[namespace] = append(namespaceContextMap[namespace], ctx)
					bkpNamespaces = append(bkpNamespaces, namespace)
				}
			}

			// Skip volume validation until other volume providers are implemented.
			for _, ctx := range contexts {
				ctx.SkipVolumeValidation = true
			}

			ValidateApplications(contexts)
			for _, ctx := range contexts {
				for vol, params := range GetVolumeParameters(ctx) {
					volumeParams[vol] = params
				}
			}
		})
		log.Info("Wait for IO to proceed\n")
		time.Sleep(time.Minute * 2)

		// Moment in time when tests should finish
		end := time.Now().Add(time.Duration(5) * time.Minute)
		counter := 0
		for time.Now().Before(end) {
			counter++
			aliveBackup := make(map[string]bool)
			aliveRestore := make(map[string]bool)
			sourceClusterConfigPath, err := GetSourceClusterConfigPath()
			if err != nil {
				log.Errorf("Failed to get kubeconfig path for source cluster. Error: [%v]", err)
				continue
			}

			SetClusterContext(sourceClusterConfigPath)
			for _, namespace := range bkpNamespaces {
				numBackups++
				backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
				aliveBackup[namespace] = true
				Step(fmt.Sprintf("Create backup full name %s:%s:%s",
					SourceClusterName, namespace, backupName), func() {
					err = CreateBackupGetErr(backupName,
						SourceClusterName, backupLocationName, BackupLocationUID,
						[]string{namespace}, labelSelectores, OrgID)
					if err != nil {
						aliveBackup[namespace] = false
						log.Errorf("Failed to create backup [%s] in org [%s]. Error: [%v]", backupName, OrgID, err)
					}
				})
			}
			for _, namespace := range bkpNamespaces {
				if !aliveBackup[namespace] {
					continue
				}
				backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
				Step(fmt.Sprintf("Wait for backup %s to complete", backupName), func() {
					err = Inst().Backup.WaitForBackupCompletion(
						ctx,
						backupName, OrgID,
						BackupRestoreCompletionTimeoutMin*time.Minute,
						RetrySeconds*time.Second)
					if err == nil {
						log.Infof("Backup [%s] completed successfully", backupName)
						successfulBackups++
					} else {
						log.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
							backupName, err)
						aliveBackup[namespace] = false
					}
				})
			}

			// Set kubeconfig to destination for restore
			destClusterConfigPath, err := GetDestinationClusterConfigPath()
			Expect(err).NotTo(HaveOccurred(),
				fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

			SetClusterContext(destClusterConfigPath)
			for _, namespace := range bkpNamespaces {
				if !aliveBackup[namespace] {
					continue
				}
				backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
				numRestores++
				aliveRestore[namespace] = true
				restoreName := fmt.Sprintf("%s-%s-%d", restoreNamePrefix, namespace, counter)
				Step(fmt.Sprintf("Create restore full name %s:%s:%s",
					destinationClusterName, namespace, restoreName), func() {
					err = CreateRestoreGetErr(restoreName, backupName, namespaceMapping,
						destinationClusterName, OrgID)
					if err != nil {
						log.Errorf("Failed to create restore [%s] in org [%s] on cluster [%s]. Error: [%v]",
							restoreName, OrgID, clusterName, err)
						aliveRestore[namespace] = false
					}
				})
			}
			for _, namespace := range bkpNamespaces {
				if !aliveRestore[namespace] {
					continue
				}
				restoreName := fmt.Sprintf("%s-%s-%d", restoreNamePrefix, namespace, counter)
				Step(fmt.Sprintf("Wait for restore %s:%s to complete",
					namespace, restoreName), func() {
					err = Inst().Backup.WaitForRestoreCompletion(ctx, restoreName, OrgID,
						BackupRestoreCompletionTimeoutMin*time.Minute,
						RetrySeconds*time.Second)
					if err == nil {
						log.Infof("Restore [%s] completed successfully", restoreName)
						successfulRestores++
					} else {
						log.Errorf("Failed to wait for restore [%s] to complete. Error: [%v]",
							restoreName, err)
						aliveRestore[namespace] = false
					}
				})
			}
			for namespace, alive := range aliveBackup {
				if alive {
					backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
					successfulBackupNames = append(successfulBackupNames, backupName)
				}
			}
			remainingContexts := make([]*scheduler.Context, 0)
			for namespace, alive := range aliveRestore {
				if alive {
					restoreName := fmt.Sprintf("%s-%s-%d", restoreNamePrefix, namespace, counter)
					successfulRestoreNames = append(successfulRestoreNames, restoreName)
					for _, ctx := range namespaceContextMap[namespace] {
						remainingContexts = append(remainingContexts, ctx)
					}
				}
			}
			// Change namespaces to restored apps only after backed up apps are cleaned up
			// to avoid switching back namespaces to backup namespaces
			Step("Validate Restored applications", func() {
				destClusterConfigPath, err := GetDestinationClusterConfigPath()
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

				SetClusterContext(destClusterConfigPath)

				// Populate contexts
				for _, ctx := range remainingContexts {
					ctx.SkipClusterScopedObject = true
					ctx.SkipVolumeValidation = true
				}
				// TODO check why PX-Backup does not copy group params correctly after restore
				for _, param := range volumeParams {
					if _, ok := param["backupGroupCheckSkip"]; !ok {
						param["backupGroupCheckSkip"] = "true"
					}
				}
				ValidateRestoredApplications(remainingContexts, volumeParams)
			})
			if successfulRestores == numRestores {
				Step("teardown all restored apps", func() {
					for _, ctx := range remainingContexts {
						TearDownContext(ctx, nil)
					}
				})
			}
		}

		if successfulBackups == numBackups && successfulRestores == numRestores {
			Step("teardown applications on source cluster", func() {
				sourceClusterConfigPath, err := GetSourceClusterConfigPath()
				if err != nil {
					log.Errorf("Failed to get kubeconfig path for source cluster. Error: [%v]", err)
				} else {
					SetClusterContext(sourceClusterConfigPath)
					for _, ctx := range contexts {
						TearDownContext(ctx, map[string]bool{
							SkipClusterScopedObjects:                    true,
							scheduler.OptionsWaitForResourceLeakCleanup: true,
							scheduler.OptionsWaitForDestroy:             true,
						})
					}
				}
			})
			Step("teardown backup/restore objects", func() {
				TearDownBackupRestoreSpecific(successfulBackupNames, successfulRestoreNames)
			})
		}
		Step("report statistics", func() {
			log.Infof("%d/%d backups succeeded.", successfulBackups, numBackups)
			log.Infof("%d/%d restores succeeded.", successfulRestores, numRestores)
		})
	})
	AfterEach(func() {
		EndTorpedoTest()
	})
})

var _ = Describe("{BackupRestoreOverPeriodSimultaneous}", func() {
	var (
		numBackups             int32 = 0
		successfulBackups      int32 = 0
		successfulBackupNames  []string
		numRestores            int32 = 0
		successfulRestores     int32 = 0
		successfulRestoreNames []string
	)
	var (
		contexts         []*scheduler.Context //for restored apps
		bkpNamespaces    []string
		namespaceMapping map[string]string
		taskNamePrefix   = "backuprestoreperiodsimultaneous"
	)
	labelSelectores := make(map[string]string)
	namespaceMapping = make(map[string]string)
	volumeParams := make(map[string]map[string]string)
	namespaceContextMap := make(map[string][]*scheduler.Context)
	combinedErrors := make([]string, 0)

	BeforeEach(func() {
		wantAllAfterSuiteActions = false
		StartTorpedoTest("BackupRestoreOverPeriodSimultaneous", "BackupRestoreOverPeriodSimultaneous", nil, 0)
	})

	It("has to connect and check the backup setup", func() {
		Step("Setup backup", func() {
			// Set cluster context to cluster where torpedo is running
			SetClusterContext("")
			SetupBackup(taskNamePrefix)
		})
		sourceClusterConfigPath, err := GetSourceClusterConfigPath()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))

		SetClusterContext(sourceClusterConfigPath)
		Step("Deploy applications", func() {
			successfulBackupNames = make([]string, 0)
			successfulRestoreNames = make([]string, 0)
			contexts = make([]*scheduler.Context, 0)
			bkpNamespaces = make([]string, 0)
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
				log.Infof("Task name %s\n", taskName)
				appContexts := ScheduleApplications(taskName)
				contexts = append(contexts, appContexts...)
				for _, ctx := range appContexts {
					// Override default App readiness time out of 5 mins with 10 mins
					ctx.ReadinessTimeout = appReadinessTimeout
					namespace := GetAppNamespace(ctx, taskName)
					namespaceContextMap[namespace] = append(namespaceContextMap[namespace], ctx)
					bkpNamespaces = append(bkpNamespaces, namespace)
				}
			}

			// Skip volume validation until other volume providers are implemented.
			for _, ctx := range contexts {
				ctx.SkipVolumeValidation = true
			}

			ValidateApplications(contexts)
			for _, ctx := range contexts {
				for vol, params := range GetVolumeParameters(ctx) {
					volumeParams[vol] = params
				}
			}
		})
		log.Info("Wait for IO to proceed\n")
		time.Sleep(time.Minute * 2)

		// Moment in time when tests should finish
		end := time.Now().Add(time.Duration(5) * time.Minute)
		counter := 0
		for time.Now().Before(end) {
			counter++
			bkpNamespaceErrors := make(map[string]error)
			sourceClusterConfigPath, err := GetSourceClusterConfigPath()
			if err != nil {
				log.Errorf("Failed to get kubeconfig path for source cluster. Error: [%v]", err)
				continue
			}
			/*Expect(err).NotTo(HaveOccurred(),
			  fmt.Sprintf("Failed to get kubeconfig path for source cluster. Error: [%v]", err))*/
			SetClusterContext(sourceClusterConfigPath)
			for _, namespace := range bkpNamespaces {
				go func(namespace string) {
					atomic.AddInt32(&numBackups, 1)
					backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
					Step(fmt.Sprintf("Create backup full name %s:%s:%s",
						SourceClusterName, namespace, backupName), func() {
						err = CreateBackupGetErr(backupName,
							SourceClusterName, backupLocationName, BackupLocationUID,
							[]string{namespace}, labelSelectores, OrgID)
						if err != nil {
							//aliveBackup[namespace] = false
							bkpNamespaceErrors[namespace] = err
							log.Errorf("Failed to create backup [%s] in org [%s]. Error: [%v]", backupName, OrgID, err)
						}
					})
				}(namespace)
			}
			var wg sync.WaitGroup
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
				error, ok := bkpNamespaceErrors[namespace]
				if ok {
					log.Warnf("Skipping waiting for backup %s because %s", backupName, error)
					continue
				}
				wg.Add(1)
				go func(wg *sync.WaitGroup, namespace, backupName string) {
					defer wg.Done()
					Step(fmt.Sprintf("Wait for backup %s to complete", backupName), func() {
						//ctx, err := backup.GetPxCentralAdminCtx()
						ctx, err := backup.GetAdminCtxFromSecret()
						if err != nil {
							log.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
							bkpNamespaceErrors[namespace] = err
						} else {
							err = Inst().Backup.WaitForBackupCompletion(
								ctx,
								backupName, OrgID,
								BackupRestoreCompletionTimeoutMin*time.Minute,
								RetrySeconds*time.Second)
							if err == nil {
								log.Infof("Backup [%s] completed successfully", backupName)
								atomic.AddInt32(&successfulBackups, 1)
							} else {
								log.Errorf("Failed to wait for backup [%s] to complete. Error: [%v]",
									backupName, err)
								bkpNamespaceErrors[namespace] = err
							}
						}
					})
				}(&wg, namespace, backupName)
			}
			wg.Wait()
			for _, namespace := range bkpNamespaces {
				_, ok := bkpNamespaceErrors[namespace]
				if !ok {
					backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
					successfulBackupNames = append(successfulBackupNames, backupName)
				}
			}
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s-%d", BackupNamePrefix, namespace, counter)
				restoreName := fmt.Sprintf("%s-%s-%d", restoreNamePrefix, namespace, counter)
				error, ok := bkpNamespaceErrors[namespace]
				if ok {
					log.Infof("Skipping create restore %s because %s", restoreName, error)
					continue
				}
				go func(namespace string) {
					atomic.AddInt32(&numRestores, 1)
					Step(fmt.Sprintf("Create restore full name %s:%s:%s",
						destinationClusterName, namespace, restoreName), func() {
						err = CreateRestoreGetErr(restoreName, backupName, namespaceMapping,
							destinationClusterName, OrgID)
						if err != nil {
							log.Errorf("Failed to create restore [%s] in org [%s] on cluster [%s]. Error: [%v]",
								restoreName, OrgID, clusterName, err)
							bkpNamespaceErrors[namespace] = err
						}
					})
				}(namespace)
			}
			for _, namespace := range bkpNamespaces {
				restoreName := fmt.Sprintf("%s-%s-%d", restoreNamePrefix, namespace, counter)
				error, ok := bkpNamespaceErrors[namespace]
				if ok {
					log.Infof("Skipping waiting for restore %s because %s", restoreName, error)
					continue
				}
				wg.Add(1)
				go func(wg *sync.WaitGroup, namespace, restoreName string) {
					defer wg.Done()
					Step(fmt.Sprintf("Wait for restore %s:%s to complete",
						namespace, restoreName), func() {
						//ctx, err := backup.GetPxCentralAdminCtx()
						ctx, err := backup.GetAdminCtxFromSecret()
						if err != nil {
							log.Errorf("Failed to fetch px-central-admin ctx: [%v]", err)
							bkpNamespaceErrors[namespace] = err
						} else {
							err = Inst().Backup.WaitForRestoreCompletion(ctx, restoreName, OrgID,
								BackupRestoreCompletionTimeoutMin*time.Minute,
								RetrySeconds*time.Second)
							if err == nil {
								log.Infof("Restore [%s] completed successfully", restoreName)
								atomic.AddInt32(&successfulRestores, 1)
							} else {
								log.Errorf("Failed to wait for restore [%s] to complete. Error: [%v]",
									restoreName, err)
								bkpNamespaceErrors[namespace] = err
							}
						}
					})
				}(&wg, namespace, restoreName)
			}
			wg.Wait()
			remainingContexts := make([]*scheduler.Context, 0)
			for _, namespace := range bkpNamespaces {
				_, ok := bkpNamespaceErrors[namespace]
				if !ok {
					restoreName := fmt.Sprintf("%s-%s-%d", restoreNamePrefix, namespace, counter)
					successfulRestoreNames = append(successfulRestoreNames, restoreName)
					for _, ctx := range namespaceContextMap[namespace] {
						remainingContexts = append(remainingContexts, ctx)
					}
				}
			}
			// Change namespaces to restored apps only after backed up apps are cleaned up
			// to avoid switching back namespaces to backup namespaces
			Step("Validate Restored applications", func() {
				destClusterConfigPath, err := GetDestinationClusterConfigPath()
				Expect(err).NotTo(HaveOccurred(),
					fmt.Sprintf("Failed to get kubeconfig path for destination cluster. Error: [%v]", err))

				SetClusterContext(destClusterConfigPath)

				// Populate contexts
				for _, ctx := range remainingContexts {
					ctx.SkipClusterScopedObject = true
					ctx.SkipVolumeValidation = true
				}
				ValidateRestoredApplicationsGetErr(remainingContexts, volumeParams, bkpNamespaceErrors)
			})
			Step("teardown all restored apps", func() {
				for _, ctx := range remainingContexts {
					TearDownContext(ctx, nil)
				}
			})
			for namespace, err := range bkpNamespaceErrors {
				errString := fmt.Sprintf("%s:%d - %s", namespace, counter, err.Error())
				combinedErrors = append(combinedErrors, errString)
			}
		}
		Step("teardown applications on source cluster", func() {
			sourceClusterConfigPath, err := GetSourceClusterConfigPath()
			if err != nil {
				log.Errorf("Failed to get kubeconfig path for source cluster. Error: [%v]", err)
			} else {
				SetClusterContext(sourceClusterConfigPath)
				for _, ctx := range contexts {
					TearDownContext(ctx, map[string]bool{
						SkipClusterScopedObjects:                    true,
						scheduler.OptionsWaitForResourceLeakCleanup: true,
						scheduler.OptionsWaitForDestroy:             true,
					})
				}
			}
		})
		Step("teardown backup/restore objects", func() {
			TearDownBackupRestoreSpecific(successfulBackupNames, successfulRestoreNames)
		})
		Step("report statistics", func() {
			log.Infof("%d/%d backups succeeded.", successfulBackups, numBackups)
			log.Infof("%d/%d restores succeeded.", successfulRestores, numRestores)
		})
		Step("view errors", func() {
			log.Infof("There were %d errors during this test", len(combinedErrors))
			if len(combinedErrors) > 0 {
				err = fmt.Errorf(strings.Join(combinedErrors, "\n"))
				Expect(err).NotTo(HaveOccurred())
			}
		})
	})
	AfterEach(func() {
		EndTorpedoTest()
	})
})

// TODO: There is no delete org API
/*func DeleteOrganization(orgID string) {
	Step(fmt.Sprintf("Delete organization [%s]", orgID), func() {
		backupDriver := Inst().Backup
		req := &api.Delete{
			CreateMetadata: &api.CreateMetadata{
				Name: orgID,
			},
		}
		_, err := backupDriver.Delete(req)
		Expect(err).NotTo(HaveOccurred())
	})
}*/

func generateEncryptionKey() string {
	var lower = []byte("abcdefghijklmnopqrstuvwxyz")
	var upper = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	var number = []byte("0123456789")
	var special = []byte("~=+%^*/()[]{}/!@#$?|")
	allChar := append(lower, upper...)
	allChar = append(allChar, number...)
	allChar = append(allChar, special...)

	b := make([]byte, 12)
	// select 1 upper, 1 lower, 1 number and 1 special
	b[0] = lower[rand.Intn(len(lower))]
	b[1] = upper[rand.Intn(len(upper))]
	b[2] = number[rand.Intn(len(number))]
	b[3] = special[rand.Intn(len(special))]
	for i := 4; i < 12; i++ {
		// randomly select 1 character from given charset
		b[i] = allChar[rand.Intn(len(allChar))]
	}

	//shuffle character
	rand.Shuffle(len(b), func(i, j int) {
		b[i], b[j] = b[j], b[i]
	})

	return string(b)
}

// This test case creates a backup location with encryption
var _ = Describe("{BackupLocationWithEncryptionKey}", func() {
	var contexts []*scheduler.Context
	var appContexts []*scheduler.Context
	var bkpNamespaces []string
	var backupLocationName string
	var CloudCredUID string
	var clusterUid string
	var CredName string
	var restoreName string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	JustBeforeEach(func() {
		StartTorpedoTest("BackupLocationWithEncryptionKey", "Creating BackupLoaction with Encryption Keys", nil, 0)
	})
	It("Creating cloud account and backup location", func() {
		log.InfoD("Creating cloud account and backup location")
		providers := getProviders()
		bucketNames := getBucketName()
		bucketName = fmt.Sprintf("%s-%s", providers[0], bucketNames[0])
		CredName = fmt.Sprintf("%s-%s", "cred", providers[0])
		backupLocationName = fmt.Sprintf("%s-%s", "location", providers[0])
		CloudCredUID = uuid.New()
		BackupLocationUID = uuid.New()
		encryptionKey := generateEncryptionKey()
		CreateCloudCredential(providers[0], CredName, CloudCredUID, orgID)
		CreateBackupLocation(providers[0], backupLocationName, BackupLocationUID, CredName, CloudCredUID, bucketName, orgID, encryptionKey)
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

		Step("Register clusters for backup", func() {
			log.InfoD("Register clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			CreateSourceAndDestClusters(orgID, "", "", ctx)
			clusterStatus, clusterUid = Inst().Backup.RegisterBackupCluster(orgID, SourceClusterName, "")
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, "Verifying backup cluster")
		})

		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				err = CreateBackup(backupName, SourceClusterName, backupLocationName, BackupLocationUID, []string{namespace},
					nil, orgID, clusterUid, "", "", "", "", ctx)
				dash.VerifyFatal(err, nil, "Verifying backup creation")
			}
		})

		Step("Restoring the backed up application", func() {
			log.InfoD("Restoring the backed up application")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				restoreName = fmt.Sprintf("%s-%s", restoreNamePrefix, backupName)
				CreateRestore(restoreName, backupName, nil, destinationClusterName, orgID, ctx)
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.Infof("Deleting backup, restore and backup location, cloud account")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		DeleteRestore(restoreName, orgID, ctx)
		for _, namespace := range bkpNamespaces {
			backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
			backupUID := getBackupUID(orgID, backupName)
			DeleteBackup(backupName, backupUID, orgID, ctx)
		}
		DeleteBackupLocation(backupLocationName, BackupLocationUID, orgID)
		DeleteCloudCredential(CredName, orgID, CloudCredUID)
	})

})

// This testcase verifies resize after the volume is restored from a backup
var _ = Describe("{ResizeOnRestoredVolume}", func() {
	var (
		appList          = Inst().AppList
		backupName       string
		contexts         []*scheduler.Context
		preRuleNameList  []string
		postRuleNameList []string
		appContexts      []*scheduler.Context
		bkpNamespaces    []string
		clusterUid       string
		clusterStatus    api.ClusterInfo_StatusInfo_Status
		restoreName      string
		namespaceMapping map[string]string
	)
	labelSelectors := make(map[string]string)
	CloudCredUIDMap := make(map[string]string)
	BackupLocationMap := make(map[string]string)
	var backupLocation string
	contexts = make([]*scheduler.Context, 0)
	bkpNamespaces = make([]string, 0)
	providers := getProviders()

	JustBeforeEach(func() {
		StartTorpedoTest("ResizeAfterRestoredVolume", "Resize after the volume is restored from a backup", nil, 0)
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
	It("Resize after the volume is restored from a backup", func() {
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
			for _, provider := range providers {
				CredName := fmt.Sprintf("%s-%s", "cred", provider)
				CloudCredUID = uuid.New()
				CloudCredUIDMap[CloudCredUID] = CredName
				CreateCloudCredential(provider, CredName, CloudCredUID, orgID)
			}
		})

		Step("Creating backup location", func() {
			log.InfoD("Creating backup location")
			bucketNames := getBucketName()
			for _, provider := range providers {
				CredName := fmt.Sprintf("%s-%s", "cred", provider)
				bucketName := fmt.Sprintf("%s-%s", provider, bucketNames[0])
				backupLocation = fmt.Sprintf("%s-%s", provider, bucketNames[0])
				BackupLocationUID = uuid.New()
				BackupLocationMap[BackupLocationUID] = backupLocation
				CreateBackupLocation(provider, backupLocation, BackupLocationUID, CredName, CloudCredUID,
					bucketName, orgID, "")
			}
		})

		Step("Register cluster for backup", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			CreateSourceAndDestClusters(orgID, "", "", ctx)
			clusterStatus, clusterUid = Inst().Backup.RegisterBackupCluster(orgID, SourceClusterName, "")
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, "Verifying backup cluster")
		})

		Step("Start backup of application to bucket", func() {
			for _, namespace := range bkpNamespaces {
				ctx, err := backup.GetAdminCtxFromSecret()
				dash.VerifyFatal(err, nil, "Getting context")
				preRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, preRuleNameList[0])
				postRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, postRuleNameList[0])
				backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, namespace, backupLocationName)
				CreateBackup(backupName, SourceClusterName, backupLocation, BackupLocationUID, []string{namespace},
					labelSelectors, orgID, clusterUid, preRuleNameList[0], preRuleUid, postRuleNameList[0], postRuleUid, ctx)
			}
		})

		Step("Restoring the backed up application", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName = fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				restoreName = fmt.Sprintf("%s-%s", "test-restore", namespace)
				err = CreateRestore(restoreName, backupName, namespaceMapping, destinationClusterName, orgID, ctx)
				dash.VerifyFatal(err, nil, "Restore failed")
			}
		})

		Step("Resize volume after the restore is completed", func() {
			log.InfoD("Resize volume after the restore is completed")
			var err error
			for _, ctx := range contexts {
				var appVolumes []*volume.Volume
				log.InfoD(fmt.Sprintf("get volumes for %s app", ctx.App.Key))
				appVolumes, err = Inst().S.GetVolumes(ctx)
				log.FailOnError(err, "Failed to get volumes for app %s", ctx.App.Key)
				dash.VerifyFatal(len(appVolumes) > 0, true, "App volumes exist?")
				var requestedVols []*volume.Volume
				log.InfoD(fmt.Sprintf("Increase volume size %s on app %s's volumes: %v",
					Inst().V.String(), ctx.App.Key, appVolumes))
				requestedVols, err = Inst().S.ResizeVolume(ctx, Inst().ConfigMap)
				log.FailOnError(err, "Volume resize successful ?")
				log.InfoD(fmt.Sprintf("validate successful volume size increase on app %s's volumes: %v",
					ctx.App.Key, appVolumes))
				for _, v := range requestedVols {
					// Need to pass token before validating volume
					params := make(map[string]string)
					if Inst().ConfigMap != "" {
						params["auth-token"], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
						log.FailOnError(err, "Failed to get token from configMap")
					}
					err := Inst().V.ValidateUpdateVolume(v, params)
					dash.VerifyFatal(err, nil, "Validate volume update successful?")
				}
			}
		})

		Step("Validate applications post restore", func() {
			ValidateApplications(contexts)
		})

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Deleting the deployed apps after the testcase")
		for i := 0; i < len(contexts); i++ {
			opts := make(map[string]bool)
			opts[SkipClusterScopedObjects] = true
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			err := Inst().S.Destroy(contexts[i], opts)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verify destroying app %s, Err: %v", taskName, err))
		}
		log.InfoD("Deleting backup location, cloud creds and clusters")
		DeleteCloudAccounts(BackupLocationMap, CredName, CloudCredUID)
	})
})

// This testcase verifies resize after same original volume is restored from a backup stored in a locked bucket
var _ = Describe("{ResizeOnRestoredVolumeFromLockedBucket}", func() {
	var (
		appList          = Inst().AppList
		backupName       string
		contexts         []*scheduler.Context
		preRuleNameList  []string
		postRuleNameList []string
		appContexts      []*scheduler.Context
		bkpNamespaces    []string
		clusterUid       string
		clusterStatus    api.ClusterInfo_StatusInfo_Status
		backupList       []string
		beforeSize       int
		podsListBefore   []int
		podListAfter     []int
	)
	labelSelectors := make(map[string]string)
	CloudCredUIDMap := make(map[string]string)
	BackupLocationMap := make(map[string]string)

	var backupLocation string
	contexts = make([]*scheduler.Context, 0)
	bkpNamespaces = make([]string, 0)
	providers := getProviders()

	JustBeforeEach(func() {
		StartTorpedoTest("ResizeOnRestoredVolumeFromLockedBucket", "Resize after the volume is restored from a backup from locked bucket", nil, 0)
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
	It("Resize after the volume is restored from a backup", func() {
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
			for _, provider := range providers {
				CredName := fmt.Sprintf("%s-%s", "cred", provider)
				CloudCredUID = uuid.New()
				CloudCredUIDMap[CloudCredUID] = CredName
				CreateCloudCredential(provider, CredName, CloudCredUID, orgID)
			}
		})

		Step("Creating a locked bucket and backup location", func() {
			log.InfoD("Creating locked buckets and backup location")
			bucketNames := getBucketName()
			modes := [2]string{"GOVERNANCE", "COMPLIANCE"}
			for _, provider := range providers {
				for _, mode := range modes {
					CredName := fmt.Sprintf("%s-%s", "cred", provider)
					bucketName := fmt.Sprintf("%s-%s-%s", provider, bucketNames[1], strings.ToLower(mode))
					backupLocation = fmt.Sprintf("%s-%s-%s-lock", provider, bucketNames[1], strings.ToLower(mode))
					err := CreateS3Bucket(bucketName, true, 3, mode)
					log.FailOnError(err, "Unable to create locked s3 bucket %s", bucketName)
					BackupLocationUID = uuid.New()
					BackupLocationMap[BackupLocationUID] = backupLocation
					CreateBackupLocation(provider, backupLocation, BackupLocationUID, CredName, CloudCredUID,
						bucketName, orgID, "")
				}
			}
			log.InfoD("Successfully created locked buckets and backup location")
		})

		Step("Register cluster for backup", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			CreateSourceAndDestClusters(orgID, "", "", ctx)
			clusterStatus, clusterUid = Inst().Backup.RegisterBackupCluster(orgID, SourceClusterName, "")
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, "Verifying backup cluster")
		})

		for _, namespace := range bkpNamespaces {
			for backupLocationUID, backupLocationName := range BackupLocationMap {
				Step("Taking backup of application to locked bucket", func() {
					ctx, err := backup.GetAdminCtxFromSecret()
					dash.VerifyFatal(err, nil, "Getting context")
					preRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, preRuleNameList[0])
					postRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, postRuleNameList[0])
					backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, namespace, backupLocationName)
					backupList = append(backupList, backupName)
					CreateBackup(backupName, SourceClusterName, backupLocationName, backupLocationUID, []string{namespace},
						labelSelectors, orgID, clusterUid, preRuleNameList[0], preRuleUid, postRuleNameList[0], postRuleUid, ctx)
				})
				Step("Restoring the backups application", func() {
					ctx, err := backup.GetAdminCtxFromSecret()
					log.FailOnError(err, "Fetching px-central-admin ctx")
					CreateRestore(fmt.Sprintf("%s-restore", backupName), backupName, nil, SourceClusterName, orgID, ctx)
				})
				Step("Getting size before resize", func() {
					pods, err := core.Instance().GetPods(namespace, labelSelectors)
					log.FailOnError(err, "Unable to fetch the pod list")
					srcClusterConfigPath, err := GetSourceClusterConfigPath()
					log.FailOnError(err, "Getting kubeconfig path for source cluster")
					for _, pod := range pods.Items {
						beforeSize, err = getSizeOfMountPoint(pod.GetName(), namespace, srcClusterConfigPath)
						log.FailOnError(err, "Unable to fetch the size")
						podsListBefore = append(podsListBefore, beforeSize)
					}
				})
				Step("Resize volume after the restore is completed", func() {
					log.InfoD("Resize volume after the restore is completed")
					var err error
					for _, ctx := range contexts {
						var appVolumes []*volume.Volume
						log.InfoD(fmt.Sprintf("get volumes for %s app", ctx.App.Key))
						appVolumes, err = Inst().S.GetVolumes(ctx)
						log.FailOnError(err, "Failed to get volumes for app %s", ctx.App.Key)
						dash.VerifyFatal(len(appVolumes) > 0, true, "App volumes exist?")
						var requestedVols []*volume.Volume
						log.InfoD(fmt.Sprintf("Increase volume size %s on app %s's volumes: %v",
							Inst().V.String(), ctx.App.Key, appVolumes))
						requestedVols, err = Inst().S.ResizeVolume(ctx, Inst().ConfigMap)
						log.FailOnError(err, "Volume resize successful ?")
						log.InfoD(fmt.Sprintf("validate successful volume size increase on app %s's volumes: %v",
							ctx.App.Key, appVolumes))
						for _, v := range requestedVols {
							// Need to pass token before validating volume
							params := make(map[string]string)
							if Inst().ConfigMap != "" {
								params["auth-token"], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
								log.FailOnError(err, "Failed to get token from configMap")
							}
							err := Inst().V.ValidateUpdateVolume(v, params)
							dash.VerifyFatal(err, nil, "Validate volume update successful?")
						}
					}
				})
				Step("Getting size after resize", func() {
					log.InfoD("Checking volume size after resize")
					pods, err := core.Instance().GetPods(namespace, labelSelectors)
					log.FailOnError(err, "Unable to fetch the pod list")
					srcClusterConfigPath, err := GetSourceClusterConfigPath()
					log.FailOnError(err, "Getting kubeconfig path for source cluster")
					for _, pod := range pods.Items {
						afterSize, err := getSizeOfMountPoint(pod.GetName(), namespace, srcClusterConfigPath)
						log.FailOnError(err, "Unable to mount size")
						podListAfter = append(podListAfter, afterSize)
					}
					for i := 0; i < len(podListAfter); i++ {
						dash.VerifyFatal(podListAfter[i] > podsListBefore[i], true, "Volume size different")
					}
				})
			}
		}
	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Deleting the deployed apps after the testcase")
		for i := 0; i < len(contexts); i++ {
			opts := make(map[string]bool)
			opts[SkipClusterScopedObjects] = true
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			err := Inst().S.Destroy(contexts[i], opts)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verify destroying app %s, Err: %v", taskName, err))
		}
		log.InfoD("Deleting backup location, cloud creds and clusters")
		DeleteCloudAccounts(BackupLocationMap, CredName, CloudCredUID)
	})
})

// Restore backup from encrypted and non-encrypted backups
var _ = Describe("{RestoreEncryptedAndNonEncryptedBackups}", func() {
	var contexts []*scheduler.Context
	var appContexts []*scheduler.Context
	var bkpNamespaces []string
	var backupNames []string
	var restoreNames []string
	var backupLocationNames []string
	var CloudCredUID string
	var BackupLocationUID string
	var BackupLocation1UID string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	providers := getProviders()
	bucketNames := getBucketName()
	JustBeforeEach(func() {
		StartTorpedoTest("RestoreEncryptedAndNonEncryptedBackups", "Restore encrypted and non encypted backups", nil, 0)
	})
	It("Creating bucket, encrypted and non-encrypted backup location", func() {
		log.InfoD("Creating bucket, encrypted and non-encrypted backup location")
		bucketName = fmt.Sprintf("%s-%s", providers[0], bucketNames[0])
		encryptionBucketName := fmt.Sprintf("%s-%s-%s", providers[0], bucketNames[0], "encryptionbucket")
		backupLocationName := fmt.Sprintf("%s-%s", "location", providers[0])
		backupLocationNames = append(backupLocationNames, backupLocationName)
		backupLocationName = fmt.Sprintf("%s-%s", "encryption-location", providers[0])
		backupLocationNames = append(backupLocationNames, backupLocationName)
		CloudCredUID = uuid.New()
		BackupLocationUID = uuid.New()
		BackupLocation1UID = uuid.New()
		encryptionkey := "px-b@ckup-@utomat!on"
		CreateBucket(providers[0], encryptionBucketName)
		CreateCloudCredential(providers[0], CredName, CloudCredUID, orgID)
		CreateBackupLocation(providers[0], backupLocationNames[0], BackupLocationUID, CredName, CloudCredUID, bucketName, orgID, "")
		CreateBackupLocation(providers[0], backupLocationNames[1], BackupLocation1UID, CredName, CloudCredUID, encryptionBucketName, orgID, encryptionkey)
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
		Step("Register cluster for backup", func() {
			log.InfoD("Register clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			CreateSourceAndDestClusters(orgID, "", "", ctx)
			clusterStatus, clusterUid = Inst().Backup.RegisterBackupCluster(orgID, SourceClusterName, "")
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, "Verifying backup cluster")
		})

		Step("Taking encrypted and non-encrypted backups", func() {
			log.InfoD("Taking encrypted and no-encrypted backups")
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				backupNames = append(backupNames, backupName)
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				err = CreateBackup(backupName, SourceClusterName, backupLocationNames[0], BackupLocationUID, []string{namespace},
					nil, orgID, clusterUid, "", "", "", "", ctx)
				dash.VerifyFatal(err, nil, "Verifying backup creation")
				encryptionbackupName := fmt.Sprintf("%s-%s-%s", "encryption", BackupNamePrefix, namespace)
				backupNames = append(backupNames, encryptionbackupName)
				err = CreateBackup(encryptionbackupName, SourceClusterName, backupLocationNames[1], BackupLocation1UID, []string{namespace},
					nil, orgID, clusterUid, "", "", "", "", ctx)
				dash.VerifyFatal(err, nil, "Verifying backup creation")
			}
		})

		Step("Restoring encrypted and no-encrypted backups", func() {
			log.InfoD("Restoring encrypted and no-encrypted backups")
			restorename := fmt.Sprintf("%s-%s", restoreNamePrefix, backupNames[0])
			restoreNames = append(restoreNames, restorename)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			CreateRestore(restorename, backupNames[0], nil, destinationClusterName, orgID, ctx)
			time.Sleep(time.Minute * 5)
			restorename = fmt.Sprintf("%s-%s", restoreNamePrefix, backupNames[1])
			restoreNames = append(restoreNames, restorename)
			CreateRestore(restorename, backupNames[1], nil, destinationClusterName, orgID, ctx)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.InfoD("Deleting Restores, Backups and Backuplocations, cloud account")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		for _, restore := range restoreNames {
			DeleteRestore(restore, orgID, ctx)
		}
		ctx, err = backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		for _, backupName := range backupNames {
			backupUID := getBackupUID(backupName, orgID)
			DeleteBackup(backupName, backupUID, orgID, ctx)
		}
		DeleteBackupLocation(backupLocationNames[0], BackupLocationUID, orgID)
		DeleteBackupLocation(backupLocationNames[1], BackupLocation1UID, orgID)
		DeleteCloudCredential(CredName, orgID, CloudCredUID)
		encryptionBucketName := fmt.Sprintf("%s-%s-%s", providers[0], bucketNames[0], "encryptionbucket")
		DeleteBucket(providers[0], encryptionBucketName)
	})

})

// createS3BackupLocation creates backup location
func createGkeBackupLocation(name string, cloudCred string, orgID string) {
	Step(fmt.Sprintf("Create GKE backup location [%s] in org [%s]", name, orgID), func() {
		// TODO(stgleb): Implement this
	})
}

// CreateProviderClusterObject creates cluster for each cluster per each cloud provider
func CreateProviderClusterObject(provider string, kubeconfigList []string, cloudCred, orgID string, ctx context.Context) {
	Step(fmt.Sprintf("Create cluster [%s-%s] in org [%s]",
		clusterName, provider, orgID), func() {
		kubeconfigPath, err := getProviderClusterConfigPath(provider, kubeconfigList)
		log.FailOnError(err, "Fetching kubeconfig path for source cluster")
		CreateCluster(fmt.Sprintf("%s-%s", clusterName, provider),
			kubeconfigPath, orgID, "", "", ctx)
	})
}

func getProviders() []string {
	providersStr := os.Getenv("PROVIDERS")
	return strings.Split(providersStr, ",")
}

func getProviderClusterConfigPath(provider string, kubeconfigs []string) (string, error) {
	log.Infof("Get kubeconfigPath from list %v and provider %s",
		kubeconfigs, provider)
	for _, kubeconfigPath := range kubeconfigs {
		if strings.Contains(provider, kubeconfigPath) {
			fullPath := path.Join(KubeconfigDirectory, kubeconfigPath)
			return fullPath, nil
		}
	}

	return "nil", fmt.Errorf("kubeconfigPath not found for provider %s in kubeconfigPath list %v",
		provider, kubeconfigs)
}

// CreateBackup creates backup
func CreateBackup(backupName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, uid string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, ctx context.Context) error {

	var bkpUid string
	backupDriver := Inst().Backup
	bkpCreateRequest := &api.BackupCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  backupName,
			OrgId: orgID,
		},
		BackupLocationRef: &api.ObjectRef{
			Name: bLocation,
			Uid:  bLocationUID,
		},
		Cluster:        clusterName,
		Namespaces:     namespaces,
		LabelSelectors: labelSelectors,
		ClusterRef: &api.ObjectRef{
			Name: clusterName,
			Uid:  uid,
		},
		PreExecRuleRef: &api.ObjectRef{
			Name: preRuleName,
			Uid:  preRuleUid,
		},
		PostExecRuleRef: &api.ObjectRef{
			Name: postRuleName,
			Uid:  postRuleUid,
		},
	}
	_, err := backupDriver.CreateBackup(ctx, bkpCreateRequest)
	if err != nil {
		return err
	}
	backupSuccessCheck := func() (interface{}, bool, error) {
		bkpUid, err = backupDriver.GetBackupUID(ctx, backupName, orgID)
		log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
		backupInspectRequest := &api.BackupInspectRequest{
			Name:  backupName,
			Uid:   bkpUid,
			OrgId: orgID,
		}
		resp, err := backupDriver.InspectBackup(ctx, backupInspectRequest)
		log.FailOnError(err, "Inspecting the backup taken with request:\n%v", backupInspectRequest)
		actual := resp.GetBackup().GetStatus().Status
		expected := api.BackupInfo_StatusInfo_Success
		if actual != expected {
			return "", true, fmt.Errorf("backup status for [%s] expected was [%s] but got [%s]", backupName, expected, actual)
		}
		return "", false, nil
	}

	task.DoRetryWithTimeout(backupSuccessCheck, 10*time.Minute, 30*time.Second)

	bkpUid, err = backupDriver.GetBackupUID(ctx, backupName, orgID)
	log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
	backupInspectRequest := &api.BackupInspectRequest{
		Name:  backupName,
		Uid:   bkpUid,
		OrgId: orgID,
	}
	_, err = backupDriver.InspectBackup(ctx, backupInspectRequest)
	if err != nil {
		return err
	}
	return nil
}
func UpdateBackup(backupName string, backupuid string, org_id string, cloudCred string, cloudCredUID string, ctx context.Context) {
	backupDriver := Inst().Backup
	bkpUpdateRequest := &api.BackupUpdateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  backupName,
			OrgId: orgID,
			Uid:   backupuid,
		},
		CloudCredential: cloudCred,
		CloudCredentialRef: &api.ObjectRef{
			Name: cloudCred,
			Uid:  cloudCredUID,
		},
	}
	_, err := backupDriver.UpdateBackup(ctx, bkpUpdateRequest)
	log.FailOnError(err, "Failed to update backup with request -\n%v", bkpUpdateRequest)

}

// CreateBackupWithoutCheck creates backup without waiting for success
func CreateBackupWithoutCheck(backupName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, uid string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, ctx context.Context) {

	backupDriver := Inst().Backup
	bkpCreateRequest := &api.BackupCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  backupName,
			OrgId: orgID,
		},
		BackupLocationRef: &api.ObjectRef{
			Name: bLocation,
			Uid:  bLocationUID,
		},
		Cluster:        clusterName,
		Namespaces:     namespaces,
		LabelSelectors: labelSelectors,
		ClusterRef: &api.ObjectRef{
			Name: clusterName,
			Uid:  uid,
		},
		PreExecRuleRef: &api.ObjectRef{
			Name: preRuleName,
			Uid:  preRuleUid,
		},
		PostExecRuleRef: &api.ObjectRef{
			Name: postRuleName,
			Uid:  postRuleUid,
		},
	}
	_, err := backupDriver.CreateBackup(ctx, bkpCreateRequest)
	log.FailOnError(err, "Failed to take backup with request -\n%v", bkpCreateRequest)
}

type BackupAccess int32

const (
	ViewOnlyAccess BackupAccess = 1
	RestoreAccess               = 2
	FullAccess                  = 3
)

// ShareBackup provides access to the mentioned groups or/add users
func ShareBackup(backupName string, groupNames []string, userNames []string, accessLevel BackupAccess, ctx context.Context) error {
	var bkpUid string
	backupDriver := Inst().Backup
	groupIDs := make([]string, 0)
	userIDs := make([]string, 0)

	bkpUid, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
	log.FailOnError(err, "Failed to get backup UID for [%s]", backupName)
	log.Infof("Backup UID for %s - %s", backupName, bkpUid)

	for _, groupName := range groupNames {
		groupID, err := backup.FetchIDOfGroup(groupName)
		log.FailOnError(err, "Error fetching group ID")
		groupIDs = append(groupIDs, groupID)
	}

	for _, userName := range userNames {
		userID, err := backup.FetchIDOfUser(userName)
		log.FailOnError(err, "Error fetching user ID")
		userIDs = append(userIDs, userID)
	}

	groupBackupShareAccessConfigs := make([]*api.BackupShare_AccessConfig, 0)

	for _, groupName := range groupNames {
		groupBackupShareAccessConfig := &api.BackupShare_AccessConfig{
			Id:     groupName,
			Access: api.BackupShare_AccessType(accessLevel),
		}
		groupBackupShareAccessConfigs = append(groupBackupShareAccessConfigs, groupBackupShareAccessConfig)
	}

	userBackupShareAccessConfigs := make([]*api.BackupShare_AccessConfig, 0)

	for _, userID := range userIDs {
		userBackupShareAccessConfig := &api.BackupShare_AccessConfig{
			Id:     userID,
			Access: api.BackupShare_AccessType(accessLevel),
		}
		userBackupShareAccessConfigs = append(userBackupShareAccessConfigs, userBackupShareAccessConfig)
	}

	shareBackupRequest := &api.BackupShareUpdateRequest{
		OrgId: orgID,
		Name:  backupName,
		Backupshare: &api.BackupShare{
			Groups:        groupBackupShareAccessConfigs,
			Collaborators: userBackupShareAccessConfigs,
		},
		Uid: bkpUid,
	}

	_, err = backupDriver.UpdateBackupShare(ctx, shareBackupRequest)
	return err

}

// ClusterUpdateBackupShare shares all backup with the users and/or groups provided for a given cluster
// addUsersOrGroups - provide true if the mentioned users/groups needs to be added
// addUsersOrGroups - provide false if the mentioned users/groups needs to be deleted or removed
func ClusterUpdateBackupShare(clusterName string, groupNames []string, userNames []string, accessLevel BackupAccess, addUsersOrGroups bool, ctx context.Context) error {
	backupDriver := Inst().Backup
	groupIDs := make([]string, 0)
	userIDs := make([]string, 0)
	_, clusterUID := backupDriver.RegisterBackupCluster(orgID, SourceClusterName, "")

	for _, groupName := range groupNames {
		groupID, err := backup.FetchIDOfGroup(groupName)
		log.FailOnError(err, "Error fetching group ID")
		groupIDs = append(groupIDs, groupID)
	}

	for _, userName := range userNames {
		userID, err := backup.FetchIDOfUser(userName)
		log.FailOnError(err, "Error fetching user ID")
		userIDs = append(userIDs, userID)
	}

	groupBackupShareAccessConfigs := make([]*api.BackupShare_AccessConfig, 0)

	for _, groupName := range groupNames {
		groupBackupShareAccessConfig := &api.BackupShare_AccessConfig{
			Id:     groupName,
			Access: api.BackupShare_AccessType(accessLevel),
		}
		groupBackupShareAccessConfigs = append(groupBackupShareAccessConfigs, groupBackupShareAccessConfig)
	}

	userBackupShareAccessConfigs := make([]*api.BackupShare_AccessConfig, 0)

	for _, userID := range userIDs {
		userBackupShareAccessConfig := &api.BackupShare_AccessConfig{
			Id:     userID,
			Access: api.BackupShare_AccessType(accessLevel),
		}
		userBackupShareAccessConfigs = append(userBackupShareAccessConfigs, userBackupShareAccessConfig)
	}

	backupShare := &api.BackupShare{
		Groups:        groupBackupShareAccessConfigs,
		Collaborators: userBackupShareAccessConfigs,
	}

	var clusterBackupShareUpdateRequest *api.ClusterBackupShareUpdateRequest

	if addUsersOrGroups {
		clusterBackupShareUpdateRequest = &api.ClusterBackupShareUpdateRequest{
			OrgId:          orgID,
			Name:           clusterName,
			AddBackupShare: backupShare,
			DelBackupShare: nil,
			Uid:            clusterUID,
		}
	} else {
		clusterBackupShareUpdateRequest = &api.ClusterBackupShareUpdateRequest{
			OrgId:          orgID,
			Name:           clusterName,
			AddBackupShare: nil,
			DelBackupShare: backupShare,
			Uid:            clusterUID,
		}
	}

	_, err := backupDriver.ClusterUpdateBackupShare(ctx, clusterBackupShareUpdateRequest)
	return err
}

func GetAllBackupsForUser(username, password string) ([]string, error) {
	var bkp *api.BackupObject
	backupNames := make([]string, 0)
	backupDriver := Inst().Backup
	ctx, err := backup.GetNonAdminCtx(username, password)
	log.FailOnError(err, "Fetching %s ctx", username)

	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: orgID}
	curBackups, err := backupDriver.EnumerateBackup(ctx, bkpEnumerateReq)
	for _, bkp = range curBackups.GetBackups() {
		backupNames = append(backupNames, bkp.GetName())
	}
	return backupNames, err
}

func GetNodesForBackup(backupName string, bkpNamespace string,
	orgID string, clusterName string, triggerOpts *driver_api.TriggerOptions) []node.Node {

	var nodes []node.Node
	backupDriver := Inst().Backup

	backupUID := getBackupUID(backupName, orgID)
	backupInspectReq := &api.BackupInspectRequest{
		Name:  backupName,
		OrgId: orgID,
		Uid:   backupUID,
	}
	err := Inst().Backup.WaitForBackupRunning(context.Background(), backupInspectReq, defaultTimeout, defaultRetryInterval)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to wait for backup [%s] to start. Error: [%v]",
			backupName, err))

	clusterInspectReq := &api.ClusterInspectRequest{
		OrgId:          orgID,
		Name:           clusterName,
		IncludeSecrets: true,
	}
	//ctx, err := backup.GetPxCentralAdminCtx()
	ctx, err := backup.GetAdminCtxFromSecret()
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
			err))
	clusterInspectRes, err := backupDriver.InspectCluster(ctx, clusterInspectReq)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to inspect cluster [%s] in org [%s]. Error: [%v]",
			clusterName, orgID, err))
	Expect(clusterInspectRes).NotTo(BeNil(),
		"Got an empty response while inspecting cluster [%s] in org [%s]", clusterName, orgID)

	cluster := clusterInspectRes.GetCluster()
	volumeBackupIDs, err := backupDriver.GetVolumeBackupIDs(context.Background(),
		backupName, bkpNamespace, cluster, orgID)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to get volume backup IDs for backup [%s] in org [%s]. Error: [%v]",
			backupName, orgID, err))
	Expect(len(volumeBackupIDs)).NotTo(Equal(0),
		"Got empty list of volumeBackup IDs from backup driver")

	for _, backupID := range volumeBackupIDs {
		n, err := Inst().V.GetNodeForBackup(backupID)
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to get node on which backup [%s] in running. Error: [%v]",
				backupName, err))

		log.Debugf("Volume backup [%s] is running on node [%s], node id: [%s]\n",
			backupID, n.GetHostname(), n.GetId())
		nodes = append(nodes, n)
	}
	return nodes
}

// CreateRestore creates restore
func CreateRestore(restoreName string, backupName string,
	namespaceMapping map[string]string, clusterName string, orgID string, ctx context.Context) error {

	var bkp *api.BackupObject
	var bkpUid string
	backupDriver := Inst().Backup
	log.Infof("Getting the UID of the backup needed to be restored")
	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: orgID}
	curBackups, _ := backupDriver.EnumerateBackup(ctx, bkpEnumerateReq)
	log.Debugf("Enumerate backup response -\n%v", curBackups)
	for _, bkp = range curBackups.GetBackups() {
		if bkp.Name == backupName {
			bkpUid = bkp.Uid
			break
		}
	}
	createRestoreReq := &api.RestoreCreateRequest{
		CreateMetadata: &api.CreateMetadata{
			Name:  restoreName,
			OrgId: orgID,
		},
		Backup:           backupName,
		Cluster:          clusterName,
		NamespaceMapping: namespaceMapping,
		BackupRef: &api.ObjectRef{
			Name: backupName,
			Uid:  bkpUid,
		},
	}
	_, err := backupDriver.CreateRestore(ctx, createRestoreReq)
	if err != nil {
		return err
	}
	restoreInspectRequest := &api.RestoreInspectRequest{
		Name:  restoreName,
		OrgId: orgID,
	}

	restoreSuccessCheck := func() (interface{}, bool, error) {
		resp, err := Inst().Backup.InspectRestore(ctx, restoreInspectRequest)
		restoreResponseStatus := resp.GetRestore().GetStatus()
		log.FailOnError(err, "Failed verifying restore for - %s", restoreName)
		if restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_PartialSuccess || restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Success {
			log.Infof("Restore status - %s", restoreResponseStatus)
			log.InfoD("Status of %s - [%s]",
				restoreName, restoreResponseStatus.GetStatus())
			return "", false, nil
		}
		return "", true, fmt.Errorf("expected status of %s - [%s] or [%s], but got [%s]",
			restoreName, api.RestoreInfo_StatusInfo_PartialSuccess.String(), api.RestoreInfo_StatusInfo_Success, restoreResponseStatus.GetStatus())
	}

	task.DoRetryWithTimeout(restoreSuccessCheck, 10*time.Minute, 30*time.Second)
	return nil
}

// TearDownBackupRestoreSpecific deletes backups and restores specified by name as well as backup location
func TearDownBackupRestoreSpecific(backups []string, restores []string) {
	for _, backupName := range backups {
		backupUID := getBackupUID(backupName, OrgID)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		DeleteBackup(backupName, backupUID, OrgID, ctx)
	}
	for _, restoreName := range restores {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		DeleteRestore(restoreName, OrgID, ctx)
	}
	provider := GetProvider()
	ctx, err := backup.GetAdminCtxFromSecret()
	log.FailOnError(err, "Fetching px-central-admin ctx")
	DeleteCluster(destinationClusterName, OrgID, ctx)
	DeleteCluster(SourceClusterName, orgID, ctx)
	// Need to add backup location UID for Delete Backup Location call
	DeleteBackupLocation(backupLocationName, "", OrgID)
	DeleteCloudCredential(CredName, OrgID, CloudCredUID)
	DeleteBucket(provider, BucketName)
}

// CreateRestoreGetErr creates restore
func CreateRestoreGetErr(restoreName string, backupName string,
	namespaceMapping map[string]string, clusterName string, orgID string) (err error) {

	Step(fmt.Sprintf("Create restore [%s] in org [%s] on cluster [%s]",
		restoreName, orgID, clusterName), func() {

		backupDriver := Inst().Backup
		createRestoreReq := &api.RestoreCreateRequest{
			CreateMetadata: &api.CreateMetadata{
				Name:  restoreName,
				OrgId: orgID,
			},
			Backup:           backupName,
			Cluster:          clusterName,
			NamespaceMapping: namespaceMapping,
		}
		//ctx, err := backup.GetPxCentralAdminCtx()
		ctx, err := backup.GetAdminCtxFromSecret()
		Expect(err).NotTo(HaveOccurred(),
			fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
				err))
		_, err = backupDriver.CreateRestore(ctx, createRestoreReq)
		if err != nil {
			log.Errorf("Failed to create restore [%s] in org [%s] on cluster [%s]. Error: [%v]",
				restoreName, orgID, clusterName, err)
		}

		// TODO: validate createClusterResponse also
	})
	return err
}

func getBackupUID(backupName, orgID string) string {
	backupDriver := Inst().Backup
	ctx, err := backup.GetAdminCtxFromSecret()
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to fetch px-central-admin ctx: [%v]",
			err))
	backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
	Expect(err).NotTo(HaveOccurred(),
		fmt.Sprintf("Failed to get backup uid for org %s backup %s ctx: [%v]",
			orgID, backupName, err))
	return backupUID
}

func getSizeOfMountPoint(podname string, namespace string, kubeconfigfile string) (int, error) {
	var number int
	ret, err := kubectlExec([]string{podname, "-n", namespace, "--kubeconfig=", kubeconfigfile, " -- /bin/df"})
	if err != nil {
		return 0, err
	}
	for _, line := range strings.SplitAfter(ret, "\n") {
		if strings.Contains(line, "pxd") {
			ret = strings.Fields(line)[3]
		}
	}
	number, err = strconv.Atoi(ret)
	if err != nil {
		return 0, err
	}
	return number, nil
}

func kubectlExec(arguments []string) (string, error) {
	if len(arguments) == 0 {
		return "", fmt.Errorf("no arguments supplied for kubectl command")
	}
	cmd := exec.Command("kubectl exec -it", arguments...)
	output, err := cmd.Output()
	log.Debugf("command output for '%s': %s", cmd.String(), string(output))
	if err != nil {
		return "", fmt.Errorf("error on executing kubectl command, Err: %+v", err)
	}
	return string(output), err
}
