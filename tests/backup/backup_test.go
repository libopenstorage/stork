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
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/log"
	storageapi "k8s.io/api/storage/v1"
	storage "github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/api/core/v1"

	. "github.com/portworx/torpedo/tests"
)

var (
	bucketName     string
	backupLocation string
)

type BackupAccess int32

const (
	ViewOnlyAccess BackupAccess = 1
	RestoreAccess               = 2
	FullAccess                  = 3
)

type userRoleAccess struct {
	user     string
	roles    backup.PxBackupRole
	accesses BackupAccess
	context  context.Context
}

var backupAccessKeyValue = map[BackupAccess]string{
	0: "ViewOnlyAccess",
	1: "RestoreAccess",
	2: "FullAccess",
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

// This is to create multiple users and groups
var _ = Describe("{CreateMultipleUsersAndGroups}", func() {
	numberOfUsers := 20
	numberOfGroups := 10
	users := make([]string, 0)
	groups := make([]string, 0)
	var groupNotCreated string
	var userNotCreated string

	JustBeforeEach(func() {
		wantAllAfterSuiteActions = false
		StartTorpedoTest("CreateMultipleUsersAndGroups", "Creation of multiple users and groups", nil, 58032)
	})
	It("Create multiple users and Group", func() {

		Step("Create Groups", func() {
			log.InfoD("Creating %d groups", numberOfGroups)
			var wg sync.WaitGroup
			for i := 1; i <= numberOfGroups; i++ {
				groupName := fmt.Sprintf("testGroup%v", i)
				wg.Add(1)
				go func(groupName string) {
					defer wg.Done()
					err := backup.AddGroup(groupName)
					log.FailOnError(err, "Failed to create group - %v", groupName)
					groups = append(groups, groupName)

				}(groupName)
			}
			wg.Wait()
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
					defer wg.Done()
					err := backup.AddUser(userName, firstName, lastName, email, "Password1")
					log.FailOnError(err, "Failed to create user - %s", userName)
					users = append(users, userName)

				}(userName, firstName, lastName, email)
			}
			wg.Wait()
		})

		//iterates through the group names slice and checks if the group name is present in the response map using map[key]
		//to get the value and key to check whether the it is present or not.
		//If it's not found, it prints the group name not found in struct slice and exit.

		Step("Validate Group", func() {
			createdgroups, err := backup.GetAllGroups()
			log.Info("created group ", createdgroups)
			log.FailOnError(err, "Failed to get group")
			responseMap := make(map[string]bool)
			for _, ctreatedgroup := range createdgroups {
				responseMap[ctreatedgroup.Name] = true
			}
			for _, group := range groups {
				if _, ok := responseMap[group]; !ok {
					groupNotCreated = group
					err = fmt.Errorf("Group Name not created - [%s]", group)
					log.FailOnError(err, "Failed to create the group")
					break
				}

			}
			dash.VerifyFatal(groupNotCreated, "", "Group Creation Verification successful")
		})

		Step("Validate User", func() {
			createdusers, err := backup.GetAllUsers()
			log.Info("created user ", createdusers)
			log.FailOnError(err, "Failed to get user")
			responseMap := make(map[string]bool)
			for _, ctreateduser := range createdusers {
				responseMap[ctreateduser.Name] = true
			}
			for _, user := range users {
				if _, ok := responseMap[user]; !ok {
					userNotCreated = user
					err = fmt.Errorf("User Name not created - [%s]", user)
					log.FailOnError(err, "Failed to create the user")
					break
				}

			}
			dash.VerifyFatal(userNotCreated, "", "User Creation Verification successful")
		})

	})

	JustAfterEach(func() {
		defer EndTorpedoTest()
		log.Infof("Cleanup started")
		err := backup.DeleteMultipleGroups(groups)
		log.FailOnError(err, "Failed to delete group(s)")
		err = backup.DeleteMultipleUsers(users)
		log.FailOnError(err, "Failed to delete user(s)")
		log.Infof("Cleanup done")

	})

})

// Validate that user can't duplicate a shared backup without registering the cluster
var _ = Describe("{DuplicateSharedBackup}", func() {
	userName := "testuser1"
	firstName := "firstName"
	lastName := "lastName"
	email := "testuser10@cnbu.com"
	password := "Password1"
	numberOfBackups := 1
	var backupName string
	userContexts := make([]context.Context, 0)
	var contexts []*scheduler.Context
	var bucketName string
	var BackupLocationName string
	var backupLocationUID string
	var cloudCredUID string
	var CloudCredUidList []string
	var appContexts []*scheduler.Context
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var credName string
	bkpNamespaces = make([]string, 0)

	JustBeforeEach(func() {
		StartTorpedoTest("DuplicateSharedBackup",
			"Share backup with user and duplicate it", nil, 82937)
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
	It("Validate shared backup is not duplicated without cluster", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		providers := getProviders()
		buckets := getBucketName()
		backupName = fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(contexts)
		})
		Step("Create User", func() {
			err = backup.AddUser(userName, firstName, lastName, email, password)
			log.FailOnError(err, "Failed to create user - %s", userName)

		})
		Step("Adding Credentials and Registering Backup Location", func() {
			log.InfoD("Creating cloud credentials and backup location")
			for _, provider := range providers {
				bucketName = fmt.Sprintf("%s-%v", provider, buckets[0])
				cloudCredUID = uuid.New()
				CloudCredUidList = append(CloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				CreateCloudCredential(provider, credName, cloudCredUID, orgID)
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				time.Sleep(time.Minute * 1)
				BackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				CreateBackupLocation(provider, BackupLocationName, backupLocationUID, credName, cloudCredUID, bucketName, orgID, "")
				log.InfoD("Created Backup Location with name - %s", BackupLocationName)
			}
		})
		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
			CreateSourceAndDestClusters(orgID, "", "", ctx)
			clusterStatus, clusterUid = Inst().Backup.RegisterBackupCluster(orgID, SourceClusterName, "")
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, "Verifying backup cluster status")
		})

		Step("Taking backup of applications", func() {
			log.InfoD("Taking Backup of application")
			err = CreateBackup(backupName, SourceClusterName, BackupLocationName, backupLocationUID, []string{bkpNamespaces[0]},
				nil, orgID, clusterUid, "", "", "", "", ctx)
			dash.VerifyFatal(err, nil, "Verifying backup creation")
		})

		Step("Share backup with user", func() {
			log.InfoD("Share backup with  user having full access")
			err := ShareBackup(backupName, nil, []string{userName}, FullAccess, ctx)
			log.FailOnError(err, "Failed to share backup %s", backupName)
		})

		Step("Duplicate shared backup", func() {
			log.InfoD("Validating to duplicate share backup without adding clutser")
			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(userName, password)
			log.FailOnError(err, "Fetching non px-central-admin user ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Validate that backups are shared with user
			log.Infof("Validating that backups are shared with %s user", userName)
			userBackups1, err := GetAllBackupsForUser(userName, password)
			log.FailOnError(err, "Not able to fetch backup for user %s", userName)
			dash.VerifyFatal(len(userBackups1), numberOfBackups, fmt.Sprintf("Validating that user [%s] has access to all shared backups [%v]", userName, userBackups1))

			//to duplicate shared backup internally it calls create backup api
			log.Infof("Duplicate shared backup")
			err = CreateBackup(backupName, SourceClusterName, BackupLocationName, backupLocationUID, []string{bkpNamespaces[0]},
				nil, orgID, clusterUid, "", "", "", "", ctxNonAdmin)
			log.Infof("user not able to duplicate shared backup without adding cluster with err - %v", err)
			errMessage := fmt.Sprintf("NotFound desc = failed to retrieve cluster [%s]: object not found", SourceClusterName)
			dash.VerifyFatal(strings.Contains(err.Error(), errMessage), true, "Verifying that shared backup can't be duplicated without adding cluster")
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

		//Deleting user
		err := backup.DeleteUser(userName)
		log.FailOnError(err, "Error deleting user %v", userName)

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")

		//Delete Backups
		backupDriver := Inst().Backup
		backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
		backupDeleteResponse, err := DeleteBackup(backupName, backupUID, orgID, ctx)
		log.FailOnError(err, "Backup [%s] could not be deleted with delete response %s", backupName, backupDeleteResponse)

		log.Infof("Deleting registered clusters for admin context")
		DeleteCluster(SourceClusterName, orgID, ctx)
		DeleteCluster(destinationClusterName, orgID, ctx)

		log.Infof("Cleaning up backup location - %s", BackupLocationName)
		DeleteBackupLocation(BackupLocationName, backupLocationUID, orgID)

		log.Infof("Cleaning cloud credential")
		time.Sleep(time.Minute * 3)
		DeleteCloudCredential(credName, orgID, cloudCredUID)

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
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, timestamp)
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
		DeleteCloudAccounts(backupLocationMap, cloudCredName, cloudCredUID, ctx)
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
	timestamp := time.Now().Unix()
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
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cloudcred", provider, timestamp)
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
		DeleteCloudAccounts(backupLocationMap, cloudCredName, cloudCredUID, ctx)
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
	timestamp := time.Now().Unix()
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
				credName = fmt.Sprintf("autogenerated-cred-%v", timestamp)
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

var _ = Describe("{ShareLargeNumberOfBackupsWithLargeNumberOfUsers}", func() {
	numberOfUsers, _ := strconv.Atoi(getEnv(usersToBeCreated, "200"))
	numberOfGroups, _ := strconv.Atoi(getEnv(groupsToBeCreated, "100"))
	groupSize, _ := strconv.Atoi(getEnv(maxUsersInGroup, "2"))
	numberOfBackups, _ := strconv.Atoi(getEnv(maxBackupsToBeCreated, "100"))
	timeBetweenConsecutiveBackups := 4 * time.Second
	users := make([]string, 0)
	groups := make([]string, 0)
	backupNames := make([]string, 0)
	numberOfSimultaneousBackups := 20
	var contexts []*scheduler.Context
	labelSelectors := make(map[string]string)
	var backupLocationUID string
	var cloudCredUID string
	var CloudCredUidList []string
	var appContexts []*scheduler.Context
	userContexts := make([]context.Context, 0)
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var customBackupLocationName string
	var credName string
	var chosenUser string
	bkpNamespaces = make([]string, 0)
	timestamp := time.Now().Unix()
	providers := getProviders()

	JustBeforeEach(func() {
		StartTorpedoTest("ShareLargeNumberOfBackupsWithLargeNumberOfUsers",
			"Share large number of backups to large number of users", nil, 82941)
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
					defer wg.Done()
					err := backup.AddUser(userName, firstName, lastName, email, "Password1")
					log.FailOnError(err, "Failed to create user - %s", userName)
					users = append(users, userName)
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
					defer wg.Done()
					err := backup.AddGroup(groupName)
					log.FailOnError(err, "Failed to create group - %v", groupName)
					groups = append(groups, groupName)
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
					defer wg.Done()
					err := backup.AddGroupToUser(users[i], groups[groupIndex])
					log.FailOnError(err, "Failed to assign group to user")
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
			log.InfoD("Using pre-provisioned bucket. Creating cloud credentials and backup location.")
			bucketNames := getBucketName()
			for _, provider := range providers {
				bucketName := fmt.Sprintf("%s-%s", provider, bucketNames[0])
				cloudCredUID = uuid.New()
				CloudCredUidList = append(CloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", timestamp)
				CreateCloudCredential(provider, credName, cloudCredUID, orgID)
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				//TODO: Eliminate time.Sleep
				time.Sleep(time.Minute * 1)
				customBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", timestamp)
				CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, bucketName, orgID, "")
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
			var sem = make(chan struct{}, numberOfSimultaneousBackups)
			var wg sync.WaitGroup
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.InfoD("Taking %d backups", numberOfBackups)
			for _, namespace := range bkpNamespaces {
				for i := 0; i < numberOfBackups; i++ {
					sem <- struct{}{}
					time.Sleep(timeBetweenConsecutiveBackups)
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

		Step("Share all backups with Full Access in source cluster with a group", func() {
			log.InfoD("Share all backups with Full Access in source cluster with a group")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = ClusterUpdateBackupShare(SourceClusterName, groups, nil, FullAccess, true, ctx)
			log.FailOnError(err, "Failed sharing all backups for cluster [%s]", SourceClusterName)
		})

		Step("Validate Full Access of backups shared at cluster level", func() {
			log.InfoD("Validate Full Access of backups shared at cluster level for a user of a group")
			// Get user from group
			var err error
			chosenUser, err = backup.GetRandomUserFromGroup(groups[rand.Intn(numberOfGroups-1)])
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
			backupName := backupNames[rand.Intn(numberOfBackups-1)]
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
		})

		Step("Share all backups with Restore Access in source cluster with a group", func() {
			log.InfoD("Share all backups with Full Access in source cluster with a group")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = ClusterUpdateBackupShare(SourceClusterName, groups, nil, RestoreAccess, true, ctx)
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
			backupName := backupNames[rand.Intn(numberOfBackups-1)]
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
		})

		Step("Share all backups with View Only Access in source cluster with a group", func() {
			log.InfoD("Share all backups with Full Access in source cluster with a group")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = ClusterUpdateBackupShare(SourceClusterName, groups, nil, ViewOnlyAccess, true, ctx)
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
			backupName := backupNames[rand.Intn(numberOfBackups-1)]
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
	timestamp := time.Now().Unix()
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
				credName = fmt.Sprintf("autogenerated-cred-%v", timestamp)
				CreateCloudCredential(provider, credName, cloudCredUID, orgID)
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				//TODO: Eliminate time.Sleep
				time.Sleep(time.Minute * 1)
				customBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", timestamp)
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
			log.InfoD("Checking backups user [%s] has after revoking", chosenUser)
			var userBackups []string
			var err error
			noAccessCheck := func() (interface{}, bool, error) {
				// Enumerate all the backups available to the user
				userBackups, err = GetAllBackupsForUser(chosenUser, "Password1")
				log.FailOnError(err, "Failed to get all backups for user - [%s]", chosenUser)
				log.Infof("Backups user [%s] has access to - %v", chosenUser, userBackups)
				if len(userBackups) > 0 {
					return "", true, fmt.Errorf("waiting for all backup access - [%v] to be revoked for user = [%s]",
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
	timestamp := time.Now().Unix()
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
				credName = fmt.Sprintf("autogenerated-cred-%v", timestamp)
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

var _ = Describe("{SharedBackupDelete}", func() {
	numberOfUsers := 10
	numberOfBackups := 10
	users := make([]string, 0)
	backupNames := make([]string, 0)
	userContexts := make([]context.Context, 0)
	var contexts []*scheduler.Context
	var BucketName string
	var BackupLocationName string
	var backupLocationUID string
	var cloudCredUID string
	var CloudCredUidList []string
	var appContexts []*scheduler.Context
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var credName string
	bkpNamespaces = make([]string, 0)
	timestamp := time.Now().Unix()
	providers := getProviders()
	buckets := getBucketName()
	JustBeforeEach(func() {
		StartTorpedoTest("SharedBackupDelete",
			"Share backup with multiple users and delete the backup", nil, 0)
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
	It("Share the backups and delete", func() {
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(contexts)
		})
		Step("Create Users", func() {
			users = createUsers(numberOfUsers)
		})
		Step("Adding Credentials and Registering Backup Location", func() {
			log.InfoD("Creating bucket, cloud credentials and backup location")
			for _, provider := range providers {
				BucketName = fmt.Sprintf("%s-%v", provider, buckets[0])
				cloudCredUID = uuid.New()
				CloudCredUidList = append(CloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", timestamp)
				CreateCloudCredential(provider, credName, cloudCredUID, orgID)
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				//TODO: Eliminate time.Sleep
				time.Sleep(time.Minute * 1)
				BackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", timestamp)
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
						CreateBackup(backupName, SourceClusterName, BackupLocationName, backupLocationUID, []string{namespace},
							nil, orgID, clusterUid, "", "", "", "", ctx)
					}(backupName)
				}
				wg.Wait()
			}
			log.Infof("List of backups - %v", backupNames)
		})
		backupMap := make(map[string]string, 0)
		Step("Share backup with multiple users", func() {
			log.InfoD("Share backup with multiple users")
			// Get Admin Context - needed to share backup and get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			// Share backups with all the users
			for _, backup := range backupNames {
				err = ShareBackup(backup, nil, users, ViewOnlyAccess, ctx)
				log.FailOnError(err, "Failed to share backup %s", backup)
			}

			for _, user := range users {
				// Get user context
				ctxNonAdmin, err := backup.GetNonAdminCtx(user, "Password1")
				log.FailOnError(err, "Fetching px-central-admin ctx")
				userContexts = append(userContexts, ctxNonAdmin)

				// Register Source and Destination cluster
				log.InfoD("Registering Source and Destination clusters from user context for user -%s", user)
				CreateSourceAndDestClusters(orgID, "", "", ctxNonAdmin)

				for _, backup := range backupNames {
					// Get Backup UID
					backupDriver := Inst().Backup
					backupUID, err := backupDriver.GetBackupUID(ctx, backup, orgID)
					log.FailOnError(err, "Failed while trying to get backup UID for - %s", backup)
					backupMap[backup] = backupUID

					// Start Restore
					restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
					err = CreateRestore(restoreName, backup, nil, destinationClusterName, orgID, ctxNonAdmin)

					// Restore validation to make sure that the user with cannot restore
					dash.VerifyFatal(strings.Contains(err.Error(), "failed to retrieve backup location"), true,
						fmt.Sprintf("Verifying backup restore [%s] is not possible for backup [%s] with user [%s]", restoreName, backup, user))

					// Delete backup to confirm that the user cannot delete the backup
					_, err = DeleteBackup(backup, backupUID, orgID, ctxNonAdmin)
					log.Infof("Error message - %s", err.Error())
					dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to delete backup"), true,
						fmt.Sprintf("Verifying backup deletion is not possible for backup [%s] with user [%s]", backup, user))
				}
			}
		})

		Step("Delete the backups and validate", func() {
			log.InfoD("Delete the backups and validate")
			// Delete the backups
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var wg sync.WaitGroup
			backupDriver := Inst().Backup
			for _, backup := range backupNames {
				wg.Add(1)
				go func(backup string) {
					defer wg.Done()
					_, err = DeleteBackup(backup, backupMap[backup], orgID, ctx)
					log.FailOnError(err, "Failed to delete backup - %s", backup)
					err = backupDriver.WaitForBackupDeletion(ctx, backup, orgID, time.Minute*10, time.Minute*1)
					log.FailOnError(err, "Error waiting for backup deletion %v", backup)
				}(backup)
			}
			wg.Wait()

			//Validate that backups are not listing with shared users
			// Get user context
			for _, user := range users {
				log.Infof("Validating user %s has access to no backups", user)
				userBackups1, _ := GetAllBackupsForUser(user, "Password1")
				dash.VerifyFatal(len(userBackups1), 0, fmt.Sprintf("Validating that user [%s] has access to no backups", user))
			}
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
	timeStamp := time.Now().Unix()
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
				CredName := fmt.Sprintf("%s-%s-%v", "cred", provider, timeStamp)
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
					CredName := fmt.Sprintf("%s-%s-%v", "cred", provider, timeStamp)
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
				CredName := fmt.Sprintf("%s-%s-%v", "cred", provider, timeStamp)
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
	timestamp := time.Now().Unix()
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
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cloudcred", provider, timestamp)
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
		DeleteCloudAccounts(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

var _ = Describe("{ShareBackupWithDifferentRoleUsers}", func() {
	var (
		contexts                 []*scheduler.Context
		appContexts              []*scheduler.Context
		bkpNamespaces            []string
		clusterUid               string
		clusterStatus            api.ClusterInfo_StatusInfo_Status
		backupLocationUID        string
		cloudCredName            string
		cloudCredUID             string
		bkpLocationName          string
		backupNames              []string
		userRoleAccessBackupList map[userRoleAccess]string
	)
	userRestoreContext := make(map[context.Context]string)
	numberOfUsers := 9
	backupLocationMap := make(map[string]string)
	users := make([]string, 0)
	userContextsList := make([]context.Context, 0)
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	timestamp := time.Now().Unix()
	JustBeforeEach(func() {
		StartTorpedoTest("ShareBackupWithDifferentRoleUsers",
			"Take backups and share it with multiple user with different access permissions and different roles", nil, 82947)
		log.InfoD("Deploy applications needed for backup")
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
	It("Share Backup With Different Users having different access level and different role", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		dash.VerifyFatal(err, nil, "Getting px-central-admin  context")

		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(contexts)
		})

		Step("Create multiple Users", func() {
			log.InfoD("Creating %d users", numberOfUsers)
			var wg sync.WaitGroup
			for i := 1; i <= numberOfUsers; i++ {
				userName := fmt.Sprintf("testuser%v", i)
				firstName := fmt.Sprintf("FirstName%v", i)
				lastName := fmt.Sprintf("LastName%v", i)
				email := fmt.Sprintf("testuser%v@cnbu.com", i)
				wg.Add(1)
				go func(userName, firstName, lastName, email string) {
					defer wg.Done()
					err := backup.AddUser(userName, firstName, lastName, email, "Password1")
					log.FailOnError(err, "Failed to create user - %s", userName)
					users = append(users, userName)

				}(userName, firstName, lastName, email)
			}
			wg.Wait()
			log.Infof("The users created are %v", users)
		})

		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			bucketNames := getBucketName()
			providers := getProviders()
			for _, provider := range providers {
				bucketName := fmt.Sprintf("%s-%s", provider, bucketNames[0])
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, timestamp)
				bkpLocationName = fmt.Sprintf("%s-%s-bl", provider, bucketNames[0])
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID)
				// TODO remove time.sleep: PA-509
				time.Sleep(time.Minute * 3)
				CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, bucketName, orgID, "")
			}
		})

		Step("Register cluster for backup", func() {
			CreateSourceAndDestClusters(orgID, "", "", ctx)
			clusterStatus, clusterUid = Inst().Backup.RegisterBackupCluster(orgID, SourceClusterName, "")
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, "Verifying backup cluster")
		})

		Step("Taking backups of application for each user", func() {
			log.InfoD("Taking backups of application for each user")
			var sem = make(chan struct{}, 10)
			var wg sync.WaitGroup
			for i := 0; i < numberOfUsers; i++ {
				sem <- struct{}{}
				time.Sleep(3 * time.Second)
				backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
				backupNames = append(backupNames, backupName)
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					defer func() { <-sem }()
					err = CreateBackup(backupName, SourceClusterName, bkpLocationName, backupLocationUID, []string{bkpNamespaces[0]},
						labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
					log.FailOnError(err, "Failed while trying to take backup of application- %s", bkpNamespaces[0])
				}(backupName)
			}
			wg.Wait()
			log.Infof("List of backups - %v", backupNames)
		})

		Step("Adding different roles to users and sharing backup with different access level", func() {
			userRoleAccessBackupList, err = AddRoleAndAccessToUsers(users, backupNames)
			dash.VerifyFatal(err, nil, "Adding roles and access level to users")
		})

		Step("Validating the shared backup with user having different access level and roles", func() {
			for key, val := range userRoleAccessBackupList {
				restoreName := fmt.Sprintf("%s-%s-%v", key.user, RestoreNamePrefix, time.Now().Unix())
				access := key.accesses
				if access != ViewOnlyAccess {
					userRestoreContext[key.context] = restoreName
				}
				ValidateSharedBackupWithUsers(key, val, restoreName)
			}
		})
	})
	JustAfterEach(func() {
		var wg sync.WaitGroup
		defer EndTorpedoTest()
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(contexts, opts)
		backupDriver := Inst().Backup
		for _, backupName := range backupNames {
			wg.Add(1)
			go func(backupName string) {
				defer wg.Done()
				backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
				log.FailOnError(err, "Error deleting user %v", backupName)
				DeleteBackup(backupName, backupUID, orgID, ctx)
			}(backupName)
		}
		wg.Wait()
		log.Infof("Deleting restore created by users")
		for userContext, restoreName := range userRestoreContext {
			DeleteRestore(restoreName, orgID, userContext)
		}
		log.Infof("Deleting registered clusters for non-admin context")
		for _, ctxNonAdmin := range userContextsList {
			DeleteCloudAccounts(make(map[string]string), "", "", ctxNonAdmin)
		}
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
		DeleteCloudAccounts(backupLocationMap, cloudCredName, cloudCredUID, ctx)

	})
})

// delete shared backups , validate that shared backups are deleted from owner
var _ = Describe("{DeleteSharedBackup}", func() {
	userName := "testuser1"
	firstName := "firstName"
	lastName := "lastName"
	email := "testuser1@cnbu.com"
	password := "Password1"
	numberOfBackups := 20
	backupNames := make([]string, 0)
	userContexts := make([]context.Context, 0)
	var contexts []*scheduler.Context
	var BucketName string
	var BackupLocationName string
	var backupLocationUID string
	var cloudCredUID string
	var CloudCredUidList []string
	var appContexts []*scheduler.Context
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var credName string
	bkpNamespaces = make([]string, 0)
	timestamp := time.Now().Unix()

	JustBeforeEach(func() {
		StartTorpedoTest("DeleteSharedBackup",
			"Share backup with multiple users and delete the backup", nil, 82937)
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
	It("Validate shared backups are deleted from owner of backup ", func() {
		providers := getProviders()
		buckets := getBucketName()
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(contexts)
		})
		Step("Create Users", func() {
			err = backup.AddUser(userName, firstName, lastName, email, password)
			dash.VerifyFatal(err, nil, "Verifying user creation")

		})
		Step("Adding Credentials and Registering Backup Location", func() {
			log.InfoD("Creating cloud credentials and backup location")
			for _, provider := range providers {
				BucketName = fmt.Sprintf("%s-%v", provider, buckets[0])
				cloudCredUID = uuid.New()
				CloudCredUidList = append(CloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", timestamp)
				CreateCloudCredential(provider, credName, cloudCredUID, orgID)
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				time.Sleep(time.Minute * 1)
				BackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", timestamp)
				CreateBackupLocation(provider, BackupLocationName, backupLocationUID, credName, cloudCredUID, BucketName, orgID, "")
				log.InfoD("Created Backup Location with name - %s", BackupLocationName)
			}
		})
		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
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
						CreateBackup(backupName, SourceClusterName, BackupLocationName, backupLocationUID, []string{namespace},
							nil, orgID, clusterUid, "", "", "", "", ctx)
					}(backupName)
				}
				wg.Wait()
			}
			log.Infof("List of backups - %v", backupNames)
		})

		Step("Share backup with user", func() {
			log.InfoD("Share backups with user")
			// Share backups with the user
			for _, backup := range backupNames {
				err = ShareBackup(backup, nil, []string{userName}, FullAccess, ctx)
				log.FailOnError(err, "Failed to share backup %s", backup)
				dash.VerifyFatal(err, nil, "Verifying backupshare")
			}
		})

		Step("Delete Shared Backups from user", func() {
			log.InfoD("register the Source and destination clutser of non-pxadmin")

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(userName, password)
			log.FailOnError(err, "Fetching non px-central-admin user ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context for user -%s", userName)
			CreateSourceAndDestClusters(orgID, "", "", ctxNonAdmin)

			// Validate that backups are shared with user
			log.Infof("Validating that backups are shared with %s user", userName)
			userBackups1, _ := GetAllBackupsForUser(userName, password)
			dash.VerifyFatal(len(userBackups1), numberOfBackups, fmt.Sprintf("Validating that user [%s] has access to all shared backups", userName))

			//Start deleting from user with whom the backups are shared
			var wg sync.WaitGroup
			backupDriver := Inst().Backup

			for _, backup := range backupNames {
				wg.Add(1)
				go func(backup string) {
					defer wg.Done()
					log.InfoD("Backup deletion started")
					backupUID, err := backupDriver.GetBackupUID(ctxNonAdmin, backup, orgID)
					backupDeleteResponse, err := DeleteBackup(backup, backupUID, orgID, ctxNonAdmin)
					log.FailOnError(err, "Backup [%s] could not be deleted by user [%s] with delete response %s", backup, userName, backupDeleteResponse)
					err = backupDriver.WaitForBackupDeletion(ctxNonAdmin, backup, orgID, time.Minute*30, time.Minute*1)
					log.FailOnError(err, "Error waiting for backup deletion %v", backup)
					dash.VerifyFatal(backupDeleteResponse.String(), "", "Verifying backup deletion is successful")

				}(backup)
			}
			wg.Wait()

		})
		Step("Validating that backups are deleted from owner of backups", func() {
			userBackups1, _ := GetAllBackupsAdmin()
			log.InfoD("%v", userBackups1)
			dash.VerifyFatal(len(userBackups1), 0, fmt.Sprintf("Validating that shared backups are deleted from owner of backup"))
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
		err := backup.DeleteUser(userName)
		log.FailOnError(err, "Error deleting user %v", userName)

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

	})

})

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
	timeStamp := time.Now().Unix()
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
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, timeStamp)
				cloudCredUID = uuid.New()
				CloudCredUIDMap[cloudCredUID] = cloudCredName
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
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, timeStamp)
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
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		DeleteCloudAccounts(backupLocationMap, cloudCredName, cloudCredUID, ctx)
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
	var bkpNamespaces []string
	var clusterUid string
	var cloudCredName string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	timeStamp := time.Now().Unix()
	bkpNamespaces = make([]string, 0)

	JustBeforeEach(func() {
		StartTorpedoTest("KillStorkWithBackupsAndRestoresInProgress", "Kill Strok when backups and restores in progress", nil, 0)
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
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, timeStamp)
				cloudCredUID = uuid.New()
				CloudCredUIDMap[cloudCredUID] = cloudCredName
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
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, timeStamp)
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

		Step("Kill stork when backup in progress", func() {
			log.InfoD("Kill stork when backup in progress")
			storkLabel := make(map[string]string)
			storkLabel["name"] = "stork"
			storkNamespace := getPXNamespace()
			pods, err := core.Instance().GetPods(storkNamespace, storkLabel)
			dash.VerifyFatal(err, nil, "Killing Stork pods")
			for _, pod := range pods.Items {
				err := core.Instance().DeletePod(pod.GetName(), storkNamespace, false)
				log.FailOnError(err, fmt.Sprintf("Failed to kill stork pod [%s]", pod.GetName()))
			}
		})

		Step("Check if backup is successful when the stork restart happened", func() {
			log.InfoD("Check if backup is successful post stork restarts")
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
		Step("Validate applications", func() {
			ValidateApplications(contexts)
		})
		Step("Restoring the backups application", func() {
			for _, namespace := range bkpNamespaces {
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, namespace, backupLocationName)
				CreateRestoreWithoutCheck(fmt.Sprintf("%s-restore", backupName), backupName, nil, SourceClusterName, orgID, ctx)
			}
		})
		Step("Kill stork when restore in-progress", func() {
			log.InfoD("Kill stork when restore in-progress")
			storkLabel := make(map[string]string)
			storkLabel["name"] = "stork"
			storkNamespace := getPXNamespace()
			pods, err := core.Instance().GetPods(storkNamespace, storkLabel)
			dash.VerifyFatal(err, nil, "Killing Stork pods")
			for _, pod := range pods.Items {
				err := core.Instance().DeletePod(pod.GetName(), storkNamespace, false)
				log.FailOnError(err, fmt.Sprintf("Failed to kill stork pod [%s]", pod.GetName()))
			}
		})
		Step("Check if restore is successful when the stork restart happened", func() {
			log.InfoD("Check if restore is successful post stork restarts")
			backupDriver := Inst().Backup
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, namespace, backupLocationName)
				restoreName := fmt.Sprintf("%s-restore", backupName)
				restoreSuccessCheck := func() (interface{}, bool, error) {
					restoreInspectRequest := &api.RestoreInspectRequest{
						Name:  restoreName,
						OrgId: orgID,
					}
					resp, err := Inst().Backup.InspectRestore(ctx, restoreInspectRequest)
					restoreResponseStatus := resp.GetRestore().GetStatus()
					log.FailOnError(err, "Failed verifying restore for - %s", restoreName)
					actual := restoreResponseStatus.GetStatus()
					expected := api.RestoreInfo_StatusInfo_PartialSuccess
					if actual != expected {
						log.Infof("Restore status - %s", restoreResponseStatus)
						log.InfoD("Status of %s - [%s]", restoreName, restoreResponseStatus.GetStatus())
						return "", true, fmt.Errorf("restore status expected was [%s] but got [%s]", expected, actual)
					}
					return "", false, nil
				}
				task.DoRetryWithTimeout(restoreSuccessCheck, 10*time.Minute, 30*time.Second)
				restoreInspectRequest := &api.RestoreInspectRequest{
					Name:  restoreName,
					OrgId: orgID,
				}
				resp, _ := backupDriver.InspectRestore(ctx, restoreInspectRequest)
				dash.VerifyFatal(resp.GetRestore().GetStatus().Status, api.RestoreInfo_StatusInfo_PartialSuccess, "Inspecting the Restore success for - "+resp.GetRestore().GetName())
			}
		})
		Step("Validate applications", func() {
			ValidateApplications(contexts)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		ctx, _ := backup.GetAdminCtxFromSecret()
		log.InfoD("Deleting the deployed apps after the testcase")
		for i := 0; i < len(contexts); i++ {
			opts := make(map[string]bool)
			opts[SkipClusterScopedObjects] = true
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			err := Inst().S.Destroy(contexts[i], opts)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verify destroying app %s, Err: %v", taskName, err))
		}

		log.InfoD("Deleting backup location, cloud creds and clusters")
		DeleteCloudAccounts(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// This test case creates a backup location with encryption
var _ = Describe("{BackupLocationWithEncryptionKey}", func() {
	var contexts []*scheduler.Context
	var appContexts []*scheduler.Context
	var bkpNamespaces []string
	var backupLocationName string
	var cloudCredUID string
	var clusterUid string
	var cloudCredName string
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
		cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", providers[0], time.Now().Unix())
		backupLocationName = fmt.Sprintf("%s-%s", "location", providers[0])
		cloudCredUID = uuid.New()
		BackupLocationUID = uuid.New()
		encryptionKey := generateEncryptionKey()
		CreateCloudCredential(providers[0], cloudCredName, CloudCredUID, orgID)
		CreateBackupLocation(providers[0], backupLocationName, BackupLocationUID, cloudCredName, CloudCredUID, bucketName, orgID, encryptionKey)
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
		DeleteCloudCredential(cloudCredName, orgID, cloudCredUID)
	})

})

// This testcase verifies resize after the volume is restored from a backup
var _ = Describe("{ResizeOnRestoredVolume}", func() {
	var (
		appList          = Inst().AppList
		contexts         []*scheduler.Context
		preRuleNameList  []string
		postRuleNameList []string
		appContexts      []*scheduler.Context
		bkpNamespaces    []string
		clusterUid       string
		clusterStatus    api.ClusterInfo_StatusInfo_Status
		restoreName      string
		namespaceMapping map[string]string
		credName         string
	)
	labelSelectors := make(map[string]string)
	CloudCredUIDMap := make(map[string]string)
	BackupLocationMap := make(map[string]string)
	var backupLocation string
	contexts = make([]*scheduler.Context, 0)
	bkpNamespaces = make([]string, 0)
	providers := getProviders()
	timeStamp := time.Now().Unix()

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
				credName = fmt.Sprintf("%s-%s-%v", "cred", provider, timeStamp)
				CloudCredUID = uuid.New()
				CloudCredUIDMap[CloudCredUID] = credName
				CreateCloudCredential(provider, credName, CloudCredUID, orgID)
			}
		})

		Step("Creating backup location", func() {
			log.InfoD("Creating backup location")
			bucketNames := getBucketName()
			for _, provider := range providers {
				credName = fmt.Sprintf("%s-%s-%v", "cred", provider, timeStamp)
				bucketName := fmt.Sprintf("%s-%s", provider, bucketNames[0])
				backupLocation = fmt.Sprintf("%s-%s", provider, bucketNames[0])
				BackupLocationUID = uuid.New()
				BackupLocationMap[BackupLocationUID] = backupLocation
				CreateBackupLocation(provider, backupLocation, BackupLocationUID, credName, CloudCredUID,
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
				backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, namespace, backupLocationName)
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
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		DeleteCloudAccounts(BackupLocationMap, credName, CloudCredUID, ctx)
	})
})

// This testcase verifies resize after same original volume is restored from a backup stored in a locked bucket
var _ = Describe("{LockedBucketResizeOnRestoredVolume}", func() {
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
		credName         string
	)
	labelSelectors := make(map[string]string)
	CloudCredUIDMap := make(map[string]string)
	BackupLocationMap := make(map[string]string)

	var backupLocation string
	contexts = make([]*scheduler.Context, 0)
	bkpNamespaces = make([]string, 0)
	providers := getProviders()
	timeStamp := time.Now().Unix()

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
				credName = fmt.Sprintf("%s-%s-%v", "cred", provider, timeStamp)
				CloudCredUID = uuid.New()
				CloudCredUIDMap[CloudCredUID] = credName
				CreateCloudCredential(provider, credName, CloudCredUID, orgID)
			}
		})

		Step("Creating a locked bucket and backup location", func() {
			log.InfoD("Creating locked buckets and backup location")
			bucketNames := getBucketName()
			modes := [2]string{"GOVERNANCE", "COMPLIANCE"}
			for _, provider := range providers {
				for _, mode := range modes {
					credName = fmt.Sprintf("%s-%s-%v", "cred", provider, timeStamp)
					bucketName := fmt.Sprintf("%s-%s-%s", provider, bucketNames[1], strings.ToLower(mode))
					backupLocation = fmt.Sprintf("%s-%s-%s-lock", provider, bucketNames[1], strings.ToLower(mode))
					err := CreateS3Bucket(bucketName, true, 3, mode)
					log.FailOnError(err, "Unable to create locked s3 bucket %s", bucketName)
					BackupLocationUID = uuid.New()
					BackupLocationMap[BackupLocationUID] = backupLocation
					CreateBackupLocation(provider, backupLocation, BackupLocationUID, credName, CloudCredUID,
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
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		DeleteCloudAccounts(BackupLocationMap, credName, CloudCredUID, ctx)
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
	var credName string
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
		credName = fmt.Sprintf("%s-%s-%v", "cred", providers[0], time.Now().Unix())
		CloudCredUID = uuid.New()
		BackupLocationUID = uuid.New()
		BackupLocation1UID = uuid.New()
		encryptionkey := "px-b@ckup-@utomat!on"
		CreateBucket(providers[0], encryptionBucketName)
		CreateCloudCredential(providers[0], credName, CloudCredUID, orgID)
		CreateBackupLocation(providers[0], backupLocationNames[0], BackupLocationUID, credName, CloudCredUID, bucketName, orgID, "")
		CreateBackupLocation(providers[0], backupLocationNames[1], BackupLocation1UID, credName, CloudCredUID, encryptionBucketName, orgID, encryptionkey)
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
		DeleteCloudCredential(credName, orgID, CloudCredUID)
		encryptionBucketName := fmt.Sprintf("%s-%s-%s", providers[0], bucketNames[0], "encryptionbucket")
		DeleteBucket(providers[0], encryptionBucketName)
	})

})

// This test does custom resource backup and restore.
var _ = Describe("{CustomResourceBackupAndRestore}", func() {
	namespaceMapping := make(map[string]string)
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
	timeStamp := time.Now().Unix()
	bkpNamespaces = make([]string, 0)

	JustBeforeEach(func() {
		StartTorpedoTest("CustomResourceBackupAndRestore", "Create custom resource backup and restore", nil, 58043)
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
	It("Create custom resource backup and restore", func() {
		Step("Validate applications", func() {
			ValidateApplications(contexts)
		})

		Step("Creating cloud credentials", func() {
			log.InfoD("Creating cloud credentials")
			providers := getProviders()
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, timeStamp)
				cloudCredUID = uuid.New()
				CloudCredUIDMap[cloudCredUID] = cloudCredName
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
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, timeStamp)
				bucketName := fmt.Sprintf("%s-%s", provider, bucketNames[0])
				backupLocation = fmt.Sprintf("%s-%s", provider, bucketNames[0])
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocation
				CreateBackupLocation(provider, backupLocation, backupLocationUID, cloudCredName, cloudCredUID,
					bucketName, orgID, "")
			}
		})
		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				err = CreateBackupWithCustomResourceType(backupName, SourceClusterName, backupLocation, backupLocationUID, []string{namespace}, nil, orgID, clusterUid, "", "", "", "", []string{"PersistentVolumeClaim"}, ctx)
				dash.VerifyFatal(err, nil, "Verifying backup creation with custom resources")
			}
		})

		Step("Restoring the backed up application", func() {
			log.InfoD("Restoring the backed up application")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, backupName)
				restoredNameSpace := fmt.Sprintf("%s-%s", namespace, "restored")
				namespaceMapping[namespace] = restoredNameSpace
				err = CreateRestore(restoreName, backupName, namespaceMapping, SourceClusterName, orgID, ctx)
				log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)
			}
		})
		Step("Compare PVCs on both namespaces", func() {
			log.InfoD("Compare PVCs on both namespaces")
			for _, namespace := range bkpNamespaces {
				pvcs, _ := core.Instance().GetPersistentVolumeClaims(namespace, labelSelectors)
				restoreNamespace := fmt.Sprintf("%s-%s", namespace, "restored")
				restoredpvcs, _ := core.Instance().GetPersistentVolumeClaims(restoreNamespace, labelSelectors)
				dash.VerifyFatal(len(pvcs.Items), len(restoredpvcs.Items), "Compare number of PVCs")
			}
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		ctx, _ := backup.GetAdminCtxFromSecret()
		log.InfoD("Deleting the deployed apps after the testcase")
		for i := 0; i < len(contexts); i++ {
			opts := make(map[string]bool)
			opts[SkipClusterScopedObjects] = true
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			err := Inst().S.Destroy(contexts[i], opts)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verify destroying app %s", taskName))
		}
		log.InfoD("Deleting backup location, cloud creds and clusters")
		DeleteCloudAccounts(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// Change replica while restoring backup through StorageClass Mapping.
var _ = Describe("{ReplicaChangeWhileRestore}", func() {
	namespaceMapping := make(map[string]string)
	storageClassMapping := make(map[string]string)
	var contexts []*scheduler.Context
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
	timestamp := strconv.Itoa(int(time.Now().Unix()))
	labelSelectors := make(map[string]string)

	JustBeforeEach(func() {
		StartTorpedoTest("ReplicaChangeWhileRestore", "Change replica while restoring backup", nil, 58043)
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
	It("Change replica while restoring backup", func() {
		Step("Validate applications", func() {
			ValidateApplications(contexts)
		})

		Step("Creating cloud credentials", func() {
			log.InfoD("Creating cloud credentials")
			providers := getProviders()
			for _, provider := range providers {
				cloudCredName := fmt.Sprintf("%s-%s-%s", "cred", provider, timestamp)
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
				cloudCredName := fmt.Sprintf("%s-%s-%v", "cred", provider, timestamp)
				bucketName := fmt.Sprintf("%s-%s", provider, bucketNames[0])
				backupLocation = fmt.Sprintf("%s-%s", provider, bucketNames[0])
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocation
				CreateBackupLocation(provider, backupLocation, backupLocationUID, cloudCredName, cloudCredUID,
					bucketName, orgID, "")
			}
		})
		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				err = CreateBackup(backupName, SourceClusterName, backupLocation, backupLocationUID, []string{namespace}, labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
				dash.VerifyFatal(err, nil, "Verifying backup creation with custom resources")
			}
		})
		Step("Create new storage class for restore", func() {
			log.InfoD("Create new storage class for restore")
			scName := fmt.Sprintf("replica-sc-%s", timestamp)
			params := make(map[string]string)
			params["repl"] = "2"
			k8sStorage := storage.Instance()
			v1obj := meta_v1.ObjectMeta{
				Name: scName,
			}
			reclaimPolicyDelete := v1.PersistentVolumeReclaimDelete
			bindMode := storageapi.VolumeBindingImmediate
			scObj := storageapi.StorageClass{
				ObjectMeta:        v1obj,
				Provisioner:       k8s.CsiProvisioner,
				Parameters:        params,
				ReclaimPolicy:     &reclaimPolicyDelete,
				VolumeBindingMode: &bindMode,
			}
			_, err := k8sStorage.CreateStorageClass(&scObj)
			dash.VerifyFatal(err, nil, "Verifying creation of new storage class")
		})

		Step("Restoring the backed up application", func() {
			log.InfoD("Restoring the backed up application")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s", BackupNamePrefix, namespace)
				restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, backupName)
				scName := fmt.Sprintf("replica-sc-%s", timestamp)
				pvcs, err := core.Instance().GetPersistentVolumeClaims(namespace, labelSelectors)
				singlePvc := pvcs.Items[0]
				dash.VerifyFatal(err, nil, "Getting PVC from namespace")
				sourceScName, err := core.Instance().GetStorageClassForPVC(&singlePvc)
				dash.VerifyFatal(err, nil, "Getting SC from PVC")
				storageClassMapping[sourceScName.Name] = scName
				restoredNameSpace := fmt.Sprintf("%s-%s", namespace, "restored")
				namespaceMapping[namespace] = restoredNameSpace
				err = CreateRestoreWithCustomStorageClass(restoreName, backupName, namespaceMapping, storageClassMapping, SourceClusterName, orgID, ctx)
				dash.VerifyFatal(err, nil, "Restoring with custom Storage Class Mapping")
			}
		})
		Step("Validate applications", func() {
			ValidateApplications(contexts)
		})
	})
	JustAfterEach(func() {
		defer EndTorpedoTest()
		ctx, _ := backup.GetAdminCtxFromSecret()
		log.InfoD("Deleting the deployed apps after the testcase")
		for i := 0; i < len(contexts); i++ {
			opts := make(map[string]bool)
			opts[SkipClusterScopedObjects] = true
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			err := Inst().S.Destroy(contexts[i], opts)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verify destroying app %s", taskName))
		}
		log.InfoD("Deleting backup location, cloud creds and clusters")
		DeleteCloudAccounts(backupLocationMap, cloudCredName, cloudCredUID, ctx)
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

// getPXNamespace fetches px namespace from env else sends backup kube-system
func getPXNamespace() string {
	namespace := os.Getenv("PX_NAMESPACE")
	if namespace != "" {
		return namespace
	}
	return storkDeploymentNamespace
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

	task.DoRetryWithTimeout(backupSuccessCheck, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)

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
	log.Infof("Backup [%s] created successfully", backupName)
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

// CreateBackup creates backup with custom resources
func CreateBackupWithCustomResourceType(backupName string, clusterName string, bLocation string, bLocationUID string,
	namespaces []string, labelSelectors map[string]string, orgID string, uid string, preRuleName string,
	preRuleUid string, postRuleName string, postRuleUid string, resourceType []string, ctx context.Context) error {

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
		ResourceTypes: resourceType,
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
	curBackups, err := backupDriver.EnumerateBackup(ctx, bkpEnumerateReq)
	log.FailOnError(err, "Enumerate backup failed for the request - %v", bkpEnumerateReq)
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
	_, err = backupDriver.CreateRestore(ctx, createRestoreReq)
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

// CreateRestoreWithoutCheck creates restore without waiting for completion
func CreateRestoreWithoutCheck(restoreName string, backupName string,
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
	return nil
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

func createUsers(numberOfUsers int) []string {
	users := make([]string, 0)
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
	return users
}

func DeleteCloudAccounts(backupLocationMap map[string]string, credName string, cloudCredUID string, ctx context.Context) {
	if len(backupLocationMap) != 0 {
		for backupLocationUID, bkpLocationName := range backupLocationMap {
			DeleteBackupLocation(bkpLocationName, backupLocationUID, orgID)
		}
		time.Sleep(time.Minute * 3)
		DeleteCloudCredential(credName, orgID, cloudCredUID)
	}
	DeleteCluster(SourceClusterName, orgID, ctx)
	DeleteCluster(destinationClusterName, orgID, ctx)
}

// AddRoleAndAccessToUsers assigns role and access level to the users
// AddRoleAndAccessToUsers return map whose key is userRoleAccess and value is backup for each user
func AddRoleAndAccessToUsers(users []string, backupNames []string) (map[userRoleAccess]string, error) {
	var access BackupAccess
	var role backup.PxBackupRole
	RoleAccessUserBackupContext := make(map[userRoleAccess]string)
	ctx, err := backup.GetAdminCtxFromSecret()
	log.FailOnError(err, "Fetching px-central-admin ctx")
	for i := 0; i < len(users); i++ {
		userIndex := i % 9
		switch userIndex {
		case 0:
			access = ViewOnlyAccess
			role = backup.ApplicationOwner
		case 1:
			access = RestoreAccess
			role = backup.ApplicationOwner
		case 2:
			access = FullAccess
			role = backup.ApplicationOwner
		case 3:
			access = ViewOnlyAccess
			role = backup.ApplicationUser
		case 4:
			access = RestoreAccess
			role = backup.ApplicationUser
		case 5:
			access = FullAccess
			role = backup.ApplicationUser
		case 6:
			access = ViewOnlyAccess
			role = backup.InfrastructureOwner
		case 7:
			access = RestoreAccess
			role = backup.InfrastructureOwner
		case 8:
			access = FullAccess
			role = backup.InfrastructureOwner
		default:
			access = ViewOnlyAccess
			role = backup.ApplicationOwner
		}
		ctxNonAdmin, err := backup.GetNonAdminCtx(users[i], "Password1")
		log.FailOnError(err, "Fetching user ctx")
		userRoleAccessContext := userRoleAccess{users[i], role, access, ctxNonAdmin}
		RoleAccessUserBackupContext[userRoleAccessContext] = backupNames[i]
		err = backup.AddRoleToUser(users[i], role, "Adding role to user")
		if err != nil {
			err = fmt.Errorf("Failed to add role %s to user  %s with err %v", role, users[i], err)
			return nil, err
		}
		err = ShareBackup(backupNames[i], nil, []string{users[i]}, access, ctx)
		if err != nil {
			return nil, err
		}
		log.Infof(" Backup %s shared with user %s", backupNames[i], users[i])
	}
	return RoleAccessUserBackupContext, nil
}

func ValidateSharedBackupWithUsers(key userRoleAccess, backupName string, restoreName string) {
	ctx, err := backup.GetAdminCtxFromSecret()
	log.FailOnError(err, "Fetching px-central-admin ctx")
	log.InfoD("Registering Source and Destination clusters from user context")
	CreateSourceAndDestClusters(orgID, "", "", key.context)
	log.InfoD("Validating if user [%s] with access [%v] and [%v] role can restore and delete backup or not", key.user, backupAccessKeyValue[key.accesses], key.roles)
	backupDriver := Inst().Backup
	switch key.accesses {
	case ViewOnlyAccess:
		// Try restore with user having ViewOnlyAccess and it should fail
		err := CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, key.context)
		dash.VerifyFatal(strings.Contains(err.Error(), "failed to retrieve backup location"), true, "Verifying backup restore is not possible")
		// Try to delete the backup with user having ViewOnlyAccess, and it should not pass
		backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
		log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
		// Delete backup to confirm that the user has ViewOnlyAccess and cannot delete backup
		_, err = DeleteBackup(backupName, backupUID, orgID, key.context)
		dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to delete backup"), true, "Verifying backup deletion is not possible")

	case RestoreAccess:
		// Try restore with user having RestoreAccess and it should pass
		err := CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, key.context)
		dash.VerifyFatal(err, nil, "Verifying that restore is possible")
		// Try to delete the backup with user having RestoreAccess, and it should not pass
		backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
		log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
		// Delete backup to confirm that the user has Restore Access and delete backup should fail
		_, err = DeleteBackup(backupName, backupUID, orgID, key.context)
		dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to delete backup"), true, "Verifying backup deletion is not possible")

	case FullAccess:
		// Try restore with user having FullAccess, and it should pass
		err := CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, key.context)
		dash.VerifyFatal(err, nil, "Verifying that restore is possible")
		// Try to delete the backup with user having FullAccess, and it should  pass
		backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
		log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
		// Delete backup to confirm that the user has Full Access
		_, err = DeleteBackup(backupName, backupUID, orgID, key.context)
		dash.VerifyFatal(err, nil, "Verifying that delete backup is possible")
	}
}

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
func getEnv(environmentVariable string, defaultValue string) string {
	value, present := os.LookupEnv(environmentVariable)
	if !present {
		value = defaultValue
	}
	return value
}

// GetAllBackupsAdmin returns all the backups that px-central-admin has access toGetAllBackupsAdmin
func GetAllBackupsAdmin() ([]string, error) {
	var bkp *api.BackupObject
	backupNames := make([]string, 0)
	backupDriver := Inst().Backup
	ctx, err := backup.GetAdminCtxFromSecret()
	log.FailOnError(err, "Fetching ctx")

	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: orgID}
	curBackups, err := backupDriver.EnumerateBackup(ctx, bkpEnumerateReq)
	log.FailOnError(err, "Falied to enumerate on backup objects")
	for _, bkp = range curBackups.GetBackups() {
		backupNames = append(backupNames, bkp.GetName())
	}
	return backupNames, err
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

// CreateRestoreWithCustomStorageClass creates restore with StorageClass mapping
func CreateRestoreWithCustomStorageClass(restoreName string, backupName string,
	namespaceMapping map[string]string, storageClassMapping map[string]string, clusterName string, orgID string, ctx context.Context) error {

	var bkp *api.BackupObject
	var bkpUid string
	backupDriver := Inst().Backup
	log.Infof("Getting the UID of the backup needed to be restored")
	bkpEnumerateReq := &api.BackupEnumerateRequest{
		OrgId: orgID}
	curBackups, err := backupDriver.EnumerateBackup(ctx, bkpEnumerateReq)
	log.FailOnError(err, "Enumerate backup failed for the request - %v", bkpEnumerateReq)
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
		StorageClassMapping: storageClassMapping,
		BackupRef: &api.ObjectRef{
			Name: backupName,
			Uid:  bkpUid,
		},
	}
	_, err = backupDriver.CreateRestore(ctx, createRestoreReq)
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
		if err != nil {
			return "", true, fmt.Errorf("Failed verifying restore for - %s", restoreName)
		}
		if restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_PartialSuccess || restoreResponseStatus.GetStatus() == api.RestoreInfo_StatusInfo_Success {
			log.Infof("Restore status - %s", restoreResponseStatus)
			log.InfoD("Status of %s - [%s]",
				restoreName, restoreResponseStatus.GetStatus())
			return "", false, nil
		}
		return "", true, fmt.Errorf("expected status of %s - [%s] or [%s], but got [%s]",
			restoreName, api.RestoreInfo_StatusInfo_PartialSuccess.String(), api.RestoreInfo_StatusInfo_Success, restoreResponseStatus.GetStatus())
	}
	_, err = task.DoRetryWithTimeout(restoreSuccessCheck, 10*time.Minute, 30*time.Second)
	return err
}
