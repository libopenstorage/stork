package tests

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	v1 "k8s.io/api/core/v1"
	storageApi "k8s.io/api/storage/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// This is to create multiple users and groups
var _ = Describe("{CreateMultipleUsersAndGroups}", func() {
	numberOfUsers := 20
	numberOfGroups := 10
	users := make([]string, 0)
	groups := make([]string, 0)
	userValidation := make([]string, 0)
	groupValidation := make([]string, 0)
	var groupNotCreated string
	var userNotCreated string

	JustBeforeEach(func() {
		StartTorpedoTest("CreateMultipleUsersAndGroups", "Creation of multiple users and groups", nil, 58032)
	})
	It("Create multiple users and Group", func() {

		Step("Create Groups", func() {
			log.InfoD("Creating %d groups", numberOfGroups)
			var wg sync.WaitGroup
			for i := 1; i <= numberOfGroups; i++ {
				groupName := fmt.Sprintf("testGroup%v", time.Now().Unix())
				time.Sleep(2 * time.Second)
				wg.Add(1)
				go func(groupName string) {
					defer GinkgoRecover()
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
				userName := fmt.Sprintf("testuser%v", time.Now().Unix())
				firstName := fmt.Sprintf("FirstName%v", i)
				lastName := fmt.Sprintf("LastName%v", i)
				email := fmt.Sprintf("testuser%v@cnbu.com", time.Now().Unix())
				time.Sleep(2 * time.Second)
				wg.Add(1)
				go func(userName, firstName, lastName, email string) {
					defer GinkgoRecover()
					defer wg.Done()
					err := backup.AddUser(userName, firstName, lastName, email, commonPassword)
					log.FailOnError(err, "Failed to create user - %s", userName)
					users = append(users, userName)
				}(userName, firstName, lastName, email)
			}
			wg.Wait()
		})

		//iterates through the group names slice and checks if the group name is present in the response map using map[key]
		//to get the value and key to check whether it is present or not.
		//If it's not found, it prints the group name not found in struct slice and exit.

		Step("Validate Group", func() {
			createdGroups, err := backup.GetAllGroups()
			log.FailOnError(err, "Failed to get group")
			responseMap := make(map[string]bool)
			for _, createdGroup := range createdGroups {
				groupValidation = append(groupValidation, createdGroup.Name)
				responseMap[createdGroup.Name] = true
			}
			for _, group := range groups {
				if _, ok := responseMap[group]; !ok {
					groupNotCreated = group
					err = fmt.Errorf("group Name not created - [%s]", group)
					log.FailOnError(err, "Failed to create the group")
					break
				}

			}
			log.Infof("Validating created groups %v", groupValidation)
			dash.VerifyFatal(groupNotCreated, "", fmt.Sprintf("Group %v Creation Verification", groups))
		})

		Step("Validate User", func() {
			createdUsers, err := backup.GetAllUsers()
			log.FailOnError(err, "Failed to get user")
			responseMap := make(map[string]bool)
			for _, createdUser := range createdUsers {
				userValidation = append(userValidation, createdUser.Name)
				responseMap[createdUser.Name] = true
			}
			for _, user := range users {
				if _, ok := responseMap[user]; !ok {
					userNotCreated = user
					err = fmt.Errorf("user name not created - [%s]", user)
					log.FailOnError(err, "Failed to create the user")
					break
				}

			}
			log.Infof("Validating created users %v", userValidation)
			dash.VerifyFatal(userNotCreated, "", fmt.Sprintf("Users %v creation verification", users))
		})

	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(make([]*scheduler.Context, 0))
		log.Infof("Cleanup started")
		err := backup.DeleteMultipleGroups(groups)
		dash.VerifySafely(err, nil, fmt.Sprintf("Delete Groups %v", groups))
		err = backup.DeleteMultipleUsers(users)
		dash.VerifySafely(err, nil, fmt.Sprintf("Delete users %v", users))
		log.Infof("Cleanup done")
	})
})

// Validate that user can't duplicate a shared backup without registering the cluster
var _ = Describe("{DuplicateSharedBackup}", func() {
	userName := "testuser1"
	firstName := "firstName"
	lastName := "lastName"
	email := "testuser10@cnbu.com"
	numberOfBackups := 1
	var backupName string
	userContexts := make([]context.Context, 0)
	var scheduledAppContexts []*scheduler.Context
	var backupLocationName string
	var backupLocationUID string
	var cloudCredUID string
	var cloudCredUidList []string
	var appContexts []*scheduler.Context
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var credName string
	bkpNamespaces = make([]string, 0)
	backupLocationMap := make(map[string]string)

	JustBeforeEach(func() {
		StartTorpedoTest("DuplicateSharedBackup",
			"Share backup with user and duplicate it", nil, 82942)
		log.InfoD("Deploy applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})
	It("Validate shared backup is not duplicated without cluster", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		providers := getProviders()
		backupName = fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Create User", func() {
			err = backup.AddUser(userName, firstName, lastName, email, commonPassword)
			log.FailOnError(err, "Failed to create user - %s", userName)

		})
		Step("Adding Credentials and Registering Backup Location", func() {
			log.InfoD("Creating cloud credentials and backup location")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err = CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				backupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationMap[backupLocationUID] = backupLocationName
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %v", backupLocationName))
				log.InfoD("Created Backup Location with name - %s", backupLocationName)
			}
		})
		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Taking backup of applications", func() {
			log.InfoD("Taking Backup of application")
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespaces[0]})
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, nil, orgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
		})

		Step("Share backup with user", func() {
			log.InfoD("Share backup with  user having full access")
			err := ShareBackup(backupName, nil, []string{userName}, FullAccess, ctx)
			log.FailOnError(err, "Failed to share backup %s", backupName)
		})

		Step("Duplicate shared backup", func() {
			log.InfoD("Validating to duplicate share backup without adding cluster")
			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(userName, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Validate that backups are shared with user
			log.Infof("Validating that backups are shared with %s user", userName)
			userBackups1, err := GetAllBackupsForUser(userName, commonPassword)
			log.FailOnError(err, "Not able to fetch backup for user %s", userName)
			dash.VerifyFatal(len(userBackups1), numberOfBackups, fmt.Sprintf("Validating that user [%s] has access to all shared backups [%v]", userName, userBackups1))

			//to duplicate shared backup internally it calls create backup api
			log.Infof("Duplicate shared backup")
			err = CreateBackup(backupName, SourceClusterName, backupLocationName, backupLocationUID, []string{bkpNamespaces[0]},
				nil, orgID, clusterUid, "", "", "", "", ctxNonAdmin)
			log.Infof("user not able to duplicate shared backup without adding cluster with err - %v", err)
			errMessage := fmt.Sprintf("NotFound desc = failed to retrieve cluster [%s]: object not found", SourceClusterName)
			dash.VerifyFatal(strings.Contains(err.Error(), errMessage), true, "Verifying that shared backup can't be duplicated without adding cluster")
		})

	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

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
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})

})

// DifferentAccessSameUser shares backup to user with Viewonly access who is part of group with FullAccess
var _ = Describe("{DifferentAccessSameUser}", func() {
	var (
		scheduledAppContexts []*scheduler.Context
		bkpNamespaces        []string
		clusterUid           string
		clusterStatus        api.ClusterInfo_StatusInfo_Status
		groupName            string
		userNames            []string
		backupName           string
		backupLocationUID    string
		cloudCredName        string
		cloudCredUID         string
		bkpLocationName      string
	)
	userContexts := make([]context.Context, 0)
	backupLocationMap := make(map[string]string)
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	numberOfUsers := 1
	JustBeforeEach(func() {
		StartTorpedoTest("DifferentAccessSameUser",
			"Take a backup and add user with readonly access and the group  with full access", nil, 82938)
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
	It("Different Access Same User", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		Step("Validate applications", func() {
			log.InfoD("Validate applications ")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Create Users", func() {
			log.InfoD("Creating users testuser")
			userNames = createUsers(numberOfUsers)
			log.Infof("Created %v users and users list is %v", numberOfUsers, userNames)
		})
		Step("Create Groups", func() {
			log.InfoD("Creating group testGroup")
			groupName = fmt.Sprintf("testGroup")
			err := backup.AddGroup(groupName)
			log.FailOnError(err, "Failed to create group - %v", groupName)

		})
		Step("Add users to group", func() {
			log.InfoD("Adding user to groups")
			err := backup.AddGroupToUser(userNames[0], groupName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Adding user %s to group %s", userNames[0], groupName))
			usersOfGroup, err := backup.GetMembersOfGroup(groupName)
			log.FailOnError(err, "Error fetching members of the group - %v", groupName)
			log.Infof("Group [%v] contains the following users: \n%v", groupName, usersOfGroup)

		})
		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			providers := getProviders()
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cloudcred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("%s-%s-%v-bl", provider, getGlobalBucketName(provider), time.Now().Unix())
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", bkpLocationName))
			}
		})
		Step("Register cluster for backup", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			log.InfoD("Uid of [%s] cluster is %s", SourceClusterName, clusterUid)
		})
		Step("Taking backup of applications", func() {
			backupName = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, bkpNamespaces[0], time.Now().Unix())
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespaces[0]})
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
		})
		Step("Share backup with user having viewonly access", func() {
			log.InfoD("Share backup with user having viewonly access")
			err = ShareBackup(backupName, nil, userNames, ViewOnlyAccess, ctx)
			log.FailOnError(err, "Failed to share backup %s", backupName)
		})
		Step("Share backup with group having full access", func() {
			log.InfoD("Share backup with group having full access")
			err = ShareBackup(backupName, []string{groupName}, nil, FullAccess, ctx)
			log.FailOnError(err, "Failed to share backup %s", backupName)
		})
		Step("Share Backup with View Only access to a user of Full access group and Validate", func() {
			log.InfoD("Backup is shared with Group having FullAccess after it is shared with user having ViewOnlyAccess, therefore user should have FullAccess")
			ctxNonAdmin, err := backup.GetNonAdminCtx(userNames[0], commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContexts = append(userContexts, ctxNonAdmin)
			log.InfoD("Registering Source and Destination clusters from user context")
			err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			// Try restore with user having RestoreAccess and it should pass
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s to validate user can delete restore  ", restoreName)
			err = DeleteRestore(restoreName, orgID, ctxNonAdmin)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
			backupDeleteResponse, err := DeleteBackup(backupName, backupUID, orgID, ctxNonAdmin)
			log.FailOnError(err, "Backup [%s] could not be deleted by user [%s] with delete response %s", backupName, userNames, backupDeleteResponse)
			dash.VerifyFatal(backupDeleteResponse.String(), "", fmt.Sprintf("Verifying backup %s deletion", backupName))
		})
	})

	JustAfterEach(func() {
		// For all the delete methods we need to add return and handle the error here
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		log.Infof("Generating user context")
		ctxNonAdmin, err := backup.GetNonAdminCtx(userNames[0], commonPassword)
		log.FailOnError(err, "Fetching non admin ctx")
		log.Infof("Deleting registered clusters for non-admin context")
		CleanupCloudSettingsAndClusters(make(map[string]string), "", "", ctxNonAdmin)
		err = backup.DeleteUser(userNames[0])
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting user %s", userNames[0]))
		err = backup.DeleteGroup(groupName)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting group %s", groupName))
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// ShareBackupWithUsersAndGroups shares backup with multiple users and groups with different access
var _ = Describe("{ShareBackupWithUsersAndGroups}", func() {
	numberOfUsers := 30
	numberOfGroups := 3
	groupSize := 10
	numberOfBackups := 9
	users := make([]string, 0)
	groups := make([]string, 0)
	backupNames := make([]string, 0)
	userContexts := make([]context.Context, 0)
	var scheduledAppContexts []*scheduler.Context
	labelSelectors := make(map[string]string)
	var backupLocationUID string
	var cloudCredUID string
	var cloudCredUidList []string
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var customBackupLocationName string
	var credName string
	bkpNamespaces = make([]string, 0)
	backupLocationMap := make(map[string]string)
	userContextsList := make([]context.Context, 0)
	var chosenUser string

	JustBeforeEach(func() {
		StartTorpedoTest("ShareBackupWithUsersAndGroups",
			"Share large number of backups with multiple users and groups with View only, Restore and Full Access", nil, 82934)
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
	It("Share large number of backups", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Create Users", func() {
			log.InfoD("Creating %d users", numberOfUsers)
			var wg sync.WaitGroup
			for i := 1; i <= numberOfUsers; i++ {
				userName := fmt.Sprintf("testuser%v", i)
				firstName := fmt.Sprintf("FirstName%v", i)
				lastName := fmt.Sprintf("LastName%v", i)
				email := fmt.Sprintf("testuser%v_%v@cnbu.com", i, time.Now().Unix())
				wg.Add(1)
				go func(userName, firstName, lastName, email string) {
					defer GinkgoRecover()
					defer wg.Done()
					err := backup.AddUser(userName, firstName, lastName, email, commonPassword)
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
					defer GinkgoRecover()
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
			for i, userName := range users {
				groupIndex := i / groupSize
				wg.Add(1)
				go func(userName string, groupIndex int) {
					defer GinkgoRecover()
					defer wg.Done()
					err := backup.AddGroupToUser(userName, groups[groupIndex])
					log.FailOnError(err, "Failed to assign group to user")
				}(userName, groupIndex)
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
			log.InfoD("Creating cloud credentials and backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err := CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				customBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationMap[backupLocationUID] = customBackupLocationName
				err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
				log.InfoD("Created Backup Location with name - %s", customBackupLocationName)
			}
		})

		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
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
					time.Sleep(10 * time.Second)
					backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
					backupNames = append(backupNames, backupName)
					wg.Add(1)
					go func(backupName string) {
						defer GinkgoRecover()
						defer wg.Done()
						defer func() { <-sem }()
						appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
						err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
						dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
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
			var err error
			chosenUser, err = backup.GetRandomUserFromGroup(groups[0])
			log.FailOnError(err, "Failed to get a random user from group [%s]", groups[0])
			log.Infof("Sharing backup with user - %s", chosenUser)

			// Get Admin Context - needed to share backup and get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			// Share backup with the user
			backupName := backupNames[0]
			err = ShareBackup(backupName, nil, []string{chosenUser}, FullAccess, ctx)
			log.FailOnError(err, "Failed to share backup %s", backupName)

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(chosenUser, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")

			// Start Restore
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)

			// Restore validation to make sure that the user with Full Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			err = DeleteRestore(restoreName, orgID, ctxNonAdmin)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))

			// Get Backup UID
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)

			// Delete backup to confirm that the user has Full Access
			backupDeleteResponse, err := DeleteBackup(backupName, backupUID, orgID, ctxNonAdmin)
			log.FailOnError(err, "Backup [%s] could not be deleted by user [%s]", backupName, chosenUser)
			dash.VerifyFatal(backupDeleteResponse.String(), "", fmt.Sprintf("Verifying backup %s deletion", backupName))
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
			ctxNonAdmin, err := backup.GetNonAdminCtx(username, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")

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
			ctxNonAdmin, err := backup.GetNonAdminCtx(username, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")

			// Start Restore
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)

			// Restore validation to make sure that the user with can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			err = DeleteRestore(restoreName, orgID, ctxNonAdmin)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))

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
			log.Infof("Sharing backup with user - %s", username)

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(username, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Get Admin Context - needed to share backup and get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")

			// Start Restore
			backupName := backupNames[3]
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)

			// Restore validation to make sure that the user with can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			err = DeleteRestore(restoreName, orgID, ctxNonAdmin)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))
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
			log.Infof("Sharing backup with user - %s", chosenUser)

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(chosenUser, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")

			// Start Restore
			backupName := backupNames[2]
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))
			log.Infof("Error while trying to restore - %s", err.Error())
			// Restore validation to make sure that the user with View Access cannot restore
			dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to restore backup"), true, "Verifying backup restore is not possible")

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
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		log.Infof("Generating user context")
		for _, userName := range users {
			ctxNonAdmin, err := backup.GetNonAdminCtx(userName, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContextsList = append(userContextsList, ctxNonAdmin)
		}
		log.Infof("Deleting registered clusters for non-admin context")
		for _, ctxNonAdmin := range userContextsList {
			CleanupCloudSettingsAndClusters(make(map[string]string), "", "", ctxNonAdmin)
		}

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

		log.Infof("Cleaning up groups")
		for _, groupName := range groups {
			wg.Add(1)
			go func(groupName string) {
				defer GinkgoRecover()
				defer wg.Done()
				err := backup.DeleteGroup(groupName)
				log.FailOnError(err, "Error deleting user %v", groupName)
			}(groupName)
		}
		wg.Wait()

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})

// ShareLargeNumberOfBackupsWithLargeNumberOfUsers shares large number of backups to large number of users
var _ = Describe("{ShareLargeNumberOfBackupsWithLargeNumberOfUsers}", func() {
	numberOfUsers, _ := strconv.Atoi(getEnv(usersToBeCreated, "200"))
	numberOfGroups, _ := strconv.Atoi(getEnv(groupsToBeCreated, "100"))
	groupSize, _ := strconv.Atoi(getEnv(maxUsersInGroup, "2"))
	numberOfBackups, _ := strconv.Atoi(getEnv(maxBackupsToBeCreated, "100"))
	timeBetweenConsecutiveBackups := 10 * time.Second
	users := make([]string, 0)
	groups := make([]string, 0)
	backupNames := make([]string, 0)
	numberOfSimultaneousBackups := 20
	var scheduledAppContexts []*scheduler.Context
	labelSelectors := make(map[string]string)
	var backupLocationUID string
	var cloudCredUID string
	var cloudCredUidList []string
	userContexts := make([]context.Context, 0)
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var customBackupLocationName string
	var credName string
	var chosenUser string
	backupLocationMapNew := make(map[string]string)
	bkpNamespaces = make([]string, 0)

	JustBeforeEach(func() {
		StartTorpedoTest("ShareLargeNumberOfBackupsWithLargeNumberOfUsers",
			"Share large number of backups to large number of users", nil, 82941)
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
	It("Share all backups at cluster level with a user group and revoke it and validate", func() {
		providers := getProviders()
		Step("Validate applications and get their labels", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
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
					err := backup.AddUser(userName, firstName, lastName, email, commonPassword)
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
					defer GinkgoRecover()
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
			for i, user := range users {
				groupIndex := i / groupSize
				wg.Add(1)
				go func(userName string, groupIndex int) {
					defer GinkgoRecover()
					defer wg.Done()
					err := backup.AddGroupToUser(userName, groups[groupIndex])
					log.FailOnError(err, "Failed to assign group to user")
				}(user, groupIndex)
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
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err := CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				customBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
				log.InfoD("Created Backup Location with name - %s", customBackupLocationName)
				backupLocationMapNew[backupLocationUID] = customBackupLocationName
			}
		})

		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
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
					time.Sleep(timeBetweenConsecutiveBackups)
					backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
					backupNames = append(backupNames, backupName)
					sem <- struct{}{}
					wg.Add(1)
					go func(backupName, namespace string) {
						defer GinkgoRecover()
						defer wg.Done()
						defer func() { <-sem }()
						appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
						err := CreateBackupWithValidation(ctx, backupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
						dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
					}(backupName, namespace)
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
			ctxNonAdmin, err := backup.GetNonAdminCtx(chosenUser, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")

			// Start Restore
			backupName := backupNames[rand.Intn(numberOfBackups-1)]
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)

			// Restore validation to make sure that the user with Full Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			err = DeleteRestore(restoreName, orgID, ctxNonAdmin)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))

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
			ctxNonAdmin, err := backup.GetNonAdminCtx(chosenUser, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")

			// Start Restore
			backupName := backupNames[rand.Intn(numberOfBackups-1)]
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)

			// Restore validation to make sure that the user with Restore Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			err = DeleteRestore(restoreName, orgID, ctxNonAdmin)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))

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
			ctxNonAdmin, err := backup.GetNonAdminCtx(chosenUser, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")

			// Start Restore
			backupName := backupNames[rand.Intn(numberOfBackups-1)]
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))

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
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

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

		log.Infof("Cleaning up groups")
		for _, groupName := range groups {
			wg.Add(1)
			go func(groupName string) {
				defer GinkgoRecover()
				defer wg.Done()
				err := backup.DeleteGroup(groupName)
				log.FailOnError(err, "Error deleting user %v", groupName)
			}(groupName)
		}
		wg.Wait()

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(backupLocationMapNew, credName, cloudCredUID, ctx)

		log.Infof("Deleting registered clusters for non-admin context")
		for _, ctxNonAdmin := range userContexts {
			CleanupCloudSettingsAndClusters(make(map[string]string), "", "", ctxNonAdmin)
		}

	})
})

// CancelClusterBackupShare shares all backup at cluster level with a user group and revokes it and validate
var _ = Describe("{CancelClusterBackupShare}", func() {
	numberOfUsers := 10
	numberOfGroups := 1
	groupSize := 10
	numberOfBackups := 6
	users := make([]string, 0)
	groups := make([]string, 0)
	backupNames := make([]string, 0)
	userContexts := make([]context.Context, 0)
	var scheduledAppContexts []*scheduler.Context
	labelSelectors := make(map[string]string)
	var backupLocationUID string
	var cloudCredUID string
	var cloudCredUidList []string
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var customBackupLocationName string
	var credName string
	var chosenUser string
	individualUser := "autogenerated-user"
	bkpNamespaces = make([]string, 0)
	backupLocationMap := make(map[string]string)
	noAccessCheckTimeout := 5 * time.Minute
	noAccessCheckRetryDuration := 30 * time.Second

	JustBeforeEach(func() {
		StartTorpedoTest("CancelClusterBackupShare",
			"Share all backups at cluster level with a user group and revoke it and validate", nil, 82935)
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
	It("Share all backups at cluster level with a user group and revoke it and validate", func() {
		providers := getProviders()
		Step("Validate applications and get their labels", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Create Users", func() {
			log.InfoD("Creating %d users to be added to the group", numberOfUsers)
			var wg sync.WaitGroup
			for i := 1; i <= numberOfUsers; i++ {
				userName := fmt.Sprintf("testuser%v", i)
				firstName := fmt.Sprintf("FirstName%v", i)
				lastName := fmt.Sprintf("LastName%v", i)
				email := fmt.Sprintf("testuser%v_%v@cnbu.com", i, time.Now().Unix())
				time.Sleep(2 * time.Second)
				wg.Add(1)
				go func(userName, firstName, lastName, email string) {
					defer GinkgoRecover()
					defer wg.Done()
					err := backup.AddUser(userName, firstName, lastName, email, commonPassword)
					log.FailOnError(err, "Failed to create user - %s", userName)
					users = append(users, userName)
				}(userName, firstName, lastName, email)
			}
			wg.Wait()

			log.InfoD("Creating a user with username - [%s] who is not part of any group", individualUser)
			firstName := "autogenerated-firstname"
			lastName := "autogenerated-last name"
			email := "autogenerated-email@cnbu.com"
			err := backup.AddUser(individualUser, firstName, lastName, email, commonPassword)
			log.FailOnError(err, "Failed to create user - %s", individualUser)
		})

		Step("Create Groups", func() {
			log.InfoD("Creating %d groups", numberOfGroups)
			var wg sync.WaitGroup
			for i := 1; i <= numberOfGroups; i++ {
				groupName := fmt.Sprintf("testGroup%v", i)
				wg.Add(1)
				go func(groupName string) {
					defer GinkgoRecover()
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
			for i, user := range users {
				time.Sleep(2 * time.Second)
				groupIndex := i / groupSize
				wg.Add(1)
				go func(userName string, groupIndex int) {
					defer GinkgoRecover()
					defer wg.Done()
					err := backup.AddGroupToUser(userName, groups[groupIndex])
					log.FailOnError(err, "Failed to assign group to user")
				}(user, groupIndex)
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
			log.InfoD("Creating cloud credentials and backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err := CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				customBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationMap[backupLocationUID] = customBackupLocationName
				err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
				log.InfoD("Created Backup Location with name - %s", customBackupLocationName)
			}
		})

		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			var sem = make(chan struct{}, 10)
			var wg sync.WaitGroup
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			for i := 0; i < numberOfBackups; i++ {
				sem <- struct{}{}
				time.Sleep(10 * time.Second)
				backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
				backupNames = append(backupNames, backupName)
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					defer func() { <-sem }()
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespaces[0]})
					err := CreateBackupWithValidation(ctx, backupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
				}(backupName)
			}
			wg.Wait()
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
			ctxNonAdmin, err := backup.GetNonAdminCtx(chosenUser, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")

			// Start Restore
			backupName := backupNames[5]
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)

			// Restore validation to make sure that the user with Full Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			err = DeleteRestore(restoreName, orgID, ctxNonAdmin)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))

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
			ctxNonAdmin, err = backup.GetNonAdminCtx(individualUser, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")

			// Start Restore
			backupName = backupNames[4]
			restoreName = fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)

			// Restore validation to make sure that the user with Full Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			err = DeleteRestore(restoreName, orgID, ctxNonAdmin)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))

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
			log.InfoD("Share all backups with Restore Access in source cluster with a group and a user who is not part of the group")
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
			ctxNonAdmin, err := backup.GetNonAdminCtx(chosenUser, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")

			// Start Restore
			backupName := backupNames[3]
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)

			// Restore validation to make sure that the user with Restore Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			err = DeleteRestore(restoreName, orgID, ctxNonAdmin)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))

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
			ctxNonAdmin, err = backup.GetNonAdminCtx(individualUser, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")

			// Start Restore
			backupName = backupNames[2]
			restoreName = fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupName, restoreName)

			// Restore validation to make sure that the user with Restore Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			err = DeleteRestore(restoreName, orgID, ctxNonAdmin)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))

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
			ctxNonAdmin, err := backup.GetNonAdminCtx(chosenUser, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")

			// Start Restore
			backupName := backupNames[1]
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))

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
			ctxNonAdmin, err = backup.GetNonAdminCtx(individualUser, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")

			// Start Restore
			restoreName = fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))

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
			log.InfoD("Revoke all the shared backups in source cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = ClusterUpdateBackupShare(SourceClusterName, []string{groups[0]}, []string{individualUser}, ViewOnlyAccess, false, ctx)
			log.FailOnError(err, "Failed sharing all backups for cluster [%s]", SourceClusterName)
		})

		Step("Validate that no groups or users have access to backups shared at cluster level", func() {
			log.InfoD("Validate no groups or users have access to backups shared at cluster level")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.Infof("User chosen to validate no access - %s", chosenUser)
			log.InfoD("Checking backups user [%s] has after revoking", chosenUser)
			userBackups, err := GetAllBackupsForUser(chosenUser, commonPassword)
			log.FailOnError(err, "Failed to get all backups for user - [%s]", chosenUser)
			noAccessCheck := func() (interface{}, bool, error) {
				if len(userBackups) > 0 {
					log.Infof("Backups user [%s] has access to - %v", chosenUser, userBackups)
					for _, backupName := range userBackups {
						backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, orgID)
						log.FailOnError(err, fmt.Sprintf("Getting UID for backup %v", backupName))
						backupInspectRequest := &api.BackupInspectRequest{
							Name:  backupName,
							Uid:   backupUID,
							OrgId: orgID,
						}
						resp, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
						log.FailOnError(err, fmt.Sprintf("error inspecting backup %v", backupName))
						deletePendingStatus := api.BackupInfo_StatusInfo_DeletePending
						deletingStatus := api.BackupInfo_StatusInfo_Deleting
						actual := resp.GetBackup().GetStatus().Status
						reason := resp.GetBackup().GetStatus().Reason
						if actual == deletePendingStatus || actual == deletingStatus {
							log.Infof("Ignoring the backup from user access as the backup is in [%s] state ,Reason:[%s]", actual, reason)
							RemoveElementByValue(&userBackups, backupName)
							continue
						} else {
							return "", true, fmt.Errorf("waiting for backup access - [%v] to be revoked for user = [%s], The backup is in [%s] state",
								backupName, chosenUser, actual)
						}
					}
				}
				return "", false, nil
			}
			_, err = DoRetryWithTimeoutWithGinkgoRecover(noAccessCheck, noAccessCheckTimeout, noAccessCheckRetryDuration)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Validating that user [%s] has access to no backups", chosenUser))
			// Now validating with individual user who is not part of any group
			// Get user context
			log.InfoD("Validate no access of backups shared at cluster level for an individual user - %s", individualUser)
			log.InfoD("Checking backups user [%s] has after revoking", individualUser)
			userBackups1, err := GetAllBackupsForUser(individualUser, commonPassword)
			log.FailOnError(err, "Failed to get all backups for user - [%s]", individualUser)
			noAccessCheck = func() (interface{}, bool, error) {
				if len(userBackups1) > 0 {
					log.Infof("Backups user [%s] has access to - %v", individualUser, userBackups1)
					for _, backupName := range userBackups1 {
						backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, orgID)
						log.FailOnError(err, fmt.Sprintf("Getting UID for backup %v", backupName))
						backupInspectRequest := &api.BackupInspectRequest{
							Name:  backupName,
							Uid:   backupUID,
							OrgId: orgID,
						}
						resp, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
						log.FailOnError(err, fmt.Sprintf("error inspecting backup %v", backupName))
						deletePendingStatus := api.BackupInfo_StatusInfo_DeletePending
						deletingStatus := api.BackupInfo_StatusInfo_Deleting
						actual := resp.GetBackup().GetStatus().Status
						reason := resp.GetBackup().GetStatus().Reason
						if actual == deletePendingStatus || actual == deletingStatus {
							log.Infof("Ignoring the backup from user access as the backup is in [%s] state ,Reason:[%s]", actual, reason)
							err = RemoveElementByValue(&userBackups1, backupName)
							log.FailOnError(err, fmt.Sprintf("error removing backup [%s] from the list", backupName))
							continue
						} else {
							return "", true, fmt.Errorf("waiting for backup access - [%v] to be revoked for user = [%s], The backup is in [%s] state ",
								backupName, individualUser, actual)
						}
					}
				}
				return "", false, nil
			}
			_, err = DoRetryWithTimeoutWithGinkgoRecover(noAccessCheck, noAccessCheckTimeout, noAccessCheckRetryDuration)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Validating that individual user [%s] has access to no backups", individualUser))
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		var wg sync.WaitGroup
		log.Infof("Cleaning up users")
		for _, userName := range users {
			wg.Add(1)
			go func(userName string) {
				defer GinkgoRecover()
				defer wg.Done()
				err := backup.DeleteUser(userName)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting user %v", userName))
			}(userName)
		}
		wg.Wait()
		err := backup.DeleteUser(individualUser)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting user %v", individualUser))

		log.Infof("Cleaning up groups")
		for _, groupName := range groups {
			wg.Add(1)
			go func(groupName string) {
				defer GinkgoRecover()
				defer wg.Done()
				err := backup.DeleteGroup(groupName)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting group %v", groupName))
			}(groupName)
		}
		wg.Wait()
		log.Infof("Deleting registered clusters for non-admin context")
		for _, ctxNonAdmin := range userContexts {
			CleanupCloudSettingsAndClusters(make(map[string]string), "", "", ctxNonAdmin)
		}
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")

		log.Infof("Removing the backups from the backupNames list which have already been deleted as part of FullAccess Validation")
		backupNames = removeStringItemFromSlice(backupNames, []string{backupNames[5], backupNames[4]})
		log.Infof(" Deleting the backups created")
		backupDriver := Inst().Backup
		for _, backupName := range backupNames {
			backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
			log.Infof("About to delete backup - %s", backupName)
			_, err = DeleteBackup(backupName, backupUID, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup - [%s]", backupName))
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})

// ShareBackupAndEdit shares backup with restore and full access and edits the shared backup
var _ = Describe("{ShareBackupAndEdit}", func() {
	numberOfUsers := 2
	users := make([]string, 0)
	backupNames := make([]string, 0)
	userContexts := make([]context.Context, 0)
	var scheduledAppContexts []*scheduler.Context
	var backupLocationName string
	var backupLocationUID string
	var cloudCredUID string
	var newCloudCredUID string
	var cloudCredUidList []string
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var credName string
	var newCredName string
	bkpNamespaces = make([]string, 0)
	backupLocationMap := make(map[string]string)
	JustBeforeEach(func() {
		StartTorpedoTest("ShareBackupAndEdit",
			"Share backup with restore and full access mode and edit the shared backup", nil, 82950)
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
	It("Share the backup and edit", func() {
		providers := getProviders()
		Step("Validate applications and get their labels", func() {
			log.InfoD("Validate applications and get their labels")
			ValidateApplications(scheduledAppContexts)
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
					defer GinkgoRecover()
					defer wg.Done()
					err := backup.AddUser(userName, firstName, lastName, email, commonPassword)
					log.FailOnError(err, "Failed to create user - %s", userName)
					users = append(users, userName)
				}(userName, firstName, lastName, email)
			}
			wg.Wait()
		})
		Step("Adding Credentials and Registering Backup Location", func() {
			log.InfoD("Creating cloud credentials and backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err := CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				backupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationMap[backupLocationUID] = backupLocationName
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocationName))
				log.InfoD("Created Backup Location with name - %s", backupLocationName)
				newCloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, newCloudCredUID)
				newCredName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err = CreateCloudCredential(provider, newCredName, newCloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", newCredName, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", newCredName)
			}
		})
		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})
		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
			backupNames = append(backupNames, backupName)
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespaces[0]})
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, nil, orgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
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

			// Update the backup with another cred
			log.InfoD("Update the backup with another cred")
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, backupNames[0], orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupNames[0])
			status, err := UpdateBackup(backupNames[0], backupUID, orgID, newCredName, newCloudCredUID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Updating backup %s with new cred %v", backupNames[0], newCredName))
			log.Infof("The status after updating backup %s with new cred %v is %v", backupNames[0], newCredName, status)

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(users[0], commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")

			// Start Restore
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupNames[0], make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupNames[0], restoreName)

			// Restore validation to make sure that the user with Full Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupNames[0], restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			err = DeleteRestore(restoreName, orgID, ctxNonAdmin)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))

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
			ctxNonAdmin, err := backup.GetNonAdminCtx(users[1], commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")

			// Get Backup UID
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, backupNames[0], orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupNames[0])

			//update the backup with another cred
			log.InfoD("Update the backup with another cred")
			status, err := UpdateBackup(backupNames[0], backupUID, orgID, credName, cloudCredUID, ctxNonAdmin)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Updating backup %s with new cred %v", backupNames[0], credName))
			log.Infof("The status after updating backup %s with new cred %v is %v", backupNames[0], credName, status)

			// Start Restore
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, backupNames[0], make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", backupNames[0], restoreName)

			// Restore validation to make sure that the user with Full Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", backupNames[0], restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			err = DeleteRestore(restoreName, orgID, ctxNonAdmin)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))

			// Delete backup to confirm that the user has Full Access
			backupDeleteResponse, err := DeleteBackup(backupNames[0], backupUID, orgID, ctxNonAdmin)
			log.FailOnError(err, "Backup [%s] could not be deleted by user [%s]", backupNames[0], users[1])
			dash.VerifyFatal(backupDeleteResponse.String(), "", "Verifying backup deletion is successful")
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		log.Infof("Deleting registered clusters for non-admin context")
		for _, ctxNonAdmin := range userContexts {
			CleanupCloudSettingsAndClusters(make(map[string]string), "", "", ctxNonAdmin)
		}

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

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
		err = DeleteCloudCredential(newCredName, orgID, newCloudCredUID)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cloud cred %s", newCredName))

	})
})

// SharedBackupDelete shares backup with multiple users and delete the backup
var _ = Describe("{SharedBackupDelete}", func() {
	numberOfUsers := 10
	numberOfBackups := 10
	users := make([]string, 0)
	backupNames := make([]string, 0)
	userContexts := make([]context.Context, 0)
	var scheduledAppContexts []*scheduler.Context
	var backupLocationName string
	var backupLocationUID string
	var cloudCredUID string
	var cloudCredUidList []string
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var credName string
	bkpNamespaces = make([]string, 0)
	backupLocationMap := make(map[string]string)
	JustBeforeEach(func() {
		StartTorpedoTest("SharedBackupDelete",
			"Share backup with multiple users and delete the backup", nil, 82946)
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
	It("Share the backups and delete", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Create Users", func() {
			users = createUsers(numberOfUsers)
		})
		Step("Adding Credentials and Registering Backup Location", func() {
			log.InfoD("Creating cloud credentials and backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err := CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				backupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationMap[backupLocationUID] = backupLocationName
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocationName))
				log.InfoD("Created Backup Location with name - %s", backupLocationName)
			}
		})
		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
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
					time.Sleep(10 * time.Second)
					backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
					backupNames = append(backupNames, backupName)
					wg.Add(1)
					go func(backupName string) {
						defer GinkgoRecover()
						defer wg.Done()
						defer func() { <-sem }()
						appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
						err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, nil, orgID, clusterUid, "", "", "", "")
						dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
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
				ctxNonAdmin, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "Fetching non admin ctx")
				userContexts = append(userContexts, ctxNonAdmin)

				// Register Source and Destination cluster
				log.InfoD("Registering Source and Destination clusters from user context for user -%s", user)
				err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
				dash.VerifyFatal(err, nil, "Creating source and destination cluster")

				for _, backup := range backupNames {
					// Get Backup UID
					backupDriver := Inst().Backup
					backupUID, err := backupDriver.GetBackupUID(ctx, backup, orgID)
					log.FailOnError(err, "Failed while trying to get backup UID for - %s", backup)
					backupMap[backup] = backupUID

					// Start Restore
					restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
					err = CreateRestore(restoreName, backup, nil, destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))

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
					defer GinkgoRecover()
					defer wg.Done()
					_, err = DeleteBackup(backup, backupMap[backup], orgID, ctx)
					log.FailOnError(err, "Failed to delete backup - %s", backup)
					err = backupDriver.WaitForBackupDeletion(ctx, backup, orgID, backupDeleteTimeout, backupDeleteRetryTime)
					log.FailOnError(err, "Error waiting for backup deletion %v", backup)
				}(backup)
			}
			wg.Wait()

			//Validate that backups are not listing with shared users
			// Get user context
			for _, user := range users {
				log.Infof("Validating user %s has access to no backups", user)
				userBackups1, _ := GetAllBackupsForUser(user, commonPassword)
				dash.VerifyFatal(len(userBackups1), 0, fmt.Sprintf("Validating that user [%s] has access to no backups", user))
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		log.Infof("Deleting registered clusters for non-admin context")
		for _, ctxNonAdmin := range userContexts {
			CleanupCloudSettingsAndClusters(make(map[string]string), "", "", ctxNonAdmin)
		}

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
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})

var _ = Describe("{ClusterBackupShareToggle}", func() {
	var (
		scheduledAppContexts       []*scheduler.Context
		cloudCredUID               string
		cloudCredName              string
		backupLocationUID          string
		backupLocationName         string
		appNamespaces              []string
		restoreNames               []string
		backupLocationMap          map[string]string
		username                   string
		periodicSchedulePolicyName string
		periodicSchedulePolicyUid  string
		scheduleName               string
		backupClusterName          string
		scheduleNames              []string
	)

	JustBeforeEach(func() {
		StartTorpedoTest("ClusterBackupShareToggle", "Verification of backup sharing and access level functionality", nil, 82936)
		log.InfoD("Scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				log.Infof("Scheduled application with namespace [%s]", namespace)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})

	It("Validates that the user is able to perform operations on a shared backup after toggling the access", func() {
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Create a user", func() {
			log.InfoD("Creating a user")
			numberOfUsers := 1
			username = createUsers(numberOfUsers)[0]
			log.InfoD("Created a user with username [%s]", username)
		})
		Step("Create cloud credentials and backup locations", func() {
			log.InfoD("Creating cloud credentials and backup locations")
			providers := getProviders()
			backupLocationMap = make(map[string]string)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				log.InfoD("Creating cloud credential named [%s] and uid [%s] using [%s] as provider", cloudCredUID, cloudCredName, provider)
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				backupLocationName = fmt.Sprintf("%s-%s-bl-%v", provider, getGlobalBucketName(provider), time.Now().Unix())
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				bucketName := getGlobalBucketName(provider)
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudCredName, cloudCredUID, bucketName, orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup location named [%s] with uid [%s] of [%s] as provider", backupLocationName, backupLocationUID, provider))
			}
		})
		Step("Configure source and destination clusters with px-central-admin and user ctx", func() {
			log.InfoD("Configuring source and destination clusters with px-central-admin and user ctx")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, destinationClusterName))
			backupClusterName = SourceClusterName
			clusterStatus, err := Inst().Backup.GetClusterStatus(orgID, backupClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", backupClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", backupClusterName))
			clusterUid, err := Inst().Backup.GetClusterUID(ctx, orgID, backupClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", backupClusterName))
			log.InfoD("Uid of [%s] cluster is %s", backupClusterName, clusterUid)
			ctxNonAdmin, err := backup.GetNonAdminCtx(username, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with [%s] ctx", SourceClusterName, destinationClusterName, username))
		})
		Step("Create schedule policy", func() {
			log.InfoD("Creating a schedule policy")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			periodicSchedulePolicyName = fmt.Sprintf("%s-%v", "periodic", time.Now().Unix())
			periodicSchedulePolicyUid = uuid.New()
			periodicSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, 15, 5)
			err = Inst().Backup.BackupSchedulePolicy(periodicSchedulePolicyName, periodicSchedulePolicyUid, orgID, periodicSchedulePolicyInfo)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval 15 minutes named [%s]", periodicSchedulePolicyName))
			periodicSchedulePolicyUid, err = Inst().Backup.GetSchedulePolicyUid(orgID, ctx, periodicSchedulePolicyName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching uid of periodic schedule policy named [%s]", periodicSchedulePolicyName))
		})
		Step("Create schedule backup", func() {
			log.InfoD("Creating a schedule backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			scheduleName = fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, time.Now().Unix())
			labelSelectors := make(map[string]string)
			_, err = CreateScheduleBackupWithValidation(ctx, scheduleName, backupClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, orgID, "", "", "", "", periodicSchedulePolicyName, periodicSchedulePolicyUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of schedule backup with schedule name [%s]", scheduleName))
			scheduleNames = append(scheduleNames, scheduleName)
		})
		Step("Validate the Access toggle", func() {
			log.InfoD("Validating the access toggle")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			accesses := []BackupAccess{ViewOnlyAccess, RestoreAccess, FullAccess}
			for _, accessLevel := range accesses {
				log.InfoD("Sharing backups of cluster [%s] with [%#v] access level to user [%s]", backupClusterName, accessLevel, username)
				err := ClusterUpdateBackupShare(backupClusterName, nil, []string{username}, accessLevel, true, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying share of all backups of cluster [%s] with [%#v] access level to user [%s]", backupClusterName, accessLevel, username))
				clusterShareCheck := func() (interface{}, bool, error) {
					userBackups, err := GetAllBackupsForUser(username, commonPassword)
					if err != nil {
						return "", false, err
					}
					if len(userBackups) == 0 {
						return "", true, fmt.Errorf("no backups were found from shared cluster named [%s] for user [%s]", backupClusterName, username)
					}
					return userBackups, false, nil
				}
				userBackups, err := DoRetryWithTimeoutWithGinkgoRecover(clusterShareCheck, 2*time.Minute, 10*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching backups from shared cluster named [%s] for user [%s]", backupClusterName, username))
				log.InfoD("User backups - %v", userBackups.([]string))
				restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
				ValidateSharedBackupWithUsers(username, accessLevel, userBackups.([]string)[len(userBackups.([]string))-1], restoreName)
				if accessLevel != ViewOnlyAccess {
					restoreNames = append(restoreNames, restoreName)
				}
				log.InfoD("Restore names - %v", restoreNames)
				if accessLevel == FullAccess {
					log.InfoD("Starting full access exit")
					break
				}
				log.InfoD("Waiting for 15 minutes for the next schedule backup to be triggered")
				time.Sleep(15 * time.Minute)
				fetchedUserBackups, err := GetAllBackupsForUser(username, commonPassword)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching backups for user [%s]", username))
				dash.VerifyFatal(len(fetchedUserBackups), len(userBackups.([]string))+1, "Verifying if new schedule backup is up or not")
				log.InfoD("All the backups for user [%s] - %v", username, fetchedUserBackups)
				recentBackupName := fetchedUserBackups[len(fetchedUserBackups)-1]
				log.InfoD("Recent backup name [%s] ", recentBackupName)
				err = backupSuccessCheckWithValidation(ctx, recentBackupName, scheduledAppContexts, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success and Validation of recent backup [%s]", recentBackupName))
			}
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		//Delete Schedule Backup-
		log.Infof("Deleting backup schedule")
		for _, scheduleName := range scheduleNames {
			err = DeleteSchedule(scheduleName, backupClusterName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		}
		log.Infof("Deleting backup schedule policy")
		policyList := []string{periodicSchedulePolicyName}
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, policyList)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", policyList))
		ctxNonAdmin, err := backup.GetNonAdminCtx(username, commonPassword)
		log.FailOnError(err, "Fetching non admin ctx")
		for _, restoreName := range restoreNames {
			err := DeleteRestore(restoreName, orgID, ctxNonAdmin)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the deletion of the restore named [%s] in [%s] ctx", restoreName, username))
		}
		CleanupCloudSettingsAndClusters(make(map[string]string), "", "", ctxNonAdmin)
		err = backup.DeleteUser(username)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the deletion of the user [%s]", username))
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.InfoD("Deleting deployed namespaces - %v", appNamespaces)
		DestroyApps(scheduledAppContexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// https://portworx.atlassian.net/browse/PB-3486
// UI testing is need to validate that user with FullAccess cannot duplicate the backup shared
var _ = Describe("{ShareBackupsAndClusterWithUser}", func() {
	var (
		scheduledAppContexts []*scheduler.Context
		bkpNamespaces        []string
		clusterUid           string
		clusterStatus        api.ClusterInfo_StatusInfo_Status
		userNames            []string
		backupName           string
		backupLocationUID    string
		cloudCredName        string
		cloudCredUID         string
		bkpLocationName      string
		userBackupName       string
		ctxNonAdmin          context.Context
	)
	backupLocationMap := make(map[string]string)
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	numberOfUsers := 1
	JustBeforeEach(func() {
		StartTorpedoTest("ShareBackupsAndClusterWithUser",
			"Share backup to user with full access and try to duplicate the backup from the shared user", nil, 82943)
		log.InfoD("Deploy applications need for taking backup")
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
	It("Share Backup With Full Access Users and try to duplicate the backup", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		Step("Validate applications", func() {
			log.InfoD("Validate applications ")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Create Users", func() {
			userNames = createUsers(numberOfUsers)
			log.Infof("Created %v users and users list is %v", numberOfUsers, userNames)
		})
		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			providers := getProviders()
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cloudcred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("%s-%s-%v-bl", provider, getGlobalBucketName(provider), time.Now().Unix())
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", bkpLocationName))
			}
		})
		Step("Register cluster for backup", func() {
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			log.InfoD("Uid of [%s] cluster is %s", SourceClusterName, clusterUid)
		})
		Step("Taking backup of applications", func() {
			backupName = fmt.Sprintf("%s-%s", BackupNamePrefix, bkpNamespaces[0])
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespaces[0]})
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))

		})
		Step("Share backup with user having full access", func() {
			log.InfoD("Share backup with user having full access")
			err = ShareBackup(backupName, nil, userNames, FullAccess, ctx)
			log.FailOnError(err, "Failed to share backup %s", backupName)
		})
		Step("Create backup from the shared user with FullAccess", func() {
			log.InfoD("Validating if user with FullAccess cannot duplicate backup shared but can create new backup")
			// User with FullAccess cannot duplicate will be validated through UI only
			for _, user := range userNames {
				ctxNonAdmin, err = backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "Fetching non admin ctx")
				log.InfoD("Registering Source and Destination clusters from user context")
				err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
				dash.VerifyFatal(err, nil, "Creating source and destination cluster")
				clusterUid, err = Inst().Backup.GetClusterUID(ctxNonAdmin, orgID, SourceClusterName)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
				log.InfoD("Uid of [%s] cluster by user [%s] is %s", SourceClusterName, user, clusterUid)
				userBackupName = fmt.Sprintf("%s-%s-%s", "user", BackupNamePrefix, bkpNamespaces[0])
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespaces[0]})
				err = CreateBackupWithValidation(ctxNonAdmin, userBackupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", userBackupName))
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		backupDriver := Inst().Backup
		log.Infof("Deleting backup created by user - %s", userNames[0])
		userBackupUID, err := backupDriver.GetBackupUID(ctxNonAdmin, userBackupName, orgID)
		dash.VerifySafely(err, nil, fmt.Sprintf("Getting backup UID of user for backup %s", userBackupName))
		_, err = DeleteBackup(userBackupName, userBackupUID, orgID, ctxNonAdmin)
		dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting backup %s created by user", userBackupName))
		err = DeleteBackupAndWait(userBackupName, ctxNonAdmin)
		log.FailOnError(err, fmt.Sprintf("Failed while waiting for backup %s to be deleted", userBackupName))

		log.Infof("Deleting registered clusters for non-admin context")
		err = DeleteCluster(SourceClusterName, orgID, ctxNonAdmin, true)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", SourceClusterName))
		err = DeleteCluster(destinationClusterName, orgID, ctxNonAdmin, true)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", destinationClusterName))

		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// ShareBackupWithDifferentRoleUsers shares backup with multiple user with different access permissions and roles
var _ = Describe("{ShareBackupWithDifferentRoleUsers}", func() {
	var (
		scheduledAppContexts     []*scheduler.Context
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
	JustBeforeEach(func() {
		StartTorpedoTest("ShareBackupWithDifferentRoleUsers",
			"Take backups and share it with multiple user with different access permissions and different roles", nil, 82947)
		log.InfoD("Deploy applications needed for backup")
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
	It("Share Backup With Different Users having different access level and different role", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")

		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Create multiple Users", func() {
			log.InfoD("Creating %d users", numberOfUsers)
			users = createUsers(numberOfUsers)
			log.Infof("Created %v users and users list is %v", numberOfUsers, users)
		})

		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			providers := getProviders()
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("%s-%s-%v", provider, getGlobalBucketName(provider), time.Now().Unix())
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err = CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", bkpLocationName))
			}
		})

		Step("Register cluster for backup", func() {
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			log.InfoD("Uid of [%s] cluster is %s", SourceClusterName, clusterUid)
		})

		Step("Taking backups of application for each user", func() {
			log.InfoD("Taking backups of application for each user")
			var sem = make(chan struct{}, 10)
			var wg sync.WaitGroup
			for i := 0; i < numberOfUsers; i++ {
				sem <- struct{}{}
				time.Sleep(10 * time.Second)
				backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
				backupNames = append(backupNames, backupName)
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					defer func() { <-sem }()
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespaces[0]})
					err := CreateBackupWithValidation(ctx, backupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s] with namespaces (scheduled contexts) [%s]", backupName, bkpNamespaces[0]))
				}(backupName)
			}
			wg.Wait()
			log.Infof("List of backups - %v", backupNames)
		})

		Step("Adding different roles to users and sharing backup with different access level", func() {
			userRoleAccessBackupList, err = AddRoleAndAccessToUsers(users, backupNames)
			dash.VerifyFatal(err, nil, "Adding roles and access level to users")
			log.Infof("The user/access/backup list is %v", userRoleAccessBackupList)
		})

		Step("Validating the shared backup with user having different access level and roles", func() {
			for key, val := range userRoleAccessBackupList {
				restoreName := fmt.Sprintf("%s-%s-%v", key.user, RestoreNamePrefix, time.Now().Unix())
				access := key.accesses
				if access != ViewOnlyAccess {
					userRestoreContext[key.context] = restoreName
				}
				if access == FullAccess {
					backupNames = removeStringItemFromSlice(backupNames, []string{val})
				}
				ValidateSharedBackupWithUsers(key.user, key.accesses, val, restoreName)
			}
		})
	})
	JustAfterEach(func() {
		var wg sync.WaitGroup
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		backupDriver := Inst().Backup
		for _, backupName := range backupNames {
			wg.Add(1)
			go func(backupName string) {
				defer GinkgoRecover()
				defer wg.Done()
				backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
				dash.VerifySafely(err, nil, fmt.Sprintf("Getting backup UID for backup %v", backupName))
				_, err = DeleteBackup(backupName, backupUID, orgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup %s", backupName))
			}(backupName)
		}
		wg.Wait()
		log.Infof("Generating user context")
		for _, userName := range users {
			ctxNonAdmin, err := backup.GetNonAdminCtx(userName, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContextsList = append(userContextsList, ctxNonAdmin)
		}
		log.Infof("Deleting restore created by users")
		for userContext, restoreName := range userRestoreContext {
			err = DeleteRestore(restoreName, orgID, userContext)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))
		}
		log.Infof("Deleting registered clusters for non-admin context")
		for _, ctxNonAdmin := range userContextsList {
			CleanupCloudSettingsAndClusters(make(map[string]string), "", "", ctxNonAdmin)
		}
		log.Infof("Cleaning up users")
		for _, userName := range users {
			wg.Add(1)
			go func(userName string) {
				defer GinkgoRecover()
				defer wg.Done()
				err := backup.DeleteUser(userName)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting user %s", userName))
			}(userName)
		}
		wg.Wait()
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// DeleteSharedBackup deletes shared backups, validate that shared backups are deleted from owner
var _ = Describe("{DeleteSharedBackup}", func() {
	userName := "testuser-82937"
	firstName := "firstName"
	lastName := "lastName"
	email := "testuser1@cnbu.com"
	numberOfBackups := 20
	backupNames := make([]string, 0)
	userContexts := make([]context.Context, 0)
	var scheduledAppContexts []*scheduler.Context
	var backupLocationName string
	var backupLocationUID string
	var cloudCredUID string
	var cloudCredUidList []string
	var bkpNamespaces []string
	var clusterUid string
	var backupNotDeleted string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var credName string
	bkpNamespaces = make([]string, 0)
	backupLocationMap := make(map[string]string)

	JustBeforeEach(func() {
		StartTorpedoTest("DeleteSharedBackup",
			"Share backup with multiple users and delete the backup", nil, 82937)
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
	It("Validate shared backups are deleted from owner of backup ", func() {
		providers := getProviders()
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Create Users", func() {
			err = backup.AddUser(userName, firstName, lastName, email, commonPassword)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying user %s creation", userName))

		})
		Step("Adding Credentials and Registering Backup Location", func() {
			log.InfoD("Creating cloud credentials and backup location")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err = CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				backupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationMap[backupLocationUID] = backupLocationName
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocationName))
				log.InfoD("Created Backup Location with name - %s", backupLocationName)
			}
		})
		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
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
					time.Sleep(10 * time.Second)
					backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
					backupNames = append(backupNames, backupName)
					wg.Add(1)
					go func(backupName string) {
						defer GinkgoRecover()
						defer wg.Done()
						defer func() { <-sem }()
						appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
						err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, nil, orgID, clusterUid, "", "", "", "")
						dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
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
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup %s share", backup))
			}
		})

		Step("Delete Shared Backups from user", func() {
			log.InfoD("register the Source and destination cluster of non-px Admin")

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(userName, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context for user -%s", userName)
			err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			// Validate that backups are shared with user
			log.Infof("Validating that backups are shared with %s user", userName)
			userBackups1, _ := GetAllBackupsForUser(userName, commonPassword)
			dash.VerifyFatal(len(userBackups1), numberOfBackups, fmt.Sprintf("Validating that user [%s] has access to all shared backups", userName))

			//Start deleting from user with whom the backups are shared
			var wg sync.WaitGroup
			backupDriver := Inst().Backup

			for _, backup := range backupNames {
				wg.Add(1)
				go func(backup string) {
					defer GinkgoRecover()
					defer wg.Done()
					log.InfoD("Backup deletion started")
					backupUID, err := backupDriver.GetBackupUID(ctxNonAdmin, backup, orgID)
					backupDeleteResponse, err := DeleteBackup(backup, backupUID, orgID, ctxNonAdmin)
					log.FailOnError(err, "Backup [%s] could not be deleted by user [%s] with delete response %s", backup, userName, backupDeleteResponse)
					err = backupDriver.WaitForBackupDeletion(ctxNonAdmin, backup, orgID, backupDeleteTimeout, backupDeleteRetryTime)
					log.FailOnError(err, "Error waiting for backup deletion %v", backup)
					dash.VerifyFatal(backupDeleteResponse.String(), "", fmt.Sprintf("Verifying backup %s deletion status", backup))

				}(backup)
			}
			wg.Wait()

		})
		Step("Validating that backups are deleted from owner of backups", func() {
			adminBackups, _ := GetAllBackupsAdmin()
			log.Infof("%v", adminBackups)
			adminBackupsMap := make(map[string]bool)
			for _, backup := range adminBackups {
				adminBackupsMap[backup] = true
			}
			for _, name := range backupNames {
				if adminBackupsMap[name] {
					backupNotDeleted = name
					break
				}
			}
			dash.VerifyFatal(backupNotDeleted, "", fmt.Sprintf("Validating that shared backups are deleted from owner of backup"))
		})

	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		log.Infof("Deleting registered clusters for non-admin context")
		for _, ctxNonAdmin := range userContexts {
			err := DeleteCluster(SourceClusterName, orgID, ctxNonAdmin, true)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", SourceClusterName))
			err = Inst().Backup.WaitForClusterDeletion(ctxNonAdmin, SourceClusterName, orgID, clusterDeleteTimeout, clusterDeleteRetryTime)
			dash.VerifySafely(err, nil, fmt.Sprintf("Waiting for cluster %s deletion", SourceClusterName))
			err = DeleteCluster(destinationClusterName, orgID, ctxNonAdmin, true)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", destinationClusterName))
			err = Inst().Backup.WaitForClusterDeletion(ctxNonAdmin, destinationClusterName, orgID, clusterDeleteTimeout, clusterDeleteRetryTime)
			dash.VerifySafely(err, nil, fmt.Sprintf("Waiting for cluster %s deletion", destinationClusterName))
		}

		err := backup.DeleteUser(userName)
		log.FailOnError(err, "Error deleting user %v", userName)

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)

	})

})

// ShareAndRemoveBackupLocation shares and remove backup location and add it back and verify
var _ = Describe("{ShareAndRemoveBackupLocation}", func() {
	var (
		scheduledAppContexts []*scheduler.Context
		bkpNamespaces        []string
		srcClusterUid        string
		srcClusterStatus     api.ClusterInfo_StatusInfo_Status
		destClusterStatus    api.ClusterInfo_StatusInfo_Status
		backupLocationUID    string
		cloudCredName        string
		cloudCredUID         string
		bkpLocationName      string
		newBkpLocationName   string
		backupNames          []string
		newBackupNames       []string
		newBackupLocationUID string
	)
	userContextsList := make([]context.Context, 0)
	accessUserBackupContext := make(map[userAccessContext]string)
	userRestoreContext := make(map[context.Context]string)
	numberOfUsers := 3
	backupLocationMap := make(map[string]string)
	newBackupLocationMap := make(map[string]string)
	users := make([]string, 0)
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	JustBeforeEach(func() {
		StartTorpedoTest("ShareAndRemoveBackupLocation",
			"Share and remove backup location and add it back and check from other users if they show up", nil, 82949)
		log.Infof("Deploy applications needed for backup")
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
	It("Share and remove backup location and add it back and check from other users if they show up", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		providers := getProviders()
		Step("Validate applications", func() {
			log.Infof("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Create multiple Users", func() {
			log.InfoD("Creating %d users", numberOfUsers)
			users = createUsers(numberOfUsers)
			log.Infof("Created %v users and users list is %v", numberOfUsers, users)
		})

		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", bkpLocationName))
			}
		})

		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Register source and destination cluster for backup")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			srcClusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(srcClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			srcClusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			destClusterStatus, err = Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(destClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
		})

		Step("Taking backups of application for each user", func() {
			log.InfoD("Taking backup of application for each user")
			var sem = make(chan struct{}, 10)
			var wg sync.WaitGroup
			for i := 0; i < numberOfUsers; i++ {
				sem <- struct{}{}
				time.Sleep(10 * time.Second)
				backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
				backupNames = append(backupNames, backupName)
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					defer func() { <-sem }()
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespaces[0]})
					err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, srcClusterUid, "", "", "", "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s] of applications [%s]", backupName, bkpNamespaces[0]))
				}(backupName)
			}
			wg.Wait()
			log.Infof("List of backups - %v", backupNames)
		})

		Step("Share backup with users with different access level", func() {
			log.InfoD("Share backup with users with different access level")
			_, err = ShareBackupWithUsersAndAccessAssignment(backupNames, users, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Sharing backup %s with users %v", backupNames, users))
		})

		Step("Removing backup location after sharing backup with all the users", func() {
			log.InfoD("Removing backup location after sharing backup with all the users")
			err = DeleteBackupLocation(bkpLocationName, backupLocationUID, orgID, true)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup location %s", bkpLocationName))
			backupLocationDeleteStatusCheck := func() (interface{}, bool, error) {
				status, err := IsBackupLocationPresent(bkpLocationName, ctx, orgID)
				if err != nil {
					return "", true, fmt.Errorf("backup location %s still present with error %v", bkpLocationName, err)
				}
				if status == true {
					return "", true, fmt.Errorf("backup location %s is not deleted yet", bkpLocationName)
				}
				return "", false, nil
			}
			_, err = DoRetryWithTimeoutWithGinkgoRecover(backupLocationDeleteStatusCheck, backupLocationDeleteTimeout, backupLocationDeleteRetryTime)
			Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Verifying backup location deletion status %s", bkpLocationName))
		})

		Step("Adding new backup location to the cluster", func() {
			log.InfoD("Adding new backup location to the cluster")
			for _, provider := range providers {
				newBkpLocationName = fmt.Sprintf("new-%s-%v-bl", provider, time.Now().Unix())
				newBackupLocationUID = uuid.New()
				newBackupLocationMap[newBackupLocationUID] = newBkpLocationName
				err := CreateBackupLocation(provider, newBkpLocationName, newBackupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating new backup location %s", newBkpLocationName))
			}
		})

		Step("Taking backups of application for each user again with new backup location", func() {
			log.InfoD("Taking backup of application for each user again with new backup location")
			var sem = make(chan struct{}, 10)
			var wg sync.WaitGroup
			for i := 0; i < numberOfUsers; i++ {
				sem <- struct{}{}
				time.Sleep(10 * time.Second)
				backupName := fmt.Sprintf("%s-%s-%v", "new", BackupNamePrefix, time.Now().Unix())
				newBackupNames = append(newBackupNames, backupName)
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					defer func() { <-sem }()
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespaces[0]})
					err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, newBkpLocationName, newBackupLocationUID, appContextsToBackup, labelSelectors, orgID, srcClusterUid, "", "", "", "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s] of applications [%s]", backupName, bkpNamespaces[0]))
				}(backupName)
			}
			wg.Wait()
			log.Infof("List of new backups - %v", newBackupNames)
		})

		Step("Share backup with users again with different access level", func() {
			log.InfoD("Share backup with users again with different access level")
			accessUserBackupContext, err = ShareBackupWithUsersAndAccessAssignment(newBackupNames, users, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Sharing backup %s with users %s", newBackupNames, users))
			log.Infof("The user/access/backup/context mapping is %v", accessUserBackupContext)
		})

		Step("Validate if the users with different access level can restore/delete backup", func() {
			log.InfoD("Validate if the users with different access level can restore/delete backup")
			for key, val := range accessUserBackupContext {
				restoreName := fmt.Sprintf("%s-%s-%v", key.user, RestoreNamePrefix, time.Now().Unix())
				access := key.accesses
				if access != ViewOnlyAccess {
					userRestoreContext[key.context] = restoreName
				}
				log.Infof("Removing the restores which will be deleted while validating FullAccess")
				if access == FullAccess {
					newBackupNames = removeStringItemFromSlice(newBackupNames, []string{val})
				}
				ValidateSharedBackupWithUsers(key.user, key.accesses, val, restoreName)
			}
		})
	})
	JustAfterEach(func() {
		var wg sync.WaitGroup
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		backupDriver := Inst().Backup
		for _, backupName := range newBackupNames {
			wg.Add(1)
			go func(backupName string) {
				defer GinkgoRecover()
				defer wg.Done()
				backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
				dash.VerifySafely(err, nil, fmt.Sprintf("Getting backup UID for backup %v", backupName))
				_, err = DeleteBackup(backupName, backupUID, orgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup %s", backupName))
			}(backupName)
		}
		wg.Wait()
		log.Infof("Generating user context")
		for _, userName := range users {
			ctxNonAdmin, err := backup.GetNonAdminCtx(userName, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContextsList = append(userContextsList, ctxNonAdmin)
		}
		log.Infof("Deleting restore created by users")
		for userContext, restoreName := range userRestoreContext {
			err = DeleteRestore(restoreName, orgID, userContext)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))
		}
		log.Infof("Deleting registered clusters for non-admin context")
		for _, ctxNonAdmin := range userContextsList {
			CleanupCloudSettingsAndClusters(make(map[string]string), "", "", ctxNonAdmin)
		}
		log.Infof("Cleaning up users")
		for _, userName := range users {
			wg.Add(1)
			go func(userName string) {
				defer GinkgoRecover()
				defer wg.Done()
				err := backup.DeleteUser(userName)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting user %v", userName))
			}(userName)
		}
		wg.Wait()
		CleanupCloudSettingsAndClusters(newBackupLocationMap, cloudCredName, cloudCredUID, ctx)

	})
})

// ViewOnlyFullBackupRestoreIncrementalBackup shares full backup with view and incremental backup with restore access
var _ = Describe("{ViewOnlyFullBackupRestoreIncrementalBackup}", func() {
	backupNames := make([]string, 0)
	userContexts := make([]context.Context, 0)
	var scheduledAppContexts []*scheduler.Context
	labelSelectors := make(map[string]string)
	var backupLocationUID string
	var cloudCredUID string
	var cloudCredUidList []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var customBackupLocationName string
	var credName string
	var fullBackupName string
	var incrementalBackupName string
	var bkpNamespaces = make([]string, 0)
	individualUser := "autogenerated-user-82939"
	backupLocationMap := make(map[string]string)

	JustBeforeEach(func() {
		StartTorpedoTest("ViewOnlyFullBackupRestoreIncrementalBackup",
			"Full backup view only and incremental backup restore access", nil, 82939)
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

	It("Full backup view only and incremental backup restore access", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Create Users", func() {
			log.InfoD("Creating a user with username - [%s] who is not part of any group", individualUser)
			firstName := "autogenerated-firstname"
			lastName := "autogenerated-last name"
			email := "autogenerated-email@cnbu.com"
			err := backup.AddUser(individualUser, firstName, lastName, email, commonPassword)
			log.FailOnError(err, "Failed to create user - %s", individualUser)

		})

		Step("Adding Credentials and Registering Backup Location", func() {
			log.InfoD("Creating cloud credentials and backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err := CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				customBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationMap[backupLocationUID] = customBackupLocationName
				err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
				log.InfoD("Created Backup Location with name - %s", customBackupLocationName)
			}
		})

		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
			// Registering for admin user
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			// Full backup
			for _, namespace := range bkpNamespaces {
				fullBackupName = fmt.Sprintf("%s-%v", "full-backup", time.Now().Unix())
				backupNames = append(backupNames, fullBackupName)
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, fullBackupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", fullBackupName))
			}

			//Incremental backup
			for _, namespace := range bkpNamespaces {
				incrementalBackupName = fmt.Sprintf("%s-%v", "incremental-backup", time.Now().Unix())
				backupNames = append(backupNames, incrementalBackupName)
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, incrementalBackupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", incrementalBackupName))
			}
			log.Infof("List of backups - %v", backupNames)
		})

		Step(fmt.Sprintf("Sharing full backup with view only access and incremental backup with full access with user [%s]", individualUser), func() {
			log.InfoD("Sharing full backup [%s] with view only access and incremental backup [%s] with full access with user [%s]", fullBackupName, incrementalBackupName, individualUser)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = ShareBackup(fullBackupName, nil, []string{individualUser}, ViewOnlyAccess, ctx)
			err = ShareBackup(incrementalBackupName, nil, []string{individualUser}, FullAccess, ctx)
		})

		Step("Validate that user with View Only access cannot restore or delete the backup", func() {
			log.InfoD("Validate that user with View Only access cannot restore or delete the backup")

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(individualUser, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContexts = append(userContexts, ctxNonAdmin)

			// Register Source and Destination cluster
			log.InfoD("Registering Source and Destination clusters from user context")
			err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")

			// Start Restore and confirm that user cannot restore
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, fullBackupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))
			log.Infof("Error returned - %s", err.Error())
			// Restore validation to make sure that the user with View Access cannot restore
			dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to restore backup"), true, "Verifying backup restore is not possible")

			// Get Admin Context - needed to get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			// Get Backup UID
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, fullBackupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", fullBackupName)

			// Delete backup to confirm that the user cannot delete the backup
			_, err = DeleteBackup(fullBackupName, backupUID, orgID, ctxNonAdmin)
			dash.VerifyFatal(strings.Contains(err.Error(), "doesn't have permission to delete backup"), true, "Verifying backup deletion is not possible")
		})

		Step("Validate that user with View Only access on full backup and full access to incremental backup can restore", func() {
			log.InfoD("Validate that user with View Only access on full backup and full access to incremental backup can restore")

			// Get user context
			ctxNonAdmin, err := backup.GetNonAdminCtx(individualUser, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")

			// Start Restore
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestore(restoreName, incrementalBackupName, make(map[string]string), destinationClusterName, orgID, ctxNonAdmin, make(map[string]string))
			log.FailOnError(err, "Restoring of backup [%s] has failed with name - [%s]", incrementalBackupName, restoreName)

			// Restore validation to make sure that the user with Full Access can restore
			log.InfoD("Restoring of backup [%s] was successful with name - [%s]", incrementalBackupName, restoreName)
			log.Infof("About to delete restore - %s", restoreName)
			err = DeleteRestore(restoreName, orgID, ctxNonAdmin)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))

			// Get Admin Context - needed to get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			// Get Backup UID
			backupDriver := Inst().Backup
			backupUID, err := backupDriver.GetBackupUID(ctx, incrementalBackupName, orgID)
			log.FailOnError(err, "Failed while trying to get backup UID for - %s", incrementalBackupName)

			// Delete backup to confirm that the user has Full Access
			backupDeleteResponse, err := DeleteBackup(incrementalBackupName, backupUID, orgID, ctxNonAdmin)
			log.FailOnError(err, "Backup [%s] could not be deleted by user [%s]", incrementalBackupName, individualUser)
			dash.VerifyFatal(backupDeleteResponse.String(), "", fmt.Sprintf("Verifying backup %s deletion", incrementalBackupName))
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")

		log.Infof("Deleting registered clusters for non-admin context")
		for _, ctxNonAdmin := range userContexts {
			err = DeleteCluster(SourceClusterName, orgID, ctxNonAdmin, true)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", SourceClusterName))
			err = DeleteCluster(destinationClusterName, orgID, ctxNonAdmin, true)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", destinationClusterName))
		}

		log.Infof("Cleaning up user")
		err = backup.DeleteUser(individualUser)
		log.FailOnError(err, "Error deleting user %v", individualUser)

		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})

// IssueMultipleRestoresWithNamespaceAndStorageClassMapping issues multiple restores with namespace and storage class mapping
var _ = Describe("{IssueMultipleRestoresWithNamespaceAndStorageClassMapping}", func() {
	var (
		scheduledAppContexts []*scheduler.Context
		bkpNamespaces        []string
		clusterUid           string
		clusterStatus        api.ClusterInfo_StatusInfo_Status
		backupLocationUID    string
		cloudCredName        string
		cloudCredUID         string
		bkpLocationName      string
		backupName           string
		userName             []string
		userCtx              context.Context
		scName               string
		restoreList          []string
		sourceScName         *storageApi.StorageClass
	)
	numberOfUsers := 1
	namespaceMap := make(map[string]string)
	backupLocationMap := make(map[string]string)
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	storageClassMapping := make(map[string]string)
	k8sStorage := storage.Instance()
	params := make(map[string]string)

	JustBeforeEach(func() {
		StartTorpedoTest("IssueMultipleRestoresWithNamespaceAndStorageClassMapping",
			"Issue multiple restores with namespace and storage class mapping", nil, 82945)
		log.InfoD("Deploy applications needed for backup")
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
	It("Issue multiple restores with namespace and storage class mapping", func() {
		namespaceMap[bkpNamespaces[0]] = fmt.Sprintf("new-namespace-%v", time.Now().Unix())
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Register cluster for backup", func() {
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Create new storage class on source and destination cluster for storage class mapping for restore", func() {
			log.InfoD("Create new storage class on source cluster for storage class mapping for restore")
			scName = fmt.Sprintf("replica-sc-%v", time.Now().Unix())
			params["repl"] = "2"
			v1obj := metaV1.ObjectMeta{
				Name: scName,
			}
			reclaimPolicyDelete := v1.PersistentVolumeReclaimDelete
			bindMode := storageApi.VolumeBindingImmediate
			scObj := storageApi.StorageClass{
				ObjectMeta:        v1obj,
				Provisioner:       k8s.CsiProvisioner,
				Parameters:        params,
				ReclaimPolicy:     &reclaimPolicyDelete,
				VolumeBindingMode: &bindMode,
			}
			_, err := k8sStorage.CreateStorageClass(&scObj)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating new storage class %v on source cluster %s", scName, SourceClusterName))

			log.InfoD("Switching cluster context to destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Failed to set destination kubeconfig")
			log.InfoD("Create new storage class on destination cluster for storage class mapping for restore")
			_, err = k8sStorage.CreateStorageClass(&scObj)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating new storage class %v on destination cluster %s", scName, destinationClusterName))
			log.InfoD("Switching cluster context back to source cluster")
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Failed to set source kubeconfig")
		})

		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", bkpLocationName))
			}
		})

		Step("Taking backup of application for different combination of restores", func() {
			log.InfoD("Taking  backup of application for different combination of restores")
			backupName = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, bkpNamespaces[0], time.Now().Unix())
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespaces[0]})
			err := CreateBackupWithValidation(ctx, backupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
		})

		Step("Getting storage class of the source cluster", func() {
			pvcs, err := core.Instance().GetPersistentVolumeClaims(bkpNamespaces[0], labelSelectors)
			singlePvc := pvcs.Items[0]
			sourceScName, err = core.Instance().GetStorageClassForPVC(&singlePvc)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Getting SC %v from PVC", sourceScName.Name))
		})

		Step("Create user", func() {
			log.InfoD("Create user")
			userName = createUsers(numberOfUsers)
			log.Infof("Created %v users and users list is %v", numberOfUsers, userName)
			userCtx, err = backup.GetNonAdminCtx(userName[0], commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
		})

		Step("Share backup with user with FullAccess", func() {
			log.InfoD("Share backup with user with FullAccess")
			err = ShareBackup(backupName, nil, userName, FullAccess, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Share backup %s with  user %s having FullAccess", backupName, userName))
			userBackups1, _ := GetAllBackupsForUser(userName[0], commonPassword)
			log.Infof(" the backup are", userBackups1)
			err = CreateApplicationClusters(orgID, "", "", userCtx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster for user")
		})

		Step("Restoring backup in the same namespace with user having FullAccess in different cluster", func() {
			log.InfoD("Restoring backup in the same namespace with user having FullAccess in different cluster")
			restoreName := fmt.Sprintf("same-namespace-full-access-diff-cluster-%s-%v", RestoreNamePrefix, time.Now().Unix())
			restoreList = append(restoreList, restoreName)
			err := CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, userCtx, make(map[string]string))
			dash.VerifyFatal(err, nil, "Restoring backup in the same namespace with user having FullAccess Access in different cluster")
		})

		Step("Restoring backup in new namespace with user having FullAccess in same cluster", func() {
			log.InfoD("Restoring backup in new namespace with user having FullAccess in same cluster")
			restoreName := fmt.Sprintf("new-namespace-full-access-same-cluster-%s-%v", RestoreNamePrefix, time.Now().Unix())
			restoreList = append(restoreList, restoreName)
			err := CreateRestore(restoreName, backupName, namespaceMap, SourceClusterName, orgID, userCtx, make(map[string]string))
			dash.VerifyFatal(err, nil, "Restoring backup in new namespace with user having FullAccess Access in same cluster")
		})

		Step("Restoring backup in new namespace with user having FullAccess in different cluster", func() {
			log.InfoD("Restoring backup in new namespace with user having FullAccess in different cluster")
			restoreName := fmt.Sprintf("new-namespace-full-access-diff-cluster-%s-%v", RestoreNamePrefix, time.Now().Unix())
			restoreList = append(restoreList, restoreName)
			err := CreateRestore(restoreName, backupName, namespaceMap, destinationClusterName, orgID, userCtx, make(map[string]string))
			dash.VerifyFatal(err, nil, "Restoring backup in new namespace with user having FullAccess Access in different cluster")
		})

		Step("Restoring backup in different storage class with user having FullAccess in same cluster", func() {
			log.InfoD("Restoring backup in different storage class with user having FullAccess Access in same cluster")
			storageClassMapping[sourceScName.Name] = scName
			restoreName := fmt.Sprintf("new-storage-class-full-access-same-cluster-%s-%v", RestoreNamePrefix, time.Now().Unix())
			restoreList = append(restoreList, restoreName)
			err = CreateRestore(restoreName, backupName, make(map[string]string), SourceClusterName, orgID, userCtx, storageClassMapping)
			dash.VerifyFatal(err, nil, "Restoring backup in different storage class with user having FullAccess in same cluster")
		})

		Step("Restoring backup in different storage class with user having FullAccess in different cluster", func() {
			log.InfoD("Restoring backup in different storage class with user having FullAccess Access in different cluster")
			storageClassMapping[sourceScName.Name] = scName
			restoreName := fmt.Sprintf("new-storage-class-full-access-diff-cluster-%s-%v", RestoreNamePrefix, time.Now().Unix())
			restoreList = append(restoreList, restoreName)
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, userCtx, storageClassMapping)
			dash.VerifyFatal(err, nil, "Restoring backup in different storage class with user having FullAccess in different cluster")
		})

		Step("Share backup with user with RestoreAccess", func() {
			err = ShareBackup(backupName, nil, userName, RestoreAccess, ctx)
			dash.VerifyFatal(err, nil, "Share backup with user with RestoreAccess")
		})

		Step("Restoring backup in the same namespace with user having RestoreAccess in different cluster", func() {
			restoreName := fmt.Sprintf("same-ns-diff-cluster-%s-%v", RestoreNamePrefix, time.Now().Unix())
			restoreList = append(restoreList, restoreName)
			err := CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, userCtx, make(map[string]string))
			dash.VerifyFatal(err, nil, "Restoring backup in the same namespace with user having RestoreAccess Access in different cluster")
		})

		Step("Restoring backup in new namespace with user having RestoreAccess in same cluster", func() {
			restoreName := fmt.Sprintf("new-namespace-same-cluster-%s-%v", RestoreNamePrefix, time.Now().Unix())
			restoreList = append(restoreList, restoreName)
			err := CreateRestore(restoreName, backupName, namespaceMap, SourceClusterName, orgID, userCtx, make(map[string]string))
			dash.VerifyFatal(err, nil, "Restoring backup in new namespace with user having RestoreAccess Access in same cluster")
		})

		Step("Restoring backup in new namespace with user having RestoreAccess in different cluster", func() {
			restoreName := fmt.Sprintf("new-namespace-diff-cluster-%s-%v", RestoreNamePrefix, time.Now().Unix())
			restoreList = append(restoreList, restoreName)
			err := CreateRestore(restoreName, backupName, namespaceMap, destinationClusterName, orgID, userCtx, make(map[string]string))
			dash.VerifyFatal(err, nil, "Restoring backup in new namespace with user having RestoreAccess Access in different cluster")
		})

		Step("Restoring backup in different storage class with user having RestoreAccess in same cluster", func() {
			log.InfoD("Restoring backup in different storage class with user having RestoreAccess in same cluster")
			storageClassMapping[sourceScName.Name] = scName
			restoreName := fmt.Sprintf("new-storage-class-restore-access-same-cluster-%s-%v", RestoreNamePrefix, time.Now().Unix())
			restoreList = append(restoreList, restoreName)
			err = CreateRestore(restoreName, backupName, make(map[string]string), SourceClusterName, orgID, userCtx, storageClassMapping)
			dash.VerifyFatal(err, nil, "Restoring backup in different storage class with user having RestoreAccess in same cluster")
		})

		Step("Restoring backup in different storage class with user having RestoreAccess in different cluster", func() {
			log.InfoD("Restoring backup in different storage class with user having RestoreAccess Access in different cluster")
			storageClassMapping[sourceScName.Name] = scName
			restoreName := fmt.Sprintf("new-storage-class-full-access-diff-cluster-%s-%v", RestoreNamePrefix, time.Now().Unix())
			restoreList = append(restoreList, restoreName)
			err = CreateRestore(restoreName, backupName, make(map[string]string), destinationClusterName, orgID, userCtx, storageClassMapping)
			dash.VerifyFatal(err, nil, "Restoring backup in different storage class with user having RestoreAccess in different cluster")
		})
	})
	JustAfterEach(func() {
		err := SetSourceKubeConfig()
		dash.VerifySafely(err, nil, "Switching context to source cluster")
		var wg sync.WaitGroup
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		log.InfoD("Deleting restore created by users")
		for _, restoreName := range restoreList {
			wg.Add(1)
			go func(restoreName string) {
				defer GinkgoRecover()
				defer wg.Done()
				err = DeleteRestore(restoreName, orgID, userCtx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))
			}(restoreName)
		}
		wg.Wait()
		log.InfoD("Deleting the newly created storage class")
		err = k8sStorage.DeleteStorageClass(scName)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting storage class %s from source cluster", scName))
		log.InfoD("Switching cluster context to destination cluster")
		err = SetDestinationKubeConfig()
		dash.VerifySafely(err, nil, "Switching context to destination cluster")
		err = k8sStorage.DeleteStorageClass(scName)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting storage class %s from destination cluster", scName))
		log.InfoD("Switching cluster context back to source cluster")
		err = SetSourceKubeConfig()
		dash.VerifySafely(err, nil, "Switching context to source cluster")
		log.InfoD("Deleting user clusters")
		CleanupCloudSettingsAndClusters(make(map[string]string), "", "", userCtx)
		log.InfoD("Cleaning up users")
		err = backup.DeleteUser(userName[0])
		dash.VerifySafely(err, nil, fmt.Sprintf("deleting user %v", userName[0]))
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// DeleteUsersRole deletes users and roles and verify
var _ = Describe("{DeleteUsersRole}", func() {

	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/58089
	numberOfUsers := 80
	roles := [3]backup.PxBackupRole{backup.ApplicationOwner, backup.InfrastructureOwner, backup.DefaultRoles}
	userRoleMapping := map[string]backup.PxBackupRole{}

	JustBeforeEach(func() {
		StartTorpedoTest("DeleteUsersRole", "Delete role and users", nil, 58089)
	})
	It("Delete user and roles", func() {
		Step("Create Users add roles", func() {
			log.InfoD("Creating %d users", numberOfUsers)
			var wg sync.WaitGroup
			for i := 1; i <= numberOfUsers; i++ {
				userName := fmt.Sprintf("testautouser%v", time.Now().Unix())
				firstName := fmt.Sprintf("FirstName%v", i)
				lastName := fmt.Sprintf("LastName%v", i)
				email := fmt.Sprintf("testuser%v@cnbu.com", time.Now().Unix())
				time.Sleep(2 * time.Second)
				role := roles[rand.Intn(len(roles))]
				wg.Add(1)
				go func(userName, firstName, lastName, email string, role backup.PxBackupRole) {
					defer GinkgoRecover()
					defer wg.Done()
					err := backup.AddUser(userName, firstName, lastName, email, commonPassword)
					log.FailOnError(err, "Failed to create user - %s", userName)
					log.InfoD("Adding role %v to user %v ", role, userName)
					err = backup.AddRoleToUser(userName, role, "")
					log.FailOnError(err, "Failed to add role to user - %s", userName)
				}(userName, firstName, lastName, email, role)
				userRoleMapping[userName] = role
			}
			wg.Wait()
		})
		Step("Delete roles from the users", func() {
			for userName, role := range userRoleMapping {
				log.Infof(fmt.Sprintf("Deleting [%s] from the user : [%s]", role, userName))
				err := backup.DeleteRoleFromUser(userName, role, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Removing role [%s] from the user [%s]", role, userName))
			}
		})
		Step("Validate if the roles are deleted from the users ", func() {
			result := false
			for user, role := range userRoleMapping {
				roles, err := backup.GetRolesForUser(user)
				log.FailOnError(err, "Failed to get roles for user - %s", user)
				for _, roleObj := range roles {
					if roleObj.Name == string(role) {
						result = true
						break
					}
				}
				dash.VerifyFatal(result, false, fmt.Sprintf("validation of deleted role [%s] from user [%s]", role, user))
			}
		})
		Step("Delete users", func() {
			for userName := range userRoleMapping {
				err := backup.DeleteUser(userName)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting the user [%s]", userName))
			}
		})
		Step("Validate if all the created users are deleted", func() {
			result := false
			remainingUsers, err := backup.GetAllUsers()
			log.FailOnError(err, "Failed to get users")
			for user := range userRoleMapping {
				for _, userObj := range remainingUsers {
					if userObj.Name == user {
						result = true
						break
					}
				}
				dash.VerifyFatal(result, false, fmt.Sprintf("validation of deleted user [%s]", user))
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(make([]*scheduler.Context, 0))
	})
})

// IssueMultipleDeletesForSharedBackup deletes the shared backup by multiple users while restoring is in-progress
var _ = Describe("{IssueMultipleDeletesForSharedBackup}", func() {
	numberOfUsers := 6
	users := make([]string, 0)
	restoreNames := make([]string, 0)
	userContexts := make([]context.Context, 0)
	namespaceMapping := make(map[string]string)
	backupLocationMap := make(map[string]string)
	var scheduledAppContexts []*scheduler.Context
	var backupName string
	var backupLocationName string
	var backupLocationUID string
	var cloudCredUID string
	var cloudCredUidList []string
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var credName string
	bkpNamespaces = make([]string, 0)
	JustBeforeEach(func() {
		StartTorpedoTest("IssueMultipleDeletesForSharedBackup",
			"Share backup with multiple users and delete the backup", nil, 82944)
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
	It("Share the backups and delete", func() {
		providers := getProviders()

		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Create Users", func() {
			users = createUsers(numberOfUsers)
		})
		Step("Adding Credentials and Registering Backup Location", func() {
			log.InfoD("Creating cloud credentials and backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err = CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				backupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationMap[backupLocationUID] = backupLocationName
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				log.FailOnError(err, "Backup location %s creation failed", backupLocationName)
			}
		})
		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			log.FailOnError(err, "Source and Destination cluster creation failed")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})
		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			backupName = fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces[0:1])
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, nil, orgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
		})
		backupMap := make(map[string]string, 0)
		Step("Share backup with multiple users", func() {
			log.InfoD("Share backup with multiple users")
			// Get Admin Context - needed to share backup and get backup UID
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			// Share backups with all the users
			err = ShareBackup(backupName, nil, users, FullAccess, ctx)
			log.FailOnError(err, "Failed to share backup %s", backupName)

			for _, user := range users {
				// Get user context
				ctxNonAdmin, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "Fetching non admin ctx")
				userContexts = append(userContexts, ctxNonAdmin)

				// Register Source and Destination cluster
				log.InfoD("Registering Source and Destination clusters from user context for user -%s", user)
				err = CreateApplicationClusters(orgID, "", "", ctxNonAdmin)
				log.FailOnError(err, "Failed to create source and destination cluster for user %s", user)

				// Get Backup UID
				backupDriver := Inst().Backup
				backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
				log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
				backupMap[backupName] = backupUID

				// Start Restore
				namespaceMapping[bkpNamespaces[0]] = bkpNamespaces[0] + "restored"
				restoreName := fmt.Sprintf("%s-%s", RestoreNamePrefix, user)
				restoreNames = append(restoreNames, restoreName)
				log.Infof("Creating restore %s for user %s", restoreName, user)
				_, err = CreateRestoreWithoutCheck(restoreName, backupName, namespaceMapping, destinationClusterName, orgID, ctxNonAdmin)
				log.FailOnError(err, "Failed to create restore %s for user %s", restoreName, user)
			}
		})
		Step("Delete the backups and validate", func() {
			log.InfoD("Delete the backups and validate")
			// Delete the backups
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var wg sync.WaitGroup
			backupDriver := Inst().Backup
			for _, user := range users {
				// Get user context
				ctxNonAdmin, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "Fetching non admin ctx")
				wg.Add(1)
				go func(user string) {
					defer GinkgoRecover()
					defer wg.Done()
					_, err = DeleteBackup(backupName, backupMap[backupName], orgID, ctxNonAdmin)
					log.FailOnError(err, "Failed to delete backup - %s", backupName)
					err = backupDriver.WaitForBackupDeletion(ctx, backupName, orgID, backupDeleteTimeout, backupDeleteRetryTime)
					log.FailOnError(err, "Error waiting for backup deletion %v", backupName)
				}(user)
			}
			wg.Wait()
		})

		Step("Validate Restores are successful", func() {
			log.InfoD("Validate Restores are successful")
			for _, user := range users {
				log.Infof("Validating Restore success for user %s", user)
				ctxNonAdmin, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "Fetching non admin ctx")
				for _, restore := range restoreNames {
					log.Infof("Validating Restore %s for user %s", restore, user)
					if strings.Contains(restore, user) {
						err = restoreSuccessCheck(restore, orgID, maxWaitPeriodForRestoreCompletionInMinute*time.Minute, 30*time.Second, ctxNonAdmin)
						dash.VerifyFatal(err, nil, "Inspecting the Restore success for - "+restore)
					}
				}
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		log.InfoD("Deleting restores")
		for _, user := range users {
			ctxNonAdmin, err := backup.GetNonAdminCtx(user, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			for _, restore := range restoreNames {
				if strings.Contains(restore, user) {
					log.Infof("deleting Restore %s for user %s", restore, user)
					err = DeleteRestore(restore, orgID, ctxNonAdmin)
					log.FailOnError(err, "Failed to delete restore %s", restore)
				}
			}
		}
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")

		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)

		for _, ctxNonAdmin := range userContexts {
			CleanupCloudSettingsAndClusters(nil, credName, cloudCredUID, ctxNonAdmin)
		}

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

// SwapShareBackup swaps backup created with same name between two users
var _ = Describe("{SwapShareBackup}", func() {

	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/82940
	numberOfUsers := 2
	users := make([]string, 0)
	userBackupLocationMapping := map[string]string{}
	var backupUIDList []string
	var backupName string
	var scheduledAppContexts []*scheduler.Context
	var backupLocationUID string
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var credNames []string
	var cloudCredUID string
	bkpNamespaces = make([]string, 0)
	userContextsList := make([]context.Context, 0)
	var backupLocationCreationInterval time.Duration

	JustBeforeEach(func() {
		StartTorpedoTest("SwapShareBackup",
			"Share backup with same name between two users", nil, 82940)
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
		backupLocationCreationInterval = 10 * time.Second
	})
	It("Share the backup with same name", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Create Users", func() {
			log.InfoD("Creating %d users", numberOfUsers)
			var wg sync.WaitGroup
			for i := 1; i <= numberOfUsers; i++ {
				time.Sleep(3 * time.Second)
				userName := fmt.Sprintf("testautouser%v", time.Now().Unix())
				firstName := fmt.Sprintf("FirstName%v", i)
				lastName := fmt.Sprintf("LastName%v", i)
				email := fmt.Sprintf("testuser%v@cnbu.com", time.Now().Unix())
				wg.Add(1)
				go func(userName, firstName, lastName, email string) {
					defer GinkgoRecover()
					defer wg.Done()
					err := backup.AddUser(userName, firstName, lastName, email, commonPassword)
					log.FailOnError(err, "Failed to create user - %s", userName)
					users = append(users, userName)
				}(userName, firstName, lastName, email)
			}
			wg.Wait()
		})
		Step(fmt.Sprintf("Adding Credentials and Registering Backup Location for %s and %s", users[0], users[1]), func() {
			log.InfoD(fmt.Sprintf("Creating cloud credentials and backup location for %s and %s", users[0], users[1]))
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()

				for _, user := range users {
					credName := fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
					err := backup.AddRoleToUser(user, backup.InfrastructureOwner, fmt.Sprintf("Adding Infra Owner role to %s", user))
					log.FailOnError(err, "Failed to add role to user - %s", user)
					ctxNonAdmin, err := backup.GetNonAdminCtx(user, commonPassword)
					log.FailOnError(err, "Fetching non admin ctx")
					err = CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctxNonAdmin)
					log.FailOnError(err, "Failed to create cloud credential - %s", err)
					credNames = append(credNames, credName)
					backupLocationName := fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
					subPath := fmt.Sprintf("%s-path", user)
					err = CreateBackupLocationWithContext(provider, backupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "", subPath, ctxNonAdmin)
					log.FailOnError(err, "Failed to add backup location %s using provider %s to user - %s", backupLocationName, provider, user)
					userBackupLocationMapping[user] = backupLocationName
					time.Sleep(backupLocationCreationInterval)
				}
			}
		})
		for _, user := range users {
			Step(fmt.Sprintf("Register source and destination cluster for backup on %s ", user), func() {
				log.InfoD("Registering Source and Destination clusters as user : %s and verifying the status", user)
				ctx, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "Fetching non admin ctx")
				err = CreateApplicationClusters(orgID, "", "", ctx)
				log.FailOnError(err, "Failed creating source and destination cluster for user : %s", user)
				clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
				log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
				dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
				clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			})
			Step(fmt.Sprintf("Taking backup of applications as %s", user), func() {
				log.InfoD("Taking backup of applications as user : %s ", user)
				backupName = "backup1-82940"
				ctx, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "Fetching non admin ctx")

				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespaces[0]})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, userBackupLocationMapping[user], backupLocationUID, appContextsToBackup, nil, orgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))

				backupDriver := Inst().Backup
				backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
				backupUIDList = append(backupUIDList, backupUID)
				log.FailOnError(err, "Failed getting backup uid for backup name %s", backupName)
			})
		}
		Step(fmt.Sprintf("Share backup with %s", users[1]), func() {
			log.InfoD(fmt.Sprintf("Share backup from %s to %s and validate", users[0], users[1]))
			ctx, err := backup.GetNonAdminCtx(users[0], commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			// Share backup with the user
			err = ShareBackup(backupName, nil, []string{users[1]}, RestoreAccess, ctx)
			log.FailOnError(err, "Failed to share backup %s", backupName)
		})
		Step(fmt.Sprintf("validate the backup shared %s is present in user context %s", backupName, users[1]), func() {
			userBackups, _ := GetAllBackupsForUser(users[1], commonPassword)
			backupCount := 0
			for _, backup := range userBackups {
				if backup == backupName {
					backupCount = backupCount + 1
				}
			}
			dash.VerifyFatal(backupCount, numberOfUsers, fmt.Sprintf("Validating the shared backup [%s] is present in user context [%s]", backupName, users[1]))
		})
		Step(fmt.Sprintf("Restore the shared backup  %s with user context %s", backupName, users[1]), func() {
			log.InfoD(fmt.Sprintf("Restore the shared backup  %s with user context %s", users[1], users[0]))
			ctxNonAdmin, err := backup.GetNonAdminCtx(users[1], commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestoreWithUID(restoreName, backupName, nil, destinationClusterName, orgID, ctxNonAdmin, nil, backupUIDList[0])
			log.FailOnError(err, "Failed to restore %s", restoreName)
		})
		Step(fmt.Sprintf("Share backup with %s", users[0]), func() {
			log.InfoD(fmt.Sprintf("Share backup from %s to %s and validate", users[1], users[0]))
			ctx, err := backup.GetNonAdminCtx(users[1], commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			// Share backup with the user
			err = ShareBackup(backupName, nil, []string{users[0]}, RestoreAccess, ctx)
			log.FailOnError(err, "Failed to share backup %s", backupName)
		})
		Step(fmt.Sprintf("validate the backup shared %s is present in user context %s", backupName, users[0]), func() {
			userBackups, _ := GetAllBackupsForUser(users[0], commonPassword)
			backupCount := 0
			for _, backup := range userBackups {
				if backup == backupName {
					backupCount = backupCount + 1
				}
			}
			dash.VerifyFatal(backupCount, numberOfUsers, fmt.Sprintf("Validating the shared backup [%s] is present in user context [%s]", backupName, users[0]))
		})
		Step(fmt.Sprintf("Restore the shared backup  %s with user context %s", backupName, users[0]), func() {
			log.InfoD(fmt.Sprintf("Restore the shared backup  %s with user context %s", users[0], users[0]))
			ctxNonAdmin, err := backup.GetNonAdminCtx(users[0], commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
			err = CreateRestoreWithUID(restoreName, backupName, nil, destinationClusterName, orgID, ctxNonAdmin, nil, backupUIDList[1])
			log.FailOnError(err, "Failed to restore %s", restoreName)
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		Step("Delete all synced backups from the admin", func() {
			log.InfoD("Delete all synced backups from the admin")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = DeleteAllBackups(ctx, orgID)
			log.FailOnError(err, "Delete all synced backups from the admin")
		})
		log.InfoD("Deleting all restores")
		for _, userName := range users {
			ctx, err := backup.GetNonAdminCtx(userName, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			allRestores, err := GetAllRestoresNonAdminCtx(ctx)
			log.FailOnError(err, "Fetching all restores for nonAdminCtx")
			for _, restoreName := range allRestores {
				err = DeleteRestore(restoreName, orgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Verifying restore deletion - %s", restoreName))
			}
		}

		log.InfoD("Delete all backups")
		for i := numberOfUsers - 1; i >= 0; i-- {
			ctx, err := backup.GetNonAdminCtx(users[i], commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			_, err = DeleteBackup(backupName, backupUIDList[i], orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying backup deletion - %s", backupName))
			backupDriver := Inst().Backup
			backupEnumerateReq := &api.BackupEnumerateRequest{
				OrgId: orgID,
			}
			backupDeletionSuccessCheck := func() (interface{}, bool, error) {
				currentBackups, err := backupDriver.EnumerateBackup(ctx, backupEnumerateReq)
				if err != nil {
					return "", true, err
				}
				for _, backupObject := range currentBackups.GetBackups() {
					if backupObject.Uid == backupUIDList[i] {
						return "", true, fmt.Errorf("backupObject [%s] is not yet deleted", backupObject.Uid)
					}
				}
				return "", false, nil
			}
			_, err = DoRetryWithTimeoutWithGinkgoRecover(backupDeletionSuccessCheck, backupDeleteTimeout, backupDeleteRetryTime)
			log.FailOnError(err, fmt.Sprintf("Error deleting backup - %s for user - %s", backupName, users[i]))
		}

		log.Infof("Generating user context")
		for _, userName := range users {
			ctxNonAdmin, err := backup.GetNonAdminCtx(userName, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			userContextsList = append(userContextsList, ctxNonAdmin)
		}

		log.Infof("Deleting registered clusters for non-admin context")
		for _, ctxNonAdmin := range userContextsList {
			CleanupCloudSettingsAndClusters(make(map[string]string), "", "", ctxNonAdmin)
		}

		// Cleanup all backup locations
		for _, userName := range users {
			ctx, err := backup.GetNonAdminCtx(userName, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			allBackupLocations, err := getAllBackupLocations(ctx)
			dash.VerifySafely(err, nil, "Verifying fetching of all backup locations")
			for backupLocationUid, backupLocationName := range allBackupLocations {
				err = DeleteBackupLocation(backupLocationName, backupLocationUid, orgID, true)
				log.FailOnError(err, fmt.Sprintf("Error while deleting backup Location - %s ", backupLocationName))
				dash.VerifySafely(err, nil, fmt.Sprintf("Verifying backup location deletion - %s", backupLocationName))
			}

			for _, backupLocationName := range allBackupLocations {
				backupLocationDeleteStatusCheck := func() (interface{}, bool, error) {
					status, err := IsBackupLocationPresent(backupLocationName, ctx, orgID)
					if err != nil {
						return "", true, fmt.Errorf("backup location %s still present with error %v", backupLocationName, err)
					}
					if status == true {
						return "", true, fmt.Errorf("backup location %s is not deleted yet", backupLocationName)
					}
					return "", false, nil
				}
				_, err = DoRetryWithTimeoutWithGinkgoRecover(backupLocationDeleteStatusCheck, backupLocationDeleteTimeout, backupLocationDeleteRetryTime)
				Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Verifying backup location deletion status %s", backupLocationName))
			}
		}

		for _, userName := range users {
			ctx, err := backup.GetNonAdminCtx(userName, commonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			allCloudCredentials, err := getAllCloudCredentials(ctx)
			dash.VerifySafely(err, nil, "Verifying fetching of all cloud credentials")
			for cloudCredentialUid, cloudCredentialName := range allCloudCredentials {
				cloudCredDeleteStatus := func() (interface{}, bool, error) {
					err := DeleteCloudCredential(cloudCredentialName, orgID, cloudCredentialUid)
					if err != nil {
						return "", true, fmt.Errorf("deleting cloud cred %s failed - err", cloudCredentialName)
					}
					return "", false, nil
				}
				_, err := DoRetryWithTimeoutWithGinkgoRecover(cloudCredDeleteStatus, cloudAccountDeleteTimeout, cloudAccountDeleteRetryTime)
				Inst().Dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cloud cred %s", cloudCredentialName))
			}
		}

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
