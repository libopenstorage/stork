package tests

import (
	"fmt"
	"strings"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/torpedo/drivers"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

// VerifyRBACforInfraAdmin Validates the RBAC operation for infra-admin user.
var _ = Describe("{VerifyRBACForInfraAdmin}", Label(TestCaseLabelsMap[VerifyRBACForInfraAdmin]...), func() {
	var (
		scheduledAppContexts            []*scheduler.Context
		backupLocationMap               = make(map[string]string)
		backupLocationNameMap           = make(map[string]string)
		backupLocationUIDMap            = make(map[string]string)
		cloudCredentialNameMap          = make(map[string]string)
		cloudCredentialUIDMap           = make(map[string]string)
		scheduleNameMap                 = make(map[string]string)
		periodicSchedulePolicyNameMap   = make(map[string]string)
		periodicSchedulePolicyUidMap    = make(map[string]string)
		preRuleNameMap                  = make(map[string]string)
		postRuleNameMap                 = make(map[string]string)
		preRuleUidMap                   = make(map[string]string)
		postRuleUidMap                  = make(map[string]string)
		numOfUsers                      = 3
		infraAdminUser                  string
		customUser                      string
		customInfraRoleName             backup.PxBackupRole
		customRoleName                  backup.PxBackupRole
		userClusterMap                  = make(map[string]map[string]string)
		backupNameMap                   = make(map[string]string)
		restoreNameMap                  = make(map[string]string)
		userBackupNamesMap              = make(map[string][]string)
		cloudCredentialFromAdmin        []string
		backupLocationsFromAdmin        []string
		userNames                       = make([]string, 0)
		providers                       = GetBackupProviders()
		bkpNamespaces                   []string
		infraAdminRole                  backup.PxBackupRole = backup.InfrastructureOwner
		labelSelectors                  map[string]string
		mutex                           sync.Mutex
		nsLabelsMap                     map[string]string
		nsLabelString                   string
		nsLabelsMapCustomUser           map[string]string
		nsLabelStringCustomUser         string
		manualBackupWithLabel           string
		restoreForManualBackupWithLabel string
		srcClusterUid                   string
		backupScheduleWithLabel         string
		scheduledBackupNameWithLabel    string
		multipleRestoreMapping          map[string]string
		customRestoreName               string
		customUserLabelledRestoreNames  []string
		customUserLabelledBackupNames   []string
		infraAdminLabelledRestoreNames  []string
		infraAdminLabelledBackupNames   []string
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("VerifyRBACForInfraAdmin", "Validates the RBAC operation for infra-admin user.", nil, 87886, Ak, Q3FY24)
		backupLocationMap = make(map[string]string)
		log.InfoD("scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, appCtx := range appContexts {
				appCtx.ReadinessTimeout = AppReadinessTimeout
				scheduledAppContexts = append(scheduledAppContexts, appCtx)
				namespace := GetAppNamespace(appCtx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})

	It("Validates the RBAC operation for infra-admin user", func() {
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Adding labels to namespaces", func() {
			log.InfoD("Adding labels to namespaces")
			nsLabelsMap = GenerateRandomLabels(2)
			err := AddLabelsToMultipleNamespaces(nsLabelsMap, bkpNamespaces)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Adding labels [%v] to namespaces [%v] for infra admin validation", nsLabelsMap, bkpNamespaces))
		})

		Step("Generating namespace label string from label map for namespaces", func() {
			log.InfoD("Generating namespace label string from label map for namespaces")
			nsLabelString = MapToKeyValueString(nsLabelsMap)
			log.Infof("label string for namespaces %s", nsLabelString)
		})

		Step(fmt.Sprintf("Create a user with %s role", infraAdminRole), func() {
			log.InfoD(fmt.Sprintf("Creating a user with %s role", infraAdminRole))
			infraAdminUser = CreateUsers(1)[0]
			err := backup.AddRoleToUser(infraAdminUser, infraAdminRole, fmt.Sprintf("Adding %v role to %s", infraAdminRole, infraAdminUser))
			log.FailOnError(err, "failed to add role %s to the user %s", infraAdminRole, infraAdminUser)
		})

		Step("Verify Infra-Admin User has permission to create cloud credential  and backup location", func() {
			log.InfoD("Verify Infra-Admin User has permission to create cloud credential and backup location")
			nonAdminCtx, err := backup.GetNonAdminCtx(infraAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", infraAdminUser)
			for _, provider := range providers {
				cloudCredentialNameMap[infraAdminUser] = fmt.Sprintf("autogenerated-cred-%v", RandomString(5))
				cloudCredentialUIDMap[infraAdminUser] = uuid.New()
				err = CreateCloudCredential(provider, cloudCredentialNameMap[infraAdminUser], cloudCredentialUIDMap[infraAdminUser], BackupOrgID, nonAdminCtx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of create cloud credential %s using provider %s for the user", cloudCredentialNameMap[infraAdminUser], provider))
				backupLocationNameMap[infraAdminUser] = fmt.Sprintf("autogenerated-backup-location-%v", RandomString(5))
				backupLocationUIDMap[infraAdminUser] = uuid.New()
				err = CreateBackupLocationWithContext(provider, backupLocationNameMap[infraAdminUser], backupLocationUIDMap[infraAdminUser], cloudCredentialNameMap[infraAdminUser], cloudCredentialUIDMap[infraAdminUser], getGlobalBucketName(provider), BackupOrgID, "", nonAdminCtx, true)
				log.FailOnError(err, "failed to create backup location %s using provider %s for the user", backupLocationNameMap[infraAdminUser], provider)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of create backup location %s using provider %s for the user", backupLocationNameMap[infraAdminUser], provider))
				backupLocationMap[backupLocationUIDMap[infraAdminUser]] = backupLocationNameMap[infraAdminUser]
			}
		})

		Step(fmt.Sprintf("Verify Infra-Admin User has permission to create a schedule policy"), func() {
			log.InfoD("Verify Infra-Admin User has permission to create a schedule policy")
			nonAdminCtx, err := backup.GetNonAdminCtx(infraAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", infraAdminUser)
			periodicSchedulePolicyNameMap[infraAdminUser] = fmt.Sprintf("%s-%v", "periodic", RandomString(5))
			periodicSchedulePolicyUidMap[infraAdminUser] = uuid.New()
			periodicSchedulePolicyInterval := int64(15)
			err = CreateBackupScheduleIntervalPolicy(5, periodicSchedulePolicyInterval, 5, periodicSchedulePolicyNameMap[infraAdminUser], periodicSchedulePolicyUidMap[infraAdminUser], BackupOrgID, nonAdminCtx, false, false)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval [%v] minutes named [%s]", periodicSchedulePolicyInterval, periodicSchedulePolicyNameMap[infraAdminUser]))
		})

		Step(fmt.Sprintf("Verify Infra-Admin User has permission to create pre and post exec rules for applications"), func() {
			log.InfoD("Verify Infra-Admin User has permission to create pre and post exec rules for applications")
			nonAdminCtx, err := backup.GetNonAdminCtx(infraAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", infraAdminUser)
			preRuleNameMap[infraAdminUser], postRuleNameMap[infraAdminUser], err = CreateRuleForBackupWithMultipleApplications(BackupOrgID, Inst().AppList, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of pre and post exec rules for applications from px-admin"))
			if preRuleNameMap[infraAdminUser] != "" {
				preRuleUidMap[infraAdminUser], err = Inst().Backup.GetRuleUid(BackupOrgID, nonAdminCtx, preRuleNameMap[infraAdminUser])
				log.FailOnError(err, "Fetching pre backup rule [%s] uid", preRuleNameMap[infraAdminUser])
				log.Infof("Pre backup rule [%s] uid: [%s]", preRuleNameMap[infraAdminUser], preRuleUidMap[infraAdminUser])
			}
			if postRuleNameMap[infraAdminUser] != "" {
				postRuleUidMap[infraAdminUser], err = Inst().Backup.GetRuleUid(BackupOrgID, nonAdminCtx, postRuleNameMap[infraAdminUser])
				log.FailOnError(err, "Fetching post backup rule [%s] uid", postRuleNameMap[infraAdminUser])
				log.Infof("Post backup rule [%s] uid: [%s]", postRuleNameMap[infraAdminUser], postRuleUidMap[infraAdminUser])
			}
		})

		Step(fmt.Sprintf("Create source and destination cluster from the Infra admin user %s", infraAdminUser), func() {
			log.InfoD(fmt.Sprintf("Creating source and destination cluster from the infra admin user %s", infraAdminUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(infraAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", infraAdminUser)
			err = CreateApplicationClusters(BackupOrgID, "", "", nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with App-User ctx", SourceClusterName, DestinationClusterName))
			for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
				clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, clusterName, nonAdminCtx)
				log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", clusterName))
				dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", clusterName))
			}
			srcClusterUid, err = Inst().Backup.GetClusterUID(nonAdminCtx, BackupOrgID, SourceClusterName)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			log.Infof("Cluster [%s] uid: [%s]", SourceClusterName, srcClusterUid)
		})

		Step("Validate taking manual backup of applications with namespace label", func() {
			log.InfoD("Validate taking manual backup of applications with namespace label")
			nonAdminCtx, err := backup.GetNonAdminCtx(infraAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", infraAdminUser)
			log.Infof("Cluster [%s] uid: [%s]", SourceClusterName, srcClusterUid)
			manualBackupWithLabel = fmt.Sprintf("%s-%v", "infraadmin", RandomString(15))
			appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateBackupWithNamespaceLabelWithValidation(nonAdminCtx, manualBackupWithLabel, SourceClusterName, backupLocationNameMap[infraAdminUser], backupLocationUIDMap[infraAdminUser], appContextsExpectedInBackup,
				nil, BackupOrgID, srcClusterUid, "", "", "", "", nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup [%s] creation with labels [%s]", manualBackupWithLabel, nsLabelString))
			infraAdminLabelledBackupNames = append(infraAdminLabelledBackupNames, manualBackupWithLabel)
			err = NamespaceLabelBackupSuccessCheck(manualBackupWithLabel, nonAdminCtx, bkpNamespaces, nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespaces [%v] are backed up and check for labels [%s] applied to backups [%s]", bkpNamespaces, nsLabelString, manualBackupWithLabel))
		})

		Step("Validate restoring manual backup of applications with namespace label", func() {
			log.InfoD("Validate restoring manual backup of applications with namespace label")
			nonAdminCtx, err := backup.GetNonAdminCtx(infraAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", infraAdminUser)
			restoreForManualBackupWithLabel = fmt.Sprintf("%s-%s", RestoreNamePrefix, manualBackupWithLabel)
			appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateRestoreWithValidation(nonAdminCtx, restoreForManualBackupWithLabel, manualBackupWithLabel, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsExpectedInBackup)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration of backup %s", manualBackupWithLabel))
			infraAdminLabelledRestoreNames = append(infraAdminLabelledRestoreNames, restoreForManualBackupWithLabel)
		})

		Step("Validate creating scheduled backup with namespace label", func() {
			log.InfoD("Validate creating scheduled backup with namespace label")
			nonAdminCtx, err := backup.GetNonAdminCtx(infraAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", infraAdminUser)
			backupScheduleWithLabel = fmt.Sprintf("%s-%v", BackupNamePrefix, RandomString(4))
			appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			scheduledBackupNameWithLabel, err = CreateScheduleBackupWithNamespaceLabelWithValidation(nonAdminCtx, backupScheduleWithLabel, SourceClusterName, backupLocationNameMap[infraAdminUser], backupLocationUIDMap[infraAdminUser], appContextsExpectedInBackup,
				nil, BackupOrgID, "", "", "", "", nsLabelString, periodicSchedulePolicyNameMap[infraAdminUser], periodicSchedulePolicyUidMap[infraAdminUser])
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of creating first schedule backup %s with labels [%v]", backupScheduleWithLabel, nsLabelString))
			err = SuspendBackupSchedule(backupScheduleWithLabel, periodicSchedulePolicyNameMap[infraAdminUser], BackupOrgID, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule [%s] for user [%s]", backupScheduleWithLabel, infraAdminUser))
			infraAdminLabelledBackupNames = append(infraAdminLabelledBackupNames, scheduledBackupNameWithLabel)
			err = NamespaceLabelBackupSuccessCheck(scheduledBackupNameWithLabel, nonAdminCtx, bkpNamespaces, nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespaces [%v] are backed up and check for labels [%s] applied to backups [%s]", bkpNamespaces, nsLabelString, scheduledBackupNameWithLabel))
		})

		Step("Validate restoring the scheduled backup with namespace label", func() {
			log.InfoD("Validate restoring the scheduled backup with namespace label")
			nonAdminCtx, err := backup.GetNonAdminCtx(infraAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", infraAdminUser)
			multipleBackupNamespace, err := FetchNamespacesFromBackup(nonAdminCtx, scheduledBackupNameWithLabel, BackupOrgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching namespaces %v from backup %v", multipleBackupNamespace, scheduledBackupNameWithLabel))
			multipleRestoreMapping = make(map[string]string)
			for _, namespace := range multipleBackupNamespace {
				restoredNameSpace := fmt.Sprintf("%s-%v", scheduledBackupNameWithLabel, RandomString(3))
				multipleRestoreMapping[namespace] = restoredNameSpace
			}
			customRestoreName = fmt.Sprintf("%s-%v", "customrestore", RandomString(10))
			appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateRestoreWithValidation(nonAdminCtx, customRestoreName, scheduledBackupNameWithLabel, multipleRestoreMapping, make(map[string]string), DestinationClusterName, BackupOrgID, appContextsExpectedInBackup)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying multiple backup restore [%s] in custom namespace [%v]", customRestoreName, multipleRestoreMapping))
			infraAdminLabelledRestoreNames = append(infraAdminLabelledRestoreNames, customRestoreName)
		})

		Step(fmt.Sprintf("Validate deleting namespace labelled backups and restores from the infra admin %s user context", infraAdminUser), func() {
			log.InfoD(fmt.Sprintf("Validate deleting namespace labelled backups and restores from the infra admin user %s user context", infraAdminUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(infraAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", infraAdminUser)
			var wg sync.WaitGroup
			for _, backupName := range infraAdminLabelledBackupNames {
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					backupUid, err := Inst().Backup.GetBackupUID(nonAdminCtx, backupName, BackupOrgID)
					log.FailOnError(err, "Failed to fetch the backup %s uid of the user %s", backupName, infraAdminUser)
					_, err = DeleteBackup(backupName, backupUid, BackupOrgID, nonAdminCtx)
					log.FailOnError(err, "Failed to delete the backup %s of the user %s", backupName, infraAdminUser)
					err = DeleteBackupAndWait(backupName, nonAdminCtx)
					log.FailOnError(err, fmt.Sprintf("waiting for backup [%s] deletion", backupName))
				}(backupName)
			}
			wg.Wait()
			for _, restoreName := range infraAdminLabelledRestoreNames {
				err := DeleteRestore(restoreName, BackupOrgID, nonAdminCtx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the deletion of the restore named [%s]", restoreName))
			}
		})

		Step(fmt.Sprintf("Delete Infra Admin %s backup schedule with label ", infraAdminUser), func() {
			log.InfoD(fmt.Sprintf("Delete Infra Admin %s backup schedule ", infraAdminUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(infraAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", infraAdminUser)
			err = DeleteSchedule(backupScheduleWithLabel, SourceClusterName, BackupOrgID, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Backup Schedule [%s] for user [%s]", backupScheduleWithLabel, infraAdminUser))
		})

		Step(fmt.Sprintf("Verify px-admin group user can list RBAC resources created by Infra-Admin User"), func() {
			log.InfoD("Verify px-admin group user can list RBAC resources created by Infra-Admin User")
			adminCtx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch admin ctx")
			for _, provider := range providers {
				if provider != drivers.ProviderNfs {
					cloudCredentials, err := GetAllCloudCredentials(adminCtx)
					log.FailOnError(err, "Fetching cloud credential from px-admin")
					for _, cloudCredentialName := range cloudCredentials {
						cloudCredentialFromAdmin = append(cloudCredentialFromAdmin, cloudCredentialName)
					}
					if !IsPresent(cloudCredentialFromAdmin, cloudCredentialNameMap[infraAdminUser]) {
						err := fmt.Errorf("cloud credential[%s] is not listed in cloud credentials from admin %s", cloudCredentialNameMap[infraAdminUser], cloudCredentialFromAdmin)
						log.FailOnError(fmt.Errorf(""), err.Error())
					}
				}
			}
			backupLocations, err := GetAllBackupLocations(adminCtx)
			log.FailOnError(err, "Fetching backup location from px-admin")
			for _, backupLocationName := range backupLocations {
				backupLocationsFromAdmin = append(backupLocationsFromAdmin, backupLocationName)
			}
			if !IsPresent(backupLocationsFromAdmin, backupLocationNameMap[infraAdminUser]) {
				err := fmt.Errorf("backup location [%s] is not listed in backup location names from admin %s", backupLocationNameMap[infraAdminUser], backupLocationsFromAdmin)
				log.FailOnError(fmt.Errorf(""), err.Error())
			}
			schedulePoliciesFromAdmin, err := Inst().Backup.GetAllSchedulePolicies(adminCtx, BackupOrgID)
			log.FailOnError(err, "Fetching backup schedules from px-admin")
			if !IsPresent(schedulePoliciesFromAdmin, periodicSchedulePolicyNameMap[infraAdminUser]) {
				err := fmt.Errorf("schedule policy [%s] is not listed in schedule policies  from admin %s", periodicSchedulePolicyNameMap[infraAdminUser], schedulePoliciesFromAdmin)
				log.FailOnError(fmt.Errorf(""), err.Error())
			}

			rulesFromAdmin, err := Inst().Backup.GetAllRules(adminCtx, BackupOrgID)
			log.FailOnError(err, "Fetching rules from px-admin")
			if preRuleNameMap[infraAdminUser] != "" {
				if !IsPresent(rulesFromAdmin, preRuleNameMap[infraAdminUser]) {
					err := fmt.Errorf("pre rule [%s] is not listed in rules from admin %s", preRuleNameMap[infraAdminUser], rulesFromAdmin)
					log.FailOnError(fmt.Errorf(""), err.Error())
				}
			}
			if postRuleNameMap[infraAdminUser] != "" {
				if !IsPresent(rulesFromAdmin, postRuleNameMap[infraAdminUser]) {
					err := fmt.Errorf("post rule [%s] is not listed in rules from admin %s", postRuleNameMap[infraAdminUser], rulesFromAdmin)
					log.FailOnError(fmt.Errorf(""), err.Error())
				}
			}
		})

		Step("Create Users with Different types of roles", func() {
			log.InfoD("Create Users with Different types of roles")
			roles := [3]backup.PxBackupRole{backup.ApplicationOwner, backup.InfrastructureOwner, backup.ApplicationUser}
			for i := 1; i <= numOfUsers/3; i++ {
				for _, role := range roles {
					userName := CreateUsers(1)[0]
					err := backup.AddRoleToUser(userName, role, fmt.Sprintf("Adding %v role to %s", role, userName))
					log.FailOnError(err, "Failed to add role for user - %s", userName)
					userNames = append(userNames, userName)
					log.FailOnError(err, "Failed to fetch uid for - %s", userName)
				}
			}
		})

		Step("Verify Infra-Admin User has permission to share RBAC resources with non-admin users", func() {
			log.InfoD("Verify Infra-Admin User has permission to share RBAC resources with non-admin users")
			nonAdminCtx, err := backup.GetNonAdminCtx(infraAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", infraAdminUser)
			for _, provider := range providers {
				if provider != drivers.ProviderNfs {
					log.Infof("Update CloudAccount - %s ownership for users - [%v]", cloudCredentialNameMap[infraAdminUser], userNames)
					err = AddCloudCredentialOwnership(cloudCredentialNameMap[infraAdminUser], cloudCredentialUIDMap[infraAdminUser], userNames, nil, Read, Invalid, nonAdminCtx, BackupOrgID)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of owbership for CloudCredential- %s", cloudCredentialNameMap[infraAdminUser]))
				}
			}
			log.InfoD("Update BackupLocation - %s ownership for users - [%v]", backupLocationNameMap[infraAdminUser], userNames)
			err = AddBackupLocationOwnership(backupLocationNameMap[infraAdminUser], backupLocationUIDMap[infraAdminUser], userNames, nil, Read, Invalid, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of owbership for backuplocation - %s", backupLocationNameMap[infraAdminUser]))
			log.InfoD("Update SchedulePolicy - %s ownership for users - [%v]", periodicSchedulePolicyNameMap[infraAdminUser], userNames)
			err = AddSchedulePolicyOwnership(periodicSchedulePolicyNameMap[infraAdminUser], periodicSchedulePolicyUidMap[infraAdminUser], userNames, nil, Read, Invalid, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for schedulepolicy - %s", periodicSchedulePolicyNameMap[infraAdminUser]))
			log.InfoD("Update Application Rules ownership for users - [%v]", userNames)
			if preRuleNameMap[infraAdminUser] != "" {
				err = AddRuleOwnership(preRuleNameMap[infraAdminUser], preRuleUidMap[infraAdminUser], userNames, nil, Read, Invalid, nonAdminCtx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for pre-rule of application"))
			}
			if postRuleNameMap[infraAdminUser] != "" {
				err = AddRuleOwnership(postRuleNameMap[infraAdminUser], postRuleUidMap[infraAdminUser], userNames, nil, Read, Invalid, nonAdminCtx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for post-rule of application"))
			}
		})
		for _, user := range userNames {
			Step(fmt.Sprintf("Create source and destination cluster from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Creating source and destination cluster from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				err = CreateApplicationClusters(BackupOrgID, "", "", nonAdminCtx)
				log.FailOnError(err, "failed create source and destination cluster from the user %s", user)
				clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, nonAdminCtx)
				log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
				dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
				userClusterMap[user] = make(map[string]string)
				for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
					userClusterUID, err := Inst().Backup.GetClusterUID(nonAdminCtx, BackupOrgID, clusterName)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", clusterName))
					userClusterMap[user][clusterName] = userClusterUID
				}
			})
		}
		createObjectsFromUser := func(user string) {
			defer GinkgoRecover()
			Step(fmt.Sprintf("Take backup of applications from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Taking backup of applications from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				backupName := fmt.Sprintf("%s-manual-single-ns-%s-with-rules-%s", BackupNamePrefix, user, RandomString(4))
				backupNameMap[user] = backupName
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
				err = CreateBackupWithValidation(nonAdminCtx, backupNameMap[user], SourceClusterName, backupLocationNameMap[infraAdminUser], backupLocationUIDMap[infraAdminUser], appContextsToBackup, labelSelectors, BackupOrgID, userClusterMap[user][SourceClusterName], preRuleNameMap[infraAdminUser], preRuleUidMap[infraAdminUser], postRuleNameMap[infraAdminUser], postRuleUidMap[infraAdminUser])
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of backup [%s] of namespace (scheduled Context) [%s]", backupName, bkpNamespaces))
				userBackupNamesMap[user] = SafeAppend(&mutex, userBackupNamesMap[user], backupNameMap[user]).([]string)
			})

			Step(fmt.Sprintf("Take schedule backup of applications from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Taking schedule backup of applications from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				userScheduleName := fmt.Sprintf("backup-schedule-%v", RandomString(5))
				scheduleNameMap[user] = userScheduleName
				scheduleBackupName, err := CreateScheduleBackupWithValidation(nonAdminCtx, userScheduleName, SourceClusterName, backupLocationNameMap[infraAdminUser], backupLocationUIDMap[infraAdminUser], scheduledAppContexts, make(map[string]string), BackupOrgID, preRuleNameMap[infraAdminUser], preRuleUidMap[infraAdminUser], postRuleNameMap[infraAdminUser], postRuleUidMap[infraAdminUser], periodicSchedulePolicyNameMap[infraAdminUser], periodicSchedulePolicyUidMap[infraAdminUser])
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of schedule backup with schedule name [%s]", userScheduleName))
				userBackupNamesMap[user] = SafeAppend(&mutex, userBackupNamesMap[user], scheduleBackupName).([]string)
				err = SuspendBackupSchedule(scheduleNameMap[user], periodicSchedulePolicyNameMap[infraAdminUser], BackupOrgID, nonAdminCtx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule [%s] for user [%s]", scheduleNameMap[user], user))
			})
		}
		err := TaskHandler(userNames, createObjectsFromUser, Parallel)
		log.FailOnError(err, "failed to create objects from user")

		cleanupUserObjectsFromUser := func(user string) {
			defer GinkgoRecover()
			Step(fmt.Sprintf("Delete user %s backups from the user context", user), func() {
				log.InfoD(fmt.Sprintf("Deleting user %s backups from the user context", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				for _, backupName := range userBackupNamesMap[user] {
					backupUid, err := Inst().Backup.GetBackupUID(nonAdminCtx, backupName, BackupOrgID)
					log.FailOnError(err, "failed to fetch backup %s uid of the user %s", backupName, user)
					_, err = DeleteBackup(backupName, backupUid, BackupOrgID, nonAdminCtx)
					log.FailOnError(err, "failed to delete backup %s of the user %s", backupName, user)
					err = DeleteBackupAndWait(backupName, nonAdminCtx)
					log.FailOnError(err, fmt.Sprintf("waiting for backup [%s] deletion", backupName))
				}
			})
			Step(fmt.Sprintf("Delete user %s backup schedule ", user), func() {
				log.InfoD(fmt.Sprintf("Delete user %s backup schedule ", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				err = DeleteSchedule(scheduleNameMap[user], SourceClusterName, BackupOrgID, nonAdminCtx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Backup Schedule [%s] for user [%s]", scheduleNameMap[user], user))
			})
			Step(fmt.Sprintf("Delete user %s source and destination cluster from the user context", user), func() {
				log.InfoD(fmt.Sprintf("Deleting user %s source and destination cluster from the user context", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
					err := DeleteCluster(clusterName, BackupOrgID, nonAdminCtx, false)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of cluster [%s] of the user %s", clusterName, user))
					err = Inst().Backup.WaitForClusterDeletion(nonAdminCtx, clusterName, BackupOrgID, ClusterDeleteTimeout, ClusterCreationRetryTime)
					log.FailOnError(err, fmt.Sprintf("waiting for cluster [%s] deletion", clusterName))
				}
			})
		}
		err = TaskHandler(userNames, cleanupUserObjectsFromUser, Parallel)
		log.FailOnError(err, "failed to cleanup user objects from admin")

		Step(fmt.Sprintf("Verify Infra-Admin User has permission to create new custom role"), func() {
			log.InfoD("Verify Infra-Admin User has permission to create new custom role")
			nonAdminCtx, err := backup.GetNonAdminCtx(infraAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", infraAdminUser)
			customInfraRoleName = backup.PxBackupRole(fmt.Sprintf("custom-infra-admin-role-%s", RandomString(4)))
			services := []RoleServices{BackupSchedulePolicy, Rules, Cloudcredential, BackupLocation}
			apis := []RoleApis{All}
			err = CreateRole(customInfraRoleName, services, apis, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of role [%s] by the infra-admin user", customInfraRoleName))
		})
		Step(fmt.Sprintf("Create a new user with custom role [%s]", customInfraRoleName), func() {
			log.InfoD(fmt.Sprintf("Create a new user with custom role [%s]", customInfraRoleName))
			customUser = CreateUsers(1)[0]
			err := backup.AddRoleToUser(customUser, customInfraRoleName, fmt.Sprintf("Adding %v role to %s", customInfraRoleName, customUser))
			log.FailOnError(err, "failed to add role %s to the user %s", infraAdminRole, infraAdminUser)
			log.Infof("username %s common password %s", infraAdminUser, CommonPassword)
		})

		Step("Verify custom user has permission to create cloud credential and backup location", func() {
			log.InfoD("Verify custom user has permission to create cloud credential and backup location")
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			for _, provider := range providers {
				cloudCredentialNameMap[customUser] = fmt.Sprintf("autogenerated-cred-%v", RandomString(5))
				cloudCredentialUIDMap[customUser] = uuid.New()
				err = CreateCloudCredential(provider, cloudCredentialNameMap[customUser], cloudCredentialUIDMap[customUser], BackupOrgID, nonAdminCtx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential %s using provider %s for the user", cloudCredentialNameMap[customUser], provider))
				backupLocationNameMap[customUser] = fmt.Sprintf("autogenerated-backup-location-%v", RandomString(5))
				backupLocationUIDMap[customUser] = uuid.New()
				err = CreateBackupLocationWithContext(provider, backupLocationNameMap[customUser], backupLocationUIDMap[customUser], cloudCredentialNameMap[customUser], cloudCredentialUIDMap[customUser], getGlobalBucketName(provider), BackupOrgID, "", nonAdminCtx, true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup location %s using provider %s for the custom user [%s]", backupLocationNameMap[customUser], provider, customUser))
			}
		})

		Step("Adding labels to namespaces for custom user", func() {
			log.InfoD("Adding labels to namespaces for custom user")
			nsLabelsMapCustomUser = GenerateRandomLabels(2)
			err = AddLabelsToMultipleNamespaces(nsLabelsMapCustomUser, bkpNamespaces)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Adding labels [%v] to namespaces [%v] for custom user validation ", nsLabelsMapCustomUser, bkpNamespaces))
		})

		Step("Generating namespace label string from label map for namespaces", func() {
			log.InfoD("Generating namespace label string from label map for namespaces")
			nsLabelStringCustomUser = MapToKeyValueString(nsLabelsMapCustomUser)
			log.Infof("label string for namespaces %s", nsLabelStringCustomUser)
		})

		Step(fmt.Sprintf("Verify custom user has permission to create a schedule policy"), func() {
			log.InfoD("Verify custom user has permission to create a schedule policy")
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			periodicSchedulePolicyNameMap[customUser] = fmt.Sprintf("%s-%v", "periodic", RandomString(5))
			periodicSchedulePolicyUidMap[customUser] = uuid.New()
			periodicSchedulePolicyInterval := int64(15)
			err = CreateBackupScheduleIntervalPolicy(5, periodicSchedulePolicyInterval, 5, periodicSchedulePolicyNameMap[customUser], periodicSchedulePolicyUidMap[customUser], BackupOrgID, nonAdminCtx, false, false)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval [%v] minutes named [%s]", periodicSchedulePolicyInterval, periodicSchedulePolicyNameMap[customUser]))
		})

		Step(fmt.Sprintf("Verify custom user has permission to create pre and post exec rules for applications"), func() {
			log.InfoD("Verify custom user has permission to create pre and post exec rules for applications")
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			preRuleNameMap[customUser], postRuleNameMap[customUser], err = CreateRuleForBackupWithMultipleApplications(BackupOrgID, Inst().AppList, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of pre and post exec rules for applications from custom user [%s]", customUser))
		})

		Step(fmt.Sprintf("Verify custom user doesn't have permission to create roles"), func() {
			log.InfoD("Verify custom user doesn't have permission to create roles")
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			customRoleName = backup.PxBackupRole(fmt.Sprintf("custom-user-role-%s", RandomString(4)))
			services := []RoleServices{BackupSchedulePolicy, Rules, Cloudcredential, BackupLocation, Role}
			apis := []RoleApis{All}
			err = CreateRole(customRoleName, services, apis, nonAdminCtx)
			dash.VerifyFatal(strings.Contains(err.Error(), "PermissionDenied desc = Access denied for [Resource: role]"), true, fmt.Sprintf("Verifying custom user doesnt have permission for creating role [%s]", customRoleName))
		})
		Step(fmt.Sprintf("Create source and destination cluster from the custom user %s", customUser), func() {
			log.InfoD(fmt.Sprintf("Creating source and destination cluster from the custom user %s", customUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			err = CreateApplicationClusters(BackupOrgID, "", "", nonAdminCtx)
			log.FailOnError(err, "failed create source and destination cluster from the user %s", customUser)
			clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, nonAdminCtx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			userClusterMap[customUser] = make(map[string]string)
			for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
				userClusterUID, err := Inst().Backup.GetClusterUID(nonAdminCtx, BackupOrgID, clusterName)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", clusterName))
				userClusterMap[customUser][clusterName] = userClusterUID
			}
		})
		Step(fmt.Sprintf("Take backup of applications from the custom user %s", customUser), func() {
			log.InfoD(fmt.Sprintf("Taking backup of applications from the custom user %s", customUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			backupNameMap[customUser] = fmt.Sprintf("%s-manual-single-ns-%s-with-rules-%s", BackupNamePrefix, customUser, RandomString(4))
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateBackupWithValidation(nonAdminCtx, backupNameMap[customUser], SourceClusterName, backupLocationNameMap[customUser], backupLocationUIDMap[customUser], appContextsToBackup, labelSelectors, BackupOrgID, userClusterMap[customUser][SourceClusterName], "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of backup [%s] of namespace (scheduled Context) [%s]", backupNameMap[customUser], bkpNamespaces))
		})

		Step(fmt.Sprintf("Take restore of applications from the custom user %s", customUser), func() {
			log.InfoD(fmt.Sprintf("Taking restore of applications from the custom user %s", customUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			restoreNameMap[customUser] = fmt.Sprintf("%s-%s", RestoreNamePrefix, backupNameMap[customUser])
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateRestoreWithValidation(nonAdminCtx, restoreNameMap[customUser], backupNameMap[customUser], make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsToBackup)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s of backup %s", restoreNameMap[customUser], backupNameMap[customUser]))
		})

		Step("Validate taking manual backup of applications with namespace label for custom user", func() {
			log.InfoD("Validate taking manual backup of applications with namespace  for custom user")
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			manualBackupWithLabel = fmt.Sprintf("%s-%v", "customuser", RandomString(4))
			appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateBackupWithNamespaceLabelWithValidation(nonAdminCtx, manualBackupWithLabel, SourceClusterName, backupLocationNameMap[customUser], backupLocationUIDMap[customUser], appContextsExpectedInBackup,
				nil, BackupOrgID, userClusterMap[customUser][SourceClusterName], "", "", "", "", nsLabelStringCustomUser)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup [%s] creation with labels [%s]", manualBackupWithLabel, nsLabelStringCustomUser))
			customUserLabelledBackupNames = append(customUserLabelledBackupNames, manualBackupWithLabel)
			err = NamespaceLabelBackupSuccessCheck(manualBackupWithLabel, nonAdminCtx, bkpNamespaces, nsLabelStringCustomUser)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespaces [%v] are backed up, and check if labels [%s] are applied to backups [%s]", bkpNamespaces, nsLabelStringCustomUser, manualBackupWithLabel))
		})

		Step("Validate restoring manual backup of applications with namespace label", func() {
			log.InfoD("Validate restoring manual backup of applications with namespace label")
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			restoreForManualBackupWithLabel = fmt.Sprintf("%s-%s", RestoreNamePrefix, manualBackupWithLabel)
			appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateRestoreWithValidation(nonAdminCtx, restoreForManualBackupWithLabel, manualBackupWithLabel, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsExpectedInBackup)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration of backup %s", manualBackupWithLabel))
			customUserLabelledRestoreNames = append(customUserLabelledRestoreNames, restoreForManualBackupWithLabel)
		})

		Step("Validate creating scheduled backup with namespace label for custom user", func() {
			log.InfoD("Validate creating scheduled backup with namespace label for custom user")
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			backupScheduleWithLabel = fmt.Sprintf("%s-%v", BackupNamePrefix, RandomString(4))
			appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			scheduledBackupNameWithLabel, err = CreateScheduleBackupWithNamespaceLabelWithValidation(nonAdminCtx, backupScheduleWithLabel, SourceClusterName, backupLocationNameMap[customUser], backupLocationUIDMap[customUser], appContextsExpectedInBackup,
				nil, BackupOrgID, "", "", "", "", nsLabelStringCustomUser, periodicSchedulePolicyNameMap[customUser], periodicSchedulePolicyUidMap[customUser])
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of creating first schedule backup %s with labels [%v]", backupScheduleWithLabel, nsLabelStringCustomUser))
			err = SuspendBackupSchedule(backupScheduleWithLabel, periodicSchedulePolicyNameMap[customUser], BackupOrgID, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule [%s] for user [%s]", backupScheduleWithLabel, customUser))
			customUserLabelledBackupNames = append(customUserLabelledBackupNames, scheduledBackupNameWithLabel)
			err = NamespaceLabelBackupSuccessCheck(scheduledBackupNameWithLabel, nonAdminCtx, bkpNamespaces, nsLabelStringCustomUser)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespaces [%v] are backed up, and check if labels [%s] are applied to backups [%s]", bkpNamespaces, nsLabelStringCustomUser, scheduledBackupNameWithLabel))
		})

		Step("Validate restoring the scheduled backup with namespace label", func() {
			log.InfoD("Validate restoring the scheduled backup with namespace label")
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			multipleBackupNamespace, err := FetchNamespacesFromBackup(nonAdminCtx, scheduledBackupNameWithLabel, BackupOrgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching namespaces %v from backup %v", multipleBackupNamespace, scheduledBackupNameWithLabel))
			multipleRestoreMapping = make(map[string]string)
			for _, namespace := range multipleBackupNamespace {
				restoredNameSpace := fmt.Sprintf("%s-%v", scheduledBackupNameWithLabel, RandomString(3))
				multipleRestoreMapping[namespace] = restoredNameSpace
			}
			customRestoreName = fmt.Sprintf("%s-%v", "customrestore", RandomString(4))
			appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateRestoreWithValidation(nonAdminCtx, customRestoreName, scheduledBackupNameWithLabel, multipleRestoreMapping, make(map[string]string), DestinationClusterName, BackupOrgID, appContextsExpectedInBackup)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying multiple backup restore [%s] in custom namespace [%v]", customRestoreName, multipleRestoreMapping))
			customUserLabelledRestoreNames = append(customUserLabelledRestoreNames, customRestoreName)
		})

		Step(fmt.Sprintf("Validate deleting namespace labelled backups and restores from the custom user %s user context", customUser), func() {
			log.InfoD(fmt.Sprintf("Validate deleting namespace labelled backups and restores from the custom user %s user context", customUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			var wg sync.WaitGroup
			for _, backupName := range customUserLabelledBackupNames {
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					backupUid, err := Inst().Backup.GetBackupUID(nonAdminCtx, backupName, BackupOrgID)
					log.FailOnError(err, "Failed to fetch the backup %s uid of the user %s", backupName, customUser)
					_, err = DeleteBackup(backupName, backupUid, BackupOrgID, nonAdminCtx)
					log.FailOnError(err, "Failed to delete the backup %s of the user %s", backupName, customUser)
					err = DeleteBackupAndWait(backupName, nonAdminCtx)
					log.FailOnError(err, fmt.Sprintf("waiting for backup [%s] deletion", backupName))
				}(backupName)
			}
			wg.Wait()
			for _, restoreName := range customUserLabelledRestoreNames {
				err := DeleteRestore(restoreName, BackupOrgID, nonAdminCtx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the deletion of the restore named [%s]", restoreName))
			}
		})

		Step(fmt.Sprintf("Delete custom user %s backup schedule ", customUser), func() {
			log.InfoD(fmt.Sprintf("Delete custom user %s backup schedule ", customUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			err = DeleteSchedule(backupScheduleWithLabel, SourceClusterName, BackupOrgID, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Backup Schedule [%s] for user [%s]", backupScheduleWithLabel, customUser))
		})

		Step(fmt.Sprintf("Delete user %s backups from the user context", customUser), func() {
			log.InfoD(fmt.Sprintf("Deleting user %s backups from the user context", customUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			backupUid, err := Inst().Backup.GetBackupUID(nonAdminCtx, backupNameMap[customUser], BackupOrgID)
			log.FailOnError(err, "failed to fetch backup %s uid of the user %s", backupNameMap[customUser], customUser)
			_, err = DeleteBackup(backupNameMap[customUser], backupUid, BackupOrgID, nonAdminCtx)
			log.FailOnError(err, "failed to delete backup %s of the user %s", backupNameMap[customUser], customUser)
			err = DeleteBackupAndWait(backupNameMap[customUser], nonAdminCtx)
			log.FailOnError(err, fmt.Sprintf("waiting for backup [%s] deletion", backupNameMap[customUser]))
		})

		Step(fmt.Sprintf("Delete user %s restores from the user context", customUser), func() {
			log.InfoD(fmt.Sprintf("Deleting user %s restores from the user context", customUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			err = DeleteRestore(restoreNameMap[customUser], BackupOrgID, nonAdminCtx)
			log.FailOnError(err, "failed to delete restore %s of the user %s", restoreNameMap[customUser], customUser)
		})
		Step(fmt.Sprintf("Delete user %s source and destination cluster from the user context", customUser), func() {
			log.InfoD(fmt.Sprintf("Deleting user %s source and destination cluster from the user context", customUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
				err := DeleteCluster(clusterName, BackupOrgID, nonAdminCtx, false)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of cluster [%s] of the user %s", clusterName, customUser))
				err = Inst().Backup.WaitForClusterDeletion(nonAdminCtx, clusterName, BackupOrgID, ClusterDeleteTimeout, ClusterDeleteRetryTime)
				log.FailOnError(err, fmt.Sprintf("waiting for cluster [%s] deletion", clusterName))
			}
		})

		Step(fmt.Sprintf("Verify px-admin group user can delete RBAC resources created by customUser [%s]", customUser), func() {
			log.InfoD(fmt.Sprintf("Verify px-admin group user can delete RBAC resources created by customUser [%s]", customUser))
			adminCtx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch admin ctx")
			nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			log.Infof("Verify deletion of backup location [%s] of user [%s] from px-admin", backupLocationNameMap[customUser], customUser)
			err = DeleteBackupLocation(backupLocationNameMap[customUser], backupLocationUIDMap[customUser], BackupOrgID, true)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of backup location [%s] of the user %s", backupLocationNameMap[customUser], customUser))
			err = Inst().Backup.WaitForBackupLocationDeletion(adminCtx, backupLocationNameMap[customUser], backupLocationUIDMap[customUser], BackupOrgID, BackupLocationDeleteTimeout, BackupLocationDeleteRetryTime)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying waiting for backup location [%s]  deletion of the user %s", backupLocationNameMap[customUser], customUser))
			log.Infof("Verify deletion of schedule policy [%s] of user [%s] from px-admin", periodicSchedulePolicyNameMap[customUser], customUser)
			err = Inst().Backup.DeleteBackupSchedulePolicy(BackupOrgID, []string{periodicSchedulePolicyNameMap[customUser]})
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of schedule policy [%s] of the user %s", periodicSchedulePolicyNameMap[customUser], customUser))
			log.Infof("Verify deletion of rules of user [%s] from px-admin", customUser)
			customUserRules, _ := Inst().Backup.GetAllRules(nonAdminCtx, BackupOrgID)
			for _, ruleName := range customUserRules {
				err := DeleteRule(ruleName, BackupOrgID, adminCtx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of rule [%s] of the user %s", ruleName, customUser))
			}
			for _, provider := range providers {
				if provider != drivers.ProviderNfs {
					log.Infof("Verify deletion of cloud credential [%s] of user [%s] from px-admin", cloudCredentialNameMap[customUser], customUser)
					err = DeleteCloudCredential(cloudCredentialNameMap[customUser], BackupOrgID, cloudCredentialUIDMap[customUser])
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of cloud credential [%s] of the user %s", cloudCredentialNameMap[customUser], customUser))
				}
			}
		})

		Step(fmt.Sprintf("Verify infra-admin user [%s] can delete RBAC resources created ", infraAdminUser), func() {
			log.InfoD(fmt.Sprintf("Verify infra-admin user [%s] can delete RBAC resources created ", infraAdminUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(infraAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", infraAdminUser)
			log.Infof("Verify deletion of schedule policy [%s] of user [%s] ", periodicSchedulePolicyNameMap[infraAdminUser], infraAdminUser)
			err = DeleteBackupSchedulePolicyWithContext(BackupOrgID, []string{periodicSchedulePolicyNameMap[infraAdminUser]}, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of schedule policy [%s] of the user %s", periodicSchedulePolicyNameMap[infraAdminUser], infraAdminUser))
			log.Infof("Verify deletion of rules of user [%s] ", infraAdminUser)
			userRules, _ := Inst().Backup.GetAllRules(nonAdminCtx, BackupOrgID)
			for _, ruleName := range userRules {
				err := DeleteRule(ruleName, BackupOrgID, nonAdminCtx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of rule [%s] of the user %s", ruleName, infraAdminUser))
			}
			log.Infof("Verify deletion of role [%s] created by user [%s] ", customInfraRoleName, infraAdminUser)
			err = DeleteRole(customInfraRoleName, BackupOrgID, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of role [%s] created by user  %s", customInfraRoleName, infraAdminUser))
		})

	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		backupDriver := Inst().Backup
		bkpScheduleEnumerateReq := &api.BackupScheduleEnumerateRequest{
			OrgId: BackupOrgID,
		}
		nonAdminCtx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
		log.FailOnError(err, "failed to fetch user %s ctx", customUser)
		currentSchedulesForCustomUser, err := backupDriver.EnumerateBackupSchedule(nonAdminCtx, bkpScheduleEnumerateReq)
		log.FailOnError(err, "Getting a list of all schedules for Custom user")
		for _, sch := range currentSchedulesForCustomUser.GetBackupSchedules() {
			err = DeleteSchedule(sch.Name, sch.Cluster, BackupOrgID, nonAdminCtx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting Backup Schedule [%s] for user [%s]", sch.Name, customUser))
		}
		nonAdminCtx, err = backup.GetNonAdminCtx(infraAdminUser, CommonPassword)
		log.FailOnError(err, "failed to fetch user %s ctx", infraAdminUser)
		currentSchedulesForInfraAdmin, err := backupDriver.EnumerateBackupSchedule(nonAdminCtx, bkpScheduleEnumerateReq)
		log.FailOnError(err, "Getting a list of all schedules for Infra admin")
		for _, sch := range currentSchedulesForInfraAdmin.GetBackupSchedules() {
			err = DeleteSchedule(sch.Name, sch.Cluster, BackupOrgID, nonAdminCtx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting Backup Schedule [%s] for user [%s]", sch.Name, infraAdminUser))
		}
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		log.InfoD("Deleting labels from namespaces - %v", bkpNamespaces)
		err = DeleteLabelsFromMultipleNamespaces(nsLabelsMap, bkpNamespaces)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting labels [%v] from namespaces [%v]", nsLabelsMap, bkpNamespaces))
		log.InfoD("Deleting the px-backup objects")
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredentialNameMap[infraAdminUser], cloudCredentialUIDMap[infraAdminUser], nonAdminCtx)
		log.InfoD("Switching context to destination cluster for clean up")
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "Unable to switch context to destination cluster [%s]", DestinationClusterName)
		DestroyApps(scheduledAppContexts, opts)
		log.InfoD("Switching back context to Source cluster")
		err = SetSourceKubeConfig()
		log.FailOnError(err, "Unable to switch context to source cluster [%s]", SourceClusterName)
	})
})

// VerifyRBACForPxAdmin Validates the RBAC operation for px-admin group user.
var _ = Describe("{VerifyRBACForPxAdmin}", Label(TestCaseLabelsMap[VerifyRBACForPxAdmin]...), func() {
	var (
		scheduledAppContexts       []*scheduler.Context
		adminBackupLocationName    string
		adminBackupLocationUID     string
		adminCloudCredentialName   string
		adminCloudCredentialUID    string
		scheduleNameMap            = make(map[string]string)
		periodicSchedulePolicyName string
		periodicSchedulePolicyUid  string
		preRuleName                string
		postRuleName               string
		preRuleUid                 string
		postRuleUid                string
		adminBackupName            string
		adminRestoreName           string
		srcClusterUid              string
		numOfUsers                 = 3
		customRoleName             backup.PxBackupRole
		userClusterMap             = make(map[string]map[string]string)
		backupNameMap              = make(map[string]string)
		userBackupNamesMap         = make(map[string][]string)
		backupLocationMap          = make(map[string]string)
		userNames                  = make([]string, 0)
		providers                  = GetBackupProviders()
		bkpNamespaces              []string
		labelSelectors             map[string]string
		mutex                      sync.Mutex
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("VerifyRBACForPxAdmin", "Validates the RBAC operation for px-admin group user.", nil, 87887, Ak, Q3FY24)
		log.InfoD("scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, appCtx := range appContexts {
				appCtx.ReadinessTimeout = AppReadinessTimeout
				scheduledAppContexts = append(scheduledAppContexts, appCtx)
				namespace := GetAppNamespace(appCtx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})

	It("Validates the RBAC operation for px-admin group user.", func() {
		Step("Validate applications", func() {
			ValidateApplications(scheduledAppContexts)
		})

		Step("Verify Px-Admin group user has permission to create cloud credential  and backup location", func() {
			log.InfoD("Verify Px-Admin group user has permission to create cloud credential and backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch px-admin ctx")
			for _, provider := range providers {
				adminCloudCredentialName = fmt.Sprintf("autogenerated-cred-%v", RandomString(5))
				adminCloudCredentialUID = uuid.New()
				err = CreateCloudCredential(provider, adminCloudCredentialName, adminCloudCredentialUID, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of create cloud credential %s using provider %s for the px-admin", adminCloudCredentialName, provider))
				adminBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", RandomString(5))
				adminBackupLocationUID = uuid.New()
				err = CreateBackupLocationWithContext(provider, adminBackupLocationName, adminBackupLocationUID, adminCloudCredentialName, adminCloudCredentialUID, getGlobalBucketName(provider), BackupOrgID, "", ctx, true)
				log.FailOnError(err, "failed to create backup location %s using provider %s for the user", adminBackupLocationName, provider)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of create backup location %s using provider %s for the px-admin", adminBackupLocationName, provider))
				backupLocationMap[adminBackupLocationUID] = adminBackupLocationName
			}
		})

		Step(fmt.Sprintf("Verify Px-Admin User has permission to create a schedule policy"), func() {
			log.InfoD("Verify Px-Admin User has permission to create a schedule policy")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch px-admin ctx")
			periodicSchedulePolicyName = fmt.Sprintf("%s-%v", "periodic", RandomString(5))
			periodicSchedulePolicyUid = uuid.New()
			periodicSchedulePolicyInterval := int64(15)
			err = CreateBackupScheduleIntervalPolicy(5, periodicSchedulePolicyInterval, 5, periodicSchedulePolicyName, periodicSchedulePolicyUid, BackupOrgID, ctx, false, false)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval [%v] minutes named [%s]", periodicSchedulePolicyInterval, periodicSchedulePolicyName))
		})

		Step(fmt.Sprintf("Verify Px-Admin User has permission to create pre and post exec rules for applications"), func() {
			log.InfoD("Verify Px-Admin User has permission to create pre and post exec rules for applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch px-admin ctx")
			preRuleName, postRuleName, err = CreateRuleForBackupWithMultipleApplications(BackupOrgID, Inst().AppList, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of pre and post exec rules for applications from px-admin"))
			if preRuleName != "" {
				preRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleName)
				log.FailOnError(err, "Fetching pre backup rule [%s] uid", preRuleName)
				log.Infof("Pre backup rule [%s] uid: [%s]", preRuleName, preRuleUid)
			}
			if postRuleName != "" {
				postRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleName)
				log.FailOnError(err, "Fetching post backup rule [%s] uid", postRuleName)
				log.Infof("Post backup rule [%s] uid: [%s]", postRuleName, postRuleUid)
			}
		})

		Step(fmt.Sprintf("Verify Px-Admin User has permission to create new custom role"), func() {
			log.InfoD("Verify Px-Admin User has permission to create new custom role")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch px-admin ctx")
			customRoleName = backup.PxBackupRole(fmt.Sprintf("custom-px-admin-role-%s", RandomString(4)))
			services := []RoleServices{BackupSchedulePolicy, Rules, Cloudcredential, BackupLocation}
			apis := []RoleApis{All}
			err = CreateRole(customRoleName, services, apis, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of role [%s] by the px-admin", customRoleName))
		})

		Step("Create Users with Different types of roles", func() {
			log.InfoD("Create Users with Different types of roles")
			roles := [3]backup.PxBackupRole{backup.ApplicationOwner, backup.InfrastructureOwner, backup.ApplicationUser}
			for i := 1; i <= numOfUsers/3; i++ {
				for _, role := range roles {
					userName := CreateUsers(1)[0]
					err := backup.AddRoleToUser(userName, role, fmt.Sprintf("Adding %v role to %s", role, userName))
					log.FailOnError(err, "Failed to add role for user - %s", userName)
					userNames = append(userNames, userName)
					log.FailOnError(err, "Failed to fetch uid for - %s", userName)
				}
			}
		})

		Step("Verify px-admin User has permission to share RBAC resources with non-admin users", func() {
			log.InfoD("Verify px-admin User has permission to share RBAC resources with non-admin users")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch px-admin ctx")
			for _, provider := range providers {
				if provider != drivers.ProviderNfs {
					log.Infof("Update CloudAccount - %s ownership for users - [%v]", adminCloudCredentialName, userNames)
					err = AddCloudCredentialOwnership(adminCloudCredentialName, adminCloudCredentialUID, userNames, nil, Read, Invalid, ctx, BackupOrgID)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of owbership for CloudCredential- %s", adminCloudCredentialName))
				}
			}
			log.InfoD("Update BackupLocation - %s ownership for users - [%v]", adminBackupLocationName, userNames)
			err = AddBackupLocationOwnership(adminBackupLocationName, adminBackupLocationUID, userNames, nil, Read, Invalid, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for backuplocation - %s", adminBackupLocationName))
			log.InfoD("Update SchedulePolicy - %s ownership for users - [%v]", periodicSchedulePolicyName, userNames)
			err = AddSchedulePolicyOwnership(periodicSchedulePolicyName, periodicSchedulePolicyUid, userNames, nil, Read, Invalid, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for schedulepolicy - %s", periodicSchedulePolicyName))
			log.InfoD("Update Application Rules ownership for users - [%v]", userNames)
			if preRuleName != "" {
				err = AddRuleOwnership(preRuleName, preRuleUid, userNames, nil, Read, Invalid, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for pre-rule of application"))
			}
			if postRuleName != "" {
				err = AddRuleOwnership(postRuleName, postRuleUid, userNames, nil, Read, Invalid, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for post-rule of application"))
			}
		})

		Step(fmt.Sprintf("Create source and destination cluster from the px-admin"), func() {
			log.InfoD(fmt.Sprintf("Create source and destination cluster from the px-admin"))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch px-admin ctx")
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			log.FailOnError(err, "failed create source and destination cluster from px-admin")
			clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			srcClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})
		Step(fmt.Sprintf("Take backup of applications from the px-admin"), func() {
			log.InfoD(fmt.Sprintf("Taking backup of applications from the px-admin"))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch px-admin ctx")
			adminBackupName = fmt.Sprintf("%s-manual-single-ns-with-rules-%s", BackupNamePrefix, RandomString(4))
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateBackupWithValidation(ctx, adminBackupName, SourceClusterName, adminBackupLocationName, adminBackupLocationUID, appContextsToBackup, labelSelectors, BackupOrgID, srcClusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of backup [%s] of namespace (scheduled Context) [%s]", adminBackupName, bkpNamespaces))
		})

		Step(fmt.Sprintf("Take restore of applications from the px-admin"), func() {
			log.InfoD(fmt.Sprintf("Taking restore of applications from the px-admin"))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch px-admin ctx")
			adminRestoreName = fmt.Sprintf("%s-%s", RestoreNamePrefix, adminBackupName)
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateRestoreWithValidation(ctx, adminRestoreName, adminBackupName, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsToBackup)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s of backup %s", adminRestoreName, adminBackupName))
		})

		Step(fmt.Sprintf("Delete px-admin backup from the px-admin context"), func() {
			log.InfoD(fmt.Sprintf("Deleting px-admin backup from the px-admin context"))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch px-admin ctx")
			backupUid, err := Inst().Backup.GetBackupUID(ctx, adminBackupName, BackupOrgID)
			log.FailOnError(err, "failed to fetch backup %s uid of the px-admin", adminBackupName)
			_, err = DeleteBackup(adminBackupName, backupUid, BackupOrgID, ctx)
			log.FailOnError(err, "failed to delete backup %s of the px-admin", adminBackupName)
			err = DeleteBackupAndWait(adminBackupName, ctx)
			log.FailOnError(err, fmt.Sprintf("waiting for backup [%s] deletion", adminBackupName))
		})

		Step(fmt.Sprintf("Delete px-admin restore from the px-admin context"), func() {
			log.InfoD(fmt.Sprintf("Deleting  px-admin restore from the px-admin context"))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch px-admin ctx")
			err = DeleteRestore(adminRestoreName, BackupOrgID, ctx)
			log.FailOnError(err, "failed to delete restore %s of the px-admin", adminRestoreName)
		})
		for _, user := range userNames {
			Step(fmt.Sprintf("Create source and destination cluster from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Creating source and destination cluster from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				err = CreateApplicationClusters(BackupOrgID, "", "", nonAdminCtx)
				log.FailOnError(err, "failed create source and destination cluster from the user %s", user)
				clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, nonAdminCtx)
				log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
				dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
				userClusterMap[user] = make(map[string]string)
				for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
					userClusterUID, err := Inst().Backup.GetClusterUID(nonAdminCtx, BackupOrgID, clusterName)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", clusterName))
					userClusterMap[user][clusterName] = userClusterUID
				}
			})
		}
		createObjectsFromUser := func(user string) {
			defer GinkgoRecover()
			Step(fmt.Sprintf("Take backup of applications from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Taking backup of applications from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				backupName := fmt.Sprintf("%s-manual-single-ns-%s-with-rules-%s", BackupNamePrefix, user, RandomString(4))
				backupNameMap[user] = backupName
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
				err = CreateBackupWithValidation(nonAdminCtx, backupNameMap[user], SourceClusterName, adminBackupLocationName, adminBackupLocationUID, appContextsToBackup, labelSelectors, BackupOrgID, userClusterMap[user][SourceClusterName], preRuleName, preRuleUid, postRuleName, postRuleUid)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of backup [%s] of namespace (scheduled Context) [%s]", backupName, bkpNamespaces))
				userBackupNamesMap[user] = SafeAppend(&mutex, userBackupNamesMap[user], backupNameMap[user]).([]string)
			})

			Step(fmt.Sprintf("Take schedule backup of applications from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Taking schedule backup of applications from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				userScheduleName := fmt.Sprintf("backup-schedule-%v", RandomString(5))
				scheduleNameMap[user] = userScheduleName
				scheduleBackupName, err := CreateScheduleBackupWithValidation(nonAdminCtx, userScheduleName, SourceClusterName, adminBackupLocationName, adminBackupLocationUID, scheduledAppContexts, make(map[string]string), BackupOrgID, preRuleName, preRuleUid, postRuleName, postRuleUid, periodicSchedulePolicyName, periodicSchedulePolicyUid)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of schedule backup with schedule name [%s]", userScheduleName))
				userBackupNamesMap[user] = SafeAppend(&mutex, userBackupNamesMap[user], scheduleBackupName).([]string)
				err = SuspendBackupSchedule(scheduleNameMap[user], periodicSchedulePolicyName, BackupOrgID, nonAdminCtx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule [%s] for user [%s]", scheduleNameMap[user], user))
			})
		}
		err := TaskHandler(userNames, createObjectsFromUser, Parallel)
		log.FailOnError(err, "failed to create objects from user")

		cleanupUserObjectsFromUser := func(user string) {
			defer GinkgoRecover()
			Step(fmt.Sprintf("Delete user %s backups from the user context", user), func() {
				log.InfoD(fmt.Sprintf("Deleting user %s backups from the user context", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				for _, backupName := range userBackupNamesMap[user] {
					backupUid, err := Inst().Backup.GetBackupUID(nonAdminCtx, backupName, BackupOrgID)
					log.FailOnError(err, "failed to fetch backup %s uid of the user %s", backupName, user)
					_, err = DeleteBackup(backupName, backupUid, BackupOrgID, nonAdminCtx)
					log.FailOnError(err, "failed to delete backup %s of the user %s", backupName, user)
					err = DeleteBackupAndWait(backupName, nonAdminCtx)
					log.FailOnError(err, fmt.Sprintf("waiting for backup [%s] deletion", backupName))
				}
			})
			Step(fmt.Sprintf("Delete user %s backup schedule ", user), func() {
				log.InfoD(fmt.Sprintf("Delete user %s backup schedule ", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				err = DeleteSchedule(scheduleNameMap[user], SourceClusterName, BackupOrgID, nonAdminCtx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Backup Schedule [%s] for user [%s]", scheduleNameMap[user], user))
			})
			Step(fmt.Sprintf("Delete user %s source and destination cluster from the user context", user), func() {
				log.InfoD(fmt.Sprintf("Deleting user %s source and destination cluster from the user context", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
					err := DeleteCluster(clusterName, BackupOrgID, nonAdminCtx, false)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of cluster [%s] of the user %s", clusterName, user))
					err = Inst().Backup.WaitForClusterDeletion(nonAdminCtx, clusterName, BackupOrgID, ClusterDeleteTimeout, ClusterDeleteRetryTime)
					log.FailOnError(err, fmt.Sprintf("waiting for cluster [%s] deletion", clusterName))
				}
			})
		}
		err = TaskHandler(userNames, cleanupUserObjectsFromUser, Parallel)
		log.FailOnError(err, "failed to cleanup user objects from user")

		Step(fmt.Sprintf("Verify px-admin group can delete RBAC resources created "), func() {
			log.InfoD(fmt.Sprintf("Verify px-admin group can delete RBAC resources created "))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch px-admin ctx")
			log.Infof("Verify deletion of schedule policy [%s] of px-admin", periodicSchedulePolicyName)
			err = Inst().Backup.DeleteBackupSchedulePolicy(BackupOrgID, []string{periodicSchedulePolicyName})
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of schedule policy [%s] of px-admin", periodicSchedulePolicyName))
			log.Infof("Verify deletion of rules of px-admin")
			pxadminRules, _ := Inst().Backup.GetAllRules(ctx, BackupOrgID)
			for _, ruleName := range pxadminRules {
				err := DeleteRule(ruleName, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of rule [%s] of the px-admin", ruleName))
			}
			log.Infof("Verify deletion of custom roles created by px-admin")
			err = DeleteRole(customRoleName, BackupOrgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of custom role [%v] from px-admin", customRoleName))
		})
	})
	JustAfterEach(func() {
		defer func() {
			err := SetSourceKubeConfig()
			log.FailOnError(err, "Unable to switch context to source cluster [%s]", SourceClusterName)
			EndPxBackupTorpedoTest(scheduledAppContexts)
		}()
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "failed to fetch px-admin ctx")
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		log.InfoD("Deleting the px-backup objects")
		CleanupCloudSettingsAndClusters(backupLocationMap, adminCloudCredentialName, adminCloudCredentialUID, ctx)
		log.InfoD("Switching context to destination cluster for clean up")
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "Unable to switch context to destination cluster [%s]", DestinationClusterName)
		DestroyApps(scheduledAppContexts, opts)
	})
})

// VerifyRBACForAppAdmin Validates the RBAC operation for app-admin user.
var _ = Describe("{VerifyRBACForAppAdmin}", Label(TestCaseLabelsMap[VerifyRBACForAppAdmin]...), func() {
	var (
		scheduledAppContexts            []*scheduler.Context
		scheduleNameMap                 = make(map[string]string)
		periodicSchedulePolicyNameMap   = make(map[string]string)
		periodicSchedulePolicyUidMap    = make(map[string]string)
		preRuleNameMap                  = make(map[string]string)
		postRuleNameMap                 = make(map[string]string)
		preRuleUidMap                   = make(map[string]string)
		postRuleUidMap                  = make(map[string]string)
		numOfUsers                      = 3
		appAdminUser                    string
		adminCredName                   string
		adminCloudCredUID               string
		adminBackupLocationName         string
		adminBackupLocationUID          string
		appAdminBackupLocationName      string
		appAdminBackupLocationUID       string
		customRoleName                  backup.PxBackupRole
		userClusterMap                  = make(map[string]map[string]string)
		backupNameMap                   = make(map[string]string)
		restoreNameMap                  = make(map[string]string)
		userBackupNamesMap              = make(map[string][]string)
		backupLocationMap               = make(map[string]string)
		backupLocationsFromAdmin        []string
		userNames                       = make([]string, 0)
		providers                       = GetBackupProviders()
		bkpNamespaces                   []string
		appAdminRole                    = backup.ApplicationOwner
		labelSelectors                  map[string]string
		mutex                           sync.Mutex
		nsLabelsMap                     map[string]string
		nsLabelString                   string
		manualBackupWithLabel           string
		restoreForManualBackupWithLabel string
		srcClusterUid                   string
		backupScheduleWithLabel         string
		scheduledBackupNameWithLabel    string
		multipleRestoreMapping          map[string]string
		customRestoreName               string
		labelledRestoreNames            []string
		labelledBackupNames             []string
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("VerifyRBACForAppAdmin", "Validates the RBAC operation for app-admin user.", nil, 87888, Ak, Q3FY24)
		log.InfoD("scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, appCtx := range appContexts {
				appCtx.ReadinessTimeout = AppReadinessTimeout
				scheduledAppContexts = append(scheduledAppContexts, appCtx)
				namespace := GetAppNamespace(appCtx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})

	It("Validates the RBAC operation for app-admin user", func() {
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Adding labels to namespaces", func() {
			log.InfoD("Adding labels to namespaces")
			nsLabelsMap = GenerateRandomLabels(2)
			err := AddLabelsToMultipleNamespaces(nsLabelsMap, bkpNamespaces)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Adding labels [%v] to namespaces [%v]", nsLabelsMap, bkpNamespaces))
		})

		Step("Generating namespace label string from label map for namespaces", func() {
			log.InfoD("Generating namespace label string from label map for namespaces")
			nsLabelString = MapToKeyValueString(nsLabelsMap)
			log.Infof("label string for namespaces %s", nsLabelString)
		})

		Step(fmt.Sprintf("Create a user with %s role", appAdminRole), func() {
			log.InfoD(fmt.Sprintf("Creating a user with %s role", appAdminRole))
			appAdminUser = CreateUsers(1)[0]
			err := backup.AddRoleToUser(appAdminUser, appAdminRole, fmt.Sprintf("Adding %v role to %s", appAdminRole, appAdminUser))
			dash.VerifyFatal(err, nil, fmt.Sprintf("failed to add role %s to the user %s", appAdminRole, appAdminUser))
		})

		Step("Verify App-Admin User doesnt have permission to create cloud credential", func() {
			log.InfoD("Verify App-Admin User doesnt have permission to create cloud credential")
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			for _, provider := range providers {
				if provider != drivers.ProviderNfs {
					cloudCredentialName := fmt.Sprintf("autogenerated-cred-%v", RandomString(5))
					cloudCredentialUID := uuid.New()
					err = CreateCloudCredential(provider, cloudCredentialName, cloudCredentialUID, BackupOrgID, nonAdminCtx)
					dash.VerifyFatal(strings.Contains(err.Error(), "PermissionDenied desc = Access denied for [Resource: cloudcredential]"), true, fmt.Sprintf("Verifying creation failure of cloud credential %s using provider %s for the user", cloudCredentialName, provider))
				}
			}
		})

		Step("Verify App-Admin User doesnt have permission to create role", func() {
			log.InfoD("Verify App-Admin User doesnt have permission to create role")
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			customRoleName = backup.PxBackupRole(fmt.Sprintf("custom-user-role-%s", RandomString(4)))
			services := []RoleServices{BackupSchedulePolicy, Rules, Cloudcredential, BackupLocation, Role}
			apis := []RoleApis{All}
			err = CreateRole(customRoleName, services, apis, nonAdminCtx)
			dash.VerifyFatal(strings.Contains(err.Error(), "PermissionDenied desc = Access denied for [Resource: role]"), true, fmt.Sprintf("Verifying app-user doesnt have permission for creating role [%s]", customRoleName))
		})

		Step(fmt.Sprintf("Adding Credentials and BackupLocation from px-admin user and making it public"), func() {
			log.InfoD(fmt.Sprintf("Adding Credentials and BackupLocation from px-admin user and making it public"))
			for _, provider := range providers {
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-admin ctx")
				adminCloudCredUID = uuid.New()
				adminCredName = fmt.Sprintf("autogenerated-cred-%v", RandomString(5))
				if provider != drivers.ProviderNfs {
					err = CreateCloudCredential(provider, adminCredName, adminCloudCredUID, BackupOrgID, ctx)
					log.FailOnError(err, "Failed to create cloud credential - %s", err)
					err = AddCloudCredentialOwnership(adminCredName, adminCloudCredUID, nil, nil, Invalid, Read, ctx, BackupOrgID)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying public ownership update for cloud credential %s ", adminCredName))
				}
				adminBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", RandomString(5))
				adminBackupLocationUID = uuid.New()
				err = CreateBackupLocationWithContext(provider, adminBackupLocationName, adminBackupLocationUID, adminCredName, adminCloudCredUID, getGlobalBucketName(provider), BackupOrgID, "", ctx, true)
				log.FailOnError(err, "Failed to add backup location %s using provider %s for px-admin user", adminBackupLocationName, provider)
				backupLocationMap[adminBackupLocationUID] = adminBackupLocationName
				err = AddBackupLocationOwnership(adminBackupLocationName, adminBackupLocationUID, nil, nil, Invalid, Read, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying public ownership update for backup location %s", adminBackupLocationName))
			}
		})

		Step(fmt.Sprintf("Verify App-Admin User can create backup location with cloud cred shared from px-admin"), func() {
			log.InfoD("Verify App-Admin User can create backup location with cloud cred shared from px-admin")
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			appAdminBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", RandomString(5))
			appAdminBackupLocationUID = uuid.New()
			for _, provider := range providers {
				err = CreateBackupLocationWithContext(provider, appAdminBackupLocationName, appAdminBackupLocationUID, adminCredName, adminCloudCredUID, getGlobalBucketName(provider), BackupOrgID, "", nonAdminCtx, true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Failed to add backup location %s using provider %s for px-admin user", appAdminBackupLocationName, provider))
			}
		})

		Step(fmt.Sprintf("Verify App-Admin User can't share backup location created with shared cloud cred from px-admin"), func() {
			log.InfoD("Verify App-Admin User can't share backup location created with shared cloud cred from px-admin")
			for _, provider := range providers {
				if provider != drivers.ProviderNfs {
					nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
					err = AddBackupLocationOwnership(appAdminBackupLocationName, appAdminBackupLocationUID, nil, nil, Invalid, Read, nonAdminCtx)
					dash.VerifyFatal(strings.Contains(err.Error(), "failed to create backup location: rpc error: code = Unknown desc = cannot share backup location name/UID"), true, fmt.Sprintf("Verifying ownership update failure for backup location %s ", appAdminBackupLocationName))
				}
			}
		})

		Step(fmt.Sprintf("Verify App-Admin User has permission to create a schedule policy"), func() {
			log.InfoD("Verify App-Admin User has permission to create a schedule policy")
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			periodicSchedulePolicyNameMap[appAdminUser] = fmt.Sprintf("%s-%v", "periodic", RandomString(5))
			periodicSchedulePolicyUidMap[appAdminUser] = uuid.New()
			periodicSchedulePolicyInterval := int64(15)
			err = CreateBackupScheduleIntervalPolicy(5, periodicSchedulePolicyInterval, 5, periodicSchedulePolicyNameMap[appAdminUser], periodicSchedulePolicyUidMap[appAdminUser], BackupOrgID, nonAdminCtx, false, false)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval [%v] minutes named [%s]", periodicSchedulePolicyInterval, periodicSchedulePolicyNameMap[appAdminUser]))
		})

		Step(fmt.Sprintf("Verify App-Admin User has permission to create pre and post exec rules for applications"), func() {
			log.InfoD("Verify App-Admin User has permission to create pre and post exec rules for applications")
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			preRuleNameMap[appAdminUser], postRuleNameMap[appAdminUser], err = CreateRuleForBackupWithMultipleApplications(BackupOrgID, Inst().AppList, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of pre and post exec rules for applications from px-admin"))
			if preRuleNameMap[appAdminUser] != "" {
				preRuleUidMap[appAdminUser], err = Inst().Backup.GetRuleUid(BackupOrgID, nonAdminCtx, preRuleNameMap[appAdminUser])
				log.FailOnError(err, "Fetching pre backup rule [%s] uid", preRuleNameMap[appAdminUser])
				log.Infof("Pre backup rule [%s] uid: [%s]", preRuleNameMap[appAdminUser], preRuleUidMap[appAdminUser])
			}
			if postRuleNameMap[appAdminUser] != "" {
				postRuleUidMap[appAdminUser], err = Inst().Backup.GetRuleUid(BackupOrgID, nonAdminCtx, postRuleNameMap[appAdminUser])
				log.FailOnError(err, "Fetching post backup rule [%s] uid", postRuleNameMap[appAdminUser])
				log.Infof("Post backup rule [%s] uid: [%s]", postRuleNameMap[appAdminUser], postRuleUidMap[appAdminUser])
			}
		})

		Step(fmt.Sprintf("Verify px-admin group user can list RBAC resources created by app-Admin User"), func() {
			log.InfoD("Verify px-admin group user can list RBAC resources created by app-Admin User")
			adminCtx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch admin ctx")
			backupLocations, err := GetAllBackupLocations(adminCtx)
			log.FailOnError(err, "Fetching backup location from px-admin")
			for _, backupLocationName := range backupLocations {
				backupLocationsFromAdmin = append(backupLocationsFromAdmin, backupLocationName)
			}
			if !IsPresent(backupLocationsFromAdmin, appAdminBackupLocationName) {
				err := fmt.Errorf("backup location [%s] is not listed in backup location names from admin %s", appAdminBackupLocationName, backupLocationsFromAdmin)
				log.FailOnError(fmt.Errorf(""), err.Error())
			}
			schedulePoliciesFromAdmin, err := Inst().Backup.GetAllSchedulePolicies(adminCtx, BackupOrgID)
			log.FailOnError(err, "Fetching backup schedules from px-admin")
			if !IsPresent(schedulePoliciesFromAdmin, periodicSchedulePolicyNameMap[appAdminUser]) {
				err := fmt.Errorf("schedule policy [%s] is not listed in schedule policies  from admin %s", periodicSchedulePolicyNameMap[appAdminUser], schedulePoliciesFromAdmin)
				log.FailOnError(fmt.Errorf(""), err.Error())
			}

			rulesFromAdmin, err := Inst().Backup.GetAllRules(adminCtx, BackupOrgID)
			log.FailOnError(err, "Fetching rules from px-admin")
			if preRuleNameMap[appAdminUser] != "" {
				if !IsPresent(rulesFromAdmin, preRuleNameMap[appAdminUser]) {
					err := fmt.Errorf("pre rule [%s] is not listed in rules from admin %s", preRuleNameMap[appAdminUser], rulesFromAdmin)
					log.FailOnError(fmt.Errorf(""), err.Error())
				}
			}
			if postRuleNameMap[appAdminUser] != "" {
				if !IsPresent(rulesFromAdmin, postRuleNameMap[appAdminUser]) {
					err := fmt.Errorf("post rule [%s] is not listed in rules from admin %s", postRuleNameMap[appAdminUser], rulesFromAdmin)
					log.FailOnError(fmt.Errorf(""), err.Error())
				}
			}
		})

		Step("Create Users with Different types of roles", func() {
			log.InfoD("Create Users with Different types of roles")
			roles := [3]backup.PxBackupRole{backup.ApplicationOwner, backup.InfrastructureOwner, backup.ApplicationUser}
			for i := 1; i <= numOfUsers/3; i++ {
				for _, role := range roles {
					userName := CreateUsers(1)[0]
					err := backup.AddRoleToUser(userName, role, fmt.Sprintf("Adding %v role to %s", role, userName))
					log.FailOnError(err, "Failed to add role for user - %s", userName)
					userNames = append(userNames, userName)
					log.FailOnError(err, "Failed to fetch uid for - %s", userName)
					log.Infof(fmt.Sprintf("Added role %v to user %s", role, userName))
				}
			}
		})

		Step("Verify App-Admin User has permission to share schedule-policy and rules with non-admin users", func() {
			log.InfoD("Verify App-Admin User has permission to share schedule-policy and rules with non-admin users")
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			log.InfoD("Update SchedulePolicy - %s ownership for users - [%v]", periodicSchedulePolicyNameMap[appAdminUser], userNames)
			err = AddSchedulePolicyOwnership(periodicSchedulePolicyNameMap[appAdminUser], periodicSchedulePolicyUidMap[appAdminUser], userNames, nil, Read, Invalid, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for schedulepolicy - %s", periodicSchedulePolicyNameMap[appAdminUser]))
			log.InfoD("Update Application Rules ownership for users - [%v]", userNames)
			if preRuleNameMap[appAdminUser] != "" {
				err = AddRuleOwnership(preRuleNameMap[appAdminUser], preRuleUidMap[appAdminUser], userNames, nil, Read, Invalid, nonAdminCtx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for pre-rule of application"))
			}
			if postRuleNameMap[appAdminUser] != "" {
				err = AddRuleOwnership(postRuleNameMap[appAdminUser], postRuleUidMap[appAdminUser], userNames, nil, Read, Invalid, nonAdminCtx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for post-rule of application"))
			}
		})
		for _, user := range userNames {
			Step(fmt.Sprintf("Create source and destination cluster from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Creating source and destination cluster from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				err = CreateApplicationClusters(BackupOrgID, "", "", nonAdminCtx)
				log.FailOnError(err, "failed create source and destination cluster from the user %s", user)
				clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, nonAdminCtx)
				log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
				dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
				userClusterMap[user] = make(map[string]string)
				for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
					userClusterUID, err := Inst().Backup.GetClusterUID(nonAdminCtx, BackupOrgID, clusterName)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", clusterName))
					userClusterMap[user][clusterName] = userClusterUID
				}
			})
		}
		createObjectsFromUser := func(user string) {
			Step(fmt.Sprintf("Take backup of applications from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Taking backup of applications from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				backupName := fmt.Sprintf("%s-manual-single-ns-%s-with-rules-%s", BackupNamePrefix, user, RandomString(4))
				backupNameMap[user] = backupName
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
				err = CreateBackupWithValidation(nonAdminCtx, backupNameMap[user], SourceClusterName, adminBackupLocationName, adminBackupLocationUID, appContextsToBackup, labelSelectors, BackupOrgID, userClusterMap[user][SourceClusterName], preRuleNameMap[appAdminUser], preRuleUidMap[appAdminUser], postRuleNameMap[appAdminUser], postRuleUidMap[appAdminUser])
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of backup [%s] of namespace (scheduled Context) [%s]", backupName, bkpNamespaces))
				userBackupNamesMap[user] = SafeAppend(&mutex, userBackupNamesMap[user], backupNameMap[user]).([]string)
			})

			Step(fmt.Sprintf("Take schedule backup of applications from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Taking schedule backup of applications from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				userScheduleName := fmt.Sprintf("backup-schedule-%v", RandomString(5))
				scheduleNameMap[user] = userScheduleName
				scheduleBackupName, err := CreateScheduleBackupWithValidation(nonAdminCtx, userScheduleName, SourceClusterName, adminBackupLocationName, adminBackupLocationUID, scheduledAppContexts, make(map[string]string), BackupOrgID, preRuleNameMap[appAdminUser], preRuleUidMap[appAdminUser], postRuleNameMap[appAdminUser], postRuleUidMap[appAdminUser], periodicSchedulePolicyNameMap[appAdminUser], periodicSchedulePolicyUidMap[appAdminUser])
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of schedule backup with schedule name [%s]", userScheduleName))
				userBackupNamesMap[user] = SafeAppend(&mutex, userBackupNamesMap[user], scheduleBackupName).([]string)
				err = SuspendBackupSchedule(scheduleNameMap[user], periodicSchedulePolicyNameMap[appAdminUser], BackupOrgID, nonAdminCtx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule [%s] for user [%s]", scheduleNameMap[user], user))
			})
		}
		err := TaskHandler(userNames, createObjectsFromUser, Parallel)
		log.FailOnError(err, "failed to create objects from user")

		cleanupUserObjectsFromUser := func(user string) {
			Step(fmt.Sprintf("Delete user %s backups from the user context", user), func() {
				log.InfoD(fmt.Sprintf("Deleting user %s backups from the user context", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				for _, backupName := range userBackupNamesMap[user] {
					backupUid, err := Inst().Backup.GetBackupUID(nonAdminCtx, backupName, BackupOrgID)
					log.FailOnError(err, "failed to fetch backup %s uid of the user %s", backupName, user)
					_, err = DeleteBackup(backupName, backupUid, BackupOrgID, nonAdminCtx)
					log.FailOnError(err, "failed to delete backup %s of the user %s", backupName, user)
					err = DeleteBackupAndWait(backupName, nonAdminCtx)
					log.FailOnError(err, fmt.Sprintf("waiting for backup [%s] deletion", backupName))
				}
			})
			Step(fmt.Sprintf("Delete user %s backup schedule ", user), func() {
				log.InfoD(fmt.Sprintf("Delete user %s backup schedule ", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				err = DeleteSchedule(scheduleNameMap[user], SourceClusterName, BackupOrgID, nonAdminCtx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Backup Schedule [%s] for user [%s]", scheduleNameMap[user], user))
			})
			Step(fmt.Sprintf("Delete user %s source and destination cluster from the user context", user), func() {
				log.InfoD(fmt.Sprintf("Deleting user %s source and destination cluster from the user context", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
					err := DeleteCluster(clusterName, BackupOrgID, nonAdminCtx, false)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of cluster [%s] of the user %s", clusterName, user))
					err = Inst().Backup.WaitForClusterDeletion(nonAdminCtx, clusterName, BackupOrgID, ClusterDeleteTimeout, ClusterDeleteRetryTime)
					log.FailOnError(err, fmt.Sprintf("waiting for cluster [%s] deletion", clusterName))
				}
			})
		}
		err = TaskHandler(userNames, cleanupUserObjectsFromUser, Parallel)
		log.FailOnError(err, "failed to cleanup user objects from user")

		Step(fmt.Sprintf("Create source and destination cluster from the app-admin user %s", appAdminUser), func() {
			log.InfoD(fmt.Sprintf("Creating source and destination cluster from the app-admin user %s", appAdminUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			err = CreateApplicationClusters(BackupOrgID, "", "", nonAdminCtx)
			log.FailOnError(err, "failed create source and destination cluster from the user %s", appAdminUser)
			clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, nonAdminCtx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			userClusterMap[appAdminUser] = make(map[string]string)
			for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
				userClusterUID, err := Inst().Backup.GetClusterUID(nonAdminCtx, BackupOrgID, clusterName)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", clusterName))
				userClusterMap[appAdminUser][clusterName] = userClusterUID
			}
		})
		Step(fmt.Sprintf("Take backup of applications from the App-admin user %s", appAdminUser), func() {
			log.InfoD(fmt.Sprintf("Taking backup of applications from the app-admin user %s", appAdminUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			userScheduleName := fmt.Sprintf("backup-schedule-%v", RandomString(5))
			scheduleNameMap[appAdminUser] = userScheduleName
			scheduleBackupName, err := CreateScheduleBackupWithValidation(nonAdminCtx, userScheduleName, SourceClusterName, appAdminBackupLocationName, appAdminBackupLocationUID, scheduledAppContexts, make(map[string]string), BackupOrgID, preRuleNameMap[appAdminUser], preRuleUidMap[appAdminUser], postRuleNameMap[appAdminUser], postRuleUidMap[appAdminUser], periodicSchedulePolicyNameMap[appAdminUser], periodicSchedulePolicyUidMap[appAdminUser])
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of schedule backup with schedule name [%s]", userScheduleName))
			userBackupNamesMap[appAdminUser] = SafeAppend(&mutex, userBackupNamesMap[appAdminUser], scheduleBackupName).([]string)
			err = SuspendBackupSchedule(userScheduleName, periodicSchedulePolicyNameMap[appAdminUser], BackupOrgID, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule [%s] for user [%s]", userScheduleName, appAdminUser))
		})

		Step(fmt.Sprintf("Take restore of applications from the App-admin user %s", appAdminUser), func() {
			log.InfoD(fmt.Sprintf("Taking restore of applications from the App-admin user %s", appAdminUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			restoreNameMap[appAdminUser] = fmt.Sprintf("%s-%s", RestoreNamePrefix, userBackupNamesMap[appAdminUser][0])
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateRestoreWithValidation(nonAdminCtx, restoreNameMap[appAdminUser], userBackupNamesMap[appAdminUser][0], make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsToBackup)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s of backup %s", restoreNameMap[appAdminUser], userBackupNamesMap[appAdminUser][0]))
		})

		Step("Validate taking manual backup of applications with namespace label", func() {
			log.InfoD("Validate taking manual backup of applications with namespace label")
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			srcClusterUid, err = Inst().Backup.GetClusterUID(nonAdminCtx, BackupOrgID, SourceClusterName)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			log.Infof("Cluster [%s] uid: [%s]", SourceClusterName, srcClusterUid)
			manualBackupWithLabel = fmt.Sprintf("%s-%v", "backup", RandomString(4))
			appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateBackupWithNamespaceLabelWithValidation(nonAdminCtx, manualBackupWithLabel, SourceClusterName, appAdminBackupLocationName, appAdminBackupLocationUID, appContextsExpectedInBackup,
				nil, BackupOrgID, srcClusterUid, "", "", "", "", nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup [%s] creation with labels [%s]", manualBackupWithLabel, nsLabelString))
			labelledBackupNames = append(labelledBackupNames, manualBackupWithLabel)
			err = NamespaceLabelBackupSuccessCheck(manualBackupWithLabel, nonAdminCtx, bkpNamespaces, nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespaces [%v] are backed up, and check if labels [%s] are applied to backups [%s]", bkpNamespaces, nsLabelString, manualBackupWithLabel))
		})

		Step("Validate restoring manual backup of applications with namespace label", func() {
			log.InfoD("Validate restoring manual backup of applications with namespace label")
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			restoreForManualBackupWithLabel = fmt.Sprintf("%s-%s", RestoreNamePrefix, manualBackupWithLabel)
			appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateRestoreWithValidation(nonAdminCtx, restoreForManualBackupWithLabel, manualBackupWithLabel, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsExpectedInBackup)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration of backup %s", manualBackupWithLabel))
			labelledRestoreNames = append(labelledRestoreNames, restoreForManualBackupWithLabel)
		})

		Step("Validate creating scheduled backup with namespace label", func() {
			log.InfoD("Validate creating scheduled backup with namespace label")
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			backupScheduleWithLabel = fmt.Sprintf("%s-%v", BackupNamePrefix, RandomString(4))
			appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			scheduledBackupNameWithLabel, err = CreateScheduleBackupWithNamespaceLabelWithValidation(nonAdminCtx, backupScheduleWithLabel, SourceClusterName, appAdminBackupLocationName, appAdminBackupLocationUID, appContextsExpectedInBackup,
				nil, BackupOrgID, "", "", "", "", nsLabelString, periodicSchedulePolicyNameMap[appAdminUser], periodicSchedulePolicyUidMap[appAdminUser])
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of creating first schedule backup %s with labels [%v]", backupScheduleWithLabel, nsLabelString))
			err = SuspendBackupSchedule(backupScheduleWithLabel, periodicSchedulePolicyNameMap[appAdminUser], BackupOrgID, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule [%s] for user [%s]", backupScheduleWithLabel, appAdminUser))
			labelledBackupNames = append(labelledBackupNames, scheduledBackupNameWithLabel)
			err = NamespaceLabelBackupSuccessCheck(scheduledBackupNameWithLabel, nonAdminCtx, bkpNamespaces, nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespaces [%v] are backed up, and check if labels [%s] are applied to backups [%s]", bkpNamespaces, nsLabelString, scheduledBackupNameWithLabel))
		})

		Step("Validate restoring the scheduled backup with namespace label", func() {
			log.InfoD("Validate restoring the scheduled backup with namespace label")
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			multipleBackupNamespace, err := FetchNamespacesFromBackup(nonAdminCtx, scheduledBackupNameWithLabel, BackupOrgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching namespaces %v from backup %v", multipleBackupNamespace, scheduledBackupNameWithLabel))
			multipleRestoreMapping = make(map[string]string)
			for _, namespace := range multipleBackupNamespace {
				restoredNameSpace := fmt.Sprintf("%s-%v", scheduledBackupNameWithLabel, RandomString(5))
				multipleRestoreMapping[namespace] = restoredNameSpace
			}
			customRestoreName = fmt.Sprintf("%s-%v", "customrestore", RandomString(4))
			appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateRestoreWithValidation(nonAdminCtx, customRestoreName, scheduledBackupNameWithLabel, multipleRestoreMapping, make(map[string]string), DestinationClusterName, BackupOrgID, appContextsExpectedInBackup)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying multiple backup restore [%s] in custom namespace [%v]", customRestoreName, multipleRestoreMapping))
			labelledRestoreNames = append(labelledRestoreNames, customRestoreName)
		})

		Step(fmt.Sprintf("Validate deleting namespace labelled backups and restores from the app-admin %s user context", appAdminUser), func() {
			log.InfoD(fmt.Sprintf("Validate deleting namespace labelled backups and restores from the app-admin %s user context", appAdminUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			var wg sync.WaitGroup
			for _, backupName := range labelledBackupNames {
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					backupUid, err := Inst().Backup.GetBackupUID(nonAdminCtx, backupName, BackupOrgID)
					log.FailOnError(err, "Failed to fetch the backup %s uid of the user %s", backupName, appAdminUser)
					_, err = DeleteBackup(backupName, backupUid, BackupOrgID, nonAdminCtx)
					log.FailOnError(err, "Failed to delete the backup %s of the user %s", backupName, appAdminUser)
					err = DeleteBackupAndWait(backupName, nonAdminCtx)
					log.FailOnError(err, fmt.Sprintf("waiting for backup [%s] deletion", backupName))
				}(backupName)
			}
			wg.Wait()
			for _, restoreName := range labelledRestoreNames {
				err := DeleteRestore(restoreName, BackupOrgID, nonAdminCtx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the deletion of the restore named [%s]", restoreName))
			}
		})

		Step(fmt.Sprintf("Delete app-admin user %s backups from the user context", appAdminUser), func() {
			log.InfoD(fmt.Sprintf("Deleting app-admin user %s backups from the user context", appAdminUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			backupUid, err := Inst().Backup.GetBackupUID(nonAdminCtx, userBackupNamesMap[appAdminUser][0], BackupOrgID)
			log.FailOnError(err, "failed to fetch backup %s uid of the user %s", userBackupNamesMap[appAdminUser][0], appAdminUser)
			_, err = DeleteBackup(userBackupNamesMap[appAdminUser][0], backupUid, BackupOrgID, nonAdminCtx)
			log.FailOnError(err, "failed to delete backup %s of the user %s", userBackupNamesMap[appAdminUser][0], appAdminUser)
			err = DeleteBackupAndWait(userBackupNamesMap[appAdminUser][0], nonAdminCtx)
			log.FailOnError(err, fmt.Sprintf("waiting for backup [%s] deletion", userBackupNamesMap[appAdminUser][0]))
		})

		Step(fmt.Sprintf("Delete app-admin user %s restores from the user context", appAdminUser), func() {
			log.InfoD(fmt.Sprintf("Deleting app-admin user %s restores from the user context", appAdminUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			err = DeleteRestore(restoreNameMap[appAdminUser], BackupOrgID, nonAdminCtx)
			log.FailOnError(err, "failed to delete restore %s of the user %s", restoreNameMap[appAdminUser], appAdminUser)
		})

		Step(fmt.Sprintf("Delete App-admin user %s backup schedule ", appAdminUser), func() {
			log.InfoD(fmt.Sprintf("Delete App-admin user %s backup schedule ", appAdminUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			err = DeleteSchedule(scheduleNameMap[appAdminUser], SourceClusterName, BackupOrgID, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Backup Schedule [%s] for user [%s]", scheduleNameMap[appAdminUser], appAdminUser))
			err = DeleteSchedule(backupScheduleWithLabel, SourceClusterName, BackupOrgID, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Backup Schedule [%s] for user [%s]", backupScheduleWithLabel, appAdminUser))
		})

		Step(fmt.Sprintf("Delete user %s source and destination cluster from the user context", appAdminUser), func() {
			log.InfoD(fmt.Sprintf("Deleting user %s source and destination cluster from the user context", appAdminUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
				err := DeleteCluster(clusterName, BackupOrgID, nonAdminCtx, false)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of cluster [%s] of the user %s", clusterName, appAdminUser))
				err = Inst().Backup.WaitForClusterDeletion(nonAdminCtx, clusterName, BackupOrgID, ClusterDeleteTimeout, ClusterDeleteRetryTime)
				log.FailOnError(err, fmt.Sprintf("waiting for cluster [%s] deletion", clusterName))
			}
		})

		Step(fmt.Sprintf("Verify px-admin group user can delete RBAC resources created by app-admin user [%s]", appAdminUser), func() {
			log.InfoD(fmt.Sprintf("Verify px-admin group user can delete RBAC resources created by app-admin user [%s]", appAdminUser))
			adminCtx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch admin ctx")
			nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
			log.Infof("Verify deletion of backup location [%s] of user [%s] from px-admin", appAdminBackupLocationName, appAdminUser)
			err = DeleteBackupLocation(appAdminBackupLocationName, appAdminBackupLocationUID, BackupOrgID, true)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of backup location [%s] of the user %s", appAdminBackupLocationName, appAdminUser))
			err = Inst().Backup.WaitForBackupLocationDeletion(adminCtx, appAdminBackupLocationName, appAdminBackupLocationUID, BackupOrgID, BackupLocationDeleteTimeout, BackupLocationDeleteRetryTime)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying waiting for backup location [%s]  deletion of the user %s", appAdminBackupLocationName, appAdminUser))
			log.Infof("Verify deletion of schedule policy [%s] of user [%s] from px-admin", periodicSchedulePolicyNameMap[appAdminUser], appAdminUser)
			err = Inst().Backup.DeleteBackupSchedulePolicy(BackupOrgID, []string{periodicSchedulePolicyNameMap[appAdminUser]})
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of schedule policy [%s] of the user %s", periodicSchedulePolicyNameMap[appAdminUser], appAdminUser))
			log.Infof("Verify deletion of rules of user [%s] from px-admin", appAdminUser)
			appAdminRules, _ := Inst().Backup.GetAllRules(nonAdminCtx, BackupOrgID)
			for _, ruleName := range appAdminRules {
				err := DeleteRule(ruleName, BackupOrgID, adminCtx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of rule [%s] of the user %s", ruleName, appAdminUser))
			}
		})
	})
	JustAfterEach(func() {
		defer func() {
			err := SetSourceKubeConfig()
			log.FailOnError(err, "Unable to switch context to source cluster [%s]", SourceClusterName)
			EndPxBackupTorpedoTest(scheduledAppContexts)
		}()
		backupDriver := Inst().Backup
		bkpScheduleEnumerateReq := &api.BackupScheduleEnumerateRequest{
			OrgId: BackupOrgID,
		}
		nonAdminCtx, err := backup.GetNonAdminCtx(appAdminUser, CommonPassword)
		log.FailOnError(err, "failed to fetch user %s ctx", appAdminUser)
		currentSchedules, err := backupDriver.EnumerateBackupSchedule(nonAdminCtx, bkpScheduleEnumerateReq)
		log.FailOnError(err, "Getting a list of all schedules")
		for _, sch := range currentSchedules.GetBackupSchedules() {
			err = DeleteSchedule(sch.Name, SourceClusterName, BackupOrgID, nonAdminCtx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting Backup Schedule [%s] for user [%s]", sch.Name, appAdminUser))
		}
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "failed to fetch admin ctx")
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		log.InfoD("Deleting labels from namespaces - %v", bkpNamespaces)
		err = DeleteLabelsFromMultipleNamespaces(nsLabelsMap, bkpNamespaces)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting labels [%v] from namespaces [%v]", nsLabelsMap, bkpNamespaces))
		log.InfoD("Deleting the px-backup objects")
		CleanupCloudSettingsAndClusters(backupLocationMap, adminCredName, adminCloudCredUID, ctx)
		log.InfoD("Switching context to destination cluster for clean up")
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "Unable to switch context to destination cluster [%s]", DestinationClusterName)
		DestroyApps(scheduledAppContexts, opts)
	})
})

// VerifyRBACForAppUser To verify all the RBAC operations for an app-user
var _ = Describe("{VerifyRBACForAppUser}", Label(TestCaseLabelsMap[VerifyRBACForAppUser]...), func() {
	var (
		scheduledAppContexts            []*scheduler.Context
		userNames                       = make([]string, 0)
		cloudCredUID                    string
		backupLocationUID               string
		credName                        string
		cloudCredName                   string
		backupLocationName              string
		bkpNamespaces                   []string
		appUser                         string
		providers                       = GetBackupProviders()
		backupLocationMap               = make(map[string]string)
		periodicSchedulePolicyName      string
		periodicSchedulePolicyUid       string
		preRuleName                     string
		postRuleName                    string
		preRuleUid                      string
		postRuleUid                     string
		scheduledBackupName             string
		userScheduleName                string
		restoreName                     string
		nsLabelsMap                     map[string]string
		nsLabelString                   string
		manualBackupWithLabel           string
		restoreForManualBackupWithLabel string
		srcClusterUid                   string
		backupScheduleWithLabel         string
		scheduledBackupNameWithLabel    string
		multipleRestoreMapping          map[string]string
		customRestoreName               string
		restoreNames                    []string
		backupNames                     []string
		scheduleNames                   []string
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("VerifyRBACForAppUser", "To verify all the RBAC operations for an app-user", nil, 87889, Sabrarhussaini, Q3FY24)
		log.InfoD("scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, appCtx := range appContexts {
				appCtx.ReadinessTimeout = AppReadinessTimeout
				scheduledAppContexts = append(scheduledAppContexts, appCtx)
				namespace := GetAppNamespace(appCtx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})

	It("To verify all the RBAC operations for an App-user", func() {
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Adding labels to namespaces", func() {
			log.InfoD("Adding labels to namespaces")
			nsLabelsMap = GenerateRandomLabels(2)
			err := AddLabelsToMultipleNamespaces(nsLabelsMap, bkpNamespaces)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Adding labels [%v] to namespaces [%v]", nsLabelsMap, bkpNamespaces))
		})

		Step("Generating namespace label string from label map for namespaces", func() {
			log.InfoD("Generating namespace label string from label map for namespaces")
			nsLabelString = MapToKeyValueString(nsLabelsMap)
			log.Infof("label string for namespaces %s", nsLabelString)
		})

		Step("Create an App-user", func() {
			log.InfoD("Creating a new user and assigning the role of App-user")
			appUser = CreateUsers(1)[0]
			err := backup.AddRoleToUser(appUser, backup.ApplicationUser, fmt.Sprintf("Adding Application User role to %s", appUser))
			log.FailOnError(err, "Failed to add role to user - [%s]", appUser)
			userNames = append(userNames, appUser)
		})

		// Verifying the backup objects creation for an app-user
		Step(fmt.Sprintf("Verify if the App-User doesn't have permission to create cloud credentials and backup location"), func() {
			log.InfoD("Verify if the App-User doesn't have permission to create cloud credentials and backup location")
			ctxNonAdmin, err := backup.GetNonAdminCtx(appUser, CommonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("cred-%s-%v", provider, RandomString(6))
				if provider != drivers.ProviderNfs {
					err = CreateCloudCredential(provider, credName, cloudCredUID, BackupOrgID, ctxNonAdmin)
					dash.VerifyFatal(strings.Contains(err.Error(), "PermissionDenied"), true, fmt.Sprintf("Verifying if App-User [%s] doesn't have permission for creating cloud credentials for provider [%s]", appUser, provider))
				} else {
					log.InfoD("Provider NFS does not require creating cloud credential")
				}
				backupLocationName = fmt.Sprintf("backup-location-%s-%v", provider, RandomString(4))
				err = CreateBackupLocationWithContext(provider, backupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), BackupOrgID, "", ctxNonAdmin, true)
				dash.VerifyFatal(strings.Contains(err.Error(), "PermissionDenied"), true, fmt.Sprintf("Verifying if App-User [%s] doesn't have permission for creating backup location", appUser))
			}
		})

		Step(fmt.Sprintf("Verify if App-User doesn't have permission to create a schedule policy"), func() {
			log.InfoD(fmt.Sprintf("Verify if App-User doesn't have permission to create a schedule policy"))
			nonAdminCtx, err := backup.GetNonAdminCtx(appUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user [%s] ctx", appUser)
			periodicSchedulePolicyName := fmt.Sprintf("policy-%v", RandomString(4))
			periodicSchedulePolicyUid := uuid.New()
			periodicSchedulePolicyInterval := int64(15)
			err = CreateBackupScheduleIntervalPolicy(5, periodicSchedulePolicyInterval, 5, periodicSchedulePolicyName, periodicSchedulePolicyUid, BackupOrgID, nonAdminCtx, false, false)
			dash.VerifyFatal(strings.Contains(err.Error(), "PermissionDenied"), true, fmt.Sprintf("Verifying if App-User [%s] doesn't have permission for creating schedule policy for user", appUser))
		})

		Step(fmt.Sprintf("Verify if the App-User doesn't have permission to create roles"), func() {
			log.InfoD("Verify if the App-User doesn't have permission to create roles")
			nonAdminCtx, err := backup.GetNonAdminCtx(appUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user [%s] ctx", appUser)
			appUserRoleName := backup.PxBackupRole(fmt.Sprintf("app-user-role-%s", RandomString(4)))
			services := []RoleServices{BackupSchedulePolicy, Rules, Cloudcredential, BackupLocation, Role}
			apis := []RoleApis{All}
			err = CreateRole(appUserRoleName, services, apis, nonAdminCtx)
			dash.VerifyFatal(strings.Contains(err.Error(), "PermissionDenied"), true, fmt.Sprintf("Verifying if App-User [%s] doesn't have permission for creating role", appUser))
		})

		Step(fmt.Sprintf("Verify if App-User doesn't have permission to create pre and post exec rules for applications"), func() {
			log.InfoD("Verify if App-User doesn't have permission to create pre and post exec rules for applications")
			nonAdminCtx, err := backup.GetNonAdminCtx(appUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user [%s] ctx", appUser)
			_, _, err = CreateRuleForBackupWithMultipleApplications(BackupOrgID, Inst().AppList, nonAdminCtx)
			dash.VerifyFatal(strings.Contains(err.Error(), "PermissionDenied"), true, fmt.Sprintf("Verifying if App-User [%s] doesn't have permission for creating rules", appUser))
		})

		// Verifying the RBAC objects creation by the Px-Admin
		Step("Validate creation of cloud credentials and backup locations for Px-Admin user", func() {
			log.InfoD("Validate creation of cloud credentials and backup locations for Px-Admin user")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, RandomString(4))
				log.InfoD("Creating cloud credential named [%s] and uid [%s] using [%s] as provider", cloudCredName, cloudCredUID, provider)
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, BackupOrgID, provider))
				backupLocationName = fmt.Sprintf("%s-%s-%v", provider, getGlobalBucketName(provider), RandomString(4))
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				bucketName := getGlobalBucketName(provider)
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudCredName, cloudCredUID, bucketName, BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup location named [%s] with uid [%s] of [%s] as provider", backupLocationName, backupLocationUID, provider))
			}
		})

		Step(fmt.Sprintf("Validate creation of schedule policy for Px-Admin user"), func() {
			log.InfoD("Validate creation of schedule policy for px-admin user")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			periodicSchedulePolicyName = fmt.Sprintf("%s-%v", "periodic", RandomString(4))
			periodicSchedulePolicyUid = uuid.New()
			periodicSchedulePolicyInterval := int64(15)
			err = CreateBackupScheduleIntervalPolicy(5, periodicSchedulePolicyInterval, 5, periodicSchedulePolicyName, periodicSchedulePolicyUid, BackupOrgID, ctx, false, false)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval [%v] minutes named [%s]", periodicSchedulePolicyInterval, periodicSchedulePolicyName))
		})

		Step(fmt.Sprintf("Verify creation of pre and post exec rules for applications for Px-Admin user"), func() {
			log.InfoD("Verify creation of pre and post exec rules for applications for Px-Admin user")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			preRuleName, postRuleName, err = CreateRuleForBackupWithMultipleApplications(BackupOrgID, Inst().AppList, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of pre and post exec rules for applications from px-admin"))
			if preRuleName != "" {
				preRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleName)
				log.FailOnError(err, "Fetching pre backup rule [%s] uid", preRuleName)
				log.Infof("Pre backup rule [%s] uid: [%s]", preRuleName, preRuleUid)
			}
			if postRuleName != "" {
				postRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleName)
				log.FailOnError(err, "Fetching post backup rule [%s] uid", postRuleName)
				log.Infof("Post backup rule [%s] uid: [%s]", postRuleName, postRuleUid)
			}
		})

		Step("Verify if the Px-Admin user has permission to share RBAC resources with the App-user", func() {
			log.InfoD("Verify if the Px-Admin user has permission to share RBAC resources with the App-user")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				if provider != drivers.ProviderNfs {
					log.Infof("Updating CloudAccount - %s ownership for user - [%v]", cloudCredName, appUser)
					err = AddCloudCredentialOwnership(cloudCredName, cloudCredUID, userNames, nil, Read, Invalid, ctx, BackupOrgID)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for CloudCredential- %s", cloudCredName))
				}
			}
			log.InfoD("Updating BackupLocation - %s ownership for users - %v", backupLocationName, userNames)
			err = AddBackupLocationOwnership(backupLocationName, backupLocationUID, userNames, nil, Read, Invalid, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for backuplocation - %s", backupLocationName))
			log.InfoD("Updating SchedulePolicy - %s ownership for users - %v", periodicSchedulePolicyName, userNames)
			err = AddSchedulePolicyOwnership(periodicSchedulePolicyName, periodicSchedulePolicyUid, userNames, nil, Read, Invalid, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for schedulepolicy - %s", periodicSchedulePolicyName))
			log.InfoD("Updating Application Rules ownership for users - %v", userNames)
			if preRuleName != "" {
				err = AddRuleOwnership(preRuleName, preRuleUid, userNames, nil, Read, Invalid, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for pre-rule of application"))
			}
			if postRuleName != "" {
				err = AddRuleOwnership(postRuleName, postRuleUid, userNames, nil, Read, Invalid, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for post-rule of application"))
			}
		})

		Step("Validate adding of source and destination clusters with App-User ctx", func() {
			log.InfoD("Validate adding of source and destination clusters with App-User ctx")
			ctxNonAdmin, err := backup.GetNonAdminCtx(appUser, CommonPassword)
			log.FailOnError(err, "Fetching non admin ctx")
			log.Infof("Creating source [%s] and destination [%s] clusters", SourceClusterName, DestinationClusterName)
			err = CreateApplicationClusters(BackupOrgID, "", "", ctxNonAdmin)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with App-User ctx", SourceClusterName, DestinationClusterName))
			srcClusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctxNonAdmin)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(srcClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			dstClusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, DestinationClusterName, ctxNonAdmin)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", DestinationClusterName))
			dash.VerifyFatal(dstClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", DestinationClusterName))
			srcClusterUid, err = Inst().Backup.GetClusterUID(ctxNonAdmin, BackupOrgID, SourceClusterName)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			log.Infof("Cluster [%s] uid: [%s]", SourceClusterName, srcClusterUid)
		})

		Step(fmt.Sprintf("Validate taking a scheduled backup of applications from the App-User [%s]", appUser), func() {
			log.InfoD(fmt.Sprintf("Validate taking a scheduled backup of applications from the App-User [%s]", appUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(appUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user [%s] ctx", appUser)
			userScheduleName = fmt.Sprintf("backup-schedule-%v", RandomString(4))
			scheduledBackupName, err = CreateScheduleBackupWithValidation(nonAdminCtx, userScheduleName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, make(map[string]string), BackupOrgID, preRuleName, preRuleUid, postRuleName, postRuleUid, periodicSchedulePolicyName, periodicSchedulePolicyUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of schedule backup with schedule name [%s]", userScheduleName))
			err = SuspendBackupSchedule(userScheduleName, periodicSchedulePolicyName, BackupOrgID, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule [%s] for user [%s]", userScheduleName, appUser))
			backupNames = append(backupNames, scheduledBackupName)
			scheduleNames = append(scheduleNames, userScheduleName)
		})

		Step(fmt.Sprintf("Validate restoring backups on destination cluster for the App-User [%s]", appUser), func() {
			log.InfoD(fmt.Sprintf("Validate restoring backups on destination cluster for the App-User [%s]", appUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(appUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user [%s] ctx", appUser)
			restoreName = fmt.Sprintf("%s-%s", RestoreNamePrefix, scheduledBackupName)
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateRestoreWithValidation(nonAdminCtx, restoreName, scheduledBackupName, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsToBackup)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s of backup %s", restoreName, scheduledBackupName))
			restoreNames = append(restoreNames, restoreName)
		})

		Step("Validate taking manual backup of applications with namespace label", func() {
			log.InfoD("Validate taking manual backup of applications with namespace label")
			nonAdminCtx, err := backup.GetNonAdminCtx(appUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user [%s] ctx", appUser)
			manualBackupWithLabel = fmt.Sprintf("%s-%v", "backup", RandomString(4))
			appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateBackupWithNamespaceLabelWithValidation(nonAdminCtx, manualBackupWithLabel, SourceClusterName, backupLocationName, backupLocationUID, appContextsExpectedInBackup,
				nil, BackupOrgID, srcClusterUid, "", "", "", "", nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup [%s] creation with labels [%s]", manualBackupWithLabel, nsLabelString))
			backupNames = append(backupNames, manualBackupWithLabel)
			err = NamespaceLabelBackupSuccessCheck(manualBackupWithLabel, nonAdminCtx, bkpNamespaces, nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespaces [%v] are backed up, and check if labels [%s] are applied to backups [%s]", bkpNamespaces, nsLabelString, manualBackupWithLabel))
		})

		Step("Validate restoring manual backup of applications with namespace label", func() {
			log.InfoD("Validate restoring manual backup of applications with namespace label")
			nonAdminCtx, err := backup.GetNonAdminCtx(appUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user [%s] ctx", appUser)
			restoreForManualBackupWithLabel = fmt.Sprintf("%s-%s", RestoreNamePrefix, manualBackupWithLabel)
			appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateRestoreWithValidation(nonAdminCtx, restoreForManualBackupWithLabel, manualBackupWithLabel, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsExpectedInBackup)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration of backup %s", manualBackupWithLabel))
			restoreNames = append(restoreNames, restoreForManualBackupWithLabel)
		})

		Step("Validate creating scheduled backup with namespace label", func() {
			log.InfoD("Validate creating scheduled backup with namespace label")
			nonAdminCtx, err := backup.GetNonAdminCtx(appUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user [%s] ctx", appUser)
			backupScheduleWithLabel = fmt.Sprintf("%s-%v", BackupNamePrefix, RandomString(4))
			appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			scheduledBackupNameWithLabel, err = CreateScheduleBackupWithNamespaceLabelWithValidation(nonAdminCtx, backupScheduleWithLabel, SourceClusterName, backupLocationName, backupLocationUID, appContextsExpectedInBackup,
				nil, BackupOrgID, "", "", "", "", nsLabelString, periodicSchedulePolicyName, periodicSchedulePolicyUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of creating first schedule backup %s with labels [%v]", backupScheduleWithLabel, nsLabelString))
			err = SuspendBackupSchedule(backupScheduleWithLabel, periodicSchedulePolicyName, BackupOrgID, nonAdminCtx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule [%s] for user [%s]", backupScheduleWithLabel, appUser))
			backupNames = append(backupNames, scheduledBackupNameWithLabel)
			scheduleNames = append(scheduleNames, backupScheduleWithLabel)
			err = NamespaceLabelBackupSuccessCheck(scheduledBackupNameWithLabel, nonAdminCtx, bkpNamespaces, nsLabelString)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the labeled namespaces [%v] are backed up, and check if labels [%s] are applied to backups [%s]", bkpNamespaces, nsLabelString, scheduledBackupNameWithLabel))
		})

		Step("Validate restoring the scheduled backup with namespace label", func() {
			log.InfoD("Validate restoring the scheduled backup with namespace label")
			nonAdminCtx, err := backup.GetNonAdminCtx(appUser, CommonPassword)
			multipleBackupNamespace, err := FetchNamespacesFromBackup(nonAdminCtx, scheduledBackupNameWithLabel, BackupOrgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching namespaces %v from backup %v", multipleBackupNamespace, scheduledBackupNameWithLabel))
			multipleRestoreMapping = make(map[string]string)
			for _, namespace := range multipleBackupNamespace {
				restoredNameSpace := fmt.Sprintf("%s-%v", scheduledBackupNameWithLabel, RandomString(5))
				multipleRestoreMapping[namespace] = restoredNameSpace
			}
			customRestoreName = fmt.Sprintf("%s-%v", "customrestore", RandomString(4))
			appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateRestoreWithValidation(nonAdminCtx, customRestoreName, scheduledBackupNameWithLabel, multipleRestoreMapping, make(map[string]string), DestinationClusterName, BackupOrgID, appContextsExpectedInBackup)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying multiple backup restore [%s] in custom namespace [%v]", customRestoreName, multipleRestoreMapping))
			restoreNames = append(restoreNames, customRestoreName)
		})

		Step(fmt.Sprintf("Validate deleting of backups for the context of App-User [%s] ", appUser), func() {
			log.InfoD(fmt.Sprintf("Validate deleting of backups  for the context of App-User [%s] ", appUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(appUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appUser)
			var wg sync.WaitGroup
			for _, backupName := range backupNames {
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					backupUid, err := Inst().Backup.GetBackupUID(nonAdminCtx, backupName, BackupOrgID)
					log.FailOnError(err, "Failed to fetch the backup %s uid of the user %s", backupName, appUser)
					_, err = DeleteBackup(backupName, backupUid, BackupOrgID, nonAdminCtx)
					log.FailOnError(err, "Failed to delete the backup %s of the user %s", backupName, appUser)
					err = DeleteBackupAndWait(backupName, nonAdminCtx)
					log.FailOnError(err, fmt.Sprintf("waiting for backup [%s] deletion", backupName))
				}(backupName)
			}
			wg.Wait()
		})

		Step(fmt.Sprintf("Validate deleting of restores for the context of App-User [%s] ", appUser), func() {
			log.InfoD(fmt.Sprintf("Validate deleting of restores for the context of App-User [%s] ", appUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(appUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appUser)
			for _, restoreName := range restoreNames {
				err := DeleteRestore(restoreName, BackupOrgID, nonAdminCtx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Verifying the deletion of the restore named [%s]", restoreName))
			}
		})

		Step(fmt.Sprintf("Validate deleting of backup schedules for the App-User [%s]", appUser), func() {
			log.InfoD(fmt.Sprintf("Validate deleting of backup schedules for the App-User [%s]", appUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(appUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appUser)
			for _, scheduleName := range scheduleNames {
				err = DeleteSchedule(scheduleName, SourceClusterName, BackupOrgID, nonAdminCtx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Backup Schedule [%s] for user [%s]", scheduleName, appUser))
			}
		})

		Step(fmt.Sprintf("Validate deleting of source and destination cluster for the App-user [%s]", appUser), func() {
			log.InfoD(fmt.Sprintf("Validate deleting of source and destination cluster for the App-user [%s]", appUser))
			nonAdminCtx, err := backup.GetNonAdminCtx(appUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", appUser)
			for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
				err := DeleteCluster(clusterName, BackupOrgID, nonAdminCtx, false)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of cluster [%s] of the user %s", clusterName, appUser))
				err = Inst().Backup.WaitForClusterDeletion(nonAdminCtx, clusterName, BackupOrgID, ClusterDeleteTimeout, ClusterDeleteRetryTime)
				log.FailOnError(err, fmt.Sprintf("waiting for cluster [%s] deletion", clusterName))
			}
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		defer func() {
			err := SetSourceKubeConfig()
			log.FailOnError(err, "Unable to switch context to source cluster [%s]", SourceClusterName)
		}()
		backupDriver := Inst().Backup
		bkpScheduleEnumerateReq := &api.BackupScheduleEnumerateRequest{
			OrgId: BackupOrgID,
		}
		nonAdminCtx, err := backup.GetNonAdminCtx(appUser, CommonPassword)
		log.FailOnError(err, "failed to fetch user %s ctx", appUser)
		currentSchedules, err := backupDriver.EnumerateBackupSchedule(nonAdminCtx, bkpScheduleEnumerateReq)
		log.FailOnError(err, "Getting a list of all schedules")
		for _, sch := range currentSchedules.GetBackupSchedules() {
			err = DeleteSchedule(sch.Name, SourceClusterName, BackupOrgID, nonAdminCtx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting Backup Schedule [%s] for user [%s]", sch.Name, appUser))
		}
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		log.InfoD("Deleting labels from namespaces - %v", bkpNamespaces)
		err = DeleteLabelsFromMultipleNamespaces(nsLabelsMap, bkpNamespaces)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting labels [%v] from namespaces [%v]", nsLabelsMap, bkpNamespaces))
		log.InfoD("Deleting the px-backup objects")
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
		UserRules, _ := Inst().Backup.GetAllRules(ctx, BackupOrgID)
		for _, ruleName := range UserRules {
			err := DeleteRule(ruleName, BackupOrgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of rule [%s] for the Px-Admin user", ruleName))
		}
		log.InfoD("Switching context to destination cluster for clean up")
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "Unable to switch context to destination cluster [%s]", DestinationClusterName)
		DestroyApps(scheduledAppContexts, opts)
		log.InfoD("Switching back context to Source cluster")
		err = SetSourceKubeConfig()
		log.FailOnError(err, "Unable to switch context to source cluster [%s]", SourceClusterName)
	})
})
