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
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

// createBackupUntilIncrementalBackup creates backup until incremental backups is created returns the name of the incremental backup created
func createBackupUntilIncrementalBackup(ctx context.Context, scheduledAppContextToBackup *scheduler.Context, customBackupLocationName string, backupLocationUID string, labelSelectors map[string]string, orgID string, clusterUid string) (string, error) {
	namespace := scheduledAppContextToBackup.ScheduleOptions.Namespace
	incrementalBackupName := fmt.Sprintf("%s-%s-%v", "incremental-backup", namespace, time.Now().Unix())
	err := CreateBackupWithValidation(ctx, incrementalBackupName, SourceClusterName, customBackupLocationName, backupLocationUID, []*scheduler.Context{scheduledAppContextToBackup}, labelSelectors, orgID, clusterUid, "", "", "", "")
	if err != nil {
		return "", fmt.Errorf("creation and validation of incremental backup [%s] creation", incrementalBackupName)
	}

	log.InfoD("Check if backups are incremental backups or not")
	backupDriver := Inst().Backup
	ctx, err = backup.GetAdminCtxFromSecret()
	if err != nil {
		return "", fmt.Errorf("fetching px-central-admin ctx")
	}
	bkpUid, err := backupDriver.GetBackupUID(ctx, incrementalBackupName, orgID)
	if err != nil {
		return "", fmt.Errorf("unable to fetch backup UID - %s", incrementalBackupName)
	}

	bkpInspectReq := &api.BackupInspectRequest{
		Name:  incrementalBackupName,
		OrgId: orgID,
		Uid:   bkpUid,
	}
	bkpInspectResponse, err := backupDriver.InspectBackup(ctx, bkpInspectReq)
	if err != nil {
		return "", fmt.Errorf("unable to fetch backup - %s", incrementalBackupName)
	}

	for _, vol := range bkpInspectResponse.GetBackup().GetVolumes() {
		backupId := vol.GetBackupId()
		log.InfoD(fmt.Sprintf("Backup Name: %s; BackupID: %s", incrementalBackupName, backupId))
		if strings.Contains(backupId, "incr") {
			return incrementalBackupName, nil
		} else {
			// Attempting to take backups and checking if they are incremental or not
			// as the original incremental backup which we took has taken a full backup this is mostly
			// because CloudSnap is taking full backup instead of incremental backup as it's hitting one of
			// the if else condition in CloudSnap which forces it to take full instead of incremental backup
			log.InfoD("New backup wasn't an incremental backup hence recreating new backup")
			listOfVolumes := make(map[string]bool)
			noFailures := true
			for maxBackupsBeforeIncremental := 0; maxBackupsBeforeIncremental < 8; maxBackupsBeforeIncremental++ {
				log.InfoD(fmt.Sprintf("Recreate incremental backup iteration: %d", maxBackupsBeforeIncremental))
				// Create a new incremental backups
				incrementalBackupName = fmt.Sprintf("%s-%s-%v", "incremental-backup", namespace, time.Now().Unix())
				err := CreateBackupWithValidation(ctx, incrementalBackupName, SourceClusterName, customBackupLocationName, backupLocationUID, []*scheduler.Context{scheduledAppContextToBackup}, labelSelectors, orgID, clusterUid, "", "", "", "")
				if err != nil {
					return "", fmt.Errorf("verifying incremental backup [%s] creation", incrementalBackupName)
				}

				// Check if they are incremental or not
				bkpUid, err = backupDriver.GetBackupUID(ctx, incrementalBackupName, orgID)
				if err != nil {
					return "", fmt.Errorf("unable to fetch backup - %s", incrementalBackupName)
				}
				bkpInspectReq := &api.BackupInspectRequest{
					Name:  incrementalBackupName,
					OrgId: orgID,
					Uid:   bkpUid,
				}
				bkpInspectResponse, err = backupDriver.InspectBackup(ctx, bkpInspectReq)
				if err != nil {
					return "", fmt.Errorf("unable to fetch backup - %s", incrementalBackupName)
				}
				for _, vol := range bkpInspectResponse.GetBackup().GetVolumes() {
					backupId := vol.GetBackupId()
					log.InfoD(fmt.Sprintf("Backup Name: %s; BackupID: %s", incrementalBackupName, backupId))
					if !strings.Contains(backupId, "incr") {
						listOfVolumes[backupId] = false
					} else {
						listOfVolumes[backupId] = true
					}
				}
				for id, isIncremental := range listOfVolumes {
					if !isIncremental {
						log.InfoD(fmt.Sprintf("Backup %s wasn't a incremental backup", id))
						noFailures = false
					}
				}
				if noFailures {
					break
				}
			}
		}
	}
	return incrementalBackupName, nil
}

// IssueDeleteOfIncrementalBackupsAndRestore Issues delete of incremental backups in between and tries to restore from
// the newest backup.
var _ = Describe("{IssueDeleteOfIncrementalBackupsAndRestore}", func() {
	var (
		credName                 string
		clusterUid               string
		cloudCredUID             string
		fullBackupName           string
		restoreName              string
		backupLocationUID        string
		customBackupLocationName string
		incrementalBackupName    string
		restoreNames             []string
		cloudCredUidList         []string
		namespaceMapping         map[string]string
		scheduledAppContexts     []*scheduler.Context
		clusterStatus            api.ClusterInfo_StatusInfo_Status
	)
	labelSelectors := make(map[string]string)
	backupNames := make([]string, 0)
	incrementalBackupNames := make([]string, 0)
	incrementalBackupNames2 := make([]string, 0)
	var bkpNamespaces = make([]string, 0)
	backupLocationMap := make(map[string]string)

	JustBeforeEach(func() {
		StartTorpedoTest("IssueDeleteOfIncrementalBackupsAndRestore",
			"Issue delete of incremental backups and try to restore the newest backup", nil, 58056)
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

	It("Issue delete of incremental backups and try to restore the newest backup", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
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
				cloudCredCreateStatus := func() (interface{}, bool, error) {
					ok, err := IsCloudCredPresent(credName, ctx, orgID)
					if err != nil {
						return "", true, fmt.Errorf("cloud cred %s is not created with error %v", credName, err)
					}
					if ok {
						return "", false, nil
					}
					return "", true, fmt.Errorf("cloud cred %s is created yet", credName)
				}
				_, err = DoRetryWithTimeoutWithGinkgoRecover(cloudCredCreateStatus, 10*time.Minute, 30*time.Second)
				customBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
				backupLocationMap[backupLocationUID] = customBackupLocationName
				log.InfoD("Created Backup Location with name - %s", customBackupLocationName)
			}
		})

		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
			// Registering for admin user
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

		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			// Full backup
			for _, namespace := range bkpNamespaces {
				fullBackupName = fmt.Sprintf("%s-%s-%v", "full-backup", namespace, time.Now().Unix())
				backupNames = append(backupNames, fullBackupName)
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err := CreateBackupWithValidation(ctx, fullBackupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of full backup [%s]", fullBackupName))
			}

			// Incremental backup set 1
			for _, namespace := range bkpNamespaces {
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				incrementalBackupName, err = createBackupUntilIncrementalBackup(ctx, appContextsToBackup[0], customBackupLocationName, backupLocationUID, labelSelectors, orgID, clusterUid)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating incremental backup [%s]", incrementalBackupName))
				incrementalBackupNames = append(incrementalBackupNames, incrementalBackupName)
			}

			// Incremental backup set 2
			for _, namespace := range bkpNamespaces {
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				incrementalBackupName, err = createBackupUntilIncrementalBackup(ctx, appContextsToBackup[0], customBackupLocationName, backupLocationUID, labelSelectors, orgID, clusterUid)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating incremental backup [%s]", incrementalBackupName))
				incrementalBackupNames2 = append(incrementalBackupNames2, incrementalBackupName)
			}

			log.InfoD("List of backups - %v", backupNames)
			log.InfoD("List of Incremental backups Set 1 - %v", incrementalBackupNames)
			log.InfoD("List of Incremental backups Set 2 - %v", incrementalBackupNames2)
		})

		Step("Deleting incremental backup", func() {
			log.InfoD("Deleting incremental backups")
			backupDriver := Inst().Backup
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range incrementalBackupNames {
				log.Infof("About to delete backup - %s", backupName)
				backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
				log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
				_, err = DeleteBackup(backupName, backupUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting backup - [%s]", backupName))
			}
		})
		Step("Restoring the backed up namespaces", func() {
			log.InfoD("Restoring the backed up namespaces")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range incrementalBackupNames2 {
				restoreName = fmt.Sprintf("%s-%s", backupName, RandomString(4))
				for strings.Contains(strings.Join(restoreNames, ","), restoreName) {
					restoreName = fmt.Sprintf("%s-%s", backupName, RandomString(4))
				}
				log.InfoD("Restoring %s backup", backupName)
				err = CreateRestore(restoreName, backupName, namespaceMapping, destinationClusterName, orgID, ctx, make(map[string]string))
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
				restoreNames = append(restoreNames, restoreName)
			}
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		// Cleaning up applications created
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		// Remove all the restores created
		log.Info("Deleting restored namespaces")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		for _, restoreName := range restoreNames {
			err := DeleteRestore(restoreName, orgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore [%s]", restoreName))
		}

		// Cleaning up px-backup cluster
		ctx, err = backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})

// DeleteIncrementalBackupsAndRecreateNew Delete Incremental Backups and Recreate
// new ones
var _ = Describe("{DeleteIncrementalBackupsAndRecreateNew}", func() {
	backupNames := make([]string, 0)
	incrementalBackupNames := make([]string, 0)
	incrementalBackupNamesRecreated := make([]string, 0)
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
	backupLocationMap := make(map[string]string)

	JustBeforeEach(func() {
		StartTorpedoTest("DeleteIncrementalBackupsAndRecreateNew",
			"Delete incremental Backups and re-create them", nil, 58039)
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

	It("Delete incremental Backups and re-create them", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
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
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating cloud credential %s", credName))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				customBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				err := CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
				backupLocationMap[backupLocationUID] = customBackupLocationName
				log.InfoD("Created Backup Location with name - %s", customBackupLocationName)
			}
		})

		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
			// Registering for admin user
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

		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			// Full backup
			for _, namespace := range bkpNamespaces {
				fullBackupName = fmt.Sprintf("%s-%s-%v", "full-backup", namespace, time.Now().Unix())
				backupNames = append(backupNames, fullBackupName)
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, fullBackupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of full backup [%s]", fullBackupName))
			}

			// Incremental backup
			for _, namespace := range bkpNamespaces {
				incrementalBackupName = fmt.Sprintf("%s-%s-%v", "incremental-backup", namespace, time.Now().Unix())
				incrementalBackupNames = append(incrementalBackupNames, incrementalBackupName)
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, incrementalBackupName, SourceClusterName, customBackupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of incremental backup [%s]", incrementalBackupName))
			}
			log.Infof("List of backups - %v", backupNames)
			log.Infof("List of Incremental backups - %v", incrementalBackupNames)

		})
		Step("Deleting incremental backup", func() {
			log.InfoD("Deleting incremental backups")
			backupDriver := Inst().Backup
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range incrementalBackupNames {
				log.Infof("About to delete backup - %s", backupName)
				backupUID, err := backupDriver.GetBackupUID(ctx, backupName, orgID)
				log.FailOnError(err, "Failed while trying to get backup UID for - %s", backupName)
				_, err = DeleteBackup(backupName, backupUID, orgID, ctx)
				log.FailOnError(err, "Failed to issue delete backup for - %s", backupName)
				err = DeleteBackupAndWait(backupName, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Deleted backup - [%s]", backupName))
			}
		})
		Step("Taking incremental backups of applications again", func() {
			log.InfoD("Taking incremental backups of applications again")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			// Incremental backup
			for _, namespace := range bkpNamespaces {
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				incrementalBackupName, err = createBackupUntilIncrementalBackup(ctx, appContextsToBackup[0], customBackupLocationName, backupLocationUID, labelSelectors, orgID, clusterUid)
				dash.VerifyFatal(err, nil, "Creating incremental backup")
				incrementalBackupNamesRecreated = append(incrementalBackupNamesRecreated, incrementalBackupName)
			}
			log.Infof("List of New Incremental backups - %v", incrementalBackupNames)
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		// Cleaning up applications created
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		// Cleaning up px-backup cluster
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})

// DeleteBucketVerifyCloudBackupMissing validates the backup state (CloudBackupMissing) when bucket is deleted.
var _ = Describe("{DeleteBucketVerifyCloudBackupMissing}", func() {
	var (
		scheduledAppContexts       []*scheduler.Context
		clusterUid                 string
		cloudAccountUID            string
		cloudAccountName           string
		bkpLocationName            string
		backupLocationUID          string
		backupLocationMap          map[string]string
		periodicSchedulePolicyName string
		periodicSchedulePolicyUid  string
		scheduleName               string
		appNamespaces              []string
		scheduleNames              []string
		backupNames                []string
		localBucketNameMap         map[string]string
	)

	providers := getProviders()
	backupLocationMap = make(map[string]string)
	localBucketNameMap = make(map[string]string)
	appContextsToBackupMap := make(map[string][]*scheduler.Context)

	JustBeforeEach(func() {
		StartTorpedoTest("DeleteBucketVerifyCloudBackupMissing", "Validates the backup state (CloudBackupMissing) when bucket is deleted.", nil, 58070)
		log.Infof("Deploying applications required for the testcase")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})

	It("Delete bucket and Validates the backup state", func() {
		Step("Validate deployed applications", func() {
			ValidateApplications(scheduledAppContexts)
		})

		Step("Adding cloud path/bucket", func() {
			log.InfoD("Adding cloud path/bucket")
			for _, provider := range providers {
				bucketNameSuffix := getBucketNameSuffix()
				bucketNamePrefix := fmt.Sprintf("local-%s", provider)
				localBucketName := fmt.Sprintf("%s-%s-%v", bucketNamePrefix, bucketNameSuffix, time.Now().Unix())
				CreateBucket(provider, localBucketName)
				log.Infof("Bucket created with name - %s", localBucketName)
				localBucketNameMap[provider] = localBucketName
			}
		})

		Step("Adding cloud account and backup location", func() {
			log.InfoD("Adding cloud account and backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudAccountName = fmt.Sprintf("%s-%s-%v", "cloudcred", provider, time.Now().Unix())
				bkpLocationName = fmt.Sprintf("%s-%s-%v-bl", provider, getGlobalBucketName(provider), time.Now().Unix())
				cloudAccountUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = bkpLocationName
				err := CreateCloudCredential(provider, cloudAccountName, cloudAccountUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud account named [%s] for org [%s] with [%s] as provider", cloudAccountName, orgID, provider))
				err = CreateBackupLocation(provider, bkpLocationName, backupLocationUID, cloudAccountName, cloudAccountUID,
					localBucketNameMap[provider], orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", bkpLocationName))
			}
		})

		Step("Creating Schedule Policies", func() {
			log.InfoD("Creating Schedule Policies")
			periodicSchedulePolicyName = fmt.Sprintf("%s-%v", "periodic", time.Now().Unix())
			periodicSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, 15, 5)
			periodicPolicyStatus := Inst().Backup.BackupSchedulePolicy(periodicSchedulePolicyName, uuid.New(), orgID, periodicSchedulePolicyInfo)
			dash.VerifyFatal(periodicPolicyStatus, nil, fmt.Sprintf("Verification of creating periodic schedule policy - %s", periodicSchedulePolicyName))
		})

		Step("Adding Clusters for backup", func() {
			log.InfoD("Adding Clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of creating source - %s and destination - %s clusters", SourceClusterName, destinationClusterName))
			clusterStatus, err := Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Creating schedule backup", func() {
			log.InfoD("Creating schedule backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			periodicSchedulePolicyUid, _ = Inst().Backup.GetSchedulePolicyUid(orgID, ctx, periodicSchedulePolicyName)
			for _, namespace := range appNamespaces {
				scheduleName = fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, time.Now().Unix())
				scheduleNames = append(scheduleNames, scheduleName)
				labelSelectors := make(map[string]string)
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				appContextsToBackupMap[scheduleName] = appContextsToBackup

				firstScheduleBackupName, err := CreateScheduleBackupWithValidation(ctx, scheduleName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, "", "", "", "", periodicSchedulePolicyName, periodicSchedulePolicyUid)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of schedule backup with schedule name [%s]", scheduleName))
				backupNames = append(backupNames, firstScheduleBackupName)
			}
		})

		Step("Creating a manual backup", func() {
			log.InfoD("Creating a manual backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range appNamespaces {
				backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
				labelSelectors := make(map[string]string)
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
				backupNames = append(backupNames, backupName)
			}
		})

		Step("Suspending the existing backup schedules", func() {
			log.InfoD("Suspending the existing backup schedules")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, scheduleName := range scheduleNames {
				err = suspendBackupSchedule(scheduleName, periodicSchedulePolicyName, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of suspending backup schedule - %s", scheduleName))
			}
		})

		Step("Delete the bucket where the backup objects are present", func() {
			log.InfoD("Delete the bucket where the backup objects are present")
			for _, provider := range providers {
				DeleteBucket(provider, localBucketNameMap[provider])
				log.Infof("Sleeping for default 10 minutes for next backup sync service to be triggered")
				time.Sleep(10 * time.Minute)
			}
		})

		Step("Verify the backups are in CloudBackupMissing state after bucket deletion", func() {
			log.InfoD("Verify the backups are in CloudBackupMissing state after bucket deletion")
			var wg sync.WaitGroup
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				wg.Add(1)
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
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

		Step("Resume the existing backup schedules", func() {
			log.InfoD("Resume the existing backup schedules")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, scheduleName := range scheduleNames {
				err = resumeBackupSchedule(scheduleName, periodicSchedulePolicyName, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of resuming backup schedule - %s", scheduleName))
			}
			log.Infof("Waiting 5 minute for another schedule backup to trigger")
			time.Sleep(5 * time.Minute)
		})

		Step("Get the latest schedule backup and verify the backup status", func() {
			log.InfoD("Get the latest schedule backup and verify the backup status")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, scheduleName := range scheduleNames {
				latestScheduleBkpName, err := GetLatestScheduleBackupName(ctx, scheduleName, orgID)
				log.FailOnError(err, "Error while getting latest schedule backup name")
				err = backupSuccessCheckWithValidation(ctx, latestScheduleBkpName, appContextsToBackupMap[scheduleName], orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success and Validation of latest schedule backup [%s]", latestScheduleBkpName))
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		for _, scheduleName := range scheduleNames {
			err = DeleteSchedule(scheduleName, SourceClusterName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		}
		log.Infof("Deleting backup schedule policy")
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, []string{periodicSchedulePolicyName})
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudAccountName, cloudAccountUID, ctx)
		log.InfoD("Delete the local bucket created")
		for _, provider := range providers {
			DeleteBucket(provider, localBucketNameMap[provider])
			log.Infof("local bucket deleted - %s", localBucketNameMap[provider])
		}
	})
})

// DeleteBackupAndCheckIfBucketIsEmpty delete backups and verify if contents are deleted from backup location or not
var _ = Describe("{DeleteBackupAndCheckIfBucketIsEmpty}", func() {
	numberOfBackups, _ := strconv.Atoi(getEnv(maxBackupsToBeCreated, "10"))
	var (
		scheduledAppContexts     []*scheduler.Context
		backupLocationUID        string
		cloudCredUID             string
		bkpNamespaces            []string
		clusterUid               string
		clusterStatus            api.ClusterInfo_StatusInfo_Status
		customBackupLocationName string
		credName                 string
	)
	timeBetweenConsecutiveBackups := 10 * time.Second
	backupNames := make([]string, 0)
	numberOfSimultaneousBackups := 4
	labelSelectors := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	backupLocationMap := make(map[string]string)
	appContextsToBackupMap := make(map[string][]*scheduler.Context)

	JustBeforeEach(func() {
		StartTorpedoTest("DeleteBackupAndCheckIfBucketIsEmpty",
			"Delete backups and verify if contents are deleted from backup location or not", nil, 58071)
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
		providers := getProviders()
		log.Info("Check if backup location is empty or not")
		for _, provider := range providers {
			result, err := IsS3BucketEmpty(getGlobalBucketName(provider))
			dash.VerifyFatal(err, nil, "Checking for errors while checking s3 bucket")
			dash.VerifyFatal(result, true, "Check in bucket is empty or not")
		}
	})
	It("Delete backups and verify if contents are deleted from backup location or not", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Adding Credentials and Backup Location", func() {
			log.InfoD("Using pre-provisioned bucket. Creating cloud credentials and backup location.")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err := CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				customBackupLocationName = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				err = CreateBackupLocation(provider, customBackupLocationName, backupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationName))
				backupLocationMap[backupLocationUID] = customBackupLocationName
			}
		})

		Step("Register source and destination cluster for backup", func() {
			log.InfoD("Registering Source and Destination clusters and verifying the status")
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

		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			var sem = make(chan struct{}, numberOfSimultaneousBackups)
			var wg sync.WaitGroup
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.InfoD("Taking %d backups", numberOfBackups)
			var mutex sync.Mutex
			for backupLocationUID, backupLocationName := range backupLocationMap {
				for _, namespace := range bkpNamespaces {
					for i := 0; i < numberOfBackups; i++ {
						time.Sleep(timeBetweenConsecutiveBackups)
						backupName := fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
						backupNames = append(backupNames, backupName)
						sem <- struct{}{}
						wg.Add(1)
						go func(backupName, backupLocationName, backupLocationUID, namespace string) {
							defer GinkgoRecover()
							defer wg.Done()
							defer func() { <-sem }()
							appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
							mutex.Lock()
							appContextsToBackupMap[backupName] = appContextsToBackup
							mutex.Unlock()
							err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, "", "", "", "")
							dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
						}(backupName, backupLocationName, backupLocationUID, namespace)
					}
				}
			}
			wg.Wait()
			log.Infof("List of backups - %v", backupNames)
		})

		Step("Delete all the backups taken in the previous step ", func() {
			log.InfoD("Deleting the backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			dash.VerifyFatal(err, nil, "Unable fetch admin context")
			backupDriver := Inst().Backup
			for _, backup := range backupNames {
				backupUID, err := backupDriver.GetBackupUID(ctx, backup, orgID)
				dash.VerifySafely(err, nil, fmt.Sprintf("trying to get backup UID for backup %s", backup))
				_, err = DeleteBackup(backup, backupUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting the backup %s", backup))
			}
			for _, backup := range backupNames {
				err := DeleteBackupAndWait(backup, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Waiting for the backup %s to be deleted", backup))
			}
		})
		Step("Check if contents are erased from the backup location or not", func() {
			log.Info("Check if backup location is empty or not")
			for _, provider := range providers {
				result, err := IsS3BucketEmpty(getGlobalBucketName(provider))
				dash.VerifyFatal(err, nil, "Checking for errors while checking s3 bucket")
				dash.VerifyFatal(result, true, "Check if bucket is empty or not")
			}
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
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})
