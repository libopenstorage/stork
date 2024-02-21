package tests

import (
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	"github.com/pborman/uuid"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/pkg/asyncdr"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

// This testcase verifies backup and restore of applications when PX sync DR configured on application namespace.
var _ = Describe("{BackupAndRestoreSyncDR}", func() {
	var (
		backupName                 string
		scheduledAppContexts       []*scheduler.Context
		AppContextsMapping         = make(map[string]*scheduler.Context)
		namespace                  string
		bkpNamespaces              = make([]string, 0)
		backupNames                = make([]string, 0)
		drBackupNames              = make([]string, 0)
		dcBackupNames              = make([]string, 0)
		scheduleNames              = make([]string, 0)
		restoreNames               = make([]string, 0)
		srcClusterUid              string
		destClusterUid             string
		clusterStatus              api.ClusterInfo_StatusInfo_Status
		cloudCredName              string
		cloudCredUID               string
		backupLocationUID          string
		bkpLocationName            string
		numDeployments             int
		providers                  []string
		backupLocationMap          = make(map[string]string)
		labelSelectors             = make(map[string]string)
		backupNamespaceMap         = make(map[string]string)
		migrationNamespaceMap      = make(map[string]string)
		preRuleName                string
		postRuleName               string
		preRuleUid                 string
		postRuleUid                string
		periodicSchedulePolicyName string
		periodicSchedulePolicyUid  string
		wg                         sync.WaitGroup
		defaultClusterPairDir      = "cluster-pair"
		metromigrationKey          = "metro-dr-"
		migrationRetryTimeout      = 10 * time.Minute
		migrationRetryInterval     = 10 * time.Second
		includeVolumesFlag         = false
		includeResourcesFlag       = true
		startApplicationsFlag      = false
		suspendSched               = false
		autoSuspend                = false
		syncSchPolicyName          = "sync-bkp-policy"
	)
	JustBeforeEach(func() {
		numDeployments = 1
		providers = GetBackupProviders()
		StartPxBackupTorpedoTest("BackupAndRestoreSyncDR", "backup and restore of applications when PX sync DR configured on application namespace", nil, 86097, Ak, Q4FY24)

		log.InfoD(fmt.Sprintf("App list %v", Inst().AppList))
		scheduledAppContexts = make([]*scheduler.Context, 0)
		log.InfoD("Starting to deploy applications")
		for i := 0; i < numDeployments; i++ {
			log.InfoD(fmt.Sprintf("Iteration %v of deploying applications", i))
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = AppReadinessTimeout
				namespace = GetAppNamespace(ctx, taskName)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
				AppContextsMapping[namespace] = ctx
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
			ValidateApplications(scheduledAppContexts)
			Step("Create cluster pair between source and destination clusters", func() {
				var removeSpecs []interface{}
				err := ScheduleValidateClusterPair(appContexts[0], true, true, defaultClusterPairDir, false)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating cluster pair for app namespace [%s]", appContexts[0].App.NameSpace))
				// ClusterPair comes under stork.libopenstorage.org which is a skipCRD for backup,
				//skipCrds :=
				//	"autopilot.libopenstorage.org":           "",
				//	"core.libopenstorage.org":                "",
				//	"volumesnapshot.external-storage.k8s.io": "",
				//	"stork.libopenstorage.org":               "",
				//	"kdmp.portworx.com":                      "",
				//
				// Hence excluding it from spec list to avoid further validation.
				for _, spec := range appContexts[0].App.SpecList {
					if clusterPairSpecObj, ok := spec.(*storkapi.ClusterPair); ok {
						spec, _ = k8s.GetUpdatedSpec(clusterPairSpecObj)
						removeSpecs = append(removeSpecs, spec)
					}
				}
				err = Inst().S.RemoveAppSpecsByName(appContexts[0], removeSpecs)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Removing cluster pair spec from App context"))
			})
		}

	})
	It("backup and restore of applications when PX sync DR configured on application namespace", func() {

		Step("Create Schedule Policy for Migration", func() {
			log.InfoD("Create Schedule Policy for Migration")
			MigrationInterval := 5
			schdPol, err := asyncdr.CreateSchedulePolicy(syncSchPolicyName, MigrationInterval)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating sync schedule policy [%s]", syncSchPolicyName))
			log.InfoD("schedule Policy [%v ]created with %v mins of interval", schdPol.Name, MigrationInterval)
		})

		Step("Create Migration Schedule for Application Namespaces", func() {
			log.InfoD("Create Migration Schedule for Application Namespaces")
			for i, currMigNamespace := range bkpNamespaces {
				migrationScheduleName := metromigrationKey + "schedule-" + fmt.Sprintf("%d", i)
				currMigSched, createMigSchedErr := asyncdr.CreateMigrationSchedule(
					migrationScheduleName, currMigNamespace, asyncdr.DefaultClusterPairName, currMigNamespace, &includeVolumesFlag,
					&includeResourcesFlag, &startApplicationsFlag, syncSchPolicyName, &suspendSched, autoSuspend,
					nil, nil, nil, nil, nil, "", "", nil, nil, nil)
				dash.VerifyFatal(createMigSchedErr, nil, fmt.Sprintf("creation of migrationschedule [%s]", migrationScheduleName))
				time.Sleep(30 * time.Second)
				migSchedResp, err := storkops.Instance().GetMigrationSchedule(currMigSched.Name, currMigNamespace)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Get migrationschedule [%s]", migrationScheduleName))
				_, err = storkops.Instance().ValidateMigrationSchedule(migSchedResp.Name, currMigNamespace, migrationRetryTimeout, migrationRetryInterval)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validation of migrationschedule [%s]", migrationScheduleName))
				migrationNamespaceMap[migrationScheduleName] = currMigNamespace
			}
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
		Step("Registering DC and DR clusters for backup", func() {
			log.InfoD("Registering DC and DR clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Adding DC and DR clusters")
			clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			srcClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			destClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, DestinationClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", DestinationClusterName))
		})

		Step(fmt.Sprintf("Verify creation of pre and post exec rules for applications "), func() {
			log.InfoD("Verify creation of pre and post exec rules for applications ")
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

		Step(fmt.Sprintf("Create schedule policy for backup schedules"), func() {
			log.InfoD("Create schedule policy for backup schedules")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			periodicSchedulePolicyName = fmt.Sprintf("%s-%s", "periodic", RandomString(5))
			periodicSchedulePolicyUid = uuid.New()
			periodicSchedulePolicyInterval := int64(15)
			err = CreateBackupScheduleIntervalPolicy(5, periodicSchedulePolicyInterval, 5, periodicSchedulePolicyName, periodicSchedulePolicyUid, BackupOrgID, ctx, false, false)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval [%v] minutes named [%s] ", periodicSchedulePolicyInterval, periodicSchedulePolicyName))

		})

		Step("Taking manual backup of namespaces with rules from the DC site", func() {
			log.InfoD(fmt.Sprintf("Taking manual backup of namespaces with rules from the DC site"))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName = fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, BackupOrgID, srcClusterUid, preRuleName, preRuleUid, postRuleName, postRuleUid)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
				backupNames = append(backupNames, backupName)
				backupNamespaceMap[backupName] = namespace
				dcBackupNames = append(dcBackupNames, backupName)
			}
		})

		Step("Taking manual backup of namespaces with rules from the DR site", func() {
			log.InfoD(fmt.Sprintf("Taking manual backup of namespaces with rules from the DR site"))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName = fmt.Sprintf("%s-%v", BackupNamePrefix, time.Now().Unix())
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, DestinationClusterName, bkpLocationName, backupLocationUID, appContextsToBackup, labelSelectors, BackupOrgID, destClusterUid, preRuleName, preRuleUid, postRuleName, postRuleUid)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
				backupNames = append(backupNames, backupName)
				drBackupNames = append(drBackupNames, backupName)
				backupNamespaceMap[backupName] = namespace
			}
		})

		Step("Taking schedule backup of namespaces with rules from the DC site", func() {
			log.InfoD(fmt.Sprintf("Taking schedule backup of namespaces with rules from the DC site"))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				scheduleName := fmt.Sprintf("%s-dc-schedule-with-rules-%s", BackupNamePrefix, RandomString(4))
				log.InfoD("Creating a schedule backup of namespace [%s] without pre and post exec rules", namespace)
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				scheduleBackupName, err := CreateScheduleBackupWithValidation(ctx, scheduleName, SourceClusterName, bkpLocationName, backupLocationUID, appContextsToBackup,
					labelSelectors, BackupOrgID, "", "", "", "", periodicSchedulePolicyName, periodicSchedulePolicyUid)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup [%s]", scheduleBackupName))
				err = SuspendBackupSchedule(scheduleName, periodicSchedulePolicyName, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule [%s] ", scheduleName))
				backupNames = append(backupNames, scheduleBackupName)
				scheduleNames = append(scheduleNames, scheduleName)
				backupNamespaceMap[scheduleBackupName] = namespace
				dcBackupNames = append(dcBackupNames, scheduleBackupName)
			}
		})

		Step("Taking schedule backup of namespaces with rules from the DR site", func() {
			log.InfoD(fmt.Sprintf("Taking schedule backup of namespaces with rules from the DR site"))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				scheduleName := fmt.Sprintf("%s-dr-schedule-with-rules-%s", BackupNamePrefix, RandomString(4))
				log.InfoD("Creating a schedule backup of namespace [%s] without pre and post exec rules", namespace)
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				scheduleBackupName, err := CreateScheduleBackupWithValidation(ctx, scheduleName, DestinationClusterName, bkpLocationName, backupLocationUID, appContextsToBackup,
					labelSelectors, BackupOrgID, "", "", "", "", periodicSchedulePolicyName, periodicSchedulePolicyUid)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup [%s]", scheduleBackupName))
				err = SuspendBackupSchedule(scheduleName, periodicSchedulePolicyName, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule [%s] ", scheduleName))
				backupNames = append(backupNames, scheduleBackupName)
				drBackupNames = append(drBackupNames, backupName)
				scheduleNames = append(scheduleNames, scheduleName)
				backupNamespaceMap[scheduleBackupName] = namespace
			}
		})

		Step("Taking restore of backups from DR to DC site", func() {
			log.InfoD("Taking restore of backups from DR to DC site")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoredNamespaces := make([]string, 0)
			for _, backupName := range drBackupNames {
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{backupNamespaceMap[backupName]})
				restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
				restoreNamespace := "custom-" + backupNamespaceMap[backupName]
				namespaceMapping := map[string]string{backupNamespaceMap[backupName]: restoreNamespace}
				restoredNamespaces = append(restoredNamespaces, restoreNamespace)
				startTime := time.Now()
				err = CreateRestore(restoreName, backupName, namespaceMapping, SourceClusterName, BackupOrgID, ctx, make(map[string]string))
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
				restoreNames = append(restoreNames, restoreName)
				// while restoring from DR to DC site, the application wont be in runnig state. Scale the replicas to activate the app.
				err = ScaleApplicationToDesiredReplicas(restoreNamespace)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Scaling the deployment to desired replicas "))
				expectedRestoredAppContexts := make([]*scheduler.Context, 0)
				for _, scheduledAppContext := range appContextsToBackup {
					expectedRestoredAppContext, _ := CloneAppContextAndTransformWithMappings(scheduledAppContext, namespaceMapping, make(map[string]string), true)
					expectedRestoredAppContexts = append(expectedRestoredAppContexts, expectedRestoredAppContext)
				}
				err = ValidateRestore(ctx, restoreName, BackupOrgID, expectedRestoredAppContexts, make([]string, 0))
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validating restore [%s]after scaling the pods", restoreName))
				log.Infof("Namespace mapping from CreateRestoreWithValidation [%v]", namespaceMapping)
				err = ValidateDataAfterRestore(expectedRestoredAppContexts, restoreName, ctx, backupName, namespaceMapping, startTime)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validating restoredata [%s]after scaling the pods", restoreName))
			}
		})

		Step("Taking restore of backups from DC to DC site", func() {
			log.InfoD("Taking restore of backups from DC to DC site")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range dcBackupNames {
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{backupNamespaceMap[backupName]})
				restoreName := fmt.Sprintf("%s-%v", RestoreNamePrefix, time.Now().Unix())
				restoreNamespace := "custom-" + backupNamespaceMap[backupName]
				namespaceMapping := map[string]string{backupNamespaceMap[backupName]: restoreNamespace}
				err = CreateRestoreWithValidation(ctx, restoreName, backupName, namespaceMapping, make(map[string]string), SourceClusterName, BackupOrgID, appContextsToBackup)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
			}
		})

	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		backupNames, err := GetAllBackupsAdmin()
		dash.VerifySafely(err, nil, fmt.Sprintf("Fetching all backups for admin"))
		for _, backupName := range backupNames {
			wg.Add(1)
			go func(backupName string) {
				defer GinkgoRecover()
				defer wg.Done()
				backupUid, err := Inst().Backup.GetBackupUID(ctx, backupName, BackupOrgID)
				_, err = DeleteBackup(backupName, backupUid, BackupOrgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Delete the backup %s ", backupName))
				err = DeleteBackupAndWait(backupName, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("waiting for backup [%s] deletion", backupName))
			}(backupName)
		}
		wg.Wait()

		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
		}
		for _, scheduleName := range scheduleNames {
			err = DeleteSchedule(scheduleName, SourceClusterName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting schedule [%s]", scheduleName))
		}
		for migrationName, migrationNamespace := range migrationNamespaceMap {
			asyncdr.DeleteAndWaitForMigrationDeletion(migrationName, migrationNamespace)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deletion of migration schedule[%s]", migrationName))
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})
