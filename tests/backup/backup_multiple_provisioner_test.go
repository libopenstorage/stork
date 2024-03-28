package tests

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

// This MultipleProvisionerCsiSnapshotDeleteBackupAndRestore testcase to test restore of namespaces with multiple provisioners using backup when snapshot is deleted
var _ = Describe("{MultipleProvisionerCsiSnapshotDeleteBackupAndRestore}", Label(TestCaseLabelsMap[MultipleProvisionerCsiSnapshotDeleteBackupAndRestore]...), func() {

	var (
		restoreNames                              []string
		scheduledAppContexts                      []*scheduler.Context
		cloudCredName                             string
		cloudCredUID                              string
		backupLocationUID                         string
		backupLocationName                        string
		backupLocationMap                         map[string]string
		providers                                 []string
		schedulePolicyName                        string
		schedulePolicyUID                         string
		scheduleUid                               string
		srcClusterUid                             string
		schedulePolicyInterval                    = int64(15)
		allAppContext                             []*scheduler.Context
		scheduleList                              []string
		randomStringLength                        = 10
		scheduledAppContextsForMultipleAppSinleNs []*scheduler.Context
		multipleProvisionerSameNsScheduleName     string
		multipleNsSchBackupName                   string
		appSpecList                               []string
		preRuleName                               string
		postRuleName                              string
		preRuleUid                                string
		postRuleUid                               string
		clusterCredentials                        string
		clusterProviderName                       = GetClusterProvider()
		provisionerDefaultSnapshotClassMap        = GetProvisionerDefaultSnapshotMap(clusterProviderName)
	)

	JustBeforeEach(func() {
		if GetClusterProvider() != "ibm" {
			// This test is meant to run only on IBM later will enable on other configs
			log.Infof("Skipping the test.This test is currently configured to run on IBM environments. Future iterations will enable on other configs")
			Skip("Skipping the test.This test is currently configured to run on IBM environments. Future iterations will enable on other configs")
		}
		StartPxBackupTorpedoTest("MultipleProvisionerCsiSnapshotDeleteBackupAndRestore", "Delete Csi snapshot and restore namespaces from backup", nil, 296725, Sn, Q4FY24)

		backupLocationMap = make(map[string]string)
		providers = GetBackupProviders()

		// Deploy multiple application in a single namespace using different provisioner
		taskName := fmt.Sprintf("%s-%s", TaskNamePrefix, RandomString(randomStringLength))
		for provisioner, _ := range provisionerDefaultSnapshotClassMap {
			appSpecList, err := GetApplicationSpecForProvisioner(clusterProviderName, provisioner)
			log.FailOnError(err, fmt.Sprintf("Fetching application spec for provisioner %s", provisioner))
			for _, appSpec := range appSpecList {
				appContexts := ScheduleApplicationsWithScheduleOptions(taskName, appSpec, provisioner)
				appContexts[0].ReadinessTimeout = AppReadinessTimeout
				scheduledAppContextsForMultipleAppSinleNs = append(scheduledAppContextsForMultipleAppSinleNs, appContexts...)
				allAppContext = append(allAppContext, appContexts...)
			}
		}
	})

	It("Backup and restore of namespaces with multiple provisioners using csi and kdmp", func() {
		Step("Validate deployed applications", func() {
			ValidateApplications(allAppContext)
		})
		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				backupLocationName = fmt.Sprintf("%s-%s-bl-%v", provider, getGlobalBucketName(provider), time.Now().Unix())
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, BackupOrgID, provider))
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, "Creating backup location")
			}
		})
		Step("Registering cluster for backup", func() {
			log.InfoD("Registering cluster for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")

			clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))

			clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, DestinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", DestinationClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", DestinationClusterName))
		})
		Step("Create schedule policy", func() {
			log.InfoD("Creating schedule policy")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			schedulePolicyName = fmt.Sprintf("%s-%v", "periodic-schedule-policy", RandomString(randomStringLength))
			schedulePolicyUID = uuid.New()
			err = CreateBackupScheduleIntervalPolicy(5, schedulePolicyInterval, 5, schedulePolicyName, schedulePolicyUID, BackupOrgID, ctx, false, false)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule policy %s", schedulePolicyName))
		})
		Step(fmt.Sprintf("Create pre and post exec rules for applications from px-admin"), func() {
			log.InfoD("Create pre and post exec rules for applications from px-admin")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-admin ctx")
			preRuleName, postRuleName, err = CreateRuleForBackupWithMultipleApplications(BackupOrgID, appSpecList, ctx)
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
		Step(fmt.Sprintf("Creating schedule backup for application deployed using multiple provisioner with default volume snapshot class"), func() {
			log.InfoD("Creating schedule backup for multiple provisioner with default volume snapshot class")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			if len(provisionerDefaultSnapshotClassMap) > 0 {
				// Modify all provisioner to select default volume snapshot class
				provisionerSelectDefaultVolumeSnapshotClass := make(map[string]string)
				for key := range provisionerDefaultSnapshotClassMap {
					provisionerSelectDefaultVolumeSnapshotClass[key] = "Default"
				}
				multipleProvisionerSameNsScheduleName = fmt.Sprintf("multiple-provisioner-same-namespace-schedule-%v", RandomString(randomStringLength))
				multipleNsSchBackupName, err = CreateScheduleBackupWithValidationWithVscMapping(ctx, multipleProvisionerSameNsScheduleName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContextsForMultipleAppSinleNs, make(map[string]string), BackupOrgID, preRuleName, preRuleUid, postRuleName, postRuleUid, schedulePolicyName, schedulePolicyUID, provisionerSelectDefaultVolumeSnapshotClass, false)
				scheduleList = append(scheduleList, multipleProvisionerSameNsScheduleName)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of scheduled backup with schedule name [%s] for backup location %s", multipleNsSchBackupName, backupLocationName))
				err = IsFullBackup(multipleNsSchBackupName, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the first schedule backup [%s] for backup location %s is a full backup", multipleNsSchBackupName, backupLocationName))
				backupUID, err := Inst().Backup.GetBackupUID(ctx, multipleNsSchBackupName, BackupOrgID)
				log.FailOnError(err, fmt.Sprintf("Getting UID for backup %v", multipleNsSchBackupName))
				backupInspectRequest := &api.BackupInspectRequest{
					Name:  multipleNsSchBackupName,
					Uid:   backupUID,
					OrgId: BackupOrgID,
				}
				resp, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
				log.FailOnError(err, fmt.Sprintf("Inspect backup %v", multipleNsSchBackupName))
				volumeObjlist := resp.Backup.Volumes
				var volumeNames []string
				for _, obj := range volumeObjlist {
					volumeNames = append(volumeNames, obj.Name)
				}
				log.InfoD("Deleting the snapshot present in the volumes which are backed up %s", volumeNames)
				clusterCredentials, err = GetIBMApiKey("default")
				err = Inst().V.DeleteSnapshotsForVolumes(volumeNames, clusterCredentials)
				log.FailOnError(err, fmt.Sprintf("Deleteing snapshot failed for volumes %v", volumeNames))
			} else {
				log.InfoD("Skipping this step as provisioner with default volume snapshot class is not found")
			}
		})
		Step("Restoring the backup taken on singe namespace with multiple application deployed with different provisioner after deleting csi snapshot", func() {
			if multipleNsSchBackupName != "" {
				log.InfoD("Restoring the backup taken on singe namespace with multiple application deployed with different provisioner after deleting csi snapshot")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				namespaceMappingMultiApp := make(map[string]string)
				for _, appCtx := range scheduledAppContextsForMultipleAppSinleNs {
					namespaceMappingMultiApp[appCtx.ScheduleOptions.Namespace] = appCtx.ScheduleOptions.Namespace + "-mul-app-snigle-ns" + RandomString(randomStringLength)
				}
				restoreName := fmt.Sprintf("%s-%s-%s", "test-restore", "multi-app-single-ns", RandomString(randomStringLength))
				log.InfoD("Restoring namespaces from the [%s] backup", multipleNsSchBackupName)
				err = CreateRestoreWithValidation(ctx, restoreName, multipleNsSchBackupName, namespaceMappingMultiApp, make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContextsForMultipleAppSinleNs)
				restoreNames = append(restoreNames, restoreName)
			}
		})
	})
	JustAfterEach(func() {
		if GetClusterProvider() != "ibm" {
			// This test is meant to run only on IBM later will enable on other configs
			log.Infof("Skipping the test.This test is currently configured to run on IBM environments. Future iterations will enable on other configs")
			Skip("Skipping the test.This test is currently configured to run on IBM environments. Future iterations will enable on other configs")
		}
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		defer func() {
			log.InfoD("switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "failed to SetClusterContext to default cluster")
		}()

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true

		log.InfoD("Deleting the deployed apps after the testcase")
		DestroyApps(allAppContext, opts)

		scheduleUid, err = Inst().Backup.GetBackupScheduleUID(ctx, multipleProvisionerSameNsScheduleName, BackupOrgID)
		err = DeleteScheduleWithUIDAndWait(multipleProvisionerSameNsScheduleName, scheduleUid, SourceClusterName, srcClusterUid, BackupOrgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting schedule [%s]", multipleProvisionerSameNsScheduleName))

		// Delete restores
		log.Info("Delete restores")
		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})
