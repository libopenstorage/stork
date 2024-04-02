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

// This MultipleProvisionerCsiKdmpBackupAndRestore testcase to test Backup and restore of namespaces with multiple provisioners using csi with volumeSnapshotClassMapping and force kdmp
var _ = Describe("{MultipleProvisionerCsiKdmpBackupAndRestore}", Label(TestCaseLabelsMap[MultipleProvisionerCsiKdmpBackupAndRestore]...), func() {
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/296724
	var (
		backupNames                               []string
		cloudCredName                             string
		cloudCredUID                              string
		backupLocationUID                         string
		backupLocationName                        string
		backupLocationMap                         map[string]string
		labelSelectors                            map[string]string
		providers                                 []string
		schedulePolicyName                        string
		schedulePolicyUID                         string
		srcClusterUid                             string
		schedulePolicyInterval                    = int64(15)
		scheduledAppContextsForMultipleAppSinleNs []*scheduler.Context
		scheduledAppContextsForDefaultVscBackup   []*scheduler.Context
		scheduledAppContextsForCustomVscBackup    []*scheduler.Context
		allAppContext                             []*scheduler.Context
		appSpecListDefaultVsc                     []string
		appSpecListCustomVsc                      []string
		defaultSchBackupName                      string
		scheduleList                              []string
		defaultProvisionerScheduleName            string
		nonDefaultVscSchBackupName                string
		randomStringLength                        = 10
		forceKdmpSchBackupName                    string
		kdmpScheduleName                          string
		multipleProvisionerSameNsScheduleName     string
		multipleNsSchBackupName                   string
		preRuleNameMultiProvisioner               string
		postRuleNameMultiProvisioner              string
		preRuleUidMultiProvisioner                string
		postRuleUidMultiProvisioner               string
		appSpecListMultiProvisioner               []string
		preRuleNameDefaultVsc                     string
		postRuleNameDefaultVsc                    string
		preRuleUidDefaultVsc                      string
		postRuleUidDefaultVsc                     string
		preRuleNameCustomVsc                      string
		postRuleNameCustomVsc                     string
		preRuleUidCustomVsc                       string
		postRuleUidCustomVsc                      string
		scheduleNames                             []string
		clusterProviderName                       = GetClusterProvider()
		provisionerDefaultSnapshotClassMap        = GetProvisionerDefaultSnapshotMap(clusterProviderName)
		provisionerSnapshotClassMap               = GetProvisionerSnapshotClassesMap(clusterProviderName)
		runOnClusterProviders                     = []string{"ibm", "openshift"}
	)

	JustBeforeEach(func() {
		if !Contains(runOnClusterProviders, GetClusterProvider()) {
			// This test is meant to run only on IBM and OpenShift; later, it will be enabled on other configurations.
			log.Infof("Skipping the test.This test is currently configured to run on IBM and openshift and environments. Future iterations will enable on other configs")
			Skip("Skipping the test.This test is currently configured to run on IBM and openshift environments. Future iterations will enable on other configs")
		}
		StartPxBackupTorpedoTest("MultipleProvisionerCsiKdmpBackupAndRestore", "Backup and restore of namespaces with multiple provisioners using csi with volumeSnapshotClassMapping and force kdmp", nil, 296724, Sn, Q1FY25)
		backupLocationMap = make(map[string]string)
		labelSelectors = make(map[string]string)
		providers = GetBackupProviders()
		// Deploy multiple application in a single namespace using different provisioner
		taskName := fmt.Sprintf("%s-%s", TaskNamePrefix, RandomString(randomStringLength))
		for provisioner, _ := range provisionerDefaultSnapshotClassMap {
			var err error
			appSpecListMultiProvisioner, err = GetApplicationSpecForProvisioner(clusterProviderName, provisioner)
			log.FailOnError(err, fmt.Sprintf("Fetching application spec for provisioner %s", provisioner))
			for _, appSpec := range appSpecListMultiProvisioner {
				appContexts := ScheduleApplicationsWithScheduleOptions(taskName, appSpec, provisioner)
				appContexts[0].ReadinessTimeout = AppReadinessTimeout
				scheduledAppContextsForMultipleAppSinleNs = append(scheduledAppContextsForMultipleAppSinleNs, appContexts...)
				allAppContext = append(allAppContext, appContexts...)
			}
		}
		// Deploy multiple application in multiple namespace for default backup
		for provisioner, _ := range provisionerDefaultSnapshotClassMap {
			var err error
			appSpecListDefaultVsc, err = GetApplicationSpecForProvisioner(clusterProviderName, provisioner)
			log.FailOnError(err, fmt.Sprintf("Fetching application spec for provisioner %s", provisioner))
			for _, appSpec := range appSpecListDefaultVsc {
				taskName := fmt.Sprintf("%s-%s", TaskNamePrefix, RandomString(randomStringLength))
				appCtx := ScheduleApplicationsWithScheduleOptions(taskName, appSpec, provisioner)
				appCtx[0].ReadinessTimeout = AppReadinessTimeout
				scheduledAppContextsForDefaultVscBackup = append(scheduledAppContextsForDefaultVscBackup, appCtx...)
				allAppContext = append(allAppContext, appCtx...)
			}
		}
		// Deploy multiple application in multiple namespace for custom backup
		for provisioner, _ := range provisionerSnapshotClassMap {
			var err error
			appSpecListCustomVsc, err = GetApplicationSpecForProvisioner(clusterProviderName, provisioner)
			log.FailOnError(err, fmt.Sprintf("Fetching application spec for provisioner %s", provisioner))
			for _, appSpec := range appSpecListCustomVsc {
				taskName := fmt.Sprintf("%s-%s", TaskNamePrefix, RandomString(randomStringLength))
				appCtx := ScheduleApplicationsWithScheduleOptions(taskName, appSpec, provisioner)
				appCtx[0].ReadinessTimeout = AppReadinessTimeout
				scheduledAppContextsForCustomVscBackup = append(scheduledAppContextsForCustomVscBackup, appCtx...)
				allAppContext = append(allAppContext, appCtx...)
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
		Step("Create source and destination clusters", func() {
			log.InfoD("Creating source and destination clusters")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.Infof("Creating source [%s] and destination [%s] clusters", SourceClusterName, DestinationClusterName)
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, DestinationClusterName))

			clusters := []string{SourceClusterName, DestinationClusterName}
			for _, c := range clusters {
				clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, c, ctx)
				log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", c))
				dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", c))
			}
			srcClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			log.Infof("Cluster [%s] uid: [%s]", SourceClusterName, srcClusterUid)
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
		Step(fmt.Sprintf("Create pre and post exec rules for applications for multiple provisioner same namespace"), func() {
			log.InfoD("Create pre and post exec rules for applications for multiple provisioner same namespace")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-admin ctx")
			preRuleNameMultiProvisioner, postRuleNameMultiProvisioner, err = CreateRuleForBackupWithMultipleApplications(BackupOrgID, appSpecListMultiProvisioner, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of pre and post exec rules for applications for multiple provisioner same namespace"))
			if preRuleNameMultiProvisioner != "" {
				preRuleUidMultiProvisioner, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleNameMultiProvisioner)
				log.FailOnError(err, "Fetching pre backup rule [%s] uid", preRuleNameMultiProvisioner)
				log.Infof("Pre backup rule [%s] uid: [%s]", preRuleNameMultiProvisioner, preRuleUidMultiProvisioner)
			}
			if postRuleNameMultiProvisioner != "" {
				postRuleUidMultiProvisioner, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleNameMultiProvisioner)
				log.FailOnError(err, "Fetching post backup rule [%s] uid", postRuleNameMultiProvisioner)
				log.Infof("Post backup rule [%s] uid: [%s]", postRuleNameMultiProvisioner, postRuleUidMultiProvisioner)
			}
		})
		Step(fmt.Sprintf("Create pre and post exec rules for applications with default vsc"), func() {
			log.InfoD("Create pre and post exec rules for applications with default vsc")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-admin ctx")
			preRuleNameDefaultVsc, postRuleNameDefaultVsc, err = CreateRuleForBackupWithMultipleApplications(BackupOrgID, appSpecListDefaultVsc, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of pre and post exec rules for applications from px-admin"))
			if preRuleNameDefaultVsc != "" {
				preRuleUidDefaultVsc, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleNameDefaultVsc)
				log.FailOnError(err, "Fetching pre backup rule [%s] uid", preRuleNameDefaultVsc)
				log.Infof("Pre backup rule [%s] uid: [%s]", preRuleNameDefaultVsc, preRuleUidDefaultVsc)
			}
			if postRuleNameDefaultVsc != "" {
				postRuleUidDefaultVsc, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleNameDefaultVsc)
				log.FailOnError(err, "Fetching post backup rule [%s] uid", postRuleUidDefaultVsc)
				log.Infof("Post backup rule [%s] uid: [%s]", postRuleUidDefaultVsc, postRuleUidDefaultVsc)
			}
		})
		Step(fmt.Sprintf("Create pre and post exec rules for applications with custom vsc"), func() {
			log.InfoD("Create pre and post exec rules for applications with custom vsc")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-admin ctx")
			preRuleNameCustomVsc, postRuleNameCustomVsc, err = CreateRuleForBackupWithMultipleApplications(BackupOrgID, appSpecListCustomVsc, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of pre and post exec rules for applications from px-admin"))
			if preRuleNameCustomVsc != "" {
				preRuleUidCustomVsc, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleNameCustomVsc)
				log.FailOnError(err, "Fetching pre backup rule [%s] uid", preRuleNameCustomVsc)
				log.Infof("Pre backup rule [%s] uid: [%s]", preRuleNameCustomVsc, preRuleUidCustomVsc)
			}
			if postRuleNameCustomVsc != "" {
				postRuleUidCustomVsc, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleNameCustomVsc)
				log.FailOnError(err, "Fetching post backup rule [%s] uid", postRuleUidCustomVsc)
				log.Infof("Post backup rule [%s] uid: [%s]", postRuleUidCustomVsc, postRuleUidCustomVsc)
			}
		})
		Step(fmt.Sprintf("Creating schedule backup for application deployed using multiple provisioner with default volume snapshot class"), func() {
			log.InfoD("Creating schedule backup for multiple provisioner with default volume snapshot class")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			if len(provisionerDefaultSnapshotClassMap) > 0 {
				// Modify all provisioner to select default volume snapshot class
				provisionerDefaultVolumeSnapshotClass := make(map[string]string)
				for key := range provisionerDefaultSnapshotClassMap {
					provisionerDefaultVolumeSnapshotClass[key] = "default"
				}
				multipleProvisionerSameNsScheduleName = fmt.Sprintf("multiple-provisioner-same-namespace-schedule-%v", RandomString(randomStringLength))
				multipleNsSchBackupName, err = CreateScheduleBackupWithValidationWithVscMapping(ctx, multipleProvisionerSameNsScheduleName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContextsForMultipleAppSinleNs, make(map[string]string), BackupOrgID, preRuleNameMultiProvisioner, preRuleUidMultiProvisioner, postRuleNameMultiProvisioner, postRuleUidMultiProvisioner, schedulePolicyName, schedulePolicyUID, provisionerDefaultVolumeSnapshotClass, false)
				scheduleList = append(scheduleList, multipleNsSchBackupName)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of scheduled backup with schedule name [%s] for backup location %s", multipleNsSchBackupName, backupLocationName))
				err = IsFullBackup(multipleNsSchBackupName, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the first schedule backup [%s] for backup location %s is a full backup", multipleNsSchBackupName, backupLocationName))
				err = SuspendBackupSchedule(multipleProvisionerSameNsScheduleName, schedulePolicyName, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of suspending backup schedule [%s]", multipleProvisionerSameNsScheduleName))
				scheduleNames = append(scheduleNames, multipleProvisionerSameNsScheduleName)
			} else {
				log.InfoD("Skipping this step as provisioner with default volume snapshot class is not found")
			}
		})
		Step(fmt.Sprintf("Creating schedule backup for each namespace having application deployed with different provisioner which has default volume snapshot class"), func() {
			log.InfoD("Creating schedule backup for multiple provisioner which has default volume snapshot class")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			if len(provisionerDefaultSnapshotClassMap) > 0 {
				// Modify all provisioner to select default volume snapshot class
				provisionerSelectDefaultVolumeSnapshotClass := make(map[string]string)
				for key := range provisionerDefaultSnapshotClassMap {
					provisionerSelectDefaultVolumeSnapshotClass[key] = "default"
				}
				defaultProvisionerScheduleName = fmt.Sprintf("default-provisioner-schedule-%v", RandomString(randomStringLength))
				defaultSchBackupName, err = CreateScheduleBackupWithValidationWithVscMapping(ctx, defaultProvisionerScheduleName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContextsForDefaultVscBackup, make(map[string]string), BackupOrgID, preRuleNameDefaultVsc, preRuleUidDefaultVsc, postRuleNameDefaultVsc, postRuleUidDefaultVsc, schedulePolicyName, schedulePolicyUID, provisionerSelectDefaultVolumeSnapshotClass, false)
				scheduleList = append(scheduleList, defaultSchBackupName)

				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of scheduled backup with schedule name [%s] for backup location %s", defaultSchBackupName, backupLocationName))
				err = IsFullBackup(defaultSchBackupName, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the first schedule backup [%s] for backup location %s is a full backup", defaultSchBackupName, backupLocationName))
				err = SuspendBackupSchedule(defaultProvisionerScheduleName, schedulePolicyName, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of suspending backup schedule [%s]", defaultProvisionerScheduleName))
				scheduleNames = append(scheduleNames, defaultProvisionerScheduleName)
			} else {
				log.InfoD("Skipping this step as provisioner with default volume snapshot class is not found")
			}
		})
		Step(fmt.Sprintf("Creating schedule backup for multiple provisioner with non default volume snapshot class"), func() {
			log.InfoD("Creating schedule backup for multiple provisioner with non default volume snapshot class")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			if len(provisionerSnapshotClassMap) > 0 {
				nonDefaultProvisionerScheduleName := fmt.Sprintf("default-provisioner-schedule-%v", RandomString(randomStringLength))
				nonDefaultVscSchBackupName, err = CreateScheduleBackupWithValidationWithVscMapping(ctx, nonDefaultProvisionerScheduleName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContextsForDefaultVscBackup, make(map[string]string), BackupOrgID, preRuleNameCustomVsc, preRuleUidCustomVsc, postRuleNameCustomVsc, postRuleUidCustomVsc, schedulePolicyName, schedulePolicyUID, provisionerSnapshotClassMap, false)
				scheduleList = append(scheduleList, nonDefaultProvisionerScheduleName)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of scheduled backup with schedule name [%s] for backup location %s", nonDefaultVscSchBackupName, backupLocationName))
				err = IsFullBackup(nonDefaultVscSchBackupName, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the first schedule backup [%s] for backup location %s is a full backup", nonDefaultVscSchBackupName, backupLocationName))
				err = SuspendBackupSchedule(nonDefaultProvisionerScheduleName, schedulePolicyName, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of suspending backup schedule [%s]", nonDefaultProvisionerScheduleName))
				scheduleNames = append(scheduleNames, nonDefaultProvisionerScheduleName)
			} else {
				log.InfoD("Skipping this step as provisioner with non-default volumeSnapshotClass is not found")
			}
		})
		Step(fmt.Sprintf("Creating schedule backup for multiple provisioner with forced kdmp option"), func() {
			log.InfoD("Creating schedule backup for multiple provisioner with forced kdmp option")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			provisionerNonDefaultSnapshotClassMap := map[string]string{}
			kdmpScheduleName = fmt.Sprintf("default-provisioner-schedule-%v", RandomString(randomStringLength))
			forceKdmpSchBackupName, err = CreateScheduleBackupWithValidationWithVscMapping(ctx, kdmpScheduleName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContextsForDefaultVscBackup, make(map[string]string), BackupOrgID, preRuleNameDefaultVsc, preRuleUidDefaultVsc, postRuleNameDefaultVsc, postRuleUidDefaultVsc, schedulePolicyName, schedulePolicyUID, provisionerNonDefaultSnapshotClassMap, true)
			scheduleList = append(scheduleList, kdmpScheduleName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of scheduled backup with schedule name [%s] for backup location %s", nonDefaultVscSchBackupName, backupLocationName))
			err = IsFullBackup(forceKdmpSchBackupName, BackupOrgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the name of the next schedule backup for schedule: [%s] for backup location %s", nonDefaultVscSchBackupName, backupLocationName))
			err = SuspendBackupSchedule(kdmpScheduleName, schedulePolicyName, BackupOrgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of suspending backup schedule [%s]", kdmpScheduleName))
			scheduleNames = append(scheduleNames, kdmpScheduleName)
		})
		Step("Taking manual backup of application from source cluster", func() {
			log.InfoD("taking manual backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			backupNames = make([]string, 0)
			if len(provisionerDefaultSnapshotClassMap) > 0 {
				for i, appCtx := range scheduledAppContextsForCustomVscBackup {
					scheduledNamespace := appCtx.ScheduleOptions.Namespace
					backupName := fmt.Sprintf("%s-%s-%v", "autogenerated-backup", scheduledNamespace, time.Now().Unix())
					provisionerVolumeSnapshotClassSubMap := make(map[string]string)
					if value, ok := provisionerDefaultSnapshotClassMap[appCtx.ScheduleOptions.StorageProvisioner]; ok {
						provisionerVolumeSnapshotClassSubMap[appCtx.ScheduleOptions.StorageProvisioner] = value
					}
					log.InfoD("creating backup [%s] in source cluster [%s] (%s), organization [%s], of namespace [%s], in backup location [%s]", backupName, SourceClusterName, srcClusterUid, BackupOrgID, scheduledNamespace, backupLocationName)
					err = CreateBackupWithValidationWithVscMapping(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContextsForCustomVscBackup[i:i+1], labelSelectors, BackupOrgID, srcClusterUid, preRuleNameDefaultVsc, preRuleUidDefaultVsc, postRuleNameDefaultVsc, postRuleUidDefaultVsc, provisionerVolumeSnapshotClassSubMap, false)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
					backupNames = append(backupNames, backupName)
				}
			} else {
				log.InfoD("Skipping this step as provisioner with default volumeSnapshotClass is not found")
			}
		})
		Step("Restoring the backup taken on singe namespace with multiple application deployed with different provisioner", func() {
			if multipleNsSchBackupName != "" {
				log.InfoD("Restoring the backup taken on singe namespace with multiple application deployed with different provisioner")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				namespaceMappingMultiApp := make(map[string]string)
				for _, appCtx := range scheduledAppContextsForMultipleAppSinleNs {
					namespaceMappingMultiApp[appCtx.ScheduleOptions.Namespace] = appCtx.ScheduleOptions.Namespace + "-mul-app-snigle-ns"
				}
				restoreName := fmt.Sprintf("%s-%s-%s", "test-restore", "multi-app-single-ns", RandomString(randomStringLength))
				log.InfoD("Restoring namespaces from the [%s] backup", multipleNsSchBackupName)
				err = CreateRestoreWithValidation(ctx, restoreName, multipleNsSchBackupName, namespaceMappingMultiApp, make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContextsForMultipleAppSinleNs)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s] from backup [%s]", restoreName, multipleNsSchBackupName))
			}
		})
		Step("Restoring the backup taken on multiple namespace with different provisioner", func() {
			if defaultSchBackupName != "" {
				log.InfoD("Restoring the backup taken on multiple namespace with different provisioner")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				namespaceMappingMultiNs := make(map[string]string)
				for _, appCtx := range scheduledAppContextsForDefaultVscBackup {
					namespaceMappingMultiNs[appCtx.ScheduleOptions.Namespace] = appCtx.ScheduleOptions.Namespace + "-mul-prov-mul-ns"
				}
				restoreName := fmt.Sprintf("%s-%s-%s", "test-restore", "multi-ns-different-provisioner", RandomString(randomStringLength))
				log.InfoD("Restoring namespaces from the [%s] backup", defaultSchBackupName)
				err = CreateRestoreWithValidation(ctx, restoreName, defaultSchBackupName, namespaceMappingMultiNs, make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContextsForDefaultVscBackup)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s] from backup [%s]", restoreName, defaultSchBackupName))
			}
		})
		Step("Restoring the backup taken on multiple provisioner with non-default volumeSnapshotClass", func() {
			if nonDefaultVscSchBackupName != "" {
				log.InfoD("Restoring the backup taken on multiple provisioner with non-default volumeSnapshotClass")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				namespaceMappingMultiNs := make(map[string]string)
				for _, appCtx := range scheduledAppContextsForDefaultVscBackup {
					namespaceMappingMultiNs[appCtx.ScheduleOptions.Namespace] = appCtx.ScheduleOptions.Namespace + "-mul-prov-mul-ns-non-def-vsc"
				}
				restoreName := fmt.Sprintf("%s-%s-%s", "test-restore", "mul-prov-mul-ns-non-default-vsc", RandomString(randomStringLength))
				log.InfoD("Restoring namespaces from the [%s] backup", nonDefaultVscSchBackupName)
				err = CreateRestoreWithValidation(ctx, restoreName, nonDefaultVscSchBackupName, namespaceMappingMultiNs, make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContextsForDefaultVscBackup)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s] from backup [%s]", restoreName, nonDefaultVscSchBackupName))
			}
		})
		Step("Restoring the backup taken on multiple provisioner with kdmp", func() {
			if forceKdmpSchBackupName != "" {
				log.InfoD("Restoring the multiple provisioner with default volumeSnapshotClass")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				namespaceMappingkdmp := make(map[string]string)
				for _, appCtx := range scheduledAppContextsForDefaultVscBackup {
					namespaceMappingkdmp[appCtx.ScheduleOptions.Namespace] = appCtx.ScheduleOptions.Namespace + "-mul-app-kdmp"
				}
				restoreName := fmt.Sprintf("%s-%s-%s", "test-restore", "kdmp", RandomString(randomStringLength))
				log.InfoD("Restoring namespaces from the [%s] backup", forceKdmpSchBackupName)
				err = CreateRestoreWithValidation(ctx, restoreName, forceKdmpSchBackupName, namespaceMappingkdmp, make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContextsForDefaultVscBackup)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s] from backup [%s]", restoreName, forceKdmpSchBackupName))
			}
		})
		Step("Restoring from the manual back backup", func() {
			if len(backupNames) > 0 {
				log.InfoD("Restoring from the manual back backup")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				for i, appCtx := range scheduledAppContextsForCustomVscBackup {
					scheduledNamespace := appCtx.ScheduleOptions.Namespace
					restoreName := fmt.Sprintf("%s-%s-%s", "test-restore-manual-backup", scheduledNamespace, RandomString(randomStringLength))
					log.InfoD("Restoring [%s] namespace from the [%s] backup", scheduledNamespace, backupNames[i])
					err = CreateRestoreWithValidation(ctx, restoreName, backupNames[i], make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContextsForCustomVscBackup[i:i+1])
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of restore [%s] from backup [%s]", restoreName, backupNames[i]))
				}
			}
		})
	})
	JustAfterEach(func() {
		if !Contains(runOnClusterProviders, GetClusterProvider()) {
			// This test is meant to run only on IBM and OpenShift; later, it will be enabled on other configurations.
			log.Infof("Skipping the test.This test is currently configured to run on IBM and openshift and environments. Future iterations will enable on other configs")
			Skip("Skipping the test.This test is currently configured to run on IBM and openshift environments. Future iterations will enable on other configs")
		}
		defer EndPxBackupTorpedoTest(allAppContext)
		defer func() {
			log.InfoD("switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "failed to SetClusterContext to default cluster")
		}()

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		for _, scheduleName := range scheduleNames {
			err = DeleteSchedule(scheduleName, SourceClusterName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		}

		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true

		log.InfoD("Deleting the deployed apps after the testcase")
		DestroyApps(allAppContext, opts)

		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})
