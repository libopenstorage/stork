package tests

import (
	"fmt"
	"github.com/portworx/sched-ops/k8s/core"
	corev1 "k8s.io/api/core/v1"
	"os"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	k8score "github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

// This testcase Verifies partial backup and restore when CSI volume backup are failed.
var _ = Describe("{BackupCSIVolumesWithPartialSuccess}", Label(TestCaseLabelsMap[BackupCSIVolumesWithPartialSuccess]...), func() {

	var (
		backupNames                     []string
		scheduleBackupNames             []string
		restoreNames                    []string
		scheduledAppContexts            []*scheduler.Context
		preRuleNameList                 []string
		postRuleNameList                []string
		sourceClusterUid                string
		cloudCredName                   string
		cloudCredUID                    string
		backupLocationUID               string
		backupLocationName              string
		backupLocationMap               = make(map[string]string)
		labelSelectors                  = make(map[string]string)
		provisionerInvalidVscMap        = make(map[string]string)
		schedulePolicyName              string
		schedulePolicyUID               string
		schedulePolicyInterval          = int64(15)
		invalidVolumeSnapShotClassNames []string
		providers                       []string
		failedVolumes                   []*corev1.PersistentVolumeClaim
		backedUpNamespaces              []string
		scheduleNames                   []string
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("BackupCSIVolumesWithPartialSuccess", "Verifies partial backup and restore when CSI volume backup are failed", nil, 299231, Ak, Q2FY25)
		providers = GetBackupProviders()
		numOfNamespace := 5
		log.InfoD("scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		Inst().AppList, _ = GetApplicationSpecForFeature("PartialBackup")
		for i := 0; i < numOfNamespace; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, appCtx := range appContexts {
				appCtx.ReadinessTimeout = AppReadinessTimeout
				scheduledAppContexts = append(scheduledAppContexts, appCtx)
				appNamespace := appCtx.ScheduleOptions.Namespace
				backedUpNamespaces = append(backedUpNamespaces, appNamespace)
			}
		}
	})

	It("Verifies partial backup and restore when CSI volume backup are failed", func() {
		defer func() {
			log.InfoD("switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "failed to SetClusterContext to default cluster")
		}()

		Step("Validating applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
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

		Step("Create schedule policy", func() {
			log.InfoD("Creating schedule policy")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			schedulePolicyName = fmt.Sprintf("%s-%v", "periodic-schedule-policy", RandomString(5))
			schedulePolicyUID = uuid.New()
			err = CreateBackupScheduleIntervalPolicy(5, schedulePolicyInterval, 5, schedulePolicyName, schedulePolicyUID, BackupOrgID, ctx, false, false)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule policy %s", schedulePolicyName))
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

			sourceClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))

			clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, DestinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", DestinationClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", DestinationClusterName))
		})

		Step("Create invalid volume snapshot class for CSI volumes", func() {
			pvcLabelSelector := make(map[string]string)
			pvcLabelSelector["backupVolumeType"] = "csi"
			k8sCore := k8score.Instance()
			for _, appCtx := range scheduledAppContexts {
				scheduledNamespace := appCtx.ScheduleOptions.Namespace
				pvcList, err := k8sCore.GetPersistentVolumeClaims(scheduledNamespace, pvcLabelSelector)
				log.FailOnError(err, "Getting PVC list with provisioner based label selector")
				for _, pvc := range pvcList.Items {
					scProvisioner, err := k8sCore.GetStorageProvisionerForPVC(&pvc)
					log.FailOnError(err, fmt.Sprintf("Getting Storage Provisioner for PVC %s ", pvc.Name))
					invalidVolumeSnapShotClassName := fmt.Sprintf("invalid-snapshotclass-%s-%s", RandomString(3), pvc.Name)
					_, err = CreateInvalidVolumeSnapshotClass(invalidVolumeSnapShotClassName, scProvisioner)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of invalid snapshot class [%s] creation for provisioner [%s]", invalidVolumeSnapShotClassName, scProvisioner))
					invalidVolumeSnapShotClassNames = append(invalidVolumeSnapShotClassNames, invalidVolumeSnapShotClassName)
					provisionerInvalidVscMap[scProvisioner] = invalidVolumeSnapShotClassName
					failedVolumes = append(failedVolumes, &pvc)
				}
				if len(failedVolumes) > 0 {
					appCtx.SkipPodValidation = true
				}
			}
			log.Infof(fmt.Sprintf("The list of failed volumes [%v]", failedVolumes))
		})

		Step("Taking manual backup of application from source cluster", func() {
			log.InfoD("taking manual backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			backupName := fmt.Sprintf("%s-%s", "partial-csi-backup", RandomString(5))
			err = CreatePartialBackupWithValidationWithVscMapping(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, BackupOrgID, sourceClusterUid, "", "", "", "", provisionerInvalidVscMap, false, failedVolumes)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of partial backup [%s]", backupName))
			backupNames = append(backupNames, backupName)
		})

		Step("Taking scheduled backup of application from source cluster", func() {
			log.InfoD("taking scheduled backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			scheduleName := fmt.Sprintf("%s-%s", "partial-csi-backup-schedule", RandomString(5))
			scheduleBackupName, err := CreatePartialScheduleBackupWithValidationWithVscMapping(ctx, scheduleName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, BackupOrgID, "", "", "", "", schedulePolicyName, schedulePolicyUID, provisionerInvalidVscMap, false, failedVolumes)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of partial schedule backup [%s]", scheduleBackupName))
			scheduleBackupNames = append(scheduleBackupNames, scheduleBackupName)
			backupNames = append(backupNames, scheduleBackupName)
			scheduleNames = append(scheduleNames, scheduleName)
			err = SuspendBackupSchedule(scheduleName, schedulePolicyName, BackupOrgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule :[%s]", scheduleName))
		})

		Step("Restoring the backed up namespaces in new namespace in destination cluster ", func() {
			log.InfoD("Restoring the backed up namespaces in new namespace in destination cluster ")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			namespaceMapping := make(map[string]string)
			for _, backupName := range backupNames {
				for _, namespace := range backedUpNamespaces {
					namespaceMapping[namespace] = namespace + RandomString(4)
				}
				restoreName := fmt.Sprintf("%s-%s-%s", "test-restore", backupName, RandomString(4))
				log.InfoD("Restoring from the [%s] backup with namespaceMapping [%v]", backupName, namespaceMapping)
				err = CreatePartialRestoreWithValidation(ctx, restoreName, backupName, namespaceMapping, make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContexts, failedVolumes)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of restore [%s]", restoreName))
				restoreNames = append(restoreNames, restoreName)
			}
		})

		Step("Restoring the backed up namespaces in same namespace on source with retain", func() {
			log.InfoD("Restoring the backed up namespaces in same namespace on source with retain")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				restoreName := fmt.Sprintf("%s-%s-%s", "restore-retain", backupName, RandomString(4))
				log.InfoD("Restoring from the [%s] backup ", backupName)
				err = CreatePartialRestoreWithValidation(ctx, restoreName, backupName, make(map[string]string), make(map[string]string), SourceClusterName, BackupOrgID, scheduledAppContexts, failedVolumes)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of restore [%s]", restoreName))
				restoreNames = append(restoreNames, restoreName)
			}
		})

		Step("Restoring the backed up namespaces in same namespace on source with replace", func() {
			log.InfoD("Restoring the backed up namespaces in same namespace on source with replace")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				restoreName := fmt.Sprintf("%s-%s-%s", "restore-replace", backupName, RandomString(4))
				err := CreatePartialRestoreWithReplacePolicyWithValidation(restoreName, backupName, make(map[string]string), SourceClusterName, BackupOrgID, ctx, make(map[string]string), 2, scheduledAppContexts, failedVolumes)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
				restoreNames = append(restoreNames, restoreName)
			}
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)

		defer func() {
			log.InfoD("switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "failed to SetClusterContext to default cluster")
		}()

		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true

		log.Info("Destroying scheduled apps on source cluster")
		DestroyApps(scheduledAppContexts, opts)

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		if len(preRuleNameList) > 0 {
			for _, ruleName := range preRuleNameList {
				err := Inst().Backup.DeleteRuleForBackup(BackupOrgID, ruleName)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup pre rules [%s]", ruleName))
			}
		}
		if len(postRuleNameList) > 0 {
			for _, ruleName := range postRuleNameList {
				err := Inst().Backup.DeleteRuleForBackup(BackupOrgID, ruleName)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup post rules [%s]", ruleName))
			}
		}
		for _, scheduleName := range scheduleNames {
			err = DeleteSchedule(scheduleName, SourceClusterName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		}

		err = Inst().Backup.DeleteBackupSchedulePolicy(BackupOrgID, []string{schedulePolicyName})
		dash.VerifySafely(err, nil, "Deleting backup schedule policies")

		// Cleanup all backups
		allBackups, err := GetAllBackupsAdmin()
		dash.VerifySafely(err, nil, "Verifying fetching of all backups")
		for _, backupName := range allBackups {
			backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, BackupOrgID)
			dash.VerifySafely(err, nil, fmt.Sprintf("Getting backuip UID for backup %s", backupName))
			_, err = DeleteBackup(backupName, backupUID, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying backup deletion - %s", backupName))
		}

		log.Info("Deleting restored namespaces")
		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting Restore [%s]", restoreName))
		}

		log.Info("Deleting csi volumesnapshot classes ")
		for _, invalidVolumeSnapShotClassName := range invalidVolumeSnapShotClassNames {
			err = Inst().S.DeleteCsiSnapshotClass(invalidVolumeSnapShotClassName)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting Volumesnapshot [%s]", invalidVolumeSnapShotClassName))
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// This testcase verified the partial backup success when few Px volumes snapshots are successful and few are failed
var _ = Describe("{PartialBackupSuccessWithPxVolumes}", func() {

	var (
		scheduledAppContexts []*scheduler.Context
		sourceClusterUid     string
		cloudCredName        string
		cloudCredUID         string
		backupLocationUID    string
		backupLocationName   string
		backupLocationMap    map[string]string
		labelSelectors       map[string]string
		providers            []string
		failedPvcs           []*corev1.PersistentVolumeClaim
		backupName           string
		namespaceMapping     map[string]string
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("PartialBackupSuccessWithPxVolumes", "Verify partial backup success when some of the Px volume snapshot fails", nil, 299229, Mkoppal, Q2FY25)

		backupLocationMap = make(map[string]string)
		namespaceMapping = make(map[string]string)
		labelSelectors = make(map[string]string)
		providers = GetBackupProviders()

		log.InfoD("scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		Inst().AppList = []string{"mysql-backup-data"}
		for i := 0; i < 3; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, appCtx := range appContexts {
				appCtx.ReadinessTimeout = AppReadinessTimeout
				scheduledAppContexts = append(scheduledAppContexts, appCtx)
			}
		}

	})

	It("Verify partial backup success when some of the Px volume snapshot fails", func() {
		defer func() {
			log.InfoD("switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "failed to SetClusterContext to default cluster")
		}()

		Step("Validating applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
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

			sourceClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))

			clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, DestinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", DestinationClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", DestinationClusterName))
		})

		Step("Taking backup of application from source cluster", func() {
			var wg sync.WaitGroup
			log.InfoD("taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			k8sCore := core.Instance()
			backupName = fmt.Sprintf("%s-%s", "autogenerated-partial-backup", RandomString(4))

			// Making a list of PVCs to fail
			for _, appCtx := range scheduledAppContexts {
				scheduledNamespace := appCtx.ScheduleOptions.Namespace
				pvcList, err := k8sCore.GetPersistentVolumeClaims(scheduledNamespace, make(map[string]string))
				log.FailOnError(err, fmt.Sprintf("error getting PVC list for namespace %s", scheduledNamespace))
				pvcs := pvcList.Items
				log.Infof("PVCs to fail in namespace %s:", scheduledNamespace)
				for j := 0; j < len(pvcs)-1; j++ {
					log.Infof("PVC Name: [%s], PVC Volume name: [%s], PVC Namespace: [%s]", pvcs[j].Name, pvcs[j].Spec.VolumeName, pvcs[j].Namespace)
					failedPvcs = append(failedPvcs, &pvcs[j])
				}
				appCtx.SkipPodValidation = true
			}

			wg.Add(2)
			// Go routine to stop backups for the PVCs
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				var wg1 sync.WaitGroup
				wg1.Add(len(failedPvcs))
				for _, pvc := range failedPvcs {
					go func(pvc *corev1.PersistentVolumeClaim) {
						defer GinkgoRecover()
						defer wg1.Done()
						log.Infof("Stopping all cs backups for %s [%s] in namespace %s", pvc.Name, pvc.Spec.VolumeName, pvc.Namespace)
						err = StopCloudsnapBackup(pvc.Name, pvc.Namespace)
						log.FailOnError(err, fmt.Sprintf("error stopping all cs backups for %s [%s] in namespace %s", pvc.Name, pvc.Spec.VolumeName, pvc.Namespace))
					}(pvc)
				}
				wg1.Wait()
			}()

			// Go routine to create backup
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				log.InfoD("creating backup [%s] in source cluster [%s] (%s), organization [%s], in backup location [%s]", backupName, SourceClusterName, sourceClusterUid, BackupOrgID, backupLocationName)
				err := CreateBackupWithPartialSuccessValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, BackupOrgID, sourceClusterUid, "", "", "", "", failedPvcs)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of partial backup [%s]", backupName))
			}()
			wg.Wait()

		})

		Step("Restoring the partial backup with namespace mapping", func() {
			log.InfoD("Restoring the partial backup with namespace mapping")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, appCtx := range scheduledAppContexts {
				namespaceMapping[appCtx.ScheduleOptions.Namespace] = appCtx.ScheduleOptions.Namespace + RandomString(4)
			}
			restoreName := fmt.Sprintf("%s-%s", "restore-partial-backup", RandomString(4))
			log.InfoD("Restoring from the [%s] backup with namespaceMapping [%v]", restoreName, namespaceMapping)
			err = CreatePartialRestoreWithValidation(ctx, restoreName, backupName, namespaceMapping, make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContexts, failedPvcs)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of restore with partial backup [%s]", restoreName))
		})

		Step("Restoring the partial backup with replace option", func() {
			log.InfoD("Restoring the partial backup with replace option")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName := fmt.Sprintf("%s-%s", "restore-partial-backup-replace", RandomString(4))
			log.InfoD("Restoring from the [%s] backup with namespaceMapping [%v]", restoreName, namespaceMapping)
			err = CreatePartialRestoreWithReplacePolicyWithValidation(restoreName, backupName, namespaceMapping, DestinationClusterName, BackupOrgID, ctx, make(map[string]string), 2, scheduledAppContexts, failedPvcs)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of restore with partial backup with replace option [%s]", restoreName))
		})

		Step("Restoring the partial backup with retain option", func() {
			log.InfoD("Restoring the partial backup with retain option")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName := fmt.Sprintf("%s-%s", "restore-partial-backup-retain", RandomString(4))
			log.InfoD("Restoring from the [%s] backup with namespaceMapping [%v]", restoreName, namespaceMapping)
			err = CreatePartialRestoreWithReplacePolicyWithValidation(restoreName, backupName, namespaceMapping, DestinationClusterName, BackupOrgID, ctx, make(map[string]string), 1, scheduledAppContexts, failedPvcs)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of restore with partial backup with retain option [%s]", restoreName))
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)

	})
})

// This testcase Verifies partial backup and restore when both Px and KDMP volumes are backed up with failing KDMP backups
var _ = Describe("{PartialBackupSuccessWithPxAndKDMPVolumes}", func() {

	var (
		backupNames            []string
		scheduleBackupNames    []string
		restoreNames           []string
		scheduledAppContexts   []*scheduler.Context
		sourceClusterUid       string
		cloudCredName          string
		cloudCredUID           string
		backupLocationUID      string
		backupLocationName     string
		backupLocationMap      = make(map[string]string)
		labelSelectors         = make(map[string]string)
		namespaceMapping       = make(map[string]string)
		schedulePolicyName     string
		schedulePolicyUID      string
		schedulePolicyInterval = int64(15)
		providers              []string
		failedVolumes          []*corev1.PersistentVolumeClaim
		backedUpNamespaces     []string
		scheduleNames          []string
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("PartialBackupSuccessWithPxAndKDMPVolumes", "Verifies partial backup and restore when both Px and KDMP volumes are backed up with failing KDMP backups",
			nil, 299230, Mkoppal, Q2FY25)
		providers = GetBackupProviders()
		numOfNamespace := 2
		log.InfoD("scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		appList := Inst().AppList
		defer func() {
			Inst().AppList = appList
		}()
		var err error
		Inst().AppList, err = GetApplicationSpecForFeature("PartialBackup")
		log.FailOnError(err, "Fetching application spec for feature PartialBackup")
		for i := 0; i < numOfNamespace; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, appCtx := range appContexts {
				appCtx.ReadinessTimeout = AppReadinessTimeout
				scheduledAppContexts = append(scheduledAppContexts, appCtx)
				appNamespace := appCtx.ScheduleOptions.Namespace
				backedUpNamespaces = append(backedUpNamespaces, appNamespace)
			}
		}
		// Setting BACKUP_TYPE env variable to "csi_offload_s3" to make CSI snapshots to be offloaded to backup-location and trigger KDMP path
		err = os.Setenv("BACKUP_TYPE", "csi_offload_s3")
		log.FailOnError(err, "Setting BACKUP_TYPE env variable")
	})

	It("Verifies partial backup and restore when both Px and KDMP volumes are backed up with failing KDMP backups", func() {
		defer func() {
			log.InfoD("switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "failed to SetClusterContext to default cluster")
		}()

		Step("Validating applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
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

		Step("Create schedule policy", func() {
			log.InfoD("Creating schedule policy")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			schedulePolicyName = fmt.Sprintf("%s-%v", "periodic-schedule-policy", RandomString(5))
			schedulePolicyUID = uuid.New()
			err = CreateBackupScheduleIntervalPolicy(5, schedulePolicyInterval, 5, schedulePolicyName, schedulePolicyUID, BackupOrgID, ctx, false, false)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule policy %s", schedulePolicyName))
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

			sourceClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))

			clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, DestinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", DestinationClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", DestinationClusterName))
		})

		Step("Making a list of all CSI volumes to fail", func() {
			log.InfoD("Making a list of all CSI volumes to fail")
			pvcLabelSelector := make(map[string]string)
			pvcLabelSelector["backupVolumeType"] = "csi"
			k8sCore := k8score.Instance()
			for _, appCtx := range scheduledAppContexts {
				scheduledNamespace := appCtx.ScheduleOptions.Namespace
				log.Infof("Getting PVC list with provisioner based label selector in namespace %s", scheduledNamespace)
				pvcList, err := k8sCore.GetPersistentVolumeClaims(scheduledNamespace, pvcLabelSelector)
				log.FailOnError(err, "Getting PVC list with provisioner based label selector")
				for _, pvc := range pvcList.Items {
					pvcCopy := pvc.DeepCopy()
					log.Infof("Adding PVC Name: [%s], PVC Volume name: [%s], PVC Namespace: [%s] to failed volumes", pvc.Name, pvc.Spec.VolumeName, pvc.Namespace)
					failedVolumes = append(failedVolumes, pvcCopy)
				}
				if len(pvcList.Items) > 0 {
					appCtx.SkipPodValidation = true
				}
			}
			log.Infof("List of volumes to fail during backup")
			for _, pvc := range failedVolumes {
				log.Infof("PVC Name: [%s], PVC Volume name: [%s], PVC Namespace: [%s]", pvc.Name, pvc.Spec.VolumeName, pvc.Namespace)
			}
		})

		Step("Taking manual backup of application from source cluster and deleting the DE CR in parallel", func() {
			log.InfoD("taking manual backup of applications")
			var wg sync.WaitGroup
			log.InfoD("taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			backupName := fmt.Sprintf("%s-%s", "auto-manual-partial-backup", RandomString(4))
			backupNames = append(backupNames, backupName)
			wg.Add(2)
			// Go routine to delete DE CRs for the PVCs
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				var wg1 sync.WaitGroup
				wg1.Add(len(failedVolumes))
				for _, pvc := range failedVolumes {
					go func(pvc *corev1.PersistentVolumeClaim) {
						defer GinkgoRecover()
						defer wg1.Done()
						log.Infof("Deleting Data export CR for %s [%s] in namespace %s", pvc.Name, pvc.Spec.VolumeName, pvc.Namespace)
						err = DeleteDataExportCRForVolume(pvc)
						log.FailOnError(err, fmt.Sprintf("error deleting DE CR for %s [%s] in namespace %s", pvc.Name, pvc.Spec.VolumeName, pvc.Namespace))
					}(pvc)
				}
				wg1.Wait()
			}()
			// Go routine to create backup
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				log.InfoD("creating backup [%s] in source cluster [%s] (%s), organization [%s], in backup location [%s]", backupName, SourceClusterName, sourceClusterUid, BackupOrgID, backupLocationName)
				err = CreatePartialBackupWithValidationWithVscMapping(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, BackupOrgID, sourceClusterUid, "", "", "", "", make(map[string]string), false, failedVolumes)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of partial backup while DE CR for CSI volumes are bring deleted [%s]", backupName))
			}()
			wg.Wait()

		})

		Step("Start watchers to kill the DE CR", func() {
			log.InfoD("Starting watchers to kill the DE CR")
			err := WatchAndKillDataExportCR(backedUpNamespaces)
			log.FailOnError(err, "Watching and killing DE CR")
		})

		Step("Taking scheduled backup of application from source cluster", func() {
			log.InfoD("taking scheduled backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			scheduleName := fmt.Sprintf("%s-%s", "schedule-partial", RandomString(4))
			scheduleBackupName, err := CreatePartialScheduleBackupWithValidationWithVscMapping(ctx, scheduleName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, BackupOrgID, "", "", "", "", schedulePolicyName, schedulePolicyUID, make(map[string]string), false, failedVolumes)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of partial schedule backup [%s]", scheduleBackupName))
			scheduleBackupNames = append(scheduleBackupNames, scheduleBackupName)
			backupNames = append(backupNames, scheduleBackupName)
			scheduleNames = append(scheduleNames, scheduleName)
			err = SuspendBackupSchedule(scheduleName, schedulePolicyName, BackupOrgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Suspending Backup Schedule :[%s]", scheduleName))
		})

		Step("Restoring the backed up namespaces in new namespace in destination cluster ", func() {
			log.InfoD("Restoring the backed up namespaces in new namespace in destination cluster ")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range backedUpNamespaces {
				namespaceMapping[namespace] = namespace + RandomString(4)
			}
			for _, backupName := range backupNames {
				restoreName := fmt.Sprintf("%s-%s-%s", "default", backupName, RandomString(4))
				log.InfoD("Restoring from the [%s] backup with namespaceMapping [%v]", backupName, namespaceMapping)
				err = CreatePartialRestoreWithValidation(ctx, restoreName, backupName, namespaceMapping, make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContexts, failedVolumes)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of restore [%s]", restoreName))
				restoreNames = append(restoreNames, restoreName)
			}
		})

		Step("Restoring the backed up namespaces in same namespace on source with retain", func() {
			log.InfoD("Restoring the backed up namespaces in same namespace on source with retain")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				restoreName := fmt.Sprintf("%s-%s-%s", "retain", backupName, RandomString(4))
				log.InfoD("Restoring from the [%s] backup ", backupName)
				err := CreatePartialRestoreWithReplacePolicyWithValidation(restoreName, backupName, namespaceMapping, DestinationClusterName, BackupOrgID, ctx, make(map[string]string), 1, scheduledAppContexts, failedVolumes)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of restore [%s]", restoreName))
				restoreNames = append(restoreNames, restoreName)
			}
		})

		Step("Restoring the backed up namespaces in same namespace on source with replace", func() {
			log.InfoD("Restoring the backed up namespaces in same namespace on source with replace")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupNames {
				restoreName := fmt.Sprintf("%s-%s-%s", "replace", backupName, RandomString(4))
				err := CreatePartialRestoreWithReplacePolicyWithValidation(restoreName, backupName, namespaceMapping, DestinationClusterName, BackupOrgID, ctx, make(map[string]string), 2, scheduledAppContexts, failedVolumes)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
				restoreNames = append(restoreNames, restoreName)
			}
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(make([]*scheduler.Context, 0))
		defer func() {
			log.Infof("Unsetting BACKUP_TYPE env variable")
			err := os.Unsetenv("BACKUP_TYPE")
			log.FailOnError(err, "Unsetting BACKUP_TYPE env variable")
		}()

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		for _, scheduleName := range scheduleNames {
			err = DeleteSchedule(scheduleName, SourceClusterName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		}
		err = Inst().Backup.DeleteBackupSchedulePolicy(BackupOrgID, []string{schedulePolicyName})
		dash.VerifySafely(err, nil, "Deleting backup schedule policies")
		// Cleanup all backups
		allBackups, err := GetAllBackupsAdmin()
		dash.VerifySafely(err, nil, "Verifying fetching of all backups")
		for _, backupName := range allBackups {
			backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, BackupOrgID)
			dash.VerifySafely(err, nil, fmt.Sprintf("Getting backuip UID for backup %s", backupName))
			_, err = DeleteBackup(backupName, backupUID, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying backup deletion - %s", backupName))
		}
		log.Info("Deleting restored namespaces")
		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting Restore [%s]", restoreName))
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})
