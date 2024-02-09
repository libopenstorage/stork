package tests

import (
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	"golang.org/x/sync/errgroup"
)

// This testcase verifies backup and restore of Kubevirt VMs in different states like Running, Stopped, Restarting
var _ = Describe("{KubevirtVMBackupRestoreWithDifferentStates}", func() {

	var (
		backupNames                []string
		restoreNames               []string
		scheduledAppContexts       []*scheduler.Context
		sourceClusterUid           string
		cloudCredName              string
		cloudCredUID               string
		backupLocationUID          string
		backupLocationName         string
		backupLocationMap          map[string]string
		labelSelectors             map[string]string
		providers                  []string
		restoreNameToAppContextMap map[string]*scheduler.Context
		namespaceMappingMixed      map[string]string
		namespaceMappingRestart    map[string]string
		namespaceMappingStopped    map[string]string
		backupWithVMRestart        string
		restoreWithVMRestart       string
		namespaceWithStoppedVM     []string
		backupWithVMMixed          string
		restoreWithVMMixed         string
		backupWithVMStopped        string
		restoreWithVMStopped       string
		controlChannel             chan string
		errorGroup                 *errgroup.Group
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("KubevirtVMBackupRestoreWithDifferentStates", "Verify backup and restore of Kubevirt VMs in different states", nil, 93011, Mkoppal, Q3FY24)

		backupLocationMap = make(map[string]string)
		labelSelectors = make(map[string]string)
		restoreNameToAppContextMap = make(map[string]*scheduler.Context)
		namespaceMappingMixed = make(map[string]string)
		namespaceMappingRestart = make(map[string]string)
		namespaceMappingStopped = make(map[string]string)
		namespaceWithStoppedVM = make([]string, 0)
		providers = GetBackupProviders()

		log.InfoD("scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < 4; i++ {
			taskName := fmt.Sprintf("%d-%d", 93011, i)
			appContexts := ScheduleApplications(taskName)
			for _, appCtx := range appContexts {
				appCtx.ReadinessTimeout = AppReadinessTimeout
				scheduledAppContexts = append(scheduledAppContexts, appCtx)
			}
		}
	})

	It("Verify backup and restore of Kubevirt VMs in different states", func() {
		defer func() {
			log.InfoD("switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "failed to SetClusterContext to default cluster")
		}()

		Step("Validating applications", func() {
			log.InfoD("Validating applications")
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})

		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, RandomString(6))
				backupLocationName = fmt.Sprintf("%s-%v", getGlobalBucketName(provider), RandomString(6))
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

		Step("Stopping VMs in a few namespaces", func() {
			log.InfoD("Stopping VMs in a few namespaces")
			namespaceToStopVMs := []string{scheduledAppContexts[0].ScheduleOptions.Namespace, scheduledAppContexts[1].ScheduleOptions.Namespace}
			namespaceWithStoppedVM = append(namespaceWithStoppedVM, namespaceToStopVMs...)
			log.InfoD("Stopping VMs in namespaces - [%v]", namespaceWithStoppedVM)
			for _, n := range namespaceWithStoppedVM {
				err := StopAllVMsInNamespace(n, true)
				log.FailOnError(err, "Failed stopping the VMs in namespace - "+n)
			}
		})

		Step("Taking individual backup of each namespace", func() {
			log.InfoD("Taking individual backup of each namespace")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var wg sync.WaitGroup
			var mutex sync.Mutex
			errors := make([]string, 0)
			backupNames = make([]string, 0)
			for _, appCtx := range scheduledAppContexts {
				backupName := fmt.Sprintf("%s-%s-%v", "auto-backup", appCtx.ScheduleOptions.Namespace, RandomString(6))
				backupNames = append(backupNames, backupName)
				wg.Add(1)
				go func(backupName string, appCtx *scheduler.Context) {
					defer GinkgoRecover()
					defer wg.Done()
					_, preRuleName, err := CreateKubevirtBackupRuleForAllVMsInNamespace(ctx, []string{appCtx.ScheduleOptions.Namespace}, "pre", "default")
					log.FailOnError(err, "Unable to create Pre Rule")
					log.Infof("Pre rule Name - [%s]", preRuleName)
					preRuleUid, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleName)
					log.FailOnError(err, "Unable fetch pre rule uid")
					_, postRuleName, err := CreateKubevirtBackupRuleForAllVMsInNamespace(ctx, []string{appCtx.ScheduleOptions.Namespace}, "post", "default")
					log.FailOnError(err, "Unable to create Post Rule")
					log.Infof("Post rule Name - [%s]", postRuleName)
					postRuleUid, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleName)
					log.FailOnError(err, "Unable fetch post rule uid")
					log.InfoD("creating backup [%s] in source cluster [%s] (%s), organization [%s], of namespace [%s], in backup location [%s]", backupName, SourceClusterName, sourceClusterUid, BackupOrgID, appCtx.ScheduleOptions.Namespace, backupLocationName)
					err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, []*scheduler.Context{appCtx}, labelSelectors, BackupOrgID, sourceClusterUid, preRuleName, preRuleUid, postRuleName, postRuleUid)
					if err != nil {
						mutex.Lock()
						errors = append(errors, fmt.Sprintf("Failed while taking backup [%s]. Error - [%s]", backupName, err.Error()))
						mutex.Unlock()
					}
				}(backupName, appCtx)
			}
			wg.Wait()
			dash.VerifyFatal(len(errors), 0, fmt.Sprintf("Errors generated while taking individual backup of each namespace -\n%s", strings.Join(errors, "}\n{")))
		})

		Step("Restoring all individual backups", func() {
			log.InfoD("Restoring all individual backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var wg sync.WaitGroup
			var mutex sync.Mutex
			errors := make([]string, 0)
			restoreNames = make([]string, 0)
			for i, appCtx := range scheduledAppContexts {
				restoreName := fmt.Sprintf("%s-%s-%v", "auto-restore", appCtx.ScheduleOptions.Namespace, RandomString(6))
				restoreNames = append(restoreNames, restoreName)
				restoreNameToAppContextMap[restoreName] = appCtx
				wg.Add(1)
				go func(restoreName string, appCtx *scheduler.Context, i int) {
					defer GinkgoRecover()
					defer wg.Done()
					log.InfoD("Restoring [%s] namespace from the [%s] backup", appCtx.ScheduleOptions.Namespace, backupNames[i])
					err = CreateRestore(restoreName, backupNames[i], make(map[string]string), DestinationClusterName, BackupOrgID, ctx, make(map[string]string))
					if err != nil {
						mutex.Lock()
						errors = append(errors, fmt.Sprintf("Failed while creating restore [%s]. Error - [%s]", restoreName, err.Error()))
						mutex.Unlock()
					}
				}(restoreName, appCtx, i)
			}
			wg.Wait()
			dash.VerifyFatal(len(errors), 0, fmt.Sprintf("Errors generated while restoring all individual backups -\n%s", strings.Join(errors, "}\n{")))
		})

		Step("Validating restores of individual backups", func() {
			log.InfoD("Validating restores of individual backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var mutex sync.Mutex
			errors := make([]string, 0)
			defer func() {
				log.InfoD("Switching cluster context back to source cluster")
				err = SetSourceKubeConfig()
				log.FailOnError(err, "Switching context to source cluster failed")
			}()
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			var wg sync.WaitGroup
			for _, restoreName := range restoreNames {
				wg.Add(1)
				go func(restoreName string) {
					defer GinkgoRecover()
					defer wg.Done()
					log.InfoD("Validating restore [%s]", restoreName)
					expectedRestoredAppContext, err := CloneAppContextAndTransformWithMappings(restoreNameToAppContextMap[restoreName], make(map[string]string), make(map[string]string), true)
					if err != nil {
						mutex.Lock()
						errors = append(errors, fmt.Sprintf("Failed while context tranforming of restore [%s]. Error - [%s]", restoreName, err.Error()))
						mutex.Unlock()
						return
					}
					err = ValidateRestore(ctx, restoreName, BackupOrgID, []*scheduler.Context{expectedRestoredAppContext}, make([]string, 0))
					if err != nil {
						mutex.Lock()
						errors = append(errors, fmt.Sprintf("Failed while validating restore [%s]. Error - [%s]", restoreName, err.Error()))
						mutex.Unlock()
					}
				}(restoreName)

			}
			wg.Wait()
			dash.VerifyFatal(len(errors), 0, fmt.Sprintf("Errors generated while validating restores of individual backups -\n%s", strings.Join(errors, "}\n{")))
		})

		Step("Take backup of all namespaces with VMs in Running and Stopped state", func() {
			log.InfoD("Take backup of all namespaces with VMs in Running and Stopped state")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var namespaces []string
			for _, appCtx := range scheduledAppContexts {
				namespaces = append(namespaces, appCtx.ScheduleOptions.Namespace)
				namespaceMappingMixed[appCtx.ScheduleOptions.Namespace] = appCtx.ScheduleOptions.Namespace + "-mixed"
			}
			_, preRuleName, err := CreateKubevirtBackupRuleForAllVMsInNamespace(ctx, namespaces, "pre", "default")
			log.FailOnError(err, "Unable to create Pre Rule")
			log.Infof("Pre rule Name - [%s]", preRuleName)
			preRuleUid, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleName)
			log.FailOnError(err, "Unable fetch pre rule uid")
			_, postRuleName, err := CreateKubevirtBackupRuleForAllVMsInNamespace(ctx, namespaces, "post", "default")
			log.FailOnError(err, "Unable to create Post Rule")
			log.Infof("Post rule Name - [%s]", postRuleName)
			postRuleUid, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleName)
			log.FailOnError(err, "Unable fetch post rule uid")
			backupWithVMMixed = fmt.Sprintf("%s-%s", "auto-backup-mixed", RandomString(6))
			backupNames = append(backupNames, backupWithVMMixed)
			log.InfoD("creating backup [%s] in cluster [%s] (%s), organization [%s], of namespace [%v], in backup location [%s]", backupWithVMMixed, SourceClusterName, sourceClusterUid, BackupOrgID, namespaces, backupLocationName)
			err = CreateBackupWithValidation(ctx, backupWithVMMixed, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts,
				nil, BackupOrgID, sourceClusterUid, preRuleName, preRuleUid, postRuleName, postRuleUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of backup [%s]", backupWithVMMixed))
		})

		Step("Restoring backup taken when VMs were in Running and Stopped state", func() {
			log.InfoD("Restoring backup taken when VMs were in Running and Stopped state")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreWithVMMixed = fmt.Sprintf("%s-%s", "auto-restore-mixed", RandomString(6))
			restoreNames = append(restoreNames, restoreWithVMMixed)
			log.InfoD("Restoring the [%s] backup", backupWithVMMixed)
			err = CreateRestoreWithValidation(ctx, restoreWithVMMixed, backupWithVMMixed, namespaceMappingMixed, make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContexts)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s from backup %s", restoreWithVMMixed, backupWithVMMixed))
		})

		Step("Starting the VMs in the namespace where it was stopped", func() {
			log.InfoD("Starting the VMs in the namespace where it was stopped")
			for _, n := range namespaceWithStoppedVM {
				log.InfoD("Starting the VMs in the namespace [%s] where it was stopped", n)
				err := StartAllVMsInNamespace(n, true)
				log.FailOnError(err, "Failed starting the VMs in namespace - "+n)
			}
		})

		Step("Take backup of all namespaces when VMs are restarting", func() {
			log.InfoD("Take backup of all namespaces when VMs are restarting")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var wg sync.WaitGroup
			var mutex sync.Mutex
			errors := make([]string, 0)
			var namespaces []string
			for _, appCtx := range scheduledAppContexts {
				namespaces = append(namespaces, appCtx.ScheduleOptions.Namespace)
				namespaceMappingRestart[appCtx.ScheduleOptions.Namespace] = appCtx.ScheduleOptions.Namespace + "-restart"
			}
			backupWithVMRestart = fmt.Sprintf("%s-%s", "auto-backup-restart", RandomString(6))
			backupNames = append(backupNames, backupWithVMRestart)
			_, preRuleName, err := CreateKubevirtBackupRuleForAllVMsInNamespace(ctx, namespaces, "pre", "default")
			log.FailOnError(err, "Unable to create Pre Rule")
			log.Infof("Pre rule Name - [%s]", preRuleName)
			preRuleUid, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleName)
			log.FailOnError(err, "Unable fetch pre rule uid")
			_, postRuleName, err := CreateKubevirtBackupRuleForAllVMsInNamespace(ctx, namespaces, "post", "default")
			log.FailOnError(err, "Unable to create Post Rule")
			log.Infof("Post rule Name - [%s]", postRuleName)
			postRuleUid, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleName)
			log.FailOnError(err, "Unable fetch post rule uid")
			log.InfoD("creating backup [%s] in cluster [%s] (%s), organization [%s], of namespace [%v], in backup location [%s]", backupWithVMRestart, SourceClusterName, sourceClusterUid, BackupOrgID, namespaces, backupLocationName)
			_, err = CreateBackupWithoutCheck(ctx, backupWithVMRestart, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, BackupOrgID, sourceClusterUid, preRuleName, preRuleUid, postRuleName, postRuleUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of backup [%s]", backupWithVMRestart))
			for _, n := range namespaces {
				wg.Add(1)
				go func(n string) {
					defer GinkgoRecover()
					defer wg.Done()
					err := RestartAllVMsInNamespace(n, true)
					if err != nil {
						mutex.Lock()
						errors = append(errors, fmt.Sprintf("Failed while restarting VMs in namespace [%s]. Error - [%s]", n, err.Error()))
						mutex.Unlock()
					}
				}(n)
			}
			wg.Wait()
			dash.VerifyFatal(len(errors), 0, fmt.Sprintf("Errors generated while starting VMs -\n%s", strings.Join(errors, "}\n{")))
			err = BackupSuccessCheck(backupWithVMRestart, BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
			log.FailOnError(err, "Failed while checking success of backup [%s]", backupWithVMRestart)
		})

		Step("Restoring backup taken when VMs were Restarting", func() {
			log.InfoD("Restoring backup taken when VMs were Restarting")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreWithVMRestart = fmt.Sprintf("%s-%s", "auto-restore-restart", RandomString(6))
			restoreNames = append(restoreNames, restoreWithVMRestart)
			log.InfoD("Restoring the [%s] backup", backupWithVMRestart)
			err = CreateRestoreWithValidation(ctx, restoreWithVMRestart, backupWithVMRestart, namespaceMappingRestart, make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContexts)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s from backup %s", restoreWithVMRestart, backupWithVMRestart))
		})

		Step("Stopping all the VMs in all the namespaces", func() {
			log.InfoD("Stopping all the VMs in all the namespaces")
			for _, appCtx := range scheduledAppContexts {
				err := StopAllVMsInNamespace(appCtx.ScheduleOptions.Namespace, true)
				log.FailOnError(err, "Failed stopping the VMs in namespace - "+appCtx.ScheduleOptions.Namespace)
			}
		})

		Step("Take backup of all namespaces with VMs in Stopped state", func() {
			log.InfoD("Take backup of all namespaces with VMs in Stopped state")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var namespaces []string
			for _, appCtx := range scheduledAppContexts {
				namespaces = append(namespaces, appCtx.ScheduleOptions.Namespace)
				namespaceMappingStopped[appCtx.ScheduleOptions.Namespace] = appCtx.ScheduleOptions.Namespace + "-stopped"
			}
			_, preRuleName, err := CreateKubevirtBackupRuleForAllVMsInNamespace(ctx, namespaces, "pre", "default")
			log.FailOnError(err, "Unable to create Pre Rule")
			log.Infof("Pre rule Name - [%s]", preRuleName)
			preRuleUid, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleName)
			log.FailOnError(err, "Unable fetch pre rule uid")
			_, postRuleName, err := CreateKubevirtBackupRuleForAllVMsInNamespace(ctx, namespaces, "post", "default")
			log.FailOnError(err, "Unable to create Post Rule")
			log.Infof("Post rule Name - [%s]", postRuleName)
			postRuleUid, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleName)
			log.FailOnError(err, "Unable fetch post rule uid")
			backupWithVMStopped = fmt.Sprintf("%s-%s", "auto-backup-stopped", RandomString(6))
			backupNames = append(backupNames, backupWithVMStopped)
			log.InfoD("creating backup [%s] in cluster [%s] (%s), organization [%s], of namespace [%v], in backup location [%s]", backupWithVMStopped, SourceClusterName, sourceClusterUid, BackupOrgID, namespaces, backupLocationName)
			err = CreateBackupWithValidation(ctx, backupWithVMStopped, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts,
				nil, BackupOrgID, sourceClusterUid, preRuleName, preRuleUid, postRuleName, postRuleUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of backup [%s]", backupWithVMStopped))
		})

		Step("Restoring all backups taken when VMs were Stopped", func() {
			log.InfoD("Restoring all backups taken when VMs were Stopped")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreWithVMStopped = fmt.Sprintf("%s-%s", "auto-restore-stopped", RandomString(6))
			restoreNames = append(restoreNames, restoreWithVMStopped)
			log.InfoD("Restoring the [%s] backup", backupWithVMStopped)
			err = CreateRestoreWithValidation(ctx, restoreWithVMStopped, backupWithVMStopped, namespaceMappingStopped, make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContexts)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s from backup %s", restoreWithVMStopped, backupWithVMStopped))
		})
	})

	JustAfterEach(func() {
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

		log.Info("Destroying scheduled apps on source cluster")
		DestroyApps(scheduledAppContexts, opts)

		log.InfoD("switching to destination context")
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "failed to switch to context to destination cluster")

		log.InfoD("Destroying restored apps on destination clusters")
		restoredAppContexts := make([]*scheduler.Context, 0)
		for _, scheduledAppContext := range scheduledAppContexts {
			restoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, make(map[string]string), make(map[string]string), true)
			if err != nil {
				log.Errorf("TransformAppContextWithMappings: %v", err)
				continue
			}
			restoredAppContexts = append(restoredAppContexts, restoredAppContext)
		}
		for _, scheduledAppContext := range scheduledAppContexts {
			restoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, namespaceMappingMixed, make(map[string]string), true)
			if err != nil {
				log.Errorf("TransformAppContextWithMappings: %v", err)
				continue
			}
			restoredAppContexts = append(restoredAppContexts, restoredAppContext)
		}
		for _, scheduledAppContext := range scheduledAppContexts {
			restoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, namespaceMappingStopped, make(map[string]string), true)
			if err != nil {
				log.Errorf("TransformAppContextWithMappings: %v", err)
				continue
			}
			restoredAppContexts = append(restoredAppContexts, restoredAppContext)
		}
		for _, scheduledAppContext := range scheduledAppContexts {
			restoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, namespaceMappingRestart, make(map[string]string), true)
			if err != nil {
				log.Errorf("TransformAppContextWithMappings: %v", err)
				continue
			}
			restoredAppContexts = append(restoredAppContexts, restoredAppContext)
		}
		err = DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")

		log.InfoD("switching to default context")
		err = SetClusterContext("")
		log.FailOnError(err, "failed to SetClusterContext to default cluster")

		log.Info("Deleting restored namespaces")
		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore [%s]", restoreName))
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// This testcase verifies backup and restore of Kubevirt VMs after upgrading kubevirt version
var _ = Describe("{KubevirtUpgradeTest}", func() {

	var (
		restoreNames         []string
		backupPreUpgrade     string
		backupPostUpgrade    string
		scheduledAppContexts []*scheduler.Context
		sourceClusterUid     string
		cloudCredName        string
		cloudCredUID         string
		backupLocationUID    string
		backupLocationName   string
		backupLocationMap    map[string]string
		providers            []string
		labelSelectors       map[string]string
		namespaceMapping     map[string]string
		controlChannel       chan string
		errorGroup           *errgroup.Group
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("KubevirtUpgradeTest", "Verify backup and restore of Kubevirt VMs after upgrading Kubevirt control plane", nil, 93013, Mkoppal, Q3FY24)

		backupLocationMap = make(map[string]string)
		labelSelectors = make(map[string]string)
		providers = GetBackupProviders()
		namespaceMapping = make(map[string]string)

		log.InfoD("scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%d-%d", 93013, i)
			appContexts := ScheduleApplications(taskName)
			for _, appCtx := range appContexts {
				appCtx.ReadinessTimeout = AppReadinessTimeout
				scheduledAppContexts = append(scheduledAppContexts, appCtx)
			}
		}
	})

	It("Verify backup and restore of Kubevirt VMs in different states", func() {
		defer func() {
			log.InfoD("switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "failed to SetClusterContext to default cluster")
		}()

		Step("Validating applications", func() {
			log.InfoD("Validating applications")
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})

		Step("Creating backup location and cloud setting", func() {
			log.InfoD("Creating backup location and cloud setting")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%s", "cred", provider, RandomString(6))
				backupLocationName = fmt.Sprintf("%s-%v", getGlobalBucketName(provider), RandomString(6))
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, BackupOrgID, provider))
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location - %s", backupLocationName))
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

		Step("Taking backup of kubevirt application pre-upgrade", func() {
			log.InfoD("Taking backup of kubevirt application pre-upgrade")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			backupPreUpgrade = fmt.Sprintf("%s-%s", "auto-pre-upgrade-backup", RandomString(6))
			err = CreateBackupWithValidation(ctx, backupPreUpgrade, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts,
				labelSelectors, BackupOrgID, sourceClusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of backup [%s] of namespace", backupPreUpgrade))
		})

		Step("Restoring kubevirt app using backup taken pre-upgrade", func() {
			log.InfoD("Restoring kubevirt app using backup taken pre-upgrade")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restorePreUpgrade := fmt.Sprintf("%s-%s", "auto-restore", RandomString(6))
			restoreNames = append(restoreNames, restorePreUpgrade)
			log.InfoD("Restoring the [%s] backup", backupPreUpgrade)
			err = CreateRestoreWithValidation(ctx, restorePreUpgrade, backupPreUpgrade, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContexts)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s from backup %s", restorePreUpgrade, backupPreUpgrade))
		})

		Step("Upgrading kubevirt", func() {
			upgradeVersion := GetKubevirtVersionToUpgrade()
			log.InfoD("Upgrading kubevirt on source cluster")
			err := UpgradeKubevirt(upgradeVersion, true)
			log.FailOnError(err, "Failed during kubevirt upgrade on source cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			log.InfoD("Upgrading kubevirt on destination cluster")
			err = UpgradeKubevirt(upgradeVersion, true)
			log.FailOnError(err, "Failed during kubevirt upgrade on destination cluster")
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
		})

		Step("Restoring kubevirt app using backup taken pre-upgrade - post-upgrade", func() {
			log.InfoD("Restoring kubevirt app using backup taken pre-upgrade - post-upgrade")
			for _, appCtx := range scheduledAppContexts {
				namespaceMapping[appCtx.ScheduleOptions.Namespace] = appCtx.ScheduleOptions.Namespace + "-new"
			}
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restorePostUpgrade := fmt.Sprintf("%s-%s", "auto-restore-post-upgrade", RandomString(6))
			restoreNames = append(restoreNames, restorePostUpgrade)
			log.InfoD("Restoring the [%s] backup", backupPreUpgrade)
			err = CreateRestoreWithValidation(ctx, restorePostUpgrade, backupPreUpgrade, namespaceMapping, make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContexts)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore with namespace mapping %s from backup %s", restorePostUpgrade, backupPreUpgrade))
		})

		Step("Taking backup of kubevirt application post-upgrade", func() {
			log.InfoD("Taking backup of kubevirt application post-upgrade")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			backupPostUpgrade = fmt.Sprintf("%s-%s", "auto-post-upgrade-backup", RandomString(6))
			err = CreateBackupWithValidation(ctx, backupPostUpgrade, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts,
				nil, BackupOrgID, sourceClusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of backup [%s] of namespace", backupPostUpgrade))
		})

		Step("Restoring kubevirt app using backup taken post-upgrade", func() {
			log.InfoD("Restoring kubevirt app using backup taken post-upgrade")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreNewPostUpgrade := fmt.Sprintf("%s-%s", "auto-restore-new-post-upgrade", RandomString(6))
			restoreNames = append(restoreNames, restoreNewPostUpgrade)
			log.InfoD("Restoring the [%s] backup", backupPostUpgrade)
			err = CreateRestoreWithValidation(ctx, restoreNewPostUpgrade, backupPostUpgrade, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContexts)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s from backup %s", restoreNewPostUpgrade, backupPostUpgrade))
		})

	})

	JustAfterEach(func() {
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

		log.Info("Destroying scheduled apps on source cluster")
		err = DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")

		log.InfoD("switching to destination context")
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "failed to switch to context to destination cluster")

		log.InfoD("Destroying restored apps on destination clusters")
		restoredAppContexts := make([]*scheduler.Context, 0)
		for _, scheduledAppContext := range scheduledAppContexts {
			restoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, make(map[string]string), make(map[string]string), true)
			if err != nil {
				log.Errorf("TransformAppContextWithMappings: %v", err)
				continue
			}
			restoredAppContexts = append(restoredAppContexts, restoredAppContext)
		}
		DestroyApps(restoredAppContexts, opts)

		log.InfoD("switching to default context")
		err = SetClusterContext("")
		log.FailOnError(err, "failed to SetClusterContext to default cluster")

		log.Info("Deleting restored namespaces")
		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore [%s]", restoreName))
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})
