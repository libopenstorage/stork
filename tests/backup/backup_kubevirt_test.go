package tests

import (
	context1 "context"
	"fmt"
	"github.com/portworx/torpedo/drivers/node"
	kubevirtv1 "kubevirt.io/api/core/v1"
	"math/rand"
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
		StartPxBackupTorpedoTest("KubevirtVMBackupRestoreWithDifferentStates", "Verify backup and restore of Kubevirt VMs in different states", nil, 296416, Mkoppal, Q3FY24)

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
		StartPxBackupTorpedoTest("KubevirtUpgradeTest", "Verify backup and restore of Kubevirt VMs after upgrading Kubevirt control plane", nil, 296418, Mkoppal, Q3FY24)

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

var _ = Describe("{KubevirtVMSshTest}", func() {
	var (
		scheduledAppContexts []*scheduler.Context
	)
	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("KubevirtVMSshTest", "Verify SSH to kubevirt VM", nil, 0, Mkoppal, Q1FY25)

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

		Step("Validating applications", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.InfoD("Validating applications")
			_, _ = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})

		Step("SSH into the kubevirt VM", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			vms, err := GetAllVMsInNamespace(scheduledAppContexts[0].ScheduleOptions.Namespace)
			if err != nil {
				return
			}
			for _, vm := range vms {
				log.Infof("Running command for VM [%s]", vm.Name)
				output, err := RunCmdInVM(vm, "uname -a", context1.TODO())
				log.InfoD("Output of command in step - [%s]", output)
				log.FailOnError(err, "Failed to run command in VM")
			}

			for namespace, appWithData := range NamespaceAppWithDataMap {
				log.Infof("Found vm with data in %s", namespace)
				appWithData[0].InsertBackupData(ctx, "default", []string{})
			}

		})
	})
})

// This testcase verifies Simultaneous backups/ Simultaneous Backup and Restore/ Simultaneous Restore and Source Virtual Machine Deletion
var _ = Describe("{KubevirtVMBackupOrDeletionInProgress}", func() {
	var (
		backupNames          []string
		labelSelectors       map[string]string
		restoreNames         []string
		allVirtualMachines   []kubevirtv1.VirtualMachine
		scheduledAppContexts []*scheduler.Context
		sourceClusterUid     string
		cloudCredName        string
		cloudCredUID         string
		backupLocationUID    string
		backupLocationName   string
		backupLocationMap    map[string]string
		providers            []string
		bkpNamespaces        []string
		isSourceAppDeleted   bool
	)
	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("KubevirtVMBackupOrDeletionInProgress", "Verify backup and restore of a VM if a backup or VM deletion is already inprogress", nil, 296425, ATrivedi, Q1FY25)
		labelSelectors = make(map[string]string)
		providers = GetBackupProviders()
		backupLocationMap = make(map[string]string)
		bkpNamespaces = make([]string, 0)
		isSourceAppDeleted = false

		log.InfoD("scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%d-%d", 93013, i)
			appContexts := ScheduleApplications(taskName)
			for _, appCtx := range appContexts {
				appCtx.ReadinessTimeout = AppReadinessTimeout
				scheduledAppContexts = append(scheduledAppContexts, appCtx)
				bkpNamespaces = append(bkpNamespaces, appCtx.ScheduleOptions.Namespace)
			}
		}

		for _, appCtx := range scheduledAppContexts {
			allVms, err := GetAllVMsInNamespace(appCtx.ScheduleOptions.Namespace)
			log.FailOnError(err, "Unable to get Virtual Machines from [%s]", appCtx.ScheduleOptions.Namespace)
			allVirtualMachines = append(allVirtualMachines, allVms...)
		}

	})

	It("Verify backup and restore of a VM if a backup or VM deletion is already inprogress", func() {
		defer func() {
			log.InfoD("switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "failed to SetClusterContext to default cluster")
		}()

		Step("Validating applications", func() {
			// TODO: Data validation needs to enabled once SSH issue with kubevirt is fixed
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
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

		Step("Triggering two backups simultaneously on the Same virtual machines", func() {
			log.InfoD("Taking backup of virtual machines")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var wg sync.WaitGroup
			var mutex sync.Mutex
			errors := make([]string, 0)

			backupNames = make([]string, 0)
			for i := 0; i < 4; i++ {
				wg.Add(1)
				log.Infof("Triggering backup of Virtual Machine [%v] time", i+1)
				backupName := fmt.Sprintf("%s-%v-%s", "parallel-vm-backup", time.Now().Unix(), RandomString(5))
				go func(backupName string) {
					defer GinkgoRecover()
					defer wg.Done()
					log.InfoD("creating backup [%s] in source cluster [%s] (%s), organization [%s], of virtualMachines [%v], in backup location [%s]", backupName, SourceClusterName, sourceClusterUid, BackupOrgID, allVirtualMachines, backupLocationName)
					err := CreateVMBackup(backupName, allVirtualMachines, SourceClusterName, backupLocationName, backupLocationUID, labelSelectors, BackupOrgID, sourceClusterUid, "", "", "", "", false, ctx)
					backupNames = append(backupNames, backupName)
					if err != nil {
						mutex.Lock()
						errors = append(errors, fmt.Sprintf("Failed while taking backup [%s]. Error - [%s]", backupName, err.Error()))
						mutex.Unlock()
					}
				}(backupName)
			}
			wg.Wait()
			dash.VerifyFatal(len(errors), 0, fmt.Sprintf("Simultaneous backup failed with below errors - \n\n%s", strings.Join(errors, "\n")))
		})

		Step("Triggering backup and restore simultaneously on the same Virtual machine", func() {
			log.InfoD("Taking backup of virtual machines and restoring simultaneously")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var wg sync.WaitGroup
			var mutex sync.Mutex
			errorBackup := make([]string, 0)
			errorRestore := make([]string, 0)

			log.Infof("Triggering backup of Virtual Machine [%v] time", time.Now().Unix())
			backupName := fmt.Sprintf("%s-%v-%s", "vm-backup-with-restore", time.Now().Unix(), RandomString(5))
			wg.Add(1)
			go func(backupName string) {
				defer GinkgoRecover()
				defer wg.Done()
				log.InfoD("creating backup [%s] in source cluster [%s] (%s), organization [%s], of virtualMachines [%v], in backup location [%s]", backupName, SourceClusterName, sourceClusterUid, BackupOrgID, allVirtualMachines, backupLocationName)
				err := CreateVMBackup(backupName, allVirtualMachines, SourceClusterName, backupLocationName, backupLocationUID, labelSelectors, BackupOrgID, sourceClusterUid, "", "", "", "", false, ctx)
				backupNames = append(backupNames, backupName)
				if err != nil {
					mutex.Lock()
					errorBackup = append(errorBackup, fmt.Sprintf("Failed while taking backup [%s]. Error - [%s]", backupName, err.Error()))
					mutex.Unlock()
				}
			}(backupName)

			restoreName := fmt.Sprintf("%s-%v-%s", "vm-restore-with-backup", time.Now().Unix(), RandomString(5))
			wg.Add(1)
			go func(restoreName string) {
				defer GinkgoRecover()
				defer wg.Done()
				restoreNames = append(restoreNames, restoreName)
				log.InfoD("Restoring the [%s] backup", backupNames[0])
				err = CreateRestore(restoreName, backupNames[0], make(map[string]string), DestinationClusterName, BackupOrgID, ctx, make(map[string]string))
				if err != nil {
					mutex.Lock()
					errorRestore = append(errorRestore, fmt.Sprintf("Failed to restore from [%s]. Error - [%s]", backupNames[0], err.Error()))
					mutex.Unlock()
				}
			}(restoreName)

			wg.Wait()

			dash.VerifyFatal(len(errorBackup), 0, fmt.Sprintf("Simultaneous backup with restore failed with below errors - \n\n%s", strings.Join(errorBackup, "\n")))
			dash.VerifyFatal(len(errorRestore), 0, fmt.Sprintf("Simultaneous restore with backup failed with below errors - \n\n%s", strings.Join(errorRestore, "\n")))

		})

		// TODO: This steps needs to be uncommented once the Node Port issue for VM restore is fixed
		//Step("Validating restore for all Virtual Machine Instances restored - Simultaneous restore with backup", func() {
		//	defer func() {
		//		log.InfoD("switching to default context")
		//		err := SetClusterContext("")
		//		log.FailOnError(err, "failed to SetClusterContext to default cluster")
		//	}()
		//	ctx, err := backup.GetAdminCtxFromSecret()
		//	log.FailOnError(err, "Fetching px-central-admin ctx")
		//	err = SetDestinationKubeConfig()
		//	log.FailOnError(err, "failed to switch to context to destination cluster")
		//
		//	expectedRestoredAppContexts := make([]*scheduler.Context, 0)
		//	for _, scheduledAppContext := range scheduledAppContexts {
		//		expectedRestoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, make(map[string]string), make(map[string]string), true)
		//		if err != nil {
		//			log.Errorf("TransformAppContextWithMappings: %v", err)
		//			continue
		//		}
		//		expectedRestoredAppContexts = append(expectedRestoredAppContexts, expectedRestoredAppContext)
		//	}
		//
		//	err = ValidateRestore(ctx, restoreNames[0], BackupOrgID, expectedRestoredAppContexts, make([]string, 0))
		//	log.FailOnError(err, "Restore Validation Failed for [%s]", restoreNames[0])
		//})

		Step("Triggering new restore and deleting source VM simultaneously on the same Virtual machine", func() {
			log.InfoD("Taking backup of virtual machines and deleting the source simultaneously")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var wg sync.WaitGroup
			var mutex sync.Mutex
			errordeleteVM := make([]string, 0)
			errorRestore := make([]string, 0)

			log.Infof("Deleting VMs from source namespace")
			wg.Add(1)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				log.InfoD("Deleting the kubevirt VMs from the namespace")
				for _, namespace := range bkpNamespaces {
					err := DeleteAllVMsInNamespace(namespace)
					if err != nil {
						errordeleteVM = append(errordeleteVM, err.Error())
					}
				}
			}()

			restoreName := fmt.Sprintf("%s-%v-%s", "vm-restore-with-backup", time.Now().Unix(), RandomString(5))
			wg.Add(1)
			go func(restoreName string) {
				defer GinkgoRecover()
				defer wg.Done()
				restoreNames = append(restoreNames, restoreName)
				log.InfoD("Restoring the [%s] backup", backupNames[1])
				err = CreateRestore(restoreName, backupNames[1], make(map[string]string), DestinationClusterName, BackupOrgID, ctx, make(map[string]string))
				if err != nil {
					mutex.Lock()
					errorRestore = append(errorRestore, fmt.Sprintf("Failed to restore from [%s]. Error - [%s]", backupNames[1], err.Error()))
					mutex.Unlock()
				}
			}(restoreName)

			wg.Wait()

			dash.VerifyFatal(len(errordeleteVM), 0, fmt.Sprintf("Source application delete failed with below errors - \n\n%s", strings.Join(errordeleteVM, "\n")))
			isSourceAppDeleted = true
			dash.VerifyFatal(len(errorRestore), 0, fmt.Sprintf("Simultaneous restore with backup failed with below errors - \n\n%s", strings.Join(errorRestore, "\n")))

		})

		//TODO: This steps needs to be uncommented once the Node Port issue for VM restore is fixed
		//Step("Validating contexts for all Virtual Machine Instances restored - Simultaneous restore with deletion", func() {
		//	defer func() {
		//		log.InfoD("switching to default context")
		//		err := SetClusterContext("")
		//		log.FailOnError(err, "failed to SetClusterContext to default cluster")
		//	}()
		//	ctx, err := backup.GetAdminCtxFromSecret()
		//	log.FailOnError(err, "Fetching px-central-admin ctx")
		//	err = SetDestinationKubeConfig()
		//	log.FailOnError(err, "failed to switch to context to destination cluster")
		//
		//	expectedRestoredAppContexts := make([]*scheduler.Context, 0)
		//	for _, scheduledAppContext := range scheduledAppContexts {
		//		expectedRestoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, make(map[string]string), make(map[string]string), true)
		//		if err != nil {
		//			log.Errorf("TransformAppContextWithMappings: %v", err)
		//			continue
		//		}
		//		expectedRestoredAppContexts = append(expectedRestoredAppContexts, expectedRestoredAppContext)
		//	}
		//err = ValidateRestore(ctx, restoreNames[1], BackupOrgID, expectedRestoredAppContexts, make([]string, 0))
		//log.FailOnError(err, "Restore Validation Failed for [%s]", restoreNames[1])
		//})

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
		for _, eachBackup := range backupNames {
			backupUid, err := Inst().Backup.GetBackupUID(ctx, eachBackup, BackupOrgID)
			log.FailOnError(err, "Unable to fetch backup UID")
			// Delete backup to confirm that the user cannot delete the backup
			_, err = DeleteBackup(eachBackup, backupUid, BackupOrgID, ctx)
			log.FailOnError(err, "Unable to delete backups")
		}

		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true

		if !isSourceAppDeleted {
			log.Info("Destroying scheduled apps on source cluster")
			DestroyApps(scheduledAppContexts, opts)
		}

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

// This testcase verifies backup and restore of Kubevirt VMs in with node selector
var _ = Describe("{KubevirtVMBackupRestoreWithNodeSelector}", func() {

	var (
		backupNames                  []string
		restoreNames                 []string
		scheduledAppContexts         []*scheduler.Context
		sourceClusterUid             string
		cloudCredName                string
		cloudCredUID                 string
		backupLocationUID            string
		backupLocationName           string
		backupLocationMap            map[string]string
		providers                    []string
		namespacewithcorrectlabels   []string
		namespacewithincorrectlabels []string
		contextswithcorrectlabels    []*scheduler.Context
		contextswithincorrectlabels  []*scheduler.Context
		nodeSelectorPresent          map[string]string
		nodeSelectorNotPresent       map[string]string
		nodeToBeUsed                 node.Node
		namespaces                   []string
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("KubevirtVMBackupRestoreWithNodeSelector", "Verify backup and restore of Kubevirt VMs with node selector specified", nil, 296426, ATrivedi, Q1FY25)

		backupLocationMap = make(map[string]string)
		providers = GetBackupProviders()
		nodeSelectorPresent = make(map[string]string)
		nodeSelectorNotPresent = make(map[string]string)

		log.InfoD("scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < 4; i++ {
			taskName := fmt.Sprintf("%d-%d", 93011, i)
			appContexts := ScheduleApplications(taskName)
			for _, appCtx := range appContexts {
				appCtx.ReadinessTimeout = AppReadinessTimeout
				scheduledAppContexts = append(scheduledAppContexts, appCtx)
				if i%2 == 0 {
					namespacewithcorrectlabels = append(namespacewithcorrectlabels, appCtx.ScheduleOptions.Namespace)
					contextswithcorrectlabels = append(contextswithcorrectlabels, appCtx)
				} else {
					namespacewithincorrectlabels = append(namespacewithincorrectlabels, appCtx.ScheduleOptions.Namespace)
					contextswithincorrectlabels = append(contextswithincorrectlabels, appCtx)
				}
			}
		}

		// Generate random labels to be added to node and VM as node selector
		nodeSelectorPresent[fmt.Sprintf("node_selector_%s", RandomString(4))] = fmt.Sprintf("value_%s", RandomString(6))
		// Generate random labels to be added to node and VM as node selector
		nodeSelectorNotPresent[fmt.Sprintf("node_selector_%s", RandomString(4))] = fmt.Sprintf("value_%s", RandomString(6))
	})

	It("Verify backup and restore of Kubevirt VMs with node selector specified", func() {
		defer func() {
			log.InfoD("switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "failed to SetClusterContext to default cluster")
		}()

		Step("Validating applications", func() {
			// To Do: Add Data validation for this test case
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
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

		Step("Getting the number of worker nodes in destination cluster and applying label to one of the worker nodes", func() {
			defer func() {
				log.InfoD("switching to source context")
				err := SetSourceKubeConfig()
				log.FailOnError(err, "failed to SetClusterContext to source cluster")
			}()
			err := SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			log.InfoD("Getting the total number of worker nodes in source cluster")
			clusterWorkerNodes := node.GetWorkerNodes()
			log.InfoD("Total number of worker nodes in source cluster are %v", len(clusterWorkerNodes))
			log.Infof("Selecting a random node out of all worker nodes")
			// Selecting one worker node randomly out of all worker nodes
			nodeToBeUsed = clusterWorkerNodes[rand.Intn(len(clusterWorkerNodes))]
			log.Infof("Applying label [%v] to [%s] the worker node on source cluster", nodeSelectorPresent, nodeToBeUsed.Name)
			// Applying node selector to the selected worker node
			for key, value := range nodeSelectorPresent {
				err = Inst().S.AddLabelOnNode(nodeToBeUsed, key, value)
				log.FailOnError(err, fmt.Sprintf("Failed to apply label [%s:%s] to source cluster worker node %v", key, value, nodeToBeUsed.Name))
			}
		})

		Step("Adding correct node selector to few virtual machine instances", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			// Adding node selector which is already added as label on destination to few virtual machines
			for _, namespace := range namespacewithcorrectlabels {
				vms, err := GetAllVMsInNamespace(namespace)
				if err != nil {
					return
				}
				for _, vm := range vms {
					err = AddNodeToVirtualMachine(vm, nodeSelectorPresent, ctx)
					log.FailOnError(err, "Unable to apply node selector to VM")
				}
			}
		})

		Step("Adding incorrect node selector to few virtual machine instances", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			// Adding node selector which is NOT added as label on destination to few virtual machines
			for _, namespace := range namespacewithincorrectlabels {
				vms, err := GetAllVMsInNamespace(namespace)
				if err != nil {
					return
				}
				for _, vm := range vms {
					err = AddNodeToVirtualMachine(vm, nodeSelectorNotPresent, ctx)
					log.FailOnError(err, "Unable to apply node selector to VM")
				}
			}
		})

		Step("Take backup of all namespaces with VMs having correct and incorrect labels", func() {
			log.InfoD("Take backup of all namespaces with VMs having correct and incorrect labels")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, appCtx := range scheduledAppContexts {
				namespaces = append(namespaces, appCtx.ScheduleOptions.Namespace)
			}
			backupName := fmt.Sprintf("%s-%s", "node-test-backup-all", RandomString(6))
			backupNames = append(backupNames, backupName)
			log.InfoD("creating backup [%s] in cluster [%s] (%s), organization [%s], of namespace [%v], in backup location [%s]", backupName, SourceClusterName, sourceClusterUid, BackupOrgID, namespaces, backupLocationName)
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts,
				nil, BackupOrgID, sourceClusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of backup [%s]", backupName))
		})

		Step("Restoring backup with namespaces having virtual machines with correct and incorrect labels", func() {
			log.InfoD("Restoring backup taken when VMs were in Running and Stopped state")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreAll := fmt.Sprintf("%s-%s", "auto-restore-all", RandomString(6))
			restoreNames = append(restoreNames, restoreAll)
			log.InfoD("Restoring the [%s] backup", backupNames[0])
			// Not restoring with validation as it will fail for all the VMs which are gone in scheduling state
			err = CreateRestore(restoreAll, backupNames[0], make(map[string]string), DestinationClusterName, BackupOrgID, ctx, make(map[string]string))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s from backup %s", restoreAll, backupNames[0]))
		})

		Step("Validating restore for all Virtual Machine Instances expected to be running", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			err = SetDestinationKubeConfig()
			log.FailOnError(err, "failed to switch to context to destination cluster")

			expectedRestoredAppContexts := make([]*scheduler.Context, 0)
			for _, scheduledAppContext := range contextswithcorrectlabels {
				expectedRestoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, make(map[string]string), make(map[string]string), true)
				if err != nil {
					log.Errorf("TransformAppContextWithMappings: %v", err)
					continue
				}
				expectedRestoredAppContexts = append(expectedRestoredAppContexts, expectedRestoredAppContext)
			}
			err = ValidateRestore(ctx, restoreNames[0], BackupOrgID, expectedRestoredAppContexts, make([]string, 0))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Restore Validation Failed for [%s]", restoreNames[0]))
		})

		Step("Verifying state and nodes for all restored Virtual Machine Instances", func() {
			defer func() {
				log.InfoD("switching to default context")
				err := SetClusterContext("")
				log.FailOnError(err, "failed to SetClusterContext to default cluster")
			}()

			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			log.Infof("Verifying nodes with correct labels")
			for _, namespace := range namespacewithcorrectlabels {
				err = CompareNodeAndStatusOfVMInNamespace(namespace, nodeToBeUsed, "Running", ctx)
				log.FailOnError(err, "Node or State validation failed for Virtual Machine Instance")
			}
			log.Infof("Verifying nodes with incorrect labels")
			for _, namespace := range namespacewithincorrectlabels {
				err = CompareNodeAndStatusOfVMInNamespace(namespace, node.Node{}, "Scheduling", ctx)
				log.FailOnError(err, "Node or State validation failed for Virtual Machine Instance")

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

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		backupUid, err := Inst().Backup.GetBackupUID(ctx, backupNames[0], BackupOrgID)
		log.FailOnError(err, "Unable to fetch backup UID")
		// Delete backup to confirm that the user cannot delete the backup
		_, err = DeleteBackup(backupNames[0], backupUid, BackupOrgID, ctx)
		log.FailOnError(err, "Unable to delete backups")

		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true

		log.Info("Destroying scheduled apps on source cluster")
		DestroyApps(scheduledAppContexts, opts)
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

var _ = Describe("{KubevirtVMWithFreezeUnfreeze}", func() {
	var (
		// TODO: Need to uncomment the below code once we have data validation for kubevirt VMs implemented
		//controlChannel       chan string
		//errorGroup           *errgroup.Group
		scheduledAppContexts []*scheduler.Context
		sourceClusterUid     string
		cloudCredName        string
		cloudCredUID         string
		backupLocationUID    string
		backupLocationName   string
		backupLocationMap    map[string]string
		providers            []string
		labelSelectors       map[string]string
		allVMs               []kubevirtv1.VirtualMachine
		allVMNames           []string
		backupName           string
		backupNames          []string
		freezeRuleName       string
		unfreezeRuleName     string
		freezeRuleUid        string
		unfreezeRuleUid      string
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("KubevirtVMWithFreezeUnfreeze", "Verify VM backup with freeze rule and without unfreeze", nil, 296422, Mkoppal, Q1FY25)

		backupLocationMap = make(map[string]string)
		providers = GetBackupProviders()

		log.InfoD("scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%d-%d", 296422, i)
			appContexts := ScheduleApplications(taskName)
			for _, appCtx := range appContexts {
				appCtx.ReadinessTimeout = AppReadinessTimeout
				scheduledAppContexts = append(scheduledAppContexts, appCtx)
			}
		}
	})

	It("Verify VM backup with freeze rule and without unfreeze", func() {

		Step("Validating applications", func() {
			log.InfoD("Validating applications")
			// TODO: Need to uncomment the below code once we have data validation for kubevirt VMs implemented
			//ctx, err := backup.GetAdminCtxFromSecret()
			//log.FailOnError(err, "Fetching px-central-admin ctx")
			//controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
			ValidateApplications(scheduledAppContexts)
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

			clusters := []string{SourceClusterName, DestinationClusterName}
			for _, c := range clusters {
				clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, c, ctx)
				log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", c))
				dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", c))

			}

			sourceClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Creating freeze and unfreeze rules", func() {
			log.InfoD("Creating freeze and unfreeze rules")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, appCtx := range scheduledAppContexts {
				vms, err := GetAllVMsInNamespace(appCtx.ScheduleOptions.Namespace)
				log.FailOnError(err, "Failed to get VMs in namespace - %s", appCtx.ScheduleOptions.Namespace)
				allVMs = append(allVMs, vms...)
			}
			for _, v := range allVMs {
				allVMNames = append(allVMNames, v.Name)
			}
			freezeRuleName = fmt.Sprintf("vm-freeze-rule-%s", RandomString(4))
			err = CreateRuleForVMBackup(freezeRuleName, allVMs, Freeze, ctx)
			log.FailOnError(err, "Failed to create freeze rule %s for VMs - %v", freezeRuleName, allVMNames)
			unfreezeRuleName = fmt.Sprintf("vm-unfreeze-rule-%s", RandomString(4))
			err = CreateRuleForVMBackup(unfreezeRuleName, allVMs, Unfreeze, ctx)
			log.FailOnError(err, "Failed to create unfreeze rule %s for VMs - %v", unfreezeRuleName, allVMNames)
		})

		Step("SSH into the kubevirt VM to check pre-backup VM health", func() {
			log.InfoD("SSH into the kubevirt VM to check pre-backup VM health")
			for _, vm := range allVMs {
				log.Infof("Running command for VM [%s]", vm.Name)
				output, err := RunCmdInVM(vm, "uname -a", context1.TODO())
				log.InfoD("Output of command in step - [%s]", output)
				log.FailOnError(err, "Failed to run command in VM")
			}
		})

		Step("Taking backup of kubevirt VM with freeze rule only and without unfreeze rule", func() {
			log.InfoD("Taking backup of kubevirt VM with freeze rule only and without unfreeze rule")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			backupName = fmt.Sprintf("%s-%s", "backup-freeze-without-unfreeze", RandomString(6))
			backupNames = append(backupNames, backupName)
			var vmNames []string
			for _, v := range allVMs {
				vmNames = append(vmNames, v.Name)
			}
			log.Infof("VMs to be backed up - %v", vmNames)
			freezeRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, freezeRuleName)
			log.FailOnError(err, "Unable to fetch freeze rule uid - %s", freezeRuleName)
			err = CreateVMBackupWithValidation(ctx, backupName, allVMs, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts,
				labelSelectors, BackupOrgID, sourceClusterUid, freezeRuleName, freezeRuleUid, "", "", true)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of VM backup [%s]", backupName))
		})

		Step("SSH into the kubevirt VM to check VM health after freeze rule", func() {
			log.InfoD("SSH into the kubevirt VM to check VM health after freeze rule")
			for _, vm := range allVMs {
				log.Infof("Running command for VM [%s]", vm.Name)
				output, err := RunCmdInVM(vm, "uname -a", context1.TODO())
				log.InfoD("Output of command in step - [%s]", output)
				log.FailOnError(err, "Failed to run command in VM")
			}
		})

		Step("Taking backup of kubevirt VM without freeze rule and with unfreeze rule", func() {
			log.InfoD("Taking backup of kubevirt VM without freeze rule and with unfreeze rule")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			backupName = fmt.Sprintf("%s-%s", "backup-unfreeze-without-freeze", RandomString(6))
			backupNames = append(backupNames, backupName)
			var vmNames []string
			for _, v := range allVMs {
				vmNames = append(vmNames, v.Name)
			}
			log.Infof("VMs to be backed up - %v", vmNames)
			unfreezeRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, unfreezeRuleName)
			log.FailOnError(err, "Unable to fetch unfreeze rule uid - %s", unfreezeRuleName)
			err = CreateVMBackupWithValidation(ctx, backupName, allVMs, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts,
				labelSelectors, BackupOrgID, sourceClusterUid, "", "", unfreezeRuleName, unfreezeRuleUid, true)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of VM backup [%s]", backupName))
		})

		Step("SSH into the kubevirt VM to check VM health after unfreeze rule", func() {
			log.InfoD("SSH into the kubevirt VM to check VM health after unfreeze rule")
			for _, vm := range allVMs {
				log.Infof("Running command for VM [%s]", vm.Name)
				output, err := RunCmdInVM(vm, "uname -a", context1.TODO())
				log.InfoD("Output of command in step - [%s]", output)
				log.FailOnError(err, "Failed to run command in VM")
			}
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true

		RuleEnumerateReq := &api.RuleEnumerateRequest{
			OrgId: BackupOrgID,
		}
		ruleList, err := Inst().Backup.EnumerateRule(ctx, RuleEnumerateReq)
		for _, r := range ruleList.GetRules() {
			log.Infof("Deleting rule [%s]", r.Name)
			_, err := Inst().Backup.DeleteRule(ctx, &api.RuleDeleteRequest{
				OrgId: BackupOrgID,
				Name:  r.Name,
				Uid:   r.Uid,
			})
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting rule [%s]", r.Name))
		}

		log.Info("Destroying scheduled apps on source cluster")
		// TODO: Need to uncomment the below code once we have data validation for kubevirt VMs implemented
		//err = DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		DestroyApps(scheduledAppContexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

var _ = Describe("{KubevirtInPlaceRestoreWithReplaceAndRetain}", func() {
	var (
		// TODO: Need to uncomment the below code once we have data validation for kubevirt VMs implemented
		//controlChannel       chan string
		//errorGroup           *errgroup.Group
		restoreNames         []string
		restoreName          string
		scheduledAppContexts []*scheduler.Context
		sourceClusterUid     string
		cloudCredName        string
		cloudCredUID         string
		backupLocationUID    string
		backupLocationName   string
		backupLocationMap    map[string]string
		providers            []string
		labelSelectors       map[string]string
		allVMs               []kubevirtv1.VirtualMachine
		allVMNames           []string
		backupName           string
		backupNames          []string
		freezeRuleName       string
		freezeRuleUid        string
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("KubevirtInPlaceRestoreWithReplaceAndRetain", "Verify in-place restore with retain and replace options when VM is in frozen state", nil, 296423, Mkoppal, Q1FY25)

		backupLocationMap = make(map[string]string)
		providers = GetBackupProviders()

		log.InfoD("scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%d-%d", 296423, i)
			appContexts := ScheduleApplications(taskName)
			for _, appCtx := range appContexts {
				appCtx.ReadinessTimeout = AppReadinessTimeout
				scheduledAppContexts = append(scheduledAppContexts, appCtx)
			}
		}
	})

	It("Verify VM backup with freeze rule and without unfreeze", func() {

		Step("Validating applications", func() {
			log.InfoD("Validating applications")
			// TODO: Need to uncomment the below code once we have data validation for kubevirt VMs implemented
			//ctx, err := backup.GetAdminCtxFromSecret()
			//log.FailOnError(err, "Fetching px-central-admin ctx")
			//controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
			ValidateApplications(scheduledAppContexts)
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

			clusters := []string{SourceClusterName, DestinationClusterName}
			for _, c := range clusters {
				clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, c, ctx)
				log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", c))
				dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", c))

			}

			sourceClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Creating freeze rule", func() {
			log.InfoD("Creating freeze rule")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, appCtx := range scheduledAppContexts {
				vms, err := GetAllVMsInNamespace(appCtx.ScheduleOptions.Namespace)
				log.FailOnError(err, "Failed to get VMs in namespace - %s", appCtx.ScheduleOptions.Namespace)
				allVMs = append(allVMs, vms...)
			}
			for _, v := range allVMs {
				allVMNames = append(allVMNames, v.Name)
			}
			freezeRuleName = fmt.Sprintf("vm-freeze-rule-%s", RandomString(4))
			err = CreateRuleForVMBackup(freezeRuleName, allVMs, Freeze, ctx)
			log.FailOnError(err, "Failed to create freeze rule %s for VMs - %v", freezeRuleName, allVMNames)
		})

		Step("Taking backup of kubevirt VM with freeze rule only and without unfreeze rule", func() {
			log.InfoD("Taking backup of kubevirt VM with freeze rule only and without unfreeze rule")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			backupName = fmt.Sprintf("%s-%s", "backup-freeze-without-unfreeze", RandomString(6))
			backupNames = append(backupNames, backupName)
			var vmNames []string
			for _, v := range allVMs {
				vmNames = append(vmNames, v.Name)
			}
			log.Infof("VMs to be backed up - %v", vmNames)
			freezeRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, freezeRuleName)
			log.FailOnError(err, "Unable to fetch freeze rule uid - %s", freezeRuleName)
			err = CreateVMBackupWithValidation(ctx, backupName, allVMs, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts,
				labelSelectors, BackupOrgID, sourceClusterUid, freezeRuleName, freezeRuleUid, "", "", true)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of VM backup [%s]", backupName))
		})

		Step("Performing in-place restore with retain option", func() {
			log.InfoD("Performing in-place restore with retain option")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName = fmt.Sprintf("%s-%s", "in-place-restore-retain", RandomString(6))
			restoreNames = append(restoreNames, restoreName)
			log.InfoD("Restoring the [%s] backup", backupName)
			// Not restoring with validation as it will fail for all the VMs which are gone in scheduling state
			err = CreateRestoreWithReplacePolicy(restoreName, backupName, make(map[string]string), DestinationClusterName, BackupOrgID, ctx, make(map[string]string), ReplacePolicyRetain)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s from backup %s", restoreName, backupName))
		})

		Step("Validating in place restore with retain on frozen VM", func() {
			log.InfoD("Validating in place restore with retain on frozen VM")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			expectedRestoredAppContexts := make([]*scheduler.Context, 0)
			for _, scheduledAppContext := range scheduledAppContexts {
				expectedRestoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, make(map[string]string), make(map[string]string), true)
				if err != nil {
					log.Errorf("TransformAppContextWithMappings: %v", err)
					continue
				}
				expectedRestoredAppContexts = append(expectedRestoredAppContexts, expectedRestoredAppContext)
			}
			err = ValidateRestore(ctx, restoreName, BackupOrgID, expectedRestoredAppContexts, make([]string, 0))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Validating restore [%s] with retain", restoreName))
		})

		Step("Performing in-place restore with replace option", func() {
			log.InfoD("Performing in-place restore with replace option")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName = fmt.Sprintf("%s-%s", "in-place-restore-replace", RandomString(6))
			restoreNames = append(restoreNames, restoreName)
			log.InfoD("Restoring the [%s] backup", backupName)
			// Not restoring with validation as it will fail for all the VMs which are gone in scheduling state
			err = CreateRestoreWithReplacePolicy(restoreName, backupName, make(map[string]string), DestinationClusterName, BackupOrgID, ctx, make(map[string]string), ReplacePolicyDelete)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s from backup %s", restoreName, backupName))
		})

		Step("Validating in place restore with replace", func() {
			log.InfoD("Validating in place restore with replace")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			expectedRestoredAppContexts := make([]*scheduler.Context, 0)
			for _, scheduledAppContext := range scheduledAppContexts {
				expectedRestoredAppContext, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, make(map[string]string), make(map[string]string), true)
				if err != nil {
					log.Errorf("TransformAppContextWithMappings: %v", err)
					continue
				}
				expectedRestoredAppContexts = append(expectedRestoredAppContexts, expectedRestoredAppContext)
			}
			err = ValidateRestore(ctx, restoreName, BackupOrgID, expectedRestoredAppContexts, make([]string, 0))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Validating restore [%s] with replace", restoreName))
		})

	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true

		RuleEnumerateReq := &api.RuleEnumerateRequest{
			OrgId: BackupOrgID,
		}
		ruleList, err := Inst().Backup.EnumerateRule(ctx, RuleEnumerateReq)
		for _, r := range ruleList.GetRules() {
			log.Infof("Deleting rule [%s]", r.Name)
			_, err := Inst().Backup.DeleteRule(ctx, &api.RuleDeleteRequest{
				OrgId: BackupOrgID,
				Name:  r.Name,
				Uid:   r.Uid,
			})
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting rule [%s]", r.Name))
		}

		log.Info("Destroying scheduled apps on source cluster")
		// TODO: Need to uncomment the below code once we have data validation for kubevirt VMs implemented
		//err = DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		DestroyApps(scheduledAppContexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})
