package tests

import (
	context1 "context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/task"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/node"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	corev1 "k8s.io/api/core/v1"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

// This testcase verifies backup and restore of Kubevirt VMs in different states like Running, Stopped, Restarting
var _ = Describe("{KubevirtVMBackupRestoreWithDifferentStates}", Label(TestCaseLabelsMap[KubevirtVMBackupRestoreWithDifferentStates]...), func() {

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
		allVMs                     []kubevirtv1.VirtualMachine
		allVMNames                 []string
		freezeRuleName             string
		unfreezeRuleName           string
		freezeRuleUid              string
		unfreezeRuleUid            string
		//controlChannel             chan string
		//errorGroup                 *errgroup.Group
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
			freezeRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, freezeRuleName)
			log.FailOnError(err, "Failed to get freeze rule uid for rule %s", freezeRuleName)
			unfreezeRuleName = fmt.Sprintf("vm-unfreeze-rule-%s", RandomString(4))
			err = CreateRuleForVMBackup(unfreezeRuleName, allVMs, Unfreeze, ctx)
			log.FailOnError(err, "Failed to create unfreeze rule %s for VMs - %v", unfreezeRuleName, allVMNames)
			unfreezeRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, unfreezeRuleName)
			log.FailOnError(err, "Failed to get unfreeze rule uid for rule %s", unfreezeRuleName)
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
					log.InfoD("creating backup [%s] in source cluster [%s] (%s), organization [%s], of namespace [%s], in backup location [%s]", backupName, SourceClusterName, sourceClusterUid, BackupOrgID, appCtx.ScheduleOptions.Namespace, backupLocationName)
					err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, []*scheduler.Context{appCtx}, labelSelectors, BackupOrgID, sourceClusterUid, freezeRuleName, freezeRuleUid, unfreezeRuleName, unfreezeRuleUid)
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
				namespaceMappingMixed[appCtx.ScheduleOptions.Namespace] = appCtx.ScheduleOptions.Namespace + "-mxd"
			}
			backupWithVMMixed = fmt.Sprintf("%s-%s", "auto-backup-mixed", RandomString(6))
			backupNames = append(backupNames, backupWithVMMixed)
			log.InfoD("creating backup [%s] in cluster [%s] (%s), organization [%s], of namespace [%v], in backup location [%s]", backupWithVMMixed, SourceClusterName, sourceClusterUid, BackupOrgID, namespaces, backupLocationName)
			err = CreateBackupWithValidation(ctx, backupWithVMMixed, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts,
				nil, BackupOrgID, sourceClusterUid, freezeRuleName, freezeRuleUid, unfreezeRuleName, unfreezeRuleUid)
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
				namespaceMappingRestart[appCtx.ScheduleOptions.Namespace] = appCtx.ScheduleOptions.Namespace + "-rst"
			}
			backupWithVMRestart = fmt.Sprintf("%s-%s", "auto-backup-restart", RandomString(6))
			backupNames = append(backupNames, backupWithVMRestart)
			log.InfoD("creating backup [%s] in cluster [%s] (%s), organization [%s], of namespace [%v], in backup location [%s]", backupWithVMRestart, SourceClusterName, sourceClusterUid, BackupOrgID, namespaces, backupLocationName)
			_, err = CreateBackupWithoutCheck(ctx, backupWithVMRestart, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, BackupOrgID, sourceClusterUid, freezeRuleName, freezeRuleUid, unfreezeRuleName, unfreezeRuleUid)
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
				namespaceMappingStopped[appCtx.ScheduleOptions.Namespace] = appCtx.ScheduleOptions.Namespace + "-stp"
			}
			backupWithVMStopped = fmt.Sprintf("%s-%s", "auto-backup-stopped", RandomString(6))
			backupNames = append(backupNames, backupWithVMStopped)
			log.InfoD("creating backup [%s] in cluster [%s] (%s), organization [%s], of namespace [%v], in backup location [%s]", backupWithVMStopped, SourceClusterName, sourceClusterUid, BackupOrgID, namespaces, backupLocationName)
			err = CreateBackupWithValidation(ctx, backupWithVMStopped, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts,
				nil, BackupOrgID, sourceClusterUid, freezeRuleName, freezeRuleUid, unfreezeRuleName, unfreezeRuleUid)
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
		DestroyApps(scheduledAppContexts, opts)
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
var _ = Describe("{KubevirtUpgradeTest}", Label(TestCaseLabelsMap[KubevirtUpgradeTest]...), func() {

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
		//controlChannel       chan string
		//errorGroup           *errgroup.Group
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

// This testcase verifies Simultaneous backups/ Simultaneous Backup and Restore/ Simultaneous Restore and Source Virtual Machine Deletion
var _ = Describe("{KubevirtVMBackupOrDeletionInProgress}", Label(TestCaseLabelsMap[KubevirtVMBackupOrDeletionInProgress]...), func() {
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
var _ = Describe("{KubevirtVMBackupRestoreWithNodeSelector}", Label(TestCaseLabelsMap[KubevirtVMBackupRestoreWithNodeSelector]...), func() {

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
		ctx                          context1.Context
		err                          error
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
		ctx, err = backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
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
			log.InfoD("Getting the total number of worker nodes in destination cluster and applying label to one of the worker nodes")
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
			log.InfoD("Adding correct node selector to few virtual machine instances")
			// Adding node selector which is already added as label on destination to few virtual machines
			for _, namespace := range namespacewithcorrectlabels {
				vms, err := GetAllVMsInNamespace(namespace)
				if err != nil {
					return
				}
				for _, vm := range vms {
					err = AddNodeToVirtualMachine(vm, nodeSelectorPresent)
					log.FailOnError(err, "Unable to apply node selector to VM")
				}
			}
		})

		Step("Adding incorrect node selector to few virtual machine instances", func() {
			log.InfoD("Adding incorrect node selector to few virtual machine instances")
			// Adding node selector which is NOT added as label on destination to few virtual machines
			for _, namespace := range namespacewithincorrectlabels {
				vms, err := GetAllVMsInNamespace(namespace)
				if err != nil {
					return
				}
				for _, vm := range vms {
					err = AddNodeToVirtualMachine(vm, nodeSelectorNotPresent)
					log.FailOnError(err, "Unable to apply node selector to VM")
				}
			}
		})

		Step("Take backup of all namespaces with VMs having correct and incorrect labels", func() {
			log.InfoD("Take backup of all namespaces with VMs having correct and incorrect labels")
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
			restoreAll := fmt.Sprintf("%s-%s", "auto-restore-all", RandomString(6))
			restoreNames = append(restoreNames, restoreAll)
			log.InfoD("Restoring the [%s] backup", backupNames[0])
			// Not restoring with validation as it will fail for all the VMs which are gone in scheduling state
			err = CreateRestore(restoreAll, backupNames[0], make(map[string]string), DestinationClusterName, BackupOrgID, ctx, make(map[string]string))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s from backup %s", restoreAll, backupNames[0]))
		})

		Step("Validating restore for all Virtual Machine Instances expected to be running", func() {
			log.InfoD("Validating restore for all Virtual Machine Instances expected to be running")
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
			dash.VerifyFatal(err, nil, fmt.Sprintf("Restore Validation for [%s]", restoreNames[0]))
		})

		Step("Verifying state and nodes for all restored Virtual Machine Instances", func() {
			defer func() {
				log.InfoD("switching to default context")
				err := SetClusterContext("")
				log.FailOnError(err, "failed to SetClusterContext to default cluster")
			}()

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

var _ = Describe("{KubevirtVMWithFreezeUnfreeze}", Label(TestCaseLabelsMap[KubevirtVMWithFreezeUnfreeze]...), func() {
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

var _ = Describe("{KubevirtInPlaceRestoreWithReplaceAndRetain}", Label(TestCaseLabelsMap[KubevirtInPlaceRestoreWithReplaceAndRetain]...), func() {
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
			err = CreateRestoreWithReplacePolicy(restoreName, backupName, make(map[string]string), SourceClusterName, BackupOrgID, ctx, make(map[string]string), ReplacePolicyRetain)
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
			err = CreateRestoreWithReplacePolicy(restoreName, backupName, make(map[string]string), SourceClusterName, BackupOrgID, ctx, make(map[string]string), ReplacePolicyDelete)
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

var _ = Describe("{KubevirtVMRestoreWithAfterChangingVMConfig}", Label(TestCaseLabelsMap[KubevirtVMRestoreWithAfterChangingVMConfig]...), func() {
	var (
		// TODO: Need to uncomment the below code once we have data validation for kubevirt VMs implemented
		//controlChannel       chan string
		//errorGroup           *errgroup.Group
		restoreNames            []string
		restoreName             string
		scheduledAppContexts    []*scheduler.Context
		restoredAppContexts     []*scheduler.Context
		sourceClusterUid        string
		cloudCredName           string
		cloudCredUID            string
		backupLocationUID       string
		backupLocationName      string
		backupLocationMap       map[string]string
		providers               []string
		labelSelectors          map[string]string
		backupName              string
		backupNames             []string
		numberOfVolumes         int
		virtLauncherPodMap      map[string]map[string]string
		numberOfAdditionalDisks int
		diskCount               int
		bootTime                time.Duration
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("KubevirtVMRestoreWithAfterChangingVMConfig", "Verify the restore of VM backup on the destination cluster in a namespace where the same VM is already running with Replace/Retain option by changing the number of disks",
			nil, 296421, Mkoppal, Q1FY25)

		backupLocationMap = make(map[string]string)
		virtLauncherPodMap = make(map[string]map[string]string)
		providers = GetBackupProviders()
		numberOfAdditionalDisks = 2
		bootTime = 3 * time.Minute

		log.InfoD("scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		appList := Inst().AppList
		numberOfVolumes = 5
		defer func() {
			Inst().AppList = appList
		}()
		Inst().AppList = []string{"kubevirt-cirros-cd-with-pvc"}
		Inst().CustomAppConfig["kubevirt-cirros-cd-with-pvc"] = scheduler.AppConfig{
			ClaimsCount: numberOfVolumes,
		}
		err := Inst().S.RescanSpecs(Inst().SpecDir, Inst().V.String())
		log.FailOnError(err, "Failed to rescan specs from %s for storage provider %s", Inst().SpecDir, Inst().V.String())
		taskName := fmt.Sprintf("%d", 296421)
		appContexts := ScheduleApplications(taskName)
		for _, appCtx := range appContexts {
			appCtx.ReadinessTimeout = AppReadinessTimeout
			scheduledAppContexts = append(scheduledAppContexts, appCtx)
		}
	})

	It("Verify the restore of VM backup on the destination cluster in a namespace where the same VM is already running with Replace/Retain option by changing the number of disks", func() {

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

		Step("Taking backup of kubevirt VM", func() {
			log.InfoD("Taking backup of kubevirt VM")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			backupName = fmt.Sprintf("%s-%s", "vm-backup", RandomString(6))
			backupNames = append(backupNames, backupName)
			vms, err := GetAllVMsFromScheduledContexts(scheduledAppContexts)
			log.FailOnError(err, "Failed to get VMs from scheduled contexts")
			var vmNames []string
			for _, v := range vms {
				vmNames = append(vmNames, v.Name)
			}
			log.Infof("VMs to be backed up - %v", vmNames)
			err = CreateVMBackupWithValidation(ctx, backupName, vms, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts,
				labelSelectors, BackupOrgID, sourceClusterUid, "", "", "", "", false)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of VM backup [%s]", backupName))
		})

		Step("Performing restore on destination cluster", func() {
			log.InfoD("Performing restore on destination cluster")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName = fmt.Sprintf("%s-%s", "vm-restore", RandomString(6))
			restoreNames = append(restoreNames, restoreName)
			log.InfoD("Restoring the [%s] backup", backupName)
			err = CreateRestoreWithValidation(ctx, restoreName, backupName, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContexts)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s from backup %s", restoreName, backupName))
			log.Infof("Waiting for the VMs to boot for [%s]", bootTime)
			time.Sleep(bootTime)
		})

		Step("Restoring the same backup with retain option and verify that VM does not restart", func() {
			log.InfoD("Restoring the same backup with retain option and verify that VM does not restart")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			defer func() {
				err := SetSourceKubeConfig()
				log.FailOnError(err, "failed to switch context to source cluster")
			}()
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "failed to switch to context to destination cluster")
			for _, scheduledAppContext := range scheduledAppContexts {
				c, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, make(map[string]string), make(map[string]string), true)
				if err != nil {
					log.Errorf("TransformAppContextWithMappings: %v", err)
					continue
				}
				restoredAppContexts = append(restoredAppContexts, c)
			}
			vms, err := GetAllVMsFromScheduledContexts(restoredAppContexts)
			log.FailOnError(err, "Failed to get VMs from scheduled contexts")
			for _, v := range vms {
				name, err := GetVirtLauncherPodName(v)
				log.FailOnError(err, "Fetching virt launcher pod name for VM - [%s]", v.Name)
				if virtLauncherPodMap[v.Namespace] == nil {
					virtLauncherPodMap[v.Namespace] = make(map[string]string)
				}
				virtLauncherPodMap[v.Namespace][v.Name] = name
			}
			err = SetSourceKubeConfig()
			log.FailOnError(err, "failed to switch context to source cluster")
			restoreName = fmt.Sprintf("%s-%s", "vm-restore-retain", RandomString(6))
			restoreNames = append(restoreNames, restoreName)
			log.InfoD("Restoring the [%s] backup", backupName)
			err = CreateRestoreWithReplacePolicyWithValidation(restoreName, backupName, make(map[string]string), DestinationClusterName, BackupOrgID, ctx, make(map[string]string), ReplacePolicyRetain, scheduledAppContexts)
			log.FailOnError(err, "Validating restore [%s] with retain", restoreName)
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "failed to switch to context to destination cluster")
			for _, v := range vms {
				name, err := GetVirtLauncherPodName(v)
				log.FailOnError(err, "Fetching virt launcher pod name for VM - [%s]", v.Name)
				log.InfoD("Virt launcher pod name for VM - [%s] in namespace [%s] is [%s]", v.Name, v.Namespace, name)
				dash.VerifyFatal(name, virtLauncherPodMap[v.Namespace][v.Name], fmt.Sprintf("Verifying that VM [%s] in namespace [%s] did not restart", v.Name, v.Namespace))
				virtLauncherPodMap[v.Namespace][v.Name] = name
			}
		})

		Step("Collecting the number of disks in the VMs", func() {
			log.InfoD("Collecting the number of disks in the VMs")
			defer func() {
				err := SetSourceKubeConfig()
				log.FailOnError(err, "failed to switch context to source cluster")
			}()
			err := SetDestinationKubeConfig()
			log.FailOnError(err, "failed to switch to context to destination cluster")
			vms, err := GetAllVMsFromScheduledContexts(scheduledAppContexts)
			log.FailOnError(err, "Failed to get VMs from scheduled contexts")
			for _, v := range vms {
				t := func() (interface{}, bool, error) {
					diskCountOutput, err := GetNumberOfDisksInVM(v)
					if err != nil {
						return nil, false, fmt.Errorf("failed to get number of disks in VM [%s] in namespace [%s]", v.Name, v.Namespace)
					}
					// Total disks will be numberOfVolumes plus the container disk
					if diskCountOutput != numberOfVolumes+1 {
						return nil, true, fmt.Errorf("expected number of disks in VM [%s] in namespace [%s] is [%d] but got [%d]", v.Name, v.Namespace, numberOfVolumes+1, diskCountOutput)
					}
					return diskCountOutput, false, nil
				}
				d, err := task.DoRetryWithTimeout(t, bootTime, 30*time.Second)
				log.FailOnError(err, "Failed to get number of disks in VM [%s] in namespace [%s] after retry", v.Name, v.Namespace)
				diskCount = d.(int)
				log.InfoD("Number of disks in VM [%s] in namespace [%s] is [%d]", v.Name, v.Namespace, diskCount)
			}
		})

		Step("Restoring the same backup with replace option and verify that VM restarts", func() {
			log.InfoD("Restoring the same backup with replace option and verify that VM restarts")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName = fmt.Sprintf("%s-%s", "vm-restore-replace", RandomString(6))
			restoreNames = append(restoreNames, restoreName)
			log.InfoD("Restoring the [%s] backup", backupName)
			err = CreateRestoreWithReplacePolicyWithValidation(restoreName, backupName, make(map[string]string), DestinationClusterName, BackupOrgID, ctx, make(map[string]string), ReplacePolicyDelete, scheduledAppContexts)
			log.FailOnError(err, "Validating restore [%s] with retain", restoreName)
			time.Sleep(bootTime)
			defer func() {
				err := SetSourceKubeConfig()
				log.FailOnError(err, "failed to switch context to source cluster")
			}()
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "failed to switch to context to destination cluster")
			vms, err := GetAllVMsFromScheduledContexts(restoredAppContexts)
			log.FailOnError(err, "Failed to get VMs from scheduled contexts")
			for _, v := range vms {
				name, err := GetVirtLauncherPodName(v)
				log.FailOnError(err, "Fetching virt launcher pod name for VM - [%s]", v.Name)
				log.InfoD("Virt launcher pod name for VM - [%s] in namespace [%s] is [%s]", v.Name, v.Namespace, name)
				dash.VerifyFatal(name != virtLauncherPodMap[v.Namespace][v.Name], true, fmt.Sprintf("Verifying that VM [%s] in namespace [%s] restarted", v.Name, v.Namespace))
				virtLauncherPodMap[v.Namespace][v.Name] = name
			}
		})

		Step("Adding additional disks to the VMs and restarting it", func() {
			log.InfoD("Adding additional disks to the VMs and restarting it")
			for _, appCtx := range scheduledAppContexts {
				vms, err := GetAllVMsFromScheduledContexts([]*scheduler.Context{appCtx})
				log.FailOnError(err, "Failed to get VMs from scheduled contexts")
				for _, v := range vms {
					pvcs, err := CreatePVCsForVM(v, numberOfAdditionalDisks, "kubevirt-sc-for-cirros-cd", "5Gi")
					log.FailOnError(err, "Failed to create PVCs for VM [%s] in namespace [%s]", v.Name, v.Namespace)
					specListInterfaces := make([]interface{}, len(pvcs))
					for i, pvc := range pvcs {
						// Converting each PVC to interface for appending to SpecList
						specListInterfaces[i] = pvc
					}
					appCtx.App.SpecList = append(appCtx.App.SpecList, specListInterfaces...)

					err = AddPVCsToVirtualMachine(v, pvcs)
					log.FailOnError(err, "Failed to add PVCs to VM [%s] in namespace [%s]", v.Name, v.Namespace)

					err = RestartKubevirtVM(v.Name, v.Namespace, true)
					log.FailOnError(err, "Failed to restart VM [%s] in namespace [%s]", v.Name, v.Namespace)

					// Perhaps moving it outside all loops is more efficient.
					log.Infof("Waiting for VM [%s] in namespace [%s] to boot. Sleeping for %d minutes...", v.Name, v.Namespace, bootTime)
					time.Sleep(bootTime)
				}
			}

		})

		Step("Collecting the number of disks in the VMs after adding disks", func() {
			log.InfoD("Collecting the number of disks in the VMs after adding disks")
			// Creating a new slice of VMs because the VM UID would have changed after restart
			vms, err := GetAllVMsFromScheduledContexts(scheduledAppContexts)
			log.FailOnError(err, "Failed to get VMs from scheduled contexts")
			for _, v := range vms {
				t := func() (interface{}, bool, error) {
					diskCountOutput, err := GetNumberOfDisksInVM(v)
					if err != nil {
						return nil, false, fmt.Errorf("failed to get number of disks in VM [%s] in namespace [%s]", v.Name, v.Namespace)
					}
					// Total disks will be numberOfVolumes plus the container disk
					if diskCountOutput != diskCount+numberOfAdditionalDisks {
						return nil, true, fmt.Errorf("expected number of disks in VM [%s] in namespace [%s] is [%d] but got [%d]", v.Name, v.Namespace, diskCount+numberOfAdditionalDisks, diskCountOutput)
					}
					return diskCountOutput, false, nil
				}
				d, err := task.DoRetryWithTimeout(t, bootTime, 30*time.Second)
				log.FailOnError(err, "Failed to get number of disks in VM [%s] in namespace [%s] after retry", v.Name, v.Namespace)
				diskCountAfterAddingDisks := d.(int)
				log.InfoD("Number of disks in VM [%s] in namespace [%s] is [%d] after adding [%d] disks", v.Name, v.Namespace, diskCountAfterAddingDisks, numberOfAdditionalDisks)
			}
		})

		Step("Backup the VMs after disk addition", func() {
			log.InfoD("Backup the VMs after disk addition")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			vms, err := GetAllVMsFromScheduledContexts(scheduledAppContexts)
			log.FailOnError(err, "Failed to get VMs from scheduled contexts")
			backupName = fmt.Sprintf("%s-%s", "backup-after-disk-addition", RandomString(6))
			backupNames = append(backupNames, backupName)
			err = CreateVMBackupWithValidation(ctx, backupName, vms, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts,
				labelSelectors, BackupOrgID, sourceClusterUid, "", "", "", "", false)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of VM backup [%s]", backupName))

		})

		Step("Performing restore on destination cluster after disk addition", func() {
			log.InfoD("Performing restore on destination cluster after disk addition")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName = fmt.Sprintf("%s-%s", "restore-with-additional-disks-replace", RandomString(6))
			restoreNames = append(restoreNames, restoreName)
			log.InfoD("Restoring the [%s] backup", backupName)
			err = CreateRestoreWithReplacePolicyWithValidation(restoreName, backupName, make(map[string]string), DestinationClusterName, BackupOrgID, ctx, make(map[string]string), ReplacePolicyDelete, scheduledAppContexts)
			log.FailOnError(err, "Validating restore [%s] with replace", restoreName)
			// Waiting for VMs to restart and complete boot process otherwise the disk count will be incorrect
			log.Infof("Waiting for VM to boot. Sleeping for %d minutes...", bootTime)
			time.Sleep(bootTime)
		})

		Step("Verifying the additional disks added to VM after restore", func() {
			log.InfoD("Verifying the additional disks added to VM after restore")
			// Creating newRestoredAppContexts since the scheduledAppContext has changed after adding additional PVCs to it
			var newRestoredAppContexts []*scheduler.Context
			defer func() {
				err := SetSourceKubeConfig()
				log.FailOnError(err, "failed to switch context to source cluster")
			}()
			err := SetDestinationKubeConfig()
			log.FailOnError(err, "failed to switch to context to destination cluster")
			for _, scheduledAppContext := range scheduledAppContexts {
				c, err := CloneAppContextAndTransformWithMappings(scheduledAppContext, make(map[string]string), make(map[string]string), true)
				if err != nil {
					log.Errorf("TransformAppContextWithMappings: %v", err)
					continue
				}
				newRestoredAppContexts = append(newRestoredAppContexts, c)
			}
			vms, err := GetAllVMsFromScheduledContexts(newRestoredAppContexts)
			log.FailOnError(err, "Failed to get VMs from scheduled contexts")
			for _, v := range vms {
				t := func() (interface{}, bool, error) {
					diskCountOutput, err := GetNumberOfDisksInVM(v)
					if err != nil {
						return nil, false, fmt.Errorf("failed to get number of disks in VM [%s] in namespace [%s]", v.Name, v.Namespace)
					}
					// Total disks will be numberOfVolumes plus the container disk
					if diskCountOutput != diskCount+numberOfAdditionalDisks {
						return nil, true, fmt.Errorf("expected number of disks in VM [%s] in namespace [%s] is [%d] but got [%d]", v.Name, v.Namespace, diskCount+numberOfAdditionalDisks, diskCountOutput)
					}
					return diskCountOutput, false, nil
				}
				d, err := task.DoRetryWithTimeout(t, bootTime, 30*time.Second)
				log.FailOnError(err, "Failed to get number of disks in VM [%s] in namespace [%s] after retry", v.Name, v.Namespace)
				diskCountAfterAddingDisks := d.(int)
				log.InfoD("Number of disks in VM [%s] in namespace [%s] is [%d] after adding [%d] disks", v.Name, v.Namespace, diskCountAfterAddingDisks, numberOfAdditionalDisks)
				dash.VerifyFatal(diskCountAfterAddingDisks, diskCount+numberOfAdditionalDisks, fmt.Sprintf("Verifying that VM [%s] in namespace [%s] has [%d] disks after restore", v.Name, v.Namespace, diskCount+numberOfAdditionalDisks))
			}
		})

	})

	JustAfterEach(func() {
		log.Infof("Test execution completed")
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

// This testcase verifies default backup & restore with both Kubevirt VMs and non-kubevirt resources
var _ = Describe("{DefaultBackupRestoreWithKubevirtAndNonKubevirtNS}", Label(TestCaseLabelsMap[DefaultBackupRestoreWithKubevirtAndNonKubevirtNS]...), func() {
	var (
		backupName                 string
		restoreName                string
		scheduleBackupName         string
		backupNames                []string
		scheduleNames              []string
		scheduledAppContexts       []*scheduler.Context
		singleScheduledAppContexts []*scheduler.Context
		multiScheduledAppContexts  []*scheduler.Context
		sourceClusterUID           string
		cloudCredName              string
		cloudCredUID               string
		backupLocationUID          string
		backupLocationName         string
		backupLocationMap          map[string]string
		labelSelectors             map[string]string
		providers                  []string
		appNamespaces              []string
		schPolicyUid               string
		periodicPolicyName         string
		preRuleName                string
		postRuleName               string
		preRuleUid                 string
		postRuleUid                string
		preRuleNames               []string
		postRuleNames              []string
		allVMs                     []kubevirtv1.VirtualMachine
		allVMNames                 []string
		preRuleList                []*api.RulesInfo_RuleItem
		postRuleList               []*api.RulesInfo_RuleItem
		testAppList                []string
	)

	backupLocationMap = make(map[string]string)
	backupNames = make([]string, 0)
	labelSelectors = make(map[string]string)
	appNamespaces = make([]string, 0)
	backupNamespaceMap := make(map[string]string)
	periodicPolicyName = fmt.Sprintf("%s-%s", "periodic", RandomString(6))
	testAppList = []string{"mysql-backup", "kubevirt-cirros-cd-with-pvc"}

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("DefaultBackupRestoreWithKubevirtAndNonKubevirtNS", "Verify default backup & restore with both Kubevirt and Non-Kubevirt namespaces", nil, 93006, Vpinisetti, Q1FY25)
		providers = GetBackupProviders()
		actualAppList := Inst().AppList
		defer func() {
			Inst().AppList = actualAppList
		}()
		Inst().AppList = testAppList
		log.InfoD("Deploying all provided applications in a single namespace")
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d-%d", TaskNamePrefix, 93006, i)
			namespace := fmt.Sprintf("single-ns-multi-app-%s-%v", taskName, time.Now().Unix())
			singleScheduledAppContexts = ScheduleApplicationsOnNamespace(namespace, taskName)
			appNamespaces = append(appNamespaces, namespace)
			scheduledAppContexts = append(scheduledAppContexts, singleScheduledAppContexts...)
			for _, appCtx := range singleScheduledAppContexts {
				appCtx.ReadinessTimeout = AppReadinessTimeout
			}
		}
		log.InfoD("Deploying all provided applications in separate namespaces")
		multiScheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			multiScheduledAppContexts = ScheduleApplications(taskName)
			scheduledAppContexts = append(scheduledAppContexts, multiScheduledAppContexts...)
			for _, appCtx := range multiScheduledAppContexts {
				namespace := GetAppNamespace(appCtx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				appCtx.ReadinessTimeout = AppReadinessTimeout
			}
		}

	})

	It("Verify default backup & restore with both Kubevirt and Non-Kubevirt namespaces", func() {
		defer func() {
			log.InfoD("Switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "Failed to set ClusterContext to default cluster")
		}()

		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Create cloud credentials and backup location", func() {
			log.InfoD("Creating cloud credentials and backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, RandomString(6))
				backupLocationName = fmt.Sprintf("%s-%v", getGlobalBucketName(provider), RandomString(6))
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, BackupOrgID, provider))
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, "Creation of backup location")
			}
		})

		Step("Register clusters for backup & restore", func() {
			log.InfoD("Registering clusters for backup & restore")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")

			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination clusters")

			clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))

			sourceClusterUID, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))

			clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, DestinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", DestinationClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", DestinationClusterName))
		})

		Step("Create schedule policies", func() {
			log.InfoD("Creating schedule policies")
			periodicSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(10, 15, 2)
			periodicPolicyStatus := Inst().Backup.BackupSchedulePolicy(periodicPolicyName, uuid.New(), BackupOrgID, periodicSchedulePolicyInfo)
			dash.VerifyFatal(periodicPolicyStatus, nil, fmt.Sprintf("Creation of periodic schedule policy - %s", periodicPolicyName))
		})

		Step("Create pre & post exec rules", func() {
			log.InfoD("Creating pre & post exec rules")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			// Application pre & post exec rules
			preRuleName, postRuleName, err = CreateRuleForBackupWithMultipleApplications(BackupOrgID, testAppList, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of pre & post exec rules"))
			if preRuleName != "" {
				preRuleNames = append(preRuleNames, preRuleName)
			}
			if postRuleName != "" {
				postRuleNames = append(postRuleNames, postRuleName)
			}
			// Vm freeze and unfreeze rules
			for _, namespace := range appNamespaces {
				vms, err := GetAllVMsInNamespace(namespace)
				log.FailOnError(err, "Failed to get VMs in namespace - %s", namespace)
				allVMs = append(allVMs, vms...)
			}
			for _, v := range allVMs {
				allVMNames = append(allVMNames, v.Name)
			}
			freezeRuleName := fmt.Sprintf("vm-freeze-rule-%s", RandomString(4))
			err = CreateRuleForVMBackup(freezeRuleName, allVMs, Freeze, ctx)
			log.FailOnError(err, "Failed to create freeze rule %s for VMs - %v", freezeRuleName, allVMNames)
			unfreezeRuleName := fmt.Sprintf("vm-unfreeze-rule-%s", RandomString(4))
			err = CreateRuleForVMBackup(unfreezeRuleName, allVMs, Unfreeze, ctx)
			log.FailOnError(err, "Failed to create unfreeze rule %s for VMs - %v", unfreezeRuleName, allVMNames)
			preRuleNames = append(preRuleNames, freezeRuleName)
			postRuleNames = append(postRuleNames, unfreezeRuleName)

			// processing pre exec rules
			log.InfoD("Actual pre rules are %v", preRuleNames)
			for _, preRule := range preRuleNames {
				preRuleID, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRule)
				log.FailOnError(err, "Fetching pre backup rule [%s] uid", preRule)
				log.Infof("Pre backup rule name [%s] & uid [%s]", preRule, preRuleID)
				ruleInspectRequest := &api.RuleInspectRequest{
					OrgId: BackupOrgID,
					Name:  preRule,
					Uid:   preRuleID,
				}
				resp, _ := Inst().Backup.InspectRule(ctx, ruleInspectRequest)
				for _, rule := range resp.GetRule().GetRules() {
					preRuleList = append(preRuleList, rule)
				}
			}

			preRuleName = fmt.Sprintf("final-pre-exec-rule-%s", RandomString(4))
			preRuleCreateReq := &api.RuleCreateRequest{
				CreateMetadata: &api.CreateMetadata{
					Name:  preRuleName,
					OrgId: BackupOrgID,
				},
				RulesInfo: &api.RulesInfo{
					preRuleList,
				},
			}
			log.InfoD("Creating final pre backup rule [%s]", preRuleName)
			_, err = Inst().Backup.CreateRule(ctx, preRuleCreateReq)
			log.FailOnError(err, "Failed while creating final pre backup rule [%s]", preRuleName)
			preRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleName)
			log.FailOnError(err, "Fetching final pre backup rule [%s] uid", preRuleName)
			log.Infof("Final pre backup rule [%s] with uid [%s]", preRuleName, preRuleUid)

			// processing post exec rules
			log.InfoD("Actual post rules are : %v", postRuleNames)
			for _, postRule := range postRuleNames {
				log.InfoD("Processing post rule [%s]", postRule)
				postRuleID, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRule)
				log.FailOnError(err, "Fetching post backup rule [%s] uid", postRule)
				log.Infof("Post backup rule name [%s] & uid [%s]", postRule, postRuleID)
				ruleInspectRequest := &api.RuleInspectRequest{
					OrgId: BackupOrgID,
					Name:  postRule,
					Uid:   postRuleID,
				}
				resp, _ := Inst().Backup.InspectRule(ctx, ruleInspectRequest)
				for _, rule := range resp.GetRule().GetRules() {
					postRuleList = append(postRuleList, rule)
				}
			}

			postRuleName = fmt.Sprintf("final-post-exec-rule-%s", RandomString(4))
			postRuleCreateReq := &api.RuleCreateRequest{
				CreateMetadata: &api.CreateMetadata{
					Name:  postRuleName,
					OrgId: BackupOrgID,
				},
				RulesInfo: &api.RulesInfo{
					postRuleList,
				},
			}
			log.InfoD("Creating final post backup rule [%s]", postRuleName)
			_, err = Inst().Backup.CreateRule(ctx, postRuleCreateReq)
			log.FailOnError(err, "Failed while creating final post backup rule [%s]", postRuleName)
			postRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleName)
			log.FailOnError(err, "Fetching final post backup rule [%s] uid", postRuleName)
			log.Infof("Final post backup rule [%s] with uid: [%s]", postRuleName, postRuleUid)
		})

		// Manual backups with pre & post exec rules
		Step("Creating manual backup with single namespace contains both kubevirt and non-kubevirt using exec rules", func() {
			log.InfoD("Creating manual backup with single namespace contains both kubevirt and non-kubevirt using exec rules")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			backupName = fmt.Sprintf("manual-rule-single-ns-multi-apps-%v", RandomString(6))
			backupNames = append(backupNames, backupName)
			log.InfoD("Creating a backup [%s] in [%s] with namespace [%s] in backup location [%s]", backupName, SourceClusterName, appNamespaces[0], backupLocationName)
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, singleScheduledAppContexts, labelSelectors, BackupOrgID, sourceClusterUID, preRuleName, preRuleUid, postRuleName, postRuleUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of single manual backup [%s] with single namespace contains multiple apps using exec rules.", backupName))
		})

		Step("Creating single manual backup with all namespaces using exec rules", func() {
			log.InfoD(fmt.Sprintf("Creating single manual backup with all namespaces using exec rules : %v", appNamespaces))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			backupName = fmt.Sprintf("manual-rule-all-ns-%v", RandomString(6))
			backupNames = append(backupNames, backupName)
			log.InfoD("Creating a backup [%s] in [%s] with all namespaces %s in backup location [%s]", backupName, SourceClusterName, appNamespaces, backupLocationName)
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, BackupOrgID, sourceClusterUID, preRuleName, preRuleUid, postRuleName, postRuleUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of single manual backup [%s] with multiple namespaces %v using exec rules", backupName, appNamespaces))
		})

		// Schedule backups with pre & post exec rules
		Step("Creating schedule backup with single namespace contains both kubevirt and non-kubevirt using exec rules", func() {
			log.InfoD("Creating schedule backup with single namespace contains both kubevirt and non-kubevirt using exec rules")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			schPolicyUid, _ = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicPolicyName)
			backupName = fmt.Sprintf("schdule-rule-single-ns-multi-apps-%v", RandomString(6))
			scheduleNames = append(scheduleNames, backupName)
			log.InfoD("Creating a backup [%s] in [%s] with namespace [%s] in backup location [%s]", backupName, SourceClusterName, appNamespaces[0], backupLocationName)
			scheduleBackupName, err = CreateScheduleBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, singleScheduledAppContexts, labelSelectors, BackupOrgID, preRuleName, preRuleUid, postRuleName, postRuleUid, periodicPolicyName, schPolicyUid)
			backupNames = append(backupNames, scheduleBackupName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of single schedule backup [%s] with single namespace contains multiple apps using exec rules.", scheduleBackupName))
		})

		Step("Creating single schedule backup with all namespaces", func() {
			log.InfoD("Creating single schedule backup with all namespaces")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			schPolicyUid, _ = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicPolicyName)
			backupName = fmt.Sprintf("schedule-rule-all-ns-%v", RandomString(6))
			scheduleNames = append(scheduleNames, backupName)
			log.InfoD("Creating a backup [%s] in [%s] with namespaces %s in backup location [%s]", backupName, SourceClusterName, appNamespaces, backupLocationName)
			scheduleBackupName, err = CreateScheduleBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, BackupOrgID, preRuleName, preRuleUid, postRuleName, postRuleUid, periodicPolicyName, schPolicyUid)
			backupNames = append(backupNames, scheduleBackupName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of single schedule backup [%s] with all namespaces %s using exec rules", scheduleBackupName, appNamespaces))
		})

		// Manual backups
		Step("Creating manual backup with single namespace contains both kubevirt and non-kubevirt apps", func() {
			log.InfoD("Creating manual backup with single namespace contains both kubevirt and non-kubevirt apps")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			backupName = fmt.Sprintf("manual-single-ns-multi-apps-%v", RandomString(6))
			backupNames = append(backupNames, backupName)
			log.InfoD("Creating a backup [%s] in [%s] with namespace [%s] in backup location [%s]", backupName, SourceClusterName, appNamespaces[0], backupLocationName)
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, singleScheduledAppContexts, labelSelectors, BackupOrgID, sourceClusterUID, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of single manual backup [%s] with single namespace contains multiple apps.", backupName))
		})

		Step("Creating single manual backup with all namespaces", func() {
			log.InfoD(fmt.Sprintf("Creating single backup with all namespaces %v", appNamespaces))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			backupName = fmt.Sprintf("manual-all-ns-%v", RandomString(6))
			backupNames = append(backupNames, backupName)
			log.InfoD("Creating a backup [%s] in [%s] with all namespaces [%s] in backup location [%s]", backupName, SourceClusterName, appNamespaces, backupLocationName)
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, BackupOrgID, sourceClusterUID, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of single manual backup [%s] with multiple namespaces %v", backupName, appNamespaces))
		})

		Step("Creating multiple manual backups with each namespace", func() {
			log.InfoD("Creating multiple manual backups with each namespace")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			for i, appCtx := range multiScheduledAppContexts {
				scheduledNamespace := appCtx.ScheduleOptions.Namespace
				backupName = fmt.Sprintf("%s-%s-%v", "manual", scheduledNamespace, RandomString(6))
				backupNames = append(backupNames, backupName)
				log.InfoD("Creating backup [%s] in [%s] with namespace [%s] in backup location [%s]", backupName, SourceClusterName, scheduledNamespace, backupLocationName)
				err := CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, multiScheduledAppContexts[i:i+1], labelSelectors, BackupOrgID, sourceClusterUID, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of multiple manual backups [%s] with each namespace [%s]", backupName, scheduledNamespace))
			}
		})

		// Schedule backups
		Step("Creating schedule backup with single namespace contains both kubevirt and non-kubevirt apps", func() {
			log.InfoD("Creating schedule backup with single namespace contains both kubevirt and non-kubevirt apps")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			schPolicyUid, _ = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicPolicyName)
			backupName = fmt.Sprintf("schdule-single-ns-multi-apps-%v", RandomString(6))
			scheduleNames = append(scheduleNames, backupName)
			log.InfoD("Creating a backup [%s] in [%s] with namespace [%s] in backup location [%s]", backupName, SourceClusterName, appNamespaces[0], backupLocationName)
			scheduleBackupName, err = CreateScheduleBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, singleScheduledAppContexts, labelSelectors, BackupOrgID, "", "", "", "", periodicPolicyName, schPolicyUid)
			backupNames = append(backupNames, scheduleBackupName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of single schedule backup [%s] with single namespace contains multiple apps.", scheduleBackupName))
		})

		Step("Creating single schedule backup with all the namespaces", func() {
			log.InfoD("Creating single schedule backup with all the namespaces")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			schPolicyUid, _ = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicPolicyName)
			backupName = fmt.Sprintf("schedule-all-ns-%v", RandomString(6))
			scheduleNames = append(scheduleNames, backupName)
			log.InfoD("Creating a backup [%s] in [%s] with namespaces %s in backup location [%s]", backupName, SourceClusterName, appNamespaces, backupLocationName)
			scheduleBackupName, err = CreateScheduleBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, BackupOrgID, "", "", "", "", periodicPolicyName, schPolicyUid)
			backupNames = append(backupNames, scheduleBackupName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of single schedule backup [%s] with all namespaces %s", scheduleBackupName, appNamespaces))
		})

		Step("Creating multiple schedule backups with each namespace", func() {
			log.InfoD("Creating multiple schedule backups with each namespace")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			for i, appCtx := range multiScheduledAppContexts {
				scheduledNamespace := appCtx.ScheduleOptions.Namespace
				backupName = fmt.Sprintf("schedule-%s-%v", scheduledNamespace, RandomString(6))
				scheduleNames = append(scheduleNames, backupName)
				log.InfoD("Creating backup [%s] in [%s] with namespace [%s] in backup location [%s]", backupName, SourceClusterName, scheduledNamespace, backupLocationName)
				scheduleBackupName, err = CreateScheduleBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, multiScheduledAppContexts[i:i+1], labelSelectors, BackupOrgID, "", "", "", "", periodicPolicyName, schPolicyUid)
				backupNames = append(backupNames, scheduleBackupName)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of multiple schedule backup [%s] with each namespace [%s]", scheduleBackupName, scheduledNamespace))
			}
		})

		//Default Restores with retain policy
		Step("Restoring all the backups which were taken above", func() {
			log.InfoD("Restoring all the backups : %v", backupNames)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			for i, bkpName := range backupNames {
				restoreName = fmt.Sprintf("rretain-%v-%s-%s", i, bkpName, RandomString(6))
				log.InfoD("Restoring from the backup - [%s]", bkpName)
				bkpNamespace := backupNamespaceMap[bkpName]
				appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespace})
				err = CreateRestoreWithValidation(ctx, restoreName, backupNames[i], make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsExpectedInBackup)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of restore [%s] from backup [%s]", restoreName, backupNames[i]))
			}
		})

		Step(fmt.Sprintf("Default restore of backups by replacing to a new namespace"), func() {
			log.InfoD(fmt.Sprintf("Default restore of backups by replacing to a new namespace"))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			for i, bkpName := range backupNames {
				restoreName = fmt.Sprintf("rreplace-ns-%v-%s-%s", i, bkpName, RandomString(6))
				log.InfoD("Restoring from the backup - [%s]", bkpName)
				actualBackupNamespaces, err := FetchNamespacesFromBackup(ctx, bkpName, BackupOrgID)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching namespaces from schedule backup %v - [%v]", actualBackupNamespaces, bkpName))
				namespaceMapping := make(map[string]string)
				for _, namespace := range actualBackupNamespaces {
					if _, ok := namespaceMapping[namespace]; !ok {
						namespaceMapping[namespace] = namespace + "-new"
					}
				}
				log.InfoD("Backup namespace mapping : %v", namespaceMapping)
				err = CreateRestoreWithReplacePolicyWithValidation(restoreName, bkpName, namespaceMapping, DestinationClusterName, BackupOrgID, ctx, make(map[string]string), 2, scheduledAppContexts)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Default restore of backups by replacing to a new namespace [%s]", restoreName))
			}
		})

	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		defer func() {
			log.InfoD("switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "Failed to SetClusterContext to default cluster")
		}()
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.Info("Deleting backup schedules")
		var wg sync.WaitGroup
		var mutex sync.Mutex
		errors := make([]string, 0)
		for _, scheduleName := range scheduleNames {
			wg.Add(1)
			go func(scheduleName string) {
				defer GinkgoRecover()
				defer wg.Done()
				err = DeleteSchedule(scheduleName, SourceClusterName, BackupOrgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedules [%s]", scheduleName))
				if err != nil {
					mutex.Lock()
					errors = append(errors, fmt.Sprintf("Failed while deleting schedules [%s]. Error - [%s]", scheduleName, err.Error()))
					mutex.Unlock()
				}
			}(scheduleName)
		}
		wg.Wait()
		if len(errors) > 0 {
			err = fmt.Errorf("the combined list of errors while deleting backup schedules. Errors - [%v]", strings.Join(errors, ","))
			dash.VerifySafely(err, nil, "List of errors while deleting backup schedules")
		}
		log.Infof("Deleting backup schedule policy")
		schedulePolicyNames, err := Inst().Backup.GetAllSchedulePolicies(ctx, BackupOrgID)
		for _, schedulePolicyName := range schedulePolicyNames {
			err = Inst().Backup.DeleteBackupSchedulePolicy(BackupOrgID, []string{schedulePolicyName})
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policy %s ", []string{schedulePolicyName}))
		}
		log.Infof("Deleting pre & post exec rules")
		allRules, _ := Inst().Backup.GetAllRules(ctx, BackupOrgID)
		for _, ruleName := range allRules {
			err := DeleteRule(ruleName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of rule [%s]", ruleName))
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// This testcase verifies scheduled backup status when Kubevirt VMs are deleted and recreated from namespace in between schedules.
var _ = Describe("{KubevirtScheduledVMDelete}", Label(TestCaseLabelsMap[KubevirtScheduledVMDelete]...), func() {
	var (
		scheduledAppContexts           []*scheduler.Context
		bkpNamespaces                  = make([]string, 0)
		sourceClusterUid               string
		cloudCredName                  string
		cloudCredUID                   string
		backupLocationUID              string
		backupLocationName             string
		scheduleNames                  []string
		nonLabelScheduleName           string
		labelScheduleName              string
		backupLocationMap              = make(map[string]string)
		providers                      []string
		labelSelectors                 = make(map[string]string)
		nsLabelsMap                    = make(map[string]string)
		nsLabelString                  string
		periodicSchedulePolicyName     string
		periodicSchedulePolicyUid      string
		periodicSchedulePolicyInterval int64
		labelSchNamespaceMap           = make(map[string]string)
		nonLabelSchNamespaceMap        = make(map[string]string)
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("KubevirtScheduledVMDelete", "verifies scheduled backup status when Kubevirt VMs are deleted and recreated from namespace in between schedules", nil, 296428, Ak, Q1FY25)

		backupLocationMap = make(map[string]string)
		labelSelectors = make(map[string]string)
		providers = GetBackupProviders()
		log.InfoD("scheduling applications")
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("src-%s-%d", TaskNamePrefix, i)
			namespace := fmt.Sprintf("test-vm-namespace-%s", taskName)
			appContexts := ScheduleApplicationsOnNamespace(namespace, taskName)
			bkpNamespaces = append(bkpNamespaces, namespace)
			for index, appCtx := range appContexts {
				appName := Inst().AppList[index]
				appCtx.ReadinessTimeout = AppReadinessTimeout
				log.InfoD("Scheduled VM [%s] in source cluster in namespace [%s]", appName, namespace)
				scheduledAppContexts = append(scheduledAppContexts, appCtx)
			}
		}
	})

	It("verifies scheduled backup status when Kubevirt VMs are deleted and recreated in a namespace.", func() {

		Step("Validating applications", func() {
			// TODO: Add Data validation for this test case
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Adding labels to namespaces", func() {
			log.InfoD("Adding labels to namespaces")
			nsLabelsMap = GenerateRandomLabels(20)
			err := AddLabelsToMultipleNamespaces(nsLabelsMap, bkpNamespaces)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Adding labels [%v] to namespaces [%v]", nsLabelsMap, bkpNamespaces))
		})
		Step("Generating namespace label string from label map for namespaces", func() {
			log.InfoD("Generating namespace label string from label map for namespaces")
			nsLabelString = MapToKeyValueString(nsLabelsMap)
			log.Infof("label string for namespaces %s", nsLabelString)
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

		Step("Create schedule policy for backup schedules", func() {
			log.InfoD("Create schedule policy for backup schedules")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			periodicSchedulePolicyName = fmt.Sprintf("%s-%s", "periodic", RandomString(5))
			periodicSchedulePolicyUid = uuid.New()
			periodicSchedulePolicyInterval = int64(15)
			err = CreateBackupScheduleIntervalPolicy(5, periodicSchedulePolicyInterval, 5, periodicSchedulePolicyName, periodicSchedulePolicyUid, BackupOrgID, ctx, false, false)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval [%v] minutes named [%s] ", periodicSchedulePolicyInterval, periodicSchedulePolicyName))

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

		Step("Taking first schedule backup of kubevirt VMs without namespace label", func() {
			log.InfoD("Taking first schedule backup of kubevirt VMs without namespace label")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				vms, err := GetAllVMsInNamespace(namespace)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying fetching VMs from namespace[%s]", namespace))
				nonLabelScheduleName = fmt.Sprintf("%s-non-label-schedule-%s", BackupNamePrefix, RandomString(4))
				scheduleNames = append(scheduleNames, nonLabelScheduleName)
				_, err = CreateVMScheduledBackupWithValidation(nonLabelScheduleName, vms, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts,
					labelSelectors, BackupOrgID, sourceClusterUid, "", "", "", "", false, periodicSchedulePolicyName, periodicSchedulePolicyUid, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of schedule backup [%s] of namespace", nonLabelScheduleName))
				nonLabelSchNamespaceMap[namespace] = nonLabelScheduleName
			}
		})

		Step("Taking first schedule backup of kubevirt VMs with namespace label", func() {
			log.InfoD("Taking first schedule backup of kubevirt VMs with namespace label")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				vms, err := GetAllVMsInNamespace(namespace)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying fetching VMs from namespace[%s]", namespace))
				labelScheduleName = fmt.Sprintf("%s-label-schedule-%s", BackupNamePrefix, RandomString(4))
				scheduleNames = append(scheduleNames, labelScheduleName)
				_, err = CreateVMScheduleBackupWithNamespaceLabelWithValidation(ctx, labelScheduleName, vms, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts,
					labelSelectors, BackupOrgID, "", "", "", "", nsLabelString, periodicSchedulePolicyName, periodicSchedulePolicyUid, false)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of schedule backup [%s] with namespace label", labelScheduleName))
				labelSchNamespaceMap[namespace] = labelScheduleName
			}
		})

		Step("Deleting the kubevirt VMs from the namespace", func() {
			log.InfoD("Deleting the kubevirt VMs from the namespace")
			for _, namespace := range bkpNamespaces {
				err := DeleteAllVMsInNamespace(namespace)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of kubevirt VMs from the namespace [%s]", namespace))
			}
		})

		Step("Verify next namespace labelled and non labelled scheduled backup is success state after VM deletion", func() {
			log.InfoD("Verify next namespace labelled and non labelled scheduled backup is success state after VM deletion")
			var wg sync.WaitGroup
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			wg.Add(2)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				for _, namespace := range bkpNamespaces {
					schBackupAfterVMDeletion, err := GetNextCompletedScheduleBackupName(ctx, nonLabelSchNamespaceMap[namespace], time.Duration(periodicSchedulePolicyInterval))
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying fetching next non labelled schedule backup [%s] after VM deletion ", schBackupAfterVMDeletion))
					bkpStatus, bkpReason, err := Inst().Backup.GetBackupStatusWithReason(schBackupAfterVMDeletion, ctx, BackupOrgID)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup [%s] status  ", schBackupAfterVMDeletion))
					if bkpStatus == api.BackupInfo_StatusInfo_Success {
						log.Infof(fmt.Sprintf("The backup [%s] succeeded when the schedules VMs were deleted from namespace", schBackupAfterVMDeletion))
					} else {
						err := fmt.Errorf("The backup [%s] is in failed state when expected to success , Reason [%s]", schBackupAfterVMDeletion, bkpReason)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the status of non labelled scheduled backup [%s]", schBackupAfterVMDeletion))
					}
				}

			}()
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				for _, namespace := range bkpNamespaces {
					labelledSchBackupAfterVMDeletion, err := GetNextCompletedScheduleBackupName(ctx, labelSchNamespaceMap[namespace], time.Duration(periodicSchedulePolicyInterval))
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying fetching next labelled schedule [%s] backup after VM deletion ", labelledSchBackupAfterVMDeletion))
					bkpStatus, bkpReason, err := Inst().Backup.GetBackupStatusWithReason(labelledSchBackupAfterVMDeletion, ctx, BackupOrgID)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying backup [%s] status  ", labelledSchBackupAfterVMDeletion))
					if bkpStatus == api.BackupInfo_StatusInfo_Success {
						log.Infof("The backup is success state for labelled scheduled backup [%s] after VM delete")
					} else {
						err := fmt.Errorf("The backup failed for labelled scheduled backup [%s] after VM delete : Reason [%s]", labelledSchBackupAfterVMDeletion, bkpReason)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the status of labelled scheduled backup [%s]", labelledSchBackupAfterVMDeletion))
					}
				}

			}()
			wg.Wait()
		})

		Step("Recreating the VM deleted with same name in same namespace", func() {
			log.InfoD("Recreating the VM deleted with same name in same namespace")
			log.InfoD("scheduling applications")
			for i := 0; i < Inst().GlobalScaleFactor; i++ {
				taskName := fmt.Sprintf("src-%s-%d", TaskNamePrefix, i)
				namespace := fmt.Sprintf("test-vm-namespace-%s", taskName)
				appContexts := ScheduleApplicationsOnNamespace(namespace, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
				for index, appCtx := range appContexts {
					appName := Inst().AppList[index]
					appCtx.ReadinessTimeout = AppReadinessTimeout
					log.InfoD("Scheduled VM [%s] in source cluster in namespace [%s]", appName, namespace)
				}
			}
		})

		Step("Validating applications", func() {
			// TODO: Add Data validation for this test case
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Verify the recreated VM is included in next namespace labelled and non labelled scheduled backup", func() {
			log.InfoD("Verify the recreated VM is included in next namespace labelled nd non labelled scheduled backup")
			var wg sync.WaitGroup
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			wg.Add(2)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				for _, namespace := range bkpNamespaces {
					schBackupAfterVMcreation, err := GetNextCompletedScheduleBackupName(ctx, nonLabelSchNamespaceMap[namespace], time.Duration(periodicSchedulePolicyInterval))
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying fetching next schedule backup [%s] after VM creation ", schBackupAfterVMcreation))
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					err = GetUpdatedKubeVirtVMSpecForBackup(appContextsToBackup)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of getting the updated kubevirt VM context"))
					err = BackupSuccessCheckWithValidation(ctx, schBackupAfterVMcreation, appContextsToBackup, BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success of next VM schedule backup [%s] of schedule %s", schBackupAfterVMcreation, nonLabelSchNamespaceMap[namespace]))
				}

			}()
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				for _, namespace := range bkpNamespaces {
					labelledSchBackupAfterVMcreation, err := GetNextCompletedScheduleBackupName(ctx, labelSchNamespaceMap[namespace], time.Duration(periodicSchedulePolicyInterval))
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying fetching next labelled schedule [%s] backup after VM creation ", labelledSchBackupAfterVMcreation))
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					err = GetUpdatedKubeVirtVMSpecForBackup(appContextsToBackup)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of getting the updated kubevirt VM context"))
					err = BackupSuccessCheckWithValidation(ctx, labelledSchBackupAfterVMcreation, appContextsToBackup, BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success of next VM labelled schedule backup [%s] of schedule %s", labelledSchBackupAfterVMcreation, labelScheduleName))
				}
			}()
			wg.Wait()
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		for _, scheduleName := range scheduleNames {
			err = DeleteSchedule(scheduleName, SourceClusterName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		}
		log.Infof("Deleting backup schedule policy")
		err = Inst().Backup.DeleteBackupSchedulePolicy(BackupOrgID, []string{periodicSchedulePolicyName})
		dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting schedule policy - %s", periodicSchedulePolicyName))
		log.Info("Destroying scheduled apps on source cluster")
		DestroyApps(scheduledAppContexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// This testcase verifies custom backup & restore with both Kubevirt VMs and non-kubevirt resources
var _ = Describe("{CustomBackupRestoreWithKubevirtAndNonKubevirtNS}", Label(TestCaseLabelsMap[CustomBackupRestoreWithKubevirtAndNonKubevirtNS]...), func() {
	var (
		backupName                 string
		restoreName                string
		scheduleBackupName         string
		backupNames                []string
		scheduleNames              []string
		scheduledAppContexts       []*scheduler.Context
		singleScheduledAppContexts []*scheduler.Context
		multiScheduledAppContexts  []*scheduler.Context
		sourceClusterUID           string
		cloudCredName              string
		cloudCredUID               string
		backupLocationUID          string
		backupLocationName         string
		backupLocationMap          map[string]string
		labelSelectors             map[string]string
		providers                  []string
		numOfDeployments           int
		appNamespaces              []string
		schPolicyUid               string
		periodicPolicyName         string
		preRuleName                string
		postRuleName               string
		preRuleUid                 string
		postRuleUid                string
		preRuleNames               []string
		postRuleNames              []string
		allVMs                     []kubevirtv1.VirtualMachine
		allVMNames                 []string
		preRuleList                []*api.RulesInfo_RuleItem
		postRuleList               []*api.RulesInfo_RuleItem
		testAppList                []string
	)

	backupLocationMap = make(map[string]string)
	backupNames = make([]string, 0)
	labelSelectors = make(map[string]string)
	appNamespaces = make([]string, 0)
	periodicPolicyName = fmt.Sprintf("%s-%s", "periodic", RandomString(6))
	testAppList = []string{"mysql-backup", "kubevirt-cirros-cd-with-pvc"}

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("CustomBackupRestoreWithKubevirtAndNonKubevirtNS", "Verify custom backup & restore with both Kubevirt and Non-Kubevirt namespaces", nil, 93007, Vpinisetti, Q1FY25)
		numOfDeployments = Inst().GlobalScaleFactor
		providers = GetBackupProviders()
		actualAppList := Inst().AppList
		defer func() {
			Inst().AppList = actualAppList
		}()
		Inst().AppList = testAppList
		log.InfoD("Deploying all provided applications in a single namespace")
		for i := 0; i < numOfDeployments; i++ {
			taskName := fmt.Sprintf("%s-%d-%d", TaskNamePrefix, 93007, i)
			namespace := fmt.Sprintf("single-ns-multi-app-%s-%v", taskName, time.Now().Unix())
			singleScheduledAppContexts = ScheduleApplicationsOnNamespace(namespace, taskName)
			appNamespaces = append(appNamespaces, namespace)
			scheduledAppContexts = append(scheduledAppContexts, singleScheduledAppContexts...)
			for _, appCtx := range singleScheduledAppContexts {
				appCtx.ReadinessTimeout = AppReadinessTimeout
			}
		}
		log.InfoD("Deploying all provided applications in separate namespaces")
		multiScheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < numOfDeployments; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			multiScheduledAppContexts = ScheduleApplications(taskName)
			scheduledAppContexts = append(scheduledAppContexts, multiScheduledAppContexts...)
			for _, appCtx := range multiScheduledAppContexts {
				namespace := GetAppNamespace(appCtx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				appCtx.ReadinessTimeout = AppReadinessTimeout
			}
		}
	})

	It("Verify custom backup & restore with both Kubevirt and Non-Kubevirt namespaces", func() {
		defer func() {
			log.InfoD("Switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "Failed to set ClusterContext to default cluster")
		}()

		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Create cloud credentials and backup location", func() {
			log.InfoD("Creating cloud credentials and backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, RandomString(6))
				backupLocationName = fmt.Sprintf("%s-%v", getGlobalBucketName(provider), RandomString(6))
				cloudCredUID = uuid.New()
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, BackupOrgID, provider))
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, "Creation of backup location")
			}
		})

		Step("Register clusters for backup & restore", func() {
			log.InfoD("Registering clusters for backup & restore")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")

			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination clusters")

			clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))

			sourceClusterUID, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))

			clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, DestinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", DestinationClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", DestinationClusterName))
		})

		Step("Create schedule policies", func() {
			log.InfoD("Creating schedule policies")
			periodicSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(10, 15, 2)
			periodicPolicyStatus := Inst().Backup.BackupSchedulePolicy(periodicPolicyName, uuid.New(), BackupOrgID, periodicSchedulePolicyInfo)
			dash.VerifyFatal(periodicPolicyStatus, nil, fmt.Sprintf("Creation of periodic schedule policy - %s", periodicPolicyName))
		})

		Step("Create pre & post exec rules", func() {
			log.InfoD("Creating pre & post exec rules")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			// Application pre & post exec rules
			preRuleName, postRuleName, err = CreateRuleForBackupWithMultipleApplications(BackupOrgID, testAppList, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of pre & post exec rules"))
			if preRuleName != "" {
				preRuleNames = append(preRuleNames, preRuleName)
			}
			if postRuleName != "" {
				postRuleNames = append(postRuleNames, postRuleName)
			}
			// Vm freeze and unfreeze rules
			for _, namespace := range appNamespaces {
				vms, err := GetAllVMsInNamespace(namespace)
				log.FailOnError(err, "Failed to get VMs in namespace - %s", namespace)
				allVMs = append(allVMs, vms...)
			}
			for _, v := range allVMs {
				allVMNames = append(allVMNames, v.Name)
			}
			freezeRuleName := fmt.Sprintf("vm-freeze-rule-%s", RandomString(4))
			err = CreateRuleForVMBackup(freezeRuleName, allVMs, Freeze, ctx)
			log.FailOnError(err, "Failed to create freeze rule %s for VMs - %v", freezeRuleName, allVMNames)
			unfreezeRuleName := fmt.Sprintf("vm-unfreeze-rule-%s", RandomString(4))
			err = CreateRuleForVMBackup(unfreezeRuleName, allVMs, Unfreeze, ctx)
			log.FailOnError(err, "Failed to create unfreeze rule %s for VMs - %v", unfreezeRuleName, allVMNames)
			preRuleNames = append(preRuleNames, freezeRuleName)
			postRuleNames = append(postRuleNames, unfreezeRuleName)

			// processing pre exec rules
			log.InfoD("Actual pre rules are [%v]", preRuleNames)
			for _, preRule := range preRuleNames {
				preRuleID, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRule)
				log.FailOnError(err, "Fetching pre backup rule [%s] uid", preRule)
				log.Infof("Pre backup rule name [%s] & uid [%s]", preRule, preRuleID)
				ruleInspectRequest := &api.RuleInspectRequest{
					OrgId: BackupOrgID,
					Name:  preRule,
					Uid:   preRuleID,
				}
				resp, _ := Inst().Backup.InspectRule(ctx, ruleInspectRequest)
				for _, rule := range resp.GetRule().GetRules() {
					preRuleList = append(preRuleList, rule)
				}
			}

			preRuleName = fmt.Sprintf("final-pre-exec-rule-%s", RandomString(4))
			preRuleCreateReq := &api.RuleCreateRequest{
				CreateMetadata: &api.CreateMetadata{
					Name:  preRuleName,
					OrgId: BackupOrgID,
				},
				RulesInfo: &api.RulesInfo{
					Rules: preRuleList,
				},
			}
			log.InfoD("Creating final pre backup rule [%s]", preRuleName)
			_, err = Inst().Backup.CreateRule(ctx, preRuleCreateReq)
			log.FailOnError(err, "Failed while creating final pre backup rule [%s]", preRuleName)
			preRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleName)
			log.FailOnError(err, "Fetching final pre backup rule [%s] uid", preRuleName)
			log.Infof("Final pre backup rule [%s] with uid [%s]", preRuleName, preRuleUid)

			// processing post exec rules
			log.InfoD("Actual post rules are : %v", postRuleNames)
			for _, postRule := range postRuleNames {
				log.InfoD("Processing post rule [%s]", postRule)
				postRuleID, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRule)
				log.FailOnError(err, "Fetching post backup rule [%s] uid", postRule)
				log.Infof("Post backup rule name [%s] & uid [%s]", postRule, postRuleID)
				ruleInspectRequest := &api.RuleInspectRequest{
					OrgId: BackupOrgID,
					Name:  postRule,
					Uid:   postRuleID,
				}
				resp, _ := Inst().Backup.InspectRule(ctx, ruleInspectRequest)
				for _, rule := range resp.GetRule().GetRules() {
					postRuleList = append(postRuleList, rule)
				}
			}

			postRuleName = fmt.Sprintf("final-post-exec-rule-%s", RandomString(4))
			postRuleCreateReq := &api.RuleCreateRequest{
				CreateMetadata: &api.CreateMetadata{
					Name:  postRuleName,
					OrgId: BackupOrgID,
				},
				RulesInfo: &api.RulesInfo{
					postRuleList,
				},
			}
			log.InfoD("Creating final post backup rule [%s]", postRuleName)
			_, err = Inst().Backup.CreateRule(ctx, postRuleCreateReq)
			log.FailOnError(err, "Failed while creating final post backup rule [%s]", postRuleName)
			postRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleName)
			log.FailOnError(err, "Fetching final post backup rule [%s] uid", postRuleName)
			log.Infof("Final post backup rule [%s] with uid: [%s]", postRuleName, postRuleUid)
		})

		// Manual backups with pre & post exec rules
		Step("Creating manual backup with single namespace contains both kubevirt and non-kubevirt using exec rules", func() {
			log.InfoD("Creating manual backup with single namespace contains both kubevirt and non-kubevirt using exec rules")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			backupName = fmt.Sprintf("manual-rule-single-ns-multi-apps-%v", RandomString(6))
			backupNames = append(backupNames, backupName)
			log.InfoD("Creating a backup [%s] in [%s] with namespace [%s] in backup location [%s]", backupName, SourceClusterName, appNamespaces[0], backupLocationName)
			err = CreateBackupWithCustomResourceTypeWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, singleScheduledAppContexts, []string{"PersistentVolumeClaim"}, labelSelectors, BackupOrgID, sourceClusterUID, preRuleName, preRuleUid, postRuleName, postRuleUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of single manual backup [%s] with single namespace contains multiple apps using exec rules.", backupName))
		})

		Step("Creating single manual backup with all namespaces using exec rules", func() {
			log.InfoD(fmt.Sprintf("Creating single manual backup with all namespaces using exec rules : %v", appNamespaces))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			backupName = fmt.Sprintf("manual-rule-all-ns-%v", RandomString(6))
			backupNames = append(backupNames, backupName)
			log.InfoD("Creating a backup [%s] in [%s] with all namespaces %s in backup location [%s]", backupName, SourceClusterName, appNamespaces, backupLocationName)
			err = CreateBackupWithCustomResourceTypeWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, []string{"PersistentVolumeClaim"}, labelSelectors, BackupOrgID, sourceClusterUID, preRuleName, preRuleUid, postRuleName, postRuleUid)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of single manual backup [%s] with multiple namespaces %v using exec rules", backupName, appNamespaces))
		})

		// Schedule backups with pre & post exec rules
		Step("Creating schedule backup with single namespace contains both kubevirt and non-kubevirt using exec rules", func() {
			log.InfoD("Creating schedule backup with single namespace contains both kubevirt and non-kubevirt using exec rules")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			schPolicyUid, _ = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicPolicyName)
			backupName = fmt.Sprintf("schdule-rule-single-ns-multi-apps-%v", RandomString(6))
			scheduleNames = append(scheduleNames, backupName)
			log.InfoD("Creating a backup [%s] in [%s] with namespace [%s] in backup location [%s]", backupName, SourceClusterName, appNamespaces[0], backupLocationName)
			scheduleBackupName, err = CreateScheduleBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, singleScheduledAppContexts, labelSelectors, BackupOrgID, preRuleName, preRuleUid, postRuleName, postRuleUid, periodicPolicyName, schPolicyUid, []string{"PersistentVolumeClaim"}...)
			backupNames = append(backupNames, scheduleBackupName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of single schedule backup [%s] with single namespace contains multiple apps using exec rules.", scheduleBackupName))
		})

		Step("Creating single schedule backup with all namespaces", func() {
			log.InfoD("Creating single schedule backup with all namespaces")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			schPolicyUid, _ = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicPolicyName)
			backupName = fmt.Sprintf("schedule-rule-all-ns-%v", RandomString(6))
			scheduleNames = append(scheduleNames, backupName)
			log.InfoD("Creating a backup [%s] in [%s] with namespaces %s in backup location [%s]", backupName, SourceClusterName, appNamespaces, backupLocationName)
			scheduleBackupName, err = CreateScheduleBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, BackupOrgID, preRuleName, preRuleUid, postRuleName, postRuleUid, periodicPolicyName, schPolicyUid, []string{"PersistentVolumeClaim"}...)
			backupNames = append(backupNames, scheduleBackupName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of single schedule backup [%s] with all namespaces %s using exec rules", scheduleBackupName, appNamespaces))
		})

		// Manual backups
		Step("Creating manual backup with single namespace contains both kubevirt and non-kubevirt apps", func() {
			log.InfoD("Creating manual backup with single namespace contains both kubevirt and non-kubevirt apps")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			backupName = fmt.Sprintf("manual-single-ns-multi-apps-%v", RandomString(6))
			backupNames = append(backupNames, backupName)
			log.InfoD("Creating a backup [%s] in [%s] with namespace [%s] in backup location [%s]", backupName, SourceClusterName, appNamespaces[0], backupLocationName)
			err = CreateBackupWithCustomResourceTypeWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, singleScheduledAppContexts, []string{"PersistentVolumeClaim"}, labelSelectors, BackupOrgID, sourceClusterUID, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of single manual backup [%s] with single namespace contains multiple apps.", backupName))
		})

		Step("Creating single manual backup with all namespaces", func() {
			log.InfoD(fmt.Sprintf("Creating single backup with all namespaces %v", appNamespaces))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			backupName = fmt.Sprintf("manual-all-ns-%v", RandomString(6))
			backupNames = append(backupNames, backupName)
			log.InfoD("Creating a backup [%s] in [%s] with all namespaces [%s] in backup location [%s]", backupName, SourceClusterName, appNamespaces, backupLocationName)
			err = CreateBackupWithCustomResourceTypeWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, []string{"PersistentVolumeClaim"}, labelSelectors, BackupOrgID, sourceClusterUID, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of single manual backup [%s] with multiple namespaces %v", backupName, appNamespaces))
		})

		Step("Creating multiple manual backups with each namespace", func() {
			log.InfoD("Creating multiple manual backups with each namespace")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			for i, appCtx := range multiScheduledAppContexts {
				scheduledNamespace := appCtx.ScheduleOptions.Namespace
				backupName = fmt.Sprintf("%s-%s-%v", "manual", scheduledNamespace, RandomString(6))
				backupNames = append(backupNames, backupName)
				log.InfoD("Creating backup [%s] in [%s] with namespace [%s] in backup location [%s]", backupName, SourceClusterName, scheduledNamespace, backupLocationName)
				err := CreateBackupWithCustomResourceTypeWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, multiScheduledAppContexts[i:i+1], []string{"PersistentVolumeClaim"}, labelSelectors, BackupOrgID, sourceClusterUID, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of multiple manual backups [%s] with each namespace [%s]", backupName, scheduledNamespace))
			}
		})

		// Schedule backups
		Step("Creating schedule backup with single namespace contains both kubevirt and non-kubevirt apps", func() {
			log.InfoD("Creating schedule backup with single namespace contains both kubevirt and non-kubevirt apps")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			schPolicyUid, _ = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicPolicyName)
			backupName = fmt.Sprintf("schdule-single-ns-multi-apps-%v", RandomString(6))
			scheduleNames = append(scheduleNames, backupName)
			log.InfoD("Creating a backup [%s] in [%s] with namespace [%s] in backup location [%s]", backupName, SourceClusterName, appNamespaces[0], backupLocationName)
			scheduleBackupName, err = CreateScheduleBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, singleScheduledAppContexts, labelSelectors, BackupOrgID, "", "", "", "", periodicPolicyName, schPolicyUid, []string{"PersistentVolumeClaim"}...)
			backupNames = append(backupNames, scheduleBackupName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of single schedule backup [%s] with single namespace contains multiple apps.", scheduleBackupName))
		})

		Step("Creating single schedule backup with all the namespaces", func() {
			log.InfoD("Creating single schedule backup with all the namespaces")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			schPolicyUid, _ = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicPolicyName)
			backupName = fmt.Sprintf("schedule-all-ns-%v", RandomString(6))
			scheduleNames = append(scheduleNames, backupName)
			log.InfoD("Creating a backup [%s] in [%s] with namespaces %s in backup location [%s]", backupName, SourceClusterName, appNamespaces, backupLocationName)
			scheduleBackupName, err = CreateScheduleBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, labelSelectors, BackupOrgID, "", "", "", "", periodicPolicyName, schPolicyUid, []string{"PersistentVolumeClaim"}...)
			backupNames = append(backupNames, scheduleBackupName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of single schedule backup [%s] with all namespaces %s", scheduleBackupName, appNamespaces))
		})

		Step("Creating multiple schedule backups with each namespace", func() {
			log.InfoD("Creating multiple schedule backups with each namespace")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			for i, appCtx := range multiScheduledAppContexts {
				scheduledNamespace := appCtx.ScheduleOptions.Namespace
				backupName = fmt.Sprintf("schedule-%s-%v", scheduledNamespace, RandomString(6))
				scheduleNames = append(scheduleNames, backupName)
				log.InfoD("Creating backup [%s] in [%s] with namespace [%s] in backup location [%s]", backupName, SourceClusterName, scheduledNamespace, backupLocationName)
				scheduleBackupName, err = CreateScheduleBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, multiScheduledAppContexts[i:i+1], labelSelectors, BackupOrgID, "", "", "", "", periodicPolicyName, schPolicyUid, []string{"PersistentVolumeClaim"}...)
				backupNames = append(backupNames, scheduleBackupName)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of multiple schedule backup [%s] with each namespace [%s]", scheduleBackupName, scheduledNamespace))
			}
		})

		// Default Restores with Replace Policy
		Step(fmt.Sprintf("Default restore of backups by replacing the existing resources"), func() {
			log.InfoD(fmt.Sprintf("Default restore of backups by replacing the existing resources"))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			log.InfoD("Total backups to restore : %v", backupNames)
			for i, bkpName := range backupNames {
				restoreName = fmt.Sprintf("rreplace-%v-%s-%s", i, bkpName, RandomString(6))
				log.InfoD("Restoring from the backup - [%s]", bkpName)
				err = CreateRestoreWithReplacePolicyWithValidation(restoreName, bkpName, make(map[string]string), DestinationClusterName, BackupOrgID, ctx, make(map[string]string), 2, scheduledAppContexts)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Default restore of backups by replacing the existing resources [%s]", restoreName))
			}
		})

		Step(fmt.Sprintf("Default restore of backups by replacing to a new namespace"), func() {
			log.InfoD(fmt.Sprintf("Default restore of backups by replacing to a new namespace"))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			log.InfoD("Total backups to restore : %v", backupNames)
			for i, bkpName := range backupNames {
				restoreName = fmt.Sprintf("rreplace-ns-%v-%s-%s", i, bkpName, RandomString(6))
				log.InfoD("Restoring from the backup - [%s]", bkpName)
				actualBackupNamespaces, err := FetchNamespacesFromBackup(ctx, bkpName, BackupOrgID)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching namespaces from schedule backup %v - [%v]", actualBackupNamespaces, bkpName))
				namespaceMapping := make(map[string]string)
				for _, namespace := range actualBackupNamespaces {
					if _, ok := namespaceMapping[namespace]; !ok {
						namespaceMapping[namespace] = namespace + "-new"
					}
				}
				log.InfoD("Backup namespace mapping : %v", namespaceMapping)
				err = CreateRestoreWithReplacePolicyWithValidation(restoreName, bkpName, namespaceMapping, DestinationClusterName, BackupOrgID, ctx, make(map[string]string), 2, scheduledAppContexts)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Default restore of backups by replacing to a new namespace [%s]", restoreName))
			}
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		defer func() {
			log.InfoD("switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "Failed to SetClusterContext to default cluster")
		}()
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.Info("Deleting backup schedules")
		var wg sync.WaitGroup
		var mutex sync.Mutex
		errors := make([]string, 0)
		for _, scheduleName := range scheduleNames {
			wg.Add(1)
			go func(scheduleName string) {
				defer GinkgoRecover()
				defer wg.Done()
				err = DeleteSchedule(scheduleName, SourceClusterName, BackupOrgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedules [%s]", scheduleName))
				if err != nil {
					mutex.Lock()
					errors = append(errors, fmt.Sprintf("Failed while deleting schedules [%s]. Error - [%s]", scheduleName, err.Error()))
					mutex.Unlock()
				}
			}(scheduleName)
		}
		wg.Wait()
		if len(errors) > 0 {
			err = fmt.Errorf("the combined list of errors while deleting backup schedules. Errors - [%v]", strings.Join(errors, ","))
			dash.VerifySafely(err, nil, "List of errors while deleting backup schedules")
		}
		log.Infof("Deleting backup schedule policy")
		schedulePolicyNames, err := Inst().Backup.GetAllSchedulePolicies(ctx, BackupOrgID)
		for _, schedulePolicyName := range schedulePolicyNames {
			err = Inst().Backup.DeleteBackupSchedulePolicy(BackupOrgID, []string{schedulePolicyName})
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policy %s ", []string{schedulePolicyName}))
		}
		log.Infof("Deleting pre & post exec rules")
		allRules, _ := Inst().Backup.GetAllRules(ctx, BackupOrgID)
		for _, ruleName := range allRules {
			err := DeleteRule(ruleName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of rule [%s]", ruleName))
		}
		log.InfoD("Cleaning up cloud settings and application clusters")
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// This testcase verifies backup & restore during Kubevirt VM migration.
var _ = Describe("{KubevirtVMMigrationTest}", Label(TestCaseLabelsMap[KubevirtVMMigrationTestLabel]...), func() {
	var (
		backupNames          = make([]string, 0)
		sourceClusterUid     string
		cloudCredName        string
		cloudCredUID         string
		backupLocationUID    string
		backupLocationName   string
		providers            []string
		restoreNames         []string
		backupLocationMap    = make(map[string]string)
		backupNamespaceMap   = make(map[string]string)
		labelSelectors       = make(map[string]string)
		wg                   sync.WaitGroup
		mu                   sync.RWMutex
		scheduledAppContexts []*scheduler.Context
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("KubevirtVMMigrationTest", "verifies backup & restore during Kubevirt VM migration.", nil, 93442, Ak, Q1FY25)
		providers = GetBackupProviders()
		log.InfoD("scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%d-%d", 93442, i)
			appContexts := ScheduleApplications(taskName)
			for _, appCtx := range appContexts {
				appCtx.ReadinessTimeout = AppReadinessTimeout
				scheduledAppContexts = append(scheduledAppContexts, appCtx)
			}
		}
	})

	It("Verify backup & restore during Kubevirt VM migration", func() {
		defer func() {
			log.InfoD("Switching to default context")
			err := SetClusterContext("")
			log.FailOnError(err, "Failed to set ClusterContext to default cluster")
		}()

		Step("Validate applications", func() {
			log.InfoD("Validating applications")
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

		// func for createVMBackup
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		createVMBackup := func(backupName string, namespace string, vms []kubevirtv1.VirtualMachine) {
			defer GinkgoRecover()
			defer wg.Done()
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
			err := CreateVMBackupWithValidation(ctx, backupName, vms, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup,
				labelSelectors, BackupOrgID, sourceClusterUid, "", "", "", "", false)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of VM backup and Validation of backup [%s]", backupName))
			mu.Lock()
			backupNames = append(backupNames, backupName)
			backupNamespaceMap[backupName] = namespace
			defer mu.Unlock()
		}

		Step("Take backups during cordoning and draining the nodes in which VM is running", func() {
			log.InfoD("Take backups during cordoning and draining the nodes in which VM is running")
			k8sCore := core.Instance()
			for _, appCtx := range scheduledAppContexts {
				scheduledNamespace := appCtx.ScheduleOptions.Namespace
				vms, err := GetAllVMsInNamespace(scheduledNamespace)
				log.FailOnError(err, "Failed to get VMs deployed")
				for _, vm := range vms {
					log.Infof("Taking backup of VMs before cordoning the node")
					wg.Add(1)
					backupName := fmt.Sprintf("%s-%s-%v", "pre-cordon-backup", scheduledNamespace, time.Now().Unix())
					go createVMBackup(backupName, scheduledNamespace, vms)
					wg.Wait()
					log.Infof("Taking backup of VMs during cordoning the node")
					wg.Add(1)
					backupName = fmt.Sprintf("%s-%s-%v", "cordon-backup", scheduledNamespace, time.Now().Unix())
					go createVMBackup(backupName, scheduledNamespace, vms)
					//Get the node where the vm is scheduled.
					preNodeName, err := GetNodeOfVM(vm)
					log.FailOnError(err, fmt.Sprintf("Failed to get nodename for the VM %s", vm.Name))
					defer func() {
						err = k8sCore.UnCordonNode(preNodeName, 1*time.Minute, 5*time.Second)
						log.FailOnError(err, fmt.Sprintf("Verifying uncordoning the node %s", preNodeName))
					}()
					err = k8sCore.CordonNode(preNodeName, 1*time.Minute, 5*time.Second)
					log.FailOnError(err, fmt.Sprintf("Verifying cordoning the node %s", preNodeName))
					vmPod, err := GetVirtLauncherPodObject(vm)
					log.FailOnError(err, fmt.Sprintf("Failed to get pod for the VM %s", vm.Name))
					err = k8sCore.DrainPodsFromNode(preNodeName, []corev1.Pod{*vmPod}, 30*time.Minute, 30*time.Second)
					log.FailOnError(err, fmt.Sprintf("Verifying draining the pods [%v] from the node %s", []corev1.Pod{*vmPod}, preNodeName))
					err = ValidateVMMigration(vm, preNodeName)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verfiying VM [%s] is up and running after cordoning the node [%s]", vm.Name, preNodeName))
					wg.Wait()
					err = k8sCore.UnCordonNode(preNodeName, 1*time.Minute, 5*time.Second)
					log.FailOnError(err, fmt.Sprintf("Verifying uncordoning the node %s", preNodeName))
					log.Infof("Taking backup of VMs after uncordoning the node")
					wg.Add(1)
					backupName = fmt.Sprintf("%s-%s-%v", "post-cordon-backup", scheduledNamespace, time.Now().Unix())
					go createVMBackup(backupName, scheduledNamespace, vms)
					wg.Wait()
				}
			}
		})

		Step("Take backups during Live migration of the VM", func() {
			log.InfoD("Take backups during Live migration of the VM")
			for _, appCtx := range scheduledAppContexts {
				scheduledNamespace := appCtx.ScheduleOptions.Namespace
				vms, err := GetAllVMsInNamespace(appCtx.ScheduleOptions.Namespace)
				log.FailOnError(err, fmt.Sprintf("Failed to get VMs deployed in namespace [%s]", appCtx.ScheduleOptions.Namespace))
				for _, vm := range vms {
					log.Infof("Taking backup of VMs during migration")
					wg.Add(1)
					backupName := fmt.Sprintf("%s-%s-%v", "migration-backup", scheduledNamespace, time.Now().Unix())
					go createVMBackup(backupName, scheduledNamespace, vms)
					log.Infof(fmt.Sprintf("Starting migration vm [%s] under namespace [%s] ", vm.Name, vm.Namespace))
					nodeName, err := GetNodeOfVM(vm)
					log.FailOnError(err, fmt.Sprintf("Failed to get nodename for the VM %s", vm.Name))
					err = StartAndWaitForVMIMigration(appCtx, context1.TODO())
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verfiying migration of VM [%s] from the node [%s] ", vm.Name, nodeName))
					wg.Wait()
					log.Infof("Taking backup of VMs post migration")
					wg.Add(1)
					backupName = fmt.Sprintf("%s-%s-%v", "post-migration-backup", scheduledNamespace, time.Now().Unix())
					go createVMBackup(backupName, scheduledNamespace, vms)
					wg.Wait()
				}
			}
		})

		Step("Take backups during rebooting the node in which VM is running", func() {
			log.InfoD("Take backups during rebooting the node in which VM is running")
			for _, appCtx := range scheduledAppContexts {
				scheduledNamespace := appCtx.ScheduleOptions.Namespace
				vms, err := GetAllVMsInNamespace(appCtx.ScheduleOptions.Namespace)
				log.FailOnError(err, "Failed to get VMs deployed")
				for _, vm := range vms {
					//Get the node where the vm is scheduled.
					nodeName, err := GetNodeOfVM(vm)
					log.FailOnError(err, fmt.Sprintf("Failed to get nodename for the VM %s", vm.Name))
					nodeObject, err := node.GetNodeByName(nodeName)
					log.FailOnError(err, fmt.Sprintf("Failed to get node object for the node %s", nodeName))
					err = RebootNodeAndWaitForPxUp(nodeObject)
					log.FailOnError(err, "Failed to reboot node and wait till it is up")
					err = ValidateVMMigration(vm, nodeName)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verfiying VM [%s] is up and running after rebooting the node [%s]", vm.Name, nodeName))
					log.Infof("Validating all px-backup pods are ready after reboot")
					err = ValidateAllPodsInPxBackupNamespace()
					dash.VerifyFatal(err, nil, "verifing px-backups pods are in running state after reboot")
					t := func() (interface{}, bool, error) {
						PxBackupVersion, err := GetPxBackupVersionString()
						if err != nil {
							return nil, true, fmt.Errorf("failed to get px-backup version string , error [%s]", err.Error())
						}
						log.Infof(fmt.Sprintf("fetched px-backup version %s", PxBackupVersion))
						return nil, false, nil
					}
					_, err = task.DoRetryWithTimeout(t, 30*time.Minute, 30*time.Second)
					dash.VerifyFatal(err, nil, "verifing px-backup version after reboot")
					log.Infof("Taking backup of VMs after node reboot")
					wg.Add(1)
					backupName := fmt.Sprintf("%s-%s-%v", "post-reboot-backup", scheduledNamespace, time.Now().Unix())
					go createVMBackup(backupName, scheduledNamespace, vms)
					wg.Wait()
				}
			}
		})

		Step("Restoring all the backups which were taken above", func() {
			log.InfoD("Restoring all the backups : %v", backupNames)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to fetch px-central-admin ctx")
			for i, bkpName := range backupNames {
				restoreName := fmt.Sprintf("rretain-%v-%s-%s", i, bkpName, RandomString(6))
				log.InfoD("Restoring from the backup - [%s]", bkpName)
				bkpNamespace := backupNamespaceMap[bkpName]
				appContextsExpectedInBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespace})
				err = CreateRestoreWithValidation(ctx, restoreName, bkpName, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsExpectedInBackup)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation of restore [%s] from backup [%s]", restoreName, bkpName))
				restoreNames = append(restoreNames, restoreName)
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
		log.FailOnError(err, "Data validations failed")

		log.Info("Deleting restored VMs")
		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore [%s]", restoreName))
		}

		log.InfoD("Cleaning up cloud settings and application clusters")
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})
