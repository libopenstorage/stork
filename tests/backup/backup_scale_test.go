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

// MultipleBackupLocationWithSameEndpoint Create Backup and Restore for Multiple backup location added using same endpoint.
var _ = Describe("{MultipleBackupLocationWithSameEndpoint}", func() {
	var (
		scheduledAppContexts          []*scheduler.Context
		backupLocationNameMap         = make(map[int]string)
		backupLocationUIDMap          = make(map[int]string)
		backupLocationMap             = make(map[string]string)
		restoreNsMapping              = make(map[string]map[string]string)
		bkpNamespaces                 []string
		cloudCredName                 string
		cloudCredUID                  string
		clusterUid                    string
		labelSelectors                map[string]string
		wg                            sync.WaitGroup
		userBackupMap                 = make(map[int]map[string]string)
		restoreNames                  []string
		numberOfBackupLocation        = 1000
		numberOfBackups               = 30
		providers                     = GetBackupProviders()
		timeBetweenConsecutiveBackups = 10 * time.Second
		controlChannel                chan string
		errorGroup                    *errgroup.Group
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("MultipleBackupLocationWithSameEndpoint", "Create Backup and Restore for Multiple backup location added using same endpoint", nil, 84902, Ak, Q3FY24)
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

	It("Create Backup and Restore for Multiple backup location added using same endpoint", func() {
		Step("Validate applications", func() {
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})
		Step(fmt.Sprintf("Creating a cloud credentials from px-admin"), func() {
			log.InfoD(fmt.Sprintf("Creating a cloud credentials from px-admin"))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				cloudCredUID = uuid.New()
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, BackupOrgID, provider))
			}
		})
		Step(fmt.Sprintf("Creating [%d] backup locations from px-admin", numberOfBackupLocation), func() {
			log.InfoD(fmt.Sprintf("Creating [%d] backup locations from px-admin", numberOfBackupLocation))
			for i := 0; i <= numberOfBackupLocation; i++ {
				for _, provider := range providers {
					log.InfoD(fmt.Sprintf("Creating backup locations with index [%d]", i))
					backupLocationNameMap[i] = fmt.Sprintf("%s-%d-%s", getGlobalBucketName(provider), i, RandomString(6))
					backupLocationUIDMap[i] = uuid.New()
					err := CreateBackupLocation(provider, backupLocationNameMap[i], backupLocationUIDMap[i], cloudCredName, cloudCredUID, getGlobalBucketName(provider), BackupOrgID, "", true)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup location [%s]", backupLocationNameMap[i]))
					backupLocationMap[backupLocationUIDMap[i]] = backupLocationNameMap[i]
				}
			}
		})
		Step("Registering cluster for backup from px-admin", func() {
			log.InfoD("Registering cluster for backup from px-admin")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			log.InfoD("Verifying cluster status for both source and destination clusters")
			clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, DestinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", DestinationClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", DestinationClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})
		Step(fmt.Sprintf("Taking [%d] backup for the each application from px-admin", numberOfBackups), func() {
			log.InfoD(fmt.Sprintf("Taking [%d] backup for the each application from px-admin", numberOfBackups))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch ctx for admin")
			createBackup := func(backupName string, namespace string, index int) {
				defer GinkgoRecover()
				defer wg.Done()
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationNameMap[index], backupLocationUIDMap[index], appContextsToBackup, labelSelectors, BackupOrgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of backup [%s] of namespace (scheduled Context) [%s]", backupName, namespace))
			}
			semaphore := make(chan int, 4)
			for _, namespace := range bkpNamespaces {
				for index := 0; index < numberOfBackups; index++ {
					time.Sleep(timeBetweenConsecutiveBackups)
					backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, backupLocationNameMap[index], RandomString(4))
					userBackupMap[index] = make(map[string]string)
					userBackupMap[index][backupName] = namespace
					wg.Add(1)
					semaphore <- 0
					go func(backupName string, namespace string, index int) {
						defer func() {
							<-semaphore
						}()
						createBackup(backupName, namespace, index)
					}(backupName, namespace, index)
				}
			}
			wg.Wait()
		})

		Step("Taking restore for each backups created from px-admin", func() {
			log.InfoD(fmt.Sprintf("Taking restore for each backups created from px-admin"))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var wg sync.WaitGroup
			var mu sync.Mutex
			errors := make([]string, 0)
			for index := 0; index < numberOfBackups; index++ {
				for backupName, namespace := range userBackupMap[index] {
					wg.Add(1)
					go func(backupName, namespace string) {
						defer GinkgoRecover()
						defer wg.Done()
						mu.Lock()
						restoreName := fmt.Sprintf("%s-%s-%s", RestoreNamePrefix, backupName, RandomString(5))
						customNamespace := "custom-" + namespace + RandomString(5)
						namespaceMapping := map[string]string{namespace: customNamespace}
						restoreNsMapping[restoreName] = namespaceMapping
						mu.Unlock()
						err := CreateRestore(restoreName, backupName, namespaceMapping, SourceClusterName, BackupOrgID, ctx, make(map[string]string))
						if err != nil {
							mu.Lock()
							errors = append(errors, fmt.Sprintf("Failed while taking restore [%s]. Error - [%s]", restoreName, err.Error()))
							mu.Unlock()
						}
					}(backupName, namespace)
				}
			}
			wg.Wait()
			dash.VerifyFatal(len(errors), 0, fmt.Sprintf("Creating restores : -\n%s", strings.Join(errors, "}\n{")))
			log.InfoD("All  mapping list %v", restoreNsMapping)

		})

		Step("Validating all restores", func() {
			log.InfoD("Validating all restores")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			var mutex sync.Mutex
			errors := make([]string, 0)
			var wg sync.WaitGroup
			for restoreName, namespaceMapping := range restoreNsMapping {
				wg.Add(1)
				go func(restoreName string, namespaceMapping map[string]string) {
					defer GinkgoRecover()
					defer wg.Done()
					log.InfoD("Validating restore [%s] with namespace mapping", restoreName)
					expectedRestoredAppContext, _ := CloneAppContextAndTransformWithMappings(scheduledAppContexts[0], namespaceMapping, make(map[string]string), true)
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
				}(restoreName, namespaceMapping)
			}
			wg.Wait()
			dash.VerifyFatal(len(errors), 0, fmt.Sprintf("Validating restores of individual backups -\n%s", strings.Join(errors, "}\n{")))

		})
		Step("Delete all Backup locations from px-admin", func() {
			log.InfoD("Delete Backup locations from px-admin")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch ctx for admin")
			for backupLocationUID, backupLocationName := range backupLocationMap {
				wg.Add(1)
				go func(backupLocationName, backupLocationUID string) {
					defer GinkgoRecover()
					defer wg.Done()
					err := DeleteBackupLocationWithContext(backupLocationName, backupLocationUID, BackupOrgID, true, ctx)
					Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of backup location [%s]", backupLocationName))
				}(backupLocationName, backupLocationUID)
			}
			wg.Wait()
		})
		Step("Wait for Backup location deletion", func() {
			log.InfoD("Wait for Backup location deletion")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch ctx for admin")
			AllBackupLocationMap, err := GetAllBackupLocations(ctx)
			log.FailOnError(err, "Fetching all backup locations")
			for backupLocationUID, backupLocationName := range AllBackupLocationMap {
				wg.Add(1)
				go func(backupLocationName, backupLocationUID string) {
					defer GinkgoRecover()
					defer wg.Done()
					err := Inst().Backup.WaitForBackupLocationDeletion(ctx, backupLocationName, backupLocationUID, BackupOrgID, BackupLocationDeleteTimeout, BackupLocationDeleteRetryTime)
					Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying waiting for backup location [%s] deletion", backupLocationName))
				}(backupLocationName, backupLocationUID)
			}
			wg.Wait()
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Deleting the restores")
		for _, restoreName := range restoreNames {
			wg.Add(1)
			go func(restoreName string) {
				defer GinkgoRecover()
				defer wg.Done()
				err = DeleteRestore(restoreName, BackupOrgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
			}(restoreName)
		}
		wg.Wait()
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
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		err = DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")
		log.InfoD("Deleting the px-backup objects")
		backupLocationMap, err := GetAllBackupLocations(ctx)
		log.FailOnError(err, "Fetching all backup locations")
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})
