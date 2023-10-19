package tests

import (
	"fmt"
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

// MutipleBackupLocationWithSameEndpoint Create Backup and Restore for Mutiple backup location added using same endpoint.
var _ = Describe("{MutipleBackupLocationWithSameEndpoint}", func() {
	var (
		scheduledAppContexts          []*scheduler.Context
		backupLocationNameMap         = make(map[int]string)
		backupLocationUIDMap          = make(map[int]string)
		backupLocationMap             = make(map[string]string)
		bkpNamespaces                 []string
		cloudCredName                 string
		cloudCredUID                  string
		clusterUid                    string
		labelSelectors                map[string]string
		mutex                         sync.Mutex
		wg                            sync.WaitGroup
		userBackupMap                 = make(map[int]map[string]string)
		restoreNames                  []string
		numberOfBackupLocation        = 1000
		numberOfBackups               = 30
		providers                     = getProviders()
		timeBetweenConsecutiveBackups = 10 * time.Second
	)

	JustBeforeEach(func() {
		StartTorpedoTest("MutipleBackupLocationWithSameEndpoint", "Create Backup and Restore for Mutiple backup location added using same endpoint", nil, 86088)
		log.InfoD("scheduling applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, appCtx := range appContexts {
				appCtx.ReadinessTimeout = appReadinessTimeout
				scheduledAppContexts = append(scheduledAppContexts, appCtx)
				namespace := GetAppNamespace(appCtx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
			}
		}
	})

	It("Create Backup and Restore for Mutiple backup location added using same endpoint", func() {
		Step("Validate applications", func() {
			ValidateApplications(scheduledAppContexts)
		})
		Step(fmt.Sprintf("Creating a cloud credentials from px-admin"), func() {
			log.InfoD(fmt.Sprintf("Creating a cloud credentials from px-admin"))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				cloudCredUID = uuid.New()
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, orgID, provider))
			}
		})
		Step(fmt.Sprintf("Creating [%d] backup locations from px-admin", numberOfBackupLocation), func() {
			log.InfoD(fmt.Sprintf("Creating [%d] backup locations from px-admin", numberOfBackupLocation))
			for i := 0; i <= numberOfBackupLocation; i++ {
				for _, provider := range providers {
					log.InfoD(fmt.Sprintf("Creating backup locations with index [%d]", i))
					backupLocationNameMap[i] = fmt.Sprintf("%s-%d-%s", getGlobalBucketName(provider), i, RandomString(6))
					backupLocationUIDMap[i] = uuid.New()
					err := CreateBackupLocation(provider, backupLocationNameMap[i], backupLocationUIDMap[i], cloudCredName, cloudCredUID, getGlobalBucketName(provider), orgID, "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup location [%s]", backupLocationNameMap[i]))
					backupLocationMap[backupLocationUIDMap[i]] = backupLocationNameMap[i]
				}
			}
		})
		Step("Registering cluster for backup from px-admin", func() {
			log.InfoD("Registering cluster for backup from px-admin")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			log.InfoD("Verifying cluster status for both source and destination clusters")
			clusterStatus, err := Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, destinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", destinationClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", destinationClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})
		Step(fmt.Sprintf("Taking [%d] backup for the each application from px-admin", numberOfBackups), func() {
			log.InfoD(fmt.Sprintf("Taking [%d] backup for the each application from px-admin", numberOfBackups))
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch ctx for admin")
			createBackup := func(backupName string, namespace string, index int) {
				defer GinkgoRecover()
				defer wg.Done()
				err := CreateBackup(backupName, SourceClusterName, backupLocationNameMap[index], backupLocationUIDMap[index], []string{namespace}, labelSelectors, orgID, clusterUid, "", "", "", "", ctx)
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
			log.InfoD("Taking restore for each backups created from px-admin")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch ctx for admin")
			createRestore := func(backupName string, restoreName string, namespace string) {
				defer GinkgoRecover()
				defer wg.Done()
				customNamespace := "custom-" + namespace + RandomString(4)
				namespaceMapping := map[string]string{namespace: customNamespace}
				err := CreateRestore(restoreName, backupName, namespaceMapping, destinationClusterName, orgID, ctx, make(map[string]string))
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s of backup %s", restoreName, backupName))
				restoreNames = SafeAppend(&mutex, restoreNames, restoreName).([]string)
			}
			for index := 0; index < numberOfBackups; index++ {
				for backupName, namespace := range userBackupMap[index] {
					wg.Add(1)
					restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, backupName)
					go createRestore(backupName, restoreName, namespace)
				}
			}
			wg.Wait()
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
					err := DeleteBackupLocationWithContext(backupLocationName, backupLocationUID, orgID, true, ctx)
					Inst().Dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of backup location [%s]", backupLocationName))
				}(backupLocationName, backupLocationUID)
			}
			wg.Wait()
		})
		Step("Wait for Backup location deletion", func() {
			log.InfoD("Wait for Backup location deletion")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch ctx for admin")
			AllBackupLocationMap, err := getAllBackupLocations(ctx)
			log.FailOnError(err, "Fetching all backup locations")
			for backupLocationUID, backupLocationName := range AllBackupLocationMap {
				wg.Add(1)
				go func(backupLocationName, backupLocationUID string) {
					defer GinkgoRecover()
					defer wg.Done()
					err := Inst().Backup.WaitForBackupLocationDeletion(ctx, backupLocationName, backupLocationUID, orgID, backupLocationDeleteTimeout, backupLocationDeleteRetryTime)
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
				err = DeleteRestore(restoreName, orgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
			}(restoreName)
		}
		wg.Wait()
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		log.InfoD("Deleting the px-backup objects")
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})
