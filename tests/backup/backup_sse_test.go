package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/node/ssh"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	v1 "k8s.io/api/core/v1"
	storageApi "k8s.io/api/storage/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
	"sync"
	"time"
)

var _ = Describe("{CreateBackupAndRestoreForAllCombinationsOfSSES3AndDenyPolicy}", func() {

	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/93014
	var (
		scheduledAppContexts                 []*scheduler.Context
		backupLocationUID                    string
		cloudCredUID                         string
		cloudCredUidForBlWithoutSse          string
		backupsWithSse                       []string
		backupsWithOutSse                    []string
		cloudCredUidList                     []string
		customBackupLocationWithSse          string
		customBucketWithOutPolicy            string
		scheduleName                         string
		backupLocationUidWithoutSse          string
		backupLocationWithoutSse             string
		credName                             string
		credNameForBlWithoutSse              string
		customBucketsWithDenyPolicy          []string
		bkpNamespaces                        []string
		sourceScName                         *storageApi.StorageClass
		scCount                              int
		scNames                              []string
		clusterStatus                        api.ClusterInfo_StatusInfo_Status
		clusterUid                           string
		restoreList                          []string
		schedulePolicyName                   string
		schedulePolicyUid                    string
		latestScheduleBackupName             string
		backupName                           string
		appContextsToBackup                  []*scheduler.Context
		backupNameAfterRemovalOfDenyPolicy   string
		newBackupLocationWithSseAfterRestart string
		backupNameAfterPxBackupRestart       string
		customBuckets                        []string
		randomStringLength                   = 10
		backupsAfterSettingSseTrue           []string
	)
	params := make(map[string]string)
	k8sStorage := storage.Instance()
	backupLocationMap := make(map[string]string)
	namespaceBackupMap := make(map[string][]*scheduler.Context)
	restoreNsScMap := make(map[string][]map[string]string)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("CreateBackupAndRestoreForAllCombinationsOfSSES3AndDenyPolicy",
			"Backup and restore from Backup location created with SSE-S3 and deny policy set from aws s3", nil, 93014, Sn, Q3FY24)
		log.InfoD("Deploy applications needed for backup")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		namespaces := 4
		for i := 0; i < namespaces; i++ {
			taskName := fmt.Sprintf("%s-%d-%s", taskNamePrefix, i, RandomString(randomStringLength))
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})
	It("Create backup and restore for all combinations of SSE-S3 form px-backup side and deny policy configured on AWS S3 bucket", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		providers := getProviders()

		for _, provider := range providers {
			Step("Validate applications", func() {
				log.InfoD("Validate applications")
				ValidateApplications(scheduledAppContexts)
			})
			Step("Register cluster for backup", func() {
				log.InfoD(fmt.Sprintf("Creating source and destination cluster"))
				err = CreateApplicationClusters(orgID, "", "", ctx)
				dash.VerifyFatal(err, nil, "Creating source and destination cluster")
				clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
				log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
				dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
				clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			})
			Step("Create bucket without deny policy", func() {
				log.InfoD(fmt.Sprintf("Create bucket without deny policy"))
				bucketWithOutPolicy := "sse-bucket-without-deny-policy"
				customBucketWithOutPolicy = GetCustomBucketName(provider, bucketWithOutPolicy)
				// Update custom bucket for cleanup purpose
				customBuckets = append(customBuckets, customBucketWithOutPolicy)
			})
			Step("Create bucket with deny policy", func() {
				log.InfoD(fmt.Sprintf("Create bucket with deny policy"))
				numberOfBucketsWithDenyPolicy := 2
				bucketWithDenyPolicy := "sse-bucket-with-deny-policy"
				for i := 0; i < numberOfBucketsWithDenyPolicy; i++ {
					customBucketWithDenyPolicy := GetCustomBucketName(provider, bucketWithDenyPolicy)
					policy, err := GenerateS3BucketPolicy(AwsS3Sid, AwsS3encryptionPolicy, customBucketWithDenyPolicy)
					log.FailOnError(err, "Failed to generate s3 bucket policy check for the correctness of policy parameters")
					err = UpdateS3BucketPolicy(customBucketWithDenyPolicy, policy)
					log.FailOnError(err, "Failed to apply bucket policy")
					customBucketsWithDenyPolicy = append(customBucketsWithDenyPolicy, customBucketWithDenyPolicy)
					customBuckets = append(customBuckets, customBucketWithDenyPolicy)
					log.InfoD("Updated S3 bucket with deny policy - %s", customBucketWithDenyPolicy)
				}
			})
			Step("Create BackupLocation with bucketWithPolicy and SSE set to false", func() {
				log.InfoD(fmt.Sprintf("Create BackupLocation with bucketWithOutPolicy and SSE set to false"))
				cloudCredUidForBlWithoutSse = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUidWithoutSse = uuid.New()
				credNameForBlWithoutSse = fmt.Sprintf("autogenerated-cred-%s-%v", RandomString(randomStringLength), time.Now().Unix())
				err := CreateCloudCredential(provider, credNameForBlWithoutSse, cloudCredUidForBlWithoutSse, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credNameForBlWithoutSse, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credNameForBlWithoutSse)
				backupLocationWithoutSse = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				err = CreateBackupLocation(provider, backupLocationWithoutSse, backupLocationUidWithoutSse, credNameForBlWithoutSse, cloudCredUidForBlWithoutSse, customBucketsWithDenyPolicy[0], orgID, "", true)
				dash.VerifyFatal(strings.Contains(err.Error(), "AccessDenied"), true, fmt.Sprintf("verifying backup location creation fail in case of bucket with deny policy and SSE set to false : %s", err.Error()))
			})
			Step("Create BackupLocation with bucketWithOutPolicy and SSE set to false", func() {
				log.InfoD(fmt.Sprintf("Create BackupLocation with bucketWithOutPolicy and SSE set to false"))
				cloudCredUidForBlWithoutSse = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUidWithoutSse = uuid.New()
				credNameForBlWithoutSse = fmt.Sprintf("autogenerated-cred-%s", RandomString(randomStringLength))
				err := CreateCloudCredential(provider, credNameForBlWithoutSse, cloudCredUidForBlWithoutSse, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credNameForBlWithoutSse, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credNameForBlWithoutSse)
				backupLocationWithoutSse = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				err = CreateBackupLocation(provider, backupLocationWithoutSse, backupLocationUidWithoutSse, credNameForBlWithoutSse, cloudCredUidForBlWithoutSse, customBucketWithOutPolicy, orgID, "", true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocationWithoutSse))
				backupLocationMap[backupLocationUidWithoutSse] = backupLocationWithoutSse
				log.Infof("created backup location successfully")
			})
			Step("Create BackupLocation with bucketWithPolicy and SSE set to true", func() {
				log.InfoD("Create BackupLocation with bucketWithPolicy and SSE set to true")
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
				err := CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				customBackupLocationWithSse = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				err = CreateS3BackupLocation(customBackupLocationWithSse, backupLocationUID, credName, cloudCredUID, customBucketsWithDenyPolicy[0], orgID, "", true, api.S3Config_SSE_S3)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", customBackupLocationWithSse))
				backupLocationMap[backupLocationUID] = customBackupLocationWithSse
				log.Infof("created backup location successfully")
			})
			// Calculate the midpoint to split the namespaces
			midpoint := len(bkpNamespaces) / 2
			Step("Create Backup with backupLocationWithoutSse", func() {
				log.InfoD("Taking backup of application with backupLocationWithoutSse for different combination of restores")
				var mutex sync.Mutex
				errors := make([]string, 0)
				var wg sync.WaitGroup
				bkpNamespacesForWithoutSse := bkpNamespaces[:midpoint]
				for _, backupNameSpace := range bkpNamespacesForWithoutSse {
					backupName = fmt.Sprintf("%s-%s-%s-%v", BackupNamePrefix, backupNameSpace, RandomString(randomStringLength), time.Now().Unix())
					wg.Add(1)
					go func(backupNameSpace string, backupName string) {
						defer GinkgoRecover()
						defer wg.Done()
						appContextsToBackup = FilterAppContextsByNamespace(scheduledAppContexts, []string{backupNameSpace})
						err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationWithoutSse, backupLocationUidWithoutSse, appContextsToBackup, make(map[string]string), orgID, clusterUid, "", "", "", "")
						if err != nil {
							mutex.Lock()
							errors = append(errors, fmt.Sprintf("Failed while creating backup %s . Error - [%s]", backupName, err.Error()))
							mutex.Unlock()
							return
						}
					}(backupNameSpace, backupName)
					backupsWithOutSse = append(backupsWithOutSse, backupName)
				}
				wg.Wait()
				dash.VerifyFatal(len(errors), 0, fmt.Sprintf("Creation and Validation of backup"))
			})
			Step("Create Backup with backupLocationWithSse", func() {
				log.InfoD("Taking backup of application with backupLocationWithSse in parallel for different combination of restores")
				var mutex sync.Mutex
				errors := make([]string, 0)
				var wg sync.WaitGroup
				bkpNamespacesWithSse := bkpNamespaces[midpoint:]
				for _, backupNameSpace := range bkpNamespacesWithSse {
					backupName = fmt.Sprintf("%s-%s-%s-%v", BackupNamePrefix, backupNameSpace, RandomString(randomStringLength), time.Now().Unix())
					wg.Add(1)
					go func(backupNameSpace string, backupName string) {
						defer GinkgoRecover()
						defer wg.Done()
						appContextsToBackup = FilterAppContextsByNamespace(scheduledAppContexts, []string{backupNameSpace})
						err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, customBackupLocationWithSse, backupLocationUID, appContextsToBackup, make(map[string]string), orgID, clusterUid, "", "", "", "")
						if err != nil {
							mutex.Lock()
							errors = append(errors, fmt.Sprintf("Failed while creating backup %s . Error - [%s]", backupName, err.Error()))
							mutex.Unlock()
							return
						}
					}(backupNameSpace, backupName)
					backupsWithSse = append(backupsWithSse, backupName)
				}
				wg.Wait()
				dash.VerifyFatal(len(errors), 0, fmt.Sprintf("Creation and Validation of backup"))
			})
			Step("Create and validate restore with replace policy set to retain", func() {
				log.InfoD("Create and validate restore with replace policy set to retain")
				restoreName := fmt.Sprintf("restore-with-replace-%s-%s-%v", RestoreNamePrefix, RandomString(randomStringLength), time.Now().Unix())
				err = CreateRestoreWithReplacePolicy(restoreName, backupsWithSse[0], make(map[string]string), SourceClusterName, orgID, ctx, make(map[string]string), 2)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
				restoreList = append(restoreList, restoreName)
				backupNamespace, err := FetchNamespacesFromBackup(ctx, backupsWithSse[0], orgID)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching namespaces %s from schedule backup %s", backupNamespace, backupsWithSse[0]))
				appContextsToBackup = FilterAppContextsByNamespace(scheduledAppContexts, backupNamespace)
				expectedRestoredAppContextListNew := make([]*scheduler.Context, 0)
				for _, appContextToBackup := range appContextsToBackup {
					expectedRestoredAppContext, err := CloneAppContextAndTransformWithMappings(appContextToBackup, make(map[string]string), make(map[string]string), true)
					if err != nil {
						log.FailOnError(err, "Failed while context transforming of restore")
					}
					expectedRestoredAppContextListNew = append(expectedRestoredAppContextListNew, expectedRestoredAppContext)
				}
				err = ValidateRestore(ctx, restoreName, orgID, expectedRestoredAppContextListNew, make([]string, 0))
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validating restores %s of  backups -%s", restoreName, backupsWithSse[0]))
			})
			Step("Create new storage class on source cluster for storage class mapping for restore", func() {
				log.InfoD("Create new storage class on source cluster for storage class mapping for restore")
				scCount = 3
				for i := 0; i < scCount; i++ {
					scName := fmt.Sprintf("replica-sc-%d-%s-%v", time.Now().Unix(), RandomString(randomStringLength), i)
					params["repl"] = "2"
					v1obj := metaV1.ObjectMeta{
						Name: scName,
					}
					reclaimPolicyDelete := v1.PersistentVolumeReclaimDelete
					bindMode := storageApi.VolumeBindingImmediate
					scObj := storageApi.StorageClass{
						ObjectMeta:        v1obj,
						Provisioner:       k8s.CsiProvisioner,
						Parameters:        params,
						ReclaimPolicy:     &reclaimPolicyDelete,
						VolumeBindingMode: &bindMode,
					}

					_, err := k8sStorage.CreateStorageClass(&scObj)
					dash.VerifyFatal(err, nil, fmt.Sprintf("c %v on source cluster %s", scName, SourceClusterName))
					scNames = append(scNames, scName)
				}
			})
			Step("Multiple restore for same backup in different storage class at the same time", func() {
				log.InfoD(fmt.Sprintf("Multiple restore for same backup into %d different storage class at the same time", scCount))
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				backupNamespace, err := FetchNamespacesFromBackup(ctx, backupsWithSse[0], orgID)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching namespaces from schedule backup %s", backupsWithSse[0]))
				pvcs, err := core.Instance().GetPersistentVolumeClaims(backupNamespace[0], make(map[string]string))
				singlePvc := pvcs.Items[0]
				sourceScName, err = core.Instance().GetStorageClassForPVC(&singlePvc)
				var mutex sync.Mutex
				errors := make([]string, 0)
				var wg sync.WaitGroup
				for i := 0; i < scCount; i++ {
					storageClassMapping := make(map[string]string)
					namespaceMap := make(map[string]string)
					storageClassMapping[sourceScName.Name] = scNames[i]
					namespaceMap[bkpNamespaces[midpoint]] = fmt.Sprintf("new-namespace-%s-%v", RandomString(randomStringLength), time.Now().Unix())
					restoreName := fmt.Sprintf("restore-new-storage-class-%s-%s", scNames[i], RestoreNamePrefix)
					restoreList = append(restoreList, restoreName)
					restoreNsScMap[restoreName] = []map[string]string{namespaceMap, storageClassMapping}
					wg.Add(1)
					go func(scName string) {
						defer GinkgoRecover()
						defer wg.Done()
						err = CreateRestore(restoreName, backupsWithSse[0], namespaceMap, SourceClusterName, orgID, ctx, storageClassMapping)
						if err != nil {
							mutex.Lock()
							errors = append(errors, fmt.Sprintf("Failed while restoring %s backup %s with sc mapping %s . Error - [%s]", backupsWithSse[0], restoreName, scName, err.Error()))
							mutex.Unlock()
							return
						}
					}(scNames[i])
				}
				wg.Wait()
				dash.VerifyFatal(len(errors), 0, fmt.Sprintf("Restoring backup %v using storage class", backupName))
			})

			Step("Validating restores with sc and ns mapping", func() {
				log.InfoD("Validating restores with sc and ns mapping")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				backupNamespace, err := FetchNamespacesFromBackup(ctx, backupsWithSse[0], orgID)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching namespaces %s from schedule backup %s", backupNamespace, backupsWithSse[0]))
				appContextsToBackups := FilterAppContextsByNamespace(scheduledAppContexts, backupNamespace)
				var mutex sync.Mutex
				errors := make([]string, 0)
				errChan := make(chan error, len(restoreList))
				semaphore := make(chan int, 4)
				var wg sync.WaitGroup
				for restoreName, mapNsSc := range restoreNsScMap {
					expectedRestoredAppContextList := make([]*scheduler.Context, 0)
					wg.Add(1)
					mutex.Lock()
					go func(restoreName string, mapNsSc []map[string]string) {
						defer GinkgoRecover()
						defer wg.Done()
						semaphore <- 0
						defer func() { <-semaphore }()
						log.InfoD("Validating restore [%s]", restoreName)
						for _, appContextsToBackup := range appContextsToBackups {
							expectedRestoredAppContext, err := CloneAppContextAndTransformWithMappings(appContextsToBackup, mapNsSc[0], mapNsSc[1], true)
							if err != nil {
								errChan <- err
							}
							expectedRestoredAppContextList = append(expectedRestoredAppContextList, expectedRestoredAppContext)
						}
						err = ValidateRestore(ctx, restoreName, orgID, expectedRestoredAppContextList, make([]string, 0))
						if err != nil {
							errChan <- err
						}
						mutex.Unlock()
					}(restoreName, mapNsSc)
				}
				wg.Wait()
				close(errChan)
				close(semaphore)
				for err := range errChan {
					errors = append(errors, err.Error())
				}
				dash.VerifyFatal(len(errors), 0, fmt.Sprintf("Validating restores with sc and ns mapping  for restores with error -%v", errors))

			})

			Step("Update backup location backupLocationUidWithoutSse to sse true", func() {
				log.InfoD("Update backup location backupLocationUidWithoutSse to sse true")
				err = UpdateBackupLocation(provider, backupLocationWithoutSse, backupLocationUidWithoutSse, orgID, credNameForBlWithoutSse, cloudCredUidForBlWithoutSse, ctx, api.S3Config_SSE_S3)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Updation of backuplocation [%s]", backupLocationWithoutSse))
			})

			// Take backup with backupLocationWithoutSse
			Step("Create backup of application with backupLocationWithoutSse after updating the sse to true", func() {
				log.InfoD("Taking backup of application with backupLocationWithoutSse after updating the sse to true")
				var wg sync.WaitGroup
				var mutex sync.Mutex
				errors := make([]string, 0)
				bkpNamespacesForWithoutSse := bkpNamespaces[:midpoint]
				for _, backupNameSpace := range bkpNamespacesForWithoutSse {
					backupName = fmt.Sprintf("%s-%s-%s-%v", BackupNamePrefix, backupNameSpace, RandomString(randomStringLength), time.Now().Unix())
					wg.Add(1)
					go func(backupNameSpace string, backupName string) {
						defer GinkgoRecover()
						defer wg.Done()
						appContextsToBackup = FilterAppContextsByNamespace(scheduledAppContexts, []string{backupNameSpace})
						err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationWithoutSse, backupLocationUidWithoutSse, appContextsToBackup, make(map[string]string), orgID, clusterUid, "", "", "", "")
						if err != nil {
							mutex.Lock()
							errors = append(errors, fmt.Sprintf("Failed while creating backup [%s]. Error - [%s]", backupName, err.Error()))
							mutex.Unlock()
							return
						}
						namespaceBackupMap[backupName] = appContextsToBackup
					}(backupNameSpace, backupName)
					backupsAfterSettingSseTrue = append(backupsAfterSettingSseTrue, backupName)
				}
				wg.Wait()
				dash.VerifyFatal(len(errors), 0, fmt.Sprintf("Creation backup"))
			})
			Step("Restoring the backed up applications after setting the sse to true of backupLocationWithoutSse", func() {
				log.InfoD("Restoring the backed up applications after setting the sse to true of backupLocationWithoutSse")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				restoreName := fmt.Sprintf("%s-%s-%v", "restore-after-bl-sse-true", RandomString(randomStringLength), time.Now().Unix())
				err = CreateRestoreWithValidation(ctx, restoreName, backupsAfterSettingSseTrue[0], make(map[string]string), make(map[string]string), destinationClusterName, orgID, namespaceBackupMap[backupsAfterSettingSseTrue[0]])
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
				restoreList = append(restoreList, restoreName)
			})
			Step("kill stork", func() {
				log.InfoD("Kill stork")
				pxNamespace, err := ssh.GetExecPodNamespace()
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching PX namespace %s", pxNamespace))
				err = DeletePodWithWithoutLabelInNamespace(pxNamespace, storkLabel, false)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Killing stork after toggling SSE-S3 type"))
			})
			Step("Restart backup pod", func() {
				log.InfoD("Restart backup pod")
				backupPodLabel := make(map[string]string)
				backupPodLabel["app"] = "px-backup"
				pxbNamespace, err := backup.GetPxBackupNamespace()
				dash.VerifyFatal(err, nil, "Getting px-backup namespace")
				err = DeletePodWithWithoutLabelInNamespace(pxbNamespace, backupPodLabel, false)
				dash.VerifyFatal(err, nil, "Restart backup pod")
				err = ValidatePodByLabel(backupPodLabel, pxbNamespace, 5*time.Minute, 30*time.Second)
				log.FailOnError(err, "Checking if px-backup pod is in running state")
			})
			Step("Create a schedule policy", func() {
				log.InfoD("Create a schedule policy")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				schedulePolicyintervalInMins := 15
				log.InfoD("Creating a schedule policy with interval [%v] mins", schedulePolicyintervalInMins)
				schedulePolicyName = fmt.Sprintf("interval-%v-%v", schedulePolicyintervalInMins, time.Now().Unix())
				schedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, int64(schedulePolicyintervalInMins), 5)
				err = Inst().Backup.BackupSchedulePolicy(schedulePolicyName, uuid.New(), orgID, schedulePolicyInfo)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule policy [%s] with interval [%v] mins", schedulePolicyName, schedulePolicyintervalInMins))
				schedulePolicyUid, err = Inst().Backup.GetSchedulePolicyUid(orgID, ctx, schedulePolicyName)
				log.FailOnError(err, "Fetching uid of schedule policy [%s]", schedulePolicyName)
				log.Infof("Schedule policy [%s] uid: [%s]", schedulePolicyName, schedulePolicyUid)
			})
			Step("Create schedule backup", func() {
				log.InfoD("Creating a schedule backup")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				scheduleName = fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, time.Now().Unix())
				labelSelectors := make(map[string]string)
				latestScheduleBackupName, err = CreateScheduleBackupWithValidation(ctx, scheduleName, SourceClusterName, customBackupLocationWithSse, backupLocationUID, scheduledAppContexts, labelSelectors, orgID, "", "", "", "", schedulePolicyName, schedulePolicyUid)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of schedule backup with schedule name [%s]", scheduleName))
			})
			Step("Restoring the backed up applications from latest scheduled backup which is created after stork and px-backup pod restart", func() {
				log.InfoD("Restoring the backed up applications from latest scheduled backup")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				restoreName := fmt.Sprintf("%s-%s", "restore-from-schedule", RandomString(10))
				err = CreateRestoreWithValidation(ctx, restoreName, latestScheduleBackupName, make(map[string]string), make(map[string]string), destinationClusterName, orgID, scheduledAppContexts)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
				restoreList = append(restoreList, restoreName)
			})
			// Create BackupLocation with bucketWithPolicy and SSE set to true after px-backup and stork restart
			Step("Create BackupLocation with bucketWithPolicy and SSE set to true post px-backup and stork restart", func() {
				log.InfoD("Create BackupLocation with bucketWithPolicy and SSE set to true post px-backup and stork restart")
				cloudCredUID = uuid.New()
				cloudCredUidList = append(cloudCredUidList, cloudCredUID)
				backupLocationUID = uuid.New()
				credName = fmt.Sprintf("autogenerated-cred-%s-%v", RandomString(randomStringLength), time.Now().Unix())
				err := CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, orgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				newBackupLocationWithSseAfterRestart = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				err = CreateS3BackupLocation(newBackupLocationWithSseAfterRestart, backupLocationUID, credName, cloudCredUID, customBucketsWithDenyPolicy[1], orgID, "", true, api.S3Config_SSE_S3)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", newBackupLocationWithSseAfterRestart))
				backupLocationMap[backupLocationUID] = newBackupLocationWithSseAfterRestart
				log.Infof("created backup location successfully")
			})
			Step("Taking backup of application with new BackupLocation post px-backup and stork restart", func() {
				log.InfoD("Taking backup of application with new BackupLocation post px-backup and stork restart")
				backupNameAfterPxBackupRestart = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, bkpNamespaces[0], time.Now().Unix())
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespaces[0]})
				err = CreateBackupWithValidation(ctx, backupNameAfterPxBackupRestart, SourceClusterName, newBackupLocationWithSseAfterRestart, backupLocationUID, appContextsToBackup, make(map[string]string), orgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup with new BackupLocation post px-backup and stork restart [%s]", backupNameAfterPxBackupRestart))
			})
			Step("Create restore with backup taken on new BackupLocation created post px-backup and stork restart", func() {
				log.InfoD("Create restore with backup taken on new BackupLocation created post px-backup and stork restart")
				restoreName := fmt.Sprintf("restore-with-replace-from-new-bl-%s-%v", RestoreNamePrefix, time.Now().Unix())
				err = CreateRestoreWithReplacePolicy(restoreName, backupNameAfterPxBackupRestart, make(map[string]string), SourceClusterName, orgID, ctx, make(map[string]string), 2)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
				restoreList = append(restoreList, restoreName)
				backupNamespace, err := FetchNamespacesFromBackup(ctx, backupNameAfterPxBackupRestart, orgID)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching namespaces %s from schedule backup %s", backupNamespace, backupNameAfterPxBackupRestart))
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, backupNamespace)
				err = ValidateRestore(ctx, restoreName, orgID, appContextsToBackup, make([]string, 0))
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validating restores %s of  backups -%s", restoreName, backupNameAfterPxBackupRestart))
			})
			Step("Remove deny policy from S3 bucket which is used in customBackupLocationWithSse", func() {
				log.InfoD("Remove deny policy from S3 bucket")
				err = RemoveS3BucketPolicy(customBucketsWithDenyPolicy[1])
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verify removal of deny policy from s3 bucket"))
			})
			Step("Taking backup of application after removal of deny policy and sse still set to true", func() {
				log.InfoD("Taking backup of application after removal of deny policy and sse still set to true")
				backupNameAfterRemovalOfDenyPolicy = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, bkpNamespaces[0], time.Now().Unix())
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespaces[0]})
				err = CreateBackupWithValidation(ctx, backupNameAfterRemovalOfDenyPolicy, SourceClusterName, newBackupLocationWithSseAfterRestart, backupLocationUID, appContextsToBackup, make(map[string]string), orgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup after removal of deny policy and sse still set to true [%s]", backupNameAfterRemovalOfDenyPolicy))
			})
			Step("Create restore with replace policy set to retain after removal of deny policy", func() {
				log.InfoD("Create restore with replace policy set to retain after removal of deny policy")
				restoreName := fmt.Sprintf("restore-with-replace-post-deny-policy-removal-%s-%v", RestoreNamePrefix, time.Now().Unix())
				err = CreateRestoreWithReplacePolicy(restoreName, backupNameAfterRemovalOfDenyPolicy, make(map[string]string), SourceClusterName, orgID, ctx, make(map[string]string), 2)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore [%s]", restoreName))
				restoreList = append(restoreList, restoreName)
				backupNamespace, err := FetchNamespacesFromBackup(ctx, backupNameAfterRemovalOfDenyPolicy, orgID)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching namespaces %s from schedule backup %s", backupNamespace, backupNameAfterRemovalOfDenyPolicy))
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, backupNamespace)
				err = ValidateRestore(ctx, restoreName, orgID, appContextsToBackup, make([]string, 0))
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validating restores %s of  backups -%s", restoreName, backupNameAfterRemovalOfDenyPolicy))
			})
		}
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		// Delete backup schedule policy
		log.Infof("Deleting backup schedule policy")
		err = DeleteSchedule(scheduleName, SourceClusterName, orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
		// Delete restores
		for _, restoreName := range restoreList {
			err = DeleteRestore(restoreName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
		}
		// Delete custom buckets
		providers := getProviders()
		for _, provider := range providers {
			for _, customBucket := range customBuckets {
				log.InfoD("Deleting bucket  - %s", customBucket)
				DeleteBucket(provider, customBucket)
			}
		}
	})
})
