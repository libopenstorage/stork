package tests

import (
	"fmt"
	"sync"
	"time"

	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"golang.org/x/sync/errgroup"

	. "github.com/onsi/ginkgo/v2"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/storage"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/backup/portworx"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/scheduler/k8s"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/log"
	v1 "k8s.io/api/core/v1"
	storageApi "k8s.io/api/storage/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/portworx/torpedo/tests"
)

// This test case creates a backup location with encryption
var _ = Describe("{BackupLocationWithEncryptionKey}", Label(TestCaseLabelsMap[BackupLocationWithEncryptionKey]...), func() {
	var (
		scheduledAppContexts []*scheduler.Context
		bkpNamespaces        []string
		backupLocationName   string
		backupLocationUID    string
		cloudCredUID         string
		clusterUid           string
		cloudCredName        string
		restoreName          string
		backupName           string
		clusterStatus        api.ClusterInfo_StatusInfo_Status
		controlChannel       chan string
		errorGroup           *errgroup.Group
	)

	var (
		backupLocationMap = make(map[string]string)
		providers         = GetBackupProviders()
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("BackupLocationWithEncryptionKey", "Creating Backup Location with Encryption Keys", nil, 79918, Skonda, Q4FY23)

		log.InfoD("Deploy applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = AppReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})

	It("Creating Backup Location with Encryption Keys", func() {

		Step("Validate applications", func() {
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
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
				encryptionKey := GenerateEncryptionKey()
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), BackupOrgID, encryptionKey, true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location [%s] with CloudCred [%s]", backupLocationName, cloudCredName))
			}
		})

		Step("Register clusters for backup", func() {
			log.InfoD("Register clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			backupName = fmt.Sprintf("%s-%v", BackupNamePrefix, RandomString(10))
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, nil, BackupOrgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
		})

		Step("Restoring the backed up application", func() {
			log.InfoD("Restoring the backed up application")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName = fmt.Sprintf("%s-%s-%v", RestoreNamePrefix, backupName, time.Now().Unix())
			appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateRestoreWithValidation(ctx, restoreName, backupName, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsToBackup)
			log.FailOnError(err, "%s restore failed", restoreName)
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.Infof("Deleting backup, restore and backup location, cloud account")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		err = DeleteRestore(restoreName, BackupOrgID, ctx)
		dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))
		backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, BackupOrgID)
		dash.VerifyFatal(err, nil, fmt.Sprintf("Getting backup UID for backup %s", backupName))
		_, err = DeleteBackup(backupName, backupUID, BackupOrgID, ctx)
		dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting backup %s", backupName))
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
		opts := make(map[string]bool)
		err = DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")
	})
})

// Change replica while restoring backup through StorageClass Mapping.
var _ = Describe("{ReplicaChangeWhileRestore}", Label(TestCaseLabelsMap[ReplicaChangeWhileRestore]...), func() {
	var (
		controlChannel chan string
		errorGroup     *errgroup.Group
	)
	namespaceMapping := make(map[string]string)
	storageClassMapping := make(map[string]string)
	var scheduledAppContexts []*scheduler.Context
	CloudCredUIDMap := make(map[string]string)
	var backupLocation string
	var backupLocationUID string
	var cloudCredUID string
	backupLocationMap := make(map[string]string)
	var bkpNamespaces []string
	var clusterUid string
	var cloudCredName string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	var backupName string
	var restoreName string
	bkpNamespaces = make([]string, 0)
	labelSelectors := make(map[string]string)
	params := make(map[string]string)
	var backupNames []string
	var scName string

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("ReplicaChangeWhileRestore", "Change replica while restoring backup", nil, 58065, Kshithijiyer, Q4FY23)
		log.InfoD("Deploy applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = AppReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})

	It("Change replica while restoring backup", func() {
		Step("Validate applications", func() {
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})

		Step("Creating cloud credentials", func() {
			log.InfoD("Creating cloud credentials")
			providers := GetBackupProviders()
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudCredName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				cloudCredUID = uuid.New()
				CloudCredUIDMap[cloudCredUID] = cloudCredName
				err := CreateCloudCredential(provider, cloudCredName, cloudCredUID, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudCredName, BackupOrgID, provider))
			}
		})

		Step("Register cluster for backup", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Creating backup location", func() {
			log.InfoD("Creating backup location")
			providers := GetBackupProviders()
			for _, provider := range providers {
				backupLocation = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocation
				err := CreateBackupLocation(provider, backupLocation, backupLocationUID, cloudCredName, cloudCredUID, getGlobalBucketName(provider), BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocation))
			}
		})

		Step("Taking backup of applications", func() {
			log.InfoD("Taking backup of applications")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocation, backupLocationUID, appContextsToBackup, labelSelectors, BackupOrgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
				backupNames = append(backupNames, backupName)
			}
		})

		Step("Create new storage class for restore", func() {
			log.InfoD("Create new storage class for restore")
			scName = fmt.Sprintf("replica-sc-%v", time.Now().Unix())
			params["repl"] = "2"
			k8sStorage := storage.Instance()
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
			dash.VerifyFatal(err, nil, "Verifying creation of new storage class")
		})

		Step("Restoring the backed up application", func() {
			log.InfoD("Restoring the backed up application")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")

			for i, backupName := range backupNames {
				restoreName = fmt.Sprintf("%s-%s-%v", RestoreNamePrefix, backupName, time.Now().Unix())
				pvcs, err := core.Instance().GetPersistentVolumeClaims(bkpNamespaces[i], labelSelectors)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Getting all PVCs from namespace [%s]. Total PVCs - %d", bkpNamespaces[i], len(pvcs.Items)))
				singlePvc := pvcs.Items[0]
				sourceScName, err := core.Instance().GetStorageClassForPVC(&singlePvc)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Getting SC from PVC - %s", singlePvc.GetName()))
				storageClassMapping[sourceScName.Name] = scName
				restoredNameSpace := fmt.Sprintf("%s-%s", bkpNamespaces[i], "restored")
				namespaceMapping[bkpNamespaces[i]] = restoredNameSpace
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{bkpNamespaces[i]})
				err = CreateRestoreWithValidation(ctx, restoreName, backupName, namespaceMapping, storageClassMapping, SourceClusterName, BackupOrgID, appContextsToBackup)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Restoring with custom Storage Class Mapping - %v", namespaceMapping))
			}
		})
		Step("Validate applications", func() {
			ValidateApplications(scheduledAppContexts)
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		err = DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")

		backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, BackupOrgID)
		dash.VerifyFatal(err, nil, fmt.Sprintf("Getting backup UID for backup %s", backupName))
		_, err = DeleteBackup(backupName, backupUID, BackupOrgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup [%s]", backupName))
		err = DeleteRestore(restoreName, BackupOrgID, ctx)
		dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore [%s]", restoreName))
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudCredName, cloudCredUID, ctx)
	})
})

// This testcase verifies resize after the volume is restored from a backup
var _ = Describe("{ResizeOnRestoredVolume}", Label(TestCaseLabelsMap[ResizeOnRestoredVolume]...), func() {
	var (
		appList              = Inst().AppList
		scheduledAppContexts []*scheduler.Context
		preRuleNameList      []string
		postRuleNameList     []string
		bkpNamespaces        []string
		clusterUid           string
		clusterStatus        api.ClusterInfo_StatusInfo_Status
		restoreName          string
		namespaceMapping     map[string]string
		credName             string
		controlChannel       chan string
		errorGroup           *errgroup.Group
	)
	labelSelectors := make(map[string]string)
	CloudCredUIDMap := make(map[string]string)
	BackupLocationMap := make(map[string]string)
	var backupLocation string
	scheduledAppContexts = make([]*scheduler.Context, 0)
	bkpNamespaces = make([]string, 0)
	backupNamespaceMap := make(map[string]string)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("ResizeOnRestoredVolume", "Resize after the volume is restored from a backup", nil, 58064, Kshithijiyer, Q4FY23)
		log.InfoD("Verifying if the pre/post rules for the required apps are present in the list or not")
		for i := 0; i < len(appList); i++ {
			if Contains(PostRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["post_action_list"]; ok {
					dash.VerifyFatal(ok, true, "Post Rule details mentioned for the apps")
				}
			}
			if Contains(PreRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["pre_action_list"]; ok {
					dash.VerifyFatal(ok, true, "Pre Rule details mentioned for the apps")
				}
			}
		}
		log.InfoD("Deploy applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = AppReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})
	It("Resize after the volume is restored from a backup", func() {
		providers := GetBackupProviders()
		Step("Validate applications", func() {
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})

		Step("Creating rules for backup", func() {
			log.InfoD("Creating pre rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				preRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], BackupOrgID, "pre")
				log.FailOnError(err, "Creating pre rule for deployed apps failed")
				dash.VerifyFatal(preRuleStatus, true, "Verifying pre rule for backup")
				preRuleNameList = append(preRuleNameList, ruleName)
			}
			log.InfoD("Creating post rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				postRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], BackupOrgID, "post")
				log.FailOnError(err, "Creating post rule for deployed apps failed")
				dash.VerifyFatal(postRuleStatus, true, "Verifying Post rule for backup")
				postRuleNameList = append(postRuleNameList, ruleName)
			}
		})

		Step("Creating cloud credentials", func() {
			log.InfoD("Creating cloud credentials")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				credName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				CloudCredUID = uuid.New()
				CloudCredUIDMap[CloudCredUID] = credName
				err := CreateCloudCredential(provider, credName, CloudCredUID, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, BackupOrgID, provider))
			}
		})

		Step("Creating backup location", func() {
			log.InfoD("Creating backup location")
			for _, provider := range providers {
				backupLocation = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				BackupLocationUID = uuid.New()
				BackupLocationMap[BackupLocationUID] = backupLocation
				err := CreateBackupLocation(provider, backupLocation, BackupLocationUID, credName, CloudCredUID, getGlobalBucketName(provider), BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, "Creating backup location")
				log.InfoD("Created Backup Location with name - %s", backupLocation)
			}
		})

		Step("Register cluster for backup", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Start backup of application to bucket", func() {
			for _, namespace := range bkpNamespaces {
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				preRuleUid, _ := Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleNameList[0])
				postRuleUid, _ := Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleNameList[0])
				backupName := fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				backupNamespaceMap[namespace] = backupName
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocation, BackupLocationUID, appContextsToBackup, labelSelectors, BackupOrgID, clusterUid, preRuleNameList[0], preRuleUid, postRuleNameList[0], postRuleUid)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
			}
		})

		Step("Restoring the backed up application", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName := backupNamespaceMap[namespace]
				restoreName = fmt.Sprintf("%s-%s", "test-restore", namespace)
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err = CreateRestoreWithValidation(ctx, restoreName, backupName, namespaceMapping, make(map[string]string), DestinationClusterName, BackupOrgID, appContextsToBackup)
				dash.VerifyFatal(err, nil, "Restore failed")
			}
		})

		Step("Resize volume after the restore is completed", func() {
			log.InfoD("Resize volume after the restore is completed")
			var err error
			for _, ctx := range scheduledAppContexts {
				var appVolumes []*volume.Volume
				log.InfoD(fmt.Sprintf("get volumes for %s app", ctx.App.Key))
				appVolumes, err = Inst().S.GetVolumes(ctx)
				log.FailOnError(err, "Failed to get volumes for app %s", ctx.App.Key)
				dash.VerifyFatal(len(appVolumes) > 0, true, "App volumes exist?")
				var requestedVols []*volume.Volume
				log.InfoD(fmt.Sprintf("Increase volume size %s on app %s's volumes: %v",
					Inst().V.String(), ctx.App.Key, appVolumes))
				requestedVols, err = Inst().S.ResizeVolume(ctx, Inst().ConfigMap)
				log.FailOnError(err, "Volume resize successful ?")
				log.InfoD(fmt.Sprintf("validate successful volume size increase on app %s's volumes: %v",
					ctx.App.Key, appVolumes))
				for _, v := range requestedVols {
					// Need to pass token before validating volume
					params := make(map[string]string)
					if Inst().ConfigMap != "" {
						params["auth-token"], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
						log.FailOnError(err, "Failed to get token from configMap")
					}
					err := Inst().V.ValidateUpdateVolume(v, params)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Validate volume %v update status", v))
				}
			}
		})

		Step("Validate applications post restore", func() {
			ValidateApplications(scheduledAppContexts)
		})

	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		err := DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")

		log.InfoD("Deleting backup location, cloud creds and clusters")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(BackupLocationMap, credName, CloudCredUID, ctx)
	})
})

// Restore backup from encrypted and non-encrypted backups
var _ = Describe("{RestoreEncryptedAndNonEncryptedBackups}", Label(TestCaseLabelsMap[RestoreEncryptedAndNonEncryptedBackups]...), func() {
	var (
		scheduledAppContexts []*scheduler.Context
		appContextsToBackup  []*scheduler.Context
		bkpNamespaces        []string
		backupNames          []string
		restoreNames         []string
		backupLocationNames  []string
		CloudCredUID         string
		BackupLocationUID    string
		BackupLocation1UID   string
		clusterUid           string
		clusterStatus        api.ClusterInfo_StatusInfo_Status
		CredName             string
		backupName           string
		encryptionBucketName string
		encryptedBackupName  string
		controlChannel       chan string
		errorGroup           *errgroup.Group
	)

	providers := GetBackupProviders()
	backupLocationMap := make(map[string]string)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("RestoreEncryptedAndNonEncryptedBackups", "Restore encrypted and non encrypted backups", nil, 79915, Skonda, Q4FY23)

		log.InfoD("Deploy applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = AppReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})

	It("Restore encrypted and non encrypted backups", func() {
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})

		Step("Creating encrypted and non-encrypted backup location", func() {
			log.InfoD("Creating bucket, encrypted and non-encrypted backup location")
			encryptionBucketName = fmt.Sprintf("%s-%s-%v", providers[0], "encryptionbucket", time.Now().Unix())
			backupLocationName := fmt.Sprintf("%s-%s", "location", providers[0])
			backupLocationNames = append(backupLocationNames, backupLocationName)
			backupLocationName = fmt.Sprintf("%s-%s-%v", "encryption-location", providers[0], time.Now().Unix())
			backupLocationNames = append(backupLocationNames, backupLocationName)
			CredName = fmt.Sprintf("%s-%s-%v", "cred", providers[0], time.Now().Unix())
			CloudCredUID = uuid.New()
			BackupLocationUID = uuid.New()
			BackupLocation1UID = uuid.New()
			encryptionKey := "px-b@ckup-@utomat!on"
			CreateBucket(providers[0], encryptionBucketName)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateCloudCredential(providers[0], CredName, CloudCredUID, BackupOrgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", CredName, BackupOrgID, providers[0]))
			err = CreateBackupLocation(providers[0], backupLocationNames[0], BackupLocationUID, CredName, CloudCredUID, getGlobalBucketName(providers[0]), BackupOrgID, "", true)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocationNames[0]))
			backupLocationMap[BackupLocationUID] = backupLocationNames[0]
			err = CreateBackupLocation(providers[0], backupLocationNames[1], BackupLocation1UID, CredName, CloudCredUID, encryptionBucketName, BackupOrgID, encryptionKey, true)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocationNames[1]))
			backupLocationMap[BackupLocation1UID] = backupLocationNames[1]
		})

		Step("Register cluster for backup", func() {
			log.InfoD("Register clusters for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Taking encrypted and non-encrypted backups", func() {
			log.InfoD("Taking encrypted and no-encrypted backups")
			backupName = fmt.Sprintf("%s-%v", BackupNamePrefix, RandomString(10))
			backupNames = append(backupNames, backupName)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			appContextsToBackup = FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
			err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationNames[0], BackupLocationUID, appContextsToBackup, nil, BackupOrgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
			encryptionBackupName := fmt.Sprintf("%s-%s-%s", "encryption", BackupNamePrefix, RandomString(10))
			backupNames = append(backupNames, encryptionBackupName)
			err = CreateBackupWithValidation(ctx, encryptionBackupName, SourceClusterName, backupLocationNames[1], BackupLocation1UID, appContextsToBackup, nil, BackupOrgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", encryptionBackupName))

		})

		Step("Restoring encrypted and non-encrypted backups", func() {
			log.InfoD("Restoring encrypted and non-encrypted backups")
			restoreName := fmt.Sprintf("%s-%s-%v", RestoreNamePrefix, backupNames[0], time.Now().Unix())
			restoreNames = append(restoreNames, restoreName)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateRestoreWithValidation(ctx, restoreName, backupNames[0], make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsToBackup)
			log.FailOnError(err, "%s restore failed", restoreName)
			time.Sleep(time.Minute * 5)
			restoreName = fmt.Sprintf("%s-%s", RestoreNamePrefix, backupNames[1])
			restoreNames = append(restoreNames, restoreName)
			err = CreateRestoreWithValidation(ctx, restoreName, backupNames[1], make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsToBackup)
			log.FailOnError(err, "%s restore failed", restoreName)
		})

		// PB-3962: Taking encrypted backup, validating the backup location and restoring the backup to make sure the encryption key is not lost after backup location validation which will result in restore failure
		Step("Taking new encrypted backup", func() {
			log.InfoD("Taking new encrypted backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			encryptedBackupName = fmt.Sprintf("new-%s-%s-%s", "encrypted", BackupNamePrefix, RandomString(10))
			backupNames = append(backupNames, encryptedBackupName)
			err = CreateBackupWithValidation(ctx, encryptedBackupName, SourceClusterName, backupLocationNames[1], BackupLocation1UID, appContextsToBackup, nil, BackupOrgID, clusterUid, "", "", "", "")
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of new encrypted backup [%s]", encryptedBackupName))
		})

		Step("Validate the encrypted backup location after taking backup", func() {
			log.InfoD("Validate the encrypted backup location after taking backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = ValidateBackupLocation(ctx, BackupOrgID, backupLocationNames[1], BackupLocation1UID)
			log.FailOnError(err, "backup location %s validation failed", backupLocationNames[1])
			err = WaitForBackupLocationAddition(ctx, backupLocationNames[1], BackupLocation1UID, BackupOrgID, BackupLocationValidationTimeout, BackupLocationValidationRetryTime)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Validation of backup location  [%s]", backupLocationNames[1]))
		})

		Step("Restore the encrypted backups after validating the encrypted backup location", func() {
			log.InfoD("Restore the encrypted backups after validating the encrypted backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			restoreName := fmt.Sprintf("%s-%s-encrypted", RestoreNamePrefix, encryptedBackupName)
			restoreNames = append(restoreNames, restoreName)
			err = CreateRestoreWithValidation(ctx, restoreName, encryptedBackupName, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsToBackup)
			log.FailOnError(err, "%s restore failed", restoreName)
		})
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting Restores, Backups and Backup locations, cloud account")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		for _, restore := range restoreNames {
			err = DeleteRestore(restore, BackupOrgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting Restore %s", restore))
		}
		ctx, err = backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		for _, backupName := range backupNames {
			backupUID, err := Inst().Backup.GetBackupUID(ctx, backupName, BackupOrgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Getting backup UID for backup %s", backupName))
			_, err = DeleteBackup(backupName, backupUID, BackupOrgID, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Deleting backup %s", backupName))
		}
		CleanupCloudSettingsAndClusters(backupLocationMap, CredName, CloudCredUID, ctx)
		DeleteBucket(providers[0], encryptionBucketName)
		opts := make(map[string]bool)
		err = DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")
	})

})

// This testcase verifies schedule backups are successful while volume resize is in progress
var _ = Describe("{ResizeVolumeOnScheduleBackup}", Label(TestCaseLabelsMap[ResizeVolumeOnScheduleBackup]...), func() {
	var (
		appList                     = Inst().AppList
		scheduledAppContexts        []*scheduler.Context
		appContextsToBackup         []*scheduler.Context
		preRuleNameList             []string
		postRuleNameList            []string
		appNamespaces               []string
		beforeSize                  int
		credName                    string
		periodicSchedulePolicyName  string
		periodicSchedulePolicyNames []string
		periodicSchedulePolicyUid   string
		periodicSchedulePolicyUids  []string
		scheduleName                string
		scheduleNames               []string
		cloudCredUID                string
		firstScheduleBackupName     string
		appClusterName              string
		nextScheduleBackupNameRef   interface{}
		restoreNames                []string
		nextScheduleBackupName      string
		volumeMounts                []string
		podList                     []v1.Pod
		controlChannel              chan string
		errorGroup                  *errgroup.Group
	)
	labelSelectors := make(map[string]string)
	cloudCredUIDMap := make(map[string]string)
	backupLocationMap := make(map[string]string)
	AppContextsMapping := make(map[string]*scheduler.Context)
	volListBeforeSizeMap := make(map[string]int)
	volListAfterSizeMap := make(map[string]int)

	var backupLocation string
	scheduledAppContexts = make([]*scheduler.Context, 0)
	appNamespaces = make([]string, 0)
	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("ResizeVolumeOnScheduleBackup", "Verify schedule backups are successful while volume resize is in progress", nil, 58050, Sn, Q1FY24)
		log.InfoD("Verifying if the pre/post rules for the required apps are present in the list or not")
		for i := 0; i < len(appList); i++ {
			if Contains(PostRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["post_action_list"]; ok {
					dash.VerifyFatal(ok, true, "Post Rule details mentioned for the apps")
				}
			}
			if Contains(PreRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["pre_action_list"]; ok {
					dash.VerifyFatal(ok, true, "Pre Rule details mentioned for the apps")
				}
			}
		}
		log.InfoD("Deploy applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = AppReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
				AppContextsMapping[namespace] = ctx
			}
		}
	})
	It("Schedule backup while resizing the volume", func() {
		providers := GetBackupProviders()
		Step("Validate applications", func() {
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})
		Step("Creating rules for backup", func() {
			log.InfoD("Creating pre and post rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				preRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], BackupOrgID, "pre")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating pre rule for deployed apps for %v with status %v", appList[i], preRuleStatus))
				dash.VerifyFatal(preRuleStatus, true, "Verifying pre rule for backup")
				preRuleNameList = append(preRuleNameList, ruleName)
				postRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], BackupOrgID, "post")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating post rule for deployed apps for %v with status %v", appList[i], postRuleStatus))
				dash.VerifyFatal(postRuleStatus, true, "Verifying post rule for backup")
				postRuleNameList = append(postRuleNameList, ruleName)
			}
		})
		Step("Creating cloud credentials and backup location", func() {
			log.InfoD("Creating cloud credentials and backup location")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				credName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				cloudCredUID = uuid.New()
				cloudCredUIDMap[cloudCredUID] = credName
				err := CreateCloudCredential(provider, credName, cloudCredUID, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, BackupOrgID, provider))
				log.InfoD("Created Cloud Credentials with name - %s", credName)
				backupLocation = fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
				BackupLocationUID = uuid.New()
				backupLocationMap[BackupLocationUID] = backupLocation
				log.InfoD("Backup location with name - %s", backupLocation)
				err = CreateBackupLocation(provider, backupLocation, BackupLocationUID, credName, cloudCredUID, getGlobalBucketName(provider), BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, "Creating backup location")
				log.InfoD("Created Backup Location with name - %s", backupLocation)
			}
		})
		Step("Configure source and destination clusters with px-central-admin ctx", func() {
			log.InfoD("Configuring source and destination clusters with px-central-admin ctx")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, DestinationClusterName))
			appClusterName = DestinationClusterName
			clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, appClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", appClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", appClusterName))
			clusterUid, err := Inst().Backup.GetClusterUID(ctx, BackupOrgID, appClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", appClusterName))
			log.InfoD("Uid of [%s] cluster is %s", appClusterName, clusterUid)
		})
		for _, namespace := range appNamespaces {
			for backupLocationUID, backupLocationName := range backupLocationMap {
				Step("Getting size of volume before resizing", func() {
					log.InfoD("Getting size of volume before resizing")
					label, err := GetAppLabelFromSpec(AppContextsMapping[namespace])
					dash.VerifyFatal(err, nil, fmt.Sprintf("unable to get the label from the application spec %s", AppContextsMapping[namespace].App.Key))
					labelSelectors["app"] = label["app"]
					pods, err := core.Instance().GetPods(namespace, labelSelectors)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the pod list"))
					srcClusterConfigPath, err := GetSourceClusterConfigPath()
					dash.VerifyFatal(err, nil, fmt.Sprintf("Getting kubeconfig path for source cluster %v", srcClusterConfigPath))
					podList = pods.Items
					for _, pod := range pods.Items {
						containerPaths := schedops.GetContainerPVCMountMap(pod)
						for containerName, paths := range containerPaths {
							log.Infof("container [%s] has paths [%v]", containerName, paths)
							for _, path := range paths {
								beforeSize, err = GetSizeOfMountPoint(pod.GetName(), namespace, srcClusterConfigPath, path, containerName)
								dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the size of volume before resizing %v from pod %v", beforeSize, pod.GetName()))
								volListBeforeSizeMap[path] = beforeSize
							}
						}
					}
				})
				Step("Create schedule policy", func() {
					log.InfoD("Create schedule policy")
					ctx, err := backup.GetAdminCtxFromSecret()
					dash.VerifyFatal(err, nil, "Fetching px-central-admin ctx")
					periodicSchedulePolicyName = fmt.Sprintf("%s-%v", "periodic", time.Now().Unix())
					periodicSchedulePolicyNames = append(periodicSchedulePolicyNames, periodicSchedulePolicyName)
					periodicSchedulePolicyUid = uuid.New()
					periodicSchedulePolicyUids = append(periodicSchedulePolicyUids, periodicSchedulePolicyUid)
					periodicSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, 15, 5)
					err = Inst().Backup.BackupSchedulePolicy(periodicSchedulePolicyName, periodicSchedulePolicyUid, BackupOrgID, periodicSchedulePolicyInfo)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval 15 minutes named [%s]", periodicSchedulePolicyName))
					periodicSchedulePolicyUid, err = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicSchedulePolicyName)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching uid of periodic schedule policy named [%s]", periodicSchedulePolicyName))
				})
				Step("Resize the volume before backup schedule", func() {
					log.InfoD("Resize the volume before backup schedule")
					for _, ctx := range scheduledAppContexts {
						var appVolumes []*volume.Volume
						log.InfoD(fmt.Sprintf("get volumes for %s app", ctx.App.Key))
						appVolumes, err := Inst().S.GetVolumes(ctx)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Fetch volumes for app %s", ctx.App.Key))
						dash.VerifyFatal(len(appVolumes) > 0, true, "Verifying if app volumes exist")
						var requestedVols []*volume.Volume
						log.InfoD(fmt.Sprintf("Increase volume size %s on app %s",
							Inst().V.String(), ctx.App.Key))
						requestedVols, err = Inst().S.ResizeVolume(ctx, Inst().ConfigMap)
						dash.VerifyFatal(err, nil, "Verifying volume resize")
						log.InfoD(fmt.Sprintf("validate successful volume size increase on app %s",
							ctx.App.Key))
						for _, volume := range requestedVols {
							// Need to pass token before validating volume
							params := make(map[string]string)
							if Inst().ConfigMap != "" {
								params["auth-token"], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
								dash.VerifyFatal(err, nil, "Fetching token from configMap")
							}
							err := Inst().V.ValidateUpdateVolume(volume, params)
							dash.VerifyFatal(err, nil, fmt.Sprintf("Validate volume %v update status", volume))
						}
					}
				})
				Step("Create schedule backup immediately after initiating volume resize", func() {
					log.InfoD("Create schedule backup after initiating volume resize")
					ctx, err := backup.GetAdminCtxFromSecret()
					dash.VerifyFatal(err, nil, "Fetching px-central-admin ctx")
					scheduleName = fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, time.Now().Unix())
					scheduleNames = append(scheduleNames, scheduleName)
					preRuleUid, _ := Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleNameList[0])
					postRuleUid, _ := Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleNameList[0])
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					firstScheduleBackupName, err = CreateScheduleBackupWithValidation(ctx, scheduleName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, make(map[string]string), BackupOrgID, preRuleNameList[0], preRuleUid, postRuleNameList[0], postRuleUid, periodicSchedulePolicyName, periodicSchedulePolicyUid)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of schedule backup with schedule name [%s]", scheduleName))
				})
				Step("Checking size of volume after resize", func() {
					log.InfoD("Checking size of volume after resize")
					srcClusterConfigPath, err := GetSourceClusterConfigPath()
					dash.VerifyFatal(err, nil, fmt.Sprintf("Getting kubeconfig path for source cluster %v", srcClusterConfigPath))
					for _, pod := range podList {
						containerPaths := schedops.GetContainerPVCMountMap(pod)
						for containerName, paths := range containerPaths {
							log.Infof("container [%s] has paths [%v]", containerName, paths)
							for _, path := range paths {
								afterSize, err := GetSizeOfMountPoint(pod.GetName(), namespace, srcClusterConfigPath, path, containerName)
								dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the size of volume after resizing %v from pod %v", afterSize, pod.GetName()))
								volListAfterSizeMap[path] = afterSize
							}
						}
					}
					for _, volumeMount := range volumeMounts {
						dash.VerifyFatal(volListAfterSizeMap[volumeMount] > volListBeforeSizeMap[volumeMount], true, fmt.Sprintf("Verifying volume size has increased for pod %s", volumeMount))
					}
				})
				Step("Verifying backup success after initializing volume resize", func() {
					log.InfoD("Verifying backup success after initializing volume resize")
					ctx, err := backup.GetAdminCtxFromSecret()
					dash.VerifyFatal(err, nil, "Fetching px-central-admin ctx")
					appContextsToBackup = FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					err = BackupSuccessCheckWithValidation(ctx, firstScheduleBackupName, appContextsToBackup, BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success and Validation of recent backup [%s]", firstScheduleBackupName))
					allScheduleBackupNames, err := Inst().Backup.GetAllScheduleBackupNames(ctx, scheduleName, BackupOrgID)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching all schedule backups %v", allScheduleBackupNames))
					currentScheduleBackupCount := len(allScheduleBackupNames)
					log.InfoD("Current number of schedule backups is [%v]", currentScheduleBackupCount)
					nextScheduleBackupOrdinal := currentScheduleBackupCount + 1
					log.InfoD("Ordinal of the next schedule backup is [%v]", nextScheduleBackupOrdinal)
					log.InfoD("Waiting for 15 minutes for the next schedule backup to be triggered")
					time.Sleep(15 * time.Minute)
					checkNextScheduleBackupCreation := func() (interface{}, bool, error) {
						ordinalScheduleBackupName, err := GetOrdinalScheduleBackupName(ctx, scheduleName, nextScheduleBackupOrdinal, BackupOrgID)
						if err != nil {
							return "", true, err
						}
						return ordinalScheduleBackupName, false, nil
					}
					nextScheduleBackupNameRef, err = DoRetryWithTimeoutWithGinkgoRecover(checkNextScheduleBackupCreation, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching next schedule backup name of ordinal [%v] of schedule named [%s]", nextScheduleBackupOrdinal, scheduleName))
					nextScheduleBackupName = nextScheduleBackupNameRef.(string)
					log.InfoD("Next schedule backup name [%s]", nextScheduleBackupName)
					err = BackupSuccessCheckWithValidation(ctx, nextScheduleBackupName, appContextsToBackup, BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success and Validation of next schedule backup named [%s] of schedule named [%s]", nextScheduleBackupName, scheduleName))
				})
				Step("Restoring application from first schedule backup", func() {
					log.InfoD(fmt.Sprintf("Restoring the backed up application with backup name : %v", firstScheduleBackupName))
					ctx, err := backup.GetAdminCtxFromSecret()
					log.FailOnError(err, "Fetching px-central-admin ctx")
					restoreName := fmt.Sprintf("%s-%s-%v", "test-restore", namespace, time.Now().Unix())
					restoreNames = append(restoreNames, restoreName)
					err = CreateRestoreWithValidation(ctx, restoreName, firstScheduleBackupName, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsToBackup)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Restore %s from backup %s", restoreName, firstScheduleBackupName))
				})
				Step("Restoring application from recent backup", func() {
					log.InfoD(fmt.Sprintf("Restoring the backed up application with backup name : %v", nextScheduleBackupName))
					ctx, err := backup.GetAdminCtxFromSecret()
					log.FailOnError(err, "Fetching px-central-admin ctx")
					restoreName := fmt.Sprintf("%s-%s-%v", "test-restore-recent-backup", namespace, time.Now().Unix())
					restoreNames = append(restoreNames, restoreName)
					err = CreateRestoreWithValidation(ctx, restoreName, nextScheduleBackupName, make(map[string]string), make(map[string]string), DestinationClusterName, BackupOrgID, appContextsToBackup)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Restore %s from backup %s", restoreName, nextScheduleBackupName))
				})
			}
		}
	})
	JustAfterEach(func() {
		var wg sync.WaitGroup
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		dash.VerifySafely(err, nil, "Fetching px-central-admin ctx")
		for i := 0; i < len(scheduleNames); i++ {
			err = DeleteSchedule(scheduleNames[i], SourceClusterName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of schedule named [%s] and schedule policies [%v]", scheduleNames[i], periodicSchedulePolicyNames[i]))
		}
		log.InfoD("Deleting created restores")
		for _, restoreName := range restoreNames {
			wg.Add(1)
			go func(restoreName string) {
				defer GinkgoRecover()
				defer wg.Done()
				err = DeleteRestore(restoreName, BackupOrgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting Restore %s", restoreName))
			}(restoreName)
		}
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		log.InfoD("Deleting deployed namespaces - %v", appNamespaces)
		err = DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})
