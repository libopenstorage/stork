package tests

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/portworx/torpedo/drivers/volume/portworx/schedops"
	"golang.org/x/sync/errgroup"

	. "github.com/onsi/ginkgo/v2"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/backup/portworx"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
	v1 "k8s.io/api/core/v1"
)

// This testcase verifies alternating backups between locked and unlocked bucket
var _ = Describe("{BackupAlternatingBetweenLockedAndUnlockedBuckets}", Label(TestCaseLabelsMap[BackupAlternatingBetweenLockedAndUnlockedBuckets]...), func() {
	var (
		appList        = Inst().AppList
		credName       string
		restoreNames   []string
		controlChannel chan string
		errorGroup     *errgroup.Group
	)
	var preRuleNameList []string
	var postRuleNameList []string
	var scheduledAppContexts []*scheduler.Context
	labelSelectors := make(map[string]string)
	CloudCredUIDMap := make(map[string]string)
	BackupLocationMap := make(map[string]string)
	var backupList []string
	var backupLocation string
	var bkpNamespaces []string
	var clusterUid string
	var clusterStatus api.ClusterInfo_StatusInfo_Status
	bkpNamespaces = make([]string, 0)
	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("BackupAlternatingBetweenLockedAndUnlockedBuckets", "Deploying backup", nil, 60018, Kshithijiyer, Q4FY23)
		log.InfoD("Verifying if the pre/post rules for the required apps are present in the list or not")
		for i := 0; i < len(appList); i++ {
			if Contains(PostRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["post_action_list"]; ok {
					dash.VerifyFatal(ok, true, fmt.Sprintf("Post Rule details mentioned for the apps %s", appList[i]))
				}
			}
			if Contains(PreRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["pre_action_list"]; ok {
					dash.VerifyFatal(ok, true, fmt.Sprintf("Pre Rule details mentioned for the apps %s", appList[i]))
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
	It("Backup alternating between locked and unlocked buckets", func() {
		providers := GetBackupProviders()
		Step("Validate applications", func() {
			log.InfoD("Validating apps")
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})

		Step("Creating rules for backup", func() {
			log.InfoD("Creating pre rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				preRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], BackupOrgID, "pre")
				log.FailOnError(err, "Creating pre rule for deployed apps failed")
				dash.VerifyFatal(preRuleStatus, true, fmt.Sprintf("Verifying pre rule %s for backup", ruleName))
				preRuleNameList = append(preRuleNameList, ruleName)
			}
			log.InfoD("Creating post rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				postRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], BackupOrgID, "post")
				log.FailOnError(err, "Creating post rule for deployed apps failed")
				dash.VerifyFatal(postRuleStatus, true, fmt.Sprintf("Verifying post rule %s for backup", ruleName))
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
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", CredName, BackupOrgID, provider))
			}
		})

		Step("Creating a locked bucket and backup location", func() {
			log.InfoD("Creating locked buckets and backup location")
			modes := [2]string{"GOVERNANCE", "COMPLIANCE"}
			for _, provider := range providers {
				for _, mode := range modes {
					bucketName := fmt.Sprintf("%s-%s-%v", getGlobalLockedBucketName(provider), strings.ToLower(mode), time.Now().Unix())
					backupLocation = fmt.Sprintf("%s-%s-lock-%v", getGlobalLockedBucketName(provider), strings.ToLower(mode), time.Now().Unix())
					err := CreateS3Bucket(bucketName, true, 3, mode)
					log.FailOnError(err, "Unable to create locked s3 bucket %s", bucketName)
					BackupLocationUID = uuid.New()
					err = CreateBackupLocation(provider, backupLocation, BackupLocationUID, credName, CloudCredUID, bucketName, BackupOrgID, "", true)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocation))
					BackupLocationMap[BackupLocationUID] = backupLocation
				}
			}
			log.InfoD("Successfully created locked buckets and backup location")
		})

		Step("Creating backup location for unlocked bucket", func() {
			log.InfoD("Creating backup location for unlocked bucket")
			for _, provider := range providers {
				bucketName := fmt.Sprintf("%s-%v", getGlobalBucketName(provider), time.Now().Unix())
				backupLocation = fmt.Sprintf("%s-%s-unlockedbucket-%v", provider, getGlobalBucketName(provider), time.Now().Unix())
				BackupLocationUID = uuid.New()
				err := CreateBackupLocation(provider, backupLocation, BackupLocationUID, credName, CloudCredUID, bucketName, BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocation))
				BackupLocationMap[BackupLocationUID] = backupLocation
			}
		})

		Step("Register cluster for backup", func() {
			log.InfoD("Register cluster for backup")
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

		Step("Taking backup of application to locked and unlocked bucket", func() {
			log.InfoD("Taking backup of application to locked and unlocked bucket")
			for backupLocationUID, backupLocationName := range BackupLocationMap {
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				preRuleUid, _ := Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleNameList[0])
				postRuleUid, _ := Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleNameList[0])
				backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, RandomString(5), backupLocationName)
				backupList = append(backupList, backupName)
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, BackupOrgID, clusterUid, preRuleNameList[0], preRuleUid, postRuleNameList[0], postRuleUid)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
			}
		})
		Step("Restoring the backups application", func() {
			log.InfoD("Restoring the backups application")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, backupName := range backupList {
				restoreName := fmt.Sprintf("%s-restore-%v", backupName, time.Now().Unix())
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
				err = CreateRestoreWithValidation(ctx, restoreName, backupName, make(map[string]string), make(map[string]string), SourceClusterName, BackupOrgID, appContextsToBackup)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore %s", restoreName))
				restoreNames = append(restoreNames, restoreName)
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		err := DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		for _, restoreName := range restoreNames {
			err := DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
		}

		log.InfoD("Deleting backup location and cloud setting")
		for backupLocationUID, backupLocationName := range BackupLocationMap {
			err := DeleteBackupLocation(backupLocationName, backupLocationUID, BackupOrgID, false)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup location %s", backupLocationName))
		}
		// Need sleep as it takes some time for
		time.Sleep(time.Minute * 1)
		for CloudCredUID, CredName := range CloudCredUIDMap {
			err := DeleteCloudCredential(CredName, BackupOrgID, CloudCredUID)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cloud cred %s", CredName))
		}
		ctx, err = backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")

		log.Infof("Deleting registered clusters for admin context")
		err = DeleteCluster(SourceClusterName, BackupOrgID, ctx, true)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", SourceClusterName))
		err = DeleteCluster(DestinationClusterName, BackupOrgID, ctx, true)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", DestinationClusterName))
	})
})

// This testcase verifies resize after same original volume is restored from a backup stored in a locked bucket
var _ = Describe("{LockedBucketResizeOnRestoredVolume}", Label(TestCaseLabelsMap[LockedBucketResizeOnRestoredVolume]...), func() {
	var (
		appList              = Inst().AppList
		backupName           string
		scheduledAppContexts []*scheduler.Context
		preRuleNameList      []string
		postRuleNameList     []string
		bkpNamespaces        []string
		clusterUid           string
		clusterStatus        api.ClusterInfo_StatusInfo_Status
		backupList           []string
		beforeSize           int
		credName             string
		volumeMounts         []string
		podList              []v1.Pod
		restoreNames         []string
		controlChannel       chan string
		errorGroup           *errgroup.Group
	)
	labelSelectors := make(map[string]string)
	CloudCredUIDMap := make(map[string]string)
	BackupLocationMap := make(map[string]string)
	AppContextsMapping := make(map[string]*scheduler.Context)
	volListBeforeSizeMap := make(map[string]int)
	volListAfterSizeMap := make(map[string]int)

	var backupLocation string
	scheduledAppContexts = make([]*scheduler.Context, 0)
	bkpNamespaces = make([]string, 0)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("LockedBucketResizeOnRestoredVolume", "Resize after the volume is restored from a backup from locked bucket", nil, 59904, Kshithijiyer, Q4FY23)
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
				AppContextsMapping[namespace] = ctx
			}
		}
	})
	It("Resize after the volume is restored from a backup", func() {
		providers := GetBackupProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
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

		Step("Creating a locked bucket and backup location", func() {
			log.InfoD("Creating locked buckets and backup location")
			modes := [2]string{"GOVERNANCE", "COMPLIANCE"}
			for _, provider := range providers {
				for _, mode := range modes {
					bucketName := fmt.Sprintf("%s-%v", getGlobalLockedBucketName(provider), time.Now().Unix())
					backupLocation = fmt.Sprintf("%s-%s-lock-%v", getGlobalLockedBucketName(provider), strings.ToLower(mode), time.Now().Unix())
					err := CreateS3Bucket(bucketName, true, 3, mode)
					log.FailOnError(err, "Unable to create locked s3 bucket %s", bucketName)
					BackupLocationUID = uuid.New()
					err = CreateBackupLocation(provider, backupLocation, BackupLocationUID, credName, CloudCredUID, bucketName, BackupOrgID, "", true)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocation))
					BackupLocationMap[BackupLocationUID] = backupLocation
				}
			}
			log.InfoD("Successfully created locked buckets and backup location")
		})

		Step("Register cluster for backup", func() {
			log.InfoD("Register cluster for backup")
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

		for _, namespace := range bkpNamespaces {
			Step("Taking backup of application to locked bucket", func() {
				log.InfoD("Taking backup of application to locked bucket")
				for backupLocationUID, backupLocationName := range BackupLocationMap {
					ctx, err := backup.GetAdminCtxFromSecret()
					log.FailOnError(err, "Fetching px-central-admin ctx")
					preRuleUid, _ := Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleNameList[0])
					postRuleUid, _ := Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleNameList[0])
					backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, namespace, backupLocationName)
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, BackupOrgID, clusterUid, preRuleNameList[0], preRuleUid, postRuleNameList[0], postRuleUid)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
					backupList = append(backupList, backupName)
				}
			})
			Step("Restoring the backups application", func() {
				log.InfoD("Restoring the backups application")
				for _, backupName = range backupList {
					ctx, err := backup.GetAdminCtxFromSecret()
					log.FailOnError(err, "Fetching px-central-admin ctx")
					restoreName := fmt.Sprintf("%s-restore-%v", backupName, time.Now().Unix())
					err = CreateRestore(restoreName, backupName, nil, SourceClusterName, BackupOrgID, ctx, make(map[string]string))
					log.FailOnError(err, "%s restore failed", fmt.Sprintf("%s-restore", backupName))
					restoreNames = append(restoreNames, restoreName)
				}
			})
			Step("Getting size before resize", func() {
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
			Step("Getting size after resize", func() {
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
		}
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		err := DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		for _, restoreName := range restoreNames {
			err := DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
		}

		log.InfoD("Deleting backup location, cloud creds and clusters")
		ctx, err = backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(BackupLocationMap, credName, CloudCredUID, ctx)
	})
})

// This testcase verifies schedule backups are successful while volume resize is in progress for locked bucket
var _ = Describe("{LockedBucketResizeVolumeOnScheduleBackup}", Label(TestCaseLabelsMap[LockedBucketResizeVolumeOnScheduleBackup]...), func() {
	var (
		beforeSize                 int
		credName                   string
		periodicSchedulePolicyName string
		periodicSchedulePolicyUid  string
		scheduleName               string
		cloudCredUID               string
		backupLocation             string
		appList                    = Inst().AppList
		scheduledAppContexts       []*scheduler.Context
		scheduleNames              []string
		preRuleNameList            []string
		postRuleNameList           []string
		appNamespaces              []string
		clusterStatus              api.ClusterInfo_StatusInfo_Status
		volumeMounts               []string
		podList                    []v1.Pod
		controlChannel             chan string
		errorGroup                 *errgroup.Group
	)
	labelSelectors := make(map[string]string)
	cloudCredUIDMap := make(map[string]string)
	backupLocationMap := make(map[string]string)
	scheduledAppContexts = make([]*scheduler.Context, 0)
	appNamespaces = make([]string, 0)
	AppContextsMapping := make(map[string]*scheduler.Context)
	volListBeforeSizeMap := make(map[string]int)
	volListAfterSizeMap := make(map[string]int)
	modes := [2]string{"GOVERNANCE", "COMPLIANCE"}
	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("LockedBucketResizeVolumeOnScheduleBackup", "Verify schedule backups are successful while volume resize is in progress for locked bucket", nil, 59899, Apimpalgaonkar, Q1FY24)
		log.InfoD("Verifying if the pre/post rules for the required apps are present in the list or not")
		for i := 0; i < len(appList); i++ {
			if Contains(PostRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["post_action_list"]; ok {
					dash.VerifyFatal(ok, true, fmt.Sprintf("Post Rule details mentioned for the app %v", appList[i]))
				}
			}
			if Contains(PreRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["pre_action_list"]; ok {
					dash.VerifyFatal(ok, true, fmt.Sprintf("Pre Rule details mentioned for the app %v", appList[i]))
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
			log.InfoD("Validate applications")
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})
		Step("Creating pre and post rule for deployed apps", func() {
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
		Step("Creating cloud credentials", func() {
			log.InfoD("Creating cloud credentials")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to px-central-admin ctx")
			for _, provider := range providers {
				credName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				cloudCredUID = uuid.New()
				cloudCredUIDMap[cloudCredUID] = credName
				err = CreateCloudCredential(provider, credName, cloudCredUID, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating cloud credentials %v", credName))
			}
		})
		Step("Creating a locked bucket and backup location", func() {
			log.InfoD("Creating a locked bucket and backup location")
			for _, provider := range providers {
				for _, mode := range modes {
					bucketName := fmt.Sprintf("%s-%v", getGlobalLockedBucketName(provider), time.Now().Unix())
					backupLocation = fmt.Sprintf("%s-%s-lock-%v", getGlobalLockedBucketName(provider), strings.ToLower(mode), time.Now().Unix())
					err := CreateS3Bucket(bucketName, true, 3, mode)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating locked s3 bucket %s", bucketName))
					BackupLocationUID = uuid.New()
					backupLocationMap[BackupLocationUID] = backupLocation
					err = CreateBackupLocation(provider, backupLocation, BackupLocationUID, credName, cloudCredUID, bucketName, BackupOrgID, "", true)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocation))
				}
			}
		})
		Step("Configure source and destination clusters with px-central-admin ctx", func() {
			log.InfoD("Configure source and destination clusters with px-central-admin ctx")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
		})
		Step("Create schedule policy", func() {
			log.InfoD("Create schedule policy")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Unable to px-central-admin ctx")
			periodicSchedulePolicyName = fmt.Sprintf("%s-%v", "periodic", time.Now().Unix())
			periodicSchedulePolicyUid = uuid.New()
			periodicSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, 15, 5)
			err = Inst().Backup.BackupSchedulePolicy(periodicSchedulePolicyName, periodicSchedulePolicyUid, BackupOrgID, periodicSchedulePolicyInfo)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval 15 minutes named [%s]", periodicSchedulePolicyName))
			periodicSchedulePolicyUid, err = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicSchedulePolicyName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching uid of periodic schedule policy named [%s]", periodicSchedulePolicyName))
		})
		for i, namespace := range appNamespaces {
			Step("Getting size of volume before resizing", func() {
				log.InfoD("Getting size of volume before resizing")
				label, err := GetAppLabelFromSpec(AppContextsMapping[namespace])
				dash.VerifyFatal(err, nil, fmt.Sprintf("unable to get the label from the application spec %s", AppContextsMapping[namespace].App.Key))
				log.Infof("Pod label from the spec %s", label)
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
			Step("Resize the volume before backup schedule", func() {
				log.InfoD("Resize the volume before backup schedule")
				for _, ctx := range scheduledAppContexts {
					var appVolumes []*volume.Volume
					log.InfoD(fmt.Sprintf("get volumes for %s app", ctx.App.Key))
					appVolumes, err := Inst().S.GetVolumes(ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetch volumes for app %s", ctx.App.Key))
					dash.VerifyFatal(len(appVolumes) > 0, true, "Verifying if app volumes exist")
					var requestedVols []*volume.Volume
					log.InfoD(fmt.Sprintf("Increase volume size %s on app %s's volumes",
						Inst().V.String(), ctx.App.Key))
					requestedVols, err = Inst().S.ResizeVolume(ctx, Inst().ConfigMap)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying volume resize [%v]", requestedVols))
					log.InfoD(fmt.Sprintf("validate successful volume size increase on app %s's volumes",
						ctx.App.Key))
					for _, volume := range requestedVols {
						// Need to pass token before validating volume
						params := make(map[string]string)
						if Inst().ConfigMap != "" {
							params["auth-token"], err = Inst().S.GetTokenFromConfigMap(Inst().ConfigMap)
							dash.VerifyFatal(err, nil, "Fetching token from configMap")
						}
						err := Inst().V.ValidateUpdateVolume(volume, params)
						dash.VerifyFatal(err, nil, "Validate volume update status")
					}
				}
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
			Step("Validate applications before taking backup", func() {
				log.InfoD("Validate applications")
				ctx, _ := backup.GetAdminCtxFromSecret()
				controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
			})
			Step("Create schedule backup after initializing volume resize", func() {
				log.InfoD("Create schedule backup after initializing volume resize")
				for backupLocationUID, backupLocationName := range backupLocationMap {
					log.InfoD("Create schedule backup after initializing volume resize")
					ctx, err := backup.GetAdminCtxFromSecret()
					log.FailOnError(err, "Unable to px-central-admin ctx")
					preRuleUid := ""
					if preRuleNameList[i] != "" {
						preRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleNameList[i])
						log.FailOnError(err, "Unable to fetch pre rule Uid")
					}
					postRuleUid := ""
					if postRuleNameList[i] != "" {
						postRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleNameList[i])
						log.FailOnError(err, "Unable to fetch post rule Uid")
					}
					scheduleName = fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, time.Now().Unix())
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					_, err = CreateScheduleBackupWithValidation(ctx, scheduleName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, make(map[string]string), BackupOrgID, preRuleNameList[i], preRuleUid, postRuleNameList[i], postRuleUid, periodicSchedulePolicyName, periodicSchedulePolicyUid)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of schedule backup with schedule name [%s]", scheduleName))
					scheduleNames = append(scheduleNames, scheduleName)
				}
			})
			Step("Verifying backup success after initializing volume resize", func() {
				log.InfoD("Waiting for 15 minutes for the next schedule backup to be triggered")
				time.Sleep(15 * time.Minute)
				for _, scheduleName := range scheduleNames {
					log.InfoD("Verifying backup success after initializing volume resize")
					ctx, err := backup.GetAdminCtxFromSecret()
					log.FailOnError(err, "Unable to px-central-admin ctx")
					backupName, err := GetOrdinalScheduleBackupName(ctx, scheduleName, 2, BackupOrgID)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching recent backup %v", backupName))
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					err = BackupSuccessCheckWithValidation(ctx, backupName, appContextsToBackup, BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the success of recent backup named [%s]", backupName))
				}
			})
		}
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Unable to px-central-admin ctx")
		for _, scheduleName := range scheduleNames {
			err = DeleteSchedule(scheduleName, SourceClusterName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		}
		err = Inst().Backup.DeleteBackupSchedulePolicy(BackupOrgID, []string{periodicSchedulePolicyName})
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", []string{periodicSchedulePolicyName}))
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		err = DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})

// DeleteLockedBucketUserObjectsFromAdmin delete backups, backup schedules, restore and cluster objects created with locked bucket from the admin
var _ = Describe("{DeleteLockedBucketUserObjectsFromAdmin}", Label(TestCaseLabelsMap[DeleteLockedBucketUserObjectsFromAdmin]...), func() {
	var (
		scheduledAppContexts                           = make([]*scheduler.Context, 0)
		appNamespaces                                  = make([]string, 0)
		infraAdminUsers                                = make([]string, 0)
		providers                                      = GetBackupProviders()
		userCloudCredentialMap                         = make(map[string]map[string]string)
		userBackupLocationMap                          = make(map[string]map[string]string)
		userClusterMap                                 = make(map[string]map[string]string)
		userSchedulePolicyInterval                     = int64(15)
		userSchedulePolicyMap                          = make(map[string]map[string]string)
		userBackupMap                                  = make(map[string]map[string]string)
		userScheduleNameMap                            = make(map[string]string)
		userRestoreMap                                 = make(map[string]map[string]string)
		numberOfUsers                                  = 1
		numberOfBackups                                = 1
		infraAdminRole             backup.PxBackupRole = backup.InfrastructureOwner
		restoreNames               []string
		controlChannel             chan string
		errorGroup                 *errgroup.Group
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("DeleteLockedBucketUserObjectsFromAdmin", "Delete backups, backup schedules, restore and cluster objects created with locked bucket from the admin", nil, 87566, Kshithijiyer, Q3FY24)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = AppReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
		log.InfoD("Scheduled application namespaces: %v", appNamespaces)
	})

	It("Deletes backups, backup schedules, restore and cluster objects created by multiple user with same name from the admin", func() {
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		Step("Validate applications", func() {
			log.InfoD("Validating applications")
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})
		Step(fmt.Sprintf("Create %d users with %s role", numberOfUsers, infraAdminRole), func() {
			log.InfoD(fmt.Sprintf("Creating %d users with %s role", numberOfUsers, infraAdminRole))
			for _, user := range CreateUsers(numberOfUsers) {
				err := backup.AddRoleToUser(user, infraAdminRole, fmt.Sprintf("Adding %v role to %s", infraAdminRole, user))
				log.FailOnError(err, "failed to add role %s to the user %s", infraAdminRole, user)
				infraAdminUsers = append(infraAdminUsers, user)
			}
		})
		createObjectsFromUser := func(user string) {
			Step(fmt.Sprintf("Create cloud credential and locked bucket backup location from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Creating cloud credential and locked bucket backup location from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				modes := [2]string{"GOVERNANCE", "COMPLIANCE"}
				for _, provider := range providers {
					userCloudCredentialName := fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
					userCloudCredentialUID := uuid.New()
					err = CreateCloudCredential(provider, userCloudCredentialName, userCloudCredentialUID, BackupOrgID, nonAdminCtx)
					log.FailOnError(err, "failed to create cloud credential %s using provider %s for the user", userCloudCredentialName, provider)
					userCloudCredentialMap[user] = map[string]string{userCloudCredentialUID: userCloudCredentialName}
					for _, mode := range modes {
						userBackupLocationName := fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
						userBackupLocationUID := uuid.New()
						lockedBucketName := fmt.Sprintf("%s-%s-%s-locked", provider, getGlobalLockedBucketName(provider), strings.ToLower(mode))
						err := CreateS3Bucket(lockedBucketName, true, 3, mode)
						log.FailOnError(err, "failed to create locked s3 bucket %s", lockedBucketName)
						err = CreateBackupLocationWithContext(provider, userBackupLocationName, userBackupLocationUID, userCloudCredentialName, userCloudCredentialUID, lockedBucketName, BackupOrgID, "", nonAdminCtx, true)
						log.FailOnError(err, "failed to create locked bucket backup location %s using provider %s for the user", userBackupLocationName, provider)
						userBackupLocationMap[user] = map[string]string{userBackupLocationUID: userBackupLocationName}
					}
				}
			})
			Step(fmt.Sprintf("Create source and destination cluster from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Creating source and destination cluster from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				err = CreateApplicationClusters(BackupOrgID, "", "", nonAdminCtx)
				log.FailOnError(err, "failed create source and destination cluster from the user %s", user)
				clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, nonAdminCtx)
				log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
				dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
				userClusterMap[user] = make(map[string]string)
				for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
					userClusterUID, err := Inst().Backup.GetClusterUID(nonAdminCtx, BackupOrgID, clusterName)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", clusterName))
					userClusterMap[user][clusterName] = userClusterUID
				}
			})
			Step(fmt.Sprintf("Take backup of applications from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Taking backup of applications from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				var wg sync.WaitGroup
				var mu sync.RWMutex
				userBackupMap[user] = make(map[string]string)
				createBackup := func(backupName string, namespace string) {
					defer GinkgoRecover()
					defer wg.Done()
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					for backupLocationUID, backupLocationName := range userBackupLocationMap[user] {
						err := CreateBackupWithValidation(nonAdminCtx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, make(map[string]string), BackupOrgID, userClusterMap[user][SourceClusterName], "", "", "", "")
						dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of backup [%s] of namespace (scheduled Context) [%s]", backupName, namespace))
						break
					}
					mu.Lock()
					defer mu.Unlock()
					userBackupMap[user][backupName] = namespace
				}
				for _, namespace := range appNamespaces {
					for i := 0; i < numberOfBackups; i++ {
						backupName := fmt.Sprintf("%s-%s-%d-%v", BackupNamePrefix, namespace, i, time.Now().Unix())
						wg.Add(1)
						go createBackup(backupName, namespace)
					}
				}
				wg.Wait()
				log.Infof("The list of user backups taken are: %v", userBackupMap)
			})
			Step(fmt.Sprintf("Create schedule policy from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Creating schedule policy from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				userSchedulePolicyName := fmt.Sprintf("%s-%v", "periodic", time.Now().Unix())
				userSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, userSchedulePolicyInterval, 5)
				userSchedulePolicyCreateRequest := &api.SchedulePolicyCreateRequest{
					CreateMetadata: &api.CreateMetadata{
						Name:  userSchedulePolicyName,
						OrgId: BackupOrgID,
					},
					SchedulePolicy: userSchedulePolicyInfo,
				}
				userSchedulePolicyCreateRequest.SchedulePolicy.ForObjectLock = true
				_, err = Inst().Backup.CreateSchedulePolicy(nonAdminCtx, userSchedulePolicyCreateRequest)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation schedule policy %s", userSchedulePolicyName))
				userSchedulePolicyUID, err := Inst().Backup.GetSchedulePolicyUid(BackupOrgID, nonAdminCtx, userSchedulePolicyName)
				log.FailOnError(err, "failed to fetch schedule policy uid %s of user %s", userSchedulePolicyName, user)
				userSchedulePolicyMap[user] = map[string]string{userSchedulePolicyUID: userSchedulePolicyName}
			})
			Step(fmt.Sprintf("Take schedule backup of applications from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Taking schedule backup of applications from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				userScheduleName := fmt.Sprintf("backup-schedule-%v", time.Now().Unix())
				for backupLocationUID, backupLocationName := range userBackupLocationMap[user] {
					for schedulePolicyUID, schedulePolicyName := range userSchedulePolicyMap[user] {
						_, err = CreateScheduleBackupWithValidation(nonAdminCtx, userScheduleName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, make(map[string]string), BackupOrgID, "", "", "", "", schedulePolicyName, schedulePolicyUID)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of schedule backup with schedule name [%s]", schedulePolicyName))
						break
					}
					break
				}
				userScheduleNameMap[user] = userScheduleName
			})
		}
		err = TaskHandler(infraAdminUsers, createObjectsFromUser, Parallel)
		log.FailOnError(err, "failed to create objects from user")
		for _, user := range infraAdminUsers {
			Step(fmt.Sprintf("Take restore of backups from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Taking restore of backups from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				var wg sync.WaitGroup
				var mu sync.RWMutex
				userRestoreMap[user] = make(map[string]string, 0)
				createRestore := func(backupName string, restoreName string, namespace string) {
					defer GinkgoRecover()
					defer wg.Done()
					customNamespace := fmt.Sprintf("custom-%s", namespace)
					namespaceMapping := map[string]string{namespace: customNamespace}
					err = CreateRestoreWithValidation(nonAdminCtx, restoreName, backupName, namespaceMapping, make(map[string]string), DestinationClusterName, BackupOrgID, scheduledAppContexts)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s of backup %s", restoreName, backupName))
					restoreUid, err := Inst().Backup.GetRestoreUID(nonAdminCtx, restoreName, BackupOrgID)
					log.FailOnError(err, "failed to fetch restore %s uid of the user %s", restoreName, user)
					mu.Lock()
					defer mu.Unlock()
					userRestoreMap[user][restoreUid] = restoreName
				}
				for backupName, namespace := range userBackupMap[user] {
					wg.Add(1)
					restoreName := fmt.Sprintf("%s-%s-%v", RestoreNamePrefix, backupName, time.Now().Unix())
					go createRestore(backupName, restoreName, namespace)
					restoreNames = append(restoreNames, restoreName)
				}
				wg.Wait()
				log.Infof("The list of user restores taken are: %v", userRestoreMap)
			})
			Step(fmt.Sprintf("Verify backups of the user %s from the admin", user), func() {
				log.InfoD(fmt.Sprintf("Verifying backups of the user %s from the admin", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				userOwnerID, err := portworx.GetSubFromCtx(nonAdminCtx)
				log.FailOnError(err, "failed to fetch user owner id %s", user)
				backupNamesByOwnerID, err := GetAllBackupNamesByOwnerID(userOwnerID, BackupOrgID, ctx)
				log.FailOnError(err, "failed to fetch backup names with owner id %s from the admin", userOwnerID)
				for backupName := range userBackupMap[user] {
					if !IsPresent(backupNamesByOwnerID, backupName) {
						err := fmt.Errorf("backup %s is not listed in backup names %s", backupName, backupNamesByOwnerID)
						log.FailOnError(fmt.Errorf(""), err.Error())
					}
				}
			})
			Step(fmt.Sprintf("Verify backup schedules of the user %s from the admin", user), func() {
				log.InfoD(fmt.Sprintf("Verifying backup schedules of the user %s from the admin", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				userOwnerID, err := portworx.GetSubFromCtx(nonAdminCtx)
				log.FailOnError(err, "failed to fetch user owner id %s", user)
				backupScheduleNamesByOwnerID, err := GetAllBackupScheduleNamesByOwnerID(userOwnerID, BackupOrgID, ctx)
				log.FailOnError(err, "failed to fetch backup schedule names with owner id %s from the admin", userOwnerID)
				for _, backupScheduleName := range userScheduleNameMap {
					if !IsPresent(backupScheduleNamesByOwnerID, backupScheduleName) {
						err := fmt.Errorf("backup schedule %s is not listed in backup schedule names %s", backupScheduleName, backupScheduleNamesByOwnerID)
						log.FailOnError(fmt.Errorf(""), err.Error())
					}
				}
			})
			Step(fmt.Sprintf("Verify restores of the user %s from the admin", user), func() {
				log.InfoD(fmt.Sprintf("Verifying restores of the user %s from the admin", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				userOwnerID, err := portworx.GetSubFromCtx(nonAdminCtx)
				log.FailOnError(err, "failed to fetch user owner id %s", user)
				restoreNamesByOwnerID, err := GetAllRestoreNamesByOwnerID(userOwnerID, BackupOrgID, ctx)
				log.FailOnError(err, "failed to fetch restore names with owner id %s from the admin", userOwnerID)
				for _, restoreName := range userRestoreMap[user] {
					if !IsPresent(restoreNamesByOwnerID, restoreName) {
						err := fmt.Errorf("restore %s is not listed in restore names %s", restoreName, restoreNamesByOwnerID)
						log.FailOnError(fmt.Errorf(""), err.Error())
					}
				}
			})
		}
		cleanupUserObjectsFromAdmin := func(user string) {
			defer GinkgoRecover()
			Step(fmt.Sprintf("Delete user %s schedule backups, backup schedule and schedule policy from the admin", user), func() {
				log.InfoD(fmt.Sprintf("Deleting user %s schedule backups, backup schedule and schedule policy from the admin", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				allScheduleBackupNames, err := Inst().Backup.GetAllScheduleBackupNames(nonAdminCtx, userScheduleNameMap[user], BackupOrgID)
				log.FailOnError(err, "failed to get all schedule backup names with schedule name %s of the user %s", userScheduleNameMap[user], user)
				for i := len(allScheduleBackupNames) - 1; i >= 0; i-- {
					backupName := allScheduleBackupNames[i]
					backupUid, err := Inst().Backup.GetBackupUID(nonAdminCtx, backupName, BackupOrgID)
					log.FailOnError(err, "failed to fetch backup %s uid of the user %s", backupName, user)
					_, err = DeleteBackupWithClusterUID(backupName, backupUid, SourceClusterName, userClusterMap[user][SourceClusterName], BackupOrgID, ctx)
					log.FailOnError(err, "failed to delete schedule backup %s of the user %s", backupName, user)
				}
				scheduleUid, err := Inst().Backup.GetBackupScheduleUID(nonAdminCtx, userScheduleNameMap[user], BackupOrgID)
				log.FailOnError(err, "failed to fetch backup schedule %s uid of the user %s", userScheduleNameMap[user], user)
				err = DeleteScheduleWithUID(userScheduleNameMap[user], scheduleUid, BackupOrgID, ctx)
				log.FailOnError(err, "failed to delete schedule %s of the user %s", userScheduleNameMap[user], user)
			})
			Step(fmt.Sprintf("Delete user %s backups from the admin", user), func() {
				log.InfoD(fmt.Sprintf("Deleting user %s backups from the admin", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				for backupName := range userBackupMap[user] {
					backupUid, err := Inst().Backup.GetBackupUID(nonAdminCtx, backupName, BackupOrgID)
					log.FailOnError(err, "failed to fetch backup %s uid of the user %s", backupName, user)
					_, err = DeleteBackupWithClusterUID(backupName, backupUid, SourceClusterName, userClusterMap[user][SourceClusterName], BackupOrgID, ctx)
					log.FailOnError(err, "failed to delete backup %s of the user %s", backupName, user)
				}
			})
			Step(fmt.Sprintf("Delete user %s restores from the admin", user), func() {
				log.InfoD(fmt.Sprintf("Deleting user %s restores from the admin", user))
				for restoreUid, restoreName := range userRestoreMap[user] {
					err = DeleteRestoreWithUID(restoreName, restoreUid, BackupOrgID, ctx)
					log.FailOnError(err, "failed to delete restore %s of the user %s", restoreName, user)
				}
			})
			Step(fmt.Sprintf("Wait for the backups and backup schedule to be deleted"), func() {
				log.InfoD("Waiting for the backups and backup schedule to be deleted")
				nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				clusterInspectReq := &api.ClusterInspectRequest{
					OrgId:          BackupOrgID,
					Name:           SourceClusterName,
					Uid:            userClusterMap[user][SourceClusterName],
					IncludeSecrets: true,
				}
				clusterResp, err := Inst().Backup.InspectCluster(nonAdminCtx, clusterInspectReq)
				log.FailOnError(err, "failed to inspect cluster %s", SourceClusterName)
				var wg sync.WaitGroup
				namespace := "*"
				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					err = Inst().Backup.WaitForBackupScheduleDeletion(
						nonAdminCtx,
						userScheduleNameMap[user],
						namespace,
						BackupOrgID,
						clusterResp.GetCluster(),
						BackupLocationDeleteTimeout,
						BackupLocationDeleteRetryTime,
					)
					log.FailOnError(err, "failed while waiting for backup schedule %s to be deleted for the user %s", userScheduleNameMap[user], user)
					for schedulePolicyUID, schedulePolicyName := range userSchedulePolicyMap[user] {
						schedulePolicyDeleteRequest := &api.SchedulePolicyDeleteRequest{
							Name:  schedulePolicyName,
							Uid:   schedulePolicyUID,
							OrgId: BackupOrgID,
						}
						_, err = Inst().Backup.DeleteSchedulePolicy(ctx, schedulePolicyDeleteRequest)
						log.FailOnError(err, "failed to delete schedule policy %s of the user %s", schedulePolicyName, user)
						break
					}
				}()
				for backupName := range userBackupMap[user] {
					wg.Add(1)
					go func(backupName string) {
						defer GinkgoRecover()
						defer wg.Done()
						err = Inst().Backup.WaitForBackupDeletion(nonAdminCtx, backupName, BackupOrgID, BackupDeleteTimeout, BackupDeleteRetryTime)
						log.FailOnError(err, "failed while waiting for backup %s to be deleted", backupName)
					}(backupName)
				}
				wg.Wait()
			})
			Step(fmt.Sprintf("Delete user %s source and destination cluster from the admin", user), func() {
				log.InfoD(fmt.Sprintf("Deleting user %s source and destination cluster from the admin", user))
				for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
					err := DeleteClusterWithUID(clusterName, userClusterMap[user][clusterName], BackupOrgID, ctx, false)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of cluster [%s] of the user %s", clusterName, user))
				}
			})
		}
		err = TaskHandler(infraAdminUsers, cleanupUserObjectsFromAdmin, Parallel)
		log.FailOnError(err, "failed to cleanup user objects from admin")
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Destroying the scheduled applications")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		err := DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")

		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		for _, restoreName := range restoreNames {
			err := DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
		}

		cleanupUserObjects := func(user string) {
			nonAdminCtx, err := backup.GetNonAdminCtx(user, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", user)
			for cloudCredentialUID, cloudCredentialName := range userCloudCredentialMap[user] {
				CleanupCloudSettingsAndClusters(userBackupLocationMap[user], cloudCredentialName, cloudCredentialUID, nonAdminCtx)
				break
			}
			err = backup.DeleteUser(user)
			log.FailOnError(err, "failed to delete user %s", user)
		}
		err = TaskHandler(infraAdminUsers, cleanupUserObjects, Parallel)
		log.FailOnError(err, "failed to cleanup user objects from user")
	})
})

// BackupToLockedBucketWithSharedObjects creates backup with shared backup objects
var _ = Describe("{BackupToLockedBucketWithSharedObjects}", Label(TestCaseLabelsMap[BackupToLockedBucketWithSharedObjects]...), func() {
	var (
		preRuleName           string
		postRuleName          string
		credName              string
		restoreNames          []string
		schedulePolicyNames   []string
		scheduledAppContexts  []*scheduler.Context
		backupList            []string
		backupLocation        string
		clusterUid            string
		scheduleList          []string
		clusterStatus         api.ClusterInfo_StatusInfo_Status
		labelSelectors        = make(map[string]string)
		CloudCredUIDMap       = make(map[string]string)
		BackupLocationMap     = make(map[string]string)
		userNames             = make([]string, 0)
		backupAndUserMap      = make(map[string][]string)
		restoreAndUserMap     = make(map[string][]string)
		bkpNamespaces         = make([]string, 0)
		restoreAndUserMapSchd = make(map[string][]string)
		backupSchedAndUserMap = make(map[string][]string)
	)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("BackupToLockedBucketWithSharedObjects", "Backup with Shared objects", nil, 59893, Kshithijiyer, Q4FY24)

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
	It("Backup to locked bucket using shared objects like Rules, Schedule Polices and Backup Locations", func() {
		providers := GetBackupProviders()
		Step("Validate applications", func() {
			log.InfoD("Validating apps")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Create Users with Different types of roles", func() {
			log.InfoD("Create Users with Different types of roles")
			roles := [3]backup.PxBackupRole{backup.ApplicationOwner, backup.InfrastructureOwner, backup.ApplicationUser}
			for _, role := range roles {
				userName := CreateUsers(1)[0]
				err := backup.AddRoleToUser(userName, role, fmt.Sprintf("Adding %v role to %s", role, userName))
				log.FailOnError(err, "Failed to add role for user - %s", userName)
				userNames = append(userNames, userName)
				log.FailOnError(err, "Failed to fetch uid for - %s", userName)
			}
		})

		Step(fmt.Sprintf("Verify creation of pre and post exec rules for applications for Px-Admin user"), func() {
			log.InfoD("Verify creation of pre and post exec rules for applications for Px-Admin user")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			preRuleName, postRuleName, err = CreateRuleForBackupWithMultipleApplications(BackupOrgID, Inst().AppList, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of pre and post exec rules for applications from px-admin"))
			if preRuleName != "" {
				preRuleUid, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleName)
				log.FailOnError(err, "Fetching pre backup rule [%s] uid", preRuleName)
				log.Infof("Pre backup rule [%s] uid: [%s]", preRuleName, preRuleUid)
			}
			if postRuleName != "" {
				postRuleUid, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleName)
				log.FailOnError(err, "Fetching post backup rule [%s] uid", postRuleName)
				log.Infof("Post backup rule [%s] uid: [%s]", postRuleName, postRuleUid)
			}
		})
		Step(fmt.Sprintf("Create a schedule policy with and without autodelete enabled"), func() {
			log.InfoD("Verify Px-Admin User has permission to create a schedule policy")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch px-admin ctx")
			for _, autoDelete := range []bool{true, false} {
				periodicSchedulePolicyName := fmt.Sprintf("%s-%v-auto-%v", "periodic", RandomString(5), autoDelete)
				periodicSchedulePolicyUid := uuid.New()
				periodicSchedulePolicyInterval := int64(15)
				err = CreateBackupScheduleIntervalPolicy(5, periodicSchedulePolicyInterval, 5, periodicSchedulePolicyName, periodicSchedulePolicyUid, BackupOrgID, ctx, true, autoDelete)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval [%v] minutes named [%s]", periodicSchedulePolicyInterval, periodicSchedulePolicyName))
				schedulePolicyNames = append(schedulePolicyNames, periodicSchedulePolicyName)
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
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", CredName, BackupOrgID, provider))
			}
		})

		Step("Creating a locked bucket and backup location", func() {
			log.InfoD("Creating locked buckets and backup location")
			modes := [2]string{"GOVERNANCE", "COMPLIANCE"}
			for _, provider := range providers {
				for _, mode := range modes {

					bucketName := fmt.Sprintf("%s-%s-%v", getGlobalLockedBucketName(provider), strings.ToLower(mode), time.Now().Unix())
					backupLocation = fmt.Sprintf("%s-%s-lock-%v", getGlobalLockedBucketName(provider), strings.ToLower(mode), time.Now().Unix())
					err := CreateS3Bucket(bucketName, true, 3, mode)
					log.FailOnError(err, "Unable to create locked s3 bucket %s", bucketName)

					BackupLocationUID = uuid.New()
					err = CreateBackupLocation(provider, backupLocation, BackupLocationUID, credName, CloudCredUID, bucketName, BackupOrgID, "", true)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocation))

					BackupLocationMap[BackupLocationUID] = backupLocation
				}
			}
			log.InfoD("Successfully created locked buckets and backup location")
		})

		Step("Register cluster for backup with all three users", func() {
			for _, customUser := range userNames {
				log.InfoD("Register cluster for backup with user %s", customUser)
				ctx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", customUser)

				err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
				dash.VerifyFatal(err, nil, "Creating source and destination cluster")

				for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
					clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, clusterName, ctx)
					log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status with user %s", clusterName, customUser))
					dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online with user %s", clusterName, customUser))

					clusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, clusterName)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", clusterName))
				}
			}
		})

		Step("Share backup locations with all three users", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch px-admin ctx")
			for backupLocationUID, backupLocationName := range BackupLocationMap {
				err = AddBackupLocationOwnership(backupLocationName, backupLocationUID, userNames, nil, Read, Invalid, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for backuplocation - %s", backupLocationName))
			}
		})

		Step("Share schedule policy with all three users", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch px-admin ctx")
			for _, periodicSchedulePolicyName := range schedulePolicyNames {
				periodicSchedulePolicyUid, err := Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicSchedulePolicyName)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Getting schedulepolicy object for  - %s", periodicSchedulePolicyName))
				log.InfoD("Update SchedulePolicy - %s ownership for users - [%v]", periodicSchedulePolicyName, userNames)
				err = AddSchedulePolicyOwnership(periodicSchedulePolicyName, periodicSchedulePolicyUid, userNames, nil, Read, Invalid, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for schedulepolicy - %s", periodicSchedulePolicyName))
			}
		})

		Step("Share rule with all three users", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "failed to fetch px-admin ctx")

			if preRuleName != "" {
				preRuleUid, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleName)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Getting prerule object for  - %s", preRuleName))

				log.InfoD("Update pre-rule ownership for users - [%v]", userNames)
				err = AddRuleOwnership(preRuleName, preRuleUid, userNames, nil, Read, Invalid, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for pre-rule - %s", preRuleName))
			}

			if postRuleName != "" {
				postRuleUid, err := Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleName)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Getting postrule object for  - %s", postRuleName))

				log.InfoD("Update post-rule ownership for users - [%v]", userNames)
				err = AddRuleOwnership(postRuleName, postRuleUid, userNames, nil, Read, Invalid, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying updation of ownership for post-rule - %s", postRuleName))
			}

		})

		Step("Taking backup of application to locked buckets", func() {
			log.InfoD("Taking backup of application to locked bucket")
			for _, customUser := range userNames {
				for backupLocationUID, backupLocationName := range BackupLocationMap {
					log.InfoD("Register cluster for backup with user %s", customUser)
					ctx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
					log.FailOnError(err, "failed to fetch user %s ctx", customUser)
					preRuleUid, _ := Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleName)
					postRuleUid, _ := Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleName)
					backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, RandomString(5), backupLocationName)
					clusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
					err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, BackupOrgID, clusterUid, preRuleName, preRuleUid, postRuleName, postRuleUid)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
					backupList = append(backupList, backupName)
				}
				backupAndUserMap[customUser] = backupList
				backupList = nil
			}
		})

		Step("Restoring the backups application", func() {
			log.InfoD("Restoring the backups application")
			for customUser, backups := range backupAndUserMap {
				ctx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", customUser)
				for _, backupName := range backups {
					for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
						restoreName := fmt.Sprintf("%s-restore-%v-%s", backupName, time.Now().Unix(), clusterName)
						appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
						err = CreateRestoreWithValidation(ctx, restoreName, backupName, make(map[string]string), make(map[string]string), clusterName, BackupOrgID, appContextsToBackup)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore %s", restoreName))
						restoreNames = append(restoreNames, restoreName)
					}
				}
				restoreAndUserMap[customUser] = restoreNames
				restoreNames = nil
			}
		})

		Step("Take schedule backup of applications", func() {
			log.InfoD("Taking schedule backup of applications")
			for _, customUser := range userNames {
				ctx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", customUser)

				for backupLocationUID, backupLocationName := range BackupLocationMap {
					for _, schedulePolicyName := range schedulePolicyNames {
						userScheduleName := fmt.Sprintf("backup-schedule-%v-%s", time.Now().Unix(), backupLocationName)
						preRuleUid, _ := Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleName)
						postRuleUid, _ := Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleName)
						periodicSchedulePolicyUid, err := Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, schedulePolicyName)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Getting schedulepolicy object for  - %s", schedulePolicyName))
						_, err = CreateScheduleBackupWithValidation(ctx, userScheduleName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, make(map[string]string), BackupOrgID, preRuleName, preRuleUid, postRuleName, postRuleUid, schedulePolicyName, periodicSchedulePolicyUid)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of schedule backup with schedule name [%s]", schedulePolicyName))
						scheduleList = append(scheduleList, userScheduleName)
					}
				}
				backupSchedAndUserMap[customUser] = scheduleList
				scheduleList = nil
			}
		})

		Step("Restore from scheduled backup", func() {
			log.InfoD("Restore from scheduled backup")
			for customUser, scheduleList := range backupSchedAndUserMap {
				ctx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", customUser)
				for _, scheduleName := range scheduleList {
					firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, scheduleName, BackupOrgID)
					log.FailOnError(err, fmt.Sprintf("Fetching the name of the first schedule backup [%s]", firstScheduleBackupName))
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, bkpNamespaces)
					for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
						restoreName := fmt.Sprintf("%s-restore-%v-%s", firstScheduleBackupName, time.Now().Unix(), clusterName)
						err = CreateRestoreWithValidation(ctx, restoreName, firstScheduleBackupName, make(map[string]string), make(map[string]string), clusterName, BackupOrgID, appContextsToBackup)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore %s", restoreName))
						restoreNames = append(restoreNames, restoreName)
					}
				}
				restoreAndUserMapSchd[customUser] = restoreNames
				restoreNames = nil
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		// cleaning up all the restores created in the tests
		log.Info("Removing restores created by other users")
		for customUser, restores := range restoreAndUserMap {
			ctx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			for _, restoreName := range restores {
				err := DeleteRestore(restoreName, BackupOrgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
			}
		}

		for customUser, restores := range restoreAndUserMapSchd {
			ctx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			for _, restoreName := range restores {
				err := DeleteRestore(restoreName, BackupOrgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Deleting restore [%s]", restoreName))
			}
		}

		err := SuspendAndDeleteAllSchedulesForUsers(userNames, SourceClusterName, BackupOrgID, false)
		dash.VerifyFatal(err, nil, "Deleting Backup schedules for non root users")

		log.Infof("Deleting registered clusters for users context")
		for _, customUser := range userNames {
			ctx, err := backup.GetNonAdminCtx(customUser, CommonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", customUser)
			for _, clusterName := range []string{SourceClusterName, DestinationClusterName} {
				err := DeleteCluster(clusterName, BackupOrgID, ctx, false)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying deletion of cluster [%s] of the user %s", clusterName, customUser))
			}
		}
	})
})
