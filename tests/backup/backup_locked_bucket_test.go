package tests

import (
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo"
	"github.com/pborman/uuid"
	api "github.com/portworx/px-backup-api/pkg/apis/v1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/torpedo/drivers/backup"
	"github.com/portworx/torpedo/drivers/backup/portworx"
	"github.com/portworx/torpedo/drivers/scheduler"
	"github.com/portworx/torpedo/drivers/volume"
	"github.com/portworx/torpedo/pkg/log"
	. "github.com/portworx/torpedo/tests"
)

// This testcase verifies alternating backups between locked and unlocked bucket
var _ = Describe("{BackupAlternatingBetweenLockedAndUnlockedBuckets}", func() {
	var (
		appList  = Inst().AppList
		credName string
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
		StartTorpedoTest("BackupAlternatingBetweenLockedAndUnlockedBuckets", "Deploying backup", nil, 60018)
		log.InfoD("Verifying if the pre/post rules for the required apps are present in the list or not")
		for i := 0; i < len(appList); i++ {
			if Contains(postRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["post_action_list"]; ok {
					dash.VerifyFatal(ok, true, fmt.Sprintf("Post Rule details mentioned for the apps %s", appList[i]))
				}
			}
			if Contains(preRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["pre_action_list"]; ok {
					dash.VerifyFatal(ok, true, fmt.Sprintf("Pre Rule details mentioned for the apps %s", appList[i]))
				}
			}
		}
		log.InfoD("Deploy applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
			}
		}
	})
	It("Backup alternating between locked and unlocked buckets", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validating apps")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Creating rules for backup", func() {
			log.InfoD("Creating pre rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				preRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "pre")
				log.FailOnError(err, "Creating pre rule for deployed apps failed")
				dash.VerifyFatal(preRuleStatus, true, fmt.Sprintf("Verifying pre rule %s for backup", ruleName))
				preRuleNameList = append(preRuleNameList, ruleName)
			}
			log.InfoD("Creating post rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				postRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "post")
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
				err := CreateCloudCredential(provider, credName, CloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", CredName, orgID, provider))
			}
		})

		Step("Creating a locked bucket and backup location", func() {
			log.InfoD("Creating locked buckets and backup location")
			modes := [2]string{"GOVERNANCE", "COMPLIANCE"}
			for _, provider := range providers {
				for _, mode := range modes {
					bucketName := fmt.Sprintf("%s-%s-%v", getGlobalLockedBucketName(provider), strings.ToLower(mode), time.Now().Unix())
					backupLocation = fmt.Sprintf("%s-%s-lock", getGlobalLockedBucketName(provider), strings.ToLower(mode))
					err := CreateS3Bucket(bucketName, true, 3, mode)
					log.FailOnError(err, "Unable to create locked s3 bucket %s", bucketName)
					BackupLocationUID = uuid.New()
					err = CreateBackupLocation(provider, backupLocation, BackupLocationUID, credName, CloudCredUID,
						bucketName, orgID, "")
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
				backupLocation = fmt.Sprintf("%s-%s-unlockedbucket", provider, getGlobalBucketName(provider))
				BackupLocationUID = uuid.New()
				err := CreateBackupLocation(provider, backupLocation, BackupLocationUID, credName, CloudCredUID,
					bucketName, orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocation))
				BackupLocationMap[BackupLocationUID] = backupLocation
			}
		})

		Step("Register cluster for backup", func() {
			log.InfoD("Register cluster for backup")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Taking backup of application to locked and unlocked bucket", func() {
			log.InfoD("Taking backup of application to locked and unlocked bucket")
			for _, namespace := range bkpNamespaces {
				for backupLocationUID, backupLocationName := range BackupLocationMap {
					ctx, err := backup.GetAdminCtxFromSecret()
					log.FailOnError(err, "Fetching px-central-admin ctx")
					preRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, preRuleNameList[0])
					postRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, postRuleNameList[0])
					backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, namespace, backupLocationName)
					backupList = append(backupList, backupName)
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, preRuleNameList[0], preRuleUid, postRuleNameList[0], postRuleUid)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
				}
			}
		})
		Step("Restoring the backups application", func() {
			log.InfoD("Restoring the backups application")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for range bkpNamespaces {
				for _, backupName := range backupList {
					restoreName := fmt.Sprintf("%s-restore", backupName)
					err = CreateRestore(restoreName, backupName, nil, SourceClusterName, orgID, ctx, make(map[string]string))
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating restore %s", restoreName))
				}
			}
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)

		log.InfoD("Deleting backup location and cloud setting")
		for backupLocationUID, backupLocationName := range BackupLocationMap {
			err := DeleteBackupLocation(backupLocationName, backupLocationUID, orgID, false)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup location %s", backupLocationName))
		}
		// Need sleep as it takes some time for
		time.Sleep(time.Minute * 1)
		for CloudCredUID, CredName := range CloudCredUIDMap {
			err := DeleteCloudCredential(CredName, orgID, CloudCredUID)
			dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cloud cred %s", CredName))
		}
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")

		log.Infof("Deleting registered clusters for admin context")
		err = DeleteCluster(SourceClusterName, orgID, ctx, true)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", SourceClusterName))
		err = DeleteCluster(destinationClusterName, orgID, ctx, true)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", destinationClusterName))
	})
})

// This testcase verifies resize after same original volume is restored from a backup stored in a locked bucket
var _ = Describe("{LockedBucketResizeOnRestoredVolume}", func() {
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
		podList              []string
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
		StartTorpedoTest("LockedBucketResizeOnRestoredVolume", "Resize after the volume is restored from a backup from locked bucket", nil, 59904)
		log.InfoD("Verifying if the pre/post rules for the required apps are present in the list or not")
		for i := 0; i < len(appList); i++ {
			if Contains(postRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["post_action_list"]; ok {
					dash.VerifyFatal(ok, true, "Post Rule details mentioned for the apps")
				}
			}
			if Contains(preRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["pre_action_list"]; ok {
					dash.VerifyFatal(ok, true, "Pre Rule details mentioned for the apps")
				}
			}
		}
		log.InfoD("Deploy applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				bkpNamespaces = append(bkpNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
				AppContextsMapping[namespace] = ctx
			}
		}
	})
	It("Resize after the volume is restored from a backup", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})

		Step("Creating rules for backup", func() {
			log.InfoD("Creating pre rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				preRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "pre")
				log.FailOnError(err, "Creating pre rule for deployed apps failed")
				dash.VerifyFatal(preRuleStatus, true, "Verifying pre rule for backup")
				preRuleNameList = append(preRuleNameList, ruleName)
			}
			log.InfoD("Creating post rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				postRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "post")
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
				err := CreateCloudCredential(provider, credName, CloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", credName, orgID, provider))
			}
		})

		Step("Creating a locked bucket and backup location", func() {
			log.InfoD("Creating locked buckets and backup location")
			modes := [2]string{"GOVERNANCE", "COMPLIANCE"}
			for _, provider := range providers {
				for _, mode := range modes {
					bucketName := fmt.Sprintf("%s-%v", getGlobalLockedBucketName(provider), time.Now().Unix())
					backupLocation = fmt.Sprintf("%s-%s-lock", getGlobalLockedBucketName(provider), strings.ToLower(mode))
					err := CreateS3Bucket(bucketName, true, 3, mode)
					log.FailOnError(err, "Unable to create locked s3 bucket %s", bucketName)
					BackupLocationUID = uuid.New()
					err = CreateBackupLocation(provider, backupLocation, BackupLocationUID, credName, CloudCredUID,
						bucketName, orgID, "")
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
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		for _, namespace := range bkpNamespaces {
			Step("Taking backup of application to locked bucket", func() {
				log.InfoD("Taking backup of application to locked bucket")
				for backupLocationUID, backupLocationName := range BackupLocationMap {
					ctx, err := backup.GetAdminCtxFromSecret()
					log.FailOnError(err, "Fetching px-central-admin ctx")
					preRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, preRuleNameList[0])
					postRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, postRuleNameList[0])
					backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, namespace, backupLocationName)
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, preRuleNameList[0], preRuleUid, postRuleNameList[0], postRuleUid)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
					backupList = append(backupList, backupName)
				}
			})
			Step("Restoring the backups application", func() {
				log.InfoD("Restoring the backups application")
				for _, backupName = range backupList {
					ctx, err := backup.GetAdminCtxFromSecret()
					log.FailOnError(err, "Fetching px-central-admin ctx")
					err = CreateRestore(fmt.Sprintf("%s-restore", backupName), backupName, nil, SourceClusterName, orgID, ctx, make(map[string]string))
					log.FailOnError(err, "%s restore failed", fmt.Sprintf("%s-restore", backupName))
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
				for _, pod := range pods.Items {
					volumeMounts, err := GetVolumeMounts(AppContextsMapping[namespace])
					dash.VerifyFatal(err, nil, fmt.Sprintf("unable to get the mountpoints from the application spec %s", AppContextsMapping[namespace].App.Key))
					for _, volumeMount := range volumeMounts {
						beforeSize, err = getSizeOfMountPoint(pod.GetName(), namespace, srcClusterConfigPath, volumeMount)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the size of volume before resizing %v from pod %v", beforeSize, pod.GetName()))
						volListBeforeSizeMap[volumeMount] = beforeSize
						podList = append(podList, pod.Name)
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
				for _, podName := range podList {
					volumeMounts, err := GetVolumeMounts(AppContextsMapping[namespace])
					dash.VerifyFatal(err, nil, fmt.Sprintf("unable to get the mountpoints from the application spec %s", AppContextsMapping[namespace].App.Key))
					for _, volumeMount := range volumeMounts {
						afterSize, err := getSizeOfMountPoint(podName, namespace, srcClusterConfigPath, volumeMount)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the size of volume ater resizing %v from pod %v", afterSize, podName))
						volListAfterSizeMap[volumeMount] = afterSize
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
		DestroyApps(scheduledAppContexts, opts)

		log.InfoD("Deleting backup location, cloud creds and clusters")
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		CleanupCloudSettingsAndClusters(BackupLocationMap, credName, CloudCredUID, ctx)
	})
})

// This testcase verifies schedule backups are successful while volume resize is in progress for locked bucket
var _ = Describe("{LockedBucketResizeVolumeOnScheduleBackup}", func() {
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
		podList                    []string
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
		StartTorpedoTest("LockedBucketResizeVolumeOnScheduleBackup", "Verify schedule backups are successful while volume resize is in progress for locked bucket", nil, 59899)
		log.InfoD("Verifying if the pre/post rules for the required apps are present in the list or not")
		for i := 0; i < len(appList); i++ {
			if Contains(postRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["post_action_list"]; ok {
					dash.VerifyFatal(ok, true, fmt.Sprintf("Post Rule details mentioned for the app %v", appList[i]))
				}
			}
			if Contains(preRuleApp, appList[i]) {
				if _, ok := portworx.AppParameters[appList[i]]["pre_action_list"]; ok {
					dash.VerifyFatal(ok, true, fmt.Sprintf("Pre Rule details mentioned for the app %v", appList[i]))
				}
			}
		}
		log.InfoD("Deploy applications")
		scheduledAppContexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
				scheduledAppContexts = append(scheduledAppContexts, ctx)
				AppContextsMapping[namespace] = ctx
			}
		}
	})
	It("Schedule backup while resizing the volume", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			log.InfoD("Validate applications")
			ValidateApplications(scheduledAppContexts)
		})
		Step("Creating pre and post rule for deployed apps", func() {
			log.InfoD("Creating pre and post rule for deployed apps")
			for i := 0; i < len(appList); i++ {
				preRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "pre")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating pre rule for deployed apps for %v with status %v", appList[i], preRuleStatus))
				dash.VerifyFatal(preRuleStatus, true, "Verifying pre rule for backup")
				preRuleNameList = append(preRuleNameList, ruleName)
				postRuleStatus, ruleName, err := Inst().Backup.CreateRuleForBackup(appList[i], orgID, "post")
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
				err = CreateCloudCredential(provider, credName, cloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating cloud credentials %v", credName))
			}
		})
		Step("Creating a locked bucket and backup location", func() {
			log.InfoD("Creating a locked bucket and backup location")
			for _, provider := range providers {
				for _, mode := range modes {
					bucketName := fmt.Sprintf("%s-%v", getGlobalLockedBucketName(provider), time.Now().Unix())
					backupLocation = fmt.Sprintf("%s-%s-lock", getGlobalLockedBucketName(provider), strings.ToLower(mode))
					err := CreateS3Bucket(bucketName, true, 3, mode)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating locked s3 bucket %s", bucketName))
					BackupLocationUID = uuid.New()
					backupLocationMap[BackupLocationUID] = backupLocation
					err = CreateBackupLocation(provider, backupLocation, BackupLocationUID, credName, cloudCredUID,
						bucketName, orgID, "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocation))
				}
			}
		})
		Step("Configure source and destination clusters with px-central-admin ctx", func() {
			log.InfoD("Configure source and destination clusters with px-central-admin ctx")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
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
			err = Inst().Backup.BackupSchedulePolicy(periodicSchedulePolicyName, periodicSchedulePolicyUid, orgID, periodicSchedulePolicyInfo)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of periodic schedule policy of interval 15 minutes named [%s]", periodicSchedulePolicyName))
			periodicSchedulePolicyUid, err = Inst().Backup.GetSchedulePolicyUid(orgID, ctx, periodicSchedulePolicyName)
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
				for _, pod := range pods.Items {
					volumeMounts, err := GetVolumeMounts(AppContextsMapping[namespace])
					dash.VerifyFatal(err, nil, fmt.Sprintf("unable to get the mountpoints from the application spec %s", AppContextsMapping[namespace].App.Key))
					for _, volumeMount := range volumeMounts {
						beforeSize, err = getSizeOfMountPoint(pod.GetName(), namespace, srcClusterConfigPath, volumeMount)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the size of volume before resizing %v from pod %v", beforeSize, pod.GetName()))
						volListBeforeSizeMap[volumeMount] = beforeSize
						podList = append(podList, pod.Name)
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
				for _, podName := range podList {
					volumeMounts, err := GetVolumeMounts(AppContextsMapping[namespace])
					dash.VerifyFatal(err, nil, fmt.Sprintf("unable to get the mountpoints from the application spec %s", AppContextsMapping[namespace].App.Key))
					for _, volumeMount := range volumeMounts {
						afterSize, err := getSizeOfMountPoint(podName, namespace, srcClusterConfigPath, volumeMount)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the size of volume ater resizing %v from pod %v", afterSize, podName))
						volListAfterSizeMap[volumeMount] = afterSize
					}
				}
				for _, volumeMount := range volumeMounts {
					dash.VerifyFatal(volListAfterSizeMap[volumeMount] > volListBeforeSizeMap[volumeMount], true, fmt.Sprintf("Verifying volume size has increased for pod %s", volumeMount))
				}

			})
			Step("Validate applications before taking backup", func() {
				log.InfoD("Validate applications")
				ValidateApplications(scheduledAppContexts)
			})
			Step("Create schedule backup after initializing volume resize", func() {
				log.InfoD("Create schedule backup after initializing volume resize")
				for backupLocationUID, backupLocationName := range backupLocationMap {
					log.InfoD("Create schedule backup after initializing volume resize")
					ctx, err := backup.GetAdminCtxFromSecret()
					log.FailOnError(err, "Unable to px-central-admin ctx")
					preRuleUid, err := Inst().Backup.GetRuleUid(orgID, ctx, preRuleNameList[i])
					log.FailOnError(err, "Unable to fetch pre rule Uid")
					postRuleUid, err := Inst().Backup.GetRuleUid(orgID, ctx, postRuleNameList[i])
					log.FailOnError(err, "Unable to fetch post rule Uid")
					scheduleName = fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, time.Now().Unix())
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					_, err = CreateScheduleBackupWithValidation(ctx, scheduleName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, make(map[string]string), orgID, preRuleNameList[i], preRuleUid, postRuleNameList[i], postRuleUid, periodicSchedulePolicyName, periodicSchedulePolicyUid)
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
					backupName, err := GetOrdinalScheduleBackupName(ctx, scheduleName, 2, orgID)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching recent backup %v", backupName))
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					err = backupSuccessCheckWithValidation(ctx, backupName, appContextsToBackup, orgID, maxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
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
			err = DeleteSchedule(scheduleName, SourceClusterName, orgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		}
		err = Inst().Backup.DeleteBackupSchedulePolicy(orgID, []string{periodicSchedulePolicyName})
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", []string{periodicSchedulePolicyName}))
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		DestroyApps(scheduledAppContexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})

// DeleteLockedBucketUserObjectsFromAdmin delete backups, backup schedules, restore and cluster objects created with locked bucket from the admin
var _ = Describe("{DeleteLockedBucketUserObjectsFromAdmin}", func() {
	var (
		scheduledAppContexts                           = make([]*scheduler.Context, 0)
		appNamespaces                                  = make([]string, 0)
		infraAdminUsers                                = make([]string, 0)
		providers                                      = getProviders()
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
	)

	JustBeforeEach(func() {
		StartTorpedoTest("DeleteLockedBucketUserObjectsFromAdmin", "Delete backups, backup schedules, restore and cluster objects created with locked bucket from the admin", nil, 87566)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
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
			ValidateApplications(scheduledAppContexts)
		})
		Step(fmt.Sprintf("Create %d users with %s role", numberOfUsers, infraAdminRole), func() {
			log.InfoD(fmt.Sprintf("Creating %d users with %s role", numberOfUsers, infraAdminRole))
			for _, user := range createUsers(numberOfUsers) {
				err := backup.AddRoleToUser(user, infraAdminRole, fmt.Sprintf("Adding %v role to %s", infraAdminRole, user))
				log.FailOnError(err, "failed to add role %s to the user %s", infraAdminRole, user)
				infraAdminUsers = append(infraAdminUsers, user)
			}
		})
		createObjectsFromUser := func(user string) {
			Step(fmt.Sprintf("Create cloud credential and locked bucket backup location from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Creating cloud credential and locked bucket backup location from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				modes := [2]string{"GOVERNANCE", "COMPLIANCE"}
				for _, provider := range providers {
					userCloudCredentialName := fmt.Sprintf("autogenerated-cred-%v", time.Now().Unix())
					userCloudCredentialUID := uuid.New()
					err = CreateCloudCredential(provider, userCloudCredentialName, userCloudCredentialUID, orgID, nonAdminCtx)
					log.FailOnError(err, "failed to create cloud credential %s using provider %s for the user", userCloudCredentialName, provider)
					userCloudCredentialMap[user] = map[string]string{userCloudCredentialUID: userCloudCredentialName}
					for _, mode := range modes {
						userBackupLocationName := fmt.Sprintf("autogenerated-backup-location-%v", time.Now().Unix())
						userBackupLocationUID := uuid.New()
						lockedBucketName := fmt.Sprintf("%s-%s-%s-locked", provider, getGlobalLockedBucketName(provider), strings.ToLower(mode))
						err := CreateS3Bucket(lockedBucketName, true, 3, mode)
						log.FailOnError(err, "failed to create locked s3 bucket %s", lockedBucketName)
						err = CreateBackupLocationWithContext(provider, userBackupLocationName, userBackupLocationUID, userCloudCredentialName, userCloudCredentialUID, lockedBucketName, orgID, "", nonAdminCtx)
						log.FailOnError(err, "failed to create locked bucket backup location %s using provider %s for the user", userBackupLocationName, provider)
						userBackupLocationMap[user] = map[string]string{userBackupLocationUID: userBackupLocationName}
					}
				}
			})
			Step(fmt.Sprintf("Create source and destination cluster from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Creating source and destination cluster from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				err = CreateApplicationClusters(orgID, "", "", nonAdminCtx)
				log.FailOnError(err, "failed create source and destination cluster from the user %s", user)
				clusterStatus, err := Inst().Backup.GetClusterStatus(orgID, SourceClusterName, nonAdminCtx)
				log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
				dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
				userClusterMap[user] = make(map[string]string)
				for _, clusterName := range []string{SourceClusterName, destinationClusterName} {
					userClusterUID, err := Inst().Backup.GetClusterUID(nonAdminCtx, orgID, clusterName)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", clusterName))
					userClusterMap[user][clusterName] = userClusterUID
				}
			})
			Step(fmt.Sprintf("Take backup of applications from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Taking backup of applications from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				var wg sync.WaitGroup
				var mu sync.RWMutex
				userBackupMap[user] = make(map[string]string)
				createBackup := func(backupName string, namespace string) {
					defer GinkgoRecover()
					defer wg.Done()
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					for backupLocationUID, backupLocationName := range userBackupLocationMap[user] {
						err := CreateBackupWithValidation(nonAdminCtx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, make(map[string]string), orgID, userClusterMap[user][SourceClusterName], "", "", "", "")
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
				nonAdminCtx, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				userSchedulePolicyName := fmt.Sprintf("%s-%v", "periodic", time.Now().Unix())
				userSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, userSchedulePolicyInterval, 5)
				userSchedulePolicyCreateRequest := &api.SchedulePolicyCreateRequest{
					CreateMetadata: &api.CreateMetadata{
						Name:  userSchedulePolicyName,
						OrgId: orgID,
					},
					SchedulePolicy: userSchedulePolicyInfo,
				}
				userSchedulePolicyCreateRequest.SchedulePolicy.ForObjectLock = true
				_, err = Inst().Backup.CreateSchedulePolicy(nonAdminCtx, userSchedulePolicyCreateRequest)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation schedule policy %s", userSchedulePolicyName))
				userSchedulePolicyUID, err := Inst().Backup.GetSchedulePolicyUid(orgID, nonAdminCtx, userSchedulePolicyName)
				log.FailOnError(err, "failed to fetch schedule policy uid %s of user %s", userSchedulePolicyName, user)
				userSchedulePolicyMap[user] = map[string]string{userSchedulePolicyUID: userSchedulePolicyName}
			})
			Step(fmt.Sprintf("Take schedule backup of applications from the user %s", user), func() {
				log.InfoD(fmt.Sprintf("Taking schedule backup of applications from the user %s", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				userScheduleName := fmt.Sprintf("backup-schedule-%v", time.Now().Unix())
				for backupLocationUID, backupLocationName := range userBackupLocationMap[user] {
					for schedulePolicyUID, schedulePolicyName := range userSchedulePolicyMap[user] {
						_, err = CreateScheduleBackupWithValidation(nonAdminCtx, userScheduleName, SourceClusterName, backupLocationName, backupLocationUID, scheduledAppContexts, make(map[string]string), orgID, "", "", "", "", schedulePolicyName, schedulePolicyUID)
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
				nonAdminCtx, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				var wg sync.WaitGroup
				var mu sync.RWMutex
				userRestoreMap[user] = make(map[string]string, 0)
				createRestore := func(backupName string, restoreName string, namespace string) {
					defer GinkgoRecover()
					defer wg.Done()
					customNamespace := fmt.Sprintf("custom-%s", namespace)
					namespaceMapping := map[string]string{namespace: customNamespace}
					err = CreateRestoreWithValidation(nonAdminCtx, restoreName, backupName, namespaceMapping, make(map[string]string), destinationClusterName, orgID, scheduledAppContexts)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of restore %s of backup %s", restoreName, backupName))
					restoreUid, err := Inst().Backup.GetRestoreUID(nonAdminCtx, restoreName, orgID)
					log.FailOnError(err, "failed to fetch restore %s uid of the user %s", restoreName, user)
					mu.Lock()
					defer mu.Unlock()
					userRestoreMap[user][restoreUid] = restoreName
				}
				for backupName, namespace := range userBackupMap[user] {
					wg.Add(1)
					restoreName := fmt.Sprintf("%s-%s", restoreNamePrefix, backupName)
					go createRestore(backupName, restoreName, namespace)
				}
				wg.Wait()
				log.Infof("The list of user restores taken are: %v", userRestoreMap)
			})
			Step(fmt.Sprintf("Verify backups of the user %s from the admin", user), func() {
				log.InfoD(fmt.Sprintf("Verifying backups of the user %s from the admin", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				userOwnerID, err := portworx.GetSubFromCtx(nonAdminCtx)
				log.FailOnError(err, "failed to fetch user owner id %s", user)
				backupNamesByOwnerID, err := GetAllBackupNamesByOwnerID(userOwnerID, orgID, ctx)
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
				nonAdminCtx, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				userOwnerID, err := portworx.GetSubFromCtx(nonAdminCtx)
				log.FailOnError(err, "failed to fetch user owner id %s", user)
				backupScheduleNamesByOwnerID, err := GetAllBackupScheduleNamesByOwnerID(userOwnerID, orgID, ctx)
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
				nonAdminCtx, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				userOwnerID, err := portworx.GetSubFromCtx(nonAdminCtx)
				log.FailOnError(err, "failed to fetch user owner id %s", user)
				restoreNamesByOwnerID, err := GetAllRestoreNamesByOwnerID(userOwnerID, orgID, ctx)
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
				nonAdminCtx, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				allScheduleBackupNames, err := Inst().Backup.GetAllScheduleBackupNames(nonAdminCtx, userScheduleNameMap[user], orgID)
				log.FailOnError(err, "failed to get all schedule backup names with schedule name %s of the user %s", userScheduleNameMap[user], user)
				for i := len(allScheduleBackupNames) - 1; i >= 0; i-- {
					backupName := allScheduleBackupNames[i]
					backupUid, err := Inst().Backup.GetBackupUID(nonAdminCtx, backupName, orgID)
					log.FailOnError(err, "failed to fetch backup %s uid of the user %s", backupName, user)
					_, err = DeleteBackupWithClusterUID(backupName, backupUid, SourceClusterName, userClusterMap[user][SourceClusterName], orgID, ctx)
					log.FailOnError(err, "failed to delete schedule backup %s of the user %s", backupName, user)
				}
				scheduleUid, err := Inst().Backup.GetBackupScheduleUID(nonAdminCtx, userScheduleNameMap[user], orgID)
				log.FailOnError(err, "failed to fetch backup schedule %s uid of the user %s", userScheduleNameMap[user], user)
				err = DeleteScheduleWithUID(userScheduleNameMap[user], scheduleUid, orgID, ctx)
				log.FailOnError(err, "failed to delete schedule %s of the user %s", userScheduleNameMap[user], user)
			})
			Step(fmt.Sprintf("Delete user %s backups from the admin", user), func() {
				log.InfoD(fmt.Sprintf("Deleting user %s backups from the admin", user))
				nonAdminCtx, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				for backupName := range userBackupMap[user] {
					backupUid, err := Inst().Backup.GetBackupUID(nonAdminCtx, backupName, orgID)
					log.FailOnError(err, "failed to fetch backup %s uid of the user %s", backupName, user)
					_, err = DeleteBackupWithClusterUID(backupName, backupUid, SourceClusterName, userClusterMap[user][SourceClusterName], orgID, ctx)
					log.FailOnError(err, "failed to delete backup %s of the user %s", backupName, user)
				}
			})
			Step(fmt.Sprintf("Delete user %s restores from the admin", user), func() {
				log.InfoD(fmt.Sprintf("Deleting user %s restores from the admin", user))
				for restoreUid, restoreName := range userRestoreMap[user] {
					err = DeleteRestoreWithUID(restoreName, restoreUid, orgID, ctx)
					log.FailOnError(err, "failed to delete restore %s of the user %s", restoreName, user)
				}
			})
			Step(fmt.Sprintf("Wait for the backups and backup schedule to be deleted"), func() {
				log.InfoD("Waiting for the backups and backup schedule to be deleted")
				nonAdminCtx, err := backup.GetNonAdminCtx(user, commonPassword)
				log.FailOnError(err, "failed to fetch user %s ctx", user)
				clusterInspectReq := &api.ClusterInspectRequest{
					OrgId:          orgID,
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
						orgID,
						clusterResp.GetCluster(),
						backupLocationDeleteTimeout,
						backupLocationDeleteRetryTime,
					)
					log.FailOnError(err, "failed while waiting for backup schedule %s to be deleted for the user %s", userScheduleNameMap[user], user)
					for schedulePolicyUID, schedulePolicyName := range userSchedulePolicyMap[user] {
						schedulePolicyDeleteRequest := &api.SchedulePolicyDeleteRequest{
							Name:  schedulePolicyName,
							Uid:   schedulePolicyUID,
							OrgId: orgID,
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
						err = Inst().Backup.WaitForBackupDeletion(nonAdminCtx, backupName, orgID, backupDeleteTimeout, backupDeleteRetryTime)
						log.FailOnError(err, "failed while waiting for backup %s to be deleted", backupName)
					}(backupName)
				}
				wg.Wait()
			})
			Step(fmt.Sprintf("Delete user %s source and destination cluster from the admin", user), func() {
				log.InfoD(fmt.Sprintf("Deleting user %s source and destination cluster from the admin", user))
				for _, clusterName := range []string{SourceClusterName, destinationClusterName} {
					err := DeleteClusterWithUID(clusterName, userClusterMap[user][clusterName], orgID, ctx, false)
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
		DestroyApps(scheduledAppContexts, opts)
		cleanupUserObjects := func(user string) {
			nonAdminCtx, err := backup.GetNonAdminCtx(user, commonPassword)
			log.FailOnError(err, "failed to fetch user %s ctx", user)
			for cloudCredentialUID, cloudCredentialName := range userCloudCredentialMap[user] {
				CleanupCloudSettingsAndClusters(userBackupLocationMap[user], cloudCredentialName, cloudCredentialUID, nonAdminCtx)
				break
			}
			err = backup.DeleteUser(user)
			log.FailOnError(err, "failed to delete user %s", user)
		}
		err := TaskHandler(infraAdminUsers, cleanupUserObjects, Parallel)
		log.FailOnError(err, "failed to cleanup user objects from user")
	})
})
