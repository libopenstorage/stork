package tests

import (
	"fmt"
	"strings"
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
		appList = Inst().AppList
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
		StartTorpedoTest("BackupAlternatingBetweenLockedAndUnlockedBucket", "Deploying backup", nil, 60018)
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
				CredName := fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				CloudCredUID = uuid.New()
				CloudCredUIDMap[CloudCredUID] = CredName
				err := CreateCloudCredential(provider, CredName, CloudCredUID, orgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", CredName, orgID, provider))
			}
		})

		Step("Creating a locked bucket and backup location", func() {
			log.InfoD("Creating locked buckets and backup location")
			modes := [2]string{"GOVERNANCE", "COMPLIANCE"}
			for _, provider := range providers {
				for _, mode := range modes {
					CredName := fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
					bucketName := fmt.Sprintf("%s-%s-%s-locked", provider, getGlobalLockedBucketName(provider), strings.ToLower(mode))
					backupLocation = fmt.Sprintf("%s-%s-%s-lock", provider, getGlobalLockedBucketName(provider), strings.ToLower(mode))
					err := CreateS3Bucket(bucketName, true, 3, mode)
					log.FailOnError(err, "Unable to create locked s3 bucket %s", bucketName)
					BackupLocationUID = uuid.New()
					BackupLocationMap[BackupLocationUID] = backupLocation
					err = CreateBackupLocation(provider, backupLocation, BackupLocationUID, CredName, CloudCredUID,
						bucketName, orgID, "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocation))
				}
			}
			log.InfoD("Successfully created locked buckets and backup location")
		})

		Step("Creating backup location for unlocked bucket", func() {
			log.InfoD("Creating backup location for unlocked bucket")
			for _, provider := range providers {
				CredName := fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				bucketName := fmt.Sprintf("%s-%s", provider, getGlobalBucketName(provider))
				backupLocation = fmt.Sprintf("%s-%s-unlockedbucket", provider, getGlobalBucketName(provider))
				BackupLocationUID = uuid.New()
				BackupLocationMap[BackupLocationUID] = backupLocation
				err := CreateBackupLocation(provider, backupLocation, BackupLocationUID, CredName, CloudCredUID,
					bucketName, orgID, "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocation))
			}
		})

		Step("Register cluster for backup", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Taking backup of application to locked and unlocked bucket", func() {
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
		ValidateAndDestroy(scheduledAppContexts, opts)

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
		err = DeleteCluster(SourceClusterName, orgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting cluster %s", SourceClusterName))
		err = DeleteCluster(destinationClusterName, orgID, ctx)
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
	)
	labelSelectors := make(map[string]string)
	CloudCredUIDMap := make(map[string]string)
	BackupLocationMap := make(map[string]string)
	podListBeforeSizeMap := make(map[string]int)
	podListAfterSizeMap := make(map[string]int)

	var backupLocation string
	scheduledAppContexts = make([]*scheduler.Context, 0)
	bkpNamespaces = make([]string, 0)

	JustBeforeEach(func() {
		StartTorpedoTest("ResizeOnRestoredVolumeFromLockedBucket", "Resize after the volume is restored from a backup from locked bucket", nil, 59904)
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
			}
		}
	})
	It("Resize after the volume is restored from a backup", func() {
		providers := getProviders()
		Step("Validate applications", func() {
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
					bucketName := fmt.Sprintf("%s-%s-%s", provider, getGlobalLockedBucketName(provider), strings.ToLower(mode))
					backupLocation = fmt.Sprintf("%s-%s-%s-lock", provider, getGlobalLockedBucketName(provider), strings.ToLower(mode))
					err := CreateS3Bucket(bucketName, true, 3, mode)
					log.FailOnError(err, "Unable to create locked s3 bucket %s", bucketName)
					BackupLocationUID = uuid.New()
					BackupLocationMap[BackupLocationUID] = backupLocation
					err = CreateBackupLocation(provider, backupLocation, BackupLocationUID, credName, CloudCredUID,
						bucketName, orgID, "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creating backup location %s", backupLocation))
				}
			}
			log.InfoD("Successfully created locked buckets and backup location")
		})

		Step("Register cluster for backup", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
			dash.VerifyFatal(err, nil, "Creating source and destination cluster")
			clusterStatus, err = Inst().Backup.GetClusterStatus(orgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, orgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		for _, namespace := range bkpNamespaces {
			for backupLocationUID, backupLocationName := range BackupLocationMap {
				Step("Taking backup of application to locked bucket", func() {
					ctx, err := backup.GetAdminCtxFromSecret()
					log.FailOnError(err, "Fetching px-central-admin ctx")
					preRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, preRuleNameList[0])
					postRuleUid, _ := Inst().Backup.GetRuleUid(orgID, ctx, postRuleNameList[0])
					backupName := fmt.Sprintf("%s-%s-%s", BackupNamePrefix, namespace, backupLocationName)
					backupList = append(backupList, backupName)
					appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
					err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, orgID, clusterUid, preRuleNameList[0], preRuleUid, postRuleNameList[0], postRuleUid)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
				})
				Step("Restoring the backups application", func() {
					ctx, err := backup.GetAdminCtxFromSecret()
					log.FailOnError(err, "Fetching px-central-admin ctx")
					err = CreateRestore(fmt.Sprintf("%s-restore", backupName), backupName, nil, SourceClusterName, orgID, ctx, make(map[string]string))
					log.FailOnError(err, "%s restore failed", fmt.Sprintf("%s-restore", backupName))
				})
				Step("Getting size before resize", func() {
					pods, err := core.Instance().GetPods(namespace, labelSelectors)
					log.FailOnError(err, "Unable to fetch the pod list")
					srcClusterConfigPath, err := GetSourceClusterConfigPath()
					log.FailOnError(err, "Getting kubeconfig path for source cluster")
					for _, pod := range pods.Items {
						beforeSize, err = getSizeOfMountPoint(pod.GetName(), namespace, srcClusterConfigPath)
						log.FailOnError(err, "Unable to fetch the size")
						podListBeforeSizeMap[pod.Name] = beforeSize
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
					log.InfoD("Checking volume size after resize")
					pods, err := core.Instance().GetPods(namespace, labelSelectors)
					log.FailOnError(err, "Unable to fetch the pod list")
					srcClusterConfigPath, err := GetSourceClusterConfigPath()
					log.FailOnError(err, "Getting kubeconfig path for source cluster")
					for _, pod := range pods.Items {
						afterSize, err := getSizeOfMountPoint(pod.GetName(), namespace, srcClusterConfigPath)
						log.FailOnError(err, "Unable to mount size")
						podListAfterSizeMap[pod.Name] = afterSize
					}
					for _, pod := range pods.Items {
						dash.VerifyFatal(podListAfterSizeMap[pod.Name] > podListBeforeSizeMap[pod.Name], true, fmt.Sprintf("Verifying Volume size for pod %s", pod.Name))
					}
				})
			}
		}
	})

	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		log.InfoD("Deleting the deployed apps after the testcase")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		ValidateAndDestroy(scheduledAppContexts, opts)

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
		contexts                   []*scheduler.Context
		scheduleNames              []string
		preRuleNameList            []string
		postRuleNameList           []string
		appNamespaces              []string
		appContexts                []*scheduler.Context
		clusterStatus              api.ClusterInfo_StatusInfo_Status
	)
	labelSelectors := make(map[string]string)
	cloudCredUIDMap := make(map[string]string)
	backupLocationMap := make(map[string]string)
	podListBeforeSizeMap := make(map[string]int)
	podListAfterSizeMap := make(map[string]int)
	contexts = make([]*scheduler.Context, 0)
	appNamespaces = make([]string, 0)
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
		contexts = make([]*scheduler.Context, 0)
		for i := 0; i < Inst().GlobalScaleFactor; i++ {
			taskName := fmt.Sprintf("%s-%d", taskNamePrefix, i)
			appContexts = ScheduleApplications(taskName)
			contexts = append(contexts, appContexts...)
			for _, ctx := range appContexts {
				ctx.ReadinessTimeout = appReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				appNamespaces = append(appNamespaces, namespace)
			}
		}
	})
	It("Schedule backup while resizing the volume", func() {
		providers := getProviders()
		Step("Validate applications", func() {
			ValidateApplications(contexts)
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
					bucketName := fmt.Sprintf("%s-%s-%s", provider, getGlobalLockedBucketName(provider), strings.ToLower(mode))
					backupLocation = fmt.Sprintf("%s-%s-%s-lock", provider, getGlobalLockedBucketName(provider), strings.ToLower(mode))
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
			err = CreateSourceAndDestClusters(orgID, "", "", ctx)
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
			for backupLocationUID, backupLocationName := range backupLocationMap {
				Step("Getting size of volume before resizing", func() {
					log.InfoD("Getting size of volume before resizing")
					pods, err := core.Instance().GetPods(namespace, labelSelectors)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the pod list"))
					srcClusterConfigPath, err := GetSourceClusterConfigPath()
					dash.VerifyFatal(err, nil, fmt.Sprintf("Getting kubeconfig path for source cluster %v", srcClusterConfigPath))
					for _, pod := range pods.Items {
						beforeSize, err = getSizeOfMountPoint(pod.GetName(), namespace, srcClusterConfigPath)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the size of volume for pod %v before resizing %v", pod.Name, beforeSize))
						podListBeforeSizeMap[pod.Name] = beforeSize
					}
				})
				Step("Resize the volume before backup schedule", func() {
					log.InfoD("Resize the volume before backup schedule")
					for _, ctx := range contexts {
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
					pods, err := core.Instance().GetPods(namespace, labelSelectors)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the pod list"))
					srcClusterConfigPath, err := GetSourceClusterConfigPath()
					dash.VerifyFatal(err, nil, fmt.Sprintf("Getting kubeconfig path for source cluster %v", srcClusterConfigPath))
					for _, pod := range pods.Items {
						afterSize, err := getSizeOfMountPoint(pod.GetName(), namespace, srcClusterConfigPath)
						dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the mount size %v", afterSize))
						podListAfterSizeMap[pod.Name] = afterSize
					}
					for _, pod := range pods.Items {
						dash.VerifyFatal(podListAfterSizeMap[pod.Name] > podListBeforeSizeMap[pod.Name], true, fmt.Sprintf("Verifying volume size for pod %s", pod.Name))
					}
				})
				Step("Create schedule backup after initializing volume resize", func() {
					log.InfoD("Create schedule backup after initializing volume resize")
					ctx, err := backup.GetAdminCtxFromSecret()
					log.FailOnError(err, "Unable to px-central-admin ctx")
					preRuleUid, err := Inst().Backup.GetRuleUid(orgID, ctx, preRuleNameList[i])
					log.FailOnError(err, "Unable to fetch pre rule Uid")
					postRuleUid, err := Inst().Backup.GetRuleUid(orgID, ctx, postRuleNameList[i])
					log.FailOnError(err, "Unable to fetch post rule Uid")
					scheduleName = fmt.Sprintf("%s-schedule-%v", BackupNamePrefix, time.Now().Unix())
					err = CreateScheduleBackup(scheduleName, SourceClusterName, backupLocationName, backupLocationUID, []string{namespace},
						labelSelectors, orgID, preRuleNameList[i], preRuleUid, postRuleNameList[i], postRuleUid, periodicSchedulePolicyName, periodicSchedulePolicyUid, ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule backup with schedule name [%s]", scheduleName))
					scheduleNames = append(scheduleNames, scheduleName)
				})
				Step("Verifying backup success after initializing volume resize", func() {
					log.InfoD("Verifying backup success after initializing volume resize")
					ctx, err := backup.GetAdminCtxFromSecret()
					log.FailOnError(err, "Unable to px-central-admin ctx")
					firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, scheduleName, orgID)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching the name of the first schedule backup [%s]", firstScheduleBackupName))
					allScheduleBackupNames, err := Inst().Backup.GetAllScheduleBackupNames(ctx, scheduleName, orgID)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching all schedule backups %v", allScheduleBackupNames))
					log.InfoD("Waiting for 15 minutes for the next schedule backup to be triggered")
					time.Sleep(15 * time.Minute)
					secondScheduleBackupName, err := GetOrdinalScheduleBackupName(ctx, scheduleName, 2, orgID)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching recent backup %v", secondScheduleBackupName))
					err = backupSuccessCheck(secondScheduleBackupName, orgID, 5, 30, ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying the success of recent backup named [%s]", secondScheduleBackupName))
				})
			}
		}
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(contexts)
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
		ValidateAndDestroy(contexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, credName, cloudCredUID, ctx)
	})
})
