package tests

import (
	"fmt"
	"github.com/hashicorp/go-version"
	k8score "github.com/portworx/sched-ops/k8s/core"
	corev1 "k8s.io/api/core/v1"
	"net/url"
	"os"
	"strconv"
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

// This testcase verifies Px Backup upgrade
var _ = Describe("{UpgradePxBackup}", Label(TestCaseLabelsMap[UpgradePxBackup]...), func() {

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("UpgradePxBackup", "Upgrading backup", nil, 0, Mkoppal, Q1FY24)
	})
	It("Upgrade Px Backup", func() {
		Step("Upgrade Px Backup", func() {
			log.InfoD("Upgrade Px Backup to version %s", LatestPxBackupVersion)
			err := PxBackupUpgrade(LatestPxBackupVersion)
			dash.VerifyFatal(err, nil, "Verifying Px Backup upgrade completion")
		})
	})
	JustAfterEach(func() {
		defer EndPxBackupTorpedoTest(make([]*scheduler.Context, 0))
		log.Infof("No cleanup required for this testcase")

	})
})

// StorkUpgradeWithBackup validates backups with stork upgrade
var _ = Describe("{StorkUpgradeWithBackup}", Label(TestCaseLabelsMap[StorkUpgradeWithBackup]...), func() {
	var (
		scheduledAppContexts []*scheduler.Context
		backupLocationName   string
		backupLocationUID    string
		cloudCredUID         string
		bkpNamespaces        []string
		scheduleNames        []string
		cloudAccountName     string
		scheduleName         string
		schBackupName        string
		schPolicyUid         string
		upgradeStorkImageStr string
		clusterUid           string
		clusterStatus        api.ClusterInfo_StatusInfo_Status
		controlChannel       chan string
		errorGroup           *errgroup.Group
	)
	// testrailID corresponds to: https://portworx.testrail.net/index.php?/cases/view/58023
	labelSelectors := make(map[string]string)
	cloudCredUIDMap := make(map[string]string)
	backupLocationMap := make(map[string]string)
	bkpNamespaces = make([]string, 0)
	timeStamp := strconv.Itoa(int(time.Now().Unix()))
	periodicPolicyName := fmt.Sprintf("%s-%s", "periodic", timeStamp)
	appContextsToBackupMap := make(map[string][]*scheduler.Context)

	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("StorkUpgradeWithBackup", "Validates the scheduled backups and creation of new backup after stork upgrade", nil, 58023, Ak, Q1FY24)
		log.Infof("Application installation")
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

	It("StorkUpgradeWithBackup", func() {
		Step("Validate deployed applications", func() {
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(scheduledAppContexts, ctx)
		})
		providers := GetBackupProviders()
		Step("Adding Cloud Account", func() {
			log.InfoD("Adding cloud account")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudAccountName = fmt.Sprintf("%s-%v", provider, timeStamp)
				cloudCredUID = uuid.New()
				cloudCredUIDMap[cloudCredUID] = cloudAccountName
				err := CreateCloudCredential(provider, cloudAccountName, cloudCredUID, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential named [%s] for org [%s] with [%s] as provider", cloudAccountName, BackupOrgID, provider))
			}
		})

		Step("Adding Backup Location", func() {
			log.InfoD("Adding Backup Location")
			for _, provider := range providers {
				cloudAccountName = fmt.Sprintf("%s-%v", provider, timeStamp)
				backupLocationName = fmt.Sprintf("auto-bl-%v", time.Now().Unix())
				backupLocationUID = uuid.New()
				backupLocationMap[backupLocationUID] = backupLocationName
				err := CreateBackupLocation(provider, backupLocationName, backupLocationUID, cloudAccountName, cloudCredUID, getGlobalBucketName(provider), BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of adding backup location - %s", backupLocationName))
			}
		})

		Step("Creating Schedule Policy", func() {
			log.InfoD("Creating Schedule Policy")
			periodicSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, 15, 5)
			periodicPolicyStatus := Inst().Backup.BackupSchedulePolicy(periodicPolicyName, uuid.New(), BackupOrgID, periodicSchedulePolicyInfo)
			dash.VerifyFatal(periodicPolicyStatus, nil, fmt.Sprintf("Verification of creating periodic schedule policy - %s", periodicPolicyName))
		})

		Step("Adding Clusters for backup", func() {
			log.InfoD("Adding application clusters")
			ctx, err := backup.GetAdminCtxFromSecret()
			dash.VerifySafely(err, nil, "Fetching px-central-admin ctx")
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of creating source - %s and destination - %s clusters", SourceClusterName, DestinationClusterName))
			clusterStatus, err = Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			clusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
		})

		Step("Creating schedule backups", func() {
			log.InfoD("Creating schedule backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			schPolicyUid, err = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, periodicPolicyName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching uid of periodic schedule policy named [%s]", periodicPolicyName))
			for _, namespace := range bkpNamespaces {
				scheduleName = fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				appContextsToBackupMap[scheduleName] = appContextsToBackup
				_, err = CreateScheduleBackupWithValidation(ctx, scheduleName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, BackupOrgID, "", "", "", "", periodicPolicyName, schPolicyUid)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of schedule backup with schedule name [%s]", scheduleName))
				scheduleNames = append(scheduleNames, scheduleName)
			}
		})

		Step("Upgrade the stork version", func() {
			log.InfoD("Upgrade the stork version")
			upgradeStorkImageStr = GetEnv(UpgradeStorkImage, LatestStorkImage)
			log.Infof("Upgrading stork version on source cluster to %s ", upgradeStorkImageStr)
			err := UpgradeStorkVersion(upgradeStorkImageStr)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of stork version upgrade to - %s on source cluster", upgradeStorkImageStr))
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			log.Infof("Upgrading stork version on destination cluster to %s ", upgradeStorkImageStr)
			err = UpgradeStorkVersion(upgradeStorkImageStr)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of stork version upgrade to - %s on destination cluster", upgradeStorkImageStr))
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
		})

		Step("Verifying scheduled backup after stork version upgrade", func() {
			log.InfoD("Verifying scheduled backup after stork version upgrade")
			log.Infof("waiting 15 minutes for another backup schedule to trigger")
			time.Sleep(15 * time.Minute)
			for _, scheduleName := range scheduleNames {
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				allScheduleBackupNames, err := Inst().Backup.GetAllScheduleBackupNames(ctx, scheduleName, BackupOrgID)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of all schedule backups with schedule name - %s", scheduleName))
				dash.VerifyFatal(len(allScheduleBackupNames) > 1, true, fmt.Sprintf("Verfiying the backup count is increased for backups with schedule name - %s", scheduleName))
				schBackupName, err = GetLatestScheduleBackupName(ctx, scheduleName, BackupOrgID)
				log.FailOnError(err, fmt.Sprintf("Failed to get latest schedule backup with schedule name - %s", scheduleName))
				err = BackupSuccessCheckWithValidation(ctx, schBackupName, appContextsToBackupMap[scheduleName], BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Validation of the latest schedule backup [%s]", schBackupName))
			}
		})

		Step("Verify creating new backups after upgrade", func() {
			log.InfoD("Verify creating new backups after upgrade")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, namespace := range bkpNamespaces {
				backupName := fmt.Sprintf("%s-%s-%v", BackupNamePrefix, namespace, time.Now().Unix())
				appContextsToBackup := FilterAppContextsByNamespace(scheduledAppContexts, []string{namespace})
				err := CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUID, appContextsToBackup, labelSelectors, BackupOrgID, clusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of backup [%s]", backupName))
			}
		})
	})

	JustAfterEach(func() {
		err := SetSourceKubeConfig()
		log.FailOnError(err, "Switching context to source cluster failed")
		defer EndPxBackupTorpedoTest(scheduledAppContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		log.InfoD("Clean up objects after test execution")
		log.Infof("Deleting backup schedule")
		for _, scheduleName := range scheduleNames {
			err = DeleteSchedule(scheduleName, SourceClusterName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verification of deleting backup schedule - %s", scheduleName))
		}
		log.Infof("Deleting backup schedule policy")
		policyList := []string{periodicPolicyName}
		err = Inst().Backup.DeleteBackupSchedulePolicy(BackupOrgID, policyList)
		dash.VerifySafely(err, nil, fmt.Sprintf("Deleting backup schedule policies %s ", policyList))
		log.Infof("Deleting the deployed apps after test execution")
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		err = DestroyAppsWithData(scheduledAppContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudAccountName, cloudCredUID, ctx)
	})
})

// This testcase executes and validates end-to-end backup and restore operations with PX-Backup upgrade
var _ = Describe("{PXBackupEndToEndBackupAndRestoreWithUpgrade}", Label(TestCaseLabelsMap[PXBackupEndToEndBackupAndRestoreWithUpgrade]...), func() {
	var (
		numDeployments                     int
		srcClusterContexts                 []*scheduler.Context
		srcClusterAppNamespaces            map[string][]string
		kubevirtScheduledAppContexts       []*scheduler.Context
		destClusterContexts                []*scheduler.Context
		destClusterAppNamespaces           map[string][]string
		cloudAccountUid                    string
		cloudAccountName                   string
		backupLocationName                 string
		backupLocationUid                  string
		backupLocationMap                  map[string]string
		srcClusterUid                      string
		destClusterUid                     string
		preRuleNames                       map[string]string
		preRuleUids                        map[string]string
		postRuleNames                      map[string]string
		postRuleUids                       map[string]string
		backupToContextMapping             map[string][]*scheduler.Context
		backupWithoutRuleNames             []string
		backupWithRuleNames                []string
		schedulePolicyName                 string
		schedulePolicyUid                  string
		intervalInMins                     int
		singleNSNamespaces                 []string
		singleNSScheduleNames              []string
		allNamespaces                      []string
		allNSScheduleName                  string
		backupAfterUpgradeWithoutRuleNames []string
		backupAfterUpgradeWithRuleNames    []string
		restoreNames                       []string
		mutex                              sync.Mutex
		controlChannel                     chan string
		errorGroup                         *errgroup.Group
		vmBackupNames                      []string
		partialAppNamespaces               []string
		partialAppContexts                 []*scheduler.Context
		partialScheduleName                string
		partialSchedulePolicyUid           string
		partialScheduledBackupName         string
		partialCloudAccountName            string
		partialCloudAccountUid             string
		partialBackupLocationName          string
		partialBackupLocationUid           string
		partialBackupLocationMap           map[string]string
		failedVolumes                      []*corev1.PersistentVolumeClaim
		stopChans                          map[int]chan struct{}
	)
	updateBackupToContextMapping := func(backupName string, appContextsToBackup []*scheduler.Context) {
		mutex.Lock()
		defer mutex.Unlock()
		backupToContextMapping[backupName] = appContextsToBackup
	}
	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("PXBackupEndToEndBackupAndRestoreWithUpgrade", "Validates end-to-end backup and restore operations with PX-Backup upgrade", nil, 84757, KPhalgun, Q1FY24)
		log.Infof("Scheduling applications")
		numDeployments = Inst().GlobalScaleFactor
		if len(Inst().AppList) == 1 && numDeployments < 2 {
			numDeployments = 2
		}
		destClusterContexts = make([]*scheduler.Context, 0)
		destClusterAppNamespaces = make(map[string][]string)
		log.InfoD("Scheduling applications in destination cluster")
		err := SetDestinationKubeConfig()
		log.FailOnError(err, "Switching context to destination cluster failed")
		for i := 0; i < numDeployments; i++ {
			taskName := fmt.Sprintf("dst-%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			destClusterContexts = append(destClusterContexts, appContexts...)
			for index, ctx := range appContexts {
				appName := Inst().AppList[index]
				ctx.ReadinessTimeout = AppReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				log.InfoD("Scheduled application [%s] in destination cluster in namespace [%s]", appName, namespace)
				destClusterAppNamespaces[appName] = append(destClusterAppNamespaces[appName], namespace)
			}
		}
		srcClusterContexts = make([]*scheduler.Context, 0)
		srcClusterAppNamespaces = make(map[string][]string)
		log.InfoD("Scheduling applications in source cluster")
		err = SetSourceKubeConfig()
		log.FailOnError(err, "Switching context to source cluster failed")
		for i := 0; i < numDeployments; i++ {
			taskName := fmt.Sprintf("src-%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			srcClusterContexts = append(srcClusterContexts, appContexts...)
			for index, ctx := range appContexts {
				appName := Inst().AppList[index]
				ctx.ReadinessTimeout = AppReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				log.InfoD("Scheduled application [%s] in source cluster in namespace [%s]", appName, namespace)
				srcClusterAppNamespaces[appName] = append(srcClusterAppNamespaces[appName], namespace)
			}
		}
	})
	It("PX-Backup End-to-End Backup and Restore with Upgrade", func() {
		Step("Provision apps for partial success validation", func() {
			log.InfoD("Provisioning two apps for partial success validation")
			appList := Inst().AppList
			defer func() {
				Inst().AppList = appList
			}()
			Inst().AppList = []string{"postgres-withdata"}
			partialAppContexts = make([]*scheduler.Context, 0)
			partialAppNamespaces = make([]string, 0)
			numDeployments = 2
			err := SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
			log.Infof("Scheduling applications")
			for i := 0; i < numDeployments; i++ {
				taskName := fmt.Sprintf("partial-app-%d", i)
				appContexts := ScheduleApplications(taskName)
				for _, appCtx := range appContexts {
					namespace := GetAppNamespace(appCtx, taskName)
					partialAppNamespaces = append(partialAppNamespaces, namespace)
					partialAppContexts = append(partialAppContexts, appCtx)
					appCtx.ReadinessTimeout = AppReadinessTimeout
				}
			}
		})

		Step("Validate app namespaces in destination cluster", func() {
			log.InfoD("Validating app namespaces in destination cluster")
			err := SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			ValidateApplications(destClusterContexts)
		})
		Step("Validate app namespaces in source cluster", func() {
			log.InfoD("Validating app namespaces in source cluster")
			err := SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
			ctx, _ := backup.GetAdminCtxFromSecret()
			controlChannel, errorGroup = ValidateApplicationsStartData(srcClusterContexts, ctx)
		})
		Step("Deploying Kubevirt Virtual Machines and validating", func() {
			if IsKubevirtInstalled() {
				log.InfoD("Deploying Kubevirt Virtual Machines and validating")
				appList := Inst().AppList
				numberOfVolumes := 2
				defer func() {
					Inst().AppList = appList
				}()
				Inst().AppList = []string{"kubevirt-cirros-cd-with-pvc"}
				Inst().CustomAppConfig["kubevirt-cirros-cd-with-pvc"] = scheduler.AppConfig{
					ClaimsCount: numberOfVolumes,
				}
				err := Inst().S.RescanSpecs(Inst().SpecDir, Inst().V.String())
				log.FailOnError(err, "Failed to rescan specs from %s for storage provider %s", Inst().SpecDir, Inst().V.String())
				for i := 0; i < 4; i++ {
					taskName := fmt.Sprintf("%d", i)
					appContexts := ScheduleApplications(taskName)
					kubevirtScheduledAppContexts = append(kubevirtScheduledAppContexts, appContexts...)
				}

				log.InfoD("Validating kubevirt applications")
				ValidateApplications(kubevirtScheduledAppContexts)
			} else {
				log.Warnf("Kubevirt is not installed. Skipping the step")
			}

		})

		Step("Validating applications for partial success", func() {
			log.InfoD("Validating applications")
			ValidateApplications(partialAppContexts)
		})

		Step("Create cloud credentials and backup locations", func() {
			log.InfoD("Creating cloud credentials and backup locations")
			providers := GetBackupProviders()
			backupLocationMap = make(map[string]string)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudAccountUid = uuid.New()
				cloudAccountName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				log.Infof("Creating a cloud credential [%s] with UID [%s] using [%s] as the provider", cloudAccountUid, cloudAccountName, provider)
				err := CreateCloudCredential(provider, cloudAccountName, cloudAccountUid, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential [%s] with UID [%s] using [%s] as the provider", cloudAccountName, BackupOrgID, provider))
				backupLocationName = fmt.Sprintf("%s-bl-%v", getGlobalBucketName(provider), time.Now().Unix())
				backupLocationUid = uuid.New()
				backupLocationMap[backupLocationUid] = backupLocationName
				bucketName := getGlobalBucketName(provider)
				log.Infof("Creating a backup location [%s] with UID [%s] using the [%s] bucket", backupLocationName, backupLocationUid, bucketName)
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUid, cloudAccountName, cloudAccountUid, bucketName, BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup location [%s] with UID [%s] using the bucket [%s]", backupLocationName, backupLocationUid, bucketName))
			}
		})

		Step("Create cloud credentials and backup locations for partial success validation", func() {
			log.InfoD("Create cloud credentials and backup locations for partial success validation")
			providers := GetBackupProviders()
			partialBackupLocationMap = make(map[string]string)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				partialCloudAccountUid = uuid.New()
				partialCloudAccountName = fmt.Sprintf("%s-%s-%v", "cred-partial-bkp", provider, time.Now().Unix())
				log.Infof("Creating a cloud credential for partial backup [%s] with UID [%s] using [%s] as the provider", partialCloudAccountUid, partialCloudAccountName, provider)
				err := CreateCloudCredential(provider, partialCloudAccountName, partialCloudAccountUid, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential [%s] with UID [%s] using [%s] as the provider", partialCloudAccountName, BackupOrgID, provider))
				partialBackupLocationName = fmt.Sprintf("%s-partial-bl-%v", getGlobalBucketName(provider), time.Now().Unix())
				partialBackupLocationUid = uuid.New()
				partialBackupLocationMap[partialBackupLocationUid] = partialBackupLocationName
				bucketName := getGlobalBucketName(provider)
				log.Infof("Creating a backup location [%s] with UID [%s] using the [%s] bucket", partialBackupLocationName, partialBackupLocationUid, bucketName)
				err = CreateBackupLocation(provider, partialBackupLocationName, partialBackupLocationUid, partialCloudAccountName, partialCloudAccountUid, bucketName, BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup location [%s] with UID [%s] using the bucket [%s]", partialBackupLocationName, partialBackupLocationUid, bucketName))
			}
		})

		Step("Create source and destination clusters", func() {
			log.InfoD("Creating source and destination clusters")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.Infof("Creating source [%s] and destination [%s] clusters", SourceClusterName, DestinationClusterName)
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, DestinationClusterName))
			srcClusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, SourceClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", SourceClusterName))
			dash.VerifyFatal(srcClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", SourceClusterName))
			srcClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			log.Infof("Cluster [%s] uid: [%s]", SourceClusterName, srcClusterUid)
			dstClusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, DestinationClusterName, ctx)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", DestinationClusterName))
			dash.VerifyFatal(dstClusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", DestinationClusterName))
			destClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, DestinationClusterName)
			log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster uid", DestinationClusterName))
			log.Infof("Cluster [%s] uid: [%s]", DestinationClusterName, destClusterUid)
		})
		Step("Create pre and post exec rules for applications", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			preRuleNames = make(map[string]string)
			preRuleUids = make(map[string]string)
			log.InfoD("Creating pre exec rules for applications %v", Inst().AppList)
			for _, appName := range Inst().AppList {
				log.Infof("Creating pre backup rule for application [%s]", appName)
				_, preRuleName, err := Inst().Backup.CreateRuleForBackup(appName, BackupOrgID, "pre")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of pre backup rule for application [%s]", appName))
				preRuleUid := ""
				if preRuleName != "" {
					preRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleName)
					log.FailOnError(err, "Fetching pre backup rule [%s] uid", preRuleName)
					log.Infof("Pre backup rule [%s] uid: [%s]", preRuleName, preRuleUid)
				}
				for i := 0; i < len(srcClusterAppNamespaces[appName]); i++ {
					preRuleNames[appName] = preRuleName
					preRuleUids[appName] = preRuleUid
				}
			}
			postRuleNames = make(map[string]string)
			postRuleUids = make(map[string]string)
			log.InfoD("Creating post exec rules for applications %v", Inst().AppList)
			for _, appName := range Inst().AppList {
				log.Infof("Creating post backup rule for application [%s]", appName)
				_, postRuleName, err := Inst().Backup.CreateRuleForBackup(appName, BackupOrgID, "post")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of post-backup rule for application [%s]", appName))
				postRuleUid := ""
				if postRuleName != "" {
					postRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleName)
					log.FailOnError(err, "Fetching post backup rule [%s] uid", postRuleName)
					log.Infof("Post backup rule [%s] uid: [%s]", postRuleName, postRuleUid)
				}
				for i := 0; i < len(srcClusterAppNamespaces[appName]); i++ {
					postRuleNames[appName] = postRuleName
					postRuleUids[appName] = postRuleUid
				}
			}
		})
		Step("Create backups with and without pre and post exec rules", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			backupToContextMapping = make(map[string][]*scheduler.Context)
			createBackupWithRulesTask := func(appName string) {
				namespace := srcClusterAppNamespaces[appName][0]
				backupName := fmt.Sprintf("%s-%s-%v-with-rules", BackupNamePrefix, namespace, time.Now().Unix())
				labelSelectors := make(map[string]string)
				log.InfoD("Creating a backup of namespace [%s] with pre and post exec rules", namespace)
				appContextsToBackup := FilterAppContextsByNamespace(srcClusterContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUid, appContextsToBackup, labelSelectors, BackupOrgID, srcClusterUid, preRuleNames[appName], preRuleUids[appName], postRuleNames[appName], postRuleUids[appName])
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup [%s]", backupName))
				err = IsFullBackup(backupName, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if backup [%s] is a full backup", backupName))
				backupWithRuleNames = SafeAppend(&mutex, backupWithRuleNames, backupName).([]string)
				//backupToContextMapping[backupName] = appContextsToBackup
				updateBackupToContextMapping(backupName, appContextsToBackup)

			}
			_ = TaskHandler(Inst().AppList, createBackupWithRulesTask, Parallel)
			createBackupWithoutRulesTask := func(appName string) {
				namespace := srcClusterAppNamespaces[appName][0]
				backupName := fmt.Sprintf("%s-%s-%v-without-rules", BackupNamePrefix, namespace, time.Now().Unix())
				labelSelectors := make(map[string]string)
				log.InfoD("Creating a backup of namespace [%s] without pre and post exec rules", namespace)
				appContextsToBackup := FilterAppContextsByNamespace(srcClusterContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUid, appContextsToBackup, labelSelectors, BackupOrgID, srcClusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup [%s]", backupName))
				backupWithoutRuleNames = SafeAppend(&mutex, backupWithoutRuleNames, backupName).([]string)
				//backupToContextMapping[backupName] = appContextsToBackup
				updateBackupToContextMapping(backupName, appContextsToBackup)
			}
			_ = TaskHandler(Inst().AppList, createBackupWithoutRulesTask, Parallel)
		})
		Step("Create a schedule policy", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			intervalInMins = 15
			log.InfoD("Creating a schedule policy with interval [%v] mins", intervalInMins)
			schedulePolicyName = fmt.Sprintf("interval-%v-%v", intervalInMins, time.Now().Unix())
			schedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, int64(intervalInMins), 5)
			err = Inst().Backup.BackupSchedulePolicy(schedulePolicyName, uuid.New(), BackupOrgID, schedulePolicyInfo)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule policy [%s] with interval [%v] mins", schedulePolicyName, intervalInMins))
			schedulePolicyUid, err = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, schedulePolicyName)
			log.FailOnError(err, "Fetching uid of schedule policy [%s]", schedulePolicyName)
			log.Infof("Schedule policy [%s] uid: [%s]", schedulePolicyName, schedulePolicyUid)
		})
		Step("Initiate a go routine to stop CR backups for PVCs", func() {
			// Making a list of PVCs to fail
			k8sCore := k8score.Instance()
			pvcList, err := k8sCore.GetPersistentVolumeClaims(partialAppNamespaces[0], make(map[string]string))
			log.FailOnError(err, fmt.Sprintf("error getting PVC list for namespace %s", partialAppNamespaces[0]))
			log.Infof("pvc list is %v", pvcList)
			for _, pvc := range pvcList.Items {
				log.Infof("pvc %v", pvc)
				failedVolumes = append(failedVolumes, &pvc)
			}
			log.Infof("Failed volumes are %v", failedVolumes)
			if len(failedVolumes) > 0 {
				partialAppContexts[0].SkipPodValidation = true
			}

			// Channel to signal termination of the go routine
			stopChans = make(map[int]chan struct{})
			stopChan := make(chan struct{})
			stopChans[0] = stopChan // Save the channel in the global map with index 0

			// Go routine to stop backups for the PVCs
			go func() {
				defer GinkgoRecover()
				timeoutChan := time.After(1 * time.Hour) // Create a timeout channel for 1 hour
				for {
					select {
					case <-stopChan:
						log.InfoD("Received signal to stop the go routine for stopping CR backups")
						return
					case <-timeoutChan:
						log.InfoD("Timeout reached. Stopping the go routine for stopping CR backups")
						return
					default:
						var wg1 sync.WaitGroup
						wg1.Add(len(failedVolumes))
						for _, pvc := range failedVolumes {
							go func(pvc *corev1.PersistentVolumeClaim) {
								defer GinkgoRecover()
								defer wg1.Done()
								log.Infof("Stopping all cs backups for %s [%s] in namespace %s", pvc.Name, pvc.Spec.VolumeName, pvc.Namespace)
								err = StopCloudsnapBackup(pvc.Name, pvc.Namespace)
								dash.VerifySafely(err, nil, fmt.Sprintf("error stopping all cs backups for %s [%s] in namespace %s", pvc.Name, pvc.Spec.VolumeName, pvc.Namespace))
							}(pvc)
						}
						wg1.Wait()
					}
				}
			}()
		})

		Step("Create a scheduled backup for partial backup validation", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.InfoD("Creating a new schedule policy for new apps for partial success validation before the upgrade")
			partialScheduleName = fmt.Sprintf("partial-backup-schedule-%v", RandomString(6))
			partialSchedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, int64(15), 5)
			err = Inst().Backup.BackupSchedulePolicy(partialScheduleName, uuid.New(), BackupOrgID, partialSchedulePolicyInfo)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of new schedule policy [%s]", partialScheduleName))
			partialSchedulePolicyUid, err = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, partialScheduleName)
			log.FailOnError(err, "Fetching uid of schedule policy [%s]", partialScheduleName)
			log.InfoD("Taking scheduled backup of namespaces [%s] before upgrade", partialAppNamespaces)
			partialScheduledBackupName = fmt.Sprintf("partial-scheduled-backup-%v", RandomString(6))
			_, err = CreateScheduleBackupWithoutCheck(partialScheduledBackupName, SourceClusterName, partialBackupLocationName, partialBackupLocationUid, partialAppNamespaces, nil, BackupOrgID, "", "", "", "", partialScheduleName, partialSchedulePolicyUid, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule backup with schedule [%s]", partialScheduleName))
		})

		Step("Verify the status of the first scheduled backup with failed volume", func() {
			log.InfoD("Verifying if the status of the first scheduled backup with failed volume is reported as Failed")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, partialScheduledBackupName, BackupOrgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching name of the first scheduled backup with schedule [%s]", firstScheduleBackupName))
			log.Infof("Validating if the first scheduled backup [%s] of schedule [%s] is Failed", partialScheduledBackupName, firstScheduleBackupName)
			err = BackupFailedCheck(firstScheduleBackupName, BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the backup [%s] of schedule [%s] is in failed state", firstScheduleBackupName, partialScheduledBackupName))
		})

		Step("Create schedule backup for each namespace", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			createSingleNSBackupTask := func(appName string) {
				namespace := srcClusterAppNamespaces[appName][0]
				labelSelectors := make(map[string]string)
				singleNSScheduleName := fmt.Sprintf("%s-single-namespace-schedule-%v", namespace, time.Now().Unix())
				log.InfoD("Creating schedule backup with schedule [%s] of source cluster namespace [%s]", singleNSScheduleName, namespace)
				err = CreateScheduleBackup(singleNSScheduleName, SourceClusterName, backupLocationName, backupLocationUid, []string{namespace},
					labelSelectors, BackupOrgID, preRuleNames[appName], preRuleUids[appName], postRuleNames[appName], postRuleUids[appName], schedulePolicyName, schedulePolicyUid, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule backup with schedule [%s]", singleNSScheduleName))
				firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, singleNSScheduleName, BackupOrgID)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching name of the first schedule backup with schedule [%s]", singleNSScheduleName))
				log.Infof("First schedule backup name: [%s]", firstScheduleBackupName)
				singleNSNamespaces = SafeAppend(&mutex, singleNSNamespaces, namespace).([]string)
				singleNSScheduleNames = SafeAppend(&mutex, singleNSScheduleNames, singleNSScheduleName).([]string)
			}
			_ = TaskHandler(Inst().AppList, createSingleNSBackupTask, Parallel)
		})
		Step("Create schedule backup of all namespaces", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, appName := range Inst().AppList {
				allNamespaces = append(allNamespaces, destClusterAppNamespaces[appName]...)
			}
			allNSScheduleName = fmt.Sprintf("%s-all-namespaces-schedule-%v", BackupNamePrefix, time.Now().Unix())
			labelSelectors := make(map[string]string)
			log.InfoD("Creating schedule backup with schedule [%s] of all namespaces of destination cluster [%s]", allNSScheduleName, allNamespaces)
			err = CreateScheduleBackup(allNSScheduleName, DestinationClusterName, backupLocationName, backupLocationUid, allNamespaces,
				labelSelectors, BackupOrgID, "", "", "", "", schedulePolicyName, schedulePolicyUid, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule backup with schedule [%s]", allNSScheduleName))
			firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, allNSScheduleName, BackupOrgID)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching name of the first schedule backup with schedule [%s]", allNSScheduleName))
			log.Infof("First schedule backup name: [%s]", firstScheduleBackupName)
		})
		Step("Upgrading px-backup", func() {
			LatestPxBackupVersionFromEnv := os.Getenv("TARGET_PXBACKUP_VERSION")
			if LatestPxBackupVersionFromEnv == "" {
				LatestPxBackupVersionFromEnv = LatestPxBackupVersion
			}
			log.InfoD("Upgrading px-backup to latest version [%s]", LatestPxBackupVersionFromEnv)
			err := PxBackupUpgrade(LatestPxBackupVersionFromEnv)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying completion of px-backup upgrade to latest version [%s]", LatestPxBackupVersionFromEnv))
			// Stork Version will be upgraded on both source and destination if env variable TARGET_STORK_VERSION is defined.

			targetStorkVersion := os.Getenv("TARGET_STORK_VERSION")
			if targetStorkVersion != "" {
				log.InfoD("Upgrade the stork version post backup upgrade")
				log.Infof("Upgrading stork version on source cluster to %s ", targetStorkVersion)
				err := UpgradeStorkVersion(targetStorkVersion)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of stork version upgrade to - %s on source cluster", targetStorkVersion))
				err = SetDestinationKubeConfig()
				log.FailOnError(err, "Switching context to destination cluster failed")
				log.Infof("Upgrading stork version on destination cluster to %s ", targetStorkVersion)
				err = UpgradeStorkVersion(targetStorkVersion)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of stork version upgrade to - %s on destination cluster", targetStorkVersion))
				err = SetSourceKubeConfig()
				log.FailOnError(err, "Switching context to source cluster failed")
			}
		})

		checkPxbVersionForKubevirt, err := CompareCurrentPxBackupVersion("2.7.2", (*version.Version).GreaterThanOrEqual)
		log.FailOnError(err, "Checking if current px-backup version is greater than or equal to 2.7.2")

		if checkPxbVersionForKubevirt {
			Step("Validating the backup type after upgrade for older backups", func() {
				log.InfoD("Validating the backup type after upgrade for older backups")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				backupEnumerateRequest := &api.BackupEnumerateRequest{
					OrgId: BackupOrgID,
				}
				resp, err := Inst().Backup.EnumerateBackup(ctx, backupEnumerateRequest)
				for _, b := range resp.Backups {
					log.InfoD("Validating backup [%s]", b.Name)
					dash.VerifyFatal(b.GetBackupObjectType().Type, api.BackupInfo_BackupObjectType_All, fmt.Sprintf("Verifying backup type of [%s]", b.Name))
				}
			})
		}

		Step("Create backups after px-backup upgrade with and without pre and post exec rules", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			createBackupWithRulesTask := func(appName string) {
				namespace := srcClusterAppNamespaces[appName][0]
				backupName := fmt.Sprintf("%s-%s-%v-with-rules", BackupNamePrefix, namespace, time.Now().Unix())
				labelSelectors := make(map[string]string)
				log.InfoD("Creating a backup of namespace [%s] after px-backup upgrade with pre and post exec rules", namespace)
				appContextsToBackup := FilterAppContextsByNamespace(srcClusterContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUid, appContextsToBackup, labelSelectors, BackupOrgID, srcClusterUid, preRuleNames[appName], preRuleUids[appName], postRuleNames[appName], postRuleUids[appName])
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup [%s]", backupName))
				backupAfterUpgradeWithRuleNames = SafeAppend(&mutex, backupAfterUpgradeWithRuleNames, backupName).([]string)
				backupToContextMapping[backupName] = appContextsToBackup
			}
			_ = TaskHandler(Inst().AppList, createBackupWithRulesTask, Parallel)
			createBackupWithoutRulesTask := func(appName string) {
				namespace := srcClusterAppNamespaces[appName][0]
				backupName := fmt.Sprintf("%s-%s-%v-without-rules", BackupNamePrefix, namespace, time.Now().Unix())
				labelSelectors := make(map[string]string)
				log.InfoD("Creating a backup of namespace [%s] after px-backup upgrade without pre and post exec rules", namespace)
				appContextsToBackup := FilterAppContextsByNamespace(srcClusterContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUid, appContextsToBackup, labelSelectors, BackupOrgID, srcClusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup [%s]", backupName))
				backupAfterUpgradeWithoutRuleNames = SafeAppend(&mutex, backupAfterUpgradeWithoutRuleNames, backupName).([]string)
				backupToContextMapping[backupName] = appContextsToBackup
			}
			_ = TaskHandler(Inst().AppList, createBackupWithoutRulesTask, Parallel)
		})
		Step("Validating the backup type after upgrade for old and new backups", func() {
			log.InfoD("Validating the backup type after upgrade for old and new backups")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			backupEnumerateRequest := &api.BackupEnumerateRequest{
				OrgId: BackupOrgID,
			}
			resp, err := Inst().Backup.EnumerateBackup(ctx, backupEnumerateRequest)
			for _, b := range resp.Backups {
				log.InfoD("Validating backup [%s]", b.Name)
				dash.VerifyFatal(b.GetBackupObjectType().Type, api.BackupInfo_BackupObjectType_All, fmt.Sprintf("Verifying backup type of [%s]", b.Name))
			}
		})

		Step("Validate the status of the scheduled backup with a failed volume after the upgrade and restore the same", func() {
			log.InfoD("Verifying if the latest scheduled backup is a backup with partial success")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			latestScheduleBackupName, err := GetNextScheduleBackupName(partialScheduledBackupName, 15, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching name of the latest schedule backup with schedule [%s]", partialScheduleName))
			log.Infof("Validating if the latest scheduled backup [%s] is of Partial Success", latestScheduleBackupName)
			err = BackupWithPartialSuccessCheck(latestScheduleBackupName, BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second, ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if the backup [%s] is in partial backup state", latestScheduleBackupName))
			if stopChan, exists := stopChans[0]; exists {
				close(stopChan) // Signal the go routine to terminate
				log.InfoD("Go routine for stopping CR backups has been signaled to stop")
			}
			log.InfoD("Restoring the scheduled backup with partial success")
			namespaceMapping := make(map[string]string)
			for _, namespace := range partialAppNamespaces {
				namespaceMapping[namespace] = namespace + RandomString(4)
			}
			log.Infof("Failed volumes are %v", failedVolumes)
			restoreName := fmt.Sprintf("%s-%s", "restore-partial-bkp", RandomString(4))
			log.InfoD("Restoring the schedule backup with partial success [%s] in cluster [%s] with restore [%s]", latestScheduleBackupName, DestinationClusterName, restoreName)
			err = CreatePartialRestoreWithValidation(ctx, restoreName, latestScheduleBackupName, namespaceMapping, make(map[string]string), DestinationClusterName, BackupOrgID, partialAppContexts, failedVolumes)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Creation and Validation of restore [%s]", restoreName))
			restoreNames = append(restoreNames, restoreName)
		})

		Step("Restore backups created before px-backup upgrade with and without pre and post exec rules", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.InfoD("Restoring backups [%s] created before px-backup upgrade with rules", backupWithRuleNames)
			for _, backupName := range backupWithRuleNames {
				namespaceMapping := make(map[string]string)
				storageClassMapping := make(map[string]string)
				restoreName := fmt.Sprintf("%s-%s-%v", "test-restore", backupName, time.Now().Unix())
				log.InfoD("Restoring backup [%s] in cluster [%s] with restore [%s]", backupName, DestinationClusterName, restoreName)
				err = CreateRestoreWithValidation(ctx, restoreName, backupName, namespaceMapping, storageClassMapping, DestinationClusterName, BackupOrgID, backupToContextMapping[backupName])
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration [%s] of backup [%s] in cluster [%s]", restoreName, backupName, DestinationClusterName))
				restoreNames = append(restoreNames, restoreName)
			}
			log.InfoD("Restoring backups [%s] created before px-backup upgrade without rules", backupWithoutRuleNames)
			for _, backupName := range backupWithoutRuleNames {
				namespaceMapping := make(map[string]string)
				storageClassMapping := make(map[string]string)
				restoreName := fmt.Sprintf("%s-%s-%v", "test-restore", backupName, time.Now().Unix())
				log.InfoD("Restoring backup [%s] in cluster [%s] with restore [%s]", backupName, DestinationClusterName, restoreName)
				err = CreateRestoreWithValidation(ctx, restoreName, backupName, namespaceMapping, storageClassMapping, DestinationClusterName, BackupOrgID, backupToContextMapping[backupName])
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration [%s] of backup [%s] in cluster [%s]", restoreName, backupName, restoreName))
				restoreNames = append(restoreNames, restoreName)
			}
		})
		Step("Restore backups created after px-backup upgrade with and without pre and post exec rules", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.InfoD("Restoring backups [%s] created after px-backup upgrade with rules", backupWithRuleNames)
			for _, backupName := range backupAfterUpgradeWithRuleNames {
				namespaceMapping := make(map[string]string)
				storageClassMapping := make(map[string]string)
				restoreName := fmt.Sprintf("%s-%s-%v", "test-restore", backupName, time.Now().Unix())
				log.InfoD("Restoring backup [%s] in cluster [%s] with restore [%s]", backupName, DestinationClusterName, restoreName)
				err = CreateRestoreWithValidation(ctx, restoreName, backupName, namespaceMapping, storageClassMapping, DestinationClusterName, BackupOrgID, backupToContextMapping[backupName])
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration [%s] of backup [%s] in cluster [%s]", restoreName, backupName, DestinationClusterName))
				restoreNames = append(restoreNames, restoreName)
			}
			log.InfoD("Restoring backups [%s] created after px-backup upgrade without rules", backupWithoutRuleNames)
			for _, backupName := range backupAfterUpgradeWithoutRuleNames {
				namespaceMapping := make(map[string]string)
				storageClassMapping := make(map[string]string)
				restoreName := fmt.Sprintf("%s-%s-%v", "test-restore", backupName, time.Now().Unix())
				log.InfoD("Restoring backup [%s] in cluster [%s] with restore [%s]", backupName, DestinationClusterName, restoreName)
				err = CreateRestoreWithValidation(ctx, restoreName, backupName, namespaceMapping, storageClassMapping, DestinationClusterName, BackupOrgID, backupToContextMapping[backupName])
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration [%s] of backup [%s] in cluster [%s]", restoreName, backupName, restoreName))
				restoreNames = append(restoreNames, restoreName)
			}
		})
		// First single namespace schedule backups are taken before px-backup upgrade
		Step("Restore first single namespace schedule backups", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			dash.VerifyFatal(err, nil, "Fetching px-central-admin ctx")
			restoreSingleNSBackupInVariousWaysTask := func(index int, namespace string) {
				firstSingleNSScheduleBackupName, err := GetFirstScheduleBackupName(ctx, singleNSScheduleNames[index], BackupOrgID)
				log.FailOnError(err, "Getting first backup name of schedule [%s] failed", singleNSScheduleNames[index])
				restoreConfigs := []struct {
					namePrefix          string
					namespaceMapping    map[string]string
					storageClassMapping map[string]string
					replacePolicy       ReplacePolicyType
				}{
					{
						"test-restore-single-ns",
						make(map[string]string),
						make(map[string]string),
						ReplacePolicyRetain,
					},
					{
						"test-custom-restore-single-ns",
						map[string]string{namespace: "custom-" + namespace},
						make(map[string]string),
						ReplacePolicyRetain,
					},
					{
						"test-replace-restore-single-ns",
						make(map[string]string),
						make(map[string]string),
						ReplacePolicyDelete,
					},
				}
				for _, config := range restoreConfigs {
					restoreName := fmt.Sprintf("%s-%s", config.namePrefix, RandomString(4))
					log.InfoD("Restoring first single namespace schedule backup [%s] in cluster [%s] with restore [%s] and namespace mapping %v", firstSingleNSScheduleBackupName, DestinationClusterName, restoreName, config.namespaceMapping)
					if config.replacePolicy == ReplacePolicyRetain {
						appContextsToBackup := FilterAppContextsByNamespace(srcClusterContexts, []string{namespace})
						err = CreateRestoreWithValidation(ctx, restoreName, firstSingleNSScheduleBackupName, config.namespaceMapping, config.storageClassMapping, DestinationClusterName, BackupOrgID, appContextsToBackup)
					} else if config.replacePolicy == ReplacePolicyDelete {
						err = CreateRestoreWithReplacePolicy(restoreName, firstSingleNSScheduleBackupName, config.namespaceMapping, DestinationClusterName, BackupOrgID, ctx, config.storageClassMapping, config.replacePolicy)
					}
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration [%s] of first single namespace schedule backup [%s] in cluster [%s]", restoreName, firstSingleNSScheduleBackupName, restoreName))
					restoreNames = SafeAppend(&mutex, restoreNames, restoreName).([]string)
				}
			}
			_ = TaskHandler(singleNSNamespaces, restoreSingleNSBackupInVariousWaysTask, Sequential)
		})
		// By the time the next single namespace schedule backups are taken, the px-backup upgrade would have been completed
		Step("Restore next single namespace schedule backups", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			dash.VerifyFatal(err, nil, "Fetching px-central-admin ctx")
			restoreSingleNSBackupInVariousWaysTask := func(index int, namespace string) {
				nextScheduleBackupName, err := GetNextScheduleBackupName(singleNSScheduleNames[index], time.Duration(intervalInMins), ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching next schedule backup name of schedule named [%s]", singleNSScheduleNames[index]))
				appContextsToBackup := FilterAppContextsByNamespace(srcClusterContexts, []string{namespace})
				err = BackupSuccessCheckWithValidation(ctx, nextScheduleBackupName, appContextsToBackup, BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success of next single namespace schedule backup [%s] of schedule %s", nextScheduleBackupName, singleNSScheduleNames[index]))
				log.InfoD("Next schedule backup name [%s]", nextScheduleBackupName)
				restoreConfigs := []struct {
					namePrefix          string
					namespaceMapping    map[string]string
					storageClassMapping map[string]string
					replacePolicy       ReplacePolicyType
				}{
					{
						"test-restore-single-ns",
						make(map[string]string),
						make(map[string]string),
						ReplacePolicyRetain,
					},
					{
						"test-custom-restore-single-ns",
						map[string]string{namespace: "custom" + namespace},
						make(map[string]string),
						ReplacePolicyRetain,
					},
					{
						"test-replace-restore-single-ns",
						make(map[string]string),
						make(map[string]string),
						ReplacePolicyDelete,
					},
				}
				for _, config := range restoreConfigs {
					restoreName := fmt.Sprintf("%s-%s", config.namePrefix, RandomString(4))
					log.InfoD("Restoring next single namespace schedule backup [%s] in cluster [%s] with restore [%s] and namespace mapping %v", nextScheduleBackupName, DestinationClusterName, restoreName, config.namespaceMapping)
					if config.replacePolicy == ReplacePolicyRetain {
						appContextsToBackup := FilterAppContextsByNamespace(srcClusterContexts, []string{namespace})
						err = CreateRestoreWithValidation(ctx, restoreName, nextScheduleBackupName, config.namespaceMapping, config.storageClassMapping, DestinationClusterName, BackupOrgID, appContextsToBackup)
					} else if config.replacePolicy == ReplacePolicyDelete {
						err = CreateRestoreWithReplacePolicy(restoreName, nextScheduleBackupName, config.namespaceMapping, DestinationClusterName, BackupOrgID, ctx, config.storageClassMapping, config.replacePolicy)
					}
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration [%s] of next single namespace schedule backup [%s] in cluster [%s]", restoreName, nextScheduleBackupName, restoreName))
					restoreNames = SafeAppend(&mutex, restoreNames, restoreName).([]string)
				}
			}
			_ = TaskHandler(singleNSNamespaces, restoreSingleNSBackupInVariousWaysTask, Sequential)
		})
		// First all namespaces schedule backup is taken before px-backup upgrade
		Step("Restore first all namespaces schedule backup", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			dash.VerifyFatal(err, nil, "Fetching px-central-admin ctx")
			firstAllNSScheduleBackupName, err := GetFirstScheduleBackupName(ctx, allNSScheduleName, BackupOrgID)
			log.FailOnError(err, "Getting first backup name of schedule [%s] failed", allNSScheduleName)
			restoreName := fmt.Sprintf("%s-%s", "test-restore-all-ns", RandomString(4))
			log.InfoD("Restoring first all namespaces schedule backup [%s] in cluster [%s] with restore [%s]", firstAllNSScheduleBackupName, SourceClusterName, restoreName)
			namespaceMapping := make(map[string]string)
			err = CreateRestoreWithValidation(ctx, restoreName, firstAllNSScheduleBackupName, namespaceMapping, make(map[string]string), SourceClusterName, BackupOrgID, destClusterContexts)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration [%s] of first all namespaces schedule backup [%s] in cluster [%s]", restoreName, firstAllNSScheduleBackupName, restoreName))
			restoreNames = append(restoreNames, restoreName)
		})
		// By the time the next all namespaces schedule backups are taken, the px-backup upgrade would have been completed
		Step("Restore next all namespaces schedule backup", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			dash.VerifyFatal(err, nil, "Fetching px-central-admin ctx")
			log.Infof("Switching cluster context to destination cluster as backup is created in destination cluster")
			err = SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			nextScheduleBackupName, err := GetNextCompletedScheduleBackupNameWithValidation(ctx, allNSScheduleName, destClusterContexts, time.Duration(intervalInMins))
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifyings next schedule backup name of schedule named [%s]", allNSScheduleName))
			log.InfoD("Next schedule backup name [%s]", nextScheduleBackupName)
			log.Infof("Switching cluster context back to source ")
			err = SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
			restoreName := fmt.Sprintf("%s-%s", "test-restore-all-ns", RandomString(4))
			log.InfoD("Restoring next all namespaces schedule backup [%s] in cluster [%s] with restore [%s]", nextScheduleBackupName, SourceClusterName, restoreName)
			namespaceMapping := make(map[string]string)
			err = CreateRestoreWithValidation(ctx, restoreName, nextScheduleBackupName, namespaceMapping, make(map[string]string), SourceClusterName, BackupOrgID, destClusterContexts)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration [%s] of next all namespaces schedule backup [%s] in cluster [%s]", restoreName, nextScheduleBackupName, restoreName))
			restoreNames = append(restoreNames, restoreName)
		})

		if IsKubevirtInstalled() && checkPxbVersionForKubevirt {
			Step("Taking backup of kubevirt VM", func() {
				log.InfoD("Taking backup of kubevirt VM")
				labelSelectors := make(map[string]string)
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				for _, appCtx := range kubevirtScheduledAppContexts {
					backupName := fmt.Sprintf("%s-%s", "vm-backup", RandomString(6))
					vmBackupNames = append(vmBackupNames, backupName)
					vms, err := GetAllVMsFromScheduledContexts([]*scheduler.Context{appCtx})
					log.FailOnError(err, "Failed to get VMs from scheduled contexts")
					var vmNames []string
					for _, v := range vms {
						vmNames = append(vmNames, v.Name)
					}
					log.Infof("VMs to be backed up - %v", vmNames)
					err = CreateVMBackupWithValidation(ctx, backupName, vms, SourceClusterName, backupLocationName, backupLocationUid, []*scheduler.Context{appCtx},
						labelSelectors, BackupOrgID, srcClusterUid, "", "", "", "", false)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation and validation of VM backup [%s]", backupName))
				}
			})

			Step("Validating the VM backup type", func() {
				log.InfoD("Validating the namespace backup type")
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				for _, backupName := range vmBackupNames {
					log.Infof("Inspecting backup [%s]", backupName)
					bkpUid, err := Inst().Backup.GetBackupUID(ctx, backupName, BackupOrgID)
					log.FailOnError(err, "Fetching backup uid")
					backupInspectRequest := &api.BackupInspectRequest{
						Name:  backupName,
						Uid:   bkpUid,
						OrgId: BackupOrgID,
					}
					backup, err := Inst().Backup.InspectBackup(ctx, backupInspectRequest)
					log.FailOnError(err, "Inspecting backup [%s]", backupName)
					dash.VerifyFatal(backup.Backup.GetBackupObjectType().Type, api.BackupInfo_BackupObjectType_VirtualMachine, fmt.Sprintf("Verifying backup type of [%s]", backupName))
				}
			})

		}

	})
	JustAfterEach(func() {
		allContexts := append(srcClusterContexts, destClusterContexts...)
		allContexts = append(allContexts, partialAppContexts...)
		defer EndPxBackupTorpedoTest(allContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		deleteSingleNSScheduleTask := func(scheduleName string) {
			log.InfoD("Deleting single namespace backup schedule [%s]", scheduleName)
			err = DeleteSchedule(scheduleName, SourceClusterName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of backup schedule [%s]", scheduleName))
		}
		_ = TaskHandler(singleNSScheduleNames, deleteSingleNSScheduleTask, Parallel)
		log.InfoD("Deleting all namespaces backup schedule [%s]", allNSScheduleName)
		err = DeleteSchedule(allNSScheduleName, SourceClusterName, BackupOrgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of backup schedule [%s]", allNSScheduleName))
		log.InfoD("Deleting partial backup schedule [%s]", partialScheduledBackupName)
		err = DeleteSchedule(partialScheduledBackupName, SourceClusterName, BackupOrgID, ctx)
		dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of backup schedule [%s]", partialScheduledBackupName))
		log.InfoD("Deleting pre exec rules %s", preRuleNames)
		for _, preRuleName := range preRuleNames {
			if preRuleName != "" {
				err := DeleteRule(preRuleName, BackupOrgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of pre backup rule [%s]", preRuleName))
			}
		}
		log.InfoD("Deleting post exec rules %s", postRuleNames)
		for _, postRuleName := range postRuleNames {
			if postRuleName != "" {
				err := DeleteRule(postRuleName, BackupOrgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of post-backup rule [%s]", postRuleName))
			}
		}
		log.InfoD("Deleting schedule policy [%s]", schedulePolicyName)
		err = Inst().Backup.DeleteBackupSchedulePolicy(BackupOrgID, []string{schedulePolicyName})
		dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of schedule policy [%s]", schedulePolicyName))
		log.InfoD("Deleting restores %s in cluster [%s]", restoreNames, DestinationClusterName)
		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of restore [%s]", restoreName))
		}
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "Switching context to destination cluster failed")
		ValidateAndDestroy(destClusterContexts, opts)
		err = SetSourceKubeConfig()
		log.FailOnError(err, "Switching context to source cluster failed")
		err = DestroyAppsWithData(srcClusterContexts, opts, controlChannel, errorGroup)
		log.FailOnError(err, "Data validations failed")
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudAccountName, cloudAccountUid, ctx)
		CleanupCloudSettingsAndClusters(partialBackupLocationMap, partialCloudAccountName, partialCloudAccountUid, ctx)
	})
})

var _ = Describe("{PXBackupClusterUpgradeTest}", Label(TestCaseLabelsMap[PXBackupClusterUpgradeTest]...), func() {
	var (
		numDeployments                     int
		srcClusterContexts                 []*scheduler.Context
		srcClusterAppNamespaces            map[string][]string
		destClusterContexts                []*scheduler.Context
		destClusterAppNamespaces           map[string][]string
		cloudAccountUid                    string
		cloudAccountName                   string
		backupLocationName                 string
		backupLocationUid                  string
		backupLocationMap                  map[string]string
		srcClusterUid                      string
		preRuleNames                       map[string]string
		preRuleUids                        map[string]string
		postRuleNames                      map[string]string
		postRuleUids                       map[string]string
		backupToContextMapping             map[string][]*scheduler.Context
		backupWithoutRuleNames             []string
		backupWithRuleNames                []string
		schedulePolicyName                 string
		schedulePolicyUid                  string
		intervalInMins                     int
		singleNSNamespaces                 []string
		singleNSScheduleNames              []string
		backupAfterUpgradeWithoutRuleNames []string
		backupAfterUpgradeWithRuleNames    []string
		restoreNames                       []string
		mutex                              sync.Mutex
	)
	updateBackupToContextMapping := func(backupName string, appContextsToBackup []*scheduler.Context) {
		mutex.Lock()
		defer mutex.Unlock()
		backupToContextMapping[backupName] = appContextsToBackup
	}
	JustBeforeEach(func() {
		StartPxBackupTorpedoTest("PXBackupClusterUpgradeTest", "Validates end-to-end backup and restore operations with cluster upgrade", nil, 296424, Kshithijiyer, Q1FY25)
		log.Infof("Scheduling applications")
		numDeployments = Inst().GlobalScaleFactor
		if len(Inst().AppList) == 1 && numDeployments < 2 {
			numDeployments = 2
		}
		destClusterContexts = make([]*scheduler.Context, 0)
		destClusterAppNamespaces = make(map[string][]string, 0)
		log.InfoD("Scheduling applications in destination cluster")
		err := SetDestinationKubeConfig()
		log.FailOnError(err, "Switching context to destination cluster failed")
		for i := 0; i < numDeployments; i++ {
			taskName := fmt.Sprintf("dst-%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			destClusterContexts = append(destClusterContexts, appContexts...)
			for index, ctx := range appContexts {
				appName := Inst().AppList[index]
				ctx.ReadinessTimeout = AppReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				log.InfoD("Scheduled application [%s] in destination cluster in namespace [%s]", appName, namespace)
				destClusterAppNamespaces[appName] = append(destClusterAppNamespaces[appName], namespace)
			}
		}
		srcClusterContexts = make([]*scheduler.Context, 0)
		srcClusterAppNamespaces = make(map[string][]string, 0)
		log.InfoD("Scheduling applications in source cluster")
		err = SetSourceKubeConfig()
		log.FailOnError(err, "Switching context to source cluster failed")
		for i := 0; i < numDeployments; i++ {
			taskName := fmt.Sprintf("src-%s-%d", TaskNamePrefix, i)
			appContexts := ScheduleApplications(taskName)
			srcClusterContexts = append(srcClusterContexts, appContexts...)
			for index, ctx := range appContexts {
				appName := Inst().AppList[index]
				ctx.ReadinessTimeout = AppReadinessTimeout
				namespace := GetAppNamespace(ctx, taskName)
				log.InfoD("Scheduled application [%s] in source cluster in namespace [%s]", appName, namespace)
				srcClusterAppNamespaces[appName] = append(srcClusterAppNamespaces[appName], namespace)
			}
		}
	})
	It("PX-Backup End-to-End Backup and Restore with cluster Upgrade", func() {
		Step("Validate app namespaces in destination cluster", func() {
			log.InfoD("Validating app namespaces in destination cluster")
			err := SetDestinationKubeConfig()
			log.FailOnError(err, "Switching context to destination cluster failed")
			ValidateApplications(destClusterContexts)
		})
		Step("Validate app namespaces in source cluster", func() {
			log.InfoD("Validating app namespaces in source cluster")
			err := SetSourceKubeConfig()
			log.FailOnError(err, "Switching context to source cluster failed")
			ValidateApplications(srcClusterContexts)
		})
		Step("Create cloud credentials and backup locations", func() {
			log.InfoD("Creating cloud credentials and backup locations")
			providers := GetBackupProviders()
			backupLocationMap = make(map[string]string)
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			for _, provider := range providers {
				cloudAccountUid = uuid.New()
				cloudAccountName = fmt.Sprintf("%s-%s-%v", "cred", provider, time.Now().Unix())
				log.Infof("Creating a cloud credential [%s] with UID [%s] using [%s] as the provider", cloudAccountUid, cloudAccountName, provider)
				err := CreateCloudCredential(provider, cloudAccountName, cloudAccountUid, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of cloud credential [%s] with UID [%s] using [%s] as the provider", cloudAccountName, BackupOrgID, provider))
				backupLocationName = fmt.Sprintf("%s-bl-%v", getGlobalBucketName(provider), time.Now().Unix())
				backupLocationUid = uuid.New()
				backupLocationMap[backupLocationUid] = backupLocationName
				bucketName := getGlobalBucketName(provider)
				log.Infof("Creating a backup location [%s] with UID [%s] using the [%s] bucket", backupLocationName, backupLocationUid, bucketName)
				err = CreateBackupLocation(provider, backupLocationName, backupLocationUid, cloudAccountName, cloudAccountUid, bucketName, BackupOrgID, "", true)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup location [%s] with UID [%s] using the bucket [%s]", backupLocationName, backupLocationUid, bucketName))
			}
		})
		Step("Create source and destination clusters", func() {
			log.InfoD("Creating source and destination clusters")
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			log.Infof("Creating source [%s] and destination [%s] clusters", SourceClusterName, DestinationClusterName)
			err = CreateApplicationClusters(BackupOrgID, "", "", ctx)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of source [%s] and destination [%s] clusters with px-central-admin ctx", SourceClusterName, DestinationClusterName))

			clusters := []string{SourceClusterName, DestinationClusterName}
			for _, c := range clusters {
				clusterStatus, err := Inst().Backup.GetClusterStatus(BackupOrgID, c, ctx)
				log.FailOnError(err, fmt.Sprintf("Fetching [%s] cluster status", c))
				dash.VerifyFatal(clusterStatus, api.ClusterInfo_StatusInfo_Online, fmt.Sprintf("Verifying if [%s] cluster is online", c))
			}
			srcClusterUid, err = Inst().Backup.GetClusterUID(ctx, BackupOrgID, SourceClusterName)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching [%s] cluster uid", SourceClusterName))
			log.Infof("Cluster [%s] uid: [%s]", SourceClusterName, srcClusterUid)
		})
		Step("Create pre and post exec rules for applications", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			preRuleNames = make(map[string]string)
			preRuleUids = make(map[string]string)
			log.InfoD("Creating pre exec rules for applications %v", Inst().AppList)
			for _, appName := range Inst().AppList {
				log.Infof("Creating pre backup rule for application [%s]", appName)
				_, preRuleName, err := Inst().Backup.CreateRuleForBackup(appName, BackupOrgID, "pre")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of pre backup rule for application [%s]", appName))
				preRuleUid := ""
				if preRuleName != "" {
					preRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, preRuleName)
					log.FailOnError(err, "Fetching pre backup rule [%s] uid", preRuleName)
					log.Infof("Pre backup rule [%s] uid: [%s]", preRuleName, preRuleUid)
				}
				preRuleNames[appName] = preRuleName
				preRuleUids[appName] = preRuleUid
			}
			postRuleNames = make(map[string]string)
			postRuleUids = make(map[string]string)
			log.InfoD("Creating post exec rules for applications %v", Inst().AppList)
			for _, appName := range Inst().AppList {
				log.Infof("Creating post backup rule for application [%s]", appName)
				_, postRuleName, err := Inst().Backup.CreateRuleForBackup(appName, BackupOrgID, "post")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of post-backup rule for application [%s]", appName))
				postRuleUid := ""
				if postRuleName != "" {
					postRuleUid, err = Inst().Backup.GetRuleUid(BackupOrgID, ctx, postRuleName)
					log.FailOnError(err, "Fetching post backup rule [%s] uid", postRuleName)
					log.Infof("Post backup rule [%s] uid: [%s]", postRuleName, postRuleUid)
				}
				for i := 0; i < len(srcClusterAppNamespaces[appName]); i++ {
					postRuleNames[appName] = postRuleName
					postRuleUids[appName] = postRuleUid
				}

			}
		})
		Step("Create backups with and without pre and post exec rules", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			backupToContextMapping = make(map[string][]*scheduler.Context, 0)
			createBackupWithRulesTask := func(appName string) {
				namespace := srcClusterAppNamespaces[appName][0]
				backupName := fmt.Sprintf("%s-%s-%v-%s-with-rules", BackupNamePrefix, namespace, time.Now().Unix(), RandomString(4))
				labelSelectors := make(map[string]string)
				log.InfoD("Creating a backup of namespace [%s] with pre and post exec rules", namespace)
				appContextsToBackup := FilterAppContextsByNamespace(srcClusterContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUid, appContextsToBackup, labelSelectors, BackupOrgID, srcClusterUid, preRuleNames[appName], preRuleUids[appName], postRuleNames[appName], postRuleUids[appName])
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup [%s]", backupName))
				err = IsFullBackup(backupName, BackupOrgID, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying if backup [%s] is a full backup", backupName))
				backupWithRuleNames = SafeAppend(&mutex, backupWithRuleNames, backupName).([]string)
				//backupToContextMapping[backupName] = appContextsToBackup
				updateBackupToContextMapping(backupName, appContextsToBackup)

			}
			_ = TaskHandler(Inst().AppList, createBackupWithRulesTask, Parallel)
			createBackupWithoutRulesTask := func(appName string) {
				namespace := srcClusterAppNamespaces[appName][0]
				backupName := fmt.Sprintf("%s-%s-%v-without-rules", BackupNamePrefix, namespace, time.Now().Unix())
				labelSelectors := make(map[string]string)
				log.InfoD("Creating a backup of namespace [%s] without pre and post exec rules", namespace)
				appContextsToBackup := FilterAppContextsByNamespace(srcClusterContexts, []string{namespace})
				err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUid, appContextsToBackup, labelSelectors, BackupOrgID, srcClusterUid, "", "", "", "")
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup [%s]", backupName))
				backupWithoutRuleNames = SafeAppend(&mutex, backupWithoutRuleNames, backupName).([]string)
				//backupToContextMapping[backupName] = appContextsToBackup
				updateBackupToContextMapping(backupName, appContextsToBackup)
			}
			_ = TaskHandler(Inst().AppList, createBackupWithoutRulesTask, Parallel)
		})
		Step("Create a schedule policy", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			intervalInMins = 15
			log.InfoD("Creating a schedule policy with interval [%v] mins", intervalInMins)
			schedulePolicyName = fmt.Sprintf("interval-%v-%v", intervalInMins, time.Now().Unix())
			schedulePolicyInfo := Inst().Backup.CreateIntervalSchedulePolicy(5, int64(intervalInMins), 5)
			err = Inst().Backup.BackupSchedulePolicy(schedulePolicyName, uuid.New(), BackupOrgID, schedulePolicyInfo)
			dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule policy [%s] with interval [%v] mins", schedulePolicyName, intervalInMins))
			schedulePolicyUid, err = Inst().Backup.GetSchedulePolicyUid(BackupOrgID, ctx, schedulePolicyName)
			log.FailOnError(err, "Fetching uid of schedule policy [%s]", schedulePolicyName)
			log.Infof("Schedule policy [%s] uid: [%s]", schedulePolicyName, schedulePolicyUid)
		})
		Step("Create schedule backup for each namespace", func() {
			ctx, err := backup.GetAdminCtxFromSecret()
			log.FailOnError(err, "Fetching px-central-admin ctx")
			createSingleNSBackupTask := func(appName string) {
				namespace := srcClusterAppNamespaces[appName][0]
				labelSelectors := make(map[string]string)
				singleNSScheduleName := fmt.Sprintf("%s-single-namespace-schedule-%v", namespace, time.Now().Unix())
				log.InfoD("Creating schedule backup with schedule [%s] of source cluster namespace [%s]", singleNSScheduleName, namespace)
				err = CreateScheduleBackup(singleNSScheduleName, SourceClusterName, backupLocationName, backupLocationUid, []string{namespace},
					labelSelectors, BackupOrgID, preRuleNames[appName], preRuleUids[appName], postRuleNames[appName], postRuleUids[appName], schedulePolicyName, schedulePolicyUid, ctx)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of schedule backup with schedule [%s]", singleNSScheduleName))
				firstScheduleBackupName, err := GetFirstScheduleBackupName(ctx, singleNSScheduleName, BackupOrgID)
				dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching name of the first schedule backup with schedule [%s]", singleNSScheduleName))
				log.Infof("First schedule backup name: [%s]", firstScheduleBackupName)
				singleNSNamespaces = SafeAppend(&mutex, singleNSNamespaces, namespace).([]string)
				singleNSScheduleNames = SafeAppend(&mutex, singleNSScheduleNames, singleNSScheduleName).([]string)
			}
			_ = TaskHandler(Inst().AppList, createSingleNSBackupTask, Parallel)
		})
		var versions []string
		if len(Inst().SchedUpgradeHops) > 0 {
			versions = strings.Split(Inst().SchedUpgradeHops, ",")
		}
		dash.VerifyFatal(len(versions) > 0, true, "Check if upgrade versions provided is provided")

		for _, version := range versions {
			Step("Upgrading K8s cluster", func() {

				log.InfoD("verify [%s] upgrade to [%s] is successful", Inst().S.String(), version)

				err := SwitchBothKubeConfigANDContext("source")
				dash.VerifyFatal(err, nil, "Switching context and kubeconfig to source cluster")

				err = Inst().S.UpgradeScheduler(version)
				dash.VerifyFatal(err, nil, fmt.Sprintf("verify [%s] upgrade to [%s] is successful", Inst().S.String(), version))
				PrintK8sClusterInfo()

				err = SwitchBothKubeConfigANDContext("destination")
				dash.VerifyFatal(err, nil, "Switching context and Kubeconfig to destination cluster")

				err = Inst().S.UpgradeScheduler(version)
				dash.VerifyFatal(err, nil, fmt.Sprintf("verify [%s] upgrade to [%s] is successful", Inst().S.String(), version))
				PrintK8sClusterInfo()

				err = SwitchBothKubeConfigANDContext("source")
				dash.VerifyFatal(err, nil, "Switching context and kubeconfig to source cluster")

				// TODO: Change this to a logic with DoRetryTimeout
				log.InfoD("Waiting for nodes to stabilise")
				time.Sleep(120 * time.Minute)

			})
			if Inst().Provisioner == "portworx" {
				Step("validate storage components", func() {

					err := SwitchBothKubeConfigANDContext("source")
					dash.VerifyFatal(err, nil, "Switching context and kubeconfig to source cluster")

					// Update NodeRegistry, this is needed as node names and IDs might change after upgrade
					err = Inst().S.RefreshNodeRegistry()
					log.FailOnError(err, "Refresh Node Registry failed")

					// Refresh Driver Endpoints
					err = Inst().V.RefreshDriverEndpoints()
					log.FailOnError(err, "Refresh Driver Endpoints failed")

					urlToParse := fmt.Sprintf("%s/%s", Inst().StorageDriverUpgradeEndpointURL, Inst().StorageDriverUpgradeEndpointVersion)
					u, err := url.Parse(urlToParse)
					log.FailOnError(err, fmt.Sprintf("error parsing PX version the url [%s]", urlToParse))
					err = Inst().V.ValidateDriver(u.String(), true)
					dash.VerifyFatal(err, nil, fmt.Sprintf("verify volume driver after upgrade to %s", version))

					// Printing cluster node info after the upgrade
					PrintK8sClusterInfo()

					err = SwitchBothKubeConfigANDContext("destination")
					dash.VerifyFatal(err, nil, "Switching context and Kubeconfig to destination cluster")

					// Update NodeRegistry, this is needed as node names and IDs might change after upgrade
					err = Inst().S.RefreshNodeRegistry()
					log.FailOnError(err, "Refresh Node Registry failed")

					// Refresh Driver Endpoints
					err = Inst().V.RefreshDriverEndpoints()
					log.FailOnError(err, "Refresh Driver Endpoints failed")

					urlToParse = fmt.Sprintf("%s/%s", Inst().StorageDriverUpgradeEndpointURL, Inst().StorageDriverUpgradeEndpointVersion)
					u, err = url.Parse(urlToParse)
					log.FailOnError(err, fmt.Sprintf("error parsing PX version the url [%s]", urlToParse))
					err = Inst().V.ValidateDriver(u.String(), true)
					dash.VerifyFatal(err, nil, fmt.Sprintf("verify volume driver after upgrade to %s", version))

					// Printing cluster node info after the upgrade
					PrintK8sClusterInfo()

					err = SwitchBothKubeConfigANDContext("source")
					dash.VerifyFatal(err, nil, "Switching context and kubeconfig to source cluster")
				})
			}

			Step("Create backups after cluster upgrade with and without pre and post exec rules", func() {
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				createBackupWithRulesTask := func(appName string) {
					namespace := srcClusterAppNamespaces[appName][0]
					backupName := fmt.Sprintf("%s-%s-%v-with-rules", BackupNamePrefix, namespace, time.Now().Unix())
					labelSelectors := make(map[string]string)
					log.InfoD("Creating a backup of namespace [%s] after cluster upgrade with pre and post exec rules", namespace)
					appContextsToBackup := FilterAppContextsByNamespace(srcClusterContexts, []string{namespace})
					err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUid, appContextsToBackup, labelSelectors, BackupOrgID, srcClusterUid, preRuleNames[appName], preRuleUids[appName], postRuleNames[appName], postRuleUids[appName])
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup [%s]", backupName))
					backupAfterUpgradeWithRuleNames = SafeAppend(&mutex, backupAfterUpgradeWithRuleNames, backupName).([]string)
					backupToContextMapping[backupName] = appContextsToBackup
				}
				_ = TaskHandler(Inst().AppList, createBackupWithRulesTask, Parallel)
				createBackupWithoutRulesTask := func(appName string) {
					namespace := srcClusterAppNamespaces[appName][0]
					backupName := fmt.Sprintf("%s-%s-%v-without-rules", BackupNamePrefix, namespace, time.Now().Unix())
					labelSelectors := make(map[string]string)
					log.InfoD("Creating a backup of namespace [%s] after cluster upgrade without pre and post exec rules", namespace)
					appContextsToBackup := FilterAppContextsByNamespace(srcClusterContexts, []string{namespace})
					err = CreateBackupWithValidation(ctx, backupName, SourceClusterName, backupLocationName, backupLocationUid, appContextsToBackup, labelSelectors, BackupOrgID, srcClusterUid, "", "", "", "")
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying creation of backup [%s]", backupName))
					backupAfterUpgradeWithoutRuleNames = SafeAppend(&mutex, backupAfterUpgradeWithoutRuleNames, backupName).([]string)
					backupToContextMapping[backupName] = appContextsToBackup
				}
				_ = TaskHandler(Inst().AppList, createBackupWithoutRulesTask, Parallel)
			})
			Step("Restore backups created before cluster upgrade with and without pre and post exec rules", func() {
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				log.InfoD("Restoring backups [%s] created before cluster upgrade with rules", backupWithRuleNames)
				for _, backupName := range backupWithRuleNames {
					namespaceMapping := make(map[string]string)
					storageClassMapping := make(map[string]string)
					restoreName := fmt.Sprintf("%s-%s-%v", "test-restore", backupName, time.Now().Unix())
					log.InfoD("Restoring backup [%s] in cluster [%s] with restore [%s]", backupName, DestinationClusterName, restoreName)
					err = CreateRestoreWithValidation(ctx, restoreName, backupName, namespaceMapping, storageClassMapping, DestinationClusterName, BackupOrgID, backupToContextMapping[backupName])
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration [%s] of backup [%s] in cluster [%s]", restoreName, backupName, DestinationClusterName))
					restoreNames = append(restoreNames, restoreName)
				}
				log.InfoD("Restoring backups [%s] created before cluster upgrade without rules", backupWithoutRuleNames)
				for _, backupName := range backupWithoutRuleNames {
					namespaceMapping := make(map[string]string)
					storageClassMapping := make(map[string]string)
					restoreName := fmt.Sprintf("%s-%s-%v", "test-restore", backupName, time.Now().Unix())
					log.InfoD("Restoring backup [%s] in cluster [%s] with restore [%s]", backupName, DestinationClusterName, restoreName)
					err = CreateRestoreWithValidation(ctx, restoreName, backupName, namespaceMapping, storageClassMapping, DestinationClusterName, BackupOrgID, backupToContextMapping[backupName])
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration [%s] of backup [%s] in cluster [%s]", restoreName, backupName, restoreName))
					restoreNames = append(restoreNames, restoreName)
				}
			})
			Step("Restore backups created after cluster upgrade with and without pre and post exec rules", func() {
				ctx, err := backup.GetAdminCtxFromSecret()
				log.FailOnError(err, "Fetching px-central-admin ctx")
				log.InfoD("Restoring backups [%s] created after px-backup upgrade with rules", backupWithRuleNames)
				for _, backupName := range backupAfterUpgradeWithRuleNames {
					namespaceMapping := make(map[string]string)
					storageClassMapping := make(map[string]string)
					restoreName := fmt.Sprintf("%s-%s-%v", "test-restore", backupName, time.Now().Unix())
					log.InfoD("Restoring backup [%s] in cluster [%s] with restore [%s]", backupName, DestinationClusterName, restoreName)
					err = CreateRestoreWithValidation(ctx, restoreName, backupName, namespaceMapping, storageClassMapping, DestinationClusterName, BackupOrgID, backupToContextMapping[backupName])
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration [%s] of backup [%s] in cluster [%s]", restoreName, backupName, DestinationClusterName))
					restoreNames = append(restoreNames, restoreName)
				}
				log.InfoD("Restoring backups [%s] created after cluster upgrade without rules", backupWithoutRuleNames)
				for _, backupName := range backupAfterUpgradeWithoutRuleNames {
					namespaceMapping := make(map[string]string)
					storageClassMapping := make(map[string]string)
					restoreName := fmt.Sprintf("%s-%s-%v", "test-restore", backupName, time.Now().Unix())
					log.InfoD("Restoring backup [%s] in cluster [%s] with restore [%s]", backupName, DestinationClusterName, restoreName)
					err = CreateRestoreWithValidation(ctx, restoreName, backupName, namespaceMapping, storageClassMapping, DestinationClusterName, BackupOrgID, backupToContextMapping[backupName])
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration [%s] of backup [%s] in cluster [%s]", restoreName, backupName, restoreName))
					restoreNames = append(restoreNames, restoreName)
				}
			})
			// By the time the next single namespace schedule backups are taken, the cluster upgrade would have been completed
			Step("Restore latest single namespace schedule backups", func() {
				ctx, err := backup.GetAdminCtxFromSecret()
				dash.VerifyFatal(err, nil, "Fetching px-central-admin ctx")
				restoreSingleNSBackupInVariousWaysTask := func(index int, namespace string) {
					nextScheduleBackupName, err := GetNextScheduleBackupName(singleNSScheduleNames[index], time.Duration(intervalInMins), ctx)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Fetching next schedule backup name of schedule named [%s]", singleNSScheduleNames[index]))
					appContextsToBackup := FilterAppContextsByNamespace(srcClusterContexts, []string{namespace})
					err = BackupSuccessCheckWithValidation(ctx, nextScheduleBackupName, appContextsToBackup, BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
					if err != nil {
						log.InfoD("Attempting for the second time")
						nextScheduleBackupName, err = GetNextScheduleBackupName(singleNSScheduleNames[index], time.Duration(intervalInMins), ctx)
					}
					err = BackupSuccessCheckWithValidation(ctx, nextScheduleBackupName, appContextsToBackup, BackupOrgID, MaxWaitPeriodForBackupCompletionInMinutes*time.Minute, 30*time.Second)
					dash.VerifyFatal(err, nil, fmt.Sprintf("Verification of success of next single namespace schedule backup [%s] of schedule %s", nextScheduleBackupName, singleNSScheduleNames[index]))
					log.InfoD("Next schedule backup name [%s]", nextScheduleBackupName)
					restoreConfigs := []struct {
						namePrefix          string
						namespaceMapping    map[string]string
						storageClassMapping map[string]string
						replacePolicy       ReplacePolicyType
					}{
						{
							"test-restore-single-ns",
							make(map[string]string),
							make(map[string]string),
							ReplacePolicyRetain,
						},
						{
							"test-custom-restore-single-ns",
							map[string]string{namespace: "custom" + namespace},
							make(map[string]string),
							ReplacePolicyRetain,
						},
						{
							"test-replace-restore-single-ns",
							make(map[string]string),
							make(map[string]string),
							ReplacePolicyDelete,
						},
					}
					for _, config := range restoreConfigs {
						restoreName := fmt.Sprintf("%s-%s", config.namePrefix, RandomString(4))
						log.InfoD("Restoring next single namespace schedule backup [%s] in cluster [%s] with restore [%s] and namespace mapping %v", nextScheduleBackupName, DestinationClusterName, restoreName, config.namespaceMapping)
						if config.replacePolicy == ReplacePolicyRetain {
							appContextsToBackup := FilterAppContextsByNamespace(srcClusterContexts, []string{namespace})
							err = CreateRestoreWithValidation(ctx, restoreName, nextScheduleBackupName, config.namespaceMapping, config.storageClassMapping, DestinationClusterName, BackupOrgID, appContextsToBackup)
						} else if config.replacePolicy == ReplacePolicyDelete {
							err = CreateRestoreWithReplacePolicy(restoreName, nextScheduleBackupName, config.namespaceMapping, DestinationClusterName, BackupOrgID, ctx, config.storageClassMapping, config.replacePolicy)
						}
						dash.VerifyFatal(err, nil, fmt.Sprintf("Verifying restoration [%s] of next single namespace schedule backup [%s] in cluster [%s]", restoreName, nextScheduleBackupName, restoreName))
						restoreNames = SafeAppend(&mutex, restoreNames, restoreName).([]string)
					}
				}
				_ = TaskHandler(singleNSNamespaces, restoreSingleNSBackupInVariousWaysTask, Sequential)
			})
		}
	})
	JustAfterEach(func() {
		allContexts := append(srcClusterContexts, destClusterContexts...)
		defer EndPxBackupTorpedoTest(allContexts)
		ctx, err := backup.GetAdminCtxFromSecret()
		log.FailOnError(err, "Fetching px-central-admin ctx")
		deleteSingleNSScheduleTask := func(scheduleName string) {
			log.InfoD("Deleting single namespace backup schedule [%s]", scheduleName)
			err = DeleteSchedule(scheduleName, SourceClusterName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of backup schedule [%s]", scheduleName))
		}
		_ = TaskHandler(singleNSScheduleNames, deleteSingleNSScheduleTask, Parallel)
		log.InfoD("Deleting post exec rules %s", postRuleNames)
		for _, postRuleName := range postRuleNames {
			if postRuleName != "" {
				err := DeleteRule(postRuleName, BackupOrgID, ctx)
				dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of post-backup rule [%s]", postRuleName))
			}
		}
		log.InfoD("Deleting schedule policy [%s]", schedulePolicyName)
		err = Inst().Backup.DeleteBackupSchedulePolicy(BackupOrgID, []string{schedulePolicyName})
		dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of schedule policy [%s]", schedulePolicyName))
		log.InfoD("Deleting restores %s in cluster [%s]", restoreNames, DestinationClusterName)
		for _, restoreName := range restoreNames {
			err = DeleteRestore(restoreName, BackupOrgID, ctx)
			dash.VerifySafely(err, nil, fmt.Sprintf("Verifying deletion of restore [%s]", restoreName))
		}
		opts := make(map[string]bool)
		opts[SkipClusterScopedObjects] = true
		err = SetDestinationKubeConfig()
		log.FailOnError(err, "Switching context to destination cluster failed")
		DestroyApps(destClusterContexts, opts)
		err = SetSourceKubeConfig()
		log.FailOnError(err, "Switching context to source cluster failed")
		DestroyApps(srcClusterContexts, opts)
		CleanupCloudSettingsAndClusters(backupLocationMap, cloudAccountName, cloudAccountUid, ctx)
	})
})
